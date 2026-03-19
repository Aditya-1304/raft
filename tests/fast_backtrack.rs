use raft::{
    core::node::RaftNode,
    entry::LogEntry,
    message::{AppendEntriesRequest, AppendEntriesResponse, Envelope, Message},
    storage::mem::MemStorage,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{HardState, Role},
};

type TestCmd = u64;
type TestStorage = MemStorage<TestCmd, ()>;
type TestNode = RaftNode<TestCmd, TestStorage, TestStorage>;

const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;

fn new_node(id: u64, peers: Vec<u64>) -> TestNode {
    RaftNode::new(
        id,
        peers,
        MemStorage::new(),
        MemStorage::new(),
        ELECTION_TIMEOUT,
        HEARTBEAT_INTERVAL,
    )
}

fn new_node_with_log(id: u64, peers: Vec<u64>, entries: &[(u64, u64, TestCmd)]) -> TestNode {
    let mut log = MemStorage::new();

    let seeded_entries: Vec<LogEntry<TestCmd>> = entries
        .iter()
        .map(|(index, term, command)| LogEntry {
            index: *index,
            term: *term,
            command: *command,
        })
        .collect();

    log.append(&seeded_entries);

    let current_term = entries.last().map(|(_, term, _)| *term).unwrap_or(0);
    let mut stable = MemStorage::new();
    stable.set_hard_state(HardState {
        current_term,
        voted_for: None,
        commit: 0,
    });

    RaftNode::new(id, peers, log, stable, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL)
}

fn deliver(nodes: &mut [TestNode; 3], messages: Vec<Envelope<TestCmd>>) {
    for msg in messages {
        let idx = (msg.to - 1) as usize;
        nodes[idx].step(msg);
    }
}

fn take_messages(node: &mut TestNode) -> Vec<Envelope<TestCmd>> {
    node.ready().messages
}

fn flush_cluster(nodes: &mut [TestNode; 3], max_rounds: usize) {
    for _ in 0..max_rounds {
        let mut pending = Vec::new();

        for node in nodes.iter_mut() {
            pending.extend(take_messages(node));
        }

        if pending.is_empty() {
            return;
        }

        deliver(nodes, pending);
    }

    panic!("cluster did not quiesce within {max_rounds} rounds");
}

fn elect_leader(nodes: &mut [TestNode; 3], leader_idx: usize) {
    nodes[leader_idx].tick(ELECTION_TIMEOUT);
    flush_cluster(nodes, 20);
    assert_eq!(nodes[leader_idx].role(), &Role::Leader);
    flush_cluster(nodes, 20);
}

#[test]
fn follower_reports_conflict_term_and_first_index() {
    let mut follower = new_node_with_log(
        2,
        vec![1, 3],
        &[(1, 1, 10), (2, 2, 20), (3, 2, 21), (4, 2, 22)],
    );

    follower.step(Envelope {
        from: 1,
        to: 2,
        msg: Message::AppendEntries(AppendEntriesRequest {
            term: 3,
            leader_id: 1,
            prev_log_index: 4,
            prev_log_term: 3,
            entries: Vec::new(),
            leader_commit: 0,
        }),
    });

    let ready = follower.ready();
    assert_eq!(ready.messages.len(), 1);

    match &ready.messages[0].msg {
        Message::AppendEntriesResponse(resp) => {
            assert!(!resp.success);
            assert_eq!(resp.term, 3);
            assert_eq!(resp.match_index, None);
            assert_eq!(resp.conflict_term, Some(2));
            assert_eq!(resp.conflict_index, Some(2));
        }
        _ => panic!("expected AppendEntriesResponse"),
    }
}

#[test]
fn leader_skips_to_end_of_conflict_term_on_rejection() {
    let mut nodes = [
        new_node_with_log(
            1,
            vec![2, 3],
            &[(1, 1, 10), (2, 2, 20), (3, 2, 21), (4, 2, 22), (5, 3, 30)],
        ),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];

    elect_leader(&mut nodes, 0);
    let current_term = nodes[0].current_term();

    nodes[0].step(Envelope {
        from: 2,
        to: 1,
        msg: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: current_term,
            success: false,
            match_index: None,
            conflict_term: Some(2),
            conflict_index: Some(2),
        }),
    });

    let retries = take_messages(&mut nodes[0]);
    assert_eq!(retries.len(), 1);

    match &retries[0].msg {
        Message::AppendEntries(req) => {
            assert_eq!(retries[0].to, 2);
            assert_eq!(req.prev_log_index, 4);
            assert_eq!(req.prev_log_term, 2);
            assert_eq!(req.entries.len(), 1);
            assert_eq!(req.entries[0].index, 5);
            assert_eq!(req.entries[0].term, 3);
            assert_eq!(req.entries[0].command, 30);
        }
        _ => panic!("expected retry AppendEntries"),
    }
}
