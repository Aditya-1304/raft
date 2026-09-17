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
type TestNode = RaftNode<TestCmd, (), TestStorage, TestStorage>;

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
        .map(|(index, term, command)| LogEntry::normal(*index, *term, *command))
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

fn deliver(nodes: &mut [TestNode; 3], messages: Vec<Envelope<TestCmd, ()>>) {
    for msg in messages {
        let idx = (msg.to.get() - 1) as usize;
        nodes[idx].step(msg);
    }
}

fn take_messages(node: &mut TestNode) -> Vec<Envelope<TestCmd, ()>> {
    let Some(ready) = node.ready() else {
        return Vec::new();
    };
    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();
    ready.messages
}

fn take_ready(node: &mut TestNode) -> raft::core::ready::Ready<TestCmd, ()> {
    let ready = node.ready().expect("expected Ready generation");
    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();
    ready
}

fn elect_leader(nodes: &mut [TestNode; 3], leader_idx: usize) {
    nodes[leader_idx].tick(ELECTION_TIMEOUT);

    let prevotes = take_messages(&mut nodes[leader_idx]);
    deliver(nodes, prevotes);

    let mut prevote_responses = Vec::new();
    for (idx, node) in nodes.iter_mut().enumerate() {
        if idx != leader_idx {
            prevote_responses.extend(take_messages(node));
        }
    }
    deliver(nodes, prevote_responses);

    let vote_requests = take_messages(&mut nodes[leader_idx]);
    deliver(nodes, vote_requests);

    let mut vote_responses = Vec::new();
    for (idx, node) in nodes.iter_mut().enumerate() {
        if idx != leader_idx {
            vote_responses.extend(take_messages(node));
        }
    }
    deliver(nodes, vote_responses);

    assert_eq!(nodes[leader_idx].role(), &Role::Leader);
}

#[test]
fn follower_reports_conflict_term_and_first_index() {
    let mut follower = new_node_with_log(
        2,
        vec![1, 3],
        &[(1, 1, 10), (2, 2, 20), (3, 2, 21), (4, 2, 22)],
    );

    follower.step(Envelope {
        from: raft::types::ReplicaId::must(1),
        to: raft::types::ReplicaId::must(2),
        msg: Message::AppendEntries(AppendEntriesRequest {
            term: 3,
            leader_id: raft::types::ReplicaId::must(1),
            generation: 0,
            prev_log_index: 4,
            prev_log_term: 3,
            entries: Vec::new(),
            leader_commit: 0,
        }),
    });

    let ready = take_ready(&mut follower);
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
    let _initial_heartbeats = take_messages(&mut nodes[0]);
    let current_term = nodes[0].current_term();
    let generation = nodes[0]
        .progress(raft::types::ReplicaId::must(2))
        .unwrap()
        .generation;

    nodes[0].step(Envelope {
        from: raft::types::ReplicaId::must(2),
        to: raft::types::ReplicaId::must(1),
        msg: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: current_term,
            generation,
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
            assert_eq!(retries[0].to.get(), 2);
            assert_eq!(req.prev_log_index, 4);
            assert_eq!(req.prev_log_term, 2);
            assert_eq!(req.entries.len(), 1);
            assert_eq!(req.entries[0].index, 5);
            assert_eq!(req.entries[0].term, 3);
            assert_eq!(req.entries[0].command(), Some(&30));
        }
        _ => panic!("expected retry AppendEntries"),
    }
}
