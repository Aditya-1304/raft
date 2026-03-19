use raft::{
    core::node::RaftNode,
    entry::LogEntry,
    message::Envelope,
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

fn drain_messages(node: &mut TestNode) -> Vec<Envelope<TestCmd>> {
    node.ready().messages
}

fn flush_cluster(nodes: &mut [TestNode; 3], max_rounds: usize) {
    for _ in 0..max_rounds {
        let mut pending = Vec::new();

        for node in nodes.iter_mut() {
            pending.extend(drain_messages(node));
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
fn majority_replication() {
    let mut nodes = [
        new_node(1, vec![2, 3]),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];

    elect_leader(&mut nodes, 0);

    let proposed_index = nodes[0].propose(10).unwrap();
    assert_eq!(proposed_index, 1);

    let ready = nodes[0].ready();
    assert_eq!(ready.entries_to_persist.len(), 1);
    assert_eq!(ready.entries_to_persist[0].index, 1);
    assert_eq!(ready.entries_to_persist[0].term, nodes[0].current_term());
    assert_eq!(ready.messages.len(), 2);

    let to_n2: Vec<_> = ready
        .messages
        .into_iter()
        .filter(|msg| msg.to == 2)
        .collect();
    deliver(&mut nodes, to_n2);

    let n2_responses = drain_messages(&mut nodes[1]);
    assert_eq!(n2_responses.len(), 1);
    deliver(&mut nodes, n2_responses);

    assert_eq!(nodes[0].last_log_index(), 1);
    assert_eq!(nodes[1].last_log_index(), 1);
    assert_eq!(nodes[2].last_log_index(), 0);
}

#[test]
fn follower_catch_up() {
    let mut nodes = [
        new_node(1, vec![2, 3]),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];

    elect_leader(&mut nodes, 0);

    nodes[0].propose(10).unwrap();
    let first_ready = nodes[0].ready();

    let only_n2: Vec<_> = first_ready
        .messages
        .into_iter()
        .filter(|msg| msg.to == 2)
        .collect();
    deliver(&mut nodes, only_n2);

    let n2_responses = drain_messages(&mut nodes[1]);
    deliver(&mut nodes, n2_responses);

    assert_eq!(nodes[2].last_log_index(), 0);

    nodes[0].propose(20).unwrap();
    flush_cluster(&mut nodes, 20);

    assert_eq!(nodes[0].last_log_index(), 2);
    assert_eq!(nodes[1].last_log_index(), 2);
    assert_eq!(nodes[2].last_log_index(), 2);
    assert_eq!(nodes[2].last_log_term(), nodes[0].last_log_term());
}

#[test]
fn conflicting_suffix_repair() {
    let mut nodes = [
        new_node_with_log(1, vec![2, 3], &[(1, 1, 10), (2, 1, 20)]),
        new_node(2, vec![1, 3]),
        new_node_with_log(3, vec![1, 2], &[(1, 1, 10), (2, 2, 99)]),
    ];

    assert_eq!(nodes[2].last_log_index(), 2);
    assert_eq!(nodes[2].last_log_term(), 2);

    elect_leader(&mut nodes, 0);

    assert_eq!(nodes[0].role(), &Role::Leader);
    assert_eq!(nodes[2].last_log_index(), 2);
    assert_eq!(nodes[2].last_log_term(), 1);
}
