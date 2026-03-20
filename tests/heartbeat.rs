use raft::{
    core::node::RaftNode,
    message::{Envelope, Message},
    storage::mem::MemStorage,
    types::Role,
};

type TestStorage = MemStorage<(), ()>;
type TestNode = RaftNode<(), (), TestStorage, TestStorage>;

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

fn take_messages(node: &mut TestNode) -> Vec<Envelope<(), ()>> {
    node.ready().messages
}

fn tick_to_timeout(node: &mut TestNode) {
    let timeout = node.current_election_timeout();
    node.tick(timeout);
}

fn assert_heartbeat_batch(messages: &[Envelope<(), ()>]) {
    for msg in messages {
        match &msg.msg {
            Message::AppendEntries(req) => assert!(req.entries.is_empty()),
            _ => panic!("expected heartbeat AppendEntries"),
        }
    }
}

fn deliver(
    n1: &mut TestNode,
    n2: &mut TestNode,
    n3: &mut TestNode,
    messages: Vec<Envelope<(), ()>>,
) {
    for msg in messages {
        match msg.to {
            1 => n1.step(msg),
            2 => n2.step(msg),
            3 => n3.step(msg),
            _ => unreachable!(),
        }
    }
}

fn elect_leader(n1: &mut TestNode, n2: &mut TestNode, n3: &mut TestNode) {
    tick_to_timeout(n1);

    let prevotes = take_messages(n1);
    assert_eq!(prevotes.len(), 2);
    for msg in &prevotes {
        assert!(matches!(msg.msg, Message::PreVote(_)));
    }
    deliver(n1, n2, n3, prevotes);

    let mut prevote_responses = take_messages(n2);
    prevote_responses.extend(take_messages(n3));
    assert_eq!(prevote_responses.len(), 2);
    for msg in &prevote_responses {
        assert!(matches!(msg.msg, Message::PreVoteResponse(_)));
    }
    deliver(n1, n2, n3, prevote_responses);

    let vote_requests = take_messages(n1);
    assert_eq!(vote_requests.len(), 2);
    for msg in &vote_requests {
        assert!(matches!(msg.msg, Message::RequestVote(_)));
    }
    deliver(n1, n2, n3, vote_requests);

    let mut vote_responses = take_messages(n2);
    vote_responses.extend(take_messages(n3));
    assert_eq!(vote_responses.len(), 2);
    for msg in &vote_responses {
        assert!(matches!(msg.msg, Message::RequestVoteResponse(_)));
    }
    deliver(n1, n2, n3, vote_responses);

    assert_eq!(n1.role(), &Role::Leader);

    let initial_heartbeats = take_messages(n1);
    assert_eq!(initial_heartbeats.len(), 2);
    assert_heartbeat_batch(&initial_heartbeats);
    deliver(n1, n2, n3, initial_heartbeats);

    let mut heartbeat_responses = take_messages(n2);
    heartbeat_responses.extend(take_messages(n3));
    assert_eq!(heartbeat_responses.len(), 2);
    deliver(n1, n2, n3, heartbeat_responses);
}

#[test]
fn stable_leader_under_heartbeats() {
    let mut n1 = new_node(1, vec![2, 3]);
    let mut n2 = new_node(2, vec![1, 3]);
    let mut n3 = new_node(3, vec![1, 2]);

    elect_leader(&mut n1, &mut n2, &mut n3);

    for _ in 0..3 {
        n1.tick(HEARTBEAT_INTERVAL);

        let heartbeats = take_messages(&mut n1);
        assert_eq!(heartbeats.len(), 2);
        assert_heartbeat_batch(&heartbeats);
        deliver(&mut n1, &mut n2, &mut n3, heartbeats);

        let responses_from_n2 = take_messages(&mut n2);
        let dropped_from_n3 = take_messages(&mut n3);

        assert_eq!(responses_from_n2.len(), 1);
        assert_eq!(dropped_from_n3.len(), 1);

        deliver(&mut n1, &mut n2, &mut n3, responses_from_n2);

        n2.tick(1);
        n3.tick(1);

        assert_eq!(n1.role(), &Role::Leader);
        assert_eq!(n2.role(), &Role::Follower);
        assert_eq!(n3.role(), &Role::Follower);
    }
}

#[test]
fn leader_steps_down_without_quorum_responses() {
    let mut n1 = new_node(1, vec![2, 3]);
    let mut n2 = new_node(2, vec![1, 3]);
    let mut n3 = new_node(3, vec![1, 2]);

    elect_leader(&mut n1, &mut n2, &mut n3);
    assert_eq!(n1.role(), &Role::Leader);

    n1.tick(ELECTION_TIMEOUT);

    let heartbeats = take_messages(&mut n1);
    assert_eq!(heartbeats.len(), 2);
    assert_heartbeat_batch(&heartbeats);
    deliver(&mut n1, &mut n2, &mut n3, heartbeats);

    let dropped_from_n2 = take_messages(&mut n2);
    let dropped_from_n3 = take_messages(&mut n3);
    assert_eq!(dropped_from_n2.len(), 1);
    assert_eq!(dropped_from_n3.len(), 1);

    assert_eq!(n1.role(), &Role::Leader);

    n1.tick(ELECTION_TIMEOUT);

    assert_eq!(n1.role(), &Role::Follower);
    assert_eq!(n1.leader_id(), None);
    assert!(take_messages(&mut n1).is_empty());
}

#[test]
fn stale_leader_steps_down() {
    let mut n1 = new_node(1, vec![2, 3]);
    let mut n2 = new_node(2, vec![1, 3]);
    let mut n3 = new_node(3, vec![1, 2]);

    elect_leader(&mut n1, &mut n2, &mut n3);
    assert_eq!(n1.role(), &Role::Leader);

    tick_to_timeout(&mut n2);

    let prevotes = take_messages(&mut n2);
    assert_eq!(prevotes.len(), 2);
    for msg in &prevotes {
        assert!(matches!(msg.msg, Message::PreVote(_)));
    }
    deliver(&mut n1, &mut n2, &mut n3, prevotes);

    let mut prevote_responses = take_messages(&mut n1);
    prevote_responses.extend(take_messages(&mut n3));
    assert_eq!(prevote_responses.len(), 2);
    deliver(&mut n1, &mut n2, &mut n3, prevote_responses);

    let vote_requests = take_messages(&mut n2);
    assert_eq!(vote_requests.len(), 2);
    for msg in &vote_requests {
        assert!(matches!(msg.msg, Message::RequestVote(_)));
    }
    deliver(&mut n1, &mut n2, &mut n3, vote_requests);

    let mut vote_responses = take_messages(&mut n1);
    vote_responses.extend(take_messages(&mut n3));
    assert_eq!(vote_responses.len(), 2);
    deliver(&mut n1, &mut n2, &mut n3, vote_responses);

    assert_eq!(n2.role(), &Role::Leader);
    assert_eq!(n1.role(), &Role::Follower);
}
