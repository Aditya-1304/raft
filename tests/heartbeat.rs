use raft::{
    core::node::RaftNode,
    message::{Envelope, Message},
    storage::mem::MemStorage,
    types::Role,
};

type TestStorage = MemStorage<(), ()>;
type TestNode = RaftNode<(), (), TestStorage, TestStorage>;

fn new_node(id: u64, peers: Vec<u64>, election_timeout: u64, heartbeat_interval: u64) -> TestNode {
    RaftNode::new(
        id,
        peers,
        MemStorage::new(),
        MemStorage::new(),
        election_timeout,
        heartbeat_interval,
    )
}

fn take_messages(node: &mut TestNode) -> Vec<Envelope<(), ()>> {
    node.ready().messages
}

fn tick_to_timeout(node: &mut TestNode) {
    let timeout = node.current_election_timeout();
    node.tick(timeout);
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
    deliver(n1, n2, n3, vote_responses);

    assert_eq!(n1.role(), &Role::Leader);

    let initial_heartbeats = take_messages(n1);
    assert_eq!(initial_heartbeats.len(), 2);
    for msg in &initial_heartbeats {
        match &msg.msg {
            Message::AppendEntries(req) => assert!(req.entries.is_empty()),
            _ => panic!("expected initial heartbeat"),
        }
    }
    deliver(n1, n2, n3, initial_heartbeats);

    let mut heartbeat_responses = take_messages(n2);
    heartbeat_responses.extend(take_messages(n3));
    deliver(n1, n2, n3, heartbeat_responses);

    assert_eq!(n2.role(), &Role::Follower);
    assert_eq!(n3.role(), &Role::Follower);
}

#[test]
fn stable_leader_under_heartbeats() {
    let mut n1 = new_node(1, vec![2, 3], 5, 2);
    let mut n2 = new_node(2, vec![1, 3], 5, 2);
    let mut n3 = new_node(3, vec![1, 2], 5, 2);

    elect_leader(&mut n1, &mut n2, &mut n3);

    for _ in 0..3 {
        n1.tick(2);

        let heartbeats = take_messages(&mut n1);
        assert_eq!(heartbeats.len(), 2);

        for msg in &heartbeats {
            match &msg.msg {
                Message::AppendEntries(req) => assert!(req.entries.is_empty()),
                _ => panic!("expected heartbeat AppendEntries"),
            }
        }

        deliver(&mut n1, &mut n2, &mut n3, heartbeats);

        let mut follower_responses = take_messages(&mut n2);
        follower_responses.extend(take_messages(&mut n3));
        deliver(&mut n1, &mut n2, &mut n3, follower_responses);

        n2.tick(1);
        n3.tick(1);

        assert_eq!(n1.role(), &Role::Leader);
        assert_eq!(n2.role(), &Role::Follower);
        assert_eq!(n3.role(), &Role::Follower);
    }
}

#[test]
fn stale_leader_steps_down() {
    let mut n1 = new_node(1, vec![2, 3], 5, 2);
    let mut n2 = new_node(2, vec![1, 3], 5, 2);
    let mut n3 = new_node(3, vec![1, 2], 5, 2);

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
    deliver(&mut n1, &mut n2, &mut n3, prevote_responses);

    let vote_requests = take_messages(&mut n2);
    assert_eq!(vote_requests.len(), 2);
    for msg in &vote_requests {
        assert!(matches!(msg.msg, Message::RequestVote(_)));
    }
    deliver(&mut n1, &mut n2, &mut n3, vote_requests);

    let mut vote_responses = take_messages(&mut n1);
    vote_responses.extend(take_messages(&mut n3));
    deliver(&mut n1, &mut n2, &mut n3, vote_responses);

    assert_eq!(n2.role(), &Role::Leader);
    assert_eq!(n1.role(), &Role::Follower);
}
