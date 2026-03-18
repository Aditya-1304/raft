use raft::{
    core::node::RaftNode,
    message::{Envelope, Message, RequestVoteRequest},
    storage::mem::MemStorage,
    traits::stable_store::StableStore,
    types::{HardState, Role},
};

type TestStorage = MemStorage<(), ()>;
type TestNode = RaftNode<(), TestStorage, TestStorage>;

fn new_node(id: u64, peers: Vec<u64>, election_timeout: u64) -> TestNode {
    RaftNode::new(
        id,
        peers,
        MemStorage::new(),
        MemStorage::new(),
        election_timeout,
        2,
    )
}

fn take_messages(node: &mut TestNode) -> Vec<Envelope<()>> {
    node.ready().messages
}

#[test]
fn one_leader_is_elected() {
    let mut n1 = new_node(1, vec![2, 3], 5);
    let mut n2 = new_node(2, vec![1, 3], 5);
    let mut n3 = new_node(3, vec![1, 2], 5);

    n1.tick(5);
    let requests = take_messages(&mut n1);

    assert_eq!(n1.role(), &Role::Candidate);
    assert_eq!(requests.len(), 2);

    for msg in requests {
        match msg.to {
            2 => n2.step(msg),
            3 => n3.step(msg),
            _ => unreachable!(),
        }
    }

    let mut responses = Vec::new();
    responses.extend(take_messages(&mut n2));
    responses.extend(take_messages(&mut n3));

    for msg in responses {
        n1.step(msg);
    }

    assert_eq!(n1.role(), &Role::Leader);
    assert_eq!(n1.leader_id(), Some(1));
}

#[test]
fn stale_vote_request_is_rejected() {
    let log = MemStorage::<(), ()>::new();
    let mut stable = MemStorage::<(), ()>::new();

    stable.set_hard_state(HardState {
        current_term: 3,
        voted_for: None,
        commit: 0,
    });

    let mut voter = RaftNode::new(2, vec![1, 3], log, stable, 5, 2);

    voter.step(Envelope {
        from: 1,
        to: 2,
        msg: Message::RequestVote(RequestVoteRequest {
            term: 2,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }),
    });

    let responses = take_messages(&mut voter);
    assert_eq!(responses.len(), 1);

    match &responses[0].msg {
        Message::RequestVoteResponse(resp) => {
            assert!(!resp.vote_granted);
            assert_eq!(resp.term, 3);
        }
        _ => panic!("expected RequestVoteResponse"),
    }
}

#[test]
fn split_vote_eventually_resolves() {
    let mut n1 = new_node(1, vec![2, 3, 4], 5);
    let mut n2 = new_node(2, vec![1, 3, 4], 5);
    let mut n3 = new_node(3, vec![1, 2, 4], 5);
    let mut n4 = new_node(4, vec![1, 2, 3], 5);

    n1.tick(5);
    n2.tick(5);

    let requests_1 = take_messages(&mut n1);
    let requests_2 = take_messages(&mut n2);

    let mut delayed = Vec::new();

    for msg in requests_1 {
        match msg.to {
            3 => n3.step(msg),
            2 | 4 => delayed.push(msg),
            _ => unreachable!(),
        }
    }

    for msg in requests_2 {
        match msg.to {
            4 => n4.step(msg),
            1 | 3 => delayed.push(msg),
            _ => unreachable!(),
        }
    }

    for msg in take_messages(&mut n3) {
        n1.step(msg);
    }
    for msg in take_messages(&mut n4) {
        n2.step(msg);
    }

    assert_ne!(n1.role(), &Role::Leader);
    assert_ne!(n2.role(), &Role::Leader);

    for msg in delayed {
        match msg.to {
            1 => n1.step(msg),
            2 => n2.step(msg),
            3 => n3.step(msg),
            4 => n4.step(msg),
            _ => unreachable!(),
        }
    }

    let mut cleanup = Vec::new();
    cleanup.extend(take_messages(&mut n1));
    cleanup.extend(take_messages(&mut n2));
    cleanup.extend(take_messages(&mut n3));
    cleanup.extend(take_messages(&mut n4));

    for msg in cleanup {
        match msg.to {
            1 => n1.step(msg),
            2 => n2.step(msg),
            3 => n3.step(msg),
            4 => n4.step(msg),
            _ => unreachable!(),
        }
    }

    assert_ne!(n1.role(), &Role::Leader);
    assert_ne!(n2.role(), &Role::Leader);

    n1.tick(5);

    for msg in take_messages(&mut n1) {
        match msg.to {
            2 => n2.step(msg),
            3 => n3.step(msg),
            4 => n4.step(msg),
            _ => unreachable!(),
        }
    }

    let mut responses = Vec::new();
    responses.extend(take_messages(&mut n2));
    responses.extend(take_messages(&mut n3));
    responses.extend(take_messages(&mut n4));

    for msg in responses {
        n1.step(msg);
    }

    assert_eq!(n1.role(), &Role::Leader);
    assert_eq!(n1.leader_id(), Some(1));
}
