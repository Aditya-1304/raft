use raft::{
    core::node::RaftNode,
    message::{Envelope, Message, RequestVoteRequest},
    storage::mem::MemStorage,
    traits::stable_store::StableStore,
    types::{HardState, Role},
};

type TestStorage = MemStorage<(), ()>;
type TestNode = RaftNode<(), (), TestStorage, TestStorage>;

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

fn take_messages(node: &mut TestNode) -> Vec<Envelope<(), ()>> {
    node.ready().messages
}

fn tick_to_timeout(node: &mut TestNode) {
    let timeout = node.current_election_timeout();
    node.tick(timeout);
}

fn assert_all_prevote(messages: &[Envelope<(), ()>], candidate_id: u64, term: u64) {
    for msg in messages {
        match &msg.msg {
            Message::PreVote(req) => {
                assert_eq!(req.term, term);
                assert_eq!(req.candidate_id, candidate_id);
                assert_eq!(req.last_log_index, 0);
                assert_eq!(req.last_log_term, 0);
            }
            _ => panic!("expected PreVote"),
        }
    }
}

fn assert_all_request_vote(messages: &[Envelope<(), ()>], candidate_id: u64, term: u64) {
    for msg in messages {
        match &msg.msg {
            Message::RequestVote(req) => {
                assert_eq!(req.term, term);
                assert_eq!(req.candidate_id, candidate_id);
                assert_eq!(req.last_log_index, 0);
                assert_eq!(req.last_log_term, 0);
            }
            _ => panic!("expected RequestVote"),
        }
    }
}

#[test]
fn one_leader_is_elected() {
    let mut n1 = new_node(1, vec![2, 3], 5);
    let mut n2 = new_node(2, vec![1, 3], 5);
    let mut n3 = new_node(3, vec![1, 2], 5);

    tick_to_timeout(&mut n1);
    let prevotes = take_messages(&mut n1);

    assert_eq!(n1.role(), &Role::Candidate);
    assert_eq!(n1.current_term(), 0);
    assert_eq!(prevotes.len(), 2);
    assert_all_prevote(&prevotes, 1, 1);

    for msg in prevotes {
        match msg.to {
            2 => n2.step(msg),
            3 => n3.step(msg),
            _ => unreachable!(),
        }
    }

    let mut prevote_responses = Vec::new();
    prevote_responses.extend(take_messages(&mut n2));
    prevote_responses.extend(take_messages(&mut n3));

    for msg in &prevote_responses {
        match &msg.msg {
            Message::PreVoteResponse(resp) => {
                assert!(resp.vote_granted);
                assert_eq!(resp.term, 0);
            }
            _ => panic!("expected PreVoteResponse"),
        }
    }

    for msg in prevote_responses {
        n1.step(msg);
    }

    let vote_requests = take_messages(&mut n1);
    assert_eq!(n1.current_term(), 1);
    assert_eq!(vote_requests.len(), 2);
    assert_all_request_vote(&vote_requests, 1, 1);

    for msg in vote_requests {
        match msg.to {
            2 => n2.step(msg),
            3 => n3.step(msg),
            _ => unreachable!(),
        }
    }

    let mut vote_responses = Vec::new();
    vote_responses.extend(take_messages(&mut n2));
    vote_responses.extend(take_messages(&mut n3));

    for msg in vote_responses {
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

    tick_to_timeout(&mut n1);
    tick_to_timeout(&mut n2);

    let prevotes_1 = take_messages(&mut n1);
    let prevotes_2 = take_messages(&mut n2);

    assert_all_prevote(&prevotes_1, 1, 1);
    assert_all_prevote(&prevotes_2, 2, 1);

    let mut delayed_prevotes = Vec::new();

    for msg in prevotes_1 {
        match msg.to {
            3 => n3.step(msg),
            2 | 4 => delayed_prevotes.push(msg),
            _ => unreachable!(),
        }
    }

    for msg in prevotes_2 {
        match msg.to {
            4 => n4.step(msg),
            1 | 3 => delayed_prevotes.push(msg),
            _ => unreachable!(),
        }
    }

    for msg in take_messages(&mut n3) {
        n1.step(msg);
    }
    for msg in take_messages(&mut n4) {
        n2.step(msg);
    }

    assert_eq!(n1.current_term(), 0);
    assert_eq!(n2.current_term(), 0);
    assert_ne!(n1.role(), &Role::Leader);
    assert_ne!(n2.role(), &Role::Leader);

    for msg in delayed_prevotes {
        match msg.to {
            1 => n1.step(msg),
            2 => n2.step(msg),
            3 => n3.step(msg),
            4 => n4.step(msg),
            _ => unreachable!(),
        }
    }

    let mut prevote_responses = Vec::new();
    prevote_responses.extend(take_messages(&mut n1));
    prevote_responses.extend(take_messages(&mut n2));
    prevote_responses.extend(take_messages(&mut n3));
    prevote_responses.extend(take_messages(&mut n4));

    for msg in prevote_responses {
        match msg.to {
            1 => n1.step(msg),
            2 => n2.step(msg),
            3 => n3.step(msg),
            4 => n4.step(msg),
            _ => unreachable!(),
        }
    }

    let vote_requests_1 = take_messages(&mut n1);
    let vote_requests_2 = take_messages(&mut n2);

    assert_eq!(n1.current_term(), 1);
    assert_eq!(n2.current_term(), 1);
    assert_all_request_vote(&vote_requests_1, 1, 1);
    assert_all_request_vote(&vote_requests_2, 2, 1);

    let mut delayed_votes = Vec::new();

    for msg in vote_requests_1 {
        match msg.to {
            3 => n3.step(msg),
            2 | 4 => delayed_votes.push(msg),
            _ => unreachable!(),
        }
    }

    for msg in vote_requests_2 {
        match msg.to {
            4 => n4.step(msg),
            1 | 3 => delayed_votes.push(msg),
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

    for msg in delayed_votes {
        match msg.to {
            1 => n1.step(msg),
            2 => n2.step(msg),
            3 => n3.step(msg),
            4 => n4.step(msg),
            _ => unreachable!(),
        }
    }

    let mut vote_cleanup = Vec::new();
    vote_cleanup.extend(take_messages(&mut n1));
    vote_cleanup.extend(take_messages(&mut n2));
    vote_cleanup.extend(take_messages(&mut n3));
    vote_cleanup.extend(take_messages(&mut n4));

    for msg in vote_cleanup {
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

    tick_to_timeout(&mut n1);

    let prevote_retry = take_messages(&mut n1);
    assert_all_prevote(&prevote_retry, 1, 2);

    for msg in prevote_retry {
        match msg.to {
            2 => n2.step(msg),
            3 => n3.step(msg),
            4 => n4.step(msg),
            _ => unreachable!(),
        }
    }

    let mut prevote_retry_responses = Vec::new();
    prevote_retry_responses.extend(take_messages(&mut n2));
    prevote_retry_responses.extend(take_messages(&mut n3));
    prevote_retry_responses.extend(take_messages(&mut n4));

    for msg in prevote_retry_responses {
        n1.step(msg);
    }

    let vote_retry = take_messages(&mut n1);
    assert_all_request_vote(&vote_retry, 1, 2);

    for msg in vote_retry {
        match msg.to {
            2 => n2.step(msg),
            3 => n3.step(msg),
            4 => n4.step(msg),
            _ => unreachable!(),
        }
    }

    let mut vote_retry_responses = Vec::new();
    vote_retry_responses.extend(take_messages(&mut n2));
    vote_retry_responses.extend(take_messages(&mut n3));
    vote_retry_responses.extend(take_messages(&mut n4));

    for msg in vote_retry_responses {
        n1.step(msg);
    }

    assert_eq!(n1.role(), &Role::Leader);
    assert_eq!(n1.leader_id(), Some(1));
}

#[test]
fn timeout_starts_prevote_and_rearms_within_randomized_window() {
    let mut node = new_node(1, vec![2, 3], 5);

    assert_eq!(node.current_election_timeout(), 5);

    tick_to_timeout(&mut node);
    let prevotes = take_messages(&mut node);

    assert_eq!(node.role(), &Role::Candidate);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.voted_for(), None);
    assert_eq!(prevotes.len(), 2);
    assert_all_prevote(&prevotes, 1, 1);

    assert!(node.current_election_timeout() >= 5);
    assert!(node.current_election_timeout() < 10);
}
