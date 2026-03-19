use raft::{core::node::RaftNode, message::Envelope, storage::mem::MemStorage, types::Role};

type TestStorage = MemStorage<(), ()>;
type TestNode = RaftNode<(), TestStorage, TestStorage>;

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

fn take_messages(node: &mut TestNode) -> Vec<Envelope<()>> {
    node.ready().messages
}

#[test]
fn stable_leader_under_heartbeats() {
    let mut n1 = new_node(1, vec![2, 3], 5, 2);
    let mut n2 = new_node(2, vec![1, 3], 5, 2);
    let mut n3 = new_node(3, vec![1, 2], 5, 2);

    n1.tick(5);

    for msg in take_messages(&mut n1) {
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

    for _ in 0..3 {
        n1.tick(2);

        for msg in take_messages(&mut n1) {
            match msg.to {
                2 => n2.step(msg),
                3 => n3.step(msg),
                _ => unreachable!(),
            }
        }

        let follower_responses_2 = take_messages(&mut n2);
        let follower_responses_3 = take_messages(&mut n3);

        for msg in follower_responses_2.into_iter().chain(follower_responses_3) {
            n1.step(msg);
        }

        n2.tick(1);
        n3.tick(1);

        assert_eq!(n2.role(), &Role::Follower);
        assert_eq!(n3.role(), &Role::Follower);
    }
}

#[test]
fn stale_leader_steps_down() {
    let mut n1 = new_node(1, vec![2, 3], 5, 2);
    let mut n2 = new_node(2, vec![1, 3], 5, 2);
    let mut n3 = new_node(3, vec![1, 2], 5, 2);

    n1.tick(5);

    for msg in take_messages(&mut n1) {
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

    n2.tick(5);

    for msg in take_messages(&mut n2) {
        match msg.to {
            1 => n1.step(msg),
            3 => n3.step(msg),
            _ => unreachable!(),
        }
    }

    for msg in take_messages(&mut n3) {
        n2.step(msg);
    }

    for msg in take_messages(&mut n1) {
        n2.step(msg);
    }

    assert_eq!(n2.role(), &Role::Leader);
    assert_eq!(n1.role(), &Role::Follower);
}
