use raft::{
    core::node::{LeadershipTransferError, ProposeError, RaftNode},
    message::{Envelope, Message, TimeoutNowRequest},
    storage::mem::MemStorage,
    types::{ConfChange, ConfChangeKind, ConfState, Role},
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
    let Some(ready) = node.ready() else {
        return Vec::new();
    };
    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();
    ready.messages
}

fn deliver(nodes: &mut [TestNode; 3], messages: Vec<Envelope<(), ()>>) {
    for message in messages {
        match message.to.get() {
            1 => nodes[0].step(message),
            2 => nodes[1].step(message),
            3 => nodes[2].step(message),
            other => panic!("unexpected replica {other}"),
        }
    }
}

fn flush_messages(nodes: &mut [TestNode; 3], max_rounds: usize) {
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

    panic!("test Raft cluster did not quiesce");
}

fn elect_leader(nodes: &mut [TestNode; 3]) {
    nodes[0].tick(ELECTION_TIMEOUT);
    flush_messages(nodes, 20);
    assert_eq!(nodes[0].role(), &Role::Leader);
    flush_messages(nodes, 20);
}

#[test]
fn transfer_rejects_unknown_learner_and_lagging_targets() {
    let mut nodes = [
        new_node(1, vec![2, 3]),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];
    elect_leader(&mut nodes);

    assert_eq!(
        nodes[0].transfer_leadership(raft::types::ReplicaId::must(99), 5),
        Err(LeadershipTransferError::UnknownTarget(
            raft::types::ReplicaId::must(99)
        ))
    );

    let mut learner = RaftNode::bootstrap(
        raft::types::ReplicaId::must(1),
        ConfState::new(
            1,
            [raft::types::ReplicaId::must(1)],
            [raft::types::ReplicaId::must(2)],
        )
        .unwrap(),
        MemStorage::new(),
        MemStorage::new(),
        ELECTION_TIMEOUT,
        HEARTBEAT_INTERVAL,
    )
    .unwrap();
    learner.tick(ELECTION_TIMEOUT);
    let _ = take_messages(&mut learner);

    assert_eq!(
        learner.transfer_leadership(raft::types::ReplicaId::must(2), 5),
        Err(LeadershipTransferError::TargetIsLearner(
            raft::types::ReplicaId::must(2)
        ))
    );

    let proposed = nodes[0].propose(()).unwrap();
    assert_eq!(proposed, 1);
    let _ = take_messages(&mut nodes[0]);

    assert_eq!(
        nodes[0].transfer_leadership(raft::types::ReplicaId::must(2), 5),
        Err(LeadershipTransferError::TargetNotCaughtUp {
            target: raft::types::ReplicaId::must(2),
            match_index: 0,
            leader_last_index: 1,
        })
    );
}

#[test]
fn transfer_fences_application_and_configuration_proposals() {
    let mut nodes = [
        new_node(1, vec![2, 3]),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];
    elect_leader(&mut nodes);

    nodes[0]
        .transfer_leadership(raft::types::ReplicaId::must(2), 5)
        .unwrap();

    assert!(matches!(
        nodes[0].propose(()),
        Err(ProposeError::LeadershipTransferInProgress { target, .. })
            if target == raft::types::ReplicaId::must(2)
    ));
    assert!(matches!(
        nodes[0].propose_conf_change(ConfChange {
            expected_version: 1,
            kind: ConfChangeKind::AddLearner(raft::types::ReplicaId::must(4)),
        }),
        Err(ProposeError::LeadershipTransferInProgress { target, .. })
            if target == raft::types::ReplicaId::must(2)
    ));
}

#[test]
fn caught_up_voter_becomes_leader_through_targeted_transfer() {
    let mut nodes = [
        new_node(1, vec![2, 3]),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];
    elect_leader(&mut nodes);

    nodes[0]
        .transfer_leadership(raft::types::ReplicaId::must(2), 20)
        .unwrap();
    let messages = take_messages(&mut nodes[0]);
    assert!(messages.iter().any(|message| matches!(
        message.msg,
        Message::TimeoutNow(TimeoutNowRequest {
            term: 1,
            leader_id,
            target_id,
            ..
        }) if leader_id.get() == 1 && target_id.get() == 2
    )));

    deliver(&mut nodes, messages);
    flush_messages(&mut nodes, 40);

    assert_eq!(nodes[1].role(), &Role::Leader);
    assert_eq!(nodes[1].leader_id().map(|id| id.get()), Some(2));
    assert_eq!(nodes[0].role(), &Role::Follower);
}

#[test]
fn timed_out_transfer_releases_the_proposal_fence() {
    let mut nodes = [
        new_node(1, vec![2, 3]),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];
    elect_leader(&mut nodes);

    nodes[0]
        .transfer_leadership(raft::types::ReplicaId::must(2), 2)
        .unwrap();
    let _ = take_messages(&mut nodes[0]);

    nodes[0].tick(2);
    let _ = take_messages(&mut nodes[0]);

    assert_eq!(nodes[0].propose(()), Ok(1));
}

#[test]
fn delayed_timeout_now_from_an_old_term_is_ignored() {
    let mut nodes = [
        new_node(1, vec![2, 3]),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];
    elect_leader(&mut nodes);

    nodes[0]
        .transfer_leadership(raft::types::ReplicaId::must(2), 20)
        .unwrap();
    let delayed = take_messages(&mut nodes[0])
        .into_iter()
        .find(|message| matches!(message.msg, Message::TimeoutNow(_)))
        .expect("transfer must emit TimeoutNow");

    deliver(&mut nodes, vec![delayed.clone()]);
    flush_messages(&mut nodes, 40);
    assert_eq!(nodes[1].role(), &Role::Leader);
    assert_eq!(nodes[1].current_term(), 2);

    nodes[1].step(delayed);
    assert_eq!(nodes[1].role(), &Role::Leader);
    assert_eq!(nodes[1].current_term(), 2);
}
