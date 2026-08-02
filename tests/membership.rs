use raft::{
    core::node::{InitError, RaftNode, StepError},
    entry::EntryPayload,
    message::{Envelope, Message, PreVoteResponse, RequestVoteRequest},
    storage::mem::MemStorage,
    traits::stable_store::StableStore,
    types::{ConfChange, ConfChangeKind, ConfState, Role},
};

type TestStorage = MemStorage<(), ()>;
type TestNode = RaftNode<(), (), TestStorage, TestStorage>;

fn new_node(id: u64, peers: Vec<u64>) -> TestNode {
    RaftNode::new(id, peers, MemStorage::new(), MemStorage::new(), 5, 2)
}

fn persist_one_ready(node: &mut TestNode) -> raft::core::ready::Ready<(), ()> {
    let ready = node.ready().expect("expected Ready generation");
    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();
    ready
}

#[test]
fn non_member_prevote_response_cannot_advance_an_election() {
    let mut candidate = new_node(1, vec![2, 3]);

    candidate.tick(candidate.current_election_timeout());
    assert_eq!(candidate.role(), &Role::Candidate);
    assert_eq!(candidate.current_term(), 0);

    // Replica 99 is not part of the active voting configuration. Counting its
    // response would let one forged packet combine with the candidate's own
    // vote and manufacture a quorum in this three-voter group.
    let result = candidate.step_checked(Envelope {
        from: 99,
        to: 1,
        msg: Message::PreVoteResponse(PreVoteResponse {
            term: 0,
            vote_granted: true,
        }),
    });

    assert_eq!(result, Err(StepError::UnknownReplica(99)));
    assert_eq!(candidate.current_term(), 0);
    assert_eq!(candidate.role(), &Role::Candidate);
}

#[test]
fn vote_request_rejects_an_envelope_payload_identity_mismatch() {
    let mut follower = new_node(1, vec![2, 3]);

    // The transport identifies replica 2 as the sender, while the signed Raft
    // payload claims that replica 99 is the candidate. Accepting this request
    // would persist a vote for an identity that never sent the message.
    let result = follower.step_checked(Envelope {
        from: 2,
        to: 1,
        msg: Message::RequestVote(RequestVoteRequest {
            term: 1,
            candidate_id: 99,
            last_log_index: 0,
            last_log_term: 0,
        }),
    });

    assert_eq!(
        result,
        Err(StepError::PayloadIdentityMismatch {
            envelope: 2,
            payload: 99,
        })
    );
    assert_eq!(follower.hard_state().voted_for, None);
}

#[test]
fn learner_never_starts_an_election() {
    let conf_state = ConfState::new(1, [1, 3], [2]).unwrap();
    let mut learner: TestNode =
        RaftNode::bootstrap(2, conf_state, MemStorage::new(), MemStorage::new(), 5, 2).unwrap();

    learner.tick(100);

    assert_eq!(learner.role(), &Role::Follower);
    assert_eq!(learner.current_term(), 0);
}

#[test]
fn restart_requires_a_durable_configuration() {
    let result = RaftNode::<(), (), TestStorage, TestStorage>::restart(
        1,
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    );

    assert!(matches!(
        result,
        Err(InitError::MissingDurableConfiguration)
    ));
}

#[test]
fn legacy_restart_does_not_replace_durable_membership_with_changed_peers() {
    let durable = ConfState::new(7, [1, 2, 3], []).unwrap();
    let mut stable = MemStorage::<(), ()>::new();
    stable.set_conf_state(durable.clone());

    // The legacy constructor remains available to the demo runtime, but its
    // peer argument is ignored once a committed configuration is durable.
    let node: TestNode = RaftNode::new(1, vec![8, 9], MemStorage::new(), stable, 5, 2);

    assert_eq!(node.conf_state(), &durable);
    assert!(!node.conf_state().contains(8));
}

#[test]
fn committed_configuration_entry_emits_and_persists_conf_state() {
    let initial = ConfState::new(1, [1], []).unwrap();
    let mut node: TestNode =
        RaftNode::bootstrap(1, initial, MemStorage::new(), MemStorage::new(), 5, 2).unwrap();

    persist_one_ready(&mut node);
    node.tick(node.current_election_timeout());
    persist_one_ready(&mut node);
    assert_eq!(node.role(), &Role::Leader);

    let index = node
        .propose_conf_change(ConfChange {
            expected_version: 1,
            kind: ConfChangeKind::AddLearner(2),
        })
        .unwrap();
    let ready = persist_one_ready(&mut node);

    assert_eq!(index, 1);
    assert!(matches!(
        ready.entries_to_persist[0].payload,
        EntryPayload::Configuration(_)
    ));
    let emitted = ready
        .conf_state
        .expect("committed ConfState must be emitted");
    assert_eq!(emitted.version, 2);
    assert!(emitted.learners.contains(&2));
    assert_eq!(node.durable_conf_state(), Some(&emitted));
}
