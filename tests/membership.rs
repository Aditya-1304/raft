use raft::{
    core::node::{InitError, RaftNode, StepError},
    entry::{EntryPayload, LogEntry},
    message::{AppendEntriesRequest, Envelope, Message, PreVoteResponse, RequestVoteRequest},
    storage::mem::MemStorage,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{
        ConfChange, ConfChangeError, ConfChangeKind, ConfState, HardState, ReplicaId, Role,
        Snapshot,
    },
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
        from: raft::types::ReplicaId::must(99),
        to: raft::types::ReplicaId::must(1),
        msg: Message::PreVoteResponse(PreVoteResponse {
            term: 0,
            vote_granted: true,
        }),
    });

    assert_eq!(result, Err(StepError::UnknownReplica(ReplicaId::must(99))));
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
        from: raft::types::ReplicaId::must(2),
        to: raft::types::ReplicaId::must(1),
        msg: Message::RequestVote(RequestVoteRequest {
            term: 1,
            candidate_id: raft::types::ReplicaId::must(99),
            last_log_index: 0,
            last_log_term: 0,
        }),
    });

    assert_eq!(
        result,
        Err(StepError::PayloadIdentityMismatch {
            envelope: raft::types::ReplicaId::must(2),
            payload: raft::types::ReplicaId::must(99),
        })
    );
    assert_eq!(follower.hard_state().voted_for.map(|id| id.get()), None);
}

#[test]
fn learner_never_starts_an_election() {
    let conf_state = ConfState::new(
        1,
        [ReplicaId::must(1), ReplicaId::must(3)],
        [ReplicaId::must(2)],
    )
    .unwrap();
    let mut learner: TestNode = RaftNode::bootstrap(
        ReplicaId::must(2),
        conf_state,
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    )
    .unwrap();

    learner.tick(100);

    assert_eq!(learner.role(), &Role::Follower);
    assert_eq!(learner.current_term(), 0);
}

#[test]
fn explicit_joining_replica_is_passive_until_membership_is_committed() {
    let conf_state = ConfState::new(1, [ReplicaId::must(1), ReplicaId::must(3)], []).unwrap();
    let mut joining: TestNode = RaftNode::bootstrap_joining(
        ReplicaId::must(2),
        conf_state.clone(),
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    )
    .unwrap();

    joining.tick(100);

    assert!(joining.is_joining());
    assert_eq!(joining.role(), &Role::Follower);
    assert_eq!(joining.current_term(), 0);
    assert_eq!(joining.conf_state(), &conf_state);
}

#[test]
fn joining_restart_is_not_available_after_local_membership_is_durable() {
    let conf_state = ConfState::new(2, [ReplicaId::must(1), ReplicaId::must(2)], []).unwrap();
    let mut stable = MemStorage::<(), ()>::new();
    stable.set_conf_state(conf_state);

    let result = RaftNode::<(), (), TestStorage, TestStorage>::restart_joining(
        ReplicaId::must(2),
        MemStorage::new(),
        stable,
        5,
        2,
    );

    assert!(matches!(
        result,
        Err(InitError::JoiningReplicaAlreadyMember(id)) if id == ReplicaId::must(2)
    ));
}

#[test]
fn lagging_learner_cannot_be_promoted_before_the_committed_frontier() {
    let initial = ConfState::new(1, [ReplicaId::must(1)], [ReplicaId::must(2)]).unwrap();
    let mut node: TestNode = RaftNode::bootstrap(
        ReplicaId::must(1),
        initial,
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    )
    .unwrap();

    persist_one_ready(&mut node);
    node.tick(node.current_election_timeout());
    persist_one_ready(&mut node);
    assert_eq!(node.role(), &Role::Leader);

    let committed_index = node.propose(()).unwrap();
    persist_one_ready(&mut node);
    node.advance_applied(committed_index).unwrap();

    let result = node.propose_conf_change(ConfChange {
        expected_version: 1,
        kind: ConfChangeKind::PromoteLearner(ReplicaId::must(2)),
    });

    assert_eq!(
        result,
        Err(raft::core::node::ProposeError::LearnerNotCaughtUp {
            replica_id: ReplicaId::must(2),
            match_index: 0,
            commit_index: committed_index,
        })
    );
    assert_eq!(node.last_log_index(), committed_index);
}

#[test]
fn only_one_unapplied_configuration_change_is_admitted() {
    let initial = ConfState::new(1, [ReplicaId::must(1)], []).unwrap();
    let mut node: TestNode = RaftNode::bootstrap(
        ReplicaId::must(1),
        initial,
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    )
    .unwrap();

    persist_one_ready(&mut node);
    node.tick(node.current_election_timeout());
    persist_one_ready(&mut node);

    node.propose_conf_change(ConfChange {
        expected_version: 1,
        kind: ConfChangeKind::AddLearner(ReplicaId::must(2)),
    })
    .unwrap();

    let result = node.propose_conf_change(ConfChange {
        expected_version: 1,
        kind: ConfChangeKind::AddLearner(ReplicaId::must(3)),
    });

    assert_eq!(
        result,
        Err(raft::core::node::ProposeError::ConfigurationChangePending)
    );
}

#[test]
fn removing_the_last_voter_is_rejected_before_log_append() {
    let initial = ConfState::new(1, [ReplicaId::must(1)], []).unwrap();
    let mut node: TestNode = RaftNode::bootstrap(
        ReplicaId::must(1),
        initial,
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    )
    .unwrap();

    persist_one_ready(&mut node);
    node.tick(node.current_election_timeout());
    persist_one_ready(&mut node);
    let before = node.last_log_index();

    let result = node.propose_conf_change(ConfChange {
        expected_version: 1,
        kind: ConfChangeKind::RemoveReplica(ReplicaId::must(1)),
    });

    assert_eq!(
        result,
        Err(raft::core::node::ProposeError::CannotRemoveLeader)
    );
    assert_eq!(node.last_log_index(), before);
}

#[test]
fn restart_requires_a_durable_configuration() {
    let result = RaftNode::<(), (), TestStorage, TestStorage>::restart(
        ReplicaId::must(1),
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

/// Realistic bug caught:
///
/// Restart must not admit a durable uncommitted suffix whose membership epochs
/// cannot follow the last committed `ConfState`. Otherwise a later normal entry
/// could commit the malformed suffix after recovery and change quorum state
/// nondeterministically.
#[test]
fn restart_rejects_an_invalid_pending_configuration_suffix() {
    let durable = ConfState::new(1, [ReplicaId::must(1), ReplicaId::must(2)], []).unwrap();
    let mut stable = MemStorage::<(), ()>::new();
    stable.set_conf_state(durable);

    let mut log = MemStorage::<(), ()>::new();
    log.append(&[LogEntry {
        index: 1,
        term: 1,
        encoded_len: 24,
        payload: EntryPayload::Configuration(ConfChange {
            expected_version: 9,
            kind: ConfChangeKind::AddLearner(ReplicaId::must(3)),
        }),
    }]);

    let result = RaftNode::<(), (), TestStorage, TestStorage>::restart(
        ReplicaId::must(1),
        log,
        stable,
        5,
        2,
    );

    assert!(matches!(
        result,
        Err(InitError::InvalidPendingConfiguration {
            index: 1,
            error: ConfChangeError::VersionMismatch {
                expected: 1,
                actual: 9,
            },
        })
    ));
}

#[test]
fn legacy_restart_does_not_replace_durable_membership_with_changed_peers() {
    let durable = ConfState::new(
        7,
        [ReplicaId::must(1), ReplicaId::must(2), ReplicaId::must(3)],
        [],
    )
    .unwrap();
    let mut stable = MemStorage::<(), ()>::new();
    stable.set_conf_state(durable.clone());

    // The legacy constructor remains available to the demo runtime, but its
    // peer argument is ignored once a committed configuration is durable.
    let node: TestNode = RaftNode::new(1, vec![8, 9], MemStorage::new(), stable, 5, 2);

    assert_eq!(node.conf_state(), &durable);
    assert!(!node.conf_state().contains(ReplicaId::must(8)));
}

#[test]
fn committed_configuration_entry_emits_and_persists_conf_state() {
    let initial = ConfState::new(1, [ReplicaId::must(1)], []).unwrap();
    let mut node: TestNode = RaftNode::bootstrap(
        ReplicaId::must(1),
        initial,
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    )
    .unwrap();

    persist_one_ready(&mut node);
    node.tick(node.current_election_timeout());
    persist_one_ready(&mut node);
    assert_eq!(node.role(), &Role::Leader);

    let index = node
        .propose_conf_change(ConfChange {
            expected_version: 1,
            kind: ConfChangeKind::AddLearner(raft::types::ReplicaId::must(2)),
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
    assert!(emitted.learners.contains(&ReplicaId::must(2)));
    assert_eq!(node.durable_conf_state(), Some(&emitted));
}

/// Realistic bug caught: after a crash, a committed removal must retain the
/// exact log index, term, and resulting ConfState version needed by the
/// metadata retirement proof. Reconstructing only the final voter set would
/// make post-removal cleanup unable to prove which lifetime was removed.
#[test]
fn restart_retains_the_exact_committed_removal_proof() {
    let committed = ConfState::new(3, [ReplicaId::must(1)], []).unwrap();
    let mut stable = MemStorage::<(), ()>::new();
    stable.set_conf_state(committed);
    stable.set_hard_state(HardState {
        current_term: 4,
        voted_for: None,
        commit: 7,
    });

    let mut log = MemStorage::<(), ()>::new();
    let mut entries = (1..7)
        .map(|index| LogEntry::normal(index, 4, ()))
        .collect::<Vec<_>>();
    entries.push(LogEntry {
        index: 7,
        term: 4,
        encoded_len: 24,
        payload: EntryPayload::Configuration(ConfChange {
            expected_version: 2,
            kind: ConfChangeKind::RemoveReplica(ReplicaId::must(2)),
        }),
    });
    log.append(&entries);

    let node = RaftNode::<(), (), TestStorage, TestStorage>::restart(
        ReplicaId::must(1),
        log,
        stable,
        5,
        2,
    )
    .unwrap();

    assert_eq!(
        node.last_removed_replica(),
        Some((ReplicaId::must(2), 7, 4, 3))
    );
}

/// Realistic bug caught: once the removal entry is compacted, a snapshot
/// restore must still expose the exact proof required before metadata cleanup.
/// Without this assertion a transferred snapshot could silently erase the
/// retirement boundary and allow an unsafe tombstone decision after restart.
#[test]
fn snapshot_retains_the_exact_committed_removal_proof_after_compaction() {
    let conf_state = ConfState::new(3, [ReplicaId::must(1)], []).unwrap();
    let mut node: TestNode = RaftNode::bootstrap(
        ReplicaId::must(1),
        conf_state.clone(),
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    )
    .unwrap();

    let mut snapshot = Snapshot::new(7, 4, conf_state, ());
    snapshot.last_removed_replica = Some((ReplicaId::must(2), 7, 4, 3));
    node.restore_snapshot(snapshot);

    assert_eq!(
        node.last_removed_replica(),
        Some((ReplicaId::must(2), 7, 4, 3))
    );
}

/// Realistic bug caught:
///
/// A follower which silently accepts a configuration entry with the wrong
/// epoch can persist and acknowledge a log whose committed quorum state differs
/// from the leader. Invalid membership transitions must be rejected before the
/// follower appends or commits them.
#[test]
fn follower_rejects_invalid_configuration_before_append_or_commit() {
    let initial = ConfState::new(1, [ReplicaId::must(1), ReplicaId::must(2)], []).unwrap();
    let mut follower: TestNode = RaftNode::bootstrap(
        ReplicaId::must(1),
        initial.clone(),
        MemStorage::new(),
        MemStorage::new(),
        5,
        2,
    )
    .unwrap();
    persist_one_ready(&mut follower);

    let result = follower.step_checked(Envelope {
        from: ReplicaId::must(2),
        to: ReplicaId::must(1),
        msg: Message::AppendEntries(AppendEntriesRequest {
            term: 1,
            leader_id: ReplicaId::must(2),
            generation: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                encoded_len: 24,
                payload: EntryPayload::Configuration(ConfChange {
                    expected_version: 99,
                    kind: ConfChangeKind::AddLearner(ReplicaId::must(3)),
                }),
            }],
            leader_commit: 1,
        }),
    });

    assert_eq!(
        result,
        Err(StepError::InvalidConfigurationTransition(
            ConfChangeError::VersionMismatch {
                expected: 1,
                actual: 99,
            }
        ))
    );
    assert_eq!(follower.last_log_index(), 0);
    assert_eq!(follower.commit_index(), 0);
    assert_eq!(follower.conf_state(), &initial);
}

/// Realistic bug caught:
///
/// The durable `ConfState` shape supports joint consensus so snapshots and
/// recovery can preserve that state, but Slice 1 must not accidentally admit
/// ordinary single-configuration changes while the joint state is active.
/// Allowing one would make the next quorum transition ambiguous because the
/// old and new voter sets would no longer have a single serialized owner.
#[test]
fn configuration_changes_are_rejected_while_joint_state_is_active() {
    let mut state = ConfState::new(3, [ReplicaId::must(1), ReplicaId::must(2)], []).unwrap();
    state.outgoing_voters.insert(ReplicaId::must(3));

    let result = state.apply(&ConfChange {
        expected_version: 3,
        kind: ConfChangeKind::AddLearner(ReplicaId::must(4)),
    });

    assert!(result.is_err());
}
