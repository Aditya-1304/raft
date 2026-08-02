use std::{
    env, fs,
    path::PathBuf,
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use raft::{
    core::{
        node::RaftNode,
        ready::{AdvanceError, ReadyId},
    },
    storage::{
        codec::U64Codec,
        file::{FileLogStore, FileStableStore},
    },
    traits::log_store::LogStore,
    types::Role,
};

type TestNode = RaftNode<u64, (), FileLogStore<u64, U64Codec>, FileStableStore>;

const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;

#[derive(Debug)]
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!("raft-ready-ack-{}-{unique}", process::id()));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn new_node(dir: &TestDir) -> TestNode {
    let log = FileLogStore::open(dir.path.join("log.txt"), U64Codec).unwrap();
    let stable = FileStableStore::open(dir.path.join("hard-state.txt")).unwrap();

    RaftNode::new(
        1,
        Vec::new(),
        log,
        stable,
        ELECTION_TIMEOUT,
        HEARTBEAT_INTERVAL,
    )
}

fn elect_and_persist(node: &mut TestNode) -> ReadyId {
    node.tick(node.current_election_timeout());
    assert_eq!(node.role(), &Role::Leader);

    let ready = node.ready().expect("election must produce Ready");
    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();
    ready.id
}

/// Realistic bug caught:
///
/// A transient A-WAL append or synchronization failure must not make the core
/// forget the exact entries and HardState which remain unacknowledged.
#[test]
fn ready_is_stable_until_persisted_acknowledgement() {
    let dir = TestDir::new();
    let mut node = new_node(&dir);
    elect_and_persist(&mut node);

    node.propose(42).unwrap();

    let first = node.ready().expect("proposal must produce Ready");
    let repeated = node
        .ready()
        .expect("unacknowledged Ready must remain observable");

    assert_eq!(repeated, first);
    assert_eq!(first.entries_to_persist.len(), 1);

    node.persist_ready_to_embedded_storage(&first).unwrap();
    node.advance_persisted(first.id).unwrap();

    assert_eq!(node.ready(), None);
    assert_eq!(
        node.advance_persisted(first.id),
        Err(AdvanceError::NoReadyPending)
    );
}

/// Realistic bug caught:
///
/// Proposal acceptance must not write the replicated entry through to disk
/// before the host has admitted and synchronized the corresponding Ready.
#[test]
fn logical_proposal_does_not_write_through_the_durable_log() {
    let dir = TestDir::new();
    let mut node = new_node(&dir);
    elect_and_persist(&mut node);

    node.propose(7).unwrap();
    let ready = node.ready().expect("proposal must produce Ready");

    let before_ack =
        FileLogStore::<u64, U64Codec>::open(dir.path.join("log.txt"), U64Codec).unwrap();
    assert_eq!(before_ack.last_index(), 0);
    assert_eq!(node.last_log_index(), 1);
    assert_eq!(node.durable_log_index(), 0);

    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();

    let after_ack =
        FileLogStore::<u64, U64Codec>::open(dir.path.join("log.txt"), U64Codec).unwrap();
    assert_eq!(after_ack.last_index(), 1);
    assert_eq!(node.durable_log_index(), 1);
}

/// Realistic bug caught:
///
/// State-machine application must not move beyond the commit index contained
/// in successfully persisted HardState.
#[test]
fn applied_frontier_cannot_advance_beyond_durable_commit() {
    let dir = TestDir::new();
    let mut node = new_node(&dir);
    elect_and_persist(&mut node);

    node.propose(9).unwrap();
    let ready = node.ready().expect("proposal must produce Ready");

    assert_eq!(
        node.advance_applied(1),
        Err(AdvanceError::AppliedBeyondDurableCommit {
            durable_commit: 0,
            attempted: 1,
        })
    );

    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();

    // A single-voter group commits its own proposal immediately, but apply is
    // authorized only after the Ready containing that commit is durable.
    assert_eq!(node.durable_hard_state().commit, 1);
    assert_eq!(node.advance_applied(1), Ok(()));
}

/// Realistic bug caught:
///
/// An asynchronous completion for an older disk batch must not acknowledge a
/// newer Ready generation after the older generation has already advanced.
#[test]
fn stale_persistence_completion_cannot_acknowledge_a_new_generation() {
    let dir = TestDir::new();
    let mut node = new_node(&dir);
    let election_ready_id = elect_and_persist(&mut node);

    node.propose(11).unwrap();
    let proposal_ready = node.ready().expect("proposal must produce Ready");

    assert_eq!(
        node.advance_persisted(election_ready_id),
        Err(AdvanceError::ReadyMismatch {
            expected: proposal_ready.id,
            actual: election_ready_id,
        })
    );

    // Rejecting the stale completion must leave the current generation intact.
    assert_eq!(node.ready(), Some(proposal_ready));
}
