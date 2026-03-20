use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use raft::{
    core::{node::RaftNode, ready::Ready},
    message::Envelope,
    storage::{
        codec::U64Codec,
        file::{FileLogStore, FileStableStore},
    },
    traits::log_store::LogStore,
    types::Role,
};

type TestCmd = u64;
type TestLogStore = FileLogStore<TestCmd, U64Codec>;
type TestStableStore = FileStableStore;
type TestNode = RaftNode<TestCmd, (), TestLogStore, TestStableStore>;

const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;

#[derive(Debug, Clone)]
struct NodeFiles {
    stable: PathBuf,
    log: PathBuf,
}

#[derive(Debug)]
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = env::temp_dir().join(format!("raft-{prefix}-{}-{unique}", process::id()));

        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn cluster_files(root: &Path) -> [NodeFiles; 3] {
    [1_u64, 2, 3].map(|id| {
        let node_dir = root.join(format!("node-{id}"));
        NodeFiles {
            stable: node_dir.join("hard_state.txt"),
            log: node_dir.join("log.txt"),
        }
    })
}

fn open_node(id: u64, peers: Vec<u64>, files: &NodeFiles) -> TestNode {
    let log = FileLogStore::open(files.log.clone(), U64Codec).unwrap();
    let stable = FileStableStore::open(files.stable.clone()).unwrap();

    RaftNode::new(id, peers, log, stable, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL)
}

fn open_cluster(files: &[NodeFiles; 3]) -> [TestNode; 3] {
    [
        open_node(1, vec![2, 3], &files[0]),
        open_node(2, vec![1, 3], &files[1]),
        open_node(3, vec![1, 2], &files[2]),
    ]
}

fn reopen_log(files: &NodeFiles) -> TestLogStore {
    FileLogStore::open(files.log.clone(), U64Codec).unwrap()
}

fn deliver(nodes: &mut [TestNode; 3], messages: Vec<Envelope<TestCmd, ()>>) {
    for msg in messages {
        let idx = (msg.to - 1) as usize;
        nodes[idx].step(msg);
    }
}

fn take_ready(node: &mut TestNode) -> Ready<TestCmd, ()> {
    node.ready()
}

fn drain_messages(node: &mut TestNode) -> Vec<Envelope<TestCmd, ()>> {
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
fn crash_after_commit_recovers_persisted_state() {
    let dir = TestDir::new("crash-after-commit");
    let files = cluster_files(dir.path());
    let mut nodes = open_cluster(&files);

    elect_leader(&mut nodes, 0);
    let term_before_crash = nodes[0].current_term();

    nodes[0].propose(10).unwrap();
    let leader_ready = take_ready(&mut nodes[0]);

    assert_eq!(leader_ready.entries_to_persist.len(), 1);
    assert!(leader_ready.committed_entries.is_empty());

    let to_n2: Vec<_> = leader_ready
        .messages
        .into_iter()
        .filter(|msg| msg.to == 2)
        .collect();
    deliver(&mut nodes, to_n2);

    let follower_ready = take_ready(&mut nodes[1]);
    assert_eq!(follower_ready.entries_to_persist.len(), 1);
    assert_eq!(follower_ready.messages.len(), 1);
    deliver(&mut nodes, follower_ready.messages);

    let leader_commit_ready = take_ready(&mut nodes[0]);
    assert_eq!(leader_commit_ready.committed_entries.len(), 1);
    assert_eq!(leader_commit_ready.committed_entries[0].command, 10);
    assert_eq!(nodes[0].commit_index(), 1);
    assert_eq!(nodes[0].hard_state().commit, 1);

    drop(nodes);

    let restarted = open_node(1, vec![2, 3], &files[0]);
    assert_eq!(restarted.role(), &Role::Follower);
    assert_eq!(restarted.leader_id(), None);
    assert_eq!(restarted.current_term(), term_before_crash);
    assert_eq!(restarted.hard_state().voted_for, Some(1));
    assert_eq!(restarted.commit_index(), 1);
    assert_eq!(restarted.hard_state().commit, 1);
    assert_eq!(restarted.last_log_index(), 1);
    assert_eq!(restarted.last_log_term(), term_before_crash);

    let reopened_log = reopen_log(&files[0]);
    assert_eq!(reopened_log.entry(1).unwrap().command, 10);
}

#[test]
fn crash_before_commit_preserves_uncommitted_log() {
    let dir = TestDir::new("crash-before-commit");
    let files = cluster_files(dir.path());
    let mut nodes = open_cluster(&files);

    elect_leader(&mut nodes, 0);
    let term_before_crash = nodes[0].current_term();

    nodes[0].propose(10).unwrap();
    let leader_ready = take_ready(&mut nodes[0]);

    assert_eq!(leader_ready.entries_to_persist.len(), 1);
    assert!(leader_ready.committed_entries.is_empty());
    assert_eq!(nodes[0].commit_index(), 0);
    assert_eq!(nodes[0].hard_state().commit, 0);

    drop(nodes);

    let restarted = open_node(1, vec![2, 3], &files[0]);
    assert_eq!(restarted.role(), &Role::Follower);
    assert_eq!(restarted.leader_id(), None);
    assert_eq!(restarted.current_term(), term_before_crash);
    assert_eq!(restarted.commit_index(), 0);
    assert_eq!(restarted.hard_state().commit, 0);
    assert_eq!(restarted.last_log_index(), 1);
    assert_eq!(restarted.last_log_term(), term_before_crash);

    let reopened_log = reopen_log(&files[0]);
    assert_eq!(reopened_log.entry(1).unwrap().command, 10);
}

#[test]
fn full_restart_and_lagging_follower_catches_up() {
    let dir = TestDir::new("full-restart-catch-up");
    let files = cluster_files(dir.path());
    let mut nodes = open_cluster(&files);

    elect_leader(&mut nodes, 0);

    nodes[0].propose(10).unwrap();
    let leader_ready = take_ready(&mut nodes[0]);

    let to_n2: Vec<_> = leader_ready
        .messages
        .into_iter()
        .filter(|msg| msg.to == 2)
        .collect();
    deliver(&mut nodes, to_n2);

    let n2_ready = take_ready(&mut nodes[1]);
    deliver(&mut nodes, n2_ready.messages);

    let leader_commit_ready = take_ready(&mut nodes[0]);
    assert_eq!(leader_commit_ready.committed_entries.len(), 1);
    assert_eq!(nodes[2].last_log_index(), 0);

    drop(nodes);

    let mut restarted = open_cluster(&files);
    assert_eq!(restarted[0].role(), &Role::Follower);
    assert_eq!(restarted[1].role(), &Role::Follower);
    assert_eq!(restarted[2].role(), &Role::Follower);
    assert_eq!(restarted[2].last_log_index(), 0);

    elect_leader(&mut restarted, 0);

    assert_eq!(restarted[0].role(), &Role::Leader);
    assert_eq!(restarted[2].last_log_index(), 1);
    assert_eq!(restarted[2].last_log_term(), 1);
    assert_eq!(restarted[2].commit_index(), 1);
    assert_eq!(restarted[2].hard_state().commit, 1);

    let node3_log = reopen_log(&files[2]);
    assert_eq!(node3_log.entry(1).unwrap().command, 10);
}
