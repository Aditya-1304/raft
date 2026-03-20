use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use raft::{
    core::{node::RaftNode, ready::Ready},
    entry::LogEntry,
    message::{Envelope, InstallSnapshotRequest, Message},
    storage::{codec::U64Codec, file::FileSnapshotStore, mem::MemStorage},
    traits::{log_store::LogStore, snapshot_store::SnapshotStore, stable_store::StableStore},
    types::{HardState, Role, Snapshot},
};

type TestCmd = u64;
type TestSnap = u64;
type TestStorage = MemStorage<TestCmd, TestSnap>;
type TestNode = RaftNode<TestCmd, TestSnap, TestStorage, TestStorage>;

const ELECTION_TIMEOUT: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 2;

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

fn new_node_with_log(id: u64, peers: Vec<u64>, entries: &[(u64, u64, TestCmd)]) -> TestNode {
    let mut log = MemStorage::new();

    let seeded_entries: Vec<LogEntry<TestCmd>> = entries
        .iter()
        .map(|(index, term, command)| LogEntry {
            index: *index,
            term: *term,
            command: *command,
        })
        .collect();

    log.append(&seeded_entries);

    let current_term = entries.last().map(|(_, term, _)| *term).unwrap_or(0);
    let mut stable = MemStorage::new();
    stable.set_hard_state(HardState {
        current_term,
        voted_for: None,
        commit: 0,
    });

    RaftNode::new(id, peers, log, stable, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL)
}

fn take_ready(node: &mut TestNode) -> Ready<TestCmd, TestSnap> {
    node.ready()
}

fn take_messages(node: &mut TestNode) -> Vec<Envelope<TestCmd, TestSnap>> {
    node.ready().messages
}

#[test]
fn mem_log_compaction_retains_boundary_term_and_visible_suffix() {
    let mut store = MemStorage::<TestCmd, TestSnap>::new();

    store.append(&[
        LogEntry {
            index: 1,
            term: 1,
            command: 10,
        },
        LogEntry {
            index: 2,
            term: 1,
            command: 20,
        },
        LogEntry {
            index: 3,
            term: 2,
            command: 30,
        },
        LogEntry {
            index: 4,
            term: 2,
            command: 40,
        },
    ]);

    store.compact(2);

    assert_eq!(store.first_index(), 3);
    assert_eq!(store.last_index(), 4);
    assert_eq!(store.term(2), Some(1));
    assert_eq!(store.term(3), Some(2));
    assert_eq!(store.entry(2), None);

    let visible = store.entries(1, usize::MAX);
    assert_eq!(visible.len(), 2);
    assert_eq!(visible[0].index, 3);
    assert_eq!(visible[0].term, 2);
    assert_eq!(visible[0].command, 30);
    assert_eq!(visible[1].index, 4);
    assert_eq!(visible[1].term, 2);
    assert_eq!(visible[1].command, 40);
}

#[test]
fn file_snapshot_store_round_trips_latest_snapshot() {
    let dir = TestDir::new("file-snapshot-store");
    let path = dir.path().join("snapshot.txt");

    let snapshot = Snapshot {
        last_included_index: 5,
        last_included_term: 2,
        data: 77,
    };

    let mut store = FileSnapshotStore::open(path.clone(), U64Codec).unwrap();
    assert!(store.latest().is_none());

    store.save(snapshot.clone());
    assert_eq!(store.latest(), Some(&snapshot));
    assert_eq!(store.last_included_index(), 5);
    assert_eq!(store.last_included_term(), 2);

    drop(store);

    let reopened = FileSnapshotStore::open(path, U64Codec).unwrap();
    assert_eq!(reopened.latest(), Some(&snapshot));
    assert_eq!(reopened.last_included_index(), 5);
    assert_eq!(reopened.last_included_term(), 2);
}

#[test]
fn install_snapshot_stages_ready_snapshot_and_acks_leader() {
    let mut follower = new_node(2, vec![1, 3]);

    let snapshot = Snapshot {
        last_included_index: 5,
        last_included_term: 2,
        data: 99,
    };

    follower.step(Envelope {
        from: 1,
        to: 2,
        msg: Message::InstallSnapshot(InstallSnapshotRequest {
            term: 3,
            leader_id: 1,
            snapshot: snapshot.clone(),
        }),
    });

    let ready = take_ready(&mut follower);

    assert_eq!(follower.role(), &Role::Follower);
    assert_eq!(follower.leader_id(), Some(1));
    assert_eq!(follower.commit_index(), 5);
    assert_eq!(follower.first_log_index(), 6);
    assert_eq!(follower.last_log_index(), 5);

    let hs = ready.hard_state.expect("expected hard state update");
    assert_eq!(hs.current_term, 3);
    assert_eq!(hs.voted_for, None);
    assert_eq!(hs.commit, 5);

    assert!(ready.entries_to_persist.is_empty());
    assert_eq!(ready.snapshot, Some(snapshot.clone()));
    assert_eq!(ready.messages.len(), 1);

    match &ready.messages[0].msg {
        Message::InstallSnapshotResponse(resp) => {
            assert!(resp.success);
            assert_eq!(resp.term, 3);
            assert_eq!(resp.last_included_index, 5);
        }
        _ => panic!("expected InstallSnapshotResponse"),
    }
}

#[test]
fn restoring_staged_snapshot_promotes_it_to_latest_snapshot() {
    let mut follower = new_node(2, vec![1, 3]);

    let snapshot = Snapshot {
        last_included_index: 4,
        last_included_term: 2,
        data: 123,
    };

    follower.step(Envelope {
        from: 1,
        to: 2,
        msg: Message::InstallSnapshot(InstallSnapshotRequest {
            term: 3,
            leader_id: 1,
            snapshot: snapshot.clone(),
        }),
    });

    let ready = take_ready(&mut follower);
    let staged_snapshot = ready.snapshot.expect("expected staged snapshot");

    follower.restore_snapshot(staged_snapshot.clone());

    assert_eq!(follower.latest_snapshot(), Some(&staged_snapshot));
}

#[test]
fn lagging_follower_receives_install_snapshot_when_leader_compacted_prefix() {
    let snapshot = Snapshot {
        last_included_index: 2,
        last_included_term: 1,
        data: 500,
    };

    let mut nodes = [
        new_node_with_log(1, vec![2, 3], &[(1, 1, 10), (2, 1, 20), (3, 2, 30)]),
        new_node(2, vec![1, 3]),
        new_node(3, vec![1, 2]),
    ];

    nodes[0].restore_snapshot(snapshot.clone());
    assert_eq!(nodes[0].latest_snapshot(), Some(&snapshot));
    assert_eq!(nodes[0].first_log_index(), 3);
    assert_eq!(nodes[0].last_log_index(), 3);

    nodes[0].tick(ELECTION_TIMEOUT);
    let vote_requests = take_messages(&mut nodes[0]);
    assert_eq!(vote_requests.len(), 2);

    for msg in vote_requests {
        match msg.to {
            2 => nodes[1].step(msg),
            3 => nodes[2].step(msg),
            _ => unreachable!(),
        }
    }

    let mut vote_responses = Vec::new();
    vote_responses.extend(take_messages(&mut nodes[1]));
    vote_responses.extend(take_messages(&mut nodes[2]));

    for msg in vote_responses {
        nodes[0].step(msg);
    }

    assert_eq!(nodes[0].role(), &Role::Leader);

    let leader_ready = take_ready(&mut nodes[0]);
    let to_follower_2: Vec<_> = leader_ready
        .messages
        .into_iter()
        .filter(|msg| msg.to == 2)
        .collect();
    assert_eq!(to_follower_2.len(), 1);

    match &to_follower_2[0].msg {
        Message::AppendEntries(req) => {
            assert_eq!(req.prev_log_index, 3);
            assert_eq!(req.prev_log_term, 2);
            assert!(req.entries.is_empty());
        }
        _ => panic!("expected initial AppendEntries heartbeat"),
    }

    nodes[1].step(to_follower_2.into_iter().next().unwrap());

    let rejection = take_messages(&mut nodes[1]);
    assert_eq!(rejection.len(), 1);

    match &rejection[0].msg {
        Message::AppendEntriesResponse(resp) => {
            assert!(!resp.success);
            assert_eq!(resp.conflict_term, None);
            assert_eq!(resp.conflict_index, Some(1));
        }
        _ => panic!("expected AppendEntriesResponse rejection"),
    }

    nodes[0].step(rejection.into_iter().next().unwrap());

    let snapshot_send = take_messages(&mut nodes[0]);
    assert_eq!(snapshot_send.len(), 1);

    match &snapshot_send[0].msg {
        Message::InstallSnapshot(req) => {
            assert_eq!(req.term, nodes[0].current_term());
            assert_eq!(req.leader_id, 1);
            assert_eq!(req.snapshot, snapshot);
        }
        _ => panic!("expected InstallSnapshot"),
    }

    nodes[1].step(snapshot_send.into_iter().next().unwrap());

    let follower_ready = take_ready(&mut nodes[1]);
    assert_eq!(follower_ready.snapshot, Some(snapshot.clone()));
    assert_eq!(follower_ready.messages.len(), 1);

    match &follower_ready.messages[0].msg {
        Message::InstallSnapshotResponse(resp) => {
            assert!(resp.success);
            assert_eq!(resp.last_included_index, 2);
        }
        _ => panic!("expected InstallSnapshotResponse"),
    }

    nodes[0].step(follower_ready.messages.into_iter().next().unwrap());

    let append_after_snapshot = take_messages(&mut nodes[0]);
    assert_eq!(append_after_snapshot.len(), 1);

    match &append_after_snapshot[0].msg {
        Message::AppendEntries(req) => {
            assert_eq!(req.prev_log_index, 2);
            assert_eq!(req.prev_log_term, 1);
            assert_eq!(req.entries.len(), 1);
            assert_eq!(req.entries[0].index, 3);
            assert_eq!(req.entries[0].term, 2);
            assert_eq!(req.entries[0].command, 30);
        }
        _ => panic!("expected AppendEntries after snapshot install"),
    }

    nodes[1].step(append_after_snapshot.into_iter().next().unwrap());

    let follower_append_response = take_messages(&mut nodes[1]);
    assert_eq!(follower_append_response.len(), 1);
    nodes[0].step(follower_append_response.into_iter().next().unwrap());

    assert_eq!(nodes[1].first_log_index(), 3);
    assert_eq!(nodes[1].last_log_index(), 3);
    assert_eq!(nodes[1].last_log_term(), 2);
}
