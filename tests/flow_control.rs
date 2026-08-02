use raft::{
    core::node::{ProposeError, RaftLimits, RaftNode},
    entry::LogEntry,
    message::{Envelope, Message},
    storage::mem::MemStorage,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{HardState, ProgressMode, Role},
};

type TestStorage = MemStorage<u64, ()>;
type TestNode = RaftNode<u64, (), TestStorage, TestStorage>;

fn single_voter_leader(limits: RaftLimits) -> TestNode {
    let mut node = RaftNode::new(1, vec![], MemStorage::new(), MemStorage::new(), 5, 2);
    node.set_limits(limits);
    node.tick(node.current_election_timeout());
    assert_eq!(node.role(), &Role::Leader);
    node
}

fn take_messages(node: &mut TestNode) -> Vec<Envelope<u64, ()>> {
    let Some(ready) = node.ready() else {
        return Vec::new();
    };
    node.persist_ready_to_embedded_storage(&ready).unwrap();
    node.advance_persisted(ready.id).unwrap();
    ready.messages
}

fn elect_with_one_follower(
    leader: &mut TestNode,
    follower: &mut TestNode,
) -> Vec<Envelope<u64, ()>> {
    leader.tick(leader.current_election_timeout());
    let prevote = take_messages(leader)
        .into_iter()
        .find(|message| message.to == follower.id())
        .unwrap();
    follower.step(prevote);
    let response = take_messages(follower).pop().unwrap();
    leader.step(response);

    let vote = take_messages(leader)
        .into_iter()
        .find(|message| message.to == follower.id())
        .unwrap();
    follower.step(vote);
    let response = take_messages(follower).pop().unwrap();
    leader.step(response);

    assert_eq!(leader.role(), &Role::Leader);
    take_messages(leader)
}

#[test]
fn oversized_proposal_is_rejected_before_mutating_the_log() {
    let mut node = single_voter_leader(RaftLimits {
        max_proposal_bytes: 4,
        ..RaftLimits::default()
    });

    let result = node.propose_with_size(7, 5);

    assert_eq!(
        result,
        Err(ProposeError::ProposalTooLarge {
            encoded_bytes: 5,
            limit: 4,
        })
    );
    assert_eq!(node.last_log_index(), 0);
}

#[test]
fn catch_up_batches_respect_entry_limits_and_progress_modes() {
    let mut log = MemStorage::new();
    log.append(&[
        LogEntry::normal(1, 1, 10),
        LogEntry::normal(2, 1, 20),
        LogEntry::normal(3, 1, 30),
        LogEntry::normal(4, 1, 40),
        LogEntry::normal(5, 1, 50),
    ]);
    let mut stable = MemStorage::new();
    stable.set_hard_state(HardState {
        current_term: 1,
        voted_for: None,
        commit: 0,
    });
    let mut leader = RaftNode::new(1, vec![2, 3], log, stable, 5, 2);
    leader.set_limits(RaftLimits {
        max_append_entries: 2,
        ..RaftLimits::default()
    });
    let mut follower = RaftNode::new(2, vec![1, 3], MemStorage::new(), MemStorage::new(), 5, 2);

    let heartbeat = elect_with_one_follower(&mut leader, &mut follower)
        .into_iter()
        .find(|message| message.to == 2)
        .unwrap();
    follower.step(heartbeat);
    leader.step(take_messages(&mut follower).pop().unwrap());

    let first_batch = take_messages(&mut leader);
    let request = first_batch
        .iter()
        .find_map(|message| match &message.msg {
            Message::AppendEntries(request) if message.to == 2 => Some(request),
            _ => None,
        })
        .unwrap();
    assert_eq!(request.entries.len(), 2);
    assert_eq!(leader.progress(2).unwrap().mode, ProgressMode::Probe);
    assert_eq!(leader.progress(2).unwrap().inflight_batches, 1);

    follower.step(
        first_batch
            .into_iter()
            .find(|message| message.to == 2)
            .unwrap(),
    );
    leader.step(take_messages(&mut follower).pop().unwrap());

    let second_batch = take_messages(&mut leader);
    let request = second_batch
        .iter()
        .find_map(|message| match &message.msg {
            Message::AppendEntries(request) if message.to == 2 => Some(request),
            _ => None,
        })
        .unwrap();
    assert_eq!(request.entries.len(), 2);
    assert_eq!(leader.progress(2).unwrap().mode, ProgressMode::Replicate);
}
