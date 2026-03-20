use std::collections::HashSet;

use crate::{
    core::ready::Ready,
    entry::LogEntry,
    message::Envelope,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{HardState, LeaderState, LogIndex, NodeId, Role, Snapshot, SoftState, Term},
};

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ProposeError {
    NotLeader,
}

#[derive(Debug)]
pub struct RaftNode<C, S, LS, SS>
where
    C: Clone,
    S: Clone,
    LS: LogStore<C>,
    SS: StableStore,
{
    pub(crate) id: NodeId,
    pub(crate) peers: Vec<NodeId>,

    pub(crate) soft_state: SoftState,
    pub(crate) stable: SS,
    pub(crate) log: LS,

    pub(crate) commit_index: LogIndex,
    pub(crate) last_applied: LogIndex,

    pub(crate) leader_state: Option<LeaderState>,

    pub(crate) election_elapsed: u64,
    pub(crate) election_timeout: u64,
    pub(crate) randomized_election_timeout: u64,
    pub(crate) election_rng_state: u64,
    pub(crate) heartbeat_elapsed: u64,
    pub(crate) heartbeat_interval: u64,

    pub(crate) votes_received: HashSet<NodeId>,
    pub(crate) prevote_phase: bool,
    pub(crate) leader_recent_active: HashSet<NodeId>,

    pub(crate) outbox: Vec<Envelope<C, S>>,
    pub(crate) committed: Vec<LogEntry<C>>,

    pub(crate) pending_hard_state: Option<HardState>,
    pub(crate) pending_entries: Vec<LogEntry<C>>,
    pub(crate) pending_snapshot: Option<Snapshot<S>>,
    pub(crate) latest_snapshot: Option<Snapshot<S>>,
    pub(crate) soft_state_changed: bool,
}

impl<C, S, LS, SS> RaftNode<C, S, LS, SS>
where
    C: Clone,
    S: Clone,
    LS: LogStore<C>,
    SS: StableStore,
{
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        log: LS,
        stable: SS,
        election_timeout: u64,
        heartbeat_interval: u64,
    ) -> Self {
        let hard_state = stable.hard_state();
        let election_timeout = election_timeout.max(1);
        let election_rng_state =
            Self::initial_election_rng_seed(id, &peers, election_timeout, heartbeat_interval);

        Self {
            id,
            peers,
            soft_state: SoftState::default(),
            stable,
            log,
            commit_index: hard_state.commit,
            last_applied: 0,
            leader_state: None,
            election_elapsed: 0,
            election_timeout,
            randomized_election_timeout: election_timeout,
            election_rng_state,
            heartbeat_elapsed: 0,
            heartbeat_interval,
            votes_received: HashSet::new(),
            prevote_phase: false,
            leader_recent_active: HashSet::new(),
            outbox: Vec::new(),
            committed: Vec::new(),
            pending_hard_state: None,
            pending_entries: Vec::new(),
            pending_snapshot: None,
            latest_snapshot: None,
            soft_state_changed: false,
        }
    }

    pub fn propose(&mut self, cmd: C) -> Result<LogIndex, ProposeError> {
        if self.soft_state.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }

        let entry = LogEntry {
            index: self.last_log_index() + 1,
            term: self.current_term(),
            command: cmd,
        };

        self.log.append(std::slice::from_ref(&entry));
        self.pending_entries.push(entry.clone());

        self.broadcast_append_entries();

        Ok(entry.index)
    }

    pub fn ready(&mut self) -> Ready<C, S> {
        let ready = Ready {
            hard_state: self.pending_hard_state.take(),
            entries_to_persist: std::mem::take(&mut self.pending_entries),
            snapshot: self.pending_snapshot.take(),
            messages: std::mem::take(&mut self.outbox),
            committed_entries: std::mem::take(&mut self.committed),
            soft_state_changed: self.soft_state_changed,
        };

        self.soft_state_changed = false;
        ready
    }

    pub fn advance(&mut self, applied_through: LogIndex) {
        if applied_through > self.last_applied {
            self.last_applied = applied_through;
        }
    }

    pub fn restore_snapshot(&mut self, snapshot: Snapshot<S>) {
        let snapshot_index = snapshot.last_included_index;
        let snapshot_term = snapshot.last_included_term;

        if self.is_snapshot_stale(snapshot_index, snapshot_term) {
            return;
        }

        self.log.install_snapshot(snapshot_index, snapshot_term);
        self.commit_to_snapshot(snapshot_index);
        self.committed.retain(|entry| entry.index > snapshot_index);
        self.pending_snapshot = None;
        self.latest_snapshot = Some(snapshot);
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> &Role {
        &self.soft_state.role
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.soft_state.leader_id
    }

    pub fn hard_state(&self) -> HardState {
        self.stable.hard_state()
    }

    pub fn soft_state(&self) -> &SoftState {
        &self.soft_state
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    pub fn first_log_index(&self) -> LogIndex {
        self.log.first_index()
    }

    pub fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }

    pub fn last_log_term(&self) -> Term {
        self.log.term(self.last_log_index()).unwrap_or(0)
    }

    pub fn latest_snapshot(&self) -> Option<&Snapshot<S>> {
        self.latest_snapshot.as_ref()
    }

    pub fn current_election_timeout(&self) -> u64 {
        self.randomized_election_timeout
    }

    pub(crate) fn stage_snapshot(&mut self, snapshot: Snapshot<S>) {
        let snapshot_index = snapshot.last_included_index;
        let snapshot_term = snapshot.last_included_term;

        if self.should_ignore_staged_snapshot(snapshot_index, snapshot_term) {
            return;
        }

        self.log.install_snapshot(snapshot_index, snapshot_term);
        self.commit_to_snapshot(snapshot_index);
        self.pending_entries.clear();
        self.committed.retain(|entry| entry.index > snapshot_index);
        self.pending_snapshot = Some(snapshot);
    }

    pub(crate) fn randomize_next_election_timeout(&mut self) {
        let base = self.election_timeout.max(1);
        let jitter = self.next_election_random_u64() % base;
        self.randomized_election_timeout = base + jitter;
    }

    fn commit_to_snapshot(&mut self, snapshot_index: LogIndex) {
        if snapshot_index <= self.commit_index {
            return;
        }

        self.commit_index = snapshot_index;

        let mut hs = self.stable.hard_state();
        if hs.commit < snapshot_index {
            hs.commit = snapshot_index;
            self.set_hard_state(hs);
        }
    }

    fn current_snapshot_index(&self) -> LogIndex {
        self.log.first_index().saturating_sub(1)
    }

    fn current_snapshot_term(&self) -> Term {
        let index = self.current_snapshot_index();

        if index == 0 {
            0
        } else {
            self.log.term(index).unwrap_or(0)
        }
    }

    fn is_snapshot_stale(&self, snapshot_index: LogIndex, snapshot_term: Term) -> bool {
        let current_index = self.current_snapshot_index();
        let current_term = self.current_snapshot_term();

        snapshot_index < current_index
            || (snapshot_index == current_index && snapshot_term < current_term)
    }

    fn should_ignore_staged_snapshot(&self, snapshot_index: LogIndex, snapshot_term: Term) -> bool {
        let current_index = self.current_snapshot_index();
        let current_term = self.current_snapshot_term();

        self.is_snapshot_stale(snapshot_index, snapshot_term)
            || (snapshot_index == current_index && snapshot_term == current_term)
    }

    fn next_election_random_u64(&mut self) -> u64 {
        let mut state = self.election_rng_state;

        if state == 0 {
            state = 1;
        }

        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        self.election_rng_state = state;
        state
    }

    fn initial_election_rng_seed(
        id: NodeId,
        peers: &[NodeId],
        election_timeout: u64,
        heartbeat_interval: u64,
    ) -> u64 {
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        seed ^= id.rotate_left(7);
        seed ^= (peers.len() as u64).rotate_left(17);
        seed ^= election_timeout.rotate_left(31);
        seed ^= heartbeat_interval.rotate_left(47);

        if seed == 0 { 1 } else { seed }
    }
}
