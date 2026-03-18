use std::collections::HashSet;

use crate::{
    core::ready::Ready,
    entry::LogEntry,
    message::Envelope,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{HardState, LeaderState, LogIndex, NodeId, Role, SoftState, Term},
};

#[derive(Debug)]
pub struct RaftNode<C, LS, SS>
where
    C: Clone,
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
    pub(crate) heartbeat_elapsed: u64,
    pub(crate) heartbeat_interval: u64,

    pub(crate) votes_received: HashSet<NodeId>,

    pub(crate) outbox: Vec<Envelope<C>>,
    pub(crate) committed: Vec<LogEntry<C>>,

    pub(crate) pending_hard_state: Option<HardState>,
    pub(crate) pending_entries: Vec<LogEntry<C>>,
    pub(crate) soft_state_changed: bool,
}

impl<C, LS, SS> RaftNode<C, LS, SS>
where
    C: Clone,
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
            heartbeat_elapsed: 0,
            heartbeat_interval,
            votes_received: HashSet::new(),
            outbox: Vec::new(),
            committed: Vec::new(),
            pending_hard_state: None,
            pending_entries: Vec::new(),
            soft_state_changed: false,
        }
    }

    pub fn ready(&mut self) -> Ready<C> {
        let ready = Ready {
            hard_state: self.pending_hard_state.take(),
            entries_to_persist: std::mem::take(&mut self.pending_entries),
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

    pub fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }

    pub fn last_log_term(&self) -> Term {
        self.log.term(self.last_log_index()).unwrap_or(0)
    }
}
