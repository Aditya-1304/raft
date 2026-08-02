use std::collections::HashSet;

use crate::{
    core::ready::{AdvanceError, Ready, ReadyId},
    entry::{EntryPayload, LogEntry},
    message::Envelope,
    storage::mem::MemStorage,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{
        ConfChange, ConfChangeError, ConfState, ConfStateError, HardState, LeaderState, LogIndex,
        NodeId, Role, Snapshot, SnapshotMetadata, SoftState, Term,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ProposeError {
    NotLeader,
    ConfigurationChangePending,
    InvalidConfiguration(ConfChangeError),
    LearnerNotCaughtUp {
        replica_id: NodeId,
        match_index: LogIndex,
        commit_index: LogIndex,
    },
    CannotRemoveLeader,
    ProposalTooLarge {
        encoded_bytes: usize,
        limit: usize,
    },
    UncommittedEntriesFull {
        limit: usize,
    },
    UncommittedBytesFull {
        current: usize,
        attempted: usize,
        limit: usize,
    },
    LogIndexExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftError {
    TermExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepError {
    WrongDestination { expected: NodeId, actual: NodeId },
    UnknownReplica(NodeId),
    PayloadIdentityMismatch { envelope: NodeId, payload: NodeId },
    IndexOverflow,
    InvalidSnapshotConfiguration(ConfStateError),
}

/// Per-group resource limits enforced by the pure Raft core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftLimits {
    pub max_proposal_bytes: usize,
    pub max_uncommitted_entries: usize,
    pub max_uncommitted_bytes: usize,
    pub max_append_entries: usize,
    pub max_append_bytes: usize,
    pub max_inflight_append_batches: usize,
    pub max_inflight_append_bytes: usize,
}

impl Default for RaftLimits {
    fn default() -> Self {
        Self {
            max_proposal_bytes: 16 * 1024 * 1024,
            max_uncommitted_entries: 4_096,
            max_uncommitted_bytes: 64 * 1024 * 1024,
            max_append_entries: 256,
            max_append_bytes: 1024 * 1024,
            max_inflight_append_batches: 1,
            max_inflight_append_bytes: 1024 * 1024,
        }
    }
}

/// Rejects ambiguous startup paths before the core can exchange messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitError {
    InvalidConfiguration(ConfStateError),
    LocalReplicaNotMember(NodeId),
    AlreadyBootstrapped,
    MissingDurableConfiguration,
    ExistingDurableState,
    CommitBeyondLog {
        commit: LogIndex,
        last_index: LogIndex,
    },
    MissingSnapshotBoundaryTerm(LogIndex),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotInstallError {
    NoInstallPending,
    MetadataMismatch {
        expected: Box<SnapshotMetadata>,
        actual: Box<SnapshotMetadata>,
    },
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
    pub(crate) conf_state: ConfState,
    pub(crate) durable_conf_state: Option<ConfState>,

    pub(crate) soft_state: SoftState,
    /// Logical Raft state. Mutating this in-memory view never performs I/O.
    pub(crate) hard_state: HardState,
    pub(crate) log: MemStorage<C, ()>,

    /// Legacy stores retained only by the explicit file-runtime compatibility
    /// adapter. The Raft algorithm never writes through these fields.
    pub(crate) persistence_stable: SS,
    pub(crate) persistence_log: LS,

    /// Last HardState acknowledged durable by the host.
    pub(crate) durable_hard_state: HardState,
    pub(crate) durable_log_index: LogIndex,

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
    pub(crate) pending_conf_state: Option<ConfState>,
    pub(crate) pending_entries: Vec<LogEntry<C>>,
    pub(crate) pending_snapshot: Option<Snapshot<S>>,
    pub(crate) pending_snapshot_install: Option<SnapshotMetadata>,
    pub(crate) snapshot_install_expected: Option<SnapshotMetadata>,
    pub(crate) snapshot_install_source: Option<NodeId>,
    pub(crate) latest_snapshot: Option<Snapshot<S>>,
    pub(crate) soft_state_changed: bool,

    pub(crate) pending_ready: Option<Ready<C, S>>,
    pub(crate) next_ready_id: u64,
    pub(crate) snapshot_awaiting_restore: Option<LogIndex>,
    pub(crate) pending_conf_change_index: Option<LogIndex>,
    pub(crate) limits: RaftLimits,
    pub(crate) uncommitted_bytes: usize,
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
        let stored_conf_state = stable.conf_state();
        let conf_state = stored_conf_state.clone().unwrap_or_else(|| {
            ConfState::new(1, std::iter::once(id).chain(peers.iter().copied()), [])
                .expect("legacy RaftNode::new received an invalid voter set")
        });
        let mut node = Self::from_conf_state(
            id,
            conf_state.clone(),
            log,
            stable,
            election_timeout,
            heartbeat_interval,
        )
        .expect("legacy RaftNode::new received an invalid configuration");

        // The compatibility constructor treats its explicit peer list as an
        // initial bootstrap only when no durable configuration exists.
        if stored_conf_state.is_none() {
            node.pending_conf_state = Some(conf_state);
        }
        node
    }

    /// Creates a brand-new group from one explicit initial configuration.
    pub fn bootstrap(
        id: NodeId,
        conf_state: ConfState,
        log: LS,
        stable: SS,
        election_timeout: u64,
        heartbeat_interval: u64,
    ) -> Result<Self, InitError> {
        if stable.conf_state().is_some() {
            return Err(InitError::AlreadyBootstrapped);
        }
        if stable.hard_state() != HardState::default() || log.last_index() != 0 {
            return Err(InitError::ExistingDurableState);
        }

        let mut node = Self::from_conf_state(
            id,
            conf_state.clone(),
            log,
            stable,
            election_timeout,
            heartbeat_interval,
        )?;
        node.pending_conf_state = Some(conf_state);
        Ok(node)
    }

    /// Restarts exclusively from the committed configuration in stable state.
    pub fn restart(
        id: NodeId,
        log: LS,
        stable: SS,
        election_timeout: u64,
        heartbeat_interval: u64,
    ) -> Result<Self, InitError> {
        let conf_state = stable
            .conf_state()
            .ok_or(InitError::MissingDurableConfiguration)?;
        Self::from_conf_state(
            id,
            conf_state,
            log,
            stable,
            election_timeout,
            heartbeat_interval,
        )
    }

    fn from_conf_state(
        id: NodeId,
        conf_state: ConfState,
        log: LS,
        stable: SS,
        election_timeout: u64,
        heartbeat_interval: u64,
    ) -> Result<Self, InitError> {
        conf_state
            .validate()
            .map_err(InitError::InvalidConfiguration)?;
        if !conf_state.contains(id) {
            return Err(InitError::LocalReplicaNotMember(id));
        }

        let hard_state = stable.hard_state();
        let first_index = log.first_index();
        let last_index = log.last_index();
        let snapshot_index = first_index.saturating_sub(1);
        let snapshot_term = if snapshot_index == 0 {
            0
        } else {
            log.term(snapshot_index)
                .ok_or(InitError::MissingSnapshotBoundaryTerm(snapshot_index))?
        };
        if hard_state.commit > last_index {
            return Err(InitError::CommitBeyondLog {
                commit: hard_state.commit,
                last_index,
            });
        }

        // The core owns a logical in-memory log. The supplied store is only an
        // acknowledged durable recovery source and is never mutated by Raft
        // state transitions.
        let mut logical_log = MemStorage::<C, ()>::new();
        if snapshot_index > 0 {
            logical_log.install_snapshot(snapshot_index, snapshot_term);
        }
        if first_index <= last_index {
            logical_log.append(&log.entries(first_index, usize::MAX));
        }
        let election_timeout = election_timeout.max(1);
        let peers: Vec<NodeId> = conf_state
            .replication_targets()
            .into_iter()
            .filter(|peer| *peer != id)
            .collect();
        let election_rng_state =
            Self::initial_election_rng_seed(id, &peers, election_timeout, heartbeat_interval);

        let durable_log_index = logical_log.last_index();

        let pending_conf_change_index = logical_log
            .entries(hard_state.commit.saturating_add(1), usize::MAX)
            .into_iter()
            .find(|entry| matches!(entry.payload, EntryPayload::Configuration(_)))
            .map(|entry| entry.index);
        let uncommitted_bytes = logical_log
            .entries(hard_state.commit.saturating_add(1), usize::MAX)
            .iter()
            .map(|entry| entry.encoded_len)
            .sum();

        Ok(Self {
            id,
            peers,
            durable_conf_state: stable.conf_state(),
            conf_state,
            soft_state: SoftState::default(),
            hard_state: hard_state.clone(),
            log: logical_log,
            persistence_stable: stable,
            persistence_log: log,
            durable_hard_state: hard_state.clone(),
            durable_log_index,
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
            pending_conf_state: None,
            pending_entries: Vec::new(),
            pending_snapshot: None,
            pending_snapshot_install: None,
            snapshot_install_expected: None,
            snapshot_install_source: None,
            latest_snapshot: None,
            soft_state_changed: false,
            pending_ready: None,
            next_ready_id: 1,
            snapshot_awaiting_restore: None,
            pending_conf_change_index,
            limits: RaftLimits::default(),
            uncommitted_bytes,
        })
    }

    pub fn propose(&mut self, cmd: C) -> Result<LogIndex, ProposeError> {
        let encoded_len = std::mem::size_of_val(&cmd);
        self.propose_with_size(cmd, encoded_len)
    }

    /// Proposes an application command using its host-computed encoded size.
    pub fn propose_with_size(
        &mut self,
        cmd: C,
        encoded_len: usize,
    ) -> Result<LogIndex, ProposeError> {
        if self.soft_state.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }
        self.admit_proposal(encoded_len)?;

        let index = self
            .last_log_index()
            .checked_add(1)
            .ok_or(ProposeError::LogIndexExhausted)?;
        let entry = LogEntry {
            index,
            term: self.current_term(),
            encoded_len,
            payload: EntryPayload::Normal(cmd),
        };

        self.log.append(std::slice::from_ref(&entry));
        self.pending_entries.push(entry.clone());
        self.uncommitted_bytes = self.uncommitted_bytes.saturating_add(encoded_len);

        self.maybe_advance_commit();
        self.broadcast_append_entries();

        Ok(entry.index)
    }

    /// Proposes one validated configuration transition.
    ///
    /// Only one uncommitted configuration entry may exist at a time. The
    /// configuration becomes authoritative only when that log entry commits.
    pub fn propose_conf_change(&mut self, change: ConfChange) -> Result<LogIndex, ProposeError> {
        if self.soft_state.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }
        if self.pending_conf_change_index.is_some() {
            return Err(ProposeError::ConfigurationChangePending);
        }
        match change.kind {
            crate::types::ConfChangeKind::PromoteLearner(replica_id) => {
                let match_index = self
                    .leader_state
                    .as_ref()
                    .and_then(|leader| leader.progress.get(&replica_id))
                    .map(|progress| progress.match_index)
                    .unwrap_or(0);
                if match_index < self.commit_index {
                    return Err(ProposeError::LearnerNotCaughtUp {
                        replica_id,
                        match_index,
                        commit_index: self.commit_index,
                    });
                }
            }
            crate::types::ConfChangeKind::RemoveReplica(replica_id) if replica_id == self.id => {
                return Err(ProposeError::CannotRemoveLeader);
            }
            crate::types::ConfChangeKind::AddLearner(_)
            | crate::types::ConfChangeKind::RemoveReplica(_) => {}
        }
        self.conf_state
            .apply(&change)
            .map_err(ProposeError::InvalidConfiguration)?;

        let index = self
            .last_log_index()
            .checked_add(1)
            .ok_or(ProposeError::LogIndexExhausted)?;
        let entry = LogEntry {
            index,
            term: self.current_term(),
            encoded_len: 24,
            payload: EntryPayload::Configuration(change),
        };
        self.admit_proposal(entry.encoded_len)?;
        self.log.append(std::slice::from_ref(&entry));
        self.pending_entries.push(entry.clone());
        self.uncommitted_bytes = self.uncommitted_bytes.saturating_add(entry.encoded_len);
        self.pending_conf_change_index = Some(entry.index);
        self.maybe_advance_commit();
        self.broadcast_append_entries();
        Ok(entry.index)
    }

    /// Returns the outstanding Ready generation without consuming it.
    ///
    /// The same generation remains observable until `advance_persisted`
    /// acknowledges that its persistence portion is durable.
    pub fn ready(&mut self) -> Option<Ready<C, S>> {
        if let Some(ready) = self.pending_ready.as_ref() {
            return Some(ready.clone());
        }

        if !self.has_staged_ready() {
            return None;
        }

        let ready = Ready {
            id: ReadyId::new(self.next_ready_id),
            log_last_index: self.last_log_index(),
            hard_state: self.pending_hard_state.take(),
            conf_state: self.pending_conf_state.take(),
            entries_to_persist: std::mem::take(&mut self.pending_entries),
            snapshot: self.pending_snapshot.take(),
            snapshot_install: self.pending_snapshot_install.take(),
            messages: std::mem::take(&mut self.outbox),
            committed_entries: std::mem::take(&mut self.committed),
            soft_state_changed: self.soft_state_changed,
        };

        self.next_ready_id = self
            .next_ready_id
            .checked_add(1)
            .expect("Ready generation exhausted");
        self.soft_state_changed = false;
        self.pending_ready = Some(ready.clone());
        Some(ready)
    }

    /// Acknowledges that the exact outstanding Ready generation is durable.
    ///
    /// This method does not perform I/O. RagnorDB must call it only after the
    /// ordered A-WAL batch has synchronized through its exact final LSN.
    pub fn advance_persisted(&mut self, ready_id: ReadyId) -> Result<(), AdvanceError> {
        let Some(ready) = self.pending_ready.as_ref() else {
            return Err(AdvanceError::NoReadyPending);
        };

        if ready.id != ready_id {
            return Err(AdvanceError::ReadyMismatch {
                expected: ready.id,
                actual: ready_id,
            });
        }

        if let Some(hard_state) = ready.hard_state.as_ref() {
            self.durable_hard_state = hard_state.clone();
        }
        if let Some(conf_state) = ready.conf_state.as_ref() {
            self.durable_conf_state = Some(conf_state.clone());
        }
        self.durable_log_index = ready.log_last_index;

        if let Some(snapshot) = ready.snapshot.as_ref() {
            self.snapshot_awaiting_restore = Some(snapshot.last_included_index);
        }

        self.pending_ready = None;
        Ok(())
    }

    /// Advances the state-machine frontier after successful ordered apply.
    pub fn advance_applied(&mut self, applied_through: LogIndex) -> Result<(), AdvanceError> {
        if applied_through < self.last_applied {
            return Err(AdvanceError::AppliedIndexRegressed {
                current: self.last_applied,
                attempted: applied_through,
            });
        }

        if applied_through > self.durable_hard_state.commit {
            return Err(AdvanceError::AppliedBeyondDurableCommit {
                durable_commit: self.durable_hard_state.commit,
                attempted: applied_through,
            });
        }

        if let Some(snapshot_index) = self.snapshot_awaiting_restore {
            if applied_through < snapshot_index {
                return Err(AdvanceError::SnapshotNotRestored {
                    snapshot_index,
                    attempted: applied_through,
                });
            }
            self.snapshot_awaiting_restore = None;
        }

        self.last_applied = applied_through;
        if self
            .pending_conf_change_index
            .is_some_and(|index| index <= applied_through)
        {
            self.pending_conf_change_index = None;
        }
        Ok(())
    }

    /// Persists one Ready through the crate's immediate file-store adapter.
    ///
    /// Database integrations must persist Ready through their own ordered WAL
    /// and call `advance_persisted` themselves. This adapter exists solely to
    /// keep the standalone demonstration runtime operational.
    pub fn persist_ready_to_embedded_storage(
        &mut self,
        ready: &Ready<C, S>,
    ) -> Result<(), AdvanceError> {
        let Some(pending) = self.pending_ready.as_ref() else {
            return Err(AdvanceError::NoReadyPending);
        };

        if pending.id != ready.id {
            return Err(AdvanceError::ReadyMismatch {
                expected: pending.id,
                actual: ready.id,
            });
        }

        if let Some(snapshot) = ready.snapshot.as_ref() {
            self.persistence_log
                .install_snapshot(snapshot.last_included_index, snapshot.last_included_term);
        }

        self.persistence_log.append(&ready.entries_to_persist);

        if let Some(conf_state) = ready.conf_state.as_ref() {
            self.persistence_stable.set_conf_state(conf_state.clone());
        }

        // HardState is intentionally persisted last so no recoverable prefix
        // can reference an entry or snapshot boundary which appears later.
        if let Some(hard_state) = ready.hard_state.as_ref() {
            self.persistence_stable.set_hard_state(hard_state.clone());
        }

        Ok(())
    }

    pub fn restore_snapshot(&mut self, snapshot: Snapshot<S>) {
        let snapshot_index = snapshot.last_included_index;
        let snapshot_term = snapshot.last_included_term;

        if self.is_snapshot_stale(snapshot_index, snapshot_term) {
            return;
        }

        self.log.install_snapshot(snapshot_index, snapshot_term);
        self.install_conf_state(snapshot.conf_state.clone());
        self.commit_to_snapshot(snapshot_index);
        self.committed.retain(|entry| entry.index > snapshot_index);
        self.pending_snapshot = None;
        self.latest_snapshot = Some(snapshot);
        self.refresh_pending_conf_change();
    }

    /// Completes an externally transferred snapshot after host verification.
    pub fn complete_snapshot_install(
        &mut self,
        snapshot: Snapshot<S>,
    ) -> Result<(), SnapshotInstallError> {
        let Some(from) = self.snapshot_install_source else {
            return Err(SnapshotInstallError::NoInstallPending);
        };
        let expected = self
            .snapshot_install_expected
            .clone()
            .ok_or(SnapshotInstallError::NoInstallPending)?;
        let actual = snapshot.metadata();
        if actual != expected {
            return Err(SnapshotInstallError::MetadataMismatch {
                expected: Box::new(expected),
                actual: Box::new(actual),
            });
        }

        let snapshot_index = snapshot.last_included_index;
        self.stage_snapshot(snapshot);
        self.snapshot_install_source = None;
        self.snapshot_install_expected = None;
        self.pending_snapshot_install = None;
        self.accept_install_snapshot(from, snapshot_index);
        Ok(())
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
        self.hard_state.clone()
    }

    pub fn durable_hard_state(&self) -> HardState {
        self.durable_hard_state.clone()
    }

    pub fn durable_log_index(&self) -> LogIndex {
        self.durable_log_index
    }

    pub fn conf_state(&self) -> &ConfState {
        &self.conf_state
    }

    pub fn durable_conf_state(&self) -> Option<&ConfState> {
        self.durable_conf_state.as_ref()
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

    pub fn has_ready(&self) -> bool {
        self.pending_ready.is_some() || self.has_staged_ready()
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

    pub fn limits(&self) -> RaftLimits {
        self.limits
    }

    pub fn set_limits(&mut self, mut limits: RaftLimits) {
        limits.max_proposal_bytes = limits.max_proposal_bytes.max(1);
        limits.max_uncommitted_entries = limits.max_uncommitted_entries.max(1);
        limits.max_uncommitted_bytes = limits.max_uncommitted_bytes.max(1);
        limits.max_append_entries = limits.max_append_entries.max(1);
        limits.max_append_bytes = limits.max_append_bytes.max(1);
        limits.max_inflight_append_batches = limits.max_inflight_append_batches.max(1);
        limits.max_inflight_append_bytes = limits.max_inflight_append_bytes.max(1);
        limits.max_proposal_bytes = limits
            .max_proposal_bytes
            .min(limits.max_append_bytes)
            .min(limits.max_inflight_append_bytes);
        self.limits = limits;
    }

    pub fn uncommitted_bytes(&self) -> usize {
        self.uncommitted_bytes
    }

    pub fn progress(&self, replica_id: NodeId) -> Option<&crate::types::Progress> {
        self.leader_state
            .as_ref()
            .and_then(|leader| leader.progress.get(&replica_id))
    }

    fn admit_proposal(&self, encoded_len: usize) -> Result<(), ProposeError> {
        if encoded_len > self.limits.max_proposal_bytes {
            return Err(ProposeError::ProposalTooLarge {
                encoded_bytes: encoded_len,
                limit: self.limits.max_proposal_bytes,
            });
        }
        let uncommitted_entries = self.last_log_index().saturating_sub(self.commit_index) as usize;
        if uncommitted_entries >= self.limits.max_uncommitted_entries {
            return Err(ProposeError::UncommittedEntriesFull {
                limit: self.limits.max_uncommitted_entries,
            });
        }
        let attempted = self.uncommitted_bytes.saturating_add(encoded_len);
        if attempted > self.limits.max_uncommitted_bytes {
            return Err(ProposeError::UncommittedBytesFull {
                current: self.uncommitted_bytes,
                attempted,
                limit: self.limits.max_uncommitted_bytes,
            });
        }
        Ok(())
    }

    pub(crate) fn refresh_uncommitted_bytes(&mut self) {
        self.uncommitted_bytes = self
            .log
            .entries(self.commit_index.saturating_add(1), usize::MAX)
            .iter()
            .map(|entry| entry.encoded_len)
            .sum();
    }

    pub(crate) fn stage_snapshot(&mut self, snapshot: Snapshot<S>) {
        let snapshot_index = snapshot.last_included_index;
        let snapshot_term = snapshot.last_included_term;

        if self.should_ignore_staged_snapshot(snapshot_index, snapshot_term) {
            return;
        }

        self.log.install_snapshot(snapshot_index, snapshot_term);
        self.install_conf_state(snapshot.conf_state.clone());
        self.commit_to_snapshot(snapshot_index);
        self.pending_entries.clear();
        self.committed.retain(|entry| entry.index > snapshot_index);
        self.pending_snapshot = Some(snapshot);
        self.refresh_pending_conf_change();
    }

    fn has_staged_ready(&self) -> bool {
        self.pending_hard_state.is_some()
            || self.pending_conf_state.is_some()
            || !self.pending_entries.is_empty()
            || self.pending_snapshot.is_some()
            || self.pending_snapshot_install.is_some()
            || !self.outbox.is_empty()
            || !self.committed.is_empty()
            || self.soft_state_changed
    }

    pub(crate) fn install_conf_state(&mut self, conf_state: ConfState) {
        debug_assert!(conf_state.validate().is_ok());
        self.conf_state = conf_state.clone();
        self.peers = conf_state
            .replication_targets()
            .into_iter()
            .filter(|peer| *peer != self.id)
            .collect();
        self.pending_conf_state = Some(conf_state);

        if let Some(leader_state) = self.leader_state.as_mut() {
            leader_state
                .progress
                .retain(|replica_id, _| self.peers.contains(replica_id));
            let next_index = self.log.last_index().saturating_add(1);
            for peer in &self.peers {
                leader_state
                    .progress
                    .entry(*peer)
                    .or_insert_with(|| crate::types::Progress::new(next_index));
            }
        }

        if !self.conf_state.is_voter(self.id) && self.soft_state.role != Role::Follower {
            self.become_follower(self.current_term(), None);
        }
    }

    pub(crate) fn refresh_pending_conf_change(&mut self) {
        self.pending_conf_change_index = self
            .log
            .entries(self.last_applied.saturating_add(1), usize::MAX)
            .into_iter()
            .find(|entry| matches!(entry.payload, EntryPayload::Configuration(_)))
            .map(|entry| entry.index);
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

        let mut hs = self.hard_state.clone();
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

    pub(crate) fn is_snapshot_stale(&self, snapshot_index: LogIndex, snapshot_term: Term) -> bool {
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
