use crate::{
    entry::LogEntry,
    message::Envelope,
    types::{ConfState, HardState, LogIndex, Snapshot, SnapshotMetadata},
};

/// Identifies one ordered batch of Raft work presented to the host.
///
/// Ready identifiers are local to one `RaftNode` lifetime. The host must
/// acknowledge the exact outstanding identifier after the persistence portion
/// has completed successfully; identifiers cannot be skipped or reordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadyId(u64);

impl ReadyId {
    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
}

/// One stable generation of logical Raft output.
///
/// Calling `RaftNode::ready` repeatedly returns the same generation until the
/// host acknowledges persistence. The host must publish a referenced snapshot,
/// persist entries in increasing index order, persist HardState last, and make
/// that complete prefix durable before calling `advance_persisted`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ready<C, S> {
    pub id: ReadyId,
    /// Logical log tail represented when this generation was frozen.
    pub log_last_index: LogIndex,
    pub hard_state: Option<HardState>,
    pub conf_state: Option<ConfState>,
    pub entries_to_persist: Vec<LogEntry<C>>,
    pub snapshot: Option<Snapshot<S>>,
    /// Metadata for an image the host must fetch, verify, sync, publish, and
    /// pass to `complete_snapshot_install`; snapshot bytes never ride in Raft.
    pub snapshot_install: Option<SnapshotMetadata>,
    pub messages: Vec<Envelope<C, S>>,
    pub committed_entries: Vec<LogEntry<C>>,
    pub soft_state_changed: bool,
}

impl<C, S> Ready<C, S> {
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.conf_state.is_none()
            && self.entries_to_persist.is_empty()
            && self.snapshot.is_none()
            && self.snapshot_install.is_none()
            && self.messages.is_empty()
            && self.committed_entries.is_empty()
            && !self.soft_state_changed
    }

    /// Returns the highest state-machine index represented by this generation.
    pub fn apply_through(&self) -> Option<LogIndex> {
        self.committed_entries
            .last()
            .map(|entry| entry.index)
            .or_else(|| {
                self.snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.last_included_index)
            })
    }
}

/// Rejects host acknowledgements which would make durable and applied state
/// diverge from the ordered Ready stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvanceError {
    /// The durable outcome of the outstanding Ready is unknown. Only a fresh
    /// node reconstructed from durable storage may resume consensus work.
    RecoveryRequired,
    NoReadyPending,
    ReadyMismatch {
        expected: ReadyId,
        actual: ReadyId,
    },
    AppliedIndexRegressed {
        current: LogIndex,
        attempted: LogIndex,
    },
    AppliedBeyondDurableCommit {
        durable_commit: LogIndex,
        attempted: LogIndex,
    },
    SnapshotNotRestored {
        snapshot_index: LogIndex,
        attempted: LogIndex,
    },
}
