use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
    num::ParseIntError,
    str::FromStr,
};

/// Identifies one lifetime of one replica inside one Raft group.
///
/// Replica IDs are consensus identities, not physical server identities. A
/// removed value must never be reused for another replica lifetime.
///
/// ```compile_fail
/// use raft::types::ReplicaId;
///
/// fn accepts_replica_id(_: ReplicaId) {}
///
/// // A raw integer cannot cross the Raft boundary accidentally.
/// accepts_replica_id(7_u64);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ReplicaId(u64);

impl ReplicaId {
    /// Constructs a replica identity.
    ///
    /// Zero is reserved as the absent/invalid value in durable and wire
    /// encodings and therefore cannot become a valid consensus identity.
    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the stable scalar representation used by durable host codecs.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Constructs a replica ID for static declarations and trusted fixtures.
    ///
    /// This function panics for zero, preserving the same invariant as `new`
    /// while remaining usable in constants.
    pub const fn must(value: u64) -> Self {
        match Self::new(value) {
            Some(replica_id) => replica_id,
            None => panic!("replica ID must be non-zero"),
        }
    }
}

impl From<ReplicaId> for u64 {
    fn from(value: ReplicaId) -> Self {
        value.get()
    }
}

impl TryFrom<u64> for ReplicaId {
    type Error = ReplicaIdError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(ReplicaIdError::Zero)
    }
}

impl FromStr for ReplicaId {
    type Err = ReplicaIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value
            .parse::<u64>()
            .map_err(ReplicaIdParseError::InvalidInteger)?;

        Self::new(value).ok_or(ReplicaIdParseError::Zero)
    }
}

impl fmt::Display for ReplicaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdError {
    Zero,
}

impl fmt::Display for ReplicaIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("replica ID must be non-zero"),
        }
    }
}

impl std::error::Error for ReplicaIdError {}

#[derive(Debug)]
pub enum ReplicaIdParseError {
    InvalidInteger(ParseIntError),
    Zero,
}

impl fmt::Display for ReplicaIdParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInteger(error) => write!(formatter, "invalid replica ID: {error}"),
            Self::Zero => formatter.write_str("replica ID must be non-zero"),
        }
    }
}

impl std::error::Error for ReplicaIdParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidInteger(error) => Some(error),
            Self::Zero => None,
        }
    }
}

/// Compatibility name for standalone Raft code written before replica and
/// physical-node identities were separated by the host.
///
/// This alias remains strongly typed because its target is `ReplicaId`.
pub type NodeId = ReplicaId;

pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<ReplicaId>,
    pub commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftState {
    pub role: Role,
    pub leader_id: Option<ReplicaId>,
}

impl Default for SoftState {
    fn default() -> Self {
        Self {
            role: Role::Follower,
            leader_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    pub next_index: LogIndex,
    pub match_index: LogIndex,
    pub mode: ProgressMode,
    pub inflight_batches: usize,
    pub inflight_bytes: usize,
}

impl Progress {
    pub fn new(next_index: LogIndex) -> Self {
        Self {
            next_index,
            match_index: 0,
            mode: ProgressMode::Probe,
            inflight_batches: 0,
            inflight_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressMode {
    Probe,
    Replicate,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LeaderState {
    pub progress: HashMap<ReplicaId, Progress>,
}

/// Durable, versioned authority for election and replication membership.
///
/// `outgoing_voters` is empty for a normal configuration. When populated, a
/// quorum must be obtained from both the current and outgoing voter sets. This
/// reserves the correct durable shape for joint consensus without requiring a
/// database host to infer membership from a mutable configuration file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfState {
    pub version: u64,
    pub voters: BTreeSet<ReplicaId>,
    pub learners: BTreeSet<ReplicaId>,
    pub outgoing_voters: BTreeSet<ReplicaId>,
}

impl ConfState {
    pub fn new(
        version: u64,
        voters: impl IntoIterator<Item = ReplicaId>,
        learners: impl IntoIterator<Item = ReplicaId>,
    ) -> Result<Self, ConfStateError> {
        let state = Self {
            version,
            voters: voters.into_iter().collect(),
            learners: learners.into_iter().collect(),
            outgoing_voters: BTreeSet::new(),
        };
        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<(), ConfStateError> {
        if self.version == 0 {
            return Err(ConfStateError::ZeroVersion);
        }

        if self.voters.is_empty() {
            return Err(ConfStateError::NoVoters);
        }

        if let Some(replica_id) = self
            .learners
            .iter()
            .find(|id| self.voters.contains(id) || self.outgoing_voters.contains(id))
        {
            return Err(ConfStateError::VoterLearnerOverlap(*replica_id));
        }

        Ok(())
    }

    pub fn contains(&self, replica_id: ReplicaId) -> bool {
        self.is_voter(replica_id) || self.learners.contains(&replica_id)
    }

    pub fn is_voter(&self, replica_id: ReplicaId) -> bool {
        self.voters.contains(&replica_id) || self.outgoing_voters.contains(&replica_id)
    }

    pub fn is_learner(&self, replica_id: ReplicaId) -> bool {
        self.learners.contains(&replica_id)
    }

    pub fn replication_targets(&self) -> BTreeSet<ReplicaId> {
        self.voters
            .iter()
            .chain(self.learners.iter())
            .chain(self.outgoing_voters.iter())
            .copied()
            .collect()
    }

    pub fn has_quorum(&self, granted: &HashSet<ReplicaId>) -> bool {
        has_majority(&self.voters, granted)
            && (self.outgoing_voters.is_empty() || has_majority(&self.outgoing_voters, granted))
    }

    pub fn apply(&self, change: &ConfChange) -> Result<Self, ConfChangeError> {
        if change.expected_version != self.version {
            return Err(ConfChangeError::VersionMismatch {
                expected: self.version,
                actual: change.expected_version,
            });
        }

        let mut next = self.clone();
        next.version = self
            .version
            .checked_add(1)
            .ok_or(ConfChangeError::VersionExhausted)?;

        match change.kind {
            ConfChangeKind::AddLearner(replica_id) => {
                if next.contains(replica_id) {
                    return Err(ConfChangeError::ReplicaAlreadyExists(replica_id));
                }

                next.learners.insert(replica_id);
            }
            ConfChangeKind::PromoteLearner(replica_id) => {
                if !next.learners.remove(&replica_id) {
                    return Err(ConfChangeError::NotLearner(replica_id));
                }
                next.voters.insert(replica_id);
            }
            ConfChangeKind::RemoveReplica(replica_id) => {
                let removed = next.voters.remove(&replica_id)
                    | next.learners.remove(&replica_id)
                    | next.outgoing_voters.remove(&replica_id);
                if !removed {
                    return Err(ConfChangeError::UnknownReplica(replica_id));
                }
                if next.voters.is_empty() {
                    return Err(ConfChangeError::WouldRemoveLastVoter);
                }
            }
        }

        next.validate().map_err(ConfChangeError::InvalidState)?;
        Ok(next)
    }
}

fn has_majority(voters: &BTreeSet<ReplicaId>, granted: &HashSet<ReplicaId>) -> bool {
    let votes = voters.iter().filter(|id| granted.contains(id)).count();
    votes > voters.len() / 2
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfStateError {
    ZeroVersion,
    NoVoters,
    VoterLearnerOverlap(ReplicaId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfChange {
    pub expected_version: u64,
    pub kind: ConfChangeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfChangeKind {
    AddLearner(ReplicaId),
    PromoteLearner(ReplicaId),
    RemoveReplica(ReplicaId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfChangeError {
    VersionMismatch { expected: u64, actual: u64 },
    VersionExhausted,
    ReplicaAlreadyExists(ReplicaId),
    NotLearner(ReplicaId),
    UnknownReplica(ReplicaId),
    WouldRemoveLastVoter,
    InvalidState(ConfStateError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot<S> {
    pub snapshot_id: u64,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub conf_state: ConfState,
    pub size_bytes: u64,
    pub checksum: [u8; 32],
    pub data: S,
}

impl<S> Snapshot<S> {
    pub fn new(
        last_included_index: LogIndex,
        last_included_term: Term,
        conf_state: ConfState,
        data: S,
    ) -> Self {
        Self {
            snapshot_id: last_included_index,
            last_included_index,
            last_included_term,
            conf_state,
            size_bytes: std::mem::size_of_val(&data) as u64,
            checksum: [0; 32],
            data,
        }
    }

    pub fn metadata(&self) -> SnapshotMetadata {
        SnapshotMetadata {
            snapshot_id: self.snapshot_id,
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
            conf_state: self.conf_state.clone(),
            size_bytes: self.size_bytes,
            checksum: self.checksum,
        }
    }
}

/// Consensus metadata for an externally streamed snapshot image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMetadata {
    pub snapshot_id: u64,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub conf_state: ConfState,
    pub size_bytes: u64,
    pub checksum: [u8; 32],
}
