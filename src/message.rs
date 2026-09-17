use std::marker::PhantomData;

use crate::{
    entry::LogEntry,
    types::{LogIndex, NodeId, SnapshotMetadata, Term},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest<C> {
    pub term: Term,
    pub leader_id: NodeId,
    /// Identifies the leader's current per-follower replication window.
    /// Followers echo it in the response so delayed messages from a prior
    /// probe, rewind, or snapshot transition cannot mutate a newer window.
    pub generation: u64,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<C>>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub generation: u64,
    pub success: bool,
    pub match_index: Option<LogIndex>,
    pub conflict_term: Option<Term>,
    pub conflict_index: Option<LogIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotRequest<S> {
    pub term: Term,
    pub leader_id: NodeId,
    pub generation: u64,
    pub metadata: SnapshotMetadata,
    pub marker: PhantomData<fn() -> S>,
}

impl<S> InstallSnapshotRequest<S> {
    pub fn new(term: Term, leader_id: NodeId, metadata: SnapshotMetadata) -> Self {
        Self::new_with_generation(term, leader_id, metadata, 0)
    }

    pub fn new_with_generation(
        term: Term,
        leader_id: NodeId,
        metadata: SnapshotMetadata,
        generation: u64,
    ) -> Self {
        Self {
            term,
            leader_id,
            generation,
            metadata,
            marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub generation: u64,
    pub success: bool,
    pub last_included_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndexRequest {
    pub term: Term,
    pub leader_id: NodeId,
    /// Core-generated nonce that prevents a delayed response from completing
    /// a later request which reused the same opaque host context.
    pub request_id: u64,
    pub context: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndexResponse {
    pub term: Term,
    pub request_id: u64,
    pub context: Vec<u8>,
}

/// Requests a specific voter to begin an immediate election as part of a
/// leadership-transfer attempt.
///
/// The request is deliberately not a log entry. The current leader remains
/// authoritative until the target wins a normal election, while the term and
/// log frontier prevent a delayed request from promoting an obsolete or
/// incomplete replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutNowRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub target_id: NodeId,
    pub transfer_id: u64,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message<C, S> {
    PreVote(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntriesRequest<C>),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshotRequest<S>),
    InstallSnapshotResponse(InstallSnapshotResponse),
    ReadIndex(ReadIndexRequest),
    ReadIndexResponse(ReadIndexResponse),
    TimeoutNow(TimeoutNowRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope<C, S> {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message<C, S>,
}
