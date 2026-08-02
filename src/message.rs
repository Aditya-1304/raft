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
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<C>>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: Option<LogIndex>,
    pub conflict_term: Option<Term>,
    pub conflict_index: Option<LogIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotRequest<S> {
    pub term: Term,
    pub leader_id: NodeId,
    pub metadata: SnapshotMetadata,
    pub marker: PhantomData<fn() -> S>,
}

impl<S> InstallSnapshotRequest<S> {
    pub fn new(term: Term, leader_id: NodeId, metadata: SnapshotMetadata) -> Self {
        Self {
            term,
            leader_id,
            metadata,
            marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub success: bool,
    pub last_included_index: LogIndex,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope<C, S> {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message<C, S>,
}
