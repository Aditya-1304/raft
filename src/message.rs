use crate::{entry::LogEntry, types::{LogIndex, NodeId, Term}};

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
  pub leader_commit: LogIndex
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
  pub term: Term,
  pub success: bool,
  pub conflict_term: Option<Term>,
  pub conflict_index: Option<LogIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message<C> {
  RequestVote(RequestVoteRequest),
  RequestVoteResponse(RequestVoteResponse),
  AppendEntries(AppendEntriesRequest<C>),
  AppendEntriesResponse(AppendEntriesResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope<C> {
  pub from: NodeId,
  pub to: NodeId,
  pub msg: Message<C>,
}