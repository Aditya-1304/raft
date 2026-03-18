use std::collections::HashMap;

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
  Leader,
  Follower,
  Candidate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
  pub current_term: Term,
  pub voted_for: Option<NodeId>,
  pub commit: LogIndex,
}

impl Default for HardState {
  fn default() -> Self {
    Self { 
      current_term: 0, 
      voted_for: None, 
      commit: 0 
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftState {
  pub role: Role,
  pub leader_id: Option<NodeId>,
}

impl Default for SoftState {
  fn default() -> Self {
    Self { 
      role: Role::Follower, 
      leader_id: 
      None 
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
  pub next_index: LogIndex,
  pub match_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LeaderState {
  pub progress: HashMap<NodeId, Progress>
}