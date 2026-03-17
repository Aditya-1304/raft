use crate::types::{LogIndex, Term};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry<C> {
  pub index: LogIndex,
  pub term: Term,
  pub command: C
}