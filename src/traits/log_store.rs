//! trait for replicated log entries

use crate::{
  entry::LogEntry,
  types::{LogIndex, Term}
};

pub trait LogStore<C: Clone> {
  fn first_index(&self) -> LogIndex;
  fn last_index(&self) -> LogIndex;

  fn term(&self, index: LogIndex) -> Option<Term>;
  fn entry(&self, index: LogIndex) -> Option<LogEntry<C>>;
  fn entries(&self, from: LogIndex, max: usize) -> Vec<LogEntry<C>>;

  fn append(&mut self, entries: &[LogEntry<C>]);
  fn truncate_suffix(&mut self, from: LogIndex);
  fn compact(&mut self, through: LogIndex);
}