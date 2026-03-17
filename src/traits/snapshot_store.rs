use crate::types::{LogIndex, Term};

pub trait SnapshotStore<S> {
  fn latest(&self) -> Option<&S>;
  fn save(&mut self, snapshot: S);
  fn last_included_index(&self) -> LogIndex;
  fn last_included_term(&self) -> Term;
}