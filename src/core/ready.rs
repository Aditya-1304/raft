use crate::{entry::LogEntry, message::Envelope, types::HardState};

#[derive(Debug, Clone, Default)]
pub struct Ready<C> {
  pub hard_state: Option<HardState>,
  pub entries_to_persist: Vec<LogEntry<C>>,
  pub messages: Vec<Envelope<C>>,
  pub committed_entries: Vec<LogEntry<C>>,
  pub soft_state_changed: bool,
}

impl<C> Ready<C> {
  pub fn is_empty(&self) -> bool {
    self.hard_state.is_none()
      && self.entries_to_persist.is_empty()
      && self.messages.is_empty()
      && self.committed_entries.is_empty()
      && !self.soft_state_changed
  }
}