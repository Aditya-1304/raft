use crate::{
    entry::LogEntry,
    message::Envelope,
    types::{HardState, Snapshot},
};

#[derive(Debug, Clone, Default)]
pub struct Ready<C, S> {
    pub hard_state: Option<HardState>,
    pub entries_to_persist: Vec<LogEntry<C>>,
    pub snapshot: Option<Snapshot<S>>,
    pub messages: Vec<Envelope<C, S>>,
    pub committed_entries: Vec<LogEntry<C>>,
    pub soft_state_changed: bool,
}

impl<C, S> Ready<C, S> {
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.entries_to_persist.is_empty()
            && self.snapshot.is_none()
            && self.messages.is_empty()
            && self.committed_entries.is_empty()
            && !self.soft_state_changed
    }
}
