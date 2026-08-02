use crate::types::{ConfChange, LogIndex, Term};

/// Distinguishes application commands from quorum configuration changes.
///
/// The host may interpret `Normal` values, but only the Raft core may validate
/// and apply `Configuration` values to its committed `ConfState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryPayload<C> {
    Normal(C),
    Configuration(ConfChange),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry<C> {
    pub index: LogIndex,
    pub term: Term,
    /// Encoded payload bytes used exclusively for deterministic flow control.
    /// The host supplies this value when proposing; decoders derive it from
    /// the actual durable or wire representation.
    pub encoded_len: usize,
    pub payload: EntryPayload<C>,
}

impl<C> LogEntry<C> {
    pub fn normal(index: LogIndex, term: Term, command: C) -> Self {
        let encoded_len = std::mem::size_of_val(&command);
        Self::normal_with_size(index, term, command, encoded_len)
    }

    pub fn normal_with_size(index: LogIndex, term: Term, command: C, encoded_len: usize) -> Self {
        Self {
            index,
            term,
            encoded_len,
            payload: EntryPayload::Normal(command),
        }
    }

    /// Returns the application command carried by a normal entry.
    pub fn command(&self) -> Option<&C> {
        match &self.payload {
            EntryPayload::Normal(command) => Some(command),
            EntryPayload::Configuration(_) => None,
        }
    }
}
