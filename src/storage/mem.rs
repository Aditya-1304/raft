use crate::{
    entry::LogEntry,
    traits::{log_store::LogStore, snapshot_store::SnapshotStore, stable_store::StableStore},
    types::{HardState, LogIndex, Snapshot, Term},
};

#[derive(Debug, Clone)]
pub struct MemStorage<C, S> {
    hard_state: HardState,
    entries: Vec<LogEntry<C>>,
    snapshot: Option<Snapshot<S>>,
    snapshot_index: LogIndex,
    snapshot_term: Term,
}

impl<C, S> Default for MemStorage<C, S> {
    fn default() -> Self {
        Self {
            hard_state: HardState::default(),
            entries: Vec::new(),
            snapshot: None,
            snapshot_index: 0,
            snapshot_term: 0,
        }
    }
}

impl<C, S> MemStorage<C, S> {
    pub fn new() -> Self {
        Self::default()
    }

    fn set_snapshot_boundary(&mut self, index: LogIndex, term: Term) {
        self.snapshot_index = index;
        self.snapshot_term = term;

        let keep_snapshot = matches!(
            self.snapshot.as_ref(),
            Some(snapshot)
                if snapshot.last_included_index == index && snapshot.last_included_term == term
        );

        if !keep_snapshot {
            self.snapshot = None;
        }
    }

    fn first_log_index(&self) -> LogIndex {
        self.entries
            .first()
            .map(|entry| entry.index)
            .unwrap_or(self.snapshot_index + 1)
    }

    fn last_log_index(&self) -> LogIndex {
        self.entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(self.snapshot_index)
    }

    fn offset(&self, index: LogIndex) -> Option<usize> {
        let first = self.first_log_index();
        let last = self.last_log_index();

        if index < first || index > last {
            None
        } else {
            Some((index - first) as usize)
        }
    }
}

impl<C, S> StableStore for MemStorage<C, S> {
    fn hard_state(&self) -> HardState {
        self.hard_state.clone()
    }

    fn set_hard_state(&mut self, hs: HardState) {
        self.hard_state = hs;
    }
}

impl<C, S> SnapshotStore<S> for MemStorage<C, S> {
    fn latest(&self) -> Option<&Snapshot<S>> {
        self.snapshot.as_ref()
    }

    fn save(&mut self, snapshot: Snapshot<S>) {
        let index = snapshot.last_included_index;
        let term = snapshot.last_included_term;

        self.set_snapshot_boundary(index, term);
        self.snapshot = Some(snapshot);
    }

    fn last_included_index(&self) -> LogIndex {
        self.snapshot_index
    }

    fn last_included_term(&self) -> Term {
        self.snapshot_term
    }
}

impl<C: Clone, S> LogStore<C> for MemStorage<C, S> {
    fn first_index(&self) -> LogIndex {
        self.first_log_index()
    }

    fn last_index(&self) -> LogIndex {
        self.last_log_index()
    }

    fn term(&self, index: LogIndex) -> Option<Term> {
        if self.snapshot_index != 0 && index == self.snapshot_index {
            return Some(self.snapshot_term);
        }

        self.offset(index)
            .and_then(|offset| self.entries.get(offset))
            .map(|entry| entry.term)
    }

    fn entry(&self, index: LogIndex) -> Option<LogEntry<C>> {
        self.offset(index)
            .and_then(|offset| self.entries.get(offset))
            .cloned()
    }

    fn entries(&self, from: LogIndex, max: usize) -> Vec<LogEntry<C>> {
        if max == 0 {
            return Vec::new();
        }

        let start = from.max(self.first_log_index());
        let Some(offset) = self.offset(start) else {
            return Vec::new();
        };

        self.entries
            .iter()
            .skip(offset)
            .take(max)
            .cloned()
            .collect()
    }

    fn append(&mut self, entries: &[LogEntry<C>]) {
        if entries.is_empty() {
            return;
        }

        let first_new_index = entries[0].index;
        let expected_next = self.last_log_index() + 1;

        if first_new_index > expected_next {
            panic!(
                "attempted to append non-contiguous entries: first_new_index={}, expected_next={}",
                first_new_index, expected_next
            );
        }

        self.truncate_suffix(first_new_index);
        self.entries.extend_from_slice(entries);
    }

    fn truncate_suffix(&mut self, from: LogIndex) {
        let first = self.first_log_index();
        let last = self.last_log_index();

        if from > last || from < first {
            return;
        }

        if from == first {
            self.entries.clear();
            return;
        }

        if let Some(offset) = self.offset(from) {
            self.entries.truncate(offset);
        }
    }

    fn compact(&mut self, through: LogIndex) {
        if through <= self.snapshot_index {
            return;
        }

        let Some(term) = self.term(through) else {
            return;
        };

        let remaining = self.entries(through + 1, usize::MAX);
        self.entries = remaining;
        self.set_snapshot_boundary(through, term);
    }

    fn install_snapshot(&mut self, last_included_index: LogIndex, last_included_term: Term) {
        if last_included_index < self.snapshot_index {
            return;
        }

        if last_included_index == self.snapshot_index && last_included_term == self.snapshot_term {
            return;
        }

        let keep_suffix = matches!(
            self.term(last_included_index),
            Some(term) if term == last_included_term
        );

        self.entries = if keep_suffix {
            self.entries(last_included_index + 1, usize::MAX)
        } else {
            Vec::new()
        };

        self.set_snapshot_boundary(last_included_index, last_included_term);
    }
}
