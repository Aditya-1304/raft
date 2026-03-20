use crate::types::{LogIndex, Snapshot, Term};

pub trait SnapshotStore<S> {
    fn latest(&self) -> Option<&Snapshot<S>>;
    fn save(&mut self, snapshot: Snapshot<S>);
    fn last_included_index(&self) -> LogIndex {
        self.latest()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0)
    }
    fn last_included_term(&self) -> Term {
        self.latest()
            .map(|snapshot| snapshot.last_included_term)
            .unwrap_or(0)
    }
}
