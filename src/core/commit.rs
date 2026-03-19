use crate::{
    entry::LogEntry,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LogIndex, Role},
};

use super::node::RaftNode;

impl<C, LS, SS> RaftNode<C, LS, SS>
where
    C: Clone,
    LS: LogStore<C>,
    SS: StableStore,
{
    pub(crate) fn maybe_advance_commit(&mut self) {
        if self.soft_state.role != Role::Leader {
            return;
        }

        let Some(leader_state) = self.leader_state.as_ref() else {
            return;
        };

        let mut matched = Vec::with_capacity(leader_state.progress.len() + 1);
        matched.push(self.last_log_index());
        matched.extend(
            leader_state
                .progress
                .values()
                .map(|progress| progress.match_index),
        );
        matched.sort_unstable();

        let candidate_commit = matched[matched.len() / 2];

        if candidate_commit <= self.commit_index {
            return;
        }

        let Some(candidate_term) = self.log.term(candidate_commit) else {
            return;
        };

        if candidate_term != self.current_term() {
            return;
        }

        self.commit_to(candidate_commit);
    }

    pub(crate) fn commit_to(&mut self, new_commit: LogIndex) {
        let new_commit = new_commit.min(self.last_log_index());

        if new_commit <= self.commit_index {
            return;
        }

        let start = self.commit_index + 1;
        let count = (new_commit - start + 1) as usize;
        let newly_committed: Vec<LogEntry<C>> = self.log.entries(start, count);

        self.commit_index = new_commit;

        let mut hs = self.stable.hard_state();
        hs.commit = new_commit;
        self.set_hard_state(hs);

        self.committed.extend(newly_committed);
    }
}
