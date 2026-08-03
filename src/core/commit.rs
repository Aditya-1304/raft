use std::collections::HashSet;

use crate::{
    entry::{EntryPayload, LogEntry},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LogIndex, Role},
};

use super::node::RaftNode;

impl<C, S, LS, SS> RaftNode<C, S, LS, SS>
where
    C: Clone,
    S: Clone,
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

        for candidate_commit in (self.commit_index.saturating_add(1)..=self.last_log_index()).rev()
        {
            if self.log.term(candidate_commit) != Some(self.current_term()) {
                continue;
            }

            let mut replicated = HashSet::new();
            replicated.insert(self.id);
            replicated.extend(
                leader_state
                    .progress
                    .iter()
                    .filter(|(_, progress)| progress.match_index >= candidate_commit)
                    .map(|(replica_id, _)| *replica_id),
            );

            if self.conf_state.has_quorum(&replicated) {
                self.commit_to(candidate_commit);
                return;
            }
        }
    }

    pub(crate) fn commit_to(&mut self, new_commit: LogIndex) {
        let new_commit = new_commit.min(self.last_log_index());

        if new_commit <= self.commit_index {
            return;
        }

        let start = self.commit_index.saturating_add(1);
        let Ok(count) = usize::try_from(new_commit - start + 1) else {
            return;
        };
        let newly_committed: Vec<LogEntry<C>> = self.log.entries(start, count);

        self.commit_index = new_commit;
        self.refresh_uncommitted_bytes();

        let mut hs = self.hard_state.clone();
        hs.commit = new_commit;
        self.set_hard_state(hs);

        for entry in &newly_committed {
            if let EntryPayload::Configuration(change) = &entry.payload {
                // Configuration entries are validated before proposal, before
                // follower append, and while reconstructing a durable suffix.
                // Reaching this point with an invalid transition is therefore
                // an internal safety invariant violation, never a condition to
                // ignore while advancing the committed quorum state.
                let next = self
                    .conf_state
                    .apply(change)
                    .expect("committed configuration entry was not prevalidated");
                self.install_conf_state(next);
            }
        }

        self.committed.extend(newly_committed);
    }
}
