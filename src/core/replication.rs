use crate::{
    entry::LogEntry,
    message::{AppendEntriesRequest, AppendEntriesResponse, Envelope, Message},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LogIndex, NodeId, Role, Term},
};

use super::node::RaftNode;

impl<C, LS, SS> RaftNode<C, LS, SS>
where
    C: Clone,
    LS: LogStore<C>,
    SS: StableStore,
{
    pub(crate) fn maybe_send_heartbeats(&mut self) {
        if self.soft_state.role != Role::Leader {
            return;
        }

        if self.heartbeat_elapsed < self.heartbeat_interval {
            return;
        }

        self.broadcast_heartbeats();
        self.reset_heartbeat_timer();
    }

    pub(crate) fn broadcast_heartbeats(&mut self) {
        let request = AppendEntriesRequest {
            term: self.current_term(),
            leader_id: self.id,
            prev_log_index: self.last_log_index(),
            prev_log_term: self.last_log_term(),
            entries: Vec::new(),
            leader_commit: self.commit_index,
        };

        for peer in self.peers.iter().copied().filter(|peer| *peer != self.id) {
            self.outbox.push(Envelope {
                from: self.id,
                to: peer,
                msg: Message::AppendEntries(request.clone()),
            });
        }
    }

    pub(crate) fn handle_append_entries_request(
        &mut self,
        from: NodeId,
        request: AppendEntriesRequest<C>,
    ) {
        if request.term < self.current_term() {
            self.reject_append_entries(from, None, self.last_log_index() + 1);
            return;
        }

        self.become_follower(request.term, Some(request.leader_id));
        self.reset_heartbeat_timer();

        if let Err((conflict_term, conflict_index)) =
            self.check_prev_log_match(request.prev_log_index, request.prev_log_term)
        {
            self.reject_append_entries(from, conflict_term, conflict_index);
            return;
        }

        self.append_from_leader(&request.entries);
        self.follow_leader_commit(request.leader_commit);
        self.accept_append_entries(from);
    }

    pub(crate) fn handle_append_entries_response(&mut self, response: AppendEntriesResponse) {
        if response.term > self.current_term() {
            self.become_follower(response.term, None);
        }
    }

    fn accept_append_entries(&mut self, to: NodeId) {
        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term(),
                success: true,
                conflict_term: None,
                conflict_index: None,
            }),
        });
    }

    fn reject_append_entries(
        &mut self,
        to: NodeId,
        conflict_term: Option<Term>,
        conflict_index: LogIndex,
    ) {
        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term(),
                success: false,
                conflict_term,
                conflict_index: Some(conflict_index),
            }),
        });
    }

    fn check_prev_log_match(
        &self,
        prev_log_index: LogIndex,
        prev_log_term: Term,
    ) -> Result<(), (Option<Term>, LogIndex)> {
        if prev_log_index == 0 {
            return Ok(());
        }

        let Some(local_term) = self.log.term(prev_log_index) else {
            return Err((None, self.last_log_index() + 1));
        };

        if local_term == prev_log_term {
            return Ok(());
        }

        let first_index = self.first_index_of_term(local_term, prev_log_index);
        Err((Some(local_term), first_index))
    }

    fn first_index_of_term(&self, term: Term, mut index: LogIndex) -> LogIndex {
        let first_visible = self.log.first_index().max(1);

        while index > first_visible {
            let prev_index = index - 1;

            match self.log.term(prev_index) {
                Some(prev_term) if prev_term == term => index = prev_index,
                _ => break,
            }
        }

        index
    }

    fn append_from_leader(&mut self, entries: &[LogEntry<C>]) {
        if entries.is_empty() {
            return;
        }

        let mut first_new_offset = None;

        for (offset, incoming) in entries.iter().enumerate() {
            match self.log.term(incoming.index) {
                Some(local_term) if local_term == incoming.term => {}
                Some(_) => {
                    self.log.truncate_suffix(incoming.index);
                    first_new_offset = Some(offset);
                    break;
                }
                None => {
                    first_new_offset = Some(offset);
                    break;
                }
            }
        }

        if let Some(offset) = first_new_offset {
            let suffix = &entries[offset..];
            self.log.append(suffix);
            self.pending_entries.extend(suffix.iter().cloned());
        }
    }

    fn follow_leader_commit(&mut self, leader_commit: LogIndex) {
        let new_commit = leader_commit.min(self.last_log_index());

        if new_commit <= self.commit_index {
            return;
        }

        self.commit_index = new_commit;

        let mut hs = self.stable.hard_state();
        hs.commit = new_commit;
        self.set_hard_state(hs);
    }
}
