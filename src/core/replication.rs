use crate::{
    message::{AppendEntriesRequest, AppendEntriesResponse, Envelope, Message},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::Role,
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
        from: u64,
        request: AppendEntriesRequest<C>,
    ) {
        if request.term < self.current_term() {
            self.outbox.push(Envelope {
                from: self.id,
                to: from,
                msg: Message::AppendEntriesResponse(AppendEntriesResponse {
                    term: self.current_term(),
                    success: false,
                    conflict_term: None,
                    conflict_index: None,
                }),
            });
            return;
        }

        self.become_follower(request.term, Some(request.leader_id));
        self.reset_election_timer();

        self.outbox.push(Envelope {
            from: self.id,
            to: from,
            msg: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term(),
                success: true,
                conflict_term: None,
                conflict_index: None,
            }),
        });
    }

    pub(crate) fn handle_append_entries_response(&mut self, response: AppendEntriesResponse) {
        if response.term > self.current_term() {
            self.become_follower(response.term, None);
        }
    }
}
