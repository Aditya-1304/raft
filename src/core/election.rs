use std::collections::HashMap;

use crate::{
    message::{Envelope, Message, RequestVoteRequest, RequestVoteResponse},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LeaderState, NodeId, Progress, Role},
};

use super::node::RaftNode;

impl<C, S, LS, SS> RaftNode<C, S, LS, SS>
where
    C: Clone,
    S: Clone,
    LS: LogStore<C>,
    SS: StableStore,
{
    pub fn tick(&mut self, ticks: u64) {
        match self.soft_state.role {
            Role::Leader => {
                self.heartbeat_elapsed = self.heartbeat_elapsed.saturating_add(ticks);
                self.maybe_send_heartbeats();
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed = self.election_elapsed.saturating_add(ticks);

                if self.election_elapsed >= self.randomized_election_timeout {
                    self.start_election();
                }
            }
        }
    }

    pub fn step(&mut self, envelope: Envelope<C, S>) {
        if envelope.to != self.id {
            return;
        }

        let from = envelope.from;

        match envelope.msg {
            Message::RequestVote(request) => self.handle_request_vote_request(from, request),
            Message::RequestVoteResponse(response) => {
                self.handle_request_vote_response(from, response)
            }
            Message::AppendEntries(request) => {
                self.handle_append_entries_request(from, request);
            }
            Message::AppendEntriesResponse(response) => {
                self.handle_append_entries_response_from(from, response);
            }
            Message::InstallSnapshot(request) => {
                self.handle_install_snapshot_request(from, request);
            }
            Message::InstallSnapshotResponse(response) => {
                self.handle_install_snapshot_response_from(from, response);
            }
        }
    }

    fn start_election(&mut self) {
        let next_term = self.current_term() + 1;

        self.set_current_term(next_term);
        self.set_role(Role::Candidate);
        self.set_leader_id(None);
        self.rearm_election_timer();
        self.votes_received.clear();

        self.set_voted_for(Some(self.id));
        self.votes_received.insert(self.id);

        if self.votes_received.len() >= self.quorum_size() {
            self.become_leader();
            return;
        }

        let request = RequestVoteRequest {
            term: self.current_term(),
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        for peer in self.peers.iter().copied().filter(|peer| *peer != self.id) {
            self.outbox.push(Envelope {
                from: self.id,
                to: peer,
                msg: Message::RequestVote(request.clone()),
            });
        }
    }

    fn handle_request_vote_request(&mut self, from: NodeId, request: RequestVoteRequest) {
        if request.term < self.current_term() {
            self.outbox.push(Envelope {
                from: self.id,
                to: from,
                msg: Message::RequestVoteResponse(RequestVoteResponse {
                    term: self.current_term(),
                    vote_granted: false,
                }),
            });
            return;
        }

        if request.term > self.current_term() {
            self.become_follower(request.term, None);
        }

        let can_vote = self.voted_for().is_none() || self.voted_for() == Some(request.candidate_id);
        let log_ok = self.is_log_up_to_date(request.last_log_index, request.last_log_term);
        let vote_granted = can_vote && log_ok;

        if vote_granted {
            self.set_voted_for(Some(request.candidate_id));
            self.rearm_election_timer();
        }

        self.outbox.push(Envelope {
            from: self.id,
            to: from,
            msg: Message::RequestVoteResponse(RequestVoteResponse {
                term: self.current_term(),
                vote_granted,
            }),
        });
    }

    fn handle_request_vote_response(&mut self, from: NodeId, response: RequestVoteResponse) {
        if response.term > self.current_term() {
            self.become_follower(response.term, None);
            return;
        }

        if self.soft_state.role != Role::Candidate {
            return;
        }

        if response.term < self.current_term() || !response.vote_granted {
            return;
        }

        self.votes_received.insert(from);

        if self.votes_received.len() >= self.quorum_size() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        let next_index = self.last_log_index() + 1;
        let mut progress = HashMap::with_capacity(self.peers.len());

        for peer in self.peers.iter().copied().filter(|peer| *peer != self.id) {
            progress.insert(
                peer,
                Progress {
                    next_index,
                    match_index: 0,
                },
            );
        }

        self.set_role(Role::Leader);
        self.set_leader_id(Some(self.id));
        self.leader_state = Some(LeaderState { progress });
        self.votes_received.clear();
        self.reset_election_timer();
        self.reset_heartbeat_timer();
        self.broadcast_heartbeats();
    }

    fn quorum_size(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        (cluster_size / 2) + 1
    }

    fn is_log_up_to_date(&self, candidate_last_index: u64, candidate_last_term: u64) -> bool {
        let local_last_term = self.last_log_term();
        let local_last_index = self.last_log_index();

        if candidate_last_term != local_last_term {
            candidate_last_term > local_last_term
        } else {
            candidate_last_index >= local_last_index
        }
    }
}
