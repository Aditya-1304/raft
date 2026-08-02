use std::collections::HashMap;

use crate::{
    message::{
        Envelope, Message, PreVoteRequest, PreVoteResponse, RequestVoteRequest, RequestVoteResponse,
    },
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LeaderState, NodeId, Progress, Role},
};

use super::node::{RaftError, RaftNode, StepError};

impl<C, S, LS, SS> RaftNode<C, S, LS, SS>
where
    C: Clone,
    S: Clone,
    LS: LogStore<C>,
    SS: StableStore,
{
    pub fn tick(&mut self, ticks: u64) {
        let _ = self.tick_checked(ticks);
    }

    pub fn tick_checked(&mut self, ticks: u64) -> Result<(), RaftError> {
        match self.soft_state.role {
            Role::Leader => {
                self.heartbeat_elapsed = self.heartbeat_elapsed.saturating_add(ticks);
                self.election_elapsed = self.election_elapsed.saturating_add(ticks);

                if self.maybe_step_down_on_quorum_loss() {
                    return Ok(());
                }

                self.maybe_send_heartbeats();
            }
            Role::Follower | Role::Candidate => {
                if !self.conf_state.is_voter(self.id) {
                    return Ok(());
                }
                self.election_elapsed = self.election_elapsed.saturating_add(ticks);

                if self.election_elapsed >= self.randomized_election_timeout {
                    if self.current_term() == u64::MAX {
                        return Err(RaftError::TermExhausted);
                    }
                    self.start_prevote();
                }
            }
        }
        Ok(())
    }

    pub fn step(&mut self, envelope: Envelope<C, S>) {
        let _ = self.step_checked(envelope);
    }

    pub fn step_checked(&mut self, envelope: Envelope<C, S>) -> Result<(), StepError> {
        if envelope.to != self.id {
            return Err(StepError::WrongDestination {
                expected: self.id,
                actual: envelope.to,
            });
        }
        if !self.conf_state.contains(envelope.from) {
            return Err(StepError::UnknownReplica(envelope.from));
        }

        match &envelope.msg {
            Message::PreVote(request) if request.candidate_id != envelope.from => {
                return Err(StepError::PayloadIdentityMismatch {
                    envelope: envelope.from,
                    payload: request.candidate_id,
                });
            }
            Message::RequestVote(request) if request.candidate_id != envelope.from => {
                return Err(StepError::PayloadIdentityMismatch {
                    envelope: envelope.from,
                    payload: request.candidate_id,
                });
            }
            Message::AppendEntries(request) => {
                if request.leader_id != envelope.from {
                    return Err(StepError::PayloadIdentityMismatch {
                        envelope: envelope.from,
                        payload: request.leader_id,
                    });
                }
                let entry_count =
                    u64::try_from(request.entries.len()).map_err(|_| StepError::IndexOverflow)?;
                request
                    .prev_log_index
                    .checked_add(entry_count)
                    .ok_or(StepError::IndexOverflow)?;
            }
            Message::InstallSnapshot(request) => {
                if request.leader_id != envelope.from {
                    return Err(StepError::PayloadIdentityMismatch {
                        envelope: envelope.from,
                        payload: request.leader_id,
                    });
                }
                request
                    .metadata
                    .conf_state
                    .validate()
                    .map_err(StepError::InvalidSnapshotConfiguration)?;
            }
            _ => {}
        }

        let from = envelope.from;

        match envelope.msg {
            Message::PreVote(request) => {
                self.handle_prevote_request(from, request);
            }
            Message::PreVoteResponse(response) => {
                self.handle_prevote_response(from, response);
            }
            Message::RequestVote(request) => {
                self.handle_request_vote_request(from, request);
            }
            Message::RequestVoteResponse(response) => {
                self.handle_request_vote_response(from, response)
            }
            Message::AppendEntries(request) => {
                if request.term >= self.current_term() {
                    self.prevote_phase = false;
                    self.leader_recent_active.clear();
                }
                self.handle_append_entries_request(from, request);
            }
            Message::AppendEntriesResponse(response) => {
                self.handle_append_entries_response_from(from, response);
            }
            Message::InstallSnapshot(request) => {
                if request.term >= self.current_term() {
                    self.prevote_phase = false;
                    self.leader_recent_active.clear();
                }
                self.handle_install_snapshot_request(from, request);
            }
            Message::InstallSnapshotResponse(response) => {
                self.handle_install_snapshot_response_from(from, response);
            }
        }
        Ok(())
    }

    fn start_prevote(&mut self) {
        if !self.conf_state.is_voter(self.id) {
            return;
        }
        self.set_role(Role::Candidate);
        self.set_leader_id(None);
        self.rearm_election_timer();
        self.prevote_phase = true;
        self.leader_recent_active.clear();
        self.votes_received.clear();
        self.votes_received.insert(self.id);

        if self.conf_state.has_quorum(&self.votes_received) {
            self.start_election();
            return;
        }

        let request = PreVoteRequest {
            term: self.current_term().saturating_add(1),
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        for peer in self.peers.iter().copied().filter(|peer| *peer != self.id) {
            self.outbox.push(Envelope {
                from: self.id,
                to: peer,
                msg: Message::PreVote(request.clone()),
            });
        }
    }

    fn start_election(&mut self) {
        let next_term = self.current_term().saturating_add(1);

        self.prevote_phase = false;
        self.leader_recent_active.clear();
        self.set_current_term(next_term);
        self.set_role(Role::Candidate);
        self.set_leader_id(None);
        self.rearm_election_timer();
        self.votes_received.clear();

        self.set_voted_for(Some(self.id));
        self.votes_received.insert(self.id);

        if self.conf_state.has_quorum(&self.votes_received) {
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

    fn handle_prevote_request(&mut self, from: NodeId, request: PreVoteRequest) {
        let vote_granted = self.conf_state.is_voter(self.id)
            && self.conf_state.is_voter(from)
            && request.term >= self.current_term()
            && self.is_log_up_to_date(request.last_log_index, request.last_log_term);

        self.outbox.push(Envelope {
            from: self.id,
            to: from,
            msg: Message::PreVoteResponse(PreVoteResponse {
                term: self.current_term(),
                vote_granted,
            }),
        });
    }

    fn handle_prevote_response(&mut self, from: NodeId, response: PreVoteResponse) {
        if response.term > self.current_term() {
            self.prevote_phase = false;
            self.leader_recent_active.clear();
            self.become_follower(response.term, None);
            return;
        }

        if !self.prevote_phase || !self.conf_state.is_voter(from) {
            return;
        }

        if !response.vote_granted {
            return;
        }

        self.votes_received.insert(from);

        if self.conf_state.has_quorum(&self.votes_received) {
            self.start_election();
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
            self.prevote_phase = false;
            self.leader_recent_active.clear();
            self.become_follower(request.term, None);
        }

        let can_vote = self.conf_state.is_voter(self.id)
            && self.conf_state.is_voter(from)
            && (self.voted_for().is_none() || self.voted_for() == Some(request.candidate_id));
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
            self.prevote_phase = false;
            self.leader_recent_active.clear();
            self.become_follower(response.term, None);
            return;
        }

        if self.prevote_phase
            || self.soft_state.role != Role::Candidate
            || !self.conf_state.is_voter(from)
        {
            return;
        }

        if response.term < self.current_term() || !response.vote_granted {
            return;
        }

        self.votes_received.insert(from);

        if self.conf_state.has_quorum(&self.votes_received) {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        let next_index = self.last_log_index().saturating_add(1);
        let mut progress = HashMap::with_capacity(self.peers.len());

        for peer in self.peers.iter().copied().filter(|peer| *peer != self.id) {
            progress.insert(peer, Progress::new(next_index));
        }

        self.prevote_phase = false;
        self.leader_recent_active.clear();
        self.set_role(Role::Leader);
        self.set_leader_id(Some(self.id));
        self.leader_state = Some(LeaderState { progress });
        self.votes_received.clear();
        self.reset_election_timer();
        self.reset_heartbeat_timer();
        self.broadcast_heartbeats();
    }

    pub(crate) fn mark_leader_peer_active(&mut self, from: NodeId) {
        if self.soft_state.role != Role::Leader {
            return;
        }

        if from != self.id && self.conf_state.is_voter(from) {
            self.leader_recent_active.insert(from);
        }
    }

    fn maybe_step_down_on_quorum_loss(&mut self) -> bool {
        if self.soft_state.role != Role::Leader {
            return false;
        }

        if self.election_elapsed < self.election_timeout {
            return false;
        }

        let has_quorum = self.has_check_quorum();
        self.leader_recent_active.clear();
        self.reset_election_timer();

        if has_quorum {
            return false;
        }

        self.prevote_phase = false;
        self.become_follower(self.current_term(), None);
        true
    }

    fn has_check_quorum(&self) -> bool {
        let mut active = self.leader_recent_active.clone();
        active.insert(self.id);
        self.conf_state.has_quorum(&active)
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
