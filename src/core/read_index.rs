use std::collections::HashSet;

use crate::{
    message::{Envelope, Message, ReadIndexRequest, ReadIndexResponse},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LogIndex, NodeId, Role, Term},
};

use super::node::RaftNode;

/// A quorum-confirmed read barrier delivered through `Ready`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadState {
    /// The opaque request context supplied to `RaftNode::read_index`.
    pub request_ctx: Vec<u8>,
    /// The committed index which the host must observe before serving the read.
    pub index: LogIndex,
    /// The leader term which established the quorum confirmation.
    pub term: Term,
}

/// Admission failures for the host-controlled ReadIndex boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadIndexError {
    RecoveryRequired,
    NotLeader,
    ActivationTermMismatch {
        expected: Term,
        actual: Term,
    },
    CurrentTermNotCommitted {
        term: Term,
    },
    CurrentTermNotApplied {
        required: LogIndex,
        applied: LogIndex,
    },
    NotActivated {
        term: Term,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingReadIndex {
    pub(crate) term: Term,
    pub(crate) context: Vec<u8>,
    pub(crate) acknowledgements: HashSet<NodeId>,
}

impl<C, S, LS, SS> RaftNode<C, S, LS, SS>
where
    C: Clone,
    S: Clone,
    LS: LogStore<C>,
    SS: StableStore,
{
    /// Records that the host has applied the current-term activation entry.
    ///
    /// Generic Raft cannot identify an application-specific no-op command, so
    /// the host must call this only after its current-term activation entry has
    /// been committed and applied. The core verifies the term and the
    /// observable committed/applied frontier before allowing reads.
    pub fn activate_read_index(&mut self, term: Term) -> Result<(), ReadIndexError> {
        if self.recovery_required_ready_id.is_some() {
            return Err(ReadIndexError::RecoveryRequired);
        }
        if self.soft_state.role != Role::Leader {
            return Err(ReadIndexError::NotLeader);
        }
        if term != self.current_term() {
            return Err(ReadIndexError::ActivationTermMismatch {
                expected: self.current_term(),
                actual: term,
            });
        }

        let Some(required_index) = self.first_current_term_commit(term) else {
            return Err(ReadIndexError::CurrentTermNotCommitted { term });
        };
        if self.last_applied < required_index {
            return Err(ReadIndexError::CurrentTermNotApplied {
                required: required_index,
                applied: self.last_applied,
            });
        }

        self.read_index_activation_term = Some(term);
        Ok(())
    }

    /// Starts a quorum-confirmed read barrier for an opaque host context.
    ///
    /// Admission is intentionally asynchronous: a leader which cannot reach a
    /// quorum retains the request but emits no `ReadState` until enough
    /// current-term confirmations arrive.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ReadIndexError> {
        if self.recovery_required_ready_id.is_some() {
            return Err(ReadIndexError::RecoveryRequired);
        }
        if self.soft_state.role != Role::Leader {
            return Err(ReadIndexError::NotLeader);
        }
        let term = self.current_term();
        if self.read_index_activation_term != Some(term) {
            return Err(ReadIndexError::NotActivated { term });
        }

        let mut acknowledgements = HashSet::new();
        acknowledgements.insert(self.id);
        self.pending_read_indexes.push(PendingReadIndex {
            term,
            context: context.clone(),
            acknowledgements,
        });

        for peer in self
            .peers
            .iter()
            .copied()
            .filter(|peer| *peer != self.id && self.conf_state.is_voter(*peer))
        {
            self.outbox.push(Envelope {
                from: self.id,
                to: peer,
                msg: Message::ReadIndex(ReadIndexRequest {
                    term,
                    leader_id: self.id,
                    context: context.clone(),
                }),
            });
        }

        self.complete_ready_reads();
        Ok(())
    }

    pub fn read_index_activation_term(&self) -> Option<Term> {
        self.read_index_activation_term
    }

    pub(crate) fn clear_read_index_state(&mut self) {
        self.read_index_activation_term = None;
        self.pending_read_indexes.clear();
    }

    pub(crate) fn handle_read_index_request(&mut self, from: NodeId, request: ReadIndexRequest) {
        if !self.conf_state.is_voter(from) {
            return;
        }

        if request.term < self.current_term() {
            self.outbox.push(Envelope {
                from: self.id,
                to: from,
                msg: Message::ReadIndexResponse(ReadIndexResponse {
                    term: self.current_term(),
                    context: request.context,
                }),
            });
            return;
        }

        self.become_follower(request.term, Some(request.leader_id));
        self.reset_election_timer();
        self.outbox.push(Envelope {
            from: self.id,
            to: from,
            msg: Message::ReadIndexResponse(ReadIndexResponse {
                term: self.current_term(),
                context: request.context,
            }),
        });
    }

    pub(crate) fn handle_read_index_response_from(
        &mut self,
        from: NodeId,
        response: ReadIndexResponse,
    ) {
        if response.term > self.current_term() {
            self.become_follower(response.term, None);
            return;
        }

        if response.term != self.current_term()
            || self.soft_state.role != Role::Leader
            || self.read_index_activation_term != Some(self.current_term())
            || !self.conf_state.is_voter(from)
        {
            return;
        }

        let current_term = self.current_term();
        for pending in &mut self.pending_read_indexes {
            if pending.term == current_term && pending.context == response.context {
                pending.acknowledgements.insert(from);
            }
        }

        self.complete_ready_reads();
    }

    fn complete_ready_reads(&mut self) {
        let mut retained = Vec::with_capacity(self.pending_read_indexes.len());
        let mut completed = Vec::new();
        let conf_state = self.conf_state.clone();
        let committed_index = self.commit_index;

        for pending in self.pending_read_indexes.drain(..) {
            if conf_state.has_quorum(&pending.acknowledgements) {
                completed.push(ReadState {
                    request_ctx: pending.context,
                    index: committed_index,
                    term: pending.term,
                });
            } else {
                retained.push(pending);
            }
        }

        self.pending_read_indexes = retained;
        self.pending_read_states.extend(completed);
    }

    fn first_current_term_commit(&self, term: Term) -> Option<LogIndex> {
        let snapshot_index = self.first_log_index().saturating_sub(1);
        if snapshot_index > 0
            && snapshot_index <= self.commit_index()
            && self.log.term(snapshot_index) == Some(term)
        {
            return Some(snapshot_index);
        }

        self.log
            .entries(self.first_log_index(), usize::MAX)
            .into_iter()
            .find(|entry| entry.index <= self.commit_index() && entry.term == term)
            .map(|entry| entry.index)
    }
}
