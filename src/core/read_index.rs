use std::collections::HashSet;

use crate::{
    message::{Envelope, Message, ReadIndexRequest, ReadIndexResponse},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LogIndex, NodeId, Role, Term},
};

use super::node::RaftNode;

/// Maximum number of ReadIndex requests which may remain unresolved on one
/// Raft node. A quorum outage must produce bounded backpressure rather than
/// allowing opaque contexts to grow without limit.
pub const MAX_PENDING_READ_INDEXES: usize = 1_024;

/// Maximum size of one opaque ReadIndex context. Contexts are correlation data,
/// not a transport for application payloads, so large values are rejected at
/// the consensus boundary.
pub const MAX_READ_INDEX_CONTEXT_BYTES: usize = 4 * 1_024;

/// Maximum aggregate context bytes retained by unresolved or not-yet-released
/// ReadIndex states on one Raft node.
pub const MAX_PENDING_READ_INDEX_CONTEXT_BYTES: usize = 4 * 1_024 * 1_024;

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
    EmptyContext,
    RequestIdExhausted,
    PendingReadIndexLimitReached {
        limit: usize,
    },
    ReadIndexContextTooLarge {
        max_bytes: usize,
    },
    PendingReadIndexBytesLimitReached {
        limit: usize,
    },
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
    pub(crate) request_id: u64,
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
        if context.is_empty() {
            return Err(ReadIndexError::EmptyContext);
        }
        if context.len() > MAX_READ_INDEX_CONTEXT_BYTES {
            return Err(ReadIndexError::ReadIndexContextTooLarge {
                max_bytes: MAX_READ_INDEX_CONTEXT_BYTES,
            });
        }
        let term = self.current_term();
        if self.read_index_activation_term != Some(term) {
            return Err(ReadIndexError::NotActivated { term });
        }
        if self.pending_read_index_count() >= MAX_PENDING_READ_INDEXES {
            return Err(ReadIndexError::PendingReadIndexLimitReached {
                limit: MAX_PENDING_READ_INDEXES,
            });
        }
        if self
            .pending_read_index_context_bytes()
            .saturating_add(context.len())
            > MAX_PENDING_READ_INDEX_CONTEXT_BYTES
        {
            return Err(ReadIndexError::PendingReadIndexBytesLimitReached {
                limit: MAX_PENDING_READ_INDEX_CONTEXT_BYTES,
            });
        }

        let request_id = self
            .next_read_index_id
            .checked_add(1)
            .map(|next| {
                let current = self.next_read_index_id;
                self.next_read_index_id = next;
                current
            })
            .ok_or(ReadIndexError::RequestIdExhausted)?;
        let mut acknowledgements = HashSet::new();
        acknowledgements.insert(self.id);
        self.pending_read_indexes.push(PendingReadIndex {
            term,
            request_id,
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
                    request_id,
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
        self.pending_read_states.clear();
        if let Some(ready) = self.pending_ready.as_mut() {
            // A Ready can remain outstanding while a higher-term message
            // arrives. ReadStates from the old leader term must not escape
            // that durable boundary after the term change.
            ready.read_states.clear();
        }
    }

    pub(crate) fn handle_read_index_request(&mut self, from: NodeId, request: ReadIndexRequest) {
        if !self.conf_state.is_voter(from)
            || request.request_id == 0
            || request.context.is_empty()
            || request.context.len() > MAX_READ_INDEX_CONTEXT_BYTES
        {
            return;
        }

        if request.term >= self.current_term() {
            self.prevote_phase = false;
            self.leader_recent_active.clear();
        }

        if request.term < self.current_term() {
            self.outbox.push(Envelope {
                from: self.id,
                to: from,
                msg: Message::ReadIndexResponse(ReadIndexResponse {
                    term: self.current_term(),
                    request_id: request.request_id,
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
                request_id: request.request_id,
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
            || response.context.is_empty()
            || response.context.len() > MAX_READ_INDEX_CONTEXT_BYTES
        {
            return;
        }

        // A current-term response proves that this voter can still hear the
        // leader. Count that contact for check-quorum just as append and
        // snapshot responses are counted; otherwise a read-heavy leader could
        // step down despite receiving a quorum-confirmed ReadIndex response.
        self.mark_leader_peer_active(from);

        let current_term = self.current_term();
        for pending in &mut self.pending_read_indexes {
            if pending.term == current_term
                && pending.request_id == response.request_id
                && pending.context == response.context
            {
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

    fn pending_read_index_count(&self) -> usize {
        self.pending_read_indexes
            .len()
            .saturating_add(self.pending_read_states.len())
            .saturating_add(
                self.pending_ready
                    .as_ref()
                    .map_or(0, |ready| ready.read_states.len()),
            )
    }

    fn pending_read_index_context_bytes(&self) -> usize {
        let pending = self
            .pending_read_indexes
            .iter()
            .map(|read| read.context.len())
            .sum::<usize>();
        let released = self
            .pending_read_states
            .iter()
            .map(|read| read.request_ctx.len())
            .sum::<usize>();
        let ready = self.pending_ready.as_ref().map_or(0, |ready| {
            ready
                .read_states
                .iter()
                .map(|read| read.request_ctx.len())
                .sum::<usize>()
        });
        pending.saturating_add(released).saturating_add(ready)
    }
}
