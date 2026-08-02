use crate::{
    entry::LogEntry,
    message::{
        AppendEntriesRequest, AppendEntriesResponse, Envelope, InstallSnapshotRequest,
        InstallSnapshotResponse, Message,
    },
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LogIndex, NodeId, ProgressMode, Role, Term},
};

use super::node::RaftNode;

impl<C, S, LS, SS> RaftNode<C, S, LS, SS>
where
    C: Clone,
    S: Clone,
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

    pub(crate) fn broadcast_append_entries(&mut self) {
        let peers: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|peer| *peer != self.id)
            .collect();

        for peer in peers {
            self.send_append_entries_to(peer);
        }
    }

    pub(crate) fn send_append_entries_to(&mut self, to: NodeId) {
        if self.soft_state.role != Role::Leader {
            return;
        }

        let Some(progress) = self
            .leader_state
            .as_ref()
            .and_then(|leader| leader.progress.get(&to))
            .cloned()
        else {
            return;
        };
        let next_index = progress.next_index;

        if progress.mode == ProgressMode::Snapshot
            || progress.inflight_batches >= self.limits.max_inflight_append_batches
            || progress.inflight_bytes >= self.limits.max_inflight_append_bytes
            || (progress.mode == ProgressMode::Probe && progress.inflight_batches > 0)
        {
            return;
        }

        if self.maybe_send_snapshot_to(to, next_index) {
            return;
        }

        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.log.term(prev_log_index).unwrap_or(0)
        };

        let entries = self.bounded_entries(next_index);
        let batch_bytes: usize = entries.iter().map(|entry| entry.encoded_len).sum();
        let request = AppendEntriesRequest {
            term: self.current_term(),
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };

        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg: Message::AppendEntries(request),
        });

        if batch_bytes > 0
            && let Some(progress) = self
                .leader_state
                .as_mut()
                .and_then(|leader| leader.progress.get_mut(&to))
        {
            progress.inflight_batches = progress.inflight_batches.saturating_add(1);
            progress.inflight_bytes = progress.inflight_bytes.saturating_add(batch_bytes);
        }
    }

    pub(crate) fn handle_append_entries_request(
        &mut self,
        from: NodeId,
        request: AppendEntriesRequest<C>,
    ) {
        if request.term < self.current_term() {
            self.reject_append_entries(from, None, self.last_log_index().saturating_add(1));
            return;
        }

        self.become_follower(request.term, Some(request.leader_id));
        self.reset_election_timer();

        if let Err((conflict_term, conflict_index)) =
            self.check_prev_log_match(request.prev_log_index, request.prev_log_term)
        {
            self.reject_append_entries(from, conflict_term, conflict_index);
            return;
        }

        self.append_from_leader(&request.entries);
        self.follow_leader_commit(request.leader_commit);

        let matched_index = request.prev_log_index + request.entries.len() as LogIndex;
        self.accept_append_entries(from, matched_index);
    }

    pub(crate) fn handle_append_entries_response_from(
        &mut self,
        from: NodeId,
        response: AppendEntriesResponse,
    ) {
        if response.term > self.current_term() {
            self.become_follower(response.term, None);
            return;
        }

        if response.term < self.current_term() {
            return;
        }

        if self.soft_state.role != Role::Leader {
            return;
        }

        self.mark_leader_peer_active(from);

        let leader_last_index = self.last_log_index();
        let retry_next = self.backtrack_next_index(&response);

        let should_send_more = {
            let Some(leader_state) = self.leader_state.as_mut() else {
                return;
            };

            let Some(progress) = leader_state.progress.get_mut(&from) else {
                return;
            };

            progress.inflight_batches = 0;
            progress.inflight_bytes = 0;

            if response.success {
                let acknowledged = response.match_index.unwrap_or(progress.match_index);
                progress.match_index = progress.match_index.max(acknowledged);
                progress.next_index = progress
                    .next_index
                    .max(progress.match_index.saturating_add(1));
                progress.mode = ProgressMode::Replicate;
                progress.next_index <= leader_last_index
            } else {
                let fallback_next = progress.next_index.saturating_sub(1).max(1);
                let candidate_next = retry_next.unwrap_or(fallback_next).max(1);
                progress.next_index = progress.next_index.min(candidate_next);
                progress.mode = ProgressMode::Probe;
                true
            }
        };

        if response.success {
            self.maybe_advance_commit();
        }

        if should_send_more {
            self.send_append_entries_to(from);
        }
    }

    pub(crate) fn handle_install_snapshot_request(
        &mut self,
        from: NodeId,
        request: InstallSnapshotRequest<S>,
    ) {
        if request.term < self.current_term() {
            self.reject_install_snapshot(from);
            return;
        }

        self.become_follower(request.term, Some(request.leader_id));
        self.reset_election_timer();

        if self.is_snapshot_stale(
            request.metadata.last_included_index,
            request.metadata.last_included_term,
        ) {
            self.reject_install_snapshot(from);
            return;
        }

        // The host now owns image transfer. Raft emits only immutable metadata
        // and waits for `complete_snapshot_install` before acknowledging.
        self.snapshot_install_source = Some(from);
        self.snapshot_install_expected = Some(request.metadata.clone());
        self.pending_snapshot_install = Some(request.metadata);
    }

    pub(crate) fn handle_install_snapshot_response_from(
        &mut self,
        from: NodeId,
        response: InstallSnapshotResponse,
    ) {
        if response.term > self.current_term() {
            self.become_follower(response.term, None);
            return;
        }

        if response.term < self.current_term() {
            return;
        }

        if self.soft_state.role != Role::Leader {
            return;
        }

        self.mark_leader_peer_active(from);

        let should_send_more = {
            let Some(leader_state) = self.leader_state.as_mut() else {
                return;
            };

            let Some(progress) = leader_state.progress.get_mut(&from) else {
                return;
            };

            if !response.success {
                progress.mode = ProgressMode::Probe;
                return;
            }

            progress.mode = ProgressMode::Probe;
            progress.inflight_batches = 0;
            progress.inflight_bytes = 0;
            progress.match_index = progress.match_index.max(response.last_included_index);
            progress.next_index = progress
                .next_index
                .max(response.last_included_index.saturating_add(1));
            progress.next_index <= self.last_log_index()
        };

        self.maybe_advance_commit();

        if should_send_more {
            self.send_append_entries_to(from);
        }
    }

    fn maybe_send_snapshot_to(&mut self, to: NodeId, next_index: LogIndex) -> bool {
        let Some(snapshot) = self.latest_snapshot().cloned() else {
            return false;
        };

        if next_index > snapshot.last_included_index {
            return false;
        }

        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg: Message::InstallSnapshot(InstallSnapshotRequest::new(
                self.current_term(),
                self.id,
                snapshot.metadata(),
            )),
        });

        if let Some(progress) = self
            .leader_state
            .as_mut()
            .and_then(|leader| leader.progress.get_mut(&to))
        {
            progress.mode = ProgressMode::Snapshot;
            progress.inflight_batches = 0;
            progress.inflight_bytes = 0;
        }

        true
    }

    fn bounded_entries(&self, from: LogIndex) -> Vec<LogEntry<C>> {
        let candidates = self.log.entries(from, self.limits.max_append_entries);
        let mut bytes = 0_usize;
        let mut bounded = Vec::with_capacity(candidates.len());

        for entry in candidates {
            let next_bytes = bytes.saturating_add(entry.encoded_len);
            if next_bytes > self.limits.max_append_bytes
                || next_bytes > self.limits.max_inflight_append_bytes
            {
                break;
            }
            bytes = next_bytes;
            bounded.push(entry);
        }
        bounded
    }

    fn accept_append_entries(&mut self, to: NodeId, match_index: LogIndex) {
        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term(),
                success: true,
                match_index: Some(match_index),
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
                match_index: None,
                conflict_term,
                conflict_index: Some(conflict_index),
            }),
        });
    }

    pub(crate) fn accept_install_snapshot(&mut self, to: NodeId, last_included_index: LogIndex) {
        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg: Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: self.current_term(),
                success: true,
                last_included_index,
            }),
        });
    }

    fn reject_install_snapshot(&mut self, to: NodeId) {
        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg: Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: self.current_term(),
                success: false,
                last_included_index: self.first_log_index().saturating_sub(1),
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
            return Err((None, self.last_log_index().saturating_add(1)));
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
            self.refresh_pending_conf_change();
            self.refresh_uncommitted_bytes();
        }
    }

    fn follow_leader_commit(&mut self, leader_commit: LogIndex) {
        if leader_commit <= self.commit_index {
            return;
        }

        self.commit_to(leader_commit);
    }

    fn backtrack_next_index(&self, response: &AppendEntriesResponse) -> Option<LogIndex> {
        if let Some(conflict_term) = response.conflict_term
            && let Some(last_index) = self.last_index_of_term(conflict_term)
        {
            return Some(last_index.saturating_add(1));
        }

        response.conflict_index
    }

    fn last_index_of_term(&self, term: Term) -> Option<LogIndex> {
        let first_visible = self.log.first_index().saturating_sub(1);
        let mut index = self.last_log_index();

        while index > first_visible {
            match self.log.term(index) {
                Some(found_term) if found_term == term => return Some(index),
                _ => index -= 1,
            }
        }

        None
    }
}
