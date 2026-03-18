use crate::{traits::{log_store::LogStore, stable_store::StableStore}, types::{HardState, NodeId, Role, Term}};

use super::node::RaftNode;

impl<C, LS, SS> RaftNode<C, LS, SS>
where
  C: Clone,
  LS: LogStore<C>,
  SS: StableStore,
{
  pub fn current_term(&self) -> Term {
    self.stable.hard_state().current_term
  }

  pub fn voted_for(&self) -> Option<NodeId> {
    self.stable.hard_state().voted_for
  }

  pub(crate) fn set_hard_state(&mut self, hs: HardState) {
    self.stable.set_hard_state(hs.clone());
    self.pending_hard_state = Some(hs);
  }

  pub(crate) fn set_current_term(&mut self, term: Term) {
    let mut hs = self.stable.hard_state();

    if term <= hs.current_term {
      return;
    }

    hs.current_term = term;
    hs.voted_for = None;
    self.set_hard_state(hs);
  }

  pub(crate) fn set_voted_for(&mut self, voted_for: Option<NodeId>) {
    let mut hs = self.stable.hard_state();
    hs.voted_for = voted_for;
    self.set_hard_state(hs);
  }

  pub(crate) fn set_role(&mut self, role: Role) {
    if self.soft_state.role != role {
      self.soft_state.role = role;
      self.soft_state_changed = true;
    }
  }

  pub(crate) fn set_leader_id(&mut self, leader_id: Option<NodeId>) {
    if self.soft_state.leader_id != leader_id {
      self.soft_state.leader_id = leader_id;
      self.soft_state_changed = true;
    }
  }

  pub(crate) fn reset_election_timer(&mut self) {
    self.election_elapsed = 0;
  }

  pub(crate) fn reset_heartbeat_timer(&mut self) {
    self.heartbeat_elapsed = 0;
  }

  pub(crate) fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
    if term > self.current_term() {
      self.set_current_term(term);
    }

    self.set_role(Role::Follower);
    self.set_leader_id(leader_id);
    self.leader_state = None;
    self.votes_received.clear();
    self.reset_election_timer();
    self.reset_heartbeat_timer();
  }

}