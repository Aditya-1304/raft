use std::collections::BTreeMap;

use crate::{
    traits::{log_store::LogStore, state_machine::SnapshotableStateMachine},
    types::{LogIndex, NodeId, Role, Term},
};

use super::{cluster::SimCluster, scheduler::SimScheduler};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantViolation {
    MultipleLeadersInSameTerm {
        term: Term,
        leaders: Vec<NodeId>,
    },
    LeaderMissingSelfLeaderId {
        node_id: NodeId,
        leader_id: Option<NodeId>,
    },
    CandidateHasLeaderId {
        node_id: NodeId,
        leader_id: Option<NodeId>,
    },
    CommitPastLogEnd {
        node_id: NodeId,
        commit_index: LogIndex,
        last_log_index: LogIndex,
    },
    HardStateCommitMismatch {
        node_id: NodeId,
        hard_state_commit: LogIndex,
        commit_index: LogIndex,
    },
    AppliedPastCommit {
        node_id: NodeId,
        last_applied: LogIndex,
        commit_index: LogIndex,
    },
    StateMachineApplyMismatch {
        node_id: NodeId,
        state_machine_last_applied: LogIndex,
        raft_last_applied: LogIndex,
    },
    SnapshotPastCommit {
        node_id: NodeId,
        snapshot_index: LogIndex,
        commit_index: LogIndex,
    },
    SnapshotPastApply {
        node_id: NodeId,
        snapshot_index: LogIndex,
        last_applied: LogIndex,
    },
    TermRegressed {
        node_id: NodeId,
        previous_term: Term,
        current_term: Term,
    },
    CommitRegressed {
        node_id: NodeId,
        previous_commit: LogIndex,
        current_commit: LogIndex,
    },
    AppliedRegressed {
        node_id: NodeId,
        previous_last_applied: LogIndex,
        current_last_applied: LogIndex,
    },
    LeaderAppendOnlyViolated {
        node_id: NodeId,
        term: Term,
        index: LogIndex,
        previous_term_at_index: Option<Term>,
        current_term_at_index: Option<Term>,
    },
    LogMatchingViolated {
        left_node: NodeId,
        right_node: NodeId,
        match_index: LogIndex,
        match_term: Term,
        diverging_index: LogIndex,
        left_term_at_diverging_index: Option<Term>,
        right_term_at_diverging_index: Option<Term>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InvariantReport {
    pub violations: Vec<InvariantViolation>,
}

impl InvariantReport {
    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.violations.len()
    }

    pub fn assert_valid(&self) {
        if !self.is_empty() {
            panic!("sim invariants violated:\n{:#?}", self.violations);
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SimInvariantChecker {
    previous: BTreeMap<NodeId, NodeObservation>,
}

impl SimInvariantChecker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn check_cluster<C, S, M>(&mut self, cluster: &SimCluster<C, S, M>) -> InvariantReport
    where
        C: Clone,
        S: Clone,
        M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
    {
        let mut report = InvariantReport::default();
        let mut current = BTreeMap::new();

        for &node_id in cluster.node_ids() {
            let Some(observation) = observe_node(cluster, node_id) else {
                continue;
            };

            self.check_local_invariants(&observation, &mut report);
            self.check_monotonic_invariants(&observation, &mut report);
            self.check_leader_append_only(&observation, &mut report);

            current.insert(node_id, observation);
        }

        let observations = current.values().cloned().collect::<Vec<_>>();
        self.check_election_safety(&observations, &mut report);
        self.check_log_matching(&observations, &mut report);

        self.previous = current;
        report
    }

    pub fn assert_cluster<C, S, M>(&mut self, cluster: &SimCluster<C, S, M>)
    where
        C: Clone,
        S: Clone,
        M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
    {
        self.check_cluster(cluster).assert_valid();
    }

    pub fn check_scheduler<C, S, M>(&mut self, scheduler: &SimScheduler<C, S, M>) -> InvariantReport
    where
        C: Clone,
        S: Clone,
        M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
    {
        self.check_cluster(scheduler.cluster())
    }

    pub fn assert_scheduler<C, S, M>(&mut self, scheduler: &SimScheduler<C, S, M>)
    where
        C: Clone,
        S: Clone,
        M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
    {
        self.check_scheduler(scheduler).assert_valid();
    }

    fn check_local_invariants(&self, current: &NodeObservation, report: &mut InvariantReport) {
        if current.role == Role::Leader && current.leader_id != Some(current.node_id) {
            report
                .violations
                .push(InvariantViolation::LeaderMissingSelfLeaderId {
                    node_id: current.node_id,
                    leader_id: current.leader_id,
                });
        }

        if current.role == Role::Candidate && current.leader_id.is_some() {
            report
                .violations
                .push(InvariantViolation::CandidateHasLeaderId {
                    node_id: current.node_id,
                    leader_id: current.leader_id,
                });
        }

        if current.commit_index > current.last_log_index {
            report
                .violations
                .push(InvariantViolation::CommitPastLogEnd {
                    node_id: current.node_id,
                    commit_index: current.commit_index,
                    last_log_index: current.last_log_index,
                });
        }

        if current.hard_state_commit != current.commit_index {
            report
                .violations
                .push(InvariantViolation::HardStateCommitMismatch {
                    node_id: current.node_id,
                    hard_state_commit: current.hard_state_commit,
                    commit_index: current.commit_index,
                });
        }

        if current.last_applied > current.commit_index {
            report
                .violations
                .push(InvariantViolation::AppliedPastCommit {
                    node_id: current.node_id,
                    last_applied: current.last_applied,
                    commit_index: current.commit_index,
                });
        }

        if current.state_machine_last_applied != current.last_applied {
            report
                .violations
                .push(InvariantViolation::StateMachineApplyMismatch {
                    node_id: current.node_id,
                    state_machine_last_applied: current.state_machine_last_applied,
                    raft_last_applied: current.last_applied,
                });
        }

        if let Some(snapshot_index) = current.latest_snapshot_index {
            if snapshot_index > current.commit_index {
                report
                    .violations
                    .push(InvariantViolation::SnapshotPastCommit {
                        node_id: current.node_id,
                        snapshot_index,
                        commit_index: current.commit_index,
                    });
            }

            if snapshot_index > current.last_applied {
                report
                    .violations
                    .push(InvariantViolation::SnapshotPastApply {
                        node_id: current.node_id,
                        snapshot_index,
                        last_applied: current.last_applied,
                    });
            }
        }
    }

    fn check_monotonic_invariants(&self, current: &NodeObservation, report: &mut InvariantReport) {
        let Some(previous) = self.previous.get(&current.node_id) else {
            return;
        };

        if current.term < previous.term {
            report.violations.push(InvariantViolation::TermRegressed {
                node_id: current.node_id,
                previous_term: previous.term,
                current_term: current.term,
            });
        }

        if current.commit_index < previous.commit_index {
            report.violations.push(InvariantViolation::CommitRegressed {
                node_id: current.node_id,
                previous_commit: previous.commit_index,
                current_commit: current.commit_index,
            });
        }

        if current.last_applied < previous.last_applied {
            report
                .violations
                .push(InvariantViolation::AppliedRegressed {
                    node_id: current.node_id,
                    previous_last_applied: previous.last_applied,
                    current_last_applied: current.last_applied,
                });
        }
    }

    fn check_leader_append_only(&self, current: &NodeObservation, report: &mut InvariantReport) {
        let Some(previous) = self.previous.get(&current.node_id) else {
            return;
        };

        if previous.role != Role::Leader
            || current.role != Role::Leader
            || previous.term != current.term
        {
            return;
        }

        if current.max_known_index() < previous.max_known_index() {
            report
                .violations
                .push(InvariantViolation::LeaderAppendOnlyViolated {
                    node_id: current.node_id,
                    term: current.term,
                    index: previous.max_known_index(),
                    previous_term_at_index: previous.term_at(previous.max_known_index()),
                    current_term_at_index: current.term_at(previous.max_known_index()),
                });
            return;
        }

        let (Some(previous_start), Some(current_start)) =
            (previous.min_known_index(), current.min_known_index())
        else {
            return;
        };

        let overlap_start = previous_start.max(current_start);
        let overlap_end = previous.max_known_index();

        if overlap_start > overlap_end {
            return;
        }

        for index in overlap_start..=overlap_end {
            let previous_term = previous.term_at(index);
            let current_term = current.term_at(index);

            if previous_term != current_term {
                report
                    .violations
                    .push(InvariantViolation::LeaderAppendOnlyViolated {
                        node_id: current.node_id,
                        term: current.term,
                        index,
                        previous_term_at_index: previous_term,
                        current_term_at_index: current_term,
                    });
                return;
            }
        }
    }

    fn check_election_safety(
        &self,
        observations: &[NodeObservation],
        report: &mut InvariantReport,
    ) {
        let mut leaders_by_term: BTreeMap<Term, Vec<NodeId>> = BTreeMap::new();

        for observation in observations {
            if observation.crashed || observation.role != Role::Leader {
                continue;
            }

            leaders_by_term
                .entry(observation.term)
                .or_default()
                .push(observation.node_id);
        }

        for (term, leaders) in leaders_by_term {
            if leaders.len() > 1 {
                report
                    .violations
                    .push(InvariantViolation::MultipleLeadersInSameTerm { term, leaders });
            }
        }
    }

    fn check_log_matching(&self, observations: &[NodeObservation], report: &mut InvariantReport) {
        for left_index in 0..observations.len() {
            for right_index in (left_index + 1)..observations.len() {
                let left = &observations[left_index];
                let right = &observations[right_index];

                let (Some(left_start), Some(right_start)) =
                    (left.min_known_index(), right.min_known_index())
                else {
                    continue;
                };

                let overlap_start = left_start.max(right_start);
                let overlap_end = left.max_known_index().min(right.max_known_index());

                if overlap_start > overlap_end {
                    continue;
                }

                for match_index in overlap_start..=overlap_end {
                    let left_term = left.term_at(match_index);
                    let right_term = right.term_at(match_index);

                    if left_term.is_none() || left_term != right_term {
                        continue;
                    }

                    let match_term = left_term.expect("checked is_some above");

                    for previous_index in overlap_start..match_index {
                        let left_previous = left.term_at(previous_index);
                        let right_previous = right.term_at(previous_index);

                        if left_previous != right_previous {
                            report
                                .violations
                                .push(InvariantViolation::LogMatchingViolated {
                                    left_node: left.node_id,
                                    right_node: right.node_id,
                                    match_index,
                                    match_term,
                                    diverging_index: previous_index,
                                    left_term_at_diverging_index: left_previous,
                                    right_term_at_diverging_index: right_previous,
                                });
                            return;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct NodeObservation {
    node_id: NodeId,
    crashed: bool,
    role: Role,
    leader_id: Option<NodeId>,
    term: Term,
    commit_index: LogIndex,
    hard_state_commit: LogIndex,
    last_applied: LogIndex,
    last_log_index: LogIndex,
    state_machine_last_applied: LogIndex,
    latest_snapshot_index: Option<LogIndex>,
    known_terms: BTreeMap<LogIndex, Term>,
}

impl NodeObservation {
    fn min_known_index(&self) -> Option<LogIndex> {
        self.known_terms.keys().next().copied()
    }

    fn max_known_index(&self) -> LogIndex {
        self.known_terms.keys().next_back().copied().unwrap_or(0)
    }

    fn term_at(&self, index: LogIndex) -> Option<Term> {
        self.known_terms.get(&index).copied()
    }
}

fn observe_node<C, S, M>(cluster: &SimCluster<C, S, M>, node_id: NodeId) -> Option<NodeObservation>
where
    C: Clone,
    S: Clone,
    M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
{
    let raft = cluster.node(node_id)?;
    let state_machine = cluster.state_machine(node_id)?;

    let mut known_terms = BTreeMap::new();
    let first_log_index = raft.first_log_index();
    let last_log_index = raft.last_log_index();
    let snapshot_index = first_log_index.saturating_sub(1);

    if snapshot_index > 0 {
        if let Some(snapshot_term) = raft.log.term(snapshot_index) {
            known_terms.insert(snapshot_index, snapshot_term);
        }
    }

    if first_log_index <= last_log_index {
        for index in first_log_index..=last_log_index {
            if let Some(term) = raft.log.term(index) {
                known_terms.insert(index, term);
            }
        }
    }

    Some(NodeObservation {
        node_id,
        crashed: cluster.is_crashed(node_id),
        role: *raft.role(),
        leader_id: raft.leader_id(),
        term: raft.current_term(),
        commit_index: raft.commit_index(),
        hard_state_commit: raft.hard_state().commit,
        last_applied: raft.last_applied(),
        last_log_index,
        state_machine_last_applied: state_machine.last_applied(),
        latest_snapshot_index: raft
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index),
        known_terms,
    })
}
