use std::collections::BTreeMap;

use crate::{
    core::node::ProposeError,
    message::Envelope,
    traits::state_machine::SnapshotableStateMachine,
    types::{LogIndex, NodeId, Role, Term},
};

use super::{cluster::SimCluster, network::DropReason};

#[derive(Debug, Clone)]
pub enum SimEvent<C, S> {
    NodeTick {
        at: u64,
        node_id: NodeId,
        ticks: u64,
    },
    MessageScheduled {
        at: u64,
        deliver_at: u64,
        envelope: Envelope<C, S>,
    },
    MessageDelivered {
        at: u64,
        envelope: Envelope<C, S>,
    },
    MessageDropped {
        at: u64,
        envelope: Envelope<C, S>,
        reason: DropReason,
    },
    MessageDuplicated {
        at: u64,
        deliver_at: u64,
        envelope: Envelope<C, S>,
    },
    ProposalResult {
        at: u64,
        node_id: NodeId,
        result: Result<LogIndex, ProposeError>,
    },
    NodeCrashed {
        at: u64,
        node_id: NodeId,
    },
    NodeRestarted {
        at: u64,
        node_id: NodeId,
    },
    NodeIsolated {
        at: u64,
        node_id: NodeId,
    },
    NodeHealed {
        at: u64,
        node_id: NodeId,
    },
    Partitioned {
        at: u64,
        groups: Vec<Vec<NodeId>>,
    },
    PartitionsCleared {
        at: u64,
    },
    DropRateChanged {
        at: u64,
        numerator: u64,
        denominator: u64,
    },
    DelayRangeChanged {
        at: u64,
        min_ticks: u64,
        max_ticks: u64,
    },
    RoleChanged {
        at: u64,
        node_id: NodeId,
        from: Role,
        to: Role,
    },
    TermChanged {
        at: u64,
        node_id: NodeId,
        from: Term,
        to: Term,
    },
    CommitAdvanced {
        at: u64,
        node_id: NodeId,
        from: LogIndex,
        to: LogIndex,
    },
}

#[derive(Debug, Clone)]
struct NodeSnapshot {
    crashed: bool,
    role: Role,
    term: Term,
    commit_index: LogIndex,
}

#[derive(Debug, Clone)]
pub struct SimRecorder<C, S> {
    seed: u64,
    events: Vec<SimEvent<C, S>>,
    previous: BTreeMap<NodeId, NodeSnapshot>,
}

impl<C, S> SimRecorder<C, S> {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            events: Vec::new(),
            previous: BTreeMap::new(),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn events(&self) -> &[SimEvent<C, S>] {
        &self.events
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn clear(&mut self) {
        self.events.clear();
    }

    pub fn events_since(&self, start: usize) -> &[SimEvent<C, S>] {
        let start = start.min(self.events.len());
        &self.events[start..]
    }

    pub fn record_event(&mut self, event: SimEvent<C, S>) {
        self.events.push(event);
    }

    pub fn observe_cluster<M>(&mut self, cluster: &SimCluster<C, S, M>, at: u64)
    where
        C: Clone,
        S: Clone,
        M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
    {
        for &node_id in cluster.node_ids() {
            let Some(node) = cluster.node(node_id) else {
                continue;
            };

            let current = NodeSnapshot {
                crashed: cluster.is_crashed(node_id),
                role: *node.role(),
                term: node.current_term(),
                commit_index: node.commit_index(),
            };

            if let Some(previous) = self.previous.get(&node_id) {
                if !previous.crashed && !current.crashed {
                    if previous.role != current.role {
                        self.events.push(SimEvent::RoleChanged {
                            at,
                            node_id,
                            from: previous.role,
                            to: current.role,
                        });
                    }

                    if previous.term != current.term {
                        self.events.push(SimEvent::TermChanged {
                            at,
                            node_id,
                            from: previous.term,
                            to: current.term,
                        });
                    }

                    if current.commit_index > previous.commit_index {
                        self.events.push(SimEvent::CommitAdvanced {
                            at,
                            node_id,
                            from: previous.commit_index,
                            to: current.commit_index,
                        });
                    }
                }
            }

            self.previous.insert(node_id, current);
        }
    }
}
