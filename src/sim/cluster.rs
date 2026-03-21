use std::collections::BTreeMap;

use crate::{
    core::{
        node::{ProposeError, RaftNode},
        ready::Ready,
    },
    message::Envelope,
    storage::mem::MemStorage,
    traits::state_machine::SnapshotableStateMachine,
    types::{LogIndex, NodeId, Snapshot},
};

use super::network::SimNetwork;

pub type SimStorage<C, S> = MemStorage<C, S>;
pub type SimRaftNode<C, S> = RaftNode<C, S, SimStorage<C, S>, SimStorage<C, S>>;

pub struct SimNode<C, S, M>
where
    C: Clone,
    S: Clone,
    M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
{
    pub raft: SimRaftNode<C, S>,
    pub state_machine: M,
    pub crashed: bool,

    persisted_log: SimStorage<C, S>,
    persisted_stable: SimStorage<C, S>,
    persisted_snapshot: Option<Snapshot<S>>,
    persisted_state_machine: M,
}

impl<C, S, M> SimNode<C, S, M>
where
    C: Clone,
    S: Clone,
    M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
{
    fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        election_timeout: u64,
        heartbeat_interval: u64,
        state_machine: M,
    ) -> Self {
        let log = MemStorage::new();
        let stable = MemStorage::new();
        let raft = RaftNode::new(
            id,
            peers,
            log.clone(),
            stable.clone(),
            election_timeout,
            heartbeat_interval,
        );

        Self {
            raft,
            state_machine: state_machine.clone(),
            crashed: false,
            persisted_log: log,
            persisted_stable: stable,
            persisted_snapshot: None,
            persisted_state_machine: state_machine,
        }
    }

    fn restart(
        &mut self,
        id: NodeId,
        peers: Vec<NodeId>,
        election_timeout: u64,
        heartbeat_interval: u64,
    ) {
        let mut raft = RaftNode::new(
            id,
            peers,
            self.persisted_log.clone(),
            self.persisted_stable.clone(),
            election_timeout,
            heartbeat_interval,
        );

        if let Some(snapshot) = self.persisted_snapshot.clone() {
            raft.restore_snapshot(snapshot);
        }

        let restored_sm = self.persisted_state_machine.clone();
        raft.advance(restored_sm.last_applied());

        self.raft = raft;
        self.state_machine = restored_sm;
        self.crashed = false;
    }

    fn handle_ready(&mut self, ready: Ready<C, S>) -> Vec<Envelope<C, S>> {
        let Ready {
            snapshot,
            messages,
            committed_entries,
            ..
        } = ready;

        if let Some(snapshot) = snapshot {
            self.state_machine.restore(snapshot.data.clone());
            self.raft.restore_snapshot(snapshot.clone());
            self.persisted_snapshot = Some(snapshot);
        }

        let mut applied_through = None;

        for entry in committed_entries {
            let _ = self.state_machine.apply(entry.index, &entry.command);
            applied_through = Some(entry.index);
        }

        if let Some(index) = applied_through {
            self.raft.advance(index);
        }

        self.persisted_log = self.raft.log.clone();
        self.persisted_stable = self.raft.stable.clone();
        self.persisted_state_machine = self.state_machine.clone();

        if let Some(snapshot) = self.raft.latest_snapshot().cloned() {
            self.persisted_snapshot = Some(snapshot);
        }

        messages
    }

    pub fn persisted_snapshot(&self) -> Option<&Snapshot<S>> {
        self.persisted_snapshot.as_ref()
    }
}

pub struct SimCluster<C, S, M>
where
    C: Clone,
    S: Clone,
    M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
{
    node_ids: Vec<NodeId>,
    nodes: BTreeMap<NodeId, SimNode<C, S, M>>,
    network: SimNetwork,
    election_timeout: u64,
    heartbeat_interval: u64,
}

impl<C, S, M> SimCluster<C, S, M>
where
    C: Clone,
    S: Clone,
    M: SnapshotableStateMachine<C, Snapshot = S> + Clone + Default,
{
    pub fn new(node_ids: Vec<NodeId>, election_timeout: u64, heartbeat_interval: u64) -> Self {
        Self::with_state_machines(node_ids, election_timeout, heartbeat_interval, |_| {
            M::default()
        })
    }
}

impl<C, S, M> SimCluster<C, S, M>
where
    C: Clone,
    S: Clone,
    M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
{
    pub fn with_state_machines<F>(
        node_ids: Vec<NodeId>,
        election_timeout: u64,
        heartbeat_interval: u64,
        mut make_state_machine: F,
    ) -> Self
    where
        F: FnMut(NodeId) -> M,
    {
        let node_ids = normalize_node_ids(node_ids);
        assert!(
            !node_ids.is_empty(),
            "sim cluster requires at least one node"
        );

        let mut nodes = BTreeMap::new();

        for id in &node_ids {
            let peers = node_ids
                .iter()
                .copied()
                .filter(|peer| peer != id)
                .collect::<Vec<_>>();

            nodes.insert(
                *id,
                SimNode::new(
                    *id,
                    peers,
                    election_timeout,
                    heartbeat_interval,
                    make_state_machine(*id),
                ),
            );
        }

        Self {
            node_ids,
            nodes,
            network: SimNetwork::new(),
            election_timeout,
            heartbeat_interval,
        }
    }

    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    pub fn node(&self, id: NodeId) -> Option<&SimRaftNode<C, S>> {
        self.nodes.get(&id).map(|node| &node.raft)
    }

    pub fn state_machine(&self, id: NodeId) -> Option<&M> {
        self.nodes.get(&id).map(|node| &node.state_machine)
    }

    pub fn network(&self) -> &SimNetwork {
        &self.network
    }

    pub(crate) fn _network_mut(&mut self) -> &mut SimNetwork {
        &mut self.network
    }

    pub fn is_crashed(&self, id: NodeId) -> bool {
        self.nodes
            .get(&id)
            .map(|node| node.crashed)
            .unwrap_or(false)
    }

    pub fn is_isolated(&self, id: NodeId) -> bool {
        self.network.is_isolated(id)
    }

    pub fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.network.is_partitioned(from, to)
    }

    pub fn isolate(&mut self, id: NodeId) {
        self.network.isolate(id);
    }

    pub fn heal(&mut self, id: NodeId) {
        self.network.heal(id);
    }

    pub fn heal_all(&mut self) {
        self.network.heal_all();
    }

    pub fn partition(&mut self, groups: Vec<Vec<NodeId>>) {
        self.network.partition(groups);
    }

    pub fn clear_partitions(&mut self) {
        self.network.clear_partitions();
    }

    pub fn set_drop_rate(&mut self, numerator: u64, denominator: u64) {
        self.network.set_drop_rate(numerator, denominator);
    }

    pub fn clear_drop_rate(&mut self) {
        self.network.clear_drop_rate();
    }

    pub fn set_delay_range(&mut self, min_ticks: u64, max_ticks: u64) {
        self.network.set_delay_range(min_ticks, max_ticks);
    }

    pub fn tick(&mut self, id: NodeId, ticks: u64) -> Vec<Envelope<C, S>> {
        let Some(node) = self.nodes.get_mut(&id) else {
            return Vec::new();
        };

        if node.crashed {
            return Vec::new();
        }

        node.raft.tick(ticks);
        self.collect_ready(id)
    }

    pub fn tick_all(&mut self, ticks: u64) -> Vec<Envelope<C, S>> {
        let mut messages = Vec::new();

        for id in self.node_ids.clone() {
            messages.extend(self.tick(id, ticks));
        }

        messages
    }

    pub fn propose(
        &mut self,
        id: NodeId,
        cmd: C,
    ) -> Result<(LogIndex, Vec<Envelope<C, S>>), ProposeError> {
        let index = {
            let Some(node) = self.nodes.get_mut(&id) else {
                return Err(ProposeError::NotLeader);
            };

            if node.crashed {
                return Err(ProposeError::NotLeader);
            }

            node.raft.propose(cmd)?
        };

        Ok((index, self.collect_ready(id)))
    }

    pub fn deliver_message(&mut self, message: Envelope<C, S>) -> Vec<Envelope<C, S>> {
        let target = message.to;

        let delivered = {
            let Some(node) = self.nodes.get_mut(&target) else {
                return Vec::new();
            };

            if node.crashed {
                false
            } else {
                node.raft.step(message);
                true
            }
        };

        if delivered {
            self.collect_ready(target)
        } else {
            Vec::new()
        }
    }

    pub fn crash(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.crashed = true;
            self.network.mark_down(id);
        }
    }

    pub fn restart(&mut self, id: NodeId) -> Vec<Envelope<C, S>> {
        let peers = self
            .node_ids
            .iter()
            .copied()
            .filter(|peer| *peer != id)
            .collect::<Vec<_>>();

        let Some(node) = self.nodes.get_mut(&id) else {
            return Vec::new();
        };

        self.network.mark_up(id);
        node.restart(id, peers, self.election_timeout, self.heartbeat_interval);
        self.collect_ready(id)
    }

    pub fn persisted_snapshot(&self, id: NodeId) -> Option<&Snapshot<S>> {
        self.nodes
            .get(&id)
            .and_then(|node| node.persisted_snapshot())
    }

    fn collect_ready(&mut self, id: NodeId) -> Vec<Envelope<C, S>> {
        let Some(node) = self.nodes.get_mut(&id) else {
            return Vec::new();
        };

        if node.crashed {
            return Vec::new();
        }

        let ready = node.raft.ready();
        node.handle_ready(ready)
    }
}

fn normalize_node_ids(mut node_ids: Vec<NodeId>) -> Vec<NodeId> {
    node_ids.sort_unstable();
    node_ids.dedup();
    node_ids
}
