use std::collections::BTreeSet;

use crate::{message::Envelope, types::NodeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropReason {
    Filtered,
    RandomDrop,
    IsolatedNode(NodeId),
    DownNode(NodeId),
    Partitioned { from: NodeId, to: NodeId },
}

#[derive(Debug, Clone)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        let state = if seed == 0 {
            0x9e37_79b9_7f4a_7c15_u64
        } else {
            seed
        };

        Self { state }
    }

    pub fn seed(&self) -> u64 {
        self.state
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut state = self.state;

        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;

        self.state = state;
        state
    }

    pub fn gen_range_inclusive(&mut self, min: u64, max: u64) -> u64 {
        if min >= max {
            return min;
        }

        min + (self.next_u64() % (max - min + 1))
    }
}

#[derive(Debug, Clone)]
pub struct SimNetwork {
    isolated_nodes: BTreeSet<NodeId>,
    down_nodes: BTreeSet<NodeId>,
    blocked_links: BTreeSet<(NodeId, NodeId)>,
    drop_numerator: u64,
    drop_denominator: u64,
    min_delay_ticks: u64,
    max_delay_ticks: u64,
}

impl Default for SimNetwork {
    fn default() -> Self {
        Self {
            isolated_nodes: BTreeSet::new(),
            down_nodes: BTreeSet::new(),
            blocked_links: BTreeSet::new(),
            drop_numerator: 0,
            drop_denominator: 1,
            min_delay_ticks: 0,
            max_delay_ticks: 0,
        }
    }
}

impl SimNetwork {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn isolate(&mut self, id: NodeId) {
        self.isolated_nodes.insert(id);
    }

    pub fn heal(&mut self, id: NodeId) {
        self.isolated_nodes.remove(&id);
    }

    pub fn heal_all(&mut self) {
        self.isolated_nodes.clear();
        self.blocked_links.clear();
    }

    pub fn is_isolated(&self, id: NodeId) -> bool {
        self.isolated_nodes.contains(&id)
    }

    pub fn mark_down(&mut self, id: NodeId) {
        self.down_nodes.insert(id);
    }

    pub fn mark_up(&mut self, id: NodeId) {
        self.down_nodes.remove(&id);
    }

    pub fn is_down(&self, id: NodeId) -> bool {
        self.down_nodes.contains(&id)
    }

    pub fn partition(&mut self, groups: Vec<Vec<NodeId>>) {
        self.blocked_links.clear();

        let groups = normalize_groups(groups);

        for left in 0..groups.len() {
            for right in (left + 1)..groups.len() {
                for &from in &groups[left] {
                    for &to in &groups[right] {
                        self.block_link(from, to);
                    }
                }
            }
        }
    }

    pub fn clear_partitions(&mut self) {
        self.blocked_links.clear();
    }

    pub fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        if from == to {
            return false;
        }

        self.blocked_links.contains(&normalize_link(from, to))
    }

    pub fn set_drop_rate(&mut self, numerator: u64, denominator: u64) {
        if denominator == 0 {
            self.drop_numerator = 0;
            self.drop_denominator = 1;
            return;
        }

        self.drop_numerator = numerator.min(denominator);
        self.drop_denominator = denominator;
    }

    pub fn clear_drop_rate(&mut self) {
        self.drop_numerator = 0;
        self.drop_denominator = 1;
    }

    pub fn drop_rate(&self) -> (u64, u64) {
        (self.drop_numerator, self.drop_denominator)
    }

    pub fn set_delay_range(&mut self, min_delay_ticks: u64, max_delay_ticks: u64) {
        let (min_delay_ticks, max_delay_ticks) = if min_delay_ticks <= max_delay_ticks {
            (min_delay_ticks, max_delay_ticks)
        } else {
            (max_delay_ticks, min_delay_ticks)
        };

        self.min_delay_ticks = min_delay_ticks;
        self.max_delay_ticks = max_delay_ticks;
    }

    pub fn delay_range(&self) -> (u64, u64) {
        (self.min_delay_ticks, self.max_delay_ticks)
    }

    pub fn sample_delay(&self, rng: &mut SimRng) -> u64 {
        rng.gen_range_inclusive(self.min_delay_ticks, self.max_delay_ticks)
    }

    pub fn should_randomly_drop(&self, rng: &mut SimRng) -> bool {
        if self.drop_numerator == 0 || self.drop_denominator == 0 {
            return false;
        }

        if self.drop_numerator >= self.drop_denominator {
            return true;
        }

        rng.next_u64() % self.drop_denominator < self.drop_numerator
    }

    pub fn deliverability<C, S>(&self, message: &Envelope<C, S>) -> Result<(), DropReason> {
        if self.is_down(message.from) {
            return Err(DropReason::DownNode(message.from));
        }

        if self.is_down(message.to) {
            return Err(DropReason::DownNode(message.to));
        }

        if self.is_isolated(message.from) {
            return Err(DropReason::IsolatedNode(message.from));
        }

        if self.is_isolated(message.to) {
            return Err(DropReason::IsolatedNode(message.to));
        }

        if self.is_partitioned(message.from, message.to) {
            return Err(DropReason::Partitioned {
                from: message.from,
                to: message.to,
            });
        }

        Ok(())
    }

    fn block_link(&mut self, from: NodeId, to: NodeId) {
        if from == to {
            return;
        }

        self.blocked_links.insert(normalize_link(from, to));
    }
}

fn normalize_link(left: NodeId, right: NodeId) -> (NodeId, NodeId) {
    if left < right {
        (left, right)
    } else {
        (right, left)
    }
}

fn normalize_groups(groups: Vec<Vec<NodeId>>) -> Vec<Vec<NodeId>> {
    groups
        .into_iter()
        .map(normalize_node_ids)
        .filter(|group| !group.is_empty())
        .collect()
}

fn normalize_node_ids(mut node_ids: Vec<NodeId>) -> Vec<NodeId> {
    node_ids.sort_unstable();
    node_ids.dedup();
    node_ids
}
