use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    core::node::ProposeError,
    message::Envelope,
    traits::state_machine::SnapshotableStateMachine,
    types::{LogIndex, NodeId, Role},
};

use super::{
    cluster::SimCluster,
    invariants::{InvariantReport, SimInvariantChecker},
    network::{DropReason, SimRng},
    recorder::{SimEvent, SimRecorder},
};

const DEFAULT_SIM_SEED: u64 = 0x5eed_5eed_cafe_f00d_u64;

#[derive(Debug, Clone)]
pub enum SimAction<C, S> {
    AdvanceTime { ticks: u64 },
    TickNode { node_id: NodeId, ticks: u64 },
    DeliverNext,
    DeliverAll { max_steps: usize },
    Propose { node_id: NodeId, command: C },
    Inject { message: Envelope<C, S> },
    Crash { node_id: NodeId },
    Restart { node_id: NodeId },
    Isolate { node_id: NodeId },
    Heal { node_id: NodeId },
    Partition { groups: Vec<Vec<NodeId>> },
    ClearPartitions,
    SetDropRate { numerator: u64, denominator: u64 },
    ClearDropRate,
    SetDelayRange { min_ticks: u64, max_ticks: u64 },
    DuplicateNext,
    HealAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimStepOutcome {
    None,
    DeliveredOne {
        delivered: bool,
    },
    DeliveredAll {
        delivered: usize,
    },
    Proposed {
        result: Result<LogIndex, ProposeError>,
    },
    Duplicated {
        duplicated: bool,
    },
}

#[derive(Debug, Clone)]
pub struct SimTraceEntry<C, S> {
    pub time: u64,
    pub action: SimAction<C, S>,
    pub outcome: SimStepOutcome,
    pub pending_messages: usize,
    pub due_messages: usize,
    pub invariant_report: InvariantReport,
    pub event_range: (usize, usize),
}

#[derive(Debug, Clone)]
struct ScheduledMessage<C, S> {
    deliver_at: u64,
    seq: u64,
    envelope: Envelope<C, S>,
}

impl<C, S> PartialEq for ScheduledMessage<C, S> {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at && self.seq == other.seq
    }
}

impl<C, S> Eq for ScheduledMessage<C, S> {}

impl<C, S> PartialOrd for ScheduledMessage<C, S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<C, S> Ord for ScheduledMessage<C, S> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deliver_at
            .cmp(&self.deliver_at)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

pub struct SimScheduler<C, S, M>
where
    C: Clone,
    S: Clone,
    M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
{
    cluster: SimCluster<C, S, M>,
    rng: SimRng,
    now: u64,
    next_seq: u64,
    queue: BinaryHeap<ScheduledMessage<C, S>>,
    trace: Vec<SimTraceEntry<C, S>>,
    recorder: SimRecorder<C, S>,
    invariants: SimInvariantChecker,
    strict_invariants: bool,
}

impl<C, S, M> SimScheduler<C, S, M>
where
    C: Clone,
    S: Clone,
    M: SnapshotableStateMachine<C, Snapshot = S> + Clone,
{
    pub fn new(cluster: SimCluster<C, S, M>) -> Self {
        Self::with_seed_and_invariants(cluster, DEFAULT_SIM_SEED, true)
    }

    pub fn with_seed(cluster: SimCluster<C, S, M>, seed: u64) -> Self {
        Self::with_seed_and_invariants(cluster, seed, true)
    }

    pub fn with_seed_and_invariants(
        cluster: SimCluster<C, S, M>,
        seed: u64,
        strict_invariants: bool,
    ) -> Self {
        let mut recorder = SimRecorder::new(seed);
        let mut invariants = SimInvariantChecker::new();

        recorder.observe_cluster(&cluster, 0);

        let initial_report = invariants.check_cluster(&cluster);
        if strict_invariants {
            initial_report.assert_valid();
        }

        Self {
            cluster,
            rng: SimRng::new(seed),
            now: 0,
            next_seq: 0,
            queue: BinaryHeap::new(),
            trace: Vec::new(),
            recorder,
            invariants,
            strict_invariants,
        }
    }

    pub fn seed(&self) -> u64 {
        self.recorder.seed()
    }

    pub fn cluster(&self) -> &SimCluster<C, S, M> {
        &self.cluster
    }

    pub fn recorder(&self) -> &SimRecorder<C, S> {
        &self.recorder
    }

    pub fn time(&self) -> u64 {
        self.now
    }

    pub fn pending_messages(&self) -> usize {
        self.queue.len()
    }

    pub fn due_messages(&self) -> usize {
        self.queue
            .iter()
            .filter(|message| message.deliver_at <= self.now)
            .count()
    }

    pub fn has_due_messages(&self) -> bool {
        matches!(self.queue.peek(), Some(message) if message.deliver_at <= self.now)
    }

    pub fn trace(&self) -> &[SimTraceEntry<C, S>] {
        &self.trace
    }

    pub fn last_trace_entry(&self) -> Option<&SimTraceEntry<C, S>> {
        self.trace.last()
    }

    pub fn last_invariant_report(&self) -> Option<&InvariantReport> {
        self.last_trace_entry().map(|entry| &entry.invariant_report)
    }

    pub fn events_for_entry(&self, entry: &SimTraceEntry<C, S>) -> &[SimEvent<C, S>] {
        let (start, end) = entry.event_range;
        let events = self.recorder.events();
        let start = start.min(events.len());
        let end = end.min(events.len());
        &events[start..end]
    }

    pub fn strict_invariants(&self) -> bool {
        self.strict_invariants
    }

    pub fn set_strict_invariants(&mut self, strict_invariants: bool) {
        self.strict_invariants = strict_invariants;
    }

    pub fn clear_trace(&mut self) {
        self.trace.clear();
    }

    pub fn leaders(&self) -> Vec<NodeId> {
        let mut leaders = Vec::new();

        for &node_id in self.cluster.node_ids() {
            if self.cluster.is_crashed(node_id) {
                continue;
            }

            let Some(node) = self.cluster.node(node_id) else {
                continue;
            };

            if node.role() == &Role::Leader {
                leaders.push(node_id);
            }
        }

        leaders
    }

    pub fn leader(&self) -> Option<NodeId> {
        let leaders = self.leaders();

        if leaders.len() == 1 {
            Some(leaders[0])
        } else {
            None
        }
    }

    pub fn schedule_message(&mut self, message: Envelope<C, S>, delay_ticks: u64) {
        let deliver_at = self.now.saturating_add(delay_ticks);
        self.enqueue_scheduled(message, deliver_at);
    }

    pub fn duplicate_next(&mut self) -> bool {
        self.duplicate_next_internal()
    }

    pub fn drop_queued_by<F>(&mut self, mut predicate: F) -> usize
    where
        F: FnMut(&Envelope<C, S>) -> bool,
    {
        let scheduled = std::mem::take(&mut self.queue).into_vec();
        let mut dropped = 0;

        for message in scheduled {
            if predicate(&message.envelope) {
                dropped += 1;
                self.recorder.record_event(SimEvent::MessageDropped {
                    at: self.now,
                    envelope: message.envelope,
                    reason: DropReason::Filtered,
                });
            } else {
                self.queue.push(message);
            }
        }

        if dropped > 0 {
            let _ = self.post_transition_check();
        }

        dropped
    }

    pub fn deliver_next(&mut self) -> bool {
        self.deliver_next_due()
    }

    pub fn deliver_due(&mut self, max_steps: usize) -> usize {
        self.deliver_due_counted(max_steps)
    }

    pub fn step(&mut self, action: SimAction<C, S>) -> SimStepOutcome {
        let start_events = self.recorder.len();

        let outcome = match &action {
            SimAction::AdvanceTime { ticks } => {
                self.now = self.now.saturating_add(*ticks);

                for &node_id in self.cluster.node_ids() {
                    if !self.cluster.is_crashed(node_id) {
                        self.recorder.record_event(SimEvent::NodeTick {
                            at: self.now,
                            node_id,
                            ticks: *ticks,
                        });
                    }
                }

                let messages = self.cluster.tick_all(*ticks);
                self.schedule_outbound_messages(messages);

                SimStepOutcome::None
            }
            SimAction::TickNode { node_id, ticks } => {
                self.now = self.now.saturating_add(*ticks);

                if !self.cluster.is_crashed(*node_id) {
                    self.recorder.record_event(SimEvent::NodeTick {
                        at: self.now,
                        node_id: *node_id,
                        ticks: *ticks,
                    });
                }

                let messages = self.cluster.tick(*node_id, *ticks);
                self.schedule_outbound_messages(messages);

                SimStepOutcome::None
            }
            SimAction::DeliverNext => SimStepOutcome::DeliveredOne {
                delivered: self.deliver_next_due(),
            },
            SimAction::DeliverAll { max_steps } => SimStepOutcome::DeliveredAll {
                delivered: self.deliver_due_counted(*max_steps),
            },
            SimAction::Propose { node_id, command } => {
                let result = match self.cluster.propose(*node_id, command.clone()) {
                    Ok((index, messages)) => {
                        self.schedule_outbound_messages(messages);
                        Ok(index)
                    }
                    Err(err) => Err(err),
                };

                self.recorder.record_event(SimEvent::ProposalResult {
                    at: self.now,
                    node_id: *node_id,
                    result: result.clone(),
                });

                SimStepOutcome::Proposed { result }
            }
            SimAction::Inject { message } => {
                self.schedule_message(message.clone(), 0);
                SimStepOutcome::None
            }
            SimAction::Crash { node_id } => {
                self.cluster.crash(*node_id);
                self.recorder.record_event(SimEvent::NodeCrashed {
                    at: self.now,
                    node_id: *node_id,
                });
                SimStepOutcome::None
            }
            SimAction::Restart { node_id } => {
                let messages = self.cluster.restart(*node_id);
                self.schedule_outbound_messages(messages);
                self.recorder.record_event(SimEvent::NodeRestarted {
                    at: self.now,
                    node_id: *node_id,
                });
                SimStepOutcome::None
            }
            SimAction::Isolate { node_id } => {
                self.cluster.isolate(*node_id);
                self.recorder.record_event(SimEvent::NodeIsolated {
                    at: self.now,
                    node_id: *node_id,
                });
                SimStepOutcome::None
            }
            SimAction::Heal { node_id } => {
                self.cluster.heal(*node_id);
                self.recorder.record_event(SimEvent::NodeHealed {
                    at: self.now,
                    node_id: *node_id,
                });
                SimStepOutcome::None
            }
            SimAction::Partition { groups } => {
                self.cluster.partition(groups.clone());
                self.recorder.record_event(SimEvent::Partitioned {
                    at: self.now,
                    groups: groups.clone(),
                });
                SimStepOutcome::None
            }
            SimAction::ClearPartitions => {
                self.cluster.clear_partitions();
                self.recorder
                    .record_event(SimEvent::PartitionsCleared { at: self.now });
                SimStepOutcome::None
            }
            SimAction::SetDropRate {
                numerator,
                denominator,
            } => {
                self.cluster.set_drop_rate(*numerator, *denominator);
                self.recorder.record_event(SimEvent::DropRateChanged {
                    at: self.now,
                    numerator: *numerator,
                    denominator: *denominator,
                });
                SimStepOutcome::None
            }
            SimAction::ClearDropRate => {
                self.cluster.clear_drop_rate();
                self.recorder.record_event(SimEvent::DropRateChanged {
                    at: self.now,
                    numerator: 0,
                    denominator: 1,
                });
                SimStepOutcome::None
            }
            SimAction::SetDelayRange {
                min_ticks,
                max_ticks,
            } => {
                self.cluster.set_delay_range(*min_ticks, *max_ticks);
                self.recorder.record_event(SimEvent::DelayRangeChanged {
                    at: self.now,
                    min_ticks: *min_ticks,
                    max_ticks: *max_ticks,
                });
                SimStepOutcome::None
            }
            SimAction::DuplicateNext => SimStepOutcome::Duplicated {
                duplicated: self.duplicate_next_internal(),
            },
            SimAction::HealAll => {
                self.cluster.heal_all();
                self.recorder
                    .record_event(SimEvent::PartitionsCleared { at: self.now });
                SimStepOutcome::None
            }
        };

        let invariant_report = self.post_transition_check();
        let end_events = self.recorder.len();

        self.trace.push(SimTraceEntry {
            time: self.now,
            action,
            outcome: outcome.clone(),
            pending_messages: self.pending_messages(),
            due_messages: self.due_messages(),
            invariant_report,
            event_range: (start_events, end_events),
        });

        outcome
    }

    pub fn run<I>(&mut self, actions: I)
    where
        I: IntoIterator<Item = SimAction<C, S>>,
    {
        for action in actions {
            self.step(action);
        }
    }

    pub fn advance_time_and_deliver(&mut self, ticks: u64, max_steps: usize) -> usize {
        self.step(SimAction::AdvanceTime { ticks });

        match self.step(SimAction::DeliverAll { max_steps }) {
            SimStepOutcome::DeliveredAll { delivered } => delivered,
            _ => unreachable!(),
        }
    }

    pub fn wait_for_leader(
        &mut self,
        max_rounds: usize,
        ticks_per_round: u64,
        max_delivery_steps: usize,
    ) -> Option<NodeId> {
        if let Some(leader) = self.leader() {
            return Some(leader);
        }

        for _ in 0..max_rounds {
            self.step(SimAction::AdvanceTime {
                ticks: ticks_per_round,
            });
            self.step(SimAction::DeliverAll {
                max_steps: max_delivery_steps,
            });

            if let Some(leader) = self.leader() {
                return Some(leader);
            }
        }

        None
    }

    fn enqueue_scheduled(&mut self, envelope: Envelope<C, S>, deliver_at: u64) {
        self.recorder.record_event(SimEvent::MessageScheduled {
            at: self.now,
            deliver_at,
            envelope: envelope.clone(),
        });

        self.queue.push(ScheduledMessage {
            deliver_at,
            seq: self.next_seq,
            envelope,
        });
        self.next_seq += 1;
    }

    fn schedule_outbound_messages(&mut self, messages: Vec<Envelope<C, S>>) {
        for message in messages {
            if self.cluster.network().should_randomly_drop(&mut self.rng) {
                self.recorder.record_event(SimEvent::MessageDropped {
                    at: self.now,
                    envelope: message,
                    reason: DropReason::RandomDrop,
                });
                continue;
            }

            let delay = self.cluster.network().sample_delay(&mut self.rng);
            let deliver_at = self.now.saturating_add(delay);
            self.enqueue_scheduled(message, deliver_at);
        }
    }

    fn deliver_next_due(&mut self) -> bool {
        let Some(next) = self.queue.peek() else {
            return false;
        };

        if next.deliver_at > self.now {
            return false;
        }

        let scheduled = self.queue.pop().expect("peek returned Some");
        let envelope = scheduled.envelope;

        match self.cluster.network().deliverability(&envelope) {
            Ok(()) => {
                self.recorder.record_event(SimEvent::MessageDelivered {
                    at: self.now,
                    envelope: envelope.clone(),
                });

                let messages = self.cluster.deliver_message(envelope);
                self.schedule_outbound_messages(messages);

                let _ = self.post_transition_check();
            }
            Err(reason) => {
                self.recorder.record_event(SimEvent::MessageDropped {
                    at: self.now,
                    envelope,
                    reason,
                });
            }
        }

        true
    }

    fn deliver_due_counted(&mut self, max_steps: usize) -> usize {
        let mut delivered = 0;

        for _ in 0..max_steps {
            if !self.deliver_next_due() {
                return delivered;
            }

            delivered += 1;
        }

        if self.has_due_messages() {
            panic!("sim scheduler did not drain due messages within {max_steps} steps");
        }

        delivered
    }

    fn duplicate_next_internal(&mut self) -> bool {
        let Some(next) = self.queue.peek().cloned() else {
            return false;
        };

        self.recorder.record_event(SimEvent::MessageDuplicated {
            at: self.now,
            deliver_at: next.deliver_at,
            envelope: next.envelope.clone(),
        });

        self.queue.push(ScheduledMessage {
            deliver_at: next.deliver_at,
            seq: self.next_seq,
            envelope: next.envelope,
        });
        self.next_seq += 1;

        true
    }

    fn post_transition_check(&mut self) -> InvariantReport {
        self.recorder.observe_cluster(&self.cluster, self.now);

        let report = self.invariants.check_cluster(&self.cluster);

        if self.strict_invariants {
            report.assert_valid();
        }

        report
    }
}
