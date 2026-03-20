use std::{
    collections::VecDeque,
    io,
    sync::mpsc::{Receiver, TryRecvError},
    thread,
};

use crate::{
    core::{node::RaftNode, ready::Ready},
    message::Envelope,
    traits::{
        log_store::LogStore, snapshot_store::SnapshotStore, stable_store::StableStore,
        state_machine::SnapshotableStateMachine, ticker::Ticker, transport::Transport,
    },
    types::{LogIndex, Snapshot, Term},
};

use super::{RuntimeConfig, RuntimeError, RuntimeResult, RuntimeStats};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPolicy {
    Never,
    EveryAppliedEntries(u64),
}

impl SnapshotPolicy {
    fn threshold(self) -> Option<u64> {
        match self {
            SnapshotPolicy::Never => None,
            SnapshotPolicy::EveryAppliedEntries(0) => None,
            SnapshotPolicy::EveryAppliedEntries(value) => Some(value),
        }
    }
}

pub struct RuntimeServer<C, S, LS, SS, Snap, SM, Tp, Tk>
where
    C: Clone,
    S: Clone,
    LS: LogStore<C>,
    SS: StableStore,
    Snap: SnapshotStore<S>,
    SM: SnapshotableStateMachine<C, Snapshot = S>,
    Tp: Transport<C, S>,
    Tk: Ticker,
{
    raft: RaftNode<C, S, LS, SS>,
    snapshot_store: Snap,
    state_machine: SM,
    transport: Tp,
    ticker: Tk,
    inbound: Receiver<Envelope<C, S>>,
    config: RuntimeConfig,
    stats: RuntimeStats,
    snapshot_policy: SnapshotPolicy,
    applied_outputs: VecDeque<(LogIndex, SM::Output)>,
}

impl<C, S, LS, SS, Snap, SM, Tp, Tk> RuntimeServer<C, S, LS, SS, Snap, SM, Tp, Tk>
where
    C: Clone,
    S: Clone,
    LS: LogStore<C>,
    SS: StableStore,
    Snap: SnapshotStore<S>,
    SM: SnapshotableStateMachine<C, Snapshot = S>,
    Tp: Transport<C, S>,
    Tk: Ticker,
{
    pub fn new(
        raft: RaftNode<C, S, LS, SS>,
        snapshot_store: Snap,
        state_machine: SM,
        transport: Tp,
        ticker: Tk,
        inbound: Receiver<Envelope<C, S>>,
        config: RuntimeConfig,
    ) -> RuntimeResult<Self> {
        Self::with_snapshot_policy(
            raft,
            snapshot_store,
            state_machine,
            transport,
            ticker,
            inbound,
            config,
            SnapshotPolicy::Never,
        )
    }

    pub fn with_snapshot_policy(
        raft: RaftNode<C, S, LS, SS>,
        snapshot_store: Snap,
        state_machine: SM,
        transport: Tp,
        ticker: Tk,
        inbound: Receiver<Envelope<C, S>>,
        config: RuntimeConfig,
        snapshot_policy: SnapshotPolicy,
    ) -> RuntimeResult<Self> {
        let mut server = Self {
            raft,
            snapshot_store,
            state_machine,
            transport,
            ticker,
            inbound,
            config,
            stats: RuntimeStats::default(),
            snapshot_policy,
            applied_outputs: VecDeque::new(),
        };

        server.recover_state_machine_from_storage()?;
        Ok(server)
    }

    pub fn raft(&self) -> &RaftNode<C, S, LS, SS> {
        &self.raft
    }

    pub fn raft_mut(&mut self) -> &mut RaftNode<C, S, LS, SS> {
        &mut self.raft
    }

    pub fn snapshot_store(&self) -> &Snap {
        &self.snapshot_store
    }

    pub fn snapshot_store_mut(&mut self) -> &mut Snap {
        &mut self.snapshot_store
    }

    pub fn state_machine(&self) -> &SM {
        &self.state_machine
    }

    pub fn state_machine_mut(&mut self) -> &mut SM {
        &mut self.state_machine
    }

    pub fn transport(&self) -> &Tp {
        &self.transport
    }

    pub fn ticker(&self) -> &Tk {
        &self.ticker
    }

    pub fn ticker_mut(&mut self) -> &mut Tk {
        &mut self.ticker
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub fn stats(&self) -> &RuntimeStats {
        &self.stats
    }

    pub fn snapshot_policy(&self) -> SnapshotPolicy {
        self.snapshot_policy
    }

    pub fn set_snapshot_policy(&mut self, snapshot_policy: SnapshotPolicy) {
        self.snapshot_policy = snapshot_policy;
    }

    pub fn applied_outputs(&self) -> &VecDeque<(LogIndex, SM::Output)> {
        &self.applied_outputs
    }

    pub fn take_applied_outputs(&mut self) -> VecDeque<(LogIndex, SM::Output)> {
        std::mem::take(&mut self.applied_outputs)
    }

    pub fn force_snapshot(&mut self) -> RuntimeResult<Option<LogIndex>> {
        let snapshot_index = self.state_machine.last_applied();
        let current_snapshot_index = self.snapshot_store.last_included_index();

        if snapshot_index == 0 || snapshot_index <= current_snapshot_index {
            return Ok(None);
        }

        self.create_local_snapshot_through(snapshot_index)?;
        Ok(Some(snapshot_index))
    }

    pub fn run_once(&mut self) -> RuntimeResult<bool> {
        let mut made_progress = false;

        let inbound_processed = self.drain_inbound_messages()?;
        if inbound_processed > 0 {
            self.stats.inbound_messages_processed += inbound_processed as u64;
            made_progress = true;
        }

        let ticks = self.ticker.ticks_elapsed();
        if ticks > 0 {
            self.raft.tick(ticks);
            self.stats.ticks_observed += ticks;
            made_progress = true;
        }

        if self.drain_ready()? {
            made_progress = true;
        }

        if !made_progress && !self.config.idle_sleep.is_zero() {
            thread::sleep(self.config.idle_sleep);
        }

        Ok(made_progress)
    }

    pub fn run_until_idle(&mut self, max_loops: usize) -> RuntimeResult<usize> {
        let mut loops = 0;

        for _ in 0..max_loops {
            let made_progress = self.run_once()?;
            if !made_progress {
                break;
            }

            loops += 1;
        }

        Ok(loops)
    }

    pub fn serve(&mut self) -> RuntimeResult<()> {
        loop {
            self.run_once()?;
        }
    }

    fn recover_state_machine_from_storage(&mut self) -> RuntimeResult<()> {
        if let Some(snapshot) = self.snapshot_store.latest().cloned() {
            self.raft.restore_snapshot(snapshot.clone());
            self.state_machine.restore(snapshot.data);
            self.stats.snapshots_restored += 1;
        }

        let already_applied = self.state_machine.last_applied();
        let commit_index = self.raft.commit_index();

        if commit_index <= already_applied {
            self.raft.advance(already_applied);
            return Ok(());
        }

        let from = already_applied.saturating_add(1);
        let count = (commit_index - from + 1) as usize;
        let entries = self.raft.log.entries(from, count);

        if entries.len() != count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "state machine recovery gap: expected {} committed entries from index {}, found {}",
                    count,
                    from,
                    entries.len()
                ),
            )
            .into());
        }

        let mut applied_through = already_applied;

        for entry in entries {
            self.state_machine.apply(entry.index, &entry.command);
            applied_through = entry.index;
        }

        if applied_through > 0 {
            self.raft.advance(applied_through);
        }

        Ok(())
    }

    fn drain_inbound_messages(&mut self) -> RuntimeResult<usize> {
        let mut processed = 0;

        for _ in 0..self.config.max_inbound_messages_per_loop {
            match self.inbound.try_recv() {
                Ok(envelope) => {
                    self.raft.step(envelope);
                    processed += 1;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if processed == 0 {
                        return Err(RuntimeError::InboundClosed);
                    }

                    break;
                }
            }
        }

        Ok(processed)
    }

    fn drain_ready(&mut self) -> RuntimeResult<bool> {
        let mut made_progress = false;

        loop {
            let ready = self.raft.ready();
            if ready.is_empty() {
                break;
            }

            self.handle_ready(ready)?;
            made_progress = true;
        }

        Ok(made_progress)
    }

    fn handle_ready(&mut self, ready: Ready<C, S>) -> RuntimeResult<()> {
        let Ready {
            hard_state: _hard_state,
            entries_to_persist: _entries_to_persist,
            snapshot,
            messages,
            committed_entries,
            soft_state_changed: _soft_state_changed,
        } = ready;

        let mut applied_through = None;

        if let Some(snapshot) = snapshot {
            self.snapshot_store.save(snapshot.clone());
            self.raft.restore_snapshot(snapshot.clone());
            self.state_machine.restore(snapshot.data);
            applied_through = Some(self.state_machine.last_applied());
            self.stats.snapshots_restored += 1;
        }

        if !messages.is_empty() {
            self.stats.outbound_messages_sent += messages.len() as u64;
            self.transport.send_batch(messages);
        }

        for entry in committed_entries {
            let index = entry.index;
            let output = self.state_machine.apply(index, &entry.command);
            self.applied_outputs.push_back((index, output));
            applied_through = Some(index);
            self.stats.committed_entries_applied += 1;
        }

        if let Some(index) = applied_through {
            self.raft.advance(index);
        }

        self.maybe_create_local_snapshot()?;
        Ok(())
    }

    fn maybe_create_local_snapshot(&mut self) -> RuntimeResult<bool> {
        let Some(threshold) = self.snapshot_policy.threshold() else {
            return Ok(false);
        };

        let last_applied = self.state_machine.last_applied();
        let last_snapshot_index = self.snapshot_store.last_included_index();

        if last_applied == 0 || last_applied <= last_snapshot_index {
            return Ok(false);
        }

        if last_applied - last_snapshot_index < threshold {
            return Ok(false);
        }

        self.create_local_snapshot_through(last_applied)?;
        Ok(true)
    }

    fn create_local_snapshot_through(&mut self, snapshot_index: LogIndex) -> RuntimeResult<()> {
        let snapshot_term = self.snapshot_term(snapshot_index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cannot determine term for snapshot index {}",
                    snapshot_index
                ),
            )
        })?;

        let snapshot = Snapshot {
            last_included_index: snapshot_index,
            last_included_term: snapshot_term,
            data: self.state_machine.snapshot(),
        };

        self.snapshot_store.save(snapshot.clone());
        self.raft.restore_snapshot(snapshot);
        Ok(())
    }

    fn snapshot_term(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }

        if let Some(term) = self.raft.log.term(index) {
            return Some(term);
        }

        if self.snapshot_store.last_included_index() == index {
            return Some(self.snapshot_store.last_included_term());
        }

        self.raft
            .latest_snapshot()
            .filter(|snapshot| snapshot.last_included_index == index)
            .map(|snapshot| snapshot.last_included_term)
    }
}
