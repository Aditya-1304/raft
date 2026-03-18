use crate::types::LogIndex;

pub trait StateMachine<C> {
    type Output;

    fn apply(&mut self, index: LogIndex, cmd: &C) -> Self::Output;
}

pub trait SnapshotableStateMachine<C>: StateMachine<C> {
    type Snapshot: Clone;

    fn snapshot(&self) -> Self::Snapshot;
    fn restore(&mut self, snapshot: Self::Snapshot);
    fn last_applied(&self) -> LogIndex;
}
