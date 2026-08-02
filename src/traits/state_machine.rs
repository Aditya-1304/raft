use crate::types::LogIndex;

pub trait StateMachine<C> {
    type Output;
    type Error: std::fmt::Debug;

    fn apply(&mut self, index: LogIndex, cmd: &C) -> Result<Self::Output, Self::Error>;
}

pub trait SnapshotableStateMachine<C>: StateMachine<C> {
    type Snapshot: Clone;

    fn snapshot(&self) -> Self::Snapshot;
    fn restore(&mut self, snapshot: Self::Snapshot) -> Result<(), Self::Error>;
    fn last_applied(&self) -> LogIndex;
}
