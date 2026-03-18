use std::collections::HashMap;

use crate::{traits::state_machine::{SnapshotableStateMachine, StateMachine}, types::LogIndex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemKvCommand {
  Put { key: String, value: String },   
  Delete { key: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemKvOutput {
  Put { old: Option<String> },
  Delete { old: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemKvSnapshot {
  pub data: HashMap<String, String>,
  pub last_applied: LogIndex,
}

#[derive(Debug, Clone, Default)]
pub struct MemKv {
  data: HashMap<String, String>,
  last_applied: LogIndex,
}

impl MemKv {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn get(&self, key: &str) -> Option<&String> {
    self.data.get(key)
  }
}

impl StateMachine<MemKvCommand> for MemKv {
  type Output = MemKvOutput;

  fn apply(&mut self, index: LogIndex, cmd: &MemKvCommand) -> Self::Output {
    self.last_applied = index;

    match cmd {
      MemKvCommand::Put { key, value } => {
        let old = self.data.insert(key.clone(), value.clone());
        MemKvOutput::Put { old }
      }
      MemKvCommand::Delete { key } => {
        let old = self.data.remove(key);
        MemKvOutput::Delete { old }
      }
    }
  }
}

impl SnapshotableStateMachine<MemKvCommand> for MemKv {
  type Snapshot = MemKvSnapshot;

  fn snapshot(&self) -> Self::Snapshot {
    MemKvSnapshot {
      data: self.data.clone(),
      last_applied: self.last_applied,
    }
  }

  fn restore(&mut self, snapshot: Self::Snapshot) {
    self.data = snapshot.data;
    self.last_applied = snapshot.last_applied;
  }

  fn last_applied(&self) -> LogIndex {
    self.last_applied
  }
}