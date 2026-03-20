use std::{fmt, io, time::Duration};

pub mod client;
pub mod server;
pub mod ticker_wall;
pub mod transport_tcp;

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub max_inbound_messages_per_loop: usize,
    pub idle_sleep: Duration,
}

impl RuntimeConfig {
    pub fn new(max_inbound_messages_per_loop: usize, idle_sleep: Duration) -> Self {
        Self {
            max_inbound_messages_per_loop: max_inbound_messages_per_loop.max(1),
            idle_sleep,
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_inbound_messages_per_loop: 64,
            idle_sleep: Duration::from_millis(1),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeStats {
    pub ticks_observed: u64,
    pub inbound_messages_processed: u64,
    pub outbound_messages_sent: u64,
    pub committed_entries_applied: u64,
    pub snapshots_restored: u64,
}

#[derive(Debug)]
pub enum RuntimeError {
    Io(io::Error),
    InboundClosed,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeError::Io(err) => write!(f, "runtime I/O error: {err}"),
            RuntimeError::InboundClosed => write!(f, "runtime inbound channel closed"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RuntimeError::Io(err) => Some(err),
            RuntimeError::InboundClosed => None,
        }
    }
}

impl From<io::Error> for RuntimeError {
    fn from(err: io::Error) -> Self {
        RuntimeError::Io(err)
    }
}
