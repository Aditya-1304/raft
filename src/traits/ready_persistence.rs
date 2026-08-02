use crate::core::ready::Ready;

/// Fallible host boundary for one immutable Ready generation.
///
/// Implementations must publish snapshot metadata first, append entries in
/// increasing index order, append `ConfState` and `HardState` last, and make
/// the complete prefix durable before returning success. An uncertain result
/// must fail-stop the owning host; it must never be treated as a fresh retry.
pub trait ReadyPersistence<C, S> {
    type Error;

    fn persist_ready(&mut self, ready: &Ready<C, S>) -> Result<(), Self::Error>;
}
