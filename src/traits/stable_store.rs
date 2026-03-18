use crate::types::HardState;

pub trait StableStore {
    fn hard_state(&self) -> HardState;
    fn set_hard_state(&mut self, hs: HardState);
}
