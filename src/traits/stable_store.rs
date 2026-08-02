use crate::types::{ConfState, HardState};

pub trait StableStore {
    fn hard_state(&self) -> HardState;
    fn set_hard_state(&mut self, hs: HardState);
    fn conf_state(&self) -> Option<ConfState>;
    fn set_conf_state(&mut self, conf_state: ConfState);
}
