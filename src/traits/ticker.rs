pub trait Ticker {
  fn ticks_elapsed(&mut self) -> u64;
}