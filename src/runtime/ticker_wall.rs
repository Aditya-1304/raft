use std::time::{Duration, Instant};

use crate::traits::ticker::Ticker;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WallTickerConfig {
    pub tick_interval: Duration,
}

impl WallTickerConfig {
    pub fn new(tick_interval: Duration) -> Self {
        Self { tick_interval }
    }
}

impl Default for WallTickerConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(100),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WallTicker {
    tick_interval: Duration,
    last_observed: Instant,
}

impl WallTicker {
    pub fn new(tick_interval: Duration) -> Self {
        Self {
            tick_interval: sanitize_tick_interval(tick_interval),
            last_observed: Instant::now(),
        }
    }

    pub fn with_config(config: WallTickerConfig) -> Self {
        Self::new(config.tick_interval)
    }

    pub fn tick_interval(&self) -> Duration {
        self.tick_interval
    }

    pub fn reset(&mut self) {
        self.last_observed = Instant::now();
    }
}

impl Default for WallTicker {
    fn default() -> Self {
        Self::with_config(WallTickerConfig::default())
    }
}

impl Ticker for WallTicker {
    fn ticks_elapsed(&mut self) -> u64 {
        let elapsed = self.last_observed.elapsed();
        let tick_nanos = self.tick_interval.as_nanos();

        if tick_nanos == 0 {
            return 0;
        }

        let ticks = elapsed.as_nanos() / tick_nanos;
        if ticks == 0 {
            return 0;
        }

        let ticks = ticks.min(u64::MAX as u128) as u64;
        let advance_nanos = tick_nanos.saturating_mul(ticks as u128);
        let advance_nanos = advance_nanos.min(u64::MAX as u128) as u64;

        self.last_observed += Duration::from_nanos(advance_nanos);

        ticks
    }
}

fn sanitize_tick_interval(tick_interval: Duration) -> Duration {
    if tick_interval.is_zero() {
        Duration::from_millis(1)
    } else {
        tick_interval
    }
}
