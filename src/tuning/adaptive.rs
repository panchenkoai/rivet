//! Adaptive batch sizing — live-feedback loop that reacts to DB pressure.
//!
//! Both `PostgresSource` and `MysqlSource` sample pressure metrics every
//! [`ADAPTIVE_SAMPLE_INTERVAL`] batches (`pg_stat_bgwriter.checkpoints_req` for
//! PG; `Innodb_log_waits` for MySQL) and call [`next_adaptive_batch_size`] to
//! pick the next fetch size.

/// Number of batches between adaptive pressure samples.
pub const ADAPTIVE_SAMPLE_INTERVAL: usize = 10;

/// Hard floor for the adaptive fetch size — the loop never shrinks below this.
pub const ADAPTIVE_MIN_BATCH: usize = 500;

/// Decide the next adaptive fetch size from current pressure state.
///
/// - Under pressure: shrink to 75 %, but never below [`ADAPTIVE_MIN_BATCH`].
/// - Otherwise: grow to 125 %, but never above the schema-chosen `base` ceiling
///   (so we recover toward the initial fetch size without overshooting it).
///
/// Pure function — exported so adaptive batch-sizing can be unit-tested without
/// a live database.
pub fn next_adaptive_batch_size(current: usize, base: usize, under_pressure: bool) -> usize {
    if under_pressure {
        (current * 3 / 4).max(ADAPTIVE_MIN_BATCH)
    } else {
        (current * 5 / 4).min(base)
    }
}

/// Milliseconds between governor pressure samples (the parallelism control
/// loop in `pipeline::chunked::exec`). Coarser than batch-size adaptation:
/// spinning workers up/down churns connections, so the governor reacts more
/// deliberately than the per-batch fetch-size loop.
pub const GOVERNOR_SAMPLE_INTERVAL_MS: u64 = 1500;

/// Decide the next worker/connection count from current governor state.
///
/// Steps by **one** toward the bounds — gentler than the batch loop's ±25 %
/// ratio, because permit counts are small integers and each step opens or
/// retires a real source connection:
/// - Under pressure: shed one worker, never below `min`.
/// - Otherwise: recover one worker, never above `max`.
///
/// `min` is floored at 1 (a 0 ceiling would stall the pool) and `max` at `min`.
/// Pure function — exported so the governor can be unit-tested without a live DB.
pub fn next_parallel(current: usize, min: usize, max: usize, under_pressure: bool) -> usize {
    let lo = min.max(1);
    let hi = max.max(lo);
    let cur = current.clamp(lo, hi);
    if under_pressure {
        cur.saturating_sub(1).max(lo)
    } else {
        (cur + 1).min(hi)
    }
}

/// Decision state for the concurrency governor's control loop.
///
/// Holds the previous pressure sample and the current target parallelism so the
/// I/O parts (pressure sampling, semaphore resize, journaling) stay in the
/// execution layer while this — the actual policy — is unit-testable without a
/// live database or threads. `under_pressure` mirrors the batch loop: a sample
/// strictly higher than the previous one means pressure is rising.
#[derive(Debug)]
pub struct GovernorState {
    prev: Option<u64>,
    current: usize,
    floor: usize,
    ceiling: usize,
}

impl GovernorState {
    /// Start at `start`, clamped into `[floor, ceiling]`. `ceiling` is floored
    /// at 1 and `floor` clamped into `[1, ceiling]`.
    pub fn new(start: usize, floor: usize, ceiling: usize) -> Self {
        let ceiling = ceiling.max(1);
        let floor = floor.clamp(1, ceiling);
        Self {
            prev: None,
            current: start.clamp(floor, ceiling),
            floor,
            ceiling,
        }
    }

    /// Current target parallelism. Test-only observability accessor.
    #[cfg(test)]
    pub fn current(&self) -> usize {
        self.current
    }

    /// Fold one pressure sample into the state. Returns `Some((from, to))` when
    /// the target changed (caller should resize the semaphore + journal it), or
    /// `None` when nothing changed — including when `sample` is `None` (the
    /// engine couldn't sample, so parallelism holds flat and the baseline is
    /// left untouched).
    pub fn observe(&mut self, sample: Option<u64>) -> Option<(usize, usize)> {
        let cur_p = sample?;
        let under_pressure = self.prev.is_some_and(|p| cur_p > p);
        self.prev = Some(cur_p);
        let next = next_parallel(self.current, self.floor, self.ceiling, under_pressure);
        if next == self.current {
            None
        } else {
            let from = self.current;
            self.current = next;
            Some((from, next))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_shrinks_by_25_percent_under_pressure() {
        assert_eq!(next_adaptive_batch_size(10_000, 10_000, true), 7_500);
        assert_eq!(next_adaptive_batch_size(8_000, 10_000, true), 6_000);
    }

    #[test]
    fn adaptive_grows_by_25_percent_when_idle() {
        // 4_000 × 5/4 = 5_000; well under base ceiling.
        assert_eq!(next_adaptive_batch_size(4_000, 10_000, false), 5_000);
    }

    #[test]
    fn adaptive_recovery_caps_at_base_ceiling() {
        // 9_000 × 5/4 = 11_250, but base is 10_000 — must clamp.
        assert_eq!(next_adaptive_batch_size(9_000, 10_000, false), 10_000);
        // Already at base: stays there.
        assert_eq!(next_adaptive_batch_size(10_000, 10_000, false), 10_000);
    }

    #[test]
    fn adaptive_shrink_respects_min_floor() {
        // 600 × 3/4 = 450, but ADAPTIVE_MIN_BATCH = 500 — must clamp up.
        assert_eq!(
            next_adaptive_batch_size(600, 10_000, true),
            ADAPTIVE_MIN_BATCH
        );
        // Already at floor: stays at floor.
        assert_eq!(
            next_adaptive_batch_size(ADAPTIVE_MIN_BATCH, 10_000, true),
            ADAPTIVE_MIN_BATCH
        );
    }

    #[test]
    fn adaptive_pressure_path_ignores_base_uses_only_floor() {
        // Pressure path never consults base: shrink is computed from current,
        // then clamped only to ADAPTIVE_MIN_BATCH. A pathologically low base
        // does not artificially pin us lower than the floor.
        assert_eq!(
            next_adaptive_batch_size(ADAPTIVE_MIN_BATCH, 100, true),
            ADAPTIVE_MIN_BATCH
        );
    }

    #[test]
    fn adaptive_steady_state_oscillation_stays_bounded() {
        // Simulate 50 sample cycles under sustained pressure, then sustained recovery.
        // Verifies: the loop never wanders below floor or above base, and converges.
        let base = 5_000;
        let mut s = base;
        for _ in 0..50 {
            s = next_adaptive_batch_size(s, base, true);
        }
        assert_eq!(
            s, ADAPTIVE_MIN_BATCH,
            "sustained pressure must converge to floor"
        );
        for _ in 0..50 {
            s = next_adaptive_batch_size(s, base, false);
        }
        assert_eq!(s, base, "sustained recovery must converge to base ceiling");
    }

    // ── next_parallel (governor) ──────────────────────────────────────────────

    #[test]
    fn next_parallel_sheds_one_under_pressure() {
        assert_eq!(next_parallel(8, 1, 8, true), 7);
        assert_eq!(next_parallel(4, 1, 8, true), 3);
    }

    #[test]
    fn next_parallel_recovers_one_when_idle() {
        assert_eq!(next_parallel(4, 1, 8, false), 5);
    }

    #[test]
    fn next_parallel_shrink_respects_min_floor() {
        assert_eq!(next_parallel(2, 2, 8, true), 2, "already at min stays");
        assert_eq!(next_parallel(1, 1, 8, true), 1, "never below 1");
    }

    #[test]
    fn next_parallel_grow_respects_max_ceiling() {
        assert_eq!(next_parallel(8, 1, 8, false), 8, "already at max stays");
    }

    #[test]
    fn next_parallel_min_floored_at_one() {
        // A nonsensical min=0 must not let the count drop to 0 (would stall).
        assert_eq!(next_parallel(1, 0, 8, true), 1);
    }

    #[test]
    fn next_parallel_steady_state_converges_to_bounds() {
        let (min, max) = (2, 6);
        let mut p = max;
        for _ in 0..20 {
            p = next_parallel(p, min, max, true);
        }
        assert_eq!(p, min, "sustained pressure converges to min");
        for _ in 0..20 {
            p = next_parallel(p, min, max, false);
        }
        assert_eq!(p, max, "sustained recovery converges to max");
    }

    // ── GovernorState ─────────────────────────────────────────────────────────

    #[test]
    fn governor_state_clamps_start_into_bounds() {
        assert_eq!(GovernorState::new(99, 2, 6).current(), 6);
        assert_eq!(GovernorState::new(0, 2, 6).current(), 2);
        // floor floored at 1, ceiling at 1.
        assert_eq!(GovernorState::new(5, 0, 0).current(), 1);
    }

    #[test]
    fn governor_state_first_sample_only_sets_baseline_then_recovers() {
        // Start at ceiling: first idle sample wants to grow but is already
        // capped, so no change is reported.
        let mut g = GovernorState::new(6, 2, 6);
        assert_eq!(g.observe(Some(100)), None, "at ceiling, idle ⇒ no change");
        assert_eq!(g.current(), 6);
    }

    #[test]
    fn governor_state_backs_off_under_rising_pressure() {
        let mut g = GovernorState::new(6, 2, 6);
        assert_eq!(g.observe(Some(100)), None); // baseline, at ceiling
        assert_eq!(g.observe(Some(200)), Some((6, 5)), "rising ⇒ shed one");
        assert_eq!(g.observe(Some(300)), Some((5, 4)));
        assert_eq!(g.current(), 4);
    }

    #[test]
    fn governor_state_recovers_when_pressure_flat() {
        let mut g = GovernorState::new(3, 2, 6);
        assert_eq!(
            g.observe(Some(100)),
            Some((3, 4)),
            "flat/idle ⇒ recover one"
        );
        assert_eq!(g.observe(Some(100)), Some((4, 5)));
    }

    #[test]
    fn governor_state_none_sample_holds_flat_and_keeps_baseline() {
        let mut g = GovernorState::new(4, 2, 6);
        assert_eq!(g.observe(Some(200)), Some((4, 5))); // baseline=200, grew
        assert_eq!(g.observe(None), None, "no sample ⇒ no change");
        // Baseline stayed 200, so a later 300 still reads as rising.
        assert_eq!(
            g.observe(Some(300)),
            Some((5, 4)),
            "rising vs preserved baseline"
        );
    }
}
