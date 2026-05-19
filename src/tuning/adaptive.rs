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
}
