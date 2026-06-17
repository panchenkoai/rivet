//! Shared adaptive-batch state machine for the per-engine export loops.
//!
//! All three source engines run the **same batch-sizing policy** around their
//! engine-specific row source (PostgreSQL `FETCH N` from a server cursor, MySQL
//! row-iterator accumulation, SQL Server `tiberius` stream accumulation):
//!
//! 1. start small — a `PROBE_BATCH_SIZE` first batch to measure real per-row
//!    Arrow bytes before any memory cap can be computed;
//! 2. once that width is known, cap the batch to a memory budget (clamped to the
//!    user's `batch_size`) and re-baseline;
//! 3. every `ADAPTIVE_SAMPLE_INTERVAL` batches, shrink/grow under DB
//!    back-pressure via [`next_adaptive_batch_size`];
//! 4. sleep `throttle_ms` between batches.
//!
//! The *fetch* and the *memory-cap formula* genuinely differ per engine (PG caps
//! on `work_mem`; MySQL/MSSQL on a target-MB), and so do the *timeouts* (PG/MySQL
//! set a server-side statement timeout; MSSQL has no such `SET` and enforces a
//! client deadline). Those stay in the engines. Everything else — the delicate,
//! previously-triplicated probe → cap → adaptive bookkeeping — lives here, in one
//! unit-tested place.

use crate::tuning::{ADAPTIVE_SAMPLE_INTERVAL, SourceTuning, next_adaptive_batch_size};

/// Initial probe size: small enough to measure per-row Arrow bytes before a
/// memory cap exists, without a wide-row first batch blowing the budget.
pub(crate) const PROBE_BATCH_SIZE: usize = 500;

/// Default per-batch Arrow memory budget (MB) when the export sets no
/// `batch_size_memory_mb`. Used by the row-accumulating engines (MySQL, MSSQL)
/// to size the post-probe cap; PostgreSQL derives its own from `work_mem`.
pub(crate) const DEFAULT_BATCH_TARGET_MB: usize = 64;

/// Owns the batch-size state machine shared by all engine export loops.
pub(crate) struct AdaptiveBatchController {
    effective: usize,
    base: usize,
    configured: usize,
    adaptive: bool,
    throttle_ms: u64,
    batch_count: usize,
    last_pressure: Option<u64>,
    cap_applied: bool,
}

impl AdaptiveBatchController {
    /// `configured_batch_size` is the user's resolved `batch_size` — the size is
    /// never grown past it. The controller starts at the probe size.
    pub(crate) fn new(tuning: &SourceTuning, configured_batch_size: usize) -> Self {
        let configured = configured_batch_size.max(1);
        let effective = configured.min(PROBE_BATCH_SIZE);
        Self {
            effective,
            base: effective,
            configured,
            adaptive: tuning.adaptive,
            throttle_ms: tuning.throttle_ms,
            batch_count: 0,
            last_pressure: None,
            cap_applied: false,
        }
    }

    /// Adopt the true configured ceiling once an engine that discovers its row
    /// width *mid-stream* can finally compute its memory-driven batch size.
    /// SQL Server learns the column shape from the first `tiberius` metadata
    /// token — *after* the controller is already built — so it can't seed
    /// `configured` from `effective_batch_size` up-front the way PG and MySQL
    /// do. Calling this when the metadata arrives raises the ceiling so the
    /// post-probe memory cap can grow the batch past the static `batch_size`.
    ///
    /// Only ever *raises* the ceiling (a wide table whose memory-driven size is
    /// below the static `batch_size` is left to shrink via the cap as before),
    /// and only before the one-shot memory cap has fired — afterwards the cap
    /// already used the ceiling and re-targeting would be a silent no-op anyway.
    pub(crate) fn raise_configured_ceiling(&mut self, new_configured: usize) {
        if !self.cap_applied {
            self.configured = self.configured.max(new_configured.max(1));
        }
    }

    /// Seed the adaptive baseline with a pre-loop pressure sample so the first
    /// adaptive decision compares against a real prior reading (matches the
    /// pre-extraction PG/MySQL behaviour). No-op when not adaptive.
    pub(crate) fn seed_pressure(&mut self, initial: Option<u64>) {
        if self.adaptive {
            self.last_pressure = initial;
        }
    }

    /// Rows to request for the next batch.
    pub(crate) fn target(&self) -> usize {
        self.effective
    }

    /// One-shot, applied after the first (probe) batch: `memory_target` is the
    /// engine's memory-budget row count (PG: `work_mem`-derived; MySQL/MSSQL:
    /// `batch_size_memory_mb`-derived). Clamped to the configured `batch_size`
    /// and used as the new adaptive baseline. Returns the new size if it
    /// changed (so the engine can log it), else `None`.
    pub(crate) fn apply_memory_cap(&mut self, memory_target: usize) -> Option<usize> {
        if self.cap_applied {
            return None;
        }
        self.cap_applied = true;
        let target = memory_target.max(1).min(self.configured);
        self.base = target;
        if target != self.effective {
            self.effective = target;
            Some(target)
        } else {
            None
        }
    }

    /// Call after emitting each batch. Every `ADAPTIVE_SAMPLE_INTERVAL` batches
    /// (and only when `adaptive`), takes a fresh pressure reading via `sample`
    /// and resizes. Returns `Some((new_size, under_pressure))` when the size
    /// changed, for the engine's log line.
    pub(crate) fn after_batch(
        &mut self,
        sample: impl FnOnce() -> Option<u64>,
    ) -> Option<(usize, bool)> {
        self.batch_count += 1;
        if !self.adaptive || !self.batch_count.is_multiple_of(ADAPTIVE_SAMPLE_INTERVAL) {
            return None;
        }
        let cur = sample()?;
        let under_pressure = self.last_pressure.is_some_and(|prev| cur > prev);
        self.last_pressure = Some(cur);
        let next = next_adaptive_batch_size(self.effective, self.base, under_pressure);
        if next != self.effective {
            self.effective = next;
            Some((next, under_pressure))
        } else {
            None
        }
    }

    /// Sleep `throttle_ms` between batches if configured.
    pub(crate) fn throttle(&self) {
        if self.throttle_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.throttle_ms));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuning::SourceTuning;

    fn tuning(adaptive: bool, throttle_ms: u64) -> SourceTuning {
        let mut t = SourceTuning::from_config(None);
        t.adaptive = adaptive;
        t.throttle_ms = throttle_ms;
        t
    }

    #[test]
    fn starts_at_probe_size_capped_by_configured() {
        let c = AdaptiveBatchController::new(&tuning(false, 0), 50_000);
        assert_eq!(c.target(), PROBE_BATCH_SIZE);
        // A tiny configured size wins over the probe.
        let c2 = AdaptiveBatchController::new(&tuning(false, 100), 100);
        assert_eq!(c2.target(), 100);
    }

    #[test]
    fn memory_cap_is_one_shot_and_clamped_to_configured() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 20_000);
        // Engine asks for 9_000 rows of budget → within configured → applied.
        assert_eq!(c.apply_memory_cap(9_000), Some(9_000));
        assert_eq!(c.target(), 9_000);
        // Second call is a no-op (one-shot).
        assert_eq!(c.apply_memory_cap(1_000), None);
        assert_eq!(c.target(), 9_000);
    }

    #[test]
    fn memory_cap_never_exceeds_configured() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 5_000);
        // Engine budget would allow 1M rows; configured caps it.
        assert_eq!(c.apply_memory_cap(1_000_000), Some(5_000));
        assert_eq!(c.target(), 5_000);
    }

    #[test]
    fn raise_ceiling_lets_cap_grow_past_static_batch_size() {
        // SQL Server case: built with the static batch_size (10k) before the
        // schema is known; the metadata token then raises the ceiling so the
        // post-probe memory cap can size a narrow table's batch well past 10k.
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 10_000);
        c.raise_configured_ceiling(500_000);
        assert_eq!(c.apply_memory_cap(480_000), Some(480_000));
        assert_eq!(c.target(), 480_000);
    }

    #[test]
    fn raise_ceiling_only_raises_never_lowers() {
        // A wide table whose memory-driven size is below the static batch_size
        // must NOT lower the ceiling — the cap still shrinks it as before.
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 10_000);
        c.raise_configured_ceiling(5_000);
        assert_eq!(c.apply_memory_cap(1_000_000), Some(10_000));
        assert_eq!(c.target(), 10_000);
    }

    #[test]
    fn raise_ceiling_after_cap_is_a_no_op() {
        // Once the one-shot cap has fired the ceiling is spent; a late raise
        // can't retroactively grow the (already one-shot) cap.
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 10_000);
        c.apply_memory_cap(8_000);
        c.raise_configured_ceiling(500_000);
        assert_eq!(c.apply_memory_cap(400_000), None);
        assert_eq!(c.target(), 8_000);
    }

    #[test]
    fn adaptive_only_fires_every_interval() {
        let mut c = AdaptiveBatchController::new(&tuning(true, 0), 50_000);
        c.apply_memory_cap(10_000);
        c.seed_pressure(Some(0));
        // Batches 1..9 → no resize even though sample reports rising pressure.
        for _ in 0..(ADAPTIVE_SAMPLE_INTERVAL - 1) {
            assert_eq!(c.after_batch(|| Some(100)), None);
        }
        // Batch 10 → rising pressure (100 > seeded 0) → shrink.
        let r = c.after_batch(|| Some(100));
        assert!(
            matches!(r, Some((_, true))),
            "expected a shrink under pressure, got {r:?}"
        );
        assert!(c.target() < 10_000);
    }

    #[test]
    fn adaptive_noop_when_disabled() {
        let mut c = AdaptiveBatchController::new(&tuning(false, 0), 50_000);
        c.apply_memory_cap(10_000);
        for _ in 0..(ADAPTIVE_SAMPLE_INTERVAL * 2) {
            assert_eq!(c.after_batch(|| Some(999_999)), None);
        }
        assert_eq!(c.target(), 10_000);
    }
}
