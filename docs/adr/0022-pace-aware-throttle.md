# ADR-0022: Inter-Batch Throttle Paces by Rows Pulled, Not Per Batch

**Status**: Accepted
**Date**: 2026-06-22

---

## Context

Every per-engine export loop calls `AdaptiveBatchController::throttle()` after
emitting a batch, to pace the source ("be gentle"). The `balanced` profile sets
`throttle_ms: 50`, `safe` 500, `fast` 0. Until now `throttle()` was a flat
`std::thread::sleep(throttle_ms)` — a **fixed sleep per batch**.

That makes the throttle's total cost `batch_count × throttle_ms`, and
`batch_count` is *not* a stable property of the data: PostgreSQL caps `FETCH N`
under `work_mem × 0.7` to avoid a `pgsql_tmp/` spill, so a **wide** table is read
in many small batches. On `content_items` (1.93 M rows, ~20 wide text/jsonb
columns, default 4 MB `work_mem`) the FETCH is capped to ~420 rows → ~4 560
batches → **~228 s of `thread::sleep`**, i.e. **74 % of wall-clock** (confirmed by
a `sample` profile: the main thread is overwhelmingly in `thread::sleep`, not in
the row→Arrow→Parquet work).

The measured payoff of that 228 s of pacing, read from rivet's own `export_harm`
counters, was **~0**: with `throttle_ms: 50` vs `0` the source returned the same
`pg_tup_returned` (1.938 M vs 1.932 M), read the same `pg_blks_read` (153 104 vs
153 040), and spilled zero temp files — identical cumulative harm, output
byte-identical. Worse, the throttled run held the cursor's MVCC snapshot **6×
longer** (270 s vs 42 s), which on a busy OLTP source blocks vacuum and widens
replication lag — the *opposite* of gentleness. Re-measured by the cross-tool
harness (`dev/bench/smoke.py`): dropping the `balanced` 50 ms/batch throttle
(`tuning.profile: fast`) buys +24 % rows/s with no worse harm — see
[`docs/bench/report.html`](../bench/report.html).

The throttle was targeting the wrong variable. Cumulative source harm tracks the
*query* (a full scan), and a sensible rate limit tracks *rows (or bytes) per
second* — neither tracks *batch count*, which `work_mem` controls.

## Decision

Make the throttle **row-proportional**: scale `throttle_ms` by the fraction of a
full `configured`-size batch the emitted batch represents, computed in
microseconds so small batches don't truncate to zero.

```
sleep_µs = throttle_ms × 1000 × rows_in_batch / configured_batch_size
```

Total throttle over a run becomes `throttle_ms × total_rows / configured` —
**independent of how many batches the row source was split into**. `throttle()`
now takes the batch's row count; all three engines pass it (`row_count` /
`batch.num_rows()` / `buf.len()`). The arithmetic lives in a pure
`throttle_sleep_us()` so it is unit-tested without timing.

Calibration is preserved, not invented: a **full** configured-size batch (the
narrow-table case, where the FETCH returns `batch_size` rows) still pauses
exactly `throttle_ms` — that path is unchanged. Only batches that `work_mem`
forced *below* the configured size now pause proportionally less.

We keep `throttle_ms` (a duration) as the config knob rather than switching to a
fraction or a rows/sec cap: it stays backward-compatible, the profile defaults
(0 / 50 / 500) keep their meaning for the common (full-batch) case, and the fix
is a one-line semantic change at the point of use.

## Consequences

- **`balanced` on wide tables stops paying ~6× wall-clock for no gentleness.**
  `content_items` full export: ~270 s → ~52 s (≈ 42 s work + ~9.6 s throttle),
  output byte-identical, `export_harm` unchanged. This is a **MINOR** behaviour
  change: `balanced`/`safe` runs on wide tables finish faster (less idle sleep)
  for the same source pressure; narrow-table runs are unchanged.
- The throttle is now a genuine **rate** limit (sleep ∝ rows pulled), so it
  bounds the source's instantaneous read rate — its one defensible benefit, on a
  contended source — without the snapshot-hold inflation.
- Microsecond granularity means a sub-200-row FETCH still pauses (e.g. 500 µs for
  100 rows) instead of the integer-ms `floor` silently dropping the throttle. The
  OS may round a sub-ms sleep up to its timer granularity, which only errs toward
  *more* throttle (conservative).
- **Bytes would be more harm-proportional than rows** (wide rows cost the source
  more per row), but `configured` is a row count and the memory cap already
  bounds batch *bytes*; row-proportional is the minimal intent-preserving change.
  Revisit if a future workload shows row-count pacing materially mis-tracking
  byte-level source load.

## References

- [`docs/bench/report.html`](../bench/report.html) — the cross-tool harness that re-measures the throttle's cost.
- ADR-0019 — the governor (adaptive *concurrency*); this is the per-batch *pacing* knob, a separate lever.
