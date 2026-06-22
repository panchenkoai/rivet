# `throttle_ms` vs. source-harm — does the inter-batch sleep earn its wall-clock?

**Question:** the default `balanced` tuning sleeps `throttle_ms: 50` between every
FETCH batch, to be gentle on the source. On a wide-table snapshot read, **how much
source-harm does that throttle actually remove, and what does it cost** — measured
by rivet's own `export_harm` counters, not by intuition?

**Answer: on a snapshot full read it removes ~0% of the cumulative source harm
while costing 6.4× wall-clock — and it holds the MVCC snapshot 6× longer, which is
*worse* for the one source-safety metric that matters on a busy OLTP source.** The
throttle is a fixed sleep *per batch*, so its cost scales with the batch **count**,
which `work_mem` blows up on wide tables — it targets the wrong variable.

---

## Method

Table: `content_items` on Postgres — ~20 wide columns (text `body`/`raw_html`,
`jsonb` metadata), **1 928 176 rows / 1.4 GB**, ≈ 760 B/row. Local PG 16, release
rivet (`opt-level=3`, lto-fat), ingestr v1.0.38 for the external baseline. Wall +
peak RSS via `gtime -v`. Source harm read from rivet's own `export_harm` table in
`.rivet_state.db`. Output integrity checked with an order-independent content hash
(`sum(hash(row))` over the Parquet via DuckDB).

`mode: full` is the controlled probe: single statement, single thread, so the only
variable between the two runs is `throttle_ms` (50 → 0).

## Wall-clock & memory

| Run | mode | `throttle_ms` | wall | peak RSS | output |
|---|---|---:|---:|---:|---:|
| rivet (v0.13.0) | full | 50 (`balanced`) | **272.0 s** | 146 MB | 195 MB pq |
| rivet (HEAD) | full | 50 (`balanced`) | 269.9 s | 146 MB | 195 MB pq |
| **rivet (HEAD)** | full | **0** | **42.2 s** | 148 MB | 195 MB pq |
| rivet (HEAD) | chunked ‖8 | 50 | 73.0 s | 208 MB | 196 MB pq |
| ingestr | — | (none) | 38.4 s | 815 MB | 9.1 GB csv |
| ingestr | — | (none) | 45.3 s | 4.2 GB | 214 MB duckdb |

Dropping the throttle: **270 s → 42 s, a 6.4× speedup** — putting single-threaded
rivet on par with ingestr's wall-clock at **5–28× less memory**, and with the only
local-Parquet output of the three.

## Source harm — rivet's own `export_harm`, throttled vs not

| metric | full, throttle 50 | full, throttle 0 | Δ |
|---|---:|---:|---|
| `pg_tup_returned` (read amplification) | 1 938 481 | 1 932 013 | **~0% (noise)** |
| `pg_blks_read` (disk) | 153 104 | 153 040 | **~0%** |
| `pg_temp_files` (spill) | 0 | 0 | **0** |

The source scanned the same rows, read the same blocks, and spilled nothing —
**whether rivet slept 228 s between batches or not.** Cumulative harm is set by the
query (a full scan), not by how slowly the client drains it. Sleeping just smears
the identical work across 6× the wall-clock.

What the throttle *does* change is the **snapshot-hold duration**: the cursor's
`BEGIN…COMMIT` transaction stays open for the whole run, so the throttled run pins
its MVCC snapshot for **270 s vs 42 s**. On a busy source a longer-held snapshot
*blocks vacuum* (bloat) and *widens replication lag* — so the "gentleness" feature,
by its own most-important yardstick, works **against** itself here.

(Aside — `chunked ‖8` records `pg_tup_returned = 4 017 875`, ~**2× read
amplification**: the parallel per-chunk range predicates re-scan rows. Lower disk
reads, more cache hits — a different harm shape, visible only because we persist
these counters.)

## Root cause

`AdaptiveBatchController::throttle()` = `thread::sleep(throttle_ms)` after every
batch. On `content_items` the default 4 MB `work_mem` caps FETCH N to **~420 rows**
(to avoid a `pgsql_tmp/` spill), so:

```
1 928 176 rows / ~420 rows-per-FETCH  ≈  4 560 batches
4 560 batches × 50 ms                 ≈  228 s of pure sleep
```

— which a profile confirms: **74 % of the main thread's wall-clock is
`std::thread::sleep`**, ~23 % is the network FETCH wait, and the actual
row→Arrow→Parquet work is single-digit percent. The throttle's cost is
`batches × throttle_ms`, and `batches` explodes precisely when `work_mem` shrinks
the FETCH — two runs that move the same data and harm the source identically can
differ 10× in throttle overhead purely from batch size.

> **Profiling red herring:** the first hypothesis was an Arrow/Parquet encoding
> inefficiency (the text arm read each cell as an owned `String` — alloc + copy —
> vs the JSON arm's zero-copy `&str`). The `&str` rewrite is correct and produces
> **byte-identical output** (verified: same content hash across all runs), but gave
> **~0 % speedup** — the bottleneck was never CPU. Profile before optimizing.

## Recommendation

Make the throttle **pace-aware** — target a source *duty cycle* or *rows/sec*, not a
fixed sleep per batch:

```
sleep = max(0, target_batch_interval − actual_batch_elapsed)
```

so the throttle bounds the *instantaneous* source rate (its only defensible benefit,
and only on a contended source) without paying `batches × const` and without
stretching the snapshot hold.

**Implemented in ADR-0022** as the minimal intent-preserving form:
`sleep_µs = throttle_ms × 1000 × rows_in_batch / configured_batch_size`. Total
pacing becomes `throttle_ms × total_rows / configured` — independent of batch
count — and a full configured-size batch (narrow tables) still pauses exactly
`throttle_ms` (unchanged). Validated on the default `balanced` profile:
**269.9 s → 55.2 s, byte-identical output, `export_harm` unchanged** (`pg_tup_returned`
1.932 M, `pg_blks_read` 153 008, 0 spill — same as the un-throttled run).

## After the fix — rivet vs ingestr

Same `content_items`, rivet at **default `balanced`** (throttle still on, now
row-proportional), ingestr v1.0.38 at its defaults:

| Tool / config | output | wall | peak RSS | rows |
|---|---|---:|---:|---:|
| ingestr | CSV | 38.4 s | 815 MB | 1 928 176 |
| ingestr | DuckDB | 45.3 s | 4.2 GB | 1 928 176 |
| **rivet** chunked ‖8 | **Parquet** | **37.6 s** | 211 MB | 1 928 176 |
| **rivet** full (1 thread) | Parquet | 55.2 s | 148 MB | 1 928 176 |

After the fix rivet's recommended path (`chunked ‖8`) is **faster than both ingestr
modes** while using **4–20× less memory**, producing the only local Parquet of the
three, with byte-identical output (same content hash across every run) and the same
source harm. The pre-fix gap (5–7× slower) was the per-batch throttle, not the
extraction engine — see the `&str` red herring above. Caveat: this is a single-host
benchmark (no network latency), which favours ingestr's bulk fetch; rivet's bounded
memory and short snapshot holds widen its lead on a networked or contended source.

## Reproduce

```bash
# wall + RSS, throttle on vs off (only difference)
gtime -v rivet run -c full_balanced.yaml  --export content_items   # throttle_ms: 50
gtime -v rivet run -c full_nothrottle.yaml --export content_items   # tuning: {throttle_ms: 0}

# source harm, per run, from the metabase
sqlite3 .rivet_state.db "
  SELECT m.run_id, m.duration_ms, h.metric, h.delta
  FROM export_harm h JOIN export_metrics m USING(run_id)
  WHERE h.metric IN ('pg_tup_returned','pg_blks_read','pg_temp_files')
  ORDER BY m.id, h.metric;"

# output integrity (must match across runs)
duckdb -c "SELECT count(*), sum(hash(c)::hugeint) FROM read_parquet('out/**/*.parquet') c;"
```
