# Parallel keyset — design (settled)

Companion to `results.md` (the prototype + bench that motivated this). Records
the decisions taken so the implementation and future contributors don't
re-litigate them.

## Decision

Parallelise keyset (seek) pagination by splitting the key space into N
**row-balanced** ranges and running a keyset seek loop per range concurrently.

**Load-bearing invariant — parity is STRUCTURAL, not sampled.** N−1 boundaries
`b₁<…<b_{n-1}` partition the key into half-open intervals
`(-∞,b₁] (b₁,b₂] … (b_{n-1},+∞)`; each worker pages `WHERE key > lo AND key <= hi`.
Every key falls in exactly one interval, so the union reads the whole table
exactly once **regardless of the boundaries** — a bad/stale sample hurts only
BALANCE (speed), never completeness. The key is unique (keyset requires a unique
index) → no ties → no empty/duplicate ranges. This decouples "how to sample"
(a perf question) from "is it complete" (structure).

## Settled decisions

| # | Decision | Choice |
|---|---|---|
| D1 | Where boundaries live | **plan-time** — sampled in `build_plan`, stored in the plan; `rivet plan` shows the ranges (reviewable, deterministic). Staleness is benign (completeness holds). |
| D2 | Boundary sampler | A **per-engine seam** (`sample_key_quantiles(key, n)`), OFFSET for ≤~10M rows, a sampled scan (`TABLESAMPLE` PG/MSSQL, sampled MySQL) beyond. Crossover is a bench-informed tuning constant. |
| D3 | Row-count parity | **structural** (interval partition); a live test asserts union == whole table. |
| D4 | Worker count N | **user/tuning-controlled** via `parallel:`. Bench plateaued at ~4 on a saturated local mysqld; a real server scales further. Don't guess a fixed N. |
| D5 | Activation | **explicit opt-in**: `chunk_by_key: <key>` + `parallel: N`. `rivet init`/`check` RECOMMEND it for large keyset tables but never silently enable (N concurrent scans = N× source load). |
| D6 | Iteration-1 scope | **lean-first**: N-way `thread::scope` + structural parity + per-worker run-unique parts. **NO crash-recovery** — a crashed run does a clean re-run from scratch (keyset-without-checkpoint semantics). |
| D7 | Iteration-2 (deferred) | per-range checkpoint + crash-recovery, reusing `chunked/parallel_checkpoint.rs`'s task-queue (ranges as claimable tasks, per-range high-water cursor, crashed range re-claimed). |

## Iteration-1 implementation seams

1. **Config** — `ExportConfig.parallel: Option<usize>` already exists for chunked;
   allow it alongside `chunk_by_key`. (Today `parallel` is chunked-only; keyset
   forces 1.)
2. **Plan** — `KeysetPlan` gains `parallel: usize` + `boundaries: Vec<String>`
   (typed key values as strings, like the cursor). `build_plan` samples the
   boundaries when `parallel > 1`.
3. **Sampler** — `Source::sample_key_quantiles(&base_query, key, n) ->
   Result<Vec<String>>` (default OFFSET impl; per-engine override later).
4. **Runner** — `run_keyset_parallel`: `thread::scope` spawns N workers, each the
   existing per-page seek reader bounded to its `(lo, hi]`, its own source conn
   and sink, run-unique part names. Reuses `keyset::read_seek_page` with a `hi`
   bound added.
5. **Parity test** — a live test (`roast_keyset_parallel_reads_every_row_once`)
   asserting the union of N ranges == the whole table (count + distinct-id set),
   RED against a mutant that drops a boundary row (`> lo AND < hi`, off-by-one).

## Non-goals (iteration 1)

- Crash-recovery / resume (D7, deferred).
- Auto-enabling parallel (D5 — opt-in only).
- Cross-engine sampler optimisation (D2 — OFFSET first; sampled-scan when the
  crossover bench justifies it).
