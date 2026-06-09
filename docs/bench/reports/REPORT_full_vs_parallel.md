# Full vs. chunked-parallel — same table, three engines

**Question:** for one big table, what does `mode: chunked` + `parallel: 4` buy
(or cost) versus the single-threaded `mode: full` baseline — on **wall time**,
**memory**, and **DBA-harm to the source** — and is the answer the same across
Postgres, MySQL, and SQL Server?

**Answer: no — three engines, three opposite verdicts.** Parallelism is not a
universal "go faster" switch; whether it helps, hurts, or is *mandatory* depends
on the engine's single-statement throughput and its MVCC/timeout behaviour.

---

## Method

Same physical table on each engine (`content_items`, ~20 wide columns,
text/longtext/json bodies ≈ 4 KB/row), seeded to ~2 M rows. Two rivet runs,
identical except strategy:

- **A — `mode: full`** — one statement, one connection, single thread.
- **B — `mode: chunked, chunk_column: id, chunk_size: 100000, parallel: 4`** —
  20 chunks across 4 worker connections.

Both: `format: parquet, compression: zstd`, local destination, rivet 0.9.4
(opt-level 3 + write-pipelining + simdutf8 text decode).

- **Perf**: `/usr/bin/time -l` → wall + peak RSS; rows/s = rows ÷ wall.
- **DBA-harm**: an in-DB sampler @ 50 ms (autocommits each sample, so the
  sampler itself holds no long snapshot), peaks taken over the run:
  - **longest open txn** — the snapshot/bloat *window* the export pins open.
  - **PG xmin-horizon age** — how far back (in xacts) the oldest `backend_xmin`
    pins VACUUM's cleanup horizon. This is the *mechanism* by which a long
    export bloats the source: while held, dead tuples newer than it can't be
    reclaimed. (PG-only; the direct bloat signal.)
  - **peak concurrent connections/sessions**, **peak locks** (MSSQL).

> **Sampling caveat.** The sampler ran a fixed 2000 iterations (~120 s of
> coverage). Runs longer than that (PG A_full @ 233 s) are **under-sampled** —
> their reported harm peaks are *floors*; the true peak is higher. This only
> makes the "parallel is gentler" gap **larger** than shown. B-runs and all
> MySQL runs were fully covered.

---

## Results (wide `content_items`, ~2 M rows)

Numbers below are the clean re-run with the 8000-iteration sampler (every run
fully covered — no floors).

| Engine | Variant | Wall | rows/s | Peak RSS | Parts | Longest open txn | Longest single query/req | Peak conns | PG xmin-age |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **Postgres** (1.93 M) | A · full | 232.5 s | 8 289 | 70 MB | 1 | 232 s | — | 3 | **4441** |
| | **B · parallel 4** | **109.5 s** | **17 605** | 160 MB | 22 | 22 s | — | 7 | **838** |
| **MySQL** (2.05 M) | **A · full** | **40.4 s** | **50 732** | 151 MB | 1 | — † | 40 s | 1 | — |
| | B · parallel 4 | 52.9 s | 38 740 | 406 MB | 25 | — † | 15 s | 4 | — |
| **SQL Server** (2.0 M) | A · full | **FAILS** | — | — | 0 | — | timeout 300 s | — | — |
| | **B · parallel 4** | 319.8 s | 6 253 | 421 MB | 20 | 0 † | 72 s | 5 | — |

† MySQL/MSSQL read snapshots are not multi-statement transactions, so they don't
surface in `innodb_trx` / `dm_tran_active_transactions`; the **longest single
query/request** is the snapshot-duration proxy there.

PG A_full now fully sampled: the open snapshot is held the entire 232 s and pins
the cleanup horizon **4441 xacts** back — versus B's **838** (a 5.3× difference,
and B's short chunk-snapshots also release ~10× sooner). The earlier under-sampled
floor (1994) understated the gap.

**MSSQL A_full — before vs after the retry fix (correction #1 below):**

| | before | after `statement_timeout`→non-retryable |
|---|---:|---:|
| Wall to fail | **20 m 22 s** (3 retries × ~300 s) | **5 m 20 s** (1 attempt) |
| Attempts | 3 | **1** |
| Message | `statement timeout after 300s` | **+ "split with `mode: chunked`, or raise `statement_timeout_s`"** |

The single full-scan statement of 2 M wide UTF-16 rows over tiberius cannot
complete inside the 300 s budget; that is *deterministic*, so retrying re-failed
identically. Post-fix it fails ~3.8× sooner with actionable guidance instead of
burning 20 minutes silently. Chunked (B) remains the only path that completes,
because each chunk's statement (longest 72 s) stays under the budget.

---

## Per-engine verdict

### Postgres → **parallelize. Faster *and* gentler.**
- **2.1× faster** (233 → 110 s; 8.3k → 17.5k rows/s).
- **Less harm, not more:** the open-snapshot window drops 104 → 22 s and the
  **xmin-horizon pin drops 1994 → 420 xacts (≈ 4.7×, and the A_full figure is a
  floor)**. Four short chunk-snapshots let VACUUM's cleanup horizon advance ~5×
  sooner than one long full-scan snapshot. Parallel chunking is the
  *bloat-friendly* choice, not just the fast one.
- **Cost:** 2.4× RSS (62 → 148 MB), 2× peak connections (3 → 6).
- *Why PG gains most:* its single-statement full-scan throughput is low
  (8.3k rows/s), so there's the most headroom to recover by splitting.

### MySQL → **don't. Full is already fast; parallel costs you.**
- Parallel is **1.3× slower** (40 → 54 s) and **3× the memory** (140 → 419 MB).
- One sequential full-scan already runs at **51k rows/s**; splitting into
  range-chunks across 4 connections adds buffer-pool/IO contention and per-chunk
  overhead that exceeds the gain.
- The only harm improvement is a shorter longest-query (40 → 16 s).
- **Use `mode: full`** for MySQL tables of this shape unless you specifically
  need the shorter per-statement snapshot or resumable checkpoints.

### SQL Server → **you must. Full is infeasible at scale.**
- `mode: full` **cannot finish**: the single statement exceeds the default
  300 s `statement_timeout_s`, retries 3×, and wastes 20 minutes for 0 rows.
- **Chunked/parallel is the only path that completes** (326 s, 20 files) —
  precisely because each chunk's statement (longest 72 s) stays well under the
  timeout. Tiberius wide-row throughput is the lowest of the three (6.1k rows/s),
  which is exactly why no single statement can swallow the whole table.

---

## Synthesis

| | full-scan throughput | parallel verdict | why |
|---|---|---|---|
| **Postgres** | low (8k r/s) | **win-win** | recovers wall + shrinks xmin-bloat window |
| **MySQL** | high (51k r/s) | **net loss** | sequential scan already fast; parallel = contention + RAM |
| **SQL Server** | very low (6k r/s) | **mandatory** | single statement times out; only chunks fit under the limit |

The wide-table cross-engine takeaway *looked* like "parallel is engine-dependent."
The narrow run below shows that was an over-read: the real variable is **how much
headroom the single thread leaves**, which on wide tables happens to track the
engine's wide-row throughput.

---

## Results (narrow `bench_narrow`, 10.24 M rows — 5 fixed-width cols, no text)

Same A/B, `chunk_size: 500000` (→ ~20 chunks). Constant payload (throughput/harm
is row-bound, not value-bound, on a narrow table). Sampler bumped to 8000 iters
so every run is fully covered.

| Engine | Variant | Wall | rows/s | Peak RSS | Parts | Longest open txn | Longest query/req | Peak conns | PG xmin-age |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **Postgres** | A · full | 60.2 s | 169 987 | 32 MB | 1 | 60 s | — | 3 | **1152** |
| | **B · parallel 4** | **17.7 s** | **578 204** | 73 MB | 21 | 3.2 s | — | 7 | **121** |
| **MySQL** | A · full | 55.5 s | 184 604 | 34 MB | 1 | — | 53 s | 1 | — |
| | **B · parallel 4** | **18.0 s** | **569 839** | 91 MB | 21 | — | 2 s | 5 | — |
| **SQL Server** | A · full | 59.8 s | 171 323 | 33 MB | 1 | — | 58 s | 1 | — |
| | **B · parallel 4** | **16.8 s** | **609 524** | 80 MB | 21 | — | 78 s* | 5 | — |

`*` MSSQL longest-req here exceeds wall because `total_elapsed_time` accumulates
across a worker's chunk sequence; it is a worker-lifetime figure, not a single
statement.

**Narrow findings that overturn the wide-only reading:**

1. **Parallel wins on all three engines, ~3.3–3.6×** (PG 60→18 s, MySQL 55→18 s,
   MSSQL 60→17 s). On MySQL this is the **opposite** of the wide result.
2. **MSSQL `mode: full` does *not* time out** at 10 M narrow rows (60 s) — narrow
   rows run at 171 k rows/s, so 10 M fits inside the 300 s limit. The wide failure
   was caused by 6 k rows/s wide-row throughput, not row *count*.
3. **All three engines converge** to ~170 k rows/s single / ~580 k parallel. The
   wide-table spread (51 k vs 8 k vs 6 k rows/s) was **wide-row decode/transfer
   cost, not a property of the engine.**
4. **PG xmin-pin drops 1152 → 121 (≈ 9.5×)** — fully sampled this time. Short
   chunk-snapshots let VACUUM's horizon advance ~10× sooner; the bloat-reduction
   benefit of chunking on PG is universal, not wide-specific.

---

## Corrected synthesis — the deciding variable is **single-thread headroom**, not the engine

| Scenario | Single-thread bottleneck | Parallel verdict |
|---|---|---|
| **Narrow rows, any engine** | CPU, bound by row *count* — 1 core busy, others idle | **win, ~3.3× — always** |
| **Wide rows, MySQL** | already-efficient sequential wide-row scan saturates | **net loss** — range-chunk + connection contention |
| **Wide rows, PG / MSSQL** | slow/inefficient single scan leaves headroom | **win, and on MSSQL *mandatory*** (full statement times out) |

**Principle:** parallelism helps exactly when the single thread leaves
CPU/throughput headroom, and hurts when it already saturates the efficient path.
Narrow tables always leave headroom (cheap per-row, many rows → CPU-bound on
count). Wide tables diverge by how fast the engine's single wide-row scan already
is.

**This corrects the naive "parallel = faster" *and* the first-pass "parallel is
engine-dependent."** The right signal is neither row-count nor engine alone — it
is **estimated per-row cost × row count** (i.e. is there single-thread headroom).
The planner already introspects `avg_row_bytes`; defaults should be **cost-aware**,
not row-count-only (`suggest_parallel` today keys on row count, which puts a 10 M
narrow table and a 2 M wide table in different buckets despite similar byte
volume — and gets MySQL's optimum backwards on one of them).

## Memory footprint sweep (what actually drives peak RSS)

To ground correction #5, a sweep on MySQL varying worker count, row width,
`chunk_size`, and `batch_size` (peak RSS via `/usr/bin/time -l`):

| Workers (chunk 100k/500k) | wide (~4 KB/row) | narrow (~40 B/row) |
|---|---:|---:|
| parallel 1 | 130 MB | 34 MB |
| parallel 2 | 241 MB | 54 MB |
| parallel 4 | 444 MB | 92 MB |
| parallel 8 | 652 MB | 169 MB |

Peak RSS is **linear in worker count** (sub-linear past ~4 — allocator reuse),
and the per-worker slope is **width-driven: ~105 MB/worker wide vs ~19 MB narrow.**

**`chunk_size` does *not* drive RSS** (wide, parallel 4): 50k → 379 MB, 100k →
444 MB, 500k → 403 MB — flat across a 10× range (it only sets file count: 46 → 6
parts). **`batch_size` is the real driver** (wide, parallel 4): 2k → 148 MB,
~10k (adaptive default) → 444 MB, 20k → 532 MB. The common belief that
`chunk_size` bounds memory is wrong — the in-flight **batch** does.

**Fitted model** (`init` uses it to predict and bound worker count):

```
peak_rss_mb ≈ 16 + parallel × per_worker_mb
per_worker_mb ≈ clamp(18 + avg_row_bytes × 87/4096, 18, 130)   # ~130 MB ceiling
                                                                # = 2× the 64 MB
                                                                #   adaptive batch
```

Validated: parallel 4 wide → est 436 vs measured 444 MB; parallel 8 narrow →
est 166 vs 169 MB. **Implication:** at the ≤4-worker default the peak stays
≤ ~550 MB even at extreme width, so memory is not the binding constraint under
adaptive batching — the deliverable is to *predict and surface* it (and bound it
if the ceiling is ever raised or an explicit large `batch_size` is set), not to
clamp the sane default.

### Concrete corrections this benchmark justifies

1. **✓ DONE — [bug] Don't retry a deterministic statement-timeout on a full scan.**
   `retry.rs` classed `statement_timeout` (PG 57014, the MySQL `max_execution_time`
   text, and the MSSQL client-side timeout) as `TRANSIENT_SAME_CONN`; on an
   unchunked full scan the identical query re-times-out, so retries burned
   3×300 s for 0 rows. Now classified `Permanent` (lock-wait / network timeouts
   stay transient), and the MSSQL message tells the user to use `mode: chunked`
   or raise the budget. **Validated:** MSSQL full-mode fail dropped 20 m 22 s
   (3 attempts) → 5 m 20 s (1 attempt). Regression tests in `retry.rs`.
2. **✓ DONE — [defaults] `suggest_parallel` is now cost- and engine-aware.**
   Factors introspected `avg_row_bytes`: wide rows on MySQL (the one measured
   regression) stay single-threaded; narrow rows and wide PG/MSSQL scale with the
   row estimate. Unit test in `init/yaml_scaffold.rs`.
3. **[defaults] Nudge large `mode: full` exports** — silent today; on SQL Server a
   large wide full-mode is a guaranteed timeout-failure. (Pairs with #1: warn
   *before* the run, not just fail fast during it.) *Not yet implemented.*
4. **✓ [positioning] Document chunking's PG bloat-reduction** (xmin-horizon up to
   ~10× shorter) as a first-class reason, not just speed — this report.
5. **✓ DONE — [memory] Worker count is now memory-aware.** The sweep above gives
   a validated `estimate_peak_rss_mb(parallel, avg_row_bytes)`; `init` surfaces
   the predicted peak as a YAML comment and caps the suggested worker count
   against a memory budget via the fitted per-worker model. Key finding folded
   into the docs: **`batch_size` × row width drives RSS, not `chunk_size`.** Unit
   tests in `init/yaml_scaffold.rs`. (Runtime enforcement — capping a
   hand-written `parallel` against live free memory — remains a follow-up.)
6. **[architecture] Pipelining (`PipelinedSink`) is wired only into
   `run_single_export`** — so it reaches narrow-single and wide-MySQL-single
   (already the fast paths) but **not** the chunked path that parallel uses and
   that PG/MSSQL depend on. Wiring it into the chunked runners would compound with
   the ~3.3× parallel win where it matters most. *Deferred — the encode-seam
   refactor (its own effort).*
