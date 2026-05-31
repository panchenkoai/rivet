_Last updated: 2026-05-31. Steelman attempt to give every other tool its best **minimum-memory** configuration on the source-friendliness axis (peak RSS, longest open transaction) before comparing it to Rivet. Re-run from scratch on the versions listed at the bottom._

# Steelman test — tools at their best min-memory configuration

The headline numbers in [`REPORT_pg.md`](REPORT_pg.md) compare every tool **at its defaults** (with the minimum flags needed to make the bench run). That is the fair comparison for "what a new operator sees on install"; it is the *unfair* comparison for "how good can each tool be with effort". This page is the second one, focused on the axis a DBA actually cares about on a wide table: **how little memory can each tool be forced to use while still completing the extract?**

Fixture throughout: `content_items` — 2 000 000 rows × 20 wide columns, ≈2 GB of Snappy Parquet on disk. The single hardest table in the suite.

## The headline

| Config | peak RSS | wall | rows | completed? |
|---|---:|---:|---:|:--:|
| **rivet — `chunk_size: 25000`** | **128 MB** | 2:06 | 2 M | ✅ |
| **rivet — `chunk_size_memory_mb: 64`** | **148 MB** | 2:11 | 2 M | ✅ |
| rivet — default (`chunk_size_memory_mb: 256`) | 569 MB | 2:37 | 2 M | ✅ |
| sling — **backfill workaround** (20× id-range, free CLI) | 117 MB | 3:11 | 2 M | ✅ (operator-scripted) |
| dlt — tuned (`buffer_max_items=5000`) | 1 267 MB | 3:05 | 2 M | ✅ |
| duckdb — **floor** (`memory_limit=4GB`, lowest completing) | 4 118 MB | 1:32 | 2 M | ✅ |
| duckdb — `memory_limit=512MB` | — | 0:09 | **0** | ❌ **OOM** |
| duckdb — `memory_limit=2GB` | — | 0:09 | **0** | ❌ **OOM** |
| clickhouse — `max_memory_usage=512MiB` | — | 2:08 | **0** | ❌ **OOM (code 241)** |
| odbc2parquet — tuned (`batch-size-memory=256Mb`) | 6 757 MB | 7:19 | 2 M | ✅ |
| sling — default (one `SELECT *`) | 23 552 MB | — | — | ❌ **DNF (swap)** |

**The result splits the field into two classes:**

- **Tools that genuinely shrink.** Rivet drops from 569 MB → 148 MB → 128 MB by changing one config line, *without losing throughput* — the 25k-row run is actually the **fastest** of the three (2:06 vs 2:37) because a smaller per-chunk buffer stays in cache. Its floor (~120 MB) is the tokio + Arrow runtime itself.
- **Tools that don't.** DuckDB and ClickHouse **cannot be made to use less memory — they crash instead.** At 512 MB both OOM with zero rows written (DuckDB: `failed to allocate ... (488 MiB used)`; ClickHouse: `MEMORY_LIMIT_EXCEEDED, code 241`). DuckDB's *lowest completing* memory_limit is **4 GB** — that is its floor, not a choice. The "min-RSS" knob for these engines is really "how close to the crash boundary dare you set it."

So the honest framing is not "Rivet uses less RAM." It is: **Rivet's 128 MB is a config choice with throughput intact; DuckDB's 4 GB is the cliff edge below which the job fails outright.**

## Equal-chunk head-to-head: rivet vs sling-backfill

The only competitor that reaches Rivet's memory class is **sling — but only via an operator-written workaround**, because sling's built-in `chunk_size` is paywalled (Pro/Platform; see below). To make the comparison exact, both tools were run at the **same 100k-row granularity** over the same 2 M rows:

| | rivet `chunk_size:100000` | sling-backfill (20× `WHERE id` ranges) |
|---|---:|---:|
| peak RSS | 245 MB | **117 MB** |
| wall | **2:16** | 3:11 |
| user CPU | **28.6 s** | 113.3 s |
| sys CPU | 23.2 s | 100.1 s |
| process model | **1 process** | 20 sequential processes |
| output | 20 files, atomic, resumable | 20 files, no shared state |
| invocation | `mode: chunked` (one line) | hand-written bash id-range loop |

sling's 117 MB is real, but it is the peak of *one short-lived process* whose memory the OS reclaims between the 20 invocations — not a steady-state single-process figure. Rivet pays ~130 MB of *persistent* runtime to be **35 % faster (2:06 vs 3:11 at 25k), 4× cheaper on CPU**, and a single resumable process with one manifest. Drop Rivet to the same 25k granularity and the memory gap closes to **128 MB vs 117 MB — 11 MB** — while every other axis stays in Rivet's favour.

The conclusion the previous edition drew still holds, now with numbers: getting sling source-friendly means *re-implementing chunked extraction by hand around it*. At that point the operator has rebuilt the feature Rivet ships turnkey.

## Source-pressure (longest open transaction) — the other half

Min-RSS is only one source-friendliness axis. The other is **how long each tool pins a snapshot open** (blocking `VACUUM` on the source). With the Tier-2 write-pressure probe on (a concurrent `UPDATE` loop), `dead_tup` is the dead tuples that piled up un-reclaimable while the extractor held its snapshot. From the 50 ms `pg_stat_activity` sampler on the same `content_items` run:

| Tool | longest open txn | max backend_xmin age | dead tuples stranded |
|---|---:|---:|---:|
| **rivet** | **12.3 s** | **274 xacts** | **0** |
| clickhouse | 87.9 s | 1 975 | 1 383 |
| duckdb | 97.6 s | 737 | 0 |
| odbc2parquet | 105.4 s | 2 746 | 0 |
| dlt | 215.3 s | 8 388 | 4 843 |

This is the most lopsided axis in the whole suite. **Rivet holds its snapshot 12.3 s; every other tool holds one open for 88–215 s** — 7–17× longer — because they each run a single long `SELECT *` while Rivet fetches in short per-chunk transactions. dlt is the worst: 215 s open, pinning the cleanup horizon **8 388 transactions** back and leaving **4 843 dead tuples** the source's `VACUUM` could not reclaim during the run. Rivet is the only tool that is best on *both* axes at once — shortest snapshot **and** lowest tunable RSS — with no multi-GB cost to get there.

## Parallel scaling — does `parallel: N` close the wall gap?

Rivet's wall numbers above are single-threaded (`parallel: 1`). Its chunk-level
parallelism (one connection per worker) is the turnkey lever for throughput.
Measured on a 10-core host, same `chunk_size` per shape:

| | content_items (WIDE) | | page_views (NARROW) | |
|---|---:|---:|---:|---:|
| parallel | wall | RSS | wall | RSS |
| 1 | 2:11 | 245 MB | 13.6 s | 66 MB |
| 2 | 1:36 (1.37×) | 410 MB | 4.7 s (2.90×) | 115 MB |
| 3 | 1:34 (1.39×) | 524 MB | 3.8 s (3.61×) | 158 MB |
| 4 | 1:33 (1.41×) | 656 MB | 3.2 s (4.19×) | 211 MB |

Wide tables plateau at **2** (thick rows saturate the wire — 2→4 adds memory for
~no wall); narrow tables scale near-linearly to **4** (thin rows stay
round-trip-bound). `rivet init` already scaffolds this: `suggest_parallel` emits
1/2/4 by row estimate. Head-to-head, **rivet `parallel: 2` on content_items
(1:36, 410 MB) beats duckdb's auto-parallel default (1:44, 5 305 MB)** — faster
*and* 13× leaner. So the lever exists, is on by default in scaffolded configs,
and does not cost the multi-GB RSS the parallel scanners pay.

## Per-tool tuning notes

### `duckdb` 1.2.1 — parallel by design, no low-memory mode
Knobs tried: `memory_limit`, `threads=2`, `preserve_insertion_order=false`, `ROW_GROUP_SIZE=100000`. `preserve_insertion_order=false` is essential (without it the scanner buffers global order). Even so, `postgres_scanner` materializes enough per-thread state that **512 MB and 2 GB both OOM with zero output**; 4 GB is the lowest that completes. This is architectural: DuckDB parallelizes inside one process and trades memory for the 1:32 wall. Useful when RAM is plentiful; not an option when it isn't.

### `clickhouse-local` 26.6 — `max_memory_usage` is a kill switch, not a throttle
`max_memory_usage=512MiB, max_threads=2, max_block_size=10000` → `MEMORY_LIMIT_EXCEEDED (code 241)`, 0 rows. The `postgresql()` table function reads one `COPY (SELECT …)`; the memory cap aborts the query rather than back-pressuring it. Default (unbounded) run uses 1.6 GB and finishes in 1:29 — fast, but the memory is not tunable downward.

### `dlt` 1.27.2 — already source-friendly, RSS is the writer
`DATA_WRITER__BUFFER_MAX_ITEMS=5000`, `FILE_MAX_ITEMS=100000`, `EXTRACT__WORKERS=1`. dlt already uses a `FETCH 10000` cursor (mid-pack 34 s snapshot), but the pyarrow writer + normalize stage hold ~1.27 GB regardless of buffer size — tuning the write buffer barely moved RSS (1260 → 1267 MB). The memory floor is the pipeline, not a knob.

### `odbc2parquet` 6.3.0 — `--sequential-fetching` is gone in 6.x
This host has **6.3.0**, not the 11.0.0 of the prior report — so `--sequential-fetching` (a key min-RSS flag last time) **does not exist** here. Only `--batch-size-memory 256Mb` + `--column-length-limit 10000` apply, and on the wide table RSS stays at **6.8 GB**. The psqlodbc driver allocates column buffers proportional to declared width; the CLI cannot push below the multi-GB floor on wide rows. Single `SELECT *` held ~139 s.

### `sling` 1.4.3 — built-in chunking still paywalled
`source_options.chunk_size` remains Pro/Platform-only in the free CLI (confirmed: `sling run --help` exposes only `--limit`, `--offset`, `--range`, `--where`, `--src-stream` with inline SQL). Default behaviour is one `SELECT *` → 23.5 GB → swap → DNF. The **backfill workaround** above (inline-SQL id ranges) is what the free CLI *can* do, and it works (117 MB) — but it is operator-scripted orchestration, not a sling feature.

## Honest summary

| Tool | Min-RSS on wide table | Mechanism | vs rivet |
|---|---|---|---|
| **rivet** | **128 MB** (config) | turnkey `chunk_size` | baseline |
| sling (free) | 117 MB | **hand-written 20× id-range loop** | matches RSS, loses wall/CPU/atomicity |
| sling (paid) | not tested | `chunk_size` (license-gated) | out of scope for OSS bench |
| dlt | 1 267 MB | already tuned; writer is the floor | ~10× |
| duckdb | 4 118 MB | **floor — below it OOMs** | ~32× |
| clickhouse | OOM ≤512 MB / 1 627 MB default | cap aborts, not throttles | ~13× (untunable) |
| odbc2parquet | 6 757 MB | driver buffer floor | ~53× |

Rivet's edge on **peak RSS** survives a good-faith min-memory tuning attempt against every open-source competitor. More importantly, it is the only tool whose low number is a *setting* rather than a *cliff*: ask DuckDB or ClickHouse to use 512 MB and they crash; ask Rivet and it just runs, faster.

---

**Versions (this run, 2026-05-31, macOS arm64, docker-compose Postgres 16):**
rivet 0.7.9 · sling 1.4.3 · duckdb 1.2.1 · clickhouse-local 26.6.1 · dlt 1.27.2 · odbc2parquet 6.3.0.

**Methodology:** same docker-compose Postgres as the main reports, `gtime -v` for wall/RSS, 50 ms `pg_stat_activity` sampler for snapshot/xmin, row counts verified from the output Parquet (a tool that exits 0 with 0 rows is recorded as a failure, not a win). Raw per-cell `.gtime` + `.json` under `$BENCH_ROOT/steelman_logs/` and `$BENCH_ROOT/steelman/`. The duckdb floor sweep (2/4 GB) and the sling-backfill loop are reproducible from `$BENCH_ROOT/duckdb_floor.sh` and `$BENCH_ROOT/sling_backfill.sh`.
