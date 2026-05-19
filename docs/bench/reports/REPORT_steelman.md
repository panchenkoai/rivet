_Last updated: 2026-05-19. Steelman attempt to give the other tools their best configuration on the source-friendliness axes (longest single query, peak RSS) before comparing them to Rivet's defaults._

# Steelman test — tools at their best configuration

The headline numbers in [`REPORT_pg.md`](REPORT_pg.md) and [`REPORT_combined.md`](REPORT_combined.md) compare every tool **at its defaults** (with the minimum flags needed to make the bench actually work). That is the fair comparison for "what will a new operator see when they just install the tool"; it is the *unfair* comparison for "how good can each tool be with effort". This page is the second one.

For each non-Rivet tool we asked: does its public CLI / config surface contain knobs that, if tuned, would close the source-pressure gap to Rivet on the `content_items` fixture (2 M rows × 20 wide cols, ≈1 GB of Parquet on disk)? If yes, we ran the tool with those knobs and recorded the numbers below. If no, we say so explicitly so the result is honest in both directions.

## `odbc2parquet` 11.0.0 — tuned

Available knobs that look promising:

| Flag | Default | Tuned value | What it controls |
|---|---|---|---|
| `--batch-size-memory` | 2 GiB | **256 MiB** | Buffer budget per ODBC fetch batch. Matches Rivet's `chunk_size_memory_mb: 256`. |
| `--sequential-fetching` | off (double-buffer) | **on** | Allocate one fetch buffer instead of two — halves the steady-state buffer footprint. |
| `--column-length-limit` | 4096 | **10000** for `content_items`, 4096 elsewhere | Per-column max-element bytes. Lowered to the minimum the actual data allows so each column buffer is sized realistically rather than over-allocated. |

Result (same 50 ms `pg_stat_activity` sampler as the main report; macOS arm64 host):

| Table | Default (v11.0.0) | Tuned | Δ wall | Δ RSS | Rivet (same fixture) |
|---|---|---|---:|---:|---|
| `bench_narrow` (500 K rows, narrow) | 0.52s · 134 MB | **0.59s · 327 MB** | +13% | +144% | 0.07s · 25 MB |
| `page_views` (2 M rows, narrow) | 9.65s · 6.0 GB | **11.5s · 2.2 GB** | +19% | **-63%** | 9.0s · 440 MB |
| `content_items` (2 M × 20 wide) | 176.2s · 29.1 GB | **187.4s · 25.4 GB** | +6% | -13% | 176.1s · 443 MB |

Conclusion for odbc2parquet:

- On narrow-row tables (`page_views`), tuning **does** close the RSS gap meaningfully — 6 GB drops to 2.2 GB, a 63% reduction. Wall regresses slightly because `--sequential-fetching` trades pipelining for memory.
- On the wide-column table (`content_items`), tuning is essentially **ineffective on RSS**. The 26-29 GB floor is set by something the CLI cannot control — most plausibly the libodbc / psqlodbc driver allocating column buffers proportional to declared column max-width, plus Parquet writer dictionary pages held until row-group close. With `--row-groups-per-file 1` and `--file-size-threshold 256MiB` (forcing 671 separate output files), RSS still measures 26.6 GB.
- The longest single SQL statement is unchanged from default — still one `SELECT * FROM "content_items"` held against the server for ~136 s. odbc2parquet has no built-in chunked-extraction mode, so genuinely reducing the long-query duration requires scripting `WHERE id BETWEEN x AND y` partitions externally. At that point the operator has re-implemented half of Rivet by hand.

So the irreducible gap on this workload is **~58× peak RSS** (25.4 GB tuned vs 443 MB Rivet) and **~700× longest single query** (136 s vs 0.19 s).

## `sling` 1.2.18 — chunking is paywalled

Sling's [`source_options.chunk_size`](https://docs.slingdata.io/concepts/replication/source-options) is documented as **available with the Sling CLI Pro or Platform plans** — not the open-source CLI we used in the bench. Without it, sling's behaviour on `content_items` is what we already published: one `select * from "public"."content_items"` held for ~134 s, ~6 GB peak RSS.

We could compare against the paid tier in principle, but a benchmark whose result depends on a license file does not belong in an open-source README. If sling's chunking is genuinely good once enabled, that is a separate exercise the sling team is better placed to run than we are.

## `dlt` — already tuned for source-friendliness

dlt's default `pg_replication` / SQL source extractor already uses a server-side `FETCH FORWARD 10000` cursor (longest single query observed: **1.20 s** on content_items). It is the only other tool in the suite whose default extraction shape is comparable to Rivet's. The trade-off is downstream: dlt's pipeline materializes intermediate state on disk — 200 temp files / 3.2 GB temp_bytes per content_items run — that the more direct tools avoid. Its peak RSS (1.4 GB on PG) is mid-pack.

There is no obvious additional CLI tuning that would meaningfully change those numbers on this fixture.

## `duckdb` (`postgres_scanner`) — already parallel

DuckDB's `postgres_scanner` already opens **12 concurrent backends** on `content_items` and finishes the slowest single query in **70.6 s** — that *is* its best-case configuration. The mode is not a knob the user picks; it is the default when `postgres_scanner` decides the table is big enough to benefit. The remaining gap (vs Rivet's 0.19 s single-query) is just that the strategies are different: DuckDB parallelizes inside one client process; Rivet sequentializes thin cursor reads against one backend.

The two `pg_pages_per_task` / `parallel` settings would lower the backend count at the cost of wall time. Useful in production if 12 concurrent backends are too many for the source, but it doesn't change the architectural class.

## `clickhouse-local` — would need external partitioning

ClickHouse's `postgresql(...)` table function reads in **one** `COPY (SELECT ...) TO STDOUT` per call. To get chunked behaviour you would have to:

1. Pre-compute id-range partitions on the client.
2. Issue N separate `clickhouse-local --query "INSERT INTO outfile ... FROM postgresql(...) WHERE id BETWEEN $start AND $end"` invocations.
3. Stitch the output together yourself.

This is implementable in ~30 lines of bash, but it's no longer "what `clickhouse-local` does on this query" — it's "what a custom orchestration around `clickhouse-local` does". We chose not to include that in the bench: the comparison stops being "tool X vs tool Y" and becomes "tool X vs script-with-X-as-a-primitive". If a user is willing to write that script, they are also willing to use Rivet directly.

## Honest summary

| Tool | Has built-in chunked extraction? | Tunable RSS on wide-table case? | Tunable longest-query? |
|---|---|---|---|
| Rivet | yes (turnkey, PK auto-resolved) | n/a (already ~443 MB) | n/a (already 0.19 s) |
| odbc2parquet | no (single `query` arg) | partial (works on narrow tables, ~60% reduction; on wide tables stuck at ~26 GB floor) | no |
| sling (free) | no | n/a | n/a |
| sling (paid) | yes (`chunk_size`) | not tested — paywalled | not tested — paywalled |
| dlt | yes (already on, FETCH 10000) | already mid-pack; intermediate state is the limit | already 1.2 s |
| duckdb | yes (parallel scanner, on by default for big tables) | not the bottleneck (1.6 GB on PG) | 70 s — by design (parallel + REPEATABLE READ snapshot) |
| clickhouse-local | no (one COPY per call) | not exposed | only via external scripting |

Rivet's edge on **`peak RSS`** and **`longest single query`** survives a good-faith tuning attempt against every open-source competitor in the suite. The edge on `peak RSS` shrinks substantially on narrow-row tables once odbc2parquet is tuned — but the wide-row case (which is the one a DBA actually worries about) does not shift.

Methodology is unchanged from the main reports: same docker-compose Postgres, same `gtime -v` capture, same 50 ms `pg_stat_activity` sampler. Raw output for the tuned cells lives under `$BENCH_ROOT/logs/odbc2parquet_tuned*.gtime` for reproducibility.
