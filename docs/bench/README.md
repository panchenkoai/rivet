# Cross-tool benchmark

How Rivet compares to five other Postgres / MySQL → Parquet extractors
(`sling`, `dlt`, `duckdb` postgres/mysql scanner, `clickhouse-local`, `odbc2parquet`)
on the same workload, and how to reproduce the numbers yourself.

The harness here measures two things:

1. **Wall / RSS / CPU / output** per (tool, table) — every tool produces the
   same Parquet file from the same source row set, captured via `gtime -v`.
2. **DB-side signals** during the run — `pg_stat_database` /
   `pg_stat_activity` deltas on PG, `SHOW GLOBAL STATUS` /
   `information_schema.processlist` on MySQL. Reveals what each tool's wire
   pattern looks like from the database's seat (long queries, temp spill,
   concurrent backends, query templates).

## Headline (latest run)

| Axis | PG winner | MySQL winner |
|---|---|---|
| Total wall (all tables) | clickhouse, ~150 s | rivet, ~180 s |
| Total user CPU | rivet, ~30 s | rivet, ~30 s |
| Peak RSS | rivet, ~440 MB | rivet, ~580 MB |
| Longest single query (content_items) | rivet, ~0.06 s | rivet, ~4 s |
| temp_bytes / Created_tmp_disk_tables | rivet, 0 | rivet, near-baseline noise |
| Failures across all tables | 0 (rivet, sling, clickhouse) | 0 (all five) |

Rivet wins or ties on every **source-friendliness** axis on both databases.
On total wall, `clickhouse-local` is faster on PG (one big `COPY (SELECT …)
TO STDOUT` per table); on MySQL Rivet is fastest because its size-capped
row_buf decouples per-flush RAM from chunk size.

Full numbers in [`reports/`](reports/):

- [`REPORT_pg.md`](reports/REPORT_pg.md) — Postgres, 6 tools × 22 tables (defaults)
- [`REPORT_mysql.md`](reports/REPORT_mysql.md) — MySQL, 5 tools × 17 tables (defaults)
- [`REPORT_combined.md`](reports/REPORT_combined.md) — PG ↔ MySQL drift per tool
- [`REPORT_steelman.md`](reports/REPORT_steelman.md) — same suite re-run with every other tool's chunking / memory knobs tuned, to answer "do they catch up if you put effort in?"

## Reproducing

### Prereqs

- macOS / Linux with [`docker compose up -d`](../../docker-compose.yaml) running
  the project's `postgres` and `mysql` containers (creds `rivet:rivet`).
- Tools on `$PATH`: `rivet`, `sling`, `duckdb`, `gtime` (coreutils),
  `psql`, `mysql`. `odbc2parquet` + `psqlodbc` are PG-only and optional.

### Seed data

Postgres uses [`dev/bench/seed_bench_pg.sql`](../../dev/bench/seed_bench_pg.sql);
MySQL uses [`dev/bench/seed_bench_mysql.sql`](../../dev/bench/seed_bench_mysql.sql).
Both seed the same `content_items`, `page_views`, `audit_log`, `users`, etc.
shapes used in the published runs. Apply them once after the containers come up.

### One-time setup

```bash
bash docs/bench/harness/setup.sh
```

Creates a Python venv at `$BENCH_ROOT/.venv` (default `/tmp/rivet_bench`),
installs `dlt + pyarrow + pymysql + psycopg2-binary`, downloads
`clickhouse-local` into `$BENCH_ROOT/bin/`. Verifies the other tools are
already on `$PATH`.

### Run the benches

```bash
# Both databases, perf only (~5 min wall on local docker):
docs/bench/harness/run_all.sh both

# Add the DB-signal sampler over the three big tables (~20 min more):
docs/bench/harness/run_all.sh both signals

# Just one DB:
docs/bench/harness/run_all.sh pg
docs/bench/harness/run_all.sh mysql signals
```

The orchestrator writes:

- Per-cell JSON to `$BENCH_ROOT/results/`, `$BENCH_ROOT/results_mysql/`,
  `$BENCH_ROOT/db_signals/`, `$BENCH_ROOT/db_signals_mysql/`.
- Aggregated Markdown to `docs/bench/reports/` (overwrites in place).

To re-render reports without re-running benches:

```bash
$BENCH_ROOT/.venv/bin/python docs/bench/harness/aggregate.py
```

### Running a single (tool, table) cell

```bash
export BENCH_ROOT=/tmp/rivet_bench
docs/bench/harness/bench.sh rivet content_items
docs/bench/harness/bench_mysql.sh clickhouse audit_log
docs/bench/harness/db_bench.sh rivet content_items     # adds pg_stat snapshots
```

### Rivet config used

The Rivet runs use [`configs/rivet_pg.yaml`](configs/rivet_pg.yaml) and
[`configs/rivet_mysql.yaml`](configs/rivet_mysql.yaml) — the same
`table: + mode: chunked + chunk_size_memory_mb` strategy you'd write
for a production export. The harness extracts one export at a time
(via the YAML loader at the top of `bench.sh`) so each cell's `gtime`
reading is clean.

## What the JSON records

Per perf cell (`$BENCH_ROOT/results/<tool>_<table>.json`):

```json
{
  "tool": "rivet",
  "table": "content_items",
  "rc": 0,
  "wall": "2:48.7",
  "user_s": "29.1",
  "rss_kb": "440032",
  "pq_files": 25,
  "pq_bytes": 2018870706,
  "rows": 2001291
}
```

Per DB-signal cell (`$BENCH_ROOT/db_signals/<tool>_<table>.json`):

```json
{
  "pg_stat_database_delta": {
    "xact_commit": 4663, "temp_bytes": 0, "temp_files": 0,
    "blks_read": 924552, "blks_hit": 9218424
  },
  "activity_during_run": {
    "peak_active_backends": 1,
    "longest_query_seconds": 0.06,
    "distinct_query_templates": 4,
    "templates_observed": ["FETCH 50000 FROM _rive", "SELECT COUNT(*) FROM …"]
  }
}
```

## Methodology notes

- **gtime is GNU `time` from `coreutils`** (`brew install coreutils` on macOS);
  built-in `/usr/bin/time` reports a subset of fields.
- **Output size counts only `*.parquet`** — dlt's `_dlt_loads` / `_dlt_version`
  metadata files are excluded so the comparison stays apples-to-apples.
- **DB signals are *cluster-wide* counters**, not per-session. Single-tenant
  test box → numbers are clean; shared prod host → expect noise.
- **MySQL `processlist.time`** is whole-second granular. A reading of `0 s`
  means the longest probed query finished in under one sampling interval
  (50 ms), not that it was literally instantaneous.
- **The Rivet configs ANALYZE-assume tables are already analysed** — `rivet`
  reads `pg_class.reltuples` / `information_schema.TABLES.AVG_ROW_LENGTH` for
  the chunk-size derivation. On a freshly seeded DB run `ANALYZE` /
  `ANALYZE TABLE` once before the bench for stable chunk counts.
- **Tool versions used in the published numbers** (refresh with
  `<tool> --version` if reproducing later):
  rivet **0.5.3** (numbers in this report were measured on 0.5.3; v0.6.0
  carries the same Postgres extraction path with no perf-affecting changes,
  and the MySQL batch-memory `row_buf` cap shipped in 0.6.0 only reduces
  RSS further on the wide `content_items` fixture — a re-run against 0.6.0
  is planned) · sling (current `brew install slingdata-io/sling/sling`) ·
  dlt (latest pip release in the harness venv) · duckdb (current `brew`) ·
  clickhouse-local (downloaded by `setup.sh`) ·
  **odbc2parquet 11.0.0** (2026-04-15 release; refreshed
  in commit-pending-this-row from 6.0.7. Wall improved ~15% on the
  full suite — see [`reports/REPORT_pg.md`](reports/REPORT_pg.md) — but the
  architectural shape is unchanged: one `SELECT *` per table held for
  ~2 min on `content_items` at ~29 GB peak RSS).

## What's *not* in this bench (yet)

- Real production network conditions (TLS, cross-region latency, packet loss).
  Everything here is localhost / docker compose.
- Parallel exports — every cell runs serially. Rivet's `--parallel-exports`
  story is in a separate benchmark.
- Destinations other than local Parquet/Snappy. S3 / GCS would change the
  output side but not the source-side signals.
- Postgres `pg_stat_statements` per-template breakdown (extension requires a
  restart we didn't want to put on the test instance). The
  `pg_stat_activity` sampler is the closest substitute we have without that.
