# Cross-tool, cross-engine benchmark

How rivet compares to seven other extraction tools — `duckdb`, `clickhouse-local`,
`sling`, `ingestr`, `dlt`, `odbc2parquet` — reading **Postgres / MySQL / SQL Server
/ MongoDB → Parquet** on the same fixture, and how to reproduce it.

**One source of truth, one runner, one report:**

| File | Role |
|---|---|
| [`matrix.yaml`](matrix.yaml) | the SINGLE source — metric catalog, tool set + versions + steelman configs, seed sizes, per-engine harm metrics |
| [`../../dev/bench/smoke.py`](../../dev/bench/smoke.py) | the runner — reads `matrix.yaml`, runs each tool, prints three matrices; **guards against metric drift** |
| [`report.html`](report.html) | the rendered headline report (open in a browser) |

## What it measures — three matrices, one run per tool

1. **Benchmark** — wall, rows/s, peak RSS, output MB, output files, type-drift count.
2. **Harm to the source** — the point of the bench. Universal axes (a co-running
   OLTP p99 probe, longest query / txn, held locks, connections, cache footprint,
   source CPU) **plus each engine's native counters** (pg `pg_stat_database`,
   mysql `SHOW GLOBAL STATUS` + `data_locks`, mssql DMVs, mongo `serverStatus`).
3. **Type fidelity** — every source column vs each tool's Parquet type family
   (jsonb→text, naive-ts→timestamptz, bool→int drift). N/A for MongoDB (JSON-blob).

## Headline

rivet wins **peak memory** (15–60× lower — it streams via a server-side cursor,
never buffering the result set) and **type fidelity** (0 drift on every engine,
while also checksumming every value). It is **not** the throughput leader —
ingestr and clickhouse beat its rows/s, and the report says so. Full numbers and
the honest tradeoffs are in [`report.html`](report.html).

Engine coverage: Postgres (8 tools) · MySQL (7) · SQL Server (6 — duckdb/clickhouse
have no native reader) · MongoDB (3 — rivet/sling/ingestr only).

## Reproducing

**Prereqs** — `docker compose up -d` (the project's postgres/mysql/mssql/mongo
containers), then run with the system python (it has PyYAML + dlt; the homebrew
pythons ship a broken pyexpat):

```bash
# postgres, one table, all tools:
/usr/bin/python3 dev/bench/smoke.py --engine postgres --table content_items
# another engine / table:
/usr/bin/python3 dev/bench/smoke.py --engine mysql --table content_items
/usr/bin/python3 dev/bench/smoke.py --engine mssql --table orders
/usr/bin/python3 dev/bench/smoke.py --engine mongo --table content_items --tools rivet,ingestr,sling
```

**Fixtures** are seeded into a dedicated `rivet_bench` database **per engine** so
the live-test `rivet` fixtures are never touched. Seed via the Rust tool
(`cargo run --release --bin seed --features dev-seed -- --target <engine> \
--mssql-url/--mysql-url ...`); sizes live in `matrix.yaml`'s `seed:` block. MongoDB
is seeded via the mongo driver.

**Per-tool drivers** (documented in each tool's `matrix.yaml` note): odbc2parquet
needs a vendor ODBC driver per engine (psqlodbc / MariaDB Unicode / msodbcsql18);
dlt needs psycopg2 / pymysql / pyodbc; clickhouse's `mysql()` needs `127.0.0.1`
(not `localhost`); the rivet MongoDB source needs the worktree build (`RIVET_BIN`
prefers `target/release/rivet`).

## Notes on fidelity of the numbers

- **The OLTP p99 probe is cleanest on Postgres** (server-side `\timing`). On
  mysql/mssql it is a per-query `docker exec`, whose ~230 ms overhead compresses
  the signal toward 1×; on MongoDB it is skipped (mongosh cold-start). Read the
  harm story primarily from longq / memory / native counters on non-PG engines.
- **Steelman applied to everyone** — each tool runs its lowest-memory config that
  still completes; memory caps *flatter* competitors (lower RSS), and a self-audit
  removed a stray clickhouse thread cap. rivet's numbers include its always-on
  per-value checksum (~7%) that no competitor performs.
- **SQL Server heavy NVARCHAR(MAX)** is ~130× slower to stream over TDS for **all**
  tools (a protocol characteristic, not rivet) — the mssql matrix runs on `orders`.

MongoDB CDC and a folded-in CDC-churn dimension (`cdc_churn.sql`) are tracked for
a future revision.
