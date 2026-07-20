# Source-safe under load

The first question a serious operator asks about an extraction tool is *what
does it do to my source database while it runs?* A careless `SELECT *` holds a
long-running transaction, pins a read snapshot, inflates temp space, and spikes
p99 latency for every other query on the box. Rivet is built so that the honest
answer is **"almost nothing you'll notice."**

## It holds no long-running query

A batch export streams the source in bounded pages — one chunk / one page at a
time — and flushes each to a Parquet part before asking for the next. The
longest query Rivet ever holds open on the source is a single page, not a
full-table scan. In the cross-tool benchmark (PostgreSQL → Parquet, measured
under a concurrent OLTP workload), the *longest single server-side query* each
tool held was:

| Tool           | Longest source query | Peak RSS |
|----------------|---------------------:|---------:|
| **rivet**      |            **0.00 s** | **57 MB** |
| **rivet** (chunked) |       **0.00 s** | **57 MB** |
| duckdb         |               7.7 s | 2 067 MB |
| odbc2parquet   |              40.0 s | 3 579 MB |
| clickhouse-local |            50.3 s |   820 MB |
| sling          |              94.6 s |   129 MB |

Rivet is the only tool in the field that never parks a long-running read on the
source. Everything else holds one server-side query open for the length of a
full scan — 8 to 95 seconds here, and proportionally longer on a real table.
That is the query your DBA sees in `pg_stat_activity` blocking autovacuum, or
the one a pooler's statement timeout kills at the worst moment.

## It stays gentle on concurrent traffic

Under the same concurrent OLTP workload, the p99 latency multiplier (how much
the export slows other queries) is mid-field for Rivet — **5.1×** single-stream,
**3.3×** chunked — comparable to the rest, but achieved at **57 MB of RAM and
zero long queries** instead of hundreds of megabytes to multiple gigabytes with
a scan pinned open. Rivet trades a little client-side CPU for a source that
never sees a heavy query.

If the source is fragile, production-shared, or behind a pooler (pgBouncer,
ProxySQL, MaxScale), that trade is the whole point. See
[MSSQL gentle extraction](../best-practices/mssql-gentle-extraction.md) and
[resource-aware extraction](../best-practices/resource-aware-extraction.md) for
the per-engine knobs.

## CDC is a passive reader too

Change-data-capture reads the transaction log, not your tables. Running a
looping CDC drain against a live writer, the writer's throughput barely moves —
except on SQL Server, and that cost is inherent to the engine, not to Rivet:

| Engine     | Writer throughput under CDC | Why |
|------------|----------------------------:|-----|
| PostgreSQL |                       0.98× | logical decoding is light |
| MySQL      |                       1.07× | binlog dump is a passive reader |
| MongoDB    |                       1.01× | change stream reads the oplog |
| SQL Server |                       0.78× | the capture Agent duplicates every change into change tables — a second write SQL Server itself performs |

The full per-engine numbers, harnesses, and contracts live in the
[performance & harm ledger](https://github.com/panchenkoai/rivet/blob/main/docs/perf-matrix.yaml).
The one operational hazard that *is* per-engine — a CDC reader pinning source
log retention — is covered on its own page: [CDC cost, per engine](cdc-cost-per-engine.md).
