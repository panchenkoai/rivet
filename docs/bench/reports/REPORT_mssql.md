# SQL Server benchmark — DBA-harm signals

> **Scope.** This report covers the **DBA-harm** matrix for SQL Server (how
> gently rivet treats the source). The **competitive performance** matrix
> (rivet vs sling / dlt / duckdb / clickhouse / odbc2parquet, à la
> [`REPORT_pg.md`](REPORT_pg.md) / [`REPORT_mysql.md`](REPORT_mysql.md)) is
> deferred — it needs each external tool wired to extract from SQL Server fairly.

Measured against live **SQL Server 2022** (`docker-compose.yaml` `mssql`
service) with the harness
[`docs/bench/harness/mssql_db_bench.sh`](../harness/mssql_db_bench.sh) driving a
chunked rivet export of `bench_narrow` (500 000 rows, `BIGINT` PK,
`chunk_size: 50000` → 10 chunks; seed:
[`dev/bench/seed_bench_mssql.sql`](../../../dev/bench/seed_bench_mssql.sql)).
DMV sampler @ 50 ms.

## Why the PG harm signals don't port directly

The PG harm story ([`REPORT_pg.md`](REPORT_pg.md)) is an **MVCC** story: a long
read snapshot pins `backend_xmin`, so `VACUUM` cannot reclaim dead tuples behind
it. SQL Server has **no MVCC version horizon under READ COMMITTED** — the harm
vector is instead **lock contention** (shared locks blocking writers) and
**log** pressure. So the SQL Server matrix measures those directly, plus the
universal "is the extractor holding a long transaction / pinning the log" check.

## DBA-harm signals — `bench_narrow` (500 k rows, chunked)

| Signal | rivet | What it means |
|---|---:|---|
| Log Flush Waits delta (rivet DB) | **0** | rivet issues only `SELECT`s — **read-only**, zero write pressure on the source log |
| `log_reuse_wait_desc` after export | **NOTHING** | rivet pins **nothing** back from log truncation |
| Longest open transaction | **0 ms** | each chunk is an **autocommit** `SELECT` (no `BEGIN TRAN`) — no long snapshot held |
| Longest single request | **~17–31 ms** | per-chunk `OFFSET/FETCH`-free range scan — short, like PG's per-`FETCH` shape |
| Peak user locks held | **3–4** | tiny footprint; shared (S) locks for one chunk's scan, released between chunks |
| Peak concurrent rivet sessions | **1** | chunks run sequentially by default (raise with `parallel:`) |

## Reading it

rivet is a **gentle** SQL Server citizen on the dimensions that matter to a DBA:

- **No long-held transaction.** Unlike a single big `SELECT *` (which would hold
  one request open for the whole run), rivet's chunked autocommit `SELECT`s each
  open and close in tens of milliseconds — `longest open txn = 0 ms`,
  `log_reuse_wait_desc = NOTHING`. It never becomes the oldest-active-transaction
  that blocks log truncation or (with snapshot isolation on) inflates the
  version store.
- **Read-only.** `Log Flush Waits` delta is **0**: rivet writes nothing to the
  source, so it adds no log-flush / checkpoint pressure. (The same
  `Log Flush Waits` counter is what the OPT-2 back-pressure governor samples to
  *throttle* when a co-tenant workload is stressing the log — see
  [reference/tuning.md](../../reference/tuning.md).)
- **Small lock footprint.** Peak 3–4 locks: a chunk's range scan takes shared
  locks only for its own duration and releases them before the next chunk.

### Honest caveat — isolation level

rivet reads under SQL Server's **default READ COMMITTED**: it does **not**
downgrade to `READ UNCOMMITTED` / `WITH (NOLOCK)` nor opt into snapshot
isolation. On a quiet table that's the 3-lock footprint above; under heavy
concurrent OLTP **writes** to the same rows, the per-chunk shared locks can
briefly contend with writers (and vice-versa). If lock-light reads matter more
than read-consistency for your source, enable Read Committed Snapshot Isolation
(RCSI) on the database. Tracking lock-light read options for the MSSQL engine is
roadmap, not shipped.

## Method notes

- Signals are peaks over a 50 ms DMV sampler window (`sys.dm_exec_requests`,
  `sys.dm_tran_locks`, `sys.dm_tran_active_transactions`), excluding the
  sampler's own session and system sessions, scoped to the `rivet` database.
- Write pressure is measured in a **separate, sampler-free run** so the sampler's
  own bookkeeping `INSERT`s don't pollute the read-only Log Flush Waits number.
- `log_reuse_wait_desc` is read after a `CHECKPOINT` so it reflects the current
  blocker, not a stale one.
