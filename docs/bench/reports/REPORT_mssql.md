# SQL Server benchmark — DBA-harm signals

> **Scope.** Two matrices for SQL Server, both measured against live SQL Server
> 2022, **rivet vs sling vs dlt**: a **DBA-harm** matrix (how gently each tool
> treats the source — longest query, longest open transaction, locks, log
> pressure) and a **competitive performance** matrix (wall / RSS). The competitor
> set is narrower than [`REPORT_pg.md`](REPORT_pg.md) / [`REPORT_mysql.md`](REPORT_mysql.md)
> because DuckDB / ClickHouse have no SQL Server connector and odbc2parquet needs
> the MS ODBC Driver 18.

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

## Per-tool DBA-harm comparison — `content_items` (2 M rows)

The signals above are rivet's own footprint. Wrapping each extractor in the same
DMV sampler (harness:
[`mssql_harm_compare.sh`](../harness/mssql_harm_compare.sh)) shows how rivet's
chunked reads compare to tools that scan the whole table in one shot — the SQL
Server analogue of the PG "DB-side signals" table in
[`REPORT_pg.md`](REPORT_pg.md):

| Tool | Longest single request | Longest open txn | Peak locks | Sessions |
|---|---:|---:|---:|---:|
| **rivet** | **6.6 s** | **0 s** | 22 | 1 |
| sling | 539 s | 0 s | 7 | 1 |
| dlt | 490 s | **490 s** | 6 | 1 |

- **Longest single request** — rivet's chunked `SELECT`s top out at **6.6 s** per
  chunk; sling and dlt each run **one ~8–9 minute query** over the whole 2 M-row
  table. The source's long-running-query monitors never see rivet for longer than
  a chunk; they see the others for the entire export.
- **Longest open transaction** — rivet (autocommit chunks) and sling (autocommit
  streaming) hold **none**; **dlt holds a single transaction open for ~8 minutes**
  (a server-side cursor inside a transaction). That is the classic DBA pager
  event: an 8-minute transaction pins the version store and blocks log truncation
  for its whole duration. **rivet is the only tool here with both a short query
  and no long transaction.**
- Lock counts are all small (6–22) and all three are read-only
  (`Log Flush Waits` ≈ 0). rivet's chunked range scans hold a few more shared
  locks at once but release them per chunk.

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

---

# SQL Server benchmark — competitive performance

Tool set is **rivet · sling · dlt**. DuckDB has no SQL Server scanner and
ClickHouse has no `mssql()` table function (both drop out); odbc2parquet needs
the MS ODBC Driver 18, not installed on the bench host. Harness:
[`bench_mssql.sh`](../harness/bench_mssql.sh) (`gtime -v` for wall/RSS),
seed [`seed_bench_mssql.sql`](../../../dev/bench/seed_bench_mssql.sql), against
live SQL Server 2022. rivet config: `mode: chunked`, `chunk_size_memory_mb: 256`
— but rivet now **streams** (one Arrow batch per `batch_size` rows), so RSS is
bounded by `batch_size`, not the chunk size, on every table.

## Per-table — wall (s) / peak RSS (MB), streaming rivet

| Table (rows) | rivet | sling | dlt |
|---|---|---|---|
| bench_narrow (500 k) | **0.56 / 38** | 4.30 / 94 | 7.73 / 147 |
| bench_hc (200 k) | **0.74 / 49** | 2.39 / 94 | 4.89 / 157 |
| bench_decimal (200 k) | **0.42 / 41** | 2.50 / 95 | 4.78 / 163 |
| bench_sparse (10 k) | **0.21 / 21** | 0.99 / 84 | 1.74 / 138 |
| users (500) | **0.20 / 15** | 1.09 / 79 | 1.66 / 132 |
| orders (2.5 k) | **0.20 / 17** | 0.93 / 83 | 1.69 / 133 |
| events (5 k) | **0.21 / 18** | 0.95 / 86 | 1.69 / 134 |
| page_views (5 k) | **0.20 / 32** | 1.02 / 93 | 1.78 / 147 |
| bench_wide (100 k, 10×200 B) | 7.03 / **76** | **3.58** / 98 | 7.79 / 264 |
| content_items (60 k, heavy) | 10.30 / 155 | **4.04 / 106** | 9.88 / 455 |

(Previous, pre-streaming rivet RSS for reference: bench_narrow 209 → 38 MB,
bench_wide 340 → 76 MB, content_items 475 → 155 MB.)

## Reading it

- **Throughput / narrow-to-medium rows — rivet wins outright.** Sub-second on
  every table up to 500 k narrow rows (3–30× faster than sling/dlt) **and the
  lowest RSS** — streaming holds it at 15–49 MB, well under sling's ~90 MB.
- **Memory — rivet is now lowest or competitive everywhere**, including the wide
  tables (bench_wide 76 MB < sling 98 MB). The pre-streaming multi-GB wide-row
  RSS is gone.
- **Wide / heavy-text rows — rivet still trails on *wall*** (bench_wide 7.0 s vs
  sling 3.6 s; content_items 10.3 s vs sling 4.0 s). The remaining gap is the
  `tiberius` decode of wide rows, **not** memory. (Decode-path speedups are
  roadmap; sling/dlt's native streaming readers are faster on these shapes.)

## Big-table memory — measured on 2 M content_items

Before streaming, the only way to bound RSS on a big heavy table was a tiny
`chunk_size` → hundreds of tiny files, or `chunk_size_memory_mb` buffered
~500 k-row chunks → multi-GB. That was the per-chunk
**buffering** limitation. The engine now **streams** (emits one Arrow batch per
`batch_size` rows), so RSS is bounded by `batch_size`, not `chunk_size`.

Re-running the heavy table at **2 000 000 rows**:

| config | wall | peak RSS | files |
|---|---:|---:|---:|
| `chunk_size_memory_mb: 256` (pre-streaming) | 8m08s | 2 759 MB | 4 |
| `chunk_size: 5000` (pre-streaming) | 8m15s | 101 MB | 400 |
| **`mode: full` (streamed)** | 8m03s | **171 MB** | **1** |

**One file *and* ~170 MB** — before streaming, `mode: full` would have
materialised ~10 GB and OOM'd. `chunk_size` now controls only the file layout;
`batch_size` is the memory lever (see
[best-practices/mssql-gentle-extraction.md](../../best-practices/mssql-gentle-extraction.md)).
So the matrix's wide-row RSS numbers above are a pre-streaming artifact, not a
floor. (Row-byte introspection so `chunk_size_memory_mb` sizes by bytes is still
roadmap, but lower priority now that streaming bounds memory regardless.)
