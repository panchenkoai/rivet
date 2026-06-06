# Gentle SQL Server extraction — easy on the database *and* the worker

Extracting from SQL Server has **two** things to be gentle to, and they pull on
different knobs:

1. **The source database** — don't hold long transactions, don't block writers,
   don't add write pressure.
2. **The rivet worker** — don't let rivet's own RAM blow up on a wide/large table.

rivet is gentle to the source almost for free, but the worker side needs **one
deliberate setting** on SQL Server. This page is the why; the copy-paste config
is [`rivet_mssql_gentle.yaml`](rivet_mssql_gentle.yaml).

## TL;DR

```yaml
exports:
  - name: big_table
    table: big_table
    mode: chunked
    chunk_column: id          # range-chunk on the PK (or chunk_by_key for UUID/string PKs)
    chunk_size: 50000         # ROW COUNT — bounds rivet's RAM. NOT chunk_size_memory_mb.
    parallel: 1               # sequential = gentlest to the source
    chunk_checkpoint: true    # resumable
source:
  environment: production     # governor throttles concurrency on source write pressure
```

The one rule that matters: **on SQL Server, set `chunk_size` (rows) explicitly;
do not use `chunk_size_memory_mb`.** Everything else is the usual chunked export.

## Gentle to the source — what rivet does, and the lever you have

Measured against live SQL Server 2022 (see
[`REPORT_mssql.md`](../bench/reports/REPORT_mssql.md), harness
[`mssql_db_bench.sh`](../bench/harness/mssql_db_bench.sh)), a **properly chunked**
rivet export is a quiet tenant:

| Signal | rivet (chunked) | Why |
|---|---:|---|
| Longest open transaction | **0 ms** | each chunk is an autocommit `SELECT`, no `BEGIN TRAN` |
| Log Flush Waits delta | **0** | rivet only reads — zero write pressure |
| `log_reuse_wait_desc` | **NOTHING** | rivet pins nothing back from log truncation |
| Peak lock count | **3–4** | shared locks released as each chunk scans (READ COMMITTED) |

The lever: **`environment: production`** (or `replica`). It selects the
*Balanced* tuning profile, which turns on the OPT-2 back-pressure governor — it
samples the source's `Log Flush Waits` counter and **sheds a concurrent worker
when the counter rises**, so a busy source slows rivet down instead of the other
way round. `environment: local` (the default for dev) does **not** throttle.

> **Caveat — isolation.** rivet reads under SQL Server's default READ COMMITTED.
> It does not downgrade to `NOLOCK` / snapshot isolation, so on a table under
> heavy concurrent OLTP **writes** the per-chunk shared locks can briefly contend.
> If that matters more than read-consistency, enable RCSI on the database.
> Lock-light read options inside rivet are roadmap.

## Gentle to the worker — the one SQL Server footgun

Unlike PostgreSQL (server-side `DECLARE CURSOR` + `FETCH N`, a true stream),
**the SQL Server engine currently buffers a whole chunk into RAM** before writing
it out (no server cursor yet). So:

> **peak RSS ≈ `chunk_size` × avg_row_bytes × `parallel`**

That's fine — *as long as you bound `chunk_size`*. The trap is
**`chunk_size_memory_mb`**: on SQL Server, chunk-planning introspection doesn't
yet return an average row size, so the memory target can't be turned into a row
count — it **falls back to a large fixed row count (~500 000 rows per chunk),
ignoring the byte budget**. On wide rows each chunk then buffers multiple GB.

Measured live against SQL Server 2022, exporting `content_items`
(2 000 000 rows × ~5 KB heavy text), changing only the chunking knob:

| `content_items` 2 M | wall | peak RSS | files |
|---|---:|---:|---:|
| `chunk_size_memory_mb: 256` (→ ~500 k-row chunks) | 8m08s | **2 759 MB** | 4 |
| `chunk_size: 5000` (explicit rows) | 8m15s | **101 MB** | 400 |

**~27× less memory, same wall time, both write all 2 M rows.** On *this* host
2.76 GB fit, but on a memory-capped worker (a 1–2 GB container) the
`chunk_size_memory_mb` run OOMs while the explicit-`chunk_size` run sails at
~100 MB. The wider the rows, the worse the gap.

### Sizing `chunk_size`

Pick it from your row width and the RAM you'll give the worker:

```
chunk_size  ≈  worker_RAM_budget_MB × 1024 / avg_row_KB / parallel
```

| Row shape | avg row | chunk_size for ~256 MB/worker |
|---|---:|---:|
| narrow (ints/dates) | ~0.1 KB | 500 000 (or just leave the 100 000 default) |
| typical (mixed cols) | ~1 KB | 200 000 |
| wide / heavy text | ~5 KB | 20 000–50 000 |

When in doubt, smaller is safer: more, shorter chunks are gentler to **both** the
source (shorter SELECTs) and the worker (less buffered). The cost is only a few
extra round-trips.

## Verify it

- **Worker:** run under `/usr/bin/time -v` (or `gtime -v`) and watch
  *Maximum resident set size* — it should track `chunk_size × row_bytes`, not the
  table size.
- **Source:** run [`mssql_db_bench.sh`](../bench/harness/mssql_db_bench.sh) — it
  reports longest open txn, lock count, and Log Flush Waits delta during a live
  export.

## Roadmap (why the footgun exists)

Two engine gaps make the explicit `chunk_size` necessary on SQL Server today;
both are tracked:

1. **No `avg_row_bytes` from MSSQL introspection** → `chunk_size_memory_mb` can't
   size chunks. Fix: add a row-size probe to `introspect_mssql_table_for_chunking`.
2. **Per-chunk buffering** (`into_first_result`) instead of a server-side stream.
   Fix: batched / streaming fetch so a chunk doesn't fully materialise.

Until both land, `chunk_size` (rows) is the supported way to keep rivet gentle to
the worker on SQL Server — and it makes it gentler to the source too.
