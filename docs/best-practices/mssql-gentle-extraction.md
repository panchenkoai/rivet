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

## Gentle to the worker — `batch_size` bounds RSS, not `chunk_size`

The SQL Server engine **streams** the result set: it consumes rows from the
server incrementally and emits an Arrow batch every `tuning.batch_size` rows,
never holding more than one batch in memory (the SQL Server analogue of the
PostgreSQL cursor's `FETCH N`). So:

> **peak RSS ≈ `batch_size` × avg_row_bytes** — *independent of `chunk_size`*.

That splits the two knobs cleanly:

- **`batch_size`** is the **memory** lever.
- **`chunk_size`** is now only the **file-count** lever (one part file per
  chunk). A large `chunk_size` — or `mode: full` — gives **few large files** and
  still runs at low RSS.

Measured live against SQL Server 2022, exporting `content_items`
(2 000 000 rows × ~5 KB heavy text):

| config | wall | peak RSS | files |
|---|---:|---:|---:|
| `mode: full` (streamed, one file) | 8m03s | **171 MB** | 1 |
| `chunk_size: 5000` | 8m15s | 101 MB | 400 |

**One file *and* ~170 MB at 2 M heavy rows.** Before streaming, `mode: full`
buffered the whole table (~10 GB → OOM) and the only way to bound memory was a
tiny `chunk_size` → hundreds of tiny files. Now you pick `chunk_size` purely for
the downstream file layout; memory stays put.

### Sizing the two knobs

- **`batch_size`** (RAM): peak RSS ≈ `batch_size` × avg_row_bytes. Lower it for
  wide rows.

  | Row shape | avg row | `batch_size` for ~100 MB/worker |
  |---|---:|---:|
  | narrow (ints/dates) | ~0.1 KB | leave the profile default |
  | typical (mixed cols) | ~1 KB | ~50 000 |
  | wide / heavy text | ~5 KB | ~10 000 |

- **`chunk_size`** (files): ≈ rows ÷ desired file count. Bigger = fewer, larger
  files; memory is unaffected. `mode: full` = one file.

> Skip `chunk_size_memory_mb` on SQL Server: introspection returns no
> `avg_row_bytes`, so it can't size by bytes (it falls back to ~500 k-row
> chunks). With streaming that no longer blows up memory, but `chunk_size`
> (files) + `batch_size` (RAM) are the honest levers.

## Verify it

- **Worker:** run under `/usr/bin/time -v` (or `gtime -v`) and watch
  *Maximum resident set size* — it should track `batch_size × row_bytes`, flat
  across `chunk_size` and table size.
- **Source:** run [`mssql_db_bench.sh`](../bench/harness/mssql_db_bench.sh) — it
  reports longest open txn, lock count, and Log Flush Waits delta during a live
  export.

## Roadmap

- ✅ **Streaming export** — the engine now consumes the result set incrementally
  and emits one `batch_size` batch at a time, so RSS is bounded by `batch_size`,
  not `chunk_size`. (Was: `into_first_result` materialised the whole chunk.)
- ◻ **`avg_row_bytes` from MSSQL introspection** so `chunk_size_memory_mb` can
  size by bytes (add a row-size probe to `introspect_mssql_table_for_chunking`).
  Lower priority now that streaming bounds memory regardless.
- ◻ **Lock-light reads** (RCSI / snapshot opt-in) for sources under heavy
  concurrent OLTP writes.
