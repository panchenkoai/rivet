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
  environment: production     # Balanced profile: gentler batch/throttle/retry defaults
```

The one rule that matters: **on SQL Server, set `chunk_size` (rows) explicitly;
do not use `chunk_size_memory_mb`.** Everything else is the usual chunked export.

## Gentle to the source — what rivet does, and the lever you have

Measured against live SQL Server 2022 (the cross-tool harness —
[`dev/bench/smoke.py`](https://github.com/panchenkoai/rivet/blob/main/dev/bench/smoke.py) `--engine mssql`, results in
[`report.html`](../bench/report.html)), a **properly chunked**
rivet export is a quiet tenant:

| Signal | rivet (chunked) | Why |
|---|---:|---|
| Longest open transaction | **0 ms** | each chunk is an autocommit `SELECT`, no `BEGIN TRAN` |
| Log Flush Waits delta | **0** | rivet only reads — zero write pressure |
| `log_reuse_wait_desc` | **NOTHING** | rivet pins nothing back from log truncation |
| Peak lock count | **3–4** | shared locks released as each chunk scans (READ COMMITTED) |

The lever: **`environment: production`** (or `replica`). It selects the
*Balanced* tuning profile — gentler batch/throttle/retry defaults.
`environment: local` (the default for dev) does **not** throttle.

The OPT-2 back-pressure governor is a separate, explicit opt-in: it arms only
when you set **`tuning.adaptive: true` and `parallel > 1`** (with `parallel: 1`
there is no worker to shed). When armed, on SQL Server it samples
`Log Flush Waits/sec` — the `_Total` row of `sys.dm_os_performance_counters` —
and **sheds a concurrent worker when that counter rises**, so a source someone
*else* is hammering slows rivet down instead of the other way round.

Read the table above together with this: `Log Flush Waits delta = 0` for a
rivet export is exactly *why* it is the governor's signal. It measures redo-
**write** pressure, which a read-only export cannot inflate — so the governor
can only ever be moved by foreign write traffic, and rivet's own reads can
never talk it into shedding its own workers. An earlier version sampled the
tempdb-spill counters `Workfiles Created/sec` + `Worktables Created/sec`
instead; because a large chunked read spills to tempdb *by design*, the
governor read its own exhaust and walked parallelism 4→3→2→1 without ever
recovering (a field pool run lost 1h48m to it). That implementation is gone.

The practical consequence: **the governor does not react to rivet's own tempdb
spills.** If your export is the thing straining tempdb, the levers are
`tuning.batch_size` and `tuning.max_batch_memory_mb` (and a smaller
`chunk_size`), not `adaptive`.

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
- **Source:** run the harness (`smoke.py --engine mssql`) — its harm matrix
  reports longest open txn, lock count, and worker-time delta during a live
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
