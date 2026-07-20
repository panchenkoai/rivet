# Chunked Export Mode

## When to use

Use `mode: chunked` when the table is too large for a single `full` export. Rivet splits the data into ranges by a numeric ID column and can process multiple chunks in parallel. Best for:

- Tables with millions or billions of rows
- Tables with a numeric primary key (`BIGINT`, `SERIAL`)
- When you need parallel extraction to save time
- Initial loads of large tables

## Required fields

- `chunk_column` -- a numeric, date, or timestamp column to partition by (typically the primary key). **Auto-resolved from the primary key** if you use the `table: schema.name` shortcut on a Postgres source — log line: `auto-resolved chunk_column = 'id' from primary key on public.orders`.

## Chunking strategies — pick one

Four ways to slice the table. They differ in how chunk boundaries are computed; everything below the strategy line (parallel, checkpoint, retry) is orthogonal and combines with any of them.

| Strategy | YAML | How boundaries are computed | When to use | Mutually exclusive with |
|---|---|---|---|---|
| **Fixed size** (default) | `chunk_size: 100000` | `SELECT MIN, MAX` → `[min..min+N)`, `[min+N..min+2N)`, ... — `N` rows of range per chunk | Dense numeric PK, predictable size budget per chunk | `chunk_size_memory_mb` |
| **Fixed count** | `chunk_count: 16` | Range divided into exactly `N` equal slices; per-chunk size derived dynamically | You want exactly *N* workers / files (e.g. = CPU cores) | `chunk_dense`, `chunk_by_days` |
| **Dense** | `chunk_dense: true` | `ROW_NUMBER() OVER (ORDER BY chunk_column)` instead of range; guarantees equal **row count** per chunk regardless of gaps | Sparse IDs — UUIDs as `BIGINT`, deleted rows, hashed keys | `chunk_by_days` |
| **Date-native** | `chunk_by_days: 365` | `chunk_column` must be `DATE` / `TIMESTAMP` / `TIMESTAMPTZ`; windows of N days with `>= AND <` (open-end) semantics | Time-series, event logs, historical backfills by period | `chunk_dense` |
| **Memory-target** (PG only) | `chunk_size_memory_mb: 256` | Auto-computes `chunk_size` from a `pg_class` / `reltuples` row-size estimate; clamped to `[10_000, 5_000_000]` rows. Requires `table:` shortcut | You want to budget by megabytes, not rows; wide tables where row-width is hard to guess | explicit `chunk_size` |
| **Keyset** (seek) | `chunk_by_key: uid` | Pages with `WHERE key > last ORDER BY key LIMIT chunk_size` on a unique index — **sequential**; each page is one part file | **MySQL** tables with no single-integer PK (UUID / string / composite PK) — the only bounded shape without a server cursor. See [Keyset pagination](#keyset-seek-pagination--the-safe-shape-without-an-integer-pk) below | `chunk_column`, `chunk_dense`, `chunk_by_days`, `chunk_count` |

**Orthogonal options that combine with any strategy:**

| Field | Effect |
|---|---|
| `parallel: N` | Up to `N` chunks execute concurrently (separate DB connections). Default `1`. `rivet init` scaffolds a row-scaled value (≤500 K → 1, <5 M → 2, ≥5 M → 4) |
| `chunk_checkpoint: true` | Per-chunk row in state DB → `rivet run --resume` skips completed chunks after a crash |
| `chunk_max_attempts: 3` | Retry failed chunks up to N times before bailing the run |

> **Picking `parallel`.** Extraction is I/O-bound, so the win comes from
> overlapping FETCH round-trips, not from CPU. Measured on a 10-core host,
> 2 M-row tables: a **narrow** table scales near-linearly (`1→4` ≈ **4.2×**
> faster), while a **wide** table (many large columns) saturates the wire early
> and plateaus at **`parallel: 2`** (`2→4` buys almost nothing for +60 % RSS).
> Each worker holds its own chunk buffer, so RSS grows roughly linearly with
> `N` — raise it for narrow tables, keep it at 2 for wide ones. The `init`
> heuristic is a good starting point; tune from there if memory or source
> connection count is constrained.

**Each chunk runs a query of the form:** `SELECT … FROM (<base_query>) WHERE <chunk_column> >= $lo AND <chunk_column> < $hi`. The exact rendering for dense and date-native variants is documented further down.

## Minimal config

```yaml
source:
  type: postgres
  url: "postgresql://user:pass@host:5432/dbname"

exports:
  - name: orders_chunked
    query: "SELECT id, user_id, product, price, status, ordered_at FROM orders"
    mode: chunked
    chunk_column: id                # numeric column to split ranges on
    chunk_size: 100000              # rows per chunk (default: 100,000)
    parallel: 4                     # concurrent chunk workers
    format: parquet
    destination:
      type: local
      path: ./output
```

Output files: one per chunk, e.g. `orders_chunked_20260406_120000_chunk0.parquet`

## Run it

```bash
# Preflight — shows chunk plan (how many chunks, range distribution)
rivet check --config large_table.yaml

# Run with validation and reconciliation
rivet run --config large_table.yaml --validate --reconcile
```

## Progress bar (chunked exports)

In **`mode: chunked`**, Rivet shows a **terminal progress bar** while chunks run: export name, `current/total` chunks, running row count, elapsed time, and ETA. It appears when stderr is an **interactive TTY** (a normal terminal window). The bar does **not** depend on **`RUST_LOG`** (that variable only controls `env_logger` text lines). In CI, or when you **pipe or redirect** stderr, the bar is usually suppressed — then set **`RUST_LOG=info`** (or `debug`) to follow progress in the log instead.

![Per-chunk progress: fetched N rows, chunk k/10, validation, file written, end-of-run summary](../gifs/chunked-progress.gif)

The GIF above was recorded with `RUST_LOG=info` on a 50,000-row fixture (10 chunks of 5,000) so the per-chunk log line `export 'events': chunk N/10 (...)` and the final summary are both visible. On a real interactive terminal you would see the progress bar instead; the log lines appear when stderr is captured.

Use a **small `chunk_size`** relative to your table if you want many steps on the bar (each finished chunk advances it once). **`parallel: 1`** still updates the bar after each sequential chunk.

**Ready-made example in this repo:** [`dev/scenarios/chunked_postgres_bench.yaml`](https://github.com/panchenkoai/rivet/blob/main/dev/scenarios/chunked_postgres_bench.yaml) includes **`bench_content_p4_safe`**: PostgreSQL `content_items` with **`parallel: 4`** and **`tuning.profile: safe`** (good for trying the bar on a wide table without hammering the source). Other exports in the same file cover serial / highly parallel / fatchunk / balanced profiles.

```bash
# From repo root; Postgres up + seeded (e.g. docker compose + cargo run --bin seed ...)
mkdir -p dev/output/bench
rivet check --config dev/scenarios/chunked_postgres_bench.yaml
rivet run --config dev/scenarios/chunked_postgres_bench.yaml --export bench_content_p4_safe
# Optional: RUST_LOG=info for more log detail; RUST_LOG=warn to reduce log noise (bar unchanged in a TTY)
```

## What happens

1. Rivet queries `SELECT MIN(id), MAX(id) FROM orders` to determine the range
2. Splits into chunks: `[min..min+chunk_size)`, `[min+chunk_size..min+2*chunk_size)`, ...
3. Each chunk runs independently: `SELECT ... WHERE id >= $lo AND id < $hi`
4. With `parallel: 4`, up to 4 chunks execute concurrently
5. Each chunk writes a separate output file

## Chunk checkpoint (resume after crash)

For very large exports, enable checkpointing so you can resume from where you left off:

```yaml
exports:
  - name: orders_chunked
    query: "SELECT id, user_id, product, price, ordered_at FROM orders"
    mode: chunked
    chunk_column: id
    chunk_size: 100000
    parallel: 4
    chunk_checkpoint: true          # persist progress per chunk
    chunk_max_attempts: 3           # retry failed chunks up to 3 times
    format: parquet
    destination:
      type: local
      path: ./output
```

Resume after a crash:

```bash
# Resume only processes incomplete chunks
rivet run --config large_table.yaml --resume

# View checkpoint status
rivet state chunks --config large_table.yaml --export orders_chunked

# Clear checkpoint (to re-export from scratch)
rivet state reset-chunks --config large_table.yaml --export orders_chunked
```

### Clean re-runs are NOT idempotent

Chunked mode is **not** "extract once, skip on the next clean run". Two
plain `rivet run` invocations against the same table re-extract every
chunk both times — `chunk_checkpoint: true` only matters for `--resume`
after a crashed run. Each clean run produces a new file set with a
fresh `run_id` and timestamp suffix.

| Invocation | Behaviour |
|---|---|
| `rivet run` (fresh) | extracts all chunks, writes files with `run_id` A |
| `rivet run` (again, no crash) | extracts all chunks **again**, writes files with `run_id` B |
| `rivet run --resume` (after a crash) | extracts only the chunks `chunk_state` says are incomplete |

If you want skip-on-no-change semantics, use **`mode: incremental`**
with a `cursor_column` instead — that mode persists the cursor between
runs and uses `skip_empty: true` to avoid emitting files when nothing
new arrived.

## Chunk sizing guidance

| Table size | Suggested `chunk_size` | `parallel` |
|-----------|----------------------|------------|
| 1M rows | 100,000 | 2 |
| 10M rows | 100,000 | 4 |
| 100M+ rows | 200,000-500,000 | 4-8 |

Larger chunks = fewer queries but more memory per batch. Smaller chunks = more queries but lower peak RSS.

## Date-based chunking

When your table's natural partition boundary is **time** rather than a numeric ID, use `chunk_by_days` instead of relying on integer ranges.

```yaml
exports:
  - name: orders_by_year
    query: "SELECT id, user_id, product, price, ordered_at FROM orders"
    mode: chunked
    chunk_column: ordered_at        # DATE or TIMESTAMP column
    chunk_by_days: 365              # one chunk per ~year
    format: parquet
    destination:
      type: local
      path: ./output
```

Rivet fetches `MIN` / `MAX` of the column as text, parses the dates, then generates non-overlapping windows:

```sql
-- each chunk window (open-end exclusive):
WHERE ordered_at >= '2023-01-01' AND ordered_at < '2024-01-01'
WHERE ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'
...
```

The open-end `< end_date` bound is intentional: it correctly captures all `TIMESTAMP` values within the day, including `23:59:59.999…`.

**When to use date chunking over numeric chunking:**

- The table has no dense numeric PK (UUIDs, composite keys)
- You want even partitions by time, not by row count
- The source DB has better statistics / indexes on the timestamp column
- You want to avoid unix-epoch arithmetic that JDBC tools often get wrong

`chunk_by_days` can be combined with `parallel` for concurrent date windows, and supports `chunk_checkpoint` / `--resume` like numeric chunked mode.

`chunk_dense: true` is incompatible with `chunk_by_days` and will be rejected at config validation.

`rivet check` will report the strategy as `date-chunked(ordered_at, 365d)`.

## Sparse ID ranges

If IDs have large gaps (e.g. UUIDs cast to BIGINT, or deleted rows), many chunks may be empty. Use `chunk_dense: true` to use `ROW_NUMBER()` ordering instead:

```yaml
exports:
  - name: sparse_table
    query: "SELECT id, payload FROM orders_sparse"
    mode: chunked
    chunk_column: id
    chunk_size: 50000
    chunk_dense: true               # uses ROW_NUMBER() instead of range splitting
    format: parquet
    destination:
      type: local
      path: ./output
```

`rivet check` will warn you about sparse ranges.

> **Pick a `chunk_column` whose ordering is stable.** Dense mode pages by
> `ROW_NUMBER() OVER (ORDER BY chunk_column)`, recomputed in an independent query
> per chunk. If `chunk_column` has a large tied peer group (many equal values)
> *and* the source is being written concurrently during the export, two chunks'
> queries could order the tied band differently and a boundary row could land in
> both or neither. Against a **static** table every engine we test (PG 16,
> MySQL 8, SQL Server 2022) orders ties deterministically, so this does not occur
> — but prefer a column with no large tied groups, or use **keyset** on a unique
> key, when exporting a live-writing table. (The analog for incremental cursors
> is in [semantics.md § Known non-guarantees](../semantics.md#known-non-guarantees).
> Regression-guarded by `tests/live_chunked_dense.rs`.)

## Keyset (seek) pagination — the safe shape without an integer PK

Range chunking needs a **single integer PK** to slice `MIN..MAX`. A MySQL table
whose PK is a UUID, string, or composite key has no such column, and — unlike
PostgreSQL — MySQL has **no server-side cursor** to bound a `mode: full`
snapshot. That left a real hole: such tables could only be exported as one
long-held `SELECT *` (the exact "don't hold a long query on prod" risk Rivet
exists to avoid).

> **MongoDB** has the same shape, but under `mode: full` (a document store has
> no `chunked` mode): `source.mongo.page_size` enables keyset (seek) paging on
> `_id`, `parallel: N` fans out over disjoint `_id` ranges, and both resume on
> `_id`. See [../reference/mongodb.md](../reference/mongodb.md).

**Keyset pagination** closes it. Rivet pages the table by a unique, NOT NULL,
**index-backed** key:

```sql
-- first page
SELECT * FROM (<base>) AS _rivet ORDER BY `uid` LIMIT 1000
-- subsequent pages (cursor = last page's max key)
SELECT * FROM (<base>) AS _rivet WHERE `uid` > ? ORDER BY `uid` LIMIT 1000
```

Each page is a bounded, **index range scan** (verified `EXPLAIN`: `type: range`
on the PK, no `filesort`, no full scan) and becomes one part file. This bounds
**both** peak RSS (`≤ chunk_size` rows in flight) and longest-query time.

```yaml
exports:
  - name: events
    table: app.events            # `table:` shortcut required (index check)
    mode: chunked
    chunk_by_key: event_uuid     # single-column UNIQUE / PRIMARY, NOT NULL
    chunk_size: 1000             # rows per page
    format: parquet
    destination:
      type: local
      path: ./output
```

Output files: one per page, e.g. `events_20260529_120000_keyset0.parquet`.

**Auto-resolution (MySQL).** With the `table:` shortcut and **no** `chunk_by_key`,
if the table has no single-integer PK but *does* have a usable single-column
unique key, Rivet auto-selects keyset on it and logs a `warn` naming the key
(set `chunk_by_key:` to pin the choice and silence the warning). On PostgreSQL,
auto-resolution stays off — its `DECLARE CURSOR` snapshot is already bounded, so
`mode: full` is the safe answer there; `chunk_by_key:` still works if you want
per-page files.

**The key must be index-backed.** This is the load-bearing safety property: an
`ORDER BY` on a non-indexed column degrades to a full-scan + `filesort` — worse
than the snapshot it replaces. Rivet **refuses** a `chunk_by_key` that is not a
single-column, NOT NULL, `UNIQUE`/`PRIMARY` key rather than emit such a query:

```
chunk_by_key 'payload' is not a usable keyset key on app.events — it must be a
single-column, NOT NULL, UNIQUE or PRIMARY key. Without a unique index,
`ORDER BY payload LIMIT n` would full-scan + filesort the table. Add a unique
index on it, pick another key, or use `mode: full` to accept one long snapshot
query.
```

**Required privileges:** read-only is sufficient — the introspection probe reads
`information_schema` index metadata, no elevated grants needed.

**Limitations (current):**

- **Sequential only** — each page depends on the previous page's last key, so
  keyset does not parallelize (`parallel` is ignored). Range chunking remains the
  parallel path for integer-PK tables.
- **Single-column keys only** — composite unique keys are not yet supported.
- **No `--resume` checkpoint yet** — a keyset run restarts from the first page.

## Troubleshooting

**Many empty chunks** -- Your ID column has gaps. Add `chunk_dense: true`.

**High memory usage with parallel > 1** -- Reduce `chunk_size` or add `tuning.profile: safe`.

**Export fails midway through 1000 chunks** -- Enable `chunk_checkpoint: true` and re-run with `--resume`.
