# Chunked Export Mode

## When to use

Use `mode: chunked` when the table is too large for a single `full` export. Rivet splits the data into ranges by a numeric ID column and can process multiple chunks in parallel. Best for:

- Tables with millions or billions of rows
- Tables with a numeric primary key (`BIGINT`, `SERIAL`)
- When you need parallel extraction to save time
- Initial loads of large tables

## Required fields

- `chunk_column` -- a numeric column to partition by (typically the primary key)

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

## Troubleshooting

**Many empty chunks** -- Your ID column has gaps. Add `chunk_dense: true`.

**High memory usage with parallel > 1** -- Reduce `chunk_size` or add `tuning.profile: safe`.

**Export fails midway through 1000 chunks** -- Enable `chunk_checkpoint: true` and re-run with `--resume`.
