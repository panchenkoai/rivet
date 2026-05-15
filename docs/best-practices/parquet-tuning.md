# Parquet Tuning

Rivet writes Parquet using Apache Arrow's `ArrowWriter`. How rows are grouped
within the file — and how large each group is — affects peak memory during
write, compression ratio, and downstream read performance.

---

## What is a row group?

A Parquet file is divided into **row groups**: horizontal slices of the table.
Each row group is compressed and encoded independently.

```
┌─────────────────────────────────────┐
│  Parquet file                       │
│  ┌───────────────────────────────┐  │
│  │  Row group 1 (e.g. 100K rows) │  │
│  └───────────────────────────────┘  │
│  ┌───────────────────────────────┐  │
│  │  Row group 2 (e.g. 100K rows) │  │
│  └───────────────────────────────┘  │
│  ...                                │
└─────────────────────────────────────┘
```

Row group size is the most important Parquet tuning parameter for Rivet
because it determines how much Arrow data is buffered in memory before each
flush.

---

## Why the library default can be dangerous

Without explicit row group configuration, `ArrowWriter` uses a default limit of
1 048 576 rows per row group. For narrow tables this is fine (~100 MB). For
wide tables (large `TEXT`, `JSONB`, `BYTEA` columns) the same 1M-row group can
consume 10–50 GB of writer memory before it is flushed.

Rivet's `parquet.row_group_strategy: auto` uses the Arrow schema to estimate
row width and choose a row count that targets a configurable memory budget.

---

## Strategies

```yaml
parquet:
  row_group_strategy: auto          # schema-based estimate (recommended)
  row_group_strategy: fixed_rows    # exact row count per group
  row_group_strategy: fixed_memory  # same math as auto — alias for clarity
```

### `auto` (recommended)

Rivet estimates `avg_row_bytes` from the Arrow schema field types and computes:

```
rows_per_group = target_row_group_mb × 1024² / avg_row_bytes
```

A minimum of 1 000 rows per group is always applied (protects against
pathologically wide schemas). The result is computed once from the schema in
`on_schema` and held constant for the export.

**Accuracy note:** schema-based estimation assumes average-width values. For
columns with high variance (`TEXT`, `JSONB`) the actual group size may be larger
or smaller than the target. This is an advisory target, not a hard guarantee.

### `fixed_rows`

Use when you need exact control over row group count or size, typically for
downstream tooling that benefits from fixed chunk sizes.

```yaml
parquet:
  row_group_strategy: fixed_rows
  row_group_rows: 100000
```

### `fixed_memory`

Identical math to `auto` (`target_row_group_mb` drives the calculation). Useful
as a self-documenting alias when intent is memory-driven.

---

## Choosing a target

| Target | Best for | Trade-offs |
|---|---|---|
| **32 MB** | Wide tables, low-memory runners | More row groups, lower peak write RSS, possibly weaker compression |
| **64 MB** | Wide text/JSON tables, production default for skewed data | Balanced RSS and compression |
| **128 MB** | Narrow-to-medium tables, default balanced setting | Good compression, moderate RSS |
| **256 MB** | Narrow tables on high-RAM hosts, archive/cold storage | Best compression ratio, highest write RSS |

---

## Configuration examples

### Balanced default (most tables)

```yaml
parquet:
  row_group_strategy: auto
  target_row_group_mb: 128
```

### Wide JSON or text tables

```yaml
parquet:
  row_group_strategy: auto
  target_row_group_mb: 64
  max_row_group_mb: 128
```

`max_row_group_mb` caps the computed group size even if the schema estimate
underestimates actual row width.

### Low-memory environment (≤ 512 MB RAM)

```yaml
parquet:
  row_group_strategy: auto
  target_row_group_mb: 32
  max_row_group_mb: 64
```

### Archive / cold storage (maximise compression)

```yaml
parquet:
  row_group_strategy: auto
  target_row_group_mb: 256
compression_profile: compact
```

### Exact control for downstream tooling

```yaml
parquet:
  row_group_strategy: fixed_rows
  row_group_rows: 50000
```

---

## How row groups interact with `auto_shrink`

When `on_batch_memory_exceeded: auto_shrink` is set alongside Parquet row group
tuning, each sub-batch written by `auto_shrink` is treated as a separate batch
for row group accounting. The row group row count target still applies per
sub-batch.

In practice: if `auto_shrink` splits a 10 000-row batch into two 5 000-row
sub-batches, each sub-batch gets its own row group (or shares a partial group
with adjacent sub-batches, depending on when the writer flushes).

---

## Downstream read implications

Larger row groups generally improve Parquet scan throughput because fewer group
headers need to be read. However, predicate pushdown (column filters) works at
row group granularity — smaller groups allow more skipping when only a subset of
rows matches the filter.

For warehouse loads (DuckDB, Trino, BigQuery), `target_row_group_mb: 128` is a
reasonable default. For ad-hoc analytical queries with selective filters,
smaller groups (`32–64 MB`) improve selective read latency.

---

## See also

- [Config reference — `exports[].parquet`](../reference/config.md)
- [Resource-aware extraction](resource-aware-extraction.md)
- [Compression profiles](compression-profiles.md)
