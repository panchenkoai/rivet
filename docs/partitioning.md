# Value-Based Output Partitioning (`partition_by`)

## When to use

Set `partition_by` to split one export's rows into **one destination sub-folder
per value bucket** of a column — the Hive-style `col=value/` layout that
warehouses and query engines (Snowflake external tables, BigQuery external /
Hive-partitioned tables, Spark, DuckDB, Athena) discover automatically. Best for:

- Daily/monthly snapshots that land each day's rows under its own prefix
- Backfilling history into a partitioned lake layout in one command
- Feeding a warehouse that prunes partitions by a date column

`partition_by` is **orthogonal to `mode`**: each partition runs the export's own
mode, so `mode: chunked` chunks *within* a partition.

## Required fields

- `partition_by` — the column whose value buckets the rows (a `DATE` /
  `TIMESTAMP` / `TIMESTAMPTZ` column).
- a `{partition}` token in `destination.path` or `destination.prefix` — Rivet
  refuses the run without it, because every partition would otherwise overwrite
  the same prefix.

## Optional fields

- `partition_granularity` — `day` (default), `month`, or `year`.

## Minimal config

```yaml
source:
  type: postgres
  url: "postgresql://user:pass@host:5432/dbname"

exports:
  - name: events
    table: events
    partition_by: created_at
    partition_granularity: day
    format: parquet
    destination:
      type: s3
      bucket: my-bucket
      prefix: "events/{partition}/"      # → events/created_at=2023-01-01/
```

## Run it

```bash
rivet run --config events.yaml --validate
```

## What happens

1. Rivet reads the `[min, max]` span of `partition_by` from the source
   (`SELECT min(col)`, `SELECT max(col)` over your query).
2. It generates one contiguous bucket per day/month/year across that span.
3. Each bucket becomes its own export: the query is wrapped as
   `SELECT * FROM (<your query>) WHERE col >= '<lo>' AND col < '<hi>'`
   (half-open: `lo` inclusive, `hi` exclusive — no row counted twice), and
   `{partition}` resolves to `col=value`.
4. Rows whose `partition_by` value is **NULL** land in
   `col=__HIVE_DEFAULT_PARTITION__/` (the Hive default-partition convention) so
   **no row is ever silently dropped**.

Each partition is an independent, complete output prefix — its own
`manifest.json` and `_SUCCESS` — so it can be validated and consumed on its own.

### Example output layout

```
events/
  created_at=2023-01-01/  manifest.json  _SUCCESS  events__2023-01-01_*.parquet
  created_at=2023-01-02/  manifest.json  _SUCCESS  events__2023-01-02_*.parquet
  ...
  created_at=__HIVE_DEFAULT_PARTITION__/  manifest.json  _SUCCESS  ...
```

DuckDB reads the whole tree as one partitioned dataset and recovers the
partition column from the path:

```sql
SELECT created_at, count(*)
FROM read_parquet('events/**/*.parquet', hive_partitioning = true)
GROUP BY 1;
```

## Granularity

| `partition_granularity` | Path segment | Bucket bounds |
|-------------------------|--------------|---------------|
| `day` (default) | `col=2023-01-01` | `[day, day+1)` |
| `month` | `col=2023-01` | `[month-start, next-month-start)` |
| `year` | `col=2023` | `[year-start, next-year-start)` |

## Notes and limits

- **Index the partition column.** Each bucket is a separate range scan over your
  query. Without an index on `partition_by`, a wide span means many scans.
- **`partition_by` + `mode: chunked` — mind `chunk_size`.** Chunking runs
  *inside* each partition, but the chunk-key (`chunk_column`) range is taken from
  that partition's `[min, max]` of the key, **not** its row count. When the key
  (e.g. a global `id`) is not correlated with the partition key, a partition's
  few rows can still span most of the key range — so a small `chunk_size`
  explodes into one tiny file per row. Keep the default (`chunk_size: 100000`)
  or larger, set a `chunk_size` close to the *per-partition* row count, or use
  `chunk_dense: true` (ROW_NUMBER) when the key is sparse within a partition.
  Row counts stay correct either way; only the file fan-out is affected.
- **Time zones.** Bucket bounds are emitted as `YYYY-MM-DD` literals. For a
  `TIMESTAMPTZ` column the comparison happens at the session time zone — pin it
  (e.g. MySQL `SET time_zone = '+00:00'`) when the exact day boundary matters.
- **Not compatible with `mode: time_window`** (time_window already filters by a
  rolling window). Use `partition_by` with `full`, `chunked`, or `incremental`.
- **`--parallel-export-processes` is disabled** while partitioning is active
  (child processes re-load the config and can't see the synthesised partitions);
  the run executes in-process.
- **Validating a single partition today:** point `validate` at the concrete
  prefix —
  `rivet validate --config c.yaml --export events --prefix events/created_at=2023-01-01`.
  Validating every partition of an export by its parent name in one command is
  not yet wired.
