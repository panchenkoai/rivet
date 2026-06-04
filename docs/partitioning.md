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

- **Cloud prefixes: put a `/` after `{partition}`.** Object stores have no
  directories — the part filename is appended to the resolved prefix verbatim.
  `prefix: "events/{partition}/"` yields `events/created_at=2023-01-01/part.parquet`;
  omitting the trailing slash concatenates into `…created_at=2023-01-01part.parquet`.
  (Local `path:` joins as directories, so the slash is optional there.)
- **Index the partition column.** Expansion runs three probes over the base
  query (`min`, `max`, NULL-count). Without an index on `partition_by` those are
  up to three full scans before the first row is exported.
- **Time zones.** Bucket bounds are emitted as `YYYY-MM-DD` literals. For a
  `TIMESTAMPTZ` column the comparison happens at the session time zone — pin it
  (e.g. MySQL `SET time_zone = '+00:00'`) when the exact day boundary matters.
- **Not compatible with `mode: time_window`** (time_window already filters by a
  rolling window). Use `partition_by` with `full`, `chunked`, or `incremental`.
- **Not compatible with `chunk_by_key` (keyset).** Keyset pagination needs the
  `table:` shortcut so the planner can confirm the key is index-backed;
  partitioning rewrites the export into a `query:` subquery, so the two can't
  compose — Rivet rejects the combination up front.
- **`--parallel-export-processes` is disabled** while partitioning is active
  (child processes re-load the config and can't see the synthesised partitions);
  the run executes in-process.
- **`plan` / `check` do not expand partitions yet.** They report the *parent*
  export as one un-partitioned job (its `{partition}` token stays literal in the
  shown path, the row estimate is the whole span, strategy is the base mode).
  Treat their output as the per-partition shape, not the campaign.
- **Validating a single partition today:** point `validate` at the concrete
  prefix —
  `rivet validate --config c.yaml --export events --prefix events/created_at=2023-01-01`.
  Validating every partition of an export by its parent name in one command is
  not yet wired.

## Choosing a chunking strategy per partition (large partitions)

`partition_by` is orthogonal to `mode`, so each partition runs the export's mode
*inside* itself. The right choice depends on how big a single partition is and
whether the chunk key is dense within it. Consider a date that holds **100 M
rows**:

| Setup | Source load | Use when |
|-------|-------------|----------|
| `mode: chunked`, range `chunk_column`, key **dense/correlated** within the partition (e.g. the day ≈ the whole table) | One logical pass: `ceil(key_span / chunk_size)` indexed range scans, bounded memory. **Good.** | The common big-partition case. Keep `chunk_size` ≈ 100 k+. |
| `mode: chunked`, range `chunk_column`, key **sparse** within the partition (rows interleaved with other days across the key range) | Chunk windows are computed over the partition's `[min,max]` of the key, **not** its row count → many windows read key-range rows then discard out-of-partition ones. **Query + I/O amplification.** | Avoid — pick a key correlated with the partition, or a finer `partition_granularity` so each partition's key range tightens. |
| `chunk_dense: true` | Each chunk re-runs `ROW_NUMBER()` over the **whole** partition → `O(chunks × partition_rows)`. Fine for *small* sparse partitions, **catastrophic at 100 M**. | Small partitions with a sparse key only. |
| `mode: full` (no chunking) | One streaming `SELECT`. **Postgres:** server-side cursor, bounded memory, but a single long-running transaction (watch vacuum / replication lag). **MySQL:** buffers the whole result client-side — **OOM on 100 M**. | PG partitions that fit a long read; never MySQL at this scale. |

Row counts are always exact regardless of the choice — these trade-offs are about
source load and file fan-out, not correctness. There is **no keyset/seek option
for a partition today** (see the `chunk_by_key` limit above), so for a genuinely
huge partition with a sparse key the practical levers are a correlated range key
or a finer granularity.
