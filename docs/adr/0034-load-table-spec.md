# ADR-0034: Per-Table Load Spec — Clustering, Partitioning, and a Load That Never Reads the Source

- **Status:** Accepted (2026-09-11)
- **Date:** 2026-09-11
- **Context:** The warehouse shape of a loaded table is barely configurable today. `load.cluster_by` exists (full-load table only, applied at creation); `<table>__changes` is always clustered on `load.pk`; partitioning exists only through the EXPORT's `partition_by`, which config validation forbids together with a `load:` block (the loader would load one partition), and which BigQuery would reject anyway for a bare `TIMESTAMP` column (it needs `DATE(ts)` / `TIMESTAMP_TRUNC`, verified by dry-run). Separately, `rivet load` connects to the SOURCE to type the columns (`preflight::type_report::collect_reports` → `config.source.resolve_url()`), although everything it needs was known when the data was extracted.

---

## Decision

### D1 — The load reads the metabase, never the source

`rivet run` (on success) and `rivet init` persist a **load spec** per export into the state DB (schema v27, table `export_load_spec`): the resolved column types (`RivetType`, the same values the extractor wrote), the source **primary key** columns in order, the schema fingerprint, the run id, and whether it came from `run` or `init`. `rivet load` resolves warehouse types from that row (`ExportTarget::resolve_table` is pure) and opens no source connection. A missing row fails loudly: "run `rivet run` or `rivet init` for this export first".

Primary key sources, per engine: PostgreSQL `pg_index.indisprimary`, MySQL `KEY_COLUMN_USAGE` of `PRIMARY`, SQL Server `sys.indexes.is_primary_key`, MongoDB `_id`. Composite keys keep their order. A `query:` export takes the key `rivet init` recorded for the table it scaffolded the query from; when the metabase holds no key for it, `pk` must be declared.

### D2 — Config: `pk`, `cluster_by`, `partition` per table, defaults on the top-level `load:`

```yaml
load:
  target: bigquery
  project: my-proj
  dataset: analytics
  cluster_by: auto              # default for every export: the source primary key

exports:
  - name: page_views
    table: page_views
    mode: incremental
    cursor_column: id
    load:
      pk: auto                  # dedup key of the current-state view; default: source primary key
      cluster_by: [site_id, id] # auto | none | up to 4 columns, order matters
      partition:
        column: server_time     # DATE / DATETIME / TIMESTAMP / INT64 column
        granularity: day        # hour | day | month | year
        expiration_days: 400    # optional → partition_expiration_days
        require_filter: false   # optional → require_partition_filter
```

Alternatives to `column` + `granularity`: `range: { column, start, end, interval }` for an integer column, or `ingestion: day` (hour | day | month | year) to partition by load time. Exactly one form per table.

The spec applies to the table the load writes: the full-load `<table>`, or `<table>__changes` for incremental / CDC (the current-state view inherits nothing; queries filter `__changes` through it). Name clash avoided on purpose: `load.partition` shapes the WAREHOUSE table; `exports[].partition_by` still shapes the EXTRACT layout (Hive prefixes) and stays incompatible with `load:` until the loader is partition-aware.

**Default for an append table (incremental / CDC):** the change log follows the table it grows from. An incremental export's first run re-reads the whole table (ADR-0033 MT1) and lands as a plain `<table>`, exactly like a full load; the first delta renames that table to `<table>__changes` (`ALTER TABLE … RENAME TO`, then `ADD COLUMN __op, __pos, __seq`), so the log keeps whatever partitioning and clustering the table had — rivet's own `cluster_by`, or a shape the operator gave it by hand — and the current-state view takes the old name. The load tells a whole-table run from a delta by the manifest's `extraction.cursor_low` (absent when the run had no cursor to resume from). No copy and no query: the earlier `CREATE TABLE … AS SELECT` adoption was billed and, after a full → incremental switch, duplicated every row the first run had re-read. The same rename applies when a keyset export continues as incremental on the same key and when a CDC stream starts over a full-load table without `initial: snapshot`; a CDC load carrying a snapshot over such a table is refused (keep one baseline). On BigQuery the renamed log drops `require_partition_filter` (the view reads the whole log) and the expiry of ingestion-time partitions (expiring load dates would drop unchanged rows from the view); a `LOAD DATA INTO` declaring the meta columns first appends to a log where they are last (verified 2026-09-11). Counts come from `tables.get` metadata, since `COUNT(*)` on a table requiring a partition filter is refused. Snowflake uses the same rename script; not live-run. No main table → no partition.

### D3 — Rivet writes the BigQuery expression, the user never does

| Column type (from the metabase) | hour | day | month | year |
|---|---|---|---|---|
| `TIMESTAMP` | `TIMESTAMP_TRUNC(c, HOUR)` | `TIMESTAMP_TRUNC(c, DAY)` | `TIMESTAMP_TRUNC(c, MONTH)` | `TIMESTAMP_TRUNC(c, YEAR)` |
| `DATETIME` | `DATETIME_TRUNC(c, HOUR)` | `DATETIME_TRUNC(c, DAY)` | `DATETIME_TRUNC(c, MONTH)` | `DATETIME_TRUNC(c, YEAR)` |
| `DATE` | refused | `c` | `DATE_TRUNC(c, MONTH)` | `DATE_TRUNC(c, YEAR)` |
| `INT64` (`range`) | `RANGE_BUCKET(c, GENERATE_ARRAY(start, end, interval))` | | | |
| ingestion | `TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)` | `_PARTITIONDATE` | `TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH)` | `TIMESTAMP_TRUNC(_PARTITIONTIME, YEAR)` |

Dry-run on BigQuery (2026-09-11): `TIMESTAMP` hour / month / year, `DATETIME` hour / month, `DATE` day / month, `DATE` + hour (refused as expected), `RANGE_BUCKET`, `_PARTITIONDATE`, and `OPTIONS(partition_expiration_days, require_partition_filter)`. The rest — `TIMESTAMP` / `DATETIME` day, `DATETIME` year, `DATE` year, the ingestion hour / month / year forms, and `LOAD DATA OVERWRITE … PARTITION BY … CLUSTER BY … OPTIONS(…)` — verified 2026-09-12, then every form end to end (`docs/load-spec-matrix.yaml`).

### D4 — Validation without the source

- **Config load:** one partition form; valid granularity; `range` bounds ordered and `interval > 0`; `expiration_days > 0`; `cluster_by` ≤ 4 columns.
- **Load plan (metabase types):** the partition column exists; its type fits the form (`hour` on `DATE`, `range` on a non-integer → refused); clustering columns are clusterable in BigQuery (`BIGNUMERIC BOOL DATE DATETIME GEOGRAPHY INT64 NUMERIC RANGE STRING TIMESTAMP`, top-level, non-repeated) — `auto` skips a non-clusterable key column with a warning, an explicit one is refused.
- **Partition budget:** BigQuery allows 10,000 partitions per table and 4,000 modified by one job. The loader reads the partition column's min/max from the Parquet footers it is about to load (GCS, not the source — two small range reads per file), estimates the partitions the job touches, and refuses above 4,000 naming a coarser granularity that fits; a file without statistics for the column leaves the check to BigQuery. Hourly partitions reach 10,000 after 416 days, so an hourly key without `expiration_days ≤ 416` warns at plan time; a `range` making more than 10,000 buckets is refused when the block is read.

### D5 — Changing the spec of an existing table

- **Full load:** an existing `<table>` is overwritten only when the load ledger says rivet loaded it and its partitioning and clustering match the config (`LOAD DATA OVERWRITE` must repeat them; BigQuery refuses a changed spec with "Cannot replace a table with a different partitioning spec", verified 2026-09-11). A table rivet did not load, one whose shape differs, or a view left by an append mode fails the load naming the table and the difference; nothing is changed, and the operator decides (drop or rename the table, or align the config). Rivet never re-creates or swaps a table on its own. The partition **options** (`expiration_days`, `require_filter`) are not shape: an overwrite that declares different ones is refused too ("Specified partition_expiration_days … is different from existing table's", verified 2026-09-12), so the load declares them only when it creates the table and otherwise brings them in step with `ALTER TABLE … SET OPTIONS` after the overwrite.
- **`<table>__changes`:** it holds history; BigQuery cannot re-partition in place, only rebuild it with a billed `CREATE TABLE … AS SELECT` that reads all of it. A changed `partition` is refused, naming the bytes the rebuild reads and `rivet load --rebuild-changelog`, which rebuilds and swaps it in — a rebuild is never a side effect of a scheduled load. A changed `cluster_by` is applied to the table metadata; BigQuery clusters new data only, which the load reports.
- The current spec is read from the warehouse (`tables.get`: `timePartitioning`, `rangePartitioning`, `clustering`, `requirePartitionFilter`), never remembered by rivet, and compared as metadata rather than DDL text: BigQuery renders `TIMESTAMP_TRUNC(ts, DAY)` back as `PARTITION BY DATE(ts)`, so a text comparison would refuse rivet's own table on its second load (seen 2026-09-12).

### D6 — Snowflake

Snowflake has no user partitions. `cluster_by` maps to `CLUSTER BY (…)`; `partition` maps to a leading clustering expression (`DATE_TRUNC('<granularity>', c)`) ahead of the clustering columns, the shape Snowflake recommends for date-filtered tables. `expiration_days` / `require_filter` are refused on Snowflake.

---

## Rollout

1. **Metabase load spec + source-free load** (D1). RED first: a `rivet load` whose `source.url` points at a closed port must succeed after a `rivet run`, and fail loudly with no metabase row. **Done (2026-09-11):** `export_load_spec` (v27), recorded after every successful run in the `run_export_job` wrapper and on `apply`; live on MySQL, PostgreSQL, SQL Server and MongoDB, end to end on BigQuery with the source on a closed port. `rivet init` recording the key moves to step 2, where it is first used.
2. **`pk: auto` / `cluster_by: auto`** from the recorded primary key (D2), together with the full-table half of D5: a default `cluster_by` would otherwise fail the next load of every existing unclustered full table. **Done (2026-09-11):** `load.pk` / `load.cluster_by` take `auto | none | [cols]` and resolve at plan time (`resolve_keys`); a change log rivet creates clusters on `cluster_by`, one it renames from a table keeps that table's shape; a BigQuery full table whose clustering or partitioning changed fails the load (D5). Live on BigQuery: default clustering on a composite key in key order, a refused re-clustered full table, an incremental load without `pk:` (each RED on the previous build). `rivet init -o` records each scaffolded export's key (`origin = init`, no columns), connecting with its own URL because the scaffold's `url_env` is not set in its process; a `query:` export keeps that key through its runs (live, RED before).
3. **`load.partition` on BigQuery** for the full table and `__changes`, including the append-table default and a partition-preserving MT7 adoption (D2, D3, D4), live-tested per column type and granularity on the test project. **Done (2026-09-12):** `partition: none | { column, granularity, expiration_days, require_filter } | { range: {…} } | { ingestion }` at the top level and per export; the expression comes from the metabase column type (`plan::resolve_partition`); a full table takes it at creation, `__changes` inherits it through the rename or takes it when rivet creates the log (never `require_partition_filter`, never a load-date expiry); the existing shape is read from `tables.get` and compared as metadata; options are altered in place; the partition budget is checked from the Parquet footers before the job. Live per form: `docs/load-spec-matrix.yaml`.
4. **Spec drift** (D5) for `__changes`: a changed `partition` refused naming the rebuild, `rivet load --rebuild-changelog`, a changed `cluster_by` applied to the log's metadata.
5. **Snowflake mapping** (D6). **Done as SQL (2026-09-12), not live-run:** `partition` becomes a leading `DATE_TRUNC('<G>', c)` in the `CLUSTER BY` of both the table and `__changes`; `range`, `ingestion`, `expiration_days` and `require_filter` are refused at plan time.

Each step lands with its config-validation cells, a `docs/load-spec-matrix.yaml` (form × granularity × warehouse), and live BigQuery tests.

## Sources

- BigQuery partitioned tables: https://docs.cloud.google.com/bigquery/docs/partitioned-tables
- BigQuery clustered tables: https://docs.cloud.google.com/bigquery/docs/clustered-tables
- BigQuery quotas (partitioned tables): https://docs.cloud.google.com/bigquery/quotas
