# Loading rivet CDC into BigQuery — free ingest, cheap dedup

`rivet load` on a `mode: cdc` config does this end to end — it appends the change
log for free and builds a current-state dedup view. This note explains the model
it implements (verified against BigQuery docs + live behavior): why CDC ingest
**and** dedup to current state can be free, the way the batch loader is free. The
[one command](#the-one-command-rivet-load) is at the bottom.

## What rivet CDC produces

Per-change typed Parquet: the after-image columns plus `__op`
(`insert`/`update`/`delete`) and `__pos` (monotonic log position), append-only,
**at-least-once** (a re-run can re-emit a change).

## The one hard fact

- **Loading raw changes is FREE** — it is an ordinary `LOAD DATA` (native
  schema, partitioned, clustered), identical to the batch path.
- **Deduplication to current state is inherently cross-row** (latest row per
  primary key + drop deletes). Any *materialization* of that state is a query
  (`MERGE` / `CREATE TABLE AS SELECT`) and is **billed**. There is no free
  lunch for collapsing a change log into current state.

So "free CDC + dedup" is really: keep the *pipeline* free, and defer/limit the
dedup cost.

## Three options

| Option | Ingest | Dedup / current state | Cost | Fit |
|---|---|---|---|---|
| **Native CDC** (`_CHANGE_TYPE=UPSERT/DELETE`, Storage Write API + NOT ENFORCED PK) | streaming | automatic, by ingest order (or `_CHANGE_SEQUENCE_NUMBER`) | **billed** (streaming ingest ~$0.025–0.05/GB); the table forbids `MERGE`/DML | real-time; not a batch-file model |
| **Batch MERGE** | free `LOAD DATA` → staging | `MERGE` staging → target (upsert by PK, delete on `__op`) | **billed** per merge (scans staging + touched partitions) | standard; materializes state each run |
| **Append + view** ✅ | free `LOAD DATA` → `<table>__changes` | a **view** dedups at read time | **free** to ingest + define; billed only when current state is *read* | best fit for rivet's free batched loader |

## Recommended: append the log (free) + a dedup view (free)

1. **Ingest (free).** `LOAD DATA INTO <table>__changes (…native schema… , __op STRING, __pos STRING)`
   — the same free, native-schema, daily-batched load the batch path uses
   (`__pos` is the JSON log-coordinate string, see the view below).
   Partition `__changes` by change date, **cluster by the primary key** so the
   view below prunes efficiently.

2. **Current state (free to define).** A view collapses the log. Note `__pos`
   is a **JSON string of the log coordinate** (verified live), NOT an integer —
   MySQL renders `{"file":"binlog.000047","pos":10840633}`, PostgreSQL/SQL
   Server a `{"lsn":…}`. So the ordering must **parse** it; sorting the raw
   string is wrong (`"9"` > `"10"` lexically). The parse is therefore
   **per-engine**:

   ```sql
   -- MySQL (binlog file + position):
   CREATE OR REPLACE VIEW `<table>` AS
   SELECT * EXCEPT (__op, __pos, __seq, __rn),
          (__op = 'delete') AS __is_deleted
   FROM (
     SELECT *, ROW_NUMBER() OVER (
       PARTITION BY <pk>
       ORDER BY JSON_VALUE(__pos,'$.file') DESC,
                CAST(JSON_VALUE(__pos,'$.pos') AS INT64) DESC,
                __seq DESC
     ) AS __rn
     FROM `<table>__changes`
   )
   WHERE __rn = 1;
   -- PostgreSQL / SQL Server: ORDER BY JSON_VALUE(__pos,'$.lsn') …
   -- Snowflake: PARSE_JSON(__pos):file … and SELECT * EXCLUDE (…)
   ```

   One expression does the dedup work: **at-least-once dedup** (a re-emitted
   change has the same `(__pos,__seq)` and loses the tiebreak) and
   **latest-per-PK collapse**. **Soft delete:** the latest change is kept
   unconditionally, and its `__op` is projected into a boolean `__is_deleted`
   column — a deleted row survives as a tombstone (last-known values +
   `__is_deleted = true`) instead of silently vanishing; live state is
   `WHERE NOT __is_deleted`. Verified live: three changes (insert/update/delete)
   loaded **twice** (10 rows) collapse to 3 distinct-PK rows — the deleted PK
   present with `__is_deleted = true`, the other two `false` (2 live rows).

Ingest + view are **both free**. Reading `<table>` scans `__changes` (billed),
but clustering on `<pk>` keeps it cheap; if current state is read hot, add an
**optional daily compaction** (`CREATE OR REPLACE TABLE <table>__snapshot AS
SELECT * FROM <table>`) — one billed scan per day, not per read. This is the
classic *log + periodic compaction*.

## The base-and-buffer layout (`backfill:` streams) and `rivet compact`

A stream whose baseline comes from `cdc.backfill:` does NOT use the view above.
Its baseline legs overwrite a **physical base table** `<table>` — the source
columns plus one service column, `__is_deleted BOOL` (written as `false` inside
the baseline Parquet, so no NULL ever appears) — and the stream's runs append
into `<table>__changes`, a **per-cycle buffer** without a partition. The cycle is

```sh
rivet run     -c cfg.yaml   # anchor once, baseline once, then only the changes
rivet load    -c cfg.yaml   # baseline → <table> (batched, staging + CLONE); changes → <table>__changes
rivet compact -c cfg.yaml   # MERGE <table>__changes into <table>; DROP the buffer
```

`compact` is **one scripted job per table**: the latest change per key (the
same `__pos` order the view uses) is upserted; a **delete flags** the base row
(`__is_deleted = TRUE`, last values kept — the warehouse deletes nothing) and a
later insert un-flags it. For a day-partitioned base (init's default) the script
collects the buffer's distinct days into a variable and every `MERGE` filters
both sides with `DATE(col) IN UNNEST(days)` — measured: 172 bytes read against
48 KB for a `MIN..MAX` range on the same buffer, i.e. exactly the touched
partitions; more than 4,000 days merge in chunks of 4,000 inside the same
script. Then the script drops the buffer and the next `load` creates it again
from its run's spec. Other partition keys (hour, month, year, integer ranges)
keep a constant `MIN..MAX` range per window in separate jobs — the truncation
forms did not prune when measured. An empty buffer is just dropped.

What a cycle bills: BigQuery charges every statement that reads a table at
least 10 MB per table, so a compaction with changes bills a 30 MB floor (the
probe, and the MERGE over two tables); one without changes bills nothing. The
`load` side is free (`CREATE`, `LOAD DATA`). The script's child statements
appear in `INFORMATION_SCHEMA.JOBS` under `parent_job_id` with the same labels.

Consumers read `<table>` directly, `WHERE NOT __is_deleted` for live rows. The
buffer holds no history — `__is_deleted` in the base is the record that a row
was deleted. BigQuery only in this release; Snowflake keeps the view layout.

## Every billed step carries its own label

The whole point of the loader's job labels (`managed_by:rivet` /
`rivet_op:<op>` / `rivet_table:<table>` / `rivet_run:<load run id>`) is that
you can price **each table's update, per operation**. There are two operations:

- `rivet_op:load` — everything `rivet load` runs for a table: the free `LOAD DATA`
  jobs (one per batch of at most 4,000 partitions), the `COUNT(*)` gate, the
  table DDL, the staging `CLONE` of a batched whole-table load, the view;
- `rivet_op:merge` — everything `rivet compact` runs for a table (the billed
  `MERGE` and its partition-range probe).

`rivet_table` is the base table's short name for both the table and its
`__changes`, so `GROUP BY op, tbl` answers "what does keeping this table current
cost" in one row per table per operation:

```sql
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key='rivet_op')    AS op,      -- load | merge
  (SELECT value FROM UNNEST(labels) WHERE key='rivet_table') AS tbl,
  COUNT(*) AS jobs, SUM(total_bytes_billed) AS bytes_billed
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key='managed_by' AND value='rivet')
GROUP BY op, tbl ORDER BY bytes_billed DESC;
```

Every rivet-driven job passes through one labelled seam (`run_sql(sql, op,
table)`), so nothing rivet runs is unlabelled — only jobs you run yourself need
labels of your own.

## The one command: `rivet load`

`rivet load -c cfg.yaml` — where the export is `mode: cdc` and the config carries
a top-level `load:` block with `target: bigquery` — does both steps
automatically. The view's key is the source primary key `rivet run` recorded
(`pk: auto`, the default); `pk: [..]` overrides it and is required only when
none was recorded (a `query:` export, a table without a primary key):

1. free `LOAD DATA` of the CDC Parquet into `<table>__changes` (the same
   native-schema batched loader, with `__op`/`__pos`/`__seq` in the schema);
2. `CREATE OR REPLACE VIEW <table>` — the exact dedup view above.

```yaml
exports:
  - name: orders
    table: orders
    mode: cdc
    cdc: { until_current: true, checkpoint: /var/lib/rivet/orders.ckpt }
    destination: { type: gcs, bucket: my-bucket, prefix: cdc/orders/ }
load:
  target: bigquery      # or: snowflake (+ connection/warehouse/database/schema/storage_integration)
  project: my-proj
  dataset: analytics
  # pk: [id]            # the view's PARTITION BY; default: the source primary key
  cleanup_source: true
```

Both steps are free. The count gate (summed manifest rows == warehouse
`COUNT(*)`) and source cleanup work exactly as in the batch path. **There is no
`--cdc` flag** — the mode comes from the export's `mode: cdc`; one config drives
both `rivet run` (extract) and `rivet load`.

Live-verified end to end: this flow builds the dedup view shown above, with two
refinements over the sketch — on MySQL the binlog file is parsed numerically
(`CAST(REGEXP_EXTRACT(JSON_VALUE(__pos,'$.file'), r'[0-9]+$') AS INT64)` then
`CAST(JSON_VALUE(__pos,'$.pos') AS INT64)`), and the delete flag is
`COALESCE(__op = 'delete', FALSE) AS __is_deleted` so snapshot-backfill rows
(NULL `__op`) stay live — and a deleted PK survives as `__is_deleted = true`
rather than vanishing. See the matrix cells `cdc_backfill_snapshot_{mysql,pg,mongo}`
and the Snowflake parity `mongo_cdc_delete_flag_snowflake`.

### A whole schema: one stream, one warehouse table per source table

`rivet init --mode cdc` over a whole schema emits ONE multiplex export — every
table through one change stream (one PostgreSQL slot / one MySQL binlog
connection), rather than one export and one slot per table:

```yaml
exports:
  - name: cdc
    tables: [orders, customers, line_items]
    mode: cdc
    cdc: { initial: snapshot, until_current: true, checkpoint: /var/lib/rivet/cdc.ckpt }
    destination: { type: gcs, bucket: my-bucket, prefix: cdc/ }
load:
  target: bigquery
  project: my-proj
  dataset: analytics
```

**Loads are batched by partition span.** BigQuery writes at most 4,000
partitions per job. Before any job, `rivet load` reads the partition column's
range from every Parquet footer and packs the files, in order of their lowest
value, into jobs whose combined span fits — so a keyset export over an
autoincrement key (whose files are date-local because `id` grows with time)
loads eleven years of daily partitions in three or four jobs, never coarsened
to `month`. A whole-table (`OVERWRITE`) load that needs several jobs fills a
`<table>__staging` table and swaps it in with one zero-copy `CLONE`. The one
shape nothing splits is a single file wider than 4,000 partitions (dates
uncorrelated with the read key): that is refused before any job, naming the
file and the granularity that fits.

The capture fans each table out under `<prefix>/<table>/` (its own
`manifest.json` + `_SUCCESS`, with `initial: snapshot` nested a level below as
`<prefix>/<table>/snapshot/`), and `rivet load` follows that layout: **one
`<table>__changes` + one dedup view per SOURCE table**, each loaded from its own
sub-prefix only. Each table is keyed on its own recorded primary key; the rest of
the `load:` block is shared by every table of the stream unless the export's
`load:` carries `tables: { orders: { partition: { column: created_at,
granularity: day } }, customers: { partition: none } }` — one block per captured
table, layered over the export's and the top-level `load:`. With
`cdc: { backfill: auto, … }` in place of `initial: snapshot`, each table's
baseline is read by the batch export that names it (keyset, chunked, with its
`columns:`) into the same `<prefix>/<table>/snapshot/`, so the load is unchanged
(`rivet init --mode cdc` scaffolds that shape on MySQL and PostgreSQL); `rivet check --target bigquery` prints one resolver document
per table (`Export: cdc/orders`), so you see each table's native schema before
loading it. Live-verified against BigQuery over a 3-table PostgreSQL stream
(#252).

**Bottom line:** yes — rivet can ingest CDC into BigQuery **and** expose a
deduplicated current state entirely for free (append + view). The only
unavoidable cost is *materializing* current state, which we defer to read time
(a view) or amortize (daily compaction) — never on the ingest path.
