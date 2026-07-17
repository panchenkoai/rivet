# Loading rivet CDC into BigQuery — free ingest, cheap dedup

Research note (verified against BigQuery docs + live behavior). Question: can we
load a rivet **CDC** stream into BigQuery *and* deduplicate to current state,
for free — the way the batch loader is free?

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

## Every billed step carries its own label

The whole point of the loader's job labels (`managed_by:rivet` /
`rivet_op:<op>` / `rivet_table:<table>`) is that you can see cost **per step**.
So the CDC steps use distinct ops:

- `rivet_op:load` — the free `LOAD DATA` of the change log (`bytes_billed = 0`);
- `rivet_op:merge` — a batch `MERGE` (billed), if you materialize with option 2;
- `rivet_op:compact` — a daily compaction `CREATE OR REPLACE` (billed), if you
  compact the view.

Then the cost-attribution query (`GROUP BY rivet_op`) shows the merge/compaction
cost on its **own line**, separate from the free load — you can price exactly
what the dedup step costs per table:

```sql
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key='rivet_op')    AS op,      -- load | merge | compact
  (SELECT value FROM UNNEST(labels) WHERE key='rivet_table') AS tbl,
  COUNT(*) AS jobs, SUM(total_bytes_billed) AS bytes_billed
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key='managed_by' AND value='rivet')
GROUP BY op, tbl ORDER BY bytes_billed DESC;
```

The loader's labeling is already op-parameterized (`run_sql(sql, op, table)`),
so the `merge`/`compact` step just passes its op — no new mechanism needed.

## How this maps to `rivet load --cdc` (proposed)

`rivet load --cdc -c cfg.yaml --project P --dataset D`:
1. free `LOAD DATA` the CDC Parquet into `<table>__changes` (reuse the existing
   native-schema batched loader, with `__op`/`__pos` in the schema);
2. `CREATE OR REPLACE VIEW <table>` (the dedup view above).

Both steps free. The primary key comes from the config / source introspection
(rivet already knows it). Count validation and source cleanup work as in the
batch path.

**Bottom line:** yes — rivet can ingest CDC into BigQuery **and** expose a
deduplicated current state entirely for free (append + view). The only
unavoidable cost is *materializing* current state, which we defer to read time
(a view) or amortize (daily compaction) — never on the ingest path.
