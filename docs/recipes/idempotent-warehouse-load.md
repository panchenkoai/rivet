# Recipe: Idempotent Downstream Warehouse Loading

Rivet provides **at-least-once file delivery** to its destination.  After
a clean run, the destination prefix carries:

- one or more data parts (`part-NNNNNN.parquet` or `part-NNNNNN.csv`),
- a `manifest.json` listing every committed part with `size_bytes` and
  `content_fingerprint` (xxh3 over the part bytes),
- a `_SUCCESS` marker whose body fingerprints the exact `manifest.json`
  bytes.

What Rivet does **not** provide:

- exactly-once delivery to a destination,
- exactly-once load semantics into a downstream warehouse,
- transactional coupling between the export run and a downstream
  `MERGE` / `COPY INTO`.

A downstream loader that ignores the manifest and processes "every file
under this prefix" will eventually double-load — chunked retries write
new files alongside originals (RR5), and `rivet repair --execute`
explicitly does so.  Treat the manifest as the source of truth and
deduplicate on the warehouse side.

---

## The contract you build on

Every committed part is recorded in `manifest.json`:

```json
{
  "schema_version": 1,
  "run_id": "abc123…",
  "schema_fingerprint": "xxh3:…",
  "parts": [
    {"key": "part-000001.parquet", "size_bytes": 4_194_304, "content_fingerprint": "xxh3:…"},
    {"key": "part-000002.parquet", "size_bytes": 4_198_400, "content_fingerprint": "xxh3:…"}
  ]
}
```

`_SUCCESS` is a single line: `xxh3:<16-hex>` where the hex is the
fingerprint of the exact `manifest.json` bytes.  See
[ADR-0012](../adr/0012-cloud-manifest-contract.md) for the formal
invariants (M1–M9).

The manifest gives a downstream loader two things it cannot easily
recover from raw object listing:

1. The **exact set of parts** that were *committed* in this run (vs.
   parts left over from earlier interrupted runs, repair retries, or
   external writes).
2. A **content-addressable identity** per part (`content_fingerprint`)
   that survives object renames, lifecycle migrations, and CDN copies.

`content_fingerprint` is the **supported dedup key**: it is an xxh3 over the
exact part bytes, computed deterministically. Because rivet pins the Parquet
`created_by` to a version-independent constant, **identical rows produce
identical bytes — and therefore the same `content_fingerprint` — across rivet
releases**, not just within one build. So a re-extraction of the same window
(e.g. a crash + `--resume`, or a deliberate re-run) yields parts a downstream
`MERGE` can dedup on by fingerprint alone. Two parts with the same
`content_fingerprint` are byte-identical and interchangeable; drop one.

(The fingerprint is over file *bytes*, not logical rows — so it is stable for
the same rows + schema + compression settings. Changing `compression:` or the
column projection changes the bytes, hence the fingerprint.)

---

## Recommended pattern

The pattern is the same across warehouses:

1. Read `manifest.json` from the resolved destination prefix.
2. Verify `_SUCCESS` matches.  If it does not, abort — the export is
   not complete.
3. Load **only the parts listed in `manifest.json`**.  Do not glob the
   prefix.
4. Record the manifest identity (`run_id` + `schema_fingerprint` +
   `_SUCCESS` body) in a downstream control table.
5. Deduplicate by primary key (or natural key) when merging into the
   final table.
6. Commit the warehouse table only after the load succeeds end-to-end.

If step 4 records the run identity *before* step 3 starts and *after*
step 6 finishes (an "intent + commit" pair), the load is restartable:
on retry, skip any manifest already marked committed.

---

## BigQuery pattern

```sql
-- 1. Stage parts referenced by manifest.json into a per-run staging table.
LOAD DATA INTO project.dataset.orders_stage_<run_id>
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://my-bucket/exports/2026-05-23/orders/part-*.parquet']
);

-- 2. Tag every staged row with the run identity.
ALTER TABLE project.dataset.orders_stage_<run_id>
ADD COLUMN _rivet_run_id STRING,
ADD COLUMN _rivet_manifest_fingerprint STRING;

UPDATE project.dataset.orders_stage_<run_id>
SET _rivet_run_id = '<run_id>', _rivet_manifest_fingerprint = '<xxh3>'
WHERE _rivet_run_id IS NULL;

-- 3. Merge into final table on primary key.
MERGE project.dataset.orders AS target
USING project.dataset.orders_stage_<run_id> AS src
ON target.id = src.id
WHEN MATCHED THEN UPDATE SET ... -- or DO NOTHING for append-only sources
WHEN NOT MATCHED THEN INSERT ROW;

-- 4. Drop the staging table after the merge commits.
DROP TABLE project.dataset.orders_stage_<run_id>;
```

For `LOAD DATA`, prefer the **explicit URI list** from `manifest.parts`
over a wildcard.  The wildcard form is fine when you trust the prefix
contains exactly the manifest's parts (i.e. there is no concurrent
write into the same prefix), but the explicit list is what lets you
prove which bytes were loaded.

> **Native types.**  Bare autoload degrades several columns: `json` / `uuid`
> load as `BYTES`, a naive `timestamp` as `TIMESTAMP` (an instant, not
> wall-clock `DATETIME`), and arrays as a nested `RECORD`.  BigQuery will
> **not** coerce these on load — declaring native types in a load schema is
> rejected — so recover them with a post-load `CREATE TABLE … AS SELECT` over
> the staging table.  Load the staging table with
> `--parquet_enable_list_inference` (so arrays flatten with `UNNEST`), then run
> the recovery SQL that `rivet check --type-report --target bigquery` prints
> per export.  Full table:
> [type-mapping.md § BigQuery autoload & recovery](../type-mapping.md#bigquery-autoload--recovery-verified-live).

---

## Snowflake pattern

```sql
-- 1. COPY INTO a staging table, listing the exact files from manifest.json.
COPY INTO @my_stage/orders/orders_stage_<run_id>
FROM ('@my_stage/exports/2026-05-23/orders/part-000001.parquet',
      '@my_stage/exports/2026-05-23/orders/part-000002.parquet',
      ...)
FILE_FORMAT = (TYPE = PARQUET);

-- 2. Snowflake's COPY automatically deduplicates already-loaded files
--    via load history (default 14d). For longer retention or external
--    coordination, record the manifest fingerprint in a control table
--    and gate the COPY on it.

-- 3. Merge into the final table by primary key.
MERGE INTO orders target
USING orders_stage_<run_id> src
ON target.id = src.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

Snowflake's per-stage `COPY INTO` load history gives you a built-in
"don't load the same file twice" property within the retention window.
That is *not* a substitute for the manifest fingerprint check on the
client side — load history protects against accidental double-COPY,
not against loading a stale prefix from a half-finished export.

---

## When append-only is safe

Skipping the merge step is acceptable **only** when all of the
following hold:

- The source is **immutable** for the period in question (event logs,
  audit trails, time-partitioned analytics tables).
- The export carries a **stable primary key** that downstream consumers
  can use to deduplicate at query time.
- The downstream table is **partitioned by event date** so duplicate
  rows from a re-run land in the same partition and a one-time
  `DELETE WHERE _rivet_run_id NOT IN (current_run_id)` clean-up is
  cheap.

For mutable upstream tables (orders, users, accounts), always use the
merge pattern.  At-least-once + mutable source + append-only loader =
silent data corruption that is invisible until a downstream join breaks.

---

## What Rivet does *not* do downstream

- **No warehouse loader.**  Rivet writes files; the operator wires up
  BigQuery / Snowflake / Redshift / Trino / dbt.
- **No transactional coordination.**  Rivet does not coordinate with a
  downstream `MERGE` / `COMMIT`.  If the export run succeeds and the
  warehouse load fails, the operator is responsible for retry logic.
- **No dead-letter queue for poisoned parts.**  A part that fails
  warehouse parse (e.g. a Parquet version bump on the loader's side)
  is the loader's problem; Rivet's manifest still says the part is
  committed.

---

## See also

- [docs/cloud-destinations.md](../cloud-destinations.md) — the common
  output contract and the per-backend support matrix.
- [docs/semantics.md § Known non-guarantees](../semantics.md#known-non-guarantees)
  — what Rivet explicitly does not promise.
- [ADR-0012 — Cloud manifest contract (M1–M9)](../adr/0012-cloud-manifest-contract.md)
  — the formal invariants this recipe builds on.
- [docs/recipes/recover-interrupted-run.md](recover-interrupted-run.md)
  — what to do *before* you load downstream when the export itself was
  interrupted.
