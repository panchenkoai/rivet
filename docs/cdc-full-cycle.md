# The full CDC cycle, step by step — every engine

The operator's sequence for a `mode: cdc` export from nothing to a warehouse
table that follows the source: preflight → anchor + baseline → load → changes →
load → an interruption on the CDC leg → load → an idle cycle. Each step names
what to run and what must be true afterwards, checked by two readers that share
nothing with rivet: the source itself (`COUNT(*)`) and the warehouse (`bq`).

The same sequence runs unattended as
`full_cdc_cycle_{mysql,postgres,mssql,mongo}` in
`tests/live/live_cdc_full_cycle.rs` — one body, four engines, through the Rig.

## 0. Prerequisites per engine

| engine | what the log needs | anchor model | `cdc.checkpoint:` |
|---|---|---|---|
| MySQL | `binlog_format=ROW`, `binlog_row_image=FULL`, a user with `REPLICATION SLAVE, REPLICATION CLIENT`; on RDS/Aurora: automated backups ON (retention > 0, else `log_bin=0`, ERROR 1381) and `CALL mysql.rds_set_configuration('binlog retention hours', N)` (ERROR 1236 otherwise) | client-side file — `{file, pos, server_uuid, gtid_executed}` | **required** for any `mode: cdc` |
| PostgreSQL | `wal_level=logical`, `max_replication_slots ≥ 1`, a role with `REPLICATION` | server-side slot | not needed (the slot is the anchor) |
| SQL Server | SQL Server Agent running, `sys.sp_cdc_enable_db`, `sys.sp_cdc_enable_table` per table (one `cdc:` export per table — `tables:` is refused) | from-LSN floored at `fn_cdc_get_min_lsn` | required for a baseline |
| MongoDB | a replica set (change streams), `directConnection` if port-mapped | resume token | **required** for any `mode: cdc` |

`rivet doctor --config cfg.yaml` checks all of it and prints the fix per line.

## 1. The config: one CDC export, one recipe, one `load:`

```yaml
source: { type: mysql, url_env: SOURCE_URL }
exports:
  - name: orders                       # the RECIPE: how to read the table
    table: orders
    mode: chunked
    chunk_by_key: id
    chunk_size: 250000
    chunk_checkpoint: true
    parallel: 4
    destination: { type: gcs, bucket: my-bucket, prefix: "exports/orders/" }

  - name: stream                       # the CDC export
    tables: [orders]
    mode: cdc
    cdc:
      checkpoint: ./cdc/stream.ckpt
      backfill: auto                   # baseline through the recipe, after the anchor
      until_current: true
    destination: { type: gcs, bucket: my-bucket, prefix: "exports/stream/" }

load: { target: bigquery, project: my-proj, dataset: my_ds, pk: auto }
```

- `rivet init --source-env SOURCE_URL --mode cdc` over two or more tables (MySQL,
  PostgreSQL `public`) scaffolds exactly this shape: one recipe per table — keyset
  where the table has a single-column keysettable key, range or `full` otherwise —
  and one `tables:` stream with `backfill: auto`. A single table, SQL Server,
  MongoDB or a non-`public` schema get a per-table capture-only stream instead
  (add `initial: snapshot` or a recipe + `backfill:` yourself). Add the `load:`
  block and run.
- A stream over several tables rarely shares one partition column or one key.
  Put the per-table layer on the stream's own `load:` block:
  `load: { partition: { column: created_at, granularity: day }, tables: { customers:
  { partition: none }, line_items: { pk: [id, line_no] } } }` — each table's block
  overrides the export's, which overrides the top-level `load:`.
- `orders` is a **read recipe**: a whole-config `rivet run` (and `rivet apply`)
  skips it — at `warn` — and the CDC export runs it after the anchor into its
  own `exports/stream/orders/snapshot/`. `rivet run -e orders` still exports it
  alone. It is never a load target.
- Only a `full` or `chunked` recipe is admitted; an `incremental` one reads a
  slice and is refused at config load, before any anchor exists.
- A column typed on the recipe (`columns:`) reaches the baseline, the stream
  and the recorded load spec alike — one type per column in one `__changes`.

## 2. Preflight

```sh
rivet check  --config cfg.yaml     # grades the CDC export as a log reader, not a scan
rivet doctor --config cfg.yaml     # binlog/slot/Agent/replica-set readiness, per line
rivet plan   --config cfg.yaml     # plans the batch exports; skips the stream and the recipe, saying so
```

Expect: no DEGRADED/UNSAFE on the CDC export; every doctor line green. `plan`
skips the stream and the recipe, saying so — on the §1 config that leaves nothing
to plan and it stops with "nothing to plan" (expected, not a failure of the
config); with a plain batch export alongside it plans that one and exits 0.

## 3. Run 1 — anchor, then baseline — then load 1

```sh
rivet run  --config cfg.yaml
rivet load --config cfg.yaml
```

Order inside the run: **anchor first** (checkpoint pinned / slot created), then
the baseline read through the recipe, then the drain of whatever changed during
the baseline. A change landing mid-baseline is therefore in both — a duplicate,
which the dedup view absorbs — never in neither.

Check:

```sql
-- source
SELECT COUNT(*) FROM orders;
-- warehouse (bq query --use_legacy_sql=false)
SELECT COUNT(*) FROM `my-proj.my_ds.orders`;
```

Both equal. The warehouse object is a plain table after run 1.

If the baseline is interrupted (a kill, a statement timeout on one chunk), just
run again: a `chunk_checkpoint: true` recipe resumes its own leg on the next
plain `rivet run` — no `--export <leg> --resume`, no synthesized names.

## 4. Changes → run 2 → load 2

Insert, update and delete a few rows at the source, then:

```sh
rivet run  --config cfg.yaml
rivet load --config cfg.yaml
```

Check:

- `orders` is now a **view** over `orders__changes`. A deleted key stays in it
  as a tombstone with `__is_deleted = TRUE` — the disappearance is data too — so
  live state is `WHERE NOT __is_deleted`:

  ```sql
  SELECT COUNT(*), COUNT(DISTINCT id) FROM `my-proj.my_ds.orders` WHERE NOT __is_deleted;
  ```

  Both equal the source's `COUNT(*)`.
- `orders__changes` grew by exactly the number of changed rows — an update is
  one row, a delete is one row with `__op = 'delete'`.
- A second `rivet load` with no new run appends nothing (the load ledger).

The load into a changelog is **always an append**: `LOAD DATA INTO`, never an
overwrite, unless you asked for `--rebuild-changelog`.

## 5. An interruption ON THE CDC LEG → run 3 → load 3

Make more changes, start `rivet run`, and kill it while it is draining (`kill
-9`; the automated scenario injects `RIVET_TEST_PANIC_AT=cdc_after_flush_before_ack`
and `cdc_after_ack`). Then simply:

```sh
rivet run  --config cfg.yaml
rivet load --config cfg.yaml
```

Check: the view equals the source, one row per key. `orders__changes` may hold
a change twice if the kill landed after the part was flushed but before the
checkpoint advanced — that is at-least-once, and the view collapses it. What
must never happen is a change in neither.

## 6. Idle cycle

```sh
rivet run  --config cfg.yaml   # nothing changed
rivet load --config cfg.yaml   # "up to date"
```

Check: `orders__changes` did not grow; the view still equals the source.

## Recovery orders that matter

- **Log gone** (slot invalidated, binlog purged — ERROR 1236, MSSQL below
  retention): re-anchor FIRST (delete the checkpoint / accept a fresh slot),
  THEN re-baseline. Re-baselining first leaves every change in between in
  neither.
- **MySQL checkpoint used against another server**: refused on purpose; same
  order on the new host.
- **`rivet validate --config cfg.yaml`** certifies both legs — the baseline under
  `snapshot/` and the change parts — and never reports the baseline as stray.

## Running the automated scenario

```sh
docker compose --profile cdc up -d
export BIGQUERY_TEST_PROJECT=<gcp-project> RIVET_TEST_GCS_BUCKET=<bucket>   # RIVET_TEST_BQ_DATASET optional
cargo test --test live_suite full_cdc_cycle -- --ignored --test-threads=1
```

Without the warehouse env the four tests skip, by name.
