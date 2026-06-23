# CDC Reference

Rivet's change-data-capture (CDC) reads a source's **transaction log** — not the
tables — and emits each `INSERT` / `UPDATE` / `DELETE` as a row change. Because it
tails the log that the database already writes for replication and durability, it
adds almost no load to the OLTP path: no table scan, no locks, no read snapshot
(see [Why CDC is gentle on the source](#why-cdc-is-gentle-on-the-source)).

> **Status.** All three engines support NDJSON streaming and typed Parquet/CSV
> `--output` (real `Timestamp` / `Date32` / `Decimal128` columns). This page
> documents all three so the **permissions** are clear up front — they are the
> part operators most need to get right.

## The command

```bash
# stream changes as NDJSON to stdout (no schema resolution, fewest privileges)
rivet cdc --source 'mysql://rivet_cdc:***@db:3306/app' --table orders

# write typed Parquet files (one row per change, after-image / upsert shape)
rivet cdc --source 'mysql://rivet_cdc:***@db:3306/app' \
          --table orders --output ./cdc-out --format parquet \
          --checkpoint ./orders.ckpt --rollover 10000
```

Prefer `--source-env VAR` or `--source-file path` over an inline URL outside local
dev — the URL is otherwise visible in `ps` / shell history.

| flag | meaning |
| ------ | --------- |
| `--server-id` | replica id for the binlog connection (MySQL). **Must be unique** — distinct from the source and every real replica. Default `4271`. |
| `--checkpoint PATH` | persist/resume the log position. Omit to tail from the current position without checkpointing. |
| `--table NAME` | only emit this table (repeatable for NDJSON; **exactly one** required for `--output`, whose schema is resolved from the source). |
| `--output DIR` | write typed Parquet/CSV files instead of NDJSON. |
| `--max-events N` | stop after N changes (otherwise stream until interrupted; the per-event checkpoint makes an interrupted run resumable). |
| `--slot NAME` | PostgreSQL logical slot (default `rivet_slot`; created if absent). |
| `--capture-instance NAME` | SQL Server CDC capture instance (e.g. `dbo_orders`) — required for `sqlserver://`. |

The engine is chosen from the URL scheme (`mysql://` / `postgresql://` /
`sqlserver://`) by `create_change_stream`, the CDC sibling of the batch
`create_source`. With `--output`, each part is uploaded through the same commit
path the batch export uses (destination + content-MD5 + transit-integrity check,
ADR-0004) and a `manifest.json` + `_SUCCESS` is written at clean end — so a
`--output gs://…` / `s3://…` works via the same `DestinationConfig`. Typed columns
(real `Timestamp` / `Date32` / `Decimal128`, not strings) flow through `RivetValue`
structural typing — for all three engines (MySQL binlog values, PostgreSQL
test_decoding parse, SQL Server change-table `ColumnData`).

## The three models

Rivet normalises three different source mechanisms behind one `ChangeStream`:

| engine | mechanism | model |
| -------- | ----------- | ------- |
| **MySQL** | binlog (ROW) streamed as a replica | push — the client reads the log directly |
| **PostgreSQL** | logical replication slot (`test_decoding`) | poll the slot via `pg_logical_slot_get_changes()` |
| **SQL Server** | `cdc.*` change tables the capture Agent extracts | poll the change function by LSN window |

MySQL and PostgreSQL expose the log to the client; SQL Server does not — there a
server-side Agent extracts the log into change tables that rivet polls.

---

## Permissions & prerequisites

### MySQL — the binlog grants

Rivet registers as a **replica** and streams the binlog. Least privilege:

```sql
CREATE USER 'rivet_cdc'@'%' IDENTIFIED BY '***';

-- read the binlog stream (register as replica, COM_BINLOG_DUMP).
-- Server-wide: REPLICATION SLAVE cannot be scoped to a database/table.
GRANT REPLICATION SLAVE  ON *.* TO 'rivet_cdc'@'%';

-- read the current binlog coordinate (SHOW MASTER STATUS) when starting
-- without a checkpoint.
GRANT REPLICATION CLIENT ON *.* TO 'rivet_cdc'@'%';

-- ONLY for `--output`: rivet resolves the table's column types with
-- `SELECT * FROM <table> LIMIT 0` (metadata only, no rows). Not needed for NDJSON.
GRANT SELECT ON `app`.`orders` TO 'rivet_cdc'@'%';

FLUSH PRIVILEGES;
```

Server configuration (`my.cnf` `[mysqld]`, or `SET GLOBAL` + restart where
allowed):

```ini
log_bin           = ON       # binary logging on (often already on for replication/PITR)
binlog_format     = ROW      # rivet needs row images, not statements — MIXED/STATEMENT will not work
binlog_row_image  = FULL     # full before/after image — REQUIRED for the after-image / MERGE shape;
                             # MINIMAL drops unchanged columns and breaks "overwrite all columns"
server_id         = 1        # any unique id for the source; rivet uses a DIFFERENT --server-id
```

Notes:

- **`REPLICATION SLAVE` is server-wide by design.** You cannot grant binlog
  access for one database only — the binlog is a single server-wide stream. Scope
  data exposure with `--table` (rivet filters client-side) and the `SELECT` grant.
- **A stale `server_id` collision silently kills the stream.** Give rivet a
  `--server-id` no other replica uses.
- `binlog_row_image = FULL` is MySQL's default; the risk is a source that has set
  it to `MINIMAL` to shrink the binlog — that path needs the column-mask MERGE,
  not the simple overwrite (see [Output shape](#output-shape)).

### PostgreSQL — the logical slot

Rivet's PostgreSQL reader consumes a logical slot through the **normal SQL
connection** (`pg_logical_slot_get_changes()`), not the streaming-replication
protocol. That changes what you must grant:

```sql
-- REPLICATION attribute: required to create and read a logical slot, even via
-- the SQL functions (pg_create_logical_replication_slot / _get_changes).
ALTER ROLE rivet_cdc WITH LOGIN REPLICATION PASSWORD '***';

-- ONLY for `--output`: schema resolution (SELECT ... LIMIT 0).
GRANT SELECT ON app.orders TO rivet_cdc;
```

Server configuration (`postgresql.conf`, needs a **restart**):

```ini
wal_level            = logical   # log enough to decode row changes (default is 'replica')
max_replication_slots = 10       # >= 1 (defaults are usually fine)
max_wal_senders       = 10       # >= 1
```

Notes:

- **No `pg_hba.conf` `replication` line is required.** That entry is for the
  streaming walsender protocol; rivet's poll model uses an ordinary connection, so
  the normal `host app rivet_cdc ...` rule suffices. (This is the main way the
  poll model is operationally lighter than streaming CDC tools.)
- **A logical slot pins WAL** until it is consumed/advanced. An abandoned slot
  prevents WAL recycling and **fills the disk** — the number-one PostgreSQL CDC
  foot-gun. Drop unused slots with
  `SELECT pg_drop_replication_slot('rivet_slot');`.
- `wal2json` / `pgoutput` are alternatives to `test_decoding`; `test_decoding` is
  always built in and needs no extension.

### SQL Server — CDC change tables

SQL Server has **no client-streamable log**. A server-side Agent job extracts the
log into `cdc.*` change tables, which rivet polls. Two distinct privilege levels:

```sql
-- ONE-TIME ENABLE (requires sysadmin or db_owner):
EXEC sys.sp_cdc_enable_db;                         -- creates the cdc schema + capture job
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo', @source_name = N'orders',
     @role_name = N'cdc_reader',                   -- gating role for readers (or NULL = no gate)
     @capture_instance = N'dbo_orders',
     @supports_net_changes = 0;

-- RUNTIME READER (what rivet connects as — least privilege):
CREATE USER rivet_cdc FOR LOGIN rivet_cdc;
GRANT SELECT ON SCHEMA::cdc TO rivet_cdc;          -- read the change tables + functions
ALTER ROLE cdc_reader ADD MEMBER rivet_cdc;        -- if a gating role was set above
```

Notes:

- **SQL Server Agent must be running.** The capture job (default ~5 s scan cycle)
  is what populates the change tables. If the Agent stops, the change tables
  silently freeze **and the transaction log can't truncate** — disk pressure. A
  production reader should watch for a non-advancing `sys.fn_cdc_get_max_lsn()`,
  not read "no rows" as "no changes".
- **Edition gate:** CDC is on Enterprise / Standard / Developer — **not Express or
  Web**. On Express, use Change Tracking instead (different, lighter, but only
  tells you *which* rows changed, not the data).
- Enabling CDC needs `sysadmin`/`db_owner`; the **runtime reader needs only the
  `SELECT` grant** above.
- **Retention:** the cleanup job keeps ~3 days by default. If rivet is offline
  longer than retention, the saved LSN falls below `sys.fn_cdc_get_min_lsn()` and
  the read errors — fall back to a full re-snapshot.

---

## Output shape

`--output` writes one row per change in the **typed after-image (upsert)** shape:

```
__op     __pos                              id   name    amount
insert   {"file":"binlog.000046","pos":681} 1    alice   100
update   {"file":"binlog.000046","pos":682} 1    alice   150
delete   {"file":"binlog.000046","pos":683} 2    bob     200
```

- `__op` — `insert` / `update` / `delete`.
- `__pos` — the engine resume position (the same value rivet checkpoints).
- the source columns, **typed** (resolved from the source schema), carrying the
  **after-image** for insert/update and the **key (before-image)** for delete.

Downstream applies it by primary key:

```sql
MERGE target t USING staged s ON t.id = s.id
WHEN MATCHED AND s.__op = 'delete' THEN DELETE
WHEN MATCHED                       THEN UPDATE SET t.* = s.*   -- overwrite all columns
WHEN NOT MATCHED AND s.__op <> 'delete' THEN INSERT (...);
```

With a full row image, *which* columns changed is irrelevant — the latest image
per key (highest `__pos`) already contains every prior change, so dedup-by-key +
overwrite is correct. This is why `binlog_row_image = FULL` matters.

Without `--output`, rivet emits the same information as NDJSON (one JSON object
per change) to stdout.

## Why CDC is gentle on the source

| | batch (`SELECT`) | CDC (log) |
| --- | --- | --- |
| touches | the **table** | the **log only** |
| locks / read snapshot | yes | **no** |
| buffer-pool eviction | yes (scans cold pages) | **no** |
| cost scales with | **table size** (re-scan) | **change rate** (deltas) |
| when it costs | actively, every run | latently (log retention / disk) |

The log is written anyway (WAL for durability, binlog for replication/PITR), so on
MySQL/PostgreSQL CDC mostly *reads what already exists* — near-zero incremental
OLTP cost. The one real CDC cost is **disk via log retention** if the consumer
lags (PG slots pin WAL; MySQL keeps binlog until read). SQL Server is the
exception: its Agent **writes** changes into change tables (extra write volume +
storage), so CDC there trades read-contention for an ongoing write/storage
overhead.

## Limitations (current)

- **Typed file output** is complete for MySQL; PostgreSQL and SQL Server currently
  carry op + table + position (typed before/after is their completion step).
- **Coarse types:** ints/floats/bool/string land typed; temporal / decimal /
  binary columns are written as strings for now (full per-type fidelity is in
  progress).
- **Checkpoint granularity** is per output file (after it is durably written); a
  commit-boundary checkpoint (never split a transaction across files) is a planned
  refinement.
- **Cloud destinations + manifest** (GCS/S3 + content-MD5 verification, as the
  batch path has) are the next layer; today `--output` writes local files.
