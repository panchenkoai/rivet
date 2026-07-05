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
| `--checkpoint PATH` | persist/resume the log position. Omit to tail from the current position without checkpointing. On the **first** checkpointed MySQL run the open position is persisted immediately (the client-side analogue of PostgreSQL's slot pinning at creation), so an idle first run still anchors the resume position — without it, changes landing between two idle scheduler cycles would be skipped. |
| `--table NAME` | only emit this table (repeatable for NDJSON; **exactly one** required for `--output`, whose schema is resolved from the source). |
| `--output DIR` | write typed Parquet/CSV files instead of NDJSON. |
| `--max-events N` | stop after N changes (otherwise stream until interrupted; the per-event checkpoint makes an interrupted run resumable). |
| `--slot NAME` | PostgreSQL logical slot (default `rivet_slot`; created if absent). |
| `--capture-instance NAME` | SQL Server CDC capture instance (e.g. `dbo_orders`) — required for `sqlserver://`. |
| `--until-current` | Catch up to the source's current log end, then **exit** instead of streaming. For MySQL this sets `BINLOG_DUMP_NON_BLOCK`; PostgreSQL / SQL Server already drain-and-exit. With `--max-events N`, the run stops at the smaller of "N events" or "end of log" — so it never blocks waiting for the N-th event. Ideal for a scheduler. |

The engine is chosen from the URL scheme (`mysql://` / `postgresql://` /
`sqlserver://`) by `create_change_stream`, the CDC sibling of the batch
`create_source`. With `--output`, each part is uploaded through the same commit
path the batch export uses (destination + content-MD5 + transit-integrity check,
ADR-0004) and a `manifest.json` + `_SUCCESS` is written at clean end — so a
`--output gs://…` / `s3://…` works via the same `DestinationConfig`. Typed columns
(real `Timestamp` / `Date32` / `Decimal128`, not strings) flow through `RivetValue`
structural typing — for all three engines (MySQL binlog values, PostgreSQL
test_decoding parse, SQL Server change-table `ColumnData`).

## From config (`rivet run`)

CDC also runs as an export in a config, so a scheduled `rivet run` captures
changes alongside batch exports and records the run the same way:

```yaml
source:
  type: mysql
  url_env: DATABASE_URL          # credentials out of the file
  tls: { mode: verify-full }     # required for a remote host (see below)
exports:
  - name: orders_cdc
    table: orders
    mode: cdc
    format: parquet
    cdc:
      checkpoint: /var/lib/rivet/orders.ckpt
      until_current: true        # drain to now and exit — for a scheduler
      # per-engine, all optional:
      server_id: 4271            # MySQL replica id
      slot: rivet_orders         # PostgreSQL logical slot
      capture_instance: dbo_orders  # SQL Server (required for sqlserver://)
    destination: { type: gcs, bucket: my-bucket, prefix: cdc/orders }
```

```bash
rivet run --config cdc.yaml      # captures, writes typed Parquet, records the run
rivet metrics                    # the CDC run appears with mode=cdc, like a batch
```

A `mode: cdc` export reuses the export's `table`, `destination`, and `format`; the
`cdc:` block carries only the CDC-specific knobs.

**`initial: snapshot` — the safe switch, enforced by construction.** On the
first run (no anchor yet) rivet performs, in order: ① create the resume anchor
(PostgreSQL slot / MySQL binlog checkpoint / SQL Server max-LSN checkpoint),
② run a full batch snapshot of each table into
`<destination>[/<table>]/snapshot/` (its own parts + `manifest.json` +
`_SUCCESS`), ③ drain the change stream. Because the anchor predates the
snapshot read, a change landing mid-snapshot appears in **both** the snapshot
and the stream — an overlap the PK + `__op` dedupe absorbs, never a gap.
Subsequent runs see the `snapshot/_SUCCESS` marker and go straight to draining;
a run that crashes mid-snapshot re-snapshots on retry (the anchor stays put, so
nothing is lost). Once any snapshot completed, a MISSING server-side anchor
(a dropped PostgreSQL slot) is a loud error, never a silent re-anchor — see
"A vanished slot" below. Load order downstream: the snapshot prefix as the base table,
then MERGE the CDC parts. MySQL / SQL Server require `cdc.checkpoint:` with
`initial: snapshot` (it is the anchor); PostgreSQL anchors in the slot.

```yaml
  - name: orders_cdc
    table: orders
    mode: cdc
    format: parquet
    cdc: { initial: snapshot, checkpoint: /var/lib/rivet/orders.ckpt, until_current: true }
    destination: { type: gcs, bucket: my-bucket, prefix: cdc/orders }
```

**Multiple CDC exports: each owns its stream resources.** A PostgreSQL slot has
ONE consumer (a shared slot is advanced past changes the other export never
read), a MySQL `server_id` has ONE connection (the server kills the older one),
and a checkpoint file has ONE writer. Config validation rejects two `mode: cdc`
exports that resolve to the same slot / `server_id` / checkpoint — **including
the defaults** (`rivet_slot`, `4271`): a multi-table CDC config must set them
explicitly per export:

```yaml
exports:
  - name: orders_cdc
    table: orders
    mode: cdc
    cdc: { slot: rivet_orders, checkpoint: /var/lib/rivet/orders.ckpt }
    ...
  - name: users_cdc
    table: users
    mode: cdc
    cdc: { slot: rivet_users, checkpoint: /var/lib/rivet/users.ckpt }
    ...
```

**Or multiplex: several tables through ONE stream (`tables:`).** N single-table
exports cost N slots — and PostgreSQL decodes the WAL once **per slot** (MySQL:
N binlog connections). One export with `tables:` rides a single slot/connection
and a single checkpoint, and routes each table's changes to its own sub-prefix
(`<destination>/<table>/`, each with its own parts + `manifest.json` +
`_SUCCESS` — exactly like N exports, minus the N−1 slots):

```yaml
exports:
  - name: app_cdc
    tables: [orders, users, payments]
    mode: cdc
    format: parquet
    cdc: { slot: rivet_app, checkpoint: /var/lib/rivet/app.ckpt, until_current: true }
    destination: { type: local, path: /data/cdc }   # → /data/cdc/orders/, /data/cdc/users/, …
```

The resume position is a property of the *stream*, so the at-least-once
sequence generalises: every table's buffered part is flushed **before** the one
checkpoint/ack advances — a crash mid-roll re-reads for all tables rather than
losing any one of them.

**`columns:` overrides on a multi-table export** support two key shapes: a bare
column name applies to **every** captured table that has it, and a qualified
`"table.column"` key targets one table and **wins** over the bare key there —
so same-named columns needing different treatments never collide:

```yaml
    columns:
      amount: "decimal(20,4)"          # every table's `amount`
      "legacy_orders.amount": text     # …except this one
```

A qualified key naming a table the export does not capture is a **config
error** (a typo must fail at load, never silently miss its target). `tables:` is PostgreSQL/MySQL only for now — SQL Server
capture instances are per-table (use one export per table there; sharing a
`capture_instance` between exports is safe, since the change-table poll is
read-only and resume state lives in the per-export checkpoint). Each run produces the standard
per-export summary block and an `export_metrics` row (rows / files / bytes /
duration / status), so CDC shows up in `rivet metrics` and the run aggregate
exactly like a batch export. **TLS:** unlike the CLI (which is loopback-only),
the config path passes `source.tls` to the change stream — so a remote source over
TLS requires the `tls:` block, and a remote host without it is refused before any
connection (the same gate the batch path uses).

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
- **Connect CDC *directly* to MySQL — not through ProxySQL / MaxScale.** The binlog
  stream is `COM_BINLOG_DUMP`, a replication protocol query proxies don't carry; the
  batch path can go through a pooler, CDC cannot. Rivet probes the connection and
  fails fast with this exact reason if it sees a proxy, so point the source at the
  MySQL host (the replication endpoint), not the proxy port.
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

## Reading from a replica (no primary access)

A common real-world constraint: you're handed a database but **only a read
replica**, never the master. Rivet reads the **log of whatever host you point
`source.url` at** — it never needs the primary *specifically*. Whether a replica
can serve that log is an engine + replica-config question, not a rivet limitation:

| engine | from a replica? | what the replica needs |
| --- | --- | --- |
| **MySQL** | ✅ yes | `log_bin = ON` **and `log_replica_updates = ON`** (`log_slave_updates` pre-8.0) so the replica re-logs replicated changes into its *own* binlog — this is **off by default**: a replica applies changes but does not re-log them without it. Plus the `REPLICATION SLAVE` / `REPLICATION CLIENT` grant and a `server_id` distinct from both the primary and the replica. |
| **PostgreSQL** | ⚠️ PG **16+** only | Logical decoding on a standby is a PostgreSQL 16 feature. On < 16 a logical slot can only be created on the primary — rivet's slot creation fails (PostgreSQL refuses logical decoding *"while in recovery"*). On 16+, set `hot_standby_feedback = on` on the standby (or a physical `primary_slot_name`) so the primary doesn't recycle WAL the standby's slot still needs. |
| **SQL Server** | ✅ yes (readable secondary) | CDC is enabled on the **primary**; the `cdc.*` change tables are ordinary tables in the database, so they **replicate to an Always On readable secondary**. Point rivet at the secondary — the reads are plain `SELECT`s on the replicated change tables. The capture job still runs on the primary; LSNs are consistent across the availability group. |

**MySQL caveat — the checkpoint is replica-local.** Rivet resumes by binlog
`{file, pos}` (not GTID), and a replica's binlog coordinates are its *own*, not the
primary's. A checkpoint taken against one replica does **not** transfer to another
host: if you fail over (to a different replica, or to the primary), re-snapshot
(`mode: full`) and restart CDC from a fresh checkpoint there.

So the answer to "can I read the log from a slave?" is **yes for MySQL (with
`log_replica_updates`) and SQL Server (readable secondary), and PostgreSQL 16+** —
point `source.url` at the replica; everything else (grants, `mode: cdc`, output) is
identical to running against a primary.

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

### Deduplicating by position, per engine

`__pos` granularity differs by engine, and the dedup recipe follows from it:

| engine | `__pos` | unique per event? |
| --- | --- | --- |
| MySQL | `{file, pos}` — the transaction's **commit** position | per *statement* under autocommit; all events of one multi-statement transaction share it |
| PostgreSQL | `{lsn}` — the transaction's **COMMIT LSN** | all events of one transaction share it |
| SQL Server | `{lsn}` — `__$start_lsn` | per transaction (rows within share it) |

Two distinct problems:

1. **At-least-once re-delivery** (a crashed run's part re-read on resume): the
   re-delivered event is **byte-identical** — same `__op`, `__pos`, and image —
   so `SELECT DISTINCT` over the staged rows (or dedup on `(pk, __pos, __op)`)
   removes it exactly.
2. **Latest-image-per-key** (the MERGE): order by `__pos`, and break the
   within-transaction tie with **file order** — rivet writes events to parts in
   stream order, so `(file name, row number in file)` completes the total order:

```sql
-- DuckDB replay: newest surviving image per key.
WITH ev AS (
  SELECT *,
         upper(lpad(split_part(__pos->>'lsn', '/', 1), 8, '0')) ||
         upper(lpad(split_part(__pos->>'lsn', '/', 2), 8, '0')) AS lsn_key   -- PostgreSQL X/Y → sortable
  FROM read_parquet('…/sessions/cdc-*.parquet', filename = true, file_row_number = true)
), latest AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (
      PARTITION BY id
      ORDER BY lsn_key DESC, filename DESC, file_row_number DESC) AS rn
    FROM ev)
  WHERE rn = 1
)
SELECT * FROM latest WHERE __op <> 'delete';
```

(MySQL: order by `(file, pos)` from `__pos` instead of `lsn_key`; SQL Server:
the fixed-width hex `lsn` string is already lexically ordered.) In a warehouse
MERGE, apply the same window to the staged batch first, then merge the winners
by PK + `__op`.

### Downstream loading

CDC output is the **same typed Parquet** the batch export writes (same
`build_arrow_field` pipeline), so the warehouse-loading recipes apply unchanged —
the engine-specific `MERGE` and the JSON-as-`BYTES` / naive-timestamp autoload
recovery are in
[recipes/idempotent-warehouse-load.md](../recipes/idempotent-warehouse-load.md)
(BigQuery) and [recipes/snowflake-load.md](../recipes/snowflake-load.md), keyed on
the PK + `__op` above.

Verified cross-engine on a CDC part: **DuckDB** reads `json` natively, **ClickHouse**
as `String` (`JSONExtract*` parses it), **BigQuery** as `BYTES` (`PARSE_JSON` after
`SAFE_CONVERT_BYTES_TO_STRING`); integers keep their width (`INT32`/`INT64`) and
timestamps their microseconds. The JSON text round-trips losslessly in all three — the
type that auto-detects differs, the data does not.

**Part naming.** Parts are run-stamped — `cdc-<run_id>-000000.parquet` — so a
scheduler re-running into the same prefix **appends** each cycle's parts alongside
the previous cycle's (nothing is overwritten). `manifest.json` / `_SUCCESS`
describe the **latest** run only; a glob reader over the prefix sees the union of
all cycles, which is the intended at-least-once stream — dedupe by PK + `__op` +
`__pos` downstream, and archive parts you have already loaded if you want the
prefix to stay small.

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

## Failure modes & recovery

Every CDC run is **bounded and resumable**, and the durable sequence is
flush → checkpoint → ack: the resume position only advances **after** the part is
durably written. So on *any* failure — a dropped connection, a query error, a full
source disk — the run **fails loudly** (non-zero exit, with the per-engine setup
hint), the checkpoint/slot is **not advanced**, and the **next run re-reads** from
the last good position. Rivet never silently loses a change; the trade-off is
at-least-once, so a failed run's already-uploaded parts can reappear — **dedupe
downstream by primary key + `__op`** (the output is the upsert / after-image shape).

A failed run leaves its durable parts in the destination but **no `manifest.json` /
`_SUCCESS`** — that pair marks a *clean* end, so a missing `_SUCCESS` is how you (and
`rivet validate`) tell a partial run from a complete one.

### PostgreSQL — the slot fills / the source disk fills

A logical slot pins WAL until rivet advances it (`confirmed_flush_lsn`). The
behaviour depends on whether rivet is **running**:

- **Running + advancing** — each successful run reads the changes, writes them
  durably, then advances the slot, so PostgreSQL **releases the WAL** up to that
  point. The slot only ever holds the WAL *since the last advance* — it does not
  grow unbounded while rivet keeps the slot moving.
- **Stopped (abandoned slot)** — rivet does nothing (it isn't running); the slot
  keeps pinning WAL and the **source disk fills**. This is the number-one
  PostgreSQL CDC foot-gun, and it is *operator* responsibility:
  `SELECT pg_drop_replication_slot('rivet_slot');` when you stop capturing for good.
- **Source disk already full** — run rivet (it *reads* WAL to advance the slot,
  which **releases** WAL and relieves the pressure) or drop the slot. If PostgreSQL
  is too degraded to answer, rivet's query fails → the run fails → re-read next run.

**Memory is O(largest transaction).** The MySQL adapter buffers a whole
transaction until its COMMIT (parts never split a transaction — the resume
invariant). Measured: ~1.4 KB of RSS per buffered row (~14× a 100-byte
payload): a 100k-row transaction drains at ~170 MB RSS, 300k at ~440 MB —
linear, so a 10M-row bulk backfill in ONE transaction will not fit. Run bulk
backfills in batched transactions, or through `mode: full`/`initial: snapshot`
(the batch path streams). Spilling oversized transactions to disk is roadmap.

**DDL inside a capture window: safe where the engine names its columns, a
LOUD ERROR where it does not.** PostgreSQL (wire text) and SQL Server (change
tables) always name every image column, so rivet maps values by NAME and a
mid-window `DROP`/`ADD COLUMN` captures correctly. MySQL's binlog carries
names only when the server runs with **`binlog_row_metadata=FULL`** (8.0.1+ —
strongly recommended; the compose test stack sets it):

```ini
# my.cnf — makes mid-stream DDL safe for rivet CDC
binlog_row_metadata = FULL
```

Under the **default `MINIMAL` the binlog is nameless and positional — expect
runs to FAIL with an explicit error** ("an event … carries N column(s) but the
resolved schema has M") whenever a DDL lands inside a capture window. That is
deliberate: mapping by position would put values into the wrong columns
silently, and a loud stop is the only safe behavior. Recover by
re-snapshotting the table (or resetting the checkpoint past the DDL), and set
`binlog_row_metadata=FULL` to retire this error class. DDL *between* runs is
always fine — each run resolves the schema fresh. A mid-window RENAME is safe
in both modes (same arity ⇒ positional fallback keeps the value). Same-arity
TYPE changes remain undetectable without schema history (roadmap) — run type
migrations and their backfills through a re-snapshot.

**The value checksum runs on CDC too.** The same always-on two-ended check the
batch export performs — an independent fold of the decoded cells vs a fold of
the built Arrow column — runs per column before every CDC part is written; a
mismatch fails the run naming the column, never writes the corrupted part.
Failure behaviour is also parity: a value unrepresentable in the declared
column (PostgreSQL `'NaN'::numeric` in a Parquet decimal) fails loudly on both
paths, never a silent NULL.

For the full operational failure playbook — every symptom, what rivet does, how to recover, how to prevent — see [cdc-failure-modes.md](cdc-failure-modes.md).

**A vanished slot is a loud error, not a silent restart.** When a resume
checkpoint exists but the slot is gone (dropped by an operator, or invalidated
and removed), rivet refuses to re-create it — a fresh slot would anchor at the
*current* position and silently skip everything since the drop. The run fails
with the re-snapshot hint; delete the checkpoint file only when you explicitly
accept a fresh anchor.

**Bound the blast radius:** set `max_slot_wal_keep_size` (PG 13+). PostgreSQL then
**invalidates the slot** rather than fill the disk; rivet's next run fails with a
slot-invalidated error and you re-snapshot. **Monitor** `pg_replication_slots`
(`active`, and `restart_lsn` vs the current LSN = how much WAL the slot is holding).

> **`rivet doctor` automates this monitoring.** For a config with `mode: cdc`
> exports, doctor probes the engine: PostgreSQL — the export's slot (retained
> WAL, fails above 1 GiB) **and any other inactive slot pinning WAL** (the
> abandoned-slot foot-gun); MySQL — `log_bin`/`binlog_format=ROW`/
> `binlog_row_image=FULL`, and whether the checkpoint's binlog file is still
> retained (a purged file is reported *before* the run fails with ERROR 1236);
> SQL Server — CDC enabled, the capture instance exists, the checkpoint is
> within retention, and the Agent service is running.

### MySQL — the binlog was purged

If rivet is offline long enough that the saved binlog position is **purged**
(`binlog_expire_logs_seconds` / `PURGE BINARY LOGS`), the resume read fails with
MySQL **ERROR 1236** (the requested binlog file is gone). The position is
unrecoverable — **re-snapshot** (`mode: full`) and restart CDC from a fresh
checkpoint. Size binlog retention comfortably above your CDC cadence.

### SQL Server — the checkpoint fell below retention

If the saved LSN falls **below** `sys.fn_cdc_get_min_lsn()` (the cleanup job — ~3
days by default — removed the changes after it), rivet **fails loudly** — *"the
resume position is older than the change-table retention … re-snapshot"* — rather
than resume from the new min and **silently skip the gap**. Re-snapshot and restart
from a fresh checkpoint. Also watch for a **non-advancing `sys.fn_cdc_get_max_lsn()`**:
that means the **Agent capture job stopped**, so the change tables are frozen — read
"no rows" as "the job is down", not "no changes".

### Recovery, in one line

Re-run to resume from the last checkpoint (the common case). If the run reports the
position is unrecoverable (PostgreSQL slot invalidated, MySQL binlog purged, SQL
Server retention exceeded), **re-snapshot the table with `mode: full` and restart
CDC from a new checkpoint** — the only safe recovery once the source log no longer
covers the gap.

## Limitations (current)

Typed output (real `Timestamp`/`Date32`/`Decimal128`), commit-boundary
checkpointing, cloud destinations + `manifest.json`/`_SUCCESS`, and the
config-driven `rivet run` path with a recorded run are all in place for all three
engines. What remains:

- **Continuous capture for PostgreSQL / SQL Server** is poll-once-and-exit (they
  drain their backlog and stop); a long-running daemon reconstructs the stream each
  cycle. The supported continuous model today is a scheduler running
  `--until-current` (or `rivet run` with `cdc.until_current`) on an interval, each
  run resuming from the checkpoint. MySQL streams continuously without `--until-current`.
- **Schema drift:** the sink schema is frozen at the first flush — a column added
  mid-run is not picked up until the next run re-resolves the table.
- **No lag metric:** the run records rows / files / bytes / duration / status, but
  not replication lag ("how far behind the source is") — the next observability step.
- **Pre-image completeness** depends on the source config: full UPDATE/DELETE
  before-images need `binlog_row_image=FULL` (MySQL) / `REPLICA IDENTITY FULL`
  (PostgreSQL); otherwise only key columns are carried.
- **Type parity with the batch export is total**: every Rivet-mapped type —
  including PostgreSQL arrays (real `List` columns, inner NULLs preserved) and
  `NUMERIC`/`DECIMAL` above precision 38 (`Decimal256`) — is byte-identical to
  the batch export, enforced per engine by the live
  `*_full_type_matrix_matches_batch` tests (ArrayData equality).
