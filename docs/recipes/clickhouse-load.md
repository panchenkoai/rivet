# Loading rivet Parquet into ClickHouse (preview)

> **Status: Preview.** Live-tested against ClickHouse 24.8 (the stand's `clickhouse`
> compose service). See [Known limits](#known-limits) and
> [engine maturity](../engine-maturity.md) for what is still open before GA.

`rivet load` writes an export's Parquet into ClickHouse over the HTTP interface
([ADR-0035](../adr/0035-clickhouse-load-target.md)). The export may land in GCS, S3
or Azure (`rivet init` takes `--gcs-bucket` or `--s3-bucket`); the ClickHouse
database must already exist.

## Generate the config

```bash
export DATABASE_URL="postgresql://user:pass@host/db"
export CLICKHOUSE_PASSWORD=...

rivet init --source-env DATABASE_URL --mode cdc --tls verify-full \
  --gcs-bucket my-bucket \
  --clickhouse-url http://clickhouse:8123 --clickhouse-database raw --clickhouse-user loader \
  -o rivet.yaml

rivet run  -c rivet.yaml    # Parquet into GCS
rivet load -c rivet.yaml    # Parquet into ClickHouse
```

A CDC scaffold captures changes from its anchor on; rows that existed before are not
in it. For them, set `cdc.initial: snapshot` (or a `cdc.backfill:`) before the first run.

The generated block:

```yaml
load:
  target: clickhouse
  url: http://clickhouse:8123
  database: raw
  user: loader
  password_env: CLICKHOUSE_PASSWORD
  pk: auto
  cluster_by: auto
  cleanup_source: true
```

## One cycle is `run` + `load` — no compact step

```bash
rivet run  -c rivet.yaml
rivet load -c rivet.yaml
```

Put those two lines on the schedule. There is no third step: a CDC table's change
log is a `ReplacingMergeTree(__ver)`, and ClickHouse itself collapses the versions
of a key in its background merges. The view `<table>` reads the log with `FINAL`, so
it returns one row per key — the latest version — whether or not those merges have
run yet. A change delivered twice (at-least-once after an interrupted run, or a
re-run load) carries the same key and version, so it collapses the same way.

`rivet compact` on a ClickHouse config does nothing: it passes a change-log table
by with "this warehouse keeps a change log behind a view and never compacts; nothing
to merge" (and a full-load table with "a full load overwrites its table"). A deleted key stays in the log as its last version with `__is_deleted`
set, so live state is `WHERE NOT __is_deleted`.

## What lands

| Export mode | In ClickHouse |
|---|---|
| `full`, `chunked`, `time_window` | `<table>`, a `MergeTree` replaced whole by every load (filled beside it, then swapped in). Not a run that continues past its last key (`keyset_incremental`, Mongo `source.mongo.resume`): it holds only new keys and loads like `incremental` |
| `cdc` | `<table>__changes`, a `ReplacingMergeTree` keyed on the primary key, and the view `<table>` |
| `incremental` | the first run lands `<table>` as a `MergeTree`; the first delta renames it to `<table>__changes` and `<table>` becomes a view picking the latest cursor per key. A continued-key export never lands `<table>` as a table: its first run already goes to `<table>__changes` behind the view |

For a CDC table the engine keeps one version per key: the highest version, computed
from the change's source position (PostgreSQL LSN, MySQL binlog file number + offset,
SQL Server LSN) and its order within the transaction. Insert order does not matter as
long as the source's positions only grow; a MySQL binlog renumbered by `RESET MASTER`
or a failover breaks that (see [Known limits](#known-limits)). The view reads the log
with `FINAL` and flags deletes:

```sql
SELECT * FROM raw.orders WHERE NOT __is_deleted;
```

ClickHouse does not allow `PREWHERE` on the view. If you read `<table>__changes FINAL`
directly, filter non-key columns in `WHERE`: a `PREWHERE` runs before the engine
collapses versions and can return an old one.

## Partitions

`partition: { column, granularity }` partitions every table the load creates (the
full-load table, and the change log of a CDC or incremental export) by a `Date32` or
`DateTime64` column:

| `granularity` | `PARTITION BY` | partition id of 2026-03-10 14:05 |
|---|---|---|
| `hour` (timestamps only) | `intDiv(toYYYYMMDDhhmmss(c), 10000)` | `2026031014` |
| `day` | `toYYYYMMDD(c)` | `20260310` |
| `month` | `toYYYYMM(c)` | `202603` |
| `year` | `toYear(c)` | `2026` |

These functions are exact over the whole 1900–2299 range. `toDate` and
`toStartOfHour` would put a 1950 row in a 2129 partition, because they wrap outside
1970–2106. A NULL value gets a partition of its own.

```yaml
load:
  target: clickhouse
  # …
  partition: { column: created_at, granularity: month }
```

- **Pick `month` or coarser.** ClickHouse refuses an insert block that touches more
  than 100 partitions (`max_partitions_per_insert_block`, "Too many partitions for
  single INSERT block"). A part holding more than 100 days of history therefore fails
  under `day`. `rivet init` guesses `month` for ClickHouse.
- **A CDC log partitioned by a column that changes.** The engine merges the versions of
  a key only within one partition. When an update moves a row to another partition,
  its older version stays in the old partition for good, and `OPTIMIZE … FINAL` does
  not remove it either (measured on 24.8.14). The view still returns one row per key,
  the latest, because `FINAL` compares versions across partitions. The view pins
  `do_not_merge_across_partitions_select_final = 0`, so a profile that turns that
  setting on does not change it. The cost is storage and a slower `FINAL`. The load
  warns when a CDC export partitions by anything but a creation stamp (`created_at`,
  `CreatedDate`, …).
- **Change the partition before the first load, not after.** A change log that exists
  already keeps its partition. A load that declares a different one is refused before
  it writes anything, and the refusal names both. A full-load table is created again on
  every load, so it always takes the partition the config declares.
- **Not on ClickHouse:** `range:` (BigQuery's integer ranges), `ingestion:` (use
  `column: _rivet_exported_at`), `expiration_days` and `require_filter` (a TTL is the
  table owner's decision). Each is refused by name.

## Letting ClickHouse read the bucket itself

By default rivet reads each part from GCS and sends it to ClickHouse. With a
named collection ClickHouse reads the part directly, and no data passes through
the host running rivet (rivet still reads each part's footer from the store, to
check its timestamps and count its rows):

```sql
-- once, as an administrator. GCS: HMAC keys from "Interoperability"; S3: the service
-- endpoint (e.g. https://s3.<region>.amazonaws.com/ — rivet appends the bucket) and keys;
-- Azure: a connection string (the container is the export's bucket).
CREATE NAMED COLLECTION gcs_raw AS
  url = 'https://storage.googleapis.com/',
  access_key_id = '...',
  secret_access_key = '...';
CREATE NAMED COLLECTION azure_raw AS connection_string = '...';
GRANT NAMED COLLECTION ON gcs_raw TO loader;
```

```yaml
load:
  target: clickhouse
  # …
  named_collection: gcs_raw
```

## Known limits

- **Timestamp range.** `DateTime64` holds 1900-01-01 to 2299-12-31, and ClickHouse
  stores a value outside it as the nearest end, silently. So rivet reads each part's
  footer first, whether it sends the part or ClickHouse pulls it through a named
  collection, and refuses the part, inserting nothing, if a timestamp column holds a
  value outside that range or has no min/max statistics
  (`RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE`). A `Date32` outside the same range fails the
  insert: ClickHouse refuses it itself (measured on a part rivet sends).
- **Types that land as something else.** `uuid` lands as `FixedString(16)` (the 16 raw
  bytes; the type report carries the `toUUID` expression to recover it), `json`/`jsonb`
  as `String` holding the JSON text, `time` as `Decimal64` seconds since midnight, and a
  NULL array as `[]` (a ClickHouse `Array` cannot be NULL). `rivet check --type-report --target clickhouse` lists each one.
- **Retries.** Every statement is one HTTP request with a 1200-second timeout, tried up
  to 5 times. A request that never connected is always tried again. A lost answer, a
  429/502/503/504, or "too many simultaneous queries"/"too many parts" is tried again
  only where a second run changes nothing: catalog reads, the `IF NOT EXISTS` /
  `OR REPLACE` DDL, and inserts into a CDC change log (the engine collapses the copy).
  An insert into a full load's swap table or an incremental log, and the `EXCHANGE` /
  `RENAME` that swap a table in, are not: the load fails and the next `rivet load`
  starts that table over (a full load keeps serving the old table until its swap).
  ADR-0035 CH13 lists every statement.
- **MySQL binlog renumbering.** The version orders MySQL changes by binlog file number,
  then offset. After `RESET MASTER`, or a failover to a server whose binlog files are
  numbered lower, new changes carry lower versions and lose to older versions of the
  same keys.
- **Grants.** The load's user needs, on the target database (measured on 24.8):

  ```sql
  GRANT SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, DROP TABLE,
        CREATE VIEW, DROP VIEW ON raw.* TO loader;
  ```

  `DROP TABLE` covers the full load's `CREATE OR REPLACE` of its swap table and the
  `EXCHANGE TABLES` that swaps it in; the catalog reads (`system.tables`,
  `system.columns`) need nothing more. A pulled load also needs
  `GRANT NAMED COLLECTION ON <name>`.
- **TLS.** An `https://` URL uses rustls with the Mozilla root certificates built into
  rivet. For a server certificate signed by a private CA, set `ca_file:` in the `load:`
  block to a PEM file of its root certificate(s) (an absolute path; a relative one is
  read from the directory rivet runs in); a file that is missing or holds no PEM
  certificate fails the load before any statement. Not yet tested against a live
  `https://` server: the stand's ClickHouse serves only HTTP.

## Not supported

- **MongoDB CDC** into ClickHouse: the resume token has no integer order the
  change log can version by. Load it into BigQuery or Snowflake.
- **`rivet compact`** and **`layout: base_buffer`**: the engine collapses the log
  itself, so there is nothing to merge.
- **A CDC stream over a table from an earlier full load**: refused; drop or
  rename the table first. The change log holds only changes from the stream's
  anchor on, so to keep the table's existing rows also set `cdc.initial: snapshot`
  (or a `cdc.backfill:`) and `rivet run` again before the load; the stream is
  already anchored, so the snapshot overlaps it and nothing falls between them.
- **A primary-key update** leaves the old key live, as on every warehouse
  ([ADR-0030](../adr/0030-primary-key-update-representation.md)).
