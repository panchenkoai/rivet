# Loading rivet Parquet into ClickHouse

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

## What lands

| Export mode | In ClickHouse |
|---|---|
| `full`, `chunked`, `time_window` | `<table>`, a `MergeTree` replaced whole by every load (filled beside it, then swapped in) |
| `cdc` | `<table>__changes`, a `ReplacingMergeTree` keyed on the primary key, and the view `<table>` |
| `incremental` | the first run lands `<table>` as a `MergeTree`; the first delta renames it to `<table>__changes` and `<table>` becomes a view picking the latest cursor per key |

For a CDC table the engine keeps one version per key: the one with the latest
source position (PostgreSQL LSN, MySQL binlog file + offset, SQL Server LSN),
whatever order the parts were inserted in. The view reads it with `FINAL` and
flags deletes:

```sql
SELECT * FROM raw.orders WHERE NOT __is_deleted;
```

ClickHouse does not allow `PREWHERE` on the view. If you read `<table>__changes FINAL`
directly, filter non-key columns in `WHERE`: a `PREWHERE` runs before the engine
collapses versions and can return an old one.

## Letting ClickHouse read the bucket itself

By default rivet reads each part from GCS and sends it to ClickHouse. With a
named collection ClickHouse reads the part directly, and no data passes through
the host running rivet:

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

## Not supported

- **MongoDB CDC** into ClickHouse: the resume token has no integer order the
  change log can version by. Load it into BigQuery or Snowflake.
- **`partition:`**: a change log collapses versions only within a partition.
- **`rivet compact`** and **`layout: base_buffer`**: the engine collapses the log
  itself, so there is nothing to merge.
- **A CDC stream over a table from an earlier full load**: refused; drop or
  rename the table first.
- **A primary-key update** leaves the old key live, as on every warehouse
  ([ADR-0030](../adr/0030-primary-key-update-representation.md)).
