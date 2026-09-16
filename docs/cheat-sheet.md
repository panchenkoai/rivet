# Rivet Cheat Sheet

One page covering setup, extract, load and verification. Every command reads the
same YAML config (`-c rivet.yaml`). Full references: [reference/cli.md](reference/cli.md) ·
[reference/config.md](reference/config.md) · [reference/cdc.md](reference/cdc.md).

```
init → doctor → check → run → validate / reconcile → load
```

**Command builder.** On the docs site, fill in the form below and every command
and config on this page is rewritten with your values, ready to copy. On GitHub
the form is not rendered: replace the `{{…}}` placeholders by hand.

<div id="rivet-builder"></div>

---

## 0. Prerequisites: cloud and warehouse

Choose the destination and load target in the form, and the blocks below switch
to the matching setup. Skip any step you have already done.

### 0.1 Destination bucket and credentials

```bash
{{CLOUD_SETUP}}
```

| Destination | Auth rivet supports | Minimum rights |
|---|---|---|
| **GCS** | ADC (`gcloud auth application-default login`), or a service-account key (`GOOGLE_APPLICATION_CREDENTIALS` / `credentials_file:`) | `roles/storage.objectAdmin` on the bucket (create, get, list, delete) |
| **S3** | temporary keys from `aws configure export-credentials` (+ `session_token_env`), static IAM keys (`access_key_env` / `secret_key_env`), or a static-key `aws_profile:` | `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket`, `s3:GetBucketLocation` |
| **Azure** | storage account key (`account_key_env`) or SAS token (`sas_token_env`) | account key = full access; SAS needs `rwdlc` |

- rivet does not read `AWS_PROFILE`, and SSO/login profiles do not work as
  `aws_profile:`. Export temporary credentials instead (the `sso` option above).
- Temporary AWS keys expire, usually after about an hour. Re-run the
  `export-credentials` line before the next run.
- `rivet load` reads **GCS only**: both BigQuery and Snowflake need `destination: type: gcs`.
- `rivet doctor -c rivet.yaml` confirms the credentials can write to the prefix.
  On cloud storage it leaves a `.rivet_doctor_probe` object behind.

More auth paths (MinIO, Azurite, SAS details): [cloud-auth.md](cloud-auth.md) ·
[cloud-permissions.md](cloud-permissions.md).

### 0.2 Warehouse for `rivet load`

```bash
{{LOAD_SETUP}}
```

```sql
{{LOAD_SQL}}
```

- **BigQuery:** the dataset must already exist, because rivet does not create
  datasets. Keep it in the same location as the bucket.
- **Snowflake:** database, schema and storage integration must already exist.
  rivet creates the file format, stage, tables and views itself, which is why the
  role needs the `CREATE` grants above.

---

## 1. Setup

### 1.1 Batch (full / incremental / chunked)

```bash
brew install panchenkoai/rivet/rivet          # or: cargo install rivet-cli

# Credentials: the password is read without echo, never typed into the command line
printf 'DB password: '; read -rs DB_PASS; echo
export DATABASE_URL="{{URL}}"

# Scaffold a config from the live database
rivet init --source-env DATABASE_URL --table {{TABLE}} --tls {{TLS}}{{INIT_DEST}} -o rivet.yaml    # one table
rivet init --source-env DATABASE_URL --schema {{SCHEMA}} --tls {{TLS}}{{INIT_DEST}} -o rivet.yaml  # whole schema
rivet init --source-env DATABASE_URL --schema {{SCHEMA}} --include 'order*' --exclude '*_tmp' --tls {{TLS}} -o rivet.yaml
rivet init --source-env DATABASE_URL --tls {{TLS}} --discover -o discovery.json   # JSON: row estimates, cursor/chunk candidates

# Preflight
rivet doctor -c rivet.yaml                     # source + destination auth / connectivity
rivet check  -c rivet.yaml                     # EXPLAIN, index checks, verdict per export
rivet check  -c rivet.yaml --type-report --target {{LOAD_KIND}}   # bigquery | snowflake | duckdb | clickhouse
rivet check  -c rivet.yaml --strict            # non-zero exit on any unsafe type mapping
```

Minimal config:

```yaml
source:
  type: {{SOURCE_TYPE}}           # postgres | mysql | mssql | mongo
  url_env: DATABASE_URL           # or url_file: / host+user+password_env+database
  tls: { mode: {{TLS}} }          # disable | require | verify-ca | verify-full (+ ca_file:)
exports:
  - name: {{NAME}}
    table: {{TABLE}}              # or query: / query_file:
    mode: full                    # full | incremental | chunked | time_window | cdc
    format: parquet               # parquet | csv
    compression_profile: balanced # none | fast | balanced | compact
    destination: {{DEST}}
```

All destination shapes:

```yaml
destination: { type: local, path: ./output }
destination: { type: gcs,   bucket: my-bucket, prefix: exports/orders/ }
destination: { type: s3,    bucket: my-bucket, prefix: exports/orders/, region: us-east-1 }
destination: { type: azure, bucket: my-container, account_name: acct, account_key_env: RIVET_AZURE_KEY, prefix: exports/ }
```

> `.rivet_state.db` (cursors, checkpoints, run history) is created next to the
> config. Add it to `.gitignore`.

### 1.2 CDC

Scaffold:

```bash
rivet init --source-env DATABASE_URL --mode cdc --table {{TABLE}} --tls {{TLS}} -o cdc.yaml
rivet init --source-env DATABASE_URL --mode cdc --tls {{TLS}} -o cdc.yaml   # whole DB: one `tables:` export (PG/MySQL),
                                                                             # one export per table (SQL Server)
rivet doctor -c cdc.yaml    # also probes slot WAL retention / binlog config / CDC Agent + retention
```

Source prerequisites:

| Engine | Server config |
|---|---|
| **PostgreSQL** | `wal_level=logical` (restart), `max_replication_slots>=1`, `max_wal_senders>=1` |
| **MySQL** | `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL` (recommended), binlog retention ≫ the run interval |
| **SQL Server** | SQL Server Agent running; Enterprise / Standard / Developer (not Express/Web) |
| **MongoDB** | Replica set required (`?directConnection=true` for a port-mapped single node) |

Grants for the selected engine:

```sql
{{CDC_GRANTS}}
```

Rules of thumb:

- MySQL: connect **directly**, not through ProxySQL/MaxScale. Give rivet a unique `server_id`.
- **MySQL on RDS / Aurora: two settings that are not in `my.cnf`.** Binary logging
  follows automated backups — with retention at 0 the instance runs `log_bin = 0`
  and every binlog query answers `ERROR 1381`, whatever the parameter group says.
  And retention is *not* `binlog_expire_logs_seconds`: RDS purges a binlog as soon
  as the engine no longer needs it, so the next run's resume dies with `ERROR 1236`
  (measured: a checkpoint taken at 13:42 was already past retention at 13:59).
  Set it explicitly, well above the run interval:
  `CALL mysql.rds_set_configuration('binlog retention hours', 72);`
  A read replica also needs `log_replica_updates = 1`.
- PostgreSQL: an abandoned slot pins WAL and fills the disk. Drop it with
  `SELECT pg_drop_replication_slot('{{SLOT}}');`. Set `max_slot_wal_keep_size` to cap it.
- SQL Server: change-table retention defaults to about 3 days. A run that falls
  behind it fails loudly and needs a re-snapshot.
- Reading from a replica works on MySQL (`log_replica_updates=ON`), on SQL Server
  (readable secondary) and on PostgreSQL 16+ standbys.

CDC config:

```yaml
source:
  type: {{SOURCE_TYPE}}
  url_env: DATABASE_URL
  tls: { mode: {{TLS}} }
exports:
  - name: {{NAME}}_cdc
    table: {{TABLE}}               # or tables: [a, b]  (one stream, PG/MySQL only)
    mode: cdc
    format: parquet
    cdc:
      initial: snapshot            # first run: anchor → full snapshot → drain stream
      checkpoint: {{CKPT_DIR}}/{{NAME}}.ckpt   # required for MySQL/MSSQL with initial: snapshot
      until_current: true          # default: drain to the log end as of open, then exit
      {{CDC_PARAM}}
      # rollover: 100000           # rows per part (≈ drain memory)
    destination: {{CDC_DEST}}
```

> With several `mode: cdc` exports, each one needs its own `slot`, `server_id`
> and `checkpoint`. The defaults collide, and config validation rejects them.

---

## 2. Extract

### 2.1 Batch

```bash
rivet run -c rivet.yaml                         # all exports
rivet run -c rivet.yaml -e {{NAME}}             # one export
rivet run -c rivet.yaml --validate --reconcile  # verify files + source COUNT(*) match
rivet run -c rivet.yaml --parallel-exports      # exports concurrently (threads)
rivet run -c rivet.yaml --parallel-export-processes   # one child process per export
rivet run -c rivet.yaml -p day=2026-09-14       # substitutes ${day} in queries
rivet run -c rivet.yaml --json --summary-output run.json
rivet run -c rivet.yaml --resume                # continue a crashed chunked run (chunk_checkpoint: true)

# Many tables: plan waves, then apply
rivet plan  -c rivet.yaml                       # read-only schedule
rivet plan  -c rivet.yaml --annotate-waves      # write wave:/parallel_safe: into the config
rivet apply rivet.yaml                          # wave by wave
rivet apply rivet.yaml --resume                 # skip exports with _SUCCESS, resume the rest
rivet apply rivet.yaml --pool 4 --split         # work-stealing pool; split one dominant table
rivet plan  -c rivet.yaml -e {{NAME}} -o plan.json && rivet apply plan.json   # sealed replay
```

Mode snippets:

```yaml
# incremental: only rows past the stored cursor
mode: incremental
cursor_column: {{CURSOR}}
skip_empty: true
settle: { after: 1h }            # optional: export a row only once it is ≥1h old (s/m/h/d)
                                 # settle.column defaults to the cursor

# chunked, range key
mode: chunked
chunk_column: {{PK}}
chunk_size: 100000
parallel: 4
chunk_checkpoint: true           # enables --resume

# chunked, keyset (unique NOT NULL key, immune to sparse keys)
mode: chunked
chunk_by_key: {{PK}}
chunk_checkpoint: true           # crash recovery only
# keyset_incremental: true       # append-only tables: a clean re-run pulls only new keys

# time_window: rolling N days
mode: time_window
time_column: created_at
days_window: 30
```

Inspect state:

```bash
rivet state show         -c rivet.yaml               # incremental cursors
rivet state reset        -c rivet.yaml -e {{NAME}}   # re-export from scratch
rivet state chunks       -c rivet.yaml -e {{NAME}}   # chunk checkpoint status
rivet state reset-chunks -c rivet.yaml --stuck-checkpoints
rivet state progression  -c rivet.yaml               # committed / verified boundaries
rivet state runs         -c rivet.yaml --running     # run-status ledger
rivet state finish-run   -c rivet.yaml --run-id <id> # close a known-dead `running` row
rivet metrics            -c rivet.yaml --last 10
rivet journal            -c rivet.yaml -e {{NAME}}   # events, retries, quality issues
```

### 2.2 CDC

Config-driven (recommended: cloud destinations, TLS, recorded runs):

```bash
rivet run -c cdc.yaml                            # bounded: drain, write parts, checkpoint, exit
rivet run -c cdc.yaml --parallel-export-processes   # SQL Server per-table exports in parallel
rivet metrics -c cdc.yaml                        # CDC runs appear with mode=cdc
```

Schedule `rivet run` on an interval. Each run resumes from the checkpoint or slot.
`cdc.until_current: false` streams continuously, but only MySQL and MongoDB stay
up that way. PostgreSQL and SQL Server still exit on catch-up.

Ad-hoc CLI (loopback hosts only, since it has no TLS):

```bash
rivet cdc --source-env DATABASE_URL --table {{TABLE}}{{CDC_FLAG}}          # NDJSON to stdout
rivet cdc --source-env DATABASE_URL --table {{TABLE}}{{CDC_FLAG}} \
          --output ./cdc-out --format parquet --checkpoint ./{{NAME}}.ckpt   # typed Parquet (local dir)
rivet cdc --source-env DATABASE_URL --table {{TABLE}}{{CDC_FLAG}} --max-events 10000   # soft cap, stops at a commit boundary
rivet cdc --source-env DATABASE_URL --table {{TABLE}}{{CDC_FLAG}} --stream             # continuous instead of bounded
```

Output shape: one row per change.

| column | meaning |
|---|---|
| `__op` | `insert` / `update` / `delete` |
| `__pos` | JSON commit position (`{"file","pos"}` MySQL, `{"lsn"}` PG/MSSQL). Shared by a whole transaction |
| `__seq` | ordinal within the transaction. `(__pos, __seq)` is a total order |
| source columns | after-image for insert/update, key for delete |

Delivery is **at-least-once**, so dedupe downstream on PK + `(__pos, __seq)`.
Parts are named `cdc-<run_id>-NNNNNN.parquet` and accumulate across runs.
`manifest.json` and `_SUCCESS` describe the latest run.

Recovery:

| Symptom | Action |
|---|---|
| Run failed | Re-run. The checkpoint did not advance, so the data is re-read, not lost |
| PG slot invalidated/dropped, MySQL binlog purged (ERROR 1236), MSSQL below retention | Re-snapshot (`initial: snapshot` or `mode: full`), then start from a fresh checkpoint |
| MySQL checkpoint used against another server | Refused on purpose. Re-snapshot on the new host |

---

## 3. Load (BigQuery / Snowflake)

Put a top-level `load:` block in the **same** config. The load reads column types
from the state DB and never connects to the source.

```yaml
load:
  {{LOAD_TARGET}}
  pk: auto                      # auto (recorded source PK) | none | [col, ...]   — incremental/cdc dedup key
  cluster_by: auto              # auto | none | [col, ...]  (≤4 on BigQuery)
  partition:                    # none (default) | exactly one of column / range / ingestion
    column: {{CURSOR}}
    granularity: day            # hour | day | month | year
    # range: { column: n, start: 0, end: 1000000, interval: 1000 }
    # ingestion: day
    expiration_days: 90
    require_filter: false
  cleanup_source: true          # delete staged Parquet after the count gate passes
  gc_orphans: false             # also delete unmanifested crash leftovers
  allow_source_drift: false     # load even if the manifest's source count ≠ extracted
exports:
  - name: {{NAME}}
    # ...
    load: { pk: [{{PK}}], partition: none }   # per-export override (every field except target)
```

Required target fields: BigQuery takes `project` and `dataset`. Snowflake takes
`connection` (a `snow` CLI connection), `warehouse`, `database`, `schema` and
`storage_integration`.

```bash
rivet run  -c rivet.yaml              # extract → bucket
rivet load -c rivet.yaml              # load → warehouse
rivet load -c rivet.yaml --run-id "nightly-$(date +%F)"   # tag jobs (BQ label / Snowflake QUERY_TAG)
rivet load -c rivet.yaml --rebuild-changelog               # allow a billed rebuild when partitioning changed
rivet state loads -c rivet.yaml -t {{WAREHOUSE_TABLE}}
```

What the load does for each export `mode:`:

| mode | warehouse result |
|---|---|
| `full` | `OVERWRITE` the table with the latest snapshot. Re-running is idempotent |
| `incremental` | append to `<table>__changes`, plus a current-state view deduped on `pk` |
| `cdc` | append to `<table>__changes`, plus a view keeping the latest `(__pos, __seq)` per PK with `__is_deleted` (soft delete: live rows are `WHERE NOT __is_deleted`) |
| `cdc` with `tables:` | one `__changes` table and one view per source table |

Guarantees:

- **Manifest-driven.** The load uses only the parts listed in `Success` manifests, never a prefix glob.
- **Count gate.** The warehouse `COUNT(*)` must equal the summed manifest rows before the load completes or cleans up.
- **Cost labels.** BigQuery jobs are labelled `managed_by:rivet`, `rivet_op:{load,count,create,alter,view}` and `rivet_table:<t>`.

---

## 4. Data verification

### 4.1 Built into the run

```bash
rivet run -c rivet.yaml --validate      # every manifest part present at its recorded size + _SUCCESS
rivet run -c rivet.yaml --reconcile     # + source COUNT(*) == exported rows (a mismatch fails the run)
```

```yaml
exports:
  - name: {{NAME}}
    verify: content          # size (default) | content: require MD5 match per part (no download)
    on_schema_drift: fail    # warn (default) | continue | fail (exit 4)
    quality:
      row_count_min: 1000
      row_count_max: 10000000
      null_ratio_max: { {{PK}}: 0.0 }   # single runner only
      unique_columns: [{{PK}}]          # single runner only
      unique_max_entries: 1000000       # always cap memory
```

### 4.2 After the fact, without extracting

```bash
rivet validate -c rivet.yaml                       # full: manifest + parts + value-checksum re-read
rivet validate -c rivet.yaml --depth light         # manifest + _SUCCESS only (fast poll)
rivet validate -c rivet.yaml --depth sample        # + part reconcile + untracked surplus
rivet validate -c rivet.yaml -e {{NAME}} --date 2026-09-13   # a prior day's {date} prefix
rivet validate -c rivet.yaml -e {{NAME}} --prefix exports/{{NAME}}/2026-09-13/
rivet validate -c rivet.yaml --format json -o validate.json
```

For CDC, `rivet validate` descends into every table prefix and its `snapshot/`.
A missing `_SUCCESS` means the run did not finish cleanly.

### 4.3 Source vs export (chunked, `chunk_checkpoint: true`)

```bash
rivet reconcile -c rivet.yaml -e {{NAME}}                           # per-chunk recount; non-zero exit on mismatch
rivet reconcile -c rivet.yaml -e {{NAME}} --format json -o rec.json
rivet repair    -c rivet.yaml -e {{NAME}} --report rec.json         # print the repair plan
rivet repair    -c rivet.yaml -e {{NAME}} --report rec.json --execute   # re-export mismatched ranges only
```

### 4.4 Independent oracle (not rivet's own bookkeeping)

DuckDB fingerprints the source query and the Parquet separately (rows,
distinct key, non-null counts, sums, lengths). The script supports PostgreSQL
and MySQL sources.

```bash
pip install duckdb
python dev/correctness/verify_export.py \
  --source-type {{VERIFY_TYPE}} \
  --dsn "{{DSN}}" \
  --query "SELECT * FROM {{TABLE}}" \
  --parquet "/path/to/{{NAME}}/*.parquet" \
  --key {{PK}}                 # exit 0 = PASS, 1 = FAIL
```

> A prefix with orphaned pre-crash parts reads *high*. Verify only the parts
> named in `manifest.json`.

CDC replay check in DuckDB (latest image per key; the LSN parsing is PostgreSQL's):

```sql
WITH ev AS (
  SELECT *, upper(lpad(split_part(__pos->>'lsn','/',1),8,'0')) ||
            upper(lpad(split_part(__pos->>'lsn','/',2),8,'0')) AS lsn_key
  FROM read_parquet('cdc-out/cdc-*.parquet')
)
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY {{PK}} ORDER BY lsn_key DESC, __seq DESC) rn FROM ev
) WHERE rn = 1 AND __op <> 'delete';
-- compare with: SELECT * FROM {{TABLE}};  on the source
```

### 4.5 Warehouse side

```sql
-- BigQuery: what each rivet step cost
SELECT (SELECT value FROM UNNEST(labels) WHERE key='rivet_op')    AS op,
       (SELECT value FROM UNNEST(labels) WHERE key='rivet_table') AS tbl,
       COUNT(*) jobs, SUM(total_bytes_billed) bytes_billed
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key='managed_by' AND value='rivet')
GROUP BY op, tbl ORDER BY bytes_billed DESC;

-- current state of an incremental / cdc load vs the source
SELECT COUNT(*) FROM {{WAREHOUSE_SQL}} WHERE NOT __is_deleted;
```

### 4.6 Inspection

```bash
rivet state files -c rivet.yaml -e {{NAME}} --json   # files actually written
rivet metrics     -c rivet.yaml -e {{NAME}} --json   # rows / files / bytes / status per run
rivet journal     -c rivet.yaml -e {{NAME}} --run-id <id>
```

<script src="cheat-sheet.js"></script>
