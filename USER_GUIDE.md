# Rivet — User Guide

A step-by-step guide from zero to production-ready exports. Follow this path to set up Rivet, validate your configuration, run your first export, and build a reliable extraction pipeline.

---

## 1. Installation

Rivet is a single binary written in Rust.

```bash
# Clone and install
git clone <repo-url> && cd rivet
cargo install --path .

# Verify
rivet --help
```

**Optional:** enable shell completions:

```bash
# zsh
rivet completions zsh > ~/.zfunc/_rivet && echo 'fpath=(~/.zfunc $fpath); compinit' >> ~/.zshrc

# bash
rivet completions bash > /etc/bash_completion.d/rivet

# fish
rivet completions fish > ~/.config/fish/completions/rivet.fish
```

---

## 2. Connect to your database

Rivet supports PostgreSQL and MySQL. Create a file `rivet.yaml` in your project directory.

### Option A: Connection URL

```yaml
source:
  type: postgres
  url: "postgresql://user:password@host:5432/mydb"
```

### Option B: URL from environment variable

```yaml
source:
  type: postgres
  url_env: DATABASE_URL    # rivet reads the full URL from $DATABASE_URL
```

### Option C: Structured credentials

```yaml
source:
  type: postgres
  host: db.example.com
  port: 5432
  user: readonly_user
  password_env: DB_PASSWORD   # reads password from $DB_PASSWORD
  database: production
```

> **Security tip:** never put passwords in the config file directly. Use `password_env` or `url_env` to read from environment variables.

### MySQL

Everything works identically — just change `type: postgres` to `type: mysql`:

```yaml
source:
  type: mysql
  url: "mysql://user:password@host:3306/mydb"
```

---

## 3. Write your first export

Add an `exports` section. Start with a simple full export to local disk:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: users_full
    query: "SELECT id, name, email, created_at FROM users"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
```

Make sure the output directory exists:

```bash
mkdir -p output
```

---

## 4. Validate before running

Rivet has two pre-run diagnostics. **Always** run them before your first export.

### Step 1: Check source health

```bash
rivet check --config rivet.yaml
```

Expected output:

```
Export: users_full
  Strategy:     full-scan
  Mode:         full
  Row estimate: ~50K
  Scan type:    Seq Scan on users  (cost=0.00..1234.00 rows=50000 width=84)
  Verdict:      DEGRADED
  Recommended:  tuning.profile: fast
  Parallelism:  1 (only chunked mode benefits from parallelism)
  Suggestion:   No index detected -- full table scan. Add an indexed cursor
                column and switch to incremental mode.
```

**What to look for:**

| Verdict | Action |
|---------|--------|
| EFFICIENT | Good to go |
| ACCEPTABLE | Fine, but watch large datasets |
| DEGRADED | Consider adding indexes or switching mode |
| UNSAFE | Do NOT run against production without changes |

### Step 2: Verify auth

```bash
rivet doctor --config rivet.yaml
```

Expected output:

```
rivet doctor: verifying auth for config 'rivet.yaml'

[OK]  Config parsed successfully
[OK]  Source auth (Postgres)
[OK]  Destination Local(./output)

All checks passed.
```

If anything shows `[FAIL]`, fix the credential/connectivity issue before proceeding.

---

## 5. Run the export

```bash
rivet run --config rivet.yaml --validate
```

The `--validate` flag re-reads the file after writing to verify row counts match.

Output:

```
── users_full ──
  run_id:      users_full_20260329T143000.123
  status:      success
  rows:        50000
  files:       1
  bytes:       2.1 MB
  duration:    1.2s
  peak RSS:    45MB
  validated:   pass
```

Your file is now in `./output/users_full_20260329_143000.parquet`.

### Inspect the results

```bash
# Check what files were created
rivet state files --config rivet.yaml

# Check run history
rivet metrics --config rivet.yaml
```

---

## 6. Add a tuning profile

By default, Rivet uses the `balanced` profile. For production databases, use `safe`:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: safe    # small batches, long throttle, aggressive retries
```

| Profile | When to use |
|---------|------------|
| `fast` | Dedicated replica, you own the capacity |
| `balanced` | General-purpose, moderate concurrent load |
| `safe` | Production OLTP, shared resources, fragile source |

You can override individual settings:

```yaml
  tuning:
    profile: safe
    batch_size: 5000       # override the default 2000
    statement_timeout_s: 60
```

---

## 7. Incremental exports

For tables that grow over time, use incremental mode to only export new rows:

```yaml
exports:
  - name: orders_inc
    query: "SELECT id, user_id, product, amount, created_at FROM orders"
    mode: incremental
    cursor_column: id          # must be monotonically increasing
    format: parquet
    skip_empty: true           # don't create files when nothing changed
    destination:
      type: local
      path: ./output
```

**First run** — exports all rows. **Second run** — exports only rows with `id > last_exported_id`.

```bash
# First run
rivet run --config rivet.yaml --export orders_inc --validate

# Check stored cursor
rivet state show --config rivet.yaml

# Second run (only new rows)
rivet run --config rivet.yaml --export orders_inc --validate
```

**Choosing a cursor column:**

| Good cursor | Why |
|-------------|-----|
| Auto-increment `id` | Strictly monotonic, never goes backward |
| `updated_at` with trigger | Captures both inserts and updates |
| `created_at` | Captures inserts only (misses updates) |

| Bad cursor | Why |
|------------|-----|
| `status` | Not monotonic |
| `name` | Not ordered |
| Unindexed column | Slow `WHERE` filter |

> **Important:** Rivet advances the cursor only after a successful upload. If a run fails, re-running is safe — no data is skipped.

### Reset a cursor

If you need to re-export everything:

```bash
rivet state reset --config rivet.yaml --export orders_inc
```

---

## 8. Chunked extraction for large tables

For tables with millions of rows, chunked mode splits the export into ID-range windows:

```yaml
exports:
  - name: events_chunked
    query: "SELECT id, user_id, event_type, payload, created_at FROM events"
    mode: chunked
    chunk_column: id
    chunk_size: 100000        # rows per chunk (by ID range)
    parallel: 4               # concurrent database connections
    format: parquet
    destination:
      type: local
      path: ./output
```

This will:
1. Query `MIN(id)` and `MAX(id)`
2. Generate ranges: `1..100000`, `100001..200000`, ...
3. Run 4 ranges in parallel with separate connections

```bash
# Check first to see parallelism recommendation
rivet check --config rivet.yaml --export events_chunked

# Output might say:
#   Recommended:  parallel: 4 (large dataset with index support)

# Run it
rivet run --config rivet.yaml --export events_chunked
```

Output:

```
── events_chunked ──
  run_id:      events_chunked_20260329T150000.456
  status:      success
  rows:        5000000
  files:       50
  bytes:       1.2 GB
  duration:    45.3s
  peak RSS:    2100MB
```

Each chunk produces a separate file: `events_chunked_20260329_150001_chunk0.parquet`, etc.

> **Warning:** if `chunk_column` has large gaps (e.g. UUIDs cast to bigint, deleted ranges), most chunk windows will be empty. `rivet check` warns about this as "Sparse key range." Consider using `ROW_NUMBER()` as a dense surrogate — see README for details.

---

## 9. Time-window exports

For event tables where you only need recent data:

```yaml
exports:
  - name: recent_events
    query: "SELECT id, user_id, event_type, created_at FROM events"
    mode: time_window
    time_column: created_at
    time_column_type: timestamp   # timestamp | unix
    days_window: 7                # last 7 days
    format: parquet
    destination:
      type: local
      path: ./output
```

Rivet rewrites the query to add `WHERE created_at >= '2026-03-22 00:00:00'` (7 days ago).

---

## 10. Cloud destinations

### Amazon S3

```yaml
exports:
  - name: orders_s3
    query: "SELECT * FROM orders"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: my-data-lake
      prefix: raw/orders/
      region: us-east-1
      access_key_env: AWS_ACCESS_KEY_ID
      secret_key_env: AWS_SECRET_ACCESS_KEY
```

If you omit `access_key_env` / `secret_key_env`, Rivet falls back to the standard AWS credential chain (`~/.aws/credentials`, IAM role, etc.).

For S3-compatible storage (MinIO, R2):

```yaml
    destination:
      type: s3
      bucket: my-bucket
      prefix: exports/
      region: us-east-1
      endpoint: http://localhost:9000
      access_key_env: MINIO_ACCESS_KEY
      secret_key_env: MINIO_SECRET_KEY
```

### Google Cloud Storage

```yaml
exports:
  - name: orders_gcs
    query: "SELECT * FROM orders"
    mode: full
    format: parquet
    destination:
      type: gcs
      bucket: my-gcs-bucket
      prefix: raw/orders/
```

**Credential options (in priority order):**

1. `credentials_file: /path/to/service-account.json` — explicit service account
2. `GOOGLE_APPLICATION_CREDENTIALS` env var — file path
3. `gcloud auth application-default login` — local development (ADC)
4. GCE/GKE metadata — automatic in Google Cloud

For local development, the simplest path:

```bash
gcloud auth application-default login
# Then just use type: gcs with no credentials_file
```

---

## 11. Multiple exports in one config

A single config file can define many exports. They run sequentially:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: safe

exports:
  - name: users_full
    query: "SELECT id, name, email FROM users"
    mode: full
    format: csv
    destination:
      type: local
      path: ./output

  - name: orders_incremental
    query: "SELECT id, user_id, product, amount, updated_at FROM orders"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    skip_empty: true
    destination:
      type: gcs
      bucket: my-bucket
      prefix: raw/orders/

  - name: events_last_week
    query: "SELECT * FROM events"
    mode: time_window
    time_column: created_at
    time_column_type: timestamp
    days_window: 7
    format: parquet
    compression: zstd
    meta_columns:
      exported_at: true
      row_hash: true
    destination:
      type: s3
      bucket: my-lake
      prefix: raw/events/
      region: us-east-1
```

Run all exports:

```bash
rivet run --config rivet.yaml --validate
```

Run a single export:

```bash
rivet run --config rivet.yaml --export orders_incremental --validate
```

---

## 12. Meta columns for deduplication

Add metadata to every row for downstream dedup and lineage:

```yaml
exports:
  - name: page_views
    query: "SELECT * FROM page_views"
    format: parquet
    meta_columns:
      exported_at: true    # _rivet_exported_at (UTC timestamp)
      row_hash: true       # _rivet_row_hash (Int64 xxHash)
    destination:
      type: gcs
      bucket: my-bucket
```

The hash is deterministic: same row data always produces the same hash. Use this pattern in your warehouse:

```sql
-- BigQuery / DuckDB dedup
SELECT * FROM raw_page_views
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY _rivet_row_hash
  ORDER BY _rivet_exported_at DESC
) = 1
```

---

## 13. Compression

Parquet compression is configurable per export. Default is **zstd** (best ratio/speed balance):

```yaml
exports:
  - name: orders
    query: "SELECT * FROM orders"
    format: parquet
    compression: zstd          # zstd | snappy | gzip | lz4 | none
    compression_level: 9       # optional; zstd 1-22, gzip 0-10
    destination:
      type: local
      path: ./output
```

| Codec | Best for |
|-------|----------|
| `zstd` | General use (default). Best compression ratio at good speed |
| `snappy` | Maximum decompression speed. Spark/Hadoop ecosystem default |
| `gzip` | Wide compatibility. Slower but well-supported everywhere |
| `lz4` | Very fast decompression. Good for streaming reads |
| `none` | Debugging or when downstream does its own compression |

---

## 14. Observability

### Run history

```bash
rivet metrics --config rivet.yaml --last 10
```

Output:

```
EXPORT               STATUS         ROWS   DURATION      RSS  FILES      BYTES RUN ID
--------------------------------------------------------------------------------------------------------------
orders_incremental   success        1523       340ms     22MB      1   120.5 KB orders_incremental_20260329T160000.123
users_full           success       50000       1.2s     45MB      1     2.1 MB users_full_20260329T155900.456
orders_incremental   skipped           0       12ms      8MB      0          - orders_incremental_20260329T150000.789
```

### File manifest

```bash
rivet state files --config rivet.yaml
```

Shows every file ever produced, linked to its `run_id` — useful for auditing.

### Cursor state

```bash
rivet state show --config rivet.yaml
```

Output:

```
EXPORT                         LAST CURSOR                              LAST RUN
------------------------------------------------------------------------------------------
orders_incremental             2026-03-29T16:00:00.000000               2026-03-29T16:00:01+00:00
```

### Logging

Set `RUST_LOG` for detailed output:

```bash
RUST_LOG=info rivet run --config rivet.yaml      # normal verbosity
RUST_LOG=debug rivet run --config rivet.yaml     # full detail
```

---

## 15. Scheduling with cron

Rivet has no built-in scheduler. Use cron, systemd timers, or Airflow:

```cron
# Every 15 minutes: incremental orders + events
*/15 * * * * cd /opt/rivet && rivet run --config production.yaml --validate >> /var/log/rivet.log 2>&1

# Daily at 02:00: full users snapshot
0 2 * * * cd /opt/rivet && rivet run --config production.yaml --export users_full --validate >> /var/log/rivet.log 2>&1
```

> **Tip:** `skip_empty: true` on incremental exports avoids creating empty files when nothing changed.

---

## 16. Schema change detection

Rivet automatically tracks the column schema of each export. When columns are added, removed, or change type between runs, it logs a warning:

```
[WARN] export 'orders': schema changed!
[WARN]   added columns: phone (Utf8)
[WARN]   removed columns: old_field
[WARN]   type changed: price (Float64 -> Utf8)
```

The summary shows `schema: CHANGED` and the flag is persisted in metrics. No action is needed from Rivet — but you should update your downstream schemas.

---

## 17. Error handling and retries

Rivet classifies errors automatically:

| Error type | What happens |
|-----------|-------------|
| Network (connection reset, DNS) | Retry with fresh connection |
| Timeout (statement, lock) | Retry on same connection |
| Capacity (too many connections) | Retry with 15s extra delay |
| Deadlock | Retry with 1s extra delay |
| Auth / Permission | **Fail immediately** (fix config) |
| Syntax / Missing table | **Fail immediately** (fix query) |

The tuning profile controls retry behavior:

| Profile | Max retries | Backoff base |
|---------|------------|-------------|
| `fast` | 1 | 1s |
| `balanced` | 3 | 2s |
| `safe` | 5 | 5s |

Backoff is exponential: attempt 1 = base, attempt 2 = 2x base, attempt 3 = 4x base, etc.

---

## 18. Production checklist

Before deploying Rivet to production, verify each item:

### Config

- [ ] Credentials are in environment variables, not in the YAML file
- [ ] `tuning.profile: safe` for production OLTP sources
- [ ] Correct `mode` for each table (incremental for growing tables, full for snapshots)
- [ ] `skip_empty: true` on incremental exports to avoid empty files
- [ ] `--validate` flag is used in cron / scheduler
- [ ] Output directory or bucket exists and has write permissions

### Pre-flight

- [ ] `rivet check` shows no UNSAFE verdicts
- [ ] `rivet doctor` shows all `[OK]`
- [ ] Row estimates are reasonable (no accidental `SELECT *` on a billion-row table)

### Monitoring

- [ ] `rivet metrics` is checked periodically for failed runs
- [ ] `rivet state files` confirms expected file output
- [ ] Logs are captured (`RUST_LOG=info` with output redirect)
- [ ] Schema change warnings are reviewed after database migrations

### Downstream

- [ ] Warehouse load job handles duplicates (at-least-once semantics)
- [ ] Parquet schema matches destination table schema
- [ ] If using `meta_columns`, dedup query is in place

---

## Quick reference

| Task | Command |
|------|---------|
| Validate config and source health | `rivet check --config rivet.yaml` |
| Verify authentication | `rivet doctor --config rivet.yaml` |
| Run all exports | `rivet run --config rivet.yaml --validate` |
| Run one export | `rivet run --config rivet.yaml --export orders --validate` |
| Check cursor state | `rivet state show --config rivet.yaml` |
| Reset a cursor | `rivet state reset --config rivet.yaml --export orders` |
| View run history | `rivet metrics --config rivet.yaml --last 10` |
| View file manifest | `rivet state files --config rivet.yaml` |
| Enable logging | `RUST_LOG=info rivet run --config rivet.yaml` |

---

## Full example config

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: safe

exports:
  # Daily full snapshot of reference data
  - name: users_snapshot
    query: "SELECT id, name, email, role, created_at FROM users"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: gcs
      bucket: my-data-lake
      prefix: raw/users/

  # Incremental sync of transactional data
  - name: orders_sync
    query: "SELECT id, user_id, product, quantity, price, status, updated_at FROM orders"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    skip_empty: true
    meta_columns:
      exported_at: true
      row_hash: true
    destination:
      type: gcs
      bucket: my-data-lake
      prefix: raw/orders/

  # Weekly events window
  - name: events_week
    query: "SELECT id, user_id, event_type, properties, created_at FROM events"
    mode: time_window
    time_column: created_at
    time_column_type: timestamp
    days_window: 7
    format: parquet
    compression: zstd
    destination:
      type: s3
      bucket: my-s3-bucket
      prefix: raw/events/
      region: eu-west-1
      access_key_env: AWS_ACCESS_KEY_ID
      secret_key_env: AWS_SECRET_ACCESS_KEY

  # Large table backfill with parallel chunking
  - name: audit_log_backfill
    query: "SELECT id, action, actor_id, target, metadata, created_at FROM audit_log"
    mode: chunked
    chunk_column: id
    chunk_size: 500000
    parallel: 4
    format: parquet
    destination:
      type: local
      path: ./output/backfill
```

Run it:

```bash
# Pre-flight
rivet check --config rivet.yaml
rivet doctor --config rivet.yaml

# Execute
RUST_LOG=info rivet run --config rivet.yaml --validate

# Review
rivet metrics --config rivet.yaml
rivet state files --config rivet.yaml
rivet state show --config rivet.yaml
```
