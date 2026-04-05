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
  peak RSS:    45MB (sampled during run)
  validated:   pass
```

`peak RSS` is the highest resident set size seen while the export ran (background sampling), combined with start/end reads — useful for capacity planning. With **multiple exports in one process** (see §11), each job’s line still reflects process-wide RSS unless you use **separate processes** per export.

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

### Per-export tuning overrides

Defaults live on `source.tuning`. Any export can add an optional `tuning:` block; Rivet **merges** it on top of the source (for each field, the export value wins when set). Handy for comparing `fast` vs `balanced` in one file without duplicating the whole source.

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: fast
    batch_size: 1000

exports:
  - name: heavy_table
    query: "SELECT * FROM big"
    # inherits fast + batch 1000
    ...

  - name: careful_copy
    query: "SELECT * FROM big"
    tuning:
      profile: balanced   # only this export switches profile
    ...
```

Rules:

- `batch_size` and `batch_size_memory_mb` are **mutually exclusive** in the **effective** config (after merge). Rivet rejects the config if both appear across source + export.
- The run summary’s `tuning:` line shows the **YAML profile label** (`fast`, `balanced`, `safe`, or `balanced (default)`); SQLite metrics still store the heuristic profile name derived from behavior.

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

### Chunk checkpoint (SQLite plan, resume, retries)

Set `chunk_checkpoint: true` on a chunked export to store each chunk’s key range and status in `.rivet_state.db` next to your config. After each successful chunk, progress is committed; if the process dies, you can continue with the same `run_id` and completed chunks are skipped.

```yaml
exports:
  - name: big_table
    query: "SELECT id, data FROM big_table"
    mode: chunked
    chunk_column: id
    chunk_size: 50000
    parallel: 2
    chunk_checkpoint: true
    chunk_max_attempts: 5   # optional; per-chunk worker tries (default: tuning max_retries + 1)
    format: csv
    destination:
      type: local
      path: ./output
```

- **First run:** `rivet run --config rivet.yaml --export big_table` — builds the plan (same `MIN`/`MAX` ranges as without checkpoint) and processes chunks.
- **Resume:** `rivet run --config rivet.yaml --export big_table --resume` — continues the in-progress run. The fingerprint of `query` + `chunk_column` + `chunk_size` must match; otherwise Rivet errors (change detection).
- **Inspect:** `rivet state chunks --config rivet.yaml --export big_table`
- **Abandon:** `rivet state reset-chunks --config rivet.yaml --export big_table`

Transient errors use the same retry/backoff rules as other exports (`source.tuning`). Stale `running` tasks (e.g. after a crash) are reset to `pending` on resume.

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

A single config file can define many exports. **By default** they run **sequentially** in order, in **one** Rivet process (one connection to `.rivet_state.db` for state/metrics).

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

### Parallel execution of all exports

Only applies when you run **without** `--export` and the config lists **at least two** exports.

| Mode | CLI flag | YAML (optional) | What happens |
|------|----------|-----------------|--------------|
| **Threads** | `--parallel-exports` | `parallel_exports: true` | Exports run concurrently in the same process. Faster to start; **peak RSS** in each summary is still **process-wide** (not isolated per export). Each job uses its own SQLite connection (WAL). |
| **Processes** | `--parallel-export-processes` | `parallel_export_processes: true` | Parent spawns one child `rivet run --config … --export <name>` per export (children do **not** inherit parallel flags). **Peak RSS** per summary reflects that child process. Higher fork/exec overhead. |

If both YAML switches are true, **process mode wins**. You can combine parallel exports with chunked `parallel: N` inside each job — total database load is the product; watch **`max_connections`**, CPU, and I/O.

Example (bench-style config in-repo: `dev/bench_chunked_p4.yaml`):

```bash
rivet run --config dev/bench_chunked_p4.yaml --parallel-exports
rivet run --config dev/bench_chunked_p4.yaml --parallel-export-processes
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

## 14. Stdout destination (pipe workflows)

Send export output directly to stdout for piping into other tools:

```yaml
exports:
  - name: users_pipe
    query: "SELECT id, name, email FROM users LIMIT 100"
    mode: full
    format: csv
    destination:
      type: stdout
```

Rivet logs go to **stderr**, so stdout is clean data:

```bash
# Pipe CSV to DuckDB
rivet run --config rivet.yaml --export users_pipe | duckdb -c "SELECT * FROM read_csv_auto('/dev/stdin')"

# Redirect to file
rivet run --config rivet.yaml --export users_pipe > users.csv

# Pipe Parquet to a file (binary)
rivet run --config rivet.yaml --export users_parquet_pipe > users.parquet
```

> **Note:** Parquet output to stdout is valid binary — redirect to a file or pipe to tools that accept Parquet on stdin (DuckDB, `pqrs`, etc.).

---

## 15. Parameterized queries

Use `--param key=value` (repeatable) to inject values into `${key}` placeholders in queries:

```yaml
exports:
  - name: users_by_country
    query: "SELECT id, name, email FROM users WHERE country = '${COUNTRY}'"
    mode: full
    format: csv
    destination:
      type: local
      path: ./output
```

```bash
rivet run --config rivet.yaml --param COUNTRY=US
rivet run --config rivet.yaml --param COUNTRY=DE --param MAX_AGE=30
```

Params also work with `query_file`:

```yaml
exports:
  - name: filtered
    query_file: sql/filtered_users.sql   # can contain ${COUNTRY}, ${MIN_ID}
    format: parquet
    destination:
      type: local
      path: ./output
```

**Priority:** `--param` values take precedence over environment variables. If `${KEY}` matches both a param and an env var, the param wins.

Params work with `rivet check` too:

```bash
rivet check --config rivet.yaml --param COUNTRY=US
```

---

## 16. Data quality checks

Add per-export quality gates that run after extraction, before upload:

```yaml
exports:
  - name: orders
    query: "SELECT id, user_id, email, amount FROM orders"
    mode: full
    format: parquet
    quality:
      row_count_min: 100        # fail if fewer than 100 rows
      row_count_max: 10000000   # fail if more than 10M rows
      null_ratio_max:
        email: 0.05             # fail if >5% of email values are NULL
        id: 0.0                 # fail if any id is NULL
      unique_columns: [id]      # fail if id has duplicates
    destination:
      type: gcs
      bucket: my-bucket
```

If any check with **Fail** severity triggers, the export aborts before uploading:

```
[WARN] quality FAIL: column 'email': null ratio 0.1200 exceeds threshold 0.0500
[ERROR] export 'orders': quality checks failed
```

The summary shows `quality: FAIL` or `quality: pass`:

```
── orders ──
  status:      failed
  quality:     FAIL
  ...
```

> **Memory note:** `unique_columns` collects all values into a HashSet. For very high-cardinality columns (>10M), this can use significant memory. Recommended for primary keys and moderate cardinalities.

---

## 17. Memory-based batch sizing

Instead of specifying a fixed `batch_size` (row count), let Rivet auto-compute batch size based on a memory target:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    batch_size_memory_mb: 256    # target 256MB per batch
```

Rivet estimates the average row size from the Arrow schema after the first batch, then computes:

```
target_rows = memory_mb * 1024 * 1024 / estimated_row_bytes
```

The result is clamped to `[1000, 500000]` rows. This gives predictable memory usage regardless of row width.

```
# Log output:
batch_size_memory_mb=256: estimated row ~266B, computed batch_size=500000
```

> **Note:** `batch_size` and `batch_size_memory_mb` are mutually exclusive. Specify one or the other.

---

## 18. File size splitting

Split large exports into multiple files when they exceed a size threshold:

```yaml
exports:
  - name: events_export
    query: "SELECT * FROM events"
    mode: full
    format: parquet
    max_file_size: 512MB         # split into ~512MB parts
    destination:
      type: gcs
      bucket: my-bucket
      prefix: raw/events/
```

When the output reaches the threshold, Rivet finishes the current file, uploads it, and starts a new one:

```
events_export_20260329_150000_part0.parquet  (512 MB)
events_export_20260329_150000_part1.parquet  (512 MB)
events_export_20260329_150000_part2.parquet  (128 MB)
```

Supported size formats: `100KB`, `512MB`, `1GB`, `1073741824` (bytes).

If the export fits in a single file, no `_partN` suffix is added.

> **Batch boundary:** splitting happens between batches. If your batch size is large relative to `max_file_size`, individual parts may exceed the threshold. Use a smaller `batch_size` for finer-grained splitting.

> **Cursor safety:** for incremental mode, the cursor is updated only after ALL parts are successfully written.

---

## 19. Slack notifications

Get notified on failures, schema changes, or degraded verdicts:

```yaml
notifications:
  slack:
    webhook_url_env: SLACK_WEBHOOK_URL    # read URL from env var
    on: [failure, schema_change, degraded]
```

Set the webhook URL in your environment:

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../xxx"
```

When a trigger condition matches, Rivet POSTs a message with:
- Export name and run_id
- Status, row count, duration
- Error message (if failed)
- Schema change flag

You can also use `webhook_url` directly (not recommended for shared configs):

```yaml
notifications:
  slack:
    webhook_url: "https://hooks.slack.com/services/T.../B.../xxx"
    on: [failure]
```

Available triggers:

| Trigger | Fires when |
|---------|-----------|
| `failure` | Export status is `failed` |
| `schema_change` | Column schema changed since last run |
| `degraded` | Export status is `degraded` |

---

## 20. Observability

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

### Chunk checkpoint plan

For exports with `chunk_checkpoint: true`, chunk boundaries and status live in `.rivet_state.db` next to the config.

```bash
rivet state chunks --config rivet.yaml --export big_table
rivet state reset-chunks --config rivet.yaml --export big_table   # drop plan for this export; next run starts clean
```

Use `rivet run … --resume` to continue an in-progress checkpointed run (see §8).

### Dev-only: Postgres metrics (Prometheus + Grafana)

The repository `docker-compose.yaml` can start **postgres-exporter**, **Prometheus**, and **Grafana** for observing the database during load tests (ports `9187`, `9090`, `3000`). Config lives under `dev/prometheus/` and `dev/grafana/`. This stack is optional and intended for local benchmarking, not a Rivet runtime requirement.

### Logging

Set `RUST_LOG` for detailed output:

```bash
RUST_LOG=info rivet run --config rivet.yaml      # normal verbosity
RUST_LOG=debug rivet run --config rivet.yaml     # full detail
```

---

## 21. Scheduling with cron

Rivet has no built-in scheduler. Use cron, systemd timers, or Airflow:

```cron
# Every 15 minutes: incremental orders + events
*/15 * * * * cd /opt/rivet && rivet run --config production.yaml --validate >> /var/log/rivet.log 2>&1

# Daily at 02:00: full users snapshot
0 2 * * * cd /opt/rivet && rivet run --config production.yaml --export users_full --validate >> /var/log/rivet.log 2>&1
```

> **Tip:** `skip_empty: true` on incremental exports avoids creating empty files when nothing changed.

---

## 22. Schema change detection

Rivet automatically tracks the column schema of each export. When columns are added, removed, or change type between runs, it logs a warning:

```
[WARN] export 'orders': schema changed!
[WARN]   added columns: phone (Utf8)
[WARN]   removed columns: old_field
[WARN]   type changed: price (Float64 -> Utf8)
```

The summary shows `schema: CHANGED` and the flag is persisted in metrics. No action is needed from Rivet — but you should update your downstream schemas.

---

## 23. Error handling and retries

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

## 24. Production checklist

Before deploying Rivet to production, verify each item:

### Config

- [ ] Credentials are in environment variables, not in the YAML file
- [ ] `tuning.profile: safe` for production OLTP sources
- [ ] Per-export `tuning:` overrides (if any) were validated; merged `batch_size` / `batch_size_memory_mb` are not both set
- [ ] Correct `mode` for each table (incremental for growing tables, full for snapshots)
- [ ] `skip_empty: true` on incremental exports to avoid empty files
- [ ] `--validate` flag is used in cron / scheduler
- [ ] Output directory or bucket exists and has write permissions
- [ ] Chunked exports with `chunk_checkpoint: true` have a documented **resume** / **reset-chunks** procedure for operators
- [ ] If using `parallel_exports` or `parallel_export_processes`, database **`max_connections`** and lock capacity are sufficient for summed concurrency (including chunked `parallel` per export)

### Pre-flight

- [ ] `rivet check` shows no UNSAFE verdicts
- [ ] `rivet doctor` shows all `[OK]`
- [ ] Row estimates are reasonable (no accidental `SELECT *` on a billion-row table)

### Quality & Notifications

- [ ] `quality` checks configured for critical exports (row count bounds, null ratio, uniqueness)
- [ ] `notifications.slack` configured for `failure` and `schema_change` events
- [ ] `max_file_size` set for exports that may produce very large files (>1GB)
- [ ] `batch_size_memory_mb` used when row width varies across exports

### Monitoring

- [ ] `rivet metrics` is checked periodically for failed runs
- [ ] `rivet state files` confirms expected file output
- [ ] Logs are captured (`RUST_LOG=info` with output redirect)
- [ ] Schema change warnings are reviewed after database migrations
- [ ] Slack notifications are tested with a forced failure before go-live

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
| Run all exports in parallel (threads) | `rivet run --config rivet.yaml --parallel-exports` |
| Run all exports in parallel (processes) | `rivet run --config rivet.yaml --parallel-export-processes` |
| Resume chunked checkpoint | `rivet run --config rivet.yaml --export big_table --resume` |
| Run one export | `rivet run --config rivet.yaml --export orders --validate` |
| Run with params | `rivet run --config rivet.yaml --param TABLE=users --param LIMIT=1000` |
| Pipe to stdout | `rivet run --config rivet.yaml --export csv_export \| duckdb` |
| Check cursor state | `rivet state show --config rivet.yaml` |
| Reset a cursor | `rivet state reset --config rivet.yaml --export orders` |
| Chunk checkpoint status | `rivet state chunks --config rivet.yaml --export big_table` |
| Clear chunk plan | `rivet state reset-chunks --config rivet.yaml --export big_table` |
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
    batch_size_memory_mb: 256    # auto-tune batch size per schema width

notifications:
  slack:
    webhook_url_env: SLACK_WEBHOOK_URL
    on: [failure, schema_change]

exports:
  # Daily full snapshot of reference data
  - name: users_snapshot
    query: "SELECT id, name, email, role, created_at FROM users"
    mode: full
    format: parquet
    compression: zstd
    quality:
      row_count_min: 1000
      null_ratio_max:
        email: 0.01
      unique_columns: [id]
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
    max_file_size: 512MB
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
    max_file_size: 1GB
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
