# Rivet

**Lightweight, source-safe data extraction from PostgreSQL and MySQL to Parquet/CSV.**

**Names:** the project and CLI are **Rivet**; the command you run is **`rivet`**. On [crates.io](https://crates.io/crates/rivet-cli) the package is published as **`rivet-cli`** because the crate name `rivet` was already taken. Homebrew and release archives still install the **`rivet`** binary.

Rivet is a CLI tool that exports query results from relational databases to files -- locally or in cloud storage (S3, GCS). It is **extract-only**: no loading, no merging, no CDC. It is designed to be gentle on production databases through tuning profiles, preflight health checks, and intelligent retry with backoff.

### What Rivet does

- Extracts data from **PostgreSQL** and **MySQL** via standard SQL queries
- Writes **Parquet** (zstd-compressed by default; snappy, gzip, lz4, none) or **CSV** files
- Uploads to **local disk**, **Amazon S3**, **Google Cloud Storage**, or **stdout** (pipe workflows)
- Tracks incremental state in **SQLite** so the next run picks up where the last left off
- Diagnoses source health **before** extraction (`rivet check`)
- Verifies auth for all sources and destinations **before** running (`rivet doctor`)
- Prints a structured **run summary** after each export (run ID, rows, files, bytes, duration, RSS, retries, schema changes)
- Persists **metrics history**, **schema tracking**, and **file manifest** in SQLite
- Recommends **parallelism level** and **tuning profile** in preflight checks
- **Parameterized queries** via `--param key=value` and `${key}` placeholders
- **Data quality checks** — row count bounds, null ratio thresholds, uniqueness assertions
- **File size splitting** — `max_file_size: 512MB` automatically splits output into parts
- **Memory-based batch sizing** — `batch_size_memory_mb: 256` auto-tunes batch size from schema width
- **Slack notifications** on failure, schema change, or degraded verdict

### What Rivet does NOT do

- **No loading/merging** -- it produces files; you bring them into a warehouse yourself
- **No CDC** -- no WAL/binlog reading; query-based extraction only
- **No orchestration** -- no built-in scheduler; use cron, Airflow, or similar
- **No exactly-once delivery** -- at-least-once; duplicates are possible (see [Execution Semantics](#execution-semantics))
- **No web UI / API** -- CLI and YAML config only

**Documentation language:** English-only. See [CONTRIBUTING.md](CONTRIBUTING.md).

> **New to Rivet?** Start with the [Pilot Documentation](docs/) — step-by-step guides for every export mode, destination, and YAML parameter, plus quickstart templates for your first export.

## Installation

### Homebrew (macOS / Linux) — recommended

```bash
brew tap panchenkoai/rivet
brew update
brew install rivet
rivet --version
```

### cargo install (crates.io)

Requires Rust 1.94+:

```bash
cargo install rivet-cli
rivet --version
```

> The binary is named `rivet`. The crate is published as [`rivet-cli`](https://crates.io/crates/rivet-cli) because the `rivet` name on crates.io is taken.

### Pre-built binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/panchenkoai/rivet/releases):

```bash
# macOS (Apple Silicon)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-aarch64-apple-darwin.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# macOS (Intel)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-x86_64-apple-darwin.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# Linux (x86_64)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-x86_64-unknown-linux-gnu.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# Linux (arm64)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-aarch64-unknown-linux-gnu.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/
```

Verify:

```bash
rivet --version
```

### Docker

Try Rivet without installing anything — mount your config and output directory:

```bash
docker run --rm \
  -v $(pwd)/rivet.yaml:/config/rivet.yaml \
  -v $(pwd)/output:/output \
  ghcr.io/panchenkoai/rivet:latest \
  run --config /config/rivet.yaml
```

Or check the version and explore commands:

```bash
docker run --rm ghcr.io/panchenkoai/rivet:latest --version
docker run --rm ghcr.io/panchenkoai/rivet:latest --help
```

Pass environment variables for credentials:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@host:5432/db" \
  -v $(pwd)/rivet.yaml:/config/rivet.yaml \
  -v $(pwd)/output:/output \
  ghcr.io/panchenkoai/rivet:latest \
  run --config /config/rivet.yaml
```

> **Note:** To connect to a database running on your host machine, use `host.docker.internal` instead of `localhost` in the connection URL.

### Build from source

Requires Rust 1.94+:

```bash
git clone https://github.com/panchenkoai/rivet.git
cd rivet
cargo build --release
# binary is at target/release/rivet
```

## Quick Start

1. Create a config file `rivet.yaml`:

```yaml
source:
  type: postgres
  url: "postgresql://user:pass@localhost:5432/mydb"
  tuning:
    profile: safe

exports:
  - name: users
    query: "SELECT id, name, email, updated_at FROM users"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination:
      type: local
      path: ./output
```

2. Run preflight check to diagnose source health:

```bash
rivet check --config rivet.yaml
```

3. Verify auth for source and all destinations:

```bash
rivet doctor --config rivet.yaml
```

4. Run the export:

```bash
RUST_LOG=info rivet run --config rivet.yaml
```

5. Check state:

```bash
rivet state show --config rivet.yaml
```

## Working with the binary

Once installed, `rivet` is a single self-contained binary with no runtime dependencies (no JVM, no Python, no Docker required).

**Typical workflow:**

```bash
# 1. Preflight: check that the source DB is reachable and healthy
rivet check --config rivet.yaml

# 2. Auth: verify credentials for source + all destinations (S3, GCS, etc.)
rivet doctor --config rivet.yaml

# 3. Export: run all exports defined in the config
RUST_LOG=info rivet run --config rivet.yaml

# 4. Inspect: view cursor state and file manifest
rivet state show --config rivet.yaml
rivet state files --config rivet.yaml

# 5. Re-run: only new/changed rows are exported (incremental mode)
RUST_LOG=info rivet run --config rivet.yaml
```

**Useful flags:**

```bash
rivet run --config rivet.yaml --export users      # run a single export
rivet run --config rivet.yaml --validate           # reconcile row counts after write
rivet run --config rivet.yaml --param env=prod     # parameterized queries
rivet state reset --config rivet.yaml --export users  # reset cursor to re-export from scratch
```

**Logging:**

Rivet uses `RUST_LOG` for verbosity:

```bash
RUST_LOG=debug rivet run --config rivet.yaml    # verbose (SQL, batch timings, retries)
RUST_LOG=info  rivet run --config rivet.yaml    # normal (progress, summary)
RUST_LOG=warn  rivet run --config rivet.yaml    # quiet (errors and warnings only)
```

**Chunked mode:** in a normal terminal, **`rivet run`** shows a **progress bar** per chunked export (chunks completed, rows so far, ETA). It does not require **`RUST_LOG`** (that only tunes text logs). If you do not see the bar, you are probably not on a TTY (e.g. piped stderr); use `RUST_LOG=info` to follow progress in the log. Example config: [`dev/bench_chunked_p4_safe.yaml`](dev/bench_chunked_p4_safe.yaml). Details: [docs/modes/chunked.md](docs/modes/chunked.md#progress-bar-chunked-exports).

**Shell completions:**

```bash
# Bash
rivet completions bash > ~/.local/share/bash-completion/completions/rivet

# Zsh
rivet completions zsh > ~/.zfunc/_rivet

# Fish
rivet completions fish > ~/.config/fish/completions/rivet.fish
```

## CLI Reference

```
rivet run --config <path>                          # run all exports
rivet run --config <path> --export <name>          # run a specific export
rivet run --config <path> --validate               # verify row counts after write
rivet check --config <path>                        # preflight check all exports
rivet check --config <path> --export <name>        # preflight check one export
rivet doctor --config <path>                       # verify source + destination auth
rivet state show --config <path>                   # show cursor state
rivet state reset --config <path> --export <name>  # reset cursor
rivet state files --config <path>                  # show file manifest (which run created which files)
rivet metrics --config <path>                      # show export run history
rivet metrics --config <path> --export <name>      # metrics for one export
rivet metrics --config <path> --last N             # last N runs (default 20)
rivet init --source <url> [--table <name>] [--schema <name>] [-o <file>]   # scaffold YAML from DB (see docs/reference/init.md)
rivet completions <shell>                          # generate shell completions (bash|zsh|fish|powershell)
```

**Shell completions:**

```bash
# zsh (add to ~/.zshrc)
rivet completions zsh > ~/.zfunc/_rivet

# bash
rivet completions bash > /etc/bash_completion.d/rivet

# fish
rivet completions fish > ~/.config/fish/completions/rivet.fish
```

Set `RUST_LOG=info` (or `debug`) for detailed logging:

```bash
RUST_LOG=info rivet run --config rivet.yaml
```

## Choosing a Mode

| Mode | Best for | Key behavior |
|------|----------|--------------|
| `full` | Small tables, snapshots, one-off exports | Exports entire query result every run |
| `incremental` | Append-only or update-tracked tables | Resumes from the last exported value of `cursor_column` |
| `chunked` | Very large tables (10M+ rows) | Splits into ID-range windows; supports `parallel > 1` for concurrent extraction |
| `time_window` | Event logs, append-mostly data with timestamps | Exports only the last N days based on a time/date column |

**Decision rules:**

1. **Table < 1M rows, full snapshot needed** -- use `full`.
2. **Table has a monotonically increasing column** (auto-increment id, `updated_at` with triggers) -- use `incremental`. This is the most efficient mode for repeated runs.
3. **Table is very large and you need parallel extraction** -- use `chunked` with `parallel > 1`. Set `chunk_column` to the primary key. Watch out for sparse IDs (see [Sparse IDs](#sparse-ids-gaps-in-the-key-range)).
4. **You only need recent data** (e.g. last 7 days of events) -- use `time_window`. Set `time_column` and `days_window`.

**Can I combine modes?** No. Each export uses exactly one mode. If you need both incremental tracking and chunked extraction, use `incremental` for ongoing syncs and `chunked` for backfills.

## Choosing a Profile

| Profile | Source environment | Behavior |
|---------|--------------------|----------|
| `fast` | Dedicated replica, data warehouse, trusted environment | Large batches, no throttle, no timeouts, minimal retries |
| `balanced` | General-purpose source, moderate concurrent load | 10K batch, 50ms throttle, 5-min statement timeout, 3 retries |
| `safe` | Production OLTP, shared resources, fragile source | Small batches, 500ms throttle, 2-min timeout, 5 retries with long backoff |

**Decision rules:**

1. **Dedicated read replica or analytics database** -- `fast`. You own the capacity.
2. **Production database with other workloads** -- `balanced`. Good default.
3. **Production OLTP under high load, or a database you don't fully control** -- `safe`. Rivet backs off aggressively and retries patiently.

You can always override individual fields (e.g. `profile: safe` with `batch_size: 5000`).

## Config Reference

### Source

Two mutually exclusive styles for specifying database credentials:

**URL-based** -- set exactly one of `url`, `url_env`, or `url_file`:

```yaml
source:
  type: postgres   # postgres | mysql
  url: "postgresql://user:pass@host:port/db"
  tuning:          # optional, defaults to balanced
    profile: safe  # safe | balanced | fast
```

```yaml
source:
  type: mysql
  url_env: DATABASE_URL   # read full URL from this env var
```

**Structured** -- specify individual connection fields:

```yaml
source:
  type: postgres
  host: db.example.com
  port: 5433              # optional; defaults to 5432 (PG) / 3306 (MySQL)
  user: admin
  password_env: DB_PASS   # reads password from env var; or use 'password: literal'
  database: mydb
```

| Field | Required | Notes |
|-------|----------|-------|
| `host` | yes | |
| `user` | yes | |
| `database` | yes | |
| `port` | no | defaults to 5432 (postgres) / 3306 (mysql) |
| `password` | no | plaintext; prefer `password_env` |
| `password_env` | no | env var name containing the password |

URL-based and structured fields **cannot** be mixed. If both are present, validation rejects the config with a clear error.

### Source Tuning

Controls how aggressively rivet reads from the database. Three named profiles with individual field overrides:

```yaml
source:
  type: postgres
  url: "..."
  tuning:
    profile: safe              # base profile
    batch_size: 3000           # override: rows per fetch
    throttle_ms: 300           # override: sleep between fetches
    statement_timeout_s: 60    # override: per-query timeout
```

#### Profile Defaults

| Parameter | `fast` | `balanced` (default) | `safe` |
|---|---|---|---|
| `batch_size` | 50,000 | 10,000 | 2,000 |
| `throttle_ms` | 0 | 50 | 500 |
| `statement_timeout_s` | 0 (none) | 300 | 120 |
| `max_retries` | 1 | 3 | 5 |
| `retry_backoff_ms` | 1,000 | 2,000 | 5,000 |
| `lock_timeout_s` | 0 (none) | 30 | 10 |

**When to use each profile:**

- **fast** -- trusted environment, dedicated replica, need maximum throughput
- **balanced** -- general purpose, moderate load on source
- **safe** -- production OLTP database, shared resources, fragile source

If no `tuning` section is specified, `balanced` is used.

### Exports

Each export defines a query, format, mode, and destination:

```yaml
exports:
  - name: my_export            # unique name, used for state tracking
    query: "SELECT ..."        # SQL query to execute
    mode: full                 # full | incremental
    cursor_column: updated_at  # required for incremental mode
    format: parquet            # parquet | csv
    destination:
      type: local              # local | s3 | gcs
      path: ./output           # local: output directory
```

### Meta Columns

Add metadata columns to every output row -- useful for deduplication and lineage on the raw/staging layer.

```yaml
exports:
  - name: page_views
    query: "SELECT * FROM page_views"
    format: parquet
    meta_columns:
      exported_at: true   # adds _rivet_exported_at (UTC timestamp)
      row_hash: true      # adds _rivet_row_hash (xxh3_128 hex)
    destination:
      type: gcs
      bucket: my-bucket
```

| Column | Type | Description |
|--------|------|-------------|
| `_rivet_exported_at` | `Timestamp(us, UTC)` | When the batch was exported (same value for all rows in a batch) |
| `_rivet_row_hash` | `Int64` | Lower 64 bits of xxHash3-128 over all column values. Integer for fast `PARTITION BY` / `JOIN`. |

**Dedup pattern** (e.g. in BigQuery / DuckDB):

```sql
SELECT * FROM raw_page_views
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY _rivet_row_hash
  ORDER BY _rivet_exported_at DESC
) = 1
```

Both fields are optional and default to `false`. When disabled, no extra columns are added.

### Compression

Parquet compression is configurable per export. Default: **zstd** (better compression ratio than Snappy at comparable speed).

```yaml
exports:
  - name: orders
    query: "SELECT * FROM orders"
    format: parquet
    compression: zstd            # zstd | snappy | gzip | lz4 | none
    compression_level: 9         # optional; zstd 1..22 (default 3), gzip 0..10 (default 6)
    destination:
      type: local
      path: ./output
```

| Codec | Default level | Notes |
|-------|--------------|-------|
| `zstd` | 3 | Best ratio/speed tradeoff; new default |
| `snappy` | — | Fast, modest compression; previous default |
| `gzip` | 6 | Wide compatibility |
| `lz4` | — | Very fast decompression |
| `none` | — | No compression; largest files |

CSV exports ignore the compression setting.

### Skip Empty Exports

When running scheduled/incremental exports, zero new rows often means nothing changed. Use `skip_empty` to avoid creating empty files:

```yaml
exports:
  - name: events_inc
    query: "SELECT * FROM events"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    skip_empty: true             # no file created when 0 rows; cursor not advanced
    destination:
      type: gcs
      bucket: my-bucket
```

When `skip_empty: true` and the query returns 0 rows:
- No output file is created or uploaded
- Cursor state is **not** advanced (safe to rerun)
- Run summary shows `status: skipped`

Default: `false` (current behavior; 0-row exports still succeed with no file output).

### Destinations

**Local filesystem:**

```yaml
destination:
  type: local
  path: ./output
```

**Amazon S3:**

```yaml
destination:
  type: s3
  bucket: my-bucket
  prefix: exports/data/
  region: us-east-1
  endpoint: https://...       # optional, for S3-compatible storage
```

Credentials: either omit key env fields and use the default AWS chain, or set **both** `access_key_env` and `secret_key_env`. Details: [Credential precedence](#credential-precedence).

**Google Cloud Storage:**

```yaml
destination:
  type: gcs
  bucket: my-bucket
  prefix: exports/data/
  endpoint: https://...       # optional
  credentials_file: /path/to/sa.json   # optional; omit to use ADC / env (see below)
```

**GCS -- credentials:** see [Credential precedence](#credential-precedence). For day-to-day use on a workstation with a Google Cloud project, run `gcloud auth application-default login` and omit `credentials_file`; Rivet then uses **Application Default Credentials** (ADC).

**Stdout (pipe to another tool):**

```yaml
destination:
  type: stdout
```

Writes file contents directly to stdout. Useful for piping into `gzip`, `aws s3 cp -`, or other streaming consumers. Only practical with a single export (multiple exports would interleave output).

## Credential precedence

Rivet uses one predictable model for *where* secrets come from. Think of four layers (highest priority first). A higher layer **wins** when it applies; Rivet does **not** merge multiple cloud credential sources for the same destination.

| Priority | Layer | Meaning |
|----------|--------|---------|
| 1 | **Config** | Fields in `rivet.yaml` (URLs, `credentials_file`, names of env vars for S3 keys). |
| 2 | **Environment variables** | Process environment (`DATABASE_URL` via `url_env`, `${VAR}` expansion in `url`, `GOOGLE_APPLICATION_CREDENTIALS`, standard `AWS_*` variables). |
| 3 | **ADC / instance identity** | Provider default credentials with **no** explicit path in Rivet config (e.g. GCE/GKE metadata; local user ADC from `gcloud auth application-default login`). |
| 4 | **File-based material** | Secret **content** read from disk when a path is chosen by config or environment (e.g. `url_file`, `credentials_file`, or the file pointed to by `GOOGLE_APPLICATION_CREDENTIALS`). This is not a separate "guess"; it is always wired through layer 1 or 2. |

### Database (PostgreSQL / MySQL)

Two mutually exclusive styles:

**URL-based** -- set exactly **one** of `source.url`, `source.url_env`, or `source.url_file`. There is **no** fallback between them.

| Mechanism | Resolution |
|-----------|----------------|
| `url` | Connection string from config. Placeholders `${VAR}` are expanded from the environment when the config file is loaded (missing variables become empty). |
| `url_env` | The **entire** URL is read from the named environment variable. |
| `url_file` | The **entire** URL is read from the file path given in config (trimmed). |

**Structured** -- set `host`, `user`, `database` (and optionally `port`, `password` / `password_env`). Rivet builds the connection URL internally.

Cloud "ADC" does not apply to database URLs.

### Google Cloud Storage (GCS)

| Step | Source |
|------|--------|
| 1 | If `destination.credentials_file` is set -- use **only** that service account JSON path (config overrides env). |
| 2 | Else -- OpenDAL uses Google's default loader: `GOOGLE_APPLICATION_CREDENTIALS` (if set) -- JSON file at that path. |
| 3 | Else -- user ADC file from `gcloud auth application-default login` (well-known path under gcloud config). |
| 4 | Else -- GCE/GKE metadata-based service account when running on Google Cloud. |

If you omit `credentials_file`, set `RUST_LOG=info` and look for a log line stating that the default Google credential chain is in use.

### Amazon S3

| Step | Source |
|------|--------|
| 1 | If **both** `access_key_env` and `secret_key_env` are set -- read access key and secret **only** from those variable names (error if unset). |
| 2 | If **neither** is set -- OpenDAL's default AWS chain: environment variables, shared config files (e.g. `~/.aws/credentials`), then EC2/ECS instance metadata (IAM role). |

Setting **only one** of `access_key_env` or `secret_key_env` is invalid and rejected at config validation.

## Auth Diagnostics

`rivet doctor` verifies that source and destination credentials are valid before you run any exports:

```
$ rivet doctor --config rivet.yaml

rivet doctor: verifying auth for config 'rivet.yaml'

[OK]  Config parsed successfully
[OK]  Source auth (Postgres)
[OK]  Destination S3(my-bucket)
[FAIL] Destination GCS(other-bucket) -- auth error: loading credential ...

Some checks failed. Fix the issues above before running exports.
```

Error categories:

| Category | Meaning |
|----------|---------|
| `auth error` | Credentials are missing, expired, or rejected |
| `connectivity error` | Cannot reach the host (DNS, firewall, timeout) |
| `bucket not found` | Bucket or path does not exist |
| `error` | Other / uncategorized |

## Preflight Check

`rivet check` analyzes each export before running it. It connects to the source database, runs `EXPLAIN` on each query, and reports strategy, row estimates, verdicts, profile recommendations, and warnings:

```
$ rivet check --config rivet.yaml

Export: orders_incremental
  Strategy:     incremental(updated_at)
  Mode:         incremental (cursor: updated_at)
  Row estimate: ~1M
  Cursor range: 2024-01-01 .. 2025-01-30
  Scan type:    Index Scan using idx_orders_updated_at
  Verdict:      EFFICIENT
  Recommended:  tuning.profile: fast

Export: events_full
  Strategy:     full-scan
  Mode:         full
  Row estimate: ~5M
  Scan type:    Seq Scan on events
  Verdict:      DEGRADED
  Recommended:  tuning.profile: safe
  Suggestion:   No index detected -- full table scan. Add an indexed cursor
                column and switch to incremental mode. Use 'safe' tuning
                profile to limit database impact.

Export: orders_chunked
  Strategy:     chunked-parallel(id, size=100000, p=4)
  Mode:         chunked (column: id, size: 100000)
  Row estimate: ~10M
  Cursor range: 1 .. 50000000
  Scan type:    Index Scan using orders_pkey
  Verdict:      ACCEPTABLE
  Recommended:  tuning.profile: safe
  Warning:      Sparse key range: ~99% of chunk windows will be empty ...
  Suggestion:   Large dataset (~10M rows). Add parallel > 1 to speed up ...
```

### Strategy Names

| Strategy | When |
|---|---|
| `full-scan` | `mode: full`, parallel=1 |
| `full-parallel(N)` | `mode: full`, parallel > 1 |
| `incremental(col)` | `mode: incremental` |
| `chunked(col, size=N)` | `mode: chunked`, parallel=1 |
| `chunked-parallel(col, size=N, p=P)` | `mode: chunked`, parallel > 1 |
| `time-window(col, Nd)` | `mode: time_window` |

### Profile Recommendation

`rivet check` recommends a tuning profile based on row estimate and index usage:

| Condition | Recommendation |
|---|---|
| Indexed, < 1M rows | `fast` |
| Indexed, 1M-10M rows | `balanced` |
| Indexed, > 10M rows | `safe` |
| No index, < 100K rows | `fast` (or `balanced` with parallel) |
| No index, 100K-1M rows | `balanced` |
| No index, > 1M rows | `safe` |

### Warnings

| Warning | Trigger |
|---|---|
| **Sparse key range** | Chunked mode with < 10% density (range >> row count) |
| **Dense surrogate sort cost** | Query uses `ROW_NUMBER()` in chunked mode |
| **Parallel memory risk** | `parallel > 1` on > 5M rows |

### Verdicts

| Verdict | Meaning |
|---|---|
| `EFFICIENT` | Index scan on cursor column, reasonable row count (< 10M) |
| `ACCEPTABLE` | Index scan but very large dataset, or partial index coverage |
| `DEGRADED` | Full table scan detected, but row count is manageable |
| `UNSAFE` | Full scan on very large table (> 50M rows) without index support |

Suggestions are mode-aware: full exports recommend switching to incremental, chunked exports recommend indexing the chunk column, time-window exports recommend indexing the time column.

## Incremental Mode

When `mode: incremental` is set, rivet:

1. Reads the last exported cursor value from its SQLite state database
2. Appends `WHERE <cursor_column> > '<last_value>'` to the query
3. After a successful export, updates the cursor to the last row's value

The state database (`.rivet_state.db`) is created next to your config file.

## Chunked Extraction

Rivet never loads an entire table into memory with a single query. Instead:

- **PostgreSQL:** Uses server-side cursors (`DECLARE CURSOR` / `FETCH N`) to read `batch_size` rows at a time
- **MySQL:** Uses streaming result sets (`query_iter()`) to read rows incrementally

Between each batch, rivet sleeps for `throttle_ms` milliseconds, giving the database breathing room.

### Sparse IDs (gaps in the key range)

Chunked mode uses `MIN(chunk_column)` and `MAX(chunk_column)` from your export query, then issues `WHERE chunk_column BETWEEN start AND end` for each window. If the primary key is sparse (huge spread between min and max, few rows), most windows cover **no rows** but the database still plans and scans for each range.

**Mitigation:** chunk on a dense surrogate computed in SQL, for example `ROW_NUMBER() OVER (ORDER BY id) AS chunk_rownum`, and set `chunk_column: chunk_rownum` in the export. Then min/max match the row count, not the physical id span. A commented PostgreSQL example lives at [tests/fixtures/migrations/001_sparse_chunk_column_example.sql](tests/fixtures/migrations/001_sparse_chunk_column_example.sql).

**Cost tradeoff:** `ORDER BY id` (and therefore that window) is not free. The planner usually needs a global ordering of the rows you export: often a **sort** over the whole result, or an **index scan on `id`** if the shape of the query allows it -- either way you pay once per export pass, and under concurrent writes the ordering is tied to a snapshot. You are trading many cheap-but-useless `BETWEEN` probes on a sparse key for fewer chunk queries that each touch real rows, at the price of establishing dense row numbers. For very large or hot tables, prefer **`incremental`** mode on an indexed cursor column, a **precomputed dense key** (column or side table populated by batch jobs), or a **materialized view** refreshed off the critical path, if that fits your workload better than a window over live data.

## Run Summary

After each export, Rivet prints a structured summary to stdout:

```
── orders ──
  run_id:      orders_20260329T125109.336
  status:      success
  rows:        150000
  files:       1
  bytes:       12.4 MB
  duration:    3.2s
  peak RSS:    142MB
  validated:   pass
  schema:      unchanged
```

All summary fields are also persisted to the metrics table and visible via `rivet metrics`. The `run_id` links the summary to the corresponding rows in `export_metrics` and `file_manifest` tables.

| Field | Description |
|-------|-------------|
| `run_id` | Canonical identifier for this run (links summary, metrics, and files) |
| `status` | `success` or `failed` |
| `rows` | Total rows extracted |
| `files` | Number of files produced (1 for single-file modes; N for chunked) |
| `bytes` | Total file size before upload |
| `duration` | Wall-clock time for the export |
| `peak RSS` | Peak process RSS during the export (MB) |
| `retries` | Number of retry attempts (0 if no retries needed) |
| `validated` | `pass` if `--validate` succeeded; omitted if not requested |
| `schema` | `unchanged` or `CHANGED`; omitted on first run |
| `error` | Error message (only on failure) |

### File manifest

Every file produced by Rivet is recorded in the `file_manifest` table. Use `rivet state files` to inspect:

```
$ rivet state files --config rivet.yaml
RUN ID                              FILE                                         ROWS      BYTES CREATED
--------------------------------------------------------------------------------------------------------------
orders_20260329T125143.912          orders_20260329_125200_chunk3.parquet        50000    17.4 MB 2026-03-29T12:52:00+00:00
orders_20260329T125143.912          orders_20260329_125156_chunk2.parquet        50000    17.4 MB 2026-03-29T12:51:56+00:00
```

This enables post-run reconciliation: verify which run created which files and confirm row counts match expectations.

## Execution Semantics

### Export lifecycle

Every export follows a strict sequence. Steps that fail cause the entire export to fail; state is never updated on failure.

```
1. Config load + validation
2. State read (load cursor for incremental; load schema for tracking)
3. Source connect (new connection per attempt)
4. Query start
   - full/incremental: single query
   - chunked: detect min/max, generate range queries
   - time_window: rewrite query with WHERE clause
5. Batch loop
   a. FETCH batch_size rows → Arrow RecordBatch
   b. FormatWriter.write_batch() → temp file (flush per batch)
   c. Sleep throttle_ms
   d. Repeat until source exhausted
6. FormatWriter.finish() → finalize temp file
7. Validate (if --validate): read back temp file, compare row count
8. Destination.write() → upload temp file to local/S3/GCS
9. State update (incremental only): advance cursor to last row's value
10. Schema tracking: compare columns with stored schema, warn on change
11. Metrics: record run result (duration, rows, RSS, status)
```

### State update point

The cursor advances **only after** step 8 (successful upload). If any step fails, the cursor stays at its previous value. This means:

- A failed export can be safely re-run without skipping data.
- A successful upload followed by a process crash before step 9 causes the **next** run to re-export rows already uploaded (at-least-once semantics -- see Duplicates below).

### Duplicates

Rivet provides **at-least-once** delivery. Duplicates can occur in these scenarios:

| Scenario | Cause | Mitigation |
|----------|-------|------------|
| Crash after upload, before cursor update | Cursor is not advanced; next run re-exports the same window | Downstream dedup on primary key + cursor column |
| `time_window` with overlapping windows | Rows near the boundary appear in consecutive windows | Downstream dedup or idempotent merge |
| `incremental` with non-monotonic cursor | Rows inserted with cursor values older than the last exported value are missed; rows updated after export may be re-exported | Use a strictly monotonic column (e.g. auto-increment id, `updated_at` with triggers) |
| `chunked` with concurrent writes | New rows inserted during export may land in already-processed ranges | Accept overlap or run during quiescent periods |

Rivet **never** claims exactly-once delivery. Design downstream pipelines to tolerate duplicates.

### Retry semantics

On failure, Rivet classifies the error and decides whether to retry:

| Category | Retry? | Reconnect? | Extra delay | Examples |
|----------|--------|------------|-------------|----------|
| Network | yes | yes | -- | connection reset, broken pipe, DNS, SSL, EOF |
| MySQL disconnect | yes | yes | -- | server gone away, lost connection |
| Timeout | yes | no | -- | statement timeout, lock wait timeout |
| Capacity | yes | yes | +15s | too many connections, DB starting/shutting down |
| Deadlock | yes | no | +1s | deadlock detected, serialization failure |
| Auth/permission | **no** | -- | -- | permission denied, access denied, invalid credentials |
| Permanent | **no** | -- | -- | syntax error, table not found, column not found |

On each retry, a **fresh connection** is created (never reuses a failed connection). Backoff is exponential: `retry_backoff_ms * 2^(attempt-1) + extra_delay`. The tuning profile controls `max_retries` and `retry_backoff_ms`.

### Validation semantics

`--validate` re-reads the **temp file** after writing and compares the row count against the number of rows received from the source:

- **Parquet**: opens the file with the Arrow reader and reads `num_rows` from footer metadata.
- **CSV**: counts newlines (excluding header).

**What it proves:** the file on disk contains the expected number of rows (catches truncated writes, corrupt footers, I/O errors during flush).

**What it does not prove:**
- Cell-level correctness (no checksum on individual values).
- Source-to-file semantic equivalence (no re-query of the database to compare).
- Post-upload integrity (the file is validated before upload, not after).

## Supported Type Mappings

| PostgreSQL | MySQL | Arrow / Parquet |
|---|---|---|
| `BOOL` | `BIT` | Boolean |
| `INT2` / `SMALLINT` | `TINYINT`, `SMALLINT` | Int16 |
| `INT4` / `INT` | `INT`, `MEDIUMINT` | Int32 |
| `INT8` / `BIGINT` | `BIGINT` | Int64 |
| `FLOAT4` | `FLOAT` | Float32 |
| `FLOAT8` | `DOUBLE` | Float64 |
| `TEXT`, `VARCHAR` | `VARCHAR`, `TEXT` | Utf8 (String) |
| `BYTEA` | `BLOB` (binary charset) | Binary |
| `DATE` | `DATE` | Date32 |
| `TIMESTAMP(TZ)` | `DATETIME`, `TIMESTAMP` | Timestamp(us) |
| `NUMERIC` | `DECIMAL` | Utf8 (stringified) |
| `JSON` / `JSONB` | `JSON` | Utf8 |
| `UUID` | -- | Utf8 |

## Guarantees and Limitations

### What Rivet guarantees

- **At-least-once delivery**: if an export succeeds, all rows matching the query are written to at least one output file.
- **State atomicity per export**: cursor state is updated only after successful upload. A crash mid-export does not advance the cursor.
- **Schema change detection**: Rivet warns when columns are added, removed, or change type between runs.
- **Validation on demand**: `--validate` confirms row counts match between source read and file on disk.
- **Predictable auth**: credentials are resolved in a documented 4-layer order; no silent fallback surprises.

### What Rivet does NOT guarantee

- **No exactly-once delivery**: duplicates can occur on crash recovery, overlapping windows, or non-monotonic cursors.
- **No cell-level validation**: `--validate` checks row count, not individual cell values or checksums.
- **No CDC / real-time**: Rivet runs point-in-time queries; it does not read WAL, binlog, or change streams.
- **No load / merge**: Rivet produces files. Loading them into a warehouse is your responsibility.
- **No distributed execution**: Rivet runs on a single machine. `parallel` spawns threads, not remote workers.
- **No transactional consistency across exports**: each export runs its own query; there is no cross-export snapshot isolation.
- **No encryption**: output files are written in plaintext. Encrypt at the destination level if needed.

See [Execution Semantics](#execution-semantics) for detailed lifecycle, state update, duplicate, retry, and validation rules.

## Development

For **pilot documentation** (per-mode guides, destination setup, annotated YAML examples), see [docs/](docs/).

For a **step-by-step onboarding guide** (from installation to production-ready exports), see [USER_GUIDE.md](USER_GUIDE.md).

For a **manual user acceptance checklist** (CLI, modes, destinations, compression, skip-empty), see [USER_TEST_PLAN.md](USER_TEST_PLAN.md).

### Local Setup

Start PostgreSQL and MySQL with Docker:

```bash
docker compose up -d
```

Seed both databases with test data (100K users, ~1M orders, ~5M events):

```bash
cargo run --release --bin seed -- --target both --users 100000
```

The seed tool supports flags:

```
--target postgres|mysql|both    # which database to seed
--users N                       # number of users (default: 100000)
--orders-per-user N             # avg orders per user (default: 10)
--events-per-user N             # avg events per user (default: 50)
--batch-size N                  # insert batch size (default: 1000)
--pg-url URL                    # PostgreSQL connection URL
--mysql-url URL                 # MySQL connection URL
```

### Toolchain

The project pins Rust **1.94** via `rust-toolchain.toml`. Install with:

```bash
rustup install 1.94
```

### Running Tests

```bash
cargo test              # 617 unit + integration tests (no database needed)
cargo test -- --nocapture  # with output
cargo clippy --all-targets -- -D warnings  # lint check
cargo fmt --all -- --check                 # format check
```

End-to-end scripts (Docker Compose must be up, `rivet` built):

```bash
bash dev/test_permissions.sh
bash dev/test_schema_evolution.sh
```

### CI

GitHub Actions runs on every push/PR to `master`/`main`:

- **Rustfmt** — formatting check
- **Clippy** — lint check with `-D warnings`
- **Tests** — full test suite
- **Release build** — ensures `cargo build --release` succeeds
- **Security audit** — `cargo audit` via `rustsec/audit-check`

---

## Roadmap

See [rivet_roadmap.md](rivet_roadmap.md) for the full roadmap (strategy + execution status).

**Next milestones:**

| Milestone | Focus |
|-----------|-------|
| **v0.2.0** (stable) | Cross-platform release binaries, E2E test matrix, `cargo publish`, Docker image |
| **v0.3.0** | Source count reconciliation, crash/recovery tests, data shape drift detection, curated example configs |
| **Future** | CDC mode, Iceberg/Delta output, webhook destination, multi-source joins, plugin system |
