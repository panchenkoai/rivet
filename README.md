# Rivet

Export data from databases to Parquet/CSV files, stored locally or in cloud storage.
Source-aware: diagnoses database health before extraction and throttles to avoid overloading production systems.

## Features

- **Sources:** PostgreSQL, MySQL
- **Formats:** Parquet (Snappy-compressed), CSV
- **Destinations:** Local filesystem, Amazon S3, Google Cloud Storage
- **Modes:** Full export, Incremental by cursor column
- **State:** SQLite-backed cursor tracking for incremental exports
- **Config:** YAML-driven, one file defines all export jobs
- **Preflight Check:** `rivet check` diagnoses index usage, row counts, and query plans before extraction
- **Source Tuning:** Three profiles (safe/balanced/fast) control batch size, throttling, timeouts, and retry
- **Chunked Extraction:** Server-side cursors (PG) and streaming (MySQL) -- never loads entire table into memory at once
- **Retry with Backoff:** Automatic retry on transient errors (timeouts, connection resets)

## Installation

```bash
cargo install --path .
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

3. Run the export:

```bash
RUST_LOG=info rivet run --config rivet.yaml
```

4. Check state:

```bash
rivet state show --config rivet.yaml
```

## CLI Reference

```
rivet run --config <path>                          # run all exports
rivet run --config <path> --export <name>          # run a specific export
rivet check --config <path>                        # preflight check all exports
rivet check --config <path> --export <name>        # preflight check one export
rivet state show --config <path>                   # show cursor state
rivet state reset --config <path> --export <name>  # reset cursor
```

Set `RUST_LOG=info` (or `debug`) for detailed logging:

```bash
RUST_LOG=info rivet run --config rivet.yaml
```

## Config Reference

### Source

```yaml
source:
  type: postgres   # postgres | mysql
  url: "postgresql://user:pass@host:port/db"
  tuning:          # optional, defaults to balanced
    profile: safe  # safe | balanced | fast
```

For MySQL:

```yaml
source:
  type: mysql
  url: "mysql://user:pass@host:3306/db"
```

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

S3 credentials are read from standard AWS environment variables or IAM role.

**Google Cloud Storage:**

```yaml
destination:
  type: gcs
  bucket: my-bucket
  prefix: exports/data/
  endpoint: https://...       # optional
```

GCS credentials are read from standard Google Cloud environment variables or service account.

## Preflight Check

`rivet check` analyzes each export before running it. It connects to the source database, runs `EXPLAIN` on each query, and reports:

```
$ rivet check --config rivet.yaml

Export: orders_incremental
  Mode:         incremental (cursor: updated_at)
  Row estimate: ~1M
  Cursor range: 2024-01-01 .. 2025-01-30
  Scan type:    Index Scan using idx_orders_updated_at
  Verdict:      EFFICIENT

Export: events_full
  Mode:         full
  Row estimate: ~5M
  Scan type:    Seq Scan on events
  Verdict:      DEGRADED
  Suggestion:   Full scan on ~5M rows. Consider incremental mode
                with an indexed cursor column.
```

### Verdicts

| Verdict | Meaning |
|---|---|
| `EFFICIENT` | Index scan on cursor column, reasonable row count (<10M) |
| `ACCEPTABLE` | Index scan but very large dataset, or partial index coverage |
| `DEGRADED` | Full table scan detected, but row count is manageable |
| `UNSAFE` | Full scan on very large table (>50M rows) without index support |

Suggestions are provided for `DEGRADED` and `UNSAFE` verdicts.

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

## Retry with Backoff

On transient errors (connection reset, statement timeout, lock timeout, server gone away), rivet retries with exponential backoff. The number of retries and initial backoff are controlled by the tuning profile.

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

## Development

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

### Running Tests

```bash
cargo test              # unit + golden tests (no database needed)
cargo test -- --nocapture  # with output
```
