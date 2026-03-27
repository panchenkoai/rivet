# Rivet -- Product Document

## Overview

Rivet is a source-aware data extraction tool written in Rust. It exports data from PostgreSQL and MySQL databases to Parquet/CSV files, stored locally or in cloud storage (S3, GCS). Designed to work safely with production databases, it diagnoses source health before extraction and throttles to avoid overloading.

## Current Capabilities (v2)

### Sources

| Source | Driver | Streaming | Notes |
|---|---|---|---|
| PostgreSQL | `postgres` crate (sync) | Server-side cursors (DECLARE CURSOR + FETCH N) | True streaming, bounded memory |
| MySQL | `mysql` crate (sync) | `query_iter()` with BatchSink callback | Streams within scope, no Vec accumulation |

### Output Formats

| Format | Compression | Streaming Write | Validation |
|---|---|---|---|
| Parquet | Snappy | `ArrowWriter` with flush per batch | Read-back row count verification |
| CSV | None | Append per batch | Line count verification |

### Destinations

| Destination | Backend | Config |
|---|---|---|
| Local filesystem | `std::fs` | `type: local`, `path: ./output` |
| Amazon S3 | OpenDAL blocking | `type: s3`, `bucket`, `prefix`, `region` |
| Google Cloud Storage | OpenDAL blocking | `type: gcs`, `bucket`, `prefix` |

### Extraction Modes

| Mode | Description | Config |
|---|---|---|
| `full` | Complete table dump | `mode: full` |
| `incremental` | Cursor-based (WHERE cursor > last_value), SQLite state tracking | `mode: incremental`, `cursor_column: updated_at` |
| `chunked` | ID-range splitting (WHERE id BETWEEN start AND end), auto min/max detection | `mode: chunked`, `chunk_column: id`, `chunk_size: 100000` |
| `time_window` | Time-based window (WHERE col >= now - N days) | `mode: time_window`, `time_column: created_at`, `days_window: 7` |

### Parallelism

- Thread pool for `chunked` mode (`parallel: N` in config)
- Each thread opens its own database connection
- Semaphore-based concurrency control
- Files named `{export}_{timestamp}_chunk{i}.{ext}`
- Resource monitor can dynamically pause threads when memory threshold exceeded

### Source Tuning Profiles

Three named profiles control extraction aggressiveness:

| Parameter | `fast` | `balanced` (default) | `safe` |
|---|---|---|---|
| `batch_size` | 50,000 | 10,000 | 2,000 |
| `throttle_ms` | 0 | 50 | 500 |
| `statement_timeout_s` | 0 (none) | 300 | 120 |
| `max_retries` | 1 | 3 | 5 |
| `retry_backoff_ms` | 1,000 | 2,000 | 5,000 |
| `lock_timeout_s` | 0 (none) | 30 | 10 |
| `memory_threshold_mb` | 0 (none) | 0 | 0 |

Individual fields override profile defaults. Without a `tuning` section, `balanced` is used.

### Preflight Check (`rivet check`)

Connects to the source, runs EXPLAIN on each export query, and reports:

- Row count estimate
- Scan type (Index Scan vs Seq Scan / ALL)
- Cursor range (min/max)
- Health verdict: **EFFICIENT** / **ACCEPTABLE** / **DEGRADED** / **UNSAFE**
- Suggestions for degraded and unsafe verdicts

### Output Validation (`--validate`)

After writing each output file, optionally reads it back and verifies row count matches the written count. Catches corrupt Parquet files and truncated CSV exports.

### SQL Templates

Queries can be defined inline or loaded from external `.sql` files:

```yaml
exports:
  - name: orders
    query_file: sql/orders.sql    # relative to config file
    format: parquet
    destination: ...
```

### State Management

- SQLite database (`.rivet_state.db`) stored next to config file
- Tracks cursor position and last run timestamp per export
- Commands: `rivet state show`, `rivet state reset --export <name>`

### Resource Monitoring

- Platform-specific RSS monitoring (macOS via `mach_task_basic_info`, Linux via `/proc/self/statm`)
- `memory_threshold_mb` tuning parameter: pauses extraction when RSS exceeds threshold
- In parallel chunked mode, waits for memory to drop before spawning new threads

### Retry with Backoff

Automatic retry on transient errors with exponential backoff:
- Connection reset, broken pipe
- Statement timeout, lock timeout
- MySQL server gone away, lost connection

### CLI Reference

```
rivet run --config <path>                          # run all exports
rivet run --config <path> --export <name>          # run a specific export
rivet run --config <path> --validate               # validate output after write
rivet check --config <path>                        # preflight check all exports
rivet check --config <path> --export <name>        # preflight check one export
rivet state show --config <path>                   # show cursor state
rivet state reset --config <path> --export <name>  # reset cursor
```

---

## Architecture

### Data Flow

```
Source (PG/MySQL)
  │
  ├─ begin_query / DECLARE CURSOR
  │
  ├─ FETCH batch_size rows ──► Arrow RecordBatch ──► FormatWriter ──► temp file
  │       │                          │                    │
  │       │ sleep(throttle_ms)       │ (dropped)          │ flush per batch
  │       │                          │                    │
  ├─ FETCH next batch ──────► Arrow RecordBatch ──► FormatWriter ──► temp file
  │       ...                        ...                  ...
  │
  ├─ close_query / COMMIT
  │
  └─ Destination.write(temp_file) ──► local / S3 / GCS
```

### Key Traits

```rust
// Source pushes data through a sink callback
trait Source: Send {
    fn export(&mut self, query, cursor_col, cursor, tuning, sink: &mut dyn BatchSink) -> Result<()>;
    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;
}

// Sink receives schema and batches one at a time
trait BatchSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()>;
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
}

// Format writer streams output incrementally
trait FormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
}

trait Format {
    fn create_writer(&self, schema: &SchemaRef, writer: Box<dyn Write + Send>) -> Result<Box<dyn FormatWriter>>;
    fn file_extension(&self) -> &str;
}
```

### Memory Model

Peak memory = `batch_size * avg_row_size * parallel_threads`.

With `safe` profile (batch_size=2000) and 4 parallel threads on 17KB/row table:
- 2000 * 17KB * 4 = ~136MB Arrow data
- Real measured peak: **710MB RSS** (including PG connections, runtime, OS overhead)

### Project Structure

```
src/
  main.rs              CLI entry point (clap)
  lib.rs               Public modules for tests
  config.rs            YAML config + validation
  pipeline.rs          Orchestration: modes, chunking, parallel, retry, validate
  error.rs             anyhow Result alias
  tuning.rs            Profiles (safe/balanced/fast)
  resource.rs          RSS monitoring (macOS + Linux)
  preflight.rs         EXPLAIN analysis + verdicts
  state.rs             SQLite cursor state
  types.rs             CursorState
  source/
    mod.rs             Source + BatchSink traits
    postgres.rs        PG server-side cursor implementation
    mysql.rs           MySQL streaming implementation
  format/
    mod.rs             Format + FormatWriter traits
    csv.rs             CSV streaming writer
    parquet.rs         Parquet streaming writer (Snappy, flush per batch)
  destination/
    mod.rs             Destination trait + factory
    local.rs           Local filesystem
    s3.rs              S3 via OpenDAL
    gcs.rs             GCS via OpenDAL
  bin/
    seed.rs            Test data generator
tests/
  format_golden.rs     CSV/Parquet golden tests
  v2_golden.rs         Chunked, time-window, validate, resource tests
dev/
  postgres/init.sql    PG schema (users, orders, events, page_views, content_items)
  mysql/init.sql       MySQL schema (same tables)
  *.yaml               Test configs for various scenarios
docker-compose.yaml    PG 16 + MySQL 8
```

### Test Coverage

181 tests total, 0 failures:

| Category | Count | What |
|---|---|---|
| tuning.rs | 6 | Profiles, overrides, display |
| config.rs | 10 | YAML parsing, validation, new modes |
| pipeline.rs | 11 | Chunks, time-window, cursor, is_transient |
| preflight.rs | 8 | Verdicts, EXPLAIN parsing, suggestions |
| source/postgres.rs | 3 | build_query (full, incremental, cursor) |
| source/mysql.rs | 3 | build_query (same) |
| state.rs | 18 | SQLite CRUD, metrics recording/query, schema tracking |
| format_golden.rs | 7 | CSV output + Parquet round-trip |
| v2_golden.rs | 17 | Chunks, time-window, validate, resource, config |
| retry_integration.rs | 11 | Error classification: network, timeout, capacity, deadlock, permanent |

### Tested Scenarios (E2E with databases)

| Scenario | PG | MySQL | Status |
|---|---|---|---|
| Full export CSV + Parquet | yes | yes | PASS |
| Incremental + cursor tracking | yes | yes | PASS |
| Incremental second run (0 rows) | yes | yes | PASS |
| Safe profile + throttling | yes | yes | PASS |
| Preflight EFFICIENT | yes | yes | PASS |
| Preflight DEGRADED | yes | yes | PASS |
| Worst-case content_items | 200K (370MB) | 50K (370MB) | PASS |
| Multi-export in one config | yes | -- | PASS |
| State show/reset lifecycle | yes | yes | PASS |
| Chunked sequential (4 chunks) | yes | -- | PASS |
| Chunked parallel 2 threads | yes | -- | PASS |
| Chunked parallel 4 threads | yes | -- | PASS |
| Output validation (--validate) | yes | -- | PASS |

### Benchmark Results

200K content_items (660MB in PG, ~17KB/row, 4 chunks x 50K rows):

| Config | Threads | Time | Peak RSS | Peak Memory |
|---|---|---|---|---|
| Sequential, fast | 1 | 17.7s | 2.5 GB | 1.9 GB |
| Parallel 2, fast | 2 | 16.2s | 4.3 GB | 3.6 GB |
| Parallel 4, fast | 4 | 15.4s | 7.1 GB | 4.9 GB |
| Parallel 4, safe | 4 | 18.9s | **710 MB** | **243 MB** |

---

## Current Limitations

| Limitation | Details |
|---|---|
| No Load/Merge step | Extract only. No BigQuery/warehouse loading, no MERGE/upsert |
| No CDC | No WAL/binlog reading, query-based extraction only |
| No orchestration | No built-in scheduler, depends on external cron/Airflow |
| No data quality checks | No NULL ratio, uniqueness, completeness checks (planned) |
| No web UI / API | CLI only |
| No notifications | No Slack/email alerting (planned) |
| No encryption | Output files are not encrypted |
| Single-machine | No distributed execution |

---

## v3 Features (implemented)

### Metrics History (SQLite)

Every export run is recorded in `export_metrics` table:
- duration_ms, total_rows, peak_rss_mb, status, error_message, tuning_profile, format, mode
- CLI: `rivet metrics --config rivet.yaml` shows run history
- Filter by export: `--export orders`, limit: `--last 20`

### Schema Tracking

Automatic column change detection between runs:
- Stores schema as JSON in `export_schema` table
- Detects: added columns, removed columns, type changes
- Logs `[WARN]` on schema change, updates stored schema
- First run: stores silently, no warning

### Smart Retry with Reconnect

Error classification into 5 categories with appropriate retry behavior:

| Category | Retry | Reconnect | Extra Delay | Examples |
|---|---|---|---|---|
| Network | yes | yes | -- | connection reset, broken pipe, no route, DNS, SSL, EOF |
| MySQL disconnect | yes | yes | -- | gone away, lost connection, server closed |
| Timeout | yes | no | -- | statement timeout, lock wait, execution time exceeded |
| Capacity | yes | yes | +15s | too many connections, DB starting/stopping |
| Deadlock | yes | no | +1s | deadlock detected, serialization failure |
| Permanent | **no** | -- | -- | syntax error, permission denied, table not found |

Fresh connection on every retry (not reusing failed connection). 26 error patterns covered.

---

## Roadmap

### v3.1: Data Quality + Notifications (next)

| Feature | Description | Complexity |
|---|---|---|
| Data quality checks | NULL ratio, uniqueness check, row count bounds per export | Medium |
| Slack notifications | Webhook on failure, degraded verdict, schema change | Low |

### v4: Load + Transform

| Feature | Description | Complexity |
|---|---|---|
| BigQuery Load | GCS -> BQ staging table | Medium |
| MERGE / Upsert | `daily_update` merge, `full_replace` with row count validation | High |
| Deduplication | Auto-generated dedup SQL from MERGE configs | Medium |
| Delta Lake / Iceberg | Alternative output formats with ACID, time travel | High |

### v5: CDC + Real-time

| Feature | Description | Complexity |
|---|---|---|
| PG WAL | Logical replication via replication slots | High |
| MySQL Binlog | ROW-based binlog streaming | High |
| Changelog format | INSERT/UPDATE/DELETE events in Parquet | Medium |
| Initial snapshot + CDC | First run: full dump + slot creation. Then: CDC only | High |

### v6: Platform

| Feature | Description | Complexity |
|---|---|---|
| Web UI | Dashboard: config editor, run history, logs viewer | High |
| REST API | Trigger runs, check status, get metrics programmatically | Medium |
| Built-in scheduler | Cron-like scheduling, dependency DAG between exports | High |
| Distributed mode | Multi-node work queue, horizontal scaling | Very High |

---

## Dependencies

| Crate | Purpose |
|---|---|
| `clap` | CLI argument parsing |
| `serde` + `serde_yaml` + `serde_json` | Config deserialization |
| `postgres` + `postgres-types` | PostgreSQL sync driver |
| `mysql` | MySQL sync driver |
| `arrow` | In-memory columnar format |
| `parquet` | Parquet file writing |
| `csv` | CSV writing (format module) |
| `rusqlite` (bundled) | SQLite state storage |
| `opendal` | S3/GCS/local storage abstraction |
| `chrono` | Timestamp handling |
| `anyhow` | Error handling |
| `env_logger` + `log` | Logging |
| `tempfile` | Temporary file for streaming writes |
| `libc` + `mach2` | macOS memory monitoring |
| `rand` | Test data generation (seed tool) |

## Building

```bash
cargo build --release                    # build rivet (9.9MB binary)
cargo build --release --bin seed         # build seed tool
cargo test                               # run all 181 tests
cargo clippy --all-targets               # lint (0 warnings)
```

## Development Setup

```bash
docker compose up -d                     # start PG + MySQL
cargo run --release --bin seed -- --target both --users 100000
RUST_LOG=info cargo run --release --bin rivet -- run --config dev/pg_full.yaml
```
