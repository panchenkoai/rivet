# Rivet -- Product Document

## Overview

Rivet is a source-aware data extraction tool written in Rust. It exports data from PostgreSQL and MySQL databases to Parquet/CSV files, stored locally or in cloud storage (S3, GCS). Designed to work safely with production databases, it diagnoses source health before extraction and throttles to avoid overloading.

**Documentation:** Repository docs and user-visible CLI text are **English-only** (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## Current Capabilities (v2)

### Sources

| Source | Driver | Streaming | Notes |
|---|---|---|---|
| PostgreSQL | `postgres` crate (sync) | Server-side cursors (DECLARE CURSOR + FETCH N) | True streaming, bounded memory |
| MySQL | `mysql` crate (sync) | `query_iter()` with BatchSink callback | Streams within scope, no Vec accumulation |

### Output Formats

| Format | Compression | Streaming Write | Validation |
|---|---|---|---|
| Parquet | zstd (default), snappy, gzip, lz4, none — configurable per export | `ArrowWriter` with flush per batch | Read-back row count verification |
| CSV | None | Append per batch | Line count verification |

### Destinations

| Destination | Backend | Config |
|---|---|---|
| Local filesystem | `std::fs` | `type: local`, `path: ./output` |
| Amazon S3 | OpenDAL blocking | `type: s3`, `bucket`, `prefix`, `region` |
| Google Cloud Storage | OpenDAL blocking | `type: gcs`, `bucket`, `prefix` |
| Stdout | `std::io::stdout` | `type: stdout` — pipe to DuckDB, redirect to file |

### Extraction Modes

| Mode | Description | Config |
|---|---|---|
| `full` | Complete table dump | `mode: full` |
| `incremental` | Cursor-based (WHERE cursor > last_value), SQLite state tracking | `mode: incremental`, `cursor_column: updated_at` |
| `chunked` | ID-range splitting (WHERE id BETWEEN start AND end), auto min/max detection | `mode: chunked`, `chunk_column: id`, `chunk_size: 100000` |
| `time_window` | Time-based window (WHERE col >= now - N days) | `mode: time_window`, `time_column: created_at`, `days_window: 7` |

#### Chunked mode and sparse ID ranges

Chunk boundaries come from `MIN`/`MAX` on `chunk_column` over the export query. Physical keys with large gaps (deleted blocks, shard prefixes, snowflake-style ids) yield a wide `[min, max]` while row count stays small, so most chunk windows are empty scans.

**Recommended pattern:** expose a dense surrogate in the query (e.g. `ROW_NUMBER() OVER (ORDER BY id)`), set `chunk_column` to that alias, and keep natural keys in the selected columns for the output file. Reference migration-style SQL: `tests/fixtures/migrations/001_sparse_chunk_column_example.sql`. Tests in `tests/chunked_sparse_ids.rs` document the chunk-window count behavior.

That window implies **sorting (or index-ordered scanning)** over the exported rows: it avoids empty `BETWEEN` windows on sparse keys but is not a free lunch on large transactional tables. Alternatives: `incremental` on an indexed column, a stored dense sequence maintained by the application, or a materialized view.

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
rivet run --config <path> --param key=value        # inject query parameter (repeatable)
rivet check --config <path>                        # preflight check all exports
rivet check --config <path> --export <name>        # preflight check one export
rivet check --config <path> --param key=value      # check with query parameter
rivet doctor --config <path>                       # verify source + destination auth
rivet state show --config <path>                   # show cursor state
rivet state reset --config <path> --export <name>  # reset cursor
rivet state files --config <path>                  # show file manifest
rivet metrics --config <path>                      # show run history
rivet completions <shell>                          # generate shell completions
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
    fn bytes_written(&self) -> u64;   // for file-size splitting
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
  tuning.rs            Profiles (safe/balanced/fast) + memory-based batch sizing
  resource.rs          RSS monitoring (macOS + Linux)
  preflight.rs         EXPLAIN analysis + verdicts
  state.rs             SQLite cursor state
  types.rs             CursorState
  notify.rs            Slack webhook notifications
  quality.rs           Data quality checks (row count, null ratio, uniqueness)
  enrich.rs            Meta columns (_rivet_exported_at, _rivet_row_hash)
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
    stdout.rs          Stdout for pipe workflows
  bin/
    seed.rs            Test data generator
tests/
  format_golden.rs     CSV/Parquet golden tests
  v2_golden.rs         Chunked, time-window, validate, resource tests
dev/
  postgres/init.sql    PG schema (users, orders, events, page_views, content_items)
  mysql/init.sql       MySQL schema (same tables)
  *.yaml               Test configs for various scenarios
docker-compose.yaml    PG 16 + MySQL 8 + MinIO + fake-GCS + Toxiproxy
```

### Test Coverage

**265** distinct test cases; `cargo test` reports **449** passes (unit tests in `src/` run for both the library and binary targets).

| Category | Count | What |
|---|---|---|
| config.rs | 53 | YAML parsing, validation, credentials, compression, skip_empty, meta_columns, quality, file_size, params, stdout, notifications, batch_size_memory_mb |
| preflight.rs | 66 | Verdicts, EXPLAIN parsing, suggestions, strategy, profile recommendation, parallelism recommendation, sparse/dense/parallel warnings, doctor categorization |
| pipeline.rs | 19 | Chunks, time-window, cursor, classify_error, credential errors, format_bytes, RunSummary |
| state.rs | 16 | SQLite CRUD, metrics, schema tracking, file manifest |
| tuning.rs | 12 | Profiles, overrides, display, estimate_row_bytes, compute_batch_size_from_memory, effective_batch_size |
| quality.rs | 7 | Row count bounds, null ratio, uniqueness, run_checks |
| enrich.rs | 6 | Meta columns: exported_at, row_hash, determinism, null vs empty |
| notify.rs | 3 | Slack trigger matching, no-config no-op, success-no-trigger |
| destination/gcs_auth.rs | 7 | ADC parsing, urlenc, authorized_user validation |
| source/postgres.rs + mysql.rs | 6 | build_query (full, incremental, cursor) |
| format_golden.rs | 11 | CSV output + Parquet round-trip + compression variants (zstd, snappy, gzip, lz4, none) |
| v2_golden.rs | 17 | Chunks, time-window, validate, resource, config |
| retry_integration.rs | 11 | Error classification: network, timeout, capacity, deadlock, permanent |
| schema_evolution.rs | 16 | Schema diff / state store integration |
| chunked_sparse_ids.rs | 4 | Sparse/dense chunked range golden tests |

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
| No web UI / API | CLI only |
| No encryption | Output files are not encrypted |
| Single-machine | No distributed execution |

---

## v3 Features (implemented)

### Auth & Connectivity (Epic A)

Predictable credential resolution for all source and destination types:

- **Credential precedence**: 4-layer model (config > env > ADC/instance > file) documented in README
- **GCS ADC**: `gcloud auth application-default login` works without JSON key file; Rivet refreshes `authorized_user` tokens automatically
- **GCS explicit JSON**: `credentials_file` config field with existence validation at parse time
- **GCS emulator**: `allow_anonymous: true` for fake-gcs-server / local development
- **DB credentials**: URL-based (`url`, `url_env`, `url_file`) or structured (`host/user/password_env/database`); mutually exclusive with clear errors
- **S3 credentials**: `access_key_env` + `secret_key_env` or default AWS chain
- **Auth diagnostics**: `rivet doctor --config <path>` verifies source + destination auth before any export, with categorized errors (auth / connectivity / bucket-not-found)

### Preflight & Planner 2.0 (Epic B)

`rivet check` is now a planning and safety recommendation tool:

- **Strategy display**: shows extraction strategy name (e.g. `chunked-parallel(id, size=50000, p=4)`)
- **Profile recommendation**: suggests `safe`/`balanced`/`fast` based on row estimate and index usage
- **Sparse range warning**: detects sparse key ranges where most chunk windows will be empty
- **Dense surrogate warning**: warns when `ROW_NUMBER` chunking implies global sort cost
- **Parallel memory warning**: warns when parallel mode on large tables risks high RSS
- **Mode-aware suggestions**: DEGRADED/UNSAFE verdicts include concrete next steps per mode

### Execution Semantics (Epic C)

Frozen and documented execution guarantees:

- **Export lifecycle**: 11-step sequence (config -> connect -> query -> batch loop -> finish -> validate -> upload -> state -> schema -> metrics)
- **State update point**: cursor advances only **after** successful upload; failure = safe re-run
- **Duplicate semantics**: at-least-once delivery; documented per-mode overlap scenarios
- **Retry semantics**: 5 error categories with reconnect/backoff rules; auth/permission errors fail fast
- **Validation semantics**: `--validate` proves row count on disk; does not prove cell-level or post-upload correctness

All documented in README under "Execution Semantics".

### Observability and Run Summary (Epic D)

Every export now prints a structured end-of-run summary to stderr (keeps stdout clean for pipe workflows):

```
── orders ──
  status:      success
  rows:        150000
  files:       1
  bytes:       12.4 MB
  duration:    3.2s
  peak RSS:    142MB
  validated:   pass
  schema:      unchanged
```

Fields: export name, status, total rows, files produced, bytes written, duration, peak RSS, retries, validation result, schema change flag.

### Metrics History (SQLite)

Every export run is recorded in `export_metrics` table:
- duration_ms, total_rows, peak_rss_mb, status, error_message, tuning_profile, format, mode
- files_produced, bytes_written, retries, validated, schema_changed (new in v3)
- CLI: `rivet metrics --config rivet.yaml` shows run history
- Filter by export: `--export orders`, limit: `--last 20`
- Metrics output now includes files, bytes, retries, validated, and schema flags

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

### Meta Columns

Optional metadata columns appended to every output row:

- `_rivet_exported_at` (Timestamp UTC) -- when the row was exported; one value per batch
- `_rivet_row_hash` (Int64) -- lower 64 bits of xxHash3-128; integer for fast PARTITION BY / JOIN
- Enabled per-export via `meta_columns: { exported_at: true, row_hash: true }`
- Hash is deterministic: same row data = same hash across runs
- Distinguishes NULL from empty string
- Use case: downstream dedup via `PARTITION BY _rivet_row_hash ORDER BY _rivet_exported_at DESC`

### Documentation Rewrite (Epic E)

README restructured around real usage decisions:

- **Repositioned intro**: Rivet is lightweight, source-safe, predictable, extract-only
- **Choosing a mode**: decision table and rules for full / incremental / chunked / time_window
- **Choosing a profile**: decision table and rules for safe / balanced / fast
- **Auth guide**: 4-layer credential precedence matrix with per-provider steps
- **Guarantees and Limitations**: explicit list of what Rivet does and does not promise

---

**Phase 1 "Pilot Alpha Stabilization" (Epics A–E + D3/D4/L6) is complete.** All auth flows are implemented and tested, execution semantics are frozen and documented, run summaries are printed and persisted with canonical run IDs, file manifest accounting is in place, parallelism recommendations are provided by the planner, and documentation covers real usage decisions.

**v4.1 "Output Quality & DX" (F1–F6) is complete.** Stdout destination for pipe workflows, parameterized queries, Slack notifications, memory-based batch sizing, data quality checks (row count bounds, null ratio, uniqueness), and file size splitting are all implemented and tested.

## v4 Features (implemented)

### Configurable Parquet Compression (Story M1)

- `compression` field per export: `zstd` (new default), `snappy`, `gzip`, `lz4`, `none`
- Optional `compression_level` for zstd (1..22, default 3) and gzip (0..10, default 6)
- Invalid codec/level combinations rejected at config parse time
- CSV exports ignore the compression setting

### Skip Empty Exports (Story M2)

- `skip_empty: true` per export — no file created when query returns 0 rows
- Cursor state is not advanced (safe for incremental idle runs)
- Run summary shows `status: skipped` instead of `success`

### Shell Completions (Story M5)

- `rivet completions <shell>` generates completions for bash, zsh, fish, powershell
- Powered by `clap_complete`

### File Manifest Accounting (Story D3)

- Every file produced by an export is recorded in `file_manifest` SQLite table
- Tracks: `run_id`, `export_name`, `file_name`, `row_count`, `bytes`, `format`, `compression`, `created_at`
- CLI: `rivet state files --config <path>` shows recent files
- Filter by export: `--export orders`, limit: `--last 50`
- Enables post-run reconciliation: which run created which files with what row counts

### Run ID Alignment (Story D4)

- Each export run gets a canonical `run_id` (format: `{export_name}_{timestamp}`)
- `run_id` appears in: end-of-run summary, `export_metrics` table, `file_manifest` table
- `rivet metrics` output shows `run_id` instead of raw `run_at` timestamp
- Same `run_id` links summary, metrics, and files for full traceability

### Parallelism Recommendation (Story L6)

- `rivet check` now recommends a parallelism level for each export
- Only chunked mode benefits from parallelism; other modes show `1 (only chunked mode benefits)`
- Recommendation considers: row estimate, index usage, dataset size
- Scale: 1 (too small / no index + large) → 2 (moderate or no-index conservative) → 4 (large indexed)
- Includes reason string explaining the recommendation

## v4.1 Features (implemented)

### Stdout Destination (F1)

- `destination: { type: stdout }` for pipe workflows
- CSV to terminal, Parquet to file redirect, pipe to DuckDB
- Logging to stderr, data to stdout — no mixing

### Parameterized Queries (F2)

- `--param key=value` CLI flag (repeatable) for `rivet run` and `rivet check`
- `${key}` placeholders substituted in queries and `query_file` contents
- Params take precedence over env vars of the same name

### Slack Notifications (F3)

- `notifications.slack` config block with `webhook_url` or `webhook_url_env`
- Triggers: `failure`, `schema_change`, `degraded`
- POSTs color-coded Slack attachment with run_id, status, rows, error

### Memory-based Batch Sizing (F4)

- `batch_size_memory_mb: 256` in tuning config
- Auto-computes batch_size from Arrow schema width after first batch
- Heuristic: fixed-width types are exact, strings estimated at 256B
- Clamped to `[1000, 500000]` rows
- Mutually exclusive with explicit `batch_size`

### Data Quality Checks (F5)

- Per-export `quality` config: `row_count_min`, `row_count_max`, `null_ratio_max`, `unique_columns`
- Runs after extraction, before upload — fail-level issues abort the export
- Null ratios tracked incrementally (no full data buffering)
- Uniqueness via streaming HashSet (recommended for <10M cardinality)
- Summary shows `quality: pass` or `quality: FAIL`

### File Size Splitting (F6)

- `max_file_size: 512MB` per export (supports KB/MB/GB suffixes)
- After each batch, checks `bytes_written()` on the FormatWriter
- Splits on batch boundaries — use smaller `batch_size` for finer granularity
- When threshold exceeded: finishes current file, uploads part, opens new writer
- File naming: `{export}_{ts}_part{N}.{ext}` (no suffix when single file)
- Cursor advances only after ALL parts succeed

---

## v5 Features (implemented) — Plan/Apply workflow

Auditable execution: a `rivet plan` produces a sealed JSON artifact (`PlanArtifact`); `rivet apply` executes it with staleness, cursor-drift, and fingerprint guards. See [ADR-0005](docs/adr/0005-plan-apply-contracts.md).

- **PA1–PA8** — artifact is the sole input, immutable, staleness (1h warn / 24h reject), cursor-snapshot integrity, chunk-range monotonicity, fingerprint logging, state writes preserved (I1–I4), diagnostics advisory.
- **PA9** (ADR-0005, 2026-04) — **artifact credential redaction**: `password:` stripped; `scheme://user:pass@` rewritten to `REDACTED`. WARN logged so operators migrate to `password_env:`.

---

## v6 Features (implemented) — Source-aware planning (Epics A–I)

A new planning layer advises on what to extract first, what to isolate on shared sources, and where to repair. Advisory only — no runtime scheduling. See [ADR-0006](docs/adr/0006-source-aware-prioritization.md) and [planning/prioritization.md](docs/planning/prioritization.md).

### Epic A — Source-aware extraction prioritization

- Per-export `priority_score` / `priority_class` / `cost_class` / `risk_class` / `recommended_wave`.
- Explainable `reasons[]` — structured kinds (`small_table`, `weak_cursor`, `sparse_range_risk`, `reconcile_required`, …).
- Campaign view: ordered exports + grouped waves + `source_group_warnings[]` for shared-replica collisions.
- Surfaces in `rivet plan` (pretty + JSON).

### Epic B — Metadata-driven discovery

- `rivet init --discover -o out.json` — machine-readable `DiscoveryArtifact` with ranked **cursor candidates**, **chunk candidates**, per-column nullability, total bytes, suggested mode, and automatic coalesce-fallback hints.
- `rivet init --source-env / --source-file` — credentials stay out of shell history / `ps`.

### Epic C — Weak-source awareness

- `reconcile_required: true` in `exports[]` — advisory flag that folds into prioritization risk class independently of the `--reconcile` CLI flag.
- `CursorQuality` taxonomy (strong_monotonic / weak_time / weak_multi_candidate / fallback_only / none) consumed by the scorer.

### Epic D — Cursor policy model (ADR-0007 CC1–CC10)

- `incremental_cursor_mode: single_column | coalesce` — composite cursor over `COALESCE(primary, fallback)` when the primary is NULL-able.
- Synthetic result column `_rivet_coalesced_cursor` stripped before Parquet/CSV write (CC5).
- Single-level SQL with outer `ORDER BY` so the final Arrow batch carries the max coalesced value (CC6).
- Identifier quoting + cursor-value escaping (CC9, CC10).

### Epic E — Campaign-level planning

- Multi-export `rivet plan` attaches the campaign to every artifact. `source_group` collisions drive `isolate_on_source` flags (advisory).

### Epic F — Partition / window reconciliation (ADR-0009 RC1–RC6)

- `rivet reconcile -c cfg.yaml -e export_name` — re-counts every chunk partition on the source, classifies `match` / `mismatch` / `unknown`, emits a JSON `ReconcileReport`.
- Requires `chunk_checkpoint: true` (v1); time-window reconcile deferred.
- Reuses `build_chunk_query_sql` verbatim — apples-to-apples with extraction (RC2).

### Epic G — Committed / verified progression (ADR-0008 PG1–PG8)

- New state table `export_progression` (schema v4 migration) separates three boundaries: observed (preflight/plan), committed (destination write), verified (reconcile all-match).
- `rivet state progression [--export N]` — operator-facing report.
- Committed advances at end-of-run (incremental after cursor update; chunked after `finalize_chunk_run_completed`). Verified advances only from clean reconcile (PG5).

### Epic H — Targeted repair (ADR-0009 RR1–RR8)

- `rivet repair -c cfg.yaml -e export_name [--report rec.json] [--execute]` — derives a `RepairPlan` from a `ReconcileReport`, optionally re-runs only the mismatched chunks via `ChunkSource::Precomputed`.
- New files written alongside originals with `<export>_<ts>_chunk<idx>.<ext>` naming (RR5). Committed boundary untouched (RR4).

### Epic I — Historical recommendation refinement

- `plan::HistorySnapshot` summarizes the last ~20 `export_metrics` rows (success_rate, retry_rate, avg_duration, last_status).
- Bounded contribution (≤ ~15 points combined) to prioritization scoring — history is a signal, not an oracle.

---

## v6.1 Features (implemented) — SecOps hardening

See commit `08c16f2` and ADR-0005 PA9.

- **TLS for source connections** — `source.tls: { mode: disable|require|verify-ca|verify-full, ca_file, accept_invalid_certs, accept_invalid_hostnames }`. Default is `verify-full` when `tls:` block is present; omitting it connects plaintext with a WARN.
- **`rivet init --source-env / --source-file`** — URL never hits command line / `ps` / container logs.
- **Env-var hard error** — `${UNSET}` in YAML now fails at load time instead of silently substituting empty string.
- **AWS_PROFILE race fix** — `std::env::set_var` guarded by static Mutex + RAII restore.
- **Zeroize for in-memory secrets** — AWS/GCS keys and resolved DB passwords use `zeroize::Zeroizing<String>`.
- **Parameterized cursor** — MySQL binds `?`; PostgreSQL uses `E'…'` with escaping regardless of `standard_conforming_strings`.
- **Query log at debug level** — effective SQL only printed with `RUST_LOG=debug`.

---

## Compatibility

Rivet is exercised against the full end-to-end suite (83 assertions) on:

- **PostgreSQL** 12, 13, 14, 15, 16
- **MySQL** 5.7, 8.0

See [reference/compatibility.md](docs/reference/compatibility.md) for the support policy, the test matrix (7 targets × 83 assertions = 581 e2e assertions), and engine-specific notes.

---

## Roadmap

### v4.2: Resilience & Testing (in progress)

| Feature | Description | Complexity | Status |
|---|---|---|---|
| Toxiproxy integration | Network fault injection for retry/resilience testing | Low | **Done** — docker-compose service, setup script, E2E test script (Q1–Q8 all pass) |
| E2E test harness | Automated test scripts for all v4.1 features | Medium | **Done** — `dev/test_*.yaml` configs, `USER_TEST_PLAN.md` suites L–Q all pass |
| CI pipeline | GitHub Actions for build, test, lint on every PR | Medium | Pending |

### v4: Load + Transform

| Feature | Description | Complexity |
|---|---|---|
| BigQuery Load | GCS -> BQ staging table | Medium |
| MERGE / Upsert | `daily_update` merge, `full_replace` with row count validation | High |
| Deduplication | Auto-generated dedup SQL from MERGE configs | Medium |
| Delta Lake / Iceberg | Alternative output formats with ACID, time travel | High |
| Per-column Parquet encoding | Column-specific encoding hints (DELTA, RLE, DICTIONARY) | Medium |

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
| `reqwest` | HTTP client (Slack notifications) |
| `tempfile` | Temporary file for streaming writes |
| `libc` + `mach2` | macOS memory monitoring |
| `rand` | Test data generation (seed tool) |

## Building

```bash
cargo build --release                    # build rivet (9.9MB binary)
cargo build --release --bin seed         # build seed tool
cargo test                               # run all tests (see Test Coverage)
cargo clippy --all-targets               # lint (0 warnings)
```

## Development Setup

```bash
docker compose up -d                     # start PG + MySQL
cargo run --release --bin seed -- --target both --users 100000
RUST_LOG=info cargo run --release --bin rivet -- run --config dev/pg_full.yaml
```
