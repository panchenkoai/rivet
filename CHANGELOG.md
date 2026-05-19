# Changelog

## 0.6.0 (2026-05-19)

### Configuration ergonomics, MySQL parity, pooler-awareness, and a published cross-tool benchmark

A user-facing release. Seven new features (the `table:` config shortcut,
auto-resolved chunk columns from primary keys, memory-budgeted chunking on
both engines, `work_mem`-aware `FETCH` capping on Postgres, a `MysqlProxyKind`
classifier for ProxySQL / MaxScale, and a batch-memory-aware MySQL row
buffer); a packaging change — the MCP server is now shipped as a separate
binary, `rivet-mcp`; and a published cross-tool benchmark harness with a
steelman re-run that gives every competitor its best plausible config.

#### Breaking / packaging

- **`rivet-mcp` is now a separate binary.** Previously available as
  `rivet mcp <…>` subcommands, the MCP (Model Context Protocol) server is
  now its own crate binary under `src/bin/rivet-mcp.rs`. Homebrew, Docker,
  GitHub Release tarballs, and `cargo install rivet-cli` all install both
  `rivet` and `rivet-mcp` from 0.6.0 onwards. Claude Desktop / Claude Code
  configs that previously invoked `rivet mcp` should be updated to call
  `rivet-mcp --stdio` directly. The crate name remains `rivet-cli` on
  crates.io.

#### New features

- **`feat(config)`** — `table:` shortcut + `source.environment`. An export
  can now be expressed as `table: orders` instead of writing a full
  `query: "SELECT ... FROM orders"` block; Rivet derives the column list
  from the catalog and resolves the schema automatically.
  `source.environment` lets a single config carry per-environment defaults
  without YAML anchors.
- **`feat(chunked)`** — `chunk_column` is auto-resolved from the primary
  key on the `table:` shortcut. No more manually copying the PK name into
  the YAML.
- **`feat(chunked)`** — `chunk_size_memory_mb` (memory-budgeted chunk
  sizing) + a small-table escape path that runs sub-threshold tables in a
  single chunk instead of forcing the planner to emit a one-row range.
- **`feat(source)`** — `work_mem`-aware `FETCH` cap on the Postgres server
  side cursor. The longest single SQL statement Rivet issues on a 2 M-row
  wide table is now **0.19s** (`FETCH 142 FROM _rive`) instead of the
  multi-second hold that `FETCH FORWARD 10000` produced under tight
  `work_mem`. Run summary now includes `pg_temp_bytes` so an operator can
  see how much spill the export caused.
- **`feat(mysql)`** — full chunking parity with Postgres: auto-resolved
  `chunk_column` from the PK + `chunk_size_memory_mb` derived from
  `information_schema.TABLES.AVG_ROW_LENGTH` (with a `/3` correction
  above 8 KB to compensate InnoDB's BLOB inflation).
- **`feat(mysql)`** — batch-memory-aware `row_buf` cap. RSS on the wide
  MySQL `content_items` fixture no longer scales with chunk size: peak RSS
  stays at **280 MB** regardless of `chunk_size_memory_mb` setting.
- **`feat(mysql)`** — `MysqlProxyKind` 4-signal classifier identifies
  ProxySQL, MaxScale, and generic multiplexers at connect time and emits
  the pool-safety warning before the export starts (mirrors the existing
  Postgres pgBouncer / Odyssey PID-flip probe). ProxySQL is now wired into
  the nightly live test job.

#### Fixes

- **`fix(source)`** — use the requested `FETCH N` size for the cursor
  exhaustion check on Postgres. Previously the code compared returned-row
  count against the hard-coded default, so adaptive batch sizing could
  declare the cursor exhausted one batch early on tables whose row width
  triggered an early `work_mem` cap.
- **`fix(lib)`** — drop the blanket `#[allow(dead_code)]` on the
  `preflight` module by exposing it as `pub mod`. Eliminates 30+ legitimate
  dead-code warnings that the blanket allow was hiding.
- **`fix(clippy)`** — struct-init shorthand for `TuningConfig` across
  live tests (no behavior change).

#### Refactors

- **`refactor(mcp)`** — extract the MCP server into the `rivet-mcp` binary
  (see "Breaking / packaging" above).
- **`refactor(cli)`** — split the ~1000-line `src/cli/mod.rs` into
  `args.rs` / `validate.rs` / `params.rs` / `dispatch.rs` so each file owns
  a single concern.
- **`refactor(tuning)`** — split the 678-line `src/tuning.rs` into
  `tuning/{profile,memory,adaptive}.rs`. `next_adaptive_batch_size` from
  0.5.3 stays a pure function, now in `adaptive.rs`.
- **`refactor(chunked)`** — split the 661-line `pipeline/chunked/mod.rs`
  into sibling `sequential_checkpoint.rs` + `parallel_checkpoint.rs`,
  with chunk math extracted to `math.rs`. Cuts the largest pipeline file
  by ~75%.
- **`refactor(source)`** — extract Arrow-conversion machinery into
  `source/postgres/arrow_convert.rs` and `source/mysql/arrow_convert.rs`,
  leaving the `mod.rs` files focused on transport / cursor / chunking.
- **`refactor(chunked)`** — drop the subquery wrap on min/max/COUNT
  boundary queries for the `table:` shortcut. Eliminates a planner
  pessimization on tables with a partial index on the chunk column.
- **`refactor(mysql)`** — `AVG_ROW_LENGTH / 3` correction above 8 KB to
  compensate InnoDB BLOB inflation. Wide-MySQL `chunk_size_memory_mb`
  derivations are now within ~10% of measured per-row width.

#### Tests / CI

- **MySQL symmetry suite** — seven new live test files mirror the
  Postgres coverage: `live_mysql_chunked_recovery`,
  `live_mysql_crash_recovery`, `live_mysql_reconcile_repair`,
  `live_mysql_resume`, `live_mysql_retry_and_faults`,
  `live_mysql_schema_drift`, `live_mysql_chunked`. The MySQL twin of
  `live_oltp_load` validates retry / streaming under concurrent INSERTs.
- **`test(chunked)`** — parallel checkpoint recovery (cases C3, C4) +
  panic hooks in the parallel worker so a `panic!()` inside `BatchSink`
  no longer leaks a thread, the run aborts cleanly, and the checkpoint
  state machine is in a resumable state.
- **`ci`** — ProxySQL added to `docker-compose.yaml` (`pool` profile) and
  wired into `.github/workflows/nightly-live.yml` alongside the existing
  pgBouncer coverage.

#### Benchmarks

- **`bench`** — first published cross-tool benchmark harness in
  `docs/bench/`. Compares Rivet against `sling`, `dlt`, `duckdb`
  (`postgres_scanner` / `mysql_scanner`), `clickhouse-local`, and
  `odbc2parquet` 11.0.0 on a 22-table Postgres fixture (incl. a 2M-row ×
  20-wide-column `content_items`) and a 17-table MySQL fixture. Reports
  longest single SQL, peak RSS, wall, failure count, DB-side
  counter deltas (`pg_stat_database`, `Innodb_rows_read`, `processlist`).
  Rivet: PG 0.19s / 443 MB peak, MySQL 9s / 280 MB peak, 0 / 22 + 0 / 17
  table failures.
- **`bench(steelman)`** — second report that re-runs every competitor at
  its best plausible configuration (e.g. `odbc2parquet --batch-size-memory
  256MiB --sequential-fetching`, narrow-table `--column-length-limit`).
  On narrow tables the gap closes substantially; on wide `content_items`
  Rivet's edge survives (~58× peak RSS, ~700× longest single query).
- **`bench(odbc2parquet)`** — numbers refreshed against v11.0.0 (was
  v6.0.7). Wall improved ~15% on the full suite; architectural shape
  unchanged.

#### Docs

- **Trust contracts surface** — `docs/semantics.md § Crash semantics` +
  `§ Known non-guarantees`, `docs/reliability-matrix.md` (PR CI / nightly
  / manual breakdown), `docs/reference/compatibility.md` (exact PG 12–16,
  MySQL 5.7 / 8.0 versions exercised), `SECURITY.md § Sensitive local
  artifacts`, all linked from the README "Trust contracts" table.
- **Onboarding funnel consolidation** — one canonical document per stage:
  `getting-started.md` (first run), `docs/concepts.md` (glossary),
  `docs/pilot/README.md` (operator runbook). Removed parallel
  `pilot/quickstart-postgres.md` + `pilot/quickstart-mysql.md` whose
  content was duplicated from `getting-started.md`.
- **Architecture refresh** — `docs/architecture.md` updated for the
  `tuning/` and `chunked/` splits and the new `rivet-mcp` binary.
- **GIFs** — all 12 default scenarios (and the opt-in `pool-detect`)
  re-rendered against the 0.6.0 binary. New `coalesce-cursor.gif` shows
  `incremental_cursor_mode: coalesce` correctly tracking 30 late-arriving
  rows whose `updated_at` is `NULL`.
- **README** — claims now back-linked to measured numbers (the
  "Source pressure, measured" table is generated from
  `docs/bench/reports/REPORT_*.md`).

---

## 0.5.3 (2026-05-17)

### Architecture audit pass — fault-tolerance, observability, CI hardening

No new user-facing features. All changes are bug fixes, internal refactors,
new unit/CI coverage, and ADR-documented design decisions. The CLI surface
(flags, config schema, output formats) is unchanged.

#### Fixes

- **`fix(postgres)`** — RAII `PgTxnGuard` around the cursor txn. Closes G1
  from the DBA audit: a panic between `BEGIN` and `COMMIT` (e.g. inside a
  `BatchSink::on_batch`) used to leak the open transaction back into the
  pool. The guard's `Drop` now issues a best-effort `ROLLBACK` so the
  connection is returned clean even on unwind. Regression test
  `pg_panic_in_sink_releases_cursor_and_aborts_txn` exercises the panic
  path via `std::panic::catch_unwind`.
- **`fix(adaptive)`** — call `pg_stat_clear_snapshot()` before each
  `checkpoints_req` sample. PostgreSQL caches the stats snapshot at
  transaction start, so every adaptive sample inside the cursor txn was
  returning the frozen value from `BEGIN` time — making the feature blind
  to checkpoints that accumulated during the export. Adaptive batch sizing
  now actually reacts (verified end-to-end: a 200K-row content_items
  export under WAL pressure goes from `batches=40 min=5000 max=5000` to
  `batches=121 min=500 max=5000` with `adaptive: true`).
- **`fix(test)`** — `live_oltp_load.rs` had a long-standing precondition
  bug: `SELECT *` on a `NUMERIC(12,2)` column loses precision metadata in
  the subquery wrap. Tests now declare an explicit `amount` decimal
  override; the property under test (retry/streaming under concurrent
  inserts) is unrelated to NUMERIC inference.

#### Refactors

- **`refactor(retry)`** — `classify_error` now returns the typed
  `RetryClass { Permanent | Transient { needs_reconnect, extra_delay_ms } }`
  instead of a `(bool, bool, u64)` tuple. Positional destructuring made
  the two booleans easy to confuse; named accessors (`is_transient`,
  `needs_reconnect`, `extra_delay_ms`) are now used at every call site.
- **`refactor(source)`** — `Source::export` packs its 5 read-only
  parameters into a named `ExportRequest` struct. Call sites read like
  `ExportRequest { query, incremental, cursor, tuning, column_overrides }`
  instead of relying on positional order.
- **`refactor(journal)`** — `RunJournal`, `RunEvent`, `JournalEntry`, and
  `PlanSnapshot` move out of `pipeline::journal` to a new top-level
  `crate::journal`. This eliminates a layering inversion where
  `state::journal_store` was importing from `pipeline::*` (state →
  pipeline is the wrong direction). The `From<&ResolvedRunPlan>` impl
  moves to `pipeline/summary.rs` beside its sole caller.
- **`refactor(adaptive)`** — `next_adaptive_batch_size` extracted into a
  pure function in `tuning.rs` and shared between `PostgresSource` and
  `MysqlSource`. The shrink/grow decision is now unit-testable without a
  live database (6 unit tests covering floor, ceiling, oscillation
  convergence). `ADAPTIVE_SAMPLE_INTERVAL` and `ADAPTIVE_MIN_BATCH`
  promoted to public constants in `tuning`.
- **`refactor(sink)`** — `pipeline/sink.rs` converted to `pipeline/sink/`
  with `extract_last_cursor_value` and its 11 Arrow-type tests moved into
  `pipeline/sink/cursor.rs`. `sink/mod.rs` drops from 1525 → 1295 LOC.

#### Performance

- **`perf(chunked)`** — replace the busy-wait worker semaphore in
  `pipeline/chunked/exec.rs` (atomic + 50ms sleep loop) with a
  `Mutex<usize> + Condvar`-backed `resource::Semaphore`. Blocked
  acquirers now park in the kernel until a worker calls `release()`,
  instead of polling 20×/sec per blocked thread.

#### Tests

- **22 new unit tests** covering the previously-zero-coverage checkpoint
  state machine (`ensure_chunk_checkpoint_plan` 5-transition matrix +
  `record_chunked_commit` boundary advancement), the duplicate-write
  guard in `run_with_reconnect` (`decide_export_retry` 6-case matrix),
  the chunked quality gate (`run_chunked_quality_gate` 7 cases),
  adaptive batch sizing math, and `mcp::ascii_table` formatter.
- Coverage delta on critical pipeline files:
  - `pipeline/chunked/mod.rs`: 0% → 37.26% line coverage
  - `pipeline/single.rs`: 41% → 48.53%
  - `pipeline/job.rs`: 21.80% → 56.83%
  - Total: 66.78% → 69.73%

#### CI / Operational

- **`ci`** — live tests now run on every PR. Swap the upstream-yanked
  `bitnami/pgbouncer:latest` for `edoburu/pgbouncer:latest` in
  `docker-compose.yaml`, bring up pgBouncer in the per-PR `e2e` job, and
  filter the heavy 50K+-row `content_export` tests out of e2e (they ran
  for minutes and required a separate fixture). The full live suite
  including `content_load` runs nightly via the new
  `.github/workflows/nightly-live.yml` workflow (03:30 UTC +
  `workflow_dispatch`). 151 live tests now gate every merge to `main`.

#### Docs

- **ADR-0010** — "Two parallel execution engines": documents the
  in-process `thread::scope` engine (chunked-single-table) vs the
  subprocess engine (`parallel_children`) as a deliberate split, with
  the conditions that would trigger unification.
- **ADR-0011** — "`Source: Send` not `Sync`": records that the
  `Mutex<Client>` prototype produced a measured 1.7× slowdown on a
  4-thread chunked export, so per-worker connections remain the right
  shape for blocking SQL drivers.

---

## 0.5.2 (2026-05-15)

### Wave 2 live E2E test suite — full CLI flag coverage with behavioral assertions

56 live integration tests across 5 new test files. Every CLI flag now has at least
one end-to-end test that verifies observable behavior, not just exit code.

#### New test files

- **`tests/live_init.rs`** (I1–I5) — schema-wide discovery, single-table init,
  `--out` flag, MySQL init, unreachable-URL error message.
- **`tests/live_init_extended.rs`** (IE1–IE5) — `--source-env`, `--source-file`,
  `--schema`, `--discover` JSON artifact, unset env var error.
- **`tests/live_plan_apply.rs`** (PA-L1–PA-L8) — plan+apply round-trips (full +
  chunked), credential redaction, staleness gate, `--force`, missing plan file,
  `--param` substitution with DB row-count verification.
- **`tests/live_reconcile_repair.rs`** (RR1–RR6 + RR4b) — reconcile pretty/JSON/file
  output, repair dry-run, `--execute`, `--format json`, and `--report` flag
  exercising the precomputed reconcile JSON path.
- **`tests/live_cli_flags.rs`** (31 tests) — `rivet run`, `rivet check`,
  `rivet doctor`, `rivet state`, `rivet metrics`, `rivet journal`, `rivet completions`.

#### Assertion hardening (previously "conscious compromise")

All 8 previously weak behavioral assertions replaced with concrete checks:

| Test | Before | After |
|---|---|---|
| `run_reconcile_flag_exits_zero_when_counts_match` | exit 0 only | `total_rows==25` in JSON + `"MATCH"` in stderr |
| `check_json_flag_outputs_type_report_as_json` | parse first `{` line | parse full `ExportTypeReport`; verify export name, column names, zero violations |
| `check_param_flag_substitutes_in_query` | exit 0 only | `--json` verifies column discovery + zero violations |
| `check_type_report_shows_column_table` | `contains("id") \|\| contains("name")` | `contains("int8")` AND `contains("exact")` |
| `state_chunks_shows_checkpoint_table` | output text only | DB query: `COUNT(chunk_task WHERE status='completed') == 2` |
| `metrics_last_flag_limits_output` | fragile `!starts_with("EXPORT")` filter | run_id–based: newer present, older absent |
| `journal_shows_run_summary` | `"success" \|\| table.name()` | two `&&` assertions: both required |
| `check_mysql_basic_exits_zero` | `\|\| contains("pass")` matched "password" | removed; asserts `contains(table.name())` only |

#### Source fixes uncovered during test authoring

- `src/init/postgres.rs`: `::regclass` cast → `to_regclass()` for graceful
  table-disappear handling during schema-wide discovery.
- `src/init/mod.rs`: `introspect_all` skips "not found or has no columns" errors
  gracefully instead of aborting the entire init.

---

## 0.5.1 (2026-05-15)

### Stabilization: correctness tests, benchmark evidence, best-practice docs

This release closes the v0.5.x stabilization roadmap. No new features; all
changes are tests, documentation, and benchmark infrastructure.

#### Tests added

- **`tests/gremlin.rs`** (G1–G5) — live fault tests covering `row_count_min`
  on empty tables, exhausted incremental cursors, `unique_max_entries` cap
  warning, `auto_shrink` + quality gate correctness, and crash-before-quality-check
  recovery.
- **`tests/live_chunked_recovery.rs`** (C1–C2) — chunked pipeline crash+resume
  matrix. C1: crash after `complete_chunk_task` → resume skips completed chunk,
  no duplicates. C2: crash after file written but before commit → chunk 0 stuck
  in `running` → resume resets and re-runs it (at-least-once delivery documented).
- **Chunk-level fault injection** (`src/test_hook.rs`, `src/pipeline/chunked/mod.rs`)
  — new `maybe_panic_at_chunk("after_chunk_file", N)` and
  `maybe_panic_at_chunk("after_chunk_complete", N)` hooks, matching
  `RIVET_TEST_PANIC_AT=after_chunk_file:0` etc.

#### Benchmark evidence

Full Phase 2 benchmark suite run against live Postgres. Report:
[docs/benchmark_report_v0.5.x.md](docs/benchmark_report_v0.5.x.md).

Key results (measured, not estimated):

| Claim | Result |
|---|---|
| `balanced` vs `none`: same wall time, 2.6× smaller output | ✓ confirmed |
| `compact` adds no compression improvement over `balanced` on numeric data | ✓ confirmed |
| Smaller row group targets cut RSS with zero wall-time cost | ✓ confirmed |
| Memory policy cap: zero overhead when cap doesn't trigger | ✓ confirmed |
| `auto_shrink` on wide real-world table: 5.7× RSS reduction (878 → 154 MB) | ✓ confirmed |
| xxHash3-64 uniqueness tracking: < 1 MB overhead on 200K-row table | ✓ confirmed |

#### Benchmark infrastructure fixes

- Bench configs used `${VAR:-default}` bash syntax unsupported by Rivet's env
  interpolation; replaced with plain `${VAR}`.
- Integer fields (`target_row_group_mb`, `max_batch_memory_mb`) were quoted in
  YAML, causing `invalid type: string` parse errors; removed quotes so values
  interpolate as YAML integers.
- `run_bench.sh`: added `--export` flag support; introduced `BENCH_RUN_OUT`
  per-scenario output dir so each run writes to a clean, isolated path;
  added output-dir cleanup before each scenario to prevent file accumulation.

#### Documentation

All v0.5.x best-practice guides are complete:

- [Resource-aware extraction](docs/best-practices/resource-aware-extraction.md)
- [Parquet tuning](docs/best-practices/parquet-tuning.md)
- [Compression profiles](docs/best-practices/compression-profiles.md)
- [Quality checks](docs/best-practices/quality-checks.md)
- [Low-memory runners](docs/best-practices/low-memory-runners.md) — numbers measured, not estimated
- [Recovery and resume](docs/best-practices/recovery-and-resume.md)
- [Benchmark methodology](docs/best-practices/benchmark-methodology.md)

README `Resource-aware extraction` section added. `rivet plan` memory estimate
documented as advisory heuristic in `docs/reference/cli.md`.

---

## 0.5.0 (2026-05-15)

### Parquet row group auto tuning

A new `parquet:` block on any Parquet export controls how many rows Rivet places in each row group. Row group size affects memory usage during write, compression ratio, and downstream read performance (predicate pushdown, column skipping).

```yaml
exports:
  - name: events
    format: parquet
    parquet:
      row_group_strategy: auto      # auto | fixed_rows | fixed_memory
      target_row_group_mb: 128
      max_row_group_mb: 256
```

The `auto` strategy estimates row width from Arrow schema column types and computes rows-per-group to fit within the target memory budget. Narrow tables (IDs, timestamps) get large groups; wide tables (TEXT/JSON) get smaller groups automatically — no manual tuning required.

| Strategy | Behavior |
|---|---|
| `auto` | Compute from schema + `target_row_group_mb` (default 128 MB) |
| `fixed_rows` | Use `row_group_rows` as a literal row count |
| `fixed_memory` | Same math as `auto`, explicit label in logs |

When `parquet:` is omitted, Rivet uses the library default (1,048,576 rows/group). `rivet plan` shows the selected strategy and target when the block is present.

### Compression profiles

A new `compression_profile` field replaces the need to manually choose a codec and level. Set it on any export and Rivet picks the right `(codec, level)` pair:

| Profile | Codec | Best for |
|---|---|---|
| `none` | uncompressed | Debug / scratch |
| `fast` | snappy | Backfills, low-CPU |
| `balanced` | zstd level 3 | Production default |
| `compact` | zstd level 9 | Storage/network-sensitive |

```yaml
exports:
  - name: events
    format: parquet
    compression_profile: balanced
```

`compression_profile` takes precedence over `compression` + `compression_level`. Existing configs are unaffected — those fields still work.

### Batch memory hard cap (`max_batch_memory_mb`)

A new tuning parameter adds a **batch-level** memory guard independent of the process-level `memory_threshold_mb`:

```yaml
tuning:
  max_batch_memory_mb: 128
  on_batch_memory_exceeded: warn   # warn (default) | fail | auto_shrink
```

Rivet measures the actual Arrow buffer footprint of each batch using `get_array_memory_size()`. When a batch exceeds the limit:

- **`warn`** — logs the actual size, the limit, and a suggested `batch_size`. Export continues.
- **`fail`** — returns an error immediately. Use in CI to block oversized batches.
- **`auto_shrink`** — splits the batch in half recursively until each sub-batch fits, then writes them individually. Transparent to the rest of the pipeline — row count and output are identical.

The warning includes an actionable suggestion:

```
batch memory 184 MB exceeds max_batch_memory_mb=128 MB (5000 rows). Consider lowering batch_size to ~3478.
```

### Resource plan output

`rivet plan` now shows a **Resources** section in pretty output with per-batch memory estimates:

```
  Resources:
    Batch size   :  10,000 rows
    Batch memory : ~2 MB (narrow) – ~95 MB (wide)
    RSS guard    : 4,096 MB
    Throttle     : 50 ms between batches
```

The narrow/wide bounds bracket the expected per-batch memory at ~200 B/row and ~10 KB/row respectively. A `⚠` advisory appears when the upper bound exceeds 128 MB/batch, suggesting `batch_size_memory_mb` or a lower `batch_size`. No database connection is required to compute the estimate — it is derived from tuning settings alone.

### Quality uniqueness: typed hashing and memory cap

`unique_columns` quality checks now use **typed xxHash3-64** instead of string formatting. Numeric and binary columns are hashed directly from raw bytes — no intermediate string allocation. CPU overhead for quality-enabled runs is ~2.6–2.8× lower on Int64 and Utf8 columns.

A new **`unique_max_entries`** field caps the number of distinct values tracked per column:

```yaml
quality:
  unique_columns: [id, email]
  unique_max_entries: 1000000
```

When the cap is reached, a `Warn` issue is emitted and tracking stops for that column. Without this field, tracking is still unbounded — the cap is opt-in. This prevents uncontrolled RAM growth on high-cardinality columns (UUIDs, event IDs, email addresses) on very large tables.

### `rivet init` scaffolds `parquet:` auto-tuning

`rivet init` now includes a `parquet:` block in the generated YAML for every chunked export and any full-mode export estimated at more than 100 k rows. The block is pre-filled with the right strategy and a schema-aware target:

- **Narrow tables** (fewer than 5 text/JSON/bytea columns) → `target_row_group_mb: 128`
- **Wide tables** (5 or more text/JSON/bytea columns) → `target_row_group_mb: 64`

```yaml
# generated for content_items (2M rows, body + raw_html + metadata jsonb + …)
  - name: content_items
    mode: chunked
    format: parquet
    parquet:
      row_group_strategy: auto
      target_row_group_mb: 64
```

No action needed for existing configs — the block is only added to newly generated scaffolds.

### Fix: `--resume` with no checkpoint exits non-zero

Previously, running `rivet run --resume` on a chunked export that had no in-progress checkpoint would log a warning and silently start a fresh run. This masked operator mistakes (wrong `--config`, post-`reset-chunks` double-resume). The command now exits non-zero with an actionable message:

```
error: export 'big_table': --resume but no in-progress chunk checkpoint;
       run without --resume first or `rivet state reset-chunks --config <cfg> --export big_table`
```

---

## 0.4.0 (2026-05-14)

PostgreSQL state backend, type safety layer, destination path templates, and a set of reliability fixes.

### PostgreSQL state backend

`StateStore` now supports PostgreSQL as an alternative to the default SQLite file. Set `RIVET_STATE_URL` to a PostgreSQL connection string to activate:

```bash
export RIVET_STATE_URL=postgresql://rivet:rivet@localhost:5433/rivet_state
rivet run --config rivet.yaml
```

All state tables — cursor, metrics, manifest, schema drift, shape drift, chunk checkpoints, progression, run journal, run aggregate — are created automatically on first connect via versioned `PG_MIGRATIONS`. The same schema version sequence (`v1`–`v7`) is enforced for both backends; the migration runner verifies the final version after each run and cleans up superseded version rows.

**Parallel chunk workers** open their own short-lived connections per `claim` / `complete` / `fail` operation so they do not contend on a shared connection.

**Security:** passwords are redacted from log and error messages (`postgresql://user:***@host/db`). A `WARN` is emitted when connecting to a non-localhost host without TLS, prompting the use of `sslmode=require`.

A dedicated `postgres-state` service is included in `docker-compose.yaml` (port 5433) for local development.

### Type safety layer (`rivet check --type-report`)

New flags on `rivet check` surface the full type pipeline for every column in a query:

- `--type-report` — prints a table: column name, source native type, Rivet type, Arrow type, and fidelity level.
- `--strict` — exits non-zero if any column mapping is `lossy` or `unsupported`.
- `--json` — emits the report as newline-delimited JSON (one object per export); pipe-friendly.
- `--target bigquery` — adds `Target type` / `Status` columns showing BigQuery mapping (`NUMERIC`, `BIGNUMERIC`, `TIMESTAMP`, `REPEATED …`) with `ok` / `warn` / `fail` and inline notes for edge cases.

**Type fidelity levels:**

| Level | Meaning |
|---|---|
| `exact` | Round-trips without loss |
| `compatible` | Structurally compatible; minor representation difference |
| `logical_string` | Serialised to STRING/text — no native Arrow type available |
| `lossy` | Precision or range reduction |
| `unsupported` | No mapping; column is skipped in Parquet output |

**Column type overrides (`columns:`)** — per-column override block in the export YAML pins a decimal type when the inferred type is wider than needed:

```yaml
exports:
  - name: orders
    columns:
      amount: decimal(15,4)
```

**Complex types — Postgres and MySQL:**

- **Enum** (`pg: enum`, `mysql: ENUM/SET`) — written as `Utf8`.
- **Interval** (`pg: INTERVAL`) — written as `Utf8` ISO 8601 string (`P1Y2M3D`, `PT0S`).
- **Arrays / lists** (`pg: _text`, `_int8`, etc.) — written as Arrow `List(inner_type)`; BigQuery `REPEATED <inner>`.
- **MySQL TIME/TIME2** — written as `Time64(Microsecond)`.

**Unsupported-column errors** now collect all unmappable columns before returning (previously failed on the first one).

### `rivet run --json`

Prints the run aggregate summary as JSON to stdout after the run completes. Useful for CI pipelines and scripted post-processing. Compatible with `--summary-output` (both can be used together).

```bash
rivet run --config rivet.yaml --json | jq '.total_rows'
```

### Destination path templates

`path` (local) and `prefix` (S3/GCS) fields now support placeholders substituted at plan-build time:

| Placeholder | Value |
|---|---|
| `{date}` | UTC date as `YYYY-MM-DD` |
| `{export}` | Export name from config |
| `{table}` | Alias for `{export}` |

```yaml
destination:
  type: s3
  bucket: my-data
  prefix: exports/{date}/{export}/
```

### `rivet state reset-chunks --stuck-checkpoints`

Clears chunk checkpoint rows for every export in the config that still has `chunk_run.status = 'in_progress'` — a single command to recover from a crash or SIGKILL that left multiple exports stuck. Alias: `--failed`. Exports whose chunk run completed normally are skipped. Names present in state but removed from the YAML are skipped with a printed note.

```bash
rivet state reset-chunks --config rivet.yaml --stuck-checkpoints
rivet run --config rivet.yaml --resume
```

### `rivet init` — unbounded DECIMAL scaffolding

Plain `numeric` / `decimal` columns without `(p,s)` in the DDL now scaffold as `decimal(38,18)` (runs without manual editing), with a `# REVIEW:` marker on each affected line, a `# NOTE:` in the file header, and a `rivet: note` on stderr when writing `-o <file>`. Previously only a commented-out TODO line was emitted.

### RunJournal persistence

`RunJournal` is now persisted to the state database at the end of every export run (state DB migration **v7**: `run_journal` table). `store_journal`, `load_journal`, and `recent_journals` are available for auditing and future `rivet journal` CLI commands.

### Postgres NUMERIC catalog hints

For simple single-table `SELECT … FROM rel` queries, `rivet` now looks up `NUMERIC(p,s)` precision and scale from `information_schema` at export time. This resolves decimal types automatically without requiring hand-written `columns:` overrides in the common case.

### Changed

- **`rivet run --resume`** — when `--resume` is specified but no in-progress chunk checkpoint exists (e.g. after `reset-chunks` already cleared it), the run now starts fresh with a warning instead of exiting with an error. The previous workflow `reset-chunks && rivet run --resume` now works correctly.
- **`rivet init` unbounded DECIMAL** — see above; `decimal(38,18)` + `# REVIEW:` replaces the old commented-out `# TODO` line.

### Fixed

- **`memory_threshold_mb` defaults** — `balanced` profile now defaults to 4096 MB and `safe` to 2048 MB (was 0 = disabled). Prevents unbounded RSS growth on wide-row tables without any config change.
- **Retry safety guard** — transient-error retry fails fast when `dest.write()` already succeeded in a previous attempt. Prevents silent duplicate-row writes on retry.
- **PG state backend — migrate_pg** — migration runner now verifies the final schema version after migration and cleans up superseded version rows (parity with SQLite behaviour).
- **PG state backend — password in logs** — PostgreSQL URLs are redacted in all log and error messages. Passwords containing `@` are handled correctly via `rfind('@')`.
- **`chunk_count` validation** — `chunk_count: 0` is now rejected at plan-build time (would have caused a division by zero at run time). `chunk_count` combined with `chunk_dense: true` or `chunk_by_days` is also rejected (the options are mutually exclusive; previously `chunk_count` was silently ignored when either of the other two was set).
- **MySQL `--parallel-exports` connection exhaustion** — the `mysql` crate's default connection pool eagerly opens `min=10` connections per pool. With many exports running simultaneously each pool was created at startup, causing `Too many connections` when N×10 exceeded MySQL's `max_connections` (default 151). The pool is now configured with `min=1` so connections are opened lazily on demand.

### Performance

- **BufWriter 256 KB** — temp-file write buffer increased from 8 KB to 256 KB (32× fewer `write` syscalls per batch). −37% User CPU on a 1.5 M-row benchmark.
- **Inline cursor extraction** — `ExportSink` extracts the cursor value inline and frees all column buffers immediately after `on_batch` returns. Saves one full batch worth of RAM during post-run state writes.
- **`insert_chunk_tasks` batch transaction** — chunk task initialisation wraps all INSERTs in a single SQLite transaction (one WAL sync). Eliminates quadratic I/O on large chunk counts.
- **Quality unique tracking** — `HashSet<String>` → `HashSet<u64>` (xxh3-64). Eliminates one heap-allocated `String` per non-null row per tracked column in the hot path.

### Compatibility

- `RIVET_STATE_URL` is optional; SQLite remains the default.
- No YAML config changes required for any of the above features.
- `columns:` overrides are optional; Parquet output is unchanged without them.

---

## 0.3.5 (2026-04-30)

Polish release on top of 0.3.4: every multi-export mode (`--parallel-exports`
threads and `--parallel-export-processes` children) now renders the same live
cards UI, end-of-run summaries collapse to a single compact block instead of
seven lines per export, the chunked retry path no longer leaves stale progress
bars or zombie child processes behind, and a few rough edges around recovery
hints, `database is locked`, and transient OpenDAL errors are gone. No YAML
config changes; no on-disk artifact changes; `--single-export` and sequential
runs print exactly as before.

### `--parallel-exports` (threads) shares the cards UI

`--parallel-exports` previously rendered one `indicatif::MultiProgress` bar
per chunk worker, which interleaved with per-export `RunSummary` blocks and
left "ghost" 0/N bars behind on retries. 0.3.5 replaces that path with the
same `parent_ui::run_ui` renderer used by `--parallel-export-processes`:

- A single `mpsc::Sender<UiMessage>` is installed in `pipeline::ipc` for the
  duration of the run; chunked workers and `RunSummary::print` route their
  events (`Started`, `ProgressInit`, `Progress`, `Finished`) through that
  channel instead of writing directly to stderr.
- A dedicated UI thread drains the channel and redraws the card stack — one
  card per export — exactly as in `--parallel-export-processes`.
- Chunked progress bars under multi-export runs are `ProgressDrawTarget::hidden()`;
  `ChunkProgress` now implements `Drop` and calls `finish_and_clear()` on
  scope exit so that retries (a fresh `ChunkProgress` per attempt) don't
  accumulate orphaned bars in the renderer.

Sequential / single-export runs still draw a normal `indicatif` bar to
stderr — the cards path only activates when more than one export is in
flight.

### Compact end-of-run summaries

`RunSummary::print` used to emit a 7-line block per export (header, run_id,
status, mode, tuning, batch_size, metrics). Under multi-export mode that
turned a 15-export run into ~100 lines of scrollback even on success.
0.3.5:

- Sets a `MULTI_EXPORT_MODE` flag for the duration of any multi-export
  invocation; while it's set, `RunSummary::print` short-circuits to a new
  `render_compact()` path that produces a single status-icon + one-line
  summary per export.
- The aggregate `Run summary` block (`pipeline::aggregate::print`) strips
  per-export recovery hints (`run rivet state reset-chunks --export …`)
  from individual error messages and prints them once, consolidated, in a
  single `Recovery` section at the bottom.
- Long error messages are clamped to a fixed character budget; in
  particular, `parallel checkpoint worker errors` (which previously dumped
  every failed chunk's GCS URL into the summary) collapse to a single
  cause-line via `summary::compact_error` /
  `summary::summarize_parallel_chunk_errors`.

### Recovery hints always include `--config`

`rivet state reset-chunks --export <name>` and the `--resume` suggestions
emitted by `pipeline::chunked` previously omitted the `--config <path>`
flag, so copy-pasting them straight from the error message failed for
anyone whose YAML wasn't named `rivet.yaml`. Both error paths now go
through a shared `config_hint(config_path)` helper and emit fully-qualified
commands.

### `database is locked` on first-time `--parallel-export-processes`

Spawning N children against a brand-new `state.db` raced N copies of the
v1 schema migration on an exclusive lock; the loser printed
`migration v1 failed: database is locked` and exited. Fixed in two
places:

- `pipeline::run_exports_as_child_processes` now opens `StateStore` once
  in the parent before spawning children, so the migration runs exactly
  once and every child sees a fully-migrated DB.
- The ad-hoc connections opened by `state::checkpoint` for parallel chunk
  workers go through a new `open_connection` helper that applies
  `journal_mode=WAL` and `busy_timeout = 10_000` per connection. Brief
  contention on `chunk_task` / `export_metrics` now waits up to 10 s
  instead of surfacing `SQLITE_BUSY` immediately.

### No more zombie children in `htop`

`--parallel-export-processes` previously reaped children sequentially
(`for child in children { child.wait()?; }`), so on a 15-export config the
14 fastest children sat as defunct entries until the slowest one finished.
0.3.5 spawns one dedicated reaper thread per child (`rivet-reap-<name>`)
and joins them in any order; each child is `wait()`ed within milliseconds
of its actual exit. Visible improvement: the long stack of
`rivet --export …` rows in `htop` / Activity Monitor disappears as soon
as each card flips to its final metrics.

### OpenDAL transient retries (GCS + S3)

GCS occasionally returns `dispatch task is gone: runtime dropped the
dispatch task` and other `(temporary)` errors mid-stream; before 0.3.5 a
single hiccup tore down the chunk worker and forced a fresh connection +
re-fetch from Postgres. Two layers of fixes:

- `destination::gcs` / `destination::s3` now wrap the async `Operator`
  with `opendal::layers::RetryLayer::new().max_times(5).min_delay(200ms)
  .max_delay(10s).jitter()` before converting to `blocking::Operator`.
  Transient HTTP failures retry inside OpenDAL with no chunk-worker
  involvement.
- `pipeline::retry::classify_error` recognises `(temporary)` and
  `dispatch task is gone` / `runtime dropped the dispatch task` as
  transient-but-non-reconnecting, with a 500 ms extra delay. The chunk
  worker keeps its existing connection and retries the chunk write
  instead of falling back to the outer reconnect loop.

### Documentation

- `docs/gifs/chunked-progress.gif` and `docs/gifs/parallel-cards.gif` were
  re-recorded against 0.3.5 binaries so the demos reflect the new compact
  summary, the cleaned-up parallel cards (no interleaving, no ghost
  bars), and the consolidated recovery section.

### Compatibility

- No YAML config changes. `rivet check` / `rivet plan` behaviour
  unchanged.
- Sequential / `--single-export` runs print exactly as before.
- `--parallel-exports` and `--parallel-export-processes` produce strictly
  cleaner output; scripts that scraped the per-export `RunSummary` block
  from stderr should already be on `RIVET_IPC_EVENTS=1` + `Finished`
  events on stdout (stable since 0.3.4).

## 0.3.4 (2026-04-27)

Multi-process parallel exports get a real UI: `--parallel-export-processes`
now renders one card per export with a live progress bar, ETA, and rows, and
in-place final metrics — instead of four child processes' output stomping on
each other in the terminal. No YAML changes, no behavioural change for
single-process or `--parallel-exports` runs.

![Parallel cards UI](docs/gifs/parallel-cards.gif)

### `rivet run --parallel-export-processes` — parent-side cards UI

`--parallel-exports` runs every export as a thread in one process; it has
worked since 0.2 but shares a global allocator, a single connection pool, and
a single set of progress bars across exports. `--parallel-export-processes`
spawns one child `rivet` per export instead — full memory and connection
isolation — but until 0.3.4 each child wrote its own progress bar to stderr,
which interleaved badly with the others.

0.3.4 introduces a small NDJSON IPC protocol between parent and children and
a hand-rolled ANSI renderer in the parent that owns the screen for the
duration of the run:

- Children spawned with `RIVET_IPC_EVENTS=1` emit four event kinds on stdout
  — `Started`, `ProgressInit`, `Progress`, `Finished` — with `serde_json`,
  one event per line. Children also suppress their own progress bars
  (`ProgressDrawTarget::hidden()`) and skip the per-export stderr
  `RunSummary` block — that data is carried by `Finished` instead, so the
  parent has all the metrics without scraping logs.
- The parent reader thread per child decodes events with `serde_json` and
  forwards them through an `mpsc::Sender<UiMessage>`. A single dedicated UI
  thread drains the channel, redraws the card stack on every event, and on a
  200 ms idle timer so elapsed-time / ETA fields keep ticking even when no
  IPC arrives.
- Each card is seven lines: a header (`── orders ─────…`), five fixed-width
  meta lines (`run_id`, `status`, `mode`, `tuning`, `batch_size`), and a
  bottom line that is either a live progress bar with ETA or, once
  `Finished` arrives, the export's final metrics in place. Cards stay in
  scrollback after the run; below them the existing aggregate `Run summary`
  block prints exactly once.
- If a child's stdout closes without a `Finished` event (crash, OOM,
  `SIGKILL`), the parent marks the card `failed` with a synthetic warning
  line. The parent never silently loses an export.

The renderer is implemented as raw ANSI escape sequences (`\x1b[nA`, `\r`,
`\x1b[2K`) instead of `indicatif::MultiProgress` — same observable output in
real terminals, but deterministic across `vhs` / `ttyd` recordings and CI
pipes (where the cursor controls become harmless no-ops). See
`src/pipeline/parent_ui.rs` for the rationale and the corner cases handled.

### `Destination` is now `Send + Sync`

To make the cards UI possible, `Destination` instances are now shared across
threads via `Arc<dyn Destination + Send + Sync>` instead of being moved per
job. Every built-in destination (`local`, `s3`, `gcs`, `bigquery`) was
already thread-safe; the bound is added explicitly so out-of-tree
destinations get a clear compile-time error rather than a runtime
data-race.

### Documentation

- New `docs/gifs/parallel-cards.gif` (1280 × 780, ~30 s) recorded against a
  4-export Postgres fixture (`orders`, `users`, `events`, `sessions`,
  ~210 k rows total) seeded by `docs/gifs/render.sh`. The tape is
  reproducible from a clean Docker Compose stack — `./docs/gifs/render.sh
  parallel-cards`.
- `docs/reference/cli.md`: new `--parallel-export-processes — one card per
  export` subsection embedding `parallel-cards.gif`, with the seven-line
  card layout and a short note on the IPC protocol.
- `docs/gifs/README.md` lists the new scenario alongside the existing ten.

### Internals

- `src/pipeline/ipc.rs` — `ChildEvent` enum + `emit()` + `ipc_events_enabled()`.
- `src/pipeline/parent_ui.rs` — `UiMessage`, `Renderer`, `CardState`,
  cursor-positioned ANSI redraw loop. ~12 unit tests cover header padding,
  meta-line vertical alignment, progress-bar endpoints, number / duration
  formatting, and synthetic-failure rendering.
- `src/pipeline/progress.rs` — `ChunkProgressHandle` cloneable handle that
  emits `Progress` IPC events on each `inc()` and falls back to the visible
  `indicatif` bar when IPC is off.
- `src/pipeline/summary.rs` — `RunSummary::new` emits `Started`,
  `RunSummary::print` emits `Finished` and short-circuits the stderr block
  under `RIVET_IPC_EVENTS=1`.

### Security

- **RUSTSEC-2026-0104** — bumped transitive `rustls-webpki` 0.103.12 → 0.103.13.
  The advisory describes a reachable panic when parsing CRL extensions with a
  syntactically valid empty `BIT STRING` in the `onlySomeReasons` element of
  an `IssuingDistributionPoint`, before the CRL signature is verified. Rivet
  itself does not consume CRLs directly, but the bump closes the audit
  warning end-to-end (advisory pulled in via `rustls 0.23.38` →
  `hyper-rustls`/`tokio-rustls`/`rustls-platform-verifier` → `reqwest` /
  `opendal`). `cargo audit` is now clean across all 480 dependencies.

### Compatibility

- No YAML config changes. `rivet check` / `rivet plan` behaviour unchanged.
- `--parallel-exports` (single-process, multi-thread) prints exactly as
  before.
- `--parallel-export-processes` previously had garbled per-export output
  with no aggregate summary; the new behaviour is strictly better but the
  on-screen layout is different. Scripts that scraped the per-child
  `RunSummary` block from stderr should switch to `RIVET_IPC_EVENTS=1` +
  `Finished` events on stdout — the format is documented in
  `src/pipeline/ipc.rs` and stable for 0.3.x.

## 0.3.3 (2026-04-19)

QA test matrix + panic-safety follow-up. No YAML config deserializes
differently; no export artifact format changes. Three latent panics in the
pipeline are now graceful; four config combinations that used to be accepted
silently are now rejected at validation time (see **Fail-fast validation**
below — not a YAML-format break, but configs that relied on silent
acceptance will surface the error they were always hiding).

### Panic-safety fixes

Surfaced by the new fuzz suites (`tests/planner_fuzz.rs`,
`tests/format_fuzz.rs`) and by `src/pipeline/chunked/math.rs::mod tests`:

- **`generate_chunks` near `i64::MAX`** — `start + chunk_size - 1` overflowed
  and panicked when the cursor column reached the BIGINT upper bound. Fixed
  with saturating arithmetic and an explicit exit when `end == i64::MAX`.
  (`src/pipeline/chunked/math.rs`)
- **`build_time_window_query` on `days_window: u32::MAX`** — naive
  `now - Duration::days(u32::MAX as i64)` walks back ~12 million years and
  falls outside chrono's representable range. Replaced with
  `Duration::try_days().and_then(checked_sub_signed)` saturating at
  `DateTime::MIN_UTC`. (`src/plan/mod.rs`)
- **CSV writer on pathological `Date32` values** — `NaiveDate + Duration::days(i64)`
  panicked for values near `i32::MAX` (roughly 1.5 million years from
  1970-01-01). Uses `checked_add_signed` with a fallback to an empty cell
  (matching the null-cell convention already used by this writer).
  (`src/format/csv.rs`)

None of these reachable from sensible input, but each was a panic surface the
fuzz suites turned into deterministic regression tests.

### Fail-fast validation

`Config::validate` now rejects four combinations that previously parsed
successfully but produced broken runs at execution time:

- empty `exports: []` — used to be a silent no-op that looked like success
  in schedulers;
- duplicate export names — would silently share `export_state` /
  `file_manifest` / `chunk_run` rows (all keyed by name);
- `mode: chunked` with `parallel: 0` — zero workers never claim a task, so
  the run hung forever;
- `mode: chunked` with `chunk_size: 0` — before the saturating fix in
  `generate_chunks`, this was an infinite loop at the planner level.

All four come with actionable error messages that name the offending field.
Configs that hit any of these never produced usable output — this is a
fail-fast, not a breaking change.

### QA test matrix — `rivet_qa_backlog_v2.md` + `docs/reference/testing.md`

The full coverage of QA backlog tasks is now shipped as automated tests. See
the new `docs/reference/testing.md` for the file-level map and
`rivet_qa_backlog_v2.md`'s footer for the task-level status.

- **1096 offline tests** across 21 integration files + inline unit modules.
  Runs on every PR via `cargo test`.
- **46 live tests** across 10 `tests/live_*.rs` binaries exercising the full
  `rivet` binary against the docker-compose stack (Postgres 16 + MySQL 8 +
  MinIO + fake-gcs + Toxiproxy). Gated by `#[ignore]`; activated with
  `cargo test -- --ignored`.
- Shared harness in `tests/common/mod.rs`:
  - `require_alive(service)` — fast TCP reachability probe with actionable
    failure messages;
  - RAII `PgTable` / `MysqlTable` guards for per-test unique tables;
  - `unique_name(prefix)` — PID + atomic counter naming for race-free
    parallel runs;
  - Minimal Toxiproxy admin client over raw `TcpStream` (no blocking
    tokio runtime);
  - Cross-process `flock(2)` lock on `$TMPDIR/rivet_qa_toxiproxy.lock` so
    cargo's parallel integration binaries do not race on the shared admin
    API;
  - `ensure_minio_bucket` / `ensure_gcs_bucket` idempotent bucket creation.

### Test-only fault-injection hook

New env-var-driven hook in `src/test_hook.rs` reads `RIVET_TEST_PANIC_AT`
once at startup and panics at one of four named pipeline boundaries if the
value matches: `after_source_read`, `after_file_write`,
`after_manifest_update`, `after_cursor_commit`. The `tests/live_crash_recovery.rs`
suite injects each one, asserts the observable post-crash state, then
re-runs without the injection and asserts recovery produces the expected
final state. Zero overhead when unset (one relaxed atomic load per call).

See `dev/CRASH_MATRIX.md` for the boundary table and the invariant windows
(ADR-0001 I2–I4).

### Slack / webhook internals

- Extracted `build_slack_payload` and `should_notify` from `maybe_send` as
  `pub(crate)` pure functions so contract tests can pin payload shape
  without spinning up `reqwest`'s blocking client. Field marker test
  guarantees notifications include only `export_name` / `status` /
  `total_rows` / `duration_ms` / `error_message` — not `tuning_profile`,
  `format`, `mode`, or `compression` (guards against accidental over-
  exposure in webhooks).
- Mock HTTP receiver based on `std::net::TcpListener` exercises the
  webhook path end-to-end for `200 OK`, `429`, `500`, and "no trigger
  matched — no connection made" cases.

### CI

`.github/workflows/ci.yml`:

- `e2e` job now also starts `toxiproxy` (was only postgres/mysql/minio/fake-gcs);
- new step runs `cargo test --release -- --ignored` after the bash E2E
  script, wiring the entire live suite into branch-gate CI.

### Docs

- `docs/reference/testing.md` — new offline + live matrix reference.
- `docs/README.md` — reference link to the above.
- `dev/CRASH_MATRIX.md` — automated coverage section with the env-var hook
  boundary table.
- `rivet_roadmap.md` — Epic G (Toxiproxy) and Epic H (fault-injection hook)
  flipped from ⏳ to ✅ with evidence.

### Internal refactoring

- Collapsed `tests/v2_golden.rs` (5-in-1 mixed file) into focused domain
  files: `tests/time_window.rs`, `tests/resource_smoke.rs`, with
  validate / chunk parsing tests folded into
  `tests/validate_regression.rs` and `src/config/tests.rs`.

---

## 0.3.2 (2026-04-18)

SecOps audit follow-up plus an expanded database-version compatibility matrix.
No YAML config is deserialized differently; no export artifact changes format.
The one behavior change that could bite an existing config is the strict
environment-variable resolver — see **Breaking changes** below.

### Transport security — optional TLS for source connections

New `source.tls` block (defaults to plaintext so existing configs are
unchanged). Four modes, matching libpq semantics where it makes sense:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tls:
    mode: verify-full         # disable | require | verify-ca | verify-full
    ca_file: /etc/ssl/certs/rds-ca-2019-root.pem
    accept_invalid_certs: false
    accept_invalid_hostnames: false
```

- Postgres via `postgres-native-tls` (new dep).
- MySQL via `OptsBuilder::ssl_opts` (existing `mysql` crate).
- When `tls` is absent Rivet emits one startup warn-level log — `credentials
  and result rows cross the network in plaintext` — so operators aren't
  silently on NoTls in prod.
- On Linux, `native-tls` statically links vendored OpenSSL, so
  `cargo install rivet-cli` works on a bare Ubuntu / Alpine / CI runner
  without `libssl-dev`. macOS uses `SecureTransport` — no OpenSSL.

### SecOps audit fixes

Addresses the findings from the credential-leak review (see commit 08c16f2).

- **Cursor values are no longer string-interpolated into SQL.** MySQL binds
  the value via `?`; Postgres embeds it via a dedicated `E'…'` literal
  helper (`escape_pg_literal`) that escapes both `'` and `\` regardless of
  `standard_conforming_strings`. Injection-attempt tests (`'; DROP TABLE …`,
  `O'Brien`, backslash payloads) now live in `src/source/query.rs`.
- **AWS_PROFILE env mutation is serialized.** Parallel exports with
  differing `aws_profile` values no longer race — a `static Mutex` plus an
  RAII guard scopes the override and restores the previous value on drop.
- **Secrets wrapped in `zeroize::Zeroizing<String>`:** AWS access/secret
  keys, GCS `client_secret` / `refresh_token` / OAuth POST body, and the
  resolved DB password. Heap buffers are zeroed on drop.
- **GCS 4xx response body no longer propagates into anyhow errors.**
  Google's `/token` endpoint occasionally echoes back `client_id` /
  `client_secret` on failure; this was landing in `summary.error_message`
  (SQLite) and Slack notifications.
- **`executing query: …` lowered from `info` to `debug`** in both source
  drivers, so `RUST_LOG=info` (the common CI / docs setting) no longer
  surfaces SQL text that can contain `${VAR}`-expanded secrets.

### `rivet init` — credentials off the command line

New `--source-env <ENV_VAR>` and `--source-file <PATH>` accept the DB URL
from an env var or a one-line file, keeping the credential out of shell
history, `ps aux`, and `/proc/<pid>/cmdline`. Mutually exclusive with the
existing `--source <URL>`, which stays supported for local dev.

```
rivet init --source-env DATABASE_URL --table orders -o orders.yaml
rivet init --source-file ~/.rivet/source.url --schema public -o pg_all.yaml
```

### Database version compatibility matrix

The full end-to-end suite now runs against every supported server version:

| Engine     | Versions                   |
|------------|----------------------------|
| PostgreSQL | 12, 13, 14, 15, 16         |
| MySQL      | 5.7, 8.0                   |

Legacy servers opt in via `docker compose --profile legacy up -d`. Ports
5412 / 5413 / 5414 / 5415 for PG 12–15; 3357 for MySQL 5.7 (amd64 platform
on Apple Silicon). Run the full matrix with:

```
docker compose up -d postgres mysql minio fake-gcs
docker compose --profile legacy up -d \
    postgres-12 postgres-13 postgres-14 postgres-15 mysql-57
cargo build --release --bin rivet --bin seed
bash dev/legacy/run_full_matrix.sh
```

Seven targets × 83 e2e assertions = 581 per-version checks, all green.
`dev/e2e/*.yaml` and `dev/fixtures/test_*.yaml` now read their URL from
`RIVET_PG_URL` / `RIVET_MYSQL_URL` (with localhost defaults) so the same
suite re-targets cleanly without YAML edits.

MySQL 5.7 compat notes (full details in `docs/reference/compatibility.md`):

- `dev/mysql/init_57.sql` ships a view-free init because MySQL 5.7 lacks
  window functions (`ROW_NUMBER() OVER (…)`); `seed` auto-detects 5.x
  and skips the corresponding `CREATE VIEW`.
- Probe uses bash `/dev/tcp/127.0.0.1/<port>` rather than `mysqladmin` —
  Homebrew's `mysql-client@9` dropped the `mysql_native_password` plugin
  library, so modern macOS clients can't connect to 5.7 servers, but the
  Rust `mysql` crate (which Rivet depends on) still can.

### Breaking changes

- **Unset `${VAR}` in config is now a hard error.** Previously
  `resolve_vars` silently substituted an empty string, so
  `postgres://u:${DB_PASS}@host/db` with `DB_PASS` unexported would
  become `postgres://u:@host/db` — a silent auth-bypass footgun. The
  loader now refuses with a clear message. An explicit empty value
  (`export DB_PASS=""`) is still accepted. Migrate by either setting the
  variable or removing the reference; add an explicit empty assignment
  only when you really mean "no password".

### Docs

- New `docs/reference/compatibility.md` — version-support policy, test
  matrix, engine-specific notes (window functions, arm64 emulation, auth
  plugins). Linked from `docs/README.md` and `docs/getting-started.md`.

### Verification

- `cargo test --lib` — 411 passed, 0 failed.
- `cargo test --tests` (serial, live DBs) — 970 passed, 0 failed.
- `dev/legacy/run_full_matrix.sh` (7 targets × 83) — 581 passed, 0 failed.
- `dev/legacy/run_legacy.sh` (compat smoke) — 44 passed, 0 failed.
- Demo pipelines — 12 PG + 8 MySQL exports succeeded.

**Total: 2026 assertions, zero failures.**

---

## 0.3.1 (2026-04-18)

Security patch release — closes five advisories raised against `v0.3.0` by
`rustsec/audit-check@v2` (CI security-audit job).

### Fixed by upgrade

- **RUSTSEC-2026-0098** — `rustls-webpki` name constraints for URI names were
  incorrectly accepted. `rustls-webpki` 0.103.10 → 0.103.12.
- **RUSTSEC-2026-0099** — `rustls-webpki` name constraints accepted for
  wildcard DNS names (similar to CVE-2025-61727). Same bump.
- **RUSTSEC-2025-0119** — `number_prefix` unmaintained. Fixed by bumping
  `indicatif` 0.17 → 0.18, which migrated to the maintained `unit-prefix`.
  `number_prefix` is no longer in `Cargo.lock`.
- **RUSTSEC-2026-0097** (×3) — `rand` unsound under custom `log::Log` that
  calls `rand::rng()` in the reseed path. Fixed by the transitive bump
  `rand` 0.8.5 / 0.9.2 / 0.10.0 → 0.8.6 / 0.9.4 / 0.10.1.

### Policy

`.cargo/audit.toml` rewritten with explicit reachability analysis for each
remaining entry. Policy:

> An advisory stays here **only** if (a) there is no upstream fix **and**
> (b) the vulnerable code path is not reachable from Rivet's runtime.

Ignore list is now down to two entries — `RUSTSEC-2023-0071` (rsa timing
sidechannel; GCS JWT signing only; not practically exploitable for a batch
CLI) and `RUSTSEC-2024-0436` (`paste` unmaintained; `proc-macro = true`
crate, zero runtime code). Each is documented in-file with the exact
preconditions that would trigger the advisory and a pointer to the upstream
fix to track.

### Other changes

- Bulk transitive `cargo update` (rustls 0.23.37 → 0.23.38, tokio 1.51.0 →
  1.52.1, wasm-bindgen family, webpki-roots, uuid, lru, pkg-config, misc.).
- No direct-dependency API changes — all crates in `Cargo.toml` are already
  at the latest stable major/minor.

### Verification

- `cargo build --release --bin rivet --bin seed` — clean.
- `cargo test --lib` — 411 passed, 0 failed.
- `cargo audit` — 479 crates scanned, 0 advisories after ignore.

---

## 0.3.0 (2026-04-18)

Source-aware planning release — Epics A–I plus credential-hardening follow-ups.

### New features

- **Source-aware extraction prioritization** (Epic A, [ADR-0006](docs/adr/0006-source-aware-prioritization.md)) — `rivet plan` now emits per-export `priority_score` / `priority_class` / `cost_class` / `risk_class` / `recommended_wave` plus explainable `reasons[]`. Multi-export plans embed a full campaign view with ordered exports, grouped waves, and `source_group` collision warnings. Pretty and JSON output.
- **Metadata-driven discovery** (Epic B) — `rivet init --discover -o out.json` emits a machine-readable artifact with ranked cursor + chunk candidates, nullability, total bytes, and automatic `coalesce` fallback hints when the best cursor is NULL-able.
- **Composite cursor** (Epic D, [ADR-0007](docs/adr/0007-cursor-policy-contracts.md)) — `incremental_cursor_mode: coalesce` progresses on `COALESCE(primary, fallback)` for tables with a nullable `updated_at`. Synthetic `_rivet_coalesced_cursor` column is stripped before Parquet/CSV write (CC5). Single-level SQL with outer `ORDER BY` so the last batch carries the max coalesced value (CC6).
- **Partition / window reconciliation** (Epic F, [ADR-0009](docs/adr/0009-reconcile-and-repair-contracts.md)) — new `rivet reconcile -c <cfg> -e <export>` re-counts every chunk partition on the source and emits a structured `ReconcileReport`. Requires `chunk_checkpoint: true` (v1).
- **Committed / verified progression** (Epic G, [ADR-0008](docs/adr/0008-export-progression.md)) — new schema v4 migration for `export_progression` table plus `rivet state progression` command that surfaces both boundaries per export.
- **Targeted repair** (Epic H) — `rivet repair -c <cfg> -e <export> [--report rec.json] [--execute]` derives a `RepairPlan` from a `ReconcileReport` and re-runs only the flagged chunk ranges. New files land alongside originals; committed boundary is not re-stamped (RR4).
- **Historical recommendation refinement** (Epic I) — `rivet plan` folds the last ~20 rows of `export_metrics` into scoring with bounded contribution (≤ ~15 points combined). Adds `high_retry_rate_history`, `recent_failure_history`, `slow_history` reason kinds.

### Security / hardening

- **PA9 — Artifact Credential Redaction** ([ADR-0005](docs/adr/0005-plan-apply-contracts.md#pa9--artifact-credential-redaction-acr)) — `PlanArtifact::new` silently strips plaintext `password:` and rewrites `scheme://user:pass@…` → `scheme://REDACTED@…` before serialization. Operators see a WARN log so they know to migrate to `password_env:` / `url_env:`.
- Composite-cursor coalesce mode: `@` inside path/query strings is no longer confused with userinfo (dedicated path-aware parser).

### Configuration additions

- `exports[].source_group: <string>` — logical shared-source label driving campaign-level warnings (Epic A).
- `exports[].reconcile_required: bool` — advisory flag feeding into prioritization risk class (Epic C).
- `exports[].cursor_fallback_column`, `exports[].incremental_cursor_mode` — composite cursor policy (Epic D).

### CLI additions

- `rivet reconcile`
- `rivet repair`
- `rivet state progression`
- `rivet init --discover`

### Demo assets

- `demo/setup_demo_tables.sql` + `demo/setup_demo_tables_mysql.sql` — reproducible 7-table fixture with varied scale (500 → 800k rows), nullable-cursor coalesce cases, and shared-source collisions.
- `demo/demo_pipeline.yaml` / `demo_pipeline_mysql.yaml` — 12/8-export showcase configs with `source_group` declarations.
- `docs/pilot/demo-quickstart.md` — 10-minute scripted pilot demo (discovery → plan → apply → reconcile → repair → verified).

### Documentation

- New ADRs: 0006 (prioritization), 0007 (cursor policy), 0008 (progression), 0009 (reconcile & repair).
- New pilot guide: `docs/pilot/pilot-walkthrough.md` end-to-end on user data; `docs/pilot/demo-quickstart.md` for the pre-seeded demo.
- New reference: `docs/modes/incremental-coalesce.md`.
- Moved planning working docs into `docs/planning/`.
- PRODUCT.md: v5 (plan/apply) + v6 (Epics A–I) + v6.1 (SecOps) sections.

### Bug fixes

- `ExportSink` was stripping the synthetic coalesce cursor column before capturing `last_batch`/`schema`, so incremental cursors never advanced in `coalesce` mode. Fixed by keeping the raw (with-synthetic) batch + schema for cursor extraction and building a stripped `dest_batch`/`dest_schema` on the fly for the file writer.
- `on_schema` with an empty schema (zero-row runs) no longer errors when attempting to strip a missing synthetic column.

### Tests

- `cargo test --lib`: 411 passed.
- `cargo test --tests`: 434 passed.
- Integration suites (chunked_sparse_ids, format_golden, invariants, journal_invariants, recovery, retry_integration, schema_evolution, v2_golden, validate_regression): 125 passed.
- Zero failures, zero warnings.

---

## 0.2.0-beta.6 (2026-04-11)

### Fixes

- **Docker image:** install `build-essential` in the builder stage so `tikv-jemalloc-sys` can run `make` (slim images previously failed with `ENOENT` during `cargo build --release --locked`).
- **`.dockerignore`** — ignore `target/`, `.git/`, `.github/` so `docker build` / `buildx` context stays small.

### Documentation

- **`packaging/homebrew/README.md`** — troubleshooting for tap push `Authentication failed` / invalid PAT.

---

## 0.2.0-beta.4 (2026-04-12)

### New features

**`rivet init` — YAML scaffolding from a live database**

- **`--table`** — introspect one table (`schema.table` supported on PostgreSQL) and emit a single export with suggested `mode` (`full` / `incremental` / `chunked`) from metadata and row estimates.
- Omit **`--table`** — emit **one YAML** with an export per **base table and view** in a PostgreSQL schema (default `--schema public`) or in a MySQL database (from the URL path or `--schema` when the URL has no database).
- Output uses **`url_env: DATABASE_URL`** by default so passwords are not written into the file.

**Chunked export progress bar**

- Terminal progress (chunks completed, cumulative rows, ETA) during **`mode: chunked`** runs when stderr is a TTY (`indicatif`).

### Documentation

- [docs/reference/init.md](docs/reference/init.md) — full `rivet init` guide (Docker Compose, `dev/scripts/regenerate_docker_init_configs.sh`)
- [docs/reference/cli.md](docs/reference/cli.md) — `rivet init` command table
- [docs/getting-started.md](docs/getting-started.md) — optional “Scaffold with rivet init” step
- [README.md](README.md) — CLI quick reference; product vs `rivet-cli` crate name; chunked progress + `dev/scenarios/chunked_postgres_bench.yaml` example
- [USER_GUIDE.md](USER_GUIDE.md) — `rivet init` optional section
- [docs/modes/chunked.md](docs/modes/chunked.md) — progress bar (independent of `RUST_LOG`), bench config examples
- [docs/README.md](docs/README.md) — `rivet init` and chunked progress pointers
- E2E: `dev/e2e/run_e2e.sh` section 16 — `rivet init` against docker-compose Postgres/MySQL

### Fixes

**PostgreSQL `query_scalar` and date-based chunking**

`MIN`/`MAX` on `timestamp` / `timestamptz` / `date` columns were not decoded in `PostgresSource::query_scalar` (only numeric and plain text types were). That returned a false empty result and broke **`chunk_by_days`** (and any path using time-column scalars). Chrono types are now formatted to strings that `parse_date_flexible` accepts.

### Other

- `Cargo.toml` `[package] exclude` moved from invalid `[[bin]]` key to `[package]`
- `dev/scripts/regenerate_docker_init_configs.sh` — populates gitignored `dev/init_generated/` locally from docker-compose schemas
- `.gitignore` — `dev/e2e/.init_e2e_scratch/`

---

## 0.2.0-beta.3 (2026-04-12)

### New features

**Connection limit warning in `rivet check`**

`rivet check` now warns when `parallel` meets or exceeds the database's `max_connections` limit. The warning includes exact numbers and a safe recommendation (headroom of 3 reserved connections for monitoring and admin traffic).

```
WARNING: parallel=20 meets or exceeds DB max_connections=20 —
workers will compete for connections and some may fail.
Reduce parallel to at most 17.
```

If `max_connections` cannot be fetched (restricted user), rivet shows an informative "check skipped" message rather than silently passing.

Works for PostgreSQL (`current_setting('max_connections')`) and MySQL (`@@max_connections`).

**Date-native chunking (`chunk_by_days`)**

New `chunk_by_days` field on chunked exports. Partitions a table into calendar windows on a DATE or TIMESTAMP column — no unix-epoch arithmetic, correct open-end semantics for timestamps.

```yaml
exports:
  - name: orders_by_year
    query: "SELECT id, user_id, product, price, ordered_at FROM orders"
    mode: chunked
    chunk_column: ordered_at
    chunk_by_days: 365
    parallel: 4
    format: parquet
    destination:
      type: local
      path: ./output
```

Generated SQL per chunk:
```sql
WHERE ordered_at >= '2023-01-01' AND ordered_at < '2024-01-01'
```

- Works with `parallel: N` for concurrent date-window workers
- Compatible with `chunk_checkpoint` / `--resume`
- `rivet check` reports strategy as `date-chunked(ordered_at, 365d)`
- `chunk_dense: true` cannot be combined with `chunk_by_days` (rejected at config validation)
- Sparse-range check is skipped for date mode (calendar gaps ≠ numeric ID sparsity)

### Documentation

- `docs/modes/chunked.md` — new "Date-based chunking" section with SQL example and when-to-use guidance
- `docs/reference/config.md` — `chunk_by_days` field documented
- `USER_GUIDE.md` — date chunking section added
- `examples/pg_date_chunked_local.yaml` — two export examples (orders by year, events by month with parallel + checkpoint)

### Tests

- 11 new unit tests: `check_connection_limit` (7) + `parse_date_flexible` and date chunk query building (4+)
- 4 config validation tests for `chunk_by_days` edge cases
- E2E Section 14: connection limit warnings (PG + MySQL, safe and exceeded cases)
- E2E Section 15: date-chunked run, preflight strategy, `chunk_by_days + chunk_dense` rejection (PG + MySQL)

---

## 0.2.0-beta.2 (2026-03-28)

### New features

- **Homebrew tap** — `brew install panchenkoai/rivet/rivet-cli`
- **Docker image** — `ghcr.io/panchenkoai/rivet`
- **`cargo install rivet-cli`** — published to crates.io (crate renamed from `rivet` which was already taken)

### Fixes

- Homebrew tap workflow: moved `HOMEBREW_TAP_GITHUB_TOKEN` to job-level env to pass workflow validation
- `fastrand` updated to 2.4.1 (2.4.0 was yanked)

### Documentation

- README: added `cargo install` section

---

## 0.2.0-beta.1 (2026-04-05)

### Architecture

- **Split `pipeline.rs` (2447 lines) into `pipeline/` module** with 7 focused submodules:
  `chunked`, `cli`, `mod` (orchestration), `retry`, `single`, `sink`, `validate`.
- **Split `config.rs` (1708 lines) into `config/` module** with 4 submodules:
  `models`, `resolve`, `tests`, `mod` (validation & loading).
- **Split `preflight.rs` (1425 lines) into `preflight/` module** with 5 submodules:
  `analysis`, `doctor`, `mod` (orchestration), `mysql`, `postgres`.
- All public API paths unchanged — external callers unaffected.

### Reliability

- **Export failures now propagate to CLI exit code**: `run_export_job` returns
  `Result<()>` and failures are collected; `rivet run` exits with non-zero when
  any export fails (critical for CI, cron, and orchestrators).
- **SQLite migration errors are fatal**: `migrate()` returns `Result` and
  `StateStore::open()` fails if any migration step errors, preventing silent
  partial schema states.
- **Typed error classification**: `classify_error` now checks Postgres `SqlState`
  codes and MySQL numeric error codes before falling back to string matching,
  giving more precise transient-vs-permanent classification for retries.
- **Replaced production `unwrap()` calls** with `expect()` and descriptive messages
  across `pipeline/`, `config/`, `state.rs`, `format/csv.rs`, and `source/`.
- **Versioned SQLite schema migrations**: `schema_version` table tracks applied
  migrations; new databases start at v3, legacy databases are detected and upgraded
  automatically.  Future schema changes only require adding a new migration entry.
- **Graceful mutex poison handling**: parallel chunked workers use
  `unwrap_or_else(|e| e.into_inner())` instead of `expect()`, preventing
  cascading panics if a worker thread panics.

### Code quality

- **Zero clippy warnings**: resolved all `collapsible_if`, `too_many_arguments`,
  `derivable_impl`, `manual_clamp`, `if_let_some_result`, `manual_range_contains`,
  `write_literal`, `is_multiple_of`, unused-import, and needless-borrow lints.
- Added `// SAFETY:` documentation on the sole `unsafe` block (`resource.rs` macOS RSS).
- Added crate-level `//!` documentation to `lib.rs`.
- **Tuning `profile_name()` now returns the configured profile** rather than
  inferring from numeric fields, ensuring metrics and logs match the YAML config.

### Memory optimization

- **Streaming cloud uploads**: S3, GCS, and stdout destinations now use
  `std::io::copy` instead of loading entire temporary files into RAM. Memory
  footprint during upload is O(buffer) instead of O(file_size).
- **Early `drop(rows)` in Postgres source**: raw `Vec<Row>` is freed
  immediately after conversion to Arrow `RecordBatch`, reducing transient
  memory overlap.
- **jemalloc** (`tikv-jemallocator`) added as an optional default-on allocator.
  jemalloc aggressively returns freed pages to the OS, reducing peak RSS by
  ~30–40% at smaller batch sizes compared to the system allocator.

### Config validation

- **Misplaced tuning field detection**: if `batch_size`, `profile`,
  `throttle_ms`, or other tuning fields are placed directly under `source:`
  or in an `exports[]` entry instead of inside `tuning:`, Rivet now rejects
  the config with a clear error and a fix suggestion. Previously, these
  fields were silently ignored by serde, causing unexpected defaults.

### Testing

- **617 tests** (537 unit + 80 integration), up from 274.
- New test coverage for: cursor extraction (all Arrow types), strip internal
  column, quality tracking, validate_output (corrupt/empty/missing files),
  CSV golden tests (Binary, Float32, Int16, Boolean+nulls, multi-batch),
  Parquet nullable + multi-batch roundtrip, resolve_vars edge cases,
  parse_file_size regressions, notify trigger matching, quality multi-batch
  aggregation, parse_params, format_bytes boundary values.

### Dependencies

- **Replaced deprecated `serde_yaml`** with `serde_yml` 0.0.12.
- Updated 52 transitive dependencies (tokio 1.51, hyper 1.9, postgres 0.19.13,
  libc 0.2.184, and others).

### Documentation

- **USER_GUIDE.md**: added jemalloc section, memory optimization tips, streaming
  upload notes, misplaced tuning field detection, troubleshooting section,
  documented `--export` and `--last` flags for `metrics` and `state files`.
- **README.md**: added stdout destination to config reference.

### Packaging

- Added `license = "MIT"`, `repository`, `rust-version = "1.94"` to `Cargo.toml`.
- Added `LICENSE` (MIT) file.
- Added `rust-toolchain.toml` pinning toolchain to 1.94 with rustfmt + clippy.
- Added `exclude` list to `Cargo.toml` for clean `cargo publish` (excludes `dev/`,
  `tests/`, `.github/`, `USER_TEST_PLAN.md`).

### CI

- `.github/workflows/ci.yml` with five jobs: `rustfmt`, `clippy -D warnings`,
  `cargo test`, `cargo build --release`, and **`cargo audit`** (security).
- All jobs pinned to Rust **1.94** (matches `rust-version` and `rust-toolchain.toml`).

## 0.1.0

Initial release.
