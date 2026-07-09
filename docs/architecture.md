# Architecture

How Rivet extracts data end-to-end: the pipeline, the traits that make it
pluggable, the memory model, and the source tree layout.

This is a reference document. If you only want to configure or run an
export, start with [getting-started.md](getting-started.md).

---

## Data flow

Every export, regardless of mode, follows the same streaming shape:

```
Source (PostgreSQL / MySQL / SQL Server / MongoDB)
  │
  ├─ begin_query / DECLARE CURSOR
  │
  ├─ FETCH batch_size rows ─► Arrow RecordBatch ─► FormatWriter ─► temp file
  │       │                        │                    │
  │       │ sleep(throttle_ms)     │ (dropped after     │ flush per batch
  │       │                        │  hand-off)         │
  │       │                        │                    │
  ├─ FETCH next batch ──────► Arrow RecordBatch ─► FormatWriter ─► temp file
  │       ...                      ...                  ...
  │
  ├─ close_query / COMMIT
  │
  └─ Destination.write(temp_file) ─► local / S3 / GCS / Azure / stdout
```

No batch accumulates in memory beyond the current `FETCH`. Parquet
writers flush after each batch. The destination upload happens once per
output file at the end of the writer's lifetime.

For the exact sequence of state-store updates that surrounds this
pipeline (manifest, cursor, metrics, progression), see
[ADR-0001 — State update invariants](adr/0001-state-update-invariants.md)
and [ADR-0008 — Committed / verified progression](adr/0008-export-progression.md).

---

## Key traits

The pipeline is composed of four traits. Adding a new database engine,
output format, or destination means implementing exactly one of them.

```rust
// Read-only inputs for a single export call. Packs the parameters that
// used to live as 5 positional args on Source::export into a named struct.
pub struct ExportRequest<'a> {
    pub query: &'a str,
    pub incremental: Option<&'a IncrementalCursorPlan>,
    pub cursor: Option<&'a CursorState>,
    pub tuning: &'a SourceTuning,
    pub column_overrides: &'a ColumnOverrides,
}

// Source pushes data through a sink callback. `Send` not `Sync`
// — see ADR-0011.
pub trait Source: Send {
    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()>;
    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;
    // Returns column type mappings via a LIMIT-0 probe query (used by `rivet check --type-report`).
    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>>;
}

// Sink receives schema and batches one at a time.
pub trait BatchSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()>;
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
}

// Format writer streams output incrementally.
pub trait FormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
    fn bytes_written(&self) -> u64;         // for max_file_size splitting
}

pub trait Format {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        writer: Box<dyn Write + Send>,
    ) -> Result<Box<dyn FormatWriter>>;
    fn file_extension(&self) -> &str;
}
```

Implementations:

| Trait | Concrete types |
|-------|----------------|
| `Source` | `PostgresSource` (DECLARE CURSOR + FETCH N), `MysqlSource` (`query_iter`), `MssqlSource` (tiberius; `OFFSET … FETCH NEXT`), `MongoSource` (JSON-blob: `_id` + `document`) |
| `Format` | `CsvFormat`, `ParquetFormat` |
| `FormatWriter` | `CsvWriter` (`csv::Writer`), `ParquetArrowWriter` |
| `Destination` | `LocalDest`, `S3Dest` (OpenDAL), `GcsDest` (OpenDAL), `AzureDest` (OpenDAL), `StdoutDest` |

---

## Change-data capture seam

Batch export is one shape; log-based change-data capture (`mode: cdc`, and
the `rivet cdc` command) is the other. It has its own pluggable seam, parallel
to `Source`: the `ChangeStream` trait in `src/source/cdc/mod.rs`.

```rust
// A blocking pull of canonical changes. `None` ⇒ no more changes right now.
pub(crate) trait ChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>>;
    // Acknowledge that every change up to `position` is durably persisted.
    // Consume-on-read engines (PostgreSQL slot advance) defer the real consume
    // here so a crash before a durable write re-reads (at-least-once).
    fn ack(&mut self, _position: &Position) -> Result<()> { Ok(()) }
}
```

All four engines implement it — `PgChangeStream` (logical replication slot),
`MysqlChangeStream` (binlog), `MssqlChangeStream` (change tables / from-LSN),
`MongoChangeStream` (change stream, requires a replica set). The factory
`create_change_stream` dispatches by engine exactly as `create_source` does for
the batch path. Each adapter yields a canonical `ChangeEvent` (op, schema,
table, before/after image, resume position); the CDC sink writes the
after-image typed, prefixed with the meta columns `__op` / `__pos` / `__seq`
(`__seq` is the total intra-transaction change order for correct current-state
dedup). Resume is per-engine — PostgreSQL slot, MySQL binlog checkpoint file,
SQL Server from-LSN, MongoDB resume token — and each is at-least-once.

---

## Memory model

Peak in-flight Arrow memory per export:

```
peak ≈ batch_size * avg_row_size * parallel_threads
```

Real process RSS is higher because of database connection pools, the
Tokio runtime used by destinations, jemalloc overhead, and the OS page
cache on the temp file. For wide tables (TEXT / JSONB payloads), keep
`batch_size` low and prefer the `safe` profile; for narrow tables on a
read replica, `fast` with a larger `batch_size` extracts faster at
higher peak RSS.

- Full guidance: [reference/tuning.md](reference/tuning.md).
- Optional runtime guard: set `tuning.memory_threshold_mb` to pause
  fetching / chunk dispatch above a chosen RSS.

The `resource` module implements the RSS sampler — macOS uses
`mach_task_basic_info`; Linux uses `/proc/self/statm`.

---

## Connection pooler / proxy detection

Rivet relies on a number of *session-scoped* primitives at connect time:
Postgres `SET LOCAL statement_timeout`, MySQL `SET SESSION
max_execution_time`, `SET time_zone = '+00:00'`, server-side cursors
(`DECLARE CURSOR`), and per-statement diagnostics like
`pg_backend_pid()` and `CONNECTION_ID()`. Transaction-mode poolers
(pgBouncer, ProxySQL with default settings) hand each statement to a
different backend connection — silently making those primitives
ineffective.

So the SQL drivers (PostgreSQL, MySQL, SQL Server) detect the wire-level
shape of the connection at open time and warn once when it is a pooler /
proxy / gateway, rather than letting the operator find out later through
an inexplicably long-running query or session-state leak:

- **Postgres** — `detect_pg_transaction_pooler` in
  `src/source/postgres/mod.rs` compares `pg_backend_pid()` across two
  consecutive queries. Different PIDs imply transaction-mode pooling
  (pgBouncer, Odyssey). The warning explicitly names what does not
  work: `SET LOCAL` is transaction-scoped, advisory locks / `LISTEN`
  are unavailable.

- **MySQL** — `classify_mysql_proxy` in `src/source/mysql/proxy.rs` is a pure
  classifier over four signals, in this precedence order:
  1. `PROXYSQL INTERNAL SESSION` accepted as a query (strongest —
     ProxySQL intercepts this on its client port; vanilla MySQL returns
     a syntax error).
  2. `@@version_comment` banner contains `proxysql` or `maxscale`.
  3. `@@proxy_version` is set (ProxySQL-only system variable).
  4. `CONNECTION_ID()` differs across two consecutive queries on the
     same `Conn` (generic transaction-mode multiplexing — catches
     HAProxy MySQL mode, in-house balancers, ProxySQL/MaxScale that
     hide their banner).

  The classifier yields `MysqlProxyKind { Direct, ProxySql, MaxScale,
  Multiplexed }`. The non-`Direct` variants log a one-time warning
  describing the specific risk (session-state non-persistence, query
  rewriting, etc.).

- **SQL Server** — `classify_mssql_proxy` in `src/source/mssql/proxy.rs`
  is a pure classifier over the same shape, in precedence order:
  1. `@@SPID` differs across two consecutive queries → `Multiplexed`
     (statement-level connection multiplexing — the session-scoped
     primitives do not persist).
  2. `SERVERPROPERTY('EngineEdition')` of `5` (Azure SQL DB) or `8`
     (Managed Instance), or an Azure `@@VERSION` banner → `AzureGateway`
     (the connection may be redirected through the gateway).

  It yields `MssqlProxyKind { Direct, Multiplexed, AzureGateway }`; the
  non-`Direct` variants log a one-time warning as above.

This detection is best-effort and intentionally never fails an export —
it gives the operator one observable line in the logs. The session
cleanup code (RAII `PgTxnGuard` on Postgres; explicit `SET` resets on
MySQL) runs unconditionally because the same code is correct against
both direct and proxied backends; the warning is about *behavioural*
side-effects (timeouts, locks, NOTIFY) that the cleanup cannot recover.

Coverage: 18 unit tests on `classify_mysql_proxy` exhaustively cover
the signal precedence; `tests/live_pool_safety.rs` runs the full
session-leak suite against pgBouncer (transaction mode, pool_size=1)
and ProxySQL (transaction-persistent pool) under the `pool`
docker-compose profile. See [docs/reliability-matrix.md § Pool and load
pressure](reliability-matrix.md#pool-and-load-pressure).

---

## Project structure

```
src/
  main.rs                 Thin entry: env_logger init → cli::Cli::parse → cli::dispatch
  lib.rs                  Public modules for integration tests + rivet-mcp binary
  enrich.rs               Meta columns (_rivet_exported_at, _rivet_row_hash via xxh3_128)
  error.rs                Result type alias
  journal.rs              RunJournal / RunEvent / JournalEntry / PlanSnapshot (top-level
                            so state/journal_store does not have to import from pipeline)
  mcp.rs                  Stdio JSON-RPC server (read-only PG/MySQL/pgBouncer diagnostics);
                            wrapped by the dedicated bin/rivet-mcp.rs binary
  notify.rs               Slack webhook notifications
  quality.rs              Data quality checks (row count, null ratio, uniqueness)
  resource.rs             RSS sampling (macOS + Linux)
  sql.rs                  Identifier quoting + cursor escaping (CC9 / CC10)
  test_hook.rs            Test-only fault injection (see reference/testing.md)

  cli/                    Clap surface, validation, dispatch (split from a 1000-line main.rs)
    mod.rs                  Re-exports Cli + dispatch
    args.rs                 Clap derive types (Cli, Commands, StateAction, *Format) — pure grammar
    validate.rs             Cross-flag invariants that clap cannot express
    params.rs               --param KEY=VALUE parsing + --source/--source-env/--source-file resolution
    dispatch.rs             match Commands → pipeline / init / preflight entry points

  config/                 YAML parsing, validation, env/file resolution
    mod.rs, models.rs, resolve.rs, cursor.rs, tests.rs

  tuning/                 Tuning profiles + memory model (split from a single 678-line file)
    mod.rs                  Re-exports the externally-used names
    profile.rs              SourceTuning + TuningConfig + TuningProfile + BatchMemoryPolicy
    memory.rs               estimate_row_bytes + compute_batch_size_from_memory
    adaptive.rs             ADAPTIVE_SAMPLE_INTERVAL + next_adaptive_batch_size feedback loop

  source/                 Database drivers, query shaping, pooler detection, CDC
    mod.rs                  Source / BatchSink traits; ExportRequest; TableIntrospection;
                              create_source factory; warn_if_tls_disabled
    batch_controller.rs     Shared batch-loop driver (fetch → sink → throttle) across engines
    postgres/               DECLARE CURSOR + FETCH N; PgTxnGuard (RAII); detect_pg_transaction_pooler
      mod.rs, arrow_convert.rs, from_parse.rs, cdc.rs (PgChangeStream — logical slot)
    mysql/                  query_iter; MysqlProxyKind (Direct/ProxySql/MaxScale/Multiplexed)
      mod.rs, arrow_convert.rs, proxy.rs (classify_mysql_proxy), cdc.rs (MysqlChangeStream — binlog)
    mssql/                  tiberius OFFSET/FETCH + keyset; MssqlProxyKind (Direct/Multiplexed/AzureGateway)
      mod.rs, arrow_convert.rs, proxy.rs (classify_mssql_proxy), cdc.rs (MssqlChangeStream — change tables)
    mongo/                  JSON-blob model (_id + document); keyset / parallel / resume; full + cdc only
      mod.rs, cdc.rs (MongoChangeStream — change stream, replica set)
    cdc/                    Engine-neutral CDC seam
      mod.rs                  ChangeStream trait + ChangeEvent + create_change_stream factory
      sink.rs                 Typed after-image sink (__op / __pos / __seq meta columns)
      validate.rs, value.rs   Descent validation + canonical RivetValue → JSON rendering
    pg_numeric_wire.rs      NUMERIC wire-format decoding (preserves precision through subquery wrap)
    query.rs                build_incremental_query (dialect-specific WHERE/ORDER BY injection)
    tls.rs                  Postgres native-tls connector builder (verify-full / verify-ca / require)
    value_checksum.rs       Per-value integrity checksum (decoded-value → file)

  format/                 Streaming writers
    mod.rs, csv.rs, parquet.rs

  destination/            Output backends
    mod.rs, local.rs, s3.rs, gcs.rs, gcs_auth.rs, azure.rs, stdout.rs

  pipeline/               Orchestration — the actual export work
    mod.rs, cli.rs            Pipeline entry points called by cli::dispatch
    single.rs                 Single-export full / incremental loop (BEGIN → DECLARE → FETCH → COMMIT)
    job.rs                    Chunked-quality-gate wiring + per-job journal hand-off
    retry.rs                  classify_error → RetryClass {Permanent | Transient {needs_reconnect, extra_delay_ms}}
    summary.rs                RunSummary builder + per-export aggregate
    aggregate.rs              Multi-export run aggregate (--parallel-exports, --json output)
    parallel_children.rs      --parallel-export-processes orchestrator (subprocess fan-out)
    parent_ui.rs              Multi-progress UI for parallel runs
    child.rs                  Child-process entry point + IPC framing (paired with ipc.rs)
    ipc.rs                    Parent ↔ child JSON line protocol
    progress.rs               Indicatif progress bars (ChunkProgress, single-export progress)
    validate.rs               --validate output verification (row count, schema)
    sink/                     ExportSink (writer + temp file lifecycle); cursor.rs sink-cursor helper
    chunked/                  Chunked engine
      mod.rs                    run_chunked_*; sequential and parallel checkpoint loops
      detect.rs                 auto-resolve chunk_column from PK; chunk_sparsity_from_counts
      exec.rs                   Per-chunk SQL build + retry classification per worker
      math.rs                   Range-splitting, dense-ordinal math, by-days windowing
    plan_cmd.rs, apply_cmd.rs Plan generation + sealed apply
    reconcile_cmd.rs, repair_cmd.rs Reconcile / targeted repair (ADR-0009)

  plan/                   Plan artifacts + source-aware prioritization
    mod.rs, artifact.rs, build.rs, contract.rs, inputs.rs, recommend.rs,
    prioritization.rs, campaign.rs, history.rs, reconcile.rs, repair.rs, validate.rs

  types/                  Canonical type system (roadmap §14 / M1–M6)
    mod.rs                  Re-exports: RivetType, TypeMapping, TypeFidelity, ColumnOverrides
    rivet_type.rs           RivetType enum (Bool, Int*, Float*, Decimal, Date, Time, Timestamp,
                              String, Text, Binary, Json, Uuid, Enum, Interval, List{inner}, Unsupported)
    mapping.rs              TypeMapping struct: source_native_type → RivetType → Arrow DataType + fidelity
    fidelity.rs             TypeFidelity (Lossless / WidenedPrecision / WidenedRange / Lossy / Unsupported)
    policy.rs               TypePolicy (Fail/Warn/Allow per fidelity); PolicyViolation; `--strict` gate
    target.rs               ExportTarget (BigQuery); TargetCompat (Ok/Warn/Fail); Arrow→BQ type mapping
    decimal.rs              NUMERIC / DECIMAL precision+scale resolution
    override_type.rs        `exports[].columns:` per-column type overrides
    source_column.rs        SourceColumn (driver-neutral column metadata)
    cursor.rs               CursorState (last_cursor_value + type tag)

  preflight/              EXPLAIN analysis, verdicts, doctor, type reports
    mod.rs, analysis.rs, postgres.rs, mysql.rs, doctor.rs, cursor_expr.rs
    type_report.rs          `rivet check --type-report`: collects TypeMappings, applies TypePolicy,
                              checks ExportTarget compat, renders table or NDJSON

  state/                  SQLite-backed state store (schema v4+)
    mod.rs                  StateStore facade; transaction management
    cursor.rs               export_state.last_cursor_value (incremental cursor persistence)
    file_log.rs             file_log (per-export file ledger; renamed from file_manifest in v8)
    metrics.rs              export_metrics history (CLI: `rivet metrics`)
    checkpoint.rs           chunk_run / chunk_task tables (chunked checkpoint state machine)
    progression.rs          export_progression (committed / verified boundaries — ADR-0008)
    journal_store.rs        Persist RunJournal entries (linked to run_id)
    shape.rs                export_shape + shape_drift_warn_factor
    run_aggregate.rs        Cross-export aggregate persistence
    schema.rs               Schema migrations v1 → v4+

  init/                   `rivet init` scaffolding + discovery artifact
    mod.rs, artifact.rs, candidates.rs, postgres.rs, mysql.rs, yaml_scaffold.rs

  bin/
    rivet-mcp.rs            Dedicated MCP stdio binary (Claude Desktop / Claude Code integration)
    seed.rs                 Test data generator (dev fixture)

tests/                    Offline (cargo test) + live (cargo test -- --ignored)
dev/                      docker-compose fixtures, seed SQL, e2e harness
                            dev/proxysql/proxysql.cnf — backend config for the `pool` profile
docs/                     User-facing documentation (this tree)
```

The shape of this tree is mostly stable — feature work usually extends
an existing module. Earlier moves (v0.5.3 → v0.6.0) reduced the
number of multi-purpose files: `tuning.rs` (678 LoC) → `tuning/` (4
files), `cli/mod.rs` (~1000 LoC) → `cli/` (4 files), `journal.rs`
moved from `pipeline/` to a top-level crate module, and `mcp.rs` now
ships as a separate `rivet-mcp` binary rather than a `rivet mcp`
subcommand. The `source/` tree since grew from two flat driver files
(`postgres.rs`, `mysql.rs`) into a per-engine directory each with its
own `arrow_convert.rs` and `cdc.rs`, plus SQL Server (`mssql/`),
MongoDB (`mongo/`), and the engine-neutral `cdc/` seam.

---

## Where to read next

- [reference/cli.md](reference/cli.md) — every command and flag.
- [reference/config.md](reference/config.md) — every YAML field.
- [reference/testing.md](reference/testing.md) — offline + live test tiers.
- [adr/](adr/) — binding contracts (state invariants, plan/apply,
  cursor policy, progression, reconcile / repair).
