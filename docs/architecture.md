# Architecture

How Rivet extracts data end-to-end: the pipeline, the traits that make it
pluggable, the memory model, and the source tree layout.

This is a reference document. If you only want to configure or run an
export, start with [getting-started.md](getting-started.md).

---

## Data flow

Every export, regardless of mode, follows the same streaming shape:

```
Source (PG / MySQL)
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
  └─ Destination.write(temp_file) ─► local / S3 / GCS / stdout
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
// Source pushes data through a sink callback.
trait Source: Send {
    fn export(
        &mut self,
        query: &str,
        cursor_col: Option<&str>,
        cursor: Option<&CursorState>,
        tuning: &EffectiveTuning,
        sink: &mut dyn BatchSink,
    ) -> Result<()>;
    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;
}

// Sink receives schema and batches one at a time.
trait BatchSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()>;
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
}

// Format writer streams output incrementally.
trait FormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
    fn bytes_written(&self) -> u64;         // for max_file_size splitting
}

trait Format {
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
| `Source` | `PostgresSource` (DECLARE CURSOR + FETCH N), `MysqlSource` (`query_iter`) |
| `Format` | `CsvFormat`, `ParquetFormat` |
| `FormatWriter` | `CsvWriter` (`csv::Writer`), `ParquetArrowWriter` |
| `Destination` | `LocalDest`, `S3Dest` (OpenDAL), `GcsDest` (OpenDAL), `StdoutDest` |

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

## Project structure

```
src/
  main.rs                 CLI entry (clap)
  lib.rs                  Public modules for integration tests
  enrich.rs               Meta columns (_rivet_exported_at, _rivet_row_hash via xxh3_128)
  error.rs                Result type alias
  notify.rs               Slack webhook notifications
  quality.rs              Data quality checks (row count, null ratio, uniqueness)
  resource.rs             RSS sampling (macOS + Linux)
  sql.rs                  Identifier quoting + cursor escaping (CC9 / CC10)
  test_hook.rs            Test-only fault injection (see reference/testing.md)
  tuning.rs               Profiles + memory-adaptive batch sizing
  types.rs                CursorState

  config/                 YAML parsing, validation, env/file resolution
    mod.rs, models.rs, resolve.rs, cursor.rs, tests.rs

  source/                 Database drivers and query shaping
    mod.rs, postgres.rs, mysql.rs, query.rs, tls.rs

  format/                 Streaming writers
    mod.rs, csv.rs, parquet.rs

  destination/            Output backends
    mod.rs, local.rs, s3.rs, gcs.rs, gcs_auth.rs, stdout.rs

  pipeline/               Orchestration
    mod.rs, single.rs, retry.rs, sink.rs, summary.rs, validate.rs, journal.rs,
    progress.rs, chunked/, plan_cmd.rs, apply_cmd.rs, reconcile_cmd.rs,
    repair_cmd.rs, cli.rs

  plan/                   Plan artifacts + source-aware prioritization
    mod.rs, artifact.rs, inputs.rs, recommend.rs, prioritization.rs,
    campaign.rs, history.rs, reconcile.rs, repair.rs, validate.rs

  preflight/              EXPLAIN analysis, verdicts, doctor
    mod.rs, analysis.rs, postgres.rs, mysql.rs, doctor.rs, cursor_expr.rs

  state/                  SQLite-backed state store (schema v4)
    mod.rs, cursor.rs, manifest.rs, metrics.rs, checkpoint.rs,
    progression.rs, schema.rs

  init/                   `rivet init` scaffolding + discovery artifact
    mod.rs, artifact.rs, candidates.rs, postgres.rs, mysql.rs

  bin/
    seed.rs               Test data generator (dev fixture)

tests/                    Offline (cargo test) + live (cargo test -- --ignored)
dev/                      docker-compose fixtures, seed SQL, e2e harness
docs/                     User-facing documentation
```

The shape of this tree is stable — feature work usually extends an
existing module rather than adding a new top-level one.

---

## Where to read next

- [reference/cli.md](reference/cli.md) — every command and flag.
- [reference/config.md](reference/config.md) — every YAML field.
- [reference/testing.md](reference/testing.md) — offline + live test tiers.
- [adr/](adr/) — binding contracts (state invariants, plan/apply,
  cursor policy, progression, reconcile / repair).
