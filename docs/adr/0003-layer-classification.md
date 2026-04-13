# ADR-0003: Layer Classification

**Status**: Accepted  
**Date**: 2026-04  
**Context**: Rivet's pipeline has grown to include preflight analysis, execution orchestration, state management, and observability. Without explicit layer assignments, modules accumulate mixed responsibilities — execution code makes semantic decisions, and observability code owns runtime logic.

---

## Decision

Rivet's modules are classified into four layers. Each module belongs to exactly one layer. The coordinator (`pipeline/mod.rs`) is the only module permitted to bridge layers — it is the seam between planning, execution, and persistence.

---

## Layer Definitions

### L1 — Planning (Decision)

Responsible for: deriving what a run means before it starts. No I/O, no DB connections, no state reads.

| Module | Responsibility |
|--------|---------------|
| `config/` | Raw YAML model — user-facing config shapes |
| `plan/mod.rs` | `ResolvedRunPlan`, `ExtractionStrategy` with behavioral contracts |
| `plan/validate.rs` | Compatibility validation — produces `Diagnostic` list, no side effects |
| `tuning/` | Tuning profiles and parameter resolution |
| `preflight/` | Pre-run source analysis (reads DB metadata, emits diagnostics) |

**Rule**: Planning modules must not write state, open data connections, or modify files.

---

### L2 — Execution

Responsible for: running the resolved plan. Reads source data, writes destination files. May write post-execution state (invariant-ordered, see ADR-0001).

| Module | Responsibility |
|--------|---------------|
| `pipeline/single.rs` | Single-query export (Snapshot, Incremental, TimeWindow) |
| `pipeline/chunked.rs` | Chunked export: sequential, parallel-simple, parallel-checkpoint |
| `pipeline/sink.rs` | Local temp-file write path; inline quality checks |
| `pipeline/retry.rs` | Error classification for retry decisions |
| `pipeline/validate.rs` | Post-write row-count verification |
| `source/` | DB connection and Arrow batch extraction |
| `destination/` | Write to local, S3, GCS, stdout |
| `format/` | Parquet/CSV serialization |
| `quality/` | Quality check evaluation (invoked from sink) |
| `enrich/` | Meta-column injection into Arrow batches |

**Rule**: Execution modules receive a `ResolvedRunPlan` and execute it. They must not re-derive semantic decisions. Post-execution state writes (cursor, manifest, schema) are permitted only after the execution succeeds and only in the order mandated by ADR-0001.

---

### L3 — Persistence

Responsible for: durable state across runs. No execution logic.

| Module | Responsibility |
|--------|---------------|
| `state/mod.rs` | `StateStore` entry point + migration runner |
| `state/cursor.rs` | Incremental cursor positions (`export_state`) |
| `state/checkpoint.rs` | Chunk run/task lifecycle (`chunk_run`, `chunk_task`) |
| `state/metrics.rs` | Run outcome history (`export_metrics`) |
| `state/manifest.rs` | File manifest (`file_manifest`) |
| `state/schema.rs` | Schema snapshot history (`export_schema`) |

**Rule**: Persistence modules must not contain execution logic or make semantic decisions about when to write.

---

### L4 — Observability

Responsible for: surfacing what happened without affecting execution or state.

| Module | Responsibility |
|--------|---------------|
| `pipeline/summary.rs` | `RunSummary` — data accumulator for run metrics, printed at end-of-run |
| `pipeline/journal.rs` | `RunJournal` — typed event log answering the four DoD observability questions |
| `pipeline/progress.rs` | Terminal progress bar for chunked exports |
| `pipeline/cli.rs` | CLI display of state, metrics, files, chunk checkpoints |
| `notify/` | Slack notifications triggered by run outcome |
| `resource/` | RSS memory measurement |

**Rule**: Observability modules must not write state, make execution decisions, or alter the pipeline path.

---

### Coordinator (crosses layers by design)

| Module | Responsibility |
|--------|---------------|
| `pipeline/mod.rs` | Reads config → builds plan → dispatches execution → records metric → notifies |

`pipeline/mod.rs` is the only module permitted to touch all three layers. It must remain thin: its role is orchestration, not logic ownership.

**`RunOptions<'a>`** is defined in `pipeline/mod.rs` and passed through the execution stack. It bundles the per-run CLI flags (`validate`, `reconcile`, `resume`, `params`) as a named struct, replacing a sequence of positional `bool` arguments that were invisible at call sites and prone to transposition bugs. The coordinator constructs `RunOptions` once from the public `run()` signature, then passes it to `run_export_job` and (via individual fields) to `run_exports_as_child_processes`.

---

## Known Mixing (Accepted)

### `pipeline/single.rs` — execution + bounded persistence writes

`run_single_export` writes three state artifacts after a successful write:
1. File manifest entry (`record_file`) — I2
2. Cursor advance (`update`) — I3
3. Schema snapshot (`store_schema` / `detect_schema_change`) — observability

These writes are post-execution, invariant-ordered, and have no effect on the execution path. This mixing is accepted as a bounded exception documented by ADR-0001.

**Future**: If the execution/persistence boundary is ever hardened further, `run_single_export` could return a `SingleRunResult` struct carrying the data to be persisted, and the coordinator would handle the writes. This is deferred — the current mixing is contained and tested.

---

## Consequences

- Each module's `//! **Layer: …**` comment at the top declares its classification.
- New modules must declare their layer.
- Code reviews should flag layer violations: planning code making runtime decisions, execution code re-reading raw config, persistence code containing branching logic.
- `ExtractionStrategy` behavioral methods (`needs_cursor_state`, `is_resumable`, `requires_parallel_execution`, `resolve_query`) keep layer-specific decisions in the planning layer rather than in pipeline dispatch code.
