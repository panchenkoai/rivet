# Execution Semantics

How Rivet behaves under normal execution, retries, crashes, resume, repair, and reconcile. This is the contract a downstream pipeline can build on.

This page is a user-facing summary. The binding contracts live in the [ADRs](adr/), which this document links into.

---

## Scope

This document describes guarantees and known non-guarantees for:

- single-table exports (full, incremental, chunked, time-window, cdc),
- destination writes (local, S3, GCS, Azure Blob Storage, stdout),
- state and journal updates,
- automatic retries and `--resume` after crashes,
- `rivet reconcile` and `rivet repair`,
- in-process and subprocess parallelism.

It does not cover database / network / disk failures whose mode is external to Rivet (e.g. a corrupted Parquet file caused by a bad disk).

---

## Core concepts

| Term | Meaning |
|---|---|
| **Run** | One invocation of `rivet run`. Has a unique `run_id`. Produces zero or more output files. |
| **Export** | A named entry in `rivet.yaml` — a query + cursor + destination triple. One run executes one or many exports. |
| **Mode** | `full`, `incremental`, `chunked`, `time_window`, `cdc`. See [docs/modes/](modes/). |
| **Batch** | One `FETCH` worth of rows materialized as an Arrow `RecordBatch`. Streamed; never accumulated in memory. |
| **Chunk** | A row range (chunked mode) processed as one unit. Produces one output file. Has a checkpoint row in the state DB. |
| **Cursor** | The last extracted value for incremental exports. Stored in `export_state.last_cursor_value`. |
| **File log** | The per-export ledger of files written to the destination (`file_log` table, renamed from `file_manifest` in schema v8). |
| **Journal** | A per-run event log (`*.jsonl`). Authoritative for "what happened when" during one run. |
| **Progression** | The committed / verified boundary per export (`export_progression` table). Advisory only. |

---

## Normal execution

The pipeline is one straight line per export (see [architecture.md § Data flow](architecture.md#data-flow)):

```
begin_query → FETCH batch → write batch to temp file
                          → next FETCH
                          → ...
            → finalize writer
            → destination.write(temp_file)
            → record manifest entry
            → advance cursor (incremental) / record chunk completion (chunked)
            → record metric (at end of run)
```

The exact state-write ordering is defined by **[ADR-0001 — State Update Invariants](adr/0001-state-update-invariants.md)** (I1–I7). The pipeline source code references these IDs at the call sites.

---

## Retry semantics

Retries are classified by error type in [src/pipeline/retry.rs](https://github.com/panchenkoai/rivet/blob/main/src/pipeline/retry.rs):

The classifier (`RetryClass`) has two outcomes — `Transient` (retry) and `Permanent` (propagate):

| Class | Examples | Retried? |
|---|---|---|
| `Transient` | connection reset, "server has gone away", lock-wait timeout, deadlock / serialization failure, "too many connections", cloud `(temporary)` writes (S3 / GCS) | Yes — exponential backoff up to `tuning.max_retries`. The variant carries `needs_reconnect` (reopen the source connection first — e.g. a network reset or `08xxx` SQLSTATE) and `extra_delay_ms` (an added settling delay for capacity errors like "too many connections" / "database system is starting up") |
| `Permanent` | syntax error, auth failure, missing table / column, a statement-*duration* timeout (`statement_timeout` / `max_execution_time`) | No — propagates immediately. **Uncategorized errors default to `Permanent`** (they are *not* retried) |

A retried batch starts from the **same cursor position** as the failed attempt — see ADR-0001 I3 (Write Before Cursor). At-least-once delivery to the destination is therefore possible: on retry after a destination write succeeded but the cursor failed to advance, the same rows are written again, producing a duplicate file.

**Retry-safety per destination** is declared by `capabilities().retry_safe` — see **[ADR-0004 — Destination Write Contracts](adr/0004-destination-write-contracts.md)**:

| Destination | `retry_safe` |
|---|---|
| S3, GCS, Azure | `true` (no partial visible objects; safe to retry) |
| Local filesystem | `true` (staged temp file + atomic `rename`; a failure leaves nothing at the final path) |
| stdout | `false` (no commit boundary; retry produces duplicate/corrupt output) |

When retries occur against a non-retry-safe destination, the pipeline logs a `WARN` so operators see the mismatch.

---

## Crash semantics

If the process is killed (SIGKILL, OOM, host reboot), the next state depends on **where** the crash landed. The full failure-point map is in [ADR-0001 § Failure Point Map](adr/0001-state-update-invariants.md#failure-point-map). Summarised:

| Crash point | Files at destination | Manifest | Cursor | Next run does |
|---|---|---|---|---|
| Mid-extraction (before `dest.write`) | none | no entry | not advanced | re-extract from last cursor |
| After `dest.write`, before manifest | file present | no entry | not advanced | re-extract → duplicate file at destination |
| After manifest, before cursor | file present | entry | not advanced | re-extract → second duplicate + manifest entry |
| After cursor update | file present | entry | advanced | next run starts from new cursor; metric may be missing |
| Clean error (`Err` return) | none | no entry | not advanced | normal retry |

Two strong invariants hold across every crash point:

- **No row is silently skipped.** The cursor only advances after the corresponding write succeeds (I3).
- **No file at the destination is incomplete.** Writers are finalized before destination upload (I1).

The trade-off is **at-least-once at the destination**: a crash between write and cursor advancement produces a duplicate file. Downstream consumers must tolerate this — see [Known non-guarantees](#known-non-guarantees) below.

---

## Resume semantics

`rivet run --resume` (and the default behaviour for chunked runs with `chunk_checkpoint: true`) consults the state DB to decide what work is outstanding:

- **Incremental exports** resume from `export_state.last_cursor_value`.
- **Chunked exports** consult the `chunk_task` table: tasks in `completed` are skipped; tasks in `pending` or `running` (the latter reset to `pending` on resume) are re-issued; tasks in `failed` are retried while `attempts < max_chunk_attempts`.
- **Full and time-window** modes do not resume — they restart from the beginning. The previous run's output files remain at the destination unless cleaned manually.

Chunk task transitions are **strictly forward** (`pending → running → {completed | failed}`). A `completed` chunk is never re-claimed, even after a crash — see ADR-0001 I5 (Chunk Task Acyclicity).

---

## Repair semantics

`rivet repair` re-exports specific chunks identified as `mismatch` or `unknown` by a prior `rivet reconcile`. The full contract is **[ADR-0009 — Reconcile and Targeted Repair](adr/0009-reconcile-and-repair-contracts.md)**.

Key properties:

- **Repair derives only from a reconcile report** (RR1). There is no operator-typed chunk range.
- **Dry-run by default** (RR2). `--execute` is required to perform writes.
- **Repair writes new files alongside originals** (RR5). Rivet does **not** delete or overwrite prior destination files. Downstream dedup is the operator's responsibility.
- **Repair does not advance the committed boundary** (RR4). Run `rivet reconcile` again afterwards to advance `last_verified_*`.

---

## Reconcile semantics

`rivet reconcile` compares per-chunk row counts between source and destination for the latest chunked run:

| Per-partition outcome | Meaning |
|---|---|
| `match` | Counts equal |
| `mismatch` | Both counts known and different |
| `unknown` | Either count missing (e.g. chunk never completed) |

Scope and limits:

- **Chunked mode only in v1.** `time_window` and `incremental` exports surface a clear "not supported" error.
- **`COUNT(*)` based.** No hash-based partition verification yet.
- **Verified boundary advances only on a fully clean report** — zero mismatches and zero unknowns ([ADR-0008 § PG5](adr/0008-export-progression.md)).
- **Exit code gates on mismatch.** `rivet reconcile` exits **non-zero** when any partition is a `mismatch`, so `rivet reconcile && <next step>` does not proceed on disagreeing data (mirrors `rivet validate`). `unknown` partitions (an incomplete chunk, or a non-integer keyset key with no source re-count) are surfaced as a **warning** but do **not** fail the command — "could not verify" is not "verified wrong", and a keyset export is structurally all-`unknown`. The mismatch detail is always in the printed report regardless of exit code.

Reconcile reads from the same source and never writes files itself.

---

## Parallel execution semantics

Rivet runs two distinct parallel engines — see **[ADR-0010 — Two Parallel Engines](adr/0010-two-parallel-engines.md)**:

| Engine | Use case | Crash isolation |
|---|---|---|
| In-process scoped threads | Chunked export of a single table, split into N concurrent chunks | None — a panicking worker can fail the run |
| Subprocess fan-out (`--parallel-export-processes`) | Many independent exports concurrently, one child per export | OS-level — a failing child exits non-zero; the parent aggregates and returns non-zero |

Both engines honour the same state invariants. Chunk checkpoints serialise the parallel threads' state writes; subprocess children write to independent state files unless explicitly pointed at a shared one (not recommended).

---

## Quality gates

When `quality:` is configured, the pipeline evaluates row-count, null-ratio, and uniqueness checks **before** advancing committed progression. A failing gate aborts the run with a non-zero exit code; the destination files remain (manual cleanup or replacement is the operator's call). See [docs/best-practices/quality-checks.md](best-practices/quality-checks.md).

---

## Destination commit boundaries

| Destination | Commit protocol | What "Ok" means |
|---|---|---|
| S3 / GCS / Azure | `FinalizeOnClose` | Object is committed only after writer close; a mid-upload failure leaves nothing visible |
| Local filesystem | `Atomic` | `Ok` means the full file is present; staged temp file + atomic `rename`, so a failure leaves nothing at the final path (`retry_safe: true`, `partial_write_risk: false`) |
| stdout | `Streaming` | No atomic commit boundary; partial output may be observable before `write()` returns |

Full per-backend table and rationale: **[ADR-0004 — Destination Write Contracts](adr/0004-destination-write-contracts.md)**.

---

## Known non-guarantees

Rivet does **not** currently guarantee:

- **Exactly-once delivery to the destination.** Crashes between destination write and cursor advancement can produce duplicate files. Plan downstream dedup or idempotent ingestion — the manifest's per-part `content_fingerprint` is the supported dedup key: identical rows produce byte-identical parts (and the same fingerprint) across rivet releases, so a duplicate is safely droppable by fingerprint. See [recipes/idempotent-warehouse-load.md](recipes/idempotent-warehouse-load.md).
- **Continuous / near-real-time replication.** Rivet *does* capture CDC to files (`mode: cdc` — inserts/updates/deletes via a Postgres logical replication slot / MySQL binlog / SQL Server CDC change tables / MongoDB change streams, into typed Parquet/CSV — or the JSON-blob document image for MongoDB — resuming from the last committed log position each run), but it is not a continuously-running stream — changes are captured per invocation, not delivered live. For always-on near-real-time replication use Debezium or Estuary.
- **Completeness of incremental cursors that can tie.** Incremental resume uses a strict `WHERE cursor > last_value`. If two rows share the high-watermark value and the second becomes visible only *after* the run that advanced the watermark past it — e.g. a low-resolution `updated_at` (second granularity) or rows committed at the same timestamp after the read snapshot — the next run skips them and they are never exported. (Keyset pagination is unaffected: its key is planner-enforced unique + NOT NULL.) Use a **strictly per-row-distinct, monotonic** cursor (a sequence/identity id, or a timestamp with sub-value uniqueness); when the cursor can tie, re-snapshot the affected window with `full`/`chunked` mode.
- **Dense-chunking stability on a tied, concurrently-written `chunk_column`.** `chunk_dense: true` pages by `ROW_NUMBER() OVER (ORDER BY chunk_column)`, recomputed in an independent query per chunk. The ordinal partition itself never gaps or overlaps, but the ordinal→row mapping is only stable if the `ORDER BY` is deterministic. On a column with a large **tied** peer group straddling a chunk boundary *while the source is being written concurrently*, two chunk queries could order the tied band differently — duplicating or dropping a boundary row. Against a **static** table this does not occur on any tested engine (PG 16 / MySQL 8 / SQL Server 2022 — verified by `tests/live_chunked_dense.rs`). Prefer a `chunk_column` with no large tied groups, or **keyset** (`chunk_by_key`) on a unique key, when chunking a live-writing table.
- **Automatic cleanup of an interrupted write's temp file.** A crash mid-write on the local destination may leave a dot-prefixed temp file in the target directory — never a partial *final* file (the commit is an atomic `rename`, so the final path is the complete file or absent). The stray temp file is harmless and can be removed manually.
- **Schema migration handling.** If the source schema changes between runs, Rivet does not migrate the destination; it surfaces a schema-drift error (see [tests/live_schema_drift.rs](https://github.com/panchenkoai/rivet/blob/main/tests/live_schema_drift.rs)).
- **Correctness of user-authored SQL.** Rivet executes `query:` verbatim. A query that omits a `WHERE` clause or selects from the wrong table will export the wrong data — there is no semantic validation.
- **Protection from poorly indexed source queries.** Preflight (`rivet doctor`, `rivet check`) warns about missing cursor indexes and unbounded `ORDER BY`, but it does not refuse to run. The operator decides.
- **Stdout state safety.** Using stdout with cursor or manifest state is technically allowed but not meaningful; plan validation rejects `stdout + chunked` and `stdout + max_file_size` before execution (ADR-0004 Known Gap).
- **Atomicity across exports in one run.** If a run has three exports and the second fails, the first export's writes are already committed — the run does not roll back.
- **Cross-run ordering when running in parallel from multiple machines** against the same state DB. The state DB is SQLite and is designed for a single Rivet process at a time.

---

## Test coverage

The invariants on this page are exercised by:

- [tests/invariants.rs](https://github.com/panchenkoai/rivet/blob/main/tests/invariants.rs) — ADR-0001 I1–I7 structural contracts.
- [tests/journal_invariants.rs](https://github.com/panchenkoai/rivet/blob/main/tests/journal_invariants.rs) — RunJournal event ordering.
- [tests/recovery.rs](https://github.com/panchenkoai/rivet/blob/main/tests/recovery.rs) — chunk checkpoint resume semantics (I5, I6).
- [tests/live_crash_recovery.rs](https://github.com/panchenkoai/rivet/blob/main/tests/live_crash_recovery.rs) — crash-and-resume against a live database.
- [tests/live_chunked_recovery.rs](https://github.com/panchenkoai/rivet/blob/main/tests/live_chunked_recovery.rs) — chunked resume after partial completion.
- [tests/live_retry_and_faults.rs](https://github.com/panchenkoai/rivet/blob/main/tests/live_retry_and_faults.rs) — retry classification under injected faults.
- [tests/live_reconcile_repair.rs](https://github.com/panchenkoai/rivet/blob/main/tests/live_reconcile_repair.rs) — reconcile / repair end-to-end.

Invariants and recovery suites run as **named semantic gates** in PR CI ([.github/workflows/ci.yml](https://github.com/panchenkoai/rivet/blob/main/.github/workflows/ci.yml)). Branch protection blocks merges on regression. See [reliability-matrix.md](reliability-matrix.md) for the full coverage matrix.
