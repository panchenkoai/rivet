# ADR-0001: State Update Invariants

**Status**: Accepted  
**Date**: 2026-04  
**Context**: Rivet supports retries, resumable chunked exports, incremental cursors, file manifests, and metrics history. As the number of state transitions grows, the ordering rules between them must be explicit so recovery behavior is predictable after any failure.

---

## Problem

The pipeline writes to several independent state stores during a single export run:

- **Cursor store** — tracks the last extracted value for incremental exports
- **File manifest** — records the name, size, and row count of every produced file
- **Chunk checkpoint** — tracks individual chunk task lifecycle for resumable chunked exports
- **Run metrics** — records the final outcome, duration, and resource usage of each run

If these stores are updated in the wrong order — or if failures leave them in inconsistent states — recovery becomes ambiguous. Specifically:

- Should the next run re-extract rows already written to S3?
- Is a file in S3 tracked in the manifest?
- Is a chunk that crashed mid-export safe to resume?

---

## Invariants

### I1 — Finalize Before Write (FBW)

> The temp file writer must be finalized before the file is transferred to the destination.

**Rationale**: A Parquet file without its footer, or a CSV without its last chunk, is corrupt. The destination always receives a complete file.

**Current implementation**: `w.finish()` is called before the `dest.write()` loop in `pipeline/single.rs:run_single_export`.

**Failure mode if violated**: Destination receives a truncated file; downstream consumers produce read errors.

---

### I2 — Write Before Manifest (WBM)

> The manifest entry (`record_file`) is written only after the destination write succeeds. A failed write produces no manifest entry.

**Rationale**: The manifest represents files that are durably available at the destination. An entry for a file that was never written is a phantom record.

**Current implementation**: `st.record_file(...)` is called immediately after `dest.write(...)` returns `Ok` in `pipeline/single.rs:run_single_export` and in the chunked paths.

**Recovery behavior**: If the process is killed between `dest.write` and `record_file`, the file exists at the destination but is absent from the manifest. This is safe — the file is not lost, only untracked. The manifest can be reconstructed.

---

### I3 — Write Before Cursor (WBC)

> The cursor advances only after all destination writes for the current batch succeed. On any write failure, the cursor stays at the prior position.

**Rationale**: If the cursor were advanced before the write, a subsequent run would skip rows that were never durably written. Keeping the cursor behind ensures at-least-once extraction semantics.

**Current implementation**: `st.update(...)` is called after the file-writing loop in `pipeline/single.rs:run_single_export`. If any `dest.write` returns `Err`, execution exits via `?` before reaching the cursor update.

**Consequence**: On retry after a write failure, the same rows are re-extracted and re-written. Consumers of the destination must tolerate duplicate files.

---

### I4 — Metric After Verdict (MAV)

> The run metric is recorded after the final run outcome is determined — never during execution.

**Rationale**: A metric recorded before all artifacts are committed will show a misleading status. The status field in `export_metrics` always reflects the terminal state of the run.

**Current implementation**: `state.record_metric(...)` is called at the end of `pipeline/mod.rs:run_export_job`, after `run_chunked_quality_gate` has resolved the result and the status field is set to `"success"` or `"failed"`.

---

### I5 — Chunk Task Acyclicity (CTA)

> Chunk task state transitions are strictly forward: `pending → running → {completed | failed}`. A `completed` task is never re-claimed. A `failed` task can return to `running` only while `attempts < max_chunk_attempts`.

**Rationale**: Resuming a completed chunk would produce duplicate output. Retrying beyond the configured limit would loop indefinitely on permanent errors.

**Current implementation**: The `claim_next_chunk_task` SQL query selects only rows where `status = 'pending' OR (status = 'failed' AND attempts < max_chunk_attempts)`. Completed tasks are permanently excluded.

**Recovery behavior**: On resume after a crash, tasks left in `running` state are reset to `pending` via `reset_stale_running_chunk_tasks` before new claims are issued.

---

### I6 — Finalize After All Complete (FAC)

> `finalize_chunk_run_completed` must only be called after all chunk tasks are in `completed` state. The pipeline enforces this by checking `count_chunk_tasks_not_completed == 0` before finalizing, and bailing with an error otherwise.

**Rationale**: A chunk run finalized with incomplete tasks cannot be reliably resumed. The final state would show `completed` while some data windows were never exported.

**Current implementation**: `pipeline/chunked.rs:run_chunked_sequential_checkpoint` checks `count_chunk_tasks_not_completed` and calls `anyhow::bail!` if any tasks remain. `finalize_chunk_run_completed` is only reached if that check passes.

---

### I7 — Manifest Failure Is Non-Fatal (MFN)

> Manifest write failures do not abort the export. Files already at the destination are not affected. The manifest can be reconstructed by querying the destination.

**Rationale**: The manifest is an observability aid, not a write gate. Aborting an otherwise successful export because a SQLite `INSERT` failed would be disproportionate.

**Current implementation**: All `st.record_file(...)` call sites use `if let Err(e) = st.record_file(...) { log::warn!(...) }`. The error is logged at `WARN` level so operators can observe manifest drift without causing the run to fail.

---

## Failure Point Map

| Failure point | Cursor | Manifest | Metric | Recovery |
|---|---|---|---|---|
| Kill during extraction | not advanced | no entry | no entry | re-extract from last cursor |
| Kill after write, before manifest | not advanced | no entry | no entry | re-extract; duplicate file at destination |
| Kill after manifest, before cursor | not advanced | entry exists | no entry | re-extract; duplicate file + manifest entry |
| Kill after cursor update | advanced | entry exists | no entry | metric missing; next run starts from new cursor |
| Clean failure (Err return) | not advanced | no entry | `failed` status | normal retry |
| Clean success | advanced | entry exists | `success` status | — |

---

## Test Coverage

Each invariant is covered by at least one automated test. `tests/invariants.rs` covers I1–I7 structural contracts. `tests/journal_invariants.rs` covers the `RunJournal` event-ordering contracts (plan snapshot recorded first, `RunCompleted` recorded last, chunk lifecycle ordering). `tests/recovery.rs` covers chunk checkpoint resume semantics (I5/I6). All three test suites are run as semantic release gates in CI before any binary is produced.
