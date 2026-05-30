# ADR-0017: Per-Runner Durability Ordering Map

**Status**: Accepted
**Date**: 2026-05-30

---

## Context

Five runners produce output parts (file + manifest entry + state row +
journal event) on the destination + state-store boundary:

- `pipeline::single::run_single_export` (Snapshot, Incremental)
- `pipeline::keyset::run_keyset` (Keyset / Page)
- `pipeline::chunked::exec::run_chunked_sequential` (Chunked, single thread)
- `pipeline::chunked::exec::run_chunked_parallel` (Chunked, thread pool)
- `pipeline::chunked::sequential_checkpoint::run_chunked_sequential_checkpoint` (Checkpoint, single thread)
- `pipeline::chunked::parallel_checkpoint::run_chunked_parallel_checkpoint` (Checkpoint, worker pool)

All five share `pipeline::commit::record_part` for the ordered tail
(I2/M1 manifest + I7 file-log + counters + journal), introduced by
the commit_part seam work. The cursor + progression writes that follow
share `pipeline::run_store::RunStore` (ADR-0018).

But the **timing** of the file-log write (the I7 step inside
`record_part`) varies across runners. Four runners call `record_part`
synchronously per part inside the runner's main loop; one runner —
`run_chunked_parallel_checkpoint` — writes the file-log
**synchronously inside the worker** (before pushing the part to a
shared `Vec`), then has the parent thread call `record_part` with
`state = None` during the post-scope drain (the parent drain populates
`manifest_parts` + counters + journal; the file-log write is already
durable from the worker).

This ADR documents the asymmetry, why it exists, and what invariants
each variant satisfies.

## The asymmetry

| Runner | `file_log` write site | `manifest_parts` add site | Journal event site |
|---|---|---|---|
| `single` | inline, per-part | inline, per-part | inline, per-part |
| `keyset` | inline, per-page | inline, per-page | inline, per-page |
| `chunked_sequential` | inline, per-chunk | inline, per-chunk | inline, per-chunk |
| `chunked_parallel` | **post-scope drain** | **post-scope drain** | **post-scope drain** |
| `sequential_checkpoint` | inline, per-chunk | inline, per-chunk | inline, per-chunk |
| **`parallel_checkpoint`** | **per-chunk in worker (sync)** | **post-scope drain (`state=None`)** | **post-scope drain** |

The two odd rows are `chunked_parallel` and `parallel_checkpoint`.
They are odd for **different** reasons.

### `chunked_parallel` — three writes coalesced post-scope

Background: this runner has no `chunk_task` persistence (it is the
non-resumable parallel engine; resumability is `parallel_checkpoint`'s
job). All three writes (file_log + manifest_parts + journal) move to a
post-scope drain because:

- The worker has `&mut RunSummary` access via shared
  `agg_*` atomics + a `Mutex<Vec<(PartRecord, chunk_index)>>`, but
  `summary.journal` is not `Send + Sync` for ordered append.
- The post-scope parent has the `&mut summary` borrow, can drain the
  shared `Vec`, and calls `record_part(Some(state), …)` once per
  collected part.

Effect: if the **process crashes between scope-join and drain-end**,
the file is durable at the destination but file_log, manifest_parts,
and journal have **no entry** for that chunk. This is the standard
"crash after destination write, before manifest" window
(ADR-0001 I2 → I3): the file is recoverable from the destination
listing, the manifest is reconstructed from the file_log on resume.

Crash window is **the scope-join to drain-end interval — typically
microseconds** (the drain is a tight CPU loop over an in-memory Vec).
The window in practice is dominated by the destination write, not by
this drain.

### `parallel_checkpoint` — file_log split from manifest_parts

Background: this runner *does* have `chunk_task` persistence and is the
resumable parallel engine. Each chunk's success / failure flips a
`chunk_task` row, and a crash mid-run drops back into a "resume from
chunk_task state" code path.

Live test `live_chunked_recovery.rs::parallel_chunked_crash_after_chunk_complete_resume_finishes_with_no_duplicates`
(C3) panics the parent process after the worker has marked its
`chunk_task` row as `completed`. The resume code must rebuild the
manifest from the per-chunk durable `file_log` rows — there is no
in-memory `RunSummary` to drain, the prior process is gone.

If `parallel_checkpoint` wrote file_log only in the post-scope drain
(like `chunked_parallel`), a crash at this fault point would leave
`chunk_task.status = 'completed'` but `file_log` empty for that chunk.
On resume, the M8 manifest-reconcile path
(`pipeline::chunked::resume_m8`) would see the chunk_task as done but
have no file_log entry to feed back into `manifest_parts`. The C3 test
detects this directly: it asserts post-resume `file_log` and
`manifest_parts` are coherent.

The migration commit (e9b0796) initially moved file_log writes to the
post-scope drain (matching `chunked_parallel`'s shape). C3 failed
immediately. The fix: write `file_log` **synchronously per chunk in
the worker** (via `StateStore::open(&config_path_w)` — see "Known
performance smell" below), push the `PartRecord` to the shared Vec,
and have the parent drain call `record_part(state=None, …)` so the
manifest_parts + counters + journal half runs once without
double-writing the file_log.

Effect: per-chunk durability of file_log survives any crash. The
crash window for manifest_parts is the same scope-join-to-drain-end
microsecond interval as `chunked_parallel`'s, but the file_log half
that's actually needed for resume is already durable.

## Decision

The asymmetry is **kept** because each side optimizes for its
runner's resume semantics:

- `chunked_parallel` has no resume — the only consumer of file_log
  during this run is the post-run report. Coalescing all three writes
  into the drain is correct and simpler.
- `parallel_checkpoint` has resume — the resume path reads file_log
  directly without rebuilding from in-memory state. Per-chunk file_log
  durability is load-bearing for C3-class crashes.

The other four runners are inline per-part because they are
single-threaded — there is no worker/parent split forcing the
question.

## Known performance smell: per-chunk `StateStore::open`

The `parallel_checkpoint` worker opens a fresh `StateStore` connection
per chunk just to call `record_file`. `StateStore::open` on SQLite
takes ~1-5 ms (cold) — for a 1000-chunk run that amortizes to ~1-5
seconds of overhead.

This is **not fixed in this release**. The clean fix is a
`record_file_at_ref(&StateRef, run_id, name, …)` helper in
`state::file_log` that uses the shared `StateRef::Sqlite(path)`
without re-opening — matching the existing pattern for
`claim_next_chunk_task_at_ref`, `complete_chunk_task_at_ref`,
`fail_chunk_task_at_ref`. ~50 lines of state-crate work.

Tracked as follow-up; this ADR exists so future readers see the smell
was conscious and addressable, not a hidden footgun.

## Consequences

**Positive**

- Each runner's crash-window semantics are explicit and matched to
  its resume contract.
- The five runners share the maximum possible code (`commit::record_part`
  body) — the asymmetry is at the **caller** layer, not in
  `record_part` itself.
- C3 at-least-once durability under worker-crash is preserved for the
  resumable parallel engine without forcing the non-resumable one to
  pay the per-chunk-open cost.

**Negative**

- Two runners have non-obvious file_log timing. New contributors who
  read `chunked_parallel` first might assume the same pattern applies
  to `parallel_checkpoint`, or vice versa. This ADR is the
  short-circuit.
- The `StateStore::open` per chunk in `parallel_checkpoint` is a known
  performance smell, not fixed in this release.

## References

- `src/pipeline/commit.rs` — the shared `record_part` body that runs
  in all five runners.
- `src/pipeline/run_store.rs` — cursor + progression ordering at the
  next layer (see ADR-0018).
- `src/pipeline/chunked/exec.rs` — `chunked_parallel` runner with
  post-scope drain.
- `src/pipeline/chunked/parallel_checkpoint.rs` — split-write runner
  with worker-sync file_log + post-scope drain for the rest.
- `tests/live_chunked_recovery.rs::parallel_chunked_crash_after_chunk_complete_resume_finishes_with_no_duplicates`
  (C3) — the test that pins per-chunk file_log durability for the
  resumable engine.
- ADR-0001 — state invariants I1-I8; this ADR specializes the I2 → I3
  crash window per runner.
- ADR-0010 — two parallel engines (in-process chunked vs subprocess
  fan-out); this ADR is about a different parallelism axis (chunked
  workers within one process).
