# ADR-0018: Builder Facades for Runner-Level Invariant Ordering

**Status**: Accepted
**Date**: 2026-05-30

---

## Context

ADR-0001 defines eight state-update invariants (I1-I8) that govern the
ordering of writes a runner makes around each part it commits. ADR-0008
PG2 defines a separate ordering invariant for the cursor + schema +
progression tail. ADR-0012 M1 defines the manifest-parts contract on
top.

Before the session that produced this ADR, those orderings were held by
**convention at every call site**. Each of the (at that point) five
runners — `single`, `keyset`, `chunked/exec::run_chunked_sequential`,
`chunked/exec::run_chunked_parallel`, `chunked/sequential_checkpoint`,
plus the now-added `chunked/parallel_checkpoint` — hand-wrote the
per-part write block (I1 finalize → dest.write → I2/M1 manifest add →
I7 file-log → counters → journal). They also hand-wrote the
post-finalize cursor + progression block. Drift accumulated:

- `keyset` never bumped `files_committed` and had no fault hooks.
- `parallel_checkpoint` never populated `summary.manifest_parts` at
  all — the cloud manifest M1 contract was silently empty for every
  `parallel>1 + chunk_checkpoint:true` run. Documented in commit
  `e9b0796`.
- `parallel_checkpoint` opened a fresh `StateStore` connection per
  chunk just to write `file_log`. Performance smell, also documented.
- `single`'s incremental block (cursor + progression) and chunked
  `record_chunked_commit` disagreed on per-write failure semantics in
  their comments (and in one case, in their code).

The fix shipped over four commits (`034fa64`, `1db8eba`, `bb27336`,
`e9b0796` for `commit::record_part`; `58c2c5d` for `RunStore`). This
ADR documents the architectural pattern those commits picked and why.

## Decision

Two **builder facades** own the two ordered-write groups:

### `pipeline::commit::record_part` — per-part commit ordering

Two-seam split keyed on the parallel-engine fork:

- `commit::write_part_file(dest, tmp_path, rows, file_name) ->
  Result<PartRecord>` — ADR-0001 I1 (finalize) + `dest.write` +
  ADR-0012 M3 fingerprint. **Worker-safe**: takes no shared run state,
  can run off-thread.
- `commit::record_part(plan, summary, state, &PartRecord, kind) -> ()` —
  ADR-0001 I2 fault hook + counters (`bytes_written`,
  `files_produced`, `files_committed`) + ADR-0012 M1
  `manifest_parts.push` + `RunEvent::FileWritten` / `ChunkCompleted` /
  `KeysetPageWritten` journal (variant chosen by `PartKind`) + ADR-0001
  I7 `state.record_file` (warn-on-fail) + I3 fault hook. **Parent-only**:
  touches `&mut summary` and `Option<&StateStore>`.

Sequential runners call both inline per part; the parallel engine
calls `write_part_file` in workers and pushes `PartRecord`s through a
shared `Mutex<Vec<…>>`, then the parent calls `record_part` on each
during a post-scope drain (see ADR-0017 for why one variant of this
splits further).

`PartKind` is a closed enum (`File { part_index }` for snapshot;
`Chunk { chunk_index }` for chunked / checkpoint; `Page { page_index }`
for keyset). The journal-event mapping is internal to `record_part`,
keeping the per-call-site signature uniform.

### `pipeline::run_store::RunStore` — post-finalize cursor + progression

Builder over the two ordered post-finalize writes:

```rust
RunStore::finalize(state, plan, summary)
    .with_cursor(last_val)                            // I3 — fatal on error
    .with_progression(Progression::Incremental {…})    // PG2 — warn-on-fail
    .commit()?;
```

`commit()` writes cursor first (fatal on error, returns directly
without attempting the progression write — a half-finalized run would
log a misleading progression boundary), then dispatches
`Progression::Incremental` to `state.record_committed_incremental` or
`Progression::Chunked` to `chunked::record_chunked_commit` (which
walks `chunk_task` to pick the highest completed chunk_index — see
ADR-0008 PG2). The `after_cursor_commit` test fault hook fires inside
the facade so every runner inherits it.

Scope locked at **cursor + progression**. Schema (with drift policy)
stays in `single.rs::run_single_export` because schema-drift detection
+ Continue/Warn/Fail policy is a runner-level state machine that
does not generalize across modes. Metric writes
(`state.record_metric`) stay in `job.rs` because they belong on the
Coordinator layer (ADR-0003 L4), not on the runner-level Persistence
layer (L3) the facade addresses.

## Why "builder" instead of one method with `Option` args

Three shapes were considered:

| Shape | Tradeoff |
|---|---|
| **Builder** (chosen) | Callers chain only what they have. Chunked runners with no cursor skip `with_cursor`; snapshot runs skip both `with_*` and `commit()` is a no-op. Ordering enforced inside `commit()` regardless of chain order. |
| Single method + `Option`-struct | One call-site, all writes visible. But chunked runners write `Writes { cursor: None, progression: Some(…) }` — noisy. |
| Type-state | Compiler-enforced ordering. Overkill for two optional writes; idiomatic Rust does not lean on this for this scale. |
| Multiple methods on a stateful handle | Ordering becomes "the order you call methods in" — convention-at-call-site, just with a different surface. Defeats the point. |

Builder is the balance: variable writes fit cleanly, ordering rule
lives once in the impl, ceremony is bounded.

## Why "facade" not "trait"

Neither `commit::record_part` nor `RunStore` is a trait. The runners
share an **implementation pattern** (call the facade in the right
place with the right args), not an interface contract. A trait would
require a Run-level abstraction the runners can swap out at runtime —
no such abstraction exists or is needed. The facade is a free function
(for `commit_part`) or a builder struct (for `RunStore`); callers
invoke it directly.

This matches ADR-0015's "data-shape seam vs trait" reasoning at a
different layer: the seam value comes from concentrating
implementation logic, not from substitutability.

## Trade-offs

**Positive**

- **Locality**: ordering rules for I1→I3 and PG2 live in one
  implementation each. Per-write failure semantics (fatal vs
  warn-on-fail) is in the signature of the builder methods, not in
  comments at every call site.
- **Leverage**: six runners (after the OPT-4 keyset and the
  parallel_checkpoint additions) share one body each. New runners
  inherit the contract by construction.
- **Drift prevention**: the M1 gap that parallel_checkpoint had
  (silently empty `manifest_parts`) is structurally impossible under
  the facade — `record_part` always appends to `manifest_parts` when
  called. Documented as the retroactive guard provided by the
  `cfg!(debug_assertions)` coherence check in
  `pipeline::finalize::finalize_manifest`.
- **Fault-hook centralization**: `after_file_write`,
  `after_manifest_update`, `after_cursor_commit` test fault points
  fire once per facade, not once per runner-specific re-implementation
  of the hooks.

**Negative**

- The split between `commit_part` (per-part) and `RunStore`
  (post-finalize) is two facades, not one. Schema-drift stays
  outside, metrics stay above. A new contributor must learn three
  layers (per-part / post-finalize / coordinator-finalize) instead of
  one.
- Builder-with-fluent-chain may feel unidiomatic in Rust where most
  ordered writes are direct function calls. ADR-0017 explains the
  per-runner asymmetry that motivated the chain.
- Test surface includes both seam-level unit tests (in `commit.rs`
  and `run_store.rs`) and runner-level live tests (the existing
  `live_chunked_recovery`, `live_crash_recovery` suites). New runners
  need both layers of coverage.

## When this should be revisited

- If a fourth ordered-write group emerges at the runner level (e.g.,
  per-run lineage tracking, audit log of source queries) that does not
  fit cursor/progression or per-part: extend `RunStore` with a third
  `with_*` method rather than building a third facade.
- If a runner needs to **dispatch** between different per-part commit
  strategies at runtime (today every runner uses the same
  `write_part_file` → `record_part`): re-evaluate the
  facade-vs-trait choice. Until then, free functions are correct.

## References

- `src/pipeline/commit.rs` — `write_part_file`, `record_part`,
  `PartKind` definitions; module doc covers the in-step ordering
  rationale.
- `src/pipeline/run_store.rs` — `RunStore`, `Progression`; module doc
  covers per-write failure model.
- `src/pipeline/summary.rs::RunSummary::check_post_run_invariants` —
  runtime debug_assert that catches a runner bypassing the facade.
- ADR-0001 — state invariants I1-I8.
- ADR-0008 — export progression, PG2 ordering.
- ADR-0012 — cloud manifest contract, M1 / M3.
- ADR-0015 — data-shape seam vs trait (parallel reasoning at the
  source-introspection layer).
- ADR-0017 — per-runner durability ordering map (when the facade is
  called sync per-part vs in a post-scope drain).
- ADR-0019 — Governor extraction (similar deepening pattern at a
  different layer).
- Session commits: `034fa64` (extract commit), `1db8eba`
  (chunked migration), `bb27336` (sequential_checkpoint migration),
  `e9b0796` (parallel_checkpoint M1 gap fix), `58c2c5d` (RunStore).
