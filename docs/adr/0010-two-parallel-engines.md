# ADR-0010: Two Parallel Execution Engines

**Status**: Accepted
**Date**: 2026-05
**Context**: The architectural audit (2026-05) flagged that rivet runs two distinct parallel execution engines for what looks at first glance like the same job. This ADR documents *why* they coexist, what each is for, and the conditions under which we would unify them.

---

## Decision

**Keep two engines. Do not unify in v0.5.x.**

| Engine | File | Use case | Parallel unit |
|--------|------|----------|---------------|
| **In-process scoped threads** | [src/pipeline/chunked/exec.rs](https://github.com/panchenkoai/rivet/blob/main/src/pipeline/chunked/exec.rs) | Chunked export of a single table — split the row range into N chunks, run them concurrently against the same source DB. | `std::thread::scope` + per-thread `Source` connection |
| **Subprocess fan-out** | [src/pipeline/parallel_children.rs](https://github.com/panchenkoai/rivet/blob/main/src/pipeline/parallel_children.rs) | `--parallel-export-processes` — run **many independent exports** (different tables, different configs) concurrently as separate `rivet` child processes communicating via IPC. | `std::process::Command` + a JSON event stream |

---

## Rationale

The two engines exist because they answer different questions:

1. **In-process threads** are the right tool when:
   - Workers share one Arrow / parquet / OpenDAL runtime in process memory.
   - Workers cooperate (semaphore, shared `Destination`, shared progress bar).
   - Crash isolation is not a requirement — one panicking worker can take the whole process down because they share an export plan.

2. **Subprocesses** are the right tool when:
   - Each export has its own config, plan, state, and OpenDAL runtime — sharing them would mean a much larger refactor of `Destination`, `StateStore`, etc., for cross-export use.
   - **Crash isolation matters**: a failing export must not abort the others. A child process death is observable via exit code; a thread panic poisoning shared state is not.
   - Memory pressure is per-process: a child that explodes its RSS hits the OOM killer alone.

Unifying these into one engine would mean either:
- Pushing chunked exports into subprocesses → much higher overhead per chunk (cold connection, runtime spin-up, IPC for every progress event), losing the in-process semaphore and shared destination.
- Pushing multi-export concurrency into threads → losing per-export crash isolation and OpenDAL-runtime separation.

Neither trade is worth the refactor at v0.5.x scale, especially given that both engines now share `resource::Semaphore` (kernel-parking, no busy-wait) and `RetryClass` (typed error classification).

---

## What this ADR is *not* deciding

This ADR is not "we'll never unify". It is "we accept the duplication for now, and revisit when":

- A *third* parallelism need arises (e.g. async source streaming) — duplication grows from N=2 to N=3 and the cost of keeping them in sync exceeds the cost of consolidation.
- The `Source` trait becomes `Send + Sync` (see [ADR-0011](0011-source-trait-send-not-sync.md)). A shareable `Source` would unlock collapsing chunked workers to share one connection, which changes the in-process trade-offs.
- A user-facing requirement forces a single execution model (e.g. cross-export coordination during chunked exports — currently impossible because subprocesses cannot observe each other's chunk state).

---

## Consequences

### Cost
- Two retry loops, two progress UIs, two error-aggregation paths.
- New cross-cutting features (graceful shutdown, distributed tracing) must be implemented in both.

### Benefit
- Each engine is small and focused; debugging chunked behaviour does not require understanding IPC, and vice versa.
- Subprocess engine inherits OS-level isolation for free.
- Different parallel semantics are not papered over with a `mode: enum`.

---

## Considered Alternatives

1. **One engine via subprocesses only.** Rejected: chunked exports of millions of rows on `--parallel 16` would pay 16× cold connection latency at startup, and inter-chunk progress would be IPC traffic instead of a shared atomic.

2. **One engine via threads only.** Rejected: a panic in one of N exports running multiple plans simultaneously would crash the whole `rivet` process. The operator currently relies on the child-isolation property for long multi-export runs.

3. **Async/tokio for both.** Rejected: rivet has no internal async surface today; `tokio` is in `Cargo.toml` only for `reqwest`/`opendal` transitively. Migrating Source to async I/O is a strictly larger refactor than this ADR is willing to scope. Decision deferred until the Source trait redesign in [ADR-0011](0011-source-trait-send-not-sync.md) lands.

---

## When to revisit

Open a follow-up ADR if any of the following holds:

- The audit graph (`code-review-graph`) shows new flows that *cross* the two engines — currently `pipeline-chunk` (461 nodes) is one community, parallel-children lives in `src-batch`.
- A bug is reported that is impossible to express in one engine and trivial in the other — that asymmetry signals the abstraction is mis-cut.
- Multi-export *with* chunking (i.e. cross-product parallelism) becomes a real use case.
