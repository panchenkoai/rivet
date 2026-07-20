# ADR-0011: `Source: Send` (not `Sync`)

**Status**: Accepted
**Date**: 2026-05
**Context**: The architectural audit (2026-05) noted that `Source` is `Send` but not `Sync`, which forces every parallel chunk worker to open its own DB connection. With `--parallel 16` on a long export this means 16 backend processes on the source database. The audit asked whether making `Source: Send + Sync` (via interior mutability, the pattern `StateStore` already uses) would be a net improvement.

---

## Decision

**Keep `Source: Send` only. Do not introduce `Sync` in v0.5.x.**

```rust
// src/source/mod.rs
pub trait Source: Send {
    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()>;
    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;
    fn type_mappings(...) -> Result<Vec<TypeMapping>>;
}
```

Every method takes `&mut self`. A worker that needs a `Source` must own it; sharing `&Source` between threads is not supported.

---

## Rationale

### Why `Sync` would be tempting

`StateStore` already uses interior mutability ([RefCell] wrapping `postgres::Client`) so its methods take `&self`. Applying the same pattern to `Source` would let the chunked engine in `pipeline/chunked/exec.rs` share **one** connection across N worker threads. On paper that means:

- 1× DB backend instead of N — large win against a `max_connections=200` Postgres.
- No `lean_pool_opts()` workaround in MySQL (the `min=10` bug we fixed in [f6ba79f](https://github.com/panchenkoai/rivet/commit/f6ba79f)).
- Faster startup — no per-worker `Client::connect`.

### Why it is the wrong trade-off today

1. **Serialized I/O kills the point of parallelism.** A single `postgres::Client` (or `mysql::PooledConn`) is fundamentally not concurrent: each `client.query()` exchanges Postgres wire-protocol packets in lock-step. Wrapping it in `Mutex<Client>` or `RefCell<Client>` means workers contend on the mutex and the parallel design degrades into batched-sequential. We measured a 1.7× *slowdown* on a 4-thread chunked export of `content_items` (200K rows) when prototyped against a `Mutex<Client>`.

2. **`pipeline/chunked/exec.rs` already amortizes connection cost.** Workers open their connection once and reuse it for the entire chunk's lifetime. Per-chunk cost is dominated by the SQL execution, not the connect handshake.

3. **Backend explosion is a non-problem in practice.** A user running `--parallel 16` is explicitly opting into 16 concurrent backends. The exporter has guardrails: `lean_pool_opts()` keeps the MySQL pool at `min=1`, [ADR-0010](0010-two-parallel-engines.md) keeps each child process to one `Source`, and pooler detection ([detect_pg_transaction_pooler](https://github.com/panchenkoai/rivet/blob/main/src/source/postgres.rs)) warns when a pgBouncer is multiplexing. Operators who want fewer backends can run with lower `--parallel`.

4. **The `Sync` refactor blocks on multiple deps the audit flagged separately.**
   - The `postgres::Client` API is `&mut`-only; making it `Sync` requires either `Mutex` (slow, see #1) or an async client (a much larger surface change).
   - `mysql::PooledConn` has the same problem.
   - The Source trait's three methods all mutate session state (timeouts, cursors, prepared statements). Interior mutability without serialization would be unsafe by design, not just slow.

5. **The `Send` bound is exactly what we need.** Workers move their `Source` into `thread::scope`. The trait already supports the parallelism model we actually run; the missing capability (one shared connection) is not a capability we want.

---

## Consequences

### Cost
- N-worker chunked exports open N connections. Documented in the `--parallel` flag help and in pgBouncer guidance.
- No "single-conn parallel" mode. Users who want one backend run sequentially (`--parallel 1`).

### Benefit
- Source trait stays simple — `&mut self` everywhere, no `Mutex`, no `RefCell`, no `Arc<dyn Source>` to reason about.
- Each worker has independent failure semantics — a panic in one connection cannot poison another worker's mid-statement state.
- Easy to add new backends: `impl Source for NewDB` requires only `&mut self` methods, the natural shape for any blocking DB driver.

---

## Considered Alternatives

### A. `Sync` via `Mutex<Client>` inside the impl

Prototyped. Result: workers serialise on the mutex, making `--parallel N` no better than `--parallel 1`. Rejected.

### B. `Sync` via async (tokio + tokio-postgres)

Requires rewriting Source as `async`. Knock-on changes:
- `BatchSink::on_batch` becomes async, propagating through `pipeline/sink.rs` and `format/parquet.rs` (which is sync today).
- `chunked/exec.rs` switches from `thread::scope` to `tokio::join!` / `JoinSet`.
- `Destination::write` is already sync (OpenDAL blocking layer); making it async would unwind the layered design.

Estimated effort: weeks. Estimated value over current model: marginal — chunked workers already saturate the source DB's network and CPU; adding async coordination on top does not help.

Rejected for v0.5.x. Revisit if a future requirement (e.g. async incremental tailing, CDC-like consumption) needs async I/O for an unrelated reason.

### C. Lazy `Source` creation inside workers (current behaviour)

This is what we do. Each chunked worker calls `source::create_source(&plan.source)` inside `thread::scope` and owns its connection for the chunk's duration. `StateRef::Postgres(url)` propagates the connection string into workers without requiring shared state.

---

## When to revisit

Open a follow-up ADR if:
- A blocking SQL driver appears that is genuinely `Sync` without internal serialization (none currently exists for Postgres or MySQL).
- The whole pipeline migrates to async (would also affect [ADR-0010](0010-two-parallel-engines.md)).
- Profiling shows connect handshakes dominating end-to-end latency on a real workload — currently far from the case (200K-row content_items chunked export: connect <50ms, query+stream 8s).

[RefCell]: https://doc.rust-lang.org/std/cell/struct.RefCell.html
