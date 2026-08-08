# Plan: bounded concurrent part uploads in the single runner

Status: **in progress** (iteration 1 — `single` runner only)
Related: `docs/plans/cassandra-target.md` (deferred), PR #143 / #145 (context).

## Problem

The single-file runner (`src/pipeline/single.rs`, `run_single_export`) uploads
each part to the destination **strictly sequentially**:

```
for part in completed_parts:
    validate (optional)
    write_part_file(tmp → dest)   # NETWORK: one round-trip per part
    record_part(...)
```

All parts are already materialized as local temp files before the loop starts
(`sink.completed_parts`). Each part's upload therefore waits for the previous
part's full network round-trip, so total time = `N × (latency + transfer)`.
On a high-bandwidth link the transfer is fast but latency-bound links
(SSH tunnels, cross-region VPNs, high-BDP WAN) serialize the latency
component `N` times.

The multi-part runners (chunked-parallel, keyset, parallel-Mongo) already
parallelize across workers, but `single` (snapshot / incremental / time_window)
never does — `parallel:` is a chunk/page fan-out knob that `single` ignores.

## Idea (reconciled from the two session summaries)

Summary A evaluated "network overlap inside a worker" (small win when
`parallel: 4` already saturates the link). Summary B / review identified the
real sequential bottleneck: `single` uploads *ready* parts one at a time. This
plan targets exactly that — a **bounded** set of concurrent part uploads in
`single`, ordered by `part_index` on commit.

## Design

### Config key

```yaml
exports:
  - name: orders
    upload_parallelism: 4   # default 1 = current sequential behaviour
```

- `usize`, `#[serde(default)]`, default `1` (byte-identical current path).
- `0` is clamped to `1` (matches `parallel.max(1)` elsewhere).
- Threaded through: `ExportConfig` → `ResolvedRunPlan.upload_parallelism` →
  `run_single_export`. Plan artifacts keep reading old files (new field
  `#[serde(default)]`, same pattern as `verify`).

### Execution split in `run_single_export`

When `upload_parallelism > 1` **and** `completed_parts.len() > 1`:

1. **Validate inline** (unchanged semantics: per-part `ValidationResult`
   journal event, fail-fast with `?`). Local disk + CPU, does not benefit from
   network parallelism; keeping it inline preserves journaling exactly.
2. **Bounded parallel upload** — `std::thread::scope` + `resource::Semaphore`:
   - One worker per part; each calls `write_part_file` (the worker-safe half of
     the commit seam, same as chunked-parallel).
   - Results land in a `Vec<Option<PartRecord>>` indexed by `part_idx`
     (Mutex-guarded; each worker writes exactly one slot).
   - Barrier = scope join; `write_part_file` is safe off-thread (touches no
     shared run state — commit.rs Seam 1 contract).
   - Temp files stay alive on the main thread: workers only borrow the
     `tmp.path()` (`PathBuf` clone), the `NamedTempFile`s remain owned by the
     sink for the whole scope.
   - Errors: collected per part; after join, any error bails with all part
     indexes listed (chunked-parallel style).
3. **Ordered commit** — after the scope joins, iterate `part_idx` ascending and
   call `record_part(PartKind::File { part_index })`, preserving the
   `manifest_parts` / journal ordering contract (commit.rs Seam 2).

### Invariants preserved

- `record_part` is called only on the main thread, in `part_idx` order
  (manifest order + `FileWritten` journal + I7 file-log ordering).
- ADR-0001 I2/M1 → I7 ordering lives once in `record_part`; the parallel
  upload only replaces Seam 1 (`write_part_file`), which is worker-safe by
  design.
- Duplicate-guard / retry logic untouched (`files_committed` bumps only in
  `record_part`, post-join).
- No new dependencies: `std::thread::scope` + `resource::Semaphore`.

### Out of scope (iteration 1)

- Chunked / keyset runners (already parallel at the worker level; the single
  sink per chunk has one part each).
- `spool: memory` (in-memory temp) — separate opt-in, needs a cold bench.
- Reusing `parallel:` for uploads — it means "concurrent extraction fan-out";
  overloading it would silently change chunked/keyset semantics.

## Benchmarks (acceptance)

Compare total wall time of a multi-part `single` export (N parts, per-part
network delay) with `upload_parallelism: 1` vs `N`. Local stand (no docker):
a `Destination` stub whose `write()` sleeps `latency_ms` and copies the file,
so the network effect is simulated deterministically.

Measured (8 parts, latency-bound link — SSH tunnel / cross-region):

| per-part latency | sequential (old) | parallel 8 (new) | speedup |
|---|---|---|---|
| 50 ms  | 400.6 ms  | 50.3 ms  | **7.97×** |
| 200 ms | 1600.6 ms | 200.2 ms | **7.99×** |

- `cargo bench -- concurrent_upload` (criterion; shape-level: sequential vs
  scope+Semaphore over a latency-stub `Destination`).
- `cargo test --lib pipeline::single::tests::bench_parallel_upload_vs_sequential
  -- --ignored` (REAL `commit_single_parts_parallel` vs the sequential loop:
  8 × 200 ms → 1.60 s vs 200 ms = 7.98×).

On a zero-latency/throughput-bound link the arms converge (the win is
latency-overlap only); `upload_parallelism` stays opt-in (default 1) for that
reason.

## Tests

1. `tests/offline/upload_parallelism.rs` — config gate: key parses through
   `Config::from_yaml`, default = 1, `0` clamped. (CI gate "New config keys
   carry tests".)
2. Unit tests in `pipeline/single.rs` (or `commit.rs`): a fake destination
   with a latency barrier asserts that `write()` calls overlap (concurrency >
   1 observed), that commit order is `part_idx` ascending, and that a failing
   worker aborts after the join with all part indexes.
3. E2E (live/local, no network): `LocalDestination` with `max_file_size`
   rotated into several parts + `upload_parallelism: 2` → all parts present,
   manifest lists them in order, `rivet validate` passes.

## Milestones

- [x] Design + plan (this file)
- [x] Config key + plan threading
- [x] Parallel upload in `single`
- [x] Offline gate test + unit tests
- [x] E2E test
- [x] Bench (old vs new): 7.97× / 7.99× / 7.98×
- [ ] fmt/clippy/test, docgen, commit, PR
