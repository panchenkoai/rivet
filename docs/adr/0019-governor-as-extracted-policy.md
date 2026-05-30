# ADR-0019: Governor as Extracted Policy with Injectable PressureSource

**Status**: Accepted
**Date**: 2026-05-30

---

## Context

The OPT-2 adaptive concurrency governor — the loop that samples source
write-pressure (`pg_stat_bgwriter.checkpoints_req` on PostgreSQL,
`Innodb_log_waits` on MySQL) and resizes the worker semaphore on
`chunked + parallel > 1` runs — shipped originally (commit `141bf33`)
as a 44-line inline closure inside `std::thread::scope` in
`pipeline::chunked::exec::run_chunked_parallel`. The decision policy
(`tuning::next_parallel`, `tuning::GovernorState`) was a separate pure
module from day one; the **loop body** wrapping it was not.

Live coverage was the only way to exercise governor behaviour under
pressure (`tests/live_governor.rs`):

- `governor_activates_and_run_completes` — verifies the governor arms
  and the run finishes.
- `governor_backs_off_under_concurrent_write_pressure` (commit
  `c8a4150`) — drives the closed loop with a background CHECKPOINT
  writer; takes 2-4 s wall and depends on a live Postgres + tight
  `RIVET_GOVERNOR_INTERVAL_MS` env override.
- `governor_does_not_deadlock_when_chunks_fail` — regression for the
  `thread::scope` deadlock fixed in `16fc662`.

Issues with the inline-closure shape:

- The loop body was not callable without a `Box<dyn Source>` monitor
  and a `&AtomicUsize` for `finished` — both require either a live
  database or non-trivial shared-state plumbing in any test.
- Behaviour under pressure could be observed only with multi-second
  wall-clock tests, which masked timing-sensitive bugs (the deadlock
  fix in `16fc662` was missed for ~four hours of staring at the live
  test before the regression test was structured to catch it
  deterministically).
- The `RIVET_GOVERNOR_INTERVAL_MS` env override lived inline as a `let
  sample_ms = std::env::var(…).ok()…unwrap_or(GOVERNOR_SAMPLE_INTERVAL_MS)`,
  with the poll-interval clamp scattered separately. Two tunables, one
  ad-hoc reads.

## Decision

Extract the loop body into `pipeline::Governor` (in
`src/tuning/adaptive.rs`) and the pressure dependency into a narrow
`pipeline::PressureSource` trait. The runner-side binding (resize
semaphore + log + record off-thread decision) stays where it is — the
extraction is the **loop policy**, not the runner-specific side effects.

### `Governor` struct surface

```rust
pub struct Governor {
    state: GovernorState,         // existing pure decision state
    sample_interval: Duration,    // RIVET_GOVERNOR_INTERVAL_MS or default
    poll_interval: Duration,      // clamped to sample_interval
}

impl Governor {
    pub fn new(start, floor, ceiling) -> Self;           // production: reads env
    #[cfg(test)]
    pub fn with_intervals(start, floor, ceiling, sample, poll) -> Self;

    pub fn tick(&mut self, sample: Option<u64>) -> Option<(usize, usize)>;
    pub fn run<S, Stop, Decide>(
        &mut self,
        source: &mut S,
        stop: Stop,
        mut on_decision: Decide,
    ) where
        S: PressureSource + ?Sized,
        Stop: Fn() -> bool,
        Decide: FnMut(usize, usize);
}
```

`tick` is the pure decision step (delegates to `GovernorState::observe`).
`run` is the loop: poll → check stop → sample → tick → callback. The
runner calls `run(&mut monitor, || finished.load() >= total, |from, to|
{ semaphore.resize(to); log; …})`.

### `PressureSource` trait

```rust
pub trait PressureSource: Send {
    fn sample_pressure(&mut self) -> Option<u64>;
}

impl PressureSource for Box<dyn crate::source::Source> {
    fn sample_pressure(&mut self) -> Option<u64> {
        crate::source::Source::sample_pressure(self.as_mut())
    }
}
```

`Send` because the runner spawns the governor on its own thread inside
`thread::scope`. The blanket impl lets the production runner pass its
already-built monitor connection directly; tests pass a `VecSource`
that hands out canned samples.

## Why `PressureSource` lives in `tuning::` not `source::`

The `Source` trait (in `src/source/mod.rs`) is the full extraction
contract: `export(query, sink)`, `query_scalar`, `type_mappings`, +
`sample_pressure`. It is L2 / L3 per ADR-0003 — a vendor-bound type
implemented once per engine.

`PressureSource` is **what the governor needs**, not what an engine
provides. It is one method, narrower than `Source`, with a different
lifecycle (the governor owns the monitor connection separately from
the worker pool's connections; see `run_chunked_parallel`'s
`governor_monitor: Option<Box<dyn Source>>`).

Putting `PressureSource` in `tuning::` keeps the dependency direction
correct: `tuning::adaptive` defines the trait, `source::*` types that
impl `Source` get the blanket impl for free, and the governor never
needs to depend on the full `Source` surface. ADR-0011 (`Source`:
`Send` not `Sync`) is preserved — `PressureSource: Send` matches and
the blanket impl is compatible.

## Why a struct, not just functions

`Governor` owns three runtime-coupled pieces:

- `GovernorState` (mutated across ticks)
- `sample_interval` and `poll_interval` (related: poll must be ≤
  sample, and both come from the same env-var resolution path)
- The next-sample deadline (`last_sample: Instant`) — internal to
  `run`, not exposed

Bundling them makes the "what to fake, what to inject" boundary
obvious: `PressureSource` is the dependency, the `on_decision`
callback is the runner-side effect, the rest is policy.

## Test surface

Unit tests (in `src/tuning/adaptive.rs::tests`), driven on a fake
`VecSource`:

- `governor_tick_mirrors_governor_state_observe` — pins `tick` as a
  faithful surface for the pure decision; catches a future drift between
  the struct's `tick` and the underlying `GovernorState::observe`.
- `governor_run_emits_decisions_for_every_rising_sample_until_stop` —
  drives the loop on canned rising samples, asserts the exact decision
  sequence (`(6, 5), (5, 4), (4, 3), (3, 2)`) reaches the callback.
  Stop predicate keys on the **sample counter** (via shared
  `Arc<AtomicUsize>`), not the decision counter — the first sample
  only sets the baseline (no decision), so keying on decisions
  deadlocks the loop. This shape was found and fixed during
  implementation of this ADR's work; the test now documents the
  failure mode it survived.
- `governor_run_stops_promptly_within_one_poll_quantum` — regression
  cover for the `16fc662` deadlock-class bug: the stop predicate must
  exit the loop within one poll interval, not be deferred to the next
  full sample interval.

The live tests stay as they are — they cover the **production wiring**
(env var resolution, real source connection, thread-scope teardown),
which the unit tests cannot exercise.

## Consequences

**Positive**

- Governor policy is exercisable in microseconds on a fake source,
  without a live database.
- The deadlock class of bugs (`16fc662`) has a unit-level regression
  cover that fires deterministically, not as a 30-s wall-clock
  watchdog test.
- The `RIVET_GOVERNOR_INTERVAL_MS` env-var resolution + the poll/sample
  clamp lives in one place (`Governor::new`). Tests use
  `Governor::with_intervals` to set explicit values without mutating
  process-global env state.
- The runner-side closure in `run_chunked_parallel` shrinks from 44
  lines to a 14-line callback that does only resize + log + push to
  off-thread decision log.

**Negative**

- `PressureSource` is a new trait operators don't write (only Rivet
  internals implement it). The trait exists for testability, not
  external extension. Per LANGUAGE.md, "one adapter = hypothetical
  seam, two adapters = real seam" — here the two adapters are the
  production `Box<dyn Source>` blanket impl and the test `VecSource`.
  Real seam by the second-adapter rule, but the "real" adapter is
  always test-side; some readers may find this thin.
- Indirection: the inline closure was one place; now there's
  `Governor` + `PressureSource` + the callback. New contributors who
  want to understand "what happens when pressure rises" trace one
  extra hop through `tuning::adaptive`.

## When to revisit

- If a second loop-level governor concept ships (e.g., a memory-budget
  governor, a destination-backpressure governor), unify the loop shape
  across them — the `run(source, stop, on_decision)` pattern
  generalizes cleanly.
- If `PressureSource` gains a second non-test impl (e.g., a synthetic
  pressure source for chaos testing in production), promote the trait
  to a more visible location (currently only used by chunked parallel
  exec; could move to a dedicated module if scope grows).

## References

- `src/tuning/adaptive.rs` — `Governor`, `PressureSource`, `tick`,
  `run`, with unit tests at the bottom of the module.
- `src/tuning/mod.rs` — re-exports `Governor` only (not the trait or
  the constants — those are internal).
- `src/pipeline/chunked/exec.rs::run_chunked_parallel` — call site
  with the 14-line callback.
- `tests/live_governor.rs` — the three live tests that cover
  production wiring.
- ADR-0011 — `Source: Send not Sync`. `PressureSource: Send` matches.
- ADR-0017 — durability ordering map; the governor's worker is
  one of the parallel-engine workers covered there.
- ADR-0018 — Builder facades for runner-invariant ordering; the
  Governor extraction is a separate deepening at the policy-loop
  layer.
- Session commits: `141bf33` (original inline closure), `16fc662`
  (deadlock fix), `c8a4150` (back-off live test), `c7cb7f3` (this
  ADR's extraction work).
