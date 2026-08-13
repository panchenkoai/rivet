//! Shared runner-side wiring for the OPT-2 adaptive concurrency governor (#152).
//!
//! The DECISION loop lives in [`crate::tuning::Governor`]; this is the per-runner scaffolding both
//! the chunked and keyset parallel runners need — arm a monitoring connection, spawn the governor
//! thread that resizes the permit semaphore, and drain its decisions into the run journal. It was
//! copy-pasted between the two runners and drifted twice: (1) the log mutex was poison-recovered in
//! chunked but plain-`unwrap()`ed in keyset (a panicked worker would then also take down the drain),
//! and (2) keyset once drained AFTER its error check, losing every `ParallelismAdjusted` event on a
//! failed run (roast 2026-08-10). One seam, one behaviour.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::resource::Semaphore;
use crate::source::{self, Source};
use crate::tuning::Governor;

use super::summary::RunSummary;

/// Recover a poisoned lock instead of panicking: a worker that panicked mid-run must not also take
/// down the governor's decision log — those decisions are still valid to journal.
fn recover<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// RAII exit accounting for ONE parallel worker: returns the worker's semaphore permit and bumps
/// the `finished` counter [`GovernorHarness::spawn_into`]'s stop predicate reads — on EVERY exit
/// path, **including an unwinding panic**.
///
/// Why a guard and not two tail statements: a tail statement is skipped when the worker UNWINDS, so
/// a genuine panic in a worker (an Arrow/Parquet builder panic, a driver `unwrap`) leaves
/// `finished == total - 1` forever. The governor thread's only exit is `finished >= total`
/// ([`Governor::run`] has no deadline), and `std::thread::scope` cannot return until every spawned
/// thread joins — so the panic that should have failed the run HANGS the process instead, and with
/// `--parallel-exports` the whole pool stalls behind it. The leaked permit is the same class one
/// layer down: at `parallel = 1` the spawner loop blocks forever on `acquire()` even with the
/// governor disarmed. The keyset runner has carried an equivalent inline `FinishGuard` since #152;
/// the chunked runner shipped the tail-statement form (bughunt 2026-08-13).
pub(crate) struct WorkerExit<'a> {
    semaphore: &'a Semaphore,
    finished: &'a AtomicUsize,
}

impl<'a> WorkerExit<'a> {
    /// Bind the guard at the TOP of the worker closure — before any fallible or panicking work.
    pub(crate) fn new(semaphore: &'a Semaphore, finished: &'a AtomicUsize) -> Self {
        Self {
            semaphore,
            finished,
        }
    }
}

impl Drop for WorkerExit<'_> {
    fn drop(&mut self) {
        self.semaphore.release();
        self.finished.fetch_add(1, Ordering::Relaxed);
    }
}

/// The armed governor for one parallel runner invocation. Owns the monitoring connection and the
/// off-thread decision log; [`spawn_into`](Self::spawn_into) runs the governor thread and
/// [`drain_into`](Self::drain_into) records its decisions. Unarmed (adaptation off / no real
/// parallelism / monitor connect failed) → spawn is a no-op and drain records nothing.
pub(crate) struct GovernorHarness {
    floor: usize,
    ceiling: usize,
    monitor: Mutex<Option<Box<dyn Source>>>,
    log: Mutex<Vec<(usize, usize, String)>>,
}

impl GovernorHarness {
    /// Arm the governor for `plan` at `parallel` slots: compute `[floor, ceiling]` and, when the
    /// user opted into adaptation (`tuning.adaptive`) with real parallelism (`parallel > 1`), open a
    /// DEDICATED monitoring source connection. A failed monitor connection degrades gracefully to
    /// static parallelism (logged), never fails the export. `parallel` may be 0 (no work); `.max(1)`
    /// keeps `clamp`'s bounds valid (the governor is off then anyway).
    pub(crate) fn arm(plan: &ResolvedRunPlan, parallel: usize) -> Self {
        let floor = plan
            .tuning
            .min_parallel
            .unwrap_or(1)
            .clamp(1, parallel.max(1));
        let on = plan.tuning.adaptive && parallel > 1;
        let monitor: Option<Box<dyn Source>> = if on {
            match source::create_source(&plan.source) {
                Ok(s) => {
                    log::info!(
                        "export '{}': adaptive concurrency governor active (parallel {floor}..{parallel})",
                        plan.export_name
                    );
                    Some(s)
                }
                Err(e) => {
                    log::warn!(
                        "export '{}': governor monitoring connection failed; parallelism stays \
                         static at {parallel}: {e:#}",
                        plan.export_name
                    );
                    None
                }
            }
        } else {
            None
        };
        Self {
            floor,
            ceiling: parallel,
            monitor: Mutex::new(monitor),
            log: Mutex::new(Vec::new()),
        }
    }

    /// Spawn the governor thread into `scope` (no-op when unarmed). It samples source pressure on
    /// its own monitoring connection and resizes `semaphore` within `[floor, ceiling]`,
    /// self-terminating once `finished` reaches `total` — keyed on FINISHED (success OR failure),
    /// not completed, so a failing worker can't strand it and deadlock the scope (a worker that
    /// PANICS counts too — that is [`WorkerExit`]'s job). Decisions are buffered in `self.log` (the
    /// journal is not thread-shared) and recorded by [`drain_into`](Self::drain_into) after the
    /// scope joins. Two things get a `warn` line rather than silence: an armed governor whose very
    /// first probe cannot sample, and a signal that DIES mid-run (see the `on_signal_lost` callback
    /// below — the loop then also steps back toward the ceiling rather than staying pinned).
    pub(crate) fn spawn_into<'s>(
        &'s self,
        scope: &'s std::thread::Scope<'s, '_>,
        semaphore: &'s Semaphore,
        finished: &'s AtomicUsize,
        total: usize,
        export_name: &'s str,
    ) {
        let Some(mut monitor) = recover(&self.monitor).take() else {
            return;
        };
        let (floor, ceiling) = (self.floor, self.ceiling);
        let log = &self.log;
        scope.spawn(move || {
            // The decision loop is [`tuning::Governor`]; this callback is the only runner-specific
            // binding — resize the kernel-park semaphore, log the transition, and append it to the
            // off-thread log for the post-scope drain. `RIVET_GOVERNOR_INTERVAL_MS` is read inside
            // `Governor::new`.
            let mut gov = Governor::new(ceiling, floor, ceiling);
            // An armed governor whose sampler yields nothing never acts — and
            // never says so. One probe up front turns that silence into a
            // line: runtime sampling failures (permissions, dropped monitor
            // connection) otherwise leave the operator believing back-off
            // protection is active when parallelism is in fact static
            // (bughunt 2026-08-13). The probe result is not fed to the
            // decision state, so the baseline still comes from the loop's own
            // first sample.
            if crate::source::Source::sample_governor_pressure(monitor.as_mut()).is_none() {
                log::warn!(
                    "export '{export_name}': governor armed, but the source provides no \
                     pressure signal (engine without a foreign-pressure counter, or the \
                     monitor connection cannot sample) — parallelism stays at {ceiling}"
                );
            }
            gov.run(
                &mut monitor,
                || finished.load(Ordering::Relaxed) >= total,
                |from, to| {
                    semaphore.resize(to);
                    let reason = if to < from {
                        "source pressure rising: backed off"
                    } else {
                        "source pressure eased: recovered"
                    };
                    // A shed is a deliberate slowdown of the run — the operator
                    // must SEE it at the default log level (an info-level "this
                    // will be slower" is functionally silent; a field pool run
                    // lost 1h48m to invisible sheds). Recovery stays info.
                    if to < from {
                        log::warn!(
                            "export '{export_name}': governor parallelism {from} → {to} ({reason}) — raise `min_parallel` to floor it, or set `adaptive: false` to disarm"
                        );
                    } else {
                        log::info!(
                            "export '{export_name}': governor parallelism {from} → {to} ({reason})"
                        );
                    }
                    recover(log).push((from, to, reason.to_string()));
                },
                |frozen_at, ceiling| {
                    // The pressure signal died mid-run (monitor connection reaped by a
                    // pooler / server restart / tunnel drop — `postgres::Client` does not
                    // reconnect, so every later sample is `None`). Before this, the run
                    // simply stayed pinned at whatever level the last shed reached, for
                    // hours, silently: the up-front probe above fires only at tick zero,
                    // i.e. exactly when no damage has been done yet. A signal that cannot
                    // be READ is not evidence of pressure, so the loop also fails OPEN
                    // (steps back toward the ceiling) — see `GovernorState::observe`.
                    log::warn!(
                        "export '{export_name}': governor lost its pressure signal (the \
                         monitoring connection can no longer sample) — parallelism was pinned \
                         at {frozen_at} of {ceiling}; stepping back toward {ceiling}. Set \
                         `adaptive: false` to disarm the governor, or `min_parallel` to floor it"
                    );
                },
            );
        });
    }

    /// Record the buffered governor decisions into the run journal. MUST be called BEFORE the
    /// runner's error/bail check so a FAILED run still journals its `ParallelismAdjusted` events
    /// (the drift the keyset copy re-introduced). Consumes the harness; poison-recovers the log.
    pub(crate) fn drain_into(self, summary: &mut RunSummary) {
        for (from, to, reason) in recover(&self.log).drain(..) {
            summary
                .journal
                .record(RunEvent::ParallelismAdjusted { from, to, reason });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    /// Can `n` permits be taken from `sem` within `budget`?
    ///
    /// The acquiring thread is DETACHED, not scoped, on purpose: when a permit
    /// has leaked this thread parks forever, and a scoped join would turn the
    /// failure into a HANG (the very symptom under test) instead of a red
    /// assertion. A parked non-main thread does not keep the test binary alive.
    fn acquires_within(sem: &Arc<Semaphore>, n: usize, budget: Duration) -> bool {
        let got = Arc::new(AtomicBool::new(false));
        let (sem_t, got_t) = (Arc::clone(sem), Arc::clone(&got));
        std::thread::spawn(move || {
            for _ in 0..n {
                sem_t.acquire();
            }
            got_t.store(true, Ordering::Relaxed);
        });
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline {
            if got.load(Ordering::Relaxed) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    /// TWO workers, one of which UNWINDS — the fixture needs both because the
    /// subject accumulates (a counter and a permit pool): with a single worker
    /// a guard that bumps `finished` once for the whole run, or releases a
    /// permit only on the happy path, is indistinguishable from a correct one.
    ///
    /// The panic is expected and its message is printed by the test harness.
    #[test]
    fn worker_exit_guard_counts_and_releases_a_panicking_worker() {
        let sem = Arc::new(Semaphore::new(2));
        let finished = AtomicUsize::new(0);

        for panics in [true, false] {
            // The spawner takes the permit, exactly as `run_chunked_parallel` does.
            sem.acquire();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _exit = WorkerExit::new(&sem, &finished);
                if panics {
                    panic!("rivet test: injected worker panic (expected)");
                }
            }));
            assert_eq!(outcome.is_err(), panics, "worker outcome (panics={panics})");
        }

        assert_eq!(
            finished.load(Ordering::Relaxed),
            2,
            "an UNWINDING worker must still count as finished — the governor's only exit is \
             `finished >= total`, so a missed bump hangs the run forever"
        );
        assert!(
            acquires_within(&sem, 2, Duration::from_secs(5)),
            "both permits must return — a permit leaked by a panicking worker stalls the \
             spawner loop (permanently, at parallel = 1)"
        );
    }

    /// Call-site pin for the guard above.
    ///
    /// The real subject — a chunk worker panicking inside `run_chunked_parallel` —
    /// needs a live source, and BEFORE the fix it HANGS rather than fails, so it
    /// cannot be a unit test (and a live watchdog test would have to kill the
    /// process to report). What is pinned here instead is the wiring: the chunked
    /// worker accounts for its exit through `WorkerExit`, and no longer through
    /// tail statements that an unwind skips. RED against reverting either half.
    #[test]
    fn the_chunked_worker_accounts_for_its_exit_with_the_guard_not_tail_statements() {
        let src = include_str!("chunked/exec.rs");
        assert!(
            src.contains("WorkerExit::new(semaphore, finished)"),
            "the chunked worker must bind the exit guard at the top of its closure"
        );
        assert!(
            !src.contains("finished.fetch_add"),
            "a tail `finished.fetch_add` is skipped when the worker unwinds — the guard owns it"
        );
        assert!(
            !src.contains("semaphore.release()"),
            "a tail `semaphore.release()` is skipped when the worker unwinds — the guard owns it"
        );
    }
}
