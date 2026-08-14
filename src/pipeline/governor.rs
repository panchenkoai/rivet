//! Shared runner-side wiring for the OPT-2 adaptive concurrency governor (#152).
//!
//! The DECISION loop lives in [`crate::tuning::Governor`]; this is the per-runner scaffolding every
//! parallel runner needs — arm a monitoring connection, spawn the governor thread that resizes the
//! permit semaphore, and drain its decisions into the run journal. Three runners wire it today:
//! `chunked/exec.rs` (spawner shape, one thread per chunk), `chunked/parallel_checkpoint.rs` (POOL
//! shape — see [`TaskPermit`]), and `keyset.rs` (per page). A fourth, `mongo_parallel`, has no
//! shared permit ceiling to resize and is out of scope by construction. It was
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
use crate::tuning::{BlindSignal, DecisionCause, Governor};

use super::summary::RunSummary;

/// The journalled reason for one parallelism transition. A step taken while the
/// pressure signal is UNREADABLE is a fail-open, not evidence that pressure
/// eased — labelling it `"source pressure eased: recovered"` wrote a statement
/// about a signal nobody could read into the run journal (bughunt 2026-08-14).
///
/// Pure so the mapping is unit-provable; the `backed off` wording is asserted by
/// the live governor suite, so keep it stable.
fn decision_reason(cause: DecisionCause) -> &'static str {
    match cause {
        DecisionCause::PressureRising => "source pressure rising: backed off",
        DecisionCause::PressureEased => "source pressure eased: recovered",
        DecisionCause::SignalUnreadable => {
            "pressure signal unreadable: failing open toward the ceiling"
        }
    }
}

/// The operator-facing WARN for a blind span — or `None` when there is nothing
/// new to say.
///
/// [`BlindSignal::NeverReadable`] returns `None` on purpose: that state is
/// permanent, it was already reported ONCE and correctly by
/// [`GovernorHarness::spawn_into`]'s up-front probe, and saying it again in the
/// language of a loss ("lost its pressure signal … was pinned at 4 of 4;
/// stepping back toward 4") told the operator a healthy monitoring connection
/// had died — on every run of an engine that simply has no counter, drowning the
/// line that means something real. Reporting-only: the loop fails OPEN either
/// way.
///
/// Pure (returns the string rather than logging it) so both trajectories are
/// RED-provable without a live source.
fn blind_signal_warning(
    kind: BlindSignal,
    export_name: &str,
    frozen_at: usize,
    ceiling: usize,
) -> Option<String> {
    match kind {
        BlindSignal::Lost => Some(format!(
            "export '{export_name}': governor lost its pressure signal (the monitoring \
             connection can no longer sample) — parallelism was pinned at {frozen_at} of \
             {ceiling}; stepping back toward {ceiling}. Set `adaptive: false` to disarm the \
             governor, or `min_parallel` to floor it"
        )),
        BlindSignal::NeverReadable => None,
    }
}

/// Fold the governor's UP-FRONT arm probe into `gov` and return the operator line the probe earns
/// — `Some` exactly when the source could not be sampled at all.
///
/// Two jobs in one function on purpose: the probe's READABILITY is what lets a later blind span be
/// classified ([`BlindSignal`]), and separating the `note_arm_probe` call from the warn is how the
/// wiring gets forgotten. The probe's VALUE is deliberately NOT fed to the decision state — the
/// baseline still comes from the loop's own first sample.
fn arm_probe(
    gov: &mut Governor,
    probe: Option<u64>,
    export_name: &str,
    ceiling: usize,
) -> Option<String> {
    gov.note_arm_probe(probe.is_some());
    probe.is_none().then(|| {
        format!(
            "export '{export_name}': governor armed, but the source provides no pressure signal \
             (engine without a foreign-pressure counter, or the monitor connection cannot sample) \
             — parallelism stays at {ceiling}"
        )
    })
}

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

/// RAII permit for ONE unit of work in a POOL-shaped runner — a fixed set of long-lived workers
/// that claim tasks in a loop (`parallel_checkpoint`), as opposed to the SPAWNER shape
/// (`chunked/exec.rs`) where the parent takes the permit and one thread runs one chunk.
///
/// Why the permit is per TASK and not per worker: the governor sheds by SHRINKING the semaphore's
/// ceiling, and [`Semaphore::resize`] lowers it LAZILY — in-flight permits are honoured to
/// completion. A pool worker that took its permit once at spawn holds it until the run ends, so a
/// shed would resize a ceiling nobody ever re-tests and parallelism would never actually drop. One
/// permit per claimed task is what makes the shed bite, at task granularity — the same shape the
/// keyset runner uses at page granularity.
///
/// Released on EVERY exit path of the loop iteration — `break`, `continue`, an early `return`, or
/// an unwinding panic — which is why it is a guard and not an `acquire()`/`release()` pair.
pub(crate) struct TaskPermit<'a>(&'a Semaphore);

impl<'a> TaskPermit<'a> {
    /// Park until a permit is free, then hold it for the rest of this scope.
    pub(crate) fn acquire(semaphore: &'a Semaphore) -> Self {
        semaphore.acquire();
        Self(semaphore)
    }
}

impl Drop for TaskPermit<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// RAII `finished` accounting for ONE pool worker: bumps the counter
/// [`GovernorHarness::spawn_into`]'s stop predicate reads when the worker's claim loop EXITS
/// (drained queue, claim error, or an unwinding panic).
///
/// The permit half lives in [`TaskPermit`] instead of here — a pool worker's permit is per TASK,
/// so pairing the two the way [`WorkerExit`] does would release a permit the worker no longer
/// holds (an unmatched `release()` underflows the semaphore's count). Same failure mode either way
/// if the bump is skipped: the governor thread's only exit is `finished >= total`, so a missed
/// bump loops it forever and `std::thread::scope` can never join.
pub(crate) struct WorkerFinished<'a>(&'a AtomicUsize);

impl<'a> WorkerFinished<'a> {
    /// Bind at the TOP of the worker closure — before any fallible or panicking work.
    pub(crate) fn new(finished: &'a AtomicUsize) -> Self {
        Self(finished)
    }
}

impl Drop for WorkerFinished<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Does this plan get a governor? Pure so the four ways to get it wrong
/// (`&&`→`||`, `>`→`==`/`<`/`>=`) are unit-provable: the decision itself used to
/// live inline in [`GovernorHarness::arm`], one line above a `create_source`
/// call, so every mutant of it survived the whole offline suite (the first real
/// run of the un-broken mutation gate, 2026-08-14, scored four survivors here).
///
/// `parallel > 1` and not `>= 1`: a single worker has nothing to shed (the floor
/// is 1), so arming there would open a monitoring connection to drive a
/// semaphore that can never move.
fn should_arm(adaptive: bool, parallel: usize) -> bool {
    adaptive && parallel > 1
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
        let monitor: Option<Box<dyn Source>> = if should_arm(plan.tuning.adaptive, parallel) {
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
    /// scope joins. Two things get a `warn` line rather than silence, and each gets exactly ONE:
    /// an armed governor whose very first probe cannot sample (the never-readable engine), and a
    /// signal that DIES mid-run (see the blind-signal callback below — the loop then also steps
    /// back toward the ceiling rather than staying pinned). A never-readable signal does NOT also
    /// get the death notice: see [`blind_signal_warning`].
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
            // first sample. Its READABILITY is handed to the governor
            // (`note_arm_probe`) so a signal this probe could read and the loop
            // then loses is still reported as a LOSS, and one it never read is
            // not reported twice.
            let probe = crate::source::Source::sample_governor_pressure(monitor.as_mut());
            if let Some(msg) = arm_probe(&mut gov, probe, export_name, ceiling) {
                log::warn!("{msg}");
            }
            gov.run(
                &mut monitor,
                || finished.load(Ordering::Relaxed) >= total,
                |from, to, cause| {
                    semaphore.resize(to);
                    let reason = decision_reason(cause);
                    // A shed is a deliberate slowdown of the run — the operator
                    // must SEE it at the default log level (an info-level "this
                    // will be slower" is functionally silent; a field pool run
                    // lost 1h48m to invisible sheds). Recovery stays info.
                    if matches!(cause, DecisionCause::PressureRising) {
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
                |kind, frozen_at, ceiling| {
                    // The pressure signal went blind for GOVERNOR_BLIND_SAMPLES in a row.
                    // Only a signal that WAS readable and stopped is a loss — a monitor
                    // connection reaped by a pooler / server restart / tunnel drop
                    // (`postgres::Client` does not reconnect, so every later sample is
                    // `None`); before this, the run simply stayed pinned at whatever level
                    // the last shed reached, for hours, silently, because the probe above
                    // fires only at tick zero, i.e. exactly when no damage has been done
                    // yet. A signal that was NEVER readable is the probe's story, already
                    // told — `blind_signal_warning` returns `None` for it. Either way the
                    // loop fails OPEN (steps back toward the ceiling): a signal that cannot
                    // be READ is not evidence of pressure — see `GovernorState::observe`.
                    if let Some(msg) = blind_signal_warning(kind, export_name, frozen_at, ceiling) {
                        log::warn!("{msg}");
                    }
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

    /// The two blind trajectories differ in what the OPERATOR is told, and only
    /// one of them is an incident. Before the fix an engine with no
    /// foreign-pressure counter got both lines per export per run — "governor
    /// armed, but the source provides no pressure signal … parallelism stays at
    /// 4" at tick 0, then ~4.5 s later "governor LOST its pressure signal …
    /// parallelism was pinned at 4 of 4; stepping back toward 4", which is
    /// mutually exclusive with the first, describes a step that is a no-op, and
    /// makes a real mid-run monitor death (a reaped pooler connection, a tunnel
    /// drop) indistinguishable from a permanent capability gap.
    ///
    /// Both cases are graded from one call site because this is a mapping: a
    /// single case cannot show that the function DISCRIMINATES.
    #[test]
    fn a_blind_signal_is_only_reported_as_a_loss_when_it_was_ever_readable() {
        let lost = blind_signal_warning(BlindSignal::Lost, "orders", 2, 6)
            .expect("a signal that died mid-run must reach the operator");
        assert!(lost.contains("lost its pressure signal"), "text: {lost}");
        assert!(lost.contains("pinned at 2 of 6"), "text: {lost}");

        assert_eq!(
            blind_signal_warning(BlindSignal::NeverReadable, "orders", 6, 6),
            None,
            "the never-readable engine was already reported ONCE, correctly, by the \
             up-front arm probe — repeating it as a death notice is the false alarm"
        );
    }

    /// A transition taken while the signal is unreadable is a fail-open. It must
    /// not be journalled as `source pressure eased: recovered` — that is a
    /// statement about a signal nobody could read, written into the run
    /// journal's `ParallelismAdjusted` reason. All three causes from one call
    /// site: the mapping is the subject.
    #[test]
    fn a_blind_step_is_journalled_as_failing_open_not_as_pressure_easing() {
        assert_eq!(
            decision_reason(DecisionCause::PressureRising),
            "source pressure rising: backed off",
            "the live governor suite greps `backed off` — keep it stable"
        );
        assert_eq!(
            decision_reason(DecisionCause::PressureEased),
            "source pressure eased: recovered"
        );
        let blind = decision_reason(DecisionCause::SignalUnreadable);
        assert!(
            blind.contains("unreadable"),
            "a blind step must name the unreadable signal, not claim pressure eased: {blind}"
        );
        assert_ne!(blind, decision_reason(DecisionCause::PressureEased));
    }

    /// A sampler that can never read the pressure signal — the shape of an
    /// engine without a foreign-pressure counter AND of a monitor connection
    /// reaped mid-run (`postgres::Client` does not reconnect). Counts its calls
    /// so the loop's stop predicate can fire after a fixed number of samples.
    struct DeadSampler(Arc<AtomicUsize>);

    impl crate::tuning::PressureSource for DeadSampler {
        fn sample(&mut self) -> Option<u64> {
            self.0.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// The arm probe → blind-report seam, driven through the REAL producer
    /// rather than a hand-supplied value: `arm_probe` folds the probe into the
    /// governor, then the loop goes blind and reports.
    ///
    /// This is the boundary the classification actually depends on, and the
    /// obvious fix for this defect (gate the loss on `prev.is_some()`) breaks it
    /// silently: `spawn_into` deliberately does NOT feed the probe's value to
    /// the baseline, so a monitor that answered the probe and died before sample
    /// 1 leaves `prev == None` forever — a real loss, reported as a permanent
    /// capability gap. Both trajectories from one call site: a single case
    /// cannot show that the seam DISCRIMINATES.
    #[test]
    fn the_arm_probe_readability_decides_whether_a_later_blind_span_is_a_loss() {
        for (probe, want_kind) in [
            (Some(7u64), BlindSignal::Lost),
            (None, BlindSignal::NeverReadable),
        ] {
            let mut gov = Governor::with_intervals(
                4,
                2,
                4,
                Duration::from_millis(1),
                Duration::from_millis(1),
            );
            let line = arm_probe(&mut gov, probe, "orders", 4);
            assert_eq!(
                line.is_some(),
                probe.is_none(),
                "the probe's own line fires exactly when the signal is unreadable (probe={probe:?})"
            );
            if let Some(line) = &line {
                assert!(
                    line.contains("provides no pressure signal"),
                    "the live suite greps this wording: {line}"
                );
            }

            let calls = Arc::new(AtomicUsize::new(0));
            let mut source = DeadSampler(Arc::clone(&calls));
            let stop_calls = Arc::clone(&calls);
            let mut reports: Vec<BlindSignal> = Vec::new();
            gov.run(
                &mut source,
                move || stop_calls.load(Ordering::Relaxed) >= 4,
                |_, _, _| {},
                |kind, _, _| reports.push(kind),
            );

            assert_eq!(
                reports,
                vec![want_kind],
                "one report per blindness episode, and its KIND comes from the arm probe \
                 (probe={probe:?})"
            );
        }
    }

    /// The arm decision, all four ways to break it. It lived inline in
    /// [`GovernorHarness::arm`] — one line above a `create_source` call, so no
    /// unit test could reach it and every mutant of it survived (four of the
    /// eighteen survivors of the first non-vacuous mutation run, 2026-08-14).
    ///
    /// Each row is here because it kills a specific mutant, so keep all four:
    /// `(true, 1)` kills `>` → `>=` and `&&` → `||`; `(false, 8)` kills
    /// `&&` → `||` from the other side; `(true, 2)` kills `>` → `<` and
    /// `>` → `==`.
    #[test]
    fn the_governor_arms_only_for_adaptive_with_real_parallelism() {
        assert!(
            should_arm(true, 2),
            "adaptive + parallel 2 is the whole point of the knob"
        );
        assert!(should_arm(true, 8));
        assert!(
            !should_arm(true, 1),
            "one worker has nothing to shed — arming opens a monitoring connection to drive \
             a semaphore that cannot move"
        );
        assert!(
            !should_arm(true, 0),
            "no work at all: `arm` is still called (parallel may be 0), and must not connect"
        );
        assert!(
            !should_arm(false, 8),
            "the user did not opt in — `adaptive: false` must not open a monitoring connection"
        );
        assert!(!should_arm(false, 1));
    }

    /// The POOL shape's shed, end to end through the real guards: a ceiling
    /// shrunk below the pool size must actually cap concurrency, and every task
    /// must still be claimed (a shed is a slowdown, never a stranded task).
    ///
    /// Both regimes from one fixture on purpose. The shrunken run alone cannot
    /// tell a working permit ceiling from an inert fixture that never overlaps
    /// anyway — so the wide run is the activation guard: same pool, same tasks,
    /// ceiling left at the pool size, concurrency must exceed 1.
    ///
    /// The completion oracle is deadline-polled rather than joined because the
    /// mutant that matters most (a [`TaskPermit`] whose `Drop` does not release)
    /// HANGS the pool — a join would turn that into a hung test instead of a red
    /// one.
    #[test]
    fn a_shrunken_permit_ceiling_sheds_pool_workers_without_stranding_a_task() {
        const WORKERS: usize = 3;
        const TASKS: usize = 12;

        for ceiling in [1usize, WORKERS] {
            let sem = Arc::new(Semaphore::new(WORKERS));
            // What the governor does when it sheds: shrink the live ceiling of a
            // pool that is already sized `WORKERS`.
            sem.resize(ceiling);

            let next = Arc::new(AtomicUsize::new(0));
            let done = Arc::new(AtomicUsize::new(0));
            let live = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let finished = Arc::new(AtomicUsize::new(0));

            for _ in 0..WORKERS {
                let (sem, next, done, live, peak, finished) = (
                    Arc::clone(&sem),
                    Arc::clone(&next),
                    Arc::clone(&done),
                    Arc::clone(&live),
                    Arc::clone(&peak),
                    Arc::clone(&finished),
                );
                std::thread::spawn(move || {
                    let _finish = WorkerFinished::new(&finished);
                    loop {
                        // Exactly the runner's shape: permit first, then claim.
                        let _permit = TaskPermit::acquire(&sem);
                        if next.fetch_add(1, Ordering::Relaxed) >= TASKS {
                            break;
                        }
                        let now = live.fetch_add(1, Ordering::Relaxed) + 1;
                        peak.fetch_max(now, Ordering::Relaxed);
                        std::thread::sleep(Duration::from_millis(10));
                        live.fetch_sub(1, Ordering::Relaxed);
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }

            let deadline = Instant::now() + Duration::from_secs(10);
            while done.load(Ordering::Relaxed) < TASKS && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(
                done.load(Ordering::Relaxed),
                TASKS,
                "every task must still be claimed at ceiling {ceiling} — a shed slows the run \
                 down, it must never strand work (a permit that is not returned parks the pool \
                 forever)"
            );

            let observed = peak.load(Ordering::Relaxed);
            if ceiling == 1 {
                assert_eq!(
                    observed, 1,
                    "the governor shrank the ceiling to 1, so at most one task may run at a \
                     time; observed {observed} concurrent"
                );
            } else {
                assert!(
                    observed > 1,
                    "activation guard: at the full ceiling this pool MUST overlap, or the \
                     shrunken half above proves nothing; observed {observed} concurrent"
                );
            }

            let deadline = Instant::now() + Duration::from_secs(5);
            while finished.load(Ordering::Relaxed) < WORKERS && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(
                finished.load(Ordering::Relaxed),
                WORKERS,
                "every pool worker must count itself finished — the governor thread's only \
                 exit is `finished >= total`"
            );
        }
    }

    /// A task that UNWINDS must still return its permit and, when the worker
    /// dies with it, still count as finished. Two iterations because both
    /// subjects accumulate: with one task a guard that releases only on the
    /// happy path is indistinguishable from a correct one.
    ///
    /// The panic is expected and its message is printed by the test harness.
    #[test]
    fn a_panicking_pool_task_returns_its_permit_and_counts_its_worker() {
        let sem = Arc::new(Semaphore::new(2));
        let finished = AtomicUsize::new(0);

        for panics in [true, false] {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _finish = WorkerFinished::new(&finished);
                let _permit = TaskPermit::acquire(&sem);
                if panics {
                    panic!("rivet test: injected pool-task panic (expected)");
                }
            }));
            assert_eq!(outcome.is_err(), panics, "task outcome (panics={panics})");
        }

        assert_eq!(
            finished.load(Ordering::Relaxed),
            2,
            "an UNWINDING pool worker must still count as finished, or the governor loops \
             forever and `thread::scope` never joins"
        );
        assert!(
            acquires_within(&sem, 2, Duration::from_secs(5)),
            "both permits must return — a permit leaked by a panicking task parks the pool"
        );
    }

    /// Call-site pin: the CHECKPOINT parallel chunked runner is governed.
    ///
    /// Honest about its strength — this greps source text, and the real oracle
    /// is a live shed test (`governor_backs_off_under_concurrent_write_pressure`
    /// grades `chunked/exec.rs`; there is no checkpoint-runner twin yet, and
    /// this runner needs a real source + a state DB, so no unit test can drive
    /// it). What it pins is the WIRING, which is exactly what was missing: the
    /// runner shipped with no governor at all, so `tuning.adaptive: true` was a
    /// silent no-op on the `chunk_checkpoint: true` + `parallel: N` shape
    /// `rivet init` scaffolds — and `plan.strategy.is_resumable()` is the only
    /// thing that routes a config to one runner or the other (job.rs:1042).
    /// RED against deleting any of the four calls.
    #[test]
    fn the_parallel_checkpoint_runner_arms_spawns_and_drains_the_governor() {
        let src = include_str!("chunked/parallel_checkpoint.rs");
        assert!(
            src.contains("GovernorHarness::arm(plan, parallel)"),
            "the resumable chunked runner must arm the governor — without it `adaptive: true` \
             is silent on the shape `rivet init` scaffolds"
        );
        assert!(
            src.contains("governor.spawn_into(s, &semaphore, &finished, parallel,"),
            "arming without spawning never resizes anything; `total` is the POOL SIZE here \
             (workers that exit), not the task count"
        );
        assert!(
            src.contains("governor.drain_into(summary);"),
            "the ParallelismAdjusted events must reach the run journal"
        );
        assert!(
            src.contains("TaskPermit::acquire(semaphore)"),
            "a pool worker must take its permit PER TASK — one permit held for the worker's \
             whole life is a ceiling the governor can shrink with no effect"
        );
        let drain = src
            .find("governor.drain_into(summary);")
            .expect("drain call site");
        let bail = src
            .find("parallel checkpoint worker errors")
            .expect("worker-error bail");
        assert!(
            drain < bail,
            "drain BEFORE the error bail, or a failed run journals none of its governor \
             decisions — the drift the keyset copy re-introduced once"
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
