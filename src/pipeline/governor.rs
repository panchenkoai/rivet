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
    /// not completed, so a failing worker can't strand it and deadlock the scope. Decisions are
    /// buffered in `self.log` (the journal is not thread-shared) and recorded by
    /// [`drain_into`](Self::drain_into) after the scope joins.
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
