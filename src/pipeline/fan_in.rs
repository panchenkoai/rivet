//! The fan-in of a parallel runner: what its workers made durable, what they saw,
//! what committed and what failed — collected from any thread and drained on the
//! parent in ONE fixed order. Work distribution (spawner, pool, per-range) stays
//! with each runner; this module owns only the tail that kept shipping bugs when
//! four runners each wrote it (a bail above the part drain, observations dropped
//! below the bail, a panicking worker skipping the drain, guards re-implemented).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::thread::Scope;

use super::commit::{Observations, PartKind, PartRecord, UnitChecksums, UnitId, record_part};
use super::governor::{GovernorHarness, WorkerFinished};
use super::summary::RunSummary;
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

/// Collects a parallel runner's worker output; [`FanIn::finish`] drains it.
#[derive(Default)]
pub(crate) struct FanIn {
    parts: Mutex<Vec<(UnitId, PartRecord)>>,
    observed: Mutex<Observations>,
    committed: Mutex<Vec<(UnitId, UnitChecksums)>>,
    errors: Mutex<Vec<String>>,
    finished: AtomicUsize,
}

fn locked<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn inner<T>(m: Mutex<T>) -> T {
    m.into_inner().unwrap_or_else(|e| e.into_inner())
}

impl FanIn {
    /// The count the governor's exit predicate waits on: bumped once per [`Self::spawn`], panic included.
    pub(crate) fn finished(&self) -> &AtomicUsize {
        &self.finished
    }

    /// A part is durable at the destination: it is recorded even if its unit later fails.
    pub(crate) fn part(&self, unit: UnitId, rec: PartRecord) {
        locked(&self.parts).push((unit, rec));
    }

    /// What a worker SAW (ADR-0029): applied on the failure path too.
    pub(crate) fn observe(&self, o: Observations) {
        locked(&self.observed).merge(o);
    }

    /// A unit COMMITTED: its checksums enter the ledger under that unit.
    pub(crate) fn contribute(&self, unit: UnitId, c: UnitChecksums) {
        locked(&self.committed).push((unit, c));
    }

    /// A unit failed; the run fails after the drain.
    pub(crate) fn fail(&self, label: &str, msg: impl std::fmt::Display) {
        log::error!("{label} failed: {msg}");
        locked(&self.errors).push(format!("{label}: {msg}"));
    }

    /// Run `body` on a scoped thread: counted as finished on every exit, an `Err` or a panic recorded as a failure.
    pub(crate) fn spawn<'s, 'e>(
        &'s self,
        scope: &'s Scope<'s, 'e>,
        label: String,
        body: impl FnOnce() -> Result<()> + Send + 's,
    ) {
        scope.spawn(move || {
            let _finished = WorkerFinished::new(&self.finished);
            match catch_unwind(AssertUnwindSafe(body)) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => self.fail(&label, format!("{e:#}")),
                Err(payload) => self.fail(
                    &label,
                    format!("worker panicked: {}", panic_text(&*payload)),
                ),
            }
        });
    }

    /// Drain on the parent, in this order: governor log, observations, every durable part
    /// (`record_part`, `file_log` per ADR-0017), every committed unit's checksums, then the
    /// bail if any unit failed — so nothing a worker made durable is lost to an error.
    pub(crate) fn finish(
        self,
        plan: &ResolvedRunPlan,
        summary: &mut RunSummary,
        file_log: Option<&StateStore>,
        governor: Option<GovernorHarness>,
        kind: impl Fn(usize, UnitId) -> PartKind,
        on_err: impl FnOnce(&[String]) -> anyhow::Error,
    ) -> Result<()> {
        if let Some(g) = governor {
            g.drain_into(summary);
        }
        summary.ledger.observe(inner(self.observed));
        for (i, (unit, rec)) in inner(self.parts).into_iter().enumerate() {
            record_part(plan, summary, file_log, &rec, kind(i, unit), unit);
        }
        for (unit, c) in inner(self.committed) {
            summary.ledger.contribute(unit, c);
        }
        let errors = inner(self.errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(on_err(&errors))
        }
    }
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::commit::tests::{synthetic_parts, test_plan, test_summary};
    use std::sync::atomic::Ordering;

    fn chunk_kind(_: usize, u: UnitId) -> PartKind {
        match u {
            UnitId::Chunk(i) => PartKind::Chunk { chunk_index: i },
            _ => PartKind::Chunk { chunk_index: -1 },
        }
    }

    fn bail(errs: &[String]) -> anyhow::Error {
        anyhow::anyhow!("failed: {}", errs.join("; "))
    }

    #[test]
    fn a_unit_that_fails_after_a_durable_part_still_records_it_then_bails() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let fan = FanIn::default();
        let part = synthetic_parts(1).remove(0);
        std::thread::scope(|s| {
            fan.spawn(s, "chunk 0".into(), || {
                fan.part(UnitId::Chunk(0), part);
                anyhow::bail!("boom")
            });
        });
        let err = fan
            .finish(&plan, &mut summary, None, None, chunk_kind, bail)
            .unwrap_err();
        assert_eq!(err.to_string(), "failed: chunk 0: boom");
        assert_eq!(
            (summary.files_committed, summary.total_rows),
            (1, 100),
            "the durable part is counted"
        );
    }

    #[test]
    fn a_panicking_worker_is_counted_finished_and_fails_the_run_instead_of_the_process() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let fan = FanIn::default();
        let part = synthetic_parts(1).remove(0);
        std::thread::scope(|s| {
            fan.spawn(s, "range 1".into(), || {
                fan.part(UnitId::Chunk(1), part);
                panic!("kaput")
            });
            fan.spawn(s, "range 2".into(), || Ok(()));
        });
        assert_eq!(
            fan.finished().load(Ordering::Relaxed),
            2,
            "the governor's exit count sees both"
        );
        let err = fan
            .finish(&plan, &mut summary, None, None, chunk_kind, bail)
            .unwrap_err();
        assert_eq!(err.to_string(), "failed: range 1: worker panicked: kaput");
        assert_eq!(
            summary.files_committed, 1,
            "a part written before the panic is still counted"
        );
    }

    #[test]
    fn observations_reach_the_ledger_on_the_failure_path() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let fan = FanIn::default();
        let mut o = Observations::default();
        o.column_max_bytes.insert("name".into(), 42);
        fan.observe(o);
        fan.fail("chunk 3", "boom");
        assert!(
            fan.finish(&plan, &mut summary, None, None, chunk_kind, bail)
                .is_err()
        );
        assert_eq!(
            summary.ledger.observed.column_max_bytes.get("name"),
            Some(&42)
        );
    }

    #[test]
    fn a_committed_unit_covers_its_parts_and_a_clean_run_returns_ok() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let fan = FanIn::default();
        for (i, p) in synthetic_parts(2).into_iter().enumerate() {
            fan.part(UnitId::Chunk(i as i64), p);
            fan.contribute(UnitId::Chunk(i as i64), UnitChecksums::default());
        }
        fan.finish(&plan, &mut summary, None, None, chunk_kind, bail)
            .unwrap();
        assert_eq!(summary.manifest_parts.len(), 2);
        let covered = &summary.ledger.integrity.covered_units;
        assert!(covered.contains(&UnitId::Chunk(0)) && covered.contains(&UnitId::Chunk(1)));
    }
}
