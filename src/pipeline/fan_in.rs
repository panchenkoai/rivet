//! Committing a unit, in ONE fixed order (ADR-0017): what it SAW, every part it made
//! durable (`record_part`, on the failure path too), then — only once the unit
//! committed — its checksums, the `validate` verdict and the runner's checkpoint.
//! Two adapters share that order: [`commit_unit`] inline for the sequential runners,
//! [`FanIn`] collected from any thread and drained on the parent for the parallel
//! ones. Work distribution (spawner, pool, per-range) stays with each runner; this
//! module owns only the tail that kept shipping bugs when each runner wrote it (a
//! bail above the part drain, observations dropped below the bail, a panicking
//! worker skipping the drain, guards re-implemented).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::thread::Scope;

use super::commit::{Observations, PartKind, PartRecord, UnitChecksums, UnitId, record_part};
use super::governor::{GovernorHarness, WorkerFinished};
use super::summary::RunSummary;
use crate::error::Result;
use crate::journal::RunEvent;
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

    /// Record `result`'s error as a unit failure instead of dropping it.
    pub(crate) fn fail_on_err<T>(&self, label: &str, result: anyhow::Result<T>) {
        if let Err(e) = result {
            self.fail(label, format!("{e:#}"));
        }
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
    /// bail if any unit failed — so nothing a worker made durable is lost to an error — and
    /// only on a clean drain the `validate` verdict.
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
        record_units(
            plan,
            summary,
            file_log,
            inner(self.observed),
            inner(self.parts),
            kind,
            inner(self.committed),
        );
        let errors = inner(self.errors);
        if errors.is_empty() {
            mark_validated(plan, summary);
            Ok(())
        } else {
            Err(on_err(&errors))
        }
    }
}

/// Commit one unit inline: observations, every durable part, then — only if `outcome` is
/// `Ok` — its checksums, the empty-chunk journal entry, the `validate` verdict and `checkpoint`.
#[allow(clippy::too_many_arguments)] // the unit's identity, output and checkpoint are the arity
pub(crate) fn commit_unit(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    file_log: Option<&StateStore>,
    unit: UnitId,
    parts: Vec<PartRecord>,
    kind: impl Fn(usize) -> PartKind,
    observed: Observations,
    outcome: Result<UnitChecksums>,
    checkpoint: impl FnOnce(&RunSummary) -> Result<()>,
) -> Result<()> {
    let empty = parts.is_empty();
    let (committed, failed) = match outcome {
        Ok(c) => (Some((unit, c)), None),
        Err(e) => (None, Some(e)),
    };
    record_units(
        plan,
        summary,
        file_log,
        observed,
        parts.into_iter().map(|p| (unit, p)),
        |i, _| kind(i),
        committed,
    );
    if let Some(e) = failed {
        return Err(e);
    }
    if !empty {
        mark_validated(plan, summary);
    } else if let UnitId::Chunk(chunk_index) = unit {
        summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index,
            rows: 0,
            file_name: None,
        });
    }
    checkpoint(summary)
}

/// The shared order: observations, every durable part through `record_part`, then committed checksums.
fn record_units(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    file_log: Option<&StateStore>,
    observed: Observations,
    parts: impl IntoIterator<Item = (UnitId, PartRecord)>,
    kind: impl Fn(usize, UnitId) -> PartKind,
    committed: impl IntoIterator<Item = (UnitId, UnitChecksums)>,
) {
    summary.ledger.observe(observed);
    for (i, (unit, rec)) in parts.into_iter().enumerate() {
        record_part(plan, summary, file_log, &rec, kind(i, unit), unit);
    }
    for (unit, c) in committed {
        summary.ledger.contribute(unit, c);
    }
}

/// Under `validate`, record the pass once: the verdict and its journal entry.
fn mark_validated(plan: &ResolvedRunPlan, summary: &mut RunSummary) {
    if plan.validate && summary.validated != Some(true) {
        summary.validated = Some(true);
        summary
            .journal
            .record(RunEvent::ValidationResult { passed: true });
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
    fn a_failed_state_write_fails_the_run_and_a_successful_one_does_not() {
        let fan = FanIn::default();
        fan.fail_on_err("chunk 2 state", Ok(()));
        fan.fail_on_err::<()>("chunk 3 state", Err(anyhow::anyhow!("database is locked")));
        assert_eq!(
            inner(fan.errors),
            vec!["chunk 3 state: database is locked".to_string()]
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

    /// A run whose unit failed never reaches the `validate` verdict, even with every part written.
    #[test]
    fn a_failed_unit_leaves_the_validate_verdict_unreached() {
        let mut plan = test_plan();
        plan.validate = true;
        let mut summary = test_summary(&plan);
        let fan = FanIn::default();
        fan.part(UnitId::Chunk(0), synthetic_parts(1).remove(0));
        fan.fail("chunk 1", "part validation failed");
        assert!(
            fan.finish(&plan, &mut summary, None, None, chunk_kind, bail)
                .is_err()
        );
        assert_eq!(summary.validated, None);
    }

    /// A clean drain under `validate` records the pass; without `validate` it records nothing.
    #[test]
    fn a_clean_drain_records_the_validate_verdict_only_when_asked() {
        for validate in [true, false] {
            let mut plan = test_plan();
            plan.validate = validate;
            let mut summary = test_summary(&plan);
            let fan = FanIn::default();
            fan.part(UnitId::Chunk(0), synthetic_parts(1).remove(0));
            fan.finish(&plan, &mut summary, None, None, chunk_kind, bail)
                .unwrap();
            assert_eq!(
                summary.validated,
                validate.then_some(true),
                "validate={validate}"
            );
        }
    }

    fn validations(summary: &RunSummary) -> usize {
        summary
            .journal
            .entries
            .iter()
            .filter(|e| matches!(e.event, RunEvent::ValidationResult { .. }))
            .count()
    }

    /// The checkpoint runs only after every part of the unit is recorded, then the verdict is journaled once.
    #[test]
    fn a_committed_unit_checkpoints_after_its_parts_are_recorded() {
        let mut plan = test_plan();
        plan.validate = true;
        let mut summary = test_summary(&plan);
        for n in 0..2 {
            let parts = synthetic_parts(2)
                .into_iter()
                .map(|mut p| {
                    p.file_name = format!("u{n}_{}", p.file_name);
                    p
                })
                .collect();
            commit_unit(
                &plan,
                &mut summary,
                None,
                UnitId::Chunk(n),
                parts,
                |_| PartKind::Chunk { chunk_index: 0 },
                Observations::default(),
                Ok(UnitChecksums::default()),
                |s| {
                    assert_eq!(s.files_committed % 2, 0, "checkpoint before a part");
                    Ok(())
                },
            )
            .unwrap();
        }
        assert_eq!(summary.files_committed, 4);
        assert_eq!(summary.validated, Some(true));
        assert_eq!(validations(&summary), 1, "the verdict is journaled once");
        assert!(
            summary
                .ledger
                .integrity
                .covered_units
                .contains(&UnitId::Chunk(1))
        );
    }

    /// A failed unit records its durable parts and what it saw, then returns its own error: no checksums, no verdict, no checkpoint.
    #[test]
    fn a_failed_unit_records_its_parts_and_skips_the_checkpoint() {
        let mut plan = test_plan();
        plan.validate = true;
        let mut summary = test_summary(&plan);
        let mut o = Observations::default();
        o.column_max_bytes.insert("name".into(), 7);
        let err = commit_unit(
            &plan,
            &mut summary,
            None,
            UnitId::Page(3),
            synthetic_parts(1),
            |_| PartKind::Chunk { chunk_index: 3 },
            o,
            Err(anyhow::anyhow!("part 1 failed")),
            |_| panic!("a failed unit must not checkpoint"),
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "part 1 failed");
        assert_eq!(summary.files_committed, 1);
        assert_eq!(summary.validated, None);
        assert!(summary.ledger.integrity.covered_units.is_empty());
        assert_eq!(
            summary.ledger.observed.column_max_bytes.get("name"),
            Some(&7)
        );
    }

    /// An empty chunk journals its completion and checkpoints without a verdict; an empty non-chunk unit journals nothing.
    #[test]
    fn an_empty_unit_checkpoints_and_only_a_chunk_journals_its_completion() {
        for (unit, journaled) in [(UnitId::Chunk(5), 1), (UnitId::Run, 0)] {
            let mut plan = test_plan();
            plan.validate = true;
            let mut summary = test_summary(&plan);
            let mut checkpointed = false;
            commit_unit(
                &plan,
                &mut summary,
                None,
                unit,
                Vec::new(),
                |_| PartKind::Chunk { chunk_index: 5 },
                Observations::default(),
                Ok(UnitChecksums::default()),
                |_| {
                    checkpointed = true;
                    Ok(())
                },
            )
            .unwrap();
            let completed = summary
                .journal
                .entries
                .iter()
                .filter(|e| {
                    matches!(
                        e.event,
                        RunEvent::ChunkCompleted {
                            chunk_index: 5,
                            rows: 0,
                            file_name: None
                        }
                    )
                })
                .count();
            assert_eq!((completed, checkpointed), (journaled, true), "{unit:?}");
            assert_eq!(
                summary.validated, None,
                "{unit:?}: an empty unit validated nothing"
            );
        }
    }
}
