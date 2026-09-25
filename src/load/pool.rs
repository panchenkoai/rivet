//! Pool policy for the per-table `load` / `compact` loops: the ceiling warning and when a lost ledger is fatal. The executor itself is [`crate::workers::run_workers`].
//!
//! WHY THIS IS SAFE TO RUN CONCURRENTLY, and what that rests on. Unlike the export
//! pool (`pipeline/run.rs`), which serialises heavy exports because they contend for
//! ONE source database, load work is disjoint by construction: each plan owns its own
//! warehouse object and its own destination sub-prefix, and each table is guarded by a
//! per-table lease. There is therefore no `parallel_safe` notion here, and none is
//! missing.
//!
//! That disjointness is not free, and it is held up on ONE side only. The warehouse
//! OBJECT is guarded: `plan::reject_duplicate_target_tables` refuses at PLAN time two
//! exports resolving to one table (and, for non-`Full` modes, to one
//! `<table>__changes` buffer). Remove or weaken that refusal — or add a mode where
//! several exports deliberately share one object — and this pool turns a formerly
//! sequential success into an intermittent "lease is held", decided by whichever
//! worker got there first. There is no eligibility hook here to express that; it
//! would need one, or a pre-split of `plans`.
//!
//! The destination PREFIX is NOT guarded, and the difference matters because
//! `cleanup_target` runs OUTSIDE the per-table lease in all four load paths.
//! `reject_duplicate_target_tables` compares warehouse objects and never looks at
//! `gcs_prefix`, and `plan::resolve_load_prefix` passes the operator's literal
//! through — it expands `{export}`/`{table}` when written and refuses only the
//! day-specific and run-specific tokens. Two exports with different `table:` and a
//! hand-written IDENTICAL prefix therefore pass the plan-time refusal, share a
//! folder, and can be cleaned concurrently. What keeps this off the floor today is
//! `rivet init`, which writes a per-table `exports/<segment>/` prefix
//! (`yaml_scaffold::table_export_prefix`), so a GENERATED config cannot collide —
//! a hand-edited one can.
//!
//! Split out of [`super::orchestrate`] so the SCHEDULING is graded: `run_loads`
//! is a live-only body (its whole-function mutants are excluded — nothing in an
//! offline run drives a warehouse), and a live-only body may not DECIDE. The
//! executor's contracts — every item runs once, a failure isolates, results come
//! back in item order — are proven in `crate::workers` against a fake closure.

pub(crate) use crate::workers::{MAX_POOL, effective_pool, run_workers};

/// What to tell an operator who asked for more workers than the ceiling allows.
///
/// Separate from [`effective_pool`] so the DECISION is graded while the printing
/// stays glue, and pure so both can be tested without a warehouse.
///
/// It names BOTH bounds on purpose. The warehouse side is the obvious one, but the
/// STATE backend runs out first and reports an error about a database the operator
/// was not thinking about: every worker opens its own ledger connection, so N is
/// also N connections. On Postgres that meets `max_connections` (100 by default,
/// minus whatever else is connected and the superuser reserve) and fails with
/// "sorry, too many clients already" — which rivet classifies as RETRYABLE, so the
/// run would retry a condition that waiting cannot improve. On SQLite it is not
/// connections but writers: WAL allows many readers and ONE writer, so the workers
/// queue on the write lock and surface `SQLITE_BUSY` once `busy_timeout` (10s) is
/// spent.
pub(crate) fn pool_ceiling_warning(
    requested: Option<usize>,
    items: usize,
    ledger: LedgerKind,
) -> Option<String> {
    let asked = requested?;
    if asked <= MAX_POOL {
        return None;
    }
    let running = effective_pool(requested, items);
    if ledger == LedgerKind::Absent {
        // No ledger at all — the parent's own open failed, so this run opens ZERO
        // state connections and neither backend bound applies. Quoting Postgres
        // `max_connections` here (which a two-valued flag did, by folding "absent"
        // into "not SQLite") points at a database the run will never touch.
        return Some(format!(
            "--pool {asked} exceeds the ceiling of {MAX_POOL}; running {running} worker(s). \
             This run has NO state ledger — its open failed above — so no ledger bound \
             applies; what remains is the warehouse's own: BigQuery allows 100 concurrent \
             interactive queries per PROJECT, shared with everything else running there."
        ));
    }
    if ledger == LedgerKind::Sqlite {
        // Said plainly, because on SQLite the limit is not a quota that could be
        // raised — it is the storage engine. Telling an operator "capped" without
        // telling them WHY, or what to do instead, invites them to keep raising a
        // number that cannot help.
        return Some(format!(
            "--pool {asked} exceeds the ceiling of {MAX_POOL}; running {running} worker(s). \
             On a SQLite ledger rivet cannot usefully go higher in any case: WAL gives many \
             readers but exactly ONE writer, so workers queue on the write lock and stop \
             gaining past that point. The ceiling is {MAX_POOL} on every backend: a Postgres \
             state store removes the single writer, not the ceiling."
        ));
    }
    Some(format!(
        "--pool {asked} exceeds the ceiling of {MAX_POOL}; running {running} worker(s). \
         Each worker opens its own ledger connection, so N is also N connections to the \
         Postgres state backend and counts against its `max_connections` (100 by default, \
         minus the superuser reserve and whatever else is connected). The warehouse has its \
         own budget as well: BigQuery allows 100 concurrent interactive queries per PROJECT, \
         shared with everything else running there."
    ))
}

/// Which ledger the workers will open — including having NONE.
///
/// Three-valued on purpose. A `bool` for "is it SQLite" flattens the case where the
/// PARENT's own open failed: `state_ref` is then `None`, the flag reads `false`, and
/// the warning quotes Postgres `max_connections` at a run that will not open a single
/// ledger connection. That is the same absent-vs-errored flattening the tri-state
/// [`crate::load::orchestrate`] already fixed one layer up for the re-baseline guard.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum LedgerKind {
    Sqlite,
    Postgres,
    Absent,
}

/// Whether a worker that has no ledger must REFUSE its table rather than load it.
///
/// The sequential loop opened the state store ONCE, before any table, so the ledger
/// was either present for every table or absent for all of them — a whole-run
/// degradation the operator was warned about once. A pool opens one store per
/// WORKER, which invents a third state the load path never had: some tables with a
/// ledger and some without, in the same run.
///
/// That state is not a degradation, it is a per-table failure, and it must not pass
/// silently: a ledger-less worker takes NO lease (the lease is `state.map(..)`, so
/// `None` skips it rather than refusing), reads an empty skip set, loads every run
/// and records nothing — so the next run appends them again.
pub(crate) fn reconnect_failure_is_fatal(parent_had_state: bool, worker_has_state: bool) -> bool {
    parent_had_state && !worker_has_state
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ceiling warning fires only when the ASK exceeds it, and names both bounds.
    ///
    /// The boundary cases are the point: `MAX_POOL` itself must stay silent (it is
    /// allowed, not excessive) and `MAX_POOL + 1` must speak. A test that only
    /// checked "big number warns" would pass with the comparison flipped either way.
    #[test]
    fn the_ceiling_warns_only_above_it_and_says_what_to_do_per_backend() {
        assert!(
            pool_ceiling_warning(None, 50, LedgerKind::Postgres).is_none(),
            "no request, no warning"
        );
        assert!(
            pool_ceiling_warning(Some(MAX_POOL), 500, LedgerKind::Postgres).is_none(),
            "asking for exactly the ceiling is allowed, not excessive"
        );

        // No ledger at all: neither backend bound applies, and naming one points the
        // operator at a database this run will never open. The two-valued flag this
        // replaced folded `Absent` into the Postgres arm.
        let none = pool_ceiling_warning(Some(MAX_POOL + 1), 500, LedgerKind::Absent)
            .expect("the ceiling still binds without a ledger");
        assert!(
            none.contains("NO state ledger") && !none.contains("max_connections"),
            "a run with no ledger must not be sent to a state DB's limits: {none}"
        );
        assert!(
            none.contains("BigQuery"),
            "the warehouse budget is the one that still applies: {none}"
        );

        let pg = pool_ceiling_warning(Some(MAX_POOL + 1), 500, LedgerKind::Postgres)
            .expect("one over the ceiling must warn");
        assert!(
            pg.contains(&(MAX_POOL + 1).to_string()),
            "the warning must quote what was asked: {pg}"
        );
        assert!(
            pg.contains("max_connections") && pg.contains("BigQuery"),
            "on Postgres it must name BOTH bounds — the state backend runs out first, and \
             the error arrives from a database the operator was not thinking about: {pg}"
        );

        let lite = pool_ceiling_warning(Some(MAX_POOL + 1), 500, LedgerKind::Sqlite)
            .expect("the SQLite ledger must warn too");
        assert!(
            lite.contains(&format!("The ceiling is {MAX_POOL} on every backend"))
                && !lite.contains("RIVET_STATE_URL"),
            "moving the state to Postgres buys no workers past the ceiling: {lite}"
        );
        assert!(
            lite.contains("ONE writer"),
            "on SQLite the limit is the storage engine, not a quota — say so: {lite}"
        );
    }

    /// Only a worker that lost a ledger the RUN started with is fatal.
    ///
    /// All four combinations, because the defect this guards is the MIXED state a
    /// pool invents: the sequential loop opened one store before any table, so
    /// "some tables with a ledger, some without" could not happen and nothing
    /// downstream was written to notice it.
    #[test]
    fn only_a_worker_that_lost_a_ledger_the_run_had_is_fatal() {
        assert!(
            reconnect_failure_is_fatal(true, false),
            "the run had a ledger and this worker lost it — that is THIS table's failure"
        );
        assert!(
            !reconnect_failure_is_fatal(true, true),
            "both have a ledger — ordinary work"
        );
        assert!(
            !reconnect_failure_is_fatal(false, false),
            "the run never had a ledger — the documented stateless degradation, warned once"
        );
        assert!(
            !reconnect_failure_is_fatal(false, true),
            "no parent ledger yet the worker opened one — not a failure, and not a state \
             the caller can produce; pinned so a flipped operator cannot hide here"
        );
    }
}
