//! Bounded work-stealing executor for the per-table `load` / `compact` loops.
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
//! pool is generic over the work item, so its contracts — every item runs once,
//! a failure isolates, results come back in item order — are proven here against
//! a fake closure, with no warehouse, no credentials and no state DB.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The hard ceiling on worker threads.
///
/// Not the warehouse quota. BigQuery allows 100 concurrent interactive queries per
/// PROJECT (rivet's loads are query jobs — `statement_type = LOAD_DATA`, measured in
/// the job ledger), but that budget is shared with everything else in the project,
/// and the STATE backend runs out long before it: every worker opens its own ledger
/// connection. 16 keeps a default deployment well inside both — a Postgres state DB
/// at the stock `max_connections` of 100, and a SQLite one where the writers
/// serialise anyway.
pub(crate) const MAX_POOL: usize = 16;

/// Workers when the operator passes no `--pool` at all.
///
/// A LITERAL, not `MAX_POOL`, on purpose: the ceiling and the default are two
/// decisions that happen to agree today, and writing the default as the ceiling
/// would make any future raise of the ceiling silently raise what every unflagged
/// run does. Defaulting this high was measured first — 16 workers over 16 tables
/// on the default SQLite ledger lost no table and surfaced no lock error.
pub(crate) const DEFAULT_POOL: usize = 16;

/// Worker threads to run `items` on: at least one, never more than there is work,
/// never more than [`MAX_POOL`].
///
/// `None` is [`DEFAULT_POOL`]: the flag is an OVERRIDE, not an opt-in. `--pool 1`
/// is the strictly sequential pass the loop made before the pool existed, and the
/// CLI help promises exactly that, so it is pinned by a test.
pub(crate) fn effective_pool(requested: Option<usize>, items: usize) -> usize {
    // The upper bound is computed first: whichever is smaller, the work available or
    // the ceiling — and never below 1, so the clamp below cannot invert.
    let ceiling = items.clamp(1, MAX_POOL);
    requested.unwrap_or(DEFAULT_POOL).clamp(1, ceiling)
}

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
             gaining past that point. If you want more parallelism than this, move the state \
             to Postgres (set RIVET_STATE_URL) — there the bound is `max_connections`, not a \
             single writer."
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

/// The next unclaimed item, or `None` once the queue is drained.
fn take_next<'a, T>(next: &AtomicUsize, items: &'a [T]) -> Option<(usize, &'a T)> {
    let i = next.fetch_add(1, Ordering::Relaxed);
    items.get(i).map(|item| (i, item))
}

/// Run `work` over every item on `workers` threads, returning one result per
/// item **in item order** regardless of which worker finished when.
///
/// `init` runs once per worker thread, not once per item: the per-worker
/// resource (a load's own `StateStore`) is opened as many times as there are
/// workers — so `--pool 1` opens exactly one, as the sequential loop always did,
/// and the [`DEFAULT_POOL`] of 16 opens up to sixteen. Per WORKER, not per item,
/// is the guarantee; it is a ceiling on connections, not a promise of one.
///
/// Fault isolation is the contract this exists to keep: a failing item is
/// recorded and the worker takes the next one, so one poisoned table can never
/// abandon the tables behind it in the queue.
pub(crate) fn run_workers<T, W, E, I, F, P>(
    items: &[T],
    workers: usize,
    init: I,
    work: F,
    on_lost: P,
) -> Vec<Result<(), E>>
where
    T: Sync,
    E: Send,
    I: Fn() -> Option<W> + Sync,
    F: Fn(&W, usize, &T) -> Result<(), E> + Sync,
    P: Fn(&T) -> E + Sync,
{
    let next = AtomicUsize::new(0);
    let done: Mutex<Vec<(usize, Result<(), E>)>> = Mutex::new(Vec::with_capacity(items.len()));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                // `init` is caught TOO, not just `work`. It opens a state store, so
                // it is not panic-free by construction, and a panic here unwinds
                // straight through `thread::scope` — discarding every result the
                // other workers had already recorded, which is the exact loss the
                // catch below exists to prevent. This worker then simply retires;
                // its share of the queue is taken by the survivors, and if EVERY
                // worker retires the fill-in after the join still answers for each
                // item rather than returning a short vector.
                let Ok(Some(resource)) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(&init))
                else {
                    return;
                };
                while let Some((i, item)) = take_next(&next, items) {
                    // A PANIC in one item must not discard what the others already
                    // did. Unwinding out of `thread::scope` skips the fold entirely,
                    // so the run would exit without reporting the tables that had
                    // already succeeded — after they had already changed the
                    // warehouse. Caught here it becomes an ORDINARY failure for that
                    // item: same fold, same aggregate, same isolation. It is not a
                    // separate channel, because a separate channel is how it would
                    // get lost a second time.
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        work(&resource, i, item)
                    }))
                    .unwrap_or_else(|_| Err(on_lost(item)));
                    done.lock().unwrap().push((i, outcome));
                }
            });
        }
    });
    let mut out = done.into_inner().unwrap();
    // EVERY item gets an answer, even one no worker ever reached — a `init` panic
    // that retires a worker (or all of them) must not silently return a SHORTER
    // vector than the caller handed in: the caller folds these into the run's
    // failures, so a missing entry reads as a table that quietly succeeded.
    // Unconditional, with no `out.len() < items.len()` guard in front: the loop is
    // already a no-op when nothing is missing, and the guard was a DECISION whose
    // `<`/`<=` forms cannot be told apart — `out.len()` never exceeds `items.len()`
    // (each index is pushed at most once), so the two differ only where the body
    // does nothing. Deleting it is better than excusing it in `mutants.toml`.
    let seen: std::collections::HashSet<usize> = out.iter().map(|(i, _)| *i).collect();
    for (i, item) in items.iter().enumerate() {
        if !seen.contains(&i) {
            out.push((i, Err(on_lost(item))));
        }
    }
    out.sort_by_key(|(i, _)| *i);
    out.into_iter().map(|(_, outcome)| outcome).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An `init` that panics retires its worker WITHOUT losing anyone's results,
    /// and every item is still answered.
    ///
    /// `catch_unwind` used to wrap `work` only, so a panic in `init` — which opens a
    /// state store and is not panic-free by construction — unwound through
    /// `thread::scope` and discarded every result already recorded. Both halves are
    /// asserted because each fails differently: with ALL inits panicking the old
    /// shape aborted the run, and a naive fix that merely returns early would hand
    /// the caller a SHORTER vector, which folds as "those tables quietly succeeded".
    #[test]
    fn a_worker_whose_init_declines_takes_nothing_and_the_others_drain_the_queue() {
        let items: Vec<usize> = (0..20).collect();
        let inits = AtomicUsize::new(0);
        let out = run_workers(
            &items,
            3,
            || (inits.fetch_add(1, Ordering::SeqCst) != 0).then_some(()),
            |_: &(), _, _| Ok::<(), String>(()),
            |item| format!("item {item} unanswered"),
        );
        assert_eq!(out.len(), items.len());
        assert!(
            out.iter().all(Result::is_ok),
            "the declined worker must not answer for any item: {out:?}"
        );
    }

    #[test]
    fn an_init_that_panics_loses_no_result_and_leaves_no_item_unanswered() {
        let items: Vec<usize> = (0..5).collect();

        // Every worker's init panics: nothing is ever taken from the queue, and the
        // fill-in must still answer for each item.
        let out = run_workers(
            &items,
            3,
            || panic!("init blew up"),
            |_: &(), _, _| Ok::<(), String>(()),
            |item| format!("item {item} unanswered"),
        );
        assert_eq!(
            out.len(),
            items.len(),
            "a dead init must not shorten the result vector — a missing entry folds \
             as a table that quietly succeeded"
        );
        assert!(
            out.iter().all(|r| r.is_err()),
            "an item no worker could reach is a FAILURE, not a silent success"
        );

        // Only the FIRST worker's init panics: the survivors drain the queue, so
        // every item is answered exactly once and none is answered twice.
        let inits = AtomicUsize::new(0);
        let out = run_workers(
            &items,
            3,
            || {
                if inits.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("the first init blew up");
                }
                Some(())
            },
            |_: &(), _, _| Ok::<(), String>(()),
            |item| format!("item {item} unanswered"),
        );
        assert_eq!(
            out.len(),
            items.len(),
            "one dead worker, still one answer each"
        );
        assert!(
            out.iter().all(|r| r.is_ok()),
            "the survivors take the retired worker's share, so nothing is refused"
        );
    }

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
            lite.contains("ONE writer"),
            "on SQLite the limit is the storage engine, not a quota — say so: {lite}"
        );
        assert!(
            lite.contains("RIVET_STATE_URL"),
            "and say what to do instead, or the operator keeps raising a number that \
             cannot help: {lite}"
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

    /// No requested size is [`DEFAULT_POOL`]; a request is clamped to [1, items].
    ///
    /// Both default cases are here on purpose. `None` against NINE items only
    /// proves the item clamp — it stays green for any default at or above nine —
    /// so the case with work to spare is what actually pins the default's value.
    #[test]
    fn effective_pool_clamps_to_at_least_one_and_at_most_the_work() {
        assert_eq!(
            effective_pool(None, 100),
            DEFAULT_POOL,
            "with work to spare, no flag means the default pool"
        );
        assert_eq!(
            effective_pool(None, 9),
            9,
            "the default is still capped by the work available"
        );
        assert_eq!(
            effective_pool(Some(1), 9),
            1,
            "`--pool 1` is the way back to the sequential pass, as the help promises"
        );
        assert_eq!(
            effective_pool(Some(0), 9),
            1,
            "zero workers would run nothing"
        );
        assert_eq!(effective_pool(Some(4), 9), 4);
        assert_eq!(
            effective_pool(Some(500), 1000),
            MAX_POOL,
            "the ceiling stops a request the warehouse quota cannot serve"
        );
        assert_eq!(
            effective_pool(Some(500), 8),
            8,
            "with fewer tables than the ceiling, the table count still wins"
        );
        assert_eq!(
            effective_pool(Some(99), 3),
            3,
            "never more workers than tables"
        );
        assert_eq!(
            effective_pool(Some(4), 0),
            1,
            "an empty config still resolves"
        );
    }

    /// Every item runs EXACTLY once — never dropped, never run twice.
    ///
    /// Swept over several pool sizes because the bug this catches is a racing
    /// cursor, which a single worker count cannot express: 1 is the sequential
    /// path, 3 divides the work unevenly, and 8 exceeds it.
    #[test]
    fn every_item_runs_exactly_once_at_any_pool_size() {
        for workers in [1usize, 2, 3, 8] {
            let items: Vec<usize> = (0..7).collect();
            let seen: Mutex<Vec<usize>> = Mutex::new(Vec::new());
            let out = run_workers(
                &items,
                effective_pool(Some(workers), items.len()),
                || Some(()),
                |_, i, item| {
                    assert_eq!(i, *item, "the index must address its own item");
                    seen.lock().unwrap().push(i);
                    Ok::<(), String>(())
                },
                |item| format!("item {item} panicked"),
            );
            let mut seen = seen.into_inner().unwrap();
            seen.sort_unstable();
            assert_eq!(
                seen,
                (0..7).collect::<Vec<_>>(),
                "pool of {workers}: every item runs exactly once"
            );
            assert_eq!(out.len(), 7, "pool of {workers}: one result per item");
        }
    }

    /// A failing item must not abandon the ones queued behind it.
    ///
    /// This is the contract the sequential loop's comment describes — a `?`
    /// inside it abandoned every LATER table — and which no test drove: the
    /// aggregation test hands `aggregate_load_failures` a vector the TEST built,
    /// so it never observes the loop that produces one.
    #[test]
    fn a_failing_item_does_not_abandon_the_others() {
        let items: Vec<usize> = (0..6).collect();
        let ran: Mutex<Vec<usize>> = Mutex::new(Vec::new());
        let out = run_workers(
            &items,
            2,
            || Some(()),
            |_, i, _| {
                ran.lock().unwrap().push(i);
                if i % 2 == 0 {
                    Err(format!("item {i} failed"))
                } else {
                    Ok(())
                }
            },
            |item| format!("item {item} panicked"),
        );

        let mut ran = ran.into_inner().unwrap();
        ran.sort_unstable();
        assert_eq!(
            ran,
            (0..6).collect::<Vec<_>>(),
            "three failures must not stop the other three from running"
        );
        let failed: Vec<usize> = out
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.as_ref().err().map(|_| i))
            .collect();
        assert_eq!(
            failed,
            vec![0, 2, 4],
            "every failure is reported, against its own item"
        );
        assert_eq!(out[0].as_ref().unwrap_err(), "item 0 failed");
    }

    /// Results are ordered by ITEM, not by completion.
    ///
    /// The fixture forces the completion order to differ from the item order —
    /// item 0 cannot finish until item 1 has — so dropping the sort is RED here.
    /// A fixture whose items happen to complete in order proves nothing about an
    /// ordering guarantee.
    #[test]
    fn results_come_back_in_item_order_whatever_the_completion_order() {
        let items: Vec<usize> = vec![0, 1];
        let one_is_done = std::sync::atomic::AtomicBool::new(false);
        let out = run_workers(
            &items,
            2,
            || Some(()),
            |_, i, _| {
                if i == 1 {
                    one_is_done.store(true, Ordering::Release);
                } else {
                    while !one_is_done.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                }
                Err::<(), String>(format!("from item {i}"))
            },
            |item| format!("item {item} panicked"),
        );
        assert_eq!(out[0].as_ref().unwrap_err(), "from item 0");
        assert_eq!(out[1].as_ref().unwrap_err(), "from item 1");
    }

    /// One worker runs the items IN ORDER — what `--pool 1` still guarantees.
    ///
    /// Asks for the single worker EXPLICITLY. It used to say `None` and call that
    /// "the default path", which stopped being true the moment the default became
    /// [`DEFAULT_POOL`] — a test that leans on a default to express its subject
    /// starts grading the default instead of the subject.
    #[test]
    fn a_single_worker_runs_in_item_order() {
        let items: Vec<usize> = (0..5).collect();
        let order: Mutex<Vec<usize>> = Mutex::new(Vec::new());
        run_workers(
            &items,
            effective_pool(Some(1), items.len()),
            || Some(()),
            |_, i, _| {
                order.lock().unwrap().push(i);
                Ok::<(), String>(())
            },
            |item| format!("item {item} panicked"),
        );
        assert_eq!(
            order.into_inner().unwrap(),
            (0..5).collect::<Vec<_>>(),
            "`--pool 1` must keep running tables one after another — the doc above \
             says why this asks for the single worker EXPLICITLY, and this message \
             said \"the default pool\" until the same pass that fixed the doc"
        );
    }

    /// `init` runs once per WORKER, not once per item — the per-worker resource
    /// (a load's own state store) must not be opened per table.
    #[test]
    fn init_runs_once_per_worker_not_once_per_item() {
        let items: Vec<usize> = (0..20).collect();
        let inits = AtomicUsize::new(0);
        run_workers(
            &items,
            3,
            || {
                inits.fetch_add(1, Ordering::Relaxed);
                Some(())
            },
            |_, _, _| Ok::<(), String>(()),
            |item| format!("item {item} panicked"),
        );
        assert_eq!(
            inits.load(Ordering::Relaxed),
            3,
            "twenty tables on three workers must open three resources, not twenty"
        );
    }

    /// An empty config runs nothing and reports nothing.
    #[test]
    fn no_items_is_not_an_error() {
        let items: Vec<usize> = Vec::new();
        let out = run_workers(
            &items,
            effective_pool(Some(4), items.len()),
            || Some(()),
            |_, _, _| Ok::<(), String>(()),
            |item| format!("item {item} panicked"),
        );
        assert!(out.is_empty());
    }

    /// A PANIC in one item must not discard what the others already did.
    ///
    /// RED against the pre-fix executor: an unwind out of `thread::scope` skipped
    /// the fold entirely, so the run ended with NO aggregate at all — after the
    /// other tables had already changed the warehouse. Panicking on exactly one
    /// item and asserting the rest still ran is what distinguishes "caught and
    /// reported" from "crashed politely".
    ///
    /// The panic hook is silenced for the duration: its default output is a
    /// backtrace on stderr, which reads like a failed run in an otherwise green
    /// suite.
    #[test]
    fn a_panicking_item_is_recorded_and_the_others_still_report() {
        let items: Vec<usize> = (0..6).collect();
        let ran: Mutex<Vec<usize>> = Mutex::new(Vec::new());

        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let out = run_workers(
            &items,
            2,
            || Some(()),
            |_, i, _| {
                if i == 3 {
                    panic!("boom in item 3");
                }
                ran.lock().unwrap().push(i);
                Ok::<(), String>(())
            },
            |item| format!("item {item} panicked"),
        );
        std::panic::set_hook(prev);

        let mut ran = ran.into_inner().unwrap();
        ran.sort_unstable();
        assert_eq!(
            ran,
            vec![0, 1, 2, 4, 5],
            "every other item must still have run"
        );
        assert_eq!(
            out.len(),
            6,
            "one result per item, the panicking one included"
        );
        assert_eq!(
            out[3].as_ref().unwrap_err(),
            "item 3 panicked",
            "the panicking item is reported as ITS OWN failure, in its own slot"
        );
        assert!(
            out[0].is_ok() && out[5].is_ok(),
            "the others' results are untouched"
        );
    }
}
