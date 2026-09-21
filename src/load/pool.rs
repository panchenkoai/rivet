//! Bounded work-stealing executor for the per-table `load` / `compact` loops.
//!
//! WHY THIS IS SAFE TO RUN CONCURRENTLY, and what that rests on. Unlike the export
//! pool (`pipeline/run.rs`), which serialises heavy exports because they contend for
//! ONE source database, load work is disjoint by construction: each plan owns its own
//! warehouse object and its own destination sub-prefix, and each table is guarded by a
//! per-table lease. There is therefore no `parallel_safe` notion here, and none is
//! missing.
//!
//! That disjointness is not free — it is held up by `plan::reject_duplicate_target_
//! tables`, which refuses at PLAN time two exports resolving to one warehouse object
//! (and, for non-`Full` modes, to one `<table>__changes` buffer). Remove or weaken
//! that refusal — or add a mode where several exports deliberately share one object —
//! and this pool turns a formerly sequential success into an intermittent "lease is
//! held", decided by whichever worker got there first. There is no eligibility hook
//! here to express that; it would need one, or a pre-split of `plans`.
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
    sqlite_state: bool,
) -> Option<String> {
    let asked = requested?;
    if asked <= MAX_POOL {
        return None;
    }
    let running = effective_pool(requested, items);
    if sqlite_state {
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
/// workers, so the one-worker default opens exactly one — as the sequential loop
/// always did.
///
/// Fault isolation is the contract this exists to keep: a failing item is
/// recorded and the worker takes the next one, so one poisoned table can never
/// abandon the tables behind it in the queue.
pub(crate) fn run_workers<T, W, E, I, F, P>(
    items: &[T],
    workers: usize,
    init: I,
    work: F,
    on_panic: P,
) -> Vec<Result<(), E>>
where
    T: Sync,
    E: Send,
    I: Fn() -> W + Sync,
    F: Fn(&W, usize, &T) -> Result<(), E> + Sync,
    P: Fn(&T) -> E + Sync,
{
    let next = AtomicUsize::new(0);
    let done: Mutex<Vec<(usize, Result<(), E>)>> = Mutex::new(Vec::with_capacity(items.len()));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let resource = init();
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
                    .unwrap_or_else(|_| Err(on_panic(item)));
                    done.lock().unwrap().push((i, outcome));
                }
            });
        }
    });
    let mut out = done.into_inner().unwrap();
    out.sort_by_key(|(i, _)| *i);
    out.into_iter().map(|(_, outcome)| outcome).collect()
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
            pool_ceiling_warning(None, 50, false).is_none(),
            "no request, no warning"
        );
        assert!(
            pool_ceiling_warning(Some(MAX_POOL), 500, false).is_none(),
            "asking for exactly the ceiling is allowed, not excessive"
        );

        let pg = pool_ceiling_warning(Some(MAX_POOL + 1), 500, false)
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

        let lite = pool_ceiling_warning(Some(MAX_POOL + 1), 500, true)
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
                || (),
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
            || (),
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
            || (),
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
            || (),
            |_, i, _| {
                order.lock().unwrap().push(i);
                Ok::<(), String>(())
            },
            |item| format!("item {item} panicked"),
        );
        assert_eq!(
            order.into_inner().unwrap(),
            (0..5).collect::<Vec<_>>(),
            "the default pool must keep running tables one after another"
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
            || (),
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
            || (),
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
