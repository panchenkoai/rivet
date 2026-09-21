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

/// Worker threads to run `items` on: at least one, never more than there is work.
///
/// `None` is one worker — the sequential path, which is the default and must stay
/// byte-for-byte what it was before the pool existed.
pub(crate) fn effective_pool(requested: Option<usize>, items: usize) -> usize {
    requested.unwrap_or(1).clamp(1, items.max(1))
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
pub(crate) fn run_workers<T, W, E, I, F>(
    items: &[T],
    workers: usize,
    init: I,
    work: F,
) -> Vec<Result<(), E>>
where
    T: Sync,
    E: Send,
    I: Fn() -> W + Sync,
    F: Fn(&W, usize, &T) -> Result<(), E> + Sync,
{
    let next = AtomicUsize::new(0);
    let done: Mutex<Vec<(usize, Result<(), E>)>> = Mutex::new(Vec::with_capacity(items.len()));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let resource = init();
                while let Some((i, item)) = take_next(&next, items) {
                    let outcome = work(&resource, i, item);
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

    /// No requested size is one worker; a request is clamped to [1, items].
    #[test]
    fn effective_pool_clamps_to_at_least_one_and_at_most_the_work() {
        assert_eq!(
            effective_pool(None, 9),
            1,
            "the default is the sequential path"
        );
        assert_eq!(
            effective_pool(Some(0), 9),
            1,
            "zero workers would run nothing"
        );
        assert_eq!(effective_pool(Some(4), 9), 4);
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
        );
        assert_eq!(out[0].as_ref().unwrap_err(), "from item 0");
        assert_eq!(out[1].as_ref().unwrap_err(), "from item 1");
    }

    /// One worker runs the items IN ORDER — the default path is still sequential.
    #[test]
    fn a_single_worker_runs_in_item_order() {
        let items: Vec<usize> = (0..5).collect();
        let order: Mutex<Vec<usize>> = Mutex::new(Vec::new());
        run_workers(
            &items,
            effective_pool(None, items.len()),
            || (),
            |_, i, _| {
                order.lock().unwrap().push(i);
                Ok::<(), String>(())
            },
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
        );
        assert!(out.is_empty());
    }
}
