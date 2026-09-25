//! Bounded work-stealing executor: one ordered result per item, a panic isolated to its item, one ceiling for every pool in rivet.

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
pub(crate) fn run_workers<T, W, R, E, I, F, P>(
    items: &[T],
    workers: usize,
    init: I,
    work: F,
    on_lost: P,
) -> Vec<Result<R, E>>
where
    T: Sync,
    R: Send,
    E: Send,
    I: Fn() -> Option<W> + Sync,
    F: Fn(&W, usize, &T) -> Result<R, E> + Sync,
    P: Fn(&T) -> E + Sync,
{
    let next = AtomicUsize::new(0);
    let done: Mutex<Vec<(usize, Result<R, E>)>> = Mutex::new(Vec::with_capacity(items.len()));
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
                    .unwrap_or_else(|payload| {
                        log::error!("worker item {i} panicked: {}", panic_text(&*payload));
                        Err(on_lost(item))
                    });
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

/// The message a panic carried, for the log line that replaces the unwound stack.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// [`run_workers`] at the default width with no per-worker state: every item runs, a panic becomes that item's error.
pub(crate) fn run_each<T: Sync, R: Send>(
    items: &[T],
    f: impl Fn(&T) -> anyhow::Result<R> + Sync,
) -> Vec<anyhow::Result<R>> {
    run_workers(
        items,
        effective_pool(None, items.len()),
        || Some(()),
        |_, _, item| f(item),
        |_| anyhow::anyhow!("a worker panicked while running this item"),
    )
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

    /// The panic's own message reaches the log line, whatever the payload's string type.
    #[test]
    fn panic_text_reads_str_and_string_payloads() {
        let from_str = std::panic::catch_unwind(|| panic!("static boom")).unwrap_err();
        assert_eq!(panic_text(&*from_str), "static boom");
        let from_string = std::panic::catch_unwind(|| panic!("{} boom", "formatted")).unwrap_err();
        assert_eq!(panic_text(&*from_string), "formatted boom");
        let other = std::panic::catch_unwind(|| std::panic::panic_any(7_u8)).unwrap_err();
        assert_eq!(panic_text(&*other), "non-string panic payload");
    }

    /// `run_each` overlaps its items: the first can only finish once the second has started, and a failure stays in its own slot.
    #[test]
    fn run_each_overlaps_items_and_keeps_each_outcome_in_its_slot() {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let (tx, rx) = (Mutex::new(tx), Mutex::new(rx));
        let out = run_each(&[0, 1], |&i| {
            if i == 0 {
                rx.lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .map_err(|_| anyhow::anyhow!("item 1 never ran while item 0 was in flight"))
            } else {
                tx.lock().unwrap().send(()).map_err(Into::into)
            }
        });
        assert!(
            out.iter().all(Result::is_ok),
            "two items must be in flight at once"
        );

        let out = run_each(&[1, 2, 3], |&i| {
            anyhow::ensure!(i != 2, "item {i} refused");
            Ok(i)
        });
        assert_eq!(
            out.len(),
            3,
            "every item runs, the failure does not stop the rest"
        );
        assert_eq!(out[0].as_ref().unwrap(), &1);
        assert!(
            out[1]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("item 2 refused")
        );
        assert_eq!(out[2].as_ref().unwrap(), &3);
    }
}
