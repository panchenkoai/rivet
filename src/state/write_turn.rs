//! Writers of one SQLite state file in this process take turns at its write lock, first come
//! first served.
//!
//! SQLite's busy wait has no queue: a waiter sleeps and asks again, and a writer that commits back
//! to back takes the lock in the gap every time. Measured on devbox: a `BEGIN IMMEDIATE` beside a
//! sibling store committing in a loop was refused `database is locked` after 15.4 s, past the
//! 10 s busy timeout, with 530 000 writes in between (the `replan` cell, 1 in 1000 on CI). Chunk
//! workers, the run lease keeper and a run's own store are such siblings in one process. A turn
//! is held from `BEGIN IMMEDIATE` to `COMMIT` (or around one autocommit write); another process
//! still meets the file lock and SQLite's busy wait. A writer that ran out of patience while a turn
//! was held keeps its place: no later writer of this process gets a turn before it has written, so
//! its busy wait meets only the slow holder, never a sibling that commits back to back.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long a writer waits for its turn before it goes on to SQLite's busy wait: the busy timeout.
#[cfg(not(test))]
pub(super) const PATIENCE: Duration = Duration::from_millis(super::SQLITE_BUSY_TIMEOUT_MS as u64);
/// Fifty milliseconds in the unit tests, so a turn that is never handed back costs each write a
/// moment there instead of the busy timeout.
#[cfg(test)]
pub(super) const PATIENCE: Duration = Duration::from_millis(50);

/// One file's queue: the waiting tickets in order, whether a turn is taken, and the ticket that
/// ran out of patience and is in SQLite's busy wait ahead of every other.
#[derive(Default)]
struct Queue {
    waiting: VecDeque<u64>,
    taken: bool,
    overdue: Option<u64>,
    next: u64,
}

#[derive(Default)]
struct Turns {
    queue: Mutex<Queue>,
    freed: Condvar,
}

/// A turn at one file's write lock, or the place of a writer that ran out of patience (`overdue`);
/// dropping it hands the lock to the next ticket.
pub(super) struct Turn {
    turns: Arc<Turns>,
    overdue: Option<u64>,
}

impl Drop for Turn {
    fn drop(&mut self) {
        let mut q = self.turns.queue.lock().unwrap_or_else(|p| p.into_inner());
        match self.overdue {
            Some(me) if q.overdue == Some(me) => q.overdue = None,
            Some(_) => {}
            None => q.taken = false,
        }
        drop(q);
        self.turns.freed.notify_all();
    }
}

/// The queue of the file at `db`, one per file per process (the path canonicalized when it exists).
fn turns_of(db: &Path) -> Arc<Turns> {
    static ALL: OnceLock<Mutex<HashMap<PathBuf, Arc<Turns>>>> = OnceLock::new();
    let key = std::fs::canonicalize(db).unwrap_or_else(|_| db.to_path_buf());
    ALL.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(key)
        .or_default()
        .clone()
}

/// Wait for this process's turn at `db`'s write lock, at most `patience`. Past it the caller goes
/// on to SQLite's own busy wait and its verdict, holding its place (no later writer of this
/// process is served before it) unless another overdue writer holds that place already (`None`,
/// also for an in-memory store).
pub(super) fn take(db: &Path, patience: Duration) -> Option<Turn> {
    if db.as_os_str() == ":memory:" {
        return None;
    }
    let turns = turns_of(db);
    let deadline = Instant::now() + patience;
    let mut q = turns.queue.lock().unwrap_or_else(|p| p.into_inner());
    let me = q.next;
    q.next += 1;
    q.waiting.push_back(me);
    loop {
        if !q.taken && q.overdue.is_none() && q.waiting.front() == Some(&me) {
            q.waiting.pop_front();
            q.taken = true;
            drop(q);
            return Some(Turn {
                turns,
                overdue: None,
            });
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            q.waiting.retain(|t| *t != me);
            let place = q.overdue.is_none().then(|| {
                q.overdue = Some(me);
                me
            });
            drop(q);
            turns.freed.notify_all();
            return place.map(|me| Turn {
                turns,
                overdue: Some(me),
            });
        }
        q = turns
            .freed
            .wait_timeout(q, left)
            .unwrap_or_else(|p| p.into_inner())
            .0;
    }
}

#[cfg(test)]
mod tests {
    use super::take;
    use std::time::Duration;

    #[test]
    fn turns_are_served_in_the_order_they_were_asked_and_a_late_one_gives_up() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        std::fs::write(&db, b"").unwrap();
        let again = take(&db, Duration::ZERO).expect("a free lock is a turn at once");
        drop(again);
        let first = take(&db, Duration::ZERO).expect("a dropped turn is handed back at once");
        let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let waiters: Vec<_> = (0..3)
            .map(|i| {
                let (db, order) = (db.clone(), order.clone());
                std::thread::sleep(Duration::from_millis(20));
                std::thread::spawn(move || {
                    let t = take(&db, Duration::from_secs(2)).expect("a turn");
                    order.lock().unwrap().push(i);
                    drop(t);
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(50));
        let late = take(&db, Duration::from_millis(30)).expect("the late one keeps its place");
        assert!(
            take(&db, Duration::from_millis(30)).is_none(),
            "one place is kept, a second overdue writer has none"
        );
        drop(first);
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            order.lock().unwrap().is_empty(),
            "no waiter is served while the overdue one has not written"
        );
        drop(late);
        for w in waiters {
            w.join().unwrap();
        }
        assert_eq!(
            *order.lock().unwrap(),
            vec![0, 1, 2],
            "first come, first served"
        );
        assert!(take(std::path::Path::new(":memory:"), Duration::ZERO).is_none());
    }
}
