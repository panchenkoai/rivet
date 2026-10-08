//! STOP — a run stopped from outside (docs/sabotage-matrix.yaml, the `sigkill_*`, `graceful_*` and
//! `dead_owner_*` rows): parked at a point the test can see, sent one signal, reaped, and read for
//! what it left. A primitive panics unless the run was parked there and alive when the signal went,
//! so a cell cannot grade a run nobody stopped. [`Rig::stopped_then_run`] is the one grade after it:
//! the stop is not a success, and the next run delivers the source (the default oracle) or gives
//! the same coded refusal twice. [`graceful_is_worse`] holds a SIGTERM or SIGINT to the SIGKILL of
//! the same point.

use super::sabotage::Survived;
use super::*;

/// The signal that stops the run.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Stop {
    /// SIGKILL: no handler, no destructor.
    Kill,
    /// SIGTERM: a scheduler timeout, `docker stop`, a pod eviction.
    Term,
    /// SIGINT: Ctrl-C.
    Int,
}

impl Stop {
    /// The signal number.
    fn signal(self) -> i32 {
        match self {
            Stop::Kill => libc::SIGKILL,
            Stop::Term => libc::SIGTERM,
            Stop::Int => libc::SIGINT,
        }
    }
}

/// Where the run is parked when the signal arrives (`RIVET_TEST_BLOCK_AT=before_commit_rename`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Parked {
    /// Its first part is staged and no part is committed.
    FirstPartStaged,
    /// A part is committed and the next one is staged.
    APartCommitted,
}

/// What a stopped run left.
#[derive(Clone, Debug, PartialEq)]
pub struct Left {
    /// Its exit code; `None` when the signal ended it.
    pub exit: Option<i32>,
    /// The signal that ended it, if one did.
    pub signal: Option<i32>,
    /// Committed part files in the destination.
    pub parts: usize,
    /// Staged `.tmp` files in the destination.
    pub staged: usize,
    /// Whether `_SUCCESS` is there.
    pub success: bool,
    /// `run_status` rows the stopped process opened that are still `running`.
    pub running: i64,
    /// Rows of the export's run lease (Postgres state; SQLite holds it by `flock`).
    pub leases: i64,
}

/// A stopped run and how the next run ended.
#[derive(Clone, Debug, PartialEq)]
pub struct StoppedRun {
    pub left: Left,
    pub next: Survived,
}

/// Why `left` is not what a stopped run may leave, else `None`.
pub(crate) fn not_a_stop(left: &Left, how: Stop) -> Option<String> {
    let class = match (left.exit, left.signal) {
        (None, Some(s)) if s == how.signal() => None,
        (Some(c), None) if (1..=5).contains(&c) || c == 128 + how.signal() => None,
        (Some(0), _) => Some("exited 0: a stopped run reported success".to_string()),
        (exit, signal) => Some(format!(
            "ended outside the exit-class table (exit {exit:?}, signal {signal:?})"
        )),
    };
    class.or_else(|| {
        left.success
            .then(|| "left `_SUCCESS` over an export it did not finish".to_string())
    })
}

/// Why a graceful stop left a worse state than the SIGKILL of the same point, else `None`.
pub fn graceful_is_worse(graceful: &StoppedRun, killed: &StoppedRun) -> Option<String> {
    let (g, k) = (&graceful.left, &killed.left);
    let more = |what: &str, g: i64, k: i64| {
        (g > k).then(|| format!("left {g} {what}, the SIGKILL left {k}"))
    };
    more("staged `.tmp` file(s)", g.staged as i64, k.staged as i64)
        .or_else(|| more("`running` run_status row(s)", g.running, k.running))
        .or_else(|| more("run lease row(s)", g.leases, k.leases))
        .or_else(|| {
            (killed.next == Survived::Delivered && graceful.next != Survived::Delivered)
                .then(|| "the next run was refused, after the SIGKILL it delivered".to_string())
        })
}

/// The count of `running` run_status rows of the export that the process `pid` opened (a run id ends in `_<pid>`).
pub(crate) fn its_running_rows(pid: u32) -> String {
    let tail = format!("_{pid}");
    format!(
        "SELECT COUNT(*) FROM run_status WHERE status = 'running' AND export_name = '{{export}}' \
         AND substr(run_id, length(run_id) - {} + 1) = '{tail}'",
        tail.len()
    )
}

/// Why a run seen with `parts` committed and `staged` staged files is not parked at `at` yet, else `None`.
pub(crate) fn not_parked(at: Parked, parts: usize, staged: usize) -> Option<&'static str> {
    match at {
        Parked::FirstPartStaged if parts > 0 => Some("a part is committed already"),
        Parked::APartCommitted if parts == 0 => Some("no part is committed"),
        _ if staged == 0 => Some("no part is staged"),
        _ => None,
    }
}

impl Rig {
    /// Committed parts and staged `.tmp` files of this rig's local destination.
    fn parts_and_staged(&self) -> (usize, usize) {
        let files = crate::common::runner::files_under(&self.out_dir());
        let ending = |tail: &str| {
            files
                .iter()
                .filter(|f| f.to_string_lossy().ends_with(tail))
                .count()
        };
        (ending(".parquet"), ending(".tmp"))
    }

    /// Start `rivet run` with `envs`, park it at `at`, send `how` and reap it; panics unless it was parked there and alive when the signal went.
    pub fn stopped(&self, at: Parked, how: Stop, envs: &[(&str, &str)]) -> Left {
        use std::os::unix::process::ExitStatusExt as _;
        const WINDOW_MS: u64 = 4000;
        let block_ms = match at {
            Parked::FirstPartStaged => "600000".to_string(),
            Parked::APartCommitted => WINDOW_MS.to_string(),
        };
        let mut envs = envs.to_vec();
        envs.push(("RIVET_TEST_BLOCK_AT", "before_commit_rename"));
        envs.push(("RIVET_TEST_BLOCK_MS", &block_ms));
        let mut run = self.spawn_mid_run(&envs, |rig| {
            let (parts, staged) = rig.parts_and_staged();
            not_parked(at, parts, staged).is_none()
        });
        let seen = std::time::Instant::now();
        assert!(
            run.try_wait().expect("try_wait").is_none(),
            "stop: the run ended before the signal"
        );
        let pid = run.id();
        let rc = unsafe { libc::kill(pid as i32, how.signal()) };
        assert_eq!(rc, 0, "stop: the signal was not delivered");
        assert!(
            seen.elapsed().as_millis() < u128::from(WINDOW_MS / 2),
            "stop: the signal went {:?} after the run was seen parked, past half its window",
            seen.elapsed()
        );
        let status = run.wait().expect("reap the stopped run");
        let (parts, staged) = self.parts_and_staged();
        Left {
            exit: status.code(),
            signal: status.signal(),
            parts,
            staged,
            success: self.out_dir().join("_SUCCESS").exists(),
            running: self.state_count(&its_running_rows(pid)),
            leases: self.state_count(
                "SELECT COUNT(*) FROM state_lease WHERE lease_key = 'chunk-run:{export}'",
            ),
        }
    }

    /// [`Rig::stopped`], held to [`not_a_stop`], then the next plain run: it delivers the source or gives one coded refusal twice.
    pub fn stopped_then_run(&self, at: Parked, how: Stop, envs: &[(&str, &str)]) -> StoppedRun {
        let left = self.stopped(at, how, envs);
        if let Some(why) = not_a_stop(&left, how) {
            panic!("a run stopped by {how:?} at {at:?} {why}: {left:?}");
        }
        let next = self.delivers_or_refuses(&["run"], envs);
        StoppedRun { left, next }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn left(exit: Option<i32>, signal: Option<i32>) -> Left {
        Left {
            exit,
            signal,
            parts: 1,
            staged: 1,
            success: false,
            running: 1,
            leases: 0,
        }
    }

    fn stopped(left: Left, next: Survived) -> StoppedRun {
        StoppedRun { left, next }
    }

    #[test]
    fn a_stop_is_the_signal_itself_or_an_exit_of_the_class_table_and_never_a_success() {
        assert_eq!(
            not_a_stop(&left(None, Some(libc::SIGKILL)), Stop::Kill),
            None
        );
        assert_eq!(not_a_stop(&left(Some(1), None), Stop::Term), None);
        assert_eq!(
            not_a_stop(&left(Some(128 + libc::SIGINT), None), Stop::Int),
            None
        );
        assert_eq!(
            not_a_stop(&left(Some(0), None), Stop::Term).as_deref(),
            Some("exited 0: a stopped run reported success")
        );
        for other in [left(Some(101), None), left(None, Some(libc::SIGSEGV))] {
            let why = not_a_stop(&other, Stop::Term).expect("outside the table");
            assert!(
                why.starts_with("ended outside the exit-class table"),
                "{why}"
            );
        }
        let marked = Left {
            success: true,
            ..left(None, Some(libc::SIGTERM))
        };
        assert_eq!(
            not_a_stop(&marked, Stop::Term).as_deref(),
            Some("left `_SUCCESS` over an export it did not finish")
        );
    }

    #[test]
    fn a_graceful_stop_is_worse_when_it_leaves_more_or_the_next_run_no_longer_delivers() {
        let killed = stopped(left(None, Some(libc::SIGKILL)), Survived::Delivered);
        let same = stopped(left(None, Some(libc::SIGTERM)), Survived::Delivered);
        assert_eq!(graceful_is_worse(&same, &killed), None);
        let cleaner = Left {
            staged: 0,
            running: 0,
            ..left(Some(1), None)
        };
        assert_eq!(
            graceful_is_worse(&stopped(cleaner, Survived::Delivered), &killed),
            None
        );
        let term = || left(None, Some(libc::SIGTERM));
        let more = [
            (
                Left {
                    staged: 2,
                    ..term()
                },
                "left 2 staged `.tmp` file(s), the SIGKILL left 1",
            ),
            (
                Left {
                    running: 2,
                    ..term()
                },
                "left 2 `running` run_status row(s), the SIGKILL left 1",
            ),
            (
                Left {
                    leases: 1,
                    ..term()
                },
                "left 1 run lease row(s), the SIGKILL left 0",
            ),
        ];
        for (l, why) in more {
            assert_eq!(
                graceful_is_worse(&stopped(l, Survived::Delivered), &killed).as_deref(),
                Some(why)
            );
        }
        let refused = stopped(
            left(None, Some(libc::SIGTERM)),
            Survived::Refused("Error: [RIVET_X] no".to_string()),
        );
        assert_eq!(
            graceful_is_worse(&refused, &killed).as_deref(),
            Some("the next run was refused, after the SIGKILL it delivered")
        );
        assert_eq!(graceful_is_worse(&refused, &refused), None);
    }

    #[test]
    fn a_run_is_parked_only_with_a_staged_part_and_the_committed_parts_its_point_names() {
        assert_eq!(not_parked(Parked::FirstPartStaged, 0, 1), None);
        assert_eq!(not_parked(Parked::APartCommitted, 1, 1), None);
        assert_eq!(
            not_parked(Parked::FirstPartStaged, 1, 1),
            Some("a part is committed already")
        );
        assert_eq!(
            not_parked(Parked::APartCommitted, 0, 1),
            Some("no part is committed")
        );
        for at in [Parked::FirstPartStaged, Parked::APartCommitted] {
            let parts = usize::from(at == Parked::APartCommitted);
            assert_eq!(not_parked(at, parts, 0), Some("no part is staged"));
        }
    }

    #[test]
    fn running_rows_are_counted_for_one_process_of_one_export() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE run_status (run_id TEXT, export_name TEXT, status TEXT);
             INSERT INTO run_status VALUES
               ('t_20261008T000000.000_4242', 't', 'running'),
               ('t_20261008T000000.001_14242', 't', 'running'),
               ('t_20261008T000000.002_4242', 't', 'failed'),
               ('u_20261008T000000.003_4242', 'u', 'running');",
        )
        .unwrap();
        let sql = its_running_rows(4242).replace("{export}", "t");
        let n: i64 = db.query_row(&sql, [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1, "{sql}");
    }
}
