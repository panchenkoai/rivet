//! STOP — a run stopped from outside (docs/sabotage-matrix.yaml, the `kill_*`, `graceful_*` and
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
    /// `run_status` rows of the export still `running`.
    pub running: i64,
    /// Rows of the export's run lease (Postgres state; SQLite holds it by `flock`).
    pub leases: i64,
}

/// A stopped run and how the next run ended.
#[derive(Clone, Debug, PartialEq)]
pub struct Stopped {
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
pub fn graceful_is_worse(graceful: &Stopped, killed: &Stopped) -> Option<String> {
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

/// How many files under `dir` (recursively) have a name `is` accepts.
fn count_files(dir: &Path, is: &dyn Fn(&str) -> bool) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let path = e.path();
            if path.is_dir() {
                count_files(&path, is)
            } else {
                usize::from(is(&e.file_name().to_string_lossy()))
            }
        })
        .sum()
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
        let out = self.out_dir();
        (
            count_files(&out, &|n| n.ends_with(".parquet")),
            count_files(&out, &|n| n.ends_with(".tmp")),
        )
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
        let mut run = self.spawn_args_env(&[], &envs);
        let t0 = std::time::Instant::now();
        loop {
            let (parts, staged) = self.parts_and_staged();
            let Some(why) = not_parked(at, parts, staged) else {
                break;
            };
            assert!(
                run.try_wait().expect("try_wait").is_none(),
                "stop: the run ended before it was parked ({why})"
            );
            assert!(
                t0.elapsed().as_secs() < 120,
                "stop: the run was never parked ({why}; {parts} part(s), {staged} staged)"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let seen = std::time::Instant::now();
        assert!(
            run.try_wait().expect("try_wait").is_none(),
            "stop: the run ended before the signal"
        );
        let rc = unsafe { libc::kill(run.id() as i32, how.signal()) };
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
            running: self.state_count(
                "SELECT COUNT(*) FROM run_status WHERE status = 'running' AND export_name = '{export}'",
            ),
            leases: self
                .state_count("SELECT COUNT(*) FROM state_lease WHERE lease_key = 'chunk-run:{export}'"),
        }
    }

    /// [`Rig::stopped`], held to [`not_a_stop`], then the next plain run: it delivers the source or gives one coded refusal twice.
    pub fn stopped_then_run(&self, at: Parked, how: Stop, envs: &[(&str, &str)]) -> Stopped {
        let left = self.stopped(at, how, envs);
        if let Some(why) = not_a_stop(&left, how) {
            panic!("a run stopped by {how:?} at {at:?} {why}: {left:?}");
        }
        let next = self.delivers_or_refuses(&["run"], envs);
        Stopped { left, next }
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

    fn stopped(left: Left, next: Survived) -> Stopped {
        Stopped { left, next }
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
    fn files_are_counted_through_subdirectories_by_name() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("d")).unwrap();
        for f in [
            "a.parquet",
            "d/b.parquet",
            "d/c.parquet.tmp",
            "manifest.json",
        ] {
            std::fs::write(dir.path().join(f), b"x").unwrap();
        }
        assert_eq!(count_files(dir.path(), &|n| n.ends_with(".parquet")), 2);
        assert_eq!(count_files(dir.path(), &|n| n.ends_with(".tmp")), 1);
        assert_eq!(count_files(&dir.path().join("absent"), &|_| true), 0);
    }
}
