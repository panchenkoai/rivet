//! BESIDE — an export met while its run is alive (docs/sabotage-matrix.yaml: a resource taken away
//! mid-run, two processes on one export). [`Rig::beside_a_live_run`] is the one way in: it panics
//! unless the run was alive when the cell acted, so a cell cannot grade a run nobody met.
//! [`Rig::read_only`] takes a local resource away and panics unless a write is then refused.

use super::*;

/// How long a slowed live run may take to reach its mid-run mark, and to end once the cell has acted.
const CEILING: std::time::Duration = std::time::Duration::from_secs(180);

/// A local resource of a rig that a cell takes away.
#[derive(Clone, Copy, Debug)]
pub enum Local {
    /// The SQLite state files beside the config.
    State,
    /// The directory that holds the SQLite state, its one file left writable: a read-only volume mount.
    StateDirectory,
    /// The local destination directory.
    Destination,
}

/// A run met while it was alive: how it ended, and what the cell did beside it.
pub struct Met<T> {
    /// What the live run printed, and its exit.
    pub run: std::process::Output,
    /// What the action beside it returned.
    pub acted: T,
    outlived: bool,
}

impl<T> Met<T> {
    /// The run and the action's result; panics unless the run outlived the action (a command that must answer beside a LIVE run).
    pub fn answered_while_alive(self) -> (std::process::Output, T) {
        assert!(
            self.outlived,
            "fixture: the live run ended before the command beside it answered"
        );
        (self.run, self.acted)
    }
}

/// Poll until `ready`; `Err` when the run has `exited` first or `ceiling` passed, so nothing is done beside a run that is not there.
pub(crate) fn wait_mid_run(
    mut ready: impl FnMut() -> bool,
    mut exited: impl FnMut() -> bool,
    ceiling: std::time::Duration,
) -> Result<(), String> {
    let t0 = std::time::Instant::now();
    loop {
        if exited() {
            return Err("the run exited before it was seen mid-export".to_string());
        }
        if ready() {
            return Ok(());
        }
        if t0.elapsed() >= ceiling {
            return Err(format!(
                "the run did not reach its mid-run mark in {ceiling:?}"
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// Why paths made read-only are not a resource taken away, else `None`: there must be one, and a write must be refused.
pub(crate) fn not_taken_away(
    what: Local,
    paths: &[PathBuf],
    still_writable: bool,
) -> Option<String> {
    if paths.is_empty() {
        return Some(format!(
            "sabotage: no {what:?} of this rig to make read-only"
        ));
    }
    still_writable.then(|| {
        format!("sabotage: the {what:?} is still writable after chmod (running as root?): nothing was taken away")
    })
}

/// Paths made read-only until dropped.
pub struct ReadOnly(Vec<(PathBuf, u32)>);

impl ReadOnly {
    /// Give the resource back now.
    pub fn restore(self) {}
}

impl Drop for ReadOnly {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt as _;
        for (p, mode) in &self.0 {
            let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(*mode));
        }
    }
}

/// Whether a write to `p` (a new file in a directory, an append to a file) still succeeds.
fn writable(p: &Path) -> bool {
    if p.is_dir() {
        let probe = p.join(".sabotage-probe");
        let ok = std::fs::write(&probe, b"").is_ok();
        let _ = std::fs::remove_file(&probe);
        ok
    } else {
        std::fs::OpenOptions::new().append(true).open(p).is_ok()
    }
}

impl Rig {
    /// This rig reading ten rows every `ms` milliseconds, so its run stays alive long enough to be met.
    pub fn slowed(self, ms: u32) -> Self {
        self.source_line("tuning:")
            .source_line("  batch_size: 10")
            .source_line(&format!("  throttle_ms: {ms}"))
    }

    /// Whether this rig's local destination holds a parquet part yet.
    pub fn has_a_part(&self) -> bool {
        crate::common::runner::files_under(&self.out_dir())
            .iter()
            .any(|p| p.extension().is_some_and(|e| e == "parquet"))
    }

    /// Start `rivet run` and hand it back alive once `ready`; panics when it exits first.
    pub fn spawn_mid_run(
        &self,
        envs: &[(&str, &str)],
        ready: impl Fn(&Rig) -> bool,
    ) -> Spawned<'_> {
        let mut live = self.spawn_args_env(&[], envs);
        let mut exited = false;
        let seen = wait_mid_run(
            || ready(self),
            || {
                exited = live.try_wait().expect("try_wait").is_some();
                exited
            },
            CEILING,
        );
        if let Err(why) = seen {
            if !exited {
                let _ = live.kill();
            }
            let said = live.wait_with_output().expect("reap the run");
            panic!("fixture: {why}\n{}", String::from_utf8_lossy(&said.stderr));
        }
        live
    }

    /// Start `rivet run`, wait until `ready`, do `act` beside it and reap it; panics unless the run was alive when `act` began.
    pub fn beside_a_live_run<T>(
        &self,
        envs: &[(&str, &str)],
        ready: impl Fn(&Rig) -> bool,
        act: impl FnOnce(&Rig) -> T,
    ) -> Met<T> {
        let mut live = self.spawn_mid_run(envs, ready);
        let acted = act(self);
        let outlived = live.try_wait().expect("try_wait").is_none();
        let t0 = std::time::Instant::now();
        while live.try_wait().expect("try_wait").is_none() {
            if t0.elapsed() >= CEILING {
                let _ = live.kill();
                let _ = live.wait();
                panic!("the live run did not end within {CEILING:?} of the action beside it");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        Met {
            run: live.wait_with_output().expect("reap the run"),
            acted,
            outlived,
        }
    }

    /// Delete the lease files of this rig's SQLite state (what a live checkpointed run locks); panics when there is none.
    pub fn delete_lease_files(&self) -> usize {
        let cfg = self.config_path();
        let leases: Vec<PathBuf> =
            crate::common::runner::files_under(cfg.parent().expect("a config directory"))
                .into_iter()
                .filter(|p| p.to_string_lossy().contains(".rivet_state.db.lease-"))
                .collect();
        assert!(
            !leases.is_empty(),
            "sabotage: no lease file beside this rig's state to delete"
        );
        for l in &leases {
            std::fs::remove_file(l).expect("delete the lease file");
        }
        leases.len()
    }

    /// Make a local resource of this rig read-only until the guard is dropped; panics unless it exists and a write to it is then refused.
    pub fn read_only(&self, what: Local) -> ReadOnly {
        use std::os::unix::fs::PermissionsExt as _;
        let cfg = self.config_path();
        let dir = cfg.parent().expect("a config directory");
        if let Local::StateDirectory = what {
            // The last connection to close checkpoints and removes `-wal` and `-shm`, as a finished run does.
            rusqlite::Connection::open(dir.join(".rivet_state.db"))
                .and_then(|c| c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())))
                .expect("checkpoint the state");
        }
        let state: Vec<PathBuf> = crate::common::runner::files_under(dir)
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with(".rivet_state.db"))
            })
            .collect();
        let paths: Vec<PathBuf> = match what {
            Local::Destination => vec![self.out_dir()],
            Local::State => state,
            Local::StateDirectory => {
                assert_eq!(
                    state.len(),
                    1,
                    "fixture: a state no process has open is one file, with no -wal or -shm: {state:?}"
                );
                vec![dir.to_path_buf()]
            }
        };
        let paths: Vec<PathBuf> = paths.into_iter().filter(|p| p.exists()).collect();
        let guard = ReadOnly(
            paths
                .iter()
                .map(|p| {
                    let mode = std::fs::metadata(p).expect("stat").permissions().mode();
                    std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode & !0o222))
                        .expect("chmod");
                    (p.clone(), mode)
                })
                .collect(),
        );
        if let Some(why) = not_taken_away(what, &paths, paths.iter().any(|p| writable(p))) {
            drop(guard);
            panic!("{why}");
        }
        guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_is_done_beside_a_run_that_exited_or_never_got_there() {
        let soon = std::time::Duration::from_millis(60);
        let mut polls = 0;
        let third = || {
            polls += 1;
            polls >= 3
        };
        assert_eq!(wait_mid_run(third, || false, CEILING), Ok(()));
        assert_eq!(
            wait_mid_run(|| true, || true, CEILING),
            Err("the run exited before it was seen mid-export".to_string())
        );
        assert_eq!(
            wait_mid_run(|| false, || false, soon),
            Err(format!(
                "the run did not reach its mid-run mark in {soon:?}"
            ))
        );
    }

    #[test]
    fn a_resource_that_is_not_there_or_still_writable_was_not_taken_away() {
        let p = vec![PathBuf::from("out")];
        assert_eq!(not_taken_away(Local::Destination, &p, false), None);
        assert!(
            not_taken_away(Local::State, &[], false)
                .is_some_and(|w| w.contains("no State of this rig"))
        );
        assert!(
            not_taken_away(Local::Destination, &p, true)
                .is_some_and(|w| w.contains("still writable"))
        );
    }

    #[test]
    fn a_read_only_destination_refuses_a_write_until_the_guard_goes() {
        let rig = Rig::pg_batch("beside_read_only");
        let out = rig.out_dir();
        std::fs::create_dir_all(&out).unwrap();
        let guard = rig.read_only(Local::Destination);
        assert!(std::fs::write(out.join("part.parquet"), b"x").is_err());
        guard.restore();
        assert!(std::fs::write(out.join("part.parquet"), b"x").is_ok());
        assert!(rig.has_a_part());
    }

    #[test]
    #[should_panic(expected = "no State of this rig to make read-only")]
    fn a_state_that_is_not_there_cannot_be_made_read_only() {
        let _ = Rig::pg_batch("beside_no_state").read_only(Local::State);
    }

    #[test]
    #[should_panic(expected = "the live run ended before the command beside it answered")]
    fn a_command_that_answered_after_the_run_ended_did_not_answer_beside_it() {
        use std::os::unix::process::ExitStatusExt as _;
        let met = Met {
            run: std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            },
            acted: (),
            outlived: false,
        };
        let _ = met.answered_while_alive();
    }
}
