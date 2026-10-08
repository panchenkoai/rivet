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

/// The part paths more than one of `manifests` (parsed manifest documents) declares, sorted.
pub(crate) fn declared_twice(manifests: &[serde_json::Value]) -> Vec<String> {
    let mut seen = std::collections::BTreeMap::<&str, usize>::new();
    for part in manifests
        .iter()
        .filter_map(|m| m["parts"].as_array())
        .flatten()
    {
        if let Some(path) = part["path"].as_str() {
            *seen.entry(path).or_default() += 1;
        }
    }
    seen.into_iter()
        .filter(|(_, n)| *n > 1)
        .map(|(p, _)| p.to_string())
        .collect()
}

/// The parts of `manifest` whose name does not hold the stamp of the run that declares them (run id `<export>_<yyyymmdd>T<hhmmss>.<mmm>_<pid>`, stamp `<yyyymmdd>_<hhmmss>_<mmm>_<pid>`, a nonce after it), sorted.
pub(crate) fn not_named_by_its_run(manifest: &serde_json::Value) -> Vec<String> {
    let (run_id, export) = (
        manifest["run_id"].as_str().unwrap_or_default(),
        manifest["export_name"].as_str().unwrap_or_default(),
    );
    let stamp = run_id
        .strip_prefix(export)
        .unwrap_or(run_id)
        .replace(['T', '.'], "_");
    let mut odd: Vec<String> = manifest["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| p["path"].as_str())
        .filter(|path| !path.starts_with(&format!("{export}{stamp}_")))
        .map(str::to_string)
        .collect();
    odd.sort();
    odd
}

/// A local resource made read-only until dropped: the rule that names its paths, and the write bits taken.
pub struct ReadOnly {
    what: Local,
    dir: PathBuf,
    out: PathBuf,
    bits: u32,
}

impl ReadOnly {
    /// Give the resource back now.
    pub fn restore(self) {}

    /// The paths that are this resource now: SQLite creates and removes `-wal`, `-shm` and `-journal` beside the state while it is away.
    fn paths(&self) -> Vec<PathBuf> {
        match self.what {
            Local::Destination => vec![self.out.clone()],
            Local::StateDirectory => vec![self.dir.clone()],
            Local::State => state_files(&self.dir),
        }
    }
}

impl Drop for ReadOnly {
    fn drop(&mut self) {
        chmod(self.paths(), |mode| mode | self.bits);
    }
}

/// The SQLite state files in `dir`.
fn state_files(dir: &Path) -> Vec<PathBuf> {
    crate::common::runner::files_under(dir)
        .into_iter()
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(".rivet_state.db"))
        })
        .collect()
}

/// Give each of `paths` that is still there the mode `to(its mode)`: the paths changed, and the modes they had.
fn chmod(paths: Vec<PathBuf>, to: impl Fn(u32) -> u32) -> Vec<(PathBuf, u32)> {
    use std::os::unix::fs::PermissionsExt as _;
    paths
        .into_iter()
        .filter_map(|p| {
            let mode = std::fs::metadata(&p).ok()?.permissions().mode();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(to(mode))).ok()?;
            Some((p, mode))
        })
        .collect()
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

    /// Start `n` `rivet run` of this rig back to back and reap them once all have ended: what each printed and its exit, each graded like any other run.
    pub fn runs_at_once(&self, n: usize) -> Vec<std::process::Output> {
        let mut live: Vec<Spawned<'_>> = (0..n).map(|_| self.spawn_args_env(&[], &[])).collect();
        for run in &mut live {
            std::process::Child::wait(run).expect("wait for a run started beside another");
        }
        live.into_iter()
            .map(|run| run.wait_with_output().expect("reap the run"))
            .collect()
    }

    /// The part paths more than one run-unique manifest of this rig's local destination declares: one file two runs both claim.
    pub fn parts_declared_twice(&self) -> Vec<String> {
        declared_twice(&self.manifests())
    }

    /// The declared parts of this rig's local destination that are not named after the run that declares them.
    pub fn parts_not_named_by_their_run(&self) -> Vec<String> {
        self.manifests()
            .iter()
            .flat_map(not_named_by_its_run)
            .collect()
    }

    /// Every run-unique manifest of this rig's local destination, parsed.
    fn manifests(&self) -> Vec<serde_json::Value> {
        crate::common::parquet::declared_manifests(&self.out_dir())
            .iter()
            .map(|m| {
                serde_json::from_slice(&std::fs::read(m).expect("read a manifest"))
                    .expect("a JSON manifest")
            })
            .collect()
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
        let cfg = self.config_path();
        let dir = cfg.parent().expect("a config directory").to_path_buf();
        if let Local::StateDirectory = what {
            // The last connection to close checkpoints and removes `-wal` and `-shm`, as a finished run does.
            rusqlite::Connection::open(dir.join(".rivet_state.db"))
                .and_then(|c| c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())))
                .expect("checkpoint the state");
            let state = state_files(&dir);
            assert_eq!(
                state.len(),
                1,
                "fixture: a state no process has open is one file, with no -wal or -shm: {state:?}"
            );
        }
        let mut guard = ReadOnly {
            what,
            dir,
            out: self.out_dir(),
            bits: 0,
        };
        let taken = chmod(guard.paths(), |mode| mode & !0o222);
        guard.bits = taken
            .iter()
            .fold(0, |bits, (_, mode)| bits | (mode & 0o222));
        let paths: Vec<PathBuf> = taken.into_iter().map(|(p, _)| p).collect();
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
    fn a_part_two_manifests_declare_is_found_and_one_each_is_not() {
        let m = |paths: &[&str]| serde_json::json!({ "parts": paths.iter().map(|p| serde_json::json!({ "path": p })).collect::<Vec<_>>() });
        assert_eq!(
            declared_twice(&[m(&["a.parquet"]), m(&["b.parquet", "a.parquet"])]),
            ["a.parquet"]
        );
        assert!(declared_twice(&[m(&["a.parquet"]), m(&["b.parquet"]), m(&[])]).is_empty());
        assert!(declared_twice(&[serde_json::json!({})]).is_empty());
    }

    #[test]
    fn a_part_is_named_by_its_run_only_when_it_holds_the_run_stamp() {
        let m = |paths: &[&str]| {
            serde_json::json!({
                "run_id": "orders_20261008T174117.653_33972",
                "export_name": "orders",
                "parts": paths.iter().map(|p| serde_json::json!({ "path": p })).collect::<Vec<_>>(),
            })
        };
        let own = [
            "orders_20261008_174117_653_33972_9f3a1c0b5d7e2a41.parquet",
            "orders_20261008_174117_653_33972_9f3a1c0b5d7e2a41_part1.parquet",
        ];
        assert!(not_named_by_its_run(&m(&own)).is_empty());
        let other = [
            "orders_20261008_174117_653_33973_9f3a1c0b5d7e2a41.parquet",
            "orders_20261008_174122_776.parquet",
        ];
        assert_eq!(
            not_named_by_its_run(&m(&[own[0], other[1], other[0]])),
            other
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
    fn a_state_file_sqlite_creates_while_the_state_is_away_is_given_back_too() {
        let rig = Rig::pg_batch("beside_state_sibling");
        let db = rig.config_path().with_file_name(".rivet_state.db");
        let open = || rusqlite::Connection::open(&db).expect("open the state");
        open()
            .execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t (x);")
            .expect("a WAL state, closed: one file");
        let guard = rig.read_only(Local::State);
        let write = "INSERT INTO t VALUES (1)";
        assert!(open().execute(write, []).is_err());
        assert!(
            db.with_file_name(".rivet_state.db-shm").is_file(),
            "fixture: the refused write left a `-shm` beside the state"
        );
        guard.restore();
        open()
            .execute(write, [])
            .expect("the state takes a write once it is given back");
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
