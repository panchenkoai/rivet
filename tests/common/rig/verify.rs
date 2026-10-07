//! VERIFY — the rig's markers on the default oracle (tests/common/verify.rs): a rig is
//! one more caller of the config-derived path; it only adds an opt-out, a strict
//! known-defect marker and a key for a keyless relation.

use super::*;

impl Rig {
    /// Opt this rig out of the default oracle; the reason is required and counted by an offline ceiling.
    pub fn no_oracle(mut self, reason: &str) -> Self {
        assert!(
            !reason.trim().is_empty(),
            "no_oracle needs a reason — say why this rig's output must not be graded"
        );
        self.oracle_off = Some(reason.to_string());
        self
    }

    /// Declare what this rig's runs that do not exit 0 may leave behind besides their own failure record; the reason is required and each site is counted by an offline ceiling.
    pub fn a_failed_run_may_leave(mut self, what: &[crate::common::Leftover], why: &str) -> Self {
        assert!(
            !why.trim().is_empty(),
            "a_failed_run_may_leave needs a reason — say why the failure legitimately leaves this"
        );
        crate::common::refusal::assert_declarable(what);
        self.failed_run_leaves.extend_from_slice(what);
        self
    }

    /// A known product defect on this rig's own export: only failures of `class` (a name in rig_oracle.KNOWN_DEFECT_CLASSES, or `a failed run left: <kind>` for a run that does not exit 0) are excused, any other FAILs, and a rig that never shows the class fails with "now passes".
    pub fn oracle_known_defect(mut self, class: &str, reason: &str) -> Self {
        assert!(
            !reason.trim().is_empty(),
            "oracle_known_defect needs a reason — name the defect and the step that fixes it"
        );
        self.oracle_xfail = Some((class.to_string(), reason.to_string()));
        self
    }

    /// Run once with a known product defect excused for THIS run only: the run must show it, and every later run is graded strictly.
    pub fn run_ok_capture_known_defect(&mut self, class: &str, reason: &str) -> String {
        assert!(
            !reason.trim().is_empty() && self.oracle_xfail.is_none(),
            "run_ok_capture_known_defect needs a reason and a rig without a rig-wide marker"
        );
        self.oracle_xfail = Some((class.to_string(), reason.to_string()));
        self.oracle_xfailed.set(false);
        let said = self.run_ok_capture();
        let fired = self.oracle_xfailed.replace(false);
        self.oracle_xfail = None;
        assert!(
            fired,
            "oracle known defect now passes — remove the marker: {reason}"
        );
        said
    }

    /// Pin this MySQL CDC rig's stream at the server's current binlog position: its checkpoint, and the oracle's anchor image at that position.
    pub fn pin_binlog_here(&self) {
        use mysql::prelude::Queryable as _;
        assert_eq!(
            self.source_type, "mysql",
            "a binlog pin is a MySQL checkpoint"
        );
        let ckpt = self.checkpoint();
        let _ = std::fs::remove_file(&ckpt);
        let mut c = mysql::Pool::new(self.source_url.as_str())
            .expect("mysql pool")
            .get_conn()
            .expect("mysql conn");
        // `SHOW BINARY LOG STATUS` from 8.2 on (8.4 removed the old form), else `SHOW MASTER STATUS`.
        let row: mysql::Row = c
            .query_first("SHOW BINARY LOG STATUS")
            .or_else(|_| c.query_first("SHOW MASTER STATUS"))
            .expect("binlog status")
            .expect("binlog enabled");
        let (file, pos): (String, u64) = (row.get(0).unwrap(), row.get(1).unwrap());
        if self.oracle_off.is_none() {
            crate::common::verify::anchor_streams_here(&self.config_path());
        }
        std::fs::write(&ckpt, format!(r#"{{"file":"{file}","pos":{pos}}}"#))
            .expect("write the checkpoint");
    }

    /// Move this rig's CDC checkpoint `from` -> `to` for runs from `cwd`; the oracle's stream follows the file.
    pub fn move_checkpoint(&self, from: &Path, to: &Path, cwd: &Path) {
        crate::common::verify::move_checkpoint(&self.config_path(), cwd, from, to);
    }

    /// Start grading `argv` through the config-derived oracle, unless this rig opted out.
    pub(crate) fn oracle_begin(
        &self,
        argv: &[String],
        envs: &[(&str, &str)],
        cwd: Option<&Path>,
    ) -> Option<crate::common::verify::Case> {
        if let Some(why) = &self.oracle_off {
            if matches!(
                argv.first().map(String::as_str),
                Some("run" | "load" | "compact" | "apply")
            ) {
                crate::common::verify::log("OFF", &self.name, why);
            }
            return None;
        }
        crate::common::verify::begin(argv, envs, cwd)
    }

    /// Grade a finished invocation by how it exited; records an expected known-defect disagreement.
    pub(crate) fn oracle_settle(
        &self,
        case: Option<crate::common::verify::Case>,
        status: std::process::ExitStatus,
        stdout: &[u8],
        envs: &[(&str, &str)],
    ) {
        let Some(case) = case else { return };
        let opts = crate::common::verify::Opts {
            xfail: self.oracle_xfail.as_ref().map(|(class, reason)| {
                crate::common::verify::KnownDefect {
                    export: &self.name,
                    class,
                    reason,
                }
            }),
            key: self.census_key.as_deref(),
            leaves: &self.failed_run_leaves,
        };
        if crate::common::verify::settle(case, status, stdout, envs, &opts) {
            self.oracle_xfailed.set(true);
        }
    }
}

impl Drop for Rig {
    /// A known-defect marker whose rig never disagreed fails the test: the defect is fixed and the marker must go.
    fn drop(&mut self) {
        if let Some((_, why)) = &self.oracle_xfail
            && !self.oracle_xfailed.get()
            && !std::thread::panicking()
        {
            panic!("oracle known defect now passes — remove the marker: {why}");
        }
    }
}

#[test]
#[should_panic(expected = "oracle known defect now passes")]
fn a_known_defect_marker_that_never_fired_fails_the_test_at_drop() {
    let rig = Rig::pg_batch("never_run").oracle_known_defect(
        "delivered-only rows",
        "a marker on a rig that never disagreed",
    );
    drop(rig);
}

/// A rig, its begun `run`, and one file written into its destination while the run was "running"; settle it with `exit`.
#[cfg(test)]
fn settle_after_writing(rig: &Rig, file: &str, exit: i32, envs: &[(&str, &str)]) {
    use std::os::unix::process::ExitStatusExt as _;
    let argv = ["run", "--config", &rig.config_path().display().to_string()].map(String::from);
    std::fs::create_dir_all(rig.out_dir()).expect("the rig's destination");
    let case = rig.oracle_begin(&argv, envs, None);
    assert!(case.is_some(), "a rig's `run` is graded");
    std::fs::write(rig.out_dir().join(file), b"written by the failed run")
        .expect("write the leftover");
    rig.oracle_settle(
        case,
        std::process::ExitStatus::from_raw(exit << 8),
        &[],
        envs,
    );
}

#[test]
#[should_panic(expected = "- orphan-part: ")]
fn a_part_left_by_a_run_that_did_not_exit_0_fails_the_test() {
    settle_after_writing(&Rig::pg_batch("refused_part"), "part-0.parquet", 1, &[]);
}

#[test]
#[should_panic(expected = "- success-marker: ")]
fn a_success_marker_left_by_a_run_that_did_not_exit_0_fails_the_test() {
    settle_after_writing(&Rig::pg_batch("refused_marker"), "_SUCCESS", 3, &[]);
}

#[test]
fn a_declared_leftover_a_known_defect_and_a_crash_the_test_caused_do_not_fail_the_test() {
    let declared = Rig::pg_batch("refused_declared")
        .a_failed_run_may_leave(&[crate::common::Leftover::OrphanPart], "a probe");
    settle_after_writing(&declared, "part-0.parquet", 1, &[]);
    let known = Rig::pg_batch("refused_known")
        .oracle_known_defect("a failed run left: success-marker", "a probe marker");
    settle_after_writing(&known, "_SUCCESS", 1, &[]);
    assert!(
        known.oracle_xfailed.get(),
        "the marker fired on the refusal"
    );
    let crashed = Rig::pg_batch("refused_crashed");
    let fault = [("RIVET_TEST_PANIC_AT", "after_part_write")];
    settle_after_writing(&crashed, "part-0.parquet", 101, &fault);
}

#[test]
#[should_panic(expected = "- orphan-part: ")]
fn a_known_defect_of_one_kind_does_not_excuse_a_part() {
    let rig = Rig::pg_batch("refused_other_kind")
        .oracle_known_defect("a failed run left: success-marker", "a probe marker");
    rig.oracle_xfailed.set(true);
    settle_after_writing(&rig, "part-0.parquet", 1, &[]);
}
