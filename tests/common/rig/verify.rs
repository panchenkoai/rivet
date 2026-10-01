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

    /// A known product defect the oracle must keep catching: a disagreement is expected, and a rig whose graded runs all agree fails with "now passes".
    pub fn oracle_known_defect(mut self, reason: &str) -> Self {
        assert!(
            !reason.trim().is_empty(),
            "oracle_known_defect needs a reason — name the defect and the step that fixes it"
        );
        self.oracle_xfail = Some(reason.to_string());
        self
    }

    /// Start grading `argv` through the config-derived oracle, unless this rig opted out.
    pub(crate) fn oracle_begin(
        &self,
        argv: &[String],
        envs: &[(&str, &str)],
        cwd: Option<&Path>,
    ) -> Option<crate::common::verify::Case> {
        if self.oracle_off.is_some() {
            return None;
        }
        crate::common::verify::begin(argv, envs, cwd)
    }

    /// Grade a successful invocation; records an expected known-defect disagreement.
    pub(crate) fn oracle_finish(&self, case: crate::common::verify::Case, envs: &[(&str, &str)]) {
        let opts = crate::common::verify::Opts {
            xfail: self.oracle_xfail.as_deref(),
            key: self.census_key.as_deref(),
        };
        if crate::common::verify::finish(case, envs, &opts) {
            self.oracle_xfailed.set(true);
        }
    }
}

impl Drop for Rig {
    /// A known-defect marker whose rig never disagreed fails the test: the defect is fixed and the marker must go.
    fn drop(&mut self) {
        if let Some(why) = &self.oracle_xfail
            && !self.oracle_xfailed.get()
            && !std::thread::panicking()
        {
            panic!("oracle known defect now passes — remove the marker: {why}");
        }
    }
}
