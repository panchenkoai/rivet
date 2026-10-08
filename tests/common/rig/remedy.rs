//! REMEDY — the one entry point for "refuse, run again, apply each named remedy"
//! ([`Rig::refuses_twice_then`]). A refusal is expected by its registry code and exit class, never
//! by a substring; a remedy is named by the sentence of the refusal text it implements and is
//! applied from a copy of the refused state. Every invocation goes through the rig's one seam, so
//! each refusal is also graded by tests/common/refusal.rs and each delivery by the default oracle.

use super::*;

/// A refusal expected by its registry code and the exit class that code's kind decides.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Refused<'a> {
    /// `None` only for [`Refused::uncoded_known_defect`].
    code: Option<&'a str>,
    exit: i32,
}

impl<'a> Refused<'a> {
    /// The refusal `[code]` with exit class `exit`.
    pub const fn by_code(code: &'a str, exit: i32) -> Self {
        Refused {
            code: Some(code),
            exit,
        }
    }

    /// A refusal that carries no `RIVET_*` code today, which is the known defect `why` names: it must stay uncoded (a code is "now passes"), and each site is counted by an offline ceiling.
    pub fn uncoded_known_defect(exit: i32, why: &str) -> Refused<'static> {
        assert!(
            !why.trim().is_empty(),
            "an uncoded refusal is a known defect: say which code the registry owes it"
        );
        Refused { code: None, exit }
    }

    /// `[CODE] (exit N)` for a verdict line.
    fn named(self) -> String {
        match self.code {
            Some(c) => format!("[{c}] (exit {})", self.exit),
            None => format!("an uncoded refusal, a known defect (exit {})", self.exit),
        }
    }
}

/// Panic unless `out` is the one refusal `want`: for a refusal a cell provokes once (beside a live run).
pub fn assert_refused(out: &std::process::Output, want: Refused) {
    let s = said(out);
    if let Some(why) = not_the_refusal(&[&s], want) {
        panic!(
            "refusal contract: {}\n{}",
            why.replace("cycle 1 ", "the invocation "),
            s.text
        );
    }
}

/// What the invocation must do once a remedy has been applied.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Then<'a> {
    /// Exit 0, graded by the default oracle against the source.
    DeliversTheSource,
    /// Another refusal, by code.
    Refuses(Refused<'a>),
}

/// One remedy the refusal text names: the sentence, the edit that implements it, and its outcome.
pub struct Remedy<'a> {
    sentence: String,
    then: Then<'a>,
    apply: Box<dyn FnOnce(&mut Rig) + 'a>,
    rerun: Option<Vec<String>>,
    rerun_env: Option<Vec<(String, String)>>,
}

impl<'a> Remedy<'a> {
    /// The remedy `sentence` (verbatim from the refusal text) implemented by `apply`: an edit of the config, the source, or the state through a rivet command.
    pub fn new(sentence: &str, then: Then<'a>, apply: impl FnOnce(&mut Rig) + 'a) -> Self {
        assert!(
            !sentence.trim().is_empty(),
            "a remedy is named by the sentence of the refusal text it implements"
        );
        Remedy {
            sentence: sentence.to_string(),
            then,
            apply: Box::new(apply),
            rerun: None,
            rerun_env: None,
        }
    }

    /// Re-run as `argv` instead of the refused invocation, for a remedy that is itself a flag.
    pub fn rerun_as(mut self, argv: &[&str]) -> Self {
        self.rerun = Some(argv.iter().map(|a| a.to_string()).collect());
        self
    }

    /// Re-run under `envs` instead of the refused invocation's, for a remedy that is itself an environment variable.
    pub fn rerun_env(mut self, envs: &[(&str, &str)]) -> Self {
        let owned = |(k, v): &(&str, &str)| (k.to_string(), v.to_string());
        self.rerun_env = Some(envs.iter().map(owned).collect());
        self
    }
}

/// What one invocation said: its exit, the `[RIVET_*]` code and text of its `Error:` line, and everything it printed.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Said {
    pub exit: Option<i32>,
    pub code: Option<String>,
    pub line: String,
    pub text: String,
}

/// Read one finished invocation.
pub(crate) fn said(out: &std::process::Output) -> Said {
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let line = String::from_utf8_lossy(&out.stderr)
        .lines()
        .find(|l| l.starts_with("Error: "))
        .unwrap_or_default()
        .to_string();
    Said {
        exit: out.status.code(),
        code: error_code(&line),
        line,
        text,
    }
}

/// The registry code an `Error: [RIVET_*] ...` line carries.
fn error_code(line: &str) -> Option<String> {
    let rest = line.strip_prefix("Error: [RIVET_")?;
    let code = rest.split(']').next()?;
    let well_formed = !code.is_empty()
        && rest.len() > code.len()
        && code
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    well_formed.then(|| format!("RIVET_{code}"))
}

/// How an invocation ended, for a verdict line.
fn ended(s: &Said) -> String {
    let exit = s
        .exit
        .map_or("a signal".to_string(), |c| format!("exit {c}"));
    match &s.code {
        Some(c) => format!("[{c}] ({exit})"),
        None if s.exit == Some(0) => "success (exit 0)".to_string(),
        None => format!("no RIVET_* code ({exit})"),
    }
}

/// Why `cycles` are not one and the same refusal `want`, else `None`.
pub(crate) fn not_the_refusal(cycles: &[&Said], want: Refused) -> Option<String> {
    for (n, s) in cycles.iter().enumerate() {
        if let (None, Some(code), true) = (want.code, &s.code, s.exit != Some(0)) {
            return Some(format!(
                "known defect now passes: cycle {} carries [{code}]; expect the refusal by code",
                n + 1
            ));
        }
        if s.code.as_deref() != want.code || s.exit != Some(want.exit) {
            return Some(format!(
                "cycle {} ended with {}, expected {}",
                n + 1,
                ended(s),
                want.named()
            ));
        }
    }
    let first = cycles.first()?;
    cycles
        .iter()
        .position(|s| s.line != first.line)
        .map(|n| format!("cycle {} refused in other words than cycle 1", n + 1))
}

/// Collapse every run of whitespace, so a wrapped message still holds its sentence.
fn flat(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The first remedy sentence the refusal text does not hold, else `None`.
pub(crate) fn missing_sentence<'s>(text: &str, sentences: &[&'s str]) -> Option<&'s str> {
    let text = flat(text);
    sentences.iter().copied().find(|s| !text.contains(&flat(s)))
}

/// Why `after` is not the outcome a remedy declared from `refusal`, else `None`; a remedy that left the refusal as it was changed nothing.
pub(crate) fn not_the_outcome(refusal: &Said, after: &Said, then: Then) -> Option<String> {
    if (after.exit, &after.code, &after.line) == (refusal.exit, &refusal.code, &refusal.line) {
        return Some("changed nothing: the invocation refused exactly as before".to_string());
    }
    let (ok, expected) = match then {
        Then::DeliversTheSource => (
            after.exit == Some(0),
            "exit 0 and the source delivered".to_string(),
        ),
        Then::Refuses(want) => (
            after.code.as_deref() == want.code && after.exit == Some(want.exit),
            want.named(),
        ),
    };
    (!ok).then(|| format!("ended with {}, expected {expected}", ended(after)))
}

/// A copy of everything local a rig's runs read and write, and of the rig's builder.
struct RefusedState {
    builder: Rig,
    /// (the original directory or file, its copy).
    trees: Vec<(PathBuf, PathBuf)>,
    _hold: tempfile::TempDir,
}

impl Rig {
    /// This rig's builder over the same directory; its known-defect marker is the original's to answer for.
    fn builder_copy(&self) -> Rig {
        Rig {
            source_type: self.source_type,
            source_url: self.source_url.clone(),
            name: self.name.clone(),
            tables: self.tables.clone(),
            query: self.query.clone(),
            source_lines: self.source_lines.clone(),
            mode: self.mode.clone(),
            format: self.format,
            cdc_lines: self.cdc_lines.clone(),
            extra_lines: self.extra_lines.clone(),
            dest_override: self.dest_override.clone(),
            dest_precreate: self.dest_precreate,
            url_env: self.url_env.clone(),
            extra_exports: self.extra_exports.clone(),
            oracle_container_dir: self.oracle_container_dir.clone(),
            config_dir_override: self.config_dir_override.clone(),
            ckpt_override: self.ckpt_override.clone(),
            cloud_dest: self.cloud_dest.clone(),
            dest_prefix_unslashed: self.dest_prefix_unslashed,
            dest_stdout: self.dest_stdout,
            census_key: self.census_key.clone(),
            oracle_off: self.oracle_off.clone(),
            oracle_xfail: self.oracle_xfail.clone(),
            oracle_xfailed: std::cell::Cell::new(true),
            failed_run_leaves: self.failed_run_leaves.clone(),
            top_lines: self.top_lines.clone(),
            materialized_copies: self.materialized_copies.clone(),
            past_renders: self.past_renders.clone(),
            dir: self.dir.clone(),
        }
    }

    /// Apply a by-value builder edit in place, for a remedy that holds `&mut Rig`.
    pub fn rebuilt(&mut self, edit: impl FnOnce(Rig) -> Rig) {
        let fired = self.oracle_xfailed.replace(true);
        *self = edit(self.builder_copy());
        self.oracle_xfailed.set(fired);
    }

    /// The local directories and files this rig's runs read and write, outermost only.
    fn local_roots(&self) -> Vec<PathBuf> {
        let mut roots = vec![self.dir.path().to_path_buf()];
        roots.extend(self.config_dir_override.clone());
        roots.extend(self.dest_override.clone());
        roots.extend(self.ckpt_override.clone());
        let all = roots.clone();
        roots.retain(|r| !all.iter().any(|o| o != r && r.starts_with(o)));
        roots.dedup();
        roots
    }

    /// Copy the state the last invocation left.
    fn copy_refused_state(&self) -> RefusedState {
        let hold = tempfile::tempdir().expect("a directory for the refused state");
        let trees = self
            .local_roots()
            .into_iter()
            .enumerate()
            .map(|(i, root)| {
                let copy = hold.path().join(i.to_string());
                copy_files(&root, &copy);
                (root, copy)
            })
            .collect();
        RefusedState {
            builder: self.builder_copy(),
            trees,
            _hold: hold,
        }
    }

    /// Put the copied state back: the files, and the builder that rendered the config among them.
    fn put_back(&mut self, state: &RefusedState) {
        for (root, copy) in &state.trees {
            if root.is_dir() {
                std::fs::remove_dir_all(root).expect("clear the rig's directory");
            } else {
                let _ = std::fs::remove_file(root);
            }
            copy_files(copy, root);
        }
        let old = std::mem::replace(self, state.builder.builder_copy());
        self.oracle_xfailed.set(old.oracle_xfailed.replace(true));
        *self.past_renders.borrow_mut() = old.past_renders.take();
    }

    /// Refuse `argv` (`run`, `load`, ...) twice with the same exit, code and error line; then, for each remedy, put the refused state back, apply it and require the outcome it declares. Returns the refusal text.
    pub fn refuses_twice_then(
        &mut self,
        argv: &[&str],
        envs: &[(&str, &str)],
        want: Refused,
        remedies: Vec<Remedy>,
    ) -> String {
        let first = said(&self.cli_env(argv, envs));
        let second = said(&self.cli_env(argv, envs));
        if let Some(why) = not_the_refusal(&[&first, &second], want) {
            panic!(
                "refuse-then-remedy: {why}\n--- cycle 1:\n{}\n--- cycle 2:\n{}",
                first.text, second.text
            );
        }
        let sentences: Vec<&str> = remedies.iter().map(|r| r.sentence.as_str()).collect();
        if let Some(gone) = missing_sentence(&first.text, &sentences) {
            panic!(
                "refuse-then-remedy: the refusal no longer says `{gone}`: the remedy cell is stale ({})\n--- the refusal:\n{}",
                want.named(),
                first.text
            );
        }
        let delivers = remedies.iter().any(|r| r.then == Then::DeliversTheSource);
        assert!(
            !(delivers && self.oracle_off.is_some()),
            "refuse-then-remedy: `DeliversTheSource` is graded by the default oracle, which this rig turned off"
        );
        let state_url = envs
            .iter()
            .find(|(k, _)| *k == "RIVET_STATE_URL")
            .map(|(_, v)| v.to_string())
            .or_else(crate::common::state_url_under_test);
        let uncopied = if self.cloud_dest.is_some() {
            Some("a cloud destination")
        } else if state_url.is_some_and(|u| u.starts_with("postgres")) {
            Some("a Postgres state")
        } else {
            None
        };
        let state = self.copy_refused_state();
        for (n, remedy) in remedies.into_iter().enumerate() {
            let Remedy {
                sentence,
                then,
                apply,
                rerun,
                rerun_env,
            } = remedy;
            if n > 0 {
                if let Some(what) = uncopied {
                    crate::common::skip_live(&format!(
                        "refuse-then-remedy: remedy `{sentence}` not applied, {what} cannot be copied back to the refused state"
                    ));
                    continue;
                }
                self.put_back(&state);
                let again = said(&self.cli_env(argv, envs));
                if let Some(why) = not_the_refusal(&[&first, &again], want) {
                    panic!(
                        "refuse-then-remedy: the refused state did not come back before remedy `{sentence}` (an earlier remedy changed the source?): {why}\n{}",
                        again.text
                    );
                }
            }
            apply(self);
            let rerun: Option<Vec<&str>> = rerun
                .as_ref()
                .map(|a| a.iter().map(String::as_str).collect());
            let rerun_env: Option<Vec<(&str, &str)>> = rerun_env
                .as_ref()
                .map(|e| e.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect());
            let after = said(&self.cli_env(
                rerun.as_deref().unwrap_or(argv),
                rerun_env.as_deref().unwrap_or(envs),
            ));
            if let Some(why) = not_the_outcome(&first, &after, then) {
                panic!(
                    "refuse-then-remedy: remedy `{sentence}` {why}\n{}",
                    after.text
                );
            }
        }
        first.text
    }
}

/// Copy `from` (a directory's files, or one file) to `to`.
fn copy_files(from: &Path, to: &Path) {
    let files = if from.is_dir() {
        std::fs::create_dir_all(to).expect("the copy's directory");
        crate::common::runner::files_under(from)
    } else if from.is_file() {
        vec![from.to_path_buf()]
    } else {
        Vec::new()
    };
    for f in files {
        let dst = match f.strip_prefix(from) {
            Ok(rel) if !rel.as_os_str().is_empty() => to.join(rel),
            _ => to.to_path_buf(),
        };
        std::fs::create_dir_all(dst.parent().expect("a file has a parent")).expect("mkdir");
        std::fs::copy(&f, &dst).unwrap_or_else(|e| panic!("copy {}: {e}", f.display()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WANT: Refused = Refused::by_code("RIVET_STATE_RUN_IN_PROGRESS", 5);

    fn refusal(exit: i32, stderr: &str) -> Said {
        use std::os::unix::process::ExitStatusExt as _;
        said(&std::process::Output {
            status: std::process::ExitStatus::from_raw(exit << 8),
            stdout: b"summary\n".to_vec(),
            stderr: stderr.as_bytes().to_vec(),
        })
    }

    const CODED: &str = "WARN something\nError: [RIVET_STATE_RUN_IN_PROGRESS] export 'x' is running. Wait for it, or run `rivet state reset-chunks`.\n";

    #[test]
    fn two_identical_coded_refusals_are_the_refusal() {
        let (a, b) = (refusal(5, CODED), refusal(5, CODED));
        assert_eq!(a.code.as_deref(), WANT.code);
        assert_eq!(not_the_refusal(&[&a, &b], WANT), None);
    }

    #[test]
    fn a_second_cycle_that_passes_is_not_a_refusal_that_holds() {
        let (a, b) = (refusal(5, CODED), refusal(0, ""));
        let why = not_the_refusal(&[&a, &b], WANT).expect("the second cycle passed");
        assert_eq!(
            why,
            "cycle 2 ended with success (exit 0), expected [RIVET_STATE_RUN_IN_PROGRESS] (exit 5)"
        );
    }

    #[test]
    fn the_right_text_with_no_code_is_not_the_refusal() {
        let uncoded = CODED.replace("[RIVET_STATE_RUN_IN_PROGRESS] ", "");
        let (a, b) = (refusal(5, &uncoded), refusal(5, &uncoded));
        let why = not_the_refusal(&[&a, &b], WANT).expect("no code");
        assert_eq!(
            why,
            "cycle 1 ended with no RIVET_* code (exit 5), expected [RIVET_STATE_RUN_IN_PROGRESS] (exit 5)"
        );
    }

    #[test]
    fn an_uncoded_known_defect_holds_only_while_the_refusal_stays_uncoded() {
        let uncoded = CODED.replace("[RIVET_STATE_RUN_IN_PROGRESS] ", "");
        let known =
            Refused::uncoded_known_defect(1, "the registry owes it RIVET_STATE_RUN_IN_PROGRESS");
        let (a, b) = (refusal(1, &uncoded), refusal(1, &uncoded));
        assert_eq!(not_the_refusal(&[&a, &b], known), None);
        let fixed = refusal(5, CODED);
        assert_eq!(
            not_the_refusal(&[&fixed, &fixed], known).as_deref(),
            Some(
                "known defect now passes: cycle 1 carries [RIVET_STATE_RUN_IN_PROGRESS]; expect the refusal by code"
            )
        );
        let passed = refusal(0, "");
        assert_eq!(
            not_the_refusal(&[&a, &passed], known).as_deref(),
            Some(
                "cycle 2 ended with success (exit 0), expected an uncoded refusal, a known defect (exit 1)"
            )
        );
    }

    #[test]
    #[should_panic(
        expected = "refusal contract: the invocation ended with no RIVET_* code (exit 1), expected [RIVET_STATE_RUN_IN_PROGRESS] (exit 5)"
    )]
    fn one_refusal_is_held_to_its_code_too() {
        use std::os::unix::process::ExitStatusExt as _;
        assert_refused(
            &std::process::Output {
                status: std::process::ExitStatus::from_raw(1 << 8),
                stdout: Vec::new(),
                stderr: b"Error: export 'x' is running\n".to_vec(),
            },
            WANT,
        );
    }

    #[test]
    fn the_right_code_under_another_exit_or_other_words_is_not_the_refusal() {
        let (a, b) = (refusal(1, CODED), refusal(1, CODED));
        assert!(not_the_refusal(&[&a, &b], WANT).is_some());
        let reworded = CODED.replace("is running", "runs elsewhere");
        let (a, b) = (refusal(5, CODED), refusal(5, &reworded));
        assert_eq!(
            not_the_refusal(&[&a, &b], WANT).as_deref(),
            Some("cycle 2 refused in other words than cycle 1")
        );
    }

    #[test]
    fn a_code_is_read_only_from_a_bracketed_prefix_of_the_error_line() {
        for line in [
            "Error: RIVET_STATE_RUN_IN_PROGRESS happened",
            "Error: see [RIVET_STATE_RUN_IN_PROGRESS]",
            "Error: [RIVET_state] x",
            "Error: [RIVET_",
            "WARN [RIVET_STATE_RUN_IN_PROGRESS] x",
        ] {
            assert_eq!(refusal(5, line).code, None, "{line}");
        }
    }

    #[test]
    fn a_remedy_sentence_the_text_lost_is_reported() {
        let text = refusal(5, CODED).text;
        let kept = "Wait for it, or run `rivet state reset-chunks`.";
        assert_eq!(missing_sentence(&text, &[kept]), None);
        assert_eq!(
            missing_sentence(&text.replace('\n', "\n   "), &[kept]),
            None
        );
        let gone = "Pass --force to override";
        assert_eq!(missing_sentence(&text, &[kept, gone]), Some(gone));
    }

    #[test]
    fn a_remedy_that_changes_nothing_fails_whatever_outcome_it_declared() {
        let before = refusal(5, CODED);
        for then in [Then::DeliversTheSource, Then::Refuses(WANT)] {
            let why = not_the_outcome(&before, &refusal(5, CODED), then).expect("a no-op remedy");
            assert!(why.starts_with("changed nothing"), "{why}");
        }
    }

    #[test]
    fn a_remedy_is_held_to_the_outcome_it_declared() {
        let before = refusal(5, CODED);
        let other = Refused::by_code("RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH", 5);
        let blocked = refusal(
            5,
            "Error: [RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH] no\n",
        );
        assert_eq!(
            not_the_outcome(&before, &refusal(0, ""), Then::DeliversTheSource),
            None
        );
        assert_eq!(
            not_the_outcome(&before, &blocked, Then::Refuses(other)),
            None
        );
        assert_eq!(
            not_the_outcome(&before, &blocked, Then::DeliversTheSource).as_deref(),
            Some(
                "ended with [RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH] (exit 5), expected exit 0 and the source delivered"
            )
        );
        assert_eq!(
            not_the_outcome(&before, &refusal(0, ""), Then::Refuses(other)).as_deref(),
            Some(
                "ended with success (exit 0), expected [RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH] (exit 5)"
            )
        );
    }

    #[test]
    fn the_refused_state_comes_back_files_and_builder() {
        let ckpt = tempfile::tempdir().unwrap();
        let mut rig = Rig::pg_batch("remedy_copy")
            .export_line("chunk_size: 5")
            .checkpoint_path(ckpt.path().join("x.ckpt"));
        let cfg = rig.config_path();
        let refused_yaml = std::fs::read_to_string(&cfg).unwrap();
        std::fs::write(rig.out_dir().join("part-0.parquet"), b"refused").unwrap();
        std::fs::write(rig.checkpoint(), b"anchor").unwrap();
        let state = rig.copy_refused_state();

        rig.replace_export_line("chunk_size", "chunk_size: 9");
        assert_ne!(
            std::fs::read_to_string(rig.config_path()).unwrap(),
            refused_yaml
        );
        std::fs::write(rig.out_dir().join("part-0.parquet"), b"remedied").unwrap();
        std::fs::write(rig.out_dir().join("part-1.parquet"), b"new").unwrap();
        std::fs::remove_file(rig.checkpoint()).unwrap();

        rig.put_back(&state);
        assert_eq!(
            std::fs::read_to_string(rig.config_path()).unwrap(),
            refused_yaml
        );
        assert_eq!(
            std::fs::read(rig.out_dir().join("part-0.parquet")).unwrap(),
            b"refused"
        );
        assert!(!rig.out_dir().join("part-1.parquet").exists());
        assert_eq!(std::fs::read(rig.checkpoint()).unwrap(), b"anchor");
    }

    #[test]
    fn a_known_defect_marker_survives_the_copy_and_answers_once() {
        let mut rig = Rig::pg_batch("remedy_marker");
        rig.oracle_xfail = Some(("delivered-only rows".into(), "a probe marker".into()));
        rig.config_path();
        let state = rig.copy_refused_state();
        rig.oracle_xfailed.set(true);
        rig.put_back(&state);
        assert!(rig.oracle_xfailed.get(), "a fired marker stays fired");
        assert!(rig.oracle_xfail.is_some());
    }
}
