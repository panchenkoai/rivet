//! SABOTAGE — damage to what a run reads about itself (docs/sabotage-matrix.yaml): the manifest,
//! marker and parts of a local destination, and the export's rows in the state database. A
//! primitive panics unless the damage happened, so a cell cannot grade an export nobody touched;
//! [`Rig::delivers_or_refuses`] is the one grade after it: exit 0 through the default oracle, or
//! the same coded refusal twice. `rivet validate` reports instead of refusing: [`Rig::validate_fails`]
//! holds it to exit 3 and the `RIVET_VERIFY_*` finding in its report, twice.

use super::remedy::{Said, said};
use super::*;

/// One way to damage a file of a destination.
pub enum Damage<'a> {
    /// Keep the first half of its bytes.
    Truncated,
    /// Replace it with text that is not JSON.
    NotJson,
    /// Delete it.
    Removed,
    /// Replace its bytes with another file's.
    ReplacedBy(&'a Path),
    /// Rewrite it as the JSON document this edit leaves.
    Json(&'a dyn Fn(&mut serde_json::Value)),
}

/// How an invocation after a sabotage ended.
#[derive(Clone, Debug, PartialEq)]
pub enum Survived {
    /// Exit 0, graded by the default oracle against the source.
    Delivered,
    /// The same coded refusal twice: everything the first printed.
    Refused(String),
}

/// The bytes `how` leaves of a file holding `before`, `None` for a removal; `Err` when it would change nothing.
pub(crate) fn damaged(before: &[u8], how: &Damage) -> Result<Option<Vec<u8>>, String> {
    let after = match how {
        Damage::Removed => return Ok(None),
        Damage::Truncated => before[..before.len() / 2].to_vec(),
        Damage::NotJson => b"{not json".to_vec(),
        Damage::ReplacedBy(other) => {
            std::fs::read(other).map_err(|e| format!("read {}: {e}", other.display()))?
        }
        Damage::Json(edit) => {
            let was: serde_json::Value =
                serde_json::from_slice(before).map_err(|e| format!("not JSON before: {e}"))?;
            let mut doc = was.clone();
            edit(&mut doc);
            if doc == was {
                return Err("the edit left the same document".to_string());
            }
            serde_json::to_vec_pretty(&doc).expect("JSON")
        }
    };
    if after == before {
        return Err("the damage left the same bytes".to_string());
    }
    Ok(Some(after))
}

/// Why two runs of one invocation are neither a delivery nor one refusal, else how it ended.
pub(crate) fn survived(first: &Said, second: Option<&Said>) -> Result<Survived, String> {
    let exit = |s: &Said| {
        s.exit
            .map_or("a signal".to_string(), |c| format!("exit {c}"))
    };
    match (first.exit, second) {
        (Some(0), _) => Ok(Survived::Delivered),
        (Some(1..=5), _) if first.code.is_none() => Err(format!(
            "refused with no RIVET_* code ({}): a refusal is expected by its registry code",
            exit(first)
        )),
        (Some(1..=5), Some(again)) => {
            if (again.exit, &again.code) == (first.exit, &first.code)
                && super::remedy::same_words(&again.line, &first.line)
            {
                Ok(Survived::Refused(first.text.clone()))
            } else {
                Err(format!(
                    "refused with {}, then ended otherwise ({}) on the same input",
                    exit(first),
                    exit(again)
                ))
            }
        }
        (Some(1..=5), None) => Err("a refusal is read twice".to_string()),
        _ => Err(format!(
            "neither a delivery nor a refusal: ended with {}",
            exit(first)
        )),
    }
}

/// How an invocation ended once a resource was taken away under it or beside it.
#[derive(Clone, Debug, PartialEq)]
pub enum Stopped {
    /// Exit 0, graded by the default oracle against the source.
    Delivered,
    /// A loud failure: everything it printed.
    Failed(String),
}

/// Why an invocation is neither a delivery nor a loud failure (exit 1 to 5 with an `Error:` line), else how it ended.
pub(crate) fn stopped(s: &Said) -> Result<Stopped, String> {
    match s.exit {
        Some(0) => Ok(Stopped::Delivered),
        Some(1..=5) if s.line.is_empty() => Err(format!(
            "failed silently: exit {} with no `Error:` line",
            s.exit.unwrap_or_default()
        )),
        Some(1..=5) => Ok(Stopped::Failed(s.text.clone())),
        Some(c) => Err(format!(
            "neither a delivery nor a loud failure: ended with exit {c}"
        )),
        None => Err("neither a delivery nor a loud failure: ended by a signal".to_string()),
    }
}

/// Why two `rivet validate` reports are not one failed verification naming `finding`, else `None`.
pub(crate) fn not_a_failed_validation(
    first: &Said,
    second: &Said,
    finding: &str,
) -> Option<String> {
    for (n, s) in [first, second].into_iter().enumerate() {
        if s.exit != Some(3) {
            let exit = s
                .exit
                .map_or("a signal".to_string(), |c| format!("exit {c}"));
            return Some(format!(
                "report {} ended with {exit}, expected exit 3 (integrity)",
                n + 1
            ));
        }
        if !s.text.contains(&format!("[{finding}")) {
            return Some(format!("report {} does not name [{finding}", n + 1));
        }
    }
    None
}

/// Why a state edit that changed `changed` rows is not the damage a cell declared, else `None`.
pub(crate) fn not_the_edit(sql: &str, changed: u64, rows: u64) -> Option<String> {
    (changed != rows).then(|| {
        format!(
            "sabotage: `{sql}` changed {changed} row(s), expected {rows}: the state was not damaged as the cell says"
        )
    })
}

impl Rig {
    /// The part files `manifest.json` of this rig's local destination names, in manifest order.
    pub fn manifest_parts(&self) -> Vec<PathBuf> {
        let out = self.out_dir();
        let doc: serde_json::Value = serde_json::from_slice(
            &std::fs::read(out.join("manifest.json")).expect("fixture: manifest.json"),
        )
        .expect("fixture: a JSON manifest");
        let parts: Vec<PathBuf> = doc["parts"]
            .as_array()
            .expect("fixture: manifest parts")
            .iter()
            .map(|p| out.join(p["path"].as_str().expect("a part path")))
            .collect();
        assert!(!parts.is_empty(), "fixture: the manifest names a part");
        parts
    }

    /// Damage `file`; panics unless it was there and is different afterwards.
    pub fn damage(&self, file: &Path, how: Damage) {
        let before = std::fs::read(file)
            .unwrap_or_else(|e| panic!("sabotage: {} is not there to damage: {e}", file.display()));
        match damaged(&before, &how) {
            Err(why) => panic!("sabotage: {}: {why}", file.display()),
            Ok(None) => std::fs::remove_file(file).expect("remove"),
            Ok(Some(after)) => std::fs::write(file, after).expect("rewrite"),
        }
    }

    /// Run one statement on the state database this rig's runs use (`{export}` is the export's name); panics unless it changed exactly `rows` rows.
    pub fn edit_state(&self, sql: &str, rows: u64) {
        let sql = sql.replace("{export}", &self.name);
        let changed = match crate::common::state_url_under_test() {
            Some(url) => postgres::Client::connect(&url, postgres::NoTls)
                .expect("connect to the Postgres state (RIVET_GATE_STATE_URL)")
                .execute(sql.as_str(), &[])
                .unwrap_or_else(|e| panic!("sabotage: `{sql}`: {e}")),
            None => {
                rusqlite::Connection::open(self.config_path().with_file_name(".rivet_state.db"))
                    .expect("open the SQLite state")
                    .execute(&sql, [])
                    .unwrap_or_else(|e| panic!("sabotage: `{sql}`: {e}")) as u64
            }
        };
        if let Some(why) = not_the_edit(&sql, changed, rows) {
            panic!("{why}");
        }
    }

    /// One count read from the state database this rig's runs use (`{export}` is the export's name).
    pub fn state_count(&self, sql: &str) -> i64 {
        let sql = sql.replace("{export}", &self.name);
        match crate::common::state_url_under_test() {
            Some(url) => postgres::Client::connect(&url, postgres::NoTls)
                .expect("connect to the Postgres state (RIVET_GATE_STATE_URL)")
                .query_one(sql.as_str(), &[])
                .unwrap_or_else(|e| panic!("state read: `{sql}`: {e}"))
                .get(0),
            None => {
                rusqlite::Connection::open(self.config_path().with_file_name(".rivet_state.db"))
                    .expect("open the SQLite state")
                    .query_row(&sql, [], |r| r.get(0))
                    .unwrap_or_else(|e| panic!("state read: `{sql}`: {e}"))
            }
        }
    }

    /// `rivet validate` on this rig's export fails twice with exit 3 and names the `RIVET_VERIFY_*` `finding` in its report.
    pub fn validate_fails(&self, finding: &str) {
        let first = said(&self.cli(&["validate"]));
        let second = said(&self.cli(&["validate"]));
        if let Some(why) = not_a_failed_validation(&first, &second, finding) {
            panic!("`rivet validate`: {why}\n{}", first.text);
        }
    }

    /// Grade a finished invocation a resource was taken from: exit 0 (graded by the default oracle at the seam) or a loud failure; a crash, an internal error or a silent failure panics.
    pub fn delivered_or_failed_loudly(&self, what: &str, out: &std::process::Output) -> Stopped {
        let s = said(out);
        let how = stopped(&s).unwrap_or_else(|why| panic!("{what} {why}\n{}", s.text));
        eprintln!("sabotage: {what} ended with {:?}: {}", s.exit, s.line);
        how
    }

    /// Grade a finished invocation that met another process: exit 0 (graded by the default oracle at the seam) or a refusal by its registry code; anything else panics.
    pub fn delivered_or_refused(&self, what: &str, out: &std::process::Output) -> Survived {
        let s = said(out);
        let how = survived(&s, Some(&s)).unwrap_or_else(|why| panic!("{what} {why}\n{}", s.text));
        eprintln!("sabotage: {what} ended with {:?}: {}", s.exit, s.line);
        how
    }

    /// Run `argv` with the resource still away, then [`Rig::delivered_or_failed_loudly`].
    pub fn delivers_or_fails_loudly(&self, argv: &[&str], envs: &[(&str, &str)]) -> Stopped {
        let what = format!("`rivet {}`", argv.join(" "));
        self.delivered_or_failed_loudly(&what, &self.cli_env(argv, envs))
    }

    /// Run `argv` after a sabotage: exit 0 (graded by the default oracle) or the same coded refusal twice; anything else panics.
    pub fn delivers_or_refuses(&self, argv: &[&str], envs: &[(&str, &str)]) -> Survived {
        let first = said(&self.cli_env(argv, envs));
        let second = (first.exit != Some(0)).then(|| said(&self.cli_env(argv, envs)));
        survived(&first, second.as_ref()).unwrap_or_else(|why| {
            panic!(
                "after the sabotage `rivet {}` {why}\n--- first:\n{}\n--- second:\n{}",
                argv.join(" "),
                first.text,
                second.map(|s| s.text).unwrap_or_default()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ended(exit: i32, stderr: &str) -> Said {
        use std::os::unix::process::ExitStatusExt as _;
        said(&std::process::Output {
            status: std::process::ExitStatus::from_raw(exit << 8),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        })
    }

    #[test]
    fn exit_0_is_a_delivery_and_one_refusal_twice_is_a_refusal() {
        assert_eq!(survived(&ended(0, ""), None), Ok(Survived::Delivered));
        let no = ended(5, "Error: [RIVET_STATE_SCHEMA_NEWER] no\n");
        assert_eq!(
            survived(&no, Some(&no)),
            Ok(Survived::Refused(no.text.clone()))
        );
    }

    #[test]
    fn a_crash_an_internal_error_or_a_refusal_that_does_not_repeat_is_neither() {
        for exit in [6, 101] {
            let crash = ended(exit, "Error: boom\n");
            assert_eq!(
                survived(&crash, Some(&crash)),
                Err(format!(
                    "neither a delivery nor a refusal: ended with exit {exit}"
                ))
            );
        }
        let uncoded = ended(1, "Error: db error: invalid input syntax\n");
        assert_eq!(
            survived(&uncoded, Some(&uncoded)),
            Err(
                "refused with no RIVET_* code (exit 1): a refusal is expected by its registry code"
                    .to_string()
            )
        );
        let no = ended(5, "Error: [RIVET_STATE_SCHEMA_NEWER] no\n");
        assert_eq!(
            survived(&no, Some(&ended(0, ""))),
            Err("refused with exit 5, then ended otherwise (exit 0) on the same input".to_string())
        );
        assert!(survived(&no, Some(&ended(5, "Error: other words\n"))).is_err());
        assert!(survived(&no, None).is_err());
    }

    #[test]
    fn a_refusal_that_names_another_run_id_is_the_same_refusal() {
        let gone = |run: &str| {
            ended(
                5,
                &format!(
                    "Error: [RIVET_STATE_CHUNK_CHECKPOINT_GONE] chunk checkpoint run 't_{run}' has no row\n"
                ),
            )
        };
        let (a, b) = (
            gone("20261008T074244.506_29062"),
            gone("20261008T074244.802_29063"),
        );
        assert_eq!(
            survived(&a, Some(&b)),
            Ok(Survived::Refused(a.text.clone()))
        );
        let other = ended(
            5,
            "Error: [RIVET_STATE_CHUNK_CHECKPOINT_GONE] chunk checkpoint run 'u_20261008T074244.802_29063' has no row\n",
        );
        assert!(survived(&a, Some(&other)).is_err());
    }

    #[test]
    fn a_run_a_resource_was_taken_from_delivers_or_fails_loudly() {
        assert_eq!(stopped(&ended(0, "")), Ok(Stopped::Delivered));
        for exit in 1..=5 {
            let loud = ended(exit, "Error: Permission denied (os error 13)\n");
            assert_eq!(stopped(&loud), Ok(Stopped::Failed(loud.text.clone())));
        }
        assert_eq!(
            stopped(&ended(1, "warning: could not write\n")),
            Err("failed silently: exit 1 with no `Error:` line".to_string())
        );
        for exit in [6, 101] {
            assert_eq!(
                stopped(&ended(exit, "Error: boom\n")),
                Err(format!(
                    "neither a delivery nor a loud failure: ended with exit {exit}"
                ))
            );
        }
        use std::os::unix::process::ExitStatusExt as _;
        let killed = said(&std::process::Output {
            status: std::process::ExitStatus::from_raw(9),
            stdout: Vec::new(),
            stderr: Vec::new(),
        });
        assert_eq!(
            stopped(&killed),
            Err("neither a delivery nor a loud failure: ended by a signal".to_string())
        );
    }

    #[test]
    fn a_validation_fails_only_with_exit_3_and_the_finding_twice() {
        let report = "failure:   [RIVET_VERIFY_PART_MISSING] part 1\nError: rivet validate: 1 export(s) failed verification\n";
        let (bad, passed) = (ended(3, report), ended(0, "status: PASSED\n"));
        let part = "RIVET_VERIFY_PART_";
        assert_eq!(not_a_failed_validation(&bad, &bad, part), None);
        assert_eq!(
            not_a_failed_validation(&passed, &passed, part).as_deref(),
            Some("report 1 ended with exit 0, expected exit 3 (integrity)")
        );
        assert_eq!(
            not_a_failed_validation(&bad, &passed, part).as_deref(),
            Some("report 2 ended with exit 0, expected exit 3 (integrity)")
        );
        assert_eq!(
            not_a_failed_validation(&bad, &bad, "RIVET_VERIFY_MANIFEST").as_deref(),
            Some("report 1 does not name [RIVET_VERIFY_MANIFEST")
        );
        assert!(not_a_failed_validation(&ended(1, report), &bad, part).is_some());
    }

    #[test]
    fn damage_that_changes_nothing_is_an_error() {
        assert_eq!(
            damaged(b"abcd", &Damage::Truncated),
            Ok(Some(b"ab".to_vec()))
        );
        assert_eq!(damaged(b"abcd", &Damage::Removed), Ok(None));
        assert!(damaged(b"", &Damage::Truncated).is_err());
        assert!(damaged(b"{not json", &Damage::NotJson).is_err());
        let same = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(same.path(), b"abcd").unwrap();
        assert!(damaged(b"abcd", &Damage::ReplacedBy(same.path())).is_err());
        assert!(damaged(b"{\"a\": 1}", &Damage::Json(&|_| {})).is_err());
        let edit = |d: &mut serde_json::Value| d["a"] = serde_json::json!(2);
        let after = damaged(b"{\"a\": 1}", &Damage::Json(&edit))
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&after).unwrap()["a"],
            2
        );
    }

    #[test]
    #[should_panic(expected = "is not there to damage")]
    fn damaging_a_file_that_is_not_there_panics() {
        let rig = Rig::pg_batch("sabotage_absent");
        rig.damage(&rig.out_dir().join("manifest.json"), Damage::NotJson);
    }

    #[test]
    fn a_state_edit_that_touches_another_number_of_rows_is_not_the_damage() {
        assert_eq!(not_the_edit("UPDATE t SET a = 1", 1, 1), None);
        for changed in [0, 2] {
            let why = not_the_edit("UPDATE t SET a = 1", changed, 1).expect("not the damage");
            assert!(
                why.contains(&format!("changed {changed} row(s), expected 1")),
                "{why}"
            );
        }
    }
}
