//! Shrink-only ceilings for the rig's default oracle.
//!
//! Every successful `Rig` run is graded by `dev/release_oracle/rig_oracle.py`
//! (tests/common/rig/verify.rs); a run started outside the rig is not. Three
//! counts may only go down:
//!
//! * `.no_oracle("<reason>")` opt-outs — each one is a live test whose output
//!   no independent reader checks;
//! * call sites of the Rust-side DuckDB helpers (`tests/common/duckdb.rs`, the
//!   `duckdb_*` readers in `tests/common/parquet.rs`, the rig's container
//!   census) — the one DuckDB session belongs to the Python oracle, and these
//!   are the calls still to migrate onto it;
//! * raw rivet invocations in tests/live (`run_rivet*(`, `Command::new` of the
//!   binary) — runs no oracle grades.
//!
//! A count below its ceiling fails too, so every migration lowers the ceiling
//! in the same diff and cannot be spent later as silent slack.

use std::path::{Path, PathBuf};

/// `.no_oracle(` call sites across tests/, excluding the rig's own definition.
const NO_ORACLE_CEILING: usize = 17;

/// Rust DuckDB-helper call sites across tests/ (see [`duckdb_helper_names`]).
// 626 -> 630 (2026-10-01): #378 merged first and added 4 calls in its Mongo null-_id tests.
const DUCKDB_HELPER_CEILING: usize = 630;

/// Owned by a concurrent branch and migrated after it lands; not counted.
const EXCLUDED: &[&str] = &["tests/live/live_cdc_type_parity.rs"];

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read tests/").flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// (repo-relative path, text) of every counted test source.
fn sources() -> Vec<(String, String)> {
    let root = super::nonvacuity::repo_root();
    let mut files = Vec::new();
    rust_files(&root.join("tests"), &mut files);
    files
        .into_iter()
        .map(|p| {
            let rel = p.strip_prefix(&root).unwrap().display().to_string();
            let text = std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {rel}: {e}"));
            (rel, text)
        })
        .filter(|(rel, _)| !EXCLUDED.contains(&rel.as_str()) && !rel.starts_with("tests/offline/"))
        .collect()
}

/// The Rust DuckDB helpers, derived from the source: every `pub fn` in tests/common/duckdb.rs, every `fn duckdb_*` anywhere under tests/, and the rig's container-census methods.
fn duckdb_helper_names(srcs: &[(String, String)]) -> Vec<String> {
    let mut names: Vec<String> = [
        "census_oracle",
        "row_census",
        "duckdb_oracle",
        "assert_complete",
        "oracle_dir",
        "oracle_container_out",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for (rel, text) in srcs {
        for line in text.lines() {
            let l = line.trim_start();
            let decl = l
                .strip_prefix("pub fn ")
                .filter(|_| rel == "tests/common/duckdb.rs")
                .or_else(|| l.strip_prefix("pub fn duckdb_").map(|_| &l[7..]))
                .or_else(|| l.strip_prefix("fn duckdb_").map(|_| &l[3..]));
            if let Some(d) = decl {
                let name: String = d
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() && !names.contains(&name) {
                    names.push(name);
                }
            }
        }
    }
    names
}

/// Occurrences of `name(` that are not the `fn name(` declaration.
fn call_sites(text: &str, name: &str) -> usize {
    let needle = format!("{name}(");
    text.match_indices(&needle)
        .filter(|(i, _)| {
            let before = &text[..*i];
            let prev = before.chars().next_back();
            !prev.is_some_and(|c| c.is_alphanumeric() || c == '_') && !before.ends_with("fn ")
        })
        .count()
}

#[test]
fn no_oracle_opt_outs_never_grow() {
    let srcs = sources();
    super::nonvacuity::require_enumerated(srcs.len(), 100, "test sources scanned", "tests/ moved");
    let n: usize = srcs
        .iter()
        .filter(|(rel, _)| rel != "tests/common/rig/verify.rs")
        .map(|(_, t)| t.matches(".no_oracle(").count())
        .sum();
    assert_eq!(
        n, NO_ORACLE_CEILING,
        "`.no_oracle(` call sites: {n}, ceiling {NO_ORACLE_CEILING}. A new opt-out needs a reviewed \
         reason and a raised ceiling; a removed one lowers the ceiling in the same diff."
    );
}

#[test]
fn rust_duckdb_helper_call_sites_never_grow() {
    let srcs = sources();
    let names = duckdb_helper_names(&srcs);
    super::nonvacuity::require_enumerated(
        names.len(),
        20,
        "Rust DuckDB helper names",
        "the helpers moved",
    );
    let n: usize = srcs
        .iter()
        .map(|(_, t)| names.iter().map(|name| call_sites(t, name)).sum::<usize>())
        .sum();
    assert_eq!(
        n, DUCKDB_HELPER_CEILING,
        "Rust DuckDB helper call sites: {n}, ceiling {DUCKDB_HELPER_CEILING}. New reads go through \
         dev/release_oracle (the one DuckDB session); a migrated call lowers the ceiling here."
    );
}

/// `(test fn, reason prefix)` of every `.oracle_known_defect(` site: a product defect the oracle must keep catching. Removing one is allowed; adding one is a reviewed diff here.
const KNOWN_DEFECTS: &[(&str, &str)] = &[
    (
        "roast_pg_cdc_refuses_a_bare_table_name_that_matches_two_relations",
        "known defect: a bare-name capture delivers the WAL rows",
    ),
    (
        "pg_cdc_pk_changing_update_captures_and_does_not_brick",
        "known defect: a PK-changing UPDATE carries no delete",
    ),
];

/// `(enclosing fn, reason)` of every `.oracle_known_defect("…")` call in `text`.
fn known_defect_sites(text: &str) -> Vec<(String, String)> {
    text.match_indices(".oracle_known_defect(")
        .map(|(i, _)| {
            let before = &text[..i];
            let f = before
                .rfind("\nfn ")
                .map(|j| {
                    before[j + 4..]
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect::<String>()
                })
                .unwrap_or_default();
            let reason = text[i..]
                .split_once('"')
                .and_then(|(_, r)| r.split_once('"'))
                .map(|(r, _)| r.to_string())
                .unwrap_or_default();
            (f, reason)
        })
        .collect()
}

#[test]
fn oracle_known_defects_are_a_named_set_that_only_shrinks() {
    let sites: Vec<(String, String)> = sources()
        .iter()
        .filter(|(rel, _)| rel != "tests/common/rig/verify.rs")
        .flat_map(|(_, t)| known_defect_sites(t))
        .collect();
    super::nonvacuity::require_enumerated(
        sites.len(),
        1,
        "oracle_known_defect sites",
        "the marker moved",
    );
    let unlisted: Vec<&(String, String)> = sites
        .iter()
        .filter(|(f, r)| {
            !KNOWN_DEFECTS
                .iter()
                .any(|(kf, kp)| kf == f && r.starts_with(kp))
        })
        .collect();
    assert!(
        unlisted.is_empty(),
        "new `.oracle_known_defect` site(s) {unlisted:?}: a known product defect is added to \
         KNOWN_DEFECTS in a reviewed diff, never silently"
    );
}

/// Raw rivet invocations in tests/live (`run_rivet*(` and `Command::new(RIVET_BIN | rivet_bin())`): runs the rig oracle never sees.
const RAW_RIVET_CEILING: usize = 387;

/// Call sites of `run_rivet*(` helpers plus direct `Command::new` of the rivet binary in `text`.
fn raw_rivet_sites(text: &str) -> usize {
    let helpers = text
        .match_indices("run_rivet")
        .filter(|(i, _)| {
            let before = &text[..*i];
            let after = &text[i + "run_rivet".len()..];
            let tail = after
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .unwrap_or(after.len());
            !before.ends_with("fn ")
                && !before
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_')
                && after[tail..].starts_with('(')
        })
        .count();
    let direct = text.matches("Command::new(RIVET_BIN)").count()
        + text.matches("Command::new(rivet_bin())").count();
    helpers + direct
}

#[test]
fn raw_rivet_invocations_in_live_tests_never_grow() {
    let live: Vec<(String, String)> = sources()
        .into_iter()
        .filter(|(rel, _)| rel.starts_with("tests/live/"))
        .collect();
    super::nonvacuity::require_enumerated(
        live.len(),
        30,
        "live test sources scanned",
        "tests/live moved",
    );
    let n: usize = live.iter().map(|(_, t)| raw_rivet_sites(t)).sum();
    assert_eq!(
        n, RAW_RIVET_CEILING,
        "raw rivet invocations in tests/live: {n}, ceiling {RAW_RIVET_CEILING}. These runs bypass \
         the rig's default oracle; new tests drive rivet through `Rig`, and a migrated call \
         lowers the ceiling here."
    );
}
