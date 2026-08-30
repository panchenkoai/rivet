//! Drift guard over `docs/cdc-cli-surface-matrix.yaml` — the `rivet cdc`
//! subcommand's surface, per engine.
//!
//! The flag dimension is DERIVED from `docs/reference/cli-reference.md`, which the
//! pre-commit hook regenerates from clap on every commit. So a flag added to the
//! subcommand arrives with a missing row rather than silently uncovered — the
//! failure this ledger exists for, and the one the two pre-existing CLI matrices
//! could not have: measured 2026-08-25, both contained ZERO mentions of CDC, and
//! the coverage underneath was mysql 11 of 13 flags, mssql 4, postgres 3, mongo 0.
//!
//! What this guard cannot do, said plainly: it cannot tell a `test` cell from a
//! wishful one. It checks that the cited test EXISTS (the lesson from the evidence
//! matrix, where three `sound` cells cited tests defined nowhere) and no more.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde_yaml_ng::Value;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The subcommand's flags, from the generated reference — never typed in here.
fn derived_flags() -> BTreeSet<String> {
    let md = std::fs::read_to_string(repo_root().join("docs/reference/cli-reference.md"))
        .expect("read the generated cli reference");
    let block = md
        .split("\n## `rivet ")
        .find(|b| b.starts_with("cdc`"))
        .expect(
            "the reference must document `rivet cdc` — if it does not, this guard is \
                 reading the wrong artefact and would pass over anything",
        );
    let mut out = BTreeSet::new();
    for line in block.lines() {
        let t = line.trim_start();
        if !t.starts_with("* `") {
            continue;
        }
        // `* `-c`, `--config <CONFIG>` — …`  /  `* `--slot <SLOT>` — …`
        for piece in t.split('`') {
            if let Some(flag) = piece.split_whitespace().next()
                && flag.starts_with("--")
            {
                out.insert(flag.to_string());
            }
        }
    }
    assert!(
        out.len() >= 10,
        "derived only {out:?} flags for `rivet cdc` — the parse is broken, and a broken \
         derivation grades nothing while looking green"
    );
    out
}

fn matrix() -> Value {
    let text = std::fs::read_to_string(repo_root().join("docs/cdc-cli-surface-matrix.yaml"))
        .expect("read cdc-cli-surface-matrix.yaml");
    serde_yaml_ng::from_str(&text).expect("parse cdc-cli-surface-matrix.yaml")
}

fn rows(m: &Value) -> Vec<&Value> {
    m.get("flags")
        .and_then(|s| s.as_sequence())
        .expect("`flags:` sequence")
        .iter()
        .collect()
}

fn flag_id(r: &Value) -> String {
    r.get("id")
        .and_then(|v| v.as_str())
        .expect("every row has an id")
        .to_string()
}

fn cell<'a>(row: &'a Value, engine: &str) -> (&'a str, String) {
    let c = row
        .get(engine)
        .unwrap_or_else(|| panic!("flag `{}` has no cell for `{engine}`", flag_id(row)));
    let map = c.as_mapping().expect("a cell is a mapping");
    for key in ["test", "na", "gap"] {
        if let Some(v) = map.get(Value::from(key)) {
            return (key, v.as_str().unwrap_or("").to_string());
        }
    }
    panic!(
        "flag `{}` cell for `{engine}` has no recognised state (test|na|gap)",
        flag_id(row)
    );
}

#[test]
fn every_cdc_cli_flag_has_a_row_and_every_engine_a_cell() {
    let derived = derived_flags();
    let m = matrix();
    let declared: BTreeSet<String> = rows(&m).iter().map(|r| flag_id(r)).collect();
    assert_eq!(
        declared, derived,
        "the matrix's flags disagree with the set DERIVED from the generated CLI \
         reference. A flag added to `rivet cdc` must arrive with a row asking whether \
         it is exercised on each engine — a ledger whose dimension is typed in grades \
         only what its author already knew."
    );
    let engines: Vec<String> = m["engines"]
        .as_sequence()
        .expect("`engines:`")
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_string())
        .collect();
    // DERIVED, like the flags above and like cdc_evidence's engine columns: every
    // `src/source/<engine>/cdc.rs` in the tree. `assert_eq!(len, 4)` was a
    // typed-in count — a fifth CDC engine would have arrived with no column
    // asking about it (matrix audit, 2026-08-29).
    let derived_engines: BTreeSet<String> = {
        let mut out = BTreeSet::new();
        for entry in std::fs::read_dir(repo_root().join("src/source")).expect("read src/source") {
            let path = entry.expect("dir entry").path();
            if path.join("cdc.rs").is_file()
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
            {
                out.insert(name.to_string());
            }
        }
        out
    };
    assert_eq!(
        engines.iter().cloned().collect::<BTreeSet<_>>(),
        derived_engines,
        "the matrix's declared engines disagree with the set DERIVED from \
         src/source/*/cdc.rs — a ledger whose columns are typed in cannot notice \
         a new engine"
    );
    for r in rows(&m) {
        for e in &engines {
            let (state, note) = cell(r, e);
            assert!(
                !note.trim().is_empty(),
                "flag `{}` / engine `{e}` is `{state}` with an empty note",
                flag_id(r)
            );
        }
    }
}

#[test]
fn every_na_states_the_structural_reason() {
    let m = matrix();
    let engines: Vec<String> = m["engines"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_string())
        .collect();
    for r in rows(&m) {
        for e in &engines {
            let (state, note) = cell(r, e);
            if state != "na" {
                continue;
            }
            assert!(
                note.len() > 60,
                "flag `{}` / engine `{e}` is `na` with the reason `{note}`. An `na` is a \
                 CLAIM that the flag cannot apply here — say which engine owns it and \
                 that the parser rejects it elsewhere, or the next reader cannot check.",
                flag_id(r)
            );
        }
    }
}

/// A `test` cell names a test, and that test must EXIST. Three `sound` cells in the
/// evidence matrix cited tests defined nowhere in the tree — one of them over a
/// parser with zero coverage — and the guard there could not see it because it only
/// looked for a `.rs` substring.
#[test]
fn every_test_cell_cites_a_test_that_exists() {
    let mut defined = BTreeSet::new();
    let mut stack = vec![repo_root().join("tests"), repo_root().join("src")];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|x| x.to_str()) == Some("rs")
                && let Ok(src) = std::fs::read_to_string(&p)
            {
                let mut rest = src.as_str();
                while let Some(at) = rest.find("fn ") {
                    rest = &rest[at + 3..];
                    let name: String = rest
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect();
                    if name.len() > 3 {
                        defined.insert(name);
                    }
                }
            }
        }
    }
    assert!(defined.len() > 500, "the fn sweep is broken");

    let m = matrix();
    let engines: Vec<String> = m["engines"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_string())
        .collect();
    for r in rows(&m) {
        for e in &engines {
            let (state, note) = cell(r, e);
            if state != "test" {
                continue;
            }
            let mut resolved_any = false;
            for word in note.split(|c: char| !(c.is_alphanumeric() || c == '_')) {
                if word.len() > 20 && word.matches('_').count() >= 3 {
                    if defined.contains(word) {
                        resolved_any = true;
                    } else {
                        panic!(
                            "flag `{}` / engine `{e}` cites `{word}`, defined nowhere in the tree. \
                             Either the test was never written — in which case the cell is not \
                             `test` — or it was renamed and the ledger points at nothing.",
                            flag_id(r)
                        );
                    }
                }
            }
            // A `test` cell made of PROSE ("driven in the pg cli tests") named
            // nothing and passed the per-word check vacuously — the matrix audit
            // found four such cells. A claim nobody can re-run is not a claim.
            assert!(
                resolved_any,
                "flag `{}` / engine `{e}` is `test` but its note names no resolvable \
                 test fn: `{note}`. Cite at least one real test name.",
                flag_id(r)
            );
        }
    }
}

/// Shrink-only. 24 gaps on 2026-08-25, which is the honest count of what the two
/// pre-existing CLI ledgers were not asking. Lower it in the commit that closes one.
#[test]
fn cdc_cli_gaps_only_ever_shrink() {
    const CEILING: usize = 19;
    let m = matrix();
    let engines: Vec<String> = m["engines"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_string())
        .collect();
    let mut gaps: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for r in rows(&m) {
        for e in &engines {
            if cell(r, e).0 == "gap" {
                gaps.entry(flag_id(r)).or_default().push(e.clone());
            }
        }
    }
    let total: usize = gaps.values().map(Vec::len).sum();
    assert!(
        total <= CEILING,
        "{total} gaps against a ceiling of {CEILING}: {gaps:?}. Adding CLI surface \
         without a test is what this ledger records; raising the ceiling is not an option."
    );
    assert!(
        total >= CEILING.saturating_sub(3),
        "only {total} gaps remain against a ceiling of {CEILING} — lower CEILING to \
         {total} in the same commit that closed them, or the win is not banked."
    );
}
