//! Drift guard over `docs/cdc-evidence-matrix.yaml` — the ledger that grades whether
//! CDC coverage would BITE, not whether it exists.
//!
//! The engine columns are DERIVED, from the presence of `src/source/<engine>/cdc.rs`,
//! never from a list typed into the YAML. The 2026-08 CDC audit found the conformance
//! gate's four engine columns were tied to nothing, so a fifth CDC engine would have
//! arrived with no row asking about it — the same shape as a coverage ledger whose
//! dimension is hand-written. Adding a CDC engine now forces a cell in every shape.
//!
//! What this guard cannot do, said plainly: it cannot tell a `sound` cell from a
//! `vacuous` one. Only applying the mutant can, which is why every shape must NAME its
//! mutant and why the ratchet below is shrink-only — a cell is promoted by someone
//! running the mutant, not by editing a word.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde_yaml_ng::Value;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The CDC engine set, derived: every `src/source/<engine>/cdc.rs` in the tree.
fn cdc_engines() -> BTreeSet<String> {
    let dir = repo_root().join("src/source");
    let mut out = BTreeSet::new();
    for entry in std::fs::read_dir(&dir).expect("read src/source") {
        let path = entry.expect("dir entry").path();
        if path.join("cdc.rs").is_file()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
        {
            out.insert(name.to_string());
        }
    }
    assert!(
        out.len() >= 4,
        "derived only {out:?} CDC engines from src/source/*/cdc.rs — the derivation is \
         broken, and a broken derivation grades nothing while looking green"
    );
    out
}

fn matrix() -> Value {
    let path = repo_root().join("docs/cdc-evidence-matrix.yaml");
    let text = std::fs::read_to_string(&path).expect("read cdc-evidence-matrix.yaml");
    serde_yaml_ng::from_str(&text).expect("parse cdc-evidence-matrix.yaml")
}

fn shapes(m: &Value) -> Vec<&Value> {
    m.get("shapes")
        .and_then(|s| s.as_sequence())
        .expect("`shapes:` sequence")
        .iter()
        .collect()
}

fn shape_id(s: &Value) -> String {
    s.get("id")
        .and_then(|v| v.as_str())
        .expect("every shape has an id")
        .to_string()
}

/// `(state, note)` for one engine's cell — the cell is a one-key map.
fn cell<'a>(shape: &'a Value, engine: &str) -> (&'a str, String) {
    let c = shape
        .get(engine)
        .unwrap_or_else(|| panic!("shape `{}` has no cell for `{engine}`", shape_id(shape)));
    let map = c.as_mapping().expect("a cell is a mapping");
    for key in ["sound", "vacuous", "absent", "gap", "na"] {
        if let Some(v) = map.get(Value::from(key)) {
            return (key, v.as_str().unwrap_or("").to_string());
        }
    }
    panic!(
        "shape `{}` cell for `{engine}` has no recognised state (sound|vacuous|absent|gap|na)",
        shape_id(shape)
    )
}

#[test]
fn every_cdc_engine_has_a_cell_in_every_shape() {
    let engines = cdc_engines();
    let m = matrix();
    let declared: BTreeSet<String> = m
        .get("engines")
        .and_then(|e| e.as_sequence())
        .expect("`engines:` sequence")
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(
        declared, engines,
        "the matrix's declared engines disagree with the set DERIVED from \
         src/source/*/cdc.rs. A ledger whose columns are typed in cannot notice a new \
         engine — that is the defect this derivation exists to remove."
    );
    for s in shapes(&m) {
        for e in &engines {
            let (state, _) = cell(s, e);
            assert!(
                !state.is_empty(),
                "shape `{}` / engine `{e}`: empty state",
                shape_id(s)
            );
        }
    }
}

#[test]
fn every_na_states_why_not_merely_that_it_does_not_apply() {
    let engines = cdc_engines();
    let m = matrix();
    for s in shapes(&m) {
        for e in &engines {
            let (state, note) = cell(s, e);
            if state != "na" {
                continue;
            }
            assert!(
                note.len() > 60,
                "shape `{}` / engine `{e}` is `na` with the reason `{note}`. An `na` is a \
                 CLAIM that the shape cannot apply here, and it is the cell most likely to \
                 be wrong — \"the other engines don't have it\" is exactly what this ledger \
                 exists to refuse. Write the structural reason out.",
                shape_id(s)
            );
        }
    }
}

#[test]
fn every_shape_names_the_mutant_that_would_expose_it() {
    for s in shapes(&matrix()) {
        let mutant = s
            .get("mutant")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .trim()
            .to_string();
        assert!(
            mutant.len() > 40,
            "shape `{}` does not name the mutant that would expose it. A shape without a \
             mutant cannot promote any of its cells: `sound` means \"this test goes RED \
             against THAT change\", and with no change named the word is decoration.",
            shape_id(s)
        );
    }
}

#[test]
fn a_sound_cell_cites_where_the_evidence_lives() {
    let engines = cdc_engines();
    let m = matrix();
    // Every `fn` name in the tree, so a cited grader is RESOLVED and not merely
    // shaped like a test name. Three cells cited tests that exist nowhere — one of
    // them over MySQL's TRUNCATE parser, which had zero offline coverage while the
    // cell called it graded (PROVEN: stubbing `truncate_target` to `return None`
    // left the whole lib suite green at 2616 passed). A `sound` cell resting on a
    // name is the defect this file exists to refuse, and checking only for a `.rs`
    // substring could not see it.
    let mut defined: BTreeSet<String> = BTreeSet::new();
    let mut stack = vec![repo_root().join("src"), repo_root().join("tests")];
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
    assert!(
        defined.len() > 500,
        "collected only {} fn names from src/ and tests/ — the sweep is broken and \
         would resolve nothing while looking green",
        defined.len()
    );

    for s in shapes(&m) {
        for e in &engines {
            let (state, note) = cell(s, e);
            if state != "sound" {
                continue;
            }
            let cites = note.contains(".rs") || note.contains("cdc-matrix");
            assert!(
                cites,
                "shape `{}` / engine `{e}` claims `sound` with `{note}`, which names no \
                 file. A sound cell is a claim that someone ran the mutant and watched a \
                 SPECIFIC test fail; without the test named, nobody can re-run it.",
                shape_id(s)
            );
            // Any snake_case token long enough to be a test name must RESOLVE.
            for word in note.split(|c: char| !(c.is_alphanumeric() || c == '_')) {
                // Long AND multi-part: this repo's test names are sentences
                // (`a_mysql_truncate_statement_resolves_the_table_it_names`), while
                // an API name quoted as prose (`full_document`, `image_names`) is
                // short. Tuned so the check flags claims about TESTS and not every
                // identifier a note happens to mention.
                let looks_like_a_fn = word.len() > 20
                    && word.matches('_').count() >= 3
                    && word
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                    && !word.ends_with("_rs");
                // A file STEM is not a function: `live_cdc_mongo.rs` splits into
                // `live_cdc_mongo` + `rs`, and citing the file is exactly what the
                // `.rs` requirement above asks for.
                // A method or an API name the note quotes as prose (a driver call,
                // a config key) is not a claim about a test in THIS tree.
                // Not a test name: an API call in prose, a backticked identifier, or
                // a FAULT HOOK — `cdc_after_flush_before_ack` is the string a test
                // passes to RIVET_TEST_PANIC_AT, and it is defined in the product as
                // a literal rather than as a `fn`.
                let is_an_api_name = note.contains(&format!("{word}("))
                    || note.contains(&format!("`{word}`"))
                    || word.starts_with("cdc_after")
                    || word.starts_with("cdc_before");
                let is_a_file_stem = note.contains(&format!("{word}.rs"))
                    || note.contains(&format!("{word}.yaml"))
                    || note.contains(&format!("{word}.toml"));
                if !looks_like_a_fn || is_a_file_stem || is_an_api_name || defined.contains(word) {
                    continue;
                }
                panic!(
                    "shape `{}` / engine `{e}` cites `{word}`, which is defined NOWHERE in \
                     src/ or tests/. Either the test was never written — in which case the \
                     cell is not sound — or it was renamed and the ledger now points at \
                     nothing. Both are the same failure: a claim that cannot be re-run.",
                    shape_id(s)
                );
            }
        }
    }
}

/// ZERO unsound cells — and at zero this stopped being a ratchet.
///
/// It was one: a floor under the audit's findings so the same numbers would not have
/// to be rediscovered, failing DOWNWARD too so a closed cell had to be banked in the
/// same commit. It counted 20 when it was written and was lowered to 16, 12, 8, 3 and
/// now 0 as the cells closed.
///
/// At zero the contract changes shape, so the assertion does too: this ledger now has
/// the same admission policy as `cdc-matrix` — every cell is `sound` (someone ran the
/// mutant and watched a specific test fail) or a justified `na` (structurally
/// inapplicable, with the reason written out and length-checked above). There is no
/// budget left to spend, which means a NEW CDC shape arrives closed or does not arrive.
///
/// Restoring a budget is a decision someone has to argue for in a commit message, not
/// a number they can nudge.
#[test]
fn every_cell_is_sound_or_a_justified_na() {
    let engines = cdc_engines();
    let m = matrix();
    let mut unsound: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for s in shapes(&m) {
        for e in &engines {
            let (state, _) = cell(s, e);
            if state != "sound" && state != "na" {
                unsound.entry(shape_id(s)).or_default().push(e.clone());
            }
        }
    }
    assert!(
        unsound.is_empty(),
        "{} cell(s) are neither `sound` nor a justified `na`: {unsound:?}. This ledger \
         reached zero on 2026-08-25 and holds there — a cell is promoted by someone \
         running its mutant, so a new shape lands closed or it does not land. If a gap \
         genuinely has to be admitted, say why in the commit that admits it rather than \
         raising a number.",
        unsound.values().map(Vec::len).sum::<usize>()
    );
}
