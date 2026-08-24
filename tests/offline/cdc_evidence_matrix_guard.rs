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
        }
    }
}

/// Shrink-only ratchet. Not a pass/fail bar on today's state — a floor under it, so the
/// audit that produced this file does not have to be repeated to discover the same
/// numbers. It fails DOWNWARD too: close cells and lower the ceiling in the same commit,
/// or the win is not banked and drifts back.
#[test]
fn unsound_cells_only_ever_shrink() {
    const CEILING: usize = 26;
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
    let total: usize = unsound.values().map(|v| v.len()).sum();
    assert!(
        total <= CEILING,
        "{total} cells are neither `sound` nor a justified `na`, over the ceiling of \
         {CEILING}: {unsound:?}. Adding CDC surface without evidence is what this ledger \
         records; raising the ceiling to accommodate it is not an option."
    );
    assert!(
        total >= CEILING.saturating_sub(4),
        "only {total} unsound cells remain against a ceiling of {CEILING} — lower CEILING \
         to {total} in the same commit that closed them. A ratchet that is not tightened \
         is a ratchet that lets the next regression back in for free."
    );
}
