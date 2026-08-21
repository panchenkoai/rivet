//! Drift guard for `docs/scenario-artifact-matrix.yaml`.
//!
//! The ledger states, per lifecycle scenario, what rivet must LEAVE BEHIND — in
//! the meta-DB and at the destination prefix. It is only worth the file it lives
//! in if it cannot quietly rot, so this guard pins the three ways it could:
//!
//! 1. a scenario asserting an artifact class that does not exist (a typo asserts
//!    NOTHING, which is exactly the failure mode the ledger exists to prevent);
//! 2. a scenario naming an engine outside the declared set;
//! 3. a scenario restricted to fewer engines than the matrix declares WITHOUT
//!    saying why — the silent-omission shape that let a feature reach only some
//!    runners for two releases.
//!
//! Deliberately a STRUCTURAL guard, not an execution one: running the scenarios
//! needs live engines and belongs in `dev/pytools/scenario_artifacts.py`. What
//! this file guarantees is that the ledger the harness reads is internally
//! coherent — and it goes RED on each of the three, proven by mutating the YAML
//! (an unknown class, an unknown engine, an unexplained restriction).

use std::collections::BTreeSet;

fn matrix_text() -> String {
    super::nonvacuity::subject_text("docs/scenario-artifact-matrix.yaml")
}

/// Values under a top-level `engines: [a, b]` inline list.
fn declared_engines(text: &str) -> BTreeSet<String> {
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("engines: [") {
            return rest
                .trim_end_matches(']')
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }
    panic!("the matrix must declare a top-level `engines: [...]` list");
}

/// `- id: <x>` entries under `artifact_classes:`.
fn artifact_classes(text: &str) -> BTreeSet<String> {
    let start = text
        .find("\nartifact_classes:")
        .expect("the matrix must have an `artifact_classes:` section");
    let rest = &text[start + 1..];
    let end = rest.find("\nscenarios:").unwrap_or(rest.len());
    rest[..end]
        .lines()
        .filter_map(|l| {
            l.trim()
                .strip_prefix("- id: ")
                .map(|s| s.trim().to_string())
        })
        .collect()
}

/// Every scenario as (id, engines, expect-keys, has_note).
fn scenarios(text: &str) -> Vec<(String, BTreeSet<String>, BTreeSet<String>, bool)> {
    let start = text
        .find("\nscenarios:")
        .expect("the matrix must have a `scenarios:` section");
    let body = &text[start + 1..];
    let mut out = Vec::new();
    let mut cur: Option<(String, BTreeSet<String>, BTreeSet<String>, bool)> = None;
    let mut in_expect = false;
    for line in body.lines() {
        let t = line.trim();
        if let Some(id) = t.strip_prefix("- id: ") {
            if let Some(s) = cur.take() {
                out.push(s);
            }
            cur = Some((
                id.trim().to_string(),
                BTreeSet::new(),
                BTreeSet::new(),
                false,
            ));
            in_expect = false;
            continue;
        }
        let Some(c) = cur.as_mut() else { continue };
        if let Some(rest) = t.strip_prefix("engines: [") {
            c.1 = rest
                .trim_end_matches(']')
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            in_expect = false;
        } else if t.starts_with("note:") {
            c.3 = true;
            in_expect = false;
        } else if t == "expect:" {
            in_expect = true;
        } else if in_expect && t.contains(':') && !t.starts_with('#') && !t.is_empty() {
            let key = t.split(':').next().unwrap().trim();
            if !key.is_empty() {
                c.2.insert(key.to_string());
            }
        }
    }
    if let Some(s) = cur.take() {
        out.push(s);
    }
    // The `scenarios:` HEADING missing panics above; a heading whose rows this
    // hand-rolled line parser no longer recognises (a re-indent, a different
    // list style) yields zero rows — and all four guards below iterate rows, so
    // zero rows is four green tests over a ledger asserting nothing. 14 today.
    super::nonvacuity::require_enumerated(
        out.len(),
        8,
        "scenario rows parsed from docs/scenario-artifact-matrix.yaml",
        "The rows are still there and this parser stopped seeing them — fix the parser \
         rather than the floor.",
    );
    out
}

#[test]
fn every_expect_key_is_a_declared_artifact_class() {
    let text = matrix_text();
    let classes = artifact_classes(&text);
    assert!(!classes.is_empty(), "no artifact classes parsed");
    let mut bad = Vec::new();
    for (id, _, keys, _) in scenarios(&text) {
        for k in &keys {
            if !classes.contains(k) {
                bad.push(format!("{id}: `{k}`"));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "scenario(s) assert an artifact class the snapshot does not collect — a \
         mistyped key asserts NOTHING, which is the exact blindness this ledger \
         exists to remove:\n  {}\nDeclared classes: {:?}",
        bad.join("\n  "),
        classes
    );
}

/// The matrix's self-declared `engines:` universe must BE the SourceType enum
/// — not merely self-consistent. r5 wired this derive-check into release_gate
/// but left this sibling hand-declaring the same universe (r6 bughunt): a new
/// SourceType variant would not force a column here.
#[test]
fn declared_engines_are_the_source_type_enum() {
    let declared = declared_engines(&matrix_text());
    let derived: std::collections::BTreeSet<String> =
        super::chunking_matrix_guard::source_engine_variants()
            .into_iter()
            .collect();
    assert_eq!(
        declared, derived,
        "scenario-artifact matrix's declared engines drifted from SourceType:          declared {declared:?} vs enum {derived:?}"
    );
}

#[test]
fn every_scenario_engine_is_declared() {
    let text = matrix_text();
    let declared = declared_engines(&text);
    let mut bad = Vec::new();
    for (id, engines, _, _) in scenarios(&text) {
        for e in &engines {
            if !declared.contains(e) {
                bad.push(format!("{id}: `{e}`"));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "scenario(s) name an engine outside the declared set {declared:?}:\n  {}",
        bad.join("\n  ")
    );
}

#[test]
fn a_scenario_covering_fewer_engines_says_why() {
    let text = matrix_text();
    let declared = declared_engines(&text);
    let mut bad = Vec::new();
    for (id, engines, _, has_note) in scenarios(&text) {
        if engines.len() < declared.len() && !has_note {
            bad.push(format!(
                "{id} (covers {:?}, declared {:?})",
                engines, declared
            ));
        }
    }
    assert!(
        bad.is_empty(),
        "scenario(s) cover fewer engines than declared and give no reason. A \
         narrower row may be right — a runner that does not exist on that engine, \
         a credential-bound warehouse — but it must SAY so, or the omission reads \
         as coverage it does not have (the shape that let a feature reach only \
         some runners for two releases):\n  {}",
        bad.join("\n  ")
    );
}

#[test]
fn every_scenario_asserts_at_least_one_artifact() {
    let text = matrix_text();
    let empty: Vec<String> = scenarios(&text)
        .into_iter()
        .filter(|(_, _, keys, _)| keys.is_empty())
        .map(|(id, _, _, _)| id)
        .collect();
    assert!(
        empty.is_empty(),
        "scenario(s) with an empty `expect` block assert nothing and would pass \
         against any product behaviour: {empty:?}"
    );
}
