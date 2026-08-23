//! Drift guard for `docs/cdc-axis-matrix.yaml` — the ledger of which CDC axes
//! the live sweep actually drives.
//!
//! The claim this protects is "the CDC sweep covers every axis". That claim is
//! true when it is made and decays without anyone noticing, because the axes are
//! not a list a human curates — they are the FIELDS of `CdcExportConfig` and the
//! `cdc_*` fault points in the sink. Both grow by ordinary work, and nothing
//! about adding one prompts a look at the sweep.
//!
//! So neither enumeration is typed in here. The field names are parsed out of
//! `src/config/export.rs`; the hook names out of the `maybe_panic_at("cdc_…")`
//! call sites. Adding a knob or a fault point fails this test until the ledger
//! says how it is swept — or says `na`, with a reason, which is also an answer.
//!
//! Honest about its own strength, in the shape corollary 2 of the fixture rule
//! asks for: adding a field to `CdcExportConfig` does NOT compile silently
//! elsewhere (the hand-written `Default` and every construction site see it), so
//! the risk is not that a knob ships unnoticed. The risk is that once the
//! COMPILER is satisfied, nothing asks whether anything RUNS it. That is the gap
//! this closes, and it is the only one.

use std::collections::BTreeSet;
use std::fs;

use serde_yaml_ng::Value;

const MATRIX: &str = "docs/cdc-axis-matrix.yaml";
const CONFIG: &str = "src/config/export.rs";
const SINK: &str = "src/source/cdc/sink.rs";
const CDC_MOD: &str = "src/source/cdc/mod.rs";
const SWEEP: &str = "dev/pytools/cdc_sweep.py";

/// Rows with `coverage: gap`. LOWER when one gets covered; never raise without
/// the reason landing in the same commit.
const GAP_RATCHET: usize = 2;

fn matrix() -> Value {
    let s = fs::read_to_string(MATRIX).unwrap_or_else(|e| panic!("read {MATRIX}: {e}"));
    serde_yaml_ng::from_str(&s).unwrap_or_else(|e| panic!("parse {MATRIX}: {e}"))
}

/// One section of the ledger, floor-checked.
///
/// A missing section already panics; a PRESENT but empty one did not, and that
/// is the direction that passes: `every_cdc_config_field_has_a_row` compares
/// declared ids against parsed source, and with zero declared ids the
/// `difference` in the stale direction is empty while the missing direction is
/// answered by whatever the source scan found. The floors are today's counts
/// (9 / 6 / 11) minus room for ordinary edits.
fn rows(section: &str) -> Vec<Value> {
    let out: Vec<Value> = matrix()
        .get(section)
        .and_then(|v| v.as_sequence())
        .unwrap_or_else(|| panic!("{MATRIX} must have a `{section}:` sequence"))
        .clone();
    let floor = match section {
        "config_fields" => 5,
        "crash_hooks" => 4,
        "shape_axes" => 6,
        _ => 0,
    };
    super::nonvacuity::require_enumerated(
        out.len(),
        floor,
        &format!("`{section}:` rows in {MATRIX}"),
        "Restore the rows or re-point this reader — an empty axis section reads as full \
         coverage of an axis nothing is graded on.",
    );
    out
}

fn all_rows() -> Vec<Value> {
    let mut v = rows("config_fields");
    v.extend(rows("crash_hooks"));
    v.extend(rows("shape_axes"));
    v
}

fn field(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(|x| x.as_str()).map(str::to_string)
}

fn ids(section: &str) -> BTreeSet<String> {
    rows(section)
        .iter()
        .filter_map(|r| field(r, "id"))
        .collect()
}

/// The fields of `pub struct CdcExportConfig`, read off the struct itself.
///
/// Parsed rather than mirrored: a mirrored list is a second place to forget, and
/// it would go stale in exactly the situation this test exists for.
fn cdc_config_fields() -> BTreeSet<String> {
    let src = fs::read_to_string(CONFIG).unwrap_or_else(|e| panic!("read {CONFIG}: {e}"));
    let start = src
        .find("pub struct CdcExportConfig {")
        .unwrap_or_else(|| panic!("{CONFIG}: no `pub struct CdcExportConfig {{` — did it move?"));
    let body = &src[start..];
    let end = body
        .find("\n}")
        .unwrap_or_else(|| panic!("{CONFIG}: CdcExportConfig has no closing brace"));
    let mut out = BTreeSet::new();
    for line in body[..end].lines() {
        let t = line.trim();
        // `pub name: Type,` — doc comments, attributes and blank lines are not fields.
        if let Some(rest) = t.strip_prefix("pub ")
            && let Some((name, _)) = rest.split_once(':')
            && name.chars().all(|c| c.is_ascii_lowercase() || c == '_')
            && !name.is_empty()
        {
            out.insert(name.to_string());
        }
    }
    assert!(
        out.len() >= 5,
        "{CONFIG}: parsed only {} field(s) out of CdcExportConfig ({out:?}). A parser that finds \
         nothing passes every check below while grading nothing — the shape of failure this whole \
         file exists to prevent.",
        out.len()
    );
    out
}

/// Every `maybe_panic_at("cdc_…")` fault point in the CDC path.
fn cdc_crash_hooks() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in [SINK, CDC_MOD] {
        let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        for (i, _) in src.match_indices("maybe_panic_at(\"cdc_") {
            let tail = &src[i + "maybe_panic_at(\"".len()..];
            if let Some(q) = tail.find('"') {
                out.insert(tail[..q].to_string());
            }
        }
    }
    assert!(
        out.len() >= 4,
        "found only {} cdc crash hook(s) ({out:?}) across {SINK} and {CDC_MOD} — the scan found \
         nothing rather than the hooks having gone away.",
        out.len()
    );
    out
}

#[test]
fn every_cdc_config_field_has_a_row() {
    let declared = ids("config_fields");
    let actual = cdc_config_fields();
    let missing: Vec<_> = actual.difference(&declared).cloned().collect();
    let stale: Vec<_> = declared.difference(&actual).cloned().collect();
    assert!(
        missing.is_empty(),
        "CdcExportConfig field(s) with no row in {MATRIX}: {missing:?}. A new CDC knob is a new \
         axis, and the sweep does not discover it by itself — add a row saying which case drives \
         it, or `coverage: na` with the reason. Sweep: {SWEEP}"
    );
    assert!(
        stale.is_empty(),
        "{MATRIX} has row(s) for CdcExportConfig field(s) that no longer exist: {stale:?}. A \
         ledger describing a removed knob is worse than no ledger — it reports coverage of \
         something nothing runs."
    );
}

#[test]
fn every_cdc_crash_hook_has_a_row() {
    let declared = ids("crash_hooks");
    let actual = cdc_crash_hooks();
    let missing: Vec<_> = actual.difference(&declared).cloned().collect();
    let stale: Vec<_> = declared.difference(&actual).cloned().collect();
    assert!(
        missing.is_empty(),
        "cdc fault point(s) with no row in {MATRIX}: {missing:?}. A hook exists to be crashed at; \
         one nothing crashes at is a fault point that has never been shown to be survivable."
    );
    assert!(
        stale.is_empty(),
        "{MATRIX} claims crash coverage for hook(s) that are no longer in the product: {stale:?}."
    );
}

#[test]
fn every_row_resolves_to_coverage() {
    const VOCAB: [&str; 4] = ["swept", "test", "na", "gap"];
    let mut bad = Vec::new();
    for r in all_rows() {
        let id = field(&r, "id").unwrap_or_else(|| "<no id>".into());
        match field(&r, "coverage") {
            None => bad.push(format!("{id}: no `coverage:`")),
            Some(c) if !VOCAB.contains(&c.as_str()) => {
                bad.push(format!("{id}: coverage `{c}` is not one of {VOCAB:?}"))
            }
            Some(c) if (c == "swept") && field(&r, "case").is_none() => {
                bad.push(format!("{id}: `swept` but names no case in {SWEEP}"))
            }
            Some(c) if (c == "na" || c == "gap") && field(&r, "detail").is_none() => {
                bad.push(format!("{id}: `{c}` with no `detail:` saying why"))
            }
            Some(_) => {}
        }
    }
    assert!(
        bad.is_empty(),
        "CDC axis row(s) that do not resolve:\n  {}\n`na` and `gap` are legitimate answers; an \
         unanswered row is not, because it reads as covered.",
        bad.join("\n  ")
    );
}

#[test]
fn a_swept_row_names_a_case_the_sweep_actually_has() {
    // Whitespace-stripped, because the sweep's longer cases wrap:
    //   Case(
    //       "checkpoint_present",
    // and a line-oriented search for `Case("name"` finds none of them — it would
    // have reported thirteen real cases as missing, which is what it did.
    let sweep: String = fs::read_to_string(SWEEP)
        .unwrap_or_else(|e| panic!("read {SWEEP}: {e}"))
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let mut bad = Vec::new();
    for r in all_rows() {
        if field(&r, "coverage").as_deref() != Some("swept") {
            continue;
        }
        let id = field(&r, "id").unwrap_or_default();
        for case in field(&r, "case").unwrap_or_default().split(',') {
            let case = case.trim();
            // "every mysql case" / "every case" are honest descriptions of an axis
            // carried by the engine block rather than by one named case.
            if case.starts_with("every ") {
                continue;
            }
            if !sweep.contains(&format!("Case(\"{case}\"")) {
                bad.push(format!(
                    "{id}: names case `{case}`, which {SWEEP} does not define"
                ));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "CDC axis row(s) pointing at a sweep case that does not exist:\n  {}\nA ledger citing a \
         case nobody runs is the fixture-against-itself defect one level up: it reads like \
         coverage and nothing about it can go red.",
        bad.join("\n  ")
    );
}

#[test]
fn uncovered_axes_do_not_grow() {
    let n = all_rows()
        .iter()
        .filter(|r| field(r, "coverage").as_deref() == Some("gap"))
        .count();
    let declared = matrix()
        .get("gap_ratchet")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| panic!("{MATRIX} must declare `gap_ratchet:`"))
        as usize;
    assert_eq!(
        declared, GAP_RATCHET,
        "{MATRIX}'s `gap_ratchet` and this test's constant disagree — they are one number in two \
         places and must be moved together."
    );
    if n > GAP_RATCHET {
        panic!(
            "{n} CDC axes are marked `gap`, the ratchet is {GAP_RATCHET}. Leaving an axis \
             unswept is a decision; make it out loud by raising the ratchet in the commit that \
             explains why, or cover it and lower the ratchet."
        );
    }
}
