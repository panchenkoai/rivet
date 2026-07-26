//! Drift-guard for `docs/release-gate-matrix.yaml` — the release-gate coverage ledger
//! (every gate CHECK × engine × VERSION + the backend-infra dimensions).
//!
//! Keeps the ledger honest against the gate's REAL config so it can't rot:
//!   1. `grid.<engine>.gated` == the versions dev/release-oracle/matrix.yaml brings up.
//!   2. Every `sc_<id>` / `verify_<id>` in scenarios.sh has a ledger row, and every
//!      ledger scenario / status:test preflight has its function — a new gate check
//!      with no cell (or a deleted check with a stale cell) fails here.
//!   3. Every scenario carries a cell for ALL four engines (column-completeness) and
//!      each cell is EXACTLY test / {na} / {gap}.
//!   4. Total admitted gaps <= the shrink-only ratchet.

use std::collections::BTreeSet;
use std::fs;

use serde_yaml_ng::Value;

const GATE_MATRIX: &str = "docs/release-gate-matrix.yaml";
const ORACLE_MATRIX: &str = "dev/release-oracle/matrix.yaml";
const SCENARIOS_SH: &str = "dev/release-oracle/lib/scenarios.sh";
const ENGINES: [&str; 4] = ["postgres", "mysql", "mssql", "mongo"];

// Total admitted gaps = scenario `gap` cells + preflight/infra `status: gap` +
// grid version gaps. Shrink-only: LOWER when a gap is filled; never raise.
// History: 14 (initial) -> 13 (pooler_safety wired, Phase 2).
// Now: infra gap rows(7) + grid version gaps(3+2+1+0=6) = 13.
const GAP_RATCHET: usize = 13;

fn load(path: &str) -> Value {
    let s = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_yaml_ng::from_str(&s).unwrap_or_else(|e| panic!("parse {path}: {e}"))
}

fn scalar(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => format!("{other:?}"),
    }
}

fn seq<'a>(v: &'a Value, key: &str) -> Vec<&'a Value> {
    v.get(key)
        .and_then(|s| s.as_sequence())
        .map(|s| s.iter().collect())
        .unwrap_or_default()
}

/// A cell is `test` (bare string) | `{na: ...}` | `{gap: ...}`.
fn cell_kind(v: &Value) -> Option<&'static str> {
    match v {
        Value::String(s) if s == "test" => Some("test"),
        Value::Mapping(m) if m.contains_key(Value::from("na")) => Some("na"),
        Value::Mapping(m) if m.contains_key(Value::from("gap")) => Some("gap"),
        _ => None,
    }
}

#[test]
fn grid_matches_the_oracle_matrix_versions() {
    let gate = load(GATE_MATRIX);
    let oracle = load(ORACLE_MATRIX);
    let grid = gate.get("grid").expect("gate matrix has a `grid`");
    let engines = oracle.get("engines").expect("oracle matrix has `engines`");

    for eng in ENGINES {
        let gated: BTreeSet<String> = grid
            .get(eng)
            .and_then(|e| e.get("gated"))
            .and_then(|g| g.as_sequence())
            .map(|s| s.iter().map(scalar).collect())
            .unwrap_or_else(|| panic!("grid.{eng}.gated missing"));
        let actual: BTreeSet<String> = engines
            .get(eng)
            .and_then(|e| e.get("versions"))
            .and_then(|v| v.as_sequence())
            .map(|s| s.iter().filter_map(|x| x.get("tag").map(scalar)).collect())
            .unwrap_or_else(|| panic!("oracle engines.{eng}.versions missing"));
        assert_eq!(
            gated, actual,
            "release-gate-matrix `grid.{eng}.gated` must EQUAL the versions dev/release-oracle/matrix.yaml brings up \
             — a version added to (or removed from) the gate without updating the ledger drifts them apart"
        );
    }
}

#[test]
fn every_gate_function_has_a_ledger_row_and_vice_versa() {
    let sh = fs::read_to_string(SCENARIOS_SH).unwrap();
    // Function DEFINITIONS live at column 0 (`sc_x() {` / `verify_x() {`); calls are indented.
    let mut sc_fns = BTreeSet::new();
    let mut verify_fns = BTreeSet::new();
    for line in sh.lines() {
        if let Some(rest) = line.strip_prefix("sc_")
            && rest.contains("()")
        {
            sc_fns.insert(rest.split('(').next().unwrap().trim().to_string());
        } else if let Some(rest) = line.strip_prefix("verify_")
            && rest.contains("()")
        {
            verify_fns.insert(rest.split('(').next().unwrap().trim().to_string());
        }
    }

    let gate = load(GATE_MATRIX);
    let scenario_ids: BTreeSet<String> = seq(&gate, "scenarios")
        .iter()
        .map(|s| scalar(s.get("id").unwrap()))
        .collect();
    let preflights: Vec<&Value> = seq(&gate, "preflights");
    let preflight_ids: BTreeSet<String> = preflights
        .iter()
        .map(|p| scalar(p.get("id").unwrap()))
        .collect();

    for f in &sc_fns {
        assert!(
            scenario_ids.contains(f),
            "scenarios.sh defines sc_{f}() but the gate matrix has no `scenarios` row `{f}`"
        );
    }
    for id in &scenario_ids {
        assert!(
            sc_fns.contains(id),
            "gate matrix scenario `{id}` has no sc_{id}() in scenarios.sh (renamed / deleted?)"
        );
    }
    for f in &verify_fns {
        assert!(
            preflight_ids.contains(f),
            "scenarios.sh defines verify_{f}() but the gate matrix has no `preflights` row `{f}`"
        );
    }
    // A preflight marked status:test MUST have its verify_ function; a `gap` preflight
    // (not yet wired) legitimately has none.
    for p in &preflights {
        let id = scalar(p.get("id").unwrap());
        if p.get("status").map(scalar).as_deref() == Some("test") {
            assert!(
                verify_fns.contains(&id),
                "preflight `{id}` is status:test but scenarios.sh has no verify_{id}()"
            );
        }
    }
}

#[test]
fn every_scenario_covers_all_engines_with_a_valid_cell() {
    let gate = load(GATE_MATRIX);
    for s in seq(&gate, "scenarios") {
        let id = scalar(s.get("id").unwrap());
        for eng in ENGINES {
            let cell = s.get(eng).unwrap_or_else(|| {
                panic!("scenario `{id}` is missing a cell for `{eng}` (column-completeness)")
            });
            assert!(
                cell_kind(cell).is_some(),
                "scenario `{id}` cell for `{eng}` must be EXACTLY `test` / {{na: ...}} / {{gap: ...}}"
            );
        }
    }
}

#[test]
fn gaps_do_not_exceed_the_ratchet() {
    let gate = load(GATE_MATRIX);
    let mut gaps = 0usize;

    for s in seq(&gate, "scenarios") {
        for eng in ENGINES {
            if s.get(eng).and_then(cell_kind) == Some("gap") {
                gaps += 1;
            }
        }
    }
    for section in ["preflights", "infra"] {
        for e in seq(&gate, section) {
            if e.get("status").map(scalar).as_deref() == Some("gap") {
                gaps += 1;
            }
        }
    }
    if let Some(grid) = gate.get("grid").and_then(|g| g.as_mapping()) {
        for (_, e) in grid {
            gaps += e
                .get("gaps")
                .and_then(|g| g.as_sequence())
                .map(|s| s.len())
                .unwrap_or(0);
        }
    }

    assert!(
        gaps <= GAP_RATCHET,
        "release-gate-matrix has {gaps} gaps > ratchet {GAP_RATCHET} — you cannot ADD a gap; \
         wire the check (flip gap -> test / gate the version) and LOWER the ratchet"
    );
}
