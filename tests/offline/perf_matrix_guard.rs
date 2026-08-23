//! Drift-guard for `docs/perf-matrix.yaml` — the perf-baseline ledger. It was the ONE
//! coverage matrix with no guard because its cells are `{measured: ...}` (a captured
//! number), not the `test/gap/na` the shared chunking_matrix_guard understands. This
//! closes that hole with the same discipline:
//!   1. every scenario carries a cell for ALL four engines (column-completeness);
//!   2. each cell is EXACTLY `{measured}` / `{gap}` / `{na}`;
//!   3. a `measured` cell is a NON-EMPTY record (a number like rows_per_s, or a
//!      qualitative note/class — but not an empty `{}` placeholder);
//!   4. total `gap` cells <= the shrink-only ratchet.

use std::fs;

use serde_yaml_ng::Value;

const PERF_MATRIX: &str = "docs/perf-matrix.yaml";
const ENGINES: [&str; 4] = ["postgres", "mysql", "mssql", "mongo"];

// Shrink-only: LOWER when a gap is filled with a measured baseline; never raise. The
// matrix currently has ZERO `{gap}` cells, so the ratchet is 0 — for usize `<= 0` is
// exact equality, so ANY measured->gap downgrade (a lost baseline) fails immediately,
// not after N accumulate (matching the sibling guards' zero-slack convention).
const GAP_RATCHET: usize = 0;

fn load() -> Value {
    let s = fs::read_to_string(PERF_MATRIX).unwrap_or_else(|e| panic!("read {PERF_MATRIX}: {e}"));
    serde_yaml_ng::from_str(&s).unwrap_or_else(|e| panic!("parse {PERF_MATRIX}: {e}"))
}

fn scalar(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        other => format!("{other:?}"),
    }
}

/// The `scenarios:` rows, floor-checked.
///
/// The `unwrap_or_default()` below is why the floor is here and not left to the
/// callers: a renamed or re-nested key yields the EMPTY vector, and all three
/// tests in this file then pass over it — the column-completeness loop grades no
/// rows, `measured_cells_are_non_empty` grades no cells, and the gap count is
/// zero, which is exactly the ratchet. Every signal green, nothing measured.
fn scenarios(v: &Value) -> Vec<&Value> {
    let out: Vec<&Value> = v
        .get("scenarios")
        .and_then(|s| s.as_sequence())
        .map(|s| s.iter().collect())
        .unwrap_or_default();
    // 7 rows today; the floor catches a lost SECTION, not an edited row.
    super::nonvacuity::require_enumerated(
        out.len(),
        4,
        &format!("`scenarios:` rows in {PERF_MATRIX}"),
        "Re-point this parser at wherever the perf scenarios now live — a perf ledger that \
         parses to nothing reports a full house of measured baselines it never read.",
    );
    out
}

/// A perf cell is `{measured: ...}` | `{gap: ...}` | `{na: ...}`.
fn cell_kind(v: &Value) -> Option<&'static str> {
    let m = v.as_mapping()?;
    for k in ["measured", "gap", "na"] {
        if m.contains_key(Value::from(k)) {
            return Some(match k {
                "measured" => "measured",
                "gap" => "gap",
                _ => "na",
            });
        }
    }
    None
}

#[test]
fn every_perf_scenario_covers_all_engines_with_a_valid_cell() {
    let m = load();
    // r5 bughunt (restored r6 — lost when a RED-prove `git checkout` of a
    // mutant reverted this same file): this hand-typed list is ungoverned by
    // the generative column-completeness guard (perf-matrix is EXEMPT), so a
    // new SourceType variant would leave this matrix un-forced. Assert the
    // list IS the enum — RED until the matrix and this const gain the column.
    {
        let derived = super::chunking_matrix_guard::source_engine_variants();
        let listed: std::collections::HashSet<String> =
            ENGINES.iter().map(|e| e.to_string()).collect();
        assert_eq!(
            listed, derived,
            "ENGINES hand-list drifted from the SourceType enum: listed {listed:?} vs enum {derived:?}"
        );
    }
    for s in scenarios(&m) {
        let id = s.get("id").map(scalar).unwrap_or_default();
        for eng in ENGINES {
            let cell = s
                .get(eng)
                .unwrap_or_else(|| panic!("perf scenario `{id}` is missing a cell for `{eng}`"));
            assert!(
                cell_kind(cell).is_some(),
                "perf scenario `{id}` cell for `{eng}` must be `{{measured}}` / `{{gap}}` / `{{na}}`"
            );
        }
    }
}

#[test]
fn measured_cells_are_non_empty() {
    let m = load();
    for s in scenarios(&m) {
        let id = s.get("id").map(scalar).unwrap_or_default();
        for eng in ENGINES {
            let Some(cell) = s.get(eng) else { continue };
            if cell_kind(cell) == Some("measured") {
                let measured = cell.get("measured").unwrap();
                // A number (rows_per_s, peak_rss_mb, *_x) OR a qualitative note/class —
                // but not an empty `{}` placeholder that claims a baseline it doesn't hold.
                let non_empty = measured
                    .as_mapping()
                    .map(|mm| !mm.is_empty())
                    .unwrap_or_else(|| !matches!(measured, Value::Null));
                assert!(
                    non_empty,
                    "perf scenario `{id}` cell for `{eng}` is `measured` but empty — a placeholder, not a baseline"
                );
            }
        }
    }
}

#[test]
fn perf_gaps_do_not_exceed_the_ratchet() {
    let m = load();
    let mut gaps = 0usize;
    for s in scenarios(&m) {
        for eng in ENGINES {
            if s.get(eng).and_then(cell_kind) == Some("gap") {
                gaps += 1;
            }
        }
    }
    // Exact equality (zero-slack, like chunking_matrix_guard): a NEW gap fails immediately
    // and a filled one must LOWER the ratchet — no silent measured->gap downgrade window.
    assert_eq!(
        gaps, GAP_RATCHET,
        "perf-matrix gap count changed — a measured->gap downgrade (lost baseline) or a new \
         unmeasured cell. Measure the baseline, or intentionally set GAP_RATCHET to {gaps}."
    );
}
