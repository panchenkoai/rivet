//! Drift-guard for the coverage ledgers — `docs/chunking-matrix.yaml` and
//! `docs/behaviour-matrix.yaml` (see [`MATRICES`]).
//!
//! The sparse-key footgun shipped because a whole guard had ZERO engine-level
//! tests and nobody noticed. This guard makes the ledgers self-protecting:
//!
//! 1. Every scenario MUST carry a cell for every engine (deserialization fails
//!    otherwise) — no silently-missing engine.
//! 2. Every cell is EXACTLY one of `test:` / `gap:` / `na:`.
//! 3. Every `test:` fn named in the ledger MUST exist in the repo — a renamed or
//!    deleted test can't silently orphan a matrix cell.
//! 4. The number of `gap:` cells MUST NOT exceed the ratchet baseline — you
//!    cannot ADD a gap. Filling a gap (gap → test) lets you lower the baseline;
//!    the ratchet only ever tightens.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The coverage ledgers and each one's admitted-gap ratchet ceiling. LOWER a
/// ceiling every time a gap flips to a test; never raise it (the ratchet only
/// tightens). Both matrices are currently at 0 — every cell is a test or a
/// justified n/a, so any new gap fails CI outright.
///
/// chunking-matrix ratchet history: 18 → 14 (null-keyed ×3 + MSSQL keyset-resume)
/// → 12 (chunk_count ×2) → 7 (chunk_by_days ×3 + keyset-non-usable ×2) → 3
/// (sparse-gappy ×2 + memory_mb-PG + keyset-auto-MSSQL→na) → 1 (small-table-escape
/// ×2→na) → 0 (chunk_count-Mongo→na).
const MATRICES: &[(&str, usize)] = &[
    ("docs/chunking-matrix.yaml", 0),
    ("docs/behaviour-matrix.yaml", 0),
    ("docs/type-fidelity-matrix.yaml", 0),
    // Cross config × db: 15 honest holes on the non-PG engines (cloud dests, codec
    // parity, csv, tuning profile) — visible + un-growable; fill by writing the test.
    ("docs/cross-config-matrix.yaml", 0),
    // CDC — the most engine-divergent surface (12 scenarios × 4 engines). Complements
    // tests/cdc_conformance_gate.rs. 5 honest holes it surfaced: schema-drift on PG +
    // MSSQL, and dedicated until_current-terminates-under-load on the three SQL engines.
    ("docs/cdc-matrix.yaml", 5),
];

#[derive(Deserialize)]
struct Matrix {
    scenarios: Vec<Scenario>,
}

/// Every scenario names all four engine cells explicitly, so a missing engine is
/// a deserialization error, not a silent hole.
// `what:` in the YAML is human-facing documentation; serde ignores unknown keys,
// so the struct needn't carry it.
#[derive(Deserialize)]
struct Scenario {
    id: String,
    postgres: Cell,
    mysql: Cell,
    mssql: Cell,
    mongo: Cell,
}

impl Scenario {
    fn cells(&self) -> [(&str, &Cell); 4] {
        [
            ("postgres", &self.postgres),
            ("mysql", &self.mysql),
            ("mssql", &self.mssql),
            ("mongo", &self.mongo),
        ]
    }
}

#[derive(Deserialize)]
struct Cell {
    test: Option<String>,
    gap: Option<String>,
    na: Option<String>,
}

impl Cell {
    /// Exactly one of test/gap/na must be set.
    fn kind_count(&self) -> usize {
        self.test.is_some() as usize + self.gap.is_some() as usize + self.na.is_some() as usize
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn load_matrix(rel: &str) -> Matrix {
    let path = repo_root().join(rel);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse {rel}: {e}"))
}

/// Every `fn <name>` defined anywhere under src/ or tests/. Built once so the
/// per-cell existence check is a set lookup, not a re-scan.
fn all_fn_names() -> HashSet<String> {
    let mut names = HashSet::new();
    for dir in ["src", "tests"] {
        collect_fn_names(&repo_root().join(dir), &mut names);
    }
    names
}

fn collect_fn_names(dir: &Path, out: &mut HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_fn_names(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in text.lines() {
                // Cheap `fn <ident>` scan — good enough to catch a mapped test
                // name that no longer exists (the failure mode we guard).
                if let Some(rest) = line.trim_start().strip_prefix("fn ")
                    && let Some(name) = rest
                        .split(|c: char| !c.is_alphanumeric() && c != '_')
                        .next()
                    && !name.is_empty()
                {
                    out.insert(name.to_string());
                }
            }
        }
    }
}

#[test]
fn matrix_every_cell_is_exactly_one_kind() {
    for (path, _) in MATRICES {
        let matrix = load_matrix(path);
        assert!(!matrix.scenarios.is_empty(), "{path} has no scenarios");
        for sc in &matrix.scenarios {
            for (eng, cell) in sc.cells() {
                assert_eq!(
                    cell.kind_count(),
                    1,
                    "{path} scenario '{}' engine '{}': a cell must be exactly one of test/gap/na",
                    sc.id,
                    eng
                );
            }
        }
    }
}

#[test]
fn matrix_every_mapped_test_exists() {
    let fns = all_fn_names();
    for (path, _) in MATRICES {
        let matrix = load_matrix(path);
        for sc in &matrix.scenarios {
            for (eng, cell) in sc.cells() {
                if let Some(test) = &cell.test {
                    assert!(
                        fns.contains(test),
                        "{path} scenario '{}' engine '{}' maps to test `{}`, but no `fn {}` exists \
                         under src/ or tests/ — a renamed/deleted test orphaned a matrix cell",
                        sc.id,
                        eng,
                        test,
                        test
                    );
                }
            }
        }
    }
}

#[test]
fn matrix_gaps_do_not_exceed_ratchet() {
    for (path, ceiling) in MATRICES {
        let matrix = load_matrix(path);
        let gaps: usize = matrix
            .scenarios
            .iter()
            .flat_map(|sc| sc.cells())
            .filter(|(_, c)| c.gap.is_some())
            .count();
        // Exactly-equal is the ratchet in BOTH directions: `> ceiling` means a gap
        // was ADDED (fill it — gaps can't grow); `< ceiling` means one was FILLED
        // (lower the ceiling in MATRICES to lock the win).
        assert_eq!(
            gaps, *ceiling,
            "{path} has {gaps} admitted gaps; the ratchet expects exactly {ceiling}. \
             If {gaps} > {ceiling}: you ADDED a gap — fill it with a test (gaps cannot grow). \
             If {gaps} < {ceiling}: you FILLED one — lower the ceiling in MATRICES to {gaps}."
        );
    }
}
