//! Drift-guard for `docs/chunking-matrix.yaml` — the chunking coverage ledger.
//!
//! The sparse-key footgun shipped because a whole guard had ZERO engine-level
//! tests and nobody noticed. This guard makes the ledger self-protecting:
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

/// Admitted-gap ceiling. LOWER this each time a gap flips to a test; never raise
/// it. 18 at introduction (after the sparse-guard row was filled by the stand).
const MAX_GAPS: usize = 18;

#[derive(Deserialize)]
struct Matrix {
    scenarios: Vec<Scenario>,
}

/// Every scenario names all four engine cells explicitly, so a missing engine is
/// a deserialization error, not a silent hole.
#[derive(Deserialize)]
struct Scenario {
    id: String,
    #[allow(dead_code)]
    what: String,
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

fn load_matrix() -> Matrix {
    let path = repo_root().join("docs/chunking-matrix.yaml");
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse chunking-matrix.yaml: {e}"))
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
fn chunking_matrix_every_cell_is_exactly_one_kind() {
    let matrix = load_matrix();
    assert!(!matrix.scenarios.is_empty(), "matrix has no scenarios");
    for sc in &matrix.scenarios {
        for (eng, cell) in sc.cells() {
            assert_eq!(
                cell.kind_count(),
                1,
                "scenario '{}' engine '{}': a cell must be exactly one of test/gap/na",
                sc.id,
                eng
            );
        }
    }
}

#[test]
fn chunking_matrix_every_mapped_test_exists() {
    let matrix = load_matrix();
    let fns = all_fn_names();
    for sc in &matrix.scenarios {
        for (eng, cell) in sc.cells() {
            if let Some(test) = &cell.test {
                assert!(
                    fns.contains(test),
                    "scenario '{}' engine '{}' maps to test `{}`, but no `fn {}` exists under \
                     src/ or tests/ — a renamed/deleted test orphaned a matrix cell",
                    sc.id,
                    eng,
                    test,
                    test
                );
            }
        }
    }
}

#[test]
fn chunking_matrix_gaps_do_not_exceed_ratchet() {
    let matrix = load_matrix();
    let gaps: usize = matrix
        .scenarios
        .iter()
        .flat_map(|sc| sc.cells())
        .filter(|(_, c)| c.gap.is_some())
        .count();
    assert!(
        gaps <= MAX_GAPS,
        "chunking-matrix.yaml has {gaps} admitted gaps but the ratchet ceiling is {MAX_GAPS}. \
         You cannot ADD a gap — fill it with a test. (When you flip a gap to a test, LOWER \
         MAX_GAPS to match.)"
    );
    // Nudge the ceiling down as gaps are filled: if this fires, lower MAX_GAPS.
    assert_eq!(
        gaps, MAX_GAPS,
        "gaps ({gaps}) dropped below the ratchet ({MAX_GAPS}) — lower MAX_GAPS to {gaps} to lock in the win"
    );
}
