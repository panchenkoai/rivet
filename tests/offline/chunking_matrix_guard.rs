//! Drift-guard for the coverage ledgers — `docs/chunking-matrix.yaml` and
//! `docs/behaviour-matrix.yaml` (see [`MATRICES`]).
//!
//! The sparse-key footgun shipped because a whole guard had ZERO engine-level
//! tests and nobody noticed. This guard makes the ledgers self-protecting:
//!
//! 1. Every scenario MUST carry a cell for every column the matrix declares in
//!    its `engines:` list — no silently-missing column. Columns are dynamic
//!    (engine names like `postgres`, or warehouse targets like `bigquery`), so
//!    one guard covers ledgers keyed on either axis.
//! 2. Every cell is EXACTLY one of `test:` / `gap:` / `na:`.
//! 3. Every `test:` fn named in the ledger MUST exist in the repo — a renamed or
//!    deleted test can't silently orphan a matrix cell.
//! 4. The number of `gap:` cells MUST NOT exceed the ratchet baseline — you
//!    cannot ADD a gap. Filling a gap (gap → test) lets you lower the baseline;
//!    the ratchet only ever tightens.

use std::collections::{HashMap, HashSet};
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
    // tests/cdc_conformance_gate.rs. The 5 holes it surfaced (schema-drift on PG +
    // MSSQL, until_current-terminates-under-load on the three SQL engines) are now
    // filled — every cell is a test or a justified n/a.
    ("docs/cdc-matrix.yaml", 0),
    // Resilience / crash-recovery (BATCH + cross-cutting). Both Mongo holes closed:
    // batch-clobber filled with a live test; crash-after-source-read is na (that
    // hook is single.rs-only, and Mongo runs the keyset path).
    ("docs/resilience-matrix.yaml", 0),
    // Warehouse-load — the Parquet→warehouse-autoload axis, keyed on the 4
    // ExportTarget variants (duckdb/bigquery/snowflake/clickhouse), not source
    // engines. Caught + fixed 3 resolver bugs (SF/DuckDB/CH decimal ceilings). 0
    // gaps: every reachable degradation-prone (type × target) cell is tested.
    ("docs/warehouse-load-matrix.yaml", 0),
    // Fail-loud / error-surface — the inverse of silent corruption: every
    // unrecoverable degradation fails LOUD, not silently. Cross-references the CDC
    // conformance gate + chunking/resilience/warehouse ledgers for the unified view.
    ("docs/fail-loud-matrix.yaml", 0),
    // Load-mode write contracts — keyed on the 3 LoadMode variants (full /
    // incremental / cdc), not source engines. Codifies the 4 data bugs found in
    // the load layer (incremental+cleanup loss, full duplicate snapshots, full
    // ledger-skip defeating self-heal, failed-load ledger loss). 0 gaps: the last
    // one (no committed live incremental cell) is filled by incremental_dedup_mysql
    // — a live run_incremental harness cell (no-loss + cursor dedup + staging wiped).
    ("docs/load-mode-matrix.yaml", 0),
    // Fuzz coverage — the untrusted-input PARSE surface Rivet owns, per engine.
    // `test:` cells name a cargo-fuzz target (`rivet::fuzz` entry fn in src/fuzz.rs,
    // run nightly by fuzz.yml); the many `na:` cells prove structural immunity
    // (binary protocol decoded by the driver crate, panic-safe field access, or
    // write-only). 0 gaps: the surface Rivet parses is fully covered.
    ("docs/fuzz-matrix.yaml", 0),
    // URL & credential safety — the userinfo encode/decode/redact class that
    // regressed THREE times (round-1 MSSQL round-trip, round-3 redact_pg_url, and
    // the general log redactor), each invisible to point tests. `test:` cells are
    // round-trip + data-driven redaction sweeps; `na:` cells are driver-owned
    // parses or state-URL seams that don't exist per engine. 0 gaps.
    ("docs/url-safety-matrix.yaml", 0),
    // Durability ordering — the destination manifest is durable BEFORE the delivery
    // position advances, and the manifest/_SUCCESS pair stays consistent. This class
    // regressed twice (round-2 #11/#12) and escaped resilience/cdc because their
    // crash cells asserted via a parquet GLOB, masking the manifest-orphan class;
    // every `test:` here asserts MANIFEST-DRIVEN (the loader's view). 0 gaps.
    ("docs/durability-ordering-matrix.yaml", 0),
    // Config-validation — the accept-but-break class (round-2 #14/#15/#16/#17/#5/#6):
    // a config that passed validation but silently degraded / died at run. Each
    // scenario asserts the combo is rejected at CONFIG-LOAD (check == run) AND a
    // legit form is not false-rejected. 0 gaps.
    ("docs/config-validation-matrix.yaml", 0),
    // CSV writer-fidelity — the TEXT-writer class round-7 opened: the CSV writer has
    // its own value rendering AND escaping that Parquet's binary path never exercises,
    // and two silent losses lived there (pre-1970 timestamp → empty cell; un-escaped
    // header split off the data). Columns split silent-value-loss vs escape-corruption;
    // the value cells assert against an INDEPENDENT oracle (hard-coded string / DuckDB
    // re-read), not the writer's own rendering (the self-oracle that hid the bug). 0 gaps.
    ("docs/csv-fidelity-matrix.yaml", 0),
    // Runner-coverage — every PER-EXPORT feature applied on EVERY runner (single /
    // chunked / keyset / mongo_parallel), not just single. Round-8 proved the class:
    // keyset + parallel-Mongo silently dropped the on_schema_drift gate (exit 0 on
    // drift) because it lived only in single/chunked. Building this ledger surfaced a
    // bigger hole — value-checksum Form B was ABSENT on all three large-table runners.
    // Round-9 THREADED Form B through every runner (run-wide XOR harvest via the shared
    // commit::{accumulate,harvest}_column_checksums seam), each with a live test that
    // asserts the manifest records it AND `rivet validate` re-reads + matches (6 → 3).
    // The last 3 then closed: a chunked-range + a parallel-Mongo drift live test, a
    // parallel-Mongo clobber live test, and mongo schema_drift reclassified `na` (a
    // Mongo Arrow schema is a fixed {_id, document, meta} shape — the verbatim-blob
    // document column cannot structurally drift). 0 gaps — every runner × feature cell
    // is a test or a justified n/a.
    ("docs/runner-coverage-matrix.yaml", 0),
];

#[derive(Deserialize)]
struct Matrix {
    /// The column set — every scenario must carry a cell for each. Declared
    /// per-matrix so a ledger can be keyed on engines (postgres/mysql/…) or on
    /// warehouse targets (duckdb/bigquery/…) with the same guard.
    engines: Vec<String>,
    scenarios: Vec<Scenario>,
}

/// `id` + `what:` (docs) are named fields; every other key flattens into
/// `cells` as one Cell per column. A missing column is caught at validation
/// (against `Matrix::engines`), a malformed cell at deserialization.
#[derive(Deserialize)]
struct Scenario {
    id: String,
    #[serde(default, rename = "what")]
    _what: Option<String>,
    #[serde(flatten)]
    cells: HashMap<String, Cell>,
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

impl Scenario {
    /// `(column, cell)` for every column the matrix declares; panics if a
    /// column is absent (a silently-missing column is exactly the hole the
    /// guard exists to catch).
    fn resolved_cells<'a>(&'a self, engines: &'a [String], path: &str) -> Vec<(&'a str, &'a Cell)> {
        engines
            .iter()
            .map(|eng| {
                let cell = self.cells.get(eng).unwrap_or_else(|| {
                    panic!("{path} scenario '{}' is missing column '{}'", self.id, eng)
                });
                (eng.as_str(), cell)
            })
            .collect()
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
                // name that no longer exists (the failure mode we guard). Strip
                // visibility/async modifiers first: a bare `fn `-only scan
                // would false-orphan a matrix test declared `pub fn`/`async fn`
                // (latent fragility the matrix audit flagged).
                let mut rest = line.trim_start();
                for prefix in [
                    "pub(crate) ",
                    "pub(super) ",
                    "pub ",
                    "async ",
                    "const ",
                    "unsafe ",
                ] {
                    if let Some(r) = rest.strip_prefix(prefix) {
                        rest = r;
                    }
                }
                if let Some(rest) = rest.strip_prefix("fn ")
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
        assert!(
            !matrix.engines.is_empty(),
            "{path} declares no engines/columns"
        );
        for sc in &matrix.scenarios {
            for (eng, cell) in sc.resolved_cells(&matrix.engines, path) {
                assert_eq!(
                    cell.kind_count(),
                    1,
                    "{path} scenario '{}' column '{}': a cell must be exactly one of test/gap/na",
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
            for (eng, cell) in sc.resolved_cells(&matrix.engines, path) {
                if let Some(test) = &cell.test {
                    assert!(
                        fns.contains(test),
                        "{path} scenario '{}' column '{}' maps to test `{}`, but no `fn {}` exists \
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
            .flat_map(|sc| sc.resolved_cells(&matrix.engines, path))
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
