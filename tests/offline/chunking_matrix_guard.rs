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
//! 5. GENERATIVE column-completeness: a matrix keyed on source engines must
//!    declare EVERY `SourceType`, one keyed on warehouse targets must declare
//!    EVERY `ExportTarget` — the required set is derived from the enums THEMSELVES
//!    (parsed from product source), so a new engine/target, or a silently-dropped
//!    column, forces a `test`/`gap`/`na` cell instead of an invisible hole. Guards
//!    1-4 are DESCRIPTIVE (they only check what an author wrote down); this one is
//!    GENERATIVE (product code enumerates what MUST be there) — the coverage-audit
//!    meta-fix that stops the un-enumerated-sibling class at CI.
//! 6. GENERATIVE row-completeness (the sibling of #5 on the OTHER axis): every
//!    `RivetType` variant must map to a `type_*` scenario row in the CDC
//!    type-fidelity matrix — derived from the `RivetType` enum itself. #5 stops a
//!    dropped/missing COLUMN (engine/target); #6 stops a dropped/missing ROW
//!    (type). Together the column AND row axes are product-code-enumerated, so the
//!    per-type-CDC quadrant (findings #2/#3/#4) can no longer grow a silent hole.
//! 7. ORACLE-STRENGTH ratchet: #1-#6 ensure a cell EXISTS and names a real test;
//!    this grades HOW STRONG the oracle is. A `differential` cell (CDC==batch) is a
//!    self-oracle over the SHARED decode — it passes a bug both siblings share (the
//!    class that hid #5/#6/#8, and that the value-checksum Form A also misses). The
//!    ratchet counts the weak (differential/self/un-annotated) `test:` cells per
//!    oracle-tracked matrix and forbids the count from GROWING; upgrading a cell to
//!    an INDEPENDENT oracle (a DuckDB/source-vs-dest re-read, outside rivet's decode
//!    family) lowers the ceiling. So the shared-decode self-oracle debt is visible
//!    and monotonically shrinks toward 0.

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
/// Coverage ledgers that this guard does NOT cover, each naming the guard that
/// does. Membership is checked against the file GLOB, and each named guard file
/// must exist — a claim of coverage is not coverage.
///
/// The list used to be implicit: `MATRICES` held 20 of the 25
/// `docs/*matrix*.yaml` files and nothing compared it to what is on disk, so a
/// ledger could sit ungoverned indefinitely and read as governed. One of the
/// five did worse than that — `scenario-artifact-matrix.yaml` was not valid
/// YAML (three keys at column 0 among siblings indented 6) and neither of its
/// two readers noticed, because both scan lines and strip them rather than
/// parsing. Fixed in the same commit; the parse check below is what would have
/// caught it (audit 2026-08-17).
const EXEMPT: &[(&str, &str)] = &[
    (
        "docs/attestation-matrix.yaml",
        "tests/offline/attestation_matrix_guard.rs",
    ),
    (
        "docs/cdc-axis-matrix.yaml",
        "tests/offline/cdc_axis_matrix_guard.rs",
    ),
    (
        "docs/perf-matrix.yaml",
        "tests/offline/perf_matrix_guard.rs",
    ),
    (
        "docs/release-gate-matrix.yaml",
        "tests/offline/release_gate_matrix_guard.rs",
    ),
    (
        "docs/scenario-artifact-matrix.yaml",
        "tests/offline/scenario_artifact_matrix_guard.rs",
    ),
];

const MATRICES: &[(&str, usize)] = &[
    ("docs/chunking-matrix.yaml", 0),
    // Export-STRATEGY flag × engine, verified on GOLDEN fixtures + a distilled
    // GARBAGE profile (anonymized shape of a 200+-table field DB). Two layers:
    // the offline scaffold_strategy oracle (all shapes) + the live chunking_stand
    // (representative subset). Engine-specific garbage failure modes (unsigned
    // cursor = MySQL, regclass-throw = PG, STRING_AGG cap = MSSQL) are one test
    // + justified n/a. 0 gaps — every cell is a test or a justified n/a.
    ("docs/cli-flag-matrix.yaml", 0),
    // Destination-backend correctness (local/gcs/s3/azure × scenario): the dogfood
    // cloud findings (prefix normalization B, --validate-is-advisory A) + the
    // emulator round-trip + cross-backend parity.
    //
    // Lowered 1 -> 0 (2026-08-18): the azurite full-round-trip is written
    // (`stand_dest_azure_{mysql,postgres}`), reading CONTENT back through the same
    // `azure_parquet_total_rows` the multipart test uses rather than a second
    // per-backend definition of delivered.
    ("docs/destination-matrix.yaml", 0),
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
    // CLI-surface — CROSS-COMMAND contracts keyed on the state-inspect COMMAND
    // (not engine/target). The 0.21.2 dogfood found the "wired into only some
    // commands" class (typo'd -e accepted silently on files/chunks/progression
    // while metrics/journal/reset rejected it). Makes each contract × command a
    // cell so a NEW inspect command that skips it goes red, not silent. 0 gaps.
    ("docs/cli-surface-matrix.yaml", 0),
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
    // document column cannot structurally drift).
    //
    // 0 → 2 on 2026-08-16, and this is the ONE direction the ratchet may move up: the
    // `chunked` column was SPLIT into `chunked` (chunked/exec.rs) + `chunked_checkpoint`
    // (sequential_checkpoint.rs + parallel_checkpoint.rs), the two runner families
    // job.rs dispatches on `is_resumable()`. Nothing regressed; two admissions that
    // already existed in PROSE became cells a guard can read. The allowance is for
    // EXACTLY these two, and no others:
    //   1. adaptive_concurrency_governor × chunked_checkpoint — the checkpoint runner's
    //      END-TO-END shed has only an ad-hoc live run; the wiring + pool-shed mechanism
    //      are proven offline. Was a paragraph in the cell's own `what` that said
    //      "OPEN … not a committed test" next to a `test:` cell. Fill = a checkpoint
    //      twin of governor_backs_off_under_concurrent_write_pressure.
    // FILLED 2026-08-16 (2 → 1): failed_chunk_fails_the_run × chunked. Plain
    // chunked_PARALLEL bails on collected worker errors AFTER the loop (exec.rs), the
    // end-of-loop guard shape no PANIC test can reach, and it was not merely untested
    // but UNTESTABLE — maybe_error_at_index("chunk_export", …) was wired into the two
    // checkpoint runners and keyset/mongo, never into exec.rs. Wiring the hook there
    // (the returning error a panic can never be) let the checkpoint-less twin
    // `a_failed_chunk_must_fail_the_plain_parallel_run_not_ship_a_short_export` go RED
    // against the removed guard: 100 of 150 rows shipped with `status: success`, exit 0.
    // Unlike the checkpoint twin (RED only with BOTH guards off) this is a single-guard
    // RED — the plain runner keeps no chunk_task ledger, so that one bail is all there is.
    //
    // Lowered 1 -> 0 (2026-08-18): the last admitted gap — the parallel checkpoint
    // runner's END-TO-END governor shed, which two offline proofs could not reach —
    // was filled by `checkpoint_governor_backs_off_under_concurrent_write_pressure`.
    // At 0 this ledger admits nothing: any new gap cell fails here immediately.
    ("docs/runner-coverage-matrix.yaml", 0),
    // Pool-split — `apply --pool --split` per (strategy × source engine). Split is a
    // scheduler layer above the runners (each unit runs through chunked/keyset), so its
    // per-engine behaviour (boundary probe, crash-recovery, finding-2 exact-partition
    // resume) is proven on every SQL engine via the Rig stand + a DuckDB manifest oracle;
    // Mongo is `na` (no inline SQL range literal → left whole). 0 gaps.
    ("docs/pool-split-matrix.yaml", 0),
    // CDC per-type value fidelity — the change-stream sibling of type-fidelity, the
    // axis where findings #2 (MSSQL MONEY>2^53), #3 (MySQL ENUM cross-db) and #4
    // (BIT(64) bit 63) lived: batch correct, CDC/edge sibling not. Workhorse cells
    // cite each engine's *_cdc_full_type_matrix_matches_batch (ArrayData equality
    // CDC==batch); edge scenarios cite the range-specific tests. Row axis is
    // GENERATIVELY complete over RivetType (matrix_cdc_type_rows_cover_every_rivet_type).
    // 0 gaps: every (type × engine) cell is a test or a justified n/a.
    ("docs/cdc-type-fidelity-matrix.yaml", 0),
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
    /// Oracle STRENGTH of a `test:` cell (guard #7, on oracle-tracked matrices):
    /// `independent` (a reader OUTSIDE rivet's decode family — foreign reader /
    /// source-vs-dest / hard-coded literal — catches a SHARED-decode value bug),
    /// `fail_loud` (asserts the error path, not a value compare), `differential`
    /// (sibling-vs-sibling, e.g. CDC==batch — the self-oracle that MISSES a shared
    /// bug), `self`. ABSENT on a `test:` cell defaults to `differential` (weak),
    /// so a new cell is assumed weak until proven independent.
    oracle: Option<String>,
}

impl Cell {
    /// Exactly one of test/gap/na must be set.
    fn kind_count(&self) -> usize {
        self.test.is_some() as usize + self.gap.is_some() as usize + self.na.is_some() as usize
    }

    /// Is this a `test:` cell whose oracle could HIDE a shared-decode value bug?
    /// (`differential`/`self`, or an un-annotated test — weak by default.) The
    /// ratchet target: drive these to `independent`/`fail_loud`, never grow them.
    fn is_weak_oracle(&self) -> bool {
        self.test.is_some()
            && !matches!(
                self.oracle.as_deref(),
                Some("independent") | Some("fail_loud")
            )
    }
}

/// The valid `oracle:` strengths (typo-guarded).
const ORACLE_STRENGTHS: &[&str] = &["independent", "differential", "self", "fail_loud"];

/// Oracle-tracked matrices + their weak-oracle (differential/self/un-annotated)
/// ratchet ceiling. LOWER the ceiling each time a cell is upgraded from a
/// batch-differential to an INDEPENDENT oracle (a DuckDB/source re-read); never
/// raise it — the ratchet drives the shared-decode-blind self-oracle debt to 0.
const ORACLE_TRACKED: &[(&str, usize)] = &[
    // CDC value-decode — the differential debt was ground to 0: every (type × SQL
    // engine) cell is now an INDEPENDENT DuckDB-vs-source oracle (via the three
    // *_cdc_typed_values_match_source_via_duckdb_not_batch tests) or a fail_loud/na,
    // never the shared-decode *_matches_batch self-oracle. Ceiling 0: any new
    // differential CDC-type cell fails CI.
    ("docs/cdc-type-fidelity-matrix.yaml", 0),
    // Batch type fidelity — 24 independent value cells (golden hard-coded + DuckDB/
    // pyarrow foreign readers); the last 6 schema-only cells were repointed to the
    // per-column null-profile source-vs-dest oracle. Ceiling 0.
    ("docs/type-fidelity-matrix.yaml", 0),
];

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

/// Every fn under src/ or tests/ that actually RUNS as a test — i.e. one carrying a
/// test attribute (`#[test]`, `#[tokio::test]`, `#[rstest]`, `#[test_case(..)]`).
///
/// [`all_fn_names`] answers "does this name exist", which is one step short of what a
/// `test:` cell claims. A helper (`manifest_count`, `seed_pg_wide_table`) satisfies the
/// existence check while proving nothing, so "fill the gap" can be faked by naming a
/// helper — the ledger-grading sibling of "a coverage ledger must grade the CALL SITE,
/// not the definition". This set is the call-site half: a cell may only name something
/// the test runner will execute.
fn all_test_fn_names() -> HashSet<String> {
    let mut names = HashSet::new();
    for dir in ["src", "tests"] {
        collect_test_fn_names(&repo_root().join(dir), &mut names);
    }
    names
}

/// Is this line a TEST attribute (not `#[cfg(test)]`, not `#[ignore]`)? Compares the
/// attribute path only — `#[test]`, `#[tokio::test]`, `#[rstest]`, `#[test_case(..)]`.
fn is_test_attr(line: &str) -> bool {
    let Some(rest) = line.trim_start().strip_prefix("#[") else {
        return false;
    };
    let path: &str = rest.split(['(', ']', '=']).next().unwrap_or("").trim();
    path == "test" || path.ends_with("::test") || path == "rstest" || path == "test_case"
}

fn collect_test_fn_names(dir: &Path, out: &mut HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_test_fn_names(&path, out);
            continue;
        }
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        // A test fn is preceded by a contiguous run of attributes/doc comments, at
        // least one of which is a test attribute. Any other statement resets the run.
        let mut armed = false;
        for line in text.lines() {
            let t = line.trim_start();
            if is_test_attr(t) {
                armed = true;
                continue;
            }
            if t.starts_with("#[") || t.starts_with("//") || t.is_empty() {
                continue; // #[ignore]/#[should_panic]/docs sit between the attr and the fn
            }
            let mut rest = t;
            for prefix in ["pub(crate) ", "pub(super) ", "pub ", "async ", "unsafe "] {
                if let Some(r) = rest.strip_prefix(prefix) {
                    rest = r;
                }
            }
            if armed
                && let Some(rest) = rest.strip_prefix("fn ")
                && let Some(name) = rest
                    .split(|c: char| !c.is_alphanumeric() && c != '_')
                    .next()
                && !name.is_empty()
            {
                out.insert(name.to_string());
            }
            armed = false;
        }
    }
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

/// Matrices whose `test:` cells legitimately name something that is NOT a `#[test]` fn,
/// each with the reason. Keep this list at one entry unless a ledger genuinely grades a
/// non-`cargo test` runner.
const NON_TEST_CELL_MATRICES: &[(&str, &str)] = &[(
    "docs/fuzz-matrix.yaml",
    "cells name cargo-fuzz ENTRY fns in src/fuzz.rs (driven by fuzz.yml nightly), which \
     are `pub fn`, not `#[test]` fns — asserted to live in src/fuzz.rs instead",
)];

/// Guard #3b — a `test:` cell must name a fn the test runner EXECUTES, not merely one
/// that exists. [`matrix_every_mapped_test_exists`] accepts any `fn <name>` anywhere
/// under src/ or tests/, so a helper (`manifest_count`, `chunk_run_id`, a seeding fn)
/// satisfies it while proving nothing — and that is the cheapest way to make a `gap`
/// look filled, which is exactly the move the gap ratchet makes tempting. Same rule as
/// `every_gate_function_has_a_call_site` in release_gate_matrix_guard: registration is a
/// claim about behaviour, only the runnable call site is evidence.
#[test]
fn matrix_mapped_tests_are_real_test_functions() {
    let test_fns = all_test_fn_names();
    // Parse sanity: a broken collector would silently accept everything.
    assert!(
        test_fns.len() > 500 && test_fns.contains("matrix_every_mapped_test_exists"),
        "test-attribute scan produced {} names — the collector is broken, so this guard \
         would pass vacuously",
        test_fns.len()
    );
    let fuzz_src = std::fs::read_to_string(repo_root().join("src/fuzz.rs")).unwrap_or_default();
    for (path, _) in MATRICES {
        let exempt = NON_TEST_CELL_MATRICES.iter().find(|(p, _)| p == path);
        let matrix = load_matrix(path);
        for sc in &matrix.scenarios {
            for (eng, cell) in sc.resolved_cells(&matrix.engines, path) {
                let Some(name) = &cell.test else { continue };
                if test_fns.contains(name) {
                    continue;
                }
                let Some((_, why)) = exempt else {
                    panic!(
                        "{path} scenario '{}' column '{}' maps to `{name}`, which exists but \
                         carries NO test attribute (#[test]/#[tokio::test]/…) — a helper fn is \
                         not a proof. Name the test that RUNS, or record the cell as a `gap:` \
                         with the reason.",
                        sc.id, eng
                    );
                };
                assert!(
                    fuzz_src.contains(&format!("fn {name}(")),
                    "{path} scenario '{}' column '{}' maps to `{name}`, which is neither a \
                     #[test] fn nor a fuzz entry point in src/fuzz.rs. The exemption is: {why}",
                    sc.id,
                    eng
                );
            }
        }
    }
}

/// The engine-agnostic `na`-shared-seam scenarios in pool-split-matrix.yaml name their RED-proven
/// regression test in `what` (not a `test:` cell, so [`matrix_every_mapped_test_exists`] does not
/// reach them). Those tests ARE the regression barrier — the matrix only maps each split bug this
/// session found to the guard that keeps it fixed. Assert every named test still EXISTS, so a
/// renamed/deleted guard fails loud here instead of silently unmapping a bug's coverage.
#[test]
fn pool_split_shared_seam_scenarios_name_existing_regression_tests() {
    let fns = all_fn_names();
    // The regression tests the pool-split-matrix na-shared-seam `what` fields cite, one per split
    // bug found in the post-0.24.3 review + the #217/#218 bughunts. Keep in sync with the matrix.
    const NAMED: &[&str] = &[
        "reconstruct_covers_a_leading_adjacent_crash_instead_of_re_sampling",
        "reconstruct_fills_an_interior_adjacent_crash_without_overlapping_a_survivor",
        "reconstruct_keeps_checkpoint_for_an_exactly_recovered_single_crash",
        "reconstruct_runs_the_open_tail_fresh_after_a_trailing_adjacent_crash",
        "verify_over_a_split_prefix_catches_a_missing_non_last_unit_part",
        "split_unit_manifests_folds_every_same_family_split_sibling",
        "latest_full_over_a_split_family_selects_every_unit_not_just_the_last",
        "latest_full_over_a_split_family_takes_the_newest_run_per_unit",
        "select_runs_full_refuses_a_mixed_generation_split_prefix",
        "select_runs_full_refuses_an_equal_count_split_generation_with_an_interior_hole",
        "incremental_and_cdc_exports_are_not_splittable",
        "reconstruct_refuses_to_resurrect_a_now_unsplittable_export_on_resume",
        "mongo_source_is_not_range_split_capable",
    ];
    // Cross-check the list against the matrix text so a `what` that stops naming a test (or names a
    // new one) is caught — the list must not drift from the ledger it guards.
    let matrix_text = std::fs::read_to_string("docs/pool-split-matrix.yaml").unwrap();
    for name in NAMED {
        assert!(
            fns.contains(*name),
            "pool-split-matrix names regression test `{name}` in a `what` field, but no `fn {name}` \
             exists under src/ or tests/ — a renamed/deleted split-bug guard unmapped its coverage"
        );
        assert!(
            matrix_text.contains(name),
            "regression test `{name}` is in the guard list but no longer cited by any \
             pool-split-matrix `what` — drop it here or re-cite it there"
        );
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

/// The CamelCase variant idents of an enum, parsed from product source — the same
/// "derive from authoritative product code" trick [`all_fn_names`] uses. Handles
/// unit variants (`Postgres,`) AND struct/tuple variants (`Decimal {`, taking the
/// leading ident), skipping doc/line comments and attributes. Brace depth is
/// tracked so a struct variant's OWN field lines (depth 2) aren't mistaken for
/// variants, and its `{…}` doesn't end the scan early. Adding a variant grows the
/// derived set automatically — no hand-kept list.
fn enum_variants(rel: &str, enum_name: &str) -> HashSet<String> {
    let text = std::fs::read_to_string(repo_root().join(rel))
        .unwrap_or_else(|e| panic!("read {rel}: {e}"));
    let needle = format!("enum {enum_name} {{");
    let start = text
        .find(&needle)
        .unwrap_or_else(|| panic!("`{needle}` not found in {rel}"));
    let body = &text[start + needle.len()..];
    let mut out = HashSet::new();
    let mut depth = 1usize; // already inside the enum's `{`
    for line in body.lines() {
        let t = line.trim_start();
        let at_variant_depth = depth == 1;
        // Update depth AFTER classifying this line (an opening `{` affects the NEXT
        // lines, not this variant's own line). Stop at the enum's closing brace.
        let opens = t.matches('{').count();
        let closes = t.matches('}').count();
        if at_variant_depth && !t.is_empty() && !t.starts_with("//") && !t.starts_with('#') {
            let ident: String = t
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if ident.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
                out.insert(ident);
            }
        }
        depth = depth + opens - closes.min(depth);
        if depth == 0 {
            break; // enum's closing brace
        }
    }
    out
}

/// [`enum_variants`] lowercased — `SourceType::Postgres` → `"postgres"`,
/// `ExportTarget::DuckDb` → `"duckdb"`, matching the matrix column labels exactly.
/// The SourceType enum's variants, lowercased — for sibling matrix guards that
/// hand-list engine columns and must fail if that list drifts from the enum
/// (r5 bughunt: perf/release-gate hand-typed `const ENGINES` was ungoverned by
/// the generative column check, which only iterates the non-EXEMPT MATRICES).
pub(crate) fn source_engine_variants() -> HashSet<String> {
    enum_variants_lowercased("src/config/source.rs", "SourceType")
}

fn enum_variants_lowercased(rel: &str, enum_name: &str) -> HashSet<String> {
    enum_variants(rel, enum_name)
        .into_iter()
        .map(|v| v.to_ascii_lowercase())
        .collect()
}

/// GENERATIVE column-completeness — the coverage-audit meta-fix. The other three
/// guards are DESCRIPTIVE: they only check the columns an author already wrote
/// down, so a matrix that silently DROPS an engine/target — or never adds one for
/// a new enum variant — stays green (the un-enumerated-sibling hole the audit
/// found). This derives the required column set from the `SourceType` and
/// `ExportTarget` enums THEMSELVES: a matrix keyed on source engines must declare
/// EVERY `SourceType`, one keyed on warehouse targets must declare EVERY
/// `ExportTarget`. Adding a variant to either enum forces a column into every
/// relevant matrix — a `test`/`gap`/`na` cell, never a silent omission. Composes
/// with the gaps==0 ratchet: a forced column can be a justified `na`, but can't be
/// papered over as a growable gap.
#[test]
fn matrix_columns_cover_every_source_and_target_enum_variant() {
    let sources = enum_variants_lowercased("src/config/source.rs", "SourceType");
    let targets = enum_variants_lowercased("src/types/target.rs", "ExportTarget");
    // The THIRD axis. This guard derived two of the six enums a ledger can be
    // keyed on, so `docs/destination-matrix.yaml` — `engines: [local, gcs, s3,
    // azure]` — matched neither predicate and was generatively ungraded: it was
    // missing `stdout`, `DestinationType`'s fifth variant, and NO guard could
    // see the hole. That is worse than an unproven cell, because the check that
    // exists to find missing columns structurally could not (audit 2026-08-17).
    let dests = enum_variants_lowercased("src/config/destination.rs", "DestinationType");
    // Parse sanity: a drift here would silently UNDER-require, defeating the guard.
    assert!(
        sources.len() == 4 && sources.contains("postgres") && sources.contains("mongo"),
        "SourceType parse produced {sources:?} (expected the 4 source engines)"
    );
    assert!(
        targets.len() == 4 && targets.contains("duckdb") && targets.contains("clickhouse"),
        "ExportTarget parse produced {targets:?} (expected the 4 warehouse targets)"
    );
    assert!(
        dests.len() == 5 && dests.contains("local") && dests.contains("stdout"),
        "DestinationType parse produced {dests:?} (expected the 5 destination kinds)"
    );

    for (path, _) in MATRICES {
        let matrix = load_matrix(path);
        let declared: HashSet<&str> = matrix.engines.iter().map(String::as_str).collect();
        let keyed_on_sources = matrix.engines.iter().any(|e| sources.contains(e.as_str()));
        let keyed_on_targets = matrix.engines.iter().any(|e| targets.contains(e.as_str()));
        // Ordered after the other two on purpose: `local`/`s3`/`gcs` are unique
        // to DestinationType, but a ledger keyed on SOURCES must not be dragged
        // in by a coincidental name, so this only claims a matrix no other axis
        // claimed.
        let keyed_on_dests = !keyed_on_sources
            && !keyed_on_targets
            && matrix.engines.iter().any(|e| dests.contains(e.as_str()));
        if keyed_on_sources {
            for s in &sources {
                assert!(
                    declared.contains(s.as_str()),
                    "{path} is keyed on source engines but is MISSING the '{s}' column. Every \
                     SourceType must be a column (a test/gap/na cell per scenario) — a \
                     silently-absent engine is the un-enumerated-sibling hole the audit found. \
                     Add it, or n/a it with a reason."
                );
            }
        }
        if keyed_on_dests {
            for d in &dests {
                assert!(
                    declared.contains(d.as_str()),
                    "{path} is keyed on destination kinds but is MISSING the '{d}' column. \
                     Every DestinationType must be a column (test/gap/na per scenario) — \
                     `stdout` was absent from this ledger and no guard could see it."
                );
            }
        }
        if keyed_on_targets {
            for t in &targets {
                assert!(
                    declared.contains(t.as_str()),
                    "{path} is keyed on warehouse targets but is MISSING the '{t}' column. Every \
                     ExportTarget must be a column — add it (test/gap/na per scenario)."
                );
            }
        }
    }
}

/// The RivetType FAMILY → cdc-type scenario id each variant must map to: the
/// authoritative type enumeration paired with the required matrix row. Parametric
/// variants (`Decimal{}`, `Time{}`, `Timestamp{}`, `List{}`) map by family;
/// `Unsupported` is not a real column type (n/a by nature) and is excluded below.
const RIVET_TYPE_ROWS: &[(&str, &str)] = &[
    ("Bool", "type_boolean"),
    ("Int16", "type_integer_families"),
    ("Int32", "type_integer_families"),
    ("Int64", "type_integer_families"),
    ("UInt64", "type_integer_families"),
    ("Float32", "type_float"),
    ("Float64", "type_float"),
    ("Decimal", "type_decimal"),
    ("Date", "type_date_time"),
    ("Time", "type_date_time"),
    ("Timestamp", "type_timestamp_tz"),
    ("String", "type_text"),
    ("Text", "type_text"),
    ("Binary", "type_binary"),
    ("Json", "type_json"),
    ("Uuid", "type_uuid"),
    ("Enum", "type_enum"),
    ("Interval", "type_interval"),
    ("List", "type_list"),
];

/// GENERATIVE row-completeness (the audit's row-axis sibling of the column-axis
/// guard #5). Guard #5 forces every ENGINE column; this forces every TYPE row. The
/// required rows are derived from the `RivetType` enum itself: every variant
/// (except `Unsupported`) must map to a cdc-type scenario in `RIVET_TYPE_ROWS`, and
/// every mapped scenario must EXIST in the CDC type-fidelity matrix. So a new
/// `RivetType` cannot ship without a CDC-fidelity row — the per-type-CDC axis where
/// findings #2/#3/#4 lived can no longer grow a silent hole.
#[test]
fn matrix_cdc_type_rows_cover_every_rivet_type() {
    let variants = enum_variants("src/types/rivet_type.rs", "RivetType");
    assert!(
        variants.contains("Decimal") && variants.contains("List") && variants.len() >= 19,
        "RivetType parse produced {variants:?} (expected the full type universe)"
    );
    let mapped: HashSet<&str> = RIVET_TYPE_ROWS.iter().map(|(v, _)| *v).collect();
    for v in &variants {
        if v == "Unsupported" {
            continue; // not a real column type — n/a by nature
        }
        assert!(
            mapped.contains(v.as_str()),
            "RivetType::{v} has no row mapping in RIVET_TYPE_ROWS. A NEW type must not ship \
             without a CDC-fidelity row — map it to a `type_*` scenario (add the row to \
             docs/cdc-type-fidelity-matrix.yaml if it is a new family)."
        );
    }
    let matrix = load_matrix("docs/cdc-type-fidelity-matrix.yaml");
    let ids: HashSet<&str> = matrix.scenarios.iter().map(|s| s.id.as_str()).collect();
    for (variant, scenario) in RIVET_TYPE_ROWS {
        assert!(
            ids.contains(scenario),
            "RivetType::{variant} maps to cdc-type row '{scenario}', MISSING from \
             docs/cdc-type-fidelity-matrix.yaml — add the scenario (a test/gap/na cell per engine)."
        );
    }
}

/// Guard #7 — GENERATIVE oracle-strength ratchet. Guards #1-#6 ensure a cell
/// EXISTS and names a real test; this one grades HOW STRONG that test's oracle is.
/// A `differential` cell (CDC==batch) is a self-oracle over the SHARED decode — it
/// passes a bug both siblings share (the class the value-checksum Form A also
/// misses, and that the audit found hiding #5/#6/#8). The ratchet counts the weak
/// (differential / self / un-annotated) cells per tracked matrix and forbids the
/// count from GROWING; upgrading a cell to an INDEPENDENT oracle (a DuckDB /
/// source-vs-dest re-read, outside rivet's decode family) lowers the ceiling. So
/// the shared-decode self-oracle debt is visible and can only shrink.
#[test]
fn matrix_oracle_strength_ratchet() {
    for (path, ceiling) in ORACLE_TRACKED {
        let matrix = load_matrix(path);
        let mut weak = 0usize;
        for sc in &matrix.scenarios {
            for (eng, cell) in sc.resolved_cells(&matrix.engines, path) {
                // Typo-guard any declared strength.
                if let Some(o) = cell.oracle.as_deref() {
                    assert!(
                        ORACLE_STRENGTHS.contains(&o),
                        "{path} scenario '{}' column '{}' has oracle: '{o}' — must be one of {ORACLE_STRENGTHS:?}",
                        sc.id,
                        eng
                    );
                    // `oracle:` only means something on a `test:` cell.
                    assert!(
                        cell.test.is_some(),
                        "{path} scenario '{}' column '{}' declares oracle: '{o}' but is not a `test` cell",
                        sc.id,
                        eng
                    );
                }
                weak += cell.is_weak_oracle() as usize;
            }
        }
        assert_eq!(
            weak, *ceiling,
            "{path} has {weak} WEAK-oracle cells (differential / self / un-annotated); the \
             ratchet expects exactly {ceiling}. If {weak} > {ceiling}: you added a weak cell — \
             give it an INDEPENDENT oracle (DuckDB/source re-read) or fail_loud, not a \
             batch-differential. If {weak} < {ceiling}: you UPGRADED one — lower the ceiling in \
             ORACLE_TRACKED to {weak} to lock the win."
        );
    }
}

/// Every `docs/*matrix*.yaml` on disk is either ratcheted here or exempted by
/// name — and every exemption names a guard file that EXISTS.
///
/// The dimension is the GLOB, not a list: a hand-written list of ledgers cannot
/// notice a ledger nobody added to it, which is the defect this repo has now
/// corrected in five places. Before this, `MATRICES` held 20 of 25 and the other
/// five were invisible; one of them was not even valid YAML.
#[test]
fn every_coverage_ledger_is_ratcheted_here_or_exempted_by_name() {
    let docs = repo_root().join("docs");
    let mut on_disk: Vec<String> = std::fs::read_dir(&docs)
        .expect("read docs/")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains("matrix") && n.ends_with(".yaml"))
        .map(|n| format!("docs/{n}"))
        .collect();
    on_disk.sort();
    assert!(
        on_disk.len() >= 20,
        "found only {} ledger(s) under docs/ — the glob lost its subject, and an \
         empty dimension is exactly the failure this check replaces: {on_disk:?}",
        on_disk.len()
    );

    let ratcheted: Vec<&str> = MATRICES.iter().map(|(f, _)| *f).collect();
    let exempt: Vec<&str> = EXEMPT.iter().map(|(f, _)| *f).collect();
    let ungoverned: Vec<&String> = on_disk
        .iter()
        .filter(|f| !ratcheted.contains(&f.as_str()) && !exempt.contains(&f.as_str()))
        .collect();
    assert!(
        ungoverned.is_empty(),
        "these coverage ledgers are governed by nothing — add them to MATRICES \
         (with a ratchet) or to EXEMPT (naming the guard that covers them): {ungoverned:?}"
    );

    // An exemption is a CLAIM about another guard. Check the claim.
    for (ledger, guard) in EXEMPT {
        assert!(
            repo_root().join(guard).exists(),
            "{ledger} is exempted on the grounds that {guard} covers it, and that \
             file does not exist"
        );
        assert!(
            on_disk.contains(&ledger.to_string()),
            "{ledger} is exempted but no longer exists — delete the exemption"
        );
    }
    for (ledger, _) in MATRICES {
        assert!(
            on_disk.contains(&ledger.to_string()),
            "{ledger} is ratcheted but no longer exists — delete the entry"
        );
    }
}

/// Every YAML under `docs/` actually parses as YAML.
///
/// A NEW axis, not a duplicate of the cell checks above: those read a ledger's
/// CONTENT and only run on the files they know about. This asks whether a file
/// is the format it claims to be, which nothing asked — `scenario-artifact-
/// matrix.yaml` had three keys at column 0 among siblings indented 6 and was
/// unparseable for as long as anyone can tell, because both of its readers scan
/// lines and `strip()` them instead of parsing. It worked by luck: the day a
/// reader is switched to a real parser, the harness breaks rather than the file.
#[test]
fn every_docs_yaml_parses_as_yaml() {
    let docs = repo_root().join("docs");
    let mut checked = 0;
    for entry in std::fs::read_dir(&docs).expect("read docs/") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {name}: {e}"));
        serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&text)
            .unwrap_or_else(|e| panic!("docs/{name} is not valid YAML: {e}"));
        checked += 1;
    }
    assert!(
        checked >= 20,
        "parsed only {checked} YAML file(s) under docs/ — the walk found nothing to grade"
    );
}
