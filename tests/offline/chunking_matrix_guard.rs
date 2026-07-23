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
    // CDC value-decode is the youngest, most differential-heavy surface (the
    // *_cdc_full_type_matrix_matches_batch self-oracle). PG + MySQL + tz/enum/
    // money/mongo cells upgraded to independent so far; MSSQL + the PG float/date/
    // binary/uuid cells are the remaining debt.
    ("docs/cdc-type-fidelity-matrix.yaml", 11),
    // Batch type fidelity is already oracle-STRONG (24 independent cells: golden
    // hard-coded values + DuckDB/pyarrow foreign readers). The 6 weak are cells
    // whose CITED test is schema-coverage only (no independent VALUE re-read) —
    // upgrade by pointing them at the duckdb_validates_* value test.
    ("docs/type-fidelity-matrix.yaml", 6),
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
    // Parse sanity: a drift here would silently UNDER-require, defeating the guard.
    assert!(
        sources.len() == 4 && sources.contains("postgres") && sources.contains("mongo"),
        "SourceType parse produced {sources:?} (expected the 4 source engines)"
    );
    assert!(
        targets.len() == 4 && targets.contains("duckdb") && targets.contains("clickhouse"),
        "ExportTarget parse produced {targets:?} (expected the 4 warehouse targets)"
    );

    for (path, _) in MATRICES {
        let matrix = load_matrix(path);
        let declared: HashSet<&str> = matrix.engines.iter().map(String::as_str).collect();
        let keyed_on_sources = matrix.engines.iter().any(|e| sources.contains(e.as_str()));
        let keyed_on_targets = matrix.engines.iter().any(|e| targets.contains(e.as_str()));
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
