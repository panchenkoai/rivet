//! Drift-guard for `docs/release-gate-matrix.yaml` — the release-gate coverage ledger
//! (every gate CHECK × engine × VERSION + the backend-infra dimensions).
//!
//! Keeps the ledger honest against the gate's REAL config so it can't rot:
//!   1. `grid.<engine>.gated` == the versions dev/release-oracle/matrix.yaml brings up.
//!   2. Every `sc_<id>` / `verify_<id>` in the gate's modules has a ledger row, and every
//!      ledger scenario / status:test preflight has its function — a new gate check
//!      with no cell (or a deleted check with a stale cell) fails here.
//!   3. Every scenario carries a cell for ALL four engines (column-completeness) and
//!      each cell is EXACTLY test / {na} / {gap}.
//!   4. Total admitted gaps <= the shrink-only ratchet.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use serde_yaml_ng::Value;

const GATE_MATRIX: &str = "docs/release-gate-matrix.yaml";
const ORACLE_MATRIX: &str = "dev/release-oracle/matrix.yaml";
// A gate CHECK function (`sc_*` / `verify_*`) may live in any of the gate's
// per-stage modules, so the ledger cross-check scans them all — else a stage
// moved into its own module (verify_cdc_e2e → cdc) would silently escape the
// "every gate function has a ledger row" guard.
//
// These are the PYTHON modules: the gate was ported from `dev/release-oracle/
// lib/*.sh`, and the function names were preserved one-for-one, so this guard
// keeps its meaning while pointing at the implementation that actually runs.
//
// DERIVED from the directory, not typed in — and the comment above is exactly
// why. It already warned that "a stage moved into its own module would silently
// escape", and then a hand-written four-file list did precisely that: by the time
// this was noticed, `gifs.verify_gif_currency`, `bigquery.verify_gc_survival` and
// `rowhash.verify_row_hash` were all running in the gate while invisible to the
// guard meant to enumerate it. A list an author must remember to extend grades
// only the modules its author already knew about.
fn gate_py() -> Vec<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dev/release_oracle");
    let mut out: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "py"))
        .filter(|p| p.file_name().is_some_and(|n| n != "__init__.py"))
        .collect();
    out.sort();
    assert!(
        out.len() >= 4,
        "dev/release_oracle holds {} python modules — the gate cannot have shrunk that \
         far; this guard is reading the wrong directory",
        out.len()
    );
    out
}
const ENGINES: [&str; 4] = ["postgres", "mysql", "mssql", "mongo"];

// Total admitted gaps = scenario `gap` cells + preflight/infra `status: gap` +
// grid version gaps. Shrink-only: LOWER when a gap is filled; never raise.
// History: 14 -> 13 (pooler_safety) -> 12 (state_upgrade) -> 11 (state_concurrency)
// -> 10 (scale_memory wired as the verify_scale_memory flat-RSS preflight).
// Now: infra gap rows(0 — network_faults/cdc_standby flipped to test,
// tls_required/auth to partial with the residual named, 2026-08-29) +
// grid version gaps(0+1+1+0=2), MEASURED 2026-08-30 rather than argued.
// Three postgres images needed nothing; the mysql and mssql notes described
// blockers that had already been removed (a CTE-free seed had shipped; the real
// mssql blocker was a hardcoded tools18 path in the HARNESS, now probed). The
// sixth, `mariadb-11`, was not a gap at all — MariaDB is not a SourceType, so
// the row recorded a debt that could only be "paid" by adding an engine. A
// coverage ledger that carries an obligation the product never took on reports
// permanent debt and teaches its readers to discount it. (The line above summed to the OLD value of
// 10 until a critic recomputed it — the constant was right, its arithmetic was
// leftover: the message-truth class, in the comment over a ratchet.)
const GAP_RATCHET: usize = 2;

fn load(path: &str) -> Value {
    let s = super::nonvacuity::subject_text(path);
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

/// A top-level sequence of the ledger, floor-checked per section.
///
/// The `unwrap_or_default()` is the vacuity: a renamed or re-nested key hands
/// back the EMPTY vector, and then `every_gate_function_has_a_ledger_row_and_
/// vice_versa` finds no ids to demand functions for, the column-completeness
/// loop grades no scenarios, and the gap count is zero — under the ratchet. The
/// release-gate ledger reports a clean sweep having read nothing.
///
/// Floors are the counts the ledger holds today (9 scenarios / 22 preflights /
/// 7 infra) minus room for ordinary edits: they catch a lost SECTION, not a
/// deleted row, and a section that legitimately shrinks past its floor is a
/// deliberate diff on this line.
fn seq<'a>(v: &'a Value, key: &str) -> Vec<&'a Value> {
    let out: Vec<&Value> = v
        .get(key)
        .and_then(|s| s.as_sequence())
        .map(|s| s.iter().collect())
        .unwrap_or_default();
    let floor = match key {
        "scenarios" => 5,
        "preflights" => 12,
        "infra" => 4,
        _ => 0,
    };
    super::nonvacuity::require_enumerated(
        out.len(),
        floor,
        &format!("`{key}:` rows in {GATE_MATRIX}"),
        "Re-point this reader at the section's new name/nesting — the guards below cannot \
         tell an empty ledger from a fully covered one.",
    );
    out
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

    // r5 bughunt: this hand-typed list is ungoverned by the generative
    // column-completeness guard (it iterates only the non-EXEMPT MATRICES),
    // so a new SourceType variant would leave this matrix un-forced. Assert
    // the list IS the enum — add a variant to SourceType and this goes RED
    // until the matrix (and this const) gain the column.
    {
        let derived = super::chunking_matrix_guard::source_engine_variants();
        let listed: std::collections::HashSet<String> =
            ENGINES.iter().map(|e| e.to_string()).collect();
        assert_eq!(
            listed, derived,
            "ENGINES hand-list drifted from the SourceType enum: listed {listed:?} vs enum {derived:?}"
        );
    }
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
    let src: String = gate_py()
        .iter()
        .map(|p| fs::read_to_string(p).unwrap_or_else(|e| panic!("read {}: {e}", p.display())))
        .collect::<Vec<_>>()
        .join("\n");
    // Function DEFINITIONS live at column 0 (`def sc_x(` / `def verify_x(`); calls are indented.
    let mut sc_fns = BTreeSet::new();
    let mut verify_fns = BTreeSet::new();
    for line in src.lines() {
        if let Some(rest) = line.strip_prefix("def sc_")
            && rest.contains('(')
        {
            sc_fns.insert(rest.split('(').next().unwrap().trim().to_string());
        } else if let Some(rest) = line.strip_prefix("def verify_")
            && rest.contains('(')
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
            "the gate defines sc_{f}() but the gate matrix has no `scenarios` row `{f}`"
        );
    }
    for id in &scenario_ids {
        assert!(
            sc_fns.contains(id),
            "gate matrix scenario `{id}` has no sc_{id}() in the gate modules (renamed / deleted?)"
        );
    }
    // A `verify_X` is satisfied by a row under EITHER heading. The same check can
    // exist twice — `gc_survival` runs as a local scenario (`sc_gc_survival`) and
    // again in the BigQuery stage (`verify_gc_survival`) — and which heading its
    // ledger row sits under is a fact about the stage, not about the name.
    // `infra` rows became drivable on 2026-08-29 (network_faults / tls_required /
    // auth / cdc_standby each got a verify_ leg), so the row that satisfies a
    // verify_ function may live under any of the three headings.
    let infra_ids: std::collections::BTreeSet<String> = seq(&gate, "infra")
        .iter()
        .map(|e| scalar(e.get("id").unwrap()))
        .collect();
    for f in &verify_fns {
        assert!(
            preflight_ids.contains(f) || scenario_ids.contains(f) || infra_ids.contains(f),
            "the gate defines verify_{f}() but the gate matrix has no `preflights`, \
             `scenarios` or `infra` row `{f}`"
        );
    }
    // An infra row promoted to status:test must have its verify_ leg, same as
    // the preflights rule below — a `test` claim with no driver is decoration.
    for e in seq(&gate, "infra") {
        let id = scalar(e.get("id").unwrap());
        // state_upgrade / state_concurrency are status:test WITHOUT their own
        // verify_: their notes say (and the call-site guard verifies) they are
        // driven by the state_migrations preflight's cargo filters.
        // `partial` counts too: a critic deleted verify_tls_required outright
        // (fn + __all__ + call site) and every guard stayed green, because
        // `partial` was the one status that required nothing. Two of the four
        // infra gaps this branch banked were banked into it, so the structural
        // guard was off exactly where the claim was newest.
        if matches!(
            e.get("status").map(scalar).as_deref(),
            Some("test") | Some("partial")
        ) && id != "state_upgrade"
            && id != "state_concurrency"
        {
            // A row may be driven by a differently-named function (the
            // warehouse_load half runs through `run_bigquery_golden`), so the
            // note may carry the driver instead — and the sibling guard below
            // proves any verify_ name in a note RESOLVES, so this cannot be
            // satisfied by inventing one.
            let note = e.get("note").map(scalar).unwrap_or_default();
            let named_in_note = note.contains("verify_") || note.contains("run_bigquery_golden");
            assert!(
                verify_fns.contains(&id) || named_in_note,
                "infra row `{id}` is status:{} but nothing drives it: no \
                 verify_{id}(), and its note names no driver either",
                e.get("status").map(scalar).unwrap_or_default()
            );
        }
    }
    // A preflight marked status:test MUST have its verify_ function; a `gap` preflight
    // (not yet wired) legitimately has none.
    for p in &preflights {
        let id = scalar(p.get("id").unwrap());
        if p.get("status").map(scalar).as_deref() == Some("test") {
            assert!(
                verify_fns.contains(&id),
                "preflight `{id}` is status:test but the gate modules have no verify_{id}()"
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

    // TWO-SIDED (matrix audit, 2026-08-29): `<=` let a closed gap keep the old
    // ceiling — the win was never banked, so a later regression could spend it
    // silently. Down is as loud as up.
    assert_eq!(
        gaps, GAP_RATCHET,
        "release-gate-matrix has {gaps} gaps, the ratchet says {GAP_RATCHET}. Up: you cannot \
         ADD a gap — wire the check (flip gap -> test / gate the version). Down: a gap you \
         closed must be banked — LOWER the ratchet in this commit."
    );
}

/// Every `verify_*` token a matrix NOTE mentions must resolve to a real gate
/// function. Prose is unchecked by construction — the guard binds a row's `id`
/// to `verify_<id>`, so a note may name anything — and two notes named
/// `verify_tls_policy` / `verify_mongo_auth`, which exist nowhere (the real
/// functions are `verify_tls_required` / `verify_auth`). A reader who follows
/// the note lands on nothing: the message-truth class this repo grades
/// everywhere else.
#[test]
fn every_verify_name_in_a_matrix_note_resolves() {
    let mut defined: BTreeSet<String> = BTreeSet::new();
    for path in gate_py() {
        let src = std::fs::read_to_string(&path).unwrap_or_default();
        for line in src.lines() {
            if let Some(rest) = line.trim().strip_prefix("def verify_") {
                defined.insert(format!("verify_{}", rest.split('(').next().unwrap().trim()));
            }
        }
    }
    assert!(
        defined.len() >= 10,
        "collected only {} verify_ fns — the sweep broke and this would pass vacuously",
        defined.len()
    );
    let gate = load(GATE_MATRIX);
    let mut notes = String::new();
    for section in ["preflights", "infra", "scenarios"] {
        for e in seq(&gate, section) {
            if let Some(n) = e.get("note") {
                notes.push_str(&scalar(n));
                notes.push('\n');
            }
        }
    }
    let mut missing: Vec<String> = Vec::new();
    let mut rest = notes.as_str();
    while let Some(at) = rest.find("verify_") {
        rest = &rest[at + "verify_".len()..];
        let ident: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let full = format!("verify_{ident}");
        if !ident.is_empty() && !defined.contains(&full) {
            missing.push(full);
        }
    }
    missing.sort();
    missing.dedup();
    assert!(
        missing.is_empty(),
        "matrix notes name gate functions that do not exist: {missing:?} — the \
         note is what a reader follows, so an unresolvable name is a dead end"
    );
}

/// A `test` cell must name a check the gate actually RUNS — not one it merely defines.
///
/// The sibling guard above asks whether every `def verify_*` has a matrix row.
/// That is the weaker half of the question, and the gap between the two halves
/// was live: `blessed_path.verify_blessed_path` was registered `test` for all
/// four engines and had **no caller anywhere in the tree**. It was dead code
/// wearing four green cells, and every signal a reader has — the matrix, the
/// definition, the guard — agreed it was covered.
///
/// So this asks the other half: for every gate function, is there a CALL SITE?
/// A call is an occurrence of `name(` that is not the `def`. Cross-module calls
/// (`blessed_flow.verify_blessed_flow(...)`) and same-module ones both count;
/// what does not count is the definition alone.
#[test]
fn every_gate_function_has_a_call_site() {
    let sources: Vec<(String, String)> = gate_py()
        .iter()
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().to_string(),
                fs::read_to_string(p).unwrap_or_else(|e| panic!("read {}: {e}", p.display())),
            )
        })
        .collect();
    let all: String = sources
        .iter()
        .map(|(_, s)| s.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    let mut defined: Vec<String> = Vec::new();
    for line in all.lines() {
        for prefix in ["def sc_", "def verify_"] {
            if let Some(rest) = line.strip_prefix(prefix)
                && rest.contains('(')
            {
                let bare = rest.split('(').next().unwrap().trim();
                defined.push(format!("{}{bare}", &prefix[4..]));
            }
        }
    }
    assert!(
        defined.len() > 10,
        "found only {} gate functions — this guard is reading the wrong files",
        defined.len()
    );

    let mut orphans: Vec<String> = Vec::new();
    for f in &defined {
        let needle = format!("{f}(");
        let called = all
            .lines()
            .filter(|l| l.contains(&needle))
            .any(|l| !l.trim_start().starts_with("def "));
        if !called {
            orphans.push(f.clone());
        }
    }
    assert!(
        orphans.is_empty(),
        "these gate checks are DEFINED but never called — the matrix may register them \
         as `test` while nothing runs them (this is exactly how verify_blessed_path sat \
         dead behind four green cells): {}",
        orphans.join(", ")
    );
}
