//! AUDIT-RED live tests for cluster `preflight-table-form` (findings #3, #15).
//!
//! Bug: for an export that uses the `table:` shortcut (no `query:`) — the
//! shape `rivet init` emits — preflight reads `export.query` directly
//! (`src/preflight/postgres.rs:71`: `export.query.as_deref().unwrap_or("SELECT 1")`)
//! INSTEAD of the resolved single-table query (`ExportConfig::resolve_query`
//! renders `SELECT * FROM <table>`). So preflight EXPLAINs the placeholder
//! `SELECT 1`, which estimates 1 row regardless of how big the real table is.
//!
//! Live observation (docker postgres, seeded `orders` = 2500 rows, PK id):
//!   form A `table: orders`         → "Row estimate: ~1"   (WRONG)
//!   form B `query: SELECT * FROM orders` → "Row estimate: ~2K" (correct)
//! and in chunked mode form A is also missing the `Cursor range:` line that
//! form B prints (`1 .. 2500`) — same root cause: the min/max probe wraps the
//! `SELECT 1` placeholder, not the table.
//!
//! These tests assert the CORRECT behavior (form A must match form B / the
//! real 2500 rows) and therefore FAIL against current code.

use crate::common::*;

// ─── helpers ──────────────────────────────────────────────────────────────────

/// The pre-seeded benchmark table the live stack ships with: `orders`,
/// 2500 rows, PK `id`, plus a secondary index on `updated_at`. Both config
/// forms in this file point at this same physical table so any divergence in
/// the diagnostic is attributable purely to `table:` vs `query:`.
const SEEDED_TABLE: &str = "orders";
const SEEDED_ROWS: i64 = 2500;

/// Run `rivet check --config <cfg> --export <name>` against the live Postgres
/// and return captured stdout. `check` exits 0 on a successful diagnostic, so
/// we assert success and surface stderr on failure.
fn run_check(rig: &Rig, export_name: &str) -> String {
    let out = rig.cli_env(
        &["check", "--export", export_name],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

    assert!(
        out.status.success(),
        "rivet check must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Parse the integer row count out of a `print_diagnostic` "Row estimate:"
/// line. The renderer (`src/preflight/mod.rs::print_diagnostic`) prints one of:
///   `  Row estimate: ~1`       (raw count < 1000)
///   `  Row estimate: ~2K`      (count / 1_000, count >= 1000)
///   `  Row estimate: ~5M`      (count / 1_000_000)
/// Returns the count in *rows* so `~1` → 1 and `~2K` → 2000.
fn parse_row_estimate(stdout: &str) -> i64 {
    let line = stdout
        .lines()
        .find(|l| l.contains("Row estimate:"))
        .unwrap_or_else(|| panic!("no 'Row estimate:' line in check output:\n{stdout}"));
    let raw = line
        .split('~')
        .nth(1)
        .unwrap_or_else(|| panic!("malformed Row estimate line: {line:?}"))
        // #149 appends a source label — `~5M  (catalog estimate)` /
        // `(measured YYYY-MM-DD)`; take only the leading count token
        // (split_whitespace also trims leading space).
        .split_whitespace()
        .next()
        .unwrap_or("");
    if let Some(k) = raw.strip_suffix('K') {
        k.trim().parse::<i64>().expect("K-suffixed number") * 1_000
    } else if let Some(m) = raw.strip_suffix('M') {
        m.trim().parse::<i64>().expect("M-suffixed number") * 1_000_000
    } else {
        raw.parse::<i64>().expect("bare row-estimate number")
    }
}

/// The two config FORMS this file compares, as one rig constructor: form A is
/// the `table:` shortcut, form B the `query:` spelling of the same relation.
/// `mode_lines` is the strategy block (first line is the `mode:`, the rest are
/// export lines) — the same shapes the old string builders interpolated.
fn form_rig(table_form: bool, name: &str, mode_lines: &str) -> Rig {
    let mut rig = if table_form {
        Rig::pg_batch(SEEDED_TABLE).export_named(name)
    } else {
        Rig::pg_batch(name).query(&format!("SELECT * FROM {SEEDED_TABLE}"))
    };
    rig = rig.source_url_env("DATABASE_URL");
    for line in mode_lines.lines().map(str::trim).filter(|l| !l.is_empty()) {
        rig = match line.strip_prefix("mode: ") {
            Some(m) => rig.mode(m),
            None => rig.export_line(line),
        };
    }
    rig
}

// ─── finding #3: row estimate for the `table:` shortcut ───────────────────────

// AUDIT-RED preflight-table-form: `table:` shortcut EXPLAINs "SELECT 1" not the table, so row estimate is ~1. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_table_shortcut_row_estimate_matches_real_table() {
    require_alive(LiveService::Postgres);

    let name_a = unique_name("orders_table_form");
    let rig = form_rig(true, &name_a, "mode: full");

    let stdout = run_check(&rig, &name_a);
    let est = parse_row_estimate(&stdout);

    // The `table: orders` shortcut points at a 2500-row table. Preflight must
    // estimate in the thousands, not ~1. Current code reports ~1 because it
    // EXPLAINs the "SELECT 1" placeholder (src/preflight/postgres.rs:71).
    assert!(
        est >= 1_000,
        "table:-shortcut export must report a row estimate in the thousands \
         (real table has {SEEDED_ROWS} rows); got {est} rows.\nfull check output:\n{stdout}"
    );
}

// AUDIT-RED preflight-table-form: `table:` and `query:` on the SAME table must give the SAME row estimate. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_table_and_query_forms_agree_on_row_estimate() {
    require_alive(LiveService::Postgres);

    let name_a = unique_name("orders_table_form");
    let name_b = unique_name("orders_query_form");
    let rig_a = form_rig(true, &name_a, "mode: full");
    let rig_b = form_rig(false, &name_b, "mode: full");

    let est_a = parse_row_estimate(&run_check(&rig_a, &name_a));
    let est_b = parse_row_estimate(&run_check(&rig_b, &name_b));

    // Same physical table, so the two diagnostics must agree. Today form A
    // reports ~1 (the "SELECT 1" placeholder) while form B reports ~2K.
    assert_eq!(
        est_a, est_b,
        "`table:` form and `query:` form target the same {SEEDED_TABLE} table \
         and must report the same row estimate; table-form={est_a}, query-form={est_b}"
    );
}

// ─── finding #15: chunked `table:` shortcut loses the cursor range ────────────
//
// Same root cause as #3: the min/max range probe wraps the `SELECT 1`
// placeholder instead of the real relation, so the `Cursor range:` line that
// the `query:` form prints (`1 .. 2500`) is silently dropped for the
// `table:` form. The fix that gives form A the right base query also recovers
// this line.

// AUDIT-RED preflight-table-form: chunked `table:` shortcut drops the cursor range line that the query form prints. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_chunked_table_shortcut_reports_cursor_range() {
    require_alive(LiveService::Postgres);

    let name_a = unique_name("orders_table_chunked");
    let rig = form_rig(
        true,
        &name_a,
        "mode: chunked\nchunk_column: id\nchunk_size: 500",
    );

    let stdout = run_check(&rig, &name_a);

    // `orders.id` spans 1..2500. The `query:` form prints "Cursor range: 1 .. 2500";
    // the `table:` form must do the same. Current code drops the line because the
    // range probe runs `min/max(...) FROM (SELECT 1) AS _rivet` and finds nothing.
    assert!(
        stdout.contains("Cursor range:"),
        "chunked `table:`-shortcut export must report a 'Cursor range:' line \
         (orders.id spans 1..{SEEDED_ROWS}); it was missing.\nfull check output:\n{stdout}"
    );
}

// ─── check must not bless what plan refuses ──────────────────────────────────

/// Run `rivet plan` on a config and report whether it was accepted, with the
/// reason when it was not. `plan` is the runner's own gate: `apply` executes the
/// artifact it produces, so a config `plan` rejects is a config that cannot run.
fn plan_verdict(rig: &Rig) -> Result<(), String> {
    let out = rig.cli_env(&["plan"], &[("DATABASE_URL", POSTGRES_URL)]);
    if out.status.success() {
        return Ok(());
    }
    let err = String::from_utf8_lossy(&out.stderr);
    let msg = err
        .lines()
        .find(|l| l.starts_with("Error:"))
        .unwrap_or_else(|| err.lines().last().unwrap_or(""));
    Err(msg.trim().to_string())
}

/// Like [`run_check`] but reports the exit status instead of asserting it, so
/// the two commands can be COMPARED rather than one of them assumed green.
fn check_accepts(rig: &Rig, export_name: &str) -> bool {
    rig.cli_env(
        &["check", "--export", export_name],
        &[("DATABASE_URL", POSTGRES_URL)],
    )
    .status
    .success()
}

/// Every config `check` blesses must be one `plan` will accept.
///
/// `check` is a PREFLIGHT: an operator runs it, reads `Verdict: ACCEPTABLE`,
/// and schedules the export. If the runner then refuses the same file, the
/// preflight has spent the operator's trust on a config that cannot run — the
/// diagnostic-bypass class, in its sharpest form. It is not that `check`
/// mis-analyses something; it is that `check` and the runner disagree about
/// whether the config is legal at all.
///
/// Found live 2026-08-05 while diagnosing a field run. `chunk_size_memory_mb`
/// on a `query:`-form export (no `table:`) is refused by `plan::build` —
/// "`chunk_size_memory_mb:` only applies with the `table:` shortcut" — because
/// there is no relation to probe for the row width the budget divides by.
/// `check` printed `Strategy: chunked(id, size=100000)` and `Verdict:
/// ACCEPTABLE` for the same file: it neither applied the budget nor noticed it
/// could not.
///
/// The size in that Strategy line is the second tell — 100000 is the config
/// DEFAULT, printed unchanged at every budget, so even where the budget IS
/// legal the line does not describe what will run.
///
/// Deliberately a MATRIX over shapes rather than one case: the invariant is
/// "check and plan agree", and a single case pins one bug instead of the
/// property. Shapes that BOTH reject are fine — the guard is one-directional.
/// The matrix earned that immediately: it found a SECOND disagreement nobody
/// was looking for — `query:` + `chunk_by_key`, where keyset needs the relation
/// to verify the unique index and `check` blesses it anyway.
// AUDIT-RED preflight/planner-agreement: `check` accepts `query:` + `chunk_size_memory_mb` and `query:` + `chunk_by_key`, both of which `plan` refuses. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn check_must_not_accept_a_config_the_planner_refuses() {
    require_alive(LiveService::Postgres);

    // (label, config body) — every shape an operator plausibly writes around the
    // width-aware knob, in both source forms.
    let shapes: Vec<(&str, Rig)> = vec![
        (
            "table: + chunk_column + chunk_size_memory_mb",
            form_rig(
                true,
                "t",
                "mode: chunked\n    chunk_column: id\n    chunk_size_memory_mb: 64",
            ),
        ),
        (
            "query: + chunk_column + chunk_size_memory_mb",
            form_rig(
                false,
                "t",
                "mode: chunked\n    chunk_column: id\n    chunk_size_memory_mb: 64",
            ),
        ),
        (
            "table: + chunk_by_key + chunk_size_memory_mb",
            form_rig(
                true,
                "t",
                "mode: chunked\n    chunk_by_key: id\n    chunk_size_memory_mb: 64",
            ),
        ),
        (
            "query: + chunk_by_key (keyset needs the relation)",
            form_rig(false, "t", "mode: chunked\n    chunk_by_key: id"),
        ),
        (
            "table: + plain chunk_size (the shape init emits)",
            form_rig(
                true,
                "t",
                "mode: chunked\n    chunk_column: id\n    chunk_size: 500",
            ),
        ),
    ];

    let mut disagreements = Vec::new();
    for (label, rig) in &shapes {
        let checked = check_accepts(rig, "t");
        let planned = plan_verdict(rig);
        if let (true, Err(why)) = (checked, &planned) {
            disagreements.push(format!("  {label}\n      plan: {why}"));
        }
    }

    assert!(
        disagreements.is_empty(),
        "`rivet check` accepted {} config(s) that `rivet plan` refuses — a preflight that \
         blesses an unrunnable config is worse than no preflight, because the operator \
         stops looking:\n{}",
        disagreements.len(),
        disagreements.join("\n")
    );
}
