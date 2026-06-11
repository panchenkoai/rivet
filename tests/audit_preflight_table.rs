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

mod common;

use common::*;

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
fn run_check(config_path: &std::path::Path, export_name: &str) -> String {
    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            config_path.to_str().unwrap(),
            "--export",
            export_name,
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet check");

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
        .trim();
    if let Some(k) = raw.strip_suffix('K') {
        k.trim().parse::<i64>().expect("K-suffixed number") * 1_000
    } else if let Some(m) = raw.strip_suffix('M') {
        m.trim().parse::<i64>().expect("M-suffixed number") * 1_000_000
    } else {
        raw.parse::<i64>().expect("bare row-estimate number")
    }
}

/// Build a `table:`-shortcut config (form A) for the seeded `orders` table.
fn config_table_form(out_dir: &std::path::Path, name: &str, mode_block: &str) -> String {
    format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {name}
    table: {SEEDED_TABLE}
    {mode_block}
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

/// Build a `query:`-form config (form B) for the same seeded `orders` table.
fn config_query_form(out_dir: &std::path::Path, name: &str, mode_block: &str) -> String {
    format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {name}
    query: "SELECT * FROM {SEEDED_TABLE}"
    {mode_block}
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

// ─── finding #3: row estimate for the `table:` shortcut ───────────────────────

// AUDIT-RED preflight-table-form: `table:` shortcut EXPLAINs "SELECT 1" not the table, so row estimate is ~1. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_table_shortcut_row_estimate_matches_real_table() {
    require_alive(LiveService::Postgres);

    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let name_a = unique_name("orders_table_form");
    let yaml = config_table_form(out_dir.path(), &name_a, "mode: full");
    let cfg = write_config(&cfg_dir, &yaml);

    let stdout = run_check(&cfg, &name_a);
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

    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let name_a = unique_name("orders_table_form");
    let name_b = unique_name("orders_query_form");
    let yaml_a = config_table_form(out_dir.path(), &name_a, "mode: full");
    let yaml_b = config_query_form(out_dir.path(), &name_b, "mode: full");
    let cfg_a = write_config(&cfg_dir, &yaml_a);

    // Separate cfg dir for B so the two rivet.yaml files don't collide.
    let cfg_dir_b = tempfile::tempdir().unwrap();
    let cfg_b = write_config(&cfg_dir_b, &yaml_b);

    let est_a = parse_row_estimate(&run_check(&cfg_a, &name_a));
    let est_b = parse_row_estimate(&run_check(&cfg_b, &name_b));

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

    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let name_a = unique_name("orders_table_chunked");
    let mode_block = "mode: chunked\n    chunk_column: id\n    chunk_size: 500";
    let yaml = config_table_form(out_dir.path(), &name_a, mode_block);
    let cfg = write_config(&cfg_dir, &yaml);

    let stdout = run_check(&cfg, &name_a);

    // `orders.id` spans 1..2500. The `query:` form prints "Cursor range: 1 .. 2500";
    // the `table:` form must do the same. Current code drops the line because the
    // range probe runs `min/max(...) FROM (SELECT 1) AS _rivet` and finds nothing.
    assert!(
        stdout.contains("Cursor range:"),
        "chunked `table:`-shortcut export must report a 'Cursor range:' line \
         (orders.id spans 1..{SEEDED_ROWS}); it was missing.\nfull check output:\n{stdout}"
    );
}
