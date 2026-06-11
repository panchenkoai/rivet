//! Live E2E tests for the **exit-code taxonomy** (see `crate::error::ExitClass`).
//!
//! An unattended scheduler branches on the process exit code instead of grepping
//! stderr:
//!
//!   * `0` success
//!   * `1` generic / config / usage error
//!   * `2` retryable (transient — safe to retry the same command)
//!   * `3` data-integrity (quality gate / reconcile mismatch / duplicate-guard /
//!     manifest inconsistency — STOP, data may be wrong)
//!   * `4` schema-drift (`on_schema_drift: fail` tripped — needs review)
//!
//! The class boundaries themselves are pinned by fast unit tests in
//! `src/error.rs` (transient→2, syntax→1, schema-drift→4, data-integrity→3,
//! including the flattened-`run`-aggregate string bridge). This file proves the
//! *end-to-end wiring*: a real `rivet run` against live Postgres returns the
//! data-integrity code `3` when a quality gate fails — i.e. `main` actually
//! routes the failure through `classify_exit` and exits with the class.
//!
//! Run with: `cargo test --test sec_exit_codes -- --include-ignored`

mod common;
use common::*;

fn cfg(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = write_config(&d, yaml);
    (d, p)
}

/// A quality-gate failure (`row_count_min` above the real row count) must exit
/// with the **data-integrity** code `3` — the scheduler's signal to STOP, not
/// retry. The export wrote a (potentially wrong-shaped) dataset; blindly
/// retrying would re-produce the same failing result.
#[test]
#[ignore = "live: postgres"]
fn quality_gate_failure_exits_data_integrity_3() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5); // only 5 rows
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("xc_quality_3");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      row_count_min: 100
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert_eq!(
        result.status.code(),
        Some(3),
        "quality-gate failure must exit 3 (data-integrity); stderr:\n{stderr}"
    );
    // The human message is unchanged — the marker only sets the exit class.
    assert!(
        stderr.contains("quality check(s) failed"),
        "operator-facing quality message must be preserved verbatim; stderr:\n{stderr}"
    );
}

/// A clean export still exits `0` — the taxonomy must not regress the success
/// path. Guards against `classify_exit` (or the markers) accidentally tagging a
/// successful run.
#[test]
#[ignore = "live: postgres"]
fn clean_export_still_exits_0() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(20);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("xc_clean_0");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      row_count_min: 1
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert_eq!(
        result.status.code(),
        Some(0),
        "a clean export must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

/// A config / usage error that is *not* transient must exit `1` (generic), not
/// `2`/`3`/`4`. A missing config file is the simplest deterministic generic
/// error and needs no live infrastructure — but it shares this file because it
/// pins the same `main`→`classify_exit` wiring. (Not `#[ignore]`d: no DB.)
#[test]
fn missing_config_exits_generic_1() {
    let result = run_rivet(&[
        "run",
        "--config",
        "/nonexistent/rivet-does-not-exist.yaml",
        "--export",
        "anything",
    ]);
    assert_eq!(
        result.status.code(),
        Some(1),
        "a missing-config error is generic (exit 1), not retryable/data-integrity; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
