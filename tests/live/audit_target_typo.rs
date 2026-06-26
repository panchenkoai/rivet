//! AUDIT cluster `target-typo` — RED tests.
//!
//! Finding #14: `rivet check --target <typo>` (e.g. `bigqery`) is silently
//! dropped to `None` at `cli/dispatch.rs:194`
//! (`target.as_deref().and_then(ExportTarget::parse)`). The command exits 0
//! with no warning, so a CI gate of `check --target bigquery --strict` gives
//! false BigQuery-compat assurance when the warehouse name is misspelled.
//!
//! The config-level path is loud — `preflight/mod.rs:173` bails with
//! "unknown target '<typo>'" — so the CLI flag should mirror that. These tests
//! assert the CORRECT (loud) behavior and are expected to FAIL until the CLI
//! flag stops swallowing unknown targets.

use crate::common::*;

/// Minimal single-export Postgres config (orders-style: int + text columns).
fn simple_config(table: &str, out_dir: &std::path::Path) -> String {
    format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {table}
    query: "SELECT id, name FROM {table}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

// AUDIT-RED target-typo: `check --target <typo>` is silently dropped to None (dispatch.rs:194) — exit 0, no warning. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_check_unknown_cli_target_exits_nonzero() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = run_rivet(&[
        "check",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
        "--target",
        "nosuchwarehouse",
    ]);

    // CORRECT behavior: an unknown `--target` must be a loud error, mirroring the
    // config-level `target:` path (preflight/mod.rs:173 bails on a bad target).
    // CURRENT behavior: dispatch.rs:194 `.and_then(ExportTarget::parse)` drops it
    // to None silently and the command exits 0.
    assert!(
        !result.status.success(),
        "check --target nosuchwarehouse must exit non-zero (unknown target), \
         not silently succeed; got exit {:?}\nstdout:\n{}\nstderr:\n{}",
        result.status.code(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

// AUDIT-RED target-typo: `check --target <typo>` prints no mention of the unknown target — silent no-op gives false compat assurance. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_check_unknown_cli_target_names_the_typo() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = run_rivet(&[
        "check",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
        "--target",
        "bigqery", // common typo of "bigquery"
    ]);

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );

    // CORRECT behavior: the output must name the unknown/typo'd target so the
    // operator learns the warehouse name was not recognized. CURRENT behavior:
    // the typo is dropped to None and never mentioned anywhere.
    assert!(
        combined.contains("bigqery"),
        "check --target bigqery must name the unrecognized target 'bigqery' in its \
         output (mirroring the config-level 'unknown target' error); got:\n{combined}"
    );
}

// AUDIT-RED target-typo: contrast — config-level `target: <typo>` already errors loudly; the CLI flag must match. Asserts CORRECT behavior; this contrast assertion documents the existing loud path.
#[test]
#[ignore = "live: postgres"]
fn audit_check_unknown_config_target_errors_loudly_contrast() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    // Per-export `target:` carries the typo. `--strict` ensures the type-report
    // (and thus the config-level target validation at preflight/mod.rs:173) runs.
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: full
    format: parquet
    target: bigqery
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = run_rivet(&[
        "check",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
        "--strict",
    ]);

    // This documents the LOUD baseline the CLI flag must mirror: a config-level
    // unknown target exits non-zero and names the typo. (Expected to already
    // pass on current code — it is the contrast for the two RED tests above.)
    assert!(
        !result.status.success(),
        "config-level `target: bigqery` must exit non-zero (unknown target); \
         got exit {:?}\nstdout:\n{}\nstderr:\n{}",
        result.status.code(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        combined.contains("bigqery") && combined.to_lowercase().contains("unknown target"),
        "config-level error must name the typo 'bigqery' and say 'unknown target'; got:\n{combined}"
    );
}
