//! AUDIT-RED — cluster `state-guardrails` (findings #21, #22, #23).
//!
//! These tests drive the real `rivet state …` subcommands against the live
//! Postgres stack and assert the CORRECT behavior. They are expected to FAIL
//! against current code and pass once the guardrails are added.
//!
//! * #21 — `state reset-chunks -e <typo>` silently succeeds (exit 0,
//!   "Removed 0 …") with no export-name guardrail, unlike `state reset` which
//!   validates the name and errors with a "Known exports" hint.
//! * #22 — `state reset` does NOT clear `export_progression`, so
//!   `state progression` still reports the old committed boundary after the
//!   cursor has been reset (`state show` is empty, progression is stale).
//! * #23 — read-only inspect (`state show`) never parses the config; a garbage
//!   / missing config path yields a false "No exports have been run yet" exit 0
//!   and litters a fresh `.rivet_state.db`.
//!
//! Run with: `cargo test --test audit_state -- --ignored`

use crate::common::*;

use std::process::{Command, Output};

/// Run `rivet state <args…>` and capture exit code + stdout/stderr.
fn run_state(args: &[&str]) -> Output {
    Command::new(RIVET_BIN)
        .args(args)
        .output()
        .expect("spawn rivet state")
}

/// Minimal chunked-checkpoint config for `table` so a real chunk run is
/// recorded in the state DB (mirrors `state_reset_chunks_clears_checkpoint`
/// in tests/live_cli_flags.rs).
fn chunked_checkpoint_config(table: &str, out_dir: &std::path::Path) -> String {
    format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {table}
    query: "SELECT id, name FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: 50
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

/// Minimal incremental config for `table` with `cursor_column: created_at`
/// (mirrors `incremental_config` in tests/live_cli_flags.rs). An incremental
/// run records a committed boundary in `export_progression`.
fn incremental_config(table: &str, out_dir: &std::path::Path) -> String {
    format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {table}
    query: "SELECT id, name, created_at FROM {table}"
    mode: incremental
    cursor_column: created_at
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

/// Run `rivet run --config <cfg> --export <name>` and assert it succeeded.
fn run_export(cfg: &std::path::Path, export: &str) {
    let out = Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap(), "--export", export])
        .output()
        .expect("spawn rivet run");
    assert!(
        out.status.success(),
        "setup: rivet run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// AUDIT-RED state-guardrails: `state reset-chunks -e <typo>` silently succeeds (exit 0). Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_reset_chunks_rejects_unknown_export() {
    require_alive(LiveService::Postgres);

    // Seed + run a chunked-checkpoint export so chunk_run rows exist for the
    // real export name; the typo'd name below is NOT in the config.
    let table = seed_pg_numeric_table(100);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        &cfg_dir,
        &chunked_checkpoint_config(table.name(), out.path()),
    );
    run_export(&cfg, table.name());

    // `<name>x` is a typo: not declared in the config. Parity with
    // `state reset` requires this to be rejected, not silently "Removed 0".
    let typo = format!("{}x", table.name());
    let result = run_state(&[
        "state",
        "reset-chunks",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &typo,
    ]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);

    assert!(
        !result.status.success(),
        "reset-chunks on an unknown export '{typo}' must exit NON-zero (parity with `state reset`); \
         got exit 0.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains(&typo),
        "reset-chunks must name the unknown export '{typo}' in its error; \
         got stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

// AUDIT-RED state-guardrails: `state reset` leaves export_progression stale — progression still reports the committed boundary. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_reset_clears_progression() {
    require_alive(LiveService::Postgres);

    // Incremental export advances the cursor AND records a committed boundary
    // in export_progression.
    let table = seed_pg_numeric_table(100);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &incremental_config(table.name(), out.path()));
    run_export(&cfg, table.name());

    // Sanity: a committed boundary is recorded before the reset, otherwise the
    // post-reset assertion would pass vacuously.
    let before = run_state(&[
        "state",
        "progression",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);
    assert!(
        before.status.success(),
        "setup: state progression must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&before.stderr)
    );
    let before_out = String::from_utf8_lossy(&before.stdout);
    assert!(
        before_out.contains(table.name()) && !before_out.contains("No progression boundaries"),
        "setup: progression must report a committed boundary for '{}' before reset; got:\n{before_out}",
        table.name()
    );

    // Reset the export's state.
    let reset = run_state(&[
        "state",
        "reset",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);
    assert!(
        reset.status.success(),
        "state reset must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&reset.stderr)
    );

    // CORRECT behavior: after reset, progression must NOT still report a
    // committed boundary for the export. Currently the export_progression row
    // survives, so the old boundary is shown — stale.
    let after = run_state(&[
        "state",
        "progression",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);
    assert!(
        after.status.success(),
        "state progression must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&after.stderr)
    );
    let after_out = String::from_utf8_lossy(&after.stdout);
    assert!(
        after_out.contains("No progression boundaries"),
        "after `state reset`, progression must report no committed boundary for '{}' \
         (it is stale — the export_progression row was not cleared); got:\n{after_out}",
        table.name()
    );
}

// AUDIT-RED state-guardrails: `state show -c <garbage>.yaml` returns false "No state" exit 0 and leaks .rivet_state.db. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_state_show_validates_config_path() {
    require_alive(LiveService::Postgres);

    // Point at a config path that does not exist. A read-only inspect must
    // surface the bad path rather than silently opening a fresh state DB
    // beside it and printing "No exports have been run yet".
    let dir = tempfile::tempdir().unwrap();
    let missing_cfg = dir.path().join("does_not_exist.yaml");
    assert!(!missing_cfg.exists(), "precondition: config must be absent");

    let result = run_state(&["state", "show", "--config", missing_cfg.to_str().unwrap()]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);

    assert!(
        !result.status.success(),
        "state show on a missing config '{}' must exit NON-zero, not print \
         \"No exports have been run yet\" exit 0.\nstdout:\n{stdout}\nstderr:\n{stderr}",
        missing_cfg.display()
    );

    // And it must NOT litter a fresh state DB next to the (missing) config.
    let leaked_db = dir.path().join(".rivet_state.db");
    assert!(
        !leaked_db.exists(),
        "state show on a missing config must not create {} (read-only inspect \
         should not materialize a state DB for a bad path)",
        leaked_db.display()
    );
}
