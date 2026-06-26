//! AUDIT-RED tests for the `observability` cluster (findings #9, #24, #25).
//!
//! These tests assert the CORRECT behavior of `rivet metrics` / `rivet journal`
//! and are EXPECTED TO FAIL against current code until the findings are fixed.
//! Production code must NOT be touched in this phase.
//!
//! ## Findings under test
//!
//! * **#9** — `rivet metrics` / `journal` use `--config` ONLY to locate the
//!   `.rivet_state.db` next to it; a nonexistent/garbage config path is never
//!   validated, so the command exits 0 with empty/foreign data instead of
//!   reporting "config not found". (A *missing* `--config` FLAG correctly exits
//!   2 — the inconsistency is the bug.)
//! * **#24** — `rivet journal` prints only an aggregate `files: N  rows: X`
//!   line; it never prints the produced file names that are stored in each
//!   `FileWritten` journal event.

use crate::common::*;

/// Minimal single-export full-mode parquet config for `table` writing to
/// `out_dir`.  Mirrors the `simple_config` helper used by `live_cli_flags.rs`.
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

// ─── Finding #9: metrics/journal must validate the --config PATH ───────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
// AUDIT-RED observability: `rivet metrics -c <nonexistent>.yaml` exits 0 with "No metrics recorded yet" instead of erroring on the missing config. Asserts CORRECT behavior; expected to FAIL until fixed.
fn audit_metrics_validates_config_path() {
    require_alive(LiveService::Postgres);

    // Point --config at a path that does NOT exist, inside a *fresh* empty
    // tempdir so there is no adjacent `.rivet_state.db` either. The only reason
    // the command could exit 0 here is the bug: it never validates that the
    // config file exists, opens an empty DB next to the phantom path, finds no
    // metrics, and prints "No metrics recorded yet." → exit 0.
    let cfg_dir = tempfile::tempdir().unwrap();
    let missing_cfg = cfg_dir.path().join(unique_name("does_not_exist") + ".yaml");
    assert!(
        !missing_cfg.exists(),
        "precondition: the config path must not exist; got {}",
        missing_cfg.display()
    );

    let result = std::process::Command::new(RIVET_BIN)
        .args(["metrics", "--config", missing_cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet metrics -c <nonexistent>");

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);

    // CORRECT behavior: a nonexistent config path is a configuration error, so
    // the command must exit non-zero — exactly as a *missing* `--config` flag
    // already does (clap exit 2). Currently exits 0.
    assert!(
        !result.status.success(),
        "rivet metrics with a nonexistent --config must exit non-zero (config not found); \
         got exit {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        result.status.code()
    );

    // And it must NOT silently claim there is simply no history — that masks the
    // operator's typo'd config path with foreign/empty state.
    assert!(
        !stdout.contains("No metrics recorded yet"),
        "rivet metrics must not print 'No metrics recorded yet.' for a nonexistent config — \
         that hides the missing-config error behind empty state; stdout:\n{stdout}"
    );
}

// ─── Finding #24: journal must list the produced file names ────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
// AUDIT-RED observability: `rivet journal` prints only an aggregate `files: N rows: X` line, never the produced .parquet file names stored in each FileWritten event. Asserts CORRECT behavior; expected to FAIL until fixed.
fn audit_journal_shows_file_names() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    // Run a small export. The committed part's file_name is recorded in the
    // run journal as a `FileWritten` event (src/pipeline/commit.rs record_part)
    // — the exact same name the manifest stores and `state files` displays.
    let run = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet run");
    assert!(
        run.status.success(),
        "setup export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    // Discover the produced parquet file's basename — this is the FileWritten
    // file_name that the journal must surface.
    let produced = files_with_extension(out.path(), "parquet");
    assert_eq!(
        produced.len(),
        1,
        "setup must produce exactly one parquet file; got {produced:?}"
    );
    let file_name = produced[0]
        .file_name()
        .and_then(|n| n.to_str())
        .expect("produced parquet path must have a file name")
        .to_string();

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "journal",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet journal");

    assert!(
        result.status.success(),
        "rivet journal must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // CORRECT behavior: the journal stores each FileWritten file_name and must
    // surface it, not just the aggregate `files: N  rows: X` line. Asserting on
    // the exact produced basename also proves the name comes from the journal,
    // not coincidence.
    assert!(
        stdout.contains(&file_name),
        "rivet journal must list the produced parquet file name '{file_name}' \
         (stored in the FileWritten journal event), not only an aggregate count; \
         got:\n{stdout}"
    );
}
