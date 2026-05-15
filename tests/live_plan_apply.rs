//! Live E2E tests for `rivet plan` + `rivet apply` against real databases.
//!
//! ## Contract coverage (ADR-0005)
//!
//! | ID | Scenario | Contract |
//! |---|---|---|
//! | PA-L1 | Plan + Apply full-mode round-trip | PA1, PA7 — artifact is comm channel; state writes |
//! | PA-L2 | Plan + Apply chunked round-trip | PA5 — chunk ranges pre-computed and replayed |
//! | PA-L3 | Plan `--format pretty` prints summary to stdout | PA1 — no file written |
//! | PA-L4 | Plaintext URL credentials redacted in plan JSON | PA9 |
//! | PA-L5 | Expired plan (> 24 h) rejected without --force | PA3 |
//! | PA-L6 | `--force` bypasses 24 h staleness gate | PA3 |
//! | PA-L7 | Missing plan file → non-zero exit | PA1 |
//! | PA-L8 | `plan --param` substitutes query parameter | PA1 — artifact embeds params |

mod common;

use common::*;

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Build a YAML config that uses `url_env: DATABASE_URL` (safe for plan/apply
/// round-trips — plaintext URL credentials would be redacted in the artifact
/// and cause apply to fail to reconnect).
fn pg_url_env_config(table: &str, mode_block: &str, out_dir: &std::path::Path) -> String {
    format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {table}
    query: "SELECT id, name FROM {table}"
    {mode_block}
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

/// Patch `created_at` in a plan JSON to a fixed old timestamp (2020-01-01T00:00:00Z),
/// making the artifact appear > 24 h old to the staleness check.
fn make_stale_plan(plan_json: &str) -> String {
    // The field appears once at the top level: `"created_at":"<RFC3339>"`.
    // Find it and replace just the value portion.
    // The artifact is serialized with serde_json pretty-print: `"created_at": "…"`.
    const KEY: &str = r#""created_at": ""#;
    if let Some(start) = plan_json.find(KEY) {
        let value_start = start + KEY.len();
        if let Some(end_offset) = plan_json[value_start..].find('"') {
            let value_end = value_start + end_offset;
            let mut out = plan_json.to_string();
            out.replace_range(value_start..value_end, "2020-01-01T00:00:00Z");
            return out;
        }
    }
    panic!("make_stale_plan: could not find 'created_at' field in plan JSON");
}

// ─── PA-L1: full-mode plan + apply round-trip ─────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_and_apply_full_export_round_trip() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(30);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = pg_url_env_config(table.name(), "mode: full", out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    // ── rivet plan ────────────────────────────────────────────────────────────
    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan");

    assert!(
        plan_out.status.success(),
        "rivet plan must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );
    assert!(
        plan_path.exists(),
        "plan.json must be written by rivet plan"
    );

    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");
    let plan: serde_json::Value =
        serde_json::from_str(&plan_json).expect("plan.json must be valid JSON");

    assert_eq!(
        plan["export_name"].as_str().unwrap_or(""),
        table.name(),
        "plan.json must embed the correct export_name"
    );
    assert!(
        plan["plan_id"]
            .as_str()
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "plan.json must have a non-empty plan_id"
    );
    assert_eq!(
        plan["strategy"].as_str().unwrap_or(""),
        "full",
        "strategy must be 'full' for a full-mode export"
    );

    // ── rivet apply ───────────────────────────────────────────────────────────
    let apply_out = std::process::Command::new(RIVET_BIN)
        .args(["apply", plan_path.to_str().unwrap()])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet apply");

    assert!(
        apply_out.status.success(),
        "rivet apply must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&apply_out.stderr),
        String::from_utf8_lossy(&apply_out.stdout)
    );

    // Parquet file must exist in the output dir.
    let parquet = files_with_extension(out_dir.path(), "parquet");
    assert!(
        !parquet.is_empty(),
        "at least 1 parquet file must exist after apply; out_dir: {:?}",
        out_dir.path()
    );
}

// ─── PA-L2: chunked plan + apply — chunk_ranges pre-computed and replayed ─────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_and_apply_chunked_export_round_trip_uses_precomputed_ranges() {
    require_alive(LiveService::Postgres);

    // 150 rows / chunk_size 50 → exactly 3 chunks.
    let table = seed_pg_numeric_table(150);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let mode_block = "mode: chunked\n    chunk_column: id\n    chunk_size: 50".to_string();
    let yaml = pg_url_env_config(table.name(), &mode_block, out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    // ── rivet plan ────────────────────────────────────────────────────────────
    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan");

    assert!(
        plan_out.status.success(),
        "rivet plan (chunked) must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );

    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");
    let plan: serde_json::Value = serde_json::from_str(&plan_json).unwrap();

    // Chunk ranges must be pre-computed in the artifact (PA5).
    let chunk_ranges = plan["computed"]["chunk_ranges"]
        .as_array()
        .expect("computed.chunk_ranges must be an array");
    assert_eq!(
        chunk_ranges.len(),
        3,
        "150 rows / chunk_size 50 must produce exactly 3 chunk ranges; got: {chunk_ranges:?}"
    );
    assert_eq!(
        plan["computed"]["chunk_count"].as_i64().unwrap_or(0),
        3,
        "computed.chunk_count must be 3"
    );
    assert_eq!(
        plan["strategy"].as_str().unwrap_or(""),
        "chunked",
        "strategy must be 'chunked'"
    );

    // ── rivet apply ───────────────────────────────────────────────────────────
    let apply_out = std::process::Command::new(RIVET_BIN)
        .args(["apply", plan_path.to_str().unwrap()])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet apply");

    assert!(
        apply_out.status.success(),
        "rivet apply (chunked) must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&apply_out.stderr),
        String::from_utf8_lossy(&apply_out.stdout)
    );

    // 3 chunks → 3 parquet files.
    let parquet = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(
        parquet.len(),
        3,
        "3 chunks must produce 3 parquet files; found: {parquet:?}"
    );
}

// ─── PA-L3: plan --format pretty prints human-readable summary to stdout ──────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_pretty_format_prints_summary_to_stdout() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = pg_url_env_config(table.name(), "mode: full", out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);

    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "pretty",
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan --format pretty");

    assert!(
        plan_out.status.success(),
        "rivet plan --format pretty must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );

    let stdout = String::from_utf8_lossy(&plan_out.stdout);
    assert!(
        stdout.contains(table.name()),
        "pretty plan output must mention the export/table name; got:\n{stdout}"
    );
    // No plan file should be written when --format pretty without --output.
    let plan_path = cfg_dir.path().join("plan.json");
    assert!(
        !plan_path.exists(),
        "no plan.json file must be written with --format pretty and no --output"
    );
}

// ─── PA-L4: credential redaction (PA9) ────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_json_redacts_plaintext_url_credentials() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    // Config uses a plaintext URL with embedded password.
    // The POSTGRES_URL is "postgresql://rivet:rivet@127.0.0.1:5432/rivet".
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
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out_dir.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ])
        .output()
        .expect("spawn rivet plan");

    assert!(
        plan_out.status.success(),
        "rivet plan must exit 0 even with plaintext URL; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );

    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");

    // The plaintext password "rivet:rivet" must NOT appear in the artifact.
    assert!(
        !plan_json.contains("rivet:rivet"),
        "plan JSON must not contain plaintext credentials 'rivet:rivet' (PA9 redaction failed)"
    );
    // The redacted marker must be present.
    assert!(
        plan_json.contains("REDACTED"),
        "plan JSON must contain 'REDACTED' in place of credentials; got:\n{plan_json}"
    );
}

// ─── PA-L5: expired plan rejected without --force (PA3) ───────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn apply_rejects_expired_plan_without_force() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = pg_url_env_config(table.name(), "mode: full", out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    // Generate a fresh plan.
    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan");
    assert!(plan_out.status.success(), "plan generation must succeed");

    // Patch created_at to make the plan > 24 h old.
    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");
    let stale_json = make_stale_plan(&plan_json);
    let stale_path = cfg_dir.path().join("stale_plan.json");
    std::fs::write(&stale_path, &stale_json).expect("write stale plan");

    // Apply without --force must reject.
    let apply_out = std::process::Command::new(RIVET_BIN)
        .args(["apply", stale_path.to_str().unwrap()])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet apply");

    assert!(
        !apply_out.status.success(),
        "rivet apply on a > 24 h old plan must exit non-zero (PA3); \
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&apply_out.stdout),
        String::from_utf8_lossy(&apply_out.stderr)
    );
    let stderr = String::from_utf8_lossy(&apply_out.stderr);
    assert!(
        stderr.contains("old") || stderr.contains("stale") || stderr.contains("24"),
        "error message must mention staleness; got:\n{stderr}"
    );
}

// ─── PA-L6: --force bypasses the 24 h staleness gate ─────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn apply_force_flag_bypasses_expired_plan() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = pg_url_env_config(table.name(), "mode: full", out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    // Generate plan.
    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan");
    assert!(plan_out.status.success(), "plan generation must succeed");

    // Patch to make it stale.
    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");
    let stale_json = make_stale_plan(&plan_json);
    let stale_path = cfg_dir.path().join("stale_plan.json");
    std::fs::write(&stale_path, &stale_json).expect("write stale plan");

    // Apply WITH --force must succeed despite staleness.
    let apply_out = std::process::Command::new(RIVET_BIN)
        .args(["apply", "--force", stale_path.to_str().unwrap()])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet apply --force");

    assert!(
        apply_out.status.success(),
        "rivet apply --force on a stale plan must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&apply_out.stderr),
        String::from_utf8_lossy(&apply_out.stdout)
    );

    // Data must actually be exported.
    let parquet = files_with_extension(out_dir.path(), "parquet");
    assert!(
        !parquet.is_empty(),
        "parquet file must exist after forced apply; out_dir: {:?}",
        out_dir.path()
    );
}

// ─── PA-L8: plan --param substitutes query parameter ─────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_param_flag_substitutes_in_query() {
    require_alive(LiveService::Postgres);

    // Seed 40 rows (ids 0–39); plan with param max_id=19 → 20 rows.
    let table = seed_pg_numeric_table(40);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {name}
    query: "SELECT id, name FROM {name} WHERE id <= ${{max_id}}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out_dir.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan_param.json");

    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--param",
            "max_id=19",
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan --param");

    assert!(
        plan_out.status.success(),
        "plan --param must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );
    assert!(plan_path.exists(), "plan.json must be written");

    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");

    // The param must be embedded in the artifact.
    assert!(
        plan_json.contains("max_id") || plan_json.contains("19"),
        "plan artifact must embed the param value; snippet:\n{}",
        &plan_json[..plan_json.len().min(500)]
    );

    // Apply the plan and verify the row count.
    let apply_out = std::process::Command::new(RIVET_BIN)
        .args(["apply", plan_path.to_str().unwrap()])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet apply");

    assert!(
        apply_out.status.success(),
        "apply of param plan must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&apply_out.stderr),
        String::from_utf8_lossy(&apply_out.stdout)
    );

    let parquet_files = files_with_extension(out_dir.path(), "parquet");
    assert!(
        !parquet_files.is_empty(),
        "at least one parquet file must exist after apply"
    );

    // Verify exactly 20 rows (ids 0..=19) were written: query the state DB.
    let db_path = cfg_dir.path().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open state db after apply");
    let rows_written: i64 = conn
        .query_row(
            "SELECT rows_written FROM run_journal \
             WHERE export_name = ?1 ORDER BY finished_at DESC LIMIT 1",
            [table.name()],
            |r| r.get(0),
        )
        .expect("query rows_written from run_journal");
    assert_eq!(
        rows_written, 20,
        "plan --param max_id=19 must export exactly 20 rows (ids 0..=19); got {rows_written}"
    );
}

// ─── PA-L7: missing plan file → non-zero exit (PA1) ──────────────────────────

#[test]
#[ignore = "live: requires docker compose (any)"]
fn apply_missing_plan_file_exits_nonzero() {
    require_alive(LiveService::Postgres);

    let apply_out = std::process::Command::new(RIVET_BIN)
        .args(["apply", "/tmp/rivet_nonexistent_plan_xyz.json"])
        .output()
        .expect("spawn rivet apply");

    assert!(
        !apply_out.status.success(),
        "rivet apply with missing plan file must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&apply_out.stderr);
    assert!(
        !stderr.is_empty(),
        "stderr must not be empty when plan file is missing"
    );
}
