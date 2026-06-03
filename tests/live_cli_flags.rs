//! Live E2E flag-coverage tests for commands that had zero live test coverage.
//!
//! Each test exercises exactly the flag(s) in its name against the real
//! docker-compose stack.  The goal is: every CLI flag is accepted and
//! produces the expected exit code + minimal output shape.
//!
//! ## Coverage map
//!
//! ### `rivet run`
//! | Flag | Test |
//! |------|------|
//! | `--json` | `run_json_flag_prints_aggregate_summary_to_stdout` |
//! | `--summary-output` | `run_summary_output_writes_json_to_file` |
//! | `--param` (single) | `run_param_flag_substitutes_in_query` |
//! | `--param` (multi) | `run_multi_param_substitutes_multiple_variables` |
//! | `--reconcile` | `run_reconcile_flag_exits_zero_when_counts_match` |
//! | `--parallel-exports` | `run_parallel_exports_flag_runs_both_exports` |
//! | `--parallel-export-processes` | `run_parallel_export_processes_flag_runs_both_exports` |
//!
//! ### `rivet check`
//! | Flag | Test |
//! |------|------|
//! | `--type-report` | `check_type_report_shows_column_table` |
//! | `--strict` (safe types → exit 0) | `check_strict_exits_zero_for_safe_types` |
//! | `--strict` (unsafe type → exit 1) | `check_strict_exits_nonzero_for_unsafe_type` |
//! | `--json` | `check_json_flag_outputs_type_report_as_json` |
//! | `--target` | `check_target_flag_accepts_bigquery_target` |
//! | `--param` | `check_param_flag_substitutes_in_query` |
//! | MySQL check | `check_mysql_basic_exits_zero` |
//!
//! ### `rivet doctor`
//! | Command | Test |
//! |---------|------|
//! | `doctor` (Postgres) | `doctor_exits_zero_when_source_reachable` |
//! | `doctor` (MySQL) | `doctor_exits_zero_when_mysql_source_reachable` |
//!
//! ### `rivet state`
//! | Subcommand | Test |
//! |------------|------|
//! | `state show` (incremental cursor) | `state_show_displays_cursor_after_incremental_export` |
//! | `state files` | `state_files_shows_manifest_row` |
//! | `state files --last` | `state_files_last_flag_limits_file_count` |
//! | `state reset` | `state_reset_clears_export_state` |
//! | `state chunks` | `state_chunks_shows_checkpoint_table` |
//! | `state reset-chunks` | `state_reset_chunks_clears_checkpoint` |
//! | `state reset-chunks --stuck-checkpoints` (no-op) | `state_reset_chunks_stuck_flag_is_idempotent` |
//! | `state reset-chunks --stuck-checkpoints` (real reset) | `state_reset_chunks_stuck_flag_resets_in_progress_runs` |
//! | `state progression` | `state_progression_shows_committed_boundary` |
//!
//! ### `rivet metrics`
//! | Flag | Test |
//! |------|------|
//! | (command) | `metrics_shows_run_history` |
//! | `--last` | `metrics_last_flag_limits_output` |
//!
//! ### `rivet journal`
//! | Flag | Test |
//! |------|------|
//! | (command) | `journal_shows_run_summary` |
//! | `--last` | `journal_last_flag_limits_output` |
//! | `--run-id` | `journal_run_id_shows_specific_run` |
//!
//! ### `rivet completions`
//! | Arg | Test |
//! |-----|------|
//! | `bash` | `completions_bash_outputs_nonempty_script` |

mod common;

use common::*;

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Write a minimal config for a single full-mode export of `table`.
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

/// Run export, return stdout.  Panics if the export fails.
fn run_export(cfg: &std::path::Path, export: &str, extra_args: &[&str]) -> std::process::Output {
    let mut args = vec!["run", "--config", cfg.to_str().unwrap(), "--export", export];
    args.extend_from_slice(extra_args);
    let out = std::process::Command::new(RIVET_BIN)
        .args(&args)
        .output()
        .expect("spawn rivet run");
    assert!(
        out.status.success(),
        "run_export helper failed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Query the state DB adjacent to `cfg` and return the most-recent run_id for
/// `export_name` from the `run_journal` table.
fn last_run_id(cfg: &std::path::Path, export_name: &str) -> String {
    let db_path = cfg.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(db_path).expect("open state db");
    conn.query_row(
        "SELECT run_id FROM run_journal WHERE export_name = ?1 \
         ORDER BY finished_at DESC LIMIT 1",
        [export_name],
        |r| r.get(0),
    )
    .expect("get run_id from run_journal")
}

/// Return ALL run_ids for `export_name` ordered by insertion (oldest-first).
/// Uses `rowid` rather than `finished_at` because two back-to-back runs can
/// share the same timestamp, making ORDER BY finished_at non-deterministic.
fn all_run_ids(cfg: &std::path::Path, export_name: &str) -> Vec<String> {
    let db_path = cfg.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(db_path).expect("open state db");
    let mut stmt = conn
        .prepare(
            "SELECT run_id FROM run_journal WHERE export_name = ?1 \
             ORDER BY rowid ASC",
        )
        .expect("prepare all_run_ids");
    stmt.query_map([export_name], |r| r.get(0))
        .expect("query all_run_ids")
        .filter_map(Result::ok)
        .collect()
}

/// Build a minimal incremental config for `table` with `cursor_column: created_at`.
/// The query must NOT include a cursor WHERE clause — rivet wraps the base query
/// automatically: `SELECT * FROM (base) AS _rivet WHERE created_at > $1 ORDER BY created_at`.
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

// ═══════════════════════════════════════════════════════════════════════════════
// rivet run flags
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_json_flag_prints_aggregate_summary_to_stdout() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(20);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--json",
        ])
        .output()
        .expect("spawn rivet run --json");

    assert!(
        result.status.success(),
        "run --json must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("--json output must be valid JSON");

    assert_eq!(
        json["success_count"].as_i64().unwrap_or(0),
        1,
        "success_count must be 1"
    );
    assert_eq!(
        json["total_rows"].as_i64().unwrap_or(0),
        20,
        "total_rows must equal seeded row count"
    );
    assert!(
        json["per_export"].is_array(),
        "per_export must be present and an array"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_summary_output_writes_json_to_file() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    let summary_path = cfg_dir.path().join("summary.json");

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--summary-output",
            summary_path.to_str().unwrap(),
        ])
        .output()
        .expect("spawn rivet run --summary-output");

    assert!(
        result.status.success(),
        "run --summary-output must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        summary_path.exists(),
        "summary file must be written at the specified path"
    );

    let raw = std::fs::read_to_string(&summary_path).expect("read summary file");
    let json: serde_json::Value =
        serde_json::from_str(raw.trim()).expect("summary file must be valid JSON");
    assert_eq!(
        json["success_count"].as_i64().unwrap_or(0),
        1,
        "summary must report 1 successful export"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_param_flag_substitutes_in_query() {
    require_alive(LiveService::Postgres);

    // Seed 50 rows; query will filter to id <= ${max_id} (30).
    let table = seed_pg_numeric_table(50);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

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
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--param",
            "max_id=30",
            "--json",
        ])
        .output()
        .expect("spawn rivet run --param");

    assert!(
        result.status.success(),
        "run --param must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&result.stdout).trim())
            .expect("--json output must be valid JSON");
    // IDs are 0-based: 0..49.  WHERE id <= 30 → ids 0, 1, ..., 30 → exactly 31 rows.
    let rows = json["total_rows"].as_i64().unwrap_or(-1);
    assert_eq!(
        rows, 31,
        "param substitution must export exactly 31 rows (ids 0..=30); got {rows}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_multi_param_substitutes_multiple_variables() {
    require_alive(LiveService::Postgres);

    // Seed 50 rows (ids 0–49); filter id >= ${min_id} AND id <= ${max_id}.
    let table = seed_pg_numeric_table(50);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name} WHERE id >= ${{min_id}} AND id <= ${{max_id}}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--param",
            "min_id=10",
            "--param",
            "max_id=20",
            "--json",
        ])
        .output()
        .expect("spawn rivet run --param --param");

    assert!(
        result.status.success(),
        "run with two --param flags must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&result.stdout).trim())
            .expect("--json output must be valid JSON");
    // ids 10, 11, ..., 20 → exactly 11 rows.
    let rows = json["total_rows"].as_i64().unwrap_or(-1);
    assert_eq!(
        rows, 11,
        "two --param substitutions must export exactly 11 rows (ids 10..=20); got {rows}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_reconcile_flag_exits_zero_when_counts_match() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(25);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--reconcile",
            "--json",
        ])
        .output()
        .expect("spawn rivet run --reconcile");

    assert!(
        result.status.success(),
        "run --reconcile must exit 0 when source and export counts match; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    // --json writes the aggregate to stdout; verify total_rows.
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&result.stdout).trim())
            .expect("run --reconcile --json must produce valid JSON on stdout");
    assert_eq!(
        json["total_rows"].as_i64().unwrap_or(-1),
        25,
        "run --reconcile --json must report 25 total_rows; got:\n{}",
        String::from_utf8_lossy(&result.stdout)
    );

    // Reconcile result is reported in the per-export summary block on stderr.
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("MATCH"),
        "run --reconcile stderr must contain 'MATCH' when counts agree; got:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_reconcile_reports_mismatch_when_source_grows_after_snapshot() {
    // `run --reconcile` reconciles what it exported (a consistent source
    // snapshot) against a *fresh* source COUNT(*). There is no deterministic
    // single-snapshot mismatch — by construction the export and the recount read
    // the same source moments apart — so a real mismatch is a concurrency event.
    // We construct it deterministically: full mode holds one snapshot for the
    // whole read; a throttle keeps the run alive while a writer inserts 50 rows
    // *after* the snapshot opens but *before* the end-of-run recount. The export
    // therefore sees 200 and reconcile sees 250 → MISMATCH.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(200);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
  tuning: {{batch_size: 10, throttle_ms: 60}}
exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Writer: after the snapshot has opened (and while the throttled export is
    // still running), commit 50 new rows the export's snapshot can't see.
    let table_name = table.name().to_string();
    let writer = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(600));
        let mut c = pg_connect();
        let _ = c.batch_execute(&format!(
            "INSERT INTO {table_name} (id, name, amount) \
             SELECT g, 'late', 1.0 FROM generate_series(201, 250) g"
        ));
    });

    let result = run_rivet(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
        "--reconcile",
    ]);
    let _ = writer.join();

    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("MISMATCH"),
        "run --reconcile must report MISMATCH when the source grew (250) past what \
         was exported (200); stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_reconcile_implies_validate_produces_manifest_verdict() {
    // ADR-0013: `--reconcile` *subsumes* `--validate` — a reconcile run must also
    // produce the manifest validation verdict (M5/M6) without the operator
    // passing `--validate`. Before the fix `plan.validate` and `plan.reconcile`
    // were independent, so a reconcile-only run skipped validation entirely and
    // had no `validated` verdict. A plain `rivet run` shows no `validated:` line;
    // a `--reconcile` run must.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(25);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = run_rivet(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
        "--reconcile",
    ]);
    assert!(
        result.status.success(),
        "run --reconcile must succeed on a clean export; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("validated:"),
        "ADR-0013: --reconcile must also produce the --validate verdict (a `validated:` \
         line on the run card) without --validate; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("MATCH"),
        "and still report the reconcile result; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_parallel_exports_flag_runs_both_exports() {
    require_alive(LiveService::Postgres);

    let t1 = seed_pg_numeric_table(10);
    let t2 = seed_pg_numeric_table(10);
    let out1 = tempfile::tempdir().unwrap();
    let out2 = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {n1}
    query: "SELECT id, name FROM {n1}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {o1}
  - name: {n2}
    query: "SELECT id, name FROM {n2}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {o2}
"#,
        n1 = t1.name(),
        o1 = out1.path().display(),
        n2 = t2.name(),
        o2 = out2.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--parallel-exports",
            "--json",
        ])
        .output()
        .expect("spawn rivet run --parallel-exports");

    assert!(
        result.status.success(),
        "run --parallel-exports must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&result.stdout).trim()).unwrap();
    assert_eq!(
        json["parallel_mode"].as_str().unwrap_or(""),
        "parallel-threads",
        "parallel_mode must be 'parallel-threads'"
    );
    assert_eq!(
        json["success_count"].as_i64().unwrap_or(0),
        2,
        "both exports must succeed"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_parallel_export_processes_flag_runs_both_exports() {
    require_alive(LiveService::Postgres);

    let t1 = seed_pg_numeric_table(10);
    let t2 = seed_pg_numeric_table(10);
    let out1 = tempfile::tempdir().unwrap();
    let out2 = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {n1}
    query: "SELECT id, name FROM {n1}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {o1}
  - name: {n2}
    query: "SELECT id, name FROM {n2}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {o2}
"#,
        n1 = t1.name(),
        o1 = out1.path().display(),
        n2 = t2.name(),
        o2 = out2.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--parallel-export-processes",
            "--json",
        ])
        .output()
        .expect("spawn rivet run --parallel-export-processes");

    assert!(
        result.status.success(),
        "run --parallel-export-processes must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&result.stdout).trim()).unwrap();
    assert_eq!(
        json["parallel_mode"].as_str().unwrap_or(""),
        "parallel-processes",
        "parallel_mode must be 'parallel-processes'"
    );
    assert_eq!(
        json["success_count"].as_i64().unwrap_or(0),
        2,
        "both exports must succeed"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// rivet check flags
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "live: requires docker compose postgres"]
fn check_type_report_shows_column_table() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--type-report",
        ])
        .output()
        .expect("spawn rivet check --type-report");

    assert!(
        result.status.success(),
        "check --type-report must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // Type report shows a table with Fidelity column.
    assert!(
        stdout.contains("Fidelity") || stdout.contains("fidelity"),
        "type-report must show 'Fidelity' column; got:\n{stdout}"
    );
    // id is BIGINT → rivet/source type 'int8'; both id and name have fidelity 'exact'.
    assert!(
        stdout.contains("int8"),
        "type-report must show 'int8' for the BIGINT id column; got:\n{stdout}"
    );
    assert!(
        stdout.contains("exact"),
        "type-report must show 'exact' fidelity for int8/text columns; got:\n{stdout}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn check_strict_exits_zero_for_safe_types() {
    require_alive(LiveService::Postgres);

    // Our seeded table has id (BIGINT) and name (TEXT) — both have exact fidelity.
    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    // Query only the safe columns.
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
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--strict",
        ])
        .output()
        .expect("spawn rivet check --strict");

    assert!(
        result.status.success(),
        "check --strict must exit 0 for safe (int/text) columns; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn check_strict_exits_nonzero_for_unsafe_type() {
    require_alive(LiveService::Postgres);

    // Unbounded NUMERIC (no precision/scale) maps to RivetType::Unsupported
    // → TypeFidelity::Unsupported → is_unsafe_for_strict_mode() = true.
    // rivet check --strict must exit non-zero and report the violation.
    let name = unique_name("rivet_strict_tbl");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, val NUMERIC);"
    ))
    .expect("create strict-test table");

    struct DropTable(String);
    impl Drop for DropTable {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
            }
        }
    }
    let _cleanup = DropTable(name.clone());

    let _ = c.execute(
        &format!("INSERT INTO {name} (id, val) VALUES (1, 3.14)"),
        &[],
    );

    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, val FROM {name}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &name,
            "--strict",
        ])
        .output()
        .expect("spawn rivet check --strict (unsafe type)");

    assert!(
        !result.status.success(),
        "check --strict must exit non-zero for an unbounded NUMERIC column; \
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    // The error must name the offending column AND indicate a type problem.
    assert!(
        combined.contains("val"),
        "error output must name the column 'val'; got:\n{combined}"
    );
    assert!(
        combined.contains("numeric")
            || combined.contains("Unsupported")
            || combined.contains("strict")
            || combined.contains("unsupported"),
        "error output must indicate a type violation (numeric/unsupported/strict); got:\n{combined}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn check_json_flag_outputs_type_report_as_json() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--json",
        ])
        .output()
        .expect("spawn rivet check --json");

    assert!(
        result.status.success(),
        "check --json must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // print_json emits the entire ExportTypeReport as a single JSON line.
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("check --json stdout must be valid JSON");

    assert_eq!(
        json["export"].as_str().unwrap_or(""),
        table.name(),
        "JSON 'export' field must match the export name"
    );
    let cols = json["columns"]
        .as_array()
        .expect("'columns' must be a JSON array");
    assert!(
        !cols.is_empty(),
        "check --json must report at least one column; got:\n{stdout}"
    );
    // id (BIGINT) and name (TEXT) must both appear as columns.
    let col_names: Vec<&str> = cols.iter().filter_map(|c| c["column"].as_str()).collect();
    assert!(
        col_names.contains(&"id"),
        "columns must include 'id'; got: {col_names:?}"
    );
    assert!(
        col_names.contains(&"name"),
        "columns must include 'name'; got: {col_names:?}"
    );
    // No violations for clean int/text columns.
    let violations = json["violations"]
        .as_array()
        .expect("'violations' must be a JSON array");
    assert!(
        violations.is_empty(),
        "check --json must report 0 violations for int8/text columns; got:\n{stdout}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn check_target_flag_accepts_bigquery_target() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--target",
            "bigquery",
        ])
        .output()
        .expect("spawn rivet check --target bigquery");

    // Exit 0 expected — our seeded table has int/text columns with no
    // BigQuery incompatibilities.
    assert!(
        result.status.success(),
        "check --target bigquery must exit 0 for safe types; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn check_param_flag_substitutes_in_query() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name} WHERE id <= ${{upper}}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--param",
            "upper=100",
            "--json",
        ])
        .output()
        .expect("spawn rivet check --param");

    assert!(
        result.status.success(),
        "check --param must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    // --json emits ExportTypeReport — verify the check actually ran the
    // substituted query and discovered both columns.
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&result.stdout).trim())
            .expect("check --param --json must produce valid JSON on stdout");

    let cols = json["columns"]
        .as_array()
        .expect("'columns' must be present");
    let col_names: Vec<&str> = cols.iter().filter_map(|c| c["column"].as_str()).collect();
    assert!(
        col_names.contains(&"id") && col_names.contains(&"name"),
        "check --param must discover 'id' and 'name' columns; got: {col_names:?}"
    );
    // Substitution succeeded: no violations for clean int/text columns.
    let violations = json["violations"]
        .as_array()
        .expect("'violations' must be present");
    assert!(
        violations.is_empty(),
        "check --param must report 0 violations; got:\n{}",
        String::from_utf8_lossy(&result.stdout)
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// rivet doctor
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "live: requires docker compose postgres"]
fn doctor_exits_zero_when_source_reachable() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));

    let result = std::process::Command::new(RIVET_BIN)
        .args(["doctor", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet doctor");

    assert!(
        result.status.success(),
        "doctor must exit 0 when source is reachable; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains("[OK]"),
        "doctor output must contain '[OK]' lines; got:\n{stdout}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// rivet state subcommands
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_show_displays_cursor_after_incremental_export() {
    require_alive(LiveService::Postgres);

    // Use incremental mode so state show has a cursor value to display.
    // Full-mode exports leave LAST CURSOR = "-".
    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &incremental_config(table.name(), out.path()));
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["state", "show", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet state show");

    assert!(
        result.status.success(),
        "state show must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // Must display the export name and the LAST CURSOR header.
    assert!(
        stdout.contains("LAST CURSOR"),
        "state show must print the LAST CURSOR header; got:\n{stdout}"
    );
    assert!(
        stdout.contains(table.name()),
        "state show must show the export name; got:\n{stdout}"
    );
    // Derive the cursor column offset from the header line itself — don't hardcode
    // a byte position that would silently break if show_state changes column widths.
    let header_line = stdout
        .lines()
        .find(|l| l.contains("LAST CURSOR"))
        .expect("state show must print a LAST CURSOR header");
    let cursor_col = header_line
        .find("LAST CURSOR")
        .expect("'LAST CURSOR' must appear in the header");

    let data_line = stdout
        .lines()
        .find(|l| l.contains(table.name()))
        .expect("state show must contain a data line with the export name");
    let cursor_field = data_line.get(cursor_col..).unwrap_or("").trim_start();
    assert!(
        !cursor_field.is_empty() && !cursor_field.starts_with('-'),
        "LAST CURSOR must be a non-null timestamp after an incremental export; \
         cursor field: {cursor_field:?}\nfull line: {data_line}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_files_shows_manifest_row() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "state",
            "files",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet state files");

    assert!(
        result.status.success(),
        "state files must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // Must show the parquet filename.
    assert!(
        stdout.contains(".parquet"),
        "state files must list the exported parquet file; got:\n{stdout}"
    );
    assert!(
        stdout.contains(table.name()),
        "state files must mention the export name; got:\n{stdout}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_files_last_flag_limits_file_count() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    // Two runs → two parquet files recorded in the manifest.
    run_export(&cfg, table.name(), &[]);
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "state",
            "files",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--last",
            "1",
        ])
        .output()
        .expect("spawn rivet state files --last 1");

    assert!(
        result.status.success(),
        "state files --last 1 must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // With --last 1, only 1 parquet file should appear.
    let parquet_lines: Vec<&str> = stdout.lines().filter(|l| l.contains(".parquet")).collect();
    assert_eq!(
        parquet_lines.len(),
        1,
        "state files --last 1 must show exactly 1 parquet file; got {}:\n{stdout}",
        parquet_lines.len()
    );

    // The file shown must be from the newest run.  state files output includes
    // the run_id in column 1, so we can verify it directly.
    let newest_run_id = last_run_id(&cfg, table.name());
    assert!(
        parquet_lines[0].contains(&newest_run_id),
        "state files --last 1 must show the most recent run's file (run_id {newest_run_id}); \
         got line: {}",
        parquet_lines[0]
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_reset_clears_export_state() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "state",
            "reset",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet state reset");

    assert!(
        result.status.success(),
        "state reset must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains("reset") || stdout.contains("Reset"),
        "state reset must confirm the reset; got:\n{stdout}"
    );

    // Verify the cursor record was actually removed from the DB.
    let db_path = cfg.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open state db after reset");
    let cursor_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM export_state WHERE export_name = ?1",
            [table.name()],
            |r| r.get(0),
        )
        .unwrap_or(0);
    assert_eq!(
        cursor_count, 0,
        "state reset must remove the export_state row from the DB; found {cursor_count}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_chunks_shows_checkpoint_table() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(100);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 50
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "state",
            "chunks",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet state chunks");

    assert!(
        result.status.success(),
        "state chunks must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains("completed"),
        "state chunks must show 'completed' chunk tasks; got:\n{stdout}"
    );
    assert!(
        stdout.contains(table.name()),
        "state chunks must mention the export name; got:\n{stdout}"
    );

    // 100 rows / chunk_size 50 = exactly 2 completed chunks.
    let db_path = cfg.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open state db");
    let completed_chunks: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM chunk_task \
             WHERE run_id IN (SELECT run_id FROM chunk_run WHERE export_name = ?1) \
             AND status = 'completed'",
            [table.name()],
            |r| r.get(0),
        )
        .expect("count completed chunk_tasks");
    assert_eq!(
        completed_chunks, 2,
        "100 rows / chunk_size 50 must produce exactly 2 completed chunk tasks; got {completed_chunks}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_reset_chunks_clears_checkpoint() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(100);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 50
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "state",
            "reset-chunks",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet state reset-chunks");

    assert!(
        result.status.success(),
        "state reset-chunks must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains("Removed") || stdout.contains("chunk run"),
        "state reset-chunks must confirm removal; got:\n{stdout}"
    );

    // Verify chunk_run records were actually removed from the DB.
    let db_path = cfg.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open state db after reset-chunks");
    let chunk_run_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM chunk_run", [], |r| r.get(0))
        .unwrap_or(0);
    assert_eq!(
        chunk_run_count, 0,
        "state reset-chunks must remove all chunk_run rows from the DB; found {chunk_run_count}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_reset_chunks_stuck_flag_is_idempotent() {
    // --stuck-checkpoints resets in-progress chunk runs for all exports.
    // With no stuck runs, it must still exit 0 gracefully.
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 10
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "state",
            "reset-chunks",
            "--config",
            cfg.to_str().unwrap(),
            "--stuck-checkpoints",
        ])
        .output()
        .expect("spawn rivet state reset-chunks --stuck-checkpoints");

    assert!(
        result.status.success(),
        "state reset-chunks --stuck-checkpoints must exit 0 even with no stuck runs; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_reset_chunks_stuck_flag_resets_in_progress_runs() {
    // Use RIVET_TEST_PANIC_AT=after_chunk_file:0 to crash the export after
    // writing chunk 0's file but before marking the chunk_run completed.
    // That leaves chunk_run.status = 'in_progress'.
    // --stuck-checkpoints must detect and clear it.
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(100);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 50
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // ── Step 1: inject crash after chunk 0's file is written ─────────────────
    let crash_out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .env("RIVET_TEST_PANIC_AT", "after_chunk_file:0")
        .output()
        .expect("spawn rivet run (crash injection)");

    assert!(
        !crash_out.status.success(),
        "injected crash must cause non-zero exit"
    );

    // ── Step 2: verify chunk_run is left in_progress ──────────────────────────
    let db_path = cfg.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open state db");
    let in_progress_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM chunk_run WHERE status = 'in_progress'",
            [],
            |r| r.get(0),
        )
        .expect("query chunk_run");
    assert_eq!(
        in_progress_count, 1,
        "fault injection must leave exactly 1 in_progress chunk_run; found {in_progress_count}"
    );
    drop(conn);

    // ── Step 3: reset stuck checkpoints ──────────────────────────────────────
    let reset_out = std::process::Command::new(RIVET_BIN)
        .args([
            "state",
            "reset-chunks",
            "--config",
            cfg.to_str().unwrap(),
            "--stuck-checkpoints",
        ])
        .output()
        .expect("spawn rivet state reset-chunks --stuck-checkpoints");

    assert!(
        reset_out.status.success(),
        "state reset-chunks --stuck-checkpoints must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&reset_out.stderr)
    );

    let stdout = String::from_utf8_lossy(&reset_out.stdout);
    assert!(
        stdout.contains(table.name()) || stdout.contains("Resetting"),
        "output must mention the reset export; got:\n{stdout}"
    );

    // ── Step 4: verify no more in_progress runs ───────────────────────────────
    let conn2 = rusqlite::Connection::open(&db_path).expect("re-open state db");
    let remaining: i64 = conn2
        .query_row(
            "SELECT COUNT(*) FROM chunk_run WHERE status = 'in_progress'",
            [],
            |r| r.get(0),
        )
        .expect("query chunk_run after reset");
    assert_eq!(
        remaining, 0,
        "--stuck-checkpoints must clear all in_progress chunk_runs; found {remaining}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn state_progression_shows_committed_boundary() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(100);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 50
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "state",
            "progression",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet state progression");

    assert!(
        result.status.success(),
        "state progression must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains(table.name()),
        "state progression must mention the export; got:\n{stdout}"
    );
    // After a completed chunked run, committed boundary is shown.
    assert!(
        stdout.contains("chunked") || stdout.contains("chunk"),
        "progression must show chunked mode committed boundary; got:\n{stdout}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// rivet metrics
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "live: requires docker compose postgres"]
fn metrics_shows_run_history() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["metrics", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet metrics");

    assert!(
        result.status.success(),
        "metrics must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains(table.name()),
        "metrics must show the export name; got:\n{stdout}"
    );
    assert!(
        stdout.contains("success"),
        "metrics must show run status; got:\n{stdout}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn metrics_last_flag_limits_output() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    // Run twice so --last 1 actually filters.
    run_export(&cfg, table.name(), &[]);
    run_export(&cfg, table.name(), &[]);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "metrics",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--last",
            "1",
        ])
        .output()
        .expect("spawn rivet metrics --last 1");

    assert!(
        result.status.success(),
        "metrics --last 1 must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    // The metrics output includes the run_id as the last column of each data row.
    // Use DB-sourced run_ids to verify exactly the newer run appears and the older is hidden.
    let run_ids = all_run_ids(&cfg, table.name());
    assert_eq!(
        run_ids.len(),
        2,
        "expected 2 run_journal entries; got {}",
        run_ids.len()
    );
    let (older_id, newer_id) = (&run_ids[0], &run_ids[1]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains(newer_id.as_str()),
        "metrics --last 1 must show the newer run_id ({newer_id}); got:\n{stdout}"
    );
    assert!(
        !stdout.contains(older_id.as_str()),
        "metrics --last 1 must hide the older run_id ({older_id}); got:\n{stdout}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// rivet journal
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "live: requires docker compose postgres"]
fn journal_shows_run_summary() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    run_export(&cfg, table.name(), &[]);

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
        "journal must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains("success"),
        "journal must show 'success' status; got:\n{stdout}"
    );
    assert!(
        stdout.contains(table.name()),
        "journal must show the export name; got:\n{stdout}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn journal_last_flag_limits_output() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    // Two runs so --last 1 actually filters one out.
    run_export(&cfg, table.name(), &[]);
    run_export(&cfg, table.name(), &[]);

    // Collect run_ids oldest-first so we can assert which one is excluded.
    let run_ids = all_run_ids(&cfg, table.name());
    assert_eq!(
        run_ids.len(),
        2,
        "expected 2 run_journal records; got {}",
        run_ids.len()
    );
    let (older_id, newer_id) = (&run_ids[0], &run_ids[1]);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "journal",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--last",
            "1",
        ])
        .output()
        .expect("spawn rivet journal --last 1");

    assert!(
        result.status.success(),
        "journal --last 1 must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // The newest run must appear; the older one must be excluded by --last 1.
    assert!(
        stdout.contains(newer_id.as_str()),
        "--last 1 must show the newest run_id ({newer_id}); got:\n{stdout}"
    );
    assert!(
        !stdout.contains(older_id.as_str()),
        "--last 1 must hide the older run_id ({older_id}); got:\n{stdout}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn journal_run_id_shows_specific_run() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_config(table.name(), out.path()));
    run_export(&cfg, table.name(), &[]);

    let run_id = last_run_id(&cfg, table.name());

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "journal",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--run-id",
            &run_id,
        ])
        .output()
        .expect("spawn rivet journal --run-id");

    assert!(
        result.status.success(),
        "journal --run-id must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains(&run_id),
        "journal --run-id output must mention the requested run_id; got:\n{stdout}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// MySQL coverage
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "live: requires docker compose mysql"]
fn doctor_exits_zero_when_mysql_source_reachable() {
    require_alive(LiveService::Mysql);

    let table = seed_mysql_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"

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
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["doctor", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet doctor (mysql)");

    assert!(
        result.status.success(),
        "doctor must exit 0 for reachable MySQL source; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains("[OK]"),
        "doctor output must contain '[OK]' for MySQL; got:\n{stdout}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn check_mysql_basic_exits_zero() {
    require_alive(LiveService::Mysql);

    let table = seed_mysql_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"

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
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet check (mysql)");

    assert!(
        result.status.success(),
        "check must exit 0 for MySQL int/varchar columns; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    // The type-report table always includes the export name in the "Export:" header line.
    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains(table.name()),
        "check output must mention the export name; got:\n{stdout}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// rivet completions
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "live: requires docker compose (any)"]
fn completions_bash_outputs_nonempty_script() {
    // completions doesn't need a live DB but the ignore gate ensures the stack
    // is up (so the test binary is in "live mode" context).
    require_alive(LiveService::Postgres);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["completions", "bash"])
        .output()
        .expect("spawn rivet completions bash");

    assert!(
        result.status.success(),
        "completions bash must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        !stdout.is_empty(),
        "completions bash must produce non-empty output"
    );
    // Bash completion scripts always start with a function definition.
    assert!(
        stdout.contains("rivet") || stdout.contains("_rivet"),
        "completions bash output must reference 'rivet'; got first 100 chars: {}",
        &stdout[..stdout.len().min(100)]
    );
}

// ─── OPT-6: subprocess fan-out engine crash coverage ─────────────────────────
//
// The in-process engines have a fault-injection crash matrix; the
// `--parallel-export-processes` engine had only a happy-path smoke test. These
// pin the two residual risks: (1) one child failing must not take down healthy
// siblings or leave a partial file, and (2) a hard crash must never push a
// footerless/partial Parquet to the destination (I1 + NamedTempFile).

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parallel_processes_one_child_failure_isolated_from_siblings() {
    require_alive(LiveService::Postgres);

    let good = seed_pg_numeric_table(10);
    let bad_name = unique_name("rivet_missing");
    let out_good = tempfile::tempdir().unwrap();
    let out_bad = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    // The second export queries a table that does not exist — its child fails at
    // run time (not parent plan time), exercising parent-side failure aggregation.
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {good}
    query: "SELECT id, name FROM {good}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {og}
  - name: {bad}
    query: "SELECT id FROM {bad}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {ob}
"#,
        good = good.name(),
        og = out_good.path().display(),
        bad = bad_name,
        ob = out_bad.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--parallel-export-processes",
            "--json",
        ])
        .output()
        .expect("spawn rivet run --parallel-export-processes");

    // A failed child must surface as a non-zero parent exit.
    assert!(
        !result.status.success(),
        "partial failure must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    // Isolation: the healthy export still produced its file.
    assert_eq!(
        files_with_extension(out_good.path(), "parquet").len(),
        1,
        "healthy export must complete despite a sibling child failing"
    );
    // The failed export left no partial file.
    assert!(
        files_with_extension(out_bad.path(), "parquet").is_empty(),
        "failed export must not leave a partial file at its destination"
    );
    // The aggregate JSON is still emitted and reports exactly one success.
    let stdout = String::from_utf8_lossy(&result.stdout);
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(stdout.trim()) {
        assert_eq!(
            json["parallel_mode"].as_str().unwrap_or(""),
            "parallel-processes"
        );
        assert_eq!(
            json["success_count"].as_i64().unwrap_or(-1),
            1,
            "exactly one export should succeed"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parallel_processes_hard_crash_writes_no_partial_file() {
    require_alive(LiveService::Postgres);

    let t1 = seed_pg_numeric_table(50);
    let t2 = seed_pg_numeric_table(50);
    let out1 = tempfile::tempdir().unwrap();
    let out2 = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {n1}
    query: "SELECT id, name FROM {n1}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {o1}
  - name: {n2}
    query: "SELECT id, name FROM {n2}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {o2}
"#,
        n1 = t1.name(),
        o1 = out1.path().display(),
        n2 = t2.name(),
        o2 = out2.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // `after_source_read` panics each child mid-export — after the rows are read
    // but before the writer is finalized and the file is copied to the
    // destination. The env is inherited by every spawned child.
    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--parallel-export-processes",
        ])
        .env("RIVET_TEST_PANIC_AT", "after_source_read")
        .output()
        .expect("spawn rivet run --parallel-export-processes");

    assert!(
        !result.status.success(),
        "a hard child crash must exit non-zero"
    );
    // I1 + NamedTempFile: the footerless/partial Parquet is written to a temp
    // file and only copied to the destination AFTER finalize — a crash before
    // that point must leave the destination empty (no corrupt file downstream).
    for dir in [out1.path(), out2.path()] {
        assert!(
            files_with_extension(dir, "parquet").is_empty(),
            "crashed export must leave no Parquet at the destination: {}",
            dir.display()
        );
    }
}
