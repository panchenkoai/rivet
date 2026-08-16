//! Live E2E tests for `rivet plan` + `rivet apply` against real databases.
//!
//! ## Contract coverage (ADR-0005)
//!
//! | ID | Scenario | Contract |
//! |---|---|---|
//! | PA-L1 | Plan + Apply full-mode round-trip | PA1, PA7 — artifact is comm channel; state writes |
//! | PA-L2 | Plan + Apply chunked round-trip | PA5 — chunk ranges pre-computed (outcome only) |
//! | PA-L2b | Apply after the source grew | PA5 — the artifact's ranges are REPLAYED, not re-detected |
//! | PA-L3 | Plan `--format pretty` prints summary to stdout | PA1 — no file written |
//! | PA-L4 | Plaintext URL credentials redacted in plan JSON | PA9 |
//! | PA-L5 | Expired plan (> 24 h) rejected without --force | PA3 |
//! | PA-L6 | `--force` bypasses 24 h staleness gate | PA3 |
//! | PA-L7 | Missing plan file → non-zero exit | PA1 |
//! | PA-L8 | `plan --param` substitutes query parameter | PA1 — artifact embeds params |
//! | PA-L9 | A degraded re-apply WARNs run-over-run | not ADR-0005 — the orchestrator-tail throughput self-check, which `apply` bypassed until round 7 |

use crate::common::*;

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Build a YAML config that uses `url_env: DATABASE_URL` (safe for plan/apply
/// round-trips — plaintext URL credentials would be redacted in the artifact
/// and cause apply to fail to reconnect).
fn pg_url_env_rig(table: &str, mode_block: &str) -> Rig {
    let mut r = Rig::pg_batch(table)
        .query(&format!("SELECT id, name FROM {table}"))
        .source_url_env("DATABASE_URL");
    for line in mode_block.lines().map(str::trim).filter(|l| !l.is_empty()) {
        r = match line.strip_prefix("mode:") {
            Some(m) => r.mode(m.trim()),
            None => r.export_line(line),
        };
    }
    r
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
    let cfg_dir = tempfile::tempdir().unwrap();

    let rig = pg_url_env_rig(table.name(), "mode: full");
    let plan_path = cfg_dir.path().join("plan.json");

    // ── rivet plan ────────────────────────────────────────────────────────────
    let plan_out = rig.cli_env(
        &[
            "plan",
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

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
    let apply_out = run_rivet_env(
        &["apply", plan_path.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

    assert!(
        apply_out.status.success(),
        "rivet apply must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&apply_out.stderr),
        String::from_utf8_lossy(&apply_out.stdout)
    );

    // Parquet file must exist in the output dir.
    let parquet = files_with_extension(&rig.out_dir(), "parquet");
    assert!(
        !parquet.is_empty(),
        "at least 1 parquet file must exist after apply; out_dir: {:?}",
        &rig.out_dir()
    );
    // Back the round-trip claim with an independent destination read.
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        30,
        "full plan+apply must land all 30 rows"
    );
}

// ─── PA-L2: chunked plan + apply — chunk_ranges pre-computed and replayed ─────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_and_apply_chunked_export_round_trip_uses_precomputed_ranges() {
    require_alive(LiveService::Postgres);

    // 150 rows / chunk_size 50 → exactly 3 chunks.
    let table = seed_pg_numeric_table(150);
    let cfg_dir = tempfile::tempdir().unwrap();

    let mode_block = "mode: chunked\n    chunk_column: id\n    chunk_size: 50".to_string();
    let rig = pg_url_env_rig(table.name(), &mode_block);
    let plan_path = cfg_dir.path().join("plan.json");

    // ── rivet plan ────────────────────────────────────────────────────────────
    let plan_out = rig.cli_env(
        &[
            "plan",
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

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
    let apply_out = run_rivet_env(
        &["apply", plan_path.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

    assert!(
        apply_out.status.success(),
        "rivet apply (chunked) must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&apply_out.stderr),
        String::from_utf8_lossy(&apply_out.stdout)
    );

    // 3 chunks → 3 parquet files.
    let parquet = files_with_extension(&rig.out_dir(), "parquet");
    assert_eq!(
        parquet.len(),
        3,
        "3 chunks must produce 3 parquet files; found: {parquet:?}"
    );
    // Back the chunked round-trip with an independent destination read: all 150
    // rows across the 3 chunks, distinct ids 0..150 (no drop/dup at boundaries).
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        150,
        "3 chunks must land all 150 rows"
    );
    assert_eq!(
        duckdb_dir_parquet_id_set(&rig.out_dir()),
        (0..150).collect::<std::collections::BTreeSet<i64>>(),
        "chunked round-trip must hold every source id 0..150"
    );
}

// ─── PA-L2b: apply REPLAYS the artifact's ranges — it does not re-detect ──────

/// The discriminating half of PA5, which PA-L2 above cannot express.
///
/// PA-L2 plans 150 rows at `chunk_size: 50`, applies, and asserts 3 files /
/// 150 rows / ids 0..150. Every one of those assertions is satisfied *equally*
/// by replaying the artifact's ranges and by re-detecting them from the live
/// source: the fixture is unchanged between plan and apply, so `SELECT
/// min(id), max(id)` reproduces exactly the windows the artifact already holds.
/// The test is named `..._uses_precomputed_ranges` and its contract table says
/// "pre-computed and replayed", but it passed for months while `rivet apply`
/// re-detected on the sequential path — the name described an intention the
/// code did not have.
///
/// To tell the two apart the source must CHANGE between plan and apply, so the
/// planned windows and the live ones disagree:
///
///   plan  → 150 rows (ids 0..149), chunk_size 50 → 3 ranges in the artifact
///   then  → insert ids 150..299
///   apply → replay: 3 files, 150 rows (the PLAN's windows)
///           re-detect: min/max is now 0..299 → 6 windows → 6 files, 300 rows
///
/// Ignoring rows added after planning is the POINT of `apply`, not a defect:
/// the artifact is the unit of execution, which is why staleness is warned at
/// 1 h and refused at 24 h rather than silently re-planned.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn apply_replays_the_artifacts_ranges_and_ignores_rows_added_after_planning() {
    // Both chunked runners that reach `run_export`. Measured against the released
    // 0.23.1 binary, BOTH re-detected — plain chunked and chunk_checkpoint alike
    // produced 6 files / 300 rows where the artifact pinned 3 windows / 150 rows.
    // (`parallel:` takes a different dispatch tier and already replayed.) One case
    // would leave the other free to regress alone.
    for extra in ["", "\n    chunk_checkpoint: true"] {
        apply_replay_case(extra);
    }
}

fn apply_replay_case(extra_mode_lines: &str) {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(150);
    let cfg_dir = tempfile::tempdir().unwrap();

    let mode_block =
        format!("mode: chunked\n    chunk_column: id\n    chunk_size: 50{extra_mode_lines}");
    let rig = pg_url_env_rig(table.name(), &mode_block);
    let plan_path = cfg_dir.path().join("plan.json");

    let plan_out = rig.cli_env(
        &[
            "plan",
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );
    assert!(
        plan_out.status.success(),
        "rivet plan (chunked) must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );

    let plan: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&plan_path).expect("read plan.json"))
            .unwrap();
    assert_eq!(
        plan["computed"]["chunk_ranges"]
            .as_array()
            .expect("chunk_ranges")
            .len(),
        3,
        "the artifact must carry the 3 windows planned against 150 rows"
    );

    // The source moves on AFTER the plan was written.
    {
        let mut c = pg_connect();
        let mut sql = format!(
            "INSERT INTO {} (id, name, amount, created_at) VALUES ",
            table.name()
        );
        for i in 150..300i64 {
            if i > 150 {
                sql.push(',');
            }
            sql.push_str(&format!("({i}, 'r{i}', {i}.00, now())"));
        }
        c.batch_execute(&sql).expect("insert rows after planning");
    }

    let apply_out = run_rivet_env(
        &["apply", plan_path.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );
    assert!(
        apply_out.status.success(),
        "rivet apply (chunked) must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&apply_out.stderr)
    );

    let parquet = files_with_extension(&rig.out_dir(), "parquet");
    assert_eq!(
        parquet.len(),
        3,
        "apply must execute the artifact's 3 windows, not re-detect 6 from the \
         grown table; found: {parquet:?}"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        150,
        "apply must move only the rows its plan covered"
    );
    assert_eq!(
        duckdb_dir_parquet_id_set(&rig.out_dir()),
        (0..150).collect::<std::collections::BTreeSet<i64>>(),
        "the ids must be exactly the planned range — rows added after planning \
         belong to the next run, not this artifact"
    );
}

// ─── PA-L3: plan --format pretty prints human-readable summary to stdout ──────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_pretty_format_prints_summary_to_stdout() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let cfg_dir = tempfile::tempdir().unwrap();

    let rig = pg_url_env_rig(table.name(), "mode: full");

    let plan_out = rig.cli_env(
        &["plan", "--export", table.name(), "--format", "pretty"],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

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
    let cfg_dir = tempfile::tempdir().unwrap();

    // Config uses a plaintext URL with embedded password.
    // The POSTGRES_URL is "postgresql://rivet:rivet@127.0.0.1:5432/rivet".
    // Plaintext URL ON PURPOSE: this test asserts the credential is redacted
    // into the plan artifact, so it must NOT use url_env.
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, name FROM {name}", name = table.name()))
        .mode("full");
    let plan_path = cfg_dir.path().join("plan.json");

    let plan_out = rig.cli_env(
        &[
            "plan",
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[],
    );

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
    let cfg_dir = tempfile::tempdir().unwrap();

    let rig = pg_url_env_rig(table.name(), "mode: full");
    let plan_path = cfg_dir.path().join("plan.json");

    // Generate a fresh plan.
    let plan_out = rig.cli_env(
        &[
            "plan",
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );
    assert!(plan_out.status.success(), "plan generation must succeed");

    // Patch created_at to make the plan > 24 h old.
    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");
    let stale_json = make_stale_plan(&plan_json);
    let stale_path = cfg_dir.path().join("stale_plan.json");
    std::fs::write(&stale_path, &stale_json).expect("write stale plan");

    // Apply without --force must reject.
    let apply_out = run_rivet_env(
        &["apply", stale_path.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

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
    let cfg_dir = tempfile::tempdir().unwrap();

    let rig = pg_url_env_rig(table.name(), "mode: full");
    let plan_path = cfg_dir.path().join("plan.json");

    // Generate plan.
    let plan_out = rig.cli_env(
        &[
            "plan",
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );
    assert!(plan_out.status.success(), "plan generation must succeed");

    // Patch to make it stale.
    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");
    let stale_json = make_stale_plan(&plan_json);
    let stale_path = cfg_dir.path().join("stale_plan.json");
    std::fs::write(&stale_path, &stale_json).expect("write stale plan");

    // Apply WITH --force must succeed despite staleness.
    let apply_out = run_rivet_env(
        &["apply", "--force", stale_path.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

    assert!(
        apply_out.status.success(),
        "rivet apply --force on a stale plan must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&apply_out.stderr),
        String::from_utf8_lossy(&apply_out.stdout)
    );

    // Data must actually be exported.
    let parquet = files_with_extension(&rig.out_dir(), "parquet");
    assert!(
        !parquet.is_empty(),
        "parquet file must exist after forced apply; out_dir: {:?}",
        &rig.out_dir()
    );
    // "Data must actually be exported" — back it with a destination read.
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        10,
        "forced apply must still land all 10 rows"
    );
}

// ─── PA-L8: plan --param substitutes query parameter ─────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_param_flag_substitutes_in_query() {
    require_alive(LiveService::Postgres);

    // Seed 40 rows (ids 0–39); plan with param max_id=19 → 20 rows.
    let table = seed_pg_numeric_table(40);
    let cfg_dir = tempfile::tempdir().unwrap();

    // `${max_id}` stays escaped as `${{max_id}}` inside the format! — it is a
    // rivet PARAMETER placeholder that must survive into the YAML, not a Rust
    // interpolation.
    let rig = pg_url_env_rig(table.name(), "mode: full").query(&format!(
        "SELECT id, name FROM {name} WHERE id <= ${{max_id}}",
        name = table.name()
    ));
    let plan_path = cfg_dir.path().join("plan_param.json");

    let plan_out = rig.cli_env(
        &[
            "plan",
            "--export",
            table.name(),
            "--param",
            "max_id=19",
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

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
    let apply_out = run_rivet_env(
        &["apply", plan_path.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

    assert!(
        apply_out.status.success(),
        "apply of param plan must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&apply_out.stderr),
        String::from_utf8_lossy(&apply_out.stdout)
    );

    let parquet_files = files_with_extension(&rig.out_dir(), "parquet");
    assert!(
        !parquet_files.is_empty(),
        "at least one parquet file must exist after apply"
    );

    // Verify exactly 20 rows (ids 0..=19) were written: query the state DB.
    // total_rows lives in export_metrics, not run_journal (journal stores a JSON blob).
    // The state DB lives NEXT TO THE CONFIG, and the config is now the rig's.
    let db_path = rig.config_path().parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open state db after apply");
    let rows_written: i64 = conn
        .query_row(
            "SELECT total_rows FROM export_metrics \
             WHERE export_name = ?1 ORDER BY run_at DESC LIMIT 1",
            [table.name()],
            |r| r.get(0),
        )
        .expect("query total_rows from export_metrics");
    assert_eq!(
        rows_written, 20,
        "plan --param max_id=19 must export exactly 20 rows (ids 0..=19); got {rows_written}"
    );
    // Back the export_metrics count with an independent destination read.
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        20,
        "param max_id=19 must physically land exactly 20 rows at the destination"
    );
}

// ─── PA-L7: missing plan file → non-zero exit (PA1) ──────────────────────────

#[test]
#[ignore = "live: requires docker compose (any)"]
fn apply_missing_plan_file_exits_nonzero() {
    require_alive(LiveService::Postgres);

    let apply_out = run_rivet_env(&["apply", "/tmp/rivet_nonexistent_plan_xyz.json"], &[]);

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

// ─── the run-over-run throughput self-check, from a real `rivet apply` ───────

/// One `export_metrics` success row, projected to exactly the four columns the
/// throughput self-check reads (`aggregate::warn_throughput_regressions` →
/// `ThroughputPair`). Read from the state DB the run itself wrote, so the
/// activation facts below come from the REAL producer rather than from the
/// test's own arithmetic about what it thinks it seeded.
#[derive(Debug)]
struct TputRow {
    run_id: String,
    total_rows: i64,
    duration_ms: i64,
    mode: String,
}

/// Every SUCCESS row for `export`, oldest first — the same order (`id ASC`) the
/// self-check's `ORDER BY id DESC LIMIT 1` baseline lookup walks backwards.
fn tput_success_rows(state_db: &std::path::Path, export: &str) -> Vec<TputRow> {
    let conn = rusqlite::Connection::open(state_db)
        .unwrap_or_else(|e| panic!("open state db {}: {e}", state_db.display()));
    let mut stmt = conn
        .prepare(
            "SELECT run_id, total_rows, duration_ms, mode FROM export_metrics \
             WHERE export_name = ?1 AND status = 'success' ORDER BY id ASC",
        )
        .expect("prepare export_metrics read");
    let rows = stmt
        .query_map([export], |r| {
            Ok(TputRow {
                run_id: r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                total_rows: r.get(1)?,
                duration_ms: r.get(2)?,
                mode: r.get::<_, Option<String>>(3)?.unwrap_or_default(),
            })
        })
        .expect("query export_metrics");
    rows.map(|r| r.expect("export_metrics row")).collect()
}

/// `rivet apply <plan.json>` must EMIT the run-over-run throughput regression
/// warning when the same export degrades — the value-level half of the round-7
/// finding.
///
/// ## The debt this closes
///
/// The JSON plan-artifact arm of `run_apply_command` is a full orchestrator —
/// it opens the state store, drives one export to completion and writes its
/// `export_metrics` row — but it never reached the run-over-run self-check, so a
/// real degradation exited 0 in silence AND the degraded row then became the
/// next run's baseline, hiding the regression a second time. `apply_cmd.rs` now
/// calls `run::self_check_throughput(…, RunModes::uniform(run_mode_label(1,
/// false)))`, and a derived guard in `run.rs`
/// (`every_orchestrator_tail_routes_the_self_check_through_one_seam`) pins the
/// CALL SITE. Nothing pinned the OUTPUT: a call site that reaches a check whose
/// pair is always refused emits nothing, and the guard stays green. This is the
/// test that watches the warning come out of the real binary.
///
/// ## Why the fixture sleeps — and why that is NOT the forbidden sleep
///
/// CLAUDE.md forbids "a sleep in a test that compensates for PRODUCT behaviour"
/// and REQUIRES that "fixtures must cross the mechanism's activation threshold".
/// This is the second one. The self-check DELIBERATELY refuses pairs that are
/// too small or too short to mean anything (`aggregate::incomparable`):
///
/// * `REGRESSION_MIN_ROWS = 10_000` — both sides
/// * `REGRESSION_MIN_MS   = 5_000`  — both sides
/// * `REGRESSION_MAX_SCALE = 2`     — neither side may exceed the other 2×
/// * `prev_mode != cur_mode`        — a mode switch is not a slowdown
/// * warns at `prev_tp / cur_tp >= REGRESSION_RATIO = 1.5`
///
/// A fixture that does not cross those floors cannot state the behaviour at all.
/// The naive way to cross them — export enough real rows to take >5 s — makes
/// the wall time a property of the MACHINE, so a fast host silently drops under
/// the 5 s floor and the test goes green having checked nothing. So the wall
/// time comes from `pg_sleep` on the SOURCE instead: a cross join against a
/// one-row sleeping subquery pins the duration on any hardware while 20 000 real
/// rows still move. Run 1 sleeps 6 s, run 2 sleeps 14 s over the SAME row count
/// — no scale mismatch, both past the 5 s floor, ~3.1K → ~1.4K rows/s, ratio
/// ~2.2. Do NOT "fix" the sleeps away: they are the fixture crossing a
/// documented activation threshold, not a workaround for rivet's own timing.
///
/// Both runs are `rivet apply <plan.json>`, so both record `mode: full` and the
/// same `sequential` concurrency label. The two artifacts differ ONLY in the
/// substituted `${sleep_secs}` — the self-check compares by export name against
/// the previous success and neither knows nor cares WHY the export got slower,
/// which is exactly the field scenario (a source that degraded).
///
/// ## Non-vacuity
///
/// A test that goes green on a SKIPPED pair is the defect this repo forbids, so
/// the activation facts are asserted first, and from the real producer: both
/// runs' `export_metrics` rows must clear every floor above (rows, duration,
/// scale, matching mode, distinct run_ids, ratio ≥ 1.5) BEFORE the warning is
/// asserted. Shrink the fixture and this test fails on the activation assertion
/// — never by passing quietly. The fast run is also asserted SILENT: with no
/// prior success it has no baseline, so the line in run 2 is provably the
/// run-over-run comparison firing and not something unconditional.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn apply_warns_when_the_same_export_degrades_run_over_run() {
    require_alive(LiveService::Postgres);

    // No seeded table: `generate_series` supplies the rows and `pg_sleep`
    // supplies the wall time, so the fixture owns both of the self-check's
    // floors and needs nothing from the shared fixture DB.
    let export = unique_name("tput_apply");
    let rig = Rig::pg_batch(&export).source_url_env("DATABASE_URL").query(
        "SELECT g AS id, repeat('x', 100) AS payload \
             FROM generate_series(1, 20000) g, (SELECT pg_sleep(${sleep_secs})) AS _s",
    );

    let plan_dir = tempfile::tempdir().unwrap();
    let plan_fast = plan_dir.path().join("plan_fast.json");
    let plan_slow = plan_dir.path().join("plan_slow.json");
    let envs = [("DATABASE_URL", POSTGRES_URL)];

    for (path, secs) in [(&plan_fast, 6), (&plan_slow, 14)] {
        let param = format!("sleep_secs={secs}");
        let out = rig.plan_json_env(path, &["--param", &param], &envs);
        assert!(
            out.status.success(),
            "rivet plan (sleep {secs}s) must exit 0; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Run 1: the baseline. Run 2: the SAME export, 2.3× slower per row.
    let fast = rig.apply_env(&plan_fast, &[], &envs);
    assert!(
        fast.status.success(),
        "apply (baseline) must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&fast.stderr)
    );
    let slow = rig.apply_env(&plan_slow, &[], &envs);
    assert!(
        slow.status.success(),
        "apply (degraded) must exit 0 — a throughput regression WARNS, it does not \
         fail the run; stderr:\n{}",
        String::from_utf8_lossy(&slow.stderr)
    );

    // ── activation: the pair the self-check saw must be a COMPARABLE one ──────
    // Read from the state DB the two applies wrote (state lives next to the
    // CONFIG — the artifact records its path — so it is the rig's dir).
    let state_db = rig.config_path().parent().unwrap().join(".rivet_state.db");
    let rows = tput_success_rows(&state_db, &export);
    assert_eq!(
        rows.len(),
        2,
        "both applies must have recorded a success row for '{export}'; got: {rows:?}"
    );
    let (base, degraded) = (&rows[0], &rows[1]);
    assert!(
        base.duration_ms < degraded.duration_ms,
        "fixture ordering: run 1 is the FAST baseline and run 2 the degraded one; got {rows:?}"
    );
    assert_ne!(
        base.run_id, degraded.run_id,
        "the baseline lookup excludes the current run_id — identical ids would leave \
         the degraded run with no baseline at all: {rows:?}"
    );
    for r in &rows {
        assert!(
            r.total_rows >= 10_000,
            "REGRESSION_MIN_ROWS: both sides need ≥ 10_000 rows or the pair is refused \
             as TooFewRows and this test proves nothing; got {r:?}"
        );
        assert!(
            r.duration_ms >= 5_000,
            "REGRESSION_MIN_MS: both sides need ≥ 5_000 ms or the pair is refused as \
             TooShort — this is exactly the floor the pg_sleep fixture exists to cross; \
             got {r:?}"
        );
    }
    assert!(
        base.total_rows <= degraded.total_rows * 2 && degraded.total_rows <= base.total_rows * 2,
        "REGRESSION_MAX_SCALE: the two runs must move comparable row counts or the pair \
         is refused as ScaleMismatch; got {rows:?}"
    );
    assert_eq!(
        base.mode, degraded.mode,
        "a mode switch is refused as ModeChanged — both applies must record the same \
         export mode; got {rows:?}"
    );
    let ratio = (base.total_rows as f64 / base.duration_ms as f64)
        / (degraded.total_rows as f64 / degraded.duration_ms as f64);
    assert!(
        ratio >= 1.5,
        "REGRESSION_RATIO: the fixture must degrade throughput ≥ 1.5× or there is \
         nothing to warn about; measured {ratio:.2}× from {rows:?}"
    );

    // ── the warning itself, out of the degraded run's stderr ──────────────────
    let stderr = String::from_utf8_lossy(&slow.stderr);
    let needle = format!("export '{export}': throughput ");
    let line = stderr
        .lines()
        .find(|l| l.contains(&needle))
        .unwrap_or_else(|| {
            panic!(
                "`rivet apply` must WARN that '{export}' degraded {ratio:.1}× run-over-run \
             (rows/durations: {rows:?}) — the apply tail reached no self-check. \
             stderr:\n{stderr}"
            )
        });
    assert!(
        line.contains("slower than its last success"),
        "the warning must name the run-over-run comparison, not a bare rate: {line}"
    );
    // The mode `apply_cmd` passes is `run_mode_label(1, false)` = "sequential":
    // a sealed single-export replay overlaps nothing, so the line must keep its
    // ACTIONABLE tail rather than excusing the drop as source sharing.
    assert!(
        line.contains("check governor sheds"),
        "a sealed single-export apply shares the source with nothing — the warning must \
         keep the actionable tail, not the concurrent-run excuse: {line}"
    );
    // The printed ratio must agree with the metrics rows it was derived from.
    // (Same inputs, so this grades the derivation/formatting, not the values.)
    let printed: f64 = line
        .split('(')
        .nth(1)
        .and_then(|s| s.split('×').next())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or_else(|| panic!("the warning must print a ratio: {line}"));
    assert!(
        (printed - ratio).abs() < 0.2,
        "the printed ratio {printed:.1}× must match the metrics rows ({ratio:.2}×): {line}"
    );

    // The baseline run must be SILENT — it had no previous success to compare
    // against, so the line above is provably the run-over-run comparison.
    let fast_stderr = String::from_utf8_lossy(&fast.stderr);
    assert!(
        !fast_stderr.contains("slower than its last success"),
        "the first apply has no baseline and must not warn; stderr:\n{fast_stderr}"
    );
}

/// Round-3/4: an INCREMENTAL keyset export through the `rivet apply` wrapper must
/// clear its resume anchor at finalize — the SAME post-finalize clear `rivet run`
/// does (`finalize_keyset_anchor`) — so the run_id ROTATES across repeated applies.
/// Before the two-adapter seam, the apply wrapper forgot the clear: the run_id
/// froze, both applies wrote the SAME `manifest-<run_id>.json` copy (collision),
/// and a run_id-deduping loader silently skips the second delta. This is the
/// apply-wrapper half of the seam, RED-proven on the run-unique manifest COPIES —
/// a parquet glob cannot see a run_id collision. Applies share `.rivet_state.db`
/// next to the plan (apply_cmd.rs), so the resume anchor persists across them.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn incremental_keyset_apply_rotates_run_id_across_repeated_applies() {
    require_alive(LiveService::Postgres);
    let table = unique_name("ks_apply_incr");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (k TEXT PRIMARY KEY, name TEXT NOT NULL); \
         INSERT INTO {table} SELECT 'k' || lpad(g::text, 6, '0'), 'n' || g \
         FROM generate_series(1, 100) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());

    let cfg_dir = tempfile::tempdir().unwrap();
    // `table:` rather than `query:` — the keyset planner needs the shortcut form.
    let rig = Rig::pg_batch(&table)
        .source_url_env("DATABASE_URL")
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 40");
    let plan_path = cfg_dir.path().join("plan.json");

    let plan = rig.cli_env(
        &[
            "plan",
            "--export",
            &table,
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );
    assert!(
        plan.status.success(),
        "plan stderr:\n{}",
        String::from_utf8_lossy(&plan.stderr)
    );

    let apply = |label: &str| {
        let out = run_rivet_env(
            &["apply", plan_path.to_str().unwrap()],
            &[("DATABASE_URL", POSTGRES_URL)],
        );
        assert!(
            out.status.success(),
            "{label} stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };

    // Apply 1: exports all 100 under run_id R1; finalize must clear the anchor.
    apply("apply 1");
    // An append-only batch so apply 2 has data and writes its own manifest.
    c.batch_execute(&format!(
        "INSERT INTO {table} SELECT 'k' || lpad(g::text, 6, '0'), 'n' || g \
         FROM generate_series(101, 150) g;"
    ))
    .unwrap();
    // Apply 2: with the anchor cleared it is a FRESH run (new run_id R2) pulling
    // only the new keys. With the bug the anchor is frozen → R1 reused → one copy.
    apply("apply 2");

    assert_eq!(
        dir_manifest_copy_count(&rig.out_dir()),
        2,
        "each apply must rotate the run_id → two distinct manifest-<run_id>.json copies; 1 means the apply wrapper left the resume anchor frozen (the run_id-collision silent-delta-skip class)"
    );
}
