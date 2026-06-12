//! ROAST: the small-table escape in `resolve_chunked_strategy` must not
//! override explicit chunk knobs.
//!
//! `mode: chunked` with the `table:` shortcut and no `chunk_column:` triggers
//! the introspection probe; when the `reltuples` estimate is at or below the
//! resolved `chunk_size`, the planner downgrades `Chunked → Snapshot`. That
//! heuristic is fine for a bare `mode: chunked`, but it currently fires BEFORE
//! the planner honors explicit user knobs:
//!
//!   * `chunk_count: 5` — the user asked for exactly 5 chunks; chunk_count
//!     semantics say `chunk_size` is recomputed at detect time from min/max,
//!     so comparing the estimate against the static default chunk_size (100k)
//!     is meaningless — yet the escape does exactly that and collapses the
//!     run to one snapshot file.
//!   * `chunk_checkpoint: true` — the user asked for a resumable run; a
//!     `Snapshot` has no checkpoint, so resumability is silently dropped.
//!
//! Explicit user intent must win over the stats heuristic. These tests seed a
//! small (500-row) Postgres table, ANALYZE it so `reltuples` is a meaningful
//! positive number (guaranteeing the escape is reachable — the RED must fail
//! because of the ESCAPE, not because of missing stats), run the real
//! `rivet plan` binary, and assert the resolved strategy in the plan artifact.

mod common;

use common::*;

/// Chunked-mode config using the `table:` shortcut with NO `chunk_column:` so
/// plan-build takes the introspection path where the small-table escape lives.
fn pg_chunked_table_config(table: &str, chunk_knob: &str, out_dir: &std::path::Path) -> String {
    format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {table}
    table: public.{table}
    mode: chunked
    {chunk_knob}
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

/// Seed a small canonical table and ANALYZE it so `pg_class.reltuples` holds a
/// real positive estimate (a freshly created table reports -1/0, which the
/// escape deliberately ignores).
fn seed_small_analyzed_table(rows: i64) -> PgTable {
    let table = seed_pg_numeric_table(rows);
    let mut c = pg_connect();
    c.batch_execute(&format!("ANALYZE {};", table.name()))
        .expect("ANALYZE seeded table");
    table
}

/// Run `rivet plan --format json --output …` for the export and return the
/// parsed plan artifact. Panics (setup failure, not the bug under test) if the
/// plan command itself fails.
fn run_plan(cfg_dir: &tempfile::TempDir, cfg: &std::path::Path, export: &str) -> serde_json::Value {
    let plan_path = cfg_dir.path().join("plan.json");
    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            export,
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
        "rivet plan must exit 0 (setup, not the bug under test); stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );
    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");
    serde_json::from_str(&plan_json).expect("plan.json must be valid JSON")
}

// ─── explicit chunk_count must survive the small-table escape ────────────────

#[test]
#[ignore = "live: postgres"]
fn roast_small_table_escape_respects_explicit_chunk_count() {
    require_alive(LiveService::Postgres);

    // 500 rows is far below the static default chunk_size (100k), so the
    // small-table escape fires today — even though `chunk_count: 5` means
    // chunk_size is recomputed at detect time (500 / 5 = 100-row chunks),
    // making the 100k-based comparison meaningless for this config.
    let table = seed_small_analyzed_table(500);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = pg_chunked_table_config(table.name(), "chunk_count: 5", out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);
    let plan = run_plan(&cfg_dir, &cfg, table.name());

    let strategy = plan["strategy"].as_str().unwrap_or("");
    assert_eq!(
        strategy, "chunked",
        "BUG (small-table escape vs chunk_count): explicit `chunk_count: 5` must win over \
         the small-table stats heuristic, but resolve_chunked_strategy downgraded the plan \
         to '{strategy}' — the requested 5 chunks were silently collapsed to one snapshot file"
    );
    assert_eq!(
        plan["resolved_plan"]["strategy"]["Chunked"]["chunk_count"].as_u64(),
        Some(5),
        "explicit chunk_count=5 must ride through the resolved plan; resolved strategy:\n{}",
        plan["resolved_plan"]["strategy"]
    );
}

// ─── explicit chunk_checkpoint must survive the small-table escape ───────────

#[test]
#[ignore = "live: postgres"]
fn roast_small_table_escape_respects_explicit_chunk_checkpoint() {
    require_alive(LiveService::Postgres);

    let table = seed_small_analyzed_table(500);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = pg_chunked_table_config(table.name(), "chunk_checkpoint: true", out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);
    let plan = run_plan(&cfg_dir, &cfg, table.name());

    let strategy = plan["strategy"].as_str().unwrap_or("");
    assert_eq!(
        strategy, "chunked",
        "BUG (small-table escape vs chunk_checkpoint): explicit `chunk_checkpoint: true` \
         requests a resumable chunked run, but resolve_chunked_strategy downgraded the plan \
         to '{strategy}' — a Snapshot has no checkpoint, so resumability was silently dropped"
    );
    assert_eq!(
        plan["resolved_plan"]["strategy"]["Chunked"]["checkpoint"].as_bool(),
        Some(true),
        "explicit chunk_checkpoint must ride through the resolved plan; resolved strategy:\n{}",
        plan["resolved_plan"]["strategy"]
    );
}
