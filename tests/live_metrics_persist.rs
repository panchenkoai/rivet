//! Live end-to-end: a real run persists the v9/v10 extended metric columns
//! *and* the Tier-2 source-harm rows — on BOTH the `run` and `apply` paths.
//!
//! The unit suite already proves the pieces in isolation —
//! `state::metrics::record_metric_full` maps a `MetricRow` to the right SQL
//! columns, `pipeline::job::build_metric_row` maps a summary/plan to the right
//! `MetricRow` fields, `harm_deltas` floors and intersects counter snapshots.
//! What no unit test can prove is the *whole chain* on the real run path:
//!
//!   * `build_metric_row` actually called with a live summary + plan (so the
//!     chunked dims, `longest_chunk_ms` from the journal, the reconcile
//!     verdict, and the PG `temp_bytes` delta are all populated, not defaulted);
//!   * the v9/v10 columns exist in a *freshly migrated* state DB (a missing
//!     `ALTER TABLE … ADD COLUMN` migration would make the wide INSERT fail
//!     here even though every unit test — which opens an already-migrated
//!     in-memory store — stays green);
//!   * the per-engine harm probe connects and its per-counter delta lands in
//!     `export_harm`.
//!
//! `run` and `apply` go through *separate* `build_metric_row` call sites
//! (`run_export_job` vs `run_export_job_with_chunk_source`), so both are
//! exercised — a regression in one wouldn't surface from the other.
//!
//! Sequential chunked (`parallel: 1`) is deliberate on the metric-columns run:
//! the sequential executor timestamps each `ChunkStarted`/`ChunkCompleted` as
//! it happens, so `longest_chunk_ms` is derivable (the parallel runner batches
//! completions post-scope and yields `None`).

mod common;

use common::*;
use std::process::Command;

/// The complete set of harm counters each engine's probe reads. The probe
/// queries a fixed column list and both snapshots see the same columns, so
/// `harm_deltas` emits every one — the recorded set must equal this exactly.
const PG_HARM_COUNTERS: &[&str] = &[
    "pg_blks_hit",
    "pg_blks_read",
    "pg_deadlocks",
    "pg_temp_files",
    "pg_tup_fetched",
    "pg_tup_returned",
];

const MYSQL_HARM_COUNTERS: &[&str] = &[
    "mysql_created_tmp_disk_tables",
    "mysql_handler_read_rnd_next",
    "mysql_innodb_buffer_pool_reads",
    "mysql_innodb_row_lock_time",
    "mysql_innodb_row_lock_waits",
    "mysql_innodb_rows_read",
];

const MSSQL_HARM_COUNTERS: &[&str] = &["mssql_lock_wait_ms", "mssql_lock_waits"];

/// Assert the harm rows for a run are exactly the engine's full counter set
/// (proves the probe read every column and the name mapping is complete — a
/// dropped or typo'd counter surfaces as a set mismatch), and that every delta
/// is floored at 0 (no counter persists a negative "harm").
fn assert_harm_contract(db: &StateDb, run_id: &str, expected: &[&str]) {
    let rows = db.harm_rows(run_id);
    let mut got: Vec<&str> = rows.iter().map(|(m, _)| m.as_str()).collect();
    got.sort_unstable();
    let mut want: Vec<&str> = expected.to_vec();
    want.sort_unstable();
    assert_eq!(
        got, want,
        "export_harm must record exactly the engine's full counter set; got {rows:?}"
    );
    for (metric, delta) in &rows {
        assert!(
            *delta >= 0,
            "{metric} delta must be floored at 0, got {delta}"
        );
    }
}

// ── run path: chunked + reconcile PG export ────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_chunked_run_persists_extended_metric_columns() {
    require_alive(LiveService::Postgres);

    const ROWS: i64 = 200;
    const CHUNK: i64 = 50;

    let table = seed_pg_numeric_table(ROWS);
    let export = unique_name("metrics_persist");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: {CHUNK}
    parallel: 1
    format: parquet
    compression: none
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
            "--reconcile",
        ])
        .output()
        .expect("spawn rivet run");
    assert!(
        run.status.success(),
        "chunked --reconcile run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let db = StateDb::next_to_config(&cfg);
    let run_id = db.latest_run_id(&export);
    let m = db.metrics_row(&run_id);

    // ── core verdict ──
    assert_eq!(m.status, "success", "a clean chunked run records success");
    assert_eq!(m.total_rows, Some(ROWS), "every seeded row is exported");
    // ── config dimensions (plan-derived) ──
    assert_eq!(m.source_type.as_deref(), Some("postgres"));
    assert_eq!(m.destination_type.as_deref(), Some("local"));
    assert_eq!(
        m.rivet_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "rivet_version must be the running build's version, not NULL"
    );
    // ── chunked dimensions — present only on the Chunked arm ──
    assert_eq!(
        m.chunk_size,
        Some(CHUNK),
        "chunk_size carries the plan value"
    );
    assert_eq!(m.parallel, Some(1));
    // ── per-run signals that only exist after a real export ──
    assert!(
        m.batch_size.is_some_and(|b| b > 0),
        "effective batch_size is recorded and positive"
    );
    assert!(
        m.files_committed.is_some_and(|f| f >= 1),
        "a committed parquet part is counted"
    );
    assert!(
        m.longest_chunk_ms.is_some_and(|ms| ms >= 0),
        "sequential chunked run yields a derivable longest_chunk_ms"
    );
    assert!(
        m.pg_temp_bytes_delta.is_some(),
        "pg_temp_bytes_delta is captured for a Postgres source"
    );
    // ── reconcile verdict (--reconcile on a static table) ──
    assert_eq!(
        m.source_count,
        Some(ROWS),
        "reconcile records the source COUNT(*)"
    );
    assert_eq!(
        m.reconciled,
        Some(true),
        "exported total matches source count ⇒ reconciled=true"
    );
    // ── schema fingerprint: the source-schema version recorded for the run ──
    assert!(
        m.schema_fingerprint
            .as_deref()
            .is_some_and(|f| f.starts_with("xxh3:")),
        "schema_fingerprint (xxh3 of the source schema) must be recorded after the run, got {:?}",
        m.schema_fingerprint
    );

    // `table`, `out`, `cfg_dir` are guards bound for the whole function; Rust
    // keeps Drop types to scope end, so they outlive every read above.
}

// ── apply path: plan → apply round-trip persists a metric row ───────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_apply_persists_metric_row() {
    require_alive(LiveService::Postgres);

    // The apply path uses a *different* build_metric_row call site than `run`.
    // `apply` rejects an inline-url plan (creds redact to REDACTED@…), so the
    // source must reference the URL via env (`url_env`) to stay re-resolvable.
    const ROWS: i64 = 30;
    let table = seed_pg_numeric_table(ROWS);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: {export}
    query: "SELECT id, name FROM {export}"
    mode: full
    format: parquet
    compression: none
    destination: {{type: local, path: {dir}}}
"#,
        export = table.name(),
        dir = out.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    let plan = Command::new(RIVET_BIN)
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
        plan.status.success(),
        "rivet plan must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan.stderr)
    );

    let apply = Command::new(RIVET_BIN)
        .args(["apply", plan_path.to_str().unwrap()])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet apply");
    assert!(
        apply.status.success(),
        "rivet apply must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&apply.stderr)
    );

    // apply opens state next to the original config dir (still present here).
    let db = StateDb::next_to_config(&cfg);
    let run_id = db.latest_run_id(table.name());
    let m = db.metrics_row(&run_id);

    // The apply path's build_metric_row ran and persisted the same shape.
    assert_eq!(m.status, "success");
    assert_eq!(m.total_rows, Some(ROWS));
    assert_eq!(m.source_type.as_deref(), Some("postgres"));
    assert_eq!(m.destination_type.as_deref(), Some("local"));
    assert_eq!(
        m.rivet_version.as_deref(),
        Some(env!("CARGO_PKG_VERSION")),
        "apply must stamp rivet_version too"
    );
    // full (non-chunked) export ⇒ no chunk dimensions (the `_ => None` arm).
    assert!(m.chunk_size.is_none(), "full export has no chunk_size");
    assert!(m.parallel.is_none(), "full export has no parallel");

    // `table`, `out`, `cfg_dir` are guards bound for the whole function; Rust
    // keeps Drop types to scope end, so they outlive every read above.
}

// ── harm path: per-engine source-harm rows ─────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_run_persists_source_harm_rows() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(200);
    let export = unique_name("metrics_harm_pg");
    let out = tempfile::tempdir().unwrap();
    let yaml = pg_full_yaml(&export, table.name(), out.path());
    let (cfg, _cfg_dir, run_id) = run_full_export_capture(&yaml, &export);

    assert_harm_contract(&StateDb::next_to_config(&cfg), &run_id, PG_HARM_COUNTERS);

    // `table`, `out`, `cfg_dir` are guards bound for the whole function; Rust
    // keeps Drop types to scope end, so they outlive every read above.
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_run_persists_source_harm_rows() {
    require_alive(LiveService::Mysql);

    let table = seed_mysql_numeric_table(200);
    let export = unique_name("metrics_harm_mysql");
    let out = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: full
    format: parquet
    compression: none
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display(),
    );
    let (cfg, _cfg_dir, run_id) = run_full_export_capture(&yaml, &export);

    assert_harm_contract(&StateDb::next_to_config(&cfg), &run_id, MYSQL_HARM_COUNTERS);

    // `table`, `out`, `cfg_dir` are guards bound for the whole function; Rust
    // keeps Drop types to scope end, so they outlive every read above.
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_run_persists_source_harm_rows() {
    require_alive(LiveService::Mssql);

    let table = seed_mssql_numeric_table(200);
    let export = unique_name("metrics_harm_mssql");
    let out = tempfile::tempdir().unwrap();
    // The harm probe reads sys.dm_os_wait_stats, which needs VIEW SERVER STATE;
    // the `sa` test login is sysadmin, so the LCK% aggregate always returns one
    // row and both counters persist (delta may be 0 with no contention).
    let yaml = format!(
        r#"source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: full
    format: parquet
    compression: none
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display(),
    );
    let (cfg, _cfg_dir, run_id) = run_full_export_capture(&yaml, &export);

    assert_harm_contract(&StateDb::next_to_config(&cfg), &run_id, MSSQL_HARM_COUNTERS);

    // `table`, `out`, `cfg_dir` are guards bound for the whole function; Rust
    // keeps Drop types to scope end, so they outlive every read above.
}

// ── helpers ────────────────────────────────────────────────────────────────

fn pg_full_yaml(export: &str, table: &str, out: &std::path::Path) -> String {
    format!(
        r#"source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, name FROM {table}"
    mode: full
    format: parquet
    compression: none
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.display(),
    )
}

/// Run a single export from an already-rendered `yaml` (no `--reconcile`) and
/// return `(cfg path, cfg TempDir guard, run_id)`. The caller owns the source
/// table guard and the destination TempDir.
fn run_full_export_capture(
    yaml: &str,
    export: &str,
) -> (std::path::PathBuf, tempfile::TempDir, String) {
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, yaml);
    let run = Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap(), "--export", export])
        .output()
        .expect("spawn rivet run");
    assert!(
        run.status.success(),
        "run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let run_id = StateDb::next_to_config(&cfg).latest_run_id(export);
    (cfg, cfg_dir, run_id)
}
