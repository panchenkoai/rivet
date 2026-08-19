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

use crate::common::*;

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
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line(&format!("chunk_size: {CHUNK}"))
        .export_line("parallel: 1")
        .export_line("compression: none");
    let run = rig.run_args(&["--export", &export, "--reconcile"]);
    assert!(
        run.status.success(),
        "chunked --reconcile run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let db = StateDb::next_to_config(&rig.config_path());
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
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .source_url_env("DATABASE_URL")
        .export_line("compression: none");
    let plan_path = rig.config_path().with_file_name("plan.json");

    let plan = rig.plan_json_env(&plan_path, &[], &[("DATABASE_URL", POSTGRES_URL)]);
    assert!(
        plan.status.success(),
        "rivet plan must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan.stderr)
    );

    let apply = rig.apply_env(&plan_path, &[], &[("DATABASE_URL", POSTGRES_URL)]);
    assert!(
        apply.status.success(),
        "rivet apply must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&apply.stderr)
    );

    // apply opens state next to the original config dir (the rig owns it).
    let db = StateDb::next_to_config(&rig.config_path());
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
    let rig = harm_rig(Rig::pg_batch(table.name()), table.name(), &export);
    let run_id = run_and_latest_run_id(&rig, &export);

    assert_harm_contract(
        &StateDb::next_to_config(&rig.config_path()),
        &run_id,
        PG_HARM_COUNTERS,
    );

    // `table`, `out`, `cfg_dir` are guards bound for the whole function; Rust
    // keeps Drop types to scope end, so they outlive every read above.
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_run_persists_source_harm_rows() {
    require_alive(LiveService::Mysql);

    let table = seed_mysql_numeric_table(200);
    let export = unique_name("metrics_harm_mysql");
    let rig = harm_rig(Rig::mysql_batch(table.name()), table.name(), &export);
    let run_id = run_and_latest_run_id(&rig, &export);

    assert_harm_contract(
        &StateDb::next_to_config(&rig.config_path()),
        &run_id,
        MYSQL_HARM_COUNTERS,
    );

    // `table`, `out`, `cfg_dir` are guards bound for the whole function; Rust
    // keeps Drop types to scope end, so they outlive every read above.
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_run_persists_source_harm_rows() {
    require_alive(LiveService::Mssql);

    let table = seed_mssql_numeric_table(200);
    let export = unique_name("metrics_harm_mssql");
    // The harm probe reads sys.dm_os_wait_stats, which needs VIEW SERVER STATE;
    // the `sa` test login is sysadmin, so the LCK% aggregate always returns one
    // row and both counters persist (delta may be 0 with no contention).
    // (`Rig::mssql_batch` already declares the accept_invalid_certs TLS block.)
    let rig = harm_rig(Rig::mssql_batch(table.name()), table.name(), &export);
    let run_id = run_and_latest_run_id(&rig, &export);

    assert_harm_contract(
        &StateDb::next_to_config(&rig.config_path()),
        &run_id,
        MSSQL_HARM_COUNTERS,
    );

    // `table`, `out`, `cfg_dir` are guards bound for the whole function; Rust
    // keeps Drop types to scope end, so they outlive every read above.
}

// ── helpers ────────────────────────────────────────────────────────────────

/// One shape for every engine's harm test: the seeded table read back by a
/// simple query, full mode, no compression — through the canonical Rig rather
/// than the per-file YAML builder + raw binary invocation pair this file
/// carried (the exact smell the rig replaced; rig-adoption ratchet, 2026-08-19).
fn harm_rig(base: Rig, table: &str, export: &str) -> Rig {
    base.query(&format!("SELECT id, name FROM {table}"))
        .export_named(export)
        .export_line("compression: none")
}

/// Run the rig's single export and return the run_id the state DB recorded.
fn run_and_latest_run_id(rig: &Rig, export: &str) -> String {
    let run = rig.run_args(&["--export", export]);
    assert!(
        run.status.success(),
        "run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    StateDb::next_to_config(&rig.config_path()).latest_run_id(export)
}
