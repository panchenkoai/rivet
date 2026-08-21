//! Schema drift across rivet runs.
//!
//! QA backlog Task 7.1 + 7.2.  Run the same export against a Postgres table,
//! then mutate the source schema (add column / remove column / type change),
//! run again, and assert:
//!
//!   * the second run still succeeds (does not panic on schema delta),
//!   * rivet records the change in its schema-history store,
//!   * `schema_changed` surfaces as `Some(true)` in the run metric.
//!
//! Coupled unit coverage for the schema-diff algorithm itself lives in
//! `tests/schema_evolution.rs` (offline); this file proves the pipeline
//! calls it correctly under real source conditions.

use crate::common::*;
use postgres::NoTls;

struct PgCleanup(String);
impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

/// Read the `schema_changed` column for the most recent metric row of
/// `export_name`.  `None` = row not present; `Some(Some(true))` means rivet
/// detected drift on the latest run.
fn latest_schema_changed(state_db: &std::path::Path, export_name: &str) -> Option<Option<bool>> {
    let conn = rusqlite::Connection::open(state_db).ok()?;
    conn.query_row(
        "SELECT schema_changed FROM export_metrics \
         WHERE export_name = ?1 \
         ORDER BY id DESC LIMIT 1",
        [export_name],
        |row| {
            let v: Option<i64> = row.get(0)?;
            Ok(v.map(|n| n != 0))
        },
    )
    .ok()
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn schema_drift_added_column_is_detected_and_second_run_succeeds() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("qa71_add");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL
        );
        INSERT INTO {table_name} (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');"
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("qa71_add_exp");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT * FROM {table_name}"))
        .mode("full")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    // Run #1 — baseline schema (id, name).
    assert!(
        rig.run_args(&["--export", &export_name]).status.success(),
        "run 1"
    );

    // Mutate source: add a column.
    c.batch_execute(&format!(
        "ALTER TABLE {table_name} ADD COLUMN extra_col INT DEFAULT 42;"
    ))
    .unwrap();

    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).

    // Run #2 — schema has drifted.  Must complete successfully.
    let r2 = rig.run_args(&["--export", &export_name]);
    assert!(
        r2.status.success(),
        "second run must survive an added column; stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );

    // State DB lives next to config: `.rivet_state.db`.
    let state_db = cfg.parent().unwrap().join(".rivet_state.db");
    let changed = latest_schema_changed(&state_db, &export_name);
    assert_eq!(
        changed,
        Some(Some(true)),
        "schema_changed metric flag must be true after column addition"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn schema_drift_removed_column_is_detected() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("qa71_rm");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            tmp_col INT DEFAULT 0
        );
        INSERT INTO {table_name} (id, name) VALUES (1, 'a'), (2, 'b');"
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("qa71_rm_exp");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT * FROM {table_name}"))
        .mode("full")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    assert!(
        rig.run_args(&["--export", &export_name]).status.success(),
        "run 1"
    );

    c.batch_execute(&format!("ALTER TABLE {table_name} DROP COLUMN tmp_col;"))
        .unwrap();
    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).

    let r2 = rig.run_args(&["--export", &export_name]);
    assert!(
        r2.status.success(),
        "second run must survive a removed column; stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );

    let state_db = cfg.parent().unwrap().join(".rivet_state.db");
    assert_eq!(
        latest_schema_changed(&state_db, &export_name),
        Some(Some(true)),
        "schema_changed metric flag must be true after column removal"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_export_enforces_on_schema_drift_fail() {
    // run_keyset owns its own runner (run_single_export early-returns into it), so
    // the on_schema_drift gate single mode applies must be wired here too. Pre-fix
    // a keyset export — rivet's headline large-table path — with
    // `on_schema_drift: fail` returned exit 0 on a drifted schema: the opted-in
    // guardrail was silently absent. RED before the drift check in run_keyset.
    require_alive(LiveService::Postgres);
    let table_name = unique_name("keyset_drift");
    let mut c = pg_connect();
    // TEXT primary key → chunked mode auto-selects KEYSET (range chunking needs a
    // numeric key). `tmp_col` is dropped between runs to force structural drift.
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (k TEXT PRIMARY KEY, name TEXT NOT NULL, tmp_col INT DEFAULT 0);
         INSERT INTO {table_name} (k, name) VALUES ('a','x'), ('b','y'), ('c','z');"
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("keyset_drift_exp");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&table_name)
        .export_named(&export_name)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("chunk_size: 2")
        .export_line("on_schema_drift: fail")
        .dest_path(out.path().to_path_buf());

    assert!(
        rig.run_args(&["--export", &export_name]).status.success(),
        "run 1 (records the schema) must succeed"
    );

    c.batch_execute(&format!("ALTER TABLE {table_name} DROP COLUMN tmp_col;"))
        .unwrap();

    let r2 = rig.run_args(&["--export", &export_name]);
    assert!(
        !r2.status.success(),
        "a keyset export with `on_schema_drift: fail` must FAIL (exit 4) on a dropped \
         column — got success, so the drift gate is not enforced in the keyset runner.\n\
         stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
}

/// Sibling of the keyset case for the PARALLEL keyset runner (feat/parallel-keyset):
/// `parallel: N` returns through `run_keyset_parallel`, a SEPARATE runner that never
/// reached the sequential path's post-run drift gate. Pre-fix an opted-in
/// `on_schema_drift: fail` returned exit 0 on a drifted schema on the parallel path —
/// the runner-bypass class (a per-export gate wired into only some runners). RED
/// before the `check_from_sink_schema` call in `run_keyset_parallel`.
/// (runner-coverage-matrix schema_drift_gate: keyset-parallel cell.)
#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_parallel_export_enforces_on_schema_drift_fail() {
    require_alive(LiveService::Postgres);
    let table_name = unique_name("keyset_par_drift");
    let mut c = pg_connect();
    // TEXT PK → keyset; 20 rows so `parallel: 4` fans real ranges (each worker
    // non-empty). tmp_col dropped between runs to force structural drift.
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (k TEXT PRIMARY KEY, name TEXT NOT NULL, tmp_col INT DEFAULT 0);
         INSERT INTO {table_name} (k, name) \
         SELECT 'k' || lpad(g::text, 4, '0'), 'n' || g FROM generate_series(1, 20) g;"
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("keyset_par_drift_exp");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&table_name)
        .export_named(&export_name)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("parallel: 4")
        .export_line("chunk_size: 3")
        .export_line("on_schema_drift: fail")
        .dest_path(out.path().to_path_buf());

    assert!(
        rig.run_args(&["--export", &export_name]).status.success(),
        "run 1 (records the schema) must succeed"
    );

    c.batch_execute(&format!("ALTER TABLE {table_name} DROP COLUMN tmp_col;"))
        .unwrap();

    let r2 = rig.run_args(&["--export", &export_name]);
    assert!(
        !r2.status.success(),
        "a PARALLEL keyset export with `on_schema_drift: fail` must FAIL (exit 4) on a dropped \
         column — got success, so the drift gate is not enforced in the parallel keyset runner.\n\
         stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
}

/// Sibling of the keyset case for the CHUNKED (range) runner: an INTEGER
/// chunk_column routes to range chunking (chunked/exec.rs), a different runner.
/// `on_schema_drift: fail` must trip (exit 4) on a dropped column there too.
/// (runner-coverage-matrix schema_drift_gate: chunked-range cell.)
#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_range_export_enforces_on_schema_drift_fail() {
    require_alive(LiveService::Postgres);
    let table_name = unique_name("chunked_drift");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, name TEXT NOT NULL, tmp_col INT DEFAULT 0);
         INSERT INTO {table_name} (id, name) VALUES (1,'x'), (2,'y'), (3,'z'), (4,'w');"
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("chunked_drift_exp");
    let out = tempfile::tempdir().unwrap();
    // Integer chunk_column → RANGE chunking (the chunked runner, not keyset).
    let rig = Rig::pg_batch(&table_name)
        .export_named(&export_name)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 2")
        .export_line("on_schema_drift: fail")
        .dest_path(out.path().to_path_buf());

    assert!(
        rig.run_args(&["--export", &export_name]).status.success(),
        "run 1 (records the schema) must succeed"
    );
    c.batch_execute(&format!("ALTER TABLE {table_name} DROP COLUMN tmp_col;"))
        .unwrap();
    let r2 = rig.run_args(&["--export", &export_name]);
    assert!(
        !r2.status.success(),
        "a chunked (range) export with `on_schema_drift: fail` must FAIL on a dropped column; \
         stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn stable_schema_across_runs_reports_no_drift() {
    // Negative control: no schema change between runs → `schema_changed`
    // must be Some(false) (drift explicitly not detected), NOT None.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let export_name = unique_name("qa71_stable");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT id, name, amount FROM {}", table.name()))
        .mode("full")
        .export_line("columns:")
        .export_line("  amount: \"decimal(12,2)\"")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    assert!(rig.run_args(&["--export", &export_name]).status.success());
    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).
    assert!(rig.run_args(&["--export", &export_name]).status.success());

    let state_db = cfg.parent().unwrap().join(".rivet_state.db");
    assert_eq!(
        latest_schema_changed(&state_db, &export_name),
        Some(Some(false)),
        "stable schema across runs must record schema_changed = Some(false), not None"
    );
}

/// ADR-0028 bughunt (2026-08-21, MED): a run that FAILS must still RECORD what it
/// observed — the seam's records half (fingerprint pin + Form-B harvest) applies
/// on the failure path too; only the gates (drift policy, shape warn) are
/// success-only. Pre-fix the seam ran only on runner Ok, so a drift-FAIL keyset
/// run wrote a Failed manifest whose fingerprint fell back to the STALE open
/// baseline (actively wrong: the durable parquet carries the new schema) and
/// whose Form-B checksums — which mongo/keyset recorded pre-bail before the
/// seam — were silently dropped. "Recording first is the truthful thing": the
/// Failed manifest lists the durable debris, so it must describe it honestly.
///
/// Oracle is INDEPENDENT of the code under test: run 2's Failed manifest is
/// compared against run 1's Success manifest (two artifacts on disk) — the
/// fingerprint must DIFFER (run 2 observed the post-DROP schema) and the
/// Form-B checksums must be present (the data phase completed; only the gate
/// failed). RED pre-fix on both assertions.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn drift_failed_keyset_run_still_records_observed_fingerprint_and_form_b() {
    require_alive(LiveService::Postgres);
    let table_name = unique_name("keyset_drift_rec");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (k TEXT PRIMARY KEY, name TEXT NOT NULL, tmp_col INT DEFAULT 0);
         INSERT INTO {table_name} (k, name) VALUES ('a','x'), ('b','y'), ('c','z');"
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("keyset_drift_rec_exp");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&table_name)
        .export_named(&export_name)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("chunk_size: 2")
        .export_line("on_schema_drift: fail")
        .dest_path(out.path().to_path_buf());

    assert!(
        rig.run_args(&["--export", &export_name]).status.success(),
        "run 1 (records the schema baseline) must succeed"
    );
    let m1: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.path().join("manifest.json")).expect("run-1 manifest"),
    )
    .unwrap();
    let fp1 = m1["schema_fingerprint"]
        .as_str()
        .expect("run-1 fingerprint")
        .to_string();

    c.batch_execute(&format!("ALTER TABLE {table_name} DROP COLUMN tmp_col;"))
        .unwrap();

    let r2 = rig.run_args(&["--export", &export_name]);
    assert!(
        !r2.status.success(),
        "run 2 must FAIL on the drift gate (the fixture's premise)"
    );

    let m2: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.path().join("manifest.json")).expect("run-2 manifest"),
    )
    .unwrap();
    assert_eq!(
        m2["status"].as_str(),
        Some("failed"),
        "run 2 writes a Failed manifest"
    );

    // (a) the OBSERVED fingerprint, not the stale baseline: run 2 read the
    // post-DROP schema, so its manifest fingerprint must differ from run 1's.
    let fp2 = m2["schema_fingerprint"]
        .as_str()
        .expect("run-2 fingerprint");
    assert_ne!(
        fp2, fp1,
        "a drift-FAILED run's manifest must record the fingerprint it OBSERVED \
         (post-DROP), not fall back to the stale baseline run 1 stored — the \
         durable parts carry the new schema and the manifest must describe them"
    );

    // (b) Form-B survives the gate: the data phase completed before the gate
    // fired, so the Failed manifest must carry the run-wide column checksums.
    let checks = m2["column_checksums"].as_array();
    assert!(
        checks.is_some_and(|c| !c.is_empty()),
        "a drift-FAILED keyset run must still record Form-B checksums for its \
         durable parts (it did before ADR-0028 unified the tail); got: {:?}",
        m2["column_checksums"]
    );
}
