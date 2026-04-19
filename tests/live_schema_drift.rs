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

mod common;

use common::*;
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
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    # `SELECT *` so adding a column widens the schema observed by rivet.
    query: "SELECT * FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Run #1 — baseline schema (id, name).
    assert!(
        run_rivet_export(&cfg, &export_name).status.success(),
        "run 1"
    );

    // Mutate source: add a column.
    c.batch_execute(&format!(
        "ALTER TABLE {table_name} ADD COLUMN extra_col INT DEFAULT 42;"
    ))
    .unwrap();

    // Sleep so the run_id timestamp differs (state rows are keyed by run_id;
    // we want a second metric row).
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Run #2 — schema has drifted.  Must complete successfully.
    let r2 = run_rivet_export(&cfg, &export_name);
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
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT * FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    assert!(
        run_rivet_export(&cfg, &export_name).status.success(),
        "run 1"
    );

    c.batch_execute(&format!("ALTER TABLE {table_name} DROP COLUMN tmp_col;"))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));

    let r2 = run_rivet_export(&cfg, &export_name);
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
fn stable_schema_across_runs_reports_no_drift() {
    // Negative control: no schema change between runs → `schema_changed`
    // must be Some(false) (drift explicitly not detected), NOT None.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let export_name = unique_name("qa71_stable");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name, amount FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    assert!(run_rivet_export(&cfg, &export_name).status.success());
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert!(run_rivet_export(&cfg, &export_name).status.success());

    let state_db = cfg.parent().unwrap().join(".rivet_state.db");
    assert_eq!(
        latest_schema_changed(&state_db, &export_name),
        Some(Some(false)),
        "stable schema across runs must record schema_changed = Some(false), not None"
    );
}
