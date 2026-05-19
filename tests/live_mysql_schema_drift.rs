//! Schema drift across rivet runs — MySQL twin of `tests/live_schema_drift.rs`.
//!
//! Schema-diff detection lives in `state::shape` and is driver-agnostic; the
//! pipeline calls it from `Source::type_mappings` regardless of which driver
//! produced the column list. This file mirrors the PG schema-drift matrix
//! against MySQL so a regression of the shape comparator on either driver is
//! caught with parallel evidence.
//!
//! ## Test symmetry with `live_schema_drift.rs`
//!
//! | PG test                                                            | MySQL twin (this file)                                                   |
//! |--------------------------------------------------------------------|--------------------------------------------------------------------------|
//! | `schema_drift_added_column_is_detected_and_second_run_succeeds`    | `mysql_schema_drift_added_column_is_detected_and_second_run_succeeds`    |
//! | `schema_drift_removed_column_is_detected`                          | `mysql_schema_drift_removed_column_is_detected`                          |
//! | `stable_schema_across_runs_reports_no_drift`                       | `mysql_stable_schema_across_runs_reports_no_drift`                       |

mod common;

use common::*;
use mysql::prelude::Queryable;

struct MysqlCleanup(String);
impl Drop for MysqlCleanup {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

/// Read the `schema_changed` column for the most recent metric row of
/// `export_name`. `None` = row not present; `Some(Some(true))` means rivet
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
#[ignore = "live: requires docker compose mysql"]
fn mysql_schema_drift_added_column_is_detected_and_second_run_succeeds() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("qa71my_add");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        ) ENGINE=InnoDB;"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table_name} (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');"
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("qa71my_add_exp");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: mysql, url: "{MYSQL_URL}"}}
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

    assert!(
        run_rivet_export(&cfg, &export_name).status.success(),
        "run 1"
    );

    // Mutate source: add a column.
    c.query_drop(format!(
        "ALTER TABLE {table_name} ADD COLUMN extra_col INT DEFAULT 42;"
    ))
    .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(1100));

    let r2 = run_rivet_export(&cfg, &export_name);
    assert!(
        r2.status.success(),
        "second run must survive an added column; stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );

    let state_db = cfg.parent().unwrap().join(".rivet_state.db");
    let changed = latest_schema_changed(&state_db, &export_name);
    assert_eq!(
        changed,
        Some(Some(true)),
        "schema_changed metric flag must be true after column addition"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_schema_drift_removed_column_is_detected() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("qa71my_rm");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            tmp_col INT DEFAULT 0
        ) ENGINE=InnoDB;"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table_name} (id, name) VALUES (1, 'a'), (2, 'b');"
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("qa71my_rm_exp");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: mysql, url: "{MYSQL_URL}"}}
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

    c.query_drop(format!("ALTER TABLE {table_name} DROP COLUMN tmp_col;"))
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
#[ignore = "live: requires docker compose mysql"]
fn mysql_stable_schema_across_runs_reports_no_drift() {
    // Negative control: no schema change between runs → `schema_changed`
    // must be Some(false) (drift explicitly not detected), NOT None.
    require_alive(LiveService::Mysql);
    let table = seed_mysql_numeric_table(5);
    let export_name = unique_name("qa71my_stable");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: mysql, url: "{MYSQL_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name, amount FROM {table_name}"
    mode: full
    format: parquet
    columns:
      amount: "decimal(12,2)"
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
