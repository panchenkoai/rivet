//! `on_schema_drift` on a CDC export: a column retyped between two runs must be
//! refused under `fail` before a single change is acknowledged, and captured in
//! full once the operator switches to `warn`. MongoDB is out of scope: its CDC
//! schema is the fixed document blob, so a field's type cannot drift a column.

use crate::common::*;
use mysql::prelude::Queryable as _;

const DRIFT_FAIL: &str = "on_schema_drift: fail";
const DRIFT_WARN: &str = "on_schema_drift: warn";

/// DuckDB over the declared parts: both deferred changes, as inserts, with their values.
fn duckdb_assert_deferred_changes_landed(out: &std::path::Path, second_v: i64) {
    assert_eq!(
        duckdb_declared_dir_id_set(out),
        [1, 2].into_iter().collect(),
        "both changes deferred by the refusal must be captured once drift is accepted"
    );
    assert_eq!(
        duckdb_declared_dir_scalar(out, "count(*) FILTER (WHERE __op = 'insert')"),
        2,
        "each deferred change lands exactly once, as an insert"
    );
    assert_eq!(
        duckdb_declared_dir_scalar(out, "sum(v)"),
        10 + second_v,
        "the deferred values must land intact through the widened type"
    );
}

/// The refusal names the export and the retyped column, and says how to accept it.
fn assert_drift_refusal(said: &str, export: &str) {
    assert!(
        said.contains(&format!("schema drift detected for export '{export}'")),
        "the run must name schema drift on its export:\n{said}"
    );
    assert!(
        said.contains("type changed: v"),
        "the refusal must name the retyped column `v`:\n{said}"
    );
    assert!(
        said.contains("set `on_schema_drift: warn` to accept"),
        "the refusal must name the knob that accepts the change:\n{said}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW)"]
fn mysql_cdc_retyped_column_refuses_under_fail_and_defers_not_drops() {
    let tbl = unique_name("cdc_drift_my");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .and_then(|p| p.get_conn())
        .expect("connect mysql-cdc");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let mut rig = Rig::mysql_cdc(&tbl).export_line(DRIFT_FAIL);
    rig.run_ok(); // anchors the stream and records the schema baseline

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    c.query_drop(format!("ALTER TABLE {tbl} MODIFY v BIGINT"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2, 30000000000)"))
        .unwrap();

    let ckpt_before = std::fs::read(rig.checkpoint()).ok();
    assert_drift_refusal(&rig.run_expect_fail(), rig.export_name());
    assert_eq!(
        std::fs::read(rig.checkpoint()).ok(),
        ckpt_before,
        "a refused run must not advance the checkpoint past changes it never wrote"
    );

    rig.replace_export_line("on_schema_drift", DRIFT_WARN);
    rig.run_ok();
    duckdb_assert_deferred_changes_landed(&rig.out_dir(), 30000000000);
}

#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical)"]
fn pg_cdc_retyped_column_refuses_under_fail_and_defers_not_drops() {
    use postgres::NoTls;
    let tbl = unique_name("cdc_drift_pg");
    let slot = unique_name("rivet_drift_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let mut rig = Rig::pg_cdc(&tbl, &slot).export_line(DRIFT_FAIL);
    rig.run_ok(); // creates the slot and records the schema baseline
    let _slot = Slot(slot.clone());

    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, 10); ALTER TABLE {tbl} ALTER COLUMN v TYPE BIGINT; \
         INSERT INTO {tbl} VALUES (2, 30000000000)"
    ))
    .unwrap();

    let flushed = |c: &mut postgres::Client| -> Option<String> {
        c.query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0)
    };
    let before = flushed(&mut c);
    assert_drift_refusal(&rig.run_expect_fail(), rig.export_name());
    assert_eq!(
        flushed(&mut c),
        before,
        "a refused run must not advance the slot past changes it never wrote"
    );

    rig.replace_export_line("on_schema_drift", DRIFT_WARN);
    rig.run_ok();
    duckdb_assert_deferred_changes_landed(&rig.out_dir(), 30000000000);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_retyped_column_refuses_under_fail_and_defers_not_drops() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("cdc_drift_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let mut rig = Rig::mssql_cdc(&table, &ci).export_line(DRIFT_FAIL);
    rig.run_ok(); // pins the anchor and records the schema baseline

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1, 10)"));
    mssql_cdc_exec(&format!("ALTER TABLE dbo.{table} ALTER COLUMN v BIGINT"));
    // The capture instance keeps `v` as INT, so the value must fit the old type.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (2, 20)"));
    wait_for_capture(&ci, 2);

    let ckpt_before = std::fs::read(rig.checkpoint()).ok();
    assert_drift_refusal(&rig.run_expect_fail(), rig.export_name());
    assert_eq!(
        std::fs::read(rig.checkpoint()).ok(),
        ckpt_before,
        "a refused run must not advance the checkpoint past changes it never wrote"
    );

    rig.replace_export_line("on_schema_drift", DRIFT_WARN);
    rig.run_ok();
    duckdb_assert_deferred_changes_landed(&rig.out_dir(), 20);
}
