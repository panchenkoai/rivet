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

/// Accept one HTTP POST on a loopback port; its body arrives on the channel.
fn one_shot_webhook() -> (String, std::sync::mpsc::Receiver<String>) {
    use std::io::{Read as _, Write as _};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let Ok((mut s, _)) = listener.accept() else {
            return;
        };
        s.set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        while let Ok(n) = s.read(&mut chunk) {
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&buf);
            if let Some((head, body)) = text.split_once("\r\n\r\n") {
                let want = head
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                if body.len() >= want {
                    break;
                }
            }
        }
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
        let text = String::from_utf8_lossy(&buf).into_owned();
        let _ = tx.send(
            text.split_once("\r\n\r\n")
                .map(|(_, b)| b.to_string())
                .unwrap_or_default(),
        );
    });
    (url, rx)
}

#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW)"]
fn a_failed_cdc_run_sends_the_failure_notification() {
    let tbl = unique_name("cdc_notify_my");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .and_then(|p| p.get_conn())
        .expect("connect mysql-cdc");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let (url, got) = one_shot_webhook();
    let rig = Rig::mysql_cdc(&tbl)
        .export_line(DRIFT_FAIL)
        .top_line(&format!(
            "notifications:\n  slack:\n    webhook_url: \"{url}\"\n    on: [failure]"
        ));
    rig.run_ok(); // a success sends nothing under `on: [failure]`
    c.query_drop(format!("ALTER TABLE {tbl} MODIFY v BIGINT"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 30000000000)"))
        .unwrap();

    assert_drift_refusal(&rig.run_expect_fail(), rig.export_name());
    let body = got
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("a failed CDC run must POST its failure notification to the configured webhook");
    let payload: serde_json::Value = serde_json::from_str(&body).expect("webhook body is JSON");
    let text = payload["attachments"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(
        text.contains(rig.export_name()) && text.contains("status: `failed`"),
        "the notification must name the export and its failed status: {text}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW)"]
fn a_cdc_export_names_the_batch_knobs_its_drain_ignores() {
    let tbl = unique_name("cdc_knobs_my");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .and_then(|p| p.get_conn())
        .expect("connect mysql-cdc");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl)
        .export_line("compression: gzip")
        .export_line("max_file_size: 256MB");
    let said = rig.run_ok_capture();
    assert!(
        said.contains("mode: cdc ignores compression, max_file_size on the change stream"),
        "the run must name the knobs the drain ignores:\n{said}"
    );
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    rig.run_ok();
    assert_eq!(
        duckdb_declared_dir_id_set(&rig.out_dir()),
        [1].into_iter().collect(),
        "the ignored knobs must not cost the stream a change"
    );
}
