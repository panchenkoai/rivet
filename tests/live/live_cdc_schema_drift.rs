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
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let mut rig = Rig::pg_cdc(&tbl, &slot).export_line(DRIFT_FAIL);
    rig.run_ok(); // creates the slot and records the schema baseline

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

// ── CDC drain: harm bracket + open forensics ────────────────────────────────

/// A MySQL CDC run records the source-harm counters of its own window, and a run
/// that FAILS (schema drift under `fail`) still records the server it ran against.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW)"]
fn mysql_cdc_run_persists_source_harm_and_server_context_even_when_it_fails() {
    let tbl = unique_name("cdc_harm_my");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .and_then(|p| p.get_conn())
        .expect("connect mysql-cdc");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl).export_line("on_schema_drift: fail");
    rig.run_ok(); // anchor
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    rig.run_ok();
    assert_eq!(
        duckdb_declared_dir_id_set(&rig.out_dir()),
        [1].into_iter().collect(),
        "the captured change lands"
    );
    let db = StateDb::next_to_config(&rig.config_path());
    super::live_metrics_persist::assert_harm_contract(
        &db,
        &db.latest_run_id(rig.export_name()),
        super::live_metrics_persist::MYSQL_HARM_COUNTERS,
    );

    c.query_drop(format!("ALTER TABLE {tbl} MODIFY v BIGINT"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2, 30000000000)"))
        .unwrap();
    rig.run_expect_fail();
    let failed = db.latest_run_id(rig.export_name());
    let conn =
        rusqlite::Connection::open(rig.config_path().parent().unwrap().join(".rivet_state.db"))
            .expect("open state db");
    let (status, server_ctx): (String, Option<String>) = conn
        .query_row(
            "SELECT status, server_context_json FROM export_metrics WHERE run_id = ?1",
            [&failed],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("the failed CDC run has a metrics row");
    assert_eq!(
        status, "failed",
        "fixture: the drift refusal must fail the run"
    );
    let sc = server_ctx.expect("a failed CDC run must still record the server it ran against");
    assert!(
        sc.contains("\"engine\":\"mysql\"") && sc.contains("max_execution_time_ms"),
        "{sc}"
    );
    super::live_metrics_persist::assert_harm_contract(
        &db,
        &failed,
        super::live_metrics_persist::MYSQL_HARM_COUNTERS,
    );
}

/// The `export_schema` baseline rivet 0.30.0 stored for this table (measured with the
/// 0.30.0 release binary on 2026-09-30): the bare `numeric` absent, `date[]` as its List.
const BASELINE_0_30_0: &str = r#"[{"name":"id","type":"Int64"},{"name":"d","type":"List(Field { data_type: Date32, nullable: true })"}]"#;

/// Upgrading from 0.30.0 under `fail`: server-text columns join the baseline silently; a new column still refuses.
#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical)"]
fn pg_cdc_upgrade_baseline_takes_server_text_silently_and_still_refuses_a_new_column() {
    use postgres::NoTls;
    let tbl = unique_name("cdc_drift_upg");
    let slot = unique_name("rivet_drift_upg_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, n NUMERIC, d DATE[])"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let rig = Rig::pg_cdc(&tbl, &slot).export_line(DRIFT_FAIL);
    rig.run_ok(); // creates the slot
    let db = rig.config_path().parent().unwrap().join(".rivet_state.db");
    let state = rusqlite::Connection::open(&db).unwrap();
    let baseline = || -> String {
        state
            .query_row(
                "SELECT columns_json FROM export_schema WHERE export_name = ?1",
                [rig.export_name()],
                |r| r.get(0),
            )
            .unwrap()
    };
    state
        .execute(
            "UPDATE export_schema SET columns_json = ?1 WHERE export_name = ?2",
            [BASELINE_0_30_0, rig.export_name()],
        )
        .unwrap();
    assert_eq!(
        baseline(),
        BASELINE_0_30_0,
        "the 0.30.0 baseline is in place"
    );

    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, 1.50, ARRAY['2024-01-01'::date])"
    ))
    .unwrap();
    rig.run_ok();
    let migrated: Vec<(String, String)> =
        serde_json::from_str::<Vec<serde_json::Value>>(&baseline())
            .unwrap()
            .iter()
            .map(|c| {
                (
                    c["name"].as_str().unwrap().to_string(),
                    c["type"].as_str().unwrap().to_string(),
                )
            })
            .collect();
    for (name, ty) in [("id", "Int64"), ("n", "Utf8"), ("d", "Utf8")] {
        assert!(
            migrated.contains(&(name.to_string(), ty.to_string())),
            "the baseline must now hold {name}: {ty}: {migrated:?}"
        );
    }
    assert_eq!(
        duckdb_declared_dir_id_set(&rig.out_dir()),
        [1].into_iter().collect(),
        "the run after the upgrade captures the change"
    );

    c.batch_execute(&format!(
        "ALTER TABLE {tbl} ADD COLUMN extra INT; INSERT INTO {tbl} VALUES (2, 2, NULL, 7)"
    ))
    .unwrap();
    let said = rig.run_expect_fail();
    assert!(
        said.contains(&format!(
            "schema drift detected for export '{}'",
            rig.export_name()
        )) && said.contains("extra"),
        "a genuinely new column still drifts under `fail`:\n{said}"
    );
}

// ── a column added while a run is open (#644) ────────────────────────────────

/// How long the open run parks after each part, so the test changes the table while it is open.
const HOT_PAUSE: &str = "cdc_after_parts_before_ack:20000";

/// Pin and baseline rows 1-2, then a CONTINUOUS run reads row 3 under the old schema and parks
/// after its part; meanwhile the table gains `c` and rows 4-5 carry values in it. Returns what the
/// open run said and its exit.
fn run_while_a_column_is_added(
    scn: &mut CdcScenario,
    exec: &dyn Fn(&str),
    add_column: &str,
) -> (std::process::Output, String) {
    let t = scn.table.clone();
    scn.sql(&format!("INSERT INTO {t} (id, v) VALUES (1, 10)"));
    scn.sql(&format!("INSERT INTO {t} (id, v) VALUES (2, 20)"));
    scn.settle();
    scn.rig.run_ok();
    scn.sql(&format!("INSERT INTO {t} (id, v) VALUES (3, 30)"));
    scn.settle();
    scn.rig
        .replace_cdc_line("until_current", "until_current: false");
    scn.rig.replace_cdc_line("max_events", "max_events: 3");
    let marker = scn.rig.out_dir().join("..").join("hot-add.paused");
    let marker_env = marker.display().to_string();
    let child = scn.rig.spawn_args_env(
        &[],
        &[
            ("RIVET_TEST_PAUSE_AT", HOT_PAUSE),
            ("RIVET_TEST_PAUSE_MARKER", &marker_env),
        ],
    );
    let opened = std::time::Instant::now();
    while !marker.is_file() {
        assert!(
            opened.elapsed() < std::time::Duration::from_secs(120),
            "the open run never parked after its first part"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    exec(add_column);
    exec(&format!(
        "INSERT INTO {t} (id, v, c) VALUES (4, 40, 'four')"
    ));
    exec(&format!(
        "INSERT INTO {t} (id, v, c) VALUES (5, 50, 'five')"
    ));
    let out = child.wait_with_output().expect("reap the open run");
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out, said)
}

/// Every value of the added column reaches the warehouse: the open run, then load and compact,
/// read back through `bq`.
fn a_hot_added_column_lands_every_value(mut scn: CdcScenario, exec: &dyn Fn(&str), bq: &BqLive) {
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    let (out, said) = run_while_a_column_is_added(
        &mut scn,
        exec,
        &format!("ALTER TABLE {table} ADD COLUMN c VARCHAR(16)"),
    );
    assert!(out.status.success(), "the open run ends clean:\n{said}");
    scn.rig
        .replace_cdc_line("until_current", "until_current: true");
    scn.rig.load_ok(&[], &[]);
    let compact = scn.rig.cli(&["compact"]);
    assert!(
        compact.status.success(),
        "{}",
        String::from_utf8_lossy(&compact.stderr)
    );
    let rows = bq.read_bq_rows(&format!(
        "SELECT id, c FROM `{}.{}.{table}` ORDER BY id",
        bq.project, bq.dataset
    ));
    let got: Vec<(String, Option<String>)> = rows
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap_or_default().to_string(),
                r["c"].as_str().map(str::to_string),
            )
        })
        .collect();
    let want = |id: &str, c: Option<&str>| (id.to_string(), c.map(str::to_string));
    assert_eq!(
        got,
        vec![
            want("1", None),
            want("2", None),
            want("3", None),
            want("4", Some("four")),
            want("5", Some("five")),
        ],
        "every value of the added column is in the base:\n{said}"
    );
}

/// The base-and-buffer CDC shape of the digest cell, draining one change per part.
fn hot_shape(r: Rig, t: &str, bq: &BqLive) -> Rig {
    r.cdc("backfill: auto")
        .cdc("rollover: 1")
        .cdc("max_events: 1000000")
        .also_batch_export("baseline", t, "chunked")
        .also_export_line("chunk_by_key: id")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", pk: [id]"))
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn mysql_cdc_a_column_added_while_a_run_is_open_lands_every_value() {
    let Some(bq) = BqLive::from_env("hot_add_my") else {
        return;
    };
    let scn = CdcScenario::mysql_with("hot_add_my", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        hot_shape(r, t, &bq)
    });
    let exec = |q: &str| {
        mysql::Pool::new(MYSQL_CDC_URL)
            .and_then(|p| p.get_conn())
            .expect("connect mysql-cdc")
            .query_drop(q)
            .expect(q)
    };
    a_hot_added_column_lands_every_value(scn, &exec, &bq);
}

#[test]
#[ignore = "live: requires postgres-cdc + BigQuery creds"]
fn pg_cdc_a_column_added_while_a_run_is_open_lands_every_value() {
    let Some(bq) = BqLive::from_env("hot_add_pg") else {
        return;
    };
    let scn = CdcScenario::pg_with("hot_add_pg", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        hot_shape(r, t, &bq)
    });
    let exec = |q: &str| {
        postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls)
            .expect("connect postgres-cdc")
            .batch_execute(q)
            .expect(q)
    };
    a_hot_added_column_lands_every_value(scn, &exec, &bq);
}

/// SQL Server's capture instance never carries a column added after it was created, so the open
/// run cannot write its values: it must stop by code, not end clean over a column it never read.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_a_column_added_while_a_run_is_open_stops_by_code() {
    let _serial = cross_process_serial("mssql_cdc");
    let mut scn = CdcScenario::mssql_with("hot_add_ms", "id BIGINT PRIMARY KEY, v INT", |r, _| {
        r.cdc("until_current: true")
            .cdc("rollover: 1")
            .cdc("max_events: 1000000")
            .a_failed_run_may_leave(
                &[
                    Leftover::OrphanPart,
                    Leftover::FileLog,
                    Leftover::CdcFlush,
                    Leftover::CdcCheckpoint,
                ],
                "the stop comes at the end of the drain: the parts it committed before the column was added stay, and the checkpoint follows them",
            )
    });
    let table = scn.table.clone();
    let (out, said) = run_while_a_column_is_added(
        &mut scn,
        &|q: &str| mssql_cdc_exec(q),
        &format!("ALTER TABLE dbo.{table} ADD c VARCHAR(16)"),
    );
    assert_eq!(
        out.status.code(),
        Some(5),
        "a refusal, not a clean end:\n{said}"
    );
    assert!(
        said.contains("[RIVET_SOURCE_CDC_SCHEMA_CHANGED]") && said.contains(&table),
        "the stop names its code and the table:\n{said}"
    );
}
