//! `rivet load` into ClickHouse (ADR-0035): a CDC change log that the engine
//! collapses by version, read through a `FINAL` view, checked against the source.
//!
//!     docker compose up -d clickhouse fake-gcs && docker compose --profile cdc up -d mysql-cdc
//!     cargo test --test live_suite clickhouse_load -- --ignored

use crate::common::MysqlCdcTable as Table;
use crate::common::*;
use mysql::prelude::Queryable;

const BUCKET: &str = "rivet-qa-clickhouse-load";
const PASSWORD_ENV: &str = "RIVET_TEST_CH_PASSWORD";

fn conn() -> mysql::PooledConn {
    mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn")
}

/// A fresh `(id, v)` table on the CDC stand holding ids `1..=n`.
fn seeded(prefix: &str, n: i64) -> (String, Table) {
    let mut c = conn();
    let tbl = unique_name(prefix);
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let guard = Table(tbl.clone());
    let rows: Vec<String> = (1..=n).map(|i| format!("({i}, {i})")).collect();
    c.query_drop(format!(
        "INSERT INTO {tbl} (id, v) VALUES {}",
        rows.join(", ")
    ))
    .expect("seed rows");
    (tbl, guard)
}

/// One ClickHouse statement over HTTP; the response body, or a panic naming the SQL.
fn ch(sql: &str) -> String {
    let resp = reqwest::blocking::Client::new()
        .post(CLICKHOUSE_HTTP_URL)
        .basic_auth(CLICKHOUSE_USER, Some(CLICKHOUSE_PASSWORD))
        .body(sql.to_string())
        .send()
        .expect("post to clickhouse");
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    assert!(
        status.is_success(),
        "clickhouse HTTP {status}: {body}\nSQL: {sql}"
    );
    body.trim().to_string()
}

/// A scratch ClickHouse database dropped on drop.
struct Db(String);

impl Db {
    fn new(prefix: &str) -> Self {
        let name = unique_name(prefix);
        ch(&format!("CREATE DATABASE {name}"));
        Db(name)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| ch(&format!("DROP DATABASE IF EXISTS {}", self.0)));
    }
}

/// The `(id, v)` rows of the MySQL source, ordered by id.
fn source_rows(tbl: &str) -> Vec<(i64, i64)> {
    conn()
        .query(format!("SELECT id, v FROM {tbl} ORDER BY id"))
        .expect("read source")
}

/// `id\tv` lines of `sql` parsed into pairs.
fn pairs(tsv: &str) -> Vec<(i64, i64)> {
    tsv.lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (a, b) = l.split_once('\t').expect("two columns");
            (a.parse().expect("id"), b.parse().expect("v"))
        })
        .collect()
}

/// `rig` exporting to the fake-gcs bucket and loading into ClickHouse database `db`.
fn into_clickhouse(rig: Rig, db: &Db) -> Rig {
    rig.cdc("initial: snapshot")
        .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
        .top_line(&format!(
            "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
             user: {CLICKHOUSE_USER}, password_env: {PASSWORD_ENV}, pk: [id] }}",
            db.0
        ))
}

fn load(rig: &Rig) {
    rig.load_ok(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
}

/// The view's live rows equal `source`, and exactly `deleted` is flagged.
fn assert_view_is(view: &str, source: Vec<(i64, i64)>, deleted: &str) {
    assert_eq!(
        pairs(&ch(&format!(
            "SELECT id, v FROM {view} WHERE NOT __is_deleted ORDER BY id FORMAT TSV"
        ))),
        source,
        "the view's live rows must equal the source"
    );
    assert_eq!(
        ch(&format!(
            "SELECT id FROM {view} WHERE __is_deleted ORDER BY id FORMAT TSV"
        )),
        deleted,
        "exactly the deleted keys are flagged"
    );
}

/// Snapshot, then inserts, updates (one key twice) and a delete: the view must
/// equal the source row for row, with the deleted key flagged, not missing.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_mysql_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _guard) = seeded("rivet_ch_cdc", 5);
    let db = Db::new("rivet_tmp_ch");
    let rig = into_clickhouse(Rig::mysql_cdc(&tbl), &db);
    let view = format!("{}.{tbl}", db.0);

    rig.run_ok();
    load(&rig);
    assert_view_is(&view, source_rows(&tbl), "");

    conn()
        .query_drop(format!(
            "INSERT INTO {tbl} (id, v) VALUES (6, 6), (7, 7); \
             UPDATE {tbl} SET v = 99 WHERE id = 1; \
             UPDATE {tbl} SET v = 30 WHERE id = 3; \
             UPDATE {tbl} SET v = 31 WHERE id = 3; \
             DELETE FROM {tbl} WHERE id = 2"
        ))
        .expect("changes");
    rig.run_ok();
    load(&rig);
    assert_view_is(&view, source_rows(&tbl), "2");
}

/// The same cycle from PostgreSQL: the version decodes an LSN (`hi/lo` hex).
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres-cdc"]
fn a_postgres_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let tbl = unique_name("rivet_ch_pg");
    let slot = unique_name("rivet_ch_slot");
    let _slot = Slot(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("pg");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT); \
         INSERT INTO {tbl} SELECT g, g FROM generate_series(1, 5) g"
    ))
    .expect("seed");
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let source = |c: &mut postgres::Client| -> Vec<(i64, i64)> {
        c.query(&format!("SELECT id, v FROM {tbl} ORDER BY id"), &[])
            .expect("read source")
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    };
    let db = Db::new("rivet_tmp_ch");
    let rig = into_clickhouse(Rig::pg_cdc(&tbl, &slot), &db);
    let view = format!("{}.{tbl}", db.0);

    rig.run_ok();
    load(&rig);
    assert_view_is(&view, source(&mut c), "");

    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (6, 6), (7, 7); \
         UPDATE {tbl} SET v = 99 WHERE id = 1; \
         UPDATE {tbl} SET v = 30 WHERE id = 3; \
         UPDATE {tbl} SET v = 31 WHERE id = 3; \
         DELETE FROM {tbl} WHERE id = 2"
    ))
    .expect("changes");
    rig.run_ok();
    load(&rig);
    assert_view_is(&view, source(&mut c), "2");
}

/// The same cycle from SQL Server: the version decodes a 10-byte LSN.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mssql-cdc with SQL Server Agent"]
fn a_sql_server_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("rivet_ch_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v BIGINT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,1),(2,2),(3,3),(4,4),(5,5)"
    ));
    wait_for_capture(&ci, 5);
    let source = || {
        pairs(
            &mssql_cdc_query_strings(&format!(
                "SELECT CONCAT(id, CHAR(9), v) FROM dbo.{table} ORDER BY id"
            ))
            .join("\n"),
        )
    };
    let db = Db::new("rivet_tmp_ch");
    let rig = into_clickhouse(Rig::mssql_cdc(&table, &ci).cdc("until_current: true"), &db);
    let view = format!("{}.{table}", db.0);

    rig.run_ok();
    load(&rig);
    assert_view_is(&view, source(), "");

    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (6,6),(7,7); \
         UPDATE dbo.{table} SET v = 99 WHERE id = 1; \
         UPDATE dbo.{table} SET v = 30 WHERE id = 3; \
         UPDATE dbo.{table} SET v = 31 WHERE id = 3; \
         DELETE FROM dbo.{table} WHERE id = 2"
    ));
    wait_for_capture(&ci, 14);
    rig.run_ok();
    load(&rig);
    assert_view_is(&view, source(), "2");
}

/// A load that dies after appending but before recording itself re-appends every
/// part on the next load: the copies share key and version, so the view is
/// unchanged and the count gate still passes.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_load_that_dies_after_appending_is_re_run_without_duplicating_the_view() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _guard) = seeded("rivet_ch_crash", 5);
    let db = Db::new("rivet_tmp_ch");
    let rig = into_clickhouse(Rig::mysql_cdc(&tbl), &db);
    let view = format!("{}.{tbl}", db.0);
    rig.run_ok();

    let crashed = rig.load_args_env(
        &[],
        &[
            (PASSWORD_ENV, CLICKHOUSE_PASSWORD),
            ("RIVET_TEST_PANIC_AT", "load_after_append"),
        ],
    );
    assert!(
        !crashed.status.success(),
        "the fault hook must stop the first load"
    );
    ch(&format!("SYSTEM STOP MERGES {view}__changes"));
    load(&rig);

    let physical: u64 = ch(&format!("SELECT count() FROM {view}__changes"))
        .parse()
        .expect("count");
    assert_eq!(physical, 10, "the re-run appended every row a second time");
    assert_view_is(&view, source_rows(&tbl), "");
}
