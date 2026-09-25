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

fn rig_for(tbl: &str, db: &Db) -> Rig {
    Rig::mysql_cdc(tbl)
        .cdc("initial: snapshot")
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
    let rig = rig_for(&tbl, &db);

    rig.run_ok();
    load(&rig);
    let view = format!("{}.{tbl}", db.0);
    assert_eq!(
        pairs(&ch(&format!(
            "SELECT id, v FROM {view} ORDER BY id FORMAT TSV"
        ))),
        source_rows(&tbl),
        "after the snapshot the view is the source"
    );

    let mut c = conn();
    c.query_drop(format!(
        "INSERT INTO {tbl} (id, v) VALUES (6, 6), (7, 7); \
         UPDATE {tbl} SET v = 99 WHERE id = 1; \
         UPDATE {tbl} SET v = 30 WHERE id = 3; \
         UPDATE {tbl} SET v = 31 WHERE id = 3; \
         DELETE FROM {tbl} WHERE id = 2"
    ))
    .expect("changes");
    rig.run_ok();
    load(&rig);

    assert_eq!(
        pairs(&ch(&format!(
            "SELECT id, v FROM {view} WHERE NOT __is_deleted ORDER BY id FORMAT TSV"
        ))),
        source_rows(&tbl),
        "every live row equals the source, the twice-updated key at its last value"
    );
    assert_eq!(
        ch(&format!(
            "SELECT id FROM {view} WHERE __is_deleted FORMAT TSV"
        )),
        "2",
        "the deleted key is flagged, not silently absent"
    );
}
