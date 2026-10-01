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

/// The raw TSV ClickHouse returns for `sql`, for rows that are not `(id, v)` pairs.
fn clickhouse_rows_tsv(sql: &str) -> String {
    ch(sql)
}

/// The `(id, v)` rows ClickHouse returns for `sql` (a `FORMAT TSV` query).
fn clickhouse_rows(sql: &str) -> Vec<(i64, i64)> {
    pairs(&ch(sql))
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
fn clickhouse_rows_match_source(view: &str, source: Vec<(i64, i64)>, deleted: &str) {
    assert_eq!(
        clickhouse_rows(&format!(
            "SELECT id, v FROM {view} WHERE NOT __is_deleted ORDER BY id FORMAT TSV"
        )),
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
    let db = Db::new("rivet_chtest");
    let rig = into_clickhouse(Rig::mysql_cdc(&tbl), &db);
    let view = format!("{}.{tbl}", db.0);

    rig.run_ok();
    load(&rig);
    clickhouse_rows_match_source(&view, source_rows(&tbl), "");

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
    clickhouse_rows_match_source(&view, source_rows(&tbl), "2");
}

/// A stream with no snapshot leg: the load holds exactly the rows changed after the pin run.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_stream_only_cdc_load_holds_every_row_changed_after_the_pin() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _guard) = seeded("rivet_ch_stream", 2);
    let db = Db::new("rivet_chstream");
    let rig = Rig::mysql_cdc(&tbl)
        .dest_gcs(BUCKET, &unique_name("chstream"), FAKE_GCS_ENDPOINT)
        .top_line(&format!(
            "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
             user: {CLICKHOUSE_USER}, password_env: {PASSWORD_ENV}, pk: [id] }}",
            db.0
        ));
    rig.run_ok();
    conn()
        .query_drop(format!(
            "INSERT INTO {tbl} (id, v) VALUES (3, 3), (4, 4), (5, 5)"
        ))
        .expect("changes");
    rig.run_ok();
    load(&rig);
    clickhouse_rows_match_source(&format!("{}.{tbl}", db.0), vec![(3, 3), (4, 4), (5, 5)], "");
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
    let _slot = Slot::new(slot.clone());
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
    let db = Db::new("rivet_chtest");
    let rig = into_clickhouse(Rig::pg_cdc(&tbl, &slot), &db);
    let view = format!("{}.{tbl}", db.0);

    rig.run_ok();
    load(&rig);
    clickhouse_rows_match_source(&view, source(&mut c), "");

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
    clickhouse_rows_match_source(&view, source(&mut c), "2");
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
    let db = Db::new("rivet_chtest");
    let rig = into_clickhouse(Rig::mssql_cdc(&table, &ci).cdc("until_current: true"), &db);
    let view = format!("{}.{table}", db.0);

    rig.run_ok();
    load(&rig);
    clickhouse_rows_match_source(&view, source(), "");

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
    clickhouse_rows_match_source(&view, source(), "2");
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
    let db = Db::new("rivet_chtest");
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
    clickhouse_rows_match_source(&view, source_rows(&tbl), "");
}

/// A PostgreSQL `(id, v, updated_at)` table on the main stand holding ids `1..=n`.
fn pg_batch_seeded(prefix: &str, n: i64) -> (String, PgTable, postgres::Client) {
    let mut c = pg_connect();
    let tbl = unique_name(prefix);
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT, updated_at TIMESTAMP NOT NULL); \
         INSERT INTO {tbl} SELECT g, g, TIMESTAMP '2026-01-01' FROM generate_series(1, {n}) g"
    ))
    .expect("seed");
    (tbl.clone(), PgTable::adopt(tbl), c)
}

fn pg_rows(c: &mut postgres::Client, tbl: &str) -> Vec<(i64, i64)> {
    c.query(&format!("SELECT id, v FROM {tbl} ORDER BY id"), &[])
        .expect("read source")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

fn batch_into_clickhouse(rig: Rig, db: &Db) -> Rig {
    rig.dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
        .top_line(&format!(
            "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
             user: {CLICKHOUSE_USER}, password_env: {PASSWORD_ENV}, pk: [id] }}",
            db.0
        ))
}

/// A whole-table load replaces the table: the second load serves the source as it
/// is now, not the union of both runs.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_full_load_into_clickhouse_replaces_the_table_with_the_current_source() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_full", 5);
    let db = Db::new("rivet_chtest");
    let rig = batch_into_clickhouse(Rig::pg_batch(&tbl).mode("full"), &db);
    let table = format!("{}.{tbl}", db.0);
    let loaded = || clickhouse_rows(&format!("SELECT id, v FROM {table} ORDER BY id FORMAT TSV"));

    rig.run_ok();
    load(&rig);
    assert_eq!(loaded(), pg_rows(&mut c, &tbl));

    c.batch_execute(&format!(
        "DELETE FROM {tbl} WHERE id = 2; UPDATE {tbl} SET v = 99 WHERE id = 1; \
         INSERT INTO {tbl} VALUES (6, 6, TIMESTAMP '2026-01-02')"
    ))
    .expect("changes");
    rig.run_ok();
    load(&rig);
    assert_eq!(
        loaded(),
        pg_rows(&mut c, &tbl),
        "the second load replaced the first"
    );
}

/// An incremental export: the first (cursor-less) run loads a table, the first
/// delta renames it into the change log behind a view that picks the latest cursor.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn an_incremental_export_into_clickhouse_adopts_the_table_and_serves_the_latest_rows() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_inc", 5);
    let db = Db::new("rivet_chtest");
    let rig = batch_into_clickhouse(
        Rig::pg_batch(&tbl)
            .mode("incremental")
            .export_line("cursor_column: updated_at"),
        &db,
    );
    let view = format!("{}.{tbl}", db.0);

    rig.run_ok();
    load(&rig);
    c.batch_execute(&format!(
        "UPDATE {tbl} SET v = 99, updated_at = TIMESTAMP '2026-02-01' WHERE id = 1; \
         INSERT INTO {tbl} VALUES (6, 6, TIMESTAMP '2026-02-01')"
    ))
    .expect("changes");
    rig.run_ok();
    load(&rig);

    assert_eq!(
        ch(&format!(
            "SELECT engine FROM system.tables WHERE database = '{}' AND name = '{tbl}' FORMAT TSV",
            db.0
        )),
        "View",
        "the first delta turned the table into the change log behind a view"
    );
    assert_eq!(
        clickhouse_rows(&format!("SELECT id, v FROM {view} ORDER BY id FORMAT TSV")),
        pg_rows(&mut c, &tbl)
    );
}

/// A change log keyed on one `load.pk` cannot take a load keyed on another: rows the
/// old key already collapsed cannot be told apart again, so the load refuses before
/// writing and the view keeps serving what it served.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_changed_load_pk_is_refused_before_it_rekeys_the_change_log() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let mut c = conn();
    let tbl = unique_name("rivet_ch_pk");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT, tenant BIGINT, v INT, \
         PRIMARY KEY (id, tenant)); INSERT INTO {tbl} VALUES (1, 1, 11), (1, 2, 12)"
    ))
    .expect("seed");
    let _guard = Table(tbl.clone());
    let db = Db::new("rivet_chtest");
    let keyed = |pk: &str| {
        Rig::mysql_cdc(&tbl)
            .cdc("initial: snapshot")
            .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
            .top_line(&format!(
                "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
                 user: {CLICKHOUSE_USER}, password_env: {PASSWORD_ENV}, pk: [{pk}] }}",
                db.0
            ))
    };
    let first = keyed("id, tenant");
    first.run_ok();
    load(&first);
    let view = format!("{}.{tbl}", db.0);
    let rows = || {
        ch(&format!(
            "SELECT id, tenant, v FROM {view} ORDER BY id, tenant FORMAT TSV"
        ))
    };
    let before = rows();
    assert_eq!(
        before, "1\t1\t11\n1\t2\t12",
        "the composite key keeps both rows"
    );

    let second = keyed("id");
    second.run_ok();
    let out = second.load_args_env(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && err.contains("collapses versions by (id, tenant)"),
        "a re-keyed load must refuse, naming both keys:\n{err}"
    );
    assert_eq!(rows(), before, "nothing was written: the view is unchanged");
}

/// A materialized view on the change log writes rows of its own; the load's count
/// must be the rows it inserted, or every load after the view is attached refuses.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_materialized_view_on_the_change_log_does_not_break_the_count() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _guard) = seeded("rivet_ch_mv", 3);
    let db = Db::new("rivet_chtest");
    let rig = into_clickhouse(Rig::mysql_cdc(&tbl), &db);
    let view = format!("{}.{tbl}", db.0);
    rig.run_ok();
    load(&rig);
    ch(&format!(
        "CREATE TABLE {view}_audit (id Int64) ENGINE = MergeTree ORDER BY id"
    ));
    ch(&format!(
        "CREATE MATERIALIZED VIEW {view}_audit_mv TO {view}_audit AS SELECT id FROM {view}__changes"
    ));
    conn()
        .query_drop(format!(
            "INSERT INTO {tbl} (id, v) VALUES (4, 4); UPDATE {tbl} SET v = 9 WHERE id = 1"
        ))
        .expect("changes");
    rig.run_ok();
    load(&rig);
    clickhouse_rows_match_source(&view, source_rows(&tbl), "");
}

/// uuid, jsonb, a fractional time and arrays (one NULL) load and keep their values:
/// the uuid recovers through the resolver's own cast, the time keeps its microseconds.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn uuid_json_time_and_array_columns_load_with_their_values() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let mut c = pg_connect();
    let tbl = unique_name("rivet_ch_types");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, u UUID, j JSONB, t TIME, a INT[]); \
         INSERT INTO {tbl} VALUES \
           (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{{\"k\": [1, 2]}}', '13:45:07.123456', '{{1,2}}'), \
           (2, NULL, NULL, NULL, NULL)"
    ))
    .expect("seed");
    let _t = PgTable::adopt(tbl.clone());
    let db = Db::new("rivet_chtest");
    let rig = batch_into_clickhouse(Rig::pg_batch(&tbl).mode("full"), &db);
    rig.run_ok();
    load(&rig);

    let table = format!("{}.{tbl}", db.0);
    let got = clickhouse_rows_tsv(&format!(
        "SELECT id, if(u IS NULL, '', toString(toUUID(concat(substring(lower(hex(u)),1,8),'-',\
         substring(lower(hex(u)),9,4),'-',substring(lower(hex(u)),13,4),'-',\
         substring(lower(hex(u)),17,4),'-',substring(lower(hex(u)),21,12))))), \
         ifNull(j, ''), ifNull(toString(t), ''), toString(a) FROM {table} ORDER BY id FORMAT TSV"
    ));
    let rows: Vec<Vec<&str>> = got.lines().map(|l| l.split('\t').collect()).collect();
    assert_eq!(rows.len(), 2, "{got}");
    assert_eq!(rows[0][1], "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "uuid");
    let json: serde_json::Value = serde_json::from_str(rows[0][2]).expect("json text");
    assert_eq!(json, serde_json::json!({"k": [1, 2]}), "jsonb");
    assert_eq!(
        rows[0][3], "49507.123456",
        "13:45:07.123456 as seconds since midnight"
    );
    assert_eq!(rows[0][4], "[1,2]", "array");
    assert_eq!(&rows[1][1..4], ["", "", ""], "NULLs stay NULL");
    assert_eq!(
        rows[1][4], "[]",
        "a NULL array loads as [] — the resolver says so"
    );
}

/// One CDC cycle staged on `dest`, loaded into ClickHouse — by rivet sending each part,
/// or, with `collection`, by ClickHouse reading it from the store itself.
fn staged_cdc_cycle(
    prefix: &str,
    dest: impl Fn(Rig) -> Rig,
    envs: &[(&str, &str)],
    collection: Option<&str>,
) {
    require_alive(LiveService::ClickHouse);
    let (tbl, _guard) = seeded(prefix, 5);
    let db = Db::new("rivet_chtest");
    let extra = collection
        .map(|c| format!(", named_collection: {c}"))
        .unwrap_or_default();
    let rig = dest(Rig::mysql_cdc(&tbl).cdc("initial: snapshot")).top_line(&format!(
        "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
         user: {CLICKHOUSE_USER}, password_env: {PASSWORD_ENV}, pk: [id]{extra} }}",
        db.0
    ));
    let mut all: Vec<(&str, &str)> = envs.to_vec();
    all.push((PASSWORD_ENV, CLICKHOUSE_PASSWORD));
    let view = format!("{}.{tbl}", db.0);
    let run = rig.run_args_env(&[], envs);
    assert!(
        run.status.success(),
        "run: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    rig.load_ok(&[], &all);
    clickhouse_rows_match_source(&view, source_rows(&tbl), "");
    conn()
        .query_drop(format!(
            "INSERT INTO {tbl} (id, v) VALUES (6, 6); UPDATE {tbl} SET v = 9 WHERE id = 1; \
             DELETE FROM {tbl} WHERE id = 2"
        ))
        .expect("changes");
    let run = rig.run_args_env(&[], envs);
    assert!(
        run.status.success(),
        "run: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    rig.load_ok(&[], &all);
    clickhouse_rows_match_source(&view, source_rows(&tbl), "2");
}

const MINIO_ENV: [(&str, &str); 2] = [
    ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
    ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
];
const S3_BUCKET: &str = "rivet-qa-clickhouse-load-s3";
const AZ_CONTAINER: &str = "rivet-qa-clickhouse-load-az";

fn on_s3(rig: Rig) -> Rig {
    rig.dest_s3(S3_BUCKET, &unique_name("chload"), MINIO_ENDPOINT)
}

fn on_azure(rig: Rig) -> Rig {
    rig.dest_azure(AZ_CONTAINER, &unique_name("chload"))
}

/// An export staged on S3 (MinIO) loads into ClickHouse, rivet sending the parts.
#[test]
#[ignore = "live: requires clickhouse + minio + mysql-cdc"]
fn a_cdc_stream_staged_on_s3_loads_into_clickhouse() {
    require_alive(LiveService::Minio);
    ensure_minio_bucket(S3_BUCKET);
    staged_cdc_cycle("rivet_ch_s3", on_s3, &MINIO_ENV, None);
}

/// …and ClickHouse reads the same parts from S3 itself through a named collection.
#[test]
#[ignore = "live: requires clickhouse (named collections) + minio + mysql-cdc"]
fn a_cdc_stream_staged_on_s3_is_pulled_by_clickhouse() {
    require_alive(LiveService::Minio);
    ensure_minio_bucket(S3_BUCKET);
    staged_cdc_cycle("rivet_ch_s3p", on_s3, &MINIO_ENV, Some("rivet_stand_minio"));
}

/// An export staged on Azure (Azurite) loads into ClickHouse, rivet sending the parts.
#[test]
#[ignore = "live: requires clickhouse + azurite + mysql-cdc"]
fn a_cdc_stream_staged_on_azure_loads_into_clickhouse() {
    require_alive(LiveService::Azurite);
    ensure_azure_container(AZ_CONTAINER);
    staged_cdc_cycle(
        "rivet_ch_az",
        on_azure,
        &[("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)],
        None,
    );
}

/// …and ClickHouse reads the same parts from Azure itself through a named collection.
#[test]
#[ignore = "live: requires clickhouse (named collections) + azurite + mysql-cdc"]
fn a_cdc_stream_staged_on_azure_is_pulled_by_clickhouse() {
    require_alive(LiveService::Azurite);
    ensure_azure_container(AZ_CONTAINER);
    staged_cdc_cycle(
        "rivet_ch_azp",
        on_azure,
        &[("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)],
        Some("rivet_stand_azurite"),
    );
}

/// A second load into a change log with a tz timestamp and a time column must pass the
/// change-log check: the catalog spells those types its own way (quotes, Decimal(18, p)).
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres-cdc"]
fn a_change_log_with_tz_and_time_columns_takes_a_second_load() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let tbl = unique_name("rivet_ch_tz");
    let slot = unique_name("rivet_ch_tz_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("pg");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT, at TIMESTAMPTZ, t TIME); \
         INSERT INTO {tbl} VALUES (1, 1, '2026-01-01 10:00:00+00', '10:00:00.5')"
    ))
    .expect("seed");
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let db = Db::new("rivet_chtest");
    let rig = into_clickhouse(Rig::pg_cdc(&tbl, &slot), &db);
    rig.run_ok();
    load(&rig);
    c.batch_execute(&format!("UPDATE {tbl} SET v = 2 WHERE id = 1"))
        .expect("change");
    rig.run_ok();
    load(&rig);
    assert_eq!(
        clickhouse_rows_tsv(&format!(
            "SELECT id, v, toString(t) FROM {}.{tbl} ORDER BY id FORMAT TSV",
            db.0
        )),
        "1\t2\t36000.5",
        "the second load landed and the time kept its fraction"
    );
}

/// A prefix ClickHouse's table functions cannot address as written (a space, non-ASCII)
/// still loads under `named_collection`: rivet sends those parts itself.
#[test]
#[ignore = "live: requires clickhouse (named collections) + minio + mysql-cdc"]
fn a_part_clickhouse_cannot_address_is_sent_by_rivet_instead() {
    require_alive(LiveService::Minio);
    ensure_minio_bucket(S3_BUCKET);
    let odd = |rig: Rig| {
        rig.dest_s3(
            S3_BUCKET,
            &format!("{} Order Détails", unique_name("chload")),
            MINIO_ENDPOINT,
        )
    };
    staged_cdc_cycle("rivet_ch_odd", odd, &MINIO_ENV, Some("rivet_stand_minio"));
}

/// A load that dies right after turning the full-load table into the change log leaves the
/// log and no table; the next load must append to that log and build the view, not land a
/// second table beside it (which made every later delta refuse). The shape the bughunt hit:
/// a full export switched to incremental, its first run adopting the table.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_load_that_dies_after_adopting_the_table_resumes_into_the_log() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_adopt", 5);
    let db = Db::new("rivet_chtest");
    let dir = tempfile::tempdir().expect("config dir");
    let prefix = unique_name("chload");
    let config = |rig: Rig| {
        rig.dest_gcs(BUCKET, &prefix, FAKE_GCS_ENDPOINT)
            .top_line(&format!(
                "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
                 user: {CLICKHOUSE_USER}, password_env: {PASSWORD_ENV}, pk: [id] }}",
                db.0
            ))
            .config_in(dir.path())
    };
    let cli = |args: &[&str], hook: Option<&str>| {
        let mut envs = vec![(PASSWORD_ENV, CLICKHOUSE_PASSWORD)];
        if let Some(h) = hook {
            envs.push(("RIVET_TEST_PANIC_AT", h));
        }
        run_rivet_env(args, &envs)
    };
    let ok = |args: &[&str]| {
        let out = cli(args, None);
        assert!(
            out.status.success(),
            "{args:?}:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };

    let full = config(Rig::pg_batch(&tbl).mode("full"));
    let cfg = full.to_str().expect("utf-8");
    ok(&["run", "--config", cfg]);
    ok(&["load", "--config", cfg]);

    config(
        Rig::pg_batch(&tbl)
            .mode("incremental")
            .export_line("cursor_column: updated_at"),
    );
    ok(&["run", "--config", cfg]);
    let crashed = cli(&["load", "--config", cfg], Some("load_after_adopt"));
    assert!(
        !crashed.status.success(),
        "the hook must stop the load after the adoption"
    );
    ok(&["load", "--config", cfg]);

    c.batch_execute(&format!(
        "UPDATE {tbl} SET v = 99, updated_at = TIMESTAMP '2026-02-01' WHERE id = 1"
    ))
    .expect("change");
    ok(&["run", "--config", cfg]);
    ok(&["load", "--config", cfg]);
    assert_eq!(
        clickhouse_rows(&format!(
            "SELECT id, v FROM {}.{tbl} ORDER BY id FORMAT TSV",
            db.0
        )),
        pg_rows(&mut c, &tbl)
    );
}

/// A timestamp without a zone keeps its wall-clock value in ClickHouse whatever the
/// reading SESSION's time zone (RED against a column declared in a zone). The server's own
/// zone is not flipped here: the stand's ClickHouse is shared and runs UTC.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_naive_timestamp_reads_back_the_same_wall_clock_in_any_session_zone() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let mut c = pg_connect();
    let tbl = unique_name("rivet_ch_naive");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, ts TIMESTAMP); \
         INSERT INTO {tbl} VALUES (1, TIMESTAMP '2024-01-01 00:00:00'), (2, TIMESTAMP '2024-06-30 23:30:15.5')"
    ))
    .expect("seed");
    let _t = PgTable::adopt(tbl.clone());
    let db = Db::new("rivet_chtest");
    let rig = batch_into_clickhouse(Rig::pg_batch(&tbl).mode("full"), &db);
    rig.run_ok();
    load(&rig);
    let table = format!("{}.{tbl}", db.0);
    for zone in ["UTC", "Asia/Tokyo", "America/Los_Angeles"] {
        let got = clickhouse_rows_tsv(&format!(
            "SELECT toString(ts), toString(toDate(ts)) FROM {table} ORDER BY id \
             SETTINGS session_timezone = '{zone}' FORMAT TSV"
        ));
        assert_eq!(
            got.trim(),
            "2024-01-01 00:00:00.000000\t2024-01-01\n2024-06-30 23:30:15.500000\t2024-06-30",
            "a zone-less value must read back as written in session zone {zone}"
        );
    }
}

/// A timestamp past ClickHouse DateTime64's 2299-12-31 is refused before any row is inserted,
/// never stored as the clamped end of the range (measured on 24.8: 9999-12-31 read back as
/// 2299-12-31 23:00 with no error, whatever `date_time_overflow_behavior` said).
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_timestamp_clickhouse_cannot_hold_is_refused_not_clamped() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let mut c = pg_connect();
    let tbl = unique_name("rivet_ch_far");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, ts TIMESTAMP); \
         INSERT INTO {tbl} VALUES (1, TIMESTAMP '2024-01-01 00:00:00'), \
                                  (2, TIMESTAMP '9999-12-31 00:00:00')"
    ))
    .expect("seed");
    let _t = PgTable::adopt(tbl.clone());
    let db = Db::new("rivet_chtest");
    let rig = batch_into_clickhouse(Rig::pg_batch(&tbl).mode("full"), &db);
    rig.run_ok();
    let out = rig.load_args_env(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "the load must refuse:\n{err}");
    assert!(
        err.contains("column `ts` holds 9999-12-31 00:00, outside ClickHouse DateTime64's range"),
        "{err}"
    );
    let stored = ch(&format!(
        "SELECT count() FROM system.tables WHERE database = '{}' AND name = '{tbl}' FORMAT TSV",
        db.0
    ));
    let rows = if stored.trim() == "1" {
        ch(&format!("SELECT count() FROM {}.{tbl} FORMAT TSV", db.0))
    } else {
        "0".to_string()
    };
    assert_eq!(
        rows.trim(),
        "0",
        "nothing was inserted, the clamped row least of all"
    );
}

/// A column added between the first full pass and the first delta: the adoption refuses,
/// and the remedy its message names must keep the baseline rows in the view.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_column_added_before_the_first_delta_is_refused_and_its_remedy_keeps_the_baseline() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_addcol", 5);
    let db = Db::new("rivet_chtest");
    let rig = batch_into_clickhouse(
        Rig::pg_batch(&tbl)
            .mode("incremental")
            .export_line("cursor_column: updated_at"),
        &db,
    );
    let table = format!("{}.{tbl}", db.0);
    rig.run_ok();
    load(&rig);
    c.batch_execute(&format!(
        "ALTER TABLE {tbl} ADD COLUMN c INT; \
         UPDATE {tbl} SET v = 99, c = 7, updated_at = TIMESTAMP '2026-02-01' WHERE id = 1"
    ))
    .expect("changes");
    rig.run_ok();
    let refused = rig.load_args_env(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&refused.stdout),
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(!refused.status.success(), "the adoption refuses:\n{said}");
    assert!(
        said.contains("add the export's new column(s) to the table (ALTER TABLE … ADD COLUMN")
            && said.contains("Do not rename the table aside"),
        "the refusal names the lossless remedy and warns off the rename:\n{said}"
    );
    ch(&format!("ALTER TABLE {table} ADD COLUMN c Nullable(Int32)"));
    load(&rig);
    assert_eq!(
        clickhouse_rows(&format!("SELECT id, v FROM {table} ORDER BY id FORMAT TSV")),
        pg_rows(&mut c, &tbl),
        "the baseline rows survive the remedy"
    );
}

/// An incremental first pass followed by an idle run: the load lands the table and
/// exits 0 — an empty delta is "up to date", not a failure.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn an_idle_incremental_run_after_the_first_pass_loads_cleanly() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_idle", 5);
    let db = Db::new("rivet_chtest");
    let rig = batch_into_clickhouse(
        Rig::pg_batch(&tbl)
            .mode("incremental")
            .export_line("cursor_column: updated_at"),
        &db,
    );
    let loaded = || {
        clickhouse_rows(&format!(
            "SELECT id, v FROM {}.{tbl} ORDER BY id FORMAT TSV",
            db.0
        ))
    };
    rig.run_ok();
    rig.run_ok();
    load(&rig);
    assert_eq!(loaded(), pg_rows(&mut c, &tbl));
    c.batch_execute(&format!(
        "UPDATE {tbl} SET v = 99, updated_at = TIMESTAMP '2026-02-01' WHERE id = 1"
    ))
    .expect("change");
    rig.run_ok();
    load(&rig);
    assert_eq!(loaded(), pg_rows(&mut c, &tbl), "a later delta still lands");
}

/// The `load:` line for ClickHouse database `db` at `url`, plus `extra` keys (`, k: v`).
fn load_line(url: &str, db: &Db, extra: &str) -> String {
    format!(
        "load: {{ target: clickhouse, url: \"{url}\", database: {}, user: {CLICKHOUSE_USER}, \
         password_env: {PASSWORD_ENV}, pk: [id]{extra} }}",
        db.0
    )
}

/// A 9999-12-31 timestamp staged on S3 and PULLED by ClickHouse through the named
/// collection is refused before any row lands, as a pushed one is: the pull reads the
/// part's footer from the store first (until 2026-09-29 it was stored clamped to 2299).
#[test]
#[ignore = "live: requires clickhouse (named collections) + minio + postgres"]
fn a_timestamp_clickhouse_cannot_hold_is_refused_when_pulled_too() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::Minio);
    ensure_minio_bucket(S3_BUCKET);
    let mut c = pg_connect();
    let tbl = unique_name("rivet_ch_farpull");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, ts TIMESTAMP); \
         INSERT INTO {tbl} VALUES (1, TIMESTAMP '2024-01-01 00:00:00'), \
                                  (2, TIMESTAMP '9999-12-31 00:00:00')"
    ))
    .expect("seed");
    let _t = PgTable::adopt(tbl.clone());
    let db = Db::new("rivet_chtest");
    let rig = on_s3(Rig::pg_batch(&tbl).mode("full")).top_line(&load_line(
        CLICKHOUSE_HTTP_URL,
        &db,
        ", named_collection: rivet_stand_minio",
    ));
    let run = rig.run_args_env(&[], &MINIO_ENV);
    assert!(
        run.status.success(),
        "run: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let mut envs = MINIO_ENV.to_vec();
    envs.push((PASSWORD_ENV, CLICKHOUSE_PASSWORD));
    let out = rig.load_args_env(&[], &envs);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "the pulled load must refuse:\n{err}");
    assert!(
        err.contains("column `ts` holds 9999-12-31 00:00, outside ClickHouse DateTime64's range"),
        "{err}"
    );
    let rows = ch(&format!(
        "SELECT sum(total_rows) FROM system.tables WHERE database = '{}' FORMAT TSV",
        db.0
    ));
    assert_eq!(
        rows, "0",
        "no row reached any table, the clamped one least of all"
    );
}

/// A TCP proxy in front of ClickHouse that counts INSERT requests and swallows the
/// answer of the first `lose` of them after ClickHouse ran them.
struct LossyProxy {
    port: u16,
    inserts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    budget: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl LossyProxy {
    fn start(lose: usize) -> Self {
        use std::io::{Read, Write};
        use std::net::{Shutdown, TcpListener, TcpStream};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let inserts = Arc::new(AtomicUsize::new(0));
        let budget = Arc::new(AtomicUsize::new(lose));
        let (counted, lost) = (inserts.clone(), budget.clone());
        let upstream_addr = CLICKHOUSE_HTTP_URL
            .trim_start_matches("http://")
            .to_string();
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                let upstream = TcpStream::connect(&upstream_addr).expect("reach clickhouse");
                let saw_insert = Arc::new(AtomicBool::new(false));
                let mut c_in = client.try_clone().expect("clone client");
                let mut u_out = upstream.try_clone().expect("clone upstream");
                let (saw, counted) = (saw_insert.clone(), counted.clone());
                std::thread::spawn(move || {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = c_in.read(&mut buf) {
                        if n == 0 {
                            break;
                        }
                        if buf[..n].windows(6).any(|w| w == b"INSERT") {
                            counted.fetch_add(1, SeqCst);
                            saw.store(true, SeqCst);
                        }
                        if u_out.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                    let _ = u_out.shutdown(Shutdown::Write);
                });
                let (mut u_in, mut c_out, budget) = (upstream, client, lost.clone());
                std::thread::spawn(move || {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = u_in.read(&mut buf) {
                        let lost = n > 0
                            && saw_insert.swap(false, SeqCst)
                            && budget
                                .fetch_update(SeqCst, SeqCst, |b| b.checked_sub(1))
                                .is_ok();
                        if n == 0 || lost || c_out.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                    let _ = c_out.shutdown(Shutdown::Both);
                    let _ = u_in.shutdown(Shutdown::Both);
                });
            }
        });
        LossyProxy {
            port,
            inserts,
            budget,
        }
    }

    /// Swallow the answers of the next `n` INSERTs.
    fn lose(&self, n: usize) {
        self.budget.store(n, std::sync::atomic::Ordering::SeqCst);
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn inserts(&self) -> usize {
        self.inserts.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// An INSERT into a CDC change log whose answer is lost after ClickHouse ran it is sent
/// again, and the load completes: the second copy carries the same key and version, so
/// the view still equals the source row for row.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_lost_insert_answer_is_resent_and_the_cdc_view_still_matches_the_source() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _guard) = seeded("rivet_ch_lost", 5);
    let db = Db::new("rivet_chtest");
    let proxy = LossyProxy::start(1);
    let rig = Rig::mysql_cdc(&tbl)
        .cdc("initial: snapshot")
        .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
        .top_line(&load_line(&proxy.url(), &db, ""));
    rig.run_ok();
    load(&rig);
    assert!(
        proxy.inserts() >= 2,
        "the lost INSERT was sent again ({} INSERTs)",
        proxy.inserts()
    );
    clickhouse_rows_match_source(&format!("{}.{tbl}", db.0), source_rows(&tbl), "");
}

/// When every answer is lost the retries end: the load fails after exactly five attempts
/// at the first part and says so.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_cdc_insert_whose_every_answer_is_lost_fails_after_five_attempts() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _guard) = seeded("rivet_ch_lostall", 3);
    let db = Db::new("rivet_chtest");
    let proxy = LossyProxy::start(usize::MAX);
    let rig = Rig::mysql_cdc(&tbl)
        .cdc("initial: snapshot")
        .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
        .top_line(&load_line(&proxy.url(), &db, ""));
    rig.run_ok();
    let out = rig.load_args_env(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "the load must fail:\n{err}");
    assert!(err.contains("failed on attempt 5 of 5"), "{err}");
    assert_eq!(
        proxy.inserts(),
        5,
        "one first try and four retries, no more"
    );
}

/// A full load's INSERT into its swap table is NOT resent when its answer is lost: the
/// swap table is a plain MergeTree, so a second copy would double the rows. The load
/// fails, the old table keeps serving, and the next load replaces it with the source.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_full_load_insert_whose_answer_is_lost_is_not_resent() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_lostfull", 5);
    let db = Db::new("rivet_chtest");
    let proxy = LossyProxy::start(0);
    let rig = Rig::pg_batch(&tbl)
        .mode("full")
        .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
        .top_line(&load_line(&proxy.url(), &db, ""));
    let table = format!("{}.{tbl}", db.0);
    let loaded = || clickhouse_rows(&format!("SELECT id, v FROM {table} ORDER BY id FORMAT TSV"));
    rig.run_ok();
    load(&rig);
    let before = pg_rows(&mut c, &tbl);
    assert_eq!(loaded(), before);

    c.batch_execute(&format!("UPDATE {tbl} SET v = 99 WHERE id = 1"))
        .expect("change");
    rig.run_ok();
    let sent = proxy.inserts();
    proxy.lose(1);
    let out = rig.load_args_env(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "the load must fail, not resend:\n{err}"
    );
    assert!(err.contains("failed on attempt 1 of 5"), "{err}");
    assert_eq!(
        proxy.inserts() - sent,
        1,
        "the swap-table INSERT went exactly once"
    );
    assert_eq!(loaded(), before, "the old table keeps serving");

    load(&rig);
    assert_eq!(
        loaded(),
        pg_rows(&mut c, &tbl),
        "the next load lands the source"
    );
}

/// A full load killed at each of its fault points (swap created, first part inserted,
/// every part inserted, swap exchanged in) leaves either the old table or the new one,
/// never a mix, and the next load serves the source exactly.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_full_load_killed_at_each_fault_point_re_runs_to_the_source() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_fullcrash", 6);
    let db = Db::new("rivet_chtest");
    let rig = batch_into_clickhouse(
        Rig::pg_batch(&tbl)
            .mode("chunked")
            .export_line("chunk_column: id")
            .export_line("chunk_size: 2"),
        &db,
    );
    let table = format!("{}.{tbl}", db.0);
    let loaded = || clickhouse_rows(&format!("SELECT id, v FROM {table} ORDER BY id FORMAT TSV"));
    rig.run_ok();
    load(&rig);
    let mut served = pg_rows(&mut c, &tbl);
    assert_eq!(loaded(), served);

    for (i, (hook, swapped)) in [
        ("clickhouse_full_after_swap_created", false),
        ("clickhouse_after_part:0", false),
        ("clickhouse_full_before_swap_in", false),
        ("clickhouse_full_after_exchange", true),
    ]
    .into_iter()
    .enumerate()
    {
        c.batch_execute(&format!(
            "UPDATE {tbl} SET v = v + 100 WHERE id = 1; \
             INSERT INTO {tbl} VALUES ({}, 7, TIMESTAMP '2026-01-02')",
            100 + i
        ))
        .expect("change");
        rig.run_ok();
        let crashed = rig.load_args_env(
            &[],
            &[
                (PASSWORD_ENV, CLICKHOUSE_PASSWORD),
                ("RIVET_TEST_PANIC_AT", hook),
            ],
        );
        let err = String::from_utf8_lossy(&crashed.stderr);
        assert!(
            !crashed.status.success() && err.contains("injected crash"),
            "{hook} must stop the load:\n{err}"
        );
        let source = pg_rows(&mut c, &tbl);
        let expected = if swapped { &source } else { &served };
        assert_eq!(
            &loaded(),
            expected,
            "{hook}: the old table or the new one, never a mix"
        );
        load(&rig);
        assert_eq!(loaded(), source, "{hook}: the re-run serves the source");
        served = source;
    }
}

/// `system.tables.partition_key` of `db.name`, as ClickHouse spells it.
fn partition_key(db: &Db, name: &str) -> String {
    ch(&format!(
        "SELECT partition_key FROM system.tables WHERE database = '{}' AND name = '{name}' \
         FORMAT TSVRaw",
        db.0
    ))
}

/// A partitioned whole-table load: the table carries the declared key, each row sits in the
/// month PostgreSQL itself names for it (a 1950 row included), and the data equals the source.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_full_load_into_clickhouse_is_partitioned_by_month_as_declared() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let mut c = pg_connect();
    let tbl = unique_name("rivet_ch_part_full");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT, created_at TIMESTAMP); \
         INSERT INTO {tbl} VALUES (1, 1, '1950-06-15 13:45:00'), (2, 2, '2026-01-05 00:00:00'), \
           (3, 3, '2026-01-31 23:59:59'), (4, 4, '2026-03-01 00:00:00'), (5, 5, NULL)"
    ))
    .expect("seed");
    let _t = PgTable::adopt(tbl.clone());
    let db = Db::new("rivet_chtest");
    let rig = Rig::pg_batch(&tbl)
        .mode("full")
        .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
        .top_line(&load_line(
            CLICKHOUSE_HTTP_URL,
            &db,
            ", partition: { column: created_at, granularity: month }",
        ));
    rig.run_ok();
    load(&rig);

    let table = format!("{}.{tbl}", db.0);
    assert_eq!(partition_key(&db, &tbl), "toYYYYMM(created_at)");
    let months: Vec<String> = c
        .query(
            &format!(
                "SELECT DISTINCT to_char(created_at, 'YYYYMM') FROM {tbl} \
                 WHERE created_at IS NOT NULL ORDER BY 1"
            ),
            &[],
        )
        .expect("source months")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        ch(&format!(
            "SELECT DISTINCT _partition_id FROM {table} WHERE created_at IS NOT NULL \
             ORDER BY 1 FORMAT TSV"
        )),
        months.join("\n"),
        "each row sits in the month the source names, 1950 included"
    );
    assert_eq!(
        clickhouse_rows(&format!("SELECT id, v FROM {table} ORDER BY id FORMAT TSV")),
        pg_rows(&mut c, &tbl)
    );
}

/// A CDC log partitioned by a column that moves: the key whose `event_at` changes month
/// keeps one version per partition in storage, and the view still returns exactly the
/// source's latest row per key — also under a session that turns the cross-partition
/// `FINAL` off, since the view pins it.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_cdc_log_partitioned_by_a_moving_column_serves_one_latest_row_per_key() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let mut c = conn();
    let tbl = unique_name("rivet_ch_part_cdc");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT, event_at DATETIME NOT NULL); \
         INSERT INTO {tbl} VALUES (1, 1, '2026-01-10 00:00:00'), (2, 2, '2026-01-11 00:00:00'), \
           (3, 3, '2026-02-01 00:00:00'), (4, 4, '1950-01-01 00:00:00'), \
           (5, 5, '2026-02-02 00:00:00')"
    ))
    .expect("seed");
    let _guard = Table(tbl.clone());
    let db = Db::new("rivet_chtest");
    let rig = Rig::mysql_cdc(&tbl)
        .cdc("initial: snapshot")
        .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
        .top_line(&load_line(
            CLICKHOUSE_HTTP_URL,
            &db,
            ", partition: { column: event_at, granularity: month }",
        ));
    let view = format!("{}.{tbl}", db.0);
    rig.run_ok();
    load(&rig);
    clickhouse_rows_match_source(&view, source_rows(&tbl), "");

    c.query_drop(format!(
        "UPDATE {tbl} SET v = 99, event_at = '2026-03-10 00:00:00' WHERE id = 1; \
         UPDATE {tbl} SET v = 30 WHERE id = 3; \
         UPDATE {tbl} SET v = 31, event_at = '2026-04-01 00:00:00' WHERE id = 3; \
         DELETE FROM {tbl} WHERE id = 2"
    ))
    .expect("changes");
    rig.run_ok();
    let out = rig.load_args_env(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "load: {err}");
    assert!(
        err.contains("partitioned by `event_at`, which is not a creation stamp"),
        "the load warns that the column can move:\n{err}"
    );

    assert_eq!(
        partition_key(&db, &format!("{tbl}__changes")),
        "toYYYYMM(event_at)"
    );
    ch(&format!("OPTIMIZE TABLE {view}__changes FINAL"));
    assert_eq!(
        ch(&format!(
            "SELECT groupArray(_partition_id) FROM (SELECT _partition_id FROM {view}__changes \
             WHERE id = 1 ORDER BY 1) FORMAT TSV"
        )),
        "['202601','202603']",
        "merges never cross partitions: the moved key keeps a version in each"
    );
    clickhouse_rows_match_source(&view, source_rows(&tbl), "2");
    assert_eq!(
        clickhouse_rows(&format!(
            "SELECT id, v FROM {view} WHERE NOT __is_deleted ORDER BY id \
             SETTINGS do_not_merge_across_partitions_select_final = 1 FORMAT TSV"
        )),
        source_rows(&tbl),
        "the view pins the cross-partition FINAL against the session"
    );
}

/// A change log created with one partition refuses a load declaring another, naming both,
/// and the view keeps serving what it served.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + mysql-cdc"]
fn a_changed_partition_is_refused_before_it_touches_the_change_log() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let mut c = conn();
    let tbl = unique_name("rivet_ch_part_moved");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT, created_at DATETIME NOT NULL); \
         INSERT INTO {tbl} VALUES (1, 1, '2026-01-10 00:00:00'), (2, 2, '2026-02-10 00:00:00')"
    ))
    .expect("seed");
    let _guard = Table(tbl.clone());
    let db = Db::new("rivet_chtest");
    let by = |granularity: &str| {
        Rig::mysql_cdc(&tbl)
            .cdc("initial: snapshot")
            .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
            .top_line(&load_line(
                CLICKHOUSE_HTTP_URL,
                &db,
                &format!(", partition: {{ column: created_at, granularity: {granularity} }}"),
            ))
    };
    let first = by("month");
    first.run_ok();
    load(&first);
    let view = format!("{}.{tbl}", db.0);
    let before = source_rows(&tbl);
    clickhouse_rows_match_source(&view, before.clone(), "");

    let second = by("year");
    second.run_ok();
    let out = second.load_args_env(&[], &[(PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success()
            && err.contains(
                "is partitioned by toYYYYMM(created_at), but the load declares toYear(`created_at`)"
            ),
        "a re-partitioned load must refuse, naming both keys:\n{err}"
    );
    assert_eq!(
        ch(&format!("SELECT count() FROM {view}__changes")),
        "2",
        "nothing was written"
    );
    clickhouse_rows_match_source(&view, before, "");
}

/// A partitioned incremental export: the first pass lands a partitioned table, the first
/// delta adopts it as the log (same key, so no refusal), and the view serves the source.
#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn a_partitioned_incremental_export_into_clickhouse_serves_the_latest_rows() {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(BUCKET);
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_part_inc", 5);
    let db = Db::new("rivet_chtest");
    let rig = Rig::pg_batch(&tbl)
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .dest_gcs(BUCKET, &unique_name("chload"), FAKE_GCS_ENDPOINT)
        .top_line(&load_line(
            CLICKHOUSE_HTTP_URL,
            &db,
            ", partition: { column: updated_at, granularity: month }",
        ));
    let view = format!("{}.{tbl}", db.0);

    rig.run_ok();
    load(&rig);
    assert_eq!(partition_key(&db, &tbl), "toYYYYMM(updated_at)");
    c.batch_execute(&format!(
        "UPDATE {tbl} SET v = 99, updated_at = TIMESTAMP '2026-02-01' WHERE id = 1; \
         INSERT INTO {tbl} VALUES (6, 6, TIMESTAMP '2026-03-01')"
    ))
    .expect("changes");
    rig.run_ok();
    load(&rig);

    assert_eq!(
        partition_key(&db, &format!("{tbl}__changes")),
        "toYYYYMM(updated_at)",
        "the log keeps the partition of the table it grew from"
    );
    assert_eq!(
        ch(&format!(
            "SELECT DISTINCT _partition_id FROM {view}__changes ORDER BY 1 FORMAT TSV"
        )),
        "202601\n202602\n202603"
    );
    assert_eq!(
        clickhouse_rows(&format!("SELECT id, v FROM {view} ORDER BY id FORMAT TSV")),
        pg_rows(&mut c, &tbl)
    );
}

// ─── cleanup_source + gc_orphans on every staging store, and the sibling they spare ───

/// A staging object store on the stand.
#[derive(Clone, Copy)]
enum Store {
    Gcs,
    S3,
    Azure,
}

impl Store {
    /// The service is up and the bucket/container exists.
    fn ready(self) {
        match self {
            Store::Gcs => {
                require_alive(LiveService::FakeGcs);
                ensure_gcs_bucket(BUCKET);
            }
            Store::S3 => {
                require_alive(LiveService::Minio);
                ensure_minio_bucket(S3_BUCKET);
            }
            Store::Azure => {
                require_alive(LiveService::Azurite);
                ensure_azure_container(AZ_CONTAINER);
            }
        }
    }

    /// `rig` staged on this store under `prefix`.
    fn dest(self, rig: Rig, prefix: &str) -> Rig {
        match self {
            Store::Gcs => rig.dest_gcs(BUCKET, prefix, FAKE_GCS_ENDPOINT),
            Store::S3 => rig.dest_s3(S3_BUCKET, prefix, MINIO_ENDPOINT),
            Store::Azure => rig.dest_azure(AZ_CONTAINER, prefix),
        }
    }

    /// The credentials a run or load needs for this store.
    fn env(self) -> Vec<(&'static str, &'static str)> {
        match self {
            Store::Gcs => Vec::new(),
            Store::S3 => MINIO_ENV.to_vec(),
            Store::Azure => vec![("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)],
        }
    }

    /// Write a stray object at `key`, as a crashed extract leaves one.
    fn put(self, key: &str) {
        match self {
            Store::Gcs => fake_gcs_put(BUCKET, key, b"orphan"),
            Store::S3 => minio_put(S3_BUCKET, key, b"orphan"),
            Store::Azure => azure_put(AZ_CONTAINER, key, b"orphan"),
        }
    }

    /// Object names under `prefix`, sorted, read through the store's own API.
    fn names(self, prefix: &str) -> Vec<String> {
        let mut n = match self {
            Store::Gcs => fake_gcs_names(BUCKET, prefix),
            Store::S3 => minio_object_names(S3_BUCKET, prefix),
            Store::Azure => azure_blob_names(AZ_CONTAINER, prefix),
        };
        n.sort();
        n
    }
}

/// A load with `cleanup_source` and `gc_orphans` empties its own prefix of Parquet and
/// leaves `<prefix>_archive/` alone. The prefix is written WITHOUT a trailing slash, as an
/// operator writes it, so a store's string-prefix listing of `<prefix>` also reaches the
/// sibling unless the listing is cut at the directory boundary. The sibling holds a stray
/// part and no manifest: a sibling RUN would make the load refuse two exports under one
/// prefix (`ensure_single_export`), a second guard that hides the boundary.
fn a_cleaned_prefix_spares_its_sibling(store: Store) {
    require_alive(LiveService::ClickHouse);
    store.ready();
    let (tbl, _t, mut c) = pg_batch_seeded("rivet_ch_sib", 5);
    let db = Db::new("rivet_chtest");
    let base = unique_name("chsib");
    let env = store.env();
    let mut all = env.clone();
    all.push((PASSWORD_ENV, CLICKHOUSE_PASSWORD));

    let owner = store
        .dest(Rig::pg_batch(&tbl).mode("full"), &base)
        .dest_prefix_unslashed()
        .top_line(&load_line(
            CLICKHOUSE_HTTP_URL,
            &db,
            ", cleanup_source: true, gc_orphans: true",
        ));
    let run = owner.run_args_env(&[], &env);
    assert!(
        run.status.success(),
        "run: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let mine = format!("{base}/{tbl}/");
    let theirs = format!("{base}/{tbl}_archive/");
    store.put(&format!("{mine}orphan.parquet"));
    store.put(&format!("{theirs}part-000000.parquet"));
    let sibling_before = store.names(&theirs);
    assert_eq!(
        sibling_before,
        vec![format!("{theirs}part-000000.parquet")],
        "fixture: the sibling holds one stray part"
    );
    assert!(
        store
            .names(&mine)
            .iter()
            .filter(|n| n.ends_with(".parquet"))
            .count()
            >= 2,
        "fixture: the loaded prefix holds its run's part and a stray one"
    );

    owner.load_ok(&[], &all);
    assert_eq!(
        clickhouse_rows(&format!(
            "SELECT id, v FROM {}.{tbl} ORDER BY id FORMAT TSV",
            db.0
        )),
        pg_rows(&mut c, &tbl),
        "the load delivered the source"
    );
    let left: Vec<String> = store
        .names(&mine)
        .into_iter()
        .filter(|n| n.ends_with(".parquet"))
        .collect();
    assert!(
        left.is_empty(),
        "cleanup_source removed the loaded part and gc_orphans the stray one: {left:?}"
    );
    assert_eq!(
        store.names(&theirs),
        sibling_before,
        "a sibling prefix that extends the loaded one is not this load's to clean"
    );
}

#[test]
#[ignore = "live: requires clickhouse + fake-gcs + postgres"]
fn cleanup_and_gc_on_gcs_spare_a_sibling_prefix() {
    a_cleaned_prefix_spares_its_sibling(Store::Gcs);
}

#[test]
#[ignore = "live: requires clickhouse + minio + postgres"]
fn cleanup_and_gc_on_s3_spare_a_sibling_prefix() {
    a_cleaned_prefix_spares_its_sibling(Store::S3);
}

#[test]
#[ignore = "live: requires clickhouse + azurite + postgres, and the az CLI on PATH"]
fn cleanup_and_gc_on_azure_spare_a_sibling_prefix() {
    a_cleaned_prefix_spares_its_sibling(Store::Azure);
}
