//! SQL Server `sql_variant` and CLR UDT columns are delivered as text, batch and CDC.
//!
//! tiberius 0.12 panicked (`unimplemented!`) on a `sql_variant` cell and batch had to
//! refuse the column up front; CDC had no guard at all. The oracle is the server's own
//! rendering through `sqlcmd`: `CONVERT(nvarchar(4000), v, 121)` (ISO for temporals)
//! for the variant, and the lowercase hex of `CAST(x AS varbinary(max))` for
//! `hierarchyid` / `geometry` / `geography`. Also pins the one tiberius 0.13 default
//! rivet overrides: no 30 s bound on a server round-trip.

use crate::common::*;

const COLS: &str = "id INT PRIMARY KEY, v sql_variant, h hierarchyid, g geometry, gg geography";

/// One INSERT per row: an int, an nvarchar, a datetime2(7), a decimal, a datetimeoffset, NULLs.
fn inserts(table: &str) -> Vec<String> {
    [
        "1, CAST(42 AS int), '/1/2/', geometry::STGeomFromText('POINT(1 2)', 0), \
         geography::Point(47.65, -122.34, 4326)",
        "2, CAST(N'héllo ✓' AS nvarchar(20)), NULL, NULL, NULL",
        "3, CAST('2024-02-29 12:34:56.1234567' AS datetime2(7)), '/', NULL, NULL",
        "4, CAST(12.50 AS decimal(10,2)), NULL, NULL, NULL",
        "5, CAST('2024-02-29 12:00:00.5 +05:30' AS datetimeoffset(1)), NULL, NULL, NULL",
        "6, NULL, NULL, NULL, NULL",
    ]
    .iter()
    .map(|v| format!("INSERT INTO dbo.{table} (id, v, h, g, gg) VALUES ({v})"))
    .collect()
}

/// The source's own text for every row, as `id|v|h|g|gg` with `NULL` for SQL NULL.
fn source_rendering(container: &str, table: &str) -> Vec<String> {
    let hex = |c: &str| format!("LOWER(CONVERT(varchar(max), CAST({c} AS varbinary(max)), 2))");
    sqlcmd_lines(
        container,
        &format!(
            "SELECT CONCAT(id, '|', ISNULL(CONVERT(nvarchar(4000), v, 121), N'NULL'), '|', \
             ISNULL({}, 'NULL'), '|', ISNULL({}, 'NULL'), '|', ISNULL({}, 'NULL')) \
             FROM dbo.{table} ORDER BY id",
            hex("h"),
            hex("g"),
            hex("gg")
        ),
    )
}

/// What rivet wrote, read back by DuckDB, in the same `id|v|h|g|gg` shape.
fn delivered(dir: &std::path::Path) -> Vec<String> {
    let c = stage_for_duckdb(dir);
    let v = duckdb_run_sql_json(&format!(
        "SELECT id, v, h, g, gg FROM read_parquet('{c}/**/*.parquet') ORDER BY id"
    ));
    v["rows"]
        .as_array()
        .expect("duckdb rows")
        .iter()
        .map(|r| {
            (0..5)
                .map(|i| r[i].as_str().unwrap_or("NULL").to_string())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_batch_export_delivers_sql_variant_and_udt_columns_as_the_source_renders_them() {
    require_alive(LiveService::Mssql);
    let table = unique_name("mssql_variant");
    mssql_exec(&format!("CREATE TABLE dbo.{table} ({COLS})"));
    let _guard = MssqlTable::adopt(table.clone());
    for sql in inserts(&table) {
        mssql_exec(&sql);
    }
    let want = source_rendering("rivet-mssql-1", &table);
    assert_eq!(want.len(), 6, "oracle rows: {want:?}");
    let rig = Rig::mssql_batch(&table)
        .no_oracle("sql_variant / CLR UDT: the DuckDB mssql scanner renders ToString()/WKB where rivet delivers the wire bytes; no shared rendering to compare");
    rig.run_ok();
    assert_eq!(delivered(&rig.out_dir()), want);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn a_cdc_capture_delivers_sql_variant_and_udt_columns_as_the_source_renders_them() {
    let mut sc = CdcScenario::mssql_with("mssql_cdc_variant", COLS, |r, _| {
        r.no_oracle("sql_variant / CLR UDT: the DuckDB mssql scanner renders ToString()/WKB where rivet delivers the wire bytes; no shared rendering to compare")
    });
    sc.rig.run_ok(); // pin
    let table = sc.table.clone();
    for sql in inserts(&table) {
        sc.sql(&sql);
    }
    sc.settle();
    sc.rig.run_ok();
    let want = source_rendering("rivet-mssql-cdc-1", &table);
    assert_eq!(want.len(), 6, "oracle rows: {want:?}");
    assert_eq!(delivered(&sc.rig.out_dir()), want);
}

/// A read held behind a schema lock for over 30 s completes. It does not go red when
/// `command_timeout(None)` is removed: tiberius 0.13's 30 s default did not fire while the
/// server held the lock (measured 2026-09-30), so this guards the outcome, not that setting.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_read_blocked_longer_than_thirty_seconds_still_completes() {
    require_alive(LiveService::Mssql);
    let table = format!("rivet.dbo.{}", unique_name("mssql_slow"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);\nINSERT INTO {table} VALUES (1,1);"
    ));
    let _guard = MssqlTable::adopt(table.clone());
    let rig = Rig::mssql_batch("mssql_slow")
        .query(&format!("SELECT id, v FROM {table}"))
        .source_line("tuning:")
        .source_line("  lock_timeout_s: 0")
        .source_line("  statement_timeout_s: 0")
        .source_line("  max_retries: 0");
    // A schema-modification lock blocks every reader, snapshot isolation included.
    let tx = format!(
        "BEGIN TRAN; ALTER TABLE {table} ADD blocker INT NULL;\nWAITFOR DELAY '00:01:30';\nROLLBACK;"
    );
    // Resolved before the lock: OBJECT_ID itself waits on the schema lock it would look for.
    let oid = mssql_query_i64(&format!("SELECT OBJECT_ID('{table}')"));
    let writer = std::thread::spawn(move || mssql_exec(&tx));
    let t0 = std::time::Instant::now();
    while mssql_query_i64(&format!(
        "SELECT COUNT(*) FROM sys.dm_tran_locks WHERE request_mode = 'Sch-M' \
         AND resource_associated_entity_id = {oid}"
    )) == 0
    {
        assert!(
            t0.elapsed().as_secs() < 5,
            "fixture: the writer never took the schema lock"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let started = std::time::Instant::now();
    let said = rig.run_ok_capture();
    let took = started.elapsed();
    writer.join().expect("writer thread");
    assert!(
        took.as_secs() >= 31,
        "the read was not blocked past tiberius' 30 s default ({took:?}), so this proves nothing:\n{said}"
    );
    assert_eq!(duckdb_dir_parquet_i64(&rig.out_dir(), "v"), vec![1]);
}
