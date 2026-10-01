//! A CDC drain reads the change log, not the table: capturing a handful of changes on a
//! large table must cost the source a small fraction of one full scan, on every engine
//! (docs/reference/cdc.md, "Why CDC is gentle on the source"). The oracle is the source
//! server's own counters, as the run's `export_harm` rows record them, plus DuckDB over
//! the captured parts.

use crate::common::*;

/// Rows already in the table before the stream anchors.
const TABLE_ROWS: i64 = 50_000;
/// Changes the drain captures.
const CHANGES: i64 = 5;

/// The drain run's harm deltas, read back from the state DB beside the config.
fn drain_harm(rig: &Rig) -> std::collections::BTreeMap<String, i64> {
    let db = StateDb::next_to_config(&rig.config_path());
    db.harm_rows(&db.latest_run_id(rig.export_name()))
        .into_iter()
        .collect()
}

/// The drain captured exactly the new rows (DuckDB over the declared parts).
fn assert_captured(rig: &Rig) {
    assert_eq!(
        duckdb_declared_dir_id_set(&rig.out_dir()),
        (TABLE_ROWS + 1..=TABLE_ROWS + CHANGES).collect(),
        "the drain captures exactly the new rows"
    );
}

/// The drain captured exactly the new rows, and a server read counter stayed far below
/// one scan of the table.
fn assert_gentle(rig: &Rig, counter: &str, ceiling: i64) {
    assert_captured(rig);
    let harm = drain_harm(rig);
    eprintln!("HARM {}: {harm:?}", rig.export_name());
    let read = *harm
        .get(counter)
        .unwrap_or_else(|| panic!("the drain must record `{counter}`: {harm:?}"));
    assert!(
        read <= ceiling,
        "a CDC drain of {CHANGES} changes read {read} ({counter}) on a {TABLE_ROWS}-row table — \
         more than {ceiling}, a scan the change log should have spared the source"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW) + the rivet-duckdb oracle"]
fn a_mysql_cdc_drain_reads_the_binlog_not_the_table() {
    use mysql::prelude::Queryable as _;
    let tbl = unique_name("harm_my");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .and_then(|p| p.get_conn())
        .expect("connect");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());
    c.query_drop("SET SESSION cte_max_recursion_depth = 100000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {tbl} WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g \
         WHERE n < {TABLE_ROWS}) SELECT n, n FROM g"
    ))
    .unwrap();
    let rig = Rig::mysql_cdc(&tbl).duckdb_oracle();
    rig.run_ok(); // anchor
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES {}",
        (TABLE_ROWS + 1..=TABLE_ROWS + CHANGES)
            .map(|i| format!("({i}, {i})"))
            .collect::<Vec<_>>()
            .join(",")
    ))
    .unwrap();
    rig.run_ok();
    assert_gentle(&rig, "mysql_innodb_rows_read", TABLE_ROWS / 10);
}

#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical) + the rivet-duckdb oracle"]
fn a_pg_cdc_drain_reads_the_wal_not_the_table_and_records_what_it_decoded() {
    let tbl = unique_name("harm_pg");
    let slot = unique_name("rivet_harm_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); \
         INSERT INTO {tbl} SELECT g, g FROM generate_series(1, {TABLE_ROWS}) g;"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let rig = Rig::pg_cdc(&tbl, &slot).duckdb_oracle().no_oracle(
        "the test counts reads of the source table; the oracle's own read-back would be counted",
    );
    rig.run_ok(); // creates the slot
    let _slot = Slot(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} SELECT g, g FROM generate_series({}, {}) g",
        TABLE_ROWS + 1,
        TABLE_ROWS + CHANGES
    ))
    .unwrap();
    let before = pg_table_reads(&mut c, &tbl);
    rig.run_ok();
    let read = pg_table_reads(&mut c, &tbl) - before;
    assert_captured(&rig);
    assert!(
        read <= CHANGES,
        "a CDC drain of {CHANGES} changes read {read} tuples of the {TABLE_ROWS}-row table itself \
         (pg_stat_user_tables) — the change log should have spared the scan"
    );
    assert!(
        drain_harm(&rig)
            .get("pg_slot_decoded_bytes")
            .is_some_and(|b| *b > 0),
        "decoding the captured changes must show in pg_slot_decoded_bytes"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql-cdc with SQL Server Agent + the rivet-duckdb oracle"]
fn a_mssql_cdc_drain_reads_the_change_table_not_the_source_table() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("harm_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} SELECT TOP ({TABLE_ROWS}) n, n FROM ( \
         SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n \
         FROM sys.all_objects a CROSS JOIN sys.all_objects b) q"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let rig = Rig::mssql_cdc(&table, &ci).duckdb_oracle().no_oracle(
        "the test counts reads of the source table; the oracle's own read-back would be counted",
    );
    rig.run_ok(); // anchor
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} SELECT n, n FROM (VALUES {}) v(n)",
        (TABLE_ROWS + 1..=TABLE_ROWS + CHANGES)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",")
    ));
    wait_for_capture(&ci, CHANGES);
    let before = mssql_table_accesses(&table);
    rig.run_ok();
    let accesses = mssql_table_accesses(&table) - before;
    assert_captured(&rig);
    assert_eq!(
        accesses, 0,
        "a CDC drain must not scan or seek the source table itself \
         (sys.dm_db_index_usage_stats) — it reads the change table"
    );
}

/// Scans, seeks and lookups on `dbo.<table>`'s indexes, as SQL Server counts them (live).
fn mssql_table_accesses(table: &str) -> i64 {
    mssql_query_i64_on(
        1434,
        &format!(
            "SELECT COALESCE(SUM(user_scans + user_seeks + user_lookups), 0) \
             FROM sys.dm_db_index_usage_stats \
             WHERE database_id = DB_ID() AND object_id = OBJECT_ID('dbo.{table}')"
        ),
    )
}

/// Tuples read from `table` itself (sequential + index fetches), once the counts settle.
fn pg_table_reads(c: &mut postgres::Client, table: &str) -> i64 {
    let read = |c: &mut postgres::Client| -> i64 {
        c.query_one(
            "SELECT (COALESCE(seq_tup_read, 0) + COALESCE(idx_tup_fetch, 0))::bigint \
             FROM pg_stat_user_tables WHERE relname = $1",
            &[&table],
        )
        .map(|r| r.get(0))
        .unwrap_or(0)
    };
    // A backend's counts reach the shared view when it flushes (at exit, or after a second
    // idle); read until two samples half a second apart agree, so a zero is a count, not
    // a lag.
    let mut last = read(c);
    for _ in 0..20 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        let now = read(c);
        if now == last {
            return now;
        }
        last = now;
    }
    last
}

#[test]
#[ignore = "live: requires docker compose mongo-rs + the rivet-duckdb oracle"]
fn a_mongo_cdc_drain_reads_the_oplog_not_the_collection() {
    const PORT: u16 = 27018;
    let db = unique_name("harm_mongo");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");
    m.insert_many(
        "t",
        (1..=TABLE_ROWS)
            .map(|i| mongodb::bson::doc! { "_id": i, "v": i })
            .collect(),
    );
    let rig = Rig::mongo_cdc("t")
        .source_url(&MongoTest::url(PORT, &db))
        .duckdb_oracle()
        .no_oracle("the test counts reads of the source collection; the oracle's own read-back would be counted");
    rig.run_ok(); // anchor
    for i in TABLE_ROWS + 1..=TABLE_ROWS + CHANGES {
        m.upsert_set("t", i, "v", "x");
    }
    rig.run_ok();
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    assert_eq!(
        ids.len() as i64,
        CHANGES,
        "the drain captures exactly the new documents: {ids:?}"
    );
    let harm = drain_harm(&rig);
    eprintln!("HARM {}: {harm:?}", rig.export_name());
    let scanned = *harm
        .get("mongo_docs_scanned")
        .expect("mongo_docs_scanned recorded");
    assert!(
        scanned <= TABLE_ROWS / 10,
        "a CDC drain of {CHANGES} changes scanned {scanned} documents of a {TABLE_ROWS}-document collection"
    );
}
