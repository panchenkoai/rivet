//! Every part a batch runner ships — the LAST one included — carries the partition
//! note the loader bounds it by (`rivet.partition_buckets` = `<column>|<granularity>|<n>`),
//! and `n` is the number of distinct partitions the part really holds. Oracle: DuckDB
//! reads the footers and counts the days itself; it shares no code with the writer.

use crate::common::*;

const ROWS: i64 = 5000;
const LOAD: &str = "load: { target: bigquery, project: p, dataset: d, \
                    partition: { column: created_at, granularity: day } }";

/// `ROWS` rows, one per day from 2000-01-02 — more days than one part may hold (4,000).
fn seed() -> PgTable {
    let tbl = unique_name("pfooter");
    let mut c = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl};
         CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, created_at TIMESTAMP NOT NULL, p TEXT);
         INSERT INTO {tbl} SELECT g, TIMESTAMP '2000-01-01' + g * INTERVAL '1 day',
           md5(g::text) || md5((g * 7)::text) || md5((g * 13)::text) || md5((g * 31)::text) FROM generate_series(1, {ROWS}) g;"
    ))
    .unwrap();
    PgTable::adopt(tbl)
}

/// DuckDB over every shipped part: at least `min_parts`, all rows present, and each
/// part's note names exactly the distinct days that part holds.
fn assert_every_part_notes_its_partitions(rig: &Rig, min_parts: usize) {
    let glob = format!("{}/**/*.parquet", rig.oracle_dir());
    let rows = |sql: String| -> Vec<Vec<String>> {
        duckdb_run_sql_json(&sql)["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                r.as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap_or_default().to_string())
                    .collect()
            })
            .collect()
    };
    let days: std::collections::BTreeMap<String, String> = rows(format!(
        "SELECT filename, count(DISTINCT CAST(created_at AS DATE)) FROM \
         read_parquet('{glob}', filename=true) GROUP BY filename"
    ))
    .into_iter()
    .map(|r| (r[0].clone(), r[1].clone()))
    .collect();
    let notes: std::collections::BTreeMap<String, String> = rows(format!(
        "SELECT file_name, decode(value) FROM parquet_kv_metadata('{glob}') \
         WHERE decode(key) = 'rivet.partition_buckets'"
    ))
    .into_iter()
    .map(|r| (r[0].clone(), r[1].clone()))
    .collect();
    assert!(
        days.len() >= min_parts,
        "expected at least {min_parts} parts (the fixture crosses a part boundary or the budget), got {} — the writer did not cut, or the last part is the only one checked",
        days.len()
    );
    let total: i64 = rows(format!("SELECT count(*) FROM read_parquet('{glob}')"))[0][0]
        .parse()
        .unwrap();
    assert_eq!(total, ROWS, "every row shipped");
    for (file, n) in &days {
        assert!(
            n.parse::<i64>().unwrap() <= 4000,
            "{file}: a part must stay within one load job's 4,000 partitions, holds {n}"
        );
        assert_eq!(
            notes.get(file).map(String::as_str),
            Some(format!("created_at|day|{n}").as_str()),
            "{file}: the footer must name the {n} days this part holds"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres + the rivet-duckdb oracle"]
fn the_single_runner_rotates_at_the_partition_budget_and_notes_every_part() {
    let t = seed();
    let rig = Rig::pg_batch(t.name()).duckdb_oracle().top_line(LOAD);
    rig.run_ok();
    assert_every_part_notes_its_partitions(&rig, 2);
}

#[test]
#[ignore = "live: requires docker compose postgres + the rivet-duckdb oracle"]
fn every_chunked_part_notes_its_partitions_sequential_and_parallel() {
    let t = seed();
    for parallel in ["parallel: 1", "parallel: 3"] {
        let rig = Rig::pg_batch(t.name())
            .duckdb_oracle()
            .mode("chunked")
            .export_line("chunk_column: id")
            .export_line("chunk_size: 1000")
            .export_line(parallel)
            .top_line(LOAD);
        rig.run_ok();
        assert_every_part_notes_its_partitions(&rig, 2);
    }
}

#[test]
#[ignore = "live: requires docker compose postgres + the rivet-duckdb oracle"]
fn every_chunked_checkpoint_part_notes_its_partitions_sequential_and_parallel() {
    let t = seed();
    for parallel in ["parallel: 1", "parallel: 3"] {
        let rig = Rig::pg_batch(t.name())
            .duckdb_oracle()
            .mode("chunked")
            .export_line("chunk_column: id")
            .export_line("chunk_size: 1000")
            .export_line("chunk_checkpoint: true")
            .export_line(parallel)
            .top_line(LOAD);
        rig.run_ok();
        assert_every_part_notes_its_partitions(&rig, 2);
    }
}

#[test]
#[ignore = "live: requires docker compose postgres + the rivet-duckdb oracle"]
fn every_keyset_page_notes_its_partitions_sequential_and_parallel() {
    let t = seed();
    for parallel in ["parallel: 1", "parallel: 3"] {
        let rig = Rig::pg_batch(t.name())
            .duckdb_oracle()
            .mode("chunked")
            .export_line("chunk_by_key: id")
            .export_line("chunk_size: 1000")
            .export_line(parallel)
            .top_line(LOAD);
        rig.run_ok();
        assert_every_part_notes_its_partitions(&rig, 2);
    }
}

/// The CDC load block: a partitioned change log (`log_view`), the column partitioned by day.
const CDC_LOAD: &str = "load: { target: bigquery, project: p, dataset: d, layout: log_view, \
                        partition: { column: created_at, granularity: day } }";

/// The CDC drain cuts a change flush at the partition budget when the change log is
/// itself partitioned (`layout: log_view`), and notes every part like the batch writer.
/// The cut is engine-agnostic (the shared sink); each engine delivers its dates its own
/// way, so each engine gets the test.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW) + the rivet-duckdb oracle"]
fn mysql_cdc_flush_past_the_partition_budget_is_cut_and_every_part_notes_its_partitions() {
    use mysql::prelude::Queryable as _;
    let tbl = unique_name("pfooter_my");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .and_then(|p| p.get_conn())
        .expect("connect mysql-cdc");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, created_at DATE NOT NULL, p VARCHAR(32))"
    ))
    .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());
    let rig = Rig::mysql_cdc(&tbl).duckdb_oracle().top_line(CDC_LOAD);
    rig.run_ok(); // anchor
    // One transaction, so one commit and one flush: the cut can only come from the budget.
    c.query_drop("SET SESSION cte_max_recursion_depth = 10000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {tbl} WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g \
         WHERE n < {ROWS}) SELECT n, DATE '2000-01-01' + INTERVAL n DAY, MD5(n) FROM g"
    ))
    .unwrap();
    rig.run_ok();
    assert_every_part_notes_its_partitions(&rig, 2);
}

#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical) + the rivet-duckdb oracle"]
fn pg_cdc_flush_past_the_partition_budget_is_cut_and_every_part_notes_its_partitions() {
    let tbl = unique_name("pfooter_pg");
    let slot = unique_name("rivet_pfooter_slot");
    let mut c =
        postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, created_at DATE NOT NULL, p TEXT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let rig = Rig::pg_cdc(&tbl, &slot).duckdb_oracle().top_line(CDC_LOAD);
    rig.run_ok(); // creates the slot
    let _slot = Slot(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} SELECT g, DATE '2000-01-01' + g, md5(g::text) \
         FROM generate_series(1, {ROWS}) g"
    ))
    .unwrap();
    rig.run_ok();
    assert_every_part_notes_its_partitions(&rig, 2);
}

#[test]
#[ignore = "live: requires docker compose mssql-cdc with SQL Server Agent + the rivet-duckdb oracle"]
fn mssql_cdc_flush_past_the_partition_budget_is_cut_and_every_part_notes_its_partitions() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("pfooter_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, created_at DATE NOT NULL, p VARCHAR(32))"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let rig = Rig::mssql_cdc(&table, &ci)
        .duckdb_oracle()
        .top_line(CDC_LOAD);
    rig.run_ok(); // pins the anchor
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} SELECT TOP ({ROWS}) n, DATEADD(day, n, '2000-01-01'), \
         CONVERT(VARCHAR(32), HASHBYTES('MD5', CAST(n AS VARCHAR(12))), 2) FROM ( \
         SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n \
         FROM sys.all_objects a CROSS JOIN sys.all_objects b) q"
    ));
    wait_for_capture(&ci, ROWS);
    rig.run_ok();
    assert_every_part_notes_its_partitions(&rig, 2);
}

/// MongoDB's change parts are the fixed `{_id, document}` image, so no column can carry
/// the partition: the stream must SAY its parts are unbudgeted, not ship them silently.
#[test]
#[ignore = "live: requires docker compose mongo-rs + the rivet-duckdb oracle"]
fn mongo_cdc_says_its_parts_cannot_be_budgeted_by_a_declared_partition() {
    const PORT: u16 = 27018;
    let db = unique_name("pfooter_mongo");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");
    let rig = Rig::mongo_cdc("t")
        .source_url(&MongoTest::url(PORT, &db))
        .duckdb_oracle()
        .top_line(CDC_LOAD);
    rig.run_ok(); // anchor
    m.upsert_set("t", 1, "v", "a");
    let said = rig.run_ok_capture();
    assert!(
        said.contains(
            "cdc table 't': the load partitions by `created_at`, which this stream does not write \
             as a date or timestamp column"
        ),
        "the unbudgeted parts must be said:\n{said}"
    );
    assert!(
        duckdb_declared_distinct_set(rig.oracle_dir(), "_id").contains("1"),
        "the change is still captured"
    );
}
