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
        "fixture: the runner must ship at least {min_parts} parts so the last one is not the only one, got {}",
        days.len()
    );
    let total: i64 = rows(format!("SELECT count(*) FROM read_parquet('{glob}')"))[0][0]
        .parse()
        .unwrap();
    assert_eq!(total, ROWS, "every row shipped");
    for (file, n) in &days {
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
