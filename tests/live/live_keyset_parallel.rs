//! Parallel-keyset coverage through the canonical Rig, on EVERY SQL keyset engine
//! (feat/parallel-keyset). One assertion helper × {PostgreSQL, MySQL, SQL Server}
//! × the parity/fan-out scenario. Replaces the single-engine, hand-rolled
//! `write_config`/`run_rivet_export` parallel tests with the rig seam so the
//! chunking / resilience matrices can cite a real per-engine cell.
//!
//! Mongo's `parallel: N` is the SEPARATE `_id`-range path (`mongo_parallel`), not
//! SQL keyset, so it is out of this file by construction.

use std::collections::BTreeSet;

use arrow::array::{Array, Int64Array};

use crate::common::*;

/// Read the BIGINT `id` column across every parquet part + count the DISTINCT
/// `pk_w{range}` workers the parallel runner fanned out to. A collapse to one
/// worker (a boundary probe that could not read the key) shows as workers == 1.
fn id_set_and_fanout(dir: &std::path::Path) -> (usize, BTreeSet<i64>, usize) {
    let mut count = 0usize;
    let mut keys = BTreeSet::new();
    for b in read_all_parts(dir) {
        let id = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        for i in 0..id.len() {
            count += 1;
            keys.insert(id.value(i));
        }
    }
    let workers: BTreeSet<String> = files_with_extension(dir, "parquet")
        .iter()
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
        .filter_map(|n| {
            n.split("_pk_w")
                .nth(1)
                .and_then(|s| s.split('_').next())
                .map(String::from)
        })
        .collect();
    (count, keys, workers.len())
}

/// The shared scenario: run parallel keyset on an already-seeded `1..=n` integer
/// key and assert (1) every row round-trips exactly once (structural parity across
/// the N ranges) and (2) the run FANNED OUT to ≥2 workers (did not collapse).
fn assert_parallel_parity(rig: Rig, n: usize) {
    rig.run_ok();
    let (count, keys, workers) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, n,
        "row count must round-trip exactly across all workers (no boundary drop/dupe)"
    );
    let expected: BTreeSet<i64> = (1..=n as i64).collect();
    assert_eq!(
        keys, expected,
        "the union of all workers' keys must equal 1..=n"
    );
    assert!(
        workers >= 2,
        "parallel keyset must fan out to ≥2 workers, got {workers} — the boundary probe collapsed"
    );
}

/// `chunk_by_key: id` + `parallel: 4` + a small `chunk_size` so each ~n/4 range
/// still pages several times (both the inter-worker boundary and the intra-worker
/// `key > last` boundary are exercised).
fn parallel_keyset(rig: Rig) -> Rig {
    rig.mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 500")
}

const N: usize = 3000;

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_parity_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_par_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, {N}) g;"
    ))
    .unwrap();
    assert_parallel_parity(
        parallel_keyset(Rig::pg_batch(&format!("public.{table}"))),
        N,
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_parity_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_par_my");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {N}) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    assert_parallel_parity(parallel_keyset(Rig::mysql_batch(&table)), N);
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mssql"]
fn parallel_keyset_parity_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_par_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    // A recursive-CTE tally (MAXRECURSION 0 lifts the 100 cap) seeds 1..=N.
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < {N}) \
         INSERT INTO {table} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
    ));
    assert_parallel_parity(parallel_keyset(Rig::mssql_batch(&table)), N);
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}
