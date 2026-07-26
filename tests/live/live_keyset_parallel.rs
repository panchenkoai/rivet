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

// ── Per-range crash-recovery (iteration 2) across every engine ───────────────

/// Crash after ONE range commits (durable in file_log + keyset_range `done=1`),
/// resume, and assert the DESTINATION manifest declares ALL `n` rows — the done
/// range's parts are REHYDRATED, not orphaned. Manifest-driven (not a parquet
/// re-read): the committed parquet survives regardless, so a manifest-orphaned
/// part is invisible to a data re-read. `parallel: 4` + `chunk_checkpoint: true`.
fn assert_parallel_crash_recovery(rig: Rig, n: usize) {
    // Run 1: HARD-EXIT after range 0's atomic commit (a worker panic would defer to
    // the scope join, by which point every worker has finished).
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:0");
    assert!(
        !crash.status.success(),
        "the injected hard-exit must make run 1 fail"
    );
    // Run 2: resume — skips the done range (rehydrates it), re-runs the rest.
    rig.run_ok();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(rig.out_dir().join("manifest.json")).unwrap())
            .expect("destination manifest.json");
    assert_eq!(
        manifest["row_count"].as_i64(),
        Some(n as i64),
        "resumed manifest must declare every row (done range not orphaned); got {}",
        manifest["row_count"]
    );
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, n,
        "the parquet union must hold every row once (no dup)"
    );
    assert_eq!(keys, (1..=n as i64).collect::<BTreeSet<i64>>());
}

fn crash_rig(rig: Rig) -> Rig {
    rig.mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 200")
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_crash_recovery_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_crash_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    assert_parallel_crash_recovery(crash_rig(Rig::pg_batch(&format!("public.{table}"))), 2000);
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_crash_recovery_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_crash_my");
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
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 2000) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    assert_parallel_crash_recovery(crash_rig(Rig::mysql_batch(&table)), 2000);
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mssql"]
fn parallel_keyset_crash_recovery_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_crash_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 2000) \
         INSERT INTO {table} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
    ));
    assert_parallel_crash_recovery(crash_rig(Rig::mssql_batch(&table)), 2000);
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}

// ── Incremental-by-key (iteration 3) across every engine ─────────────────────

/// Parallel incremental: a CLEAN re-run pulls only keys past the persisted anchor,
/// across N workers. Discriminator = the TOTAL row count on disk (a set dedups):
/// 1000 → 1000 (unchanged) → 1500 (after +500 insert), never a full re-read (2000).
/// `parallel: 4` + `chunk_checkpoint: true` + `keyset_incremental: true`. The
/// `seed_more` closure inserts the 500 higher keys between run 2 and run 3.
fn assert_parallel_incremental(rig: Rig, seed_more: impl FnOnce()) {
    rig.run_ok();
    assert_eq!(
        id_set_and_fanout(&rig.out_dir()).0,
        1000,
        "run 1 exports all 1000"
    );
    rig.run_ok();
    assert_eq!(
        id_set_and_fanout(&rig.out_dir()).0,
        1000,
        "unchanged re-run adds zero (a full re-read would double it to 2000)"
    );
    seed_more();
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 1500,
        "incremental adds ONLY the 500 new keys (2500 = full re-read)"
    );
    assert_eq!(keys, (1..=1500i64).collect::<BTreeSet<i64>>());
}

fn incremental_rig(rig: Rig) -> Rig {
    rig.mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 200")
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_incremental_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_inc_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 1000) g;"
    ))
    .unwrap();
    let t2 = table.clone();
    assert_parallel_incremental(
        incremental_rig(Rig::pg_batch(&format!("public.{table}"))),
        || {
            let mut c2 = pg_connect();
            c2.execute(
                &format!("INSERT INTO {t2} SELECT g, g FROM generate_series(1001, 1500) g"),
                &[],
            )
            .unwrap();
        },
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_incremental_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_inc_my");
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
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT n, n FROM seq"
    ))
    .unwrap();
    let t2 = table.clone();
    assert_parallel_incremental(incremental_rig(Rig::mysql_batch(&table)), || {
        let mut c2 = mysql_connect();
        c2.query_drop("SET SESSION cte_max_recursion_depth = 20000")
            .unwrap();
        c2.query_drop(format!(
            "INSERT INTO {t2} (id, payload) \
             WITH RECURSIVE seq AS (SELECT 1001 n UNION ALL SELECT n+1 FROM seq WHERE n < 1500) \
             SELECT n, n FROM seq"
        ))
        .unwrap();
    });
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mssql"]
fn parallel_keyset_incremental_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("pk_inc_ms");
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    mssql_exec(&format!(
        "WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 1000) \
         INSERT INTO {table} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
    ));
    let t2 = table.clone();
    assert_parallel_incremental(incremental_rig(Rig::mssql_batch(&table)), || {
        mssql_exec(&format!(
            "WITH nums AS (SELECT 1001 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 1500) \
             INSERT INTO {t2} (id, payload) SELECT n, n FROM nums OPTION (MAXRECURSION 0)"
        ));
    });
    mssql_exec(&format!("DROP TABLE IF EXISTS {table}"));
}
