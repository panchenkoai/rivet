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

/// Total rows across every parquet part — schema-agnostic (works for any key type:
/// varchar / decimal / unsigned bigint where the typed `id` reader below can't apply).
fn count_rows(dir: &std::path::Path) -> usize {
    read_all_parts(dir).iter().map(|b| b.num_rows()).sum()
}

/// Distinct `pk_w{range}` workers the parallel runner fanned out to (a collapse to
/// one worker == a boundary probe that could not read the key). Format-agnostic —
/// the `_pk_w{ridx}_` token is in every part name regardless of parquet/csv.
fn worker_fanout(dir: &std::path::Path) -> usize {
    ["parquet", "csv"]
        .iter()
        .flat_map(|ext| files_with_extension(dir, ext))
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
        .filter_map(|n| {
            n.split("_pk_w")
                .nth(1)
                .and_then(|s| s.split('_').next())
                .map(String::from)
        })
        .collect::<BTreeSet<String>>()
        .len()
}

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

/// Parquet parts per range worker, keyed `w{ridx}` (the `_pk_w{ridx}_{page}` token
/// in every part name). Lets a test assert a SPECIFIC range left N durable pages —
/// needed to prove a mid-range failure's pre-failure pages are on disk (#200-1).
fn worker_part_counts(dir: &std::path::Path) -> std::collections::BTreeMap<String, usize> {
    let mut out: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for p in files_with_extension(dir, "parquet") {
        if let Some(ridx) = p
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.split("_pk_w").nth(1))
            .and_then(|s| s.split('_').next())
        {
            *out.entry(format!("w{ridx}")).or_default() += 1;
        }
    }
    out
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

// ── Run-unique part naming: two full runs into ONE prefix must not clobber ───

/// Two consecutive FULL parallel-keyset runs into the SAME destination prefix.
/// Each run re-exports all `n` rows under a FRESH run_id, so run 2's parts must
/// sit ALONGSIDE run 1's (the part base is `{export}_{run_tag}_pk_w{ridx}_{page}`,
/// run_tag = sanitized run_id) — never overwrite them. Re-running without --resume
/// is a WARN that appends, not a clobber (finalize::rerun_warning_message).
///
/// Discriminator = the TOTAL row count across ALL parquet on disk: `2*n` if both
/// runs' parts survive, `n` if run 2's `pk_w{ridx}_{page}` names collided with
/// run 1's and idempotent_overwrite silently replaced them. This is the N-worker ×
/// 2-run collision the runner-coverage matrix warned keyset_PARALLEL could lack —
/// it goes RED if the run_tag is dropped from the part base.
fn assert_two_full_runs_no_clobber(rig: Rig, n: usize) {
    rig.run_ok();
    assert_eq!(
        id_set_and_fanout(&rig.out_dir()).0,
        n,
        "run 1 writes exactly n rows"
    );
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count,
        2 * n,
        "two full runs into one prefix must ACCUMULATE 2*n parquet rows; \
         count == n means run 2 clobbered run 1's parts (run_tag not in the part name)"
    );
    assert_eq!(
        keys,
        (1..=n as i64).collect::<BTreeSet<i64>>(),
        "the distinct key set is still 1..=n (each id present once per run)"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_two_full_runs_into_same_prefix_no_clobber_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_clob_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, {N}) g;"
    ))
    .unwrap();
    assert_two_full_runs_no_clobber(
        parallel_keyset(Rig::pg_batch(&format!("public.{table}"))),
        N,
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
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

// ── Edge-case scenarios (regression guards for the under-tested surface) ──────

/// `parallel: N` with N GREATER than the row count: the OFFSET percentile sampler
/// yields fewer distinct boundaries than requested, so the runner must produce fewer,
/// larger ranges and still export EVERY row exactly once — never crash or drop.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_more_workers_than_rows_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_fewrows");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 5) g;"
    ))
    .unwrap();
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 8")
        .export_line("chunk_size: 2");
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 5,
        "every row exported once even with more workers than rows"
    );
    assert_eq!(keys, (1..=5i64).collect::<BTreeSet<i64>>());
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Parallel keyset on a VARCHAR key: the boundary literals are text, injected + escaped
/// per dialect. Fan-out must not collapse and every key must round-trip (a text-boundary
/// mishandling would drop a range).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_varchar_key_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_vc");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (k VARCHAR(20) PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT 'k' || lpad(g::text, 6, '0'), g FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("parallel: 4")
        .export_line("chunk_size: 500");
    rig.run_ok();
    assert_eq!(
        count_rows(&rig.out_dir()),
        3000,
        "varchar-key parallel keyset exports every row"
    );
    assert!(
        worker_fanout(&rig.out_dir()) >= 2,
        "varchar key must fan out, not collapse"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Parallel keyset on a DECIMAL key must FAIL LOUD at plan time — the keyset cursor
/// cannot advance past a decimal/numeric, so accepting it would fail MID-RUN after a
/// partial write (silent data risk). The planner rejects it up front with an actionable
/// message; this guards that the rejection stays loud + pre-run (never a mid-run crash).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_decimal_key_rejected_loudly_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_dec");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (dkey DECIMAL(20,0) PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g::decimal, g FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    let err = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: dkey")
        .export_line("parallel: 4")
        .export_line("chunk_size: 500")
        .run_expect_fail();
    assert!(
        err.contains("not a usable keyset key") && err.to_lowercase().contains("decimal"),
        "a decimal keyset key must be rejected loudly at plan time, got: {err}"
    );
    // Nothing was written — the rejection is pre-run, not a mid-run partial.
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Parallel keyset on a BIGINT UNSIGNED key ENTIRELY above i64::MAX (the field
/// unsigned-keyset regime, fast small-N variant of the 10M golden): the boundary
/// literals + paging cursor must round-trip as u64, or rows past i64::MAX are dropped.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_unsigned_above_i64_max_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_u64");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT UNSIGNED PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    // 2000 keys all > i64::MAX (9223372036854775807): id = 10^19 + n*10^6.
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 2000) \
         SELECT CAST(10000000000000000000 AS UNSIGNED) + CAST(n AS UNSIGNED) * 1000000, n FROM seq"
    ))
    .unwrap();
    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 400");
    rig.run_ok();
    assert_eq!(
        count_rows(&rig.out_dir()),
        2000,
        "every u64 key above i64::MAX exported once (no signed-overflow drop)"
    );
    assert!(
        worker_fanout(&rig.out_dir()) >= 2,
        "unsigned key must fan out"
    );
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}

/// Crash after a LATER range commits (range 2 of 4), not just range 0 — resume must
/// still recover every row. Guards the assumption that recovery works regardless of
/// WHICH range's commit the crash follows.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_crash_after_later_range_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_crash2");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let rig = crash_rig(Rig::pg_batch(&format!("public.{table}")));
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:2");
    assert!(
        !crash.status.success(),
        "crash after range 2 must fail run 1"
    );
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 2000,
        "resume after a later-range crash recovers every row"
    );
    assert_eq!(keys, (1..=2000i64).collect::<BTreeSet<i64>>());
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Bug #2 regression: incremental parallel keyset on a TEXT key CONTAINING backslashes,
/// under a server whose global sql_mode includes NO_BACKSLASH_ESCAPES. The boundary
/// sampler (query_scalar) injects the escaped anchor literal; if its connection does not
/// pin sql_mode like the workers' do, `'p\\...'` mis-parses, a sampled boundary lands
/// below the true anchor, and a worker range re-reads already-exported rows -> cross-run
/// DUP. With the sampler pinned (reuse of MysqlSessionGuard), the union is exact.
/// Sets + restores @@global.sql_mode; the restore runs BEFORE the asserts so a failure
/// never leaves the server dirty.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_incremental_survives_no_backslash_escapes_mysql() {
    require_alive(LiveService::Mysql);
    use mysql::prelude::Queryable;
    let table = unique_name("pk_nbs");
    let mut c = mysql_connect();
    let orig: String = c.query_first("SELECT @@global.sql_mode").unwrap().unwrap();
    // The hostile sql_mode must be the SERVER default so rivet's OWN connections inherit
    // it — that needs SUPER / SYSTEM_VARIABLES_ADMIN. Skip where the test user lacks it
    // (the dev stack); this is a real guard on a server the tester controls.
    if c.query_drop("SET GLOBAL sql_mode = CONCAT(@@global.sql_mode, ',NO_BACKSLASH_ESCAPES')")
        .is_err()
    {
        eprintln!("skip: setting @@global.sql_mode needs SUPER (unavailable for the test user)");
        return;
    }
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (k VARCHAR(40) PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    // 1000 text keys each containing a backslash (CHAR(92)): 'p\000001'..'p\001000'.
    c.query_drop(format!(
        "INSERT INTO {table} (k, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT CONCAT('p', CHAR(92), LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 200");
    rig.run_ok();
    let r1 = count_rows(&rig.out_dir());
    // add 500 more keys ABOVE the anchor, then an incremental run past the backslash anchor.
    c.query_drop(format!(
        "INSERT INTO {table} (k, payload) \
         WITH RECURSIVE seq AS (SELECT 1001 n UNION ALL SELECT n+1 FROM seq WHERE n < 1500) \
         SELECT CONCAT('p', CHAR(92), LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();
    rig.run_ok();
    let r2 = count_rows(&rig.out_dir());

    // Restore the global sql_mode + drop BEFORE asserting.
    let _ = c.query_drop(format!(
        "SET GLOBAL sql_mode = '{}'",
        orig.replace('\'', "''")
    ));
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));

    assert_eq!(r1, 1000, "run 1 exports all 1000 backslash-keyed rows");
    assert_eq!(
        r2, 1500,
        "incremental run past a backslash anchor under NO_BACKSLASH_ESCAPES must add EXACTLY \
         the 500 new keys — not dup (>1500) via a mis-parsed sampler boundary, nor lose (<1500)"
    );
}

/// Least-privilege: a batch parallel-keyset export must succeed with a source user that
/// has ONLY SELECT (+ USAGE on the schema) — NEVER SUPER / CREATE / write. rivet's export
/// does only session-scoped SETs (SET LOCAL / SET SESSION, no privilege) + SELECT, so a
/// read-only extraction role is all a user must grant. A failure here means the export
/// reaches for a privilege it should not.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_works_with_a_select_only_source_user_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_ro");
    let role = format!("ro_{}", &table[table.len().saturating_sub(12)..]).replace('-', "_");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    // A least-privilege role: LOGIN + USAGE on the schema + SELECT on the table. Nothing
    // else — the only grants a read-only extractor may assume.
    c.batch_execute(&format!(
        "DROP ROLE IF EXISTS {role};
         CREATE ROLE {role} LOGIN PASSWORD 'ro';
         GRANT USAGE ON SCHEMA public TO {role};
         GRANT SELECT ON public.{table} TO {role};"
    ))
    .unwrap();

    // Run parallel keyset AS the SELECT-only role.
    let ro_url = POSTGRES_URL.replacen("rivet:rivet", &format!("{role}:ro"), 1);
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}")).source_url(&ro_url));
    rig.run_ok();
    let (count, keys, workers) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 3000,
        "a SELECT-only user must fully export via parallel keyset"
    );
    assert_eq!(keys, (1..=3000i64).collect::<BTreeSet<i64>>());
    assert!(
        workers >= 2,
        "fan-out must still happen under a SELECT-only user"
    );

    let _ = c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table}; DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};"
    ));
}

/// THE SEAM the retry guard reads. `decide_export_retry` bails with
/// `BailDuplicateGuard` only when `summary.files_committed > 0`
/// (`src/pipeline/single.rs`), and `record_part` is the sole production site
/// that raises it. Parallel keyset used to `bail!` on a worker error ABOVE that
/// drain, so the guard read ZERO while ranges were already durable — a TRANSIENT
/// worker failure then retried the whole export over parts on disk.
///
/// Why the existing tests could not see it, which is the point of adding this
/// one: `decide_export_retry`'s unit matrix feeds `files_committed` as a
/// parameter (0/2/5) and is correct on every input, while
/// `parallel_keyset_worker_error_aborts_cleanly_postgres` deliberately DELETES
/// the orphan parts before its clean re-run (a vacuous-oracle guard its own
/// comment explains). Neither observes the VALUE one layer hands the other.
/// This test observes exactly that value, at the boundary.
///
/// Measured before the fix: 4 parts on disk, `retry 1/2` and `retry 2/2`, and
/// with a range whose output SHRANK between attempts (a mid-backoff DELETE) the
/// earlier attempt's extra part survived unoverwritten — 751 rows / 750
/// distinct, id 501 duplicated.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_worker_error_still_counts_the_durable_parts_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_wcnt");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}")));
    let out = rig.run_with_env("RIVET_TEST_ERROR_AT", "keyset_parallel_worker:2");
    assert!(
        !out.status.success(),
        "a worker mid-range error must fail the run"
    );

    // The successful ranges DID write parquet — the premise of the whole test.
    let on_disk = files_with_extension(&rig.out_dir(), "parquet").len();
    assert!(
        on_disk > 0,
        "fixture is inert: the surviving workers wrote no parts, so there is \
         nothing for the retry guard to protect"
    );

    // …and the run must SAY so. This is the assertion that goes RED against the
    // pre-fix code: files_committed was 0 with parts already on disk.
    let db = StateDb::next_to_config(&rig.config_path());
    let run_id = db.latest_run_id(rig.export_name());
    let m = db.metrics_row(&run_id);
    assert_eq!(
        m.files_committed,
        Some(on_disk as i64),
        "a failed parallel-keyset run must count the parts it left durable \
         ({on_disk} on disk) — the retry guard reads this and nothing else"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// #200-1: the SAME durable-parts blind spot, one level deeper — page granularity
/// WITHIN a range. The worker-level template above fails a range at its FIRST page
/// (`keyset_parallel_worker:2`), so the failing range writes nothing and every
/// on-disk part belongs to a COMMITTED range — `files_committed == on_disk` holds
/// even with the bug, because the bug only drops the parts of a range that both
/// (a) wrote pages AND (b) never committed.
///
/// This fixture makes a range do exactly that: `keyset_parallel_worker_midrange:2`
/// fires only after range 2 has already made page 0 durable, so range 2 leaves a
/// parquet on disk yet never reaches its checkpoint commit. Pre-fix those parts
/// lived in the worker's `local_parts`, published to the shared count ONLY at
/// range completion — the mid-range `return` dropped them, so `files_committed`
/// under-counted the debris the retry guard reads. RED before the per-page publish:
/// `files_committed` = the committed ranges only, `on_disk` = those + range 2's
/// page 0.
///
/// Needs ≥2 pages per range so page 0 lands before the mid-range fire: 4000 rows /
/// `parallel: 4` = 1000-row ranges over `chunk_size: 500` = 2 pages each.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_midrange_error_counts_pre_failure_page_parts_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_midr");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 4000) g;"
    ))
    .unwrap();
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}")));
    let out = rig.run_with_env("RIVET_TEST_ERROR_AT", "keyset_parallel_worker_midrange:2");
    assert!(
        !out.status.success(),
        "a worker mid-range error must fail the run"
    );

    // The premise: range 2 wrote page 0 to disk BEFORE the mid-range failure, and
    // the committed ranges wrote their pages too. If range 2 wrote nothing this
    // test degenerates into the worker-level one and proves nothing new.
    let per_worker = worker_part_counts(&rig.out_dir());
    let on_disk: usize = per_worker.values().sum();
    assert!(
        per_worker.get("w2").copied().unwrap_or(0) >= 1,
        "fixture is inert for #200-1: range 2 must leave ≥1 durable page BEFORE it \
         fails mid-range, got per-worker parts {per_worker:?}"
    );
    assert!(
        on_disk > per_worker.get("w2").copied().unwrap_or(0),
        "fixture needs committed ranges too, got {per_worker:?}"
    );

    // The seam: files_committed must count EVERY durable part on disk, including
    // range 2's pre-failure page — the retry guard reads this and nothing else.
    // RED pre-fix: files_committed omitted range 2's uncommitted-but-durable page.
    let db = StateDb::next_to_config(&rig.config_path());
    let run_id = db.latest_run_id(rig.export_name());
    let m = db.metrics_row(&run_id);
    assert_eq!(
        m.files_committed,
        Some(on_disk as i64),
        "a mid-range failure must count the pages the failing range already made \
         durable ({on_disk} on disk, per-worker {per_worker:?})"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// Worker mid-range ERROR propagation (the non-crash path): when one worker's source read
/// errors (connection drop / statement timeout), the run must FAIL CLEANLY — collect the
/// error, bail, and finalize NOTHING (no _SUCCESS, no manifest), never a partial success.
/// A clean re-run then recovers every row.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_worker_error_aborts_cleanly_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_werr");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}")));
    // range 2's worker errors mid-range (Err, not a crash).
    let out = rig.run_with_env("RIVET_TEST_ERROR_AT", "keyset_parallel_worker:2");
    assert!(
        !out.status.success(),
        "a worker mid-range error must FAIL the run"
    );
    // No FALSE success: no _SUCCESS marker, and the manifest (written as a FAILED record
    // so gc_orphans can classify + delete its partial parts) is NOT a Success.
    assert!(
        !rig.out_dir().join("_SUCCESS").exists(),
        "a failed run must not write _SUCCESS"
    );
    let mf = rig.out_dir().join("manifest.json");
    if mf.exists() {
        let m: serde_json::Value = serde_json::from_slice(&std::fs::read(&mf).unwrap()).unwrap();
        let status = m["status"].as_str().unwrap_or("").to_lowercase();
        assert_ne!(
            status, "success",
            "a worker-error abort must leave a Failed/Interrupted manifest, not a Success"
        );
    }
    // ISOLATE the clean re-run: the failed run left ranges 0/1/3 as orphan parquet
    // (~75% of the key space). Reading orphans ∪ rerun would let the union hit 1..=2000
    // even if the clean run DROPPED a range — a vacuous oracle. Delete the orphans first,
    // so the completeness check sees ONLY run 2's output.
    for p in files_with_extension(&rig.out_dir(), "parquet") {
        std::fs::remove_file(&p).unwrap();
    }
    // A clean re-run recovers everything: exact count (no dupe/no orphan inflation) AND
    // the full key set — RED if the clean run drops ANY range (0/1/3 included), not just
    // range 2.
    rig.run_ok();
    let (count, keys, _) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 2000,
        "the clean re-run writes exactly 2000 rows (no dupe/orphan)"
    );
    assert_eq!(
        keys,
        (1..=2000i64).collect::<BTreeSet<i64>>(),
        "a clean re-run after a worker-error abort recovers every key"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// CSV row count across every `.csv` part (lines minus the per-file header).
fn csv_row_count(dir: &std::path::Path) -> usize {
    files_with_extension(dir, "csv")
        .iter()
        .map(|p| {
            std::fs::read_to_string(p)
                .map(|s| s.lines().count().saturating_sub(1))
                .unwrap_or(0)
        })
        .sum()
}

/// Parallel keyset writing CSV (not parquet): the text-writer path must fan out and
/// export every row (the runner is format-agnostic, but only parquet was exercised).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_csv_format_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_csv");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    let rig = parallel_keyset(Rig::pg_batch(&format!("public.{table}"))).with_format("csv");
    rig.run_ok();
    assert_eq!(
        csv_row_count(&rig.out_dir()),
        3000,
        "parallel keyset CSV exports every row"
    );
    assert!(
        worker_fanout(&rig.out_dir()) >= 2,
        "CSV parallel keyset must fan out (workers keyed in the part name like parquet)"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// partition_by + keyset is REJECTED LOUDLY at plan time: partitioning rewrites the
/// query into a subquery, which defeats the `table:` index verification keyset needs.
/// Guards that the incompatibility stays a pre-run error (not a silent wrong-plan).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_partition_by_rejected_loudly_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_part");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, grp INT NOT NULL);
         INSERT INTO {table} SELECT g, g % 4 FROM generate_series(1, 3000) g;"
    ))
    .unwrap();
    let err = parallel_keyset(Rig::pg_batch(&format!("public.{table}")))
        .export_line("partition_by: grp")
        .run_expect_fail();
    assert!(
        err.contains("partition_by is not compatible with chunk_by_key"),
        "partition_by + keyset must be rejected loudly at plan time, got: {err}"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Concurrency stress: MORE workers (parallel: 8) + a small chunk_size so each range
/// pages many times, exercising the shared Mutex/Atomic merge (parts, checksums,
/// range_max, rows) under heavier contention than the 4-worker parity tests.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_eight_workers_concurrency_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_conc");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 8000) g;"
    ))
    .unwrap();
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 8")
        .export_line("chunk_size: 250");
    rig.run_ok();
    let (count, keys, workers) = id_set_and_fanout(&rig.out_dir());
    assert_eq!(
        count, 8000,
        "8-worker keyset merges every row once (no race drop/dupe)"
    );
    assert_eq!(keys, (1..=8000i64).collect::<BTreeSet<i64>>());
    assert!(workers >= 4, "8 workers must fan out widely, got {workers}");
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Resume with a CHANGED parallel N: crash a `parallel: 4` checkpoint run, then resume
/// with `parallel: 8` (same export, same state db + out dir). The resume must RELOAD the
/// crashed run's persisted keyset_range rows (the original 4 ranges) rather than re-sample
/// with the new N, and recover every row. Built without the Rig — two configs sharing one
/// tempdir (hence one `.rivet_state.db` + out dir) is the only way to vary N across a
/// crash/resume pair.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_resume_with_changed_worker_count_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pk_resn");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("cfg.yaml");
    let out = dir.path().join("out");
    let write_cfg = |n: usize| {
        let lines = [
            format!("source: {{ type: postgres, url: \"{POSTGRES_URL}\" }}"),
            "exports:".to_string(),
            "  - name: pk_resn".to_string(),
            format!("    table: public.{table}"),
            "    mode: chunked".to_string(),
            "    chunk_by_key: id".to_string(),
            format!("    parallel: {n}"),
            "    chunk_checkpoint: true".to_string(),
            "    chunk_size: 200".to_string(),
            "    format: parquet".to_string(),
            format!(
                "    destination: {{ type: local, path: \"{}/\" }}",
                out.display()
            ),
        ];
        std::fs::write(&cfg, lines.join("\n")).unwrap();
    };
    // Run 1: parallel:4, crash after range 1 commits (durable in keyset_range).
    write_cfg(4);
    let crash = run_rivet_env(
        &["run", "-c", cfg.to_str().unwrap()],
        &[("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:1")],
    );
    assert!(!crash.status.success(), "the crash run must fail");
    // Run 2: parallel:8 — resume must reuse the crashed run's 4 persisted ranges.
    write_cfg(8);
    let resume = run_rivet_env(&["run", "-c", cfg.to_str().unwrap()], &[]);
    assert!(
        resume.status.success(),
        "resume with a changed parallel N must succeed: {}",
        String::from_utf8_lossy(&resume.stderr)
    );
    let (count, keys, _) = id_set_and_fanout(&out);
    assert_eq!(
        count, 2000,
        "resume with a changed worker count recovers every row"
    );
    assert_eq!(keys, (1..=2000i64).collect::<BTreeSet<i64>>());
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}
