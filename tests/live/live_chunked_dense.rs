//! RED-first probe: dense chunking (`chunk_dense: true`) ordinal stability on a
//! NON-UNIQUE chunk column, across all three engines (PG / MySQL / MSSQL).
//!
//! Dense chunking pages by ordinal: each chunk is an INDEPENDENT query of the
//! form `… ROW_NUMBER() OVER (ORDER BY <chunk_column>) AS rn … WHERE rn BETWEEN
//! s AND e` (src/pipeline/chunked/math.rs:53). That `ROW_NUMBER()` carries **no
//! unique tiebreaker**, and — unlike keyset / `chunk_by_key`, which the planner
//! refuses unless the key is a single-column NOT NULL UNIQUE index
//! (src/plan/build.rs:347-359) — dense imposes **no uniqueness requirement** and
//! reaches the no-probe fast path (build.rs:209) on any explicit `chunk_column`.
//!
//! So when `chunk_column` has a tied peer group that straddles an ordinal chunk
//! boundary, the ordinal→physical-row mapping is engine-defined and may differ
//! between the two independent per-chunk queries (most exposed under `parallel:`,
//! where chunks run on separate connections / transactions). A boundary row can
//! then land in BOTH adjacent chunks (DUPLICATE) or NEITHER (silent LOSS).
//!
//! These tests assert the CORRECT behaviour — every seeded id present exactly
//! once at the destination — by INDEPENDENTLY re-reading the output Parquet
//! (not trusting rivet's own row counters). If a test goes RED on any engine,
//! the dense ordinal mapping is unstable there: that is a LIVE data-loss bug and
//! the fix is upstream (append a unique tiebreaker to the dense `ORDER BY` at
//! math.rs:53, and/or refuse dense on a non-unique chunk column at plan time the
//! way keyset does). If it stays GREEN on every engine, the tests remain as a
//! regression guard locking the ordinal→row invariant.
//!
//! VERDICT (2026-06-11): GREEN on PG 16, MySQL 8.0, and SQL Server 2022, in both
//! sequential and `parallel: 4`. On a STATIC table the per-chunk scan order is
//! deterministic, so the missing tiebreaker is currently latent, not a live leak
//! (it could still surface under concurrent writes during a long export, which
//! rivet does not claim a cross-chunk-consistent snapshot against anyway). These
//! tests stay as the destination-reading regression guard the dense path lacked;
//! if any goes RED later, apply the upstream tiebreaker / uniqueness-guard fix.
//!
//! Run: `docker compose up -d postgres mysql mssql && \
//!       cargo test --test live_suite -- --ignored`

use crate::common::*;

use mysql::prelude::Queryable;
use std::collections::BTreeSet;

const ROWS: i64 = 1_000;
const CHUNK: i64 = 300;

// The non-unique `grp` layout (shared by every engine seeder): a single 600-row
// tied peer group positioned to straddle the dense ordinal boundaries at 300/301
// and 600/601 (chunk_size = 300).
//   id 1..200    -> grp = id        (distinct, < 5000)   ordinals ~1..200
//   id 201..800  -> grp = 5000      (TIED, 600 rows)     ordinals ~201..800
//   id 801..1000 -> grp = 5000 + id (distinct, > 5000)   ordinals ~801..1000
// `ORDER BY grp` orders the tied band only by engine whim, so a boundary row
// inside it can be mapped to a different ordinal by two independent per-chunk
// queries.
const GRP_CASE: &str = "CASE WHEN n <= 200 THEN n WHEN n <= 800 THEN 5000 ELSE 5000 + n END";

// ── source-agnostic destination oracle ───────────────────────────────────────

/// Assert the destination holds every seeded id EXACTLY once — re-reading the
/// Parquet, not rivet's counters. Distinguishes LOSS (missing id) from
/// DUPLICATION (count > ROWS) so the failure message names the actual fault.
fn assert_each_id_exactly_once(dir: &std::path::Path, ctx: &str) {
    let ids = duckdb_dir_parquet_ids(dir);
    let set: BTreeSet<i64> = ids.iter().copied().collect();
    let expected: BTreeSet<i64> = (1..=ROWS).collect();

    let missing: Vec<i64> = expected.difference(&set).copied().collect();
    assert!(
        missing.is_empty(),
        "{ctx}: DATA LOSS — {} seeded id(s) absent from the destination Parquet \
         (a dense chunk boundary dropped a tied row; ROW_NUMBER has no tiebreaker, \
         math.rs:53). missing (first 20): {:?}",
        missing.len(),
        &missing.iter().take(20).collect::<Vec<_>>()
    );
    assert_eq!(
        ids.len() as i64,
        ROWS,
        "{ctx}: DATA DUPLICATION — {} physical rows at the destination but {ROWS} seeded; \
         a tied row was emitted in two adjacent dense chunks (unstable ordinal→row mapping, \
         math.rs:53). distinct ids = {}",
        ids.len(),
        set.len()
    );
}

/// Turn an engine's base rig into the shared dense-chunked shape.
fn dense_rig(base: Rig, name: &str, out: &std::path::Path, parallel: usize) -> Rig {
    base.query(&format!("SELECT id, grp FROM {name}"))
        .mode("chunked")
        .export_line("chunk_column: grp")
        .export_line(&format!("chunk_size: {CHUNK}"))
        .export_line("chunk_dense: true")
        .export_line("chunk_checkpoint: true")
        .export_line(&format!("parallel: {parallel}"))
        .export_line("compression: none")
        .dest_path(out.to_path_buf())
}

/// Drive a dense export through its rig and assert exactly-once.
fn run_dense_assert(rig: &Rig, export: &str, out: &std::path::Path, ctx: &str) {
    let run = rig.run_args(&["--export", export]);
    assert!(
        run.status.success(),
        "{ctx}: dense chunked export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_each_id_exactly_once(out, ctx);
}

// ── Postgres ─────────────────────────────────────────────────────────────────

fn seed_pg_tied() -> (String, PgTable) {
    let name = unique_name("rivet_dense_ties");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, grp BIGINT NOT NULL)"
    ))
    .expect("create pg dense-ties table");
    c.batch_execute(&format!(
        "INSERT INTO {name} (id, grp) \
         SELECT n, {GRP_CASE} FROM generate_series(1, {ROWS}) AS n",
    ))
    .expect("seed pg dense-ties rows");
    (name.clone(), PgTable::adopt(name))
}

fn pg_rig(name: &str, out: &std::path::Path, parallel: usize) -> Rig {
    dense_rig(Rig::pg_batch(name), name, out, parallel)
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn dense_ties_pg_sequential_no_loss_no_dup() {
    require_alive(LiveService::Postgres);
    let (name, _g) = seed_pg_tied();
    let out = tempfile::tempdir().unwrap();
    run_dense_assert(
        &pg_rig(&name, out.path(), 1),
        &name,
        out.path(),
        "pg dense seq",
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn dense_ties_pg_parallel_no_loss_no_dup() {
    require_alive(LiveService::Postgres);
    let (name, _g) = seed_pg_tied();
    let out = tempfile::tempdir().unwrap();
    run_dense_assert(
        &pg_rig(&name, out.path(), 4),
        &name,
        out.path(),
        "pg dense parallel:4",
    );
}

// Cross-shape manifest guard (graph-surfaced runner-bypass). The two
// chunk-checkpoint runners were the ONLY batch runners not calling
// guard_manifest_mode, so a chunked-checkpoint export into a prefix that
// already held a CDC manifest would silently overwrite it — destroying the CDC
// export's audit trail. Both runners must now refuse: parallel=1 exercises the
// sequential checkpoint runner, parallel=4 the parallel one.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_checkpoint_refuses_to_clobber_a_cdc_manifest() {
    require_alive(LiveService::Postgres);
    let (name, _g) = seed_pg_tied();
    for parallel in [1usize, 4] {
        let out = tempfile::tempdir().unwrap();
        // A prior CDC run's manifest already sits at the destination prefix.
        std::fs::write(
            out.path().join("manifest.json"),
            br#"{"manifest_version":1,"run_id":"prior-cdc","mode":"cdc","parts":[]}"#,
        )
        .unwrap();
        let rig = pg_rig(&name, out.path(), parallel);
        let run = rig.run_args(&["--export", &name]);
        assert!(
            !run.status.success(),
            "parallel={parallel}: a chunked-checkpoint batch export must REFUSE to \
             overwrite a CDC manifest"
        );
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&run.stdout),
            String::from_utf8_lossy(&run.stderr)
        );
        assert!(
            combined.contains("already holds a 'cdc' manifest"),
            "parallel={parallel}: must name the cross-shape collision; got:\n{combined}"
        );
    }
}

// ── MySQL ──────────────────────────────────────────────────────────────────

fn seed_mysql_tied() -> (String, MysqlTable) {
    let name = unique_name("rivet_dense_ties");
    let mut c = mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {name}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL)"
    ))
    .expect("create mysql dense-ties table");
    // Default cte_max_recursion_depth is 1000; ROWS rows needs > ROWS.
    c.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO {name} (id, grp) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {ROWS}) \
         SELECT n, {GRP_CASE} FROM seq",
    ))
    .expect("seed mysql dense-ties rows");
    (name.clone(), MysqlTable::adopt(name))
}

fn mysql_rig(name: &str, out: &std::path::Path, parallel: usize) -> Rig {
    dense_rig(Rig::mysql_batch(name), name, out, parallel)
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn dense_ties_mysql_sequential_no_loss_no_dup() {
    require_alive(LiveService::Mysql);
    let (name, _g) = seed_mysql_tied();
    let out = tempfile::tempdir().unwrap();
    run_dense_assert(
        &mysql_rig(&name, out.path(), 1),
        &name,
        out.path(),
        "mysql dense seq",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn dense_ties_mysql_parallel_no_loss_no_dup() {
    require_alive(LiveService::Mysql);
    let (name, _g) = seed_mysql_tied();
    let out = tempfile::tempdir().unwrap();
    run_dense_assert(
        &mysql_rig(&name, out.path(), 4),
        &name,
        out.path(),
        "mysql dense parallel:4",
    );
}

// ── SQL Server ───────────────────────────────────────────────────────────────

fn seed_mssql_tied() -> (String, MssqlTable) {
    let name = unique_name("rivet_dense_ties");
    mssql_drop_table(&name);
    mssql_exec(&format!(
        "CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL)"
    ));
    mssql_exec(&format!(
        "WITH seq AS (SELECT CAST(1 AS BIGINT) AS n UNION ALL SELECT n+1 FROM seq WHERE n < {ROWS}) \
         INSERT INTO {name} (id, grp) SELECT n, {GRP_CASE} FROM seq OPTION (MAXRECURSION 0)",
    ));
    (name.clone(), MssqlTable::adopt(name))
}

fn mssql_rig(name: &str, out: &std::path::Path, parallel: usize) -> Rig {
    dense_rig(Rig::mssql_batch(name), name, out, parallel)
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn dense_ties_mssql_sequential_no_loss_no_dup() {
    require_alive(LiveService::Mssql);
    let (name, _g) = seed_mssql_tied();
    let out = tempfile::tempdir().unwrap();
    run_dense_assert(
        &mssql_rig(&name, out.path(), 1),
        &name,
        out.path(),
        "mssql dense seq",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn dense_ties_mssql_parallel_no_loss_no_dup() {
    require_alive(LiveService::Mssql);
    let (name, _g) = seed_mssql_tied();
    let out = tempfile::tempdir().unwrap();
    run_dense_assert(
        &mssql_rig(&name, out.path(), 4),
        &name,
        out.path(),
        "mssql dense parallel:4",
    );
}
