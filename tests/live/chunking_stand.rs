//! Engine-agnostic chunking scenario STAND.
//!
//! Declare a scenario once (a table SHAPE + the export CONFIG + the EXPECTed
//! outcome); the stand seeds the shape on each engine, runs rivet, and asserts.
//! The ONLY engine-specific code is the per-engine seeder (`seed_*`) and the
//! per-engine URL/Rig constructor — everything else is shared. This closes the
//! coverage gap the `docs/chunking-matrix.yaml` ledger records: several guards
//! (sparse bail/warn, NULL-keyed bail) had ZERO engine-level tests, only
//! unit coverage inside `src/pipeline/chunked/detect.rs`.
//!
//! First scenario: the sparse-key guard — the exact footgun that shipped. A key
//! whose SPAN vastly exceeds its row count makes range chunking explode into
//! near-empty windows; the planner must refuse (bail) where a scan-free estimate
//! PROVES it (PG/MSSQL) and warn where it can only suspect it (MySQL — no
//! trustworthy estimate).
//!
//! Run: `docker compose up -d postgres mysql mssql && cargo test --test live_suite -- --ignored chunking_stand`.

use crate::common::*;

use mysql::prelude::Queryable;

/// The three SQL engines the stand runs a scenario across. (Mongo pages `_id`
/// and has no BETWEEN-over-span shape, so the sparse guard is n/a there.)
#[derive(Clone, Copy)]
enum Eng {
    Pg,
    My,
    Ms,
}

impl Eng {
    fn require(self) {
        match self {
            Eng::Pg => require_alive(LiveService::Postgres),
            Eng::My => require_alive(LiveService::Mysql),
            Eng::Ms => require_alive(LiveService::Mssql),
        }
    }

    fn rig(self, table: &str) -> Rig {
        match self {
            Eng::Pg => Rig::pg_batch(&format!("public.{table}")),
            Eng::My => Rig::mysql_batch(table),
            Eng::Ms => Rig::mssql_batch(&format!("dbo.{table}")),
        }
    }
}

/// Drops the stand's temp table on scope exit, per engine.
struct StandCleanup(Eng, String);
impl Drop for StandCleanup {
    fn drop(&mut self) {
        match self.0 {
            Eng::Pg => {
                if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                    let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.1), &[]);
                }
            }
            Eng::My => {
                if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
                    && let Ok(mut c) = pool.get_conn()
                {
                    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.1));
                }
            }
            Eng::Ms => mssql_drop_table(&self.1),
        }
    }
}

/// Seed a SPARSE integer-PK table: `rows` rows whose `id` is spread across a
/// span vastly larger than the row count (`id = 1 + i*step`), so range chunking
/// at `chunk_size` produces ~span/chunk_size near-empty windows. `rows` stays
/// ABOVE chunk_size so the small-table Snapshot escape does not pre-empt the
/// range plan. Returns the table name + a cleanup guard.
fn seed_sparse(eng: Eng, rows: i64, step: i64) -> (String, StandCleanup) {
    let table = unique_name("stand_sparse");
    let guard = StandCleanup(eng, table.clone());
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
                 INSERT INTO {table} (id, payload)
                 SELECT 1 + g * {step}, g FROM generate_series(0, {n}) g;
                 ANALYZE {table};",
                n = rows - 1
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL)"
            ))
            .unwrap();
            c.query_drop(format!(
                "SET SESSION cte_max_recursion_depth = {}",
                rows + 10
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (id, payload) \
                 WITH RECURSIVE seq AS (SELECT 0 n UNION ALL SELECT n+1 FROM seq WHERE n < {last}) \
                 SELECT 1 + n * {step}, n FROM seq",
                last = rows - 1
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (id, payload) \
                 SELECT 1 + CAST(value AS BIGINT) * {step}, value \
                 FROM GENERATE_SERIES(CAST(0 AS BIGINT), CAST({last} AS BIGINT))",
                last = rows - 1
            ));
            mssql_exec(&format!("UPDATE STATISTICS {table}"));
        }
    }
    (table, guard)
}

/// Seed a table whose intended chunk key `k` is NULLABLE and actually CONTAINS
/// NULLs (every other row). Range chunking filters `WHERE k BETWEEN min AND max`,
/// which excludes NULL — so those rows would silently vanish. The planner must
/// refuse (`bail_if_null_keyed`). `id` is a NOT NULL PK so the table is otherwise
/// well-formed. Small is fine: the NULL guard fires before chunk generation.
fn seed_nullable_key(eng: Eng, rows: i64) -> (String, StandCleanup) {
    let table = unique_name("stand_nullkey");
    let guard = StandCleanup(eng, table.clone());
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k INT);
                 INSERT INTO {table} (id, k)
                 SELECT g, CASE WHEN g % 2 = 0 THEN NULL ELSE g END
                 FROM generate_series(1, {rows}) g;"
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k INT NULL)"
            ))
            .unwrap();
            c.query_drop(format!(
                "SET SESSION cte_max_recursion_depth = {}",
                rows + 10
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (id, k) \
                 WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {rows}) \
                 SELECT n, IF(n % 2 = 0, NULL, n) FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k INT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (id, k) \
                 SELECT value, IIF(value % 2 = 0, NULL, value) \
                 FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({rows} AS BIGINT))"
            ));
        }
    }
    (table, guard)
}

/// The stand body: seed sparse, run a range plan (`chunk_column: id`,
/// `chunk_size` small enough to blow the span into many windows), assert the
/// engine's expected outcome. PG/MSSQL PROVE sparseness from a scan-free estimate
/// → bail; MySQL cannot (no trustworthy estimate) → warn but run.
fn run_sparse_guard(eng: Eng) {
    eng.require();
    // 5000 rows, step 240 → span ≈ 1.2M; chunk_size 1000 → ~1200 windows vs 5
    // dense (ratio ~240). Above the 1000-window no-estimate warn floor AND the
    // 4x proven-bail ratio, and rows (5000) > chunk_size (1000) so no escape.
    let (table, _guard) = seed_sparse(eng, 5000, 240);

    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);

    match eng {
        // Proven sparse (scan-free estimate) → refuse.
        Eng::Pg | Eng::Ms => {
            assert!(
                !out.status.success(),
                "sparse range must BAIL on this engine; stderr:\n{stderr}"
            );
            assert!(
                stderr.contains("refusing to run a sparse range plan"),
                "bail must be the sparse-guard refusal; stderr:\n{stderr}"
            );
        }
        // Unprovable (no trustworthy estimate) → warn, but run.
        Eng::My => {
            assert!(
                out.status.success(),
                "MySQL sparse range must WARN (not bail) and complete; stderr:\n{stderr}"
            );
            assert!(
                stderr.contains("chunk windows on a range key"),
                "MySQL must emit the sparse WARN; stderr:\n{stderr}"
            );
        }
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_sparse_guard_postgres() {
    run_sparse_guard(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_sparse_guard_mysql() {
    run_sparse_guard(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_sparse_guard_mssql() {
    run_sparse_guard(Eng::Ms);
}

/// NULL-keyed range bail: range-chunking a nullable key with actual NULLs must
/// refuse on every engine — the NULL rows would be silently excluded by BETWEEN.
fn run_null_keyed_bail(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_nullable_key(eng, 200);
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: k")
        .export_line("chunk_size: 50");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "a NULL-keyed range plan must BAIL (BETWEEN drops NULL rows); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("found NULL in chunk_column"),
        "bail must be the NULL-keyed refusal; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_null_keyed_bail_postgres() {
    run_null_keyed_bail(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_null_keyed_bail_mysql() {
    run_null_keyed_bail(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_null_keyed_bail_mssql() {
    run_null_keyed_bail(Eng::Ms);
}
