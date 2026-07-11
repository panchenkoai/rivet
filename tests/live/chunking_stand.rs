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
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Total rows across every parquet part under `dir` — for "no row loss" asserts.
fn count_parquet_rows(dir: &std::path::Path) -> usize {
    let mut n = 0;
    for path in files_with_extension(dir, "parquet") {
        let bytes = std::fs::read(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            n += batch.unwrap().num_rows();
        }
    }
    n
}

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

/// Seed a DENSE contiguous integer-PK table (`id` = 1..rows), the well-behaved
/// shape for range chunking / chunk_count.
fn seed_dense(eng: Eng, rows: i64) -> (String, StandCleanup) {
    let table = unique_name("stand_dense");
    let guard = StandCleanup(eng, table.clone());
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
                 INSERT INTO {table} (id, payload) SELECT g, g FROM generate_series(1, {rows}) g;
                 ANALYZE {table};"
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
                 WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {rows}) \
                 SELECT n, n FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (id, payload) \
                 SELECT value, value FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({rows} AS BIGINT))"
            ));
            mssql_exec(&format!("UPDATE STATISTICS {table}"));
        }
    }
    (table, guard)
}

/// `chunk_count: N` divides the key range into EXACTLY N windows → N part files
/// on a dense key. Assert the run succeeds and emits exactly N parquet parts.
fn run_chunk_count(eng: Eng, n: usize) {
    eng.require();
    let (table, _guard) = seed_dense(eng, 4000);
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line(&format!("chunk_count: {n}"));
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "chunk_count run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let files = files_with_extension(&rig.out_dir(), "parquet");
    assert_eq!(
        files.len(),
        n,
        "chunk_count: {n} must emit exactly {n} part files on a dense key; got {}: {files:?}",
        files.len()
    );
}

/// Seed a table keyed by a DATE column `d` spanning `days` distinct days
/// (id BIGINT PK, d DATE NOT NULL), for `chunk_by_days` date-window chunking.
fn seed_dated(eng: Eng, rows: i64, days: i64) -> (String, StandCleanup) {
    let table = unique_name("stand_dated");
    let guard = StandCleanup(eng, table.clone());
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE NOT NULL);
                 INSERT INTO {table} (id, d)
                 SELECT g, DATE '2023-01-01' + ((g % {days}) || ' days')::interval
                 FROM generate_series(1, {rows}) g;
                 ANALYZE {table};"
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE NOT NULL)"
            ))
            .unwrap();
            c.query_drop(format!(
                "SET SESSION cte_max_recursion_depth = {}",
                rows + 10
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (id, d) \
                 WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {rows}) \
                 SELECT n, DATE_ADD('2023-01-01', INTERVAL (n % {days}) DAY) FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE NOT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (id, d) \
                 SELECT value, DATEADD(day, value % {days}, CAST('2023-01-01' AS DATE)) \
                 FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({rows} AS BIGINT))"
            ));
            mssql_exec(&format!("UPDATE STATISTICS {table}"));
        }
    }
    (table, guard)
}

/// `chunk_by_days: 7` on a 35-day span → 5 weekly windows → 5 part files. Assert
/// the run succeeds and emits exactly 5 parts on every engine.
fn run_chunk_by_days(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_dated(eng, 350, 35);
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: d")
        .export_line("chunk_by_days: 7");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "chunk_by_days run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let files = files_with_extension(&rig.out_dir(), "parquet");
    assert_eq!(
        files.len(),
        5,
        "chunk_by_days: 7 over a 35-day span must emit 5 weekly parts; got {}: {files:?}",
        files.len()
    );
}

/// `chunk_by_key` pointed at a NON-unique column (`payload`, no unique index)
/// must REFUSE — an unindexed ORDER BY key would filesort the whole table and a
/// non-unique key drops/dupes rows at a page boundary.
fn run_keyset_non_usable_bail(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_dense(eng, 200);
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_by_key: payload");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "chunk_by_key on a non-unique column must BAIL; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("not a usable keyset key"),
        "bail must be the keyset usable-key refusal; stderr:\n{stderr}"
    );
}

/// Seed a GAPPY-but-not-egregious key: two clusters (id 1..50 and 1001..1050)
/// with a large empty gap between them, 100 rows total. Range chunking at
/// chunk_size 100 → ~11 windows (span 1..1050), several of them EMPTY (in the
/// gap) — below the sparse-guard floor so it runs, exercising the empty-window
/// path. No row may be lost at an empty window boundary.
fn seed_gappy(eng: Eng) -> (String, StandCleanup) {
    let table = unique_name("stand_gappy");
    let guard = StandCleanup(eng, table.clone());
    // id = g for g<=50, else 950+g (so 51..100 → 1001..1050).
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
                 INSERT INTO {table} (id, payload)
                 SELECT CASE WHEN g <= 50 THEN g ELSE 950 + g END, g
                 FROM generate_series(1, 100) g;
                 ANALYZE {table};"
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
                "INSERT INTO {table} (id, payload) \
                 WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 100) \
                 SELECT IF(n <= 50, n, 950 + n), n FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (id, payload) \
                 SELECT IIF(value <= 50, value, 950 + value), value \
                 FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST(100 AS BIGINT))"
            ));
            mssql_exec(&format!("UPDATE STATISTICS {table}"));
        }
    }
    (table, guard)
}

/// Range chunk over a gappy key: empty middle windows must not lose rows and must
/// not false-fail. The run completes and all 100 rows reach the destination.
fn run_range_gappy(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_gappy(eng);
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 100");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "gappy-key range must complete (below the sparse floor); stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        count_parquet_rows(&rig.out_dir()),
        100,
        "all 100 rows must survive the empty middle windows"
    );
}

/// `chunk_size_memory_mb` derives the row-count chunk_size from a byte budget
/// (needs the introspected avg_row_bytes). The run completes with all rows.
fn run_chunk_size_memory_mb(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_dense(eng, 4000);
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size_memory_mb: 1");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "chunk_size_memory_mb run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        count_parquet_rows(&rig.out_dir()),
        4000,
        "the byte-budget-derived chunk plan must export every row"
    );
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

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_chunk_count_mysql() {
    run_chunk_count(Eng::My, 4);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_chunk_count_mssql() {
    run_chunk_count(Eng::Ms, 4);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_chunk_by_days_postgres() {
    run_chunk_by_days(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_chunk_by_days_mysql() {
    run_chunk_by_days(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_chunk_by_days_mssql() {
    run_chunk_by_days(Eng::Ms);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_keyset_non_usable_bail_postgres() {
    run_keyset_non_usable_bail(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_keyset_non_usable_bail_mysql() {
    run_keyset_non_usable_bail(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_range_gappy_mysql() {
    run_range_gappy(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_range_gappy_mssql() {
    run_range_gappy(Eng::Ms);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_chunk_size_memory_mb_postgres() {
    run_chunk_size_memory_mb(Eng::Pg);
}
