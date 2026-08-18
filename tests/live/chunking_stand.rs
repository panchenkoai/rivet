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

/// Column names of the first parquet part under `dir` — for schema-shape asserts.
fn parquet_columns(dir: &std::path::Path) -> Vec<String> {
    for path in files_with_extension(dir, "parquet") {
        let bytes = std::fs::read(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        if let Some(Ok(batch)) = reader.into_iter().next() {
            return batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
        }
    }
    vec![]
}

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

    /// The matrix's TIMEZONE-AWARE timestamp column, per engine.
    ///
    /// The names genuinely differ (`created_at_ts` on MySQL, `created_at_tz` on
    /// PostgreSQL and SQL Server), and the value-consumer guard needs to point
    /// its uniqueness gate at THIS column rather than at `id`: an integer key
    /// renders on every build, so a check over it cannot notice a rendering
    /// failure — the guard's own uniqueness leg was inert until it aimed here.
    fn tz_column(self) -> &'static str {
        match self {
            Eng::My => "created_at_ts",
            Eng::Pg | Eng::Ms => "created_at_tz",
        }
    }

    fn rig(self, table: &str) -> Rig {
        match self {
            Eng::Pg => Rig::pg_batch(&format!("public.{table}")),
            Eng::My => Rig::mysql_batch(table),
            Eng::Ms => Rig::mssql_batch(&format!("dbo.{table}")),
        }
    }

    /// The schema-qualified table name to embed in a raw `also_export` query —
    /// same qualification [`Eng::rig`] applies to the main export's `table:`.
    fn qualified(self, table: &str) -> String {
        match self {
            Eng::Pg => format!("public.{table}"),
            Eng::My => table.to_string(),
            Eng::Ms => format!("dbo.{table}"),
        }
    }
}

/// As [`insert_dense_range`], but also fills the `pad` column — for the SEEDING
/// leg of a split fixture, whose giant must carry bytes across its whole span.
/// The grow leg deliberately keeps using the narrow twin: `pad` is NULLable, and
/// the width is only needed for the PRIMING run that sets the split ratio.
fn insert_dense_range_padded(eng: Eng, table: &str, lo: i64, hi: i64, pad_bytes: usize) {
    let (pg, my, ms) = (
        format!("repeat('x', {pad_bytes})"),
        format!("REPEAT('x', {pad_bytes})"),
        format!("REPLICATE('x', {pad_bytes})"),
    );
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "INSERT INTO {table} (id, payload, pad) \
                 SELECT g, g, {pg} FROM generate_series({lo}, {hi}) g"
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "SET SESSION cte_max_recursion_depth = {}",
                hi - lo + 10
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (id, payload, pad) \
                 WITH RECURSIVE seq AS (SELECT {lo} n UNION ALL SELECT n+1 FROM seq WHERE n < {hi}) \
                 SELECT n, n, {my} FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "INSERT INTO {table} (id, payload, pad) \
                 SELECT value, value, {ms} FROM GENERATE_SERIES(CAST({lo} AS BIGINT), CAST({hi} AS BIGINT))"
            ));
        }
    }
}

/// Append `id`s `lo..=hi` (payload = id) to a `(id BIGINT PK, payload INT)` stand
/// table — the source-GROWTH leg of the split-resume scenario, per engine.
fn insert_dense_range(eng: Eng, table: &str, lo: i64, hi: i64) {
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "INSERT INTO {table} (id, payload) SELECT g, g FROM generate_series({lo}, {hi}) g"
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "SET SESSION cte_max_recursion_depth = {}",
                hi - lo + 10
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (id, payload) \
                 WITH RECURSIVE seq AS (SELECT {lo} n UNION ALL SELECT n+1 FROM seq WHERE n < {hi}) \
                 SELECT n, n FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "INSERT INTO {table} (id, payload) \
                 SELECT value, value FROM GENERATE_SERIES(CAST({lo} AS BIGINT), CAST({hi} AS BIGINT))"
            ));
        }
    }
}

/// Seed a GAPPY (but not sparse) `(id BIGINT PK, payload INT)` table at split
/// scale: two dense blocks of `block` rows separated by a `block`-wide hole
/// (`1..=block` and `2*block+1..=3*block`), `2*block` rows over a span of
/// `3*block` (ratio ~1.5, below the sparse floor so the split is not refused).
/// The interior split boundaries land in or beside the hole — the exact shape a
/// re-sample would place differently after growth. Returns the table, cleanup,
/// and the full id set.
fn seed_gappy_split(
    eng: Eng,
    block: i64,
) -> (String, StandCleanup, std::collections::BTreeSet<i64>) {
    // WIDE, for the same reason the dense split fixture is (see `seed_dense_wide`):
    // this is a `_split_` fixture, and a narrow giant does not beat the sibling's
    // fixed connect+plan floor by R=3.0. Widening the dense fixture on 08-17 left
    // this one narrow — and `stand_pool_split_gappy_key_mssql` was one of the four
    // nightly failures the next morning (giant 612 ms vs sibling 403 ms, ratio
    // 1.52). Both blocks are padded, so the giant carries the bytes across the
    // whole span rather than only its first half.
    let (table, guard) = seed_dense_wide(eng, block, SPLIT_PAD_BYTES); // 1..=block
    insert_dense_range_padded(eng, &table, 2 * block + 1, 3 * block, SPLIT_PAD_BYTES);
    let ids: std::collections::BTreeSet<i64> =
        (1..=block).chain(2 * block + 1..=3 * block).collect();
    (table, guard, ids)
}

/// Split+pool crash-resume across a stand shape — the engine-agnostic proof of
/// finding 2 (split-window persistence). `table` holds `original_ids`; a small
/// sibling export makes the giant the clear long pole so `advise_split` realizes
/// the split into the pool. Run 1 splits and crashes a unit mid-way; if `grow`
/// is set the source then GAINS rows `grow.0..=grow.1` (the input that shifts a
/// re-sampled partition); run 2 resumes. The manifest must DECLARE every
/// ORIGINAL id afterwards — read with the orphan-immune `dir_manifest_copy_id_set`
/// (a crash leaves pre-shift orphans a raw read would miscount). Whether the new
/// rows also land depends on which unit held the open window, so only the
/// original-snapshot guarantee is asserted.
fn run_pool_split_resume(
    eng: Eng,
    table: &str,
    original_ids: &std::collections::BTreeSet<i64>,
    grow: Option<(i64, i64)>,
) {
    // Default arm: RANGE chunking (`chunk_column`), crashed mid-chunk via the chunked
    // runner's `chunk_export` error hook.
    run_pool_split_resume_with(
        eng,
        table,
        original_ids,
        grow,
        "chunk_column: id",
        ("RIVET_TEST_ERROR_AT", "chunk_export:1"),
    );
}

/// As [`run_pool_split_resume`] but with the key STRATEGY line and the mid-run crash
/// hook parameterised, so the identical split→crash→(grow)→resume proof runs over BOTH
/// range chunking (`chunk_column` + `chunk_export` error) AND keyset (`chunk_by_key` +
/// the `after_keyset_page` PANIC — the keyset runner has no `chunk_export` hook, and a
/// panic is the hard-crash-with-no-manifest shape the reconstruct fill must survive).
fn run_pool_split_resume_with(
    eng: Eng,
    table: &str,
    original_ids: &std::collections::BTreeSet<i64>,
    grow: Option<(i64, i64)>,
    key_line: &str,
    crash: (&str, &str),
) {
    let n = original_ids.len() as i64;
    let chunk = (n / 4).max(1); // 2 units of n/2 → 2 chunks/pages each → the hook fires
    let sibling_q = format!("SELECT id FROM {} WHERE id <= 100", eng.qualified(table));
    let rig = eng
        .rig(table)
        .mode("chunked")
        .export_line(key_line)
        .export_line(&format!("chunk_size: {chunk}"))
        .export_line("parallel_safe: true")
        .also_export("split_sibling", &sibling_q)
        .also_export_line("parallel_safe: true");
    let cfg = rig.config_path();

    // Prime durations, clear, so the split fires on run 1.
    assert!(
        run_rivet_env(&["apply", cfg.to_str().unwrap(), "--pool", "2"], &[])
            .status
            .success(),
        "priming run must succeed"
    );
    for d in [rig.out_dir(), rig.out_dir_for("split_sibling")] {
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
    }

    // Run 1: split + crash a unit mid-way.
    let crashed = run_rivet_env(
        &["apply", cfg.to_str().unwrap(), "--pool", "2", "--split"],
        &[crash],
    );
    // PRECONDITION before the product assertion. `--split` is purely ADVISORY:
    // `pool::advise_split` declines unless the giant beats the next-longest by
    // more than R (3.0 at the `apply --pool --split` call site in run.rs) on
    // PREDICTED seconds, which come from the priming run's recorded durations.
    // The sibling is 100 rows but still pays a full connect + plan + write, so
    // on a fast engine or a loaded runner the ratio can fall under 3 and NO
    // units are produced — nothing to error, run 1 succeeds, and the assertion
    // below then blames the product for an inert fixture. That is exactly how
    // this read in CI (`stand_pool_split_gappy_key_{mysql,mssql}` and
    // `..._resume_grows_mssql`, red on the 2026-08-16 nightly while green
    // locally, and green on postgres in the same run — a ratio, not a bug).
    //
    // So: check the fixture FIRST, and when it is inert say so with the numbers
    // the advisor actually used, rather than reporting a product failure.
    {
        let db = StateDb::next_to_config(&cfg);
        let runs = db.export_runs();
        // ANY status: this run crashes its units on purpose, so a unit that did
        // its job records a FAILED row, not a successful one.
        let units = runs.iter().filter(|(n, ..)| n.contains('#')).count();
        {
            // The priming run's successes are what the advisor predicted from.
            // ONE entry per export NAME (its longest run): the advisor compares
            // the longest export to the next-longest OTHER export, so a list
            // that repeats one name would report it dominating ITSELF — ratio
            // 1.00 on a giant that in fact dominates 15x (caught RED-proving
            // this guard, 2026-08-16).
            let mut best: std::collections::BTreeMap<String, (i64, i64)> =
                std::collections::BTreeMap::new();
            for (n, st, ms, rows) in &runs {
                if n.contains('#') || st != "success" {
                    continue;
                }
                let e = best.entry(n.clone()).or_insert((0, 0));
                if *ms > e.0 {
                    *e = (*ms, *rows);
                }
            }
            let mut primed: Vec<_> = best
                .into_iter()
                .map(|(n, (ms, rows))| (n, ms, rows))
                .collect();
            primed.sort_by_key(|(_, ms, _)| -*ms);
            let ratio = match primed.as_slice() {
                [(_, a, _), (_, b, _), ..] if *b > 0 => *a as f64 / *b as f64,
                _ => f64::NAN,
            };
            // Report the MARGIN on every run, not only on the failing one. The
            // ratio is a property of the machine as much as the fixture (local
            // 15x, CI 1.52), so a nightly that passes at 3.1 is one slow disk
            // away from the failure this guard exists to explain — and without
            // this line the log says nothing until it is already red.
            eprintln!(
                "split fixture margin: ratio {ratio:.2} vs R=3.0, {units} unit(s), primed {primed:?}"
            );
            assert!(
                units >= 2,
                "FIXTURE INERT, not a product failure: `--split` produced {units} unit(s), so no \
                 unit could error and run 1 was always going to succeed — the assertion below \
                 would blame the product for the fixture's own failure to set up. \
                 `pool::advise_split` declines unless the giant beats the next-longest by more \
                 than R=3.0 on PREDICTED seconds (from the priming run) AND the split lowers the \
                 predicted wall. The priming run measured {primed:?} (ratio {ratio:.2} vs R=3.0). \
                 If the ratio is comfortably past 3, the decline came from another gate — read \
                 `pool::advise_split`. Do not relax the assertion below."
            );
        }
    }

    assert!(
        !crashed.status.success(),
        "run 1 must fail (a unit errored mid-split):\n{}",
        String::from_utf8_lossy(&crashed.stderr)
    );
    let partial = duckdb_total_parquet_rows(&rig.out_dir()) as i64;
    assert!(
        partial < n,
        "run 1 must leave the split INCOMPLETE (a unit crashed), got {partial} of {n}"
    );

    // Optional source growth between the crash and the resume.
    if let Some((lo, hi)) = grow {
        insert_dense_range(eng, table, lo, hi);
    }

    // Run 2: split + resume — must reconstruct the ORIGINAL partition.
    let resumed = run_rivet_env(
        &[
            "apply",
            cfg.to_str().unwrap(),
            "--pool",
            "2",
            "--split",
            "--resume",
        ],
        &[],
    );
    assert!(
        resumed.status.success(),
        "resume run must succeed:\n{}",
        String::from_utf8_lossy(&resumed.stderr)
    );

    let declared = dir_manifest_copy_id_set(&rig.out_dir());
    let missing: Vec<i64> = original_ids
        .iter()
        .copied()
        .filter(|id| !declared.contains(id))
        .collect();
    assert!(
        missing.is_empty(),
        "resume must preserve every ORIGINAL id in the manifest: {} of {n} missing (e.g. {:?})",
        missing.len(),
        &missing[..missing.len().min(8)]
    );
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

/// Seed a table whose intended chunk key `k` is a TEXT/VARCHAR column (a
/// NOT NULL `id` BIGINT PK keeps the table otherwise well-formed). Range
/// chunking derives integer min/max boundaries and slices with `BETWEEN`, so a
/// text key would silently drop every value between two window boundaries — the
/// planner must REFUSE it (#103). Small is fine: the integer-family guard fires
/// at plan time, before any chunk generation.
fn seed_text_keyed(eng: Eng, rows: i64) -> (String, StandCleanup) {
    let table = unique_name("stand_textkey");
    let guard = StandCleanup(eng, table.clone());
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k VARCHAR(20) NOT NULL);
                 INSERT INTO {table} (id, k) SELECT g, 'v' || g FROM generate_series(1, {rows}) g;
                 ANALYZE {table};"
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k VARCHAR(20) NOT NULL)"
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
                 SELECT n, CONCAT('v', n) FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k VARCHAR(20) NOT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (id, k) \
                 SELECT value, 'v' + CAST(value AS VARCHAR(20)) \
                 FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({rows} AS BIGINT))"
            ));
            mssql_exec(&format!("UPDATE STATISTICS {table}"));
        }
    }
    (table, guard)
}

/// Seed a table whose sole unique PK is a `DECIMAL(15,0)` — a keyset key the
/// cursor cannot advance (`extract_last_cursor_value` has no decimal arm). The
/// planner must refuse `chunk_by_key` on it at plan time, not fail mid-run after a
/// partial write (#dogfood).
fn seed_decimal_pk(eng: Eng, rows: i64) -> (String, StandCleanup) {
    let table = unique_name("stand_deckey");
    let guard = StandCleanup(eng, table.clone());
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (dkey DECIMAL(15,0) PRIMARY KEY, v TEXT NOT NULL);
                 INSERT INTO {table} (dkey, v) SELECT g, 'v'||g FROM generate_series(1, {rows}) g;
                 ANALYZE {table};"
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "CREATE TABLE {table} (dkey DECIMAL(15,0) PRIMARY KEY, v VARCHAR(20) NOT NULL)"
            ))
            .unwrap();
            c.query_drop(format!(
                "SET SESSION cte_max_recursion_depth = {}",
                rows + 10
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (dkey, v) \
                 WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {rows}) \
                 SELECT n, CONCAT('v', n) FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (dkey DECIMAL(15,0) PRIMARY KEY, v VARCHAR(20) NOT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (dkey, v) SELECT value, CONCAT('v', value) \
                 FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({rows} AS BIGINT))"
            ));
            mssql_exec(&format!("UPDATE STATISTICS {table}"));
        }
    }
    (table, guard)
}

/// #dogfood: `chunk_by_key` on a DECIMAL PK must BAIL at plan time (the cursor
/// can't read a decimal) — a clean refusal naming the type, NOT a partial write
/// then a mid-run "could not read the key value" failure.
fn run_keyset_decimal_key_bails(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_decimal_pk(eng, 250);
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_by_key: dkey")
        .export_line("chunk_size: 100");
    let out = run_rivet_env(
        &["run", "--config", rig.config_path().to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "a DECIMAL keyset key must BAIL, not run; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("not a usable keyset key") && stderr.to_lowercase().contains("decimal"),
        "the bail must name the TYPE reason (decimal), not just 'unique'; stderr:\n{stderr}"
    );
    assert_eq!(
        count_parquet_rows(&rig.out_dir()),
        0,
        "the refusal must be at PLAN time — zero rows written, no partial write"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_keyset_decimal_key_bails_postgres() {
    run_keyset_decimal_key_bails(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_keyset_decimal_key_bails_mysql() {
    run_keyset_decimal_key_bails(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_keyset_decimal_key_bails_mssql() {
    run_keyset_decimal_key_bails(Eng::Ms);
}

/// Seed a DENSE contiguous integer-PK table (`id` = 1..rows), the well-behaved
/// shape for range chunking / chunk_count.
fn seed_dense(eng: Eng, rows: i64) -> (String, StandCleanup) {
    seed_dense_wide(eng, rows, 0)
}

/// The dense fixture the `apply --pool --split` tests need.
///
/// 200 bytes a row, so the giant's export is dominated by BYTES WRITTEN rather
/// than by how fast the machine gets through 300k narrow ints. That is what
/// makes `pool::advise_split`'s R=3.0 threshold clear on a fast runner as well
/// as a slow laptop — see [`seed_dense_wide`] for the measurements that forced
/// it. The width is the only thing that changes: the id space, `dense_ids`, the
/// grow ranges and every caller stay exactly as they were.
fn seed_dense_wide_for_split(eng: Eng, rows: i64) -> (String, StandCleanup) {
    seed_dense_wide(eng, rows, SPLIT_PAD_BYTES)
}

/// One width for every split fixture. Named rather than repeated so widening it
/// again (if a future runner still lands under R=3.0) is one edit, and so a
/// reader can see the dense and gappy fixtures are deliberately the same shape.
const SPLIT_PAD_BYTES: usize = 200;

/// `seed_dense` with a `pad` column of `pad_bytes` characters.
///
/// The split fixtures need it. `pool::advise_split` declines unless the giant
/// beats the next-longest by more than R=3.0 on PREDICTED SECONDS, and the
/// sibling — 100 rows — costs whatever a connect + plan + tiny write costs on
/// that machine, a FIXED floor that does not shrink with the fixture. Measured
/// on CI 2026-08-17: the giant did 300k narrow rows in 601 ms while the sibling
/// took 201-403 ms, so the ratio landed at 1.52-2.99 against a threshold of 3.0
/// and `--split` produced no units at all. Locally the same fixture reads 15x,
/// because the local giant takes 3.3 s — the ratio was measuring the machine.
///
/// Widening the ROW rather than adding rows is deliberate: export time is
/// dominated by bytes written, so this buys the giant seconds without touching
/// `dense_ids(300_000)`, the grow ranges, or anything else every caller passes,
/// and the seed stays one bulk INSERT.
fn seed_dense_wide(eng: Eng, rows: i64, pad_bytes: usize) -> (String, StandCleanup) {
    let table = unique_name("stand_dense");
    let guard = StandCleanup(eng, table.clone());
    let (pad_col, pad_val_pg, pad_val_my, pad_val_ms) = if pad_bytes == 0 {
        (String::new(), String::new(), String::new(), String::new())
    } else {
        (
            format!(
                // NULLable on purpose: `insert_dense_range` (the GROW step) inserts
                // (id, payload) only, and the width is needed just for the PRIMING
                // run, whose durations set the split ratio — the grow happens after.
                ", pad VARCHAR({pad_bytes})"
            ),
            format!(", repeat('x', {pad_bytes})"),
            format!(", REPEAT('x', {pad_bytes})"),
            format!(", REPLICATE('x', {pad_bytes})"),
        )
    };
    let cols = if pad_bytes == 0 { "" } else { ", pad" };
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL{pad_col});
                 INSERT INTO {table} (id, payload{cols}) SELECT g, g{pad_val_pg} FROM generate_series(1, {rows}) g;
                 ANALYZE {table};"
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL{pad_col})"
            ))
            .unwrap();
            c.query_drop(format!(
                "SET SESSION cte_max_recursion_depth = {}",
                rows + 10
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (id, payload{cols}) \
                 WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {rows}) \
                 SELECT n, n{pad_val_my} FROM seq"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL{pad_col})"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (id, payload{cols}) \
                 SELECT value, value{pad_val_ms} FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({rows} AS BIGINT))"
            ));
            mssql_exec(&format!("UPDATE STATISTICS {table}"));
        }
    }
    (table, guard)
}

/// Seed a MySQL table with a `BIGINT UNSIGNED` PK (Arrow UInt64) — the field-bug
/// shape (0.21.2, prod affiliate DB). `rows` dense low ids PLUS three ids PAST
/// i64::MAX (up to u64::MAX), so a keyset cursor must page through ALL of them —
/// advancing across the signed/unsigned boundary — without loss or truncation.
/// MySQL-only: PostgreSQL has no unsigned integer type, SQL Server has no
/// unsigned BIGINT. Returns the table + cleanup guard; total rows = `rows + 3`.
fn seed_mysql_unsigned_key(rows: i64) -> (String, StandCleanup) {
    let table = unique_name("stand_unsigned");
    let guard = StandCleanup(Eng::My, table.clone());
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT UNSIGNED PRIMARY KEY, payload INT NOT NULL)"
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
    // Three ids PAST i64::MAX (9223372036854775807); u64::MAX = 18446744073709551615.
    // These break any path that reads the key as i64.
    c.query_drop(format!(
        "INSERT INTO {table} (id, payload) VALUES \
         (18446744073709551613, 100), (18446744073709551614, 101), (18446744073709551615, 102)"
    ))
    .unwrap();
    (table, guard)
}

/// `chunk_count: N` divides the key range into EXACTLY N windows → N part files
/// on a dense key. Assert the run succeeds and emits exactly N parquet parts.
fn run_chunk_count(eng: Eng, n: usize) {
    eng.require();
    const ROWS: i64 = 4000;
    let (table, _guard) = seed_dense(eng, ROWS);
    let rig = eng
        .rig(&table)
        .duckdb_oracle()
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
    // The file COUNT is the shape; the ROWS are the point. `chunk_count` divides
    // the key range into N windows, so the failure this knob can produce is a
    // divisor that drops or double-covers a boundary — and that leaves the part
    // count untouched. Until 2026-08-17 nothing anywhere in the tree read a row
    // from a `chunk_count` export, on any engine (audit).
    //
    // DuckDB, not `count_parquet_rows`: the arrow helper decodes with the same
    // crate rivet encodes with, so a fault in that shared path cancels out.
    // Distinct-on-`id` is what separates the two failures — a lost window and a
    // double-covered one can both leave the row total looking plausible.
    rig.assert_complete("id", ROWS, "chunk_count over a dense key");
}

/// Seed a table keyed by a DATE column `d` spanning `days` distinct days
/// (id BIGINT PK, d DATE NOT NULL), for `chunk_by_days` date-window chunking.
fn seed_dated(eng: Eng, rows: i64, days: i64, recent: bool) -> (String, StandCleanup) {
    let table = unique_name("stand_dated");
    let guard = StandCleanup(eng, table.clone());
    // Row `i`'s date, per engine. `recent` → the last `days` days from today
    // (time_window anchors on today, so 2023 dates fall outside its window); else a
    // fixed 2023 span (chunk_by_days only cares about the span width).
    let d = |i: &str| match (eng, recent) {
        (Eng::Pg, true) => format!("CURRENT_DATE - (({i} % {days}) || ' days')::interval"),
        (Eng::Pg, false) => format!("DATE '2023-01-01' + (({i} % {days}) || ' days')::interval"),
        (Eng::My, true) => format!("DATE_SUB(CURDATE(), INTERVAL ({i} % {days}) DAY)"),
        (Eng::My, false) => format!("DATE_ADD('2023-01-01', INTERVAL ({i} % {days}) DAY)"),
        (Eng::Ms, true) => format!("DATEADD(day, -({i} % {days}), CAST(GETDATE() AS DATE))"),
        (Eng::Ms, false) => format!("DATEADD(day, {i} % {days}, CAST('2023-01-01' AS DATE))"),
    };
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE NOT NULL);
                 INSERT INTO {table} (id, d) SELECT g, {} FROM generate_series(1, {rows}) g;
                 ANALYZE {table};",
                d("g")
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
                 SELECT n, {} FROM seq",
                d("n")
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE NOT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (id, d) \
                 SELECT value, {} FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({rows} AS BIGINT))",
                d("value")
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
    const ROWS: i64 = 350;
    let (table, _guard) = seed_dated(eng, ROWS, 35, false);
    let rig = eng
        .rig(&table)
        .duckdb_oracle()
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
    // Five files is the shape; the rows are the point. Date windows are the
    // likeliest place in the tree for an off-by-one — an inclusive/exclusive
    // slip at a week boundary loses a day's rows or emits them twice, and both
    // leave FIVE parts sitting there looking correct. Until 2026-08-17 no test
    // on any engine read a row out of a `chunk_by_days` export (audit).
    //
    // Distinct-on-`id` separates the two: a dropped boundary day shows up in the
    // row total, a double-covered one only in the distinct count.
    rig.assert_complete("id", ROWS, "chunk_by_days over a 35-day span");
}

/// `mode: time_window` — a bounded date scan (`time_column` + `days_window`)
/// anchored on today. A 40-day window over rows dated in the last 30 days
/// captures every one. Also re-exercises the MSSQL DATE-scalar min/max path (the
/// fix in this branch). Asserts the run succeeds and every row lands.
fn run_time_window(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_dated(eng, 300, 30, true);
    let rig = eng
        .rig(&table)
        .mode("time_window")
        .export_line("time_column: d")
        .export_line("days_window: 40");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "time_window run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        count_parquet_rows(&rig.out_dir()),
        300,
        "a 40-day window over rows dated in the last 30 days must export every row"
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

/// #103 (variant 1): a non-integer (text/varchar) explicit `chunk_column` under
/// range chunking must be REFUSED on every SQL engine — range slicing derives
/// integer min/max and `BETWEEN`, silently dropping values between windows.
/// `chunk_count` puts the config in `explicit_chunk_shape` so the small-table
/// escape is bypassed and the integer-family guard is what fires. This proves
/// each engine's `int_columns` introspection query classifies the real column
/// correctly (PG `pg_type`, MySQL `DATA_TYPE`, MSSQL `sys.types` STRING_AGG) —
/// the offline unit test runs on a mock introspection, this on the live catalog.
fn run_non_integer_chunk_column_bail(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_text_keyed(eng, 100);
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: k")
        .export_line("chunk_count: 4");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "a non-integer chunk_column under range chunking must BAIL; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("not an integer-family column"),
        "bail must be the #103 integer-family refusal; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_non_integer_chunk_column_bail_postgres() {
    run_non_integer_chunk_column_bail(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_non_integer_chunk_column_bail_mysql() {
    run_non_integer_chunk_column_bail(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_non_integer_chunk_column_bail_mssql() {
    run_non_integer_chunk_column_bail(Eng::Ms);
}

/// #13 bughunt (a regression THIS branch's #103 fix introduced): an explicit
/// `chunk_column` on an UNQUALIFIED `table:` that lives in a NON-default schema
/// (reached via `search_path`) sent the introspection probe — which hard-codes
/// `public`/`dbo` — into a THROWING `regclass` cast, so the plan HARD-BAILED
/// ("introspection probe failed … Set `chunk_column:` explicitly", which IS set).
/// On main this fast-pathed and exported fine. The fix degrades a probe FAILURE to
/// the fast-path Chunked plan + a loud can't-verify warning, so a formerly-working
/// config keeps working. Verify: all rows export AND the warn fires.
///
/// PG-only: this exact shape (non-default schema + unqualified table + session
/// search_path) is where the probe's hard-coded-schema assumption bites; the URL
/// carries the search_path so nothing mutates the shared role.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_chunked_nondefault_schema_probe_degrades_and_exports_all_postgres() {
    require_alive(LiveService::Postgres);
    let schema = unique_name("nd_schema");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; \
         CREATE TABLE {schema}.t (id BIGINT PRIMARY KEY, v text); \
         INSERT INTO {schema}.t SELECT g, 'r'||g FROM generate_series(1, 40) g;"
    ))
    .unwrap();
    // Session search_path resolves the UNQUALIFIED `table: t` to {schema}.t, but
    // the introspection probe hard-codes `public` and throws — the degrade path.
    let url = format!("{POSTGRES_URL}?options=-c%20search_path%3D{schema},public");
    let rig = Rig::pg_batch("t") // unqualified — NOT public.t
        .source_url(&url)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 10");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    // Clean up the schema before asserting, so a failed assert never leaks it.
    let _ = c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"));
    assert!(
        out.status.success(),
        "a probe-unreachable table with an explicit chunk_column must DEGRADE, not \
         hard-bail (#13); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("introspection probe failed"),
        "the can't-verify degrade warning must fire; stderr:\n{stderr}"
    );
    assert_eq!(
        count_parquet_rows(&rig.out_dir()),
        40,
        "all 40 rows must export via the degrade fast-path"
    );
}

/// PostgreSQL was the one engine with no `chunk_count` EXPORT at all. The
/// chunking matrix pointed its `chunk_count_n x postgres` cell at
/// `roast_small_table_escape_respects_explicit_chunk_count`, which runs `rivet
/// plan` and asserts the resolved strategy — planning, not data. So the knob's
/// row-level behaviour was unproven on PG in both directions: no export ran, and
/// the cell read `test` (audit 2026-08-17).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_chunk_count_postgres() {
    run_chunk_count(Eng::Pg, 4);
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

/// FIELD BUG (0.21.2, prod affiliate DB): keyset over a `BIGINT UNSIGNED` key
/// bailed "keyset could not read the 'id' value … unsupported type" on the first
/// MULTI-PAGE table — extract_last_cursor_value (the cursor read-back) had no
/// UInt arm, so an unsigned id (Arrow UInt64) couldn't advance the cursor. ~37
/// exports failed. The strategy harness never seeded an unsigned key (every
/// fixture was signed BIGINT), and the type harness never ran a strategy — the
/// bug fell in the seam. This closes it end-to-end: a multi-page keyset over an
/// unsigned key with three ids PAST i64::MAX must export EVERY row (none lost, no
/// dup, no truncation), where the pre-fix run bailed on page 1. MySQL-only
/// (unsigned integers don't exist on PG/MSSQL).
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_keyset_unsigned_key_completeness_mysql() {
    require_alive(LiveService::Mysql);
    let dense = 20i64;
    let (table, _guard) = seed_mysql_unsigned_key(dense);
    let expected = (dense + 3) as usize; // dense low ids + 3 past i64::MAX
    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 5"); // < dense → multi-page → forces the cursor advance
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "keyset over a BIGINT UNSIGNED key must succeed (pre-fix it bailed 'could not \
         read the id value … unsupported type'); stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Completeness: every seeded row present exactly once. A dropped page (the
    // cursor failing to advance) reads < expected; a re-read page reads > expected.
    assert_eq!(
        count_parquet_rows(&rig.out_dir()),
        expected,
        "keyset over an unsigned key must export EVERY row, incl. the three ids past i64::MAX"
    );
}

/// #21 bughunt (garbage profile — a WIDE table): the MSSQL introspection probes
/// STRING_AGG the column names, whose result caps at 8000 bytes and raises Msg 9829
/// once the table is wide enough — so EVERY chunked/keyset plan on that table failed
/// to build. A 160-int-column table (30-char names) crosses the cap. With the
/// CONVERT(nvarchar(max), ..) fix the probe returns nvarchar(max) (no cap), so the
/// plan builds and the export completes. RED against the un-CONVERTed query.
#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_wide_table_introspection_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("stand_wide");
    let guard = StandCleanup(Eng::Ms, table.clone());
    // 160 int columns of 30-char names — the STRING_AGG of the names exceeds 8000
    // bytes, the exact shape that raised Msg 9829 before the CONVERT fix.
    let cols: Vec<String> = (0..160)
        .map(|i| format!("col_{i:030} int NOT NULL DEFAULT 0"))
        .collect();
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, {})",
        cols.join(", ")
    ));
    mssql_exec(&format!(
        "INSERT INTO {table} (id) VALUES (1),(2),(3),(4),(5)"
    ));
    let rig = Rig::mssql_batch(&format!("dbo.{table}"))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 2");
    let out = run_rivet_env(
        &["run", "--config", rig.config_path().to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    drop(guard); // drop the table before asserting so a failure never leaks it
    assert!(
        out.status.success(),
        "a 160-column table must not fail the STRING_AGG introspection probe (#21, Msg 9829); \
         stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("9829"),
        "STRING_AGG must not hit the 8000-byte cap: {stderr}"
    );
    assert_eq!(
        count_parquet_rows(&rig.out_dir()),
        5,
        "all 5 rows must export once the plan builds"
    );
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

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_time_window_mysql() {
    run_time_window(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_time_window_mssql() {
    run_time_window(Eng::Ms);
}

// ── cross-config scenarios (docs/cross-config-matrix.yaml) ──────────────────

/// `meta_columns` — the export gains `_rivet_exported_at` + `_rivet_row_hash`.
/// Engine-agnostic post-read enrichment (src/enrich.rs), so one e2e run proves
/// the columns actually land in a real export (previously unit-only).
fn run_meta_columns(eng: Eng) {
    eng.require();
    // The GOLDEN TYPE MATRIX, not `seed_dense`.
    //
    // This used to run over `seed_dense` — two columns, `BIGINT` and `INT` — so
    // it proved the meta columns LAND and nothing about what they contain. The
    // gap was not theoretical: `_rivet_row_hash` could not render a timestamp
    // whose timezone is a NAME (`Some("UTC")` — what a MySQL `TIMESTAMP` maps
    // to), and on a production config that cost 63 of 65 exports on 2026-08-05.
    // The matrix has carried `created_at_ts TIMESTAMP(6)` the whole time; this
    // test simply never looked at it, so four months of green meant only that
    // two integers could be hashed.
    //
    // Measured on the type matrix's real output: `created_at_dt` is a naive
    // TIMESTAMP and `created_at_ts` is TIMESTAMP WITH TIME ZONE — both forms in
    // one export, which is the point.
    let table = "rivet_type_matrix".to_string();
    let rig = eng
        .rig(&table)
        .duckdb_oracle()
        .mode("full")
        .export_line("meta_columns:")
        .export_line("  exported_at: true")
        .export_line("  row_hash: true");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "meta_columns run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let cols = parquet_columns(&rig.out_dir());
    assert!(
        cols.iter().any(|c| c == "_rivet_exported_at"),
        "output must carry _rivet_exported_at; got {cols:?}"
    );
    assert!(
        cols.iter().any(|c| c == "_rivet_row_hash"),
        "output must carry _rivet_row_hash; got {cols:?}"
    );
    // PRESENCE is not content. The column landed for four months while the
    // temporal cells contributed NOTHING to it — before 2026-08-04 an
    // unrenderable value was swallowed (`.ok()` → skip), so two rows differing
    // only in a timestamp hashed identically. Assert the matrix's own temporal
    // columns are in the export AND that the hash actually varies across its
    // rows, which it cannot do if the cells were dropped.
    // Assert the PROPERTY, not a column name: the three engines name their
    // temporal columns differently (`created_at_ts` on MySQL, `created_at_tz`
    // on PostgreSQL), and a name list would pass on one engine and lie on the
    // others. What must hold everywhere is that the export carries a
    // TIMEZONE-AWARE timestamp — the exact shape `row_hash` could not render.
    let dir = rig.oracle_dir();
    let described = duckdb_run_sql_json(&format!(
        "DESCRIBE SELECT * FROM read_parquet('{dir}/**/*.parquet')"
    ));
    let schema = duckdb_parse_describe(&described);
    assert!(
        schema.values().any(|t| t.contains("WITH TIME ZONE")),
        "the type matrix must contribute a TIMEZONE-AWARE timestamp — that is the \
         shape row_hash could not render, and hashing it is the point of this test; \
         got {schema:?}"
    );
    // Read back with DuckDB — a decoder rivet does not share — so the claim is
    // not rivet re-reading its own rendering.
    let rows = duckdb_parquet_rows(dir);
    let distinct = duckdb_parquet_distinct(dir, "_rivet_row_hash");
    assert_eq!(
        distinct, rows,
        "{rows} matrix rows produced {distinct} distinct hashes — equal rows with fewer \
         distinct hashes is exactly what a SWALLOWED cell looks like: the differing \
         column contributed nothing"
    );
}

/// `format: csv` — the export writes a non-empty `.csv` (header + rows), not parquet.
fn run_csv(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_dense(eng, 200);
    let rig = eng.rig(&table).mode("full").with_format("csv");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "csv run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let csvs = files_with_extension(&rig.out_dir(), "csv");
    assert!(!csvs.is_empty(), "csv export must write a .csv file");
    let lines = std::fs::read_to_string(&csvs[0]).unwrap().lines().count();
    assert!(
        lines > 100,
        "csv must have a header + rows; got {lines} lines"
    );
}

/// THE GUARD: every type the fixture carries must survive every mechanism that
/// CONSUMES a cell value — in one export, so adding a column to the matrix
/// exercises all of them at once and none can be forgotten.
///
/// Three consumers exist and each renders a value independently:
///
///   `enrich::row_hash_array`      the `_rivet_row_hash` meta column
///   `ExportSink::track_checksum`  the per-column value checksum `validate` re-reads
///   `quality::check_uniqueness`   the `unique_columns` gate
///
/// Every defect this guard exists for had the same shape: a type the product
/// PRODUCES met a consumer nobody had run it through, and the consumer degraded
/// to a value instead of refusing.
///
///   2026-03-28  `_rivet_row_hash` shipped hashing arrays by their rendered text,
///               so `["a, b"]` and `["a","b"]` collided. Arrays landed six weeks
///               later and nobody asked whether the hash could canonicalise one.
///   2026-08-05  a timestamp with a NAMED timezone — 278 of 1184 columns in a real
///               production database — could not be rendered at all. `row_hash`
///               swallowed it (empty cell in the hash) until 0.24.0 and then
///               refused, costing 63 of 65 exports. The type matrix had carried
///               `created_at_ts TIMESTAMP(6)` the whole time; the row_hash test
///               ran over a two-INTEGER table instead.
///
/// So the guard is not "does the fixture contain the type" — it did — but "does
/// the type reach every consumer". Executable rather than declarative on
/// purpose: a ledger of type × mechanism would need updating by the same person
/// who forgot the mechanism.
fn run_type_matrix_through_every_value_consumer(eng: Eng) {
    eng.require();
    let table = "rivet_type_matrix".to_string();
    let rig = eng
        .rig(&table)
        .duckdb_oracle()
        .mode("full")
        // consumer 1: the row hash, over EVERY column (no declared subset)
        .export_line("meta_columns:")
        .export_line("  row_hash: true")
        // consumer 3: the uniqueness gate, over the primary key
        .export_line("quality:")
        .export_line(&format!("  unique_columns: [{}]", eng.tz_column()))
        .export_line("  unique_max_entries: 100000");
    let cfg = rig.config_path();

    // consumer 2: `--validate` re-reads the parts and re-checks the per-column
    // value checksums the sink recorded while writing.
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap(), "--validate"],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "the type matrix must survive row_hash + value checksum + quality in ONE export. \
         A failure here means a type the product can PRODUCE reached a consumer that \
         cannot handle it.\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Not just "exit 0": the hash must actually DISTINGUISH the rows. A consumer
    // that silently drops a cell still exits 0 — that is precisely how this class
    // hides — so read the output back with a decoder rivet does not share.
    let dir = rig.oracle_dir();
    let rows = duckdb_parquet_rows(dir);
    assert!(
        rows > 1,
        "the matrix must have >1 row to distinguish anything"
    );
    let distinct = duckdb_parquet_distinct(dir, "_rivet_row_hash");
    assert_eq!(
        distinct, rows,
        "{rows} matrix rows hashed to {distinct} distinct values — a collision over \
         distinct rows means a consumer swallowed the cells that differ"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres with the golden seed"]
fn stand_type_matrix_every_consumer_postgres() {
    run_type_matrix_through_every_value_consumer(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql with the golden seed"]
fn stand_type_matrix_every_consumer_mysql() {
    run_type_matrix_through_every_value_consumer(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql with the golden seed"]
fn stand_type_matrix_every_consumer_mssql() {
    run_type_matrix_through_every_value_consumer(Eng::Ms);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_meta_columns_postgres() {
    run_meta_columns(Eng::Pg);
}

// Per ENGINE, not one standing in for three. `meta_columns` is engine-agnostic
// enrichment, which is why this ran on PostgreSQL alone — but the TYPE it must
// survive is not: a MySQL `TIMESTAMP` and a SQL Server `DATETIME2` resolve
// through different mappings to reach `Some("UTC")`, and the production failure
// that motivated this test was on MySQL, the one engine it did not cover.

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_meta_columns_mysql() {
    run_meta_columns(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_meta_columns_mssql() {
    run_meta_columns(Eng::Ms);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_format_csv_mssql() {
    run_csv(Eng::Ms);
}

/// `source.environment: local` → the tuning profile defaults to `fast`; the run
/// summary names the env-derived profile on stderr. Source-level, so raw YAML
/// (the Rig has no environment knob). PG is covered in live_catalog_hints.
fn run_environment_profile(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_dense(eng, 50);
    let (src, tbl_ref) = match eng {
        Eng::My => (
            format!("source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\n  environment: local"),
            table.clone(),
        ),
        Eng::Ms => (
            format!(
                "source:\n  type: mssql\n  url: \"{MSSQL_URL}\"\n  tls:\n    \
                 accept_invalid_certs: true\n  environment: local"
            ),
            format!("dbo.{table}"),
        ),
        Eng::Pg => unreachable!("PG environment→profile is covered in live_catalog_hints"),
    };
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "{src}\nexports:\n  - name: env_prof\n    table: {tbl_ref}\n    mode: full\n    \
         format: parquet\n    destination: {{ type: local, path: {out} }}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_with_warn_log(&["run", "-c", cfg.to_str().unwrap()]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "env-profile run failed; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("fast") && stderr.contains("environment: local"),
        "expected the env-derived fast profile on stderr; got:\n{stderr}"
    );
}

/// `compression:` codec matrix — every codec (zstd/snappy/gzip/none) writes a
/// parquet that reads back with all rows. The Arrow reader decompresses each
/// codec natively, so this needs no external reader.
fn run_codec_matrix(eng: Eng) {
    eng.require();
    let (table, _guard) = seed_dense(eng, 200);
    for codec in ["zstd", "snappy", "gzip", "none"] {
        let rig = eng
            .rig(&table)
            .mode("full")
            .export_line(&format!("compression: {codec}"));
        let cfg = rig.config_path();
        let out = run_rivet_env(
            &["run", "--config", cfg.to_str().unwrap()],
            &[("RUST_LOG", "warn")],
        );
        assert!(
            out.status.success(),
            "codec `{codec}` run must succeed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            count_parquet_rows(&rig.out_dir()),
            200,
            "codec `{codec}` must round-trip all rows"
        );
    }
}

/// The source YAML block + the table reference for `eng` (for raw-YAML tests that
/// the Rig can't render — cloud destinations, environment).
fn source_block(eng: Eng, table: &str) -> (String, String) {
    match eng {
        Eng::Pg => (
            format!("source:\n  type: postgres\n  url: \"{POSTGRES_URL}\""),
            format!("public.{table}"),
        ),
        Eng::My => (
            format!("source:\n  type: mysql\n  url: \"{MYSQL_URL}\""),
            table.to_string(),
        ),
        Eng::Ms => (
            format!(
                "source:\n  type: mssql\n  url: \"{MSSQL_URL}\"\n  tls:\n    accept_invalid_certs: true"
            ),
            format!("dbo.{table}"),
        ),
    }
}

/// Count `.parquet` objects fake-gcs holds under `bucket/prefix` (its JSON list API).
fn count_gcs_parquet(bucket: &str, prefix: &str) -> usize {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect("127.0.0.1:4443").unwrap();
    let req = format!(
        "GET /storage/v1/b/{bucket}/o?prefix={prefix} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).unwrap();
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    resp.matches(".parquet").count()
}

/// `destination: gcs` (fake-gcs) — the export lands a parquet in the bucket.
fn run_dest_gcs(eng: Eng) {
    eng.require();
    require_alive(LiveService::FakeGcs);
    let (table, _guard) = seed_dense(eng, 100);
    let (src, tbl) = source_block(eng, &table);
    let bucket = "rivet-qa-stand-gcs";
    ensure_gcs_bucket(bucket);
    let prefix = unique_name("stand_gcs");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "{src}\nexports:\n  - name: cg\n    table: {tbl}\n    mode: full\n    format: parquet\n    \
         destination:\n      type: gcs\n      bucket: {bucket}\n      prefix: {prefix}\n      \
         endpoint: {FAKE_GCS_ENDPOINT}\n      allow_anonymous: true\n"
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "gcs run failed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        count_gcs_parquet(bucket, &prefix) >= 1,
        "fake-gcs bucket must hold >=1 parquet under {prefix}"
    );
    assert_eq!(
        fake_gcs_parquet_total_rows(bucket, &prefix),
        100,
        "all rows: the downloaded gcs parquet must hold every seeded row (presence is not content)"
    );
}

/// `destination: s3` (MinIO) — the export lands a parquet in the bucket (mc ls).
fn run_dest_s3(eng: Eng) {
    eng.require();
    require_alive(LiveService::Minio);
    let (table, _guard) = seed_dense(eng, 100);
    let (src, tbl) = source_block(eng, &table);
    let bucket = "rivet-qa-stand-s3";
    ensure_minio_bucket(bucket);
    let prefix = unique_name("stand_s3");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "{src}\nexports:\n  - name: cs\n    table: {tbl}\n    mode: full\n    format: parquet\n    \
         destination:\n      type: s3\n      bucket: {bucket}\n      prefix: {prefix}\n      \
         region: us-east-1\n      endpoint: {MINIO_ENDPOINT}\n      \
         access_key_env: RIVET_TEST_MINIO_AK\n      secret_key_env: RIVET_TEST_MINIO_SK\n"
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[
            ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
            ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
            ("AWS_EC2_METADATA_DISABLED", "true"),
            ("RUST_LOG", "warn"),
        ],
    );
    assert!(
        out.status.success(),
        "s3 run failed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
         mc ls --recursive local/{bucket}/{prefix} 2>/dev/null"
    );
    let ls = std::process::Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c", &script])
        .output()
        .expect("mc ls");
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.matches(".parquet").count() >= 1,
        "minio must hold >=1 parquet under {prefix}; got:\n{listing}"
    );
    assert_eq!(
        minio_parquet_total_rows(bucket, &prefix),
        100,
        "all rows: the downloaded s3 parquet must hold every seeded row (presence is not content)"
    );
}

/// The azure twin of [`run_dest_s3`] — the destination matrix's last `gap:`.
///
/// The `CloudDestination` path is shared with S3/GCS and proven there, so what
/// this adds is the AZURE-specific leg: the account/key/endpoint config shape,
/// the blob naming, and a read-back over the store's own list+get. The cell asked
/// for CONTENT rather than object presence, so it sums the downloaded parquet's
/// rows through the shared `azure_parquet_total_rows` — the same reader
/// `live_azure_multipart.rs` uses, not a second definition of delivered.
fn run_dest_azure(eng: Eng) {
    eng.require();
    require_alive(LiveService::Azurite);
    let (table, _guard) = seed_dense(eng, 100);
    let (src, tbl) = source_block(eng, &table);
    let container = unique_name("stand-az").to_lowercase().replace('_', "-");
    ensure_azure_container(&container);
    let prefix = unique_name("stand_az");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "{src}\nexports:\n  - name: ca\n    table: {tbl}\n    mode: full\n    format: parquet\n    \
         destination:\n      type: azure\n      bucket: {container}\n      prefix: {prefix}\n      \
         account_name: {AZURITE_ACCOUNT}\n      account_key_env: RIVET_TEST_AZURITE_KEY\n      \
         endpoint: {AZURITE_ENDPOINT}\n"
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[
            ("RIVET_TEST_AZURITE_KEY", AZURITE_KEY),
            ("RUST_LOG", "warn"),
        ],
    );
    assert!(
        out.status.success(),
        "azure run failed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let blobs = azure_blob_names(&container, &prefix);
    assert!(
        blobs.iter().any(|b| b.ends_with(".parquet")),
        "azurite must hold >=1 parquet under {prefix}; got: {blobs:?}"
    );
    assert_eq!(
        azure_parquet_total_rows(&container, &prefix),
        100,
        "all rows: the downloaded azure blobs must hold every seeded row (presence is not content)"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql azurite"]
fn stand_dest_azure_mysql() {
    run_dest_azure(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres azurite"]
fn stand_dest_azure_postgres() {
    run_dest_azure(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql fake-gcs"]
fn stand_dest_gcs_mysql() {
    run_dest_gcs(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql fake-gcs"]
fn stand_dest_gcs_mssql() {
    run_dest_gcs(Eng::Ms);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql minio"]
fn stand_dest_s3_mysql() {
    run_dest_s3(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql minio"]
fn stand_dest_s3_mssql() {
    run_dest_s3(Eng::Ms);
}

// ── Mongo cross-config (Mongo is not a SQL `Eng`; its config shape differs) ──

const MONGO_PORT: u16 = 27017;

/// Seed a fresh Mongo db with `n` int-`_id` docs; returns the db name.
fn mongo_seed(n: i64) -> String {
    let db = unique_name("stand_mg");
    MongoTest::connect(MONGO_PORT, &db).seed_int_id("c", n);
    db
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn stand_format_csv_mongo() {
    require_alive(LiveService::Mongo);
    let db = mongo_seed(200);
    let rig = Rig::mongo_batch("c")
        .source_url(&MongoTest::url(MONGO_PORT, &db))
        .with_format("csv");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "mongo csv run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let csvs = files_with_extension(&rig.out_dir(), "csv");
    assert!(!csvs.is_empty(), "mongo csv export must write a .csv");
    let lines = std::fs::read_to_string(&csvs[0]).unwrap().lines().count();
    assert!(
        lines > 100,
        "mongo csv must have header + rows; got {lines}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn stand_compression_codecs_mongo() {
    require_alive(LiveService::Mongo);
    let db = mongo_seed(200);
    for codec in ["zstd", "snappy", "gzip", "none"] {
        let rig = Rig::mongo_batch("c")
            .source_url(&MongoTest::url(MONGO_PORT, &db))
            .export_line(&format!("compression: {codec}"));
        let cfg = rig.config_path();
        let out = run_rivet_env(
            &["run", "--config", cfg.to_str().unwrap()],
            &[("RUST_LOG", "warn")],
        );
        assert!(
            out.status.success(),
            "mongo codec `{codec}` run must succeed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            count_parquet_rows(&rig.out_dir()),
            200,
            "mongo codec `{codec}` must round-trip all docs"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo fake-gcs"]
fn stand_dest_gcs_mongo() {
    require_alive(LiveService::Mongo);
    require_alive(LiveService::FakeGcs);
    let db = mongo_seed(100);
    let url = MongoTest::url(MONGO_PORT, &db);
    let bucket = "rivet-qa-stand-gcs";
    ensure_gcs_bucket(bucket);
    let prefix = unique_name("stand_gcs_mg");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mongo\n  url: \"{url}\"\nexports:\n  - name: cg\n    table: c\n    \
         mode: full\n    format: parquet\n    destination:\n      type: gcs\n      bucket: {bucket}\n      \
         prefix: {prefix}\n      endpoint: {FAKE_GCS_ENDPOINT}\n      allow_anonymous: true\n"
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );
    assert!(
        out.status.success(),
        "mongo gcs run failed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        count_gcs_parquet(bucket, &prefix) >= 1,
        "fake-gcs must hold >=1 parquet under {prefix}"
    );
    assert_eq!(
        fake_gcs_parquet_total_rows(bucket, &prefix),
        100,
        "all rows: the downloaded gcs parquet must hold every seeded row (presence is not content)"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo minio"]
fn stand_dest_s3_mongo() {
    require_alive(LiveService::Mongo);
    require_alive(LiveService::Minio);
    let db = mongo_seed(100);
    let url = MongoTest::url(MONGO_PORT, &db);
    let bucket = "rivet-qa-stand-s3";
    ensure_minio_bucket(bucket);
    let prefix = unique_name("stand_s3_mg");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mongo\n  url: \"{url}\"\nexports:\n  - name: cs\n    table: c\n    \
         mode: full\n    format: parquet\n    destination:\n      type: s3\n      bucket: {bucket}\n      \
         prefix: {prefix}\n      region: us-east-1\n      endpoint: {MINIO_ENDPOINT}\n      \
         access_key_env: RIVET_TEST_MINIO_AK\n      secret_key_env: RIVET_TEST_MINIO_SK\n"
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[
            ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
            ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
            ("AWS_EC2_METADATA_DISABLED", "true"),
            ("RUST_LOG", "warn"),
        ],
    );
    assert!(
        out.status.success(),
        "mongo s3 run failed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
         mc ls --recursive local/{bucket}/{prefix} 2>/dev/null"
    );
    let ls = std::process::Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c", &script])
        .output()
        .expect("mc ls");
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.matches(".parquet").count() >= 1,
        "minio must hold >=1 parquet under {prefix}; got:\n{listing}"
    );
    assert_eq!(
        minio_parquet_total_rows(bucket, &prefix),
        100,
        "all rows: the downloaded s3 parquet must hold every seeded row (presence is not content)"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_compression_codecs_mysql() {
    run_codec_matrix(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_compression_codecs_mssql() {
    run_codec_matrix(Eng::Ms);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_environment_profile_mysql() {
    run_environment_profile(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_environment_profile_mssql() {
    run_environment_profile(Eng::Ms);
}

// ─── range bounds that cannot be read must not become an empty export ────────

/// A range-chunked export whose min/max the planner cannot read as `i64` must
/// NOT report success having written nothing.
///
/// `detect::detect` reads the window bounds as
/// `src.query_scalar(min|max)?.and_then(|s| s.trim().parse::<i64>().ok()).unwrap_or(0)`
/// (src/pipeline/chunked/detect.rs:316-323). Three different inputs land on the
/// same `0`, and only the first is legitimate:
///
///   1. the scalar is NULL — the table really is empty, one empty window is right;
///   2. the text does not parse as `i64` — a DECIMAL/NUMERIC/float key;
///   3. `query_scalar` cannot render the type at all — PostgreSQL's
///      (src/source/postgres/mod.rs) tries i64, i32, f64, timestamp, date, uuid,
///      String and falls through to `Ok(None)`; NUMERIC matches no arm.
///
/// With min = max = 0 the plan is the single window `(0, 0)` and the runner
/// emits `WHERE col BETWEEN 0 AND 0`. PostgreSQL compares numeric to integer
/// happily, so the query is VALID and returns nothing: the export writes no
/// parts and reports `status: success`, exit 0.
///
/// Measured on this stand, `numeric(20,0)` PK with 100 rows:
///   `chunk_column `id` range 0 .. 0` → `0 rows  0 files  0 B` → `status: success`
/// One hundred rows of a hundred, gone, with a green run.
///
/// PER-ENGINE, and the matrix is what established it: only PostgreSQL goes RED.
/// MySQL and SQL Server return the DECIMAL bound as the text `"1"`, which parses
/// as `i64` and chunks correctly — they are the CONTROL arms here, not padding.
/// PostgreSQL is alone in case 3: its `query_scalar` renders no numeric OID at
/// all, so the bound never reaches the parser. Stating this as "decimal keys are
/// broken" would have been wrong on two engines out of three; the engine axis is
/// what made the claim precise enough to act on.
///
/// The plan-time guard does not save it: `build.rs` only verifies the column is
/// integer-family when introspection SUCCEEDED, and the `query:`-form path warns
/// and emits the Chunked plan anyway. The warning is also wrong about the
/// damage — it says "silently drops rows between windows" when the outcome is
/// every row.
///
/// Two things in the same file show this is an oversight, not a decision. Sixty
/// lines above, the DATE branch hard-errors on both a NULL bound and an
/// unparseable one (detect.rs:240-266). And the shared, error-RETURNING
/// `parse_scalar_i64` (src/scalar.rs:52) is used at detect.rs:25 for the
/// `COUNT(*)` — where a wrong value is harmless — and not for the bounds, where
/// it is catastrophic.
///
/// The existing unit test `integer_range_null_minmax_collapses_to_zero_zero`
/// pins case 1 with `null(), null()`, which is correct for an EMPTY table. No
/// test covers a NON-empty table whose bounds cannot be read — that is the hole
/// this fills.
// AUDIT-RED chunk-bounds-collapse: unreadable min/max → `unwrap_or(0)` → one `BETWEEN 0 AND 0` window → 0 of N rows exported, status success, exit 0. Asserts CORRECT behavior; expected to FAIL until fixed.
fn run_unreadable_range_bounds_must_not_export_nothing(eng: Eng) {
    eng.require();
    const ROWS: i64 = 100;
    let (table, _guard) = seed_decimal_pk(eng, ROWS);

    // `query:` rather than `table:` on purpose: with `table:` the planner
    // introspects the column type and refuses at plan time, which is the
    // behaviour we WANT. The curated-query form is what `rivet init` emits for
    // every non-keyset chunked export, and it is the shape that reaches the
    // runner unchecked.
    let rig = eng
        .rig(&table)
        .mode("chunked")
        .query(&format!("SELECT dkey, v FROM {table}"))
        .export_line("chunk_column: dkey")
        .export_line("chunk_size: 10");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );

    // Either outcome is acceptable — refuse loudly, or export the rows. What
    // must never happen is the third: exit 0 having written nothing.
    if !out.status.success() {
        return; // refused: correct
    }
    // DuckDB, not the parquet crate rivet wrote with: a symmetric read/write
    // fault cannot be seen by the reader that shares the writer's code.
    let got = duckdb_total_parquet_rows(&rig.out_dir()) as i64;
    assert_eq!(
        got,
        ROWS,
        "range-chunked export of {ROWS} rows on a {} DECIMAL key reported SUCCESS but wrote \
         {} row(s). The bounds could not be parsed as i64, so both collapsed to 0 and the plan \
         became the single window `BETWEEN 0 AND 0`. A run that exits 0 having exported nothing \
         is the worst of the three possible outcomes: refusing would be safe, exporting would be \
         correct, and this is neither.\nstderr:\n{}",
        match eng {
            Eng::Pg => "PostgreSQL numeric",
            Eng::My => "MySQL decimal",
            Eng::Ms => "SQL Server decimal",
        },
        got,
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_unreadable_range_bounds_must_not_export_nothing_postgres() {
    run_unreadable_range_bounds_must_not_export_nothing(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_unreadable_range_bounds_must_not_export_nothing_mysql() {
    run_unreadable_range_bounds_must_not_export_nothing(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_unreadable_range_bounds_must_not_export_nothing_mssql() {
    run_unreadable_range_bounds_must_not_export_nothing(Eng::Ms);
}

// ─── a PARTIAL unique index is not a unique key ──────────────────────────────

/// Seed the soft-delete shape: a column carrying duplicates, plus a unique index
/// that only covers the live rows.
///
/// `dups` rows share one key value and are all soft-deleted, so the partial index
/// permits them; `live` rows have distinct keys and `deleted_at IS NULL`. On
/// MySQL — which cannot express a partial index at all — the closest honest
/// equivalent is a plain NON-unique index, which is the control this matrix
/// needs: the probe must refuse that key.
fn seed_partial_unique(eng: Eng, dups: i64, live: i64) -> (String, StandCleanup) {
    let table = unique_name("stand_partuk");
    let guard = StandCleanup(eng, table.clone());
    match eng {
        Eng::Pg => {
            let mut c = pg_connect();
            c.batch_execute(&format!(
                "CREATE TABLE {table} (k TEXT NOT NULL, deleted_at TIMESTAMPTZ, v TEXT NOT NULL);
                 INSERT INTO {table} SELECT 'dup', now(), 'd'||g FROM generate_series(1,{dups}) g;
                 INSERT INTO {table} SELECT 'u'||lpad(g::text,4,'0'), NULL, 'l'||g
                   FROM generate_series(1,{live}) g;
                 CREATE UNIQUE INDEX {table}_live ON {table} (k) WHERE deleted_at IS NULL;
                 ANALYZE {table};"
            ))
            .unwrap();
        }
        Eng::My => {
            let mut c = mysql_connect();
            c.query_drop(format!(
                "CREATE TABLE {table} (k VARCHAR(64) NOT NULL, deleted_at DATETIME NULL, \
                 v VARCHAR(32) NOT NULL, KEY {table}_k (k))"
            ))
            .unwrap();
            c.query_drop(format!(
                "SET SESSION cte_max_recursion_depth = {}",
                dups + live + 10
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (k, deleted_at, v) \
                 WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n < {dups}) \
                 SELECT 'dup', NOW(), CONCAT('d', n) FROM s"
            ))
            .unwrap();
            c.query_drop(format!(
                "INSERT INTO {table} (k, deleted_at, v) \
                 WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n < {live}) \
                 SELECT CONCAT('u', LPAD(n, 4, '0')), NULL, CONCAT('l', n) FROM s"
            ))
            .unwrap();
        }
        Eng::Ms => {
            mssql_exec(&format!(
                "CREATE TABLE {table} (k VARCHAR(64) NOT NULL, deleted_at DATETIME2 NULL, \
                 v VARCHAR(32) NOT NULL)"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (k, deleted_at, v) SELECT 'dup', SYSDATETIME(), \
                 CONCAT('d', value) FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({dups} AS BIGINT))"
            ));
            mssql_exec(&format!(
                "INSERT INTO {table} (k, deleted_at, v) \
                 SELECT CONCAT('u', RIGHT('0000' + CAST(value AS VARCHAR(8)), 4)), NULL, \
                 CONCAT('l', value) FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST({live} AS BIGINT))"
            ));
            // A FILTERED unique index — SQL Server's spelling of a partial index.
            mssql_exec(&format!(
                "CREATE UNIQUE INDEX {table}_live ON {table} (k) WHERE deleted_at IS NULL"
            ));
            mssql_exec(&format!("UPDATE STATISTICS {table}"));
        }
    }
    (table, guard)
}

/// A keyset key backed only by a PARTIAL (filtered) unique index must not be
/// accepted — the index does not make the key unique over the rows being read.
///
/// Keyset's entire correctness argument is that the key is GLOBALLY unique, so
/// `WHERE k > <last> ORDER BY k LIMIT n` cannot skip a row sharing the boundary
/// key. The introspection that is supposed to supply that guarantee asks
/// `i.indisunique AND i.indnkeyatts = 1 AND a.attnotnull` on PostgreSQL
/// (src/source/postgres/mod.rs:380) and `i.is_unique = 1 AND c.is_nullable = 0`
/// on SQL Server (src/source/mssql/mod.rs:1132). Neither excludes a PARTIAL /
/// FILTERED index — `i.indpred IS NOT NULL` on PG, `i.has_filter = 1` on MSSQL —
/// and PG additionally never checks `i.indisvalid`.
///
/// The shape is not exotic; it is the standard soft-delete index:
///   `CREATE UNIQUE INDEX ON users (email) WHERE deleted_at IS NULL`
/// Duplicates are legal among the soft-deleted rows, so a run of them straddles
/// a page boundary and everything past the first page is skipped.
///
/// Measured on this stand (postgres, 50 duplicate + 50 distinct, page size 10):
///   `rivet check` → `Strategy: keyset(k, size=10)`, `Verdict: ACCEPTABLE`
///   `rivet run`   → `status: success`, 60 of 100 rows
/// and the reason the loss survives every ordinary check: the duplicate key
/// contributes exactly ONE page (10 of its 50 rows), so all 51 DISTINCT key
/// values are present. A `count(DISTINCT k)` comparison against the source
/// matches perfectly while 40 rows are gone.
///
/// MySQL is the CONTROL arm, not padding: it cannot express a partial index, so
/// its `NON_UNIQUE = 0` genuinely means globally unique and the equivalent seed
/// (a plain non-unique index) must be REFUSED. The guard is real on the one
/// engine that never needed it.
// AUDIT-RED keyset-partial-unique: a partial/filtered unique index is accepted as a keyset key; duplicates past the first page are dropped, status success. Asserts CORRECT behavior; expected to FAIL until fixed.
fn run_partial_unique_index_is_not_a_keyset_key(eng: Eng) {
    eng.require();
    const DUPS: i64 = 50;
    const LIVE: i64 = 50;
    let (table, _guard) = seed_partial_unique(eng, DUPS, LIVE);

    let rig = eng
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("chunk_size: 10");
    let cfg = rig.config_path();
    let out = run_rivet_env(
        &["run", "--config", cfg.to_str().unwrap()],
        &[("RUST_LOG", "warn")],
    );

    // Refusing is the correct outcome — the key is not unique over the read set.
    if !out.status.success() {
        return;
    }
    // If it ran, it must have read EVERY row. DuckDB, not the parquet crate
    // rivet wrote with.
    let got = duckdb_total_parquet_rows(&rig.out_dir()) as i64;
    assert_eq!(
        got,
        DUPS + LIVE,
        "keyset over a key backed only by a PARTIAL/FILTERED unique index reported SUCCESS with \
         {} of {} rows. {} duplicate rows share one key; the seek emits one page of them and then \
         asks for `k > <that key>`, dropping the rest. The loss is invisible to a DISTINCT check \
         — every distinct key value is still present — so counts of distinct values agree with \
         the source while whole rows are gone.\nstderr:\n{}",
        got,
        DUPS + LIVE,
        DUPS,
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_partial_unique_index_is_not_a_keyset_key_postgres() {
    run_partial_unique_index_is_not_a_keyset_key(Eng::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_partial_unique_index_is_not_a_keyset_key_mysql() {
    run_partial_unique_index_is_not_a_keyset_key(Eng::My);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_partial_unique_index_is_not_a_keyset_key_mssql() {
    run_partial_unique_index_is_not_a_keyset_key(Eng::Ms);
}

// ─── Split+pool crash-resume across engines, on GOOD and JUNK stand data ───────
//
// Finding 2 (split-window persistence) is engine-agnostic in the code but the
// RESUME mechanism is per-engine (PG slot-free chunk checkpoint, MySQL, MSSQL
// from-LSN-free chunk checkpoint), so it is proven empirically on each. GOOD
// data = a dense contiguous key (`seed_dense`); JUNK data = a gappy key with a
// hole the interior split boundaries fall into (`seed_gappy_split`). BOTH the
// dense and the gappy runs GROW the source between crash and resume — the input
// that shifts a re-sampled partition and drops the crashed unit's original range.
// The growth is what makes these go RED against the finding-2 mutant: on an
// UNCHANGED source the re-sample (`sample_key_boundaries`, pure over the row set)
// reproduces the persisted partition byte-for-byte, so a no-growth resume cannot
// tell reconstruct from re-sample and the test would be vacuous.
//
// 300k rows so the giant's scan time DECISIVELY dominates fixed per-export overhead
// and `advise_split` reliably realizes the split, matching the proven-stable size of
// the `pool_split_resume_*` toxiproxy tests. A smaller giant is mostly fixed overhead
// (its predicted duration does not clear 3× the sibling's), so the split — and thus
// the crash the test needs — becomes timing-flaky across engines/CI runners.

fn dense_ids(n: i64) -> std::collections::BTreeSet<i64> {
    (1..=n).collect()
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_pool_split_resume_grows_postgres() {
    Eng::Pg.require();
    let (table, _g) = seed_dense_wide_for_split(Eng::Pg, 300_000);
    run_pool_split_resume(
        Eng::Pg,
        &table,
        &dense_ids(300_000),
        Some((300_001, 450_000)),
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_pool_split_resume_grows_mysql() {
    Eng::My.require();
    let (table, _g) = seed_dense_wide_for_split(Eng::My, 300_000);
    run_pool_split_resume(
        Eng::My,
        &table,
        &dense_ids(300_000),
        Some((300_001, 450_000)),
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_pool_split_resume_grows_mssql() {
    Eng::Ms.require();
    let (table, _g) = seed_dense_wide_for_split(Eng::Ms, 300_000);
    run_pool_split_resume(
        Eng::Ms,
        &table,
        &dense_ids(300_000),
        Some((300_001, 450_000)),
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_pool_split_gappy_key_postgres() {
    Eng::Pg.require();
    let (table, _g, ids) = seed_gappy_split(Eng::Pg, 150_000);
    run_pool_split_resume(Eng::Pg, &table, &ids, Some((450_001, 525_000)));
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_pool_split_gappy_key_mysql() {
    Eng::My.require();
    let (table, _g, ids) = seed_gappy_split(Eng::My, 150_000);
    run_pool_split_resume(Eng::My, &table, &ids, Some((450_001, 525_000)));
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_pool_split_gappy_key_mssql() {
    Eng::Ms.require();
    let (table, _g, ids) = seed_gappy_split(Eng::Ms, 150_000);
    run_pool_split_resume(Eng::Ms, &table, &ids, Some((450_001, 525_000)));
}

// ─── Split+pool crash-resume on a KEYSET key (chunk_by_key), across engines ─────
//
// The runner-bypass check the review demanded: `chunk_by_key` (keyset) IS splittable
// (splittable_key returns Some for it — only CDC and cursor_column/incremental are
// refused), and the split window is applied at the SHARED build_plan seam
// (wrap_key_range), so it composes with the keyset runner exactly as with range
// chunking. These prove keyset pool-split works END-TO-END across a crash (split →
// crash → grow → resume → every original id declared), per engine via the Rig + DuckDB
// manifest oracle.
//
// SCOPE (honest): these do NOT isolate the finding-2 reconstruct mutant. The single
// keyset runner has only PANIC hooks (`after_keyset_page`), no returning-error hook
// like the chunked runner's `chunk_export`; a panic kills the whole pool process with
// both giant units mid-page, so NO completed giant unit survives to misalign against a
// re-sample — with reconstruct disabled the resume simply re-runs both fresh and still
// completes (verified: the mutant stays GREEN here). The finding-2 reconstruct
// guarantee is RUNNER-AGNOSTIC (it operates on the manifests, not the runner) and is
// RED-proven by the unit tests `reconstruct_{covers_a_leading,fills_an_interior}_
// adjacent_crash_*`; the RANGE stand tests above (`stand_pool_split_resume_grows_*`,
// `stand_pool_split_gappy_key_*`) are the live finding-2 guards that DO bite the mutant.
//
// A TRAILING adjacent crash needs NO extra guard: it lowers `max_pos`, so the reconstruct's
// tail unit becomes an OPEN `(b, None]` window baked into `base_query` as `WHERE key > b` (no
// upper bound). The crashed unit's PERSISTED ranges record `ceil = None` for their last range
// (a split unit is non-incremental → floor/ceil are None; the window lives in `base_query`, not
// the range endpoints), so on resume that last range re-runs `WHERE key > last_bound` under the
// WIDENED base_query and naturally covers everything up to the current max — the widened tail is
// complete, not dropped. (A post-0.24.3 bughunt finding claimed this dropped `(b', None]` by
// assuming the persisted last `hi = b'`; it is `None`, and the widened base_query covers the top
// — verified by reading `partition_ranges` + the `(None, None)` floor/ceil for split units.)

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn stand_pool_split_keyset_recovers_a_crash_postgres() {
    Eng::Pg.require();
    let (table, _g) = seed_dense_wide_for_split(Eng::Pg, 300_000);
    run_pool_split_resume_with(
        Eng::Pg,
        &table,
        &dense_ids(300_000),
        Some((300_001, 450_000)),
        "chunk_by_key: id",
        ("RIVET_TEST_PANIC_AT", "after_keyset_page:1"),
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn stand_pool_split_keyset_recovers_a_crash_mysql() {
    Eng::My.require();
    let (table, _g) = seed_dense_wide_for_split(Eng::My, 300_000);
    run_pool_split_resume_with(
        Eng::My,
        &table,
        &dense_ids(300_000),
        Some((300_001, 450_000)),
        "chunk_by_key: id",
        ("RIVET_TEST_PANIC_AT", "after_keyset_page:1"),
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn stand_pool_split_keyset_recovers_a_crash_mssql() {
    Eng::Ms.require();
    let (table, _g) = seed_dense_wide_for_split(Eng::Ms, 300_000);
    run_pool_split_resume_with(
        Eng::Ms,
        &table,
        &dense_ids(300_000),
        Some((300_001, 450_000)),
        "chunk_by_key: id",
        ("RIVET_TEST_PANIC_AT", "after_keyset_page:1"),
    );
}

/// `apply --pool --split` into a CLOUD prefix — the combination with zero
/// coverage, and the one where every silent-loss class this repo has paid for
/// meets at once.
///
/// Locally, `stand_pool_split_*` prove split (and split+resume) on a filesystem.
/// On a bucket the same run has three extra ways to lose data, all already
/// bitten here: split UNITS write into ONE shared prefix (part-name collision),
/// each unit writes its own manifest SIDECAR there (the fixed-name clobber that
/// under-counted 30 parts as 55 rows), and the read-back has historically been a
/// DIFFERENT reader from the local one (the cloud oracle that counted every
/// object under the prefix, orphans included, and read 2000 rows from 1000).
///
/// So the oracle is deliberately NOT cloud-specific: the prefix is PULLED whole
/// and graded by `dir_manifest_copy_id_set`, the identical function the local
/// tests use. One definition of "what was delivered", not two that drift.
///
/// `crash` injects a unit failure into run 1; the resume then has to reconstruct
/// the original partition with run 1's ORPHAN parts already sitting in the
/// bucket — unmanifested objects a prefix-wide reader would happily count.
fn pool_split_cloud(crash: Option<(&str, &str)>) {
    Eng::Pg.require();
    require_alive(LiveService::Minio);
    const ROWS: i64 = 300_000;
    let (table, _g) = seed_dense_wide_for_split(Eng::Pg, ROWS);

    let bucket = "rivet-qa-parity";
    ensure_minio_bucket(bucket);
    let prefix = unique_name("pool_split_cloud");

    let sibling_q = format!(
        "SELECT id FROM {} WHERE id <= 100",
        Eng::Pg.qualified(&table)
    );
    let rig = Eng::Pg
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line(&format!("chunk_size: {}", ROWS / 4))
        .export_line("parallel_safe: true")
        .also_export("split_sibling", &sibling_q)
        .also_export_line("parallel_safe: true")
        .dest_s3(bucket, &prefix, MINIO_ENDPOINT);
    let cfg = rig.config_path();
    let creds = [
        ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
        ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ];

    // Prime: `advise_split` predicts from RECORDED durations, so a first run must
    // exist before `--split` can decide anything.
    let prime = run_rivet_env(&["apply", cfg.to_str().unwrap(), "--pool", "2"], &creds);
    assert!(
        prime.status.success(),
        "priming run failed:\n{}",
        String::from_utf8_lossy(&prime.stderr)
    );

    let mut env: Vec<(&str, &str)> = creds.to_vec();
    if let Some(c) = crash {
        env.push(c);
    }
    let run1 = run_rivet_env(
        &["apply", cfg.to_str().unwrap(), "--pool", "2", "--split"],
        &env,
    );

    // PRECONDITION, before any product assertion: `--split` is ADVISORY, and on a
    // machine where the giant does not beat the sibling by R=3.0 it declines and
    // produces no units at all — then the assertion below would pass while
    // grading a plain pool run. (The same guard the local siblings carry, for the
    // same reason: it fired for real on CI at ratio 1.52.)
    let units = {
        let db = StateDb::next_to_config(&cfg);
        db.export_runs()
            .iter()
            .filter(|(n, ..)| n.contains('#'))
            .count()
    };
    assert!(
        units >= 2,
        "FIXTURE INERT: `--split` produced {units} unit(s), so nothing was written by two \
         writers into one prefix and the union below proves nothing about the class"
    );

    if crash.is_some() {
        assert!(
            !run1.status.success(),
            "run 1 must FAIL (a unit errored mid-split) — a green run here means the crash \
             hook never fired and the resume below has nothing to reconstruct:\n{}",
            String::from_utf8_lossy(&run1.stderr)
        );
        let resumed = run_rivet_env(
            &[
                "apply",
                cfg.to_str().unwrap(),
                "--pool",
                "2",
                "--split",
                "--resume",
            ],
            &creds,
        );
        assert!(
            resumed.status.success(),
            "resume run must succeed:\n{}",
            String::from_utf8_lossy(&resumed.stderr)
        );
    } else {
        assert!(
            run1.status.success(),
            "split run failed:\n{}",
            String::from_utf8_lossy(&run1.stderr)
        );
    }

    // The GIANT's sub-prefix only. The class under test is several split UNITS
    // writing into ONE export's prefix; the sibling export has its own prefix and
    // its own `_SUCCESS`, so pulling both would collide on marker names and grade
    // a population the assertion does not describe.
    let pulled = tempfile::tempdir().unwrap();
    let giant_prefix = format!("{prefix}/{}", rig.export_name());
    let objects = minio_pull_prefix(bucket, &giant_prefix, pulled.path());
    assert!(
        objects > 0,
        "nothing was pulled from s3://{bucket}/{giant_prefix} — the run reported success, so an \
         empty prefix means the destination wiring, not the data"
    );
    // MEASURED, not assumed. This variant was written expecting the crashed run to
    // leave ORPHANS — parts written before the unit died, named by no manifest —
    // and the first run of the assertion disproved it: 9 parts pulled, all 9
    // DECLARED, zero unmanifested. The resume re-declares what attempt 1 had
    // already written rather than stranding it.
    //
    // So the check is inverted into the property that measurement found, because
    // it is worth keeping: a resumed split must not leave unmanifested objects in
    // a bucket. They cost storage forever, they are what `gc_orphans` then has to
    // reason about, and a prefix-wide reader counts them as delivered rows.
    if crash.is_some() {
        let declared: std::collections::BTreeSet<String> =
            files_with_extension(pulled.path(), "json")
                .iter()
                .filter(|p| {
                    p.file_name()
                        .map(|n| n.to_string_lossy().starts_with("manifest-"))
                        .unwrap_or(false)
                })
                .filter_map(|p| std::fs::read(p).ok())
                .filter_map(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                .flat_map(|v| {
                    v.get("parts")
                        .and_then(serde_json::Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                })
                .filter_map(|part| {
                    part.get("path")
                        .and_then(serde_json::Value::as_str)
                        .map(|p| p.rsplit('/').next().unwrap_or(p).to_string())
                })
                .collect();
        let parts = files_with_extension(pulled.path(), "parquet");
        let orphans: Vec<String> = parts
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            })
            .filter(|n| !declared.contains(n))
            .collect();
        assert!(
            parts.len() >= 2 && !declared.is_empty(),
            "fixture inert: {} part(s) pulled, {} declared — nothing to check",
            parts.len(),
            declared.len()
        );
        assert!(
            orphans.is_empty(),
            "the resumed split left {} unmanifested part(s) in the bucket (e.g. {:?}) — a \
             crashed attempt's parts must be re-declared by the resume, not stranded: an \
             orphan is billed forever and reads as delivered to any prefix-wide counter",
            orphans.len(),
            orphans.iter().take(3).collect::<Vec<_>>()
        );
    }

    let ids = dir_manifest_copy_id_set(pulled.path());
    // Report the SHAPE of the gap, never the sets: a 300k-element set printed on
    // failure buries the one number a reader needs under screenfuls of ids.
    let expected = dense_ids(ROWS);
    let missing: Vec<i64> = expected.difference(&ids).take(5).copied().collect();
    let extra: Vec<i64> = ids.difference(&expected).take(5).copied().collect();
    assert!(
        ids == expected,
        "every source id must be DECLARED by a manifest copy in the cloud prefix: {} of \
         {ROWS} present, {objects} object(s) pulled. First missing: {missing:?}; unexpected: \
         {extra:?}. A gap here is the split units overwriting each other's parts or each \
         other's manifest sidecar — the shapes that survive every row count because the \
         surviving artifacts stay self-consistent. (RED-proven: a non-run-unique manifest \
         copy name leaves 150001 of 300000.)",
        ids.len()
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres minio"]
fn stand_pool_split_into_one_cloud_prefix_loses_nothing_postgres() {
    pool_split_cloud(None);
}

/// The same prefix, entered TWICE — once by a run that died mid-split.
///
/// The crash is what makes this different from its sibling above: run 1 leaves
/// ORPHAN parts in the bucket (written, never manifested), and the resume writes
/// its own parts and manifest copies beside them. `dir_manifest_copy_id_set` is
/// orphan-immune by construction — it reads only what a manifest DECLARES — which
/// is exactly the property a prefix-wide object count does not have, and exactly
/// why the cloud read-back once reported 2000 rows for a 1000-row table.
#[test]
#[ignore = "live: requires docker compose up -d postgres minio"]
fn stand_pool_split_cloud_crash_then_resume_declares_every_id_postgres() {
    pool_split_cloud(Some(("RIVET_TEST_ERROR_AT", "chunk_export:1")));
}
