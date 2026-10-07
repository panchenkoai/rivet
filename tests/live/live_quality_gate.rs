//! The `quality:` gate on EVERY runner (ADR-0028, amendment 2026-10-02).
//!
//! Each SQL case exports a 20-row table whose `v` is unique except that row 8
//! repeats row 13's value. The two copies sit in different MIDDLE chunks / pages
//! / ranges, so the run fails only if every sink's measurements reach the ledger
//! and the seam grades the merge: exit 3 with the shared failure text, exactly as
//! on `single`. Parallel-Mongo exports one `document` column, which is never
//! null or duplicated, so its case trips `row_count_min` instead.

use crate::common::*;

const DUPLICATE: &str = "column 'v': 1 duplicate values out of 20 rows";

/// Export a table with one duplicated `v` under `unique_columns: [v]`; return the run.
fn pg_duplicate_run(tag: &str, text_key: bool, mode: &str, lines: &[&str]) -> std::process::Output {
    require_alive(LiveService::Postgres);
    let table = unique_name(tag);
    let (key, key_val) = if text_key {
        ("k TEXT PRIMARY KEY", "'k' || lpad(g::text, 4, '0')")
    } else {
        ("k BIGINT PRIMARY KEY", "g")
    };
    // Row 8 (chunk 2 of 4, page 3 of 7) repeats row 13 (chunk 3, page 5).
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} ({key}, v INT NOT NULL);
         INSERT INTO {table} SELECT {key_val}, CASE WHEN g = 8 THEN 13 ELSE g END
         FROM generate_series(1, 20) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());

    let export = unique_name(&format!("{tag}_exp"));
    let out = tempfile::tempdir().unwrap();
    let mut rig = Rig::pg_batch(&table)
        .mode(mode)
        .export_named(&export)
        .export_line("quality:")
        .export_line("  unique_columns: [v]")
        .export_line("  unique_max_entries: 1000")
        .dest_path(out.path().to_path_buf());
    for l in lines {
        rig = rig.export_line(l);
    }
    rig.run_args(&["--export", &export])
}

fn assert_failed_as_single_does(runner: &str, code: Option<i32>, stderr: &[u8], detail: &str) {
    let stderr = String::from_utf8_lossy(stderr);
    assert_eq!(
        code,
        Some(3),
        "{runner}: a quality violation must exit 3 (data integrity); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("quality check(s) failed") && stderr.contains(detail),
        "{runner}: the failure must carry the shared contract and `{detail}`; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_on_the_single_runner() {
    let r = pg_duplicate_run("qg_single", false, "full", &[]);
    assert_failed_as_single_does("single", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_keyset_pages() {
    let lines = ["chunk_by_key: k", "chunk_size: 3"];
    let r = pg_duplicate_run("qg_keyset", true, "chunked", &lines);
    assert_failed_as_single_does("keyset", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_parallel_keyset_ranges() {
    let lines = ["chunk_by_key: k", "chunk_size: 3", "parallel: 4"];
    let r = pg_duplicate_run("qg_keyset_par", true, "chunked", &lines);
    assert_failed_as_single_does("keyset-parallel", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_chunks() {
    let lines = ["chunk_column: k", "chunk_size: 5"];
    let r = pg_duplicate_run("qg_chunked", false, "chunked", &lines);
    assert_failed_as_single_does("chunked", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_parallel_chunks() {
    let lines = ["chunk_column: k", "chunk_size: 5", "parallel: 2"];
    let r = pg_duplicate_run("qg_chunked_par", false, "chunked", &lines);
    assert_failed_as_single_does("chunked-parallel", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_checkpoint_chunks() {
    let lines = ["chunk_column: k", "chunk_size: 5", "chunk_checkpoint: true"];
    let r = pg_duplicate_run("qg_ckpt", false, "chunked", &lines);
    assert_failed_as_single_does("chunked-checkpoint", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_parallel_checkpoint_chunks() {
    let lines = [
        "chunk_column: k",
        "chunk_size: 5",
        "chunk_checkpoint: true",
        "parallel: 2",
    ];
    let r = pg_duplicate_run("qg_ckpt_par", false, "chunked", &lines);
    assert_failed_as_single_does(
        "chunked-checkpoint-parallel",
        r.status.code(),
        &r.stderr,
        DUPLICATE,
    );
}

/// 24 rows over `parallel: 4` keyset ranges of 6 rows, paged by 3: every range ends on an EMPTY page.
/// Row 8 (range 2) repeats row 13's `v` (range 3), so the run must fail as `single` does.
fn parallel_keyset_duplicate_run(engine: SqlEngine, tag: &str) -> std::process::Output {
    engine.alive();
    let (k, v) = (engine.col("k"), engine.col("v"));
    let i = engine.int64();
    let (table, _guard) = engine.create(tag, &format!("{k} {i} PRIMARY KEY, {v} {i} NOT NULL"));
    let rows: Vec<String> = (1..=24)
        .map(|g| format!("({g}, {})", if g == 8 { 13 } else { g }))
        .collect();
    engine.exec(&format!("INSERT INTO {table} VALUES {}", rows.join(", ")));
    let export = unique_name(&format!("{tag}_exp"));
    engine
        .rig(&table)
        .mode("chunked")
        .export_named(&export)
        .export_line("chunk_by_key: k")
        .export_line("chunk_size: 3")
        .export_line("parallel: 4")
        .export_line("quality:")
        .export_line("  unique_columns: [v]")
        .export_line("  unique_max_entries: 1000")
        .run_args(&["--export", &export])
}

const DUPLICATE_24: &str = "column 'v': 1 duplicate values out of 24 rows";

#[test]
#[ignore = "live: requires docker compose mysql"]
fn quality_gate_fails_a_duplicate_across_parallel_keyset_ranges_ending_empty_mysql() {
    let r = parallel_keyset_duplicate_run(SqlEngine::Mysql, "qg_kpar_my");
    assert_failed_as_single_does("keyset-parallel", r.status.code(), &r.stderr, DUPLICATE_24);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn quality_gate_fails_a_duplicate_across_parallel_keyset_ranges_ending_empty_mssql() {
    let r = parallel_keyset_duplicate_run(SqlEngine::Mssql, "qg_kpar_ms");
    assert_failed_as_single_does("keyset-parallel", r.status.code(), &r.stderr, DUPLICATE_24);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn quality_gate_fails_a_duplicate_across_parallel_keyset_ranges_ending_empty_oracle() {
    let r = parallel_keyset_duplicate_run(SqlEngine::Oracle, "qg_kpar_ora");
    assert_failed_as_single_does("keyset-parallel", r.status.code(), &r.stderr, DUPLICATE_24);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn quality_gate_fails_a_short_mongo_parallel_export() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mquality");
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id("bench", 4000);
    let rig = Rig::mongo_batch("bench")
        .source_url(&MongoTest::url(27017, &db))
        .mongo("page_size: 1000")
        .export_line("parallel: 4")
        .export_line("quality:")
        .export_line("  row_count_min: 5000");
    let r = rig.run_args(&[]);
    assert_failed_as_single_does(
        "mongo-parallel",
        r.status.code(),
        &r.stderr,
        "row_count 4000 below minimum 5000",
    );
    m.drop_database();
}

// ── A refused run keeps refusing: a refusal is never the evidence that lifts it ──

/// The gate a refused-run scenario trips, with its source-side breakage and remedy.
#[derive(Clone, Copy, Debug)]
enum Gate {
    /// `unique_columns: [v]` over a table whose row 8 repeats row 13's `v`.
    Duplicate,
    /// `null_ratio_max: { n: 0.1 }` over a table whose first six `n` are NULL.
    Nulls,
    /// `on_schema_drift: fail` after a clean baseline run, a new column and new rows.
    Drift,
}

/// What one run left behind, re-read from the destination and the state store.
#[derive(Clone, Debug, PartialEq)]
struct Cycle {
    exit: Option<i32>,
    refusal_named: bool,
    success_marker: bool,
    delivered_rows: usize,
    runs: Vec<String>,
    cursor: Option<String>,
}

const ROWS: i64 = 24;

/// Sorted `(id, v)` of the parts a Success manifest declares.
fn declared_id_v(rig: &Rig) -> Vec<(i64, i64)> {
    let cell = |b: &arrow::record_batch::RecordBatch, col: &str, i: usize| -> i64 {
        let a = b.column_by_name(col).unwrap_or_else(|| panic!("{col}"));
        arrow::util::display::array_value_to_string(a, i)
            .unwrap()
            .parse()
            .unwrap_or_else(|e| panic!("{col} is an integer: {e}"))
    };
    let mut rows = Vec::new();
    for b in rig.read_declared_parts() {
        for i in 0..b.num_rows() {
            rows.push((cell(&b, "id", i), cell(&b, "v", i)));
        }
    }
    rows.sort();
    rows
}

/// Re-read one finished run: exit, the refusal text, `_SUCCESS`, declared rows, the run ledger and the cursor.
fn observe(rig: &Rig, out: &std::process::Output, detail: &str, incremental: bool) -> Cycle {
    let (runs, cursor) = run_statuses_and_cursor(&rig.config_path(), rig.export_name());
    Cycle {
        exit: out.status.code(),
        refusal_named: String::from_utf8_lossy(&out.stderr).contains(detail),
        success_marker: rig.out_dir().join("_SUCCESS").is_file(),
        delivered_rows: declared_id_v(rig).len(),
        runs,
        cursor: cursor.filter(|_| incremental),
    }
}

/// Refuse three times running, then apply the gate's own remedy: every refusal must leave what the
/// first one left, and the remedy run must deliver the source.
fn assert_a_refused_run_keeps_refusing(engine: SqlEngine, tag: &str, shape: &[&str], gate: Gate) {
    engine.alive();
    let (id, v, n) = (engine.col("id"), engine.col("v"), engine.col("n"));
    let i = engine.int64();
    let (table, _guard) = engine.create(
        tag,
        &format!("{id} {i} PRIMARY KEY, {v} {i} NOT NULL, {n} {i} NULL"),
    );
    let seed = |ids: std::ops::RangeInclusive<i64>| {
        let rows: Vec<String> = ids
            .map(|g| {
                let dup = matches!(gate, Gate::Duplicate) && g == 8;
                let null = matches!(gate, Gate::Nulls) && g <= 6;
                format!(
                    "({g}, {}, {})",
                    if dup { 13 } else { g },
                    if null { "NULL" } else { "0" }
                )
            })
            .collect();
        engine.exec(&format!(
            "INSERT INTO {table} ({id}, {v}, {n}) VALUES {}",
            rows.join(", ")
        ));
    };
    seed(1..=ROWS);

    let (lines, exit, detail): (&[&str], i32, &str) = match gate {
        Gate::Duplicate => (
            &[
                "quality:",
                "  unique_columns: [v]",
                "  unique_max_entries: 1000",
            ],
            3,
            "column 'v': 1 duplicate values out of 24 rows",
        ),
        Gate::Nulls => (
            &["quality:", "  null_ratio_max:", "    n: 0.1"],
            3,
            "column 'n': null ratio 0.2500 exceeds threshold 0.1000",
        ),
        Gate::Drift => (
            &["on_schema_drift: fail"],
            4,
            "schema drift detected for export",
        ),
    };
    let incremental = shape.iter().any(|l| l.starts_with("keyset_incremental"));
    let mut rig = engine.rig(&table).mode("chunked");
    for l in shape.iter().chain(lines) {
        rig = rig.export_line(l);
    }

    // The drift gate needs a baseline: one clean run, then a new column and rows past it.
    let mut runs: Vec<String> = Vec::new();
    let mut floor: Option<String> = None;
    if matches!(gate, Gate::Drift) {
        rig.run_ok();
        engine.exec(&format!("ALTER TABLE {table} ADD extra {i} NULL"));
        seed(ROWS + 1..=2 * ROWS);
        runs.push("success".to_string());
        floor = incremental.then(|| ROWS.to_string());
    }

    let mut seen = Vec::new();
    let mut expected = Vec::new();
    for _ in 0..3 {
        let out = rig.run();
        seen.push(observe(&rig, &out, detail, incremental));
        runs.push("failed".to_string());
        expected.push(Cycle {
            exit: Some(exit),
            refusal_named: true,
            runs: runs.clone(),
            cursor: floor.clone(),
            ..seen[0].clone()
        });
    }
    assert_eq!(
        seen, expected,
        "{tag} {gate:?}: cycles 1..3 must each refuse and leave what the first refusal left"
    );
    if !matches!(gate, Gate::Drift) {
        assert_eq!(
            (seen[0].success_marker, seen[0].delivered_rows),
            (false, 0),
            "{tag} {gate:?}: a refused run delivers nothing"
        );
    }

    // The remedy the refusal names, applied from the refused state.
    match gate {
        Gate::Duplicate => engine.exec(&format!("UPDATE {table} SET {v} = 8 WHERE {id} = 8")),
        Gate::Nulls => engine.exec(&format!("UPDATE {table} SET {n} = 0 WHERE {n} IS NULL")),
        Gate::Drift => {
            rig.replace_export_line("on_schema_drift", "on_schema_drift: warn");
        }
    }
    let out = rig.run();
    let healed = observe(&rig, &out, detail, incremental);
    // Declared = the drift baseline's own run (rows 1..=ROWS) plus what the remedy run owes:
    // the rows past the cursor for an incremental export, the whole source for a full pass.
    let source = engine.id_v_pairs(&table);
    let baseline = matches!(gate, Gate::Drift);
    let mut owed: Vec<(i64, i64)> = source
        .iter()
        .copied()
        .filter(|r| !(baseline && incremental) || r.0 > ROWS)
        .collect();
    owed.extend(source.iter().copied().filter(|r| baseline && r.0 <= ROWS));
    owed.sort();
    let delivered = declared_id_v(&rig);
    runs.push("success".to_string());
    assert_eq!(
        (healed.exit, healed.success_marker, healed.runs, delivered),
        (Some(0), true, runs, owed),
        "{tag} {gate:?}: after the remedy the run succeeds, delivers every owed source row once, \
         and the refused runs stay failed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    if incremental {
        let top = source.last().map(|r| r.0.to_string());
        assert_eq!(
            healed.cursor, top,
            "{tag} {gate:?}: the cursor moves on success only"
        );
    }
}

const KEYSET_PARALLEL_CKPT: &[&str] = &[
    "chunk_by_key: id",
    "chunk_size: 3",
    "parallel: 4",
    "chunk_checkpoint: true",
];
const KEYSET_INCREMENTAL: &[&str] = &[
    "chunk_by_key: id",
    "chunk_size: 3",
    "keyset_incremental: true",
];
const KEYSET_PARALLEL_INCREMENTAL: &[&str] = &[
    "chunk_by_key: id",
    "chunk_size: 3",
    "parallel: 4",
    "keyset_incremental: true",
];
const KEYSET_CKPT: &[&str] = &[
    "chunk_by_key: id",
    "chunk_size: 3",
    "chunk_checkpoint: true",
];
const CHUNKED_CKPT: &[&str] = &[
    "chunk_column: id",
    "chunk_size: 5",
    "chunk_checkpoint: true",
];
const CHUNKED_PARALLEL_CKPT: &[&str] = &[
    "chunk_column: id",
    "chunk_size: 5",
    "chunk_checkpoint: true",
    "parallel: 2",
];

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_parallel_keyset_checkpoint_run_keeps_refusing_a_duplicate_postgres() {
    let shape = KEYSET_PARALLEL_CKPT;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_kpc_dup", shape, Gate::Duplicate);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_parallel_keyset_checkpoint_run_keeps_refusing_a_null_ratio_postgres() {
    let shape = KEYSET_PARALLEL_CKPT;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_kpc_null", shape, Gate::Nulls);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_parallel_keyset_checkpoint_run_keeps_refusing_schema_drift_postgres() {
    let shape = KEYSET_PARALLEL_CKPT;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_kpc_drift", shape, Gate::Drift);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_keyset_incremental_run_keeps_refusing_a_duplicate_postgres() {
    let shape = KEYSET_INCREMENTAL;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_ki_dup", shape, Gate::Duplicate);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_keyset_incremental_run_keeps_refusing_a_null_ratio_postgres() {
    let shape = KEYSET_INCREMENTAL;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_ki_null", shape, Gate::Nulls);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_keyset_incremental_run_keeps_refusing_schema_drift_postgres() {
    let shape = KEYSET_INCREMENTAL;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_ki_drift", shape, Gate::Drift);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_parallel_keyset_incremental_run_keeps_refusing_a_duplicate_postgres() {
    let shape = KEYSET_PARALLEL_INCREMENTAL;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_kpi_dup", shape, Gate::Duplicate);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_parallel_keyset_incremental_run_keeps_refusing_schema_drift_postgres() {
    let shape = KEYSET_PARALLEL_INCREMENTAL;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_kpi_drift", shape, Gate::Drift);
}

// The checkpointed runners that release their checkpoint when the data is complete.

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_keyset_checkpoint_run_keeps_refusing_a_duplicate_postgres() {
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_kc_dup", KEYSET_CKPT, Gate::Duplicate);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_keyset_checkpoint_run_keeps_refusing_schema_drift_postgres() {
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_kc_drift", KEYSET_CKPT, Gate::Drift);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_chunked_checkpoint_run_keeps_refusing_a_duplicate_postgres() {
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_cc_dup", CHUNKED_CKPT, Gate::Duplicate);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_refused_parallel_chunked_checkpoint_run_keeps_refusing_a_duplicate_postgres() {
    let shape = CHUNKED_PARALLEL_CKPT;
    assert_a_refused_run_keeps_refusing(SqlEngine::Pg, "rr_cpc_dup", shape, Gate::Duplicate);
}

/// A `keyset_incremental` run that BREAKS OFF is still resumed: its pages are adopted, not read
/// again, the cursor moves once the run succeeds, and the next run reads only new keys.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_crashed_keyset_incremental_run_is_resumed_not_reread_postgres() {
    let engine = SqlEngine::Pg;
    engine.alive();
    let i = engine.int64();
    let (table, _guard) = engine.create(
        "rr_ki_crash",
        &format!("id {i} PRIMARY KEY, v {i} NOT NULL"),
    );
    let seed = |ids: std::ops::RangeInclusive<i64>| {
        let rows: Vec<String> = ids.map(|g| format!("({g}, {g})")).collect();
        engine.exec(&format!("INSERT INTO {table} VALUES {}", rows.join(", ")));
    };
    seed(1..=ROWS);
    let mut rig = engine.rig(&table).mode("chunked");
    for l in KEYSET_INCREMENTAL {
        rig = rig.export_line(l);
    }
    let parts = |rig: &Rig| files_with_extension(&rig.out_dir(), "parquet").len();
    let cursor = |rig: &Rig| run_statuses_and_cursor(&rig.config_path(), rig.export_name()).1;

    let crashed = rig.run_with_env("RIVET_TEST_PANIC_AT", "after_keyset_page:2");
    assert!(!crashed.status.success(), "the injected panic fails run 1");
    assert_eq!(parts(&rig), 3, "pages 0..=2 are durable before the crash");

    rig.run_ok();
    assert_eq!(
        (declared_id_v(&rig), parts(&rig), cursor(&rig)),
        (engine.id_v_pairs(&table), 8, Some(ROWS.to_string())),
        "the resume adopts the three crashed pages and reads the other five"
    );

    seed(ROWS + 1..=ROWS + 6);
    rig.run_ok();
    assert_eq!(
        (declared_id_v(&rig), parts(&rig), cursor(&rig)),
        (engine.id_v_pairs(&table), 10, Some((ROWS + 6).to_string())),
        "the next run reads only the six new keys"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_refused_parallel_keyset_checkpoint_run_keeps_refusing_mysql() {
    let shape = KEYSET_PARALLEL_CKPT;
    assert_a_refused_run_keeps_refusing(SqlEngine::Mysql, "rr_kpc_my", shape, Gate::Duplicate);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_refused_keyset_incremental_run_keeps_refusing_mysql() {
    let shape = KEYSET_INCREMENTAL;
    assert_a_refused_run_keeps_refusing(SqlEngine::Mysql, "rr_ki_my", shape, Gate::Duplicate);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_refused_parallel_keyset_checkpoint_run_keeps_refusing_mssql() {
    let shape = KEYSET_PARALLEL_CKPT;
    assert_a_refused_run_keeps_refusing(SqlEngine::Mssql, "rr_kpc_ms", shape, Gate::Duplicate);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_refused_keyset_incremental_run_keeps_refusing_mssql() {
    let shape = KEYSET_INCREMENTAL;
    assert_a_refused_run_keeps_refusing(SqlEngine::Mssql, "rr_ki_ms", shape, Gate::Duplicate);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_refused_parallel_keyset_checkpoint_run_keeps_refusing_oracle() {
    let shape = KEYSET_PARALLEL_CKPT;
    assert_a_refused_run_keeps_refusing(SqlEngine::Oracle, "rr_kpc_ora", shape, Gate::Duplicate);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_refused_keyset_incremental_run_keeps_refusing_oracle() {
    let shape = KEYSET_INCREMENTAL;
    assert_a_refused_run_keeps_refusing(SqlEngine::Oracle, "rr_ki_ora", shape, Gate::Duplicate);
}
