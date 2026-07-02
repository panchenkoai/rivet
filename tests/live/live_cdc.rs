//! Live CDC regression — locks the invariants the CDC build kept getting wrong:
//! at-least-once resume (no gap, no dup) and the run being recorded in the state
//! DB (metric + journal) like a batch export.
//!
//! Gated `#[ignore]` like the other `live_*` tests — needs the dedicated CDC
//! engines (the `cdc` profile: MySQL :3307 with a REPLICATION grant, PostgreSQL
//! :5434 with `wal_level=logical`). Run with:
//!     docker compose --profile cdc up -d postgres-cdc mysql-cdc
//!     cargo test --test live_suite -- --ignored

use crate::common::*;
use mysql::prelude::Queryable;

/// A throwaway table dropped on `Drop`, so a panic mid-test still cleans up.
struct Table(String);
impl Drop for Table {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_CDC_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

fn conn() -> mysql::PooledConn {
    mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn")
}

/// Current `(binlog_file, pos)` written as the resume checkpoint JSON — so a CDC
/// run starts from *here* and drains only what happens after.
fn write_checkpoint(c: &mut mysql::PooledConn, path: &std::path::Path) {
    let row: mysql::Row = c
        .query_first("SHOW MASTER STATUS")
        .expect("show master status")
        .expect("binlog enabled");
    let file: String = row.get(0).unwrap();
    let pos: u64 = row.get(1).unwrap();
    std::fs::write(path, format!(r#"{{"file":"{file}","pos":{pos}}}"#)).unwrap();
}

/// A per-table MySQL replica id, so concurrent binlog connections (cargo runs
/// these tests in parallel) don't collide on `server_id` (MySQL ERROR 1236).
fn server_id_for(tbl: &str) -> u32 {
    let h = tbl.bytes().fold(2_166_136_261u32, |a, b| {
        (a ^ b as u32).wrapping_mul(16_777_619)
    });
    10_000 + (h % 50_000)
}

fn cdc_config(
    d: &tempfile::TempDir,
    tbl: &str,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(tbl),
    );
    write_config(d, &yaml)
}

fn run_cdc(cfg: &std::path::Path) {
    let out = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet");
    assert!(
        out.status.success(),
        "rivet run (cdc) failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn manifest_rows(out: &std::path::Path) -> i64 {
    let body = std::fs::read_to_string(out.join("manifest.json")).expect("manifest.json");
    let m: serde_json::Value = serde_json::from_str(&body).unwrap();
    m["row_count"].as_i64().expect("row_count")
}

/// The single `.parquet` part written under `dir` (CDC + batch each write one for
/// these small fixtures).
fn find_parquet_part(dir: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .expect("a .parquet part")
}

/// `(column, Arrow type)` for the part — the surface the type-fidelity assertion
/// compares against a batch export.
fn parquet_fields(dir: &std::path::Path) -> Vec<(String, arrow::datatypes::DataType)> {
    let f = std::fs::File::open(find_parquet_part(dir)).unwrap();
    let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    b.schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect()
}

/// The single string value under `col` in the part — for asserting captured
/// content (e.g. a JSON column round-trips as valid JSON text).
fn parquet_one_string(dir: &std::path::Path, col: &str) -> String {
    use arrow::array::{Array, StringArray};
    let f = std::fs::File::open(find_parquet_part(dir)).unwrap();
    let mut r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let batch = r.next().expect("a row").unwrap();
    let idx = batch.schema().index_of(col).expect("column present");
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    arr.value(0).to_string()
}

fn full_config(d: &tempfile::TempDir, tbl: &str, out: &std::path::Path) -> std::path::PathBuf {
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}_batch
    query: "SELECT * FROM {tbl}"
    mode: full
    format: parquet
    destination: {{ type: local, path: "{out}" }}
"#,
        out = out.display(),
    );
    write_config(d, &yaml)
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_column_types_match_a_batch_full_export() {
    // The keep-vs-coarsen invariant, end to end: a CDC export and a batch `mode: full`
    // of the *same* table must produce identical Arrow types for every source column
    // (int widths, decimal precision/scale, timestamp, JSON-as-Utf8). Catches CDC
    // drifting from the batch schema builder.
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_types");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, amount DECIMAL(10,2), n BIGINT, meta JSON)"
    ))
    .unwrap();
    c.query_drop(format!(
        r#"INSERT INTO {tbl} VALUES (1, 12.34, 9000000000, '{{"k":1}}')"#
    ))
    .unwrap();

    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    c.query_drop(format!(
        r#"INSERT INTO {tbl} VALUES (2, 56.78, 9000000001, '{{"k":2}}')"#
    ))
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &cdc_out));
    run_cdc(&full_config(&d, &tbl, &batch_out)); // run_cdc just runs `rivet run`

    let cdc: std::collections::HashMap<_, _> = parquet_fields(&cdc_out).into_iter().collect();
    for (name, batch_ty) in parquet_fields(&batch_out) {
        let cdc_ty = cdc
            .get(&name)
            .unwrap_or_else(|| panic!("cdc output is missing source column {name}"));
        assert_eq!(
            cdc_ty, &batch_ty,
            "column {name}: cdc type {cdc_ty:?} must match batch type {batch_ty:?}"
        );
    }
    // and CDC adds its meta columns the batch export doesn't have
    assert!(cdc.contains_key("__op") && cdc.contains_key("__pos"));
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_captures_json_as_valid_json() {
    // A MySQL JSON column rides through the binlog as JSONB; the sink must emit valid
    // JSON text, not a debug rendering of the driver value.
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_json");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, meta JSON)"
    ))
    .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    c.query_drop(format!(
        r#"INSERT INTO {tbl} VALUES (1, '{{"a":1,"b":[2,3]}}')"#
    ))
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out));

    let json = parquet_one_string(&out, "meta");
    let parsed: serde_json::Value = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("meta is not valid JSON ({e}): {json}"));
    assert_eq!(parsed["a"], 1);
    assert_eq!(parsed["b"][1], 3);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_picks_up_a_column_added_between_runs() {
    // Schema-drift harness: the sink resolves the table schema at the START of each
    // run, so a column added *between* runs is captured on the next run. (Within a
    // single run the schema is frozen at the first flush — that's the documented
    // limitation; run-to-run re-resolution is how drift is actually handled.)
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_drift");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);

    // Run 1: capture a row under the original (id, v) schema.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out1));
    let f1: std::collections::HashMap<_, _> = parquet_fields(&out1).into_iter().collect();
    assert!(!f1.contains_key("w"), "run 1 predates the added column");

    // Add a column, then a row that uses it.
    c.query_drop(format!("ALTER TABLE {tbl} ADD COLUMN w VARCHAR(20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2, 20, 'hello')"))
        .unwrap();

    // Run 2 (resume): re-resolves the schema → the new column is captured.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out2));
    let f2: std::collections::HashMap<_, _> = parquet_fields(&out2).into_iter().collect();
    assert!(
        f2.contains_key("w"),
        "run 2 must re-resolve and pick up the column added between runs"
    );
    assert_eq!(parquet_one_string(&out2, "w"), "hello");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_throughput_drains_a_large_backlog() {
    // Lag/throughput harness (#6): rivet exposes no replication-lag metric (a
    // documented limitation), so this measures the next best proxy — how fast a
    // backlog drains — and logs rows/s. It's also the only CDC test at non-trivial
    // scale (the others use tiny fixtures), so it doubles as a correctness-at-scale
    // check: every one of N changes must be captured.
    const N: i64 = 5_000;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_bench");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);

    // Seed N changes (1000-row INSERT batches).
    let mut id = 0;
    while id < N {
        let end = (id + 1000).min(N);
        let vals: Vec<String> = (id..end).map(|i| format!("({i},{i})")).collect();
        c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
            .unwrap();
        id = end;
    }

    // Drain the backlog, timed.
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let t = std::time::Instant::now();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out));
    let secs = t.elapsed().as_secs_f64();

    // Correctness at scale: nothing dropped under volume.
    assert_eq!(manifest_rows(&out), N, "all {N} changes must be captured");

    // Throughput: logged for trend-watching, plus a generous wall-clock ceiling so
    // a catastrophic perf regression fails the test without machine-variance flake.
    eprintln!(
        "CDC throughput: {N} changes drained in {secs:.2}s = {:.0} rows/s",
        N as f64 / secs
    );
    assert!(
        secs < 60.0,
        "draining {N} changes took {secs:.1}s (>60s — perf regression?)"
    );
}

// Idle-first-run anchor model (per-engine, see CLAUDE.md): MySQL's ONLY resume
// anchor is the client checkpoint file, and the sink writes it at part commits —
// so the first checkpointed open must persist its coordinates immediately, or an
// idle bounded run (zero changes drained) leaves no anchor and the next run
// re-anchors to a newer "current" position, silently skipping every change in
// between. This is the binary-level (`rivet run`) mirror of the stream-level
// regression `first_run_with_zero_changes_pins_the_checkpoint_at_open`.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_idle_first_run_then_change_is_captured_not_skipped() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_idle_bin");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    // Run 1: checkpoint path configured, no file yet, nothing to capture.
    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 0, "idle run 1 captures nothing");
    assert!(
        ckpt.exists(),
        "an idle first run must still pin the open position to the checkpoint"
    );

    // A change lands BETWEEN the idle run and the next scheduler cycle.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 100)"))
        .unwrap();

    // Run 2 resumes from the pinned position and must capture it.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        1,
        "the change between an idle run and the next run must be captured, not skipped"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_crash_after_flush_before_ack_re_reads_on_resume() {
    // The at-least-once guarantee under a crash: the durable sequence is
    // flush → checkpoint → ack. A crash AFTER the part is durable but BEFORE the
    // checkpoint advances must NOT lose the change — the resume re-reads it. (If the
    // checkpoint were saved before the flush, this run would lose the two changes.)
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_crash");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    // Run 1 crashes right after the part is flushed, before the checkpoint+ack.
    let crash_out = d.path().join("crash");
    std::fs::create_dir_all(&crash_out).unwrap();
    let crashed = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cdc_config(&d, &tbl, &ckpt, &crash_out).to_str().unwrap(),
        ])
        .env("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")
        .output()
        .expect("spawn rivet");
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2 (no crash): the checkpoint never advanced, so it resumes from the same
    // position and re-reads both changes — nothing was lost to the crash.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume after a crash before the checkpoint re-reads both changes (no loss)"
    );
}

// ─── PostgreSQL: the slot-advance side of at-least-once ──────────────────────

/// Drops the test's logical replication slot on teardown — a slot pins WAL until
/// removed, so leaking one across runs would fill the dev disk.
struct Slot(String);
impl Drop for Slot {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls) {
            let _ = c.execute("SELECT pg_drop_replication_slot($1)", &[&self.0]);
        }
    }
}

fn pg_cdc_config(
    d: &tempfile::TempDir,
    tbl: &str,
    slot: &str,
    out: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ slot: {slot}, until_current: true }}
    destination: {{ type: local, path: "{out}" }}
"#,
        out = out.display(),
    );
    write_config(d, &yaml)
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_resume_captures_only_new_changes() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pg");
    let slot = unique_name("rivet_regr_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    // The slot must exist *before* the changes so it captures them; the guard drops it.
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"), &[])
        .unwrap();
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_cdc(&pg_cdc_config(&d, &tbl, &slot, &out1));
    assert_eq!(manifest_rows(&out1), 2, "run 1 drains the 2 changes");

    // Resume: the slot's confirmed_flush advanced after the durable write, so run 2
    // peeks only the new changes — the PostgreSQL at-least-once / no-re-read guarantee.
    c.execute(&format!("INSERT INTO {tbl} VALUES (3,30),(4,40)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume drains only the 2 new changes (slot advanced, no re-read)"
    );
}

/// Read the (single) parquet part under `dir` into one RecordBatch.
fn read_one_batch(dir: &std::path::Path) -> arrow::record_batch::RecordBatch {
    let f = std::fs::File::open(find_parquet_part(dir)).unwrap();
    let mut r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .with_batch_size(usize::MAX)
        .build()
        .unwrap();
    r.next().expect("at least one row").unwrap()
}

/// Assert every source column of the batch export is byte-for-byte identical
/// (type AND value, via ArrayData equality) in the CDC output — the parity
/// oracle that caught the uuid/time/interval/NULL-text losses on PostgreSQL
/// and the timestamp/bit/year/enum/binary losses on MySQL.
fn assert_cdc_matches_batch(cdc_out: &std::path::Path, batch_out: &std::path::Path) {
    let batch = read_one_batch(batch_out);
    let cdc = read_one_batch(cdc_out);
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let name = field.name();
        let cidx = cdc
            .schema()
            .index_of(name)
            .unwrap_or_else(|_| panic!("cdc output is missing source column {name}"));
        assert_eq!(
            batch.column(i).to_data(),
            cdc.column(cidx).to_data(),
            "column {name}: CDC differs from the batch export (type or value drift)"
        );
    }
    assert!(cdc.schema().index_of("__op").is_ok() && cdc.schema().index_of("__pos").is_ok());
}

// The all-types parity contract for MySQL: a table covering every Rivet-mapped
// MySQL type (the union of both official type matrices), exported both ways —
// batch and CDC — must produce identical Arrow columns. This is the e2e pin for
// the binlog cell fixes: TIMESTAMP arrives as epoch text, BIT as raw bytes,
// YEAR as text, ENUM as a 1-based index, BINARY(n) NUL-trimmed, JSONB spacing.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_full_type_matrix_matches_batch() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_matrix_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (
           id BIGINT PRIMARY KEY, label VARCHAR(200), amount DECIMAL(18,2),
           created_at_dt DATETIME(6), created_at_ts TIMESTAMP(6) NULL,
           raw_bytes BINARY(4), extras JSON, flag BOOLEAN, bit1_col BIT(1),
           bit8_col BIT(8), tiny_col TINYINT, date_col DATE, time_col TIME(6),
           year_col YEAR, enum_col ENUM('a','b','c'), varbinary_col VARBINARY(4),
           blob_col BLOB,
           small_col SMALLINT, med_col MEDIUMINT, int_col INT,
           intu_col INT UNSIGNED, bigu_col BIGINT UNSIGNED,
           f_col FLOAT, d_col DOUBLE, ch_col CHAR(8), txt_col TEXT,
           set_col SET('x','y','z')) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES
           (1, 'üñíçødé', 999999999999.99, '2035-08-07 09:08:07.987654',
            '2035-08-07 09:08:07.987654', UNHEX('00000000'),
            JSON_OBJECT('tier','gold','n',1), TRUE, b'1', b'10101010', 127,
            '2024-03-15', '14:30:00.123456', 2024, 'b', 0xDEADBEEF, 0x0102,
            -32768, -8388608, -2147483648, 4294967295, 18446744073709551615,
            1.5, -2.25, 'pad', 'long text', 'x,z'),
           (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL)"
    ))
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &cdc_out));
    run_cdc(&full_config(&d, &tbl, &batch_out));
    assert_cdc_matches_batch(&cdc_out, &batch_out);
}

// `cdc.initial: snapshot` — the safe switch ordering enforced by construction:
// anchor → snapshot → drain in ONE run. The invariant this pins: rows that
// exist BEFORE the first run land in `snapshot/`, changes AFTER land in the
// change stream, and a second run does NOT re-snapshot. No row is in neither.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_initial_snapshot_covers_preexisting_rows_then_streams() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    // Pre-existing rows — the base CDC alone would never deliver.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ initial: snapshot, checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(&tbl),
    );
    let cfg = write_config(&d, &yaml);

    // Run 1: anchor → snapshot(2 rows) → drain(0).
    run_cdc(&cfg);
    let snap = out.join("snapshot");
    assert_eq!(
        manifest_rows(&snap),
        2,
        "pre-existing rows land in snapshot/"
    );
    assert_eq!(manifest_rows(&out), 0, "nothing to drain yet");
    let snap_parts = || {
        std::fs::read_dir(&snap)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "parquet")
            })
            .count()
    };
    assert_eq!(snap_parts(), 1);

    // A change AFTER the snapshot → the stream, not a re-snapshot.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30)"))
        .unwrap();
    run_cdc(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
    assert_eq!(snap_parts(), 1, "run 2 must NOT re-snapshot");
}

// PostgreSQL flavour: the slot IS the anchor (no checkpoint required).
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_initial_snapshot_covers_preexisting_rows_then_streams() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_pg");
    let slot = unique_name("rivet_init_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); \
         INSERT INTO {tbl} VALUES (1,10),(2,20)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ initial: snapshot, slot: {slot}, until_current: true }}
    destination: {{ type: local, path: "{out}" }}
"#,
        out = out.display(),
    );
    let cfg = write_config(&d, &yaml);

    run_cdc(&cfg);
    let _slot = Slot(slot.clone());
    assert_eq!(manifest_rows(&out.join("snapshot")), 2);
    assert_eq!(manifest_rows(&out), 0);

    c.execute(&format!("INSERT INTO {tbl} VALUES (3,30)"), &[])
        .unwrap();
    run_cdc(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
}

// `columns:` type overrides must apply to CDC exactly like batch — pinned for
// the finding that resolve_cdc_columns passed an EMPTY override map, silently
// ignoring the config's declarations. The canonical use: `bigint unsigned` →
// `decimal(20,0)` so a BigQuery-bound export loads (BQ has no unsigned 64).
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_column_overrides_apply_like_batch() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_ovr");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, bigu BIGINT UNSIGNED)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, 18446744073709551615)"
    ))
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let cdc_yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    columns: {{ bigu: "decimal(20,0)" }}
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{cdc_out}" }}
"#,
        ckpt = ckpt.display(),
        cdc_out = cdc_out.display(),
        sid = server_id_for(&tbl),
    );
    let batch_yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}_batch
    table: {tbl}
    mode: full
    format: parquet
    columns: {{ bigu: "decimal(20,0)" }}
    destination: {{ type: local, path: "{batch_out}" }}
"#,
        batch_out = batch_out.display(),
    );
    run_cdc(&write_config(&d, &cdc_yaml));
    run_cdc(&write_config(&d, &batch_yaml));

    let fields: std::collections::HashMap<_, _> = parquet_fields(&cdc_out).into_iter().collect();
    assert_eq!(
        fields.get("bigu"),
        Some(&arrow::datatypes::DataType::Decimal128(20, 0)),
        "the override must reach the CDC schema"
    );
    assert_cdc_matches_batch(&cdc_out, &batch_out);
}

// The all-types parity contract for PostgreSQL — pins the test_decoding parse
// fixes: uuid/bytea text→raw bytes, TIME→Time64, INTERVAL→the batch's ISO 8601
// canon, and NULLs of text-shaped columns staying NULL (not ""). Arrays are the
// known exception (CDC carries the PG literal as text until List support), so
// this matrix deliberately covers everything BUT arrays.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_full_type_matrix_matches_batch() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_matrix_pg");
    let slot = unique_name("rivet_matrix_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(
        "DO $$ BEGIN CREATE TYPE rivet_status AS ENUM ('active','inactive','pending'); \
         EXCEPTION WHEN duplicate_object THEN NULL; END $$;",
    )
    .unwrap();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (
           id BIGINT PRIMARY KEY, label TEXT, amount NUMERIC(18,2),
           created_at TIMESTAMP, created_at_tz TIMESTAMPTZ, raw_bytes BYTEA,
           uid UUID, attrs JSONB, flag BOOLEAN, int2_col SMALLINT,
           float8_col DOUBLE PRECISION, date_col DATE, time_col TIME,
           interval_col INTERVAL, enum_col rivet_status,
           doc_col JSON, ch_col CHAR(8), vc_col VARCHAR(50), float4_col REAL)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());

    // Slot first, then the changes (they must land inside the slot's window).
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES
           (1, 'üñíçødé ''q''', 999999999999.99, '2035-08-07 09:08:07.987654',
            '2019-02-03 08:07:06.554433+05', '\\x00ff01'::bytea,
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011', '{{\"n\":1}}'::jsonb, TRUE,
            32767, 2.5, '2024-03-15', '14:30:00.123456',
            INTERVAL '1 year 2 mons 3 days', 'active',
            '{{\"k\": [1, 2]}}'::json, 'pad', 'plain varchar', 3.14);
         INSERT INTO {tbl} (id) VALUES (2);"
    ))
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_cdc(&pg_cdc_config(&d, &tbl, &slot, &cdc_out));
    let batch_yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_CDC_URL}"}}
exports:
  - name: {tbl}_batch
    table: {tbl}
    mode: full
    format: parquet
    destination: {{ type: local, path: "{out}" }}
"#,
        out = batch_out.display(),
    );
    run_cdc(&write_config(&d, &batch_yaml));
    assert_cdc_matches_batch(&cdc_out, &batch_out);
}

// Slot multiplexing: several tables through ONE PostgreSQL slot (`tables:`),
// each landing under its own sub-prefix with its own manifest — and the shared
// position still resumes correctly (second run captures nothing twice).
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_multi_table_stream_uses_one_slot_and_resumes() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let t1 = unique_name("rivet_cdc_ma");
    let t2 = unique_name("rivet_cdc_mb");
    let slot = unique_name("rivet_multi_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    for t in [&t1, &t2] {
        c.batch_execute(&format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"
        ))
        .unwrap();
    }
    let (_g1, _g2) = (PgTable::adopt(t1.clone()), PgTable::adopt(t2.clone()));

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_CDC_URL}"}}
exports:
  - name: app_cdc
    tables: [{t1}, {t2}]
    mode: cdc
    format: parquet
    cdc: {{ slot: {slot}, until_current: true }}
    destination: {{ type: local, path: "{out}" }}
"#,
        out = out.display(),
    );
    let cfg = write_config(&d, &yaml);

    // Run 1 creates the ONE slot and drains nothing.
    run_cdc(&cfg);
    let _slot = Slot(slot.clone());
    let n: i64 = c
        .query_one(
            "SELECT count(*)::bigint FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 1, "two tables ride ONE slot");

    // Changes in both tables → one run captures both, routed per table.
    c.execute(&format!("INSERT INTO {t1} VALUES (1,10),(2,20)"), &[])
        .unwrap();
    c.execute(&format!("INSERT INTO {t2} VALUES (7,70)"), &[])
        .unwrap();
    run_cdc(&cfg);
    assert_eq!(manifest_rows(&out.join(&t1)), 2, "table 1 sub-prefix");
    assert_eq!(manifest_rows(&out.join(&t2)), 1, "table 2 sub-prefix");

    // Resume: the shared position advanced once for both tables.
    run_cdc(&cfg);
    assert_eq!(manifest_rows(&out.join(&t1)), 0, "no re-read for table 1");
    assert_eq!(manifest_rows(&out.join(&t2)), 0, "no re-read for table 2");
}

// MySQL flavour of the multi-table stream: one binlog connection + one
// checkpoint for both tables, idle-first-run pin included.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_multi_table_stream_one_binlog_connection_and_resumes() {
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_multi_a");
    let tb = unique_name("cdc_multi_b");
    let mut c = conn();
    for t in [&ta, &tb] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let (_g1, _g2) = (Table(ta.clone()), Table(tb.clone()));

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: app_cdc
    tables: [{ta}, {tb}]
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(&ta),
    );
    let cfg = write_config(&d, &yaml);

    // Run 1: pins the checkpoint (idle-first-run) with zero captures.
    run_cdc(&cfg);
    assert!(ckpt.exists(), "idle first run pins the shared checkpoint");

    c.query_drop(format!("INSERT INTO {ta} VALUES (1,10),(2,20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (7,70)"))
        .unwrap();
    run_cdc(&cfg);
    assert_eq!(manifest_rows(&out.join(&ta)), 2);
    assert_eq!(manifest_rows(&out.join(&tb)), 1);

    run_cdc(&cfg);
    assert_eq!(manifest_rows(&out.join(&ta)), 0, "resume: no re-read");
    assert_eq!(manifest_rows(&out.join(&tb)), 0, "resume: no re-read");
}

// The cloud sub-prefix regression, end to end against a real GCS API
// (fake-gcs): a multi-table CDC export must land each table under
// `<prefix>/<table>/…` with '/'-separated object keys. The mangled flat keys
// this pins against (`<prefix>/<table>cdc-….parquet`) shipped to a real bucket
// first — the multi-table live tests only used local destinations.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + fake-gcs"]
fn cdc_multi_table_to_gcs_lands_per_table_prefixes() {
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_gcs_a");
    let tb = unique_name("cdc_gcs_b");
    let mut c = conn();
    for t in [&ta, &tb] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let (_g1, _g2) = (Table(ta.clone()), Table(tb.clone()));

    let bucket = "rivet-qa-cdc-gcs";
    ensure_gcs_bucket(bucket);
    let prefix = unique_name("cdcgcs");
    let ckpt = d.path().join("cdc.ckpt");
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: app_cdc
    tables: [{ta}, {tb}]
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination:
      type: gcs
      bucket: {bucket}
      prefix: {prefix}
      endpoint: {FAKE_GCS_ENDPOINT}
      allow_anonymous: true
"#,
        ckpt = ckpt.display(),
        sid = server_id_for(&ta),
    );
    let cfg = write_config(&d, &yaml);

    run_cdc(&cfg); // pin
    c.query_drop(format!("INSERT INTO {ta} VALUES (1,10),(2,20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (7,70)"))
        .unwrap();
    run_cdc(&cfg); // capture → upload

    // List the object keys under the prefix via the GCS JSON API.
    let body = reqwest::blocking::get(format!(
        "{FAKE_GCS_ENDPOINT}/storage/v1/b/{bucket}/o?prefix={prefix}"
    ))
    .expect("gcs list request")
    .text()
    .expect("gcs list body");
    let json: serde_json::Value = serde_json::from_str(&body).expect("gcs list json");
    let keys: Vec<&str> = json["items"]
        .as_array()
        .map(|items| items.iter().filter_map(|o| o["name"].as_str()).collect())
        .unwrap_or_default();

    for t in [&ta, &tb] {
        assert!(
            keys.iter()
                .any(|k| *k == format!("{prefix}/{t}/manifest.json")),
            "per-table manifest key missing for {t}; keys: {keys:?}"
        );
        assert!(
            keys.iter().any(|k| *k == format!("{prefix}/{t}/_SUCCESS")),
            "per-table _SUCCESS key missing for {t}; keys: {keys:?}"
        );
        assert!(
            keys.iter()
                .any(|k| k.starts_with(&format!("{prefix}/{t}/cdc-")) && k.ends_with(".parquet")),
            "per-table part key missing for {t}; keys: {keys:?}"
        );
        assert!(
            !keys.iter().any(|k| k.contains(&format!("{t}cdc-"))
                || k.contains(&format!("{t}manifest"))
                || k.contains(&format!("{t}_SUCCESS"))),
            "mangled flat key (missing '/') detected for {t}; keys: {keys:?}"
        );
    }
}

// Retention, MySQL flavour: a checkpoint whose binlog file the server no longer
// has (purged — or, as forged here, simply nonexistent) must fail the run
// LOUDLY, never fall back to "start from current" and silently skip the gap.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_resume_from_missing_binlog_fails_loudly_not_silently() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_1236");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    // A checkpoint pointing at a binlog file the server does not have — the
    // exact shape a purged-past-retention resume presents.
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::write(&ckpt, r#"{"file":"binlog.999999","pos":4}"#).unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();

    let out_dir = d.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let cfg = cdc_config(&d, &tbl, &ckpt, &out_dir);
    let out = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet");
    assert!(
        !out.status.success(),
        "resuming from a purged/missing binlog must FAIL, not silently re-anchor"
    );
    assert!(
        !out_dir.join("_SUCCESS").exists(),
        "no _SUCCESS may be written for the failed run"
    );
}

// Retention, PostgreSQL flavour (RED for the finding): a prior run's checkpoint
// exists but the slot is GONE (dropped by an operator / invalidated and removed)
// — recreating it at the current position would silently skip every change
// since the drop. The run must fail loudly and demand a re-snapshot.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_vanished_slot_with_checkpoint_fails_loudly_not_recreates() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_gone");
    let slot = unique_name("rivet_gone_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());

    // Run 1 (with a checkpoint configured): creates the slot, captures one
    // change, persists the checkpoint.
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let yaml = |out: &std::path::Path| {
        format!(
            r#"source: {{type: postgres, url: "{POSTGRES_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ slot: {slot}, until_current: true, checkpoint: "{ckpt}" }}
    destination: {{ type: local, path: "{out}" }}
"#,
            ckpt = ckpt.display(),
            out = out.display(),
        )
    };
    run_cdc(&write_config(&d, &yaml(&out1)));
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&write_config(&d, &yaml(&out2)));
    assert_eq!(manifest_rows(&out2), 1, "run 2 captured the change");
    assert!(ckpt.exists(), "checkpoint persisted");

    // The slot vanishes behind rivet's back; a change lands after.
    c.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (2,20)"), &[])
        .unwrap();

    // Run 3 must FAIL loudly — recreating the slot would silently skip id=2.
    let out3 = d.path().join("out3");
    std::fs::create_dir_all(&out3).unwrap();
    let cfg3 = write_config(&d, &yaml(&out3));
    let out = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg3.to_str().unwrap()])
        .output()
        .expect("spawn rivet");
    assert!(
        !out.status.success(),
        "a vanished slot with an existing checkpoint must fail the run, not silently re-create"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("re-snapshot") || stderr.contains("missing"),
        "the failure must carry the re-snapshot hint, got:\n{stderr}"
    );
}

// `rivet doctor` CDC health: the slot / abandoned-slot probes automate the
// monitoring docs/reference/cdc.md asks operators to do by hand. The foreign
// inactive slot here re-enacts a real incident: an abandoned ingestr slot was
// found pinning WAL on this project's own dev instance.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn doctor_reports_cdc_slot_health_and_flags_foreign_inactive_slots() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_doc");
    let own_slot = unique_name("rivet_doc_slot");
    let foreign_slot = unique_name("abandoned_tool_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    // A foreign, inactive slot — some other tool created it and walked away.
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&foreign_slot],
    )
    .unwrap();
    let _foreign = Slot(foreign_slot.clone());

    let out_dir = d.path().join("out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let cfg = pg_cdc_config(&d, &tbl, &own_slot, &out_dir);
    let out = std::process::Command::new(RIVET_BIN)
        .args(["doctor", "--config", cfg.to_str().unwrap(), "--json"])
        .output()
        .expect("spawn rivet doctor");
    let report: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("doctor --json output");
    let checks = report["checks"].as_array().expect("checks array");

    // The export's own slot: absent → healthy "created on the first run".
    let own = checks
        .iter()
        .find(|c| c["name"].as_str().unwrap_or("").contains(&own_slot))
        .expect("own-slot check present");
    assert_eq!(own["ok"], true, "absent slot is healthy: {own}");

    // The abandoned foreign slot is surfaced by name (small → note, not FAIL).
    let foreign = checks
        .iter()
        .find(|c| {
            c["name"]
                .as_str()
                .unwrap_or("")
                .contains("other inactive slots")
        })
        .expect("foreign-slots check present");
    assert!(
        foreign["detail"]
            .as_str()
            .unwrap_or("")
            .contains(&foreign_slot),
        "the abandoned slot must be named: {foreign}"
    );
    assert_eq!(
        report["all_ok"], true,
        "small foreign slot must not fail doctor"
    );
}

// Idle-first-run anchor model (per-engine, see CLAUDE.md): PostgreSQL pins the
// resume position server-side the moment the slot is created — so a first run
// that drains ZERO changes still anchors, and a change landing between two idle
// scheduler cycles is captured by the next one. This pins that property (the
// exact hole MySQL shipped with, where the client checkpoint was the only anchor
// and an idle run never wrote it).
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_idle_first_run_then_change_is_captured_not_skipped() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgidle");
    let slot = unique_name("rivet_idle_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());

    // Run 1: the slot does not exist yet — rivet creates it and drains nothing.
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_cdc(&pg_cdc_config(&d, &tbl, &slot, &out1));
    let _slot = Slot(slot.clone());
    assert_eq!(manifest_rows(&out1), 0, "idle run 1 drains nothing");

    // A change lands BETWEEN the idle run and the next scheduler cycle.
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();

    // Run 2 must capture it — the slot created in run 1 pinned the position.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        1,
        "the change between an idle run and the next run must be captured, not skipped"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_crash_after_flush_before_ack_does_not_advance_the_slot() {
    // PostgreSQL is the consume-on-read engine — the one where reordering flush/ack
    // would actually lose data. A crash after the part is durable but before the slot
    // advances must leave the slot un-advanced, so the resume re-reads (at-least-once).
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgcrash");
    let slot = unique_name("rivet_crash_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"), &[])
        .unwrap();

    // Run 1 crashes after the part is flushed, before the slot advances.
    let crash_out = d.path().join("crash");
    std::fs::create_dir_all(&crash_out).unwrap();
    let crashed = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            pg_cdc_config(&d, &tbl, &slot, &crash_out).to_str().unwrap(),
        ])
        .env("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")
        .output()
        .expect("spawn rivet");
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2: the slot never advanced, so the peek still sees both changes.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "the slot stayed put across the crash → resume re-reads both (no loss)"
    );
}

fn pg_full_config(d: &tempfile::TempDir, tbl: &str, out: &std::path::Path) -> std::path::PathBuf {
    let yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_CDC_URL}"}}
exports:
  - name: {tbl}_batch
    query: "SELECT * FROM {tbl}"
    mode: full
    format: parquet
    destination: {{ type: local, path: "{out}" }}
"#,
        out = out.display(),
    );
    write_config(d, &yaml)
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_column_types_match_batch_export() {
    use postgres::NoTls;
    // FULL type parity with the batch export — every column, including the tz-aware
    // `timestamptz`, lands with the identical Arrow type a `mode: full` export
    // produces. (timestamptz is carried as the UTC instant + zone label, exactly like
    // batch — see docs/reference/cdc-type-parity.md.)
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgtypes");
    let slot = unique_name("rivet_types_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id int, amount numeric(10,2), \
         meta jsonb, label text, ts timestamp, tstz timestamptz, u uuid)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    c.execute(
        &format!(
            "INSERT INTO {tbl} VALUES (1, 12.34, '{{\"k\":1}}', 'hi', \
             '2026-06-23 10:00:00', '2026-06-23 10:00:00+00', gen_random_uuid())"
        ),
        &[],
    )
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_cdc(&pg_cdc_config(&d, &tbl, &slot, &cdc_out));
    run_cdc(&pg_full_config(&d, &tbl, &batch_out));

    let cdc: std::collections::HashMap<_, _> = parquet_fields(&cdc_out).into_iter().collect();
    let batch: std::collections::HashMap<_, _> = parquet_fields(&batch_out).into_iter().collect();
    for col in ["id", "amount", "meta", "label", "ts", "tstz", "u"] {
        assert_eq!(
            cdc.get(col),
            batch.get(col),
            "column {col}: CDC type must match the batch export (full parity)"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_resume_captures_only_new_changes() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_regr");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();

    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);

    // First batch of changes, then capture: drains exactly these two.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out1));
    assert_eq!(
        manifest_rows(&out1),
        2,
        "run 1 should capture the 2 new changes"
    );

    // Two more changes; the resume run (same checkpoint, now advanced) must pick
    // up ONLY these — no gap, no re-read of the first two (the at-least-once /
    // PostgreSQL at-most-once regression).
    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30),(4,40)"))
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume must capture exactly the 2 changes since the checkpoint (no gap, no dup)"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql (binlog ROW + REPLICATION grant)"]
fn cdc_run_is_recorded_in_state_db() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_regr");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();

    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20),(3,30)"))
        .unwrap();
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let cfg = cdc_config(&d, &tbl, &ckpt, &out);
    run_cdc(&cfg);

    // A CDC run must show up like a batch run: an export_metrics row with mode=cdc,
    // and a run_journal entry (FileWritten + RunCompleted) so `rivet journal` works.
    let db = d.path().join(".rivet_state.db");
    let sql = rusqlite::Connection::open(&db).expect("state db");

    let (rows, mode): (i64, String) = sql
        .query_row(
            "SELECT total_rows, mode FROM export_metrics WHERE export_name = ?1 ORDER BY rowid DESC LIMIT 1",
            [&tbl],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("export_metrics row for the cdc run");
    assert_eq!(mode, "cdc");
    assert_eq!(rows, 3, "metric total_rows = captured changes");

    let journal: String = sql
        .query_row(
            "SELECT journal_json FROM run_journal WHERE export_name = ?1 ORDER BY rowid DESC LIMIT 1",
            [&tbl],
            |r| r.get(0),
        )
        .expect("run_journal row for the cdc run");
    assert!(
        journal.contains("RunCompleted") && journal.contains("FileWritten"),
        "cdc journal must carry FileWritten + RunCompleted, got: {journal}"
    );
}
