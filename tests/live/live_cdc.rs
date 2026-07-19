//! Live CDC regression — locks the invariants the CDC build kept getting wrong:
//! at-least-once resume (no gap, no dup) and the run being recorded in the state
//! DB (metric + journal) like a batch export.
//!
//! Gated `#[ignore]` like the other `live_*` tests — needs the dedicated CDC
//! engines (the `cdc` profile: MySQL :3307 with a REPLICATION grant, PostgreSQL
//! :5434 with `wal_level=logical`). Run with:
//!     docker compose --profile cdc up -d postgres-cdc mysql-cdc
//!     cargo test --test live_suite -- --ignored

use crate::common::MysqlCdcTable as Table;
use crate::common::*;
use mysql::prelude::Queryable;

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

fn cdc_config(
    d: &tempfile::TempDir,
    tbl: &str,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = Rig::mysql_cdc(tbl)
        .checkpoint_path(ckpt.to_path_buf())
        .dest_path(out.to_path_buf())
        .yaml();
    write_config(d, &yaml)
}

/// Template-equivalence golden: the rig must render EXACTLY the config the
/// hand-rolled template produced — the contract guarantee for migrating this
/// file's 20+ resume/crash tests without touching their plumbing semantics.
#[test]
fn rig_renders_the_exact_legacy_cdc_template() {
    let yaml = Rig::mysql_cdc("t1")
        .checkpoint_path("/tmp/ck".into())
        .dest_path("/tmp/o".into())
        .yaml();
    let legacy = format!(
        "source: {{ type: mysql, url: \"{MYSQL_CDC_URL}\" }}\nexports:\n  - name: t1\n    table: t1\n    mode: cdc\n    format: parquet\n    cdc: {{ until_current: true, checkpoint: \"/tmp/ck\", server_id: {} }}\n    destination: {{ type: local, path: \"/tmp/o\" }}\n",
        server_id_for("t1"),
    );
    assert_eq!(
        yaml, legacy,
        "the rig must not drift from the proven template"
    );
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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &cdc_out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out)); // run_cdc just runs `rivet run`

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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));

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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out1));
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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out2));
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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
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

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_intra_transaction_updates_get_distinct_seq() {
    // A PK updated many times in ONE transaction: every change shares the commit
    // __pos, so ordering a current-state dedup by __pos alone picks an ARBITRARY
    // row (observed live: `counter = 1` for a row whose committed value was N).
    // `__seq` — the intra-transaction ordinal — restores the total order.
    // Regression for the silently-wrong current-state class.
    const N: i64 = 200;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_seq");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, counter BIGINT)"
    ))
    .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 0)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);

    // N updates of the SAME row in a SINGLE transaction.
    c.query_drop("START TRANSACTION").unwrap();
    for i in 1..=N {
        c.query_drop(format!("UPDATE {tbl} SET counter = {i} WHERE id = 1"))
            .unwrap();
    }
    c.query_drop("COMMIT").unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));

    assert_intra_transaction_seq(&out, N);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_sum_reconciles_across_intra_txn_updates() {
    // The strong end-to-end oracle: SUM(v) on the source must equal SUM(v) on
    // the target deduped STRICTLY by (__pos, __seq), row order discarded (as an
    // unordered warehouse table forces). Every transaction updates one PK 2–4
    // times, so a __pos-only dedup would pick an intermediate `v` and skew the
    // sum — this reconciles only because __seq totally-orders the log.
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_sum");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"
    ))
    .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);

    for txn in cdc_sum_workload(&tbl) {
        c.query_drop("START TRANSACTION").unwrap();
        for stmt in txn {
            c.query_drop(stmt).unwrap();
        }
        c.query_drop("COMMIT").unwrap();
    }
    let source_sum: i64 = c
        .query_first(format!("SELECT COALESCE(SUM(v), 0) FROM {tbl}"))
        .unwrap()
        .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));

    let changes = read_cdc_changes(&out);
    assert!(
        intra_txn_multi_change_count(&changes) > 0,
        "workload must exercise intra-transaction multi-updates or the sum passes vacuously"
    );
    let target_sum = deduped_current_sum(changes, CdcEngine::MySql);
    assert_eq!(
        source_sum, target_sum,
        "deduped-by-(__pos,__seq) SUM(v) must equal the source's SUM(v)"
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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out1));
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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        1,
        "the change between an idle run and the next run must be captured, not skipped"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(1, "insert".to_string())],
        "the captured parquet must hold exactly THE change (a count of 1 could be a wrong row)"
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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume after a crash before the checkpoint re-reads both changes (no loss)"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "the re-read parquet must hold exactly the un-acked changes"
    );
}

// ─── PostgreSQL: the slot-advance side of at-least-once ──────────────────────

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
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out1));
    assert_eq!(manifest_rows(&out1), 2, "run 1 drains the 2 changes");

    // Resume: the slot's confirmed_flush advanced after the durable write, so run 2
    // peeks only the new changes — the PostgreSQL at-least-once / no-re-read guarantee.
    c.execute(&format!("INSERT INTO {tbl} VALUES (3,30),(4,40)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume drains only the 2 new changes (slot advanced, no re-read)"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(3, "insert".to_string()), (4, "insert".to_string())],
        "the resumed parquet must hold exactly the NEW changes (count 2 cannot tell new-2 from wrong-2)"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_intra_transaction_updates_get_distinct_seq() {
    // Peer of cdc_intra_transaction_updates_get_distinct_seq. PostgreSQL emits
    // every change of a transaction at the COMMIT lsn (and marks each
    // `committed`), so __pos ties them — __seq restores the order.
    use postgres::NoTls;
    const N: i64 = 200;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pg_seq");
    let slot = unique_name("rivet_seq_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, counter BIGINT); \
         ALTER TABLE {tbl} REPLICA IDENTITY FULL; INSERT INTO {tbl} VALUES (1, 0)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    // N updates of the SAME row in ONE transaction (a DO block is one txn).
    c.batch_execute(&format!(
        "DO $$ BEGIN FOR i IN 1..{N} LOOP UPDATE {tbl} SET counter = i WHERE id = 1; END LOOP; END $$"
    ))
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    assert_intra_transaction_seq(&out, N);
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_sum_reconciles_across_intra_txn_updates() {
    // Peer of cdc_sum_reconciles_across_intra_txn_updates for PostgreSQL.
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pg_sum");
    let slot = unique_name("rivet_sum_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL); \
         ALTER TABLE {tbl} REPLICA IDENTITY FULL"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    for txn in cdc_sum_workload(&tbl) {
        c.batch_execute(&format!("BEGIN; {}; COMMIT", txn.join("; ")))
            .unwrap();
    }
    let source_sum: i64 = c
        .query_one(
            &format!("SELECT COALESCE(SUM(v), 0)::bigint FROM {tbl}"),
            &[],
        )
        .unwrap()
        .get::<_, i64>(0);

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    let changes = read_cdc_changes(&out);
    assert!(
        intra_txn_multi_change_count(&changes) > 0,
        "workload must exercise intra-transaction multi-updates or the sum passes vacuously"
    );
    let target_sum = deduped_current_sum(changes, CdcEngine::Postgres);
    assert_eq!(
        source_sum, target_sum,
        "deduped-by-(__pos,__seq) SUM(v) must equal the source's SUM(v)"
    );
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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &cdc_out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out));
    assert_cdc_matches_batch(&cdc_out, &batch_out);
}

// CSV parity: the second (and last) CDC output format. The writer is shared
// with batch, so given ArrayData parity this SHOULD follow — but "should
// follow" is a construction argument, and the CSV renderer has its own
// per-type formatting (decimal text, datetime text, NULL). Compare the
// rendered text cell-for-cell. Values are comma/quote-free by construction so
// a positional split is exact (the CDC line prefixes __op and a JSON __pos
// that DO contain commas — compare from the right).
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_csv_rendering_matches_batch_csv() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_csv_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, amount DECIMAL(18,4), dt DATETIME(6), \
         d DATE, t TIME(6), note VARCHAR(40))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let cdc_yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: csv
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(&tbl),
    );
    let cfg = write_config(&d, &cdc_yaml);
    run_rivet_ok(&cfg); // pin
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES \
         (1, 999999999999.9999, '2035-08-07 09:08:07.987654', '2024-03-15', \
          '23:59:59.999999', 'plain text'), \
         (2, NULL, NULL, NULL, NULL, NULL)"
    ))
    .unwrap();
    run_rivet_ok(&cfg);
    let batch_yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}_batch
    query: "SELECT * FROM {tbl}"
    mode: full
    format: csv
    destination: {{ type: local, path: "{out}" }}
"#,
        out = batch_out.display(),
    );
    run_rivet_ok(&write_config(&d, &batch_yaml));

    let read_csv = |dir: &std::path::Path| -> Vec<String> {
        let p = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| p.extension().is_some_and(|x| x == "csv"))
            .expect("a .csv part");
        std::fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(|l| l.to_string())
            .collect()
    };
    let cdc_lines = read_csv(&out);
    let batch_lines = read_csv(&batch_out);
    assert_eq!(
        cdc_lines.len(),
        batch_lines.len(),
        "same row count + header"
    );
    const DATA_COLS: usize = 6;
    for (cl, bl) in cdc_lines.iter().zip(batch_lines.iter()) {
        // Data columns are the LAST 6 fields of the CDC line (after __op and
        // the comma-bearing quoted __pos) and the whole batch line.
        let cdc_tail: Vec<&str> = cl.rsplitn(DATA_COLS + 1, ',').collect();
        let cdc_data: Vec<&str> = cdc_tail[..DATA_COLS].iter().rev().cloned().collect();
        let batch_data: Vec<&str> = bl.split(',').collect();
        assert_eq!(
            cdc_data, batch_data,
            "CSV rendering differs between CDC and batch"
        );
    }
}

// Non-UTC source server, MySQL: the client's server runs in a local zone
// (`SET GLOBAL time_zone`), sessions inherit it. TIMESTAMP is stored as a UTC
// instant and rendered per session zone; rivet's batch session pins UTC and
// the binlog carries the raw epoch — so BOTH paths must yield the same UTC
// instant, and DATETIME (naive wall-clock) must stay the literal wall-clock.
// Pinned because every existing test runs the server at UTC where a
// zone-handling bug is invisible.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_non_utc_server_timezone_matches_batch_and_utc_instant() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_tz_my");
    // SET GLOBAL needs SYSTEM_VARIABLES_ADMIN — use the container's root.
    let root_url = MYSQL_CDC_URL.replace("rivet:rivet@", "root:rivet@");
    let mut admin = mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()).unwrap();
    use mysql::prelude::Queryable as _;
    let old_tz: String = admin
        .query_first("SELECT @@global.time_zone")
        .unwrap()
        .unwrap();
    admin
        .query_drop("SET GLOBAL time_zone = '+09:00'")
        .expect("set global tz");
    struct TzGuard(String, String);
    impl Drop for TzGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.1).unwrap()) {
                use mysql::prelude::Queryable as _;
                let _ = c.query_drop(format!("SET GLOBAL time_zone = '{}'", self.0));
            }
        }
    }
    let _tz = TzGuard(old_tz, root_url);

    // A FRESH session (inherits the +09:00 global) creates and fills the table.
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, ts TIMESTAMP(6), dt DATETIME(6))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out)); // pin
    // Wall-clock noon in +09:00 == 03:00:00Z.
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, '2024-06-15 12:00:00', '2024-06-15 12:00:00')"
    ))
    .unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out));
    assert_cdc_matches_batch(&out, &batch_out);

    use arrow::array::TimestampMicrosecondArray;
    let b = read_one_batch(&out);
    let val = |col: &str| -> i64 {
        b.column(b.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0)
    };
    // TIMESTAMP: the UTC instant (12:00+09 → 03:00Z), NOT the wall-clock.
    assert_eq!(
        val("ts"),
        1_718_420_400_000_000,
        "TIMESTAMP must be the UTC instant 2024-06-15T03:00:00Z"
    );
    // DATETIME: the naive wall-clock, zone-independent.
    assert_eq!(
        val("dt"),
        1_718_452_800_000_000,
        "DATETIME must stay the literal wall-clock 12:00:00"
    );
}

// Non-UTC source server, PostgreSQL: test_decoding renders TIMESTAMPTZ in the
// POLLING SESSION's zone — a non-UTC database default changes the rendered
// offset ('… 12:00:00+09'), and the parser must still recover the same UTC
// instant the batch path reads over the binary protocol.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_non_utc_database_timezone_matches_batch() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_tz_pg");
    let slot = unique_name("rivet_tz_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute("ALTER DATABASE rivet SET timezone TO 'Asia/Tokyo'")
        .expect("set db tz");
    struct DbTzGuard;
    impl Drop for DbTzGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_CDC_URL, NoTls) {
                let _ = c.batch_execute("ALTER DATABASE rivet RESET timezone");
            }
        }
    }
    let _tz = DbTzGuard;

    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (
           id INT PRIMARY KEY, tstz TIMESTAMPTZ, ts TIMESTAMP)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, '2024-06-15T03:00:00Z', '2024-06-15 12:00:00')"
    ))
    .unwrap();

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));
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
    run_rivet_ok(&write_config(&d, &batch_yaml));
    assert_cdc_matches_batch(&out, &batch_out);

    use arrow::array::TimestampMicrosecondArray;
    let b = read_one_batch(&out);
    let tstz = b
        .column(b.schema().index_of("tstz").unwrap())
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        tstz.value(0),
        1_718_420_400_000_000,
        "TIMESTAMPTZ must be the UTC instant regardless of the rendered zone"
    );
    let ts = b
        .column(b.schema().index_of("ts").unwrap())
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        ts.value(0),
        1_718_452_800_000_000,
        "naive TIMESTAMP must stay the literal wall-clock"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_non_iso_datestyle_and_escape_bytea_match_batch() {
    // Session-state rendering (CLAUDE.md): test_decoding renders values in the
    // polling session's FORMAT, not just its timezone. A non-default database
    // `datestyle` ('German, DMY') nulled every timestamp (rivet's ISO parser
    // failed on DMY text) and a non-hex `bytea_output` ('escape') corrupted every
    // bytea — both silent, found by the source-parity sweep under a flipped
    // session. The CDC reader now pins datestyle/bytea_output on connect, so CDC
    // matches a batch export (binary protocol, format-immune) regardless.
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_fmt_pg");
    let slot = unique_name("rivet_fmt_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(
        "ALTER DATABASE rivet SET datestyle TO 'German, DMY'; \
         ALTER DATABASE rivet SET bytea_output TO 'escape'",
    )
    .expect("set db formats");
    struct DbFmtGuard;
    impl Drop for DbFmtGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_CDC_URL, NoTls) {
                let _ = c.batch_execute(
                    "ALTER DATABASE rivet RESET datestyle; ALTER DATABASE rivet RESET bytea_output",
                );
            }
        }
    }
    let _fmt = DbFmtGuard;

    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (
           id INT PRIMARY KEY, d DATE, ts TIMESTAMP, blob BYTEA)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, '2024-03-05', '2024-03-05 12:00:00', '\\xdeadbeef')"
    ))
    .unwrap();

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));
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
    run_rivet_ok(&write_config(&d, &batch_yaml));
    // Batch reads via the binary protocol (format-immune); CDC via test_decoding
    // TEXT. Equal ⇒ the session-state pin held: date not nulled, bytea not mangled.
    assert_cdc_matches_batch(&out, &batch_out);
}

// UPDATE and DELETE through the typed surface — the matrix tests pin INSERT
// after-images only; this pins that an UPDATE's after-image carries every
// column type identically to a batch export of the post-update state, and a
// DELETE's key-image carries the typed PK. "Same builder by construction" is
// not a test; this is.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_update_and_delete_carry_full_types() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_updel_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, amount DECIMAL(18,4), dt DATETIME(6), \
         tm TIME(6), en ENUM('a','b','c'), st SET('x','y','z'), vb VARBINARY(8), \
         big BIGINT UNSIGNED, note TEXT)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out)); // pin
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, 1.5000, '2024-01-01 00:00:00', '01:02:03', \
         'a', 'x', 0xAA, 1, 'v1')"
    ))
    .unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));

    // UPDATE every column; the after-image must equal a batch export of the
    // post-update state, type for type, value for value.
    c.query_drop(format!(
        "UPDATE {tbl} SET amount=999999999999.9999, dt='2035-08-07 09:08:07.987654', \
         tm='23:59:59.999999', en='c', st='x,y,z', vb=0xDEADBEEF, \
         big=18446744073709551615, note='üñíçødé v2' WHERE id=1"
    ))
    .unwrap();
    let upd_out = d.path().join("upd");
    std::fs::create_dir_all(&upd_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &upd_out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out));
    let upd = read_one_batch(&upd_out);
    assert_eq!(upd.num_rows(), 1, "exactly the update event");
    assert_eq!(parquet_one_string(&upd_out, "__op"), "update");
    let batch = read_one_batch(&batch_out);
    for field in batch.schema().fields() {
        let bi = batch.schema().index_of(field.name()).unwrap();
        let ci = upd.schema().index_of(field.name()).unwrap();
        assert_eq!(
            batch.column(bi).to_data(),
            upd.column(ci).to_data(),
            "update after-image column {}: differs from post-update batch",
            field.name()
        );
    }

    // DELETE: the key-image event carries the typed PK.
    c.query_drop(format!("DELETE FROM {tbl} WHERE id=1"))
        .unwrap();
    let del_out = d.path().join("del");
    std::fs::create_dir_all(&del_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &del_out));
    let del = read_one_batch(&del_out);
    assert_eq!(del.num_rows(), 1);
    assert_eq!(parquet_one_string(&del_out, "__op"), "delete");
    use arrow::array::Int32Array;
    let id = del
        .column(del.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("typed PK in the delete image");
    assert_eq!(id.value(0), 1);
}

// PostgreSQL flavour — arrays, interval, uuid and numeric included in the
// updated surface (test_decoding emits the full after-image row).
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_update_and_delete_carry_full_types() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_updel_pg");
    let slot = unique_name("rivet_updel_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (
           id BIGINT PRIMARY KEY, amount NUMERIC(18,2), ts TIMESTAMPTZ, u UUID,
           tags TEXT[], nums INTEGER[], iv INTERVAL, note TEXT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, 1.50, '2024-01-01T00:00:00Z',
           'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011', ARRAY['a'], ARRAY[1],
           INTERVAL '1 day', 'v1')"
    ))
    .unwrap();
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    c.batch_execute(&format!(
        "UPDATE {tbl} SET amount=999999999999.99, ts='2035-08-07T09:08:07.987654Z',
           u='ffffffff-ffff-ffff-ffff-ffffffffffff',
           tags=ARRAY['with,comma', NULL], nums=ARRAY[7, NULL, 9],
           iv=INTERVAL '1 year 2 mons 3 days', note='üñíçødé v2' WHERE id=1"
    ))
    .unwrap();
    let upd_out = d.path().join("upd");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&upd_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &upd_out));
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
    run_rivet_ok(&write_config(&d, &batch_yaml));
    let upd = read_one_batch(&upd_out);
    assert_eq!(upd.num_rows(), 1, "exactly the update event");
    assert_eq!(parquet_one_string(&upd_out, "__op"), "update");
    let batch = read_one_batch(&batch_out);
    for field in batch.schema().fields() {
        let bi = batch.schema().index_of(field.name()).unwrap();
        let ci = upd.schema().index_of(field.name()).unwrap();
        assert_eq!(
            batch.column(bi).to_data(),
            upd.column(ci).to_data(),
            "update after-image column {}: differs from post-update batch",
            field.name()
        );
    }

    c.execute(&format!("DELETE FROM {tbl} WHERE id=1"), &[])
        .unwrap();
    let del_out = d.path().join("del");
    std::fs::create_dir_all(&del_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &del_out));
    let del = read_one_batch(&del_out);
    assert_eq!(del.num_rows(), 1);
    assert_eq!(parquet_one_string(&del_out, "__op"), "delete");
    use arrow::array::Int64Array;
    let id = del
        .column(del.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("typed PK in the delete image");
    assert_eq!(id.value(0), 1);
}

// Hostile values, PostgreSQL: ±Infinity/NaN FLOAT8 are representable and must
// ride CDC ArrayData-equal to batch; 'NaN'::NUMERIC is NOT representable in a
// Parquet decimal — the batch export fails LOUDLY on it
// ("unsupported NaN/infinity payload"), and CDC must fail the same way, never
// silently NULL the cell.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_hostile_floats_match_batch_and_nan_numeric_fails_loudly() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_hostile_pg");
    let slot = unique_name("rivet_hostile_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (
           id INT PRIMARY KEY, f8 FLOAT8, f4 REAL, n NUMERIC(18,2))"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    // Leg 1: hostile FLOATS (representable) — full parity required.
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES
           (1, 'Infinity', '-Infinity', 1.50),
           (2, '-Infinity', 'NaN', NULL),
           (3, 'NaN', 'Infinity', 0.01),
           (4, NULL, NULL, NULL)"
    ))
    .unwrap();
    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &cdc_out));
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
    run_rivet_ok(&write_config(&d, &batch_yaml));
    assert_cdc_matches_batch(&cdc_out, &batch_out);

    // Leg 2: 'NaN'::NUMERIC — the CDC run must FAIL, naming the payload.
    c.execute(
        &format!("INSERT INTO {tbl} VALUES (5, 1.0, 1.0, 'NaN')"),
        &[],
    )
    .unwrap();
    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            pg_cdc_config(&d, &tbl, &slot, &cdc_out).to_str().unwrap(),
        ])
        .output()
        .expect("spawn rivet");
    assert!(
        !out.status.success(),
        "CDC must fail loudly on NaN::numeric, like batch — not NULL it silently"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unsupported decimal payload"),
        "the failure must name the payload: {stderr}"
    );
}

// Hostile values, MySQL: a zero-date ('0000-00-00 00:00:00', insertable with
// sql_mode='') degrades to NULL on BOTH paths (no epoch equivalent exists —
// pinned as parity, not silence), and a NUL byte embedded in a VARCHAR
// survives both paths byte-for-byte.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_hostile_zero_date_and_nul_string_match_batch() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_hostile_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, dt DATETIME, s VARCHAR(20))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out)); // pin
    c.query_drop("SET SESSION sql_mode=''").unwrap();
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, '0000-00-00 00:00:00', CONCAT('a', CHAR(0), 'b')), \
         (2, '2024-03-15 12:00:00', 'plain')"
    ))
    .unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out));
    run_rivet_ok(&full_config(&d, &tbl, &batch_out));
    assert_cdc_matches_batch(&out, &batch_out);

    // And pin the zero-date outcome explicitly: NULL, not epoch garbage.
    use arrow::array::{Array, TimestampMicrosecondArray};
    let b = read_one_batch(&out);
    let dt_idx = b.schema().index_of("dt").unwrap();
    let dt = b
        .column(dt_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert!(dt.is_null(0), "zero-date degrades to NULL (documented)");
    assert!(!dt.is_null(1), "a real datetime stays");
    // The NUL byte survives inside the string.
    let s = parquet_one_string(&out, "s");
    assert_eq!(s.as_bytes(), b"a\0b", "embedded NUL survives byte-for-byte");
}

// Table-qualified `columns:` overrides on a multi-table stream: the bare key
// applies everywhere, `"table.column"` targets ONE table and wins over the
// bare key there — the out-of-the-box answer to same-named columns needing
// different overrides in schema-wide CDC.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_qualified_overrides_target_one_table_bare_applies_to_the_rest() {
    use arrow::datatypes::DataType;
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_qo_a");
    let tb = unique_name("cdc_qo_b");
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
    // Bare `v: text` hits every table; the qualified key retargets ONLY tb.
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: app_cdc
    tables: [{ta}, {tb}]
    mode: cdc
    format: parquet
    columns: {{ v: text, "{tb}.v": "decimal(20,4)" }}
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(&ta),
    );
    let cfg = write_config(&d, &yaml);

    run_rivet_ok(&cfg); // pin
    c.query_drop(format!("INSERT INTO {ta} VALUES (1, -42)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (1, 7)"))
        .unwrap();
    run_rivet_ok(&cfg);

    let ty_of = |t: &str| {
        parquet_fields(&out.join(t))
            .into_iter()
            .find(|(n, _)| n == "v")
            .map(|(_, ty)| ty)
            .unwrap()
    };
    assert_eq!(ty_of(&ta), DataType::Utf8, "bare `v: text` applies to a");
    assert_eq!(
        ty_of(&tb),
        DataType::Decimal128(20, 4),
        "qualified key wins over the bare one for b"
    );
    assert_eq!(parquet_one_string(&out.join(&ta), "v"), "-42");
    use arrow::array::Decimal128Array;
    let b = read_one_batch(&out.join(&tb));
    let bv = b
        .column(b.schema().index_of("v").unwrap())
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(bv.value(0), 70_000, "7 at scale 4");
}

// Same column NAME, different TYPES across tables of one multi-table stream:
// resolution is per-table by construction (each TableOutput resolves its own
// schema and cell fixes), so `a.v INT` and `b.v DECIMAL(10,2)` and
// `c.v ENUM(…)` must land as three different, correctly-typed columns — no
// cross-table bleed. Pinned because schema-wide CDC makes name collisions the
// NORM, not the exception.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_multi_table_same_column_name_different_types_resolve_per_table() {
    use arrow::datatypes::DataType;
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_nm_int");
    let tb = unique_name("cdc_nm_dec");
    let tc = unique_name("cdc_nm_enum");
    let mut c = conn();
    for (t, ty) in [
        (&ta, "INT"),
        (&tb, "DECIMAL(10,2)"),
        (&tc, "ENUM('on','off')"),
    ] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v {ty})"))
            .unwrap();
    }
    let (_g1, _g2, _g3) = (Table(ta.clone()), Table(tb.clone()), Table(tc.clone()));

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: app_cdc
    tables: [{ta}, {tb}, {tc}]
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

    run_rivet_ok(&cfg); // pin
    c.query_drop(format!("INSERT INTO {ta} VALUES (1, -42)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (1, 13.37)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tc} VALUES (1, 'off')"))
        .unwrap();
    run_rivet_ok(&cfg);

    let ty_of = |t: &str| {
        parquet_fields(&out.join(t))
            .into_iter()
            .find(|(n, _)| n == "v")
            .map(|(_, ty)| ty)
            .unwrap()
    };
    assert_eq!(ty_of(&ta), DataType::Int32, "a.v stays INT");
    assert_eq!(ty_of(&tb), DataType::Decimal128(10, 2), "b.v stays DECIMAL");
    assert_eq!(ty_of(&tc), DataType::Utf8, "c.v stays ENUM→Utf8");
    // Values: the enum INDEX must have become its label in c, while a kept -42.
    assert_eq!(parquet_one_string(&out.join(&tc), "v"), "off");
    use arrow::array::{Decimal128Array, Int32Array};
    let a = read_one_batch(&out.join(&ta));
    let av = a
        .column(a.schema().index_of("v").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(av.value(0), -42);
    let b = read_one_batch(&out.join(&tb));
    let bv = b
        .column(b.schema().index_of("v").unwrap())
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(bv.value(0), 1337, "13.37 at scale 2");
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
    run_rivet_ok(&cfg);
    let snap = out.join("snapshot");
    assert_eq!(
        manifest_rows(&snap),
        2,
        "pre-existing rows land in snapshot/"
    );
    assert_eq!(manifest_rows(&out), 0, "nothing to drain yet");
    assert_eq!(
        dir_parquet_id_set(&snap).into_iter().collect::<Vec<i64>>(),
        vec![1, 2],
        "snapshot parquet must hold exactly the pre-existing ids (independent re-read)"
    );
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
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
    assert_eq!(
        cdc_id_ops(&out),
        vec![(3, "insert".to_string())],
        "streamed parquet must hold exactly the post-snapshot change (not just a count of 1)"
    );
    assert_eq!(snap_parts(), 1, "run 2 must NOT re-snapshot");
}

// Roast finding #28 (feature composition): ensure_anchor ran with
// resume_expected=false on EVERY run of an `initial: snapshot` export — so a
// VANISHED slot was silently recreated at the current position BEFORE the
// vanished-slot protection could fire, and everything since the drop was
// silently lost. With resume evidence present (a completed snapshot marker /
// a checkpoint position), a missing slot must be a LOUD failure.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_initial_snapshot_vanished_slot_fails_loudly_not_recreates() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_vslot");
    let slot = unique_name("rivet_initv_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); \
         INSERT INTO {tbl} VALUES (1,10)"
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

    // Run 1: anchor + snapshot(1 row) + drain(0).
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join("snapshot")), 1);

    // The slot vanishes (admin cleanup / WAL-size invalidation), and a change
    // lands that the dropped slot would have carried.
    c.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (2,20)"), &[])
        .unwrap();

    // Run 2 MUST fail loudly — silently recreating the slot at the current
    // position would skip row 2 forever while reporting success.
    let res = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet");
    assert!(
        !res.status.success(),
        "a vanished slot with a completed snapshot behind it must FAIL, not silently re-anchor"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        stderr.contains("slot") && (stderr.contains("missing") || stderr.contains("dropped")),
        "the failure must explain the vanished slot: {stderr}"
    );
}

// Ultrareview bug_002 (live): a transaction whose LAST event lands on an
// UNCAPTURED table (audit-log-written-last, the ubiquitous ORM shape) must
// still advance the checkpoint — MySQL marks only that last event committed.
// Before the fix the checkpoint stalled forever and every scheduler cycle
// re-captured (and re-wrote) the same rows.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_mixed_transaction_ending_on_uncaptured_table_advances_checkpoint() {
    let d = tempfile::tempdir().unwrap();
    let orders = unique_name("cdc_mix_orders");
    let audit = unique_name("cdc_mix_audit");
    let mut c = conn();
    for t in [&orders, &audit] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let (_g1, _g2) = (Table(orders.clone()), Table(audit.clone()));

    let out1 = d.path().join("out1");
    let out2 = d.path().join("out2");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out1).unwrap();
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&cdc_config(&d, &orders, &ckpt, &out1)); // pin

    // ONE transaction: captured table first, uncaptured table LAST.
    c.query_drop("START TRANSACTION").unwrap();
    c.query_drop(format!("INSERT INTO {orders} VALUES (1, 10)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {audit} VALUES (1, 99)"))
        .unwrap();
    c.query_drop("COMMIT").unwrap();

    run_rivet_ok(&cdc_config(&d, &orders, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 1, "the captured row lands");

    // Run 3 with NO new changes must capture ZERO — a stalled checkpoint
    // would re-read the same transaction and duplicate the row.
    run_rivet_ok(&cdc_config(&d, &orders, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        0,
        "checkpoint must have advanced past the mixed transaction"
    );
}

// Conformance: the stream-property commit boundary, PostgreSQL flavour.
// PG stamps committed=true on every event (commit-LSN framing), so the MySQL
// stall cannot occur structurally — this pins that property per engine.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_mixed_transaction_ending_on_uncaptured_table_advances_checkpoint() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let orders = unique_name("cdc_mixp_o");
    let audit = unique_name("cdc_mixp_a");
    let slot = unique_name("rivet_mixp_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {orders}; DROP TABLE IF EXISTS {audit}; \
         CREATE TABLE {orders} (id INT PRIMARY KEY, v INT); \
         CREATE TABLE {audit} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let (_t1, _t2) = (
        PgTable::adopt(orders.clone()),
        PgTable::adopt(audit.clone()),
    );
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    c.batch_execute(&format!(
        "BEGIN; INSERT INTO {orders} VALUES (1,10); INSERT INTO {audit} VALUES (1,99); COMMIT;"
    ))
    .unwrap();

    let out1 = d.path().join("out1");
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out1).unwrap();
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &orders, &slot, &out1));
    assert_eq!(manifest_rows(&out1), 1, "the captured row lands");
    run_rivet_ok(&pg_cdc_config(&d, &orders, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        0,
        "slot advanced past the mixed transaction"
    );
}

// Conformance: schema-qualified `table:` routing, MySQL flavour (the schema
// part is the database name on MySQL).
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_schema_qualified_table_config_captures_events() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_qual_my");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let qualified = format!("rivet.{tbl}");
    run_rivet_ok(&cdc_config(&d, &qualified, &ckpt, &out)); // pin
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    run_rivet_ok(&cdc_config(&d, &qualified, &ckpt, &out));
    assert_eq!(
        manifest_rows(&out),
        1,
        "a db-qualified table: must capture, not 0-row-success"
    );
}

// Ultrareview bug_004 (live): a schema-qualified `table:` (`public.<t>`) —
// the shape rivet's own batch docs promote — must route events, not silently
// produce a 0-row success.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_schema_qualified_table_config_captures_events() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_qual_pg");
    let slot = unique_name("rivet_qual_slot");
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
    c.execute(&format!("INSERT INTO {tbl} VALUES (1, 10)"), &[])
        .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let qualified = format!("public.{tbl}");
    run_rivet_ok(&pg_cdc_config(&d, &qualified, &slot, &out));
    assert_eq!(
        manifest_rows(&out),
        1,
        "a schema-qualified table: must capture, not 0-row-success"
    );
}

// Roast finding #25: the snapshot synth export INHERITED skip_empty — an
// EMPTY table with skip_empty=true wrote no snapshot/_SUCCESS, so the marker
// check re-snapshotted on every run, forever. The handoff must converge: an
// empty snapshot still completes (0-row manifest + _SUCCESS), and run 2 goes
// straight to draining.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_initial_snapshot_of_an_empty_table_converges_despite_skip_empty() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_empty");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

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
    skip_empty: true
    cdc: {{ initial: snapshot, checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(&tbl),
    );
    let cfg = write_config(&d, &yaml);

    run_rivet_ok(&cfg);
    let marker = out.join("snapshot").join("_SUCCESS");
    assert!(
        marker.exists(),
        "an EMPTY snapshot must still write _SUCCESS or the handoff never converges"
    );
    let stamp = std::fs::metadata(&marker).unwrap().modified().unwrap();

    // Run 2 must NOT re-snapshot (marker untouched) and must drain the change.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    run_rivet_ok(&cfg);
    assert_eq!(
        std::fs::metadata(&marker).unwrap().modified().unwrap(),
        stamp,
        "run 2 must not re-snapshot"
    );
    assert_eq!(manifest_rows(&out), 1, "the change streams normally");
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

    run_rivet_ok(&cfg);
    let _slot = Slot(slot.clone());
    assert_eq!(manifest_rows(&out.join("snapshot")), 2);
    assert_eq!(
        dir_parquet_id_set(&out.join("snapshot"))
            .into_iter()
            .collect::<Vec<i64>>(),
        vec![1, 2],
        "snapshot parquet must hold exactly the pre-existing ids (independent re-read)"
    );
    assert_eq!(manifest_rows(&out), 0);

    c.execute(&format!("INSERT INTO {tbl} VALUES (3,30)"), &[])
        .unwrap();
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
    assert_eq!(
        cdc_id_ops(&out),
        vec![(3, "insert".to_string())],
        "streamed parquet must hold exactly the post-snapshot change (not just a count of 1)"
    );
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
    run_rivet_ok(&write_config(&d, &cdc_yaml));
    run_rivet_ok(&write_config(&d, &batch_yaml));

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
// canon, NULLs of text-shaped columns staying NULL (not ""), ARRAYS as real
// List columns (elements incl. inner NULLs, commas, quotes — not the PG
// literal text), and NUMERIC(p>38) as Decimal256. Full surface, no exceptions.
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
           doc_col JSON, ch_col CHAR(8), vc_col VARCHAR(50), float4_col REAL,
           tags TEXT[], nums INTEGER[], floats DOUBLE PRECISION[],
           big_num NUMERIC(60,10))"
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
            '{{\"k\": [1, 2]}}'::json, 'pad', 'plain varchar', 3.14,
            ARRAY['with,comma', 'he said \"hi\"', NULL], ARRAY[1, NULL, 3],
            ARRAY[2.5, -0.5], 123456789012345678901234567890.0123456789);
         INSERT INTO {tbl} VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL);
         INSERT INTO {tbl} (id, tags, nums) VALUES (3, ARRAY[]::text[], '{{}}');"
    ))
    .unwrap();

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &cdc_out));
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
    run_rivet_ok(&write_config(&d, &batch_yaml));
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
    run_rivet_ok(&cfg);
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
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join(&t1)), 2, "table 1 sub-prefix");
    assert_eq!(manifest_rows(&out.join(&t2)), 1, "table 2 sub-prefix");
    assert_eq!(
        cdc_id_ops(&out.join(&t1)),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "table 1 parquet holds exactly its own changes (routing, not just counts)"
    );
    assert_eq!(
        cdc_id_ops(&out.join(&t2)),
        vec![(7, "insert".to_string())],
        "table 2 parquet holds exactly its own changes (routing, not just counts)"
    );

    // Resume: the shared position advanced once for both tables.
    run_rivet_ok(&cfg);
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
    run_rivet_ok(&cfg);
    assert!(ckpt.exists(), "idle first run pins the shared checkpoint");

    c.query_drop(format!("INSERT INTO {ta} VALUES (1,10),(2,20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (7,70)"))
        .unwrap();
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join(&ta)), 2);
    assert_eq!(manifest_rows(&out.join(&tb)), 1);
    assert_eq!(
        cdc_id_ops(&out.join(&ta)),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "table a parquet holds exactly its own changes (routing, not just counts)"
    );
    assert_eq!(
        cdc_id_ops(&out.join(&tb)),
        vec![(7, "insert".to_string())],
        "table b parquet holds exactly its own changes (routing, not just counts)"
    );

    run_rivet_ok(&cfg);
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

    run_rivet_ok(&cfg); // pin
    c.query_drop(format!("INSERT INTO {ta} VALUES (1,10),(2,20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (7,70)"))
        .unwrap();
    run_rivet_ok(&cfg); // capture → upload

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
    run_rivet_ok(&write_config(&d, &yaml(&out1)));
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&write_config(&d, &yaml(&out2)));
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
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out1));
    let _slot = Slot(slot.clone());
    assert_eq!(manifest_rows(&out1), 0, "idle run 1 drains nothing");

    // A change lands BETWEEN the idle run and the next scheduler cycle.
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();

    // Run 2 must capture it — the slot created in run 1 pinned the position.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        1,
        "the change between an idle run and the next run must be captured, not skipped"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(1, "insert".to_string())],
        "the captured parquet must hold exactly THE change (a count of 1 could be a wrong row)"
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
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "the slot stayed put across the crash → resume re-reads both (no loss)"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(1, "insert".to_string()), (2, "insert".to_string())],
        "the re-read parquet must hold exactly the un-acked changes"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_crash_in_a_re_drain_pass_stays_at_least_once() {
    // The sink re-drain loop calls roll_all (flush → checkpoint → ack) ONCE PER
    // PASS, so it introduces a new crash window: a crash while acking an
    // uncaptured span in an EARLY pass, before the captured data of a LATER pass
    // is read. This must stay at-least-once: the pass-1 ack advances the slot
    // ONLY over the consumed uncaptured span (never into the not-yet-read
    // captured transaction), so resume reads the captured transaction WHOLE — no
    // loss, no duplication. `cdc_after_ack` fires on the first (uncaptured-span)
    // ack. Oracle: the source table A; assert distinct == count == 12 (no loss,
    // no dup) after the crash + resume.
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let a = unique_name("rivet_cdc_rdcap");
    let b = unique_name("rivet_cdc_rdforgn");
    let slot = unique_name("rivet_rd_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {a}; DROP TABLE IF EXISTS {b}; \
         CREATE TABLE {a} (id BIGINT PRIMARY KEY, v INT); \
         CREATE TABLE {b} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _ta = PgTable::adopt(a.clone());
    let _tb = PgTable::adopt(b.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    // A large UNCAPTURED transaction (pass 1 consumes + acks it), THEN A's
    // in-bound data as ONE 12-row transaction (read only after the slot slides).
    c.execute(
        &format!("INSERT INTO {b} SELECT g, g FROM generate_series(1, 100) g"),
        &[],
    )
    .unwrap();
    c.execute(
        &format!("INSERT INTO {a} SELECT g, g FROM generate_series(0, 11) g"),
        &[],
    )
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&a, &slot)
        .cdc("rollover: 5")
        .dest_path(out.clone());
    // Run 1 crashes right after the FIRST ack (the pass-1 uncaptured-span ack).
    let crashed = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", rig.config_path().to_str().unwrap()])
        .env("RIVET_TEST_PANIC_AT", "cdc_after_ack")
        .output()
        .expect("spawn rivet");
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2 resumes from wherever the crash left the slot.
    let rig2 = Rig::pg_cdc(&a, &slot).dest_path(out.clone());
    run_rivet_ok(&rig2.config_path());

    let ids = dir_parquet_i64(&out, "id");
    let distinct: std::collections::BTreeSet<i64> = ids.iter().copied().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        distinct, want,
        "A's 12-row transaction must survive the mid-re-drain crash whole — got {:?} \
         (a pass-1 ack that overshot into A's un-read transaction lost part of it)",
        distinct
    );
    assert_eq!(
        ids.len(),
        12,
        "no duplication across the crash + resume — got {} rows for 12 distinct ids",
        ids.len()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_large_transaction_is_atomic_across_a_mid_flush_crash() {
    // A single source transaction LARGER than `rollover` must roll + ack as ONE
    // unit — the sink's "never split a transaction across parts" invariant. Every
    // `test_decoding` event carried `committed: true`, so the sink used to roll +
    // checkpoint + ack MID-transaction (after `rollover` rows); a crash between
    // that ack and the tail's flush advanced the slot PAST the transaction's
    // commit, and resume (reading strictly after the slot) never re-read the tail
    // — an at-least-once break. Fix: the adapter marks only the LAST event of a
    // transaction committed. RED-proof: one 12-row transaction at rollover 5,
    // crash at `cdc_after_ack` (the first ack). With the bug that ack lands after
    // 5 rows and the crash loses 7; atomic, it lands after all 12 and the run's
    // part holds the whole transaction. Oracle: the union of all parts on disk.
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgatomic");
    let slot = unique_name("rivet_atomic_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    // ONE transaction, 12 rows (> 2× the rollover of 5).
    c.execute(
        &format!("INSERT INTO {tbl} SELECT g, g FROM generate_series(0, 11) g"),
        &[],
    )
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc("rollover: 5")
        .dest_path(out.clone());
    // Run 1 crashes right after the FIRST ack.
    let crashed = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", rig.config_path().to_str().unwrap()])
        .env("RIVET_TEST_PANIC_AT", "cdc_after_ack")
        .output()
        .expect("spawn rivet");
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2 resumes from the slot (whatever position the crash left it at).
    let rig2 = Rig::pg_cdc(&tbl, &slot).dest_path(out.clone());
    run_rivet_ok(&rig2.config_path());

    let got: std::collections::BTreeSet<i64> = dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got,
        want,
        "the 12-row transaction must survive the mid-flush crash whole — got {} ids \
         (a mid-transaction ack advanced the slot past the commit and lost the tail)",
        got.len()
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_cdc_large_transaction_is_atomic_across_a_mid_flush_crash() {
    // MySQL sibling of the PG atomicity roast — the matrix cell was `na` on the
    // reasoning "the binlog adapter marks only the XID event committed", but that
    // is CURRENT-CORRECTNESS, not immunity: MySQL stamps the shared COMMIT position
    // on EVERY event of the transaction (`ev.position = commit.clone()`), exactly
    // like PG's shared commit LSN. The only thing stopping a mid-transaction
    // roll+checkpoint+ack is `ev.committed = i + 1 == n`. Flip that to `true` (the
    // committed-on-every-event mutant) and a crash between the first ack and the
    // tail's flush advances the binlog checkpoint PAST the commit — resume reads
    // strictly after it and loses the tail. RED-proof: one 12-row transaction at
    // rollover 5, crash at `cdc_after_ack`. Buggy: first ack after 5 rows → 5 ids
    // survive; atomic: first ack after all 12 → 12 ids. Oracle: union of parts.
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_myatomic");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let _drop = Table(tbl.clone());

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&tbl)
        .cdc("rollover: 5")
        .dest_path(out.clone());
    // MySQL has NO server-side anchor — pin the binlog checkpoint at open, BEFORE
    // the transaction, or the next run re-anchors to the current position and skips.
    rig.run_ok();
    // ONE transaction of 12 rows (> 2× the rollover of 5) — a single multi-row
    // INSERT is one commit.
    let vals = (0..12)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    c.query_drop(format!("INSERT INTO {tbl} VALUES {vals}"))
        .unwrap();

    // Run 1 crashes right after the FIRST ack.
    let crashed = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", rig.config_path().to_str().unwrap()])
        .env("RIVET_TEST_PANIC_AT", "cdc_after_ack")
        .output()
        .expect("spawn rivet");
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2 resumes from the checkpoint the crash left behind.
    run_rivet_ok(&rig.config_path());

    let got: std::collections::BTreeSet<i64> = dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got,
        want,
        "the 12-row transaction must survive the mid-flush crash whole — got {} ids \
         (a mid-transaction ack advanced the binlog checkpoint past the commit and lost the tail)",
        got.len()
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
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &cdc_out));
    run_rivet_ok(&pg_full_config(&d, &tbl, &batch_out));

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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out1));
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
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume must capture exactly the 2 changes since the checkpoint (no gap, no dup)"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(3, "insert".to_string()), (4, "insert".to_string())],
        "the resumed parquet must hold exactly the NEW changes (count 2 cannot tell new-2 from wrong-2)"
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
    run_rivet_ok(&cfg);

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

// ─── schema drift + bounded-run termination (coverage-matrix gap fills) ──────

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_column_added_mid_stream_is_captured() {
    // Schema-drift peer of cdc_picks_up_a_column_added_between_runs for PostgreSQL.
    // The sink re-resolves the table schema at the start of each run, and
    // test_decoding renders each change against the table's CURRENT column list,
    // so a column added between runs is captured on the next run — the pgoutput/
    // test_decoding column-add path the matrix flagged as never live-exercised.
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgdrift");
    let slot = unique_name("rivet_drift_slot");
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

    // Run 1: capture a row under the original (id, v) schema.
    c.execute(&format!("INSERT INTO {tbl} VALUES (1, 10)"), &[])
        .unwrap();
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out1));
    assert!(
        !dir_parquet_has_column(&out1, "w"),
        "run 1 predates the added column"
    );

    // Add a column, then a row that uses it.
    c.batch_execute(&format!(
        "ALTER TABLE {tbl} ADD COLUMN w TEXT; INSERT INTO {tbl} VALUES (2, 20, 'hello')"
    ))
    .unwrap();

    // Run 2 (resume, same slot): re-resolves the schema → the new column is captured.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out2));
    assert!(
        dir_parquet_has_column(&out2, "w"),
        "run 2 must re-resolve and pick up the column added between runs"
    );
    assert_eq!(parquet_one_string(&out2, "w"), "hello");
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_until_current_terminates_under_sustained_writes() {
    // Peer of the Mongo roast_until_current_terminates_under_sustained_writes.
    // A bounded run must (1) TERMINATE at the open-time WAL bound even while a
    // writer keeps committing — a drain loop that chases a moving "current" hangs
    // forever — and (2) still capture the pre-open backlog. Assert both.
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pghb");
    let slot = unique_name("rivet_hb_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    // Pre-open backlog: ids 0..30 (the slot captures them because it exists first).
    for i in 0..30i64 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    // A writer committing continuously while the bounded run drains.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_bg = stop.clone();
    let tbl_bg = tbl.clone();
    let bg = std::thread::spawn(move || {
        let mut w = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("bg connect");
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = w.execute(&format!("INSERT INTO {tbl_bg} VALUES ({i},{i})"), &[]);
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let elapsed = run_rivet_bounded(
        &pg_cdc_config(&d, &tbl, &slot, &out),
        std::time::Duration::from_secs(30),
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = bg.join();

    assert!(
        elapsed.is_some(),
        "until_current must terminate under sustained writes (killed at the 30s ceiling)"
    );
    // Termination must NOT come from dropping the backlog.
    let ids: std::collections::BTreeSet<i64> = dir_parquet_i64(&out, "id").into_iter().collect();
    for i in 0..30 {
        assert!(
            ids.contains(&i),
            "backlog id {i} must be captured, got {} ids",
            ids.len()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_until_current_terminates_under_sustained_writes() {
    // MySQL peer: the binlog is a live tail, so this is the engine most at risk of
    // a drain loop that never reaches its stop condition under continuous writes.
    // The bound must be pinned at the open-time binlog position; the backlog must
    // still survive.
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_myhb");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt); // pin before the backlog

    // Pre-open backlog: ids 0..30.
    let vals: Vec<String> = (0..30).map(|i| format!("({i},{i})")).collect();
    c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
        .unwrap();

    // A writer committing continuously while the bounded run drains.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_bg = stop.clone();
    let tbl_bg = tbl.clone();
    let bg = std::thread::spawn(move || {
        let mut w = conn();
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = w.query_drop(format!("INSERT INTO {tbl_bg} VALUES ({i},{i})"));
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let elapsed = run_rivet_bounded(
        &cdc_config(&d, &tbl, &ckpt, &out),
        std::time::Duration::from_secs(30),
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = bg.join();

    assert!(
        elapsed.is_some(),
        "until_current must terminate under sustained writes (killed at the 30s ceiling)"
    );
    let ids: std::collections::BTreeSet<i64> = dir_parquet_i64(&out, "id").into_iter().collect();
    for i in 0..30 {
        assert!(
            ids.contains(&i),
            "backlog id {i} must be captured, got {} ids",
            ids.len()
        );
    }
}

// ─── Open-time bound: "until current" means current AS OF OPEN, not a chase ──

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_until_current_open_bound_two_runs_lose_nothing() {
    // The RED shape for the pinned open-time WAL bound. `rollover: 5` makes the
    // peek limit 5 while the writer below commits faster than one roll cycle
    // (encode + part write + ack), so every re-peek returns a FULL batch and
    // the catch-up exit (short/empty peek) never fires — a drain chasing the
    // moving head runs to the kill ceiling. With the bound pinned at open,
    // run 1 is O(backlog at open) and terminates; run 2 (writer stopped)
    // drains the deferred tail. The distinct id union re-read from the parquet
    // must equal the SOURCE table's committed id set — the bound defers,
    // never drops (oracle: the source, not rivet's own counters).
    use postgres::NoTls;
    let tbl = unique_name("rivet_cdc_pgob");
    let slot = unique_name("rivet_ob_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    // Pre-open backlog: ids 0..30.
    for i in 0..30i64 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    // A writer committing a 10-row transaction every ~5 ms — each is 12 peek
    // rows (BEGIN + 10 + COMMIT), so ≥ one roll cycle's worth (the ×3-scaled
    // peek budget of 15) lands between refills and a chase-the-head drain sees
    // a FULL peek every time: the catch-up exit (short/empty peek) never
    // fires. Paced (not flooding) so the pre-open backlog stays small enough
    // for run 1 to reach its bound inside the kill ceiling at 5-row parts.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_bg = stop.clone();
    let tbl_bg = tbl.clone();
    let bg = std::thread::spawn(move || {
        let mut w = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("bg connect");
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let vals: Vec<String> = (i..i + 10).map(|k| format!("({k},{k})")).collect();
            let _ = w.batch_execute(&format!("INSERT INTO {tbl_bg} VALUES {}", vals.join(",")));
            i += 10;
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    let rig = Rig::pg_cdc(&tbl, &slot).cdc("rollover: 5");
    let cfg = rig.config_path();
    let elapsed = run_rivet_bounded(&cfg, std::time::Duration::from_secs(30));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = bg.join();
    assert!(
        elapsed.is_some(),
        "run 1 must terminate at the open-time WAL bound under sustained writes \
         (killed at the 30s ceiling ⇒ the drain chased the moving head)"
    );

    // Writer stopped ⇒ every committed change predates run 2's own bound.
    // Run 2 drains the deferred tail at the DEFAULT rollover (5-row parts would
    // grind through a multi-thousand-row tail one tiny parquet file at a time)
    // into the SAME prefix — parts are run-unique, both runs' rows accumulate.
    let rig2 = Rig::pg_cdc(&tbl, &slot).dest_path(rig.out_dir());
    let elapsed2 = run_rivet_bounded(&rig2.config_path(), std::time::Duration::from_secs(60));
    assert!(
        elapsed2.is_some(),
        "run 2 (no writers) must drain the tail and exit"
    );

    let got: std::collections::BTreeSet<i64> =
        dir_parquet_i64(&rig.out_dir(), "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = c
        .query(&format!("SELECT id FROM {tbl}"), &[])
        .unwrap()
        .iter()
        .map(|r| r.get::<_, i64>(0))
        .collect();
    assert_eq!(
        got, want,
        "run1 ∪ run2 must hold exactly the source's committed ids — the bound \
         defers the tail to run 2, never drops it"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_until_current_open_bound_two_runs_lose_nothing() {
    // MySQL peer of roast_pg_until_current_open_bound_two_runs_lose_nothing, but
    // a DIFFERENT contract: on MySQL termination comes from the engine, not the
    // explicit bound. `BINLOG_DUMP_NON_BLOCK` stops the dump at the log end as of
    // dump-start — empirically it terminates even under a flooding writer with
    // the (file, pos) bound DISABLED (verified by the disable-bound RED probe:
    // the run still exited). So the open-time (file, pos) ceiling is a
    // PRECISE-STOP refinement over NON_BLOCK, not load-bearing for termination —
    // the load-bearing engines are PostgreSQL (continuous slot re-peek, see
    // roast_pg_until_current_open_bound_two_runs_lose_nothing at rollover 5) and
    // MongoDB (tailable stream — disabling its pin hangs the sustained test).
    // What THIS test proves is DEFER-NOT-DROP: run 1 captures a prefix and exits,
    // run 2 drains the tail, and the union re-read from the parquet equals the
    // SOURCE id set. Oracle: the source table, never rivet's own counters.
    let tbl = unique_name("rivet_cdc_myob");
    let _drop = Table(tbl.clone());
    let mut c = conn();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let rig = Rig::mysql_cdc(&tbl);
    write_checkpoint(&mut c, &rig.checkpoint()); // pin before the backlog

    // Pre-open backlog: ids 0..30.
    let vals: Vec<String> = (0..30).map(|i| format!("({i},{i})")).collect();
    c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
        .unwrap();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_bg = stop.clone();
    let tbl_bg = tbl.clone();
    let bg = std::thread::spawn(move || {
        let mut w = conn();
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = w.query_drop(format!("INSERT INTO {tbl_bg} VALUES ({i},{i})"));
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    });

    let cfg = rig.config_path();
    let elapsed = run_rivet_bounded(&cfg, std::time::Duration::from_secs(30));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = bg.join();
    assert!(
        elapsed.is_some(),
        "run 1 must terminate under sustained writes (NON_BLOCK EOF; killed at 30s)"
    );

    // Writer stopped ⇒ every committed change predates run 2's own bound.
    let elapsed2 = run_rivet_bounded(&cfg, std::time::Duration::from_secs(60));
    assert!(
        elapsed2.is_some(),
        "run 2 (no writers) must drain the tail and exit"
    );

    let got: std::collections::BTreeSet<i64> =
        dir_parquet_i64(&rig.out_dir(), "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = c
        .query_map(format!("SELECT id FROM {tbl}"), |id: i64| id)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        got, want,
        "run1 ∪ run2 must hold exactly the source's committed ids — the bound \
         defers the tail to run 2, never drops it"
    );
}

/// A throwaway PostgreSQL database on the CDC server, isolating a test's logical
/// slot from every other test's WAL. A `test_decoding` slot decodes its
/// database's ENTIRE WAL, so a DENSITY- or slot-state-sensitive CDC test
/// (reach-the-open-bound-in-one-pass, confirmed_flush advance) FLAKES on the
/// shared `rivet` DB when parallel tests inject foreign WAL into the same slot's
/// view (the failing `cargo test --ignored` lanes run these in parallel). Its
/// own database makes the slot see only this test's WAL — parallel-safe by
/// construction, no `--test-threads=1` needed. Dropped (backends terminated) on
/// teardown; the table + slot live inside it, so no separate guards are needed.
struct CdcDb {
    name: String,
    url: String,
}
impl CdcDb {
    fn new(label: &str) -> Self {
        let name = unique_name(label).to_lowercase();
        let mut admin = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls)
            .expect("connect cdc admin");
        // CREATE DATABASE cannot run inside a transaction — a single simple-query
        // batch_execute autocommits it.
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .expect("create dedicated cdc db");
        let base = POSTGRES_CDC_URL
            .rsplit_once('/')
            .expect("cdc url has a /db path")
            .0;
        Self {
            url: format!("{base}/{name}"),
            name,
        }
    }
    fn url(&self) -> &str {
        &self.url
    }
    fn connect(&self) -> postgres::Client {
        postgres::Client::connect(&self.url, postgres::NoTls).expect("connect dedicated cdc db")
    }
}
impl Drop for CdcDb {
    fn drop(&mut self) {
        if let Ok(mut admin) = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls) {
            let _ = admin.batch_execute(&format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = '{}' AND pid <> pg_backend_pid()",
                self.name
            ));
            let _ = admin.batch_execute(&format!("DROP DATABASE IF EXISTS {}", self.name));
        }
    }
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_reaches_open_bound_past_a_large_empty_ddl_span() {
    // Ultracode r2 finding: a pure-EMPTY (DDL) span LARGER than one peek window,
    // sitting ahead of in-bound captured data, was drained only one window per
    // run — empty transactions yield NO events to the sink, so the sink's
    // re-drain ack never fires; only the adapter's `release_empty_frontier`
    // advances the slot, and it used to release just one window before
    // `next_change` returned None and the run wrote _SUCCESS with the in-bound
    // data still unread. Fix: `next_change` now walks the WHOLE empty span in one
    // call (release → re-peek loop). rollover 5 makes the window ~2 empty
    // transactions, so a 40-transaction DDL burst is ~20 windows. Oracle: the
    // SOURCE table A — one bounded run must capture all 12 rows.
    // Isolated in its OWN database so parallel tests' WAL never enters this slot's
    // view — the slot decodes the whole DB, the very premise this test exercises.
    let cdc_db = CdcDb::new("cdc_ddlspan");
    let a = unique_name("rivet_cdc_ddlspan");
    let slot = unique_name("rivet_ddl_slot");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!("CREATE TABLE {a} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    // A large EMPTY span: 40 DDL transactions (row-less BEGIN/COMMIT) ≫ one
    // window at rollover 5 — created AFTER the slot so they are in the WAL ahead
    // of A's in-bound rows.
    for i in 0..40 {
        c.batch_execute(&format!(
            "CREATE TABLE {a}_ddl_{i} (x int); DROP TABLE {a}_ddl_{i}"
        ))
        .unwrap();
    }
    // A's in-bound data, behind the empty span.
    for i in 0..12i64 {
        c.execute(&format!("INSERT INTO {a} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&a, &slot)
        .source_url(cdc_db.url())
        .cdc("rollover: 5")
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());

    let got: std::collections::BTreeSet<i64> = dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got,
        want,
        "one bounded run must capture all of A's in-bound rows past the large \
         empty DDL span — got {} ids (the run stopped after one window of the \
         empty span and wrote _SUCCESS with in-bound data unread)",
        got.len()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_empty_transaction_churn_must_not_pin_the_slot() {
    // DDL-only churn decodes as EMPTY transactions (BEGIN/COMMIT, no rows):
    // nothing reaches the sink, so the sink never acks, and the slot keeps
    // pinning WAL from before the noise — on an idle database, forever (the
    // uncaptured-DML case is different: it yields events and acks via the
    // bug-hunt-K final roll). A run that yields NOTHING must release the
    // data-free span itself — advancing past it can lose nothing by
    // construction. Oracle: the slot's confirmed_flush_lsn, asked of PostgreSQL
    // itself, never rivet's counters.
    // Isolated in its OWN database (see CdcDb): this test asserts confirmed_flush_lsn
    // against PostgreSQL, which a parallel test's WAL on the shared DB would perturb.
    let cdc_db = CdcDb::new("cdc_empty");
    let tbl = unique_name("rivet_cdc_pgempty");
    let slot = unique_name("rivet_empty_slot");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    let rig = Rig::pg_cdc(&tbl, &slot).source_url(cdc_db.url());
    let cfg = rig.config_path();
    run_rivet_ok(&cfg); // baseline bounded run (captures nothing)
    let before: String = c
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);

    // Empty-transaction churn: each DDL pair decodes as row-less transactions.
    for i in 0..20 {
        c.batch_execute(&format!(
            "CREATE TABLE {tbl}_junk_{i} (id INT); DROP TABLE {tbl}_junk_{i}"
        ))
        .unwrap();
    }

    run_rivet_ok(&cfg); // captures nothing — but must release the empty span
    let advanced: bool = c
        .query_one(
            &format!(
                "SELECT confirmed_flush_lsn > '{before}'::pg_lsn \
                 FROM pg_replication_slots WHERE slot_name = $1"
            ),
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(
        advanced,
        "a zero-yield run must advance the slot past the empty-transaction span \
         (confirmed_flush_lsn stuck at {before} — WAL pinned behind DDL noise)"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_ndjson_until_current_terminates_and_emits_backlog() {
    // The NDJSON driver (`rivet cdc` without --output) shares
    // create_change_stream with the file sink — this anchors the CLI path
    // (matrix: cdc_ndjson_bounded). Termination here is the driver's own: the
    // NDJSON path uses ONE `PeekBound::Unbounded` peek (a single snapshot query),
    // so it terminates regardless of the open-time bound — the bound only clips
    // which rows that one snapshot yields, it is not load-bearing for
    // termination (the ACKING file-sink path re-peeks on PostgreSQL, and the
    // tailable stream on MongoDB, are what genuinely need the bound). What THIS
    // test proves: the CLI path terminates and emits the
    // whole pre-open backlog to stdout. No ack by design (stdout is not durable,
    // ADR-0023): the slot is left for the consumer.
    use postgres::NoTls;
    let tbl = unique_name("rivet_cdc_pgnd");
    let slot = unique_name("rivet_nd_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    for i in 0..30i64 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_bg = stop.clone();
    let tbl_bg = tbl.clone();
    let bg = std::thread::spawn(move || {
        let mut w = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("bg connect");
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let vals: Vec<String> = (i..i + 10).map(|k| format!("({k},{k})")).collect();
            let _ = w.batch_execute(&format!("INSERT INTO {tbl_bg} VALUES {}", vals.join(",")));
            i += 10;
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    let out = run_rivet_args_bounded(
        &[
            "cdc",
            "--source",
            POSTGRES_CDC_URL,
            "--slot",
            &slot,
            "--table",
            &tbl,
            "--until-current",
        ],
        std::time::Duration::from_secs(30),
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = bg.join();
    let stdout = out.expect("bounded NDJSON run must terminate under sustained writes");

    let ids: std::collections::BTreeSet<i64> = stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("table").and_then(|t| t.as_str()) == Some(tbl.as_str()))
        .filter_map(|v| v.get("after")?.get(0)?.as_i64())
        .collect();
    for i in 0..30 {
        assert!(
            ids.contains(&i),
            "backlog id {i} must be emitted to stdout, got {} ids",
            ids.len()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_reaches_open_bound_past_a_large_uncaptured_transaction() {
    // The density-below-1/3 gap (ultracode HIGH): a bounded run captures table A
    // but the slot decodes the WHOLE database, so an UNCAPTURED table B's large
    // transaction sits in the WAL ahead of A's in-bound changes. The slot only
    // advances on a captured-row ack, and B's rows are dropped by the routing
    // filter — so a peek window smaller than B's transaction re-read the same
    // span forever, the run exhausted, and it wrote _SUCCESS with ZERO of A's
    // in-bound rows (deferred to the next run — the O(backlog-at-open) contract
    // broken). With the sink re-drain loop the end-of-pass ack advances the slot
    // past B, and the next pass reads A. rollover: 5 makes any B transaction of
    // >15 rows exceed the old escalated window. Oracle: the SOURCE table A.
    // Isolated in its OWN database (see CdcDb): the slot decodes the whole DB, so
    // table B's large uncaptured transaction — and no parallel test's WAL — sits
    // ahead of A. Lowercase names only: PostgreSQL folds unquoted identifiers, so
    // test_decoding renders (and routing matches) the lowercased table name.
    let cdc_db = CdcDb::new("cdc_dens");
    let a = unique_name("rivet_cdc_capa");
    let b = unique_name("rivet_cdc_forgnb");
    let slot = unique_name("rivet_dens_slot");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {a} (id BIGINT PRIMARY KEY, v INT); \
         CREATE TABLE {b} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    // One large UNCAPTURED transaction (200 rows) lands in the WAL BEFORE A's
    // in-bound data — this is the span the peek window cannot fit at rollover 5.
    c.execute(
        &format!("INSERT INTO {b} SELECT g, g FROM generate_series(1, 200) g"),
        &[],
    )
    .unwrap();
    // A's in-bound backlog: ids 0..30, committed after B's tx, before open.
    for i in 0..30i64 {
        c.execute(&format!("INSERT INTO {a} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let rig = Rig::pg_cdc(&a, &slot)
        .source_url(cdc_db.url())
        .cdc("rollover: 5");
    run_rivet_ok(&rig.config_path());

    let got: std::collections::BTreeSet<i64> =
        dir_parquet_i64(&rig.out_dir(), "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..30).collect();
    assert_eq!(
        got,
        want,
        "a single bounded run must capture ALL of A's in-bound rows past the \
         large uncaptured B transaction — got {} ids (the slot starved on B and \
         exhausted before reaching A)",
        got.len()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_drain_releases_pinned_wal_and_advances_xmin() {
    // Harm metric (the "an abandoned slot fills the disk" caveat): an un-consumed
    // logical slot pins WAL and holds `catalog_xmin` (blocks vacuum). The
    // consumer position that governs release is `confirmed_flush_lsn` — the ack
    // advances it, and `restart_lsn` (the actual WAL floor) follows at the next
    // checkpoint. A bounded until_current drain must advance confirmed_flush past
    // the drained span (so `pg_wal_lsn_diff(current, confirmed_flush_lsn)`
    // collapses) and let catalog_xmin move forward. Oracle: the server's own
    // pg_replication_slots, never rivet's counters. RED-able: a drain that
    // captured but did not ack leaves confirmed_flush pinned.
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_walret");
    let slot = unique_name("rivet_walret_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id bigint primary key, v int, pad text)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    let retained = |c: &mut postgres::Client| -> i64 {
        c.query_one(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0)
    };
    // Generate a backlog (≈20k rows of WAL) the slot now pins.
    c.execute(
        &format!(
            "INSERT INTO {tbl} SELECT g, g%1000, repeat('x',80) FROM generate_series(1,20000) g"
        ),
        &[],
    )
    .unwrap();
    let retained_before = retained(&mut c);
    assert!(
        retained_before > 1_000_000,
        "the slot must pin a real amount of WAL before the drain (got {retained_before} bytes)"
    );

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    let retained_after = retained(&mut c);
    assert_eq!(
        manifest_rows(&out),
        20000,
        "the drain must capture the whole backlog"
    );
    assert!(
        retained_after < retained_before / 4,
        "the drain must RELEASE the pinned WAL: retained {retained_before} -> {retained_after} bytes \
         (an un-acked drain leaves it pinned — the disk-fill harm). catalog_xmin (vacuum) is \
         released by the same confirmed_flush advance, at the next checkpoint."
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_captures_a_silent_update_a_watermark_sync_would_miss() {
    // The reason log-based CDC exists: it captures a row change that touches a
    // value WITHOUT bumping `updated_at` — the exact update a watermark /
    // incremental sync (`WHERE updated_at > last_seen`) MISSES. Assert (1) the
    // source's `updated_at` is UNCHANGED by the silent update (so a watermark
    // sync at that timestamp would never re-read the row), and (2) CDC captured
    // the update with the NEW value anyway. Oracle: the source row's updated_at
    // (proves the miss) + the parquet (proves the capture).
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_silent");
    let slot = unique_name("rivet_silent_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id bigint primary key, v bigint, updated_at timestamptz)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    // Insert a row with a fixed watermark.
    c.execute(
        &format!("INSERT INTO {tbl} VALUES (1, 0, '2020-01-01T00:00:00Z')"),
        &[],
    )
    .unwrap();
    let wm_before: chrono::DateTime<chrono::Utc> = c
        .query_one(&format!("SELECT updated_at FROM {tbl} WHERE id=1"), &[])
        .unwrap()
        .get(0);

    // SILENT update — changes view_count, does NOT touch updated_at.
    c.execute(&format!("UPDATE {tbl} SET v = 42 WHERE id = 1"), &[])
        .unwrap();
    let wm_after: chrono::DateTime<chrono::Utc> = c
        .query_one(&format!("SELECT updated_at FROM {tbl} WHERE id=1"), &[])
        .unwrap()
        .get(0);
    assert_eq!(
        wm_before, wm_after,
        "the silent update must NOT bump updated_at — a watermark sync would miss it"
    );

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_rivet_ok(&pg_cdc_config(&d, &tbl, &slot, &out));

    // CDC caught both the insert and the SILENT update, with the new view_count.
    let ops = cdc_id_ops(&out);
    assert_eq!(
        ops,
        vec![(1, "insert".to_string()), (1, "update".to_string())],
        "CDC must capture the insert AND the silent update the watermark missed — got {ops:?}"
    );
    let vcs: Vec<i64> = dir_parquet_i64(&out, "v");
    assert_eq!(
        vcs,
        vec![0, 42],
        "the captured after-images must carry the silent update's NEW value (0 then 42) — got {vcs:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_oversized_transaction_bails_loud_not_oom() {
    // Memory backstop: a transaction is buffered WHOLE (never split across parts),
    // so an oversized one would grow the buffer unbounded → OOM. The adapter caps
    // the per-transaction buffer at `max_tx_rows()` and bails LOUDLY instead. The
    // cap is 5M by default; `RIVET_CDC_MAX_TX_ROWS` lowers it so this is testable
    // without a 5-million-row transaction. Oracle: the run FAILS with the cap
    // message (never a silent OOM / partial capture).
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_bigtx");
    let slot = unique_name("rivet_bigtx_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id bigint primary key, v bigint)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    // ONE transaction of 20 rows — over the cap-of-10 this run sets.
    c.execute(
        &format!("INSERT INTO {tbl} SELECT g, g FROM generate_series(1, 20) g"),
        &[],
    )
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot).dest_path(out);
    let output = rig.run_with_env("RIVET_CDC_MAX_TX_ROWS", "10");
    assert!(
        !output.status.success(),
        "an over-cap transaction must FAIL the run, not OOM or silently truncate"
    );
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("more than 10 rows") && err.contains("buffered whole"),
        "the failure must name the cap and the never-split-a-transaction reason — got:\n{err}"
    );
}

#[test]
#[ignore = "live: requires the cdc-standby profile — dev/cdc/stand.sh standby (pg-cdc-standby on :5436)"]
fn roast_pg_cdc_bounded_on_a_standby_fails_loud() {
    // A bounded (until_current) CDC run against a PostgreSQL STANDBY (in recovery)
    // must fail LOUD with an actionable message: pg_current_wal_lsn() is
    // unavailable in recovery and a logical slot cannot be created there. The
    // adapter checks pg_is_in_recovery() up front and names the escape (stream
    // continuously, or point at the primary) — not a raw "recovery is in
    // progress". Oracle: the run fails, stderr names the standby + the fix.
    //
    // Opt-in profile: the cdc-standby pair (dev/cdc/stand.sh standby, :5436) is
    // NOT part of the default `cdc` stack, so a plain `--ignored` live run does
    // not provision it. Self-gate: SKIP (loudly) when :5436 is unreachable rather
    // than fail on a Connection-refused that never reaches the recovery check
    // under test. When the profile IS up, the assertions below run for real.
    let standby_url = "postgresql://rivet:rivet@127.0.0.1:5436/rivet";
    if std::net::TcpStream::connect_timeout(
        &"127.0.0.1:5436".parse().unwrap(),
        std::time::Duration::from_millis(500),
    )
    .is_err()
    {
        eprintln!(
            "SKIP roast_pg_cdc_bounded_on_a_standby_fails_loud: cdc-standby not up on :5436 \
             (bring it up with `dev/cdc/stand.sh standby`)"
        );
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("t_standby");
    let slot = unique_name("standby_slot");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    // A standby is just a source_url override on the canonical CDC rig — no
    // bespoke YAML. run_expect_fail asserts the non-zero exit and returns stderr.
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(standby_url)
        .dest_path(out);
    let err = rig.run_expect_fail();
    assert!(
        err.contains("standby") && err.contains("recovery") && err.contains("until_current: false"),
        "the failure must name the standby and the escape (stream continuously / point at the \
         primary) — got:\n{err}"
    );
    // The message alone is EQUIVALENT-masked: a fallback guard at the
    // pg_current_wal_lsn() bound-snapshot emits the same text — but only AFTER
    // pg_create_logical_replication_slot(), which on a standby BLOCKS for minutes
    // waiting for a consistent point and then LEAKS a WAL-pinning slot. The
    // proactive pg_is_in_recovery() check is what fails fast and never touches
    // the slot. Isolate it: after a bounded run refused a standby, NO slot with
    // our name may exist there. (Disabling the proactive check regresses this to
    // a leaked slot — the RED lever for the fast-fail contract.)
    let mut sc = postgres::Client::connect(standby_url, postgres::NoTls).expect("connect standby");
    let slot_created: bool = sc
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(
        !slot_created,
        "the proactive recovery check must refuse a standby BEFORE creating a slot — a slot \
         named '{slot}' was left on the standby, meaning the run blocked on slot creation and \
         leaked a WAL-pinning slot instead of failing fast"
    );
}
