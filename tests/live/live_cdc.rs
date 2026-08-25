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

/// The CDC rig for this file — ONE builder behind both accessors below.
fn cdc_rig(tbl: &str, ckpt: &std::path::Path, out: &std::path::Path) -> Rig {
    Rig::mysql_cdc(tbl)
        .checkpoint_path(ckpt.to_path_buf())
        .dest_path(out.to_path_buf())
}

/// Config PATH in a CALLER-owned dir, via [`Rig::config_in`] — the rig-level
/// answer to the temporary-rig-drop trap this helper used to work around with
/// a hand-yaml round-trip through the caller dir. Tests that need
/// `run_args_env` (fault injection) take [`cdc_rig`] instead.
fn cdc_config(
    d: &tempfile::TempDir,
    tbl: &str,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::path::PathBuf {
    cdc_rig(tbl, ckpt, out).config_in(d.path())
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
    full_rig(tbl, out, "parquet").config_in(d.path())
}

/// The `<tbl>_batch` table-form full export against a Postgres `url` — the
/// batch side of the PG cdc-vs-batch parity oracles.
fn pg_full_rig(tbl: &str, url: &str, out: &std::path::Path) -> Rig {
    Rig::pg_batch(tbl)
        .export_named(&format!("{tbl}_batch"))
        .source_url(url)
        .dest_path(out.to_path_buf())
}

/// The `<tbl>_batch` SELECT-* full export against the CDC MySQL — the batch
/// side of every cdc-vs-batch parity oracle in this file.
fn full_rig(tbl: &str, out: &std::path::Path, format: &'static str) -> Rig {
    Rig::mysql_batch(&format!("{tbl}_batch"))
        .query(&format!("SELECT * FROM {tbl}"))
        .source_url(MYSQL_CDC_URL)
        .with_format(format)
        .dest_path(out.to_path_buf())
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
    let crashed = cdc_rig(&tbl, &ckpt, &crash_out).run_args_env(
        &[],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")],
    );
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
    Rig::pg_cdc(tbl, slot)
        .dest_path(out.to_path_buf())
        .config_in(d.path())
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let cdc_rig = Rig::mysql_cdc(&tbl)
        .with_format("csv")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = cdc_rig.config_path();
    run_rivet_ok(&cfg); // pin
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES \
         (1, 999999999999.9999, '2035-08-07 09:08:07.987654', '2024-03-15', \
          '23:59:59.999999', 'plain text'), \
         (2, NULL, NULL, NULL, NULL, NULL)"
    ))
    .unwrap();
    run_rivet_ok(&cfg);
    let batch_rig = full_rig(&tbl, &batch_out, "csv");
    run_rivet_ok(&batch_rig.config_path());

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
    let d = tempfile::tempdir().unwrap();
    // Isolated DB (see CdcDb): this test ALTERs the DATABASE-level timezone, which
    // on the shared `rivet` DB would flip the rendering for every parallel test
    // (and vice versa). Its own DB confines the change — dropped with the DB, so
    // no RESET guard is needed.
    let cdc_db = CdcDb::new("cdc_tz");
    let tbl = unique_name("cdc_tz_pg");
    let slot = unique_name("rivet_tz_slot");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "ALTER DATABASE {} SET timezone TO 'Asia/Tokyo'",
        cdc_db.name()
    ))
    .expect("set db tz");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, tstz TIMESTAMPTZ, ts TIMESTAMP)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, '2024-06-15T03:00:00Z', '2024-06-15 12:00:00')"
    ))
    .unwrap();

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());
    let batch_rig = pg_full_rig(&tbl, cdc_db.url(), &batch_out);
    run_rivet_ok(&batch_rig.config_path());
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

// Unchanged-TOAST corruption (graph-surfaced), end to end. An UPDATE that leaves
// an externally-stored TOAST column untouched renders it as the unquoted
// `unchanged-toast-datum` marker in the new tuple — the value is NOT in the WAL.
// With REPLICA IDENTITY FULL the real value rides the `old-key` pre-image; rivet
// must recover it by NAME and NEVER write the literal marker string as data
// (the same silent-corruption class as the uuid→null loss caught live on GCS).
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_unchanged_toast_recovers_from_replica_identity_full() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_toast_full");
    let slot = unique_name("rivet_toast_full_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    // EXTERNAL storage forces out-of-line TOAST (no compression); FULL puts the
    // pre-image value in `old-key`, so the unchanged column is recoverable.
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, small TEXT, big TEXT); \
         ALTER TABLE {tbl} ALTER COLUMN big SET STORAGE EXTERNAL; \
         ALTER TABLE {tbl} REPLICA IDENTITY FULL;"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    // RAII, not trailing drops: a panic in the assertions below would otherwise
    // leak the slot on the shared :5434 CDC db and pin WAL against
    // max_replication_slots=32, cascading into unrelated PG-CDC failures that
    // mask the real regression (r6 bughunt — the class the file's own SlotGuard
    // comment documents; the fix had guarded one test only).
    let _slot = Slot(slot.clone());
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    // Incompressible >2KB value → genuine external TOAST; then touch only `small`
    // so `big` decodes as the unchanged-toast marker in the new tuple.
    c.batch_execute(&format!(
        "INSERT INTO {tbl} (id, small, big) VALUES \
           (1, 'a', (SELECT string_agg(md5(g::text || random()::text), '') \
                     FROM generate_series(1, 200) g)); \
         UPDATE {tbl} SET small = 'b' WHERE id = 1;"
    ))
    .unwrap();
    let real: String = c
        .query_one(&format!("SELECT big FROM {tbl} WHERE id = 1"), &[])
        .unwrap()
        .get(0);

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let batches = Rig::pg_cdc(&tbl, &slot).dest_path(out).run_and_read();

    use arrow::array::{Array, StringArray};
    let mut bigs = Vec::new();
    for b in &batches {
        let idx = b.schema().index_of("big").expect("big column present");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("big is a string column");
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                bigs.push(arr.value(i).to_string());
            }
        }
    }
    assert!(
        !bigs.iter().any(|v| v == "unchanged-toast-datum"),
        "the literal TOAST marker must NEVER be written as data; got {bigs:?}"
    );
    assert!(
        bigs.iter().filter(|v| **v == real).count() >= 2,
        "both the INSERT and the recovered UPDATE row must carry the real value"
    );
}

// The DEFAULT replica-identity case: the pre-image carries only the key, so the
// unchanged externally-stored value is nowhere in the WAL. rivet must fail LOUD
// (never fabricate the marker as data) and name the upstream fix, not silently
// corrupt or null the column.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_unchanged_toast_without_full_identity_fails_loud() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_toast_default");
    let slot = unique_name("rivet_toast_default_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    // EXTERNAL storage but DEFAULT replica identity (PK only) — no pre-image value.
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, small TEXT, big TEXT); \
         ALTER TABLE {tbl} ALTER COLUMN big SET STORAGE EXTERNAL;"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    // RAII (r6 bughunt): the run_expect_fail below panics if rivet DOESN'T fail
    // loud — the trailing drop would then leak the slot on :5434.
    let _slot = Slot(slot.clone());
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} (id, small, big) VALUES \
           (1, 'a', (SELECT string_agg(md5(g::text || random()::text), '') \
                     FROM generate_series(1, 200) g)); \
         UPDATE {tbl} SET small = 'b' WHERE id = 1;"
    ))
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let err = Rig::pg_cdc(&tbl, &slot).dest_path(out).run_expect_fail();
    assert!(
        err.contains("unchanged-TOAST"),
        "must fail loud on an unrecoverable TOAST datum; got: {err}"
    );
    assert!(
        err.contains("REPLICA IDENTITY FULL"),
        "must name the upstream fix; got: {err}"
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
    let d = tempfile::tempdir().unwrap();
    // Isolated DB (see CdcDb): ALTERing the DATABASE-level datestyle/bytea_output
    // on the shared `rivet` DB would corrupt the rendering for every parallel test.
    // Its own DB confines it — dropped with the DB, no RESET guard needed.
    let cdc_db = CdcDb::new("cdc_fmt");
    let tbl = unique_name("cdc_fmt_pg");
    let slot = unique_name("rivet_fmt_slot");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "ALTER DATABASE {db} SET datestyle TO 'German, DMY'; \
         ALTER DATABASE {db} SET bytea_output TO 'escape'",
        db = cdc_db.name()
    ))
    .expect("set db formats");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, d DATE, ts TIMESTAMP, blob BYTEA)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, '2024-03-05', '2024-03-05 12:00:00', '\\xdeadbeef')"
    ))
    .unwrap();

    let out = d.path().join("out");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());
    let batch_rig = pg_full_rig(&tbl, cdc_db.url(), &batch_out);
    run_rivet_ok(&batch_rig.config_path());
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let batch_rig = pg_full_rig(&tbl, POSTGRES_CDC_URL, &batch_out);
    run_rivet_ok(&batch_rig.config_path());
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let batch_rig = pg_full_rig(&tbl, POSTGRES_CDC_URL, &batch_out);
    run_rivet_ok(&batch_rig.config_path());
    assert_cdc_matches_batch(&cdc_out, &batch_out);

    // Leg 2: 'NaN'::NUMERIC — the CDC run must FAIL, naming the payload.
    c.execute(
        &format!("INSERT INTO {tbl} VALUES (5, 1.0, 1.0, 'NaN')"),
        &[],
    )
    .unwrap();
    let out = run_rivet_env(
        &[
            "run",
            "--config",
            pg_cdc_config(&d, &tbl, &slot, &cdc_out).to_str().unwrap(),
        ],
        &[],
    );
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
    let rig = Rig::mysql_cdc(&ta)
        .tables(&[&ta, &tb])
        .export_named("app_cdc")
        .export_line(&format!(
            "columns: {{ v: text, \"{tb}.v\": \"decimal(20,4)\" }}"
        ))
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

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
    let rig = Rig::mysql_cdc(&ta)
        .tables(&[&ta, &tb, &tc])
        .export_named("app_cdc")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

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
    let rig = Rig::mysql_cdc(&tbl)
        .cdc_line("initial: snapshot")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

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
        duckdb_dir_parquet_id_set(&snap)
            .into_iter()
            .collect::<Vec<i64>>(),
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

// Load parity: the `initial: snapshot` leg and the CDC stream BOTH feed one
// `<table>__changes` log (snapshot rows with __op/__pos/__seq NULL), and the
// current-state view is `SELECT * EXCEPT(__op,__pos,__seq,__rn)`. batch-only
// meta_columns injected on the snapshot leg ONLY would give it columns the CDC
// parquet lacks — appending both into one `__changes` table then mismatches
// (the exact "read CDC, backfill history via batch, load breaks because columns
// don't match" failure). The synth snapshot must therefore drop meta_columns.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_initial_snapshot_leg_drops_batch_meta_columns_for_load_parity() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_init_meta");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    // The CDC export REQUESTS batch meta columns — they must be dropped from the
    // snapshot leg so its parquet matches the CDC stream's columns.
    let rig = Rig::mysql_cdc(&tbl)
        .cdc_line("initial: snapshot")
        .export_line("meta_columns: { exported_at: true, row_hash: true }")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();
    run_rivet_ok(&cfg);

    let snap = out.join("snapshot");
    let snap_cols: Vec<String> = parquet_fields(&snap).into_iter().map(|(n, _)| n).collect();
    // The meta columns are `_rivet_exported_at` / `_rivet_row_hash` (SINGLE
    // underscore, src/enrich.rs) — a `__rivet` (double) check silently never
    // matched, so this assertion was VACUOUS (green even if the leg leaked them).
    // Caught while RED-proving the sibling cdc→warehouse DuckDB-load test.
    //
    // The two meta columns are now ASYMMETRIC, and load parity is exactly why
    // (§5h / cdc_job.rs::synth_snapshot_export): `exported_at` is a per-run stamp
    // only the batch leg can produce, so keeping it would make the snapshot
    // parquet's columns differ from the CDC leg's and break the shared
    // `__changes` append — it is cleared. `row_hash` is produced by BOTH legs, so
    // it is INHERITED on purpose; dropping it here is what would break parity,
    // leaving half the appended table NULL in that column.
    assert!(
        !snap_cols.iter().any(|c| c == "_rivet_exported_at"),
        "snapshot leg must NOT carry `_rivet_exported_at` — only the batch leg can \
         produce it, so it would break column parity with the CDC leg's __changes \
         append; got {snap_cols:?}"
    );
    assert!(
        snap_cols.iter().any(|c| c == "_rivet_row_hash"),
        "snapshot leg MUST carry `_rivet_row_hash` — both legs produce it, and a \
         column only one leg wrote leaves half the __changes table NULL; got \
         {snap_cols:?}"
    );
    assert!(
        snap_cols.iter().any(|c| c == "id") && snap_cols.iter().any(|c| c == "v"),
        "snapshot leg still carries the source columns; got {snap_cols:?}"
    );

    // Stream a post-snapshot change and confirm the CDC parquet's DATA columns
    // are exactly the snapshot's (the loader adds __op/__pos/__seq on top) — so
    // the two parquets append into one `__changes` log without a mismatch.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30)"))
        .unwrap();
    run_rivet_ok(&cfg);
    let cdc_cols: Vec<String> = parquet_fields(&out).into_iter().map(|(n, _)| n).collect();
    let cdc_data: Vec<&String> = cdc_cols.iter().filter(|c| !c.starts_with("__")).collect();
    let snap_data: Vec<&String> = snap_cols.iter().filter(|c| !c.starts_with("__")).collect();
    assert_eq!(
        cdc_data, snap_data,
        "the CDC stream's data columns must equal the snapshot's — else the shared \
         __changes append mismatches"
    );
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc_line("initial: snapshot")
        .dest_path(out.clone());
    let cfg = rig.config_path();

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
    let res = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[]);
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
        PgTable::adopt_on(POSTGRES_CDC_URL, orders.clone()),
        PgTable::adopt_on(POSTGRES_CDC_URL, audit.clone()),
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let rig = Rig::mysql_cdc(&tbl)
        .cdc_line("initial: snapshot")
        .export_line("skip_empty: true")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc_line("initial: snapshot")
        .dest_path(out.clone());
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    let _slot = Slot(slot.clone());
    assert_eq!(manifest_rows(&out.join("snapshot")), 2);
    assert_eq!(
        duckdb_dir_parquet_id_set(&out.join("snapshot"))
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
    let cdc_rig = Rig::mysql_cdc(&tbl)
        .export_line("columns: { bigu: \"decimal(20,0)\" }")
        .checkpoint_path(ckpt.clone())
        .dest_path(cdc_out.clone());
    let batch_rig = Rig::mysql_batch(&tbl)
        .export_named(&format!("{tbl}_batch"))
        .source_url(MYSQL_CDC_URL)
        .export_line("columns: { bigu: \"decimal(20,0)\" }")
        .dest_path(batch_out.clone());
    run_rivet_ok(&cdc_rig.config_path());
    run_rivet_ok(&batch_rig.config_path());

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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

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
    let batch_rig = pg_full_rig(&tbl, POSTGRES_CDC_URL, &batch_out);
    run_rivet_ok(&batch_rig.config_path());
    assert_cdc_matches_batch(&cdc_out, &batch_out);

    // HONEST LIMIT, first, because it changes how to read what follows: I could
    // not construct a mutant where this assertion bites and the run still
    // SUCCEEDS. Three were tried — delete the text-uuid second chance in the
    // shared `fixed_binary_bytes` (no effect on PG: that path is MySQL's
    // `columns: {uid: uuid}` override), null every `Float` cell at the
    // normalisation step, and null every text cell in the BUILDER only, which is
    // the original bug's exact shape. The last two both FAIL LOUDLY in the run
    // itself: the two-ended value checksum compares the typed fold against the
    // built batch and bails. That is a real result about the product — the
    // asymmetric version of this class is now caught before any oracle sees it.
    //
    // The version that would still be silent is a SHARED-path change, where both
    // folds agree on the wrong value; that is precisely how the uuid bug hid
    // ("side A skipped the 36-byte cell (contributing 0) and side B hashed a null
    // (also 0), so the folds agreed and the mismatch bail never fired"), and I
    // could not build one for a type this PG fixture uses. So this earns its
    // place as the INDEPENDENT witness the shared-fold case needs — not on a
    // demonstrated kill.
    //
    // SECOND oracle, and an INDEPENDENT one. The comparison above is
    // differential — CDC against batch, both decoded by rivet — so a fault the
    // two share passes its own inspection. This asks PostgreSQL instead.
    //
    // The class it exists for is not hypothetical: the `FixedSizeBinary(16)`
    // builder nulled anything not exactly 16 bytes, and `test_decoding` renders
    // uuids as 36-char TEXT, so 100% of a uuid column became NULL on a real
    // bucket while every count and sum check passed. A per-column NULL profile
    // is what sees that; a row count never can. The batch path has had this
    // oracle on three engines (type_roundtrip/duckdb_load.rs) — CDC had it on
    // none (audit 2026-08-17).
    //
    // Only the INSERT images: a CDC part holds one row per change, so the
    // source's 3 rows are the 3 inserts, and reading the whole part would
    // compare different populations.
    let cols: Vec<String> = c
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = $1 ORDER BY ordinal_position",
            &[&tbl],
        )
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert!(
        cols.len() >= 20,
        "the fixture must present a rich column set to profile; got {}",
        cols.len()
    );
    let mut columns_with_nulls = 0;
    for col in &cols {
        let src_nulls: i64 = c
            .query_one(
                &format!("SELECT count(*) - count(\"{col}\") FROM {tbl}"),
                &[],
            )
            .unwrap()
            .get(0);
        columns_with_nulls += i32::from(src_nulls > 0);
        let dst_nulls = duckdb_dir_scalar(
            &cdc_out,
            &format!("count(*) - count(\"{col}\")"),
            Some("__op = 'insert'"),
        );
        assert_eq!(
            src_nulls, dst_nulls,
            "column '{col}': NULL-count parity against PostgreSQL itself — source \
             {src_nulls}, captured {dst_nulls}. A per-cell decode that degrades to \
             NULL moves this and nothing else; the uuid column that went 100% NULL \
             through test_decoding passed every count and sum check."
        );
    }
    // The fixture must actually LOAD this axis. A column compared at 0-vs-0 is a
    // green that proves nothing, and that is what this check would decay into if
    // someone later simplified the INSERTs. Row 2 is all-NULL and row 3 sets only
    // (id, tags, nums), so nearly every column must show source NULLs.
    assert!(
        columns_with_nulls >= 15,
        "only {columns_with_nulls} of {} columns carry a source NULL — the fixture \
         stopped exercising the null axis, so the parity above is comparing zeros",
        cols.len()
    );
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
    let (_g1, _g2) = (
        PgTable::adopt_on(POSTGRES_CDC_URL, t1.clone()),
        PgTable::adopt_on(POSTGRES_CDC_URL, t2.clone()),
    );

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&t1, &slot)
        .tables(&[&t1, &t2])
        .export_named("app_cdc")
        .dest_path(out.clone());
    let cfg = rig.config_path();

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

// #252, half 1: `check --target` on a multiplex `tables:` export must emit one
// RESOLVER document per captured table, not one per export.
//
// A multiplex export has no `table:` and no `query:`, so resolving "the export's
// query" bailed and the type report was dropped with a `warn` — under `--json`
// the export's line was the TUNING diagnostic (`export_name`, no `columns`)
// instead. That is what `rivet load` consumes for the native schema, so a whole
// schema captured through one stream could not be loaded at all.
//
// Driven through the real binary (the resolver runs in-process for `load` too),
// and the per-table `columns:` narrowing is asserted alongside: a multiplex unit
// must be typed with THIS table's overrides — bare keys everywhere, a
// `"<table>.<column>"` key only on its own table — the same precedence the
// capture applies, or `check` and `run` describe different Parquet.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_multi_table_check_target_emits_one_resolver_document_per_table() {
    use postgres::NoTls;
    let t1 = unique_name("rivet_cdc_tr_a");
    let t2 = unique_name("rivet_cdc_tr_b");
    let slot = unique_name("rivet_tr_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    for t in [&t1, &t2] {
        c.batch_execute(&format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"
        ))
        .unwrap();
    }
    let (_g1, _g2) = (
        PgTable::adopt_on(POSTGRES_CDC_URL, t1.clone()),
        PgTable::adopt_on(POSTGRES_CDC_URL, t2.clone()),
    );

    // No `run` here: the report is resolved from the source catalog, so this
    // needs no slot, no capture and no destination bytes.
    let rig = Rig::pg_cdc(&t1, &slot)
        .tables(&[&t1, &t2])
        .export_named("app_cdc")
        .export_line(&format!("columns: {{ \"{t2}.v\": text }}"));
    let out = rig.cli(&["check", "--target", "bigquery", "--json"]);
    assert!(
        out.status.success(),
        "check --target must succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);

    // NDJSON: the type-report lines are the ones carrying `columns`. A
    // diagnostic-only line (the pre-fix shape) has none, so this filter is
    // exactly the assertion — it counts documents the loader could USE.
    let docs: Vec<serde_json::Value> = stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("columns").is_some())
        .collect();
    assert_eq!(
        docs.len(),
        2,
        "one resolver document per captured table — got {} from:\n{stdout}",
        docs.len()
    );
    let by_table = |t: &str| {
        docs.iter()
            .find(|d| d["table"] == serde_json::json!(t))
            .unwrap_or_else(|| panic!("no resolver document for table {t} in:\n{stdout}"))
    };
    for t in [&t1, &t2] {
        let d = by_table(t);
        assert_eq!(
            d["export"],
            serde_json::json!("app_cdc"),
            "each document still names the EXPORT the operator wrote"
        );
        let cols: Vec<&str> = d["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["column"].as_str().unwrap())
            .collect();
        assert_eq!(cols, vec!["id", "v"], "table {t} columns");
    }
    // The qualified override lands on t2 ONLY: BigQuery STRING there, INT64 on
    // the table it does not name. A unit typed with the export's whole override
    // map would make both STRING.
    let target_of = |t: &str| {
        by_table(t)["columns"].as_array().unwrap()[1]["target_type"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(target_of(&t1), "INT64", "bare table keeps its source type");
    assert_eq!(
        target_of(&t2),
        "STRING",
        "the `<table>.<column>` override applies to its own table only — the \
         same precedence the capture uses"
    );
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
    let rig = Rig::mysql_cdc(&ta)
        .tables(&[&ta, &tb])
        .export_named("app_cdc")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let cfg = rig.config_path();

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
    let rig = Rig::mysql_cdc(&ta)
        .tables(&[&ta, &tb])
        .export_named("app_cdc")
        .checkpoint_path(ckpt.clone())
        .dest_gcs(bucket, &prefix, FAKE_GCS_ENDPOINT);
    let cfg = rig.config_path();
    // The rig's cloud dest renders prefix "<prefix>/<export>/" — the per-TABLE
    // subprefixes under test now hang off that root.
    let root = format!("{prefix}/app_cdc");

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
                .any(|k| *k == format!("{root}/{t}/manifest.json")),
            "per-table manifest key missing for {t}; keys: {keys:?}"
        );
        assert!(
            keys.iter().any(|k| *k == format!("{root}/{t}/_SUCCESS")),
            "per-table _SUCCESS key missing for {t}; keys: {keys:?}"
        );
        assert!(
            keys.iter()
                .any(|k| k.starts_with(&format!("{root}/{t}/cdc-")) && k.ends_with(".parquet")),
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
    let out = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[]);
    assert!(
        !out.status.success(),
        "resuming from a purged/missing binlog must FAIL, not silently re-anchor"
    );
    assert!(
        !out_dir.join("_SUCCESS").exists(),
        "no _SUCCESS may be written for the failed run"
    );
}

// #99, MySQL flavour: a corrupt/truncated checkpoint must FAIL the run loudly,
// never be swallowed (`.ok().flatten()`) into "no checkpoint". On a client-anchor
// engine that swallow re-anchors at `SHOW MASTER STATUS` (current) and silently
// skips every change since the last good position. The guard fires at the shared
// cdc_job resume-plan site (`Position::load`) every engine hits before anchoring.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_corrupt_checkpoint_fails_loud_not_silently_absent() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_corrupt_bin");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    // Run 1 pins a valid checkpoint at the open position.
    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&cdc_config(&d, &tbl, &ckpt, &out1));
    assert!(ckpt.exists(), "run 1 pins a checkpoint");

    // The checkpoint is corrupted (a truncated write / disk fault); a change lands
    // that a silent re-anchor at 'current' would skip.
    std::fs::write(&ckpt, b"{ not valid json at all").unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 100)"))
        .unwrap();

    // Run 2 must FAIL loudly — never exit 0 having silently re-anchored past id=1.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let res = run_rivet_env(
        &[
            "run",
            "--config",
            cdc_config(&d, &tbl, &ckpt, &out2).to_str().unwrap(),
        ],
        &[],
    );
    assert!(
        !res.status.success(),
        "a corrupt checkpoint must fail the run, not silently re-anchor and skip changes"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        stderr.contains("corrupt or truncated"),
        "the failure must name the corrupt checkpoint, got:\n{stderr}"
    );
    assert!(
        !out2.join("_SUCCESS").exists(),
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    // Run 1 (with a checkpoint configured): creates the slot, captures one
    // change, persists the checkpoint.
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let rig_for = |out: &std::path::Path| {
        Rig::pg_cdc(&tbl, &slot)
            .checkpoint_path(ckpt.clone())
            .dest_path(out.to_path_buf())
    };
    let rig1 = rig_for(&out1);
    run_rivet_ok(&rig1.config_path());
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let rig2 = rig_for(&out2);
    run_rivet_ok(&rig2.config_path());
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
    let rig3 = rig_for(&out3);
    let out = rig3.run_args(&[]);
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

// #99, PostgreSQL flavour — the engine where the bug actually lived. A prior
// run's checkpoint is corrupt (truncated / not JSON); swallowing it into "no
// checkpoint" let PG treat the run as a fresh first anchor, recreate a dropped
// slot at 'current', and permanently skip every change since the loss (the
// anti-gap guard never fired). The corrupt checkpoint must fail the run loudly.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_corrupt_checkpoint_fails_loud_not_silently_absent() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgcorrupt");
    let slot = unique_name("rivet_corrupt_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let rig_for = |out: &std::path::Path| {
        Rig::pg_cdc(&tbl, &slot)
            .checkpoint_path(ckpt.clone())
            .dest_path(out.to_path_buf())
    };

    // Run 1 (idle) creates the slot — PG anchors server-side, so an idle run
    // does not yet write the checkpoint FILE (unlike the client-anchor engines).
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    let rig1 = rig_for(&out1);
    run_rivet_ok(&rig1.config_path());
    let _slot = Slot(slot.clone());

    // Run 2 captures a change and pins a valid checkpoint file.
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10)"), &[])
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let rig2 = rig_for(&out2);
    run_rivet_ok(&rig2.config_path());
    assert_eq!(manifest_rows(&out2), 1, "run 2 captures the change");
    assert!(ckpt.exists(), "run 2 pins a checkpoint");

    // The checkpoint is corrupted; a change lands that reading it as absent +
    // re-anchoring would skip.
    std::fs::write(&ckpt, b"{ truncated").unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (2,20)"), &[])
        .unwrap();

    // Run 3 must FAIL loudly — never read the corrupt checkpoint as absent.
    let out3 = d.path().join("out3");
    std::fs::create_dir_all(&out3).unwrap();
    let rig3 = rig_for(&out3);
    let res = rig3.run_args(&[]);
    assert!(
        !res.status.success(),
        "a corrupt checkpoint must fail the run, not be read as absent and re-anchor"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        stderr.contains("corrupt or truncated"),
        "the failure must name the corrupt checkpoint, got:\n{stderr}"
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let out = run_rivet(&["doctor", "--config", cfg.to_str().unwrap(), "--json"]);
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());

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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let crashed = run_rivet_env(
        &[
            "run",
            "--config",
            pg_cdc_config(&d, &tbl, &slot, &crash_out).to_str().unwrap(),
        ],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")],
    );
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
    let _ta = PgTable::adopt_on(POSTGRES_CDC_URL, a.clone());
    let _tb = PgTable::adopt_on(POSTGRES_CDC_URL, b.clone());
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
    let crashed = rig.run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "cdc_after_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2 resumes from wherever the crash left the slot.
    let rig2 = Rig::pg_cdc(&a, &slot).dest_path(out.clone());
    run_rivet_ok(&rig2.config_path());

    let ids = duckdb_dir_parquet_i64(&out, "id");
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let crashed = rig.run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "cdc_after_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2 resumes from the slot (whatever position the crash left it at).
    let rig2 = Rig::pg_cdc(&tbl, &slot).dest_path(out.clone());
    run_rivet_ok(&rig2.config_path());

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
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
    let crashed = rig.run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "cdc_after_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2 resumes from the checkpoint the crash left behind.
    run_rivet_ok(&rig.config_path());

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
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
    Rig::pg_batch(&format!("{tbl}_batch"))
        .query(&format!("SELECT * FROM {tbl}"))
        .source_url(POSTGRES_CDC_URL)
        .dest_path(out.to_path_buf())
        .config_in(d.path())
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
        !duckdb_dir_parquet_has_column(&out1, "w"),
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
        duckdb_dir_parquet_has_column(&out2, "w"),
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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

    // A writer committing continuously while the bounded run drains. RAII
    // (BgWriter): if run_rivet_bounded panics on a non-zero exit, the writer is
    // stopped+joined on unwind — never detached to hammer a table its guard is
    // dropping (r7 bughunt). Declared after _tbl/_slot so it drops first.
    let tbl_bg = tbl.clone();
    let _bg = BgWriter::spawn(move |stop| {
        let mut w = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("bg connect");
        let mut i = 10_000i64;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
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

    assert!(
        elapsed.is_some(),
        "until_current must terminate under sustained writes (killed at the 30s ceiling)"
    );
    // Termination must NOT come from dropping the backlog.
    let ids: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
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
    let tbl_bg = tbl.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
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
    bg.stop();

    assert!(
        elapsed.is_some(),
        "until_current must terminate under sustained writes (killed at the 30s ceiling)"
    );
    let ids: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let tbl_bg = tbl.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
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
    bg.stop();
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

    let got: std::collections::BTreeSet<i64> = duckdb_dir_parquet_i64(&rig.out_dir(), "id")
        .into_iter()
        .collect();
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

    let tbl_bg = tbl.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
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
    bg.stop();
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

    let got: std::collections::BTreeSet<i64> = duckdb_dir_parquet_i64(&rig.out_dir(), "id")
        .into_iter()
        .collect();
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
/// A PARTITIONED parent must be refused at OPEN — before the slot is acked past
/// changes no event could ever route.
///
/// The second half of #279. That fix taught the reader to UNQUOTE the wire
/// identity; this one compares that identity to what the CONFIG asked for, the
/// cross-check the SQL Server arm has carried since the capture-instance find
/// (`mssql/cdc.rs`). The reachable shape is not a mixed-case name — the schema
/// probe (`SELECT * FROM {table}`) already fails loudly on those — it is a
/// partitioned table, where the probe succeeds PERFECTLY (the parent has a full,
/// correct column list) and `test_decoding` then names the PARTITION every row
/// physically landed in.
///
/// Measured before the guard (2026-08-24, pg14 stand — this test's RED):
/// 2 committed rows → `status: success`, `rows: 0`, `files: 0`, slot
/// `0/AE5F8BB8` → `0/AE6010D8`, and `pg_logical_slot_peek_changes` EMPTY
/// afterwards. Re-running with the config corrected to the partition recovered
/// NOTHING while the source still held both rows. Worse than the SQL Server
/// case this is modelled on, where the change table's own retention made the
/// same routing bug a delay: here PostgreSQL frees the WAL and it is gone.
///
/// Isolated in its OWN database (see `CdcDb`): the guard reads `pg_class`, and
/// the assertion is about a slot position a parallel test's WAL would perturb.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_refuses_a_partitioned_parent_before_acking_the_slot() {
    let cdc_db = CdcDb::new("cdc_part");
    let slot = unique_name("rivet_part_slot");
    let parent = unique_name("rivet_cdc_par").to_lowercase();
    let part = format!("{parent}_2026_01");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {parent} (id BIGINT NOT NULL, ts DATE NOT NULL, v INT) \
         PARTITION BY RANGE (ts); \
         CREATE TABLE {part} PARTITION OF {parent} \
         FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(
        &format!("INSERT INTO {parent} VALUES (1,'2026-01-05',10),(2,'2026-01-06',20)"),
        &[],
    )
    .unwrap();
    // The SOURCE oracle, asked of PostgreSQL itself — never rivet's counters.
    let source_rows: i64 = c
        .query_one(&format!("SELECT count(*) FROM {parent}"), &[])
        .unwrap()
        .get(0);
    assert_eq!(source_rows, 2, "fixture seeded 2 rows through the parent");
    let before: String = c
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);

    // The config a user writes: the LOGICAL table. Its schema probe succeeds —
    // which is exactly why this was silent.
    let wrong = Rig::pg_cdc(&format!("public.{parent}"), &slot).source_url(cdc_db.url());
    let out = run_rivet(&["run", "--config", wrong.config_path().to_str().unwrap()]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "capturing a partitioned parent must fail at open, not ship a 0-row \
         success — output:\n{said}"
    );
    assert!(
        said.contains(&part),
        "the refusal must name the PARTITION `{part}` — listing the partitions is \
         the remediation, and a user who could guess that would not have hit this. \
         Output:\n{said}"
    );

    // The slot must NOT have moved. Failing at open only buys anything if it
    // happens BEFORE the ack: this assert is the whole difference between a
    // config error and two rows nobody can get back.
    let moved: bool = c
        .query_one(
            &format!(
                "SELECT confirmed_flush_lsn <> '{before}'::pg_lsn \
                 FROM pg_replication_slots WHERE slot_name = $1"
            ),
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(
        !moved,
        "the refused run acked the slot anyway — the 2 changes are unreachable \
         now and the guard bought nothing"
    );

    // Defer-not-drop: with the partition named, the same slot still holds every
    // change. Oracle: the parts the MANIFEST declares, re-read — never the
    // manifest's own `row_count`, and compared to the source count above.
    let right = Rig::pg_cdc(&format!("public.{part}"), &slot).source_url(cdc_db.url());
    run_rivet_ok(&right.config_path());
    let captured: usize = right
        .read_declared_parts()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        captured, source_rows as usize,
        "the corrected run must recover all {source_rows} changes from the \
         un-acked slot — that is what makes the refusal a delay, not a loss"
    );

    // The refusal offers TWO ways out and a message may only promise what
    // something checks. The partition-by-name route is proven above; this is the
    // other one — `mode: full` reads THROUGH the parent, because a batch SELECT
    // resolves partitions the way any query does and never sees a wire identity
    // at all. Untested, "snapshot the parent with mode: full" would be a remedy
    // nobody had run — the class CLAUDE.md records as a hint that cannot recover
    // from the state it is offered in.
    let snapshot = Rig::pg_batch(&format!("public.{parent}"))
        .source_url(cdc_db.url())
        .mode("full");
    let snapshot_rows: usize = snapshot.run_and_read().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        snapshot_rows, source_rows as usize,
        "`mode: full` on the partitioned parent must read every row — it is the \
         second remediation the refusal names, and a message may only promise \
         what a test has run"
    );
}

/// The manifest-scoped read-back must disagree with a glob — asserted through the
/// READER the CDC tests actually use, not through a sibling helper.
///
/// The first version of this test staged parquet and asserted on
/// `duckdb_declared_assert_complete`. It never called `read_cdc_changes`,
/// `cdc_id_ops` or `declared_parquet_parts`, so a mutant that undid the whole
/// commit — `declared_parquet_parts(dir)` back to `files_with_extension(dir,
/// "parquet")` — left it GREEN. Measured, by an adversarial pass, on exactly that
/// mutant. It read like proof of the change and proved a different function.
///
/// This drives a real CDC run, then removes ONE part from the manifest while
/// leaving the FILE on disk — the shape a crash leaves — and asserts the reader
/// every crash/resume cell depends on reads short of the directory.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn the_cdc_reader_reads_the_manifest_declared_parts_not_the_directory() {
    let cdc_db = CdcDb::new("cdc_declared");
    let tbl = unique_name("rivet_cdc_decl").to_lowercase();
    let slot = unique_name("rivet_decl_slot").to_lowercase();
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
    // Two SEPARATE transactions at rollover 1 ⇒ two parts. One part could not
    // express the difference this test exists to measure.
    for i in 1..=2i64 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({i},{i})"), &[])
            .unwrap();
    }

    let rig = Rig::pg_cdc(&format!("public.{tbl}"), &slot)
        .source_url(cdc_db.url())
        .cdc("rollover: 1");
    run_rivet_ok(&rig.config_path());
    let out = rig.out_dir();

    let before = cdc_id_ops(&out);
    assert_eq!(
        before.len(),
        2,
        "the fixture must produce TWO parts' worth of rows, or the manifest edit \
         below cannot express under-declaration: {before:?}"
    );
    let files_on_disk = std::fs::read_dir(&out)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .count();
    assert_eq!(files_on_disk, 2, "two committed parts on disk");

    // Under-declare: drop the LAST part from every manifest, leave the file. This
    // is what a crash between the part write and the manifest write leaves behind.
    for entry in std::fs::read_dir(&out).unwrap().flatten() {
        let path = entry.path();
        let is_manifest = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("manifest") && n.ends_with(".json"));
        if !is_manifest {
            continue;
        }
        let mut doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        if let Some(parts) = doc.get_mut("parts").and_then(|p| p.as_array_mut())
            && parts.len() > 1
        {
            parts.pop();
        }
        std::fs::write(&path, doc.to_string()).unwrap();
    }

    let after = cdc_id_ops(&out);
    assert!(
        after.len() < before.len(),
        "the reader must follow the MANIFEST: both parquet files are still on disk, \
         so a directory scan reads {} rows either way and reports delivery no \
         consumer would ever see. Got {after:?} after under-declaring, {before:?} \
         before",
        before.len()
    );
    assert_eq!(after.len(), 1, "exactly the still-declared part: {after:?}");
}

/// A configured MySQL VIEW must be refused at open — the binlog can never name it.
///
/// Found by an ADVERSARIAL re-check of a matrix cell this session had written as
/// `na` ("no MySQL relation emits under a name other than the configured one").
/// That cell generalized a correct, measured result — a RANGE-partitioned table
/// DOES capture through a config naming the parent — into a false universal. An
/// updatable view reproduces the PostgreSQL partitioned-parent shape exactly: the
/// binlog `Table_map` names the BASE table, routing is byte-exact, and every
/// change is dropped.
///
/// MEASURED before the guard (2026-08-24): 3 rows committed through a view gave
/// `rows: 0, files: 0, status: success, exit 0`, with the checkpoint advanced
/// 54945184 -> 54945514 past the commit, and fixing the config recovered NOTHING
/// from that checkpoint.
///
/// Unlike PostgreSQL this is RECOVERABLE — MySQL binlog retention does not depend
/// on the reader, so the events are still in the log and deleting the checkpoint
/// re-reads them. The message says so, and this test asserts it does, because
/// telling an operator to re-snapshot when a checkpoint reset would do is its own
/// kind of wrong answer.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_cdc_refuses_a_view_whose_binlog_identity_is_the_base_table() {
    let d = tempfile::tempdir().unwrap();
    let base = unique_name("cdc_vbase");
    let view = format!("{base}_v");
    let mut c = conn();
    c.query_drop(format!("DROP VIEW IF EXISTS {view}")).unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {base}"))
        .unwrap();
    c.query_drop(format!("CREATE TABLE {base} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    c.query_drop(format!("CREATE VIEW {view} AS SELECT * FROM {base}"))
        .unwrap();
    let _g = Table(base.clone());
    struct ViewGuard(String);
    impl Drop for ViewGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(MYSQL_CDC_URL).unwrap()) {
                use mysql::prelude::Queryable as _;
                let _ = c.query_drop(format!("DROP VIEW IF EXISTS {}", self.0));
            }
        }
    }
    let _vg = ViewGuard(view.clone());

    let msg = Rig::mysql_cdc(&view)
        .checkpoint_path(d.path().join("v.ckpt"))
        .dest_path(d.path().join("out"))
        .run_expect_fail();
    assert!(
        msg.contains("VIEW") && msg.contains("base table"),
        "the refusal must name what the relation IS and what to capture instead — \
         shipping a 0-row success past an advanced checkpoint is the alternative. \
         Got: {msg}"
    );
    assert!(
        msg.contains("NOT lost"),
        "MySQL binlog retention is reader-independent, so this is recoverable by \
         deleting the checkpoint; an operator told to re-snapshot would do work they \
         do not need. Got: {msg}"
    );
    // The refusal must NOT arrive wearing the binlog-grants hint. It is a config
    // problem, and `create_change_stream` wraps the open call in MYSQL_CDC_HINT —
    // the same trap the checkpoint validation was hoisted out of, which is why the
    // routing precheck runs before that wrap rather than inside open.
    assert!(
        !msg.contains("REPLICATION SLAVE") && !msg.contains("binlog_format"),
        "a routing/config refusal must not be prefixed with a permissions hint — an \
         operator would go read the grants docs for a problem that is not there. \
         Got: {msg}"
    );

    // Not too WIDE: the base table itself must still capture normally. Oracle is
    // the manifest-declared parts re-read, compared to what the source holds.
    let base_rig = || {
        Rig::mysql_cdc(&base)
            .checkpoint_path(d.path().join("b.ckpt"))
            .dest_path(d.path().join("out_base"))
    };
    base_rig().run_ok(); // anchor
    c.query_drop(format!("INSERT INTO {base} VALUES (1,10),(2,20),(3,30)"))
        .unwrap();
    let rows: usize = base_rig().run_and_read().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 3,
        "the guard must refuse only what the binlog cannot name — a base table has \
         to keep working, or the fix is worse than the defect"
    );
}

/// The three routing hazards an adversarial pass found in the FIRST version of
/// this guard, which read only `relkind`.
///
/// All three end the same way — a run that reports success while capturing
/// nothing, or worse — and all three are invisible to a relkind check:
///
/// 1. **A folded twin.** The config string is read by two resolvers with different
///    case rules: the schema probe interpolates it into `SELECT * FROM {table}` and
///    lets PostgreSQL FOLD it; the sink routes BYTE-EXACT. With both `"MixedCase"`
///    and `mixedcase` present, MEASURED: exit 0 throughout, writes to `mixedcase`
///    captured `rows: 0` with no warning, and writes to `"MixedCase"` landed under
///    the WRONG table's schema — columns `id, other_col, extra`, the real `v`
///    values absent entirely. Silent column loss on top of silent event loss.
/// 2. **A 3-part name.** `to_regclass` accepts `db.schema.table` when `db` is the
///    current database, so the probe resolves while `table_matches` splits on the
///    FIRST dot and compares `db` against the schema — never matching.
/// 3. **UNLOGGED.** No WAL at all, so no event can ever exist for it.
///
/// Isolated in its OWN database: the guard reads `pg_class`, and a case-collision
/// fixture on the shared DB would be visible to every parallel test.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_refuses_a_config_whose_resolved_identity_routing_cannot_match() {
    let cdc_db = CdcDb::new("cdc_ident");
    let slot = unique_name("rivet_ident_slot").to_lowercase();
    let mut c = cdc_db.connect();
    // The case collision, spelled the way a real schema acquires one: a quoted
    // relation plus an unquoted sibling. Different COLUMNS on purpose — that is
    // what makes the wrong-schema write visible.
    c.batch_execute(
        "CREATE TABLE \"MixedCase\" (id int, v text); \
         CREATE TABLE mixedcase (id int, other_col text, extra text); \
         CREATE UNLOGGED TABLE unlg (id int primary key, v text)",
    )
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    for (cfg, must_say) in [
        ("MixedCase", "public.mixedcase"),
        ("rivet.public.mixedcase", "public.mixedcase"),
        ("public.unlg", "SET LOGGED"),
    ] {
        let rig = Rig::pg_cdc(cfg, &slot).source_url(cdc_db.url());
        let out = run_rivet(&["run", "--config", rig.config_path().to_str().unwrap()]);
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            !out.status.success(),
            "`{cfg}` must be refused — it either captures nothing or writes rows \
             under another table's schema, silently. Output:\n{said}"
        );
        assert!(
            said.contains(must_say),
            "the refusal for `{cfg}` must name `{must_say}` — that is the whole \
             remediation. Output:\n{said}"
        );
        // A config problem must NOT arrive wearing the wal_level/REPLICATION hint:
        // an operator would go read the permissions docs for a problem that is not
        // there. Same trap the MySQL side was hoisted out of.
        assert!(
            !said.contains("wal_level=logical and a role"),
            "a routing refusal must not be prefixed with the permissions hint. \
             Output:\n{said}"
        );
    }

    // Not too WIDE: an ordinary table on the same database still captures. Oracle
    // is the manifest-declared parts, compared to what the source holds.
    c.batch_execute("CREATE TABLE plain_ok (id int primary key, v text)")
        .unwrap();
    c.execute("INSERT INTO plain_ok VALUES (1,'a'),(2,'b')", &[])
        .unwrap();
    let ok = Rig::pg_cdc("public.plain_ok", &slot).source_url(cdc_db.url());
    let rows: usize = ok.run_and_read().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 2,
        "the guard must refuse only what routing cannot match — an ordinary table \
         has to keep working, or the fix is worse than the defect"
    );
}

/// A TRUNCATE on a captured table must FAIL the run, not vanish.
///
/// `test_decoding` renders it as `table public.t: TRUNCATE: (no-flags)` — a line
/// with no columns, so the parser that builds a change out of the column list had
/// nothing to return and dropped it into its catch-all `None`. The
/// `unknown -> default` shape the CDC evidence audit found on all four engines.
///
/// Measured before the guard (2026-08-24, pg14 stand — this test's RED):
/// 2 inserts then a TRUNCATE left the source table EMPTY while the run reported
/// `status: success, rows: 2` and wrote both inserts to the destination. Zero
/// occurrences of the word at `RUST_LOG=trace` — not a quiet log, no log.
///
/// The divergence is PERMANENT, which is why this bails rather than warns: the
/// rows left the source with no DELETE events to carry them, so no later capture
/// can reconcile the destination back. A warning would leave a destination that
/// disagrees with its source forever and call the run a success.
///
/// Isolated in its OWN database (see `CdcDb`): the slot decodes the whole DB, so
/// a parallel test's TRUNCATE on the shared one would fail this run instead.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_refuses_a_truncate_instead_of_silently_diverging() {
    let cdc_db = CdcDb::new("cdc_trunc");
    let tbl = unique_name("rivet_cdc_tr").to_lowercase();
    let other = unique_name("rivet_cdc_trother").to_lowercase();
    let slot = unique_name("rivet_trunc_slot").to_lowercase();
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT); \
         CREATE TABLE {other} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {tbl}"), &[]).unwrap();
    // The SOURCE oracle, asked of PostgreSQL: the truncate really emptied it, so
    // any row in the destination is a row that does not exist.
    let source_rows: i64 = c
        .query_one(&format!("SELECT count(*) FROM {tbl}"), &[])
        .unwrap()
        .get(0);
    assert_eq!(
        source_rows, 0,
        "the fixture's TRUNCATE must have emptied it"
    );

    let rig = Rig::pg_cdc(&format!("public.{tbl}"), &slot).source_url(cdc_db.url());
    // Through the rig, like every other live test here: `run_expect_fail` asserts
    // the non-zero exit AND returns stdout+stderr, so the hand-rolled
    // `run_rivet(&["run", "--config", …])` this used to call was a second way of
    // doing what the rig already does — the per-file command wrapper the rig exists
    // to have replaced.
    let said = rig.run_expect_fail();
    assert!(
        said.contains("TRUNCATE"),
        "a TRUNCATE on a captured table must fail the run — shipping the 2 inserts \
         as a success leaves them in the destination with nothing to retract them. \
         Output:\n{said}"
    );
    assert!(
        said.contains("TRUNCATE") && said.contains("mode: full"),
        "the refusal must name what happened AND the re-snapshot that recovers \
         from it — a bail with no way forward just moves the operator's problem. \
         Output:\n{said}"
    );

    // ── and it must be TABLE-ADDRESSED ────────────────────────────────────────
    //
    // The slot decodes the whole DATABASE, so this TRUNCATE is in the stream every
    // other export on it reads. Failing without asking whose relation it is makes
    // one truncated table an outage for exports that never touch it — the MySQL
    // undecodable-rows guard's measured lesson (#281), pinned here before it could
    // bite again. This export is anchored on the SAME slot span that holds the
    // truncate, so the event is inside its window by construction.
    let other_slot = unique_name("rivet_trunc_slot_b").to_lowercase();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&other_slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {other} VALUES (7,70)"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {tbl}"), &[]).unwrap();
    let bystander = Rig::pg_cdc(&format!("public.{other}"), &other_slot).source_url(cdc_db.url());
    let rows: usize = bystander.run_and_read().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "an export that does not capture the truncated table must complete and \
         capture its own change"
    );

    // ── and a MULTI-relation truncate must be caught wherever ours sits ───────
    //
    // `TRUNCATE a, b` decodes as ONE line naming both
    // (`table public.a, public.b: TRUNCATE: (no-flags)`), and so does every
    // CASCADE that pulls in referencing tables. The FIRST version of this guard
    // read that list as a single name and let the statement through: MEASURED,
    // with the captured table configured, the source ended EMPTY while the run
    // reported `status: success, rows: 1`. An adversarial pass found it; re-reading
    // the code had not.
    //
    // Ours goes SECOND on purpose — a first-position-only fixture would pass
    // against the very defect this exists to catch.
    let third_slot = unique_name("rivet_trunc_slot_c").to_lowercase();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&third_slot],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (9,90)"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {other}, {tbl}"), &[]).unwrap();
    let multi = Rig::pg_cdc(&format!("public.{tbl}"), &third_slot).source_url(cdc_db.url());
    let said2 = multi.run_expect_fail();
    assert!(
        said2.contains(&tbl),
        "a TRUNCATE naming our table SECOND in a list must still refuse, and name \
         OUR table rather than whichever came first. Output:\n{said2}"
    );
    // ── and the refusal must not WEDGE the capture ────────────────────────────
    //
    // The peek is non-consuming, so the slot still sits on the truncate commit:
    // every later run meets it first and clean changes queued BEHIND it are
    // blocked, while the un-acked slot pins WAL. MEASURED: an ordinary INSERT
    // after the truncate leaves the second run failing identically.
    //
    // The message's first version said only "re-snapshot (mode: full)" — which on
    // PostgreSQL does not move the SLOT and therefore does not recover. This is
    // the same defect the unchanged-TOAST refusal had, found the same day by an
    // adversarial pass, in a guard I wrote after fixing that one.
    c.execute(&format!("INSERT INTO {tbl} VALUES (77,770)"), &[])
        .unwrap();
    // `run_expect_fail` panics if the run SUCCEEDS, which is exactly the assertion:
    // a clean change queued behind the truncate must still be blocked. If this ever
    // stops panicking the wedge is gone and the message should be softened.
    let _still_wedged = rig.run_expect_fail();
    assert!(
        said.contains("pg_replication_slot_advance"),
        "the refusal must name the way OUT of the wedge, not only the re-snapshot: \
         re-snapshotting alone leaves the slot where it is. Got:\n{said}"
    );
    // The route the message names must actually work.
    let past: String = c
        .query_one(
            &format!(
                "SELECT max(lsn)::text FROM pg_logical_slot_peek_changes('{slot}', NULL, NULL) \
                 WHERE data LIKE '%COMMIT%'"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    c.execute(
        &format!("SELECT pg_replication_slot_advance('{slot}', '{past}')"),
        &[],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (88,880)"), &[])
        .unwrap();
    Rig::pg_cdc(&format!("public.{tbl}"), &slot)
        .source_url(cdc_db.url())
        .run_ok();
}

/// MySQL peer of `roast_pg_cdc_refuses_a_truncate_instead_of_silently_diverging`,
/// and the same divergence by a DIFFERENT route: a TRUNCATE is logged as a QUERY
/// event, never as rows, so the rows path never sees it and the `_ => {}` arm
/// dropped it silently.
///
/// Measured before the guard (2026-08-24, rivet-mysql-cdc-1): 2 inserts then a
/// TRUNCATE left the source table EMPTY while the run reported
/// `status: success, rows: 2`.
///
/// The engines differ in the mechanism and agree on the outcome, which is why
/// both cells are `test` in the fail-loud ledger rather than one being inferred
/// from the other. SQL Server needs neither: it REFUSES the truncate itself
/// (Msg 4711 on a CDC-enabled table), so the divergence cannot be created.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn roast_mysql_cdc_refuses_a_truncate_instead_of_silently_diverging() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_mtr");
    let other = unique_name("cdc_mtrother");
    let mut c = conn();
    for t in [&tbl, &other] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT)"))
            .unwrap();
    }
    let _g1 = Table(tbl.clone());
    let _g2 = Table(other.clone());

    let ck = d.path().join("tr.ckpt");
    let rig = || {
        Rig::mysql_cdc(&tbl)
            .checkpoint_path(ck.to_path_buf())
            .dest_path(d.path().join("out"))
    };
    rig().run_ok(); // anchor before the truncate lands

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();
    c.query_drop(format!("TRUNCATE {tbl}")).unwrap();
    // The SOURCE oracle: the truncate really emptied it, so any row the run ships
    // is a row that does not exist.
    let left: Option<i64> = c
        .query_first(format!("SELECT count(*) FROM {tbl}"))
        .unwrap();
    assert_eq!(left, Some(0), "the fixture's TRUNCATE must have emptied it");

    let msg = rig().run_expect_fail();
    assert!(
        msg.contains("TRUNCATE") && msg.contains("mode: full"),
        "the run must refuse and name both what happened and the re-snapshot that \
         recovers from it — shipping the 2 inserts as a success leaves them in the \
         destination with nothing to retract them. Got: {msg}"
    );

    // Table-addressed, same as the undecodable-rows guard beside it (#281): the
    // binlog carries every table on the server, so an export that does not capture
    // the truncated table must still complete. Anchored BEFORE the truncate, so
    // the event is inside its window by construction rather than by timing.
    let other_ck = d.path().join("tr_other.ckpt");
    let other_rig = || {
        Rig::mysql_cdc(&other)
            .checkpoint_path(other_ck.to_path_buf())
            .dest_path(d.path().join("out_other"))
    };
    other_rig().run_ok();
    c.query_drop(format!("INSERT INTO {other} VALUES (7,70)"))
        .unwrap();
    c.query_drop(format!("TRUNCATE {tbl}")).unwrap();
    let rows: usize = other_rig()
        .run_and_read()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        rows, 1,
        "an export that does not capture the truncated table must complete and \
         capture its own change"
    );
}

/// An unqualified `table:` captures EVERY schema's relation of that name.
///
/// `sink::table_matches` matches a bare config name against any schema — it has to,
/// because a MongoDB collection has no schema qualifier and may itself contain dots.
/// On PostgreSQL that means `table: orders` silently captures `public.orders` AND
/// `archive.orders` into one export.
///
/// MEASURED before the warning (2026-08-25): `table: bare` with `public.bare` and
/// `s2.bare` both present captured 2 rows — one from each schema — into a single
/// part, with nothing in the output distinguishing them. Counts reconcile against
/// neither table alone, and a reader of the parquet cannot tell which row came from
/// where.
///
/// A WARNING, not a refusal: capturing one table under a bare name is the common and
/// correct case. What the operator cannot see is the second relation riding along.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_warns_when_a_bare_table_name_matches_two_schemas() {
    let cdc_db = CdcDb::new("cdc_bare");
    let tbl = unique_name("rivet_cdc_bare").to_lowercase();
    let slot = unique_name("rivet_bare_slot").to_lowercase();
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id int PRIMARY KEY, v text); \
         CREATE SCHEMA other; \
         CREATE TABLE other.{tbl} (id int PRIMARY KEY, v text)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1,'public'); INSERT INTO other.{tbl} VALUES (2,'other')"
    ))
    .unwrap();

    let rig = Rig::pg_cdc(&tbl, &slot).source_url(cdc_db.url());
    let said = rig.run_ok_capture();
    assert!(
        said.contains("unqualified") && said.contains(&format!("other.{tbl}")),
        "the run must WARN and name the other relation — the operator cannot see it \
         in the output otherwise. Got:\n{said}"
    );

    let rows: usize = rig.read_declared_parts().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 2,
        "both schemas' rows land in ONE export — that is the behaviour being warned \
         about, and if it ever changes the warning should change with it"
    );

    // Not too WIDE: one matching relation is the ordinary case and must stay silent,
    // or the warning becomes noise and gets ignored.
    c.batch_execute("DROP SCHEMA other CASCADE").unwrap();
    c.execute(&format!("INSERT INTO {tbl} VALUES (3,'only')"), &[])
        .unwrap();
    let quiet = Rig::pg_cdc(&tbl, &slot).source_url(cdc_db.url());
    assert!(
        !quiet.run_ok_capture().contains("unqualified"),
        "a bare name with ONE matching relation must not warn"
    );
}

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
    fn name(&self) -> &str {
        &self.name
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

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
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

/// `rivet cdc --max-events` must not be able to WEDGE a checkpointed CLI run.
///
/// The CLI leg of the driver-bypass this closed: the file sink deferred the cap
/// to a commit boundary, the NDJSON loop (`cdc::run`, i.e. `rivet cdc` with no
/// `--output`) broke on the event count alone. Nothing was LOST — the checkpoint
/// save is gated on `committed`, so a cut transaction re-emits on resume — but a
/// transaction LONGER than the cap held no boundary to save, so the checkpoint
/// never advanced and every run re-printed the same prefix and stopped in the
/// same place. Silent, and shaped like a config problem.
///
/// MySQL, deliberately: it is the engine whose NDJSON resume IS the checkpoint
/// file (`dispatch.rs` — PostgreSQL re-reads from its slot on this path and does
/// not ack by design, so the wedge is invisible there). MySQL also buffers a
/// transaction and releases it whole at XID with only the LAST row `committed`,
/// which is exactly the shape that has no boundary to stop at mid-way.
///
/// RED before the fix: run 2 re-emits rows 1..=2 forever.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn roast_mysql_cdc_cli_max_events_below_a_transaction_still_advances_the_checkpoint() {
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_cap");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck");
    write_checkpoint(&mut c, &ckpt); // anchor at NOW, so only what follows is in scope

    // ONE transaction of five rows — longer than the cap below, and released
    // whole at its XID, so a hard per-event stop lands with no boundary to save.
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3),(4,4),(5,5)"
    ))
    .expect("seed one transaction");

    let ckpt_s = ckpt.to_str().unwrap().to_string();
    let tbl_q = tbl.clone();
    let sid = server_id_for(&tbl).to_string();
    let cdc_run = move || {
        run_rivet_args_bounded(
            &[
                "cdc",
                "--source",
                MYSQL_CDC_URL,
                "--server-id",
                &sid,
                "--table",
                &tbl_q,
                "--checkpoint",
                &ckpt_s,
                "--max-events",
                "2",
            ],
            std::time::Duration::from_secs(60),
        )
    };
    let ids = |out: &str, tbl: &str| -> std::collections::BTreeSet<i64> {
        out.lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter(|v| v.get("table").and_then(|t| t.as_str()) == Some(tbl))
            .filter_map(|v| v.get("after")?.get(0)?.as_i64())
            .collect()
    };

    let first = cdc_run().expect("run 1 must terminate");
    let ids1 = ids(&first, &tbl);
    // The cap is SOFT: it overshoots to the commit boundary rather than cutting
    // the transaction, which is the only way it can checkpoint at all.
    assert_eq!(
        ids1,
        (1..=5).collect::<std::collections::BTreeSet<i64>>(),
        "a cap of 2 inside a 5-row transaction must reach the boundary, not cut it; got {ids1:?}\n{first}"
    );

    let second = cdc_run().expect("run 2 must terminate");
    let ids2 = ids(&second, &tbl);
    assert!(
        ids2.is_empty(),
        "run 2 must emit nothing — the checkpoint advanced past the transaction. Re-emitting \
         {ids2:?} is the wedge: pre-fix no boundary was ever saved, so every run re-printed the \
         same prefix and the export never progressed"
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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

    let tbl_bg = tbl.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
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
            // Bounded (until-current) is the DEFAULT now; `--stream` opts into
            // continuous. This roast asserts the default terminates + emits the
            // backlog, so it passes no flag.
        ],
        std::time::Duration::from_secs(30),
    );
    bg.stop();
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

    let got: std::collections::BTreeSet<i64> = duckdb_dir_parquet_i64(&rig.out_dir(), "id")
        .into_iter()
        .collect();
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
    let d = tempfile::tempdir().unwrap();
    // Isolated DB (see CdcDb): the bounded drain must capture the WHOLE 20k backlog
    // in one pass, which parallel tests' foreign WAL in a shared-DB slot's view
    // would break (density). Its own DB gives the slot only this test's WAL.
    let cdc_db = CdcDb::new("cdc_walret");
    let tbl = unique_name("rivet_cdc_walret");
    let slot = unique_name("rivet_walret_slot");
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id bigint primary key, v int, pad text)"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();

    let confirmed = |c: &mut postgres::Client| -> String {
        c.query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0)
    };
    let cf_before = confirmed(&mut c);
    // Generate a backlog (≈20k rows of WAL) the slot now pins.
    c.execute(
        &format!(
            "INSERT INTO {tbl} SELECT g, g%1000, repeat('x',80) FROM generate_series(1,20000) g"
        ),
        &[],
    )
    .unwrap();
    // Prove the slot IS pinning a real backlog. (pg_current_wal_lsn is the WHOLE
    // server's position, so this can be INFLATED by parallel tests' WAL — that
    // only makes it more clearly > 1 MB, so it stays a valid "is pinning" check.)
    let pinned_before: i64 = c
        .query_one(
            &format!("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '{cf_before}'::pg_lsn)::bigint"),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        pinned_before > 1_000_000,
        "the slot must pin a real amount of WAL before the drain (got {pinned_before} bytes)"
    );

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(out.clone());
    run_rivet_ok(&rig.config_path());

    assert_eq!(
        manifest_rows(&out),
        20000,
        "the drain must capture the whole backlog"
    );
    // The release metric is confirmed_flush's OWN advance (per-slot), NOT
    // pg_wal_lsn_diff(pg_current_wal_lsn(), ..): pg_current_wal_lsn is the whole
    // server's WAL, so parallel tests' WAL would swamp a "current - confirmed_flush"
    // reading (it FAILED that way — retained went UP under load). This advance is
    // this slot's alone.
    let cf_after = confirmed(&mut c);
    let advanced: i64 = c
        .query_one(
            &format!("SELECT pg_wal_lsn_diff('{cf_after}'::pg_lsn, '{cf_before}'::pg_lsn)::bigint"),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        advanced > 1_000_000,
        "the drain must RELEASE the pinned WAL by advancing confirmed_flush past the multi-MB \
         backlog — it advanced only {advanced} bytes (an un-acked drain leaves confirmed_flush \
         pinned: the disk-fill harm; catalog_xmin/vacuum follows the same advance)"
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
    let vcs: Vec<i64> = duckdb_dir_parquet_i64(&out, "v");
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
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
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
#[ignore = "live: requires the cdc-standby profile — python3 -m dev.pytools.cdc_stand standby (pg-cdc-standby on :5436)"]
fn roast_pg_cdc_bounded_on_a_standby_fails_loud() {
    // A bounded (until_current) CDC run against a PostgreSQL STANDBY (in recovery)
    // must fail LOUD with an actionable message: pg_current_wal_lsn() is
    // unavailable in recovery and a logical slot cannot be created there. The
    // adapter checks pg_is_in_recovery() up front and names the escape (stream
    // continuously, or point at the primary) — not a raw "recovery is in
    // progress". Oracle: the run fails, stderr names the standby + the fix.
    //
    // Opt-in profile: the cdc-standby pair (python3 -m dev.pytools.cdc_stand standby, :5436) is
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
        skip_live(
            "roast_pg_cdc_bounded_on_a_standby_fails_loud: cdc-standby not up on :5436 \
             (bring it up with `python3 -m dev.pytools.cdc_stand standby`)",
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

/// Finding #3: MySQL CDC enriches ENUM/SET labels from information_schema.COLUMNS.
/// The old query pinned `TABLE_SCHEMA = DATABASE()` and dropped any `db.`
/// qualifier, so a CROSS-DATABASE capture — a table in a database OTHER than the
/// connection's default (`rivet`) — enriched NOTHING and every ENUM exported as
/// its raw integer index (a SET as its bitmask), silently. Capture a qualified
/// `otherdb.orders` and assert the ENUM lands as its LABEL, from the table's own
/// schema. RED before the fix: the wire index ('3') leaked through instead of 'off'.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_cross_database_enum_enriches_from_the_tables_own_schema() {
    let d = tempfile::tempdir().unwrap();
    let dbname = unique_name("rivet_xdb").to_lowercase();
    let qualified = format!("{dbname}.orders");

    // A SECOND database (NOT the connection's default `rivet`) created via root,
    // with a SELECT grant so the rivet user can read the table + its catalog.
    let root_url = MYSQL_CDC_URL.replace("rivet:rivet@", "root:rivet@");
    let mut admin = mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()).unwrap();
    admin
        .query_drop(format!("CREATE DATABASE {dbname}"))
        .expect("create cross db");
    // Drop guard: tear the whole database down even if the test panics.
    struct DbGuard(String, String);
    impl Drop for DbGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.1).unwrap()) {
                let _ = c.query_drop(format!("DROP DATABASE IF EXISTS {}", self.0));
            }
        }
    }
    let _guard = DbGuard(dbname.clone(), root_url.clone());
    admin
        .query_drop(format!(
            "CREATE TABLE {dbname}.orders \
             (id INT PRIMARY KEY, status ENUM('active','shipped','off'))"
        ))
        .expect("create cross-db table");
    admin
        .query_drop(format!("GRANT SELECT ON {dbname}.* TO 'rivet'@'%'"))
        .expect("grant select");
    admin.query_drop("FLUSH PRIVILEGES").unwrap();

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let cfg = cdc_config(&d, &qualified, &ckpt, &out);

    run_rivet_ok(&cfg); // anchor at the current binlog position
    admin
        .query_drop(format!("INSERT INTO {dbname}.orders VALUES (1, 'off')"))
        .expect("insert enum row");
    run_rivet_ok(&cfg); // capture the change

    // 'off' is the 3rd ENUM label; the binlog delivers the INDEX (3). Only the
    // information_schema enrichment — from the table's OWN schema — turns it back
    // into the label. Pre-fix, cross-db enrichment silently failed and the index
    // leaked through as text.
    assert_eq!(
        parquet_one_string(&out, "status"),
        "off",
        "a cross-database ENUM must enrich to its LABEL from the table's own schema, \
         not the connection's default DATABASE()"
    );
}

/// Closes the CDC/incremental → warehouse LOAD quadrant (the coverage-audit HIGH
/// blind spot: every load suite sourced `mode: full` parquet, so no test ever
/// LOADED rivet's CDC `__changes` output). DuckDB is the independent-reader proxy
/// for the warehouse load: `read_parquet(union_by_name=true)` over BOTH the
/// `initial: snapshot` leg AND the CDC change leg mimics the loader's
/// declared-schema LOAD-by-name into one `<table>__changes` table (snapshot rows
/// get `__op`/`__pos`/`__seq` = NULL). It is the exact `fda1653` class: a
/// snapshot leg that KEPT its batch `meta_columns` would leak a column the CDC
/// stream lacks — silent under count/sum checks, breaks the warehouse load one
/// layer up. The current-state dedup is then verified against the SOURCE's actual
/// rows (an independent oracle, not rivet's own output).
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + duckdb"]
fn cdc_changes_parquet_loads_into_a_warehouse_and_dedups_to_current_state() {
    let d = tempfile::tempdir().unwrap();
    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("cdc_load"));
    let tbl = unique_name("cdc_load");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    // Pre-snapshot rows — the initial:snapshot backfill leg.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let ckpt = d.path().join("cdc.ckpt");
    // meta_columns are REQUESTED: the fda1653 trap. They must be dropped from the
    // snapshot leg so both legs' data columns match in the shared __changes append.
    let rig = Rig::mysql_cdc(&tbl)
        .cdc_line("initial: snapshot")
        .export_line("meta_columns: { exported_at: true, row_hash: true }")
        .checkpoint_path(ckpt.clone())
        .dest_path(host_dir.clone());
    let cfg = rig.config_path();
    run_rivet_ok(&cfg); // snapshot leg → host_dir/snapshot/*.parquet

    // Post-snapshot changes: an UPDATE (dedup must pick the new value over the
    // snapshot baseline) and an INSERT (a PK present ONLY in the change leg).
    c.query_drop(format!("UPDATE {tbl} SET v = 100 WHERE id = 1"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (3,30)"))
        .unwrap();
    run_rivet_ok(&cfg); // CDC leg → host_dir/cdc-*.parquet

    // DuckDB = the independent warehouse-load proxy. union_by_name mimics the
    // loader's declared-schema LOAD-by-name (snapshot's missing __op/__pos/__seq
    // become NULL). A LIST of globs unions the two legs into one __changes table.
    let changes = format!(
        "read_parquet(['{c}/snapshot/*.parquet','{c}/cdc-*.parquet'], union_by_name=true)",
        c = container_dir,
    );

    // (1) LOADABILITY / fda1653: the unioned __changes carries EXACTLY the three
    //     meta columns + the source data columns — NO leaked batch meta_column
    //     from the snapshot leg (which would break a real warehouse load).
    let desc = duckdb_run_sql_json(&format!("DESCRIBE SELECT * FROM {changes}"));
    let cols = duckdb_parse_describe(&desc);
    // The unioned __changes must be EXACTLY {__op, __pos, __seq} + the source data
    // columns — any other column is a leaked snapshot-leg meta_column. The meta
    // columns are `_rivet_exported_at` / `_rivet_row_hash` (SINGLE underscore, see
    // src/enrich.rs) — a `__rivet` (double) check silently never matches and is
    // vacuous (which is exactly how the sibling snapshot-parity test's assertion
    // reads today). Assert the whole set instead of a prefix so any leak is caught.
    let mut got_cols: Vec<String> = cols.keys().cloned().collect();
    got_cols.sort();
    assert_eq!(
        got_cols,
        vec![
            "__op".to_string(),
            "__pos".to_string(),
            "__seq".to_string(),
            // Written by BOTH legs since §5h, so it belongs in the union — and it
            // must be here, not merely tolerated: were only one leg producing it,
            // `union_by_name` would still succeed and leave that leg's rows NULL,
            // which is the silent half-empty column the two-leg contract exists to
            // rule out. `_rivet_exported_at` is deliberately absent (batch-only).
            "_rivet_row_hash".to_string(),
            "id".to_string(),
            "v".to_string(),
        ],
        "the unioned __changes must carry EXACTLY the 3 CDC meta columns + the \
         both-legs `_rivet_row_hash` + the source data columns; any other column is \
         a leaked snapshot-leg meta_column (fda1653 — breaks the warehouse load, \
         silent under count/sum checks)"
    );

    // (2) CURRENT-STATE: an INDEPENDENT dedup (latest change per PK wins; the
    //     snapshot baseline has __pos NULL = oldest) reconstructs current state.
    //     The oracle is the SOURCE's actual rows {1:100, 2:20, 3:30}.
    let sql = format!(
        "SELECT id, v FROM (
           SELECT id, v, ROW_NUMBER() OVER (
             PARTITION BY id ORDER BY (__pos IS NOT NULL) DESC, __seq DESC
           ) AS rn FROM {changes}
         ) WHERE rn = 1 ORDER BY id"
    );
    let res = duckdb_run_sql_json(&sql);
    let got: Vec<(String, String)> = res["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            let a = r.as_array().unwrap();
            (
                a[0].as_str().unwrap().to_string(),
                a[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".into(), "100".into()),
            ("2".into(), "20".into()),
            ("3".into(), "30".into()),
        ],
        "CDC __changes must LOAD (both legs into one table) and DEDUP to the source's \
         current state — id=1 updated to 100, id=2 unchanged, id=3 insert-only"
    );
}

/// Independent CDC per-type oracle (option A's real upgrade, via the canonical
/// Rig): the workhorse `*_cdc_full_type_matrix_matches_batch` cells are a
/// DIFFERENTIAL self-oracle — they compare CDC's decode to BATCH's decode, so a
/// bug SHARED by both (both agree on a wrong value) passes, exactly the class the
/// value-checksum Form A also misses. This reads the CDC `__changes` parquet with
/// DuckDB — a reader OUTSIDE rivet's decode family — and asserts each typed value
/// equals the SOURCE literal, not a batch re-decode. A shared-decode corruption
/// (enum index instead of label, an unsigned mangle, a decimal float) is caught
/// here where matches_batch cannot see it.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + duckdb"]
fn mysql_cdc_typed_values_match_source_via_duckdb_not_batch() {
    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("cdc_typed"));
    let tbl = unique_name("cdc_typed");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, amount DECIMAL(18,4), \
         en ENUM('active','shipped','off'), big BIGINT UNSIGNED, \
         note VARCHAR(20), d DATE, vb VARBINARY(4), uid CHAR(36), \
         fl DOUBLE, st SET('a','b','c'), flag BOOLEAN, j JSON)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    // Canonical Rig CDC (until_current + default checkpoint): anchor over the empty
    // table, then capture the typed insert on the second run.
    let rig = Rig::mysql_cdc(&tbl).dest_path(host_dir.clone());
    rig.run_ok();
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, 12345.6789, 'off', 18000000000000000000, \
         'hello', '2024-03-15', 0xDEADBEEF, '12345678-1234-1234-1234-123456789012', \
         1.5, 'a,b', 1, '{{\"k\": \"v\", \"n\": 42}}')"
    ))
    .unwrap();
    rig.run_ok();

    // DuckDB re-reads the __changes parquet and compares each value to the SOURCE
    // literal — independent of any rivet re-decode. A single boolean, true iff the
    // decimal, the ENUM LABEL (not index 3), the unsigned-64 (> i64::MAX), the text,
    // the date, and the binary (hex) all survived the change stream exactly.
    let res = duckdb_run_sql_json(&format!(
        "SELECT (amount = 12345.6789) AND (en = 'off') AND (big = 18000000000000000000) \
         AND (note = 'hello') AND (d = DATE '2024-03-15') AND (lower(to_hex(vb)) = 'deadbeef') \
         AND (uid = '12345678-1234-1234-1234-123456789012') \
         AND (fl = 1.5) AND (st = 'a,b') AND (CAST(flag AS INTEGER) = 1) \
         AND (json_extract_string(j, 'k') = 'v') AND (CAST(json_extract(j, 'n') AS INTEGER) = 42) \
         FROM read_parquet('{container_dir}/cdc-*.parquet') WHERE id = 1"
    ));
    let rows = res["rows"].as_array().expect("duckdb rows");
    assert_eq!(
        rows.len(),
        1,
        "exactly one captured change for id=1; got: {res}"
    );
    assert!(
        rows[0][0]
            .as_str()
            .is_some_and(|s| s.eq_ignore_ascii_case("true")),
        "CDC-decoded typed values must equal the SOURCE literals read back by DuckDB \
         (independent of batch): decimal 12345.6789, ENUM LABEL 'off' (not index 3), \
         unsigned 18000000000000000000 (> i64::MAX). A shared decode bug shows here; got: {res}"
    );
}

/// PG sibling of `mysql_cdc_typed_values_match_source_via_duckdb_not_batch`: the
/// independent CDC per-type oracle for PostgreSQL. CDC via `Rig::pg_cdc`, the
/// `__changes` parquet re-read by DuckDB (outside rivet's decode family), each
/// value asserted vs the SOURCE literal — catching a shared-decode bug (enum
/// index vs label, a numeric misparse) that the batch-differential misses.
/// Isolated in its own database (`CdcDb`) so the slot sees only this test's WAL.
#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical) + duckdb"]
fn pg_cdc_typed_values_match_source_via_duckdb_not_batch() {
    let cdc_db = CdcDb::new("cdc_typed_pg");
    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("cdc_typed_pg"));
    let tbl = unique_name("rivet_cdc_typed").to_lowercase();
    let slot = unique_name("rivet_typed_slot").to_lowercase();
    let mut c = cdc_db.connect();
    c.batch_execute(&format!(
        "CREATE TYPE {tbl}_status AS ENUM ('active','shipped','off'); \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, amount NUMERIC(18,4), \
         status {tbl}_status, note TEXT, big BIGINT, flag BOOLEAN, \
         fl DOUBLE PRECISION, d DATE, rb BYTEA, uid UUID, iv INTERVAL, j JSONB)"
    ))
    .unwrap();

    let rig = Rig::pg_cdc(&tbl, &slot)
        .source_url(cdc_db.url())
        .dest_path(host_dir.clone());
    rig.run_ok(); // anchor (creates the slot)
    c.execute(
        &format!(
            "INSERT INTO {tbl} VALUES (1, 12345.6789, 'off', 'hello', 9000000000000, true, \
             1.5, '2024-03-15', '\\xDEADBEEF', \
             '12345678-1234-1234-1234-123456789012', INTERVAL '1 year 2 mons 3 days', \
             '{{\"k\": \"v\", \"n\": 42}}')"
        ),
        &[],
    )
    .unwrap();
    rig.run_ok(); // capture

    let res = duckdb_run_sql_json(&format!(
        "SELECT (amount = 12345.6789) AND (status = 'off') AND (note = 'hello') \
         AND (big = 9000000000000) AND (flag = true) AND (fl = 1.5) \
         AND (d = DATE '2024-03-15') AND (lower(to_hex(rb)) = 'deadbeef') \
         AND (lower(CAST(uid AS VARCHAR)) = '12345678-1234-1234-1234-123456789012') \
         AND (iv = 'P1Y2M3D') \
         FROM read_parquet('{container_dir}/cdc-*.parquet') WHERE id = 1"
    ));
    let rows = res["rows"].as_array().expect("duckdb rows");
    assert_eq!(rows.len(), 1, "one captured change for id=1; got: {res}");
    assert!(
        rows[0][0]
            .as_str()
            .is_some_and(|s| s.eq_ignore_ascii_case("true")),
        "PG CDC typed values must equal the SOURCE literals via DuckDB (independent of \
         batch): numeric 12345.6789, enum LABEL 'off' (not an index), text, bigint, bool; \
         got: {res}"
    );
}

// The two legs of ONE `initial: snapshot` export must claim ONE source identity.
//
// `identity_source` is what `ensure_single_source` compares when a load reads a
// prefix, and the two legs derived it differently: the CDC drain records the
// capture output's table, while the snapshot leg is a batch run whose manifest
// source used to be split off the EXPORT NAME — and the leg's synthesized name
// (`orders__snapshot_orders`) has no dot, so it recorded `table: null`. Measured
// on a real run into one prefix before the fix: drain
// `{engine: postgres, table: "idsrc_orders"}` and leg `{engine: postgres,
// table: null}`, which read as `postgres:idsrc_orders` and `postgres` — two
// sources under one export name, so `rivet load` refuses the flow the docs
// describe.
//
// A name is a label; the config is the catalog. The manifest's source table now
// comes from the export's declared `table:`, carried on the plan.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn both_legs_of_an_initial_snapshot_export_claim_one_source_identity() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_ident");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,10),(2,20)"))
        .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let rig = Rig::mysql_cdc(&tbl)
        .source_url_env("MYSQL_CDC_URL")
        .cdc_line("initial: snapshot")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    // The config resolves its source through `url_env:` ON PURPOSE — this test is
    // about SOURCE IDENTITY, and the env form is the one a deployment uses when a
    // plaintext URL would be redacted out of the artifact. But nothing ever SET
    // the variable, so every run of this test died at config load with "env var
    // 'MYSQL_CDC_URL' is not set" before reaching a single assertion. Pass it.
    let out_run = rig.run_args_env(&[], &[("MYSQL_CDC_URL", MYSQL_CDC_URL)]);
    assert!(
        out_run.status.success(),
        "the snapshot+cdc run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr)
    );

    // Read what each leg actually WROTE, and compare with the product's own
    // identity rule rather than a copy of it.
    let source_of = |p: &std::path::Path| -> (String, Option<String>) {
        let body =
            std::fs::read_to_string(p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));
        let m: serde_json::Value = serde_json::from_str(&body).unwrap();
        (
            m["source"]["engine"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            m["source"]["table"].as_str().map(str::to_string),
        )
    };
    let drain = source_of(&out.join("manifest.json"));
    let leg = source_of(&out.join("snapshot").join("manifest.json"));

    // Not inert: both legs must have written a manifest with an engine at all.
    assert!(
        !drain.0.is_empty() && !leg.0.is_empty(),
        "fixture is inert — one leg wrote no manifest: drain={drain:?} leg={leg:?}"
    );
    assert_eq!(
        drain, leg,
        "the two legs of ONE export disagree about their source: a load over this prefix sees \
         two identities and refuses to load either"
    );
    assert_eq!(
        leg.1.as_deref(),
        Some(tbl.as_str()),
        "the snapshot leg must record the DECLARED table, not whatever its synthesized export \
         name happens to parse to"
    );
}

// A CDC destination carrying a `{date}` template must be resolved at WRITE time
// the same way `rivet validate` resolves it at READ time.
//
// The batch path never had this bug and that is precisely why it hid: every
// batch runner writes to `plan.destination`, which `plan::build` expands while
// building the plan. `job.rs` returns into `cdc_job::run_cdc_export` BEFORE
// `build_plan`, so CDC had no plan and no expansion — `create_destination` got
// the RAW config and made a directory named, literally, `{date}`, while
// validate/load resolved the template to today's date and reported an empty
// destination over a perfectly captured stream.
//
// The assertion is deliberately two-sided. "Rows landed somewhere" is not the
// property (they always did); the property is that they landed where the reader
// looks AND that the literal-template directory does not exist. A one-sided
// check passes against the bug — the pre-fix run also produces readable parquet,
// just at an address nothing else in rivet ever visits.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn roast_pg_cdc_destination_placeholders_resolve_like_the_batch_path() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_placeholder");
    let slot = unique_name("rivet_placeholder_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id INT PRIMARY KEY, v TEXT);"
    ))
    .unwrap();
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    // Drop the slot even if an assertion below panics. The file's other tests
    // use a TRAILING `pg_drop_replication_slot`, which leaks whenever the test
    // fails before reaching it — and a leaked logical slot is not inert: it
    // pins WAL and counts against `max_replication_slots` (32 here). Measured
    // 2026-08-05: this test alone left 25 of 32 slots held after a day of runs,
    // and the PG CDC tests then began failing in the parallel suite with
    // symptoms that looked like anything but slot exhaustion.
    struct SlotGuard(String);
    impl Drop for SlotGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls) {
                let _ = c.execute(
                    &format!("SELECT pg_drop_replication_slot('{}')", self.0),
                    &[],
                );
            }
        }
    }
    let _slot_guard = SlotGuard(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c');"
    ))
    .unwrap();

    let base = d.path().join("out");
    std::fs::create_dir_all(&base).unwrap();
    // The config carries the TEMPLATE, exactly as an operator would write it.
    Rig::pg_cdc(&tbl, &slot)
        .dest_path(base.join("{date}"))
        .run_ok();

    let literal = base.join("{date}");
    let resolved = base.join(chrono::Utc::now().format("%Y-%m-%d").to_string());

    // The oracle is WHERE THE PARQUET IS, never whether a directory exists: the
    // rig itself `create_dir_all`s the configured destination before launching
    // (`rig.rs`), so the literal `{date}` directory is present either way and an
    // existence check would grade the harness instead of the product. This cost
    // one RED run to learn — the first draft asserted `!literal.exists()` and
    // failed against the FIXED binary.
    let parquet_in = |p: &std::path::Path| -> usize {
        std::fs::read_dir(p)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                    .count()
            })
            .unwrap_or(0)
    };
    let at_resolved = parquet_in(&resolved);
    assert!(
        at_resolved > 0,
        "no parquet at the RESOLVED prefix the reader computes ({}) — the \
         template reached `create_destination` unexpanded, so validate, load and \
         gc all look somewhere the drain never wrote",
        resolved.display()
    );
    assert_eq!(
        parquet_in(&literal),
        0,
        "parquet landed under the LITERAL `{{date}}` directory ({}) — the write \
         address and the read address disagree",
        literal.display()
    );
}

// The two :3306 binlog-compression tests both flip
// `SET GLOBAL binlog_transaction_compression` on the shared batch server, so
// running them in parallel races (one turns it OFF mid-way through the other's
// ON window). They serialize via `quiet_window_guard()` — the single shared-
// — a static Mutex sat here first, and it serialized NOTHING under the
// canonical nextest runner (per-test processes; r3 bughunt find).

/// The binlog-compression guard, live.
///
/// `binlog_transaction_compression` (MySQL 8.0.20+) packs a transaction's
/// `TableMap`/`Rows`/`Xid` into one `Transaction_payload_event`. This reader
/// matches those event types individually and ignores everything else, so with
/// compression ON it captures NOTHING and says nothing — measured on 8.0.46:
/// anchor, insert two rows, resume ⇒ 0 of 2 events, empty stderr. That is the
/// worst shape a CDC bug can take, and the operator's own instinct (shrink the
/// binlog) is what triggers it.
///
/// Until the payload is expanded, the run must REFUSE.
///
/// RUNS AGAINST THE BATCH MYSQL (:3306), NOT THE CDC ONE, and that is the point
/// rather than an accident. The setting is a server GLOBAL, so while this test
/// holds it ON every concurrent reader of that server's binlog is refused too —
/// and `cargo test` runs the file's 30-odd CDC tests in parallel against :3307.
/// The first CI run proved it: `cdc_update_and_delete_carry_full_types` failed
/// with THIS guard's message, 553 passed / 1 failed, and nothing was wrong with
/// either test. The Drop guard restores the setting but cannot help a test that
/// is already mid-run.
///
/// :3306 has `log_bin=ON` and `binlog_format=ROW` (MySQL 8 defaults) — so the
/// guard's subject exists — while nothing in the suite reads its binlog
/// (`Rig::mysql_cdc` is wired to `MYSQL_CDC_URL`; only `mysql_batch` uses this
/// one, and a batch SELECT does not care how the binlog is packed). Isolating
/// the fixture removes the shared mutable state; serialising 60 call sites
/// behind a lock would only coordinate it, and slow every CDC test to do so.
///
/// It connects as ROOT, unlike every other test here, because the batch stand
/// grants `rivet` only `ALL ON rivet.*` — `dev/mysql/init.sql` hands out no
/// REPLICATION SLAVE / REPLICATION CLIENT, so a CDC open as `rivet` dies with
/// ERROR 1227 BEFORE it can reach the compression guard, and the test would
/// assert on the wrong refusal. (A locally-hand-granted container hides this:
/// the first CI run after the move failed on exactly that gap. Widening the
/// stand's grants would be the other fix, and the wrong one — it hands every
/// batch test a privilege it must not need.) Root is what the compose file
/// declares, so it exists everywhere the suite runs.
///
/// The restore guard stays anyway: a panic here must not leave a server global
/// flipped for whatever runs next.
#[test]
#[ignore = "live: requires docker compose up -d mysql (:3306, log_bin=ON)"]
fn mysql_cdc_refuses_a_compressed_binlog_instead_of_capturing_nothing() {
    let _serial = quiet_window_guard(); // :3306 GLOBAL flip — same lock as governor
    let root_url = MYSQL_URL.replace("rivet:rivet@", "root:rivet@");
    let mut admin = match mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()) {
        Ok(c) => c,
        Err(e) => panic!("cdc-profile MySQL admin connection: {e}"),
    };
    // Pre-8.0.20 servers have no such variable — the guard is a no-op there and
    // so is this test. Skip rather than fail: the engine cannot have the bug.
    let supported: Option<String> = admin
        .query_first("SELECT @@global.binlog_transaction_compression")
        .ok()
        .flatten();
    if supported.is_none() {
        skip_live("server has no binlog_transaction_compression (pre-8.0.20)");
        return;
    }

    struct CompressionGuard(String);
    impl Drop for CompressionGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.0).unwrap()) {
                let _ = c.query_drop("SET GLOBAL binlog_transaction_compression = OFF");
            }
        }
    }
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = ON")
        .expect("set global binlog_transaction_compression");
    let _restore = CompressionGuard(root_url.clone());

    let tbl = unique_name("cdc_compressed");
    // Everything in this test talks to :3306 — the isolated server, see above.
    let batch_conn =
        || mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()).expect("batch mysql");
    let mut c = batch_conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    struct BatchTable(String);
    impl Drop for BatchTable {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()) {
                let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
            }
        }
    }
    let _guard = BatchTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();

    // `cdc_config` (the file's shared helper) points at :3307; every run here
    // must go to the isolated :3306 instead, so the config is built locally.
    let cfg_into = |dest: &std::path::Path| {
        Rig::mysql_cdc(&tbl)
            .source_url(&root_url)
            .checkpoint_path(ckpt.clone())
            .dest_path(dest.to_path_buf())
            .config_in(d.path())
    };
    // The refusal must come from the OPEN, before any capture claim — so it
    // fires on the anchoring run, not only once changes exist.
    let cfg = cfg_into(&out);
    let out_text = {
        let o = run_rivet(&["run", "--config", cfg.to_str().unwrap()]);
        assert!(
            !o.status.success(),
            "a compressed binlog must FAIL the run, not capture nothing:\n{}",
            String::from_utf8_lossy(&o.stdout)
        );
        format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        )
    };
    assert!(
        out_text.contains("binlog_transaction_compression"),
        "the refusal must name the setting so it is actionable:\n{out_text}"
    );

    // And with the setting off again the SAME table captures normally —
    // proving the guard is the only thing that blocked it, not the fixture.
    // A dir per run, like every other CDC test here: two runs into one prefix
    // is the clobber scenario, not the capture assertion.
    drop(_restore);
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = OFF")
        .unwrap();
    let out1 = d.path().join("out1");
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out1).unwrap();
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&cfg_into(&out1)); // anchors
    // A FRESH connection for the write. `binlog_transaction_compression` is
    // captured into the SESSION at connect time, so `c` — opened while the
    // global was ON — still writes compressed transactions no matter what the
    // global says now. (The first version of this test used `c` here and
    // captured 0 rows: an accidental second demonstration of the very bug the
    // guard exists for, and the same session-state trap the timezone test
    // above documents.)
    let mut fresh = batch_conn();
    fresh
        .query_drop(format!("INSERT INTO {tbl} VALUES (1, 10), (2, 20)"))
        .unwrap();
    run_rivet_ok(&cfg_into(&out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "with compression off the same table must capture both rows"
    );
}

/// #200-2: the STREAM-time sibling of the open-time guard above. Compression can
/// be turned on AFTER a run has opened (the open-time `refuse_compressed_binlog`
/// saw it OFF and passed), leaving a `Transaction_payload_event` in the binlog
/// AHEAD of the checkpoint. The reader cannot expand it, and before the fix the
/// `_ => {}` arm in `fill()` SILENTLY skipped it — the resume captured NOTHING for
/// that transaction while the checkpoint advanced past it, an at-least-once break
/// that every count/manifest check would call success.
///
/// The sequence forces exactly that: anchor with compression OFF (open-time guard
/// passes), write a compressed transaction, then RESUME with compression still
/// OFF at open (guard passes again) so the compressed span reaches `fill()`. The
/// resume must FAIL loudly, not report a clean zero-capture.
#[test]
#[ignore = "live: requires docker compose up -d mysql (:3306, log_bin=ON, 8.0.20+)"]
fn mysql_cdc_compressed_payload_in_stream_refuses_not_skips() {
    let _serial = quiet_window_guard(); // :3306 GLOBAL flip — same lock as governor
    let root_url = MYSQL_URL.replace("rivet:rivet@", "root:rivet@");
    let mut admin = match mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()) {
        Ok(c) => c,
        Err(e) => panic!("cdc-profile MySQL admin connection: {e}"),
    };
    let supported: Option<String> = admin
        .query_first("SELECT @@global.binlog_transaction_compression")
        .ok()
        .flatten();
    if supported.is_none() {
        skip_live("server has no binlog_transaction_compression (pre-8.0.20)");
        return;
    }

    struct CompressionGuard(String);
    impl Drop for CompressionGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.0).unwrap()) {
                let _ = c.query_drop("SET GLOBAL binlog_transaction_compression = OFF");
            }
        }
    }
    let _restore = CompressionGuard(root_url.clone());
    // Start from OFF so the anchor run's OPEN-time guard passes.
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = OFF")
        .unwrap();

    let tbl = unique_name("cdc_cmp_stream");
    let mut c = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()).expect("batch mysql");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v TEXT)"))
        .unwrap();
    struct BatchTable(String);
    impl Drop for BatchTable {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()) {
                let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
            }
        }
    }
    let _guard = BatchTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    let cfg_into = |dest: &std::path::Path| {
        Rig::mysql_cdc(&tbl)
            .source_url(&root_url)
            .checkpoint_path(ckpt.clone())
            .dest_path(dest.to_path_buf())
            .config_in(d.path())
    };

    // 1) Anchor with compression OFF — pins the checkpoint at the current coords,
    //    open-time guard passes cleanly.
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_rivet_ok(&cfg_into(&out1));

    // 2) Turn compression ON, then write a sizeable, highly-compressible
    //    transaction from a FRESH session (the session captures the global at
    //    connect) so MySQL packs it into a Transaction_payload_event in the binlog
    //    AHEAD of the checkpoint.
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = ON")
        .unwrap();
    let mut writer = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()).expect("writer");
    writer.query_drop("BEGIN").unwrap();
    for i in 1..=50 {
        writer
            .query_drop(format!(
                "INSERT INTO {tbl} VALUES ({i}, REPEAT('rivet-compressible-', 200))"
            ))
            .unwrap();
    }
    writer.query_drop("COMMIT").unwrap();

    // 3) Turn compression OFF so the RESUME's open-time guard passes too — the
    //    only thing that can catch the compressed span now is the stream-time arm.
    admin
        .query_drop("SET GLOBAL binlog_transaction_compression = OFF")
        .unwrap();
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let o = run_rivet(&["run", "--config", cfg_into(&out2).to_str().unwrap()]);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(
        !o.status.success(),
        "a compressed transaction in the stream must FAIL the resume, not silently \
         skip it and report success:\n{text}"
    );
    assert!(
        text.contains("Transaction_payload_event")
            && text.contains("binlog_transaction_compression"),
        "the stream-time refusal must name the event + the setting so it is actionable:\n{text}"
    );
    // The compressed rows were NOT captured (correct — refused, not skipped): the
    // checkpoint did not advance past them, so once compression is truly off they
    // re-read. The point of #200-2 is that rivet said so loudly instead of
    // shipping a _SUCCESS over a hole.
    assert!(
        !out2.join("_SUCCESS").exists(),
        "a refused resume must not write _SUCCESS"
    );
}

/// `rivet cdc --output --rollover N`: two invariants that had ZERO coverage on
/// this path, and both are the shapes that have already lost data here.
///
/// 1. A part NEVER splits a transaction. `rollover` is a soft target — the sink
///    rolls at a commit boundary — so a 3-row transaction under `--rollover 2`
///    must land as ONE part, not two. Splitting it is what makes a crash between
///    the two parts leave half a transaction durable.
/// 2. Two consecutive runs into the SAME `--output` directory must not clobber
///    each other. This is the class that cost real rows: run N+1's first part
///    overwrote run N's `cdc-000000.parquet` AFTER the position had advanced past
///    those changes, so the data was gone from the source log and the destination
///    both. The config path was fixed and regression-tested; the CLI path — which
///    takes `--output` as a bare directory and is the most natural way to point
///    two scheduled runs at one place — was never tested at all.
///
/// The oracle is DuckDB over the directory, never rivet's own manifest: the
/// manifest is exactly what a clobber leaves looking consistent.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn roast_mysql_cdc_cli_rollover_keeps_transactions_whole_and_two_runs_do_not_clobber() {
    require_alive(LiveService::DuckDb);
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_roll");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    write_checkpoint(&mut c, &ckpt); // anchor at NOW

    let ckpt_s = ckpt.to_str().unwrap().to_string();
    let out_s = out.to_str().unwrap().to_string();
    let tbl_q = tbl.clone();
    let sid = server_id_for(&tbl).to_string();
    let cdc_run = move || {
        run_rivet_env(
            &[
                "cdc",
                "--source",
                MYSQL_CDC_URL,
                "--server-id",
                &sid,
                "--table",
                &tbl_q,
                "--checkpoint",
                &ckpt_s,
                "--output",
                &out_s,
                "--rollover",
                "2",
            ],
            &[],
        )
    };

    // Run 1: ONE transaction of three rows, under a rollover of two.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3)"))
        .expect("tx1");
    let r1 = cdc_run();
    assert!(
        r1.status.success(),
        "cdc run 1 failed:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let parts_after_1 = files_with_extension(&out, "parquet").len();
    assert_eq!(
        duckdb_total_parquet_rows(&out),
        3,
        "run 1 must capture the whole transaction"
    );
    assert_eq!(
        parts_after_1, 1,
        "a 3-row transaction at --rollover 2 must land as ONE part: the sink rolls at a \
         COMMIT boundary, so splitting it into 2 parts means a crash between them leaves \
         half a transaction durable"
    );

    // Run 2: a second transaction into the SAME --output directory.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (4,4),(5,5),(6,6)"))
        .expect("tx2");
    let r2 = cdc_run();
    assert!(
        r2.status.success(),
        "cdc run 2 failed:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );

    // The id SET separates the two failures a file COUNT conflates. An inert
    // fixture (run 2 captured nothing) leaves {1,2,3}; a clobber leaves {4,5,6},
    // because run 2 really did capture and then wrote over run 1's part. Counting
    // files reports both as "no new part" and blames the fixture for the product.
    let ids = duckdb_dir_parquet_id_set(&out);
    let _ = parts_after_1;
    assert!(
        (4..=6).all(|i| ids.contains(&i)),
        "run 2 captured nothing of its own (ids {ids:?}) — the fixture is inert, so the \
         union below would pass without testing anything"
    );
    assert_eq!(
        ids,
        (1..=6).collect::<std::collections::BTreeSet<i64>>(),
        "both runs' rows must survive in one --output directory; a missing 1..=3 is the \
         clobber class (run 2's first part overwriting run 1's) AFTER the checkpoint \
         advanced past them — gone from the binlog and the destination both"
    );
}

/// `rivet cdc --output --format csv` — the WIRING, not the fidelity.
///
/// Scope, stated because it decides what this test is worth: CSV value rendering
/// and RFC-4180 escaping are NOT re-tested here. Both sinks call the same
/// `fmt.create_writer` (`pipeline/sink/mod.rs` and `source/cdc/sink.rs`), so the
/// text-writer class is a shared seam the `csv-fidelity-matrix` already pins on
/// the batch path; asserting it again here would grade the same code twice and
/// read like new coverage.
///
/// What is NOT shared is whether this combination runs at all. The CDC sink hands
/// the writer a schema the batch path never produces — the source columns PLUS
/// `__op`/`__pos`/`__seq` — and CSV cannot serialize every Arrow type (rivet has
/// `csv_serializable` in preflight for exactly that reason). `rivet cdc --output
/// --format csv` is documented, and had zero tests: nobody had ever run it.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn mysql_cdc_cli_csv_output_is_wired_and_readable() {
    require_alive(LiveService::DuckDb);
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_csv");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    write_checkpoint(&mut c, &ckpt);

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3)"))
        .expect("seed");
    // Explicit unique replica id: two parallel `rivet cdc` on the shared :3307
    // with the DEFAULT server-id collide (COM_REGISTER_SLAVE) — a pre-existing
    // CLI-path flake the r5 parallel run surfaced (the rig config path already
    // routes through server_id_for; the CLI path had no id at all).
    let sid = server_id_for(&tbl).to_string();
    let r = run_rivet_env(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--server-id",
            &sid,
            "--table",
            &tbl,
            "--checkpoint",
            ckpt.to_str().unwrap(),
            "--output",
            out.to_str().unwrap(),
            "--format",
            "csv",
        ],
        &[],
    );
    assert!(
        r.status.success(),
        "cdc --format csv failed — the CDC schema carries __op/__pos/__seq beside the \
         source columns, and a type CSV cannot serialize fails the whole run:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    // The format really is CSV, not parquet under a different name: a wiring that
    // maps "csv" to the wrong FormatType writes parquet and every row-count check
    // still passes.
    assert!(
        !files_with_extension(&out, "csv").is_empty(),
        "no .csv part written; directory holds {} parquet file(s)",
        files_with_extension(&out, "parquet").len()
    );
    // And it is readable by a reader that is not rivet.
    assert_eq!(
        duckdb_dir_csv_id_set(&out),
        (1..=3).collect::<std::collections::BTreeSet<i64>>(),
        "DuckDB must read back every captured id from the CSV parts"
    );
}

/// `rivet cdc --stream` and `--server-id`: the last two CLI flags with zero test
/// references anywhere in the tree.
///
/// `--stream` is not cosmetic — it flips `until_current` OFF, i.e. selects
/// `DrainMode::Continuous`, the one mode with no open-time bound to stop it. The
/// risk it carries is the opposite of the bounded run's: a continuous drain that
/// never terminates wedges a scheduler slot forever. Bounding it with
/// `--max-events` is the only way to assert on it at all, and that pairing is
/// itself the documented way to run a capped stream.
///
/// `--server-id` is MySQL's replica identity on the binlog connection. HONEST
/// LIMIT, stated because the test name would otherwise over-promise: this pins
/// only that a NON-DEFAULT value (919191, which no default produces) is accepted
/// and the run works with it — a parse/validation regression. It does NOT prove
/// the value reaches the dump request, and that is a measured conclusion rather
/// than an untried one: with a live `--stream` connected, MySQL showed the id
/// NOWHERE — `SHOW REPLICAS` is empty and `information_schema.PROCESSLIST` has no
/// `Binlog Dump` row, because the client sends the id in COM_BINLOG_DUMP without
/// registering via COM_REGISTER_SLAVE. There is no server-side view to assert
/// against; proving it would take a protocol-level capture, which is a different
/// test than this one.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn mysql_cdc_cli_stream_with_a_cap_terminates_and_accepts_a_server_id() {
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_stream");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ck");
    write_checkpoint(&mut c, &ckpt);
    // Two transactions, so the soft cap has a boundary to stop at that is NOT the
    // end of the stream — the same activation threshold the bounded-cap test needs.
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,1),(2,2),(3,3)"))
        .expect("tx1");
    c.query_drop(format!("INSERT INTO {tbl} VALUES (4,4),(5,5),(6,6)"))
        .expect("tx2");

    // A NON-DEFAULT server id (the default is 4271): if the flag were dropped on
    // the floor this run would still pass, so the value is deliberately one no
    // default would produce, and the run must succeed WITH it.
    let out = run_rivet_args_bounded(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--table",
            &tbl,
            "--checkpoint",
            ckpt.to_str().unwrap(),
            "--stream",
            "--max-events",
            "2",
            "--server-id",
            "919191",
        ],
        std::time::Duration::from_secs(60),
    );
    // `None` == the watchdog had to kill it. THAT is the assertion: a continuous
    // drain with a cap must end itself, not be ended.
    let stdout = out.expect(
        "`--stream --max-events` must TERMINATE on its own — a continuous drain that only \
         stops when the watchdog kills it wedges every scheduler slot it runs in",
    );

    let ids: std::collections::BTreeSet<i64> = stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("table").and_then(|t| t.as_str()) == Some(tbl.as_str()))
        .filter_map(|v| v.get("after")?.get(0)?.as_i64())
        .collect();
    // The cap is SOFT: it stops at the first commit boundary past N, so tx1 lands
    // whole and tx2 does not. Asserting the exact set pins both halves — that it
    // did not cut tx1 at 2, and that it did not run the stream to its end.
    assert_eq!(
        ids,
        (1..=3).collect::<std::collections::BTreeSet<i64>>(),
        "a cap of 2 must stop at tx1's boundary: {{1,2}} means it cut the transaction, \
         {{1..=6}} means the cap stopped nothing"
    );

    // And the half that actually proves `--stream` was HONOURED. Everything above
    // holds for a bounded run too — with a cap of 2 both modes stop at the same
    // boundary — so on its own it would pass with the flag dropped on the floor.
    //
    // The distinguishing property is termination: `--stream` selects
    // `DrainMode::Continuous`, which on MySQL is a BLOCKING binlog dump with no
    // open-time bound, so with no cap it must NOT end by itself. The default
    // bounded run does (its twin: roast_pg_cdc_ndjson_until_current_terminates).
    // `None` here means the watchdog had to kill it — which is the pass.
    let ckpt2 = d.path().join("ck2");
    write_checkpoint(&mut c, &ckpt2);
    let never = run_rivet_args_bounded(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--table",
            &tbl,
            "--checkpoint",
            ckpt2.to_str().unwrap(),
            "--stream",
            // A DISTINCT non-default id here too. Omitting it took the default
            // (4271), which every other concurrent `rivet cdc` also takes, and
            // MySQL kicks the older connection off when two replicas claim one
            // server_id — the run then exits non-zero and this reads as a
            // termination bug. Caught by the full suite at --test-threads=4;
            // green in isolation, which is exactly how a shared-identity fixture
            // hides.
            "--server-id",
            "919192",
        ],
        std::time::Duration::from_secs(15),
    );
    assert!(
        never.is_none(),
        "`--stream` with no cap must keep running until it is stopped — it terminated on \
         its own, which is the BOUNDED behaviour and means the flag never reached \
         `until_current` (dispatch.rs: `until_current: !stream`)"
    );
}

/// `rivet cdc --source-env` / `--source-file`, and the ArgGroup that keeps them
/// mutually exclusive.
///
/// These are the CREDENTIAL-SAFETY path: `--source` puts the URL (password and
/// all) in `ps` output, and rivet warns about the same shape in a config file.
/// The only reference to `--source-env` anywhere in the tree was for `rivet
/// init`, so the CDC subcommand's own resolution had never been exercised — and a
/// user whose `--source-env` silently failed would go straight back to the inline
/// form the warning exists to discourage.
///
/// The oracle is the SAME capture through all three forms: whatever the flag
/// does, it must resolve to one URL. Asserting only "exit 0" would pass on a
/// resolver that connected to something else entirely.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn mysql_cdc_cli_resolves_the_source_from_env_and_file_alike() {
    let mut c = conn();
    let tbl = unique_name("rivet_cdc_src");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let _t = MysqlCdcTable(tbl.clone());

    let d = tempfile::tempdir().unwrap();
    let url_file = d.path().join("url.txt");
    std::fs::write(&url_file, MYSQL_CDC_URL).expect("write url file");

    let capture = |form: &[&str], envs: &[(&str, &str)]| -> std::collections::BTreeSet<i64> {
        let ckpt = d
            .path()
            .join(format!("ck_{}", form.join("_").replace('/', "_")));
        write_checkpoint(&mut conn(), &ckpt);
        // Each form captures the SAME change, written after its own anchor.
        conn()
            .query_drop(format!("INSERT INTO {tbl} VALUES (7,7)"))
            .expect("seed");
        conn()
            .query_drop(format!("DELETE FROM {tbl} WHERE id = 7"))
            .expect("cleanup seed");
        let mut args: Vec<&str> = vec!["cdc"];
        args.extend_from_slice(form);
        let ck = ckpt.to_str().unwrap().to_string();
        args.extend_from_slice(&["--table", &tbl, "--checkpoint", &ck]);
        let out = run_rivet_args_bounded_env(&args, envs, std::time::Duration::from_secs(60))
            .unwrap_or_else(|| panic!("`rivet cdc {}` did not terminate", form.join(" ")));
        out.lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter(|v| v.get("table").and_then(|t| t.as_str()) == Some(tbl.as_str()))
            .filter_map(|v| v.get("after")?.get(0)?.as_i64())
            .collect()
    };

    let inline = capture(&["--source", MYSQL_CDC_URL], &[]);
    assert!(
        inline.contains(&7),
        "the inline form must capture the seeded change — the fixture is inert otherwise: {inline:?}"
    );
    let from_env = capture(
        &["--source-env", "RIVET_TEST_CDC_URL"],
        &[("RIVET_TEST_CDC_URL", MYSQL_CDC_URL)],
    );
    assert_eq!(
        from_env, inline,
        "`--source-env` must resolve to the same source as `--source`; exit 0 alone would \
         pass on a resolver that connected somewhere else"
    );
    let from_file = capture(&["--source-file", url_file.to_str().unwrap()], &[]);
    assert_eq!(
        from_file, inline,
        "`--source-file` must resolve to the same source as `--source`"
    );

    // The ArgGroup: exactly one form, never two. Without it a config that sets
    // both silently picks a winner, and which one is invisible to the operator.
    let both = run_rivet_env(
        &[
            "cdc",
            "--source",
            MYSQL_CDC_URL,
            "--source-env",
            "RIVET_TEST_CDC_URL",
            "--table",
            &tbl,
        ],
        &[("RIVET_TEST_CDC_URL", MYSQL_CDC_URL)],
    );
    assert!(
        !both.status.success(),
        "passing two source forms must be REFUSED, not silently resolved to one of them"
    );
}

/// A PARTIAL_JSON binlog must be REFUSED, not silently skipped.
///
/// `binlog_row_value_options=PARTIAL_JSON` is a legal MySQL 8 setting that logs a JSON
/// diff instead of the row image. The reader cannot decode that, and the arm that met
/// it was `_ => return Ok(true)` — the rows vanished while the surrounding transaction
/// committed and the checkpoint advanced past them. Unrecoverable, and reported as
/// `status: success`.
///
/// The loss is a SUBSET, which is what makes it the dangerous half: transactions that
/// touch no JSON column are captured normally, so counts and sums keep reconciling.
///
/// Non-default-state test (CLAUDE.md): the default is exactly where the bug hides, so
/// the setting is flipped and restored by a guard rather than assumed.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn mysql_cdc_refuses_a_partial_json_binlog_instead_of_dropping_the_update() {
    // PARTIAL_JSON is set on the WRITING SESSION, never the server global — the
    // binlog record's shape is decided by whoever wrote it. An earlier version of
    // this test flipped `SET GLOBAL` and took `quiet_window_guard()` to serialize;
    // that was the wrong shape twice over. The guard only excludes tests that TAKE
    // it, and `mysql_cdc_cli_stream_with_a_cap_terminates_and_accepts_a_server_id`
    // does not — so the global window was still visible to it and rivet, correctly,
    // refused its run too (measured on #281: it failed while 633 others passed,
    // both before and after the guard was added). Session scope removes the shared
    // state entirely: nothing to serialize, nothing to restore, no window for a
    // parallel test to fall into. Verified on the stand — the UPDATE below reaches
    // the partial arm while `@@GLOBAL.binlog_row_value_options` stays empty.
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_pj");
    // `SET SESSION binlog_row_value_options` needs SESSION_VARIABLES_ADMIN, which
    // the `rivet` test user does not hold — hence root for the WRITER only. The
    // capture itself still runs as `rivet`, exactly like every other test here.
    let root_url = MYSQL_CDC_URL.replace("rivet:rivet@", "root:rivet@");
    let mut writer = mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()).unwrap();
    use mysql::prelude::Queryable as _;
    writer
        .query_drop("SET SESSION binlog_row_value_options = 'PARTIAL_JSON'")
        .expect("set partial json on this session only");

    writer
        .query_drop(format!("DROP TABLE IF EXISTS {tbl}"))
        .unwrap();
    writer
        .query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, doc JSON)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    // The document must be big enough that MySQL logs a DIFF rather than falling back
    // to a full image — a short JSON value is rewritten whole and never reaches the
    // partial arm, which would make this test pass for the wrong reason.
    writer
        .query_drop(format!(
            "INSERT INTO {tbl} VALUES (1, JSON_OBJECT('a', 1, 'pad', REPEAT('x', 400)))"
        ))
        .unwrap();

    let ck = d.path().join("pj.ckpt");
    let rig = || {
        Rig::mysql_cdc(&tbl)
            .checkpoint_path(ck.to_path_buf())
            .dest_path(d.path().join("out"))
    };
    // Anchor the stream before provoking the diff, so the update is inside the window.
    rig().run_ok();

    // The diff-logging session writes the UPDATE — this is the whole fixture.
    writer
        .query_drop(format!(
            "UPDATE {tbl} SET doc = JSON_SET(doc, '$.a', 2) WHERE id = 1"
        ))
        .unwrap();

    let msg = rig().run_expect_fail();
    assert!(
        msg.contains("partial-JSON"),
        "the run must REFUSE and name the shape it met. Silently skipping the diff \
         loses the update while the checkpoint advances past it — a subset loss no \
         count check can see. Got: {msg}"
    );
    assert!(
        msg.contains("binlog_row_value_options"),
        "the message must name the SETTING to change — it is the only remediation an \
         operator gets: {msg}"
    );
    assert!(
        msg.contains("re-read"),
        "the message must say the un-acked span is re-read; an operator who believes \
         the data is already gone has no reason to fix the source and re-run: {msg}"
    );

    // ── and the refusal must be TABLE-ADDRESSED ────────────────────────────────
    //
    // The binlog is ONE stream per server, so the undecodable event above sits in
    // the log every OTHER export reads too. Refusing without asking whose event it
    // is turns one PARTIAL_JSON table into a server-wide outage — measured on #281,
    // where this fixture failed `mysql_cdc_cli_stream_with_a_cap_terminates_and_
    // accepts_a_server_id` with this exact error, on a table it does not capture.
    //
    // That failure was a RACE (the neighbour only sees it when its window covers
    // the event), which is why it is pinned HERE instead: this export is anchored
    // BEFORE the partial update, so the event is inside its window by construction
    // and the assertion cannot pass by timing. RED against `if true` in place of
    // the `undecodable_event_is_ours` call.
    let other = unique_name("cdc_pj_other");
    writer
        .query_drop(format!("DROP TABLE IF EXISTS {other}"))
        .unwrap();
    writer
        .query_drop(format!("CREATE TABLE {other} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _other_guard = Table(other.clone());
    let other_ck = d.path().join("other.ckpt");
    let other_rig = || {
        Rig::mysql_cdc(&other)
            .checkpoint_path(other_ck.to_path_buf())
            .dest_path(d.path().join("out_other"))
    };
    other_rig().run_ok(); // anchor BEFORE the partial update lands

    writer
        .query_drop(format!(
            "UPDATE {tbl} SET doc = JSON_SET(doc, '$.a', 3) WHERE id = 1"
        ))
        .unwrap();
    writer
        .query_drop(format!("INSERT INTO {other} VALUES (7, 70)"))
        .unwrap();

    let rows: usize = other_rig()
        .run_and_read()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        rows, 1,
        "an export that does NOT capture the partial-JSON table must complete and \
         capture its own change — the undecodable event belongs to another table, \
         and the routing filter would have dropped it anyway"
    );
}

/// A guard that returns `binlog_row_metadata` to whatever the stack pinned.
///
/// The variable is GLOBAL-only in MySQL 8 — there is no session scope to flip —
/// so a test that leaves it MINIMAL silently changes every later test's engine.
struct RowMetadata(String);

impl RowMetadata {
    fn set(to: &str) -> Self {
        let mut c = conn();
        let was: String = c
            .query_first("SELECT @@GLOBAL.binlog_row_metadata")
            .expect("read binlog_row_metadata")
            .expect("a value");
        c.query_drop(format!("SET GLOBAL binlog_row_metadata={to}"))
            .expect(
                "SET GLOBAL binlog_row_metadata needs SYSTEM_VARIABLES_ADMIN — see \
                 dev/cdc/mysql-grant.sql; without it MySQL's own default configuration is \
                 the one configuration nothing tests",
            );
        Self(was)
    }
}

impl Drop for RowMetadata {
    fn drop(&mut self) {
        if let Ok(mut c) = mysql::Pool::new(MYSQL_CDC_URL).and_then(|p| p.get_conn()) {
            let _ = c.query_drop(format!("SET GLOBAL binlog_row_metadata={}", self.0));
        }
    }
}

/// MySQL's DEFAULT `binlog_row_metadata=MINIMAL` maps binlog images POSITIONALLY,
/// and a same-arity column reorder across the resume boundary silently swaps them.
///
/// The stack pins `--binlog-row-metadata=FULL` (docker-compose), which puts column
/// NAMES into TABLE_MAP — so the sink takes the by-name arm and `continue`s past
/// the arity guard entirely (`sink.rs`, `if ev.image_names.is_some()`). MySQL's own
/// default is MINIMAL. Every live MySQL CDC test therefore runs the ONE
/// configuration a user does not have, and the positional path plus the guard that
/// protects it are reachable only from the default nobody exercises.
///
/// This is the measurement that decides how loud rivet should be about it. Under
/// MINIMAL: the schema is resolved at OPEN, the events replay from the CHECKPOINT
/// — so an `ALTER TABLE ... MODIFY b AFTER id` between the two puts a row written
/// as `(id, a, b)` into columns resolved as `(id, b, a)`.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (SYSTEM_VARIABLES_ADMIN)"]
fn mysql_cdc_minimal_row_metadata_is_the_engine_default_and_reorders_silently() {
    let _serial = cross_process_serial("mysql_cdc_row_metadata");
    let _meta = RowMetadata::set("MINIMAL");
    let table = unique_name("rivet_cdc_min");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9))"
    ))
    .unwrap();
    let _t = Table(table.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    let rig = Rig::mysql_cdc(&table).checkpoint_path(ckpt.clone());

    // Written under the OLD column order, read back under the new one.
    c.query_drop(format!("INSERT INTO {table} VALUES (1,'AAA','BBB')"))
        .unwrap();
    c.query_drop(format!(
        "ALTER TABLE {table} MODIFY COLUMN b VARCHAR(9) AFTER id"
    ))
    .unwrap();

    let out = rig.out_dir();
    let said = rig.run_ok_capture();

    // The corruption is REAL and this test does not pretend otherwise: rivet cannot
    // undo it after the fact, because under MINIMAL the wire carries no names to
    // detect the reorder from. What it CAN do — and now does — is say so before a
    // single event is read.
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&out, "a"),
        ["BBB".to_string()].into_iter().collect(),
        "MEASURED: positional mapping puts the OLD order's `b` into `a`. If this ever \
         reads AAA the engine learned to map by name under MINIMAL and the warning \
         below should go with it"
    );
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&out, "b"),
        ["AAA".to_string()].into_iter().collect(),
        "...and symmetrically the OLD order's `a` into `b`"
    );

    // The load-bearing half: the operator is TOLD, at warn level, at run start.
    assert!(
        said.contains("binlog_row_metadata") && said.contains("POSITION"),
        "a run that maps by position must say so — an operator who learns it from a \
         swapped column months later cannot act on it. Got:\n{said}"
    );
    assert!(
        said.contains("binlog_row_metadata = FULL"),
        "the warning must name the ESCAPE, not just the risk. Got:\n{said}"
    );
}

/// ...and under the FULL the stack pins, the same reorder is mapped BY NAME and
/// comes back correct, with no warning.
///
/// Without this half the test above proves only that a swap happens, not that the
/// setting is what causes it — and the warning would be free to fire on every run,
/// which is how a warning gets ignored.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (SYSTEM_VARIABLES_ADMIN)"]
fn mysql_cdc_full_row_metadata_maps_the_same_reorder_by_name_and_stays_quiet() {
    let _serial = cross_process_serial("mysql_cdc_row_metadata");
    let _meta = RowMetadata::set("FULL");
    let table = unique_name("rivet_cdc_full");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9))"
    ))
    .unwrap();
    let _t = Table(table.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    let rig = Rig::mysql_cdc(&table).checkpoint_path(ckpt.clone());

    c.query_drop(format!("INSERT INTO {table} VALUES (1,'AAA','BBB')"))
        .unwrap();
    c.query_drop(format!(
        "ALTER TABLE {table} MODIFY COLUMN b VARCHAR(9) AFTER id"
    ))
    .unwrap();

    let out = rig.out_dir();
    let said = rig.run_ok_capture();
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&out, "a"),
        ["AAA".to_string()].into_iter().collect(),
        "under FULL the image carries names, so the reorder maps correctly — this is \
         the assertion that makes FULL the documented escape rather than folklore"
    );
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&out, "b"),
        ["BBB".to_string()].into_iter().collect()
    );
    assert!(
        !said.contains("binlog_row_metadata"),
        "a correctly-configured server must stay quiet, or the warning becomes noise \
         on every run and stops being read. Got:\n{said}"
    );
}

/// PostgreSQL crash between the checkpoint write and the slot ack — DOCUMENTED,
/// not guarded, and the difference is the point.
///
/// This is the one CDC fault point with no PostgreSQL test, and the reason it had
/// none turns out to be structural rather than an oversight. Three mutants were
/// applied and this test stayed GREEN against all three:
///
///   1. `stream.ack` moved BEFORE the checkpoint write  — green
///   2. `stream.ack` moved BEFORE the parts are flushed — green
///   3. `ack` advancing the slot to `pg_current_wal_lsn()` instead of the last
///      commit                                          — green
///
/// Why: the crash fires BEFORE run 1's first ack, so every ack-side mutant is
/// unreachable in this fixture; and PostgreSQL's resume authority is the SLOT,
/// which run 1 therefore never moved. The checkpoint file is consulted only as a
/// boolean ("a prior run happened", `resume_expected` in cdc/mod.rs) and never as
/// a position, so the file racing ahead of the slot has nothing to act on.
/// Contrast SQL Server, where the checkpoint IS the only position and its ack is a
/// no-op — which is why that engine's test of this hook is load-bearing and this
/// one is not.
///
/// So the DELIVERY half of this test documents rather than guards — no reordering
/// of the sink's steps can make it red, and that is a fact about PostgreSQL's
/// design, not a weakness to paper over.
///
/// The RETENTION half is a real guard, and it is the half an operator cares about.
/// A crash here PINS WAL on the slot — it must, or the un-acked span is lost — and
/// the question is whether that is a transient or a leak. MEASURED: 2304 B pinned
/// by the crash, 0 after the resume. Neutering `ack` (`advance_slot` -> `Ok(())`)
/// leaves 2480 B pinned and this test goes RED, which is the mutant it grades.
///
/// That closes the crash-side of the slot-pinning class from the direction
/// `roast_pg_cdc_empty_transaction_churn_must_not_pin_the_slot` does not cover:
/// that one is about row-less spans starving the ack, this one is about a crash
/// leaving the ack unrun. Both end at the same place — `confirmed_flush_lsn`
/// frozen while WAL accumulates on an idle database.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn roast_pg_cdc_crash_between_checkpoint_and_ack_re_reads_and_releases_the_slot() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("rivet_cdc_pgckpt");
    let slot = unique_name("rivet_ckpt_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    // Separate transactions so several parts roll: the crash must land in the
    // window, and a single transaction would give it only one chance.
    for g in 0..12 {
        c.execute(&format!("INSERT INTO {tbl} VALUES ({g}, {g})"), &[])
            .unwrap();
    }

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .cdc("rollover: 3")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let crashed = rig.run_args_env(
        &[],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_checkpoint_before_ack")],
    );
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1 — a fault that did not fire leaves this \
         test asserting an ordinary two-run export"
    );

    // The slot's OWN anchor, not its distance from `pg_current_wal_lsn()`. The first
    // version measured that distance and failed in CI at 13 243 536 B where it read
    // 0 locally: the E2E runner drives the whole live suite against one PostgreSQL,
    // so unrelated WAL moves the reference point between the two reads and the
    // "distance" grows for reasons that have nothing to do with this slot. Comparing
    // the anchor to ITSELF is immune to that — the same unscoped-measurement class
    // as counting rows without scoping to the run.
    let confirmed = |c: &mut postgres::Client| -> String {
        c.query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .map(|r| r.get(0))
        .expect("the slot must exist after the crashed run")
    };
    let after_crash = confirmed(&mut c);

    // Run 2 into a FRESH destination, so what it delivers is its own doing: a
    // shared dir would let run 1's durable parts satisfy the oracle even if the
    // resume captured nothing (the CDC audit's headline finding).
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let rig2 = Rig::pg_cdc(&tbl, &slot)
        .checkpoint_path(ckpt.clone())
        .dest_path(out2.clone());
    run_rivet_ok(&rig2.config_path());

    // The union is what at-least-once promises. Overlap between the legs is fine
    // and expected — the un-acked span is re-read by construction.
    let mut got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    got.extend(duckdb_dir_parquet_i64(&out2, "id"));
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got, want,
        "every row must survive a crash between the checkpoint write and the slot \
         ack. A missing id means resume trusted the checkpoint FILE over the slot \
         and started past changes the slot had never released."
    );
    let after_resume = confirmed(&mut c);
    let advanced: i64 = c
        .query_one(
            &format!(
                "SELECT pg_wal_lsn_diff('{after_resume}'::pg_lsn, '{after_crash}'::pg_lsn)::bigint"
            ),
            &[],
        )
        .map(|r| r.get(0))
        .expect("compare the two anchor positions");

    // A crash here PINS WAL — it must, or the un-acked span is lost — so the
    // question is transient or leak. What proves it is the slot MOVING, measured
    // against where the crash left it: MEASURED 2304 B of WAL pinned by the crash
    // and released on resume, and neutering `advance_slot` to `Ok(())` leaves the
    // anchor exactly where it was (advanced = 0) and this goes RED.
    assert!(
        advanced > 0,
        "the resume must ADVANCE the slot past what it re-read: confirmed_flush_lsn \
         went {after_crash} -> {after_resume} ({advanced} B). Standing still means \
         the WAL the crash pinned is pinned for good, and an operator who \
         crash-loops fills the disk"
    );
    // And the second leg must really have done work — otherwise the assertion above
    // is satisfied by leg 1 alone and would pass against a resume that captured zero.
    assert!(
        !duckdb_dir_parquet_i64(&out2, "id").is_empty(),
        "the resume leg delivered NOTHING: the un-acked span was not re-read, so this \
         cell proves nothing about the boundary it exists for"
    );
}

/// A slot nobody is draining pins WAL forever, and `run` used to say nothing.
///
/// `rivet doctor` has caught this since it was written — it FAILS past 1 GiB and
/// names every offender. But doctor is a thing an operator runs when they already
/// suspect something; the scheduler runs `rivet run` every cycle and it was silent.
/// Measured on a dev stand: nine abandoned slots holding 1.5 GiB each, 1552 MiB of
/// WAL on disk, and every CDC run over that instance exited 0 without a word.
///
/// Two warnings, and the split is the point. Our OWN slot being far behind is worth
/// saying but the run does fix it — so the message says so, or an operator drops a
/// slot that was about to be drained. A FOREIGN inactive slot is pinned until a
/// human acts, which is the one that fills disks.
///
/// The threshold and the wording live in `preflight::cdc_health` and are shared with
/// doctor, so the two cannot drift; this test is about the WIRING — that a run
/// reaches them at all, and stays quiet when there is nothing to say.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn roast_pg_cdc_run_warns_about_an_abandoned_slot_pinning_wal() {
    use postgres::NoTls;
    let tbl = unique_name("rivet_cdc_slotwarn");
    let slot = unique_name("rivet_slotwarn").to_lowercase();
    let orphan = unique_name("rivet_orphan").to_lowercase();
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    for s in [&slot, &orphan] {
        c.execute(
            "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
            &[s],
        )
        .unwrap();
    }
    let _slot = Slot(slot.clone());
    let _orphan_guard = Slot(orphan.clone());
    c.execute(&format!("INSERT INTO {tbl} VALUES (1)"), &[])
        .unwrap();

    // Healthy first: two small slots, nothing to report. Asserted BEFORE the noisy
    // case because a warning that fires unconditionally would satisfy the positive
    // assertion below while being useless — and this is the half that catches it.
    let rig = Rig::pg_cdc(&tbl, &slot);
    let quiet = rig.run_ok_capture();
    // The capture itself must have worked. A diagnostic test that never checks its
    // run delivered anything would pass over a 0-row success — the conformance gate
    // asks this of every live CDC test, and it caught this one.
    assert_eq!(
        duckdb_dir_parquet_i64(&rig.out_dir(), "id"),
        vec![1],
        "the run must still CAPTURE while it warns — a warning path that broke the \
         export would be worse than the silence it replaced"
    );
    assert!(
        !quiet.to_lowercase().contains("pinning"),
        "a healthy instance must stay silent, or the message is noise on every \
         scheduler cycle and stops being read. Got:\n{quiet}"
    );

    // Now the loud case. Crossing a real GiB takes minutes of writes, so the bar
    // moves instead (`RIVET_TEST_SLOT_WAL_BAR`, a test seam beside the threshold —
    // deliberately not a config knob, since an operator lowering it would get a
    // warning on every ordinary backlog and learn to ignore it).
    let loud = Rig::pg_cdc(&tbl, &slot).run_with_env("RIVET_TEST_SLOT_WAL_BAR", "1");
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&loud.stdout),
        String::from_utf8_lossy(&loud.stderr)
    );
    assert!(
        loud.status.success(),
        "a retention WARNING must never fail the run — the export is the thing that \
         drains the slot, so refusing here would make the problem permanent:\n{said}"
    );
    assert!(
        said.contains(&orphan),
        "the run must name the ABANDONED slot — it is the one nothing is draining, \
         and an operator cannot drop what they were not told about. Got:\n{said}"
    );
    assert!(
        said.contains("pg_drop_replication_slot"),
        "and hand over the command, not just the diagnosis. Got:\n{said}"
    );
    assert!(
        said.contains(&slot) && said.contains("This run will drain it"),
        "our OWN slot gets the other message — saying it will be drained, so nobody \
         drops a slot that was about to recover. Got:\n{said}"
    );
}

/// The TRUNCATE refusal must not throw away what it has already read.
///
/// The bail used to fire the instant the truncate line was scanned — before the
/// window's earlier, fully-committed transactions reached the sink. The run failed
/// with `rows: 0`, so nothing flushed and nothing acked, and the slot had not moved.
/// The refusal's own remedy then finished the job: `pg_replication_slot_advance`
/// past the truncate discards every one of them.
///
/// MEASURED before the fix: a two-table export where `public.tb` committed an
/// insert BEFORE `public.ta` was truncated lost that insert for good when the
/// remedy was followed verbatim — on a table that was never truncated, and which
/// the message does not mention.
///
/// So the truncate now ENDS the window (`exhausted`) and the refusal is raised on
/// the next fill, after the sink has flushed, checkpointed and acked. What this
/// asserts is the property that makes the remedy honest: the run still FAILS, the
/// preceding rows are DELIVERED, and the slot is left holding nothing but the
/// truncate itself.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn roast_pg_cdc_truncate_refusal_delivers_the_rows_it_already_read() {
    use postgres::NoTls;
    let ta = unique_name("rivet_cdc_trka").to_lowercase();
    let tb = unique_name("rivet_cdc_trkb").to_lowercase();
    let slot = unique_name("rivet_trk_slot").to_lowercase();
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {ta}; DROP TABLE IF EXISTS {tb}; \
         CREATE TABLE {ta}(id int PRIMARY KEY, v text); \
         CREATE TABLE {tb}(id int PRIMARY KEY, v text)"
    ))
    .unwrap();
    let _ta = PgTable::adopt_on(POSTGRES_CDC_URL, ta.clone());
    let _tb = PgTable::adopt_on(POSTGRES_CDC_URL, tb.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    // The bystander commits FIRST, in its own transaction, and is never truncated.
    c.execute(
        &format!("INSERT INTO {tb} VALUES (1,'B_MUST_SURVIVE')"),
        &[],
    )
    .unwrap();
    c.execute(&format!("INSERT INTO {ta} VALUES (1,'a1')"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {ta}"), &[]).unwrap();

    let rig = Rig::pg_cdc(&format!("public.{ta}"), &slot)
        .tables(&[&format!("public.{ta}"), &format!("public.{tb}")]);
    let said = rig.run_expect_fail();
    assert!(
        said.to_lowercase().contains("truncate"),
        "the run must still REFUSE — deferring the bail must not turn it into a \
         silent success. Got:\n{said}"
    );

    // ...and the rows it had already read must be AT the destination, not discarded
    // with the error.
    // A multi-table export writes one sub-directory PER TABLE, so the bystander's
    // rows are under `public.<tb>/` — reading the top level finds nothing and would
    // make this assertion fail for a harness reason rather than a product one.
    let got: std::collections::BTreeSet<String> =
        duckdb_dir_parquet_distinct_strings(&rig.out_dir().join(format!("public.{tb}")), "v");
    assert!(
        got.contains("B_MUST_SURVIVE"),
        "the bystander's insert committed BEFORE the truncate and must be delivered \
         — the remedy advances the slot past this point, so anything not delivered \
         here is lost for good. Got: {got:?}"
    );

    // The property that makes the remedy honest: nothing but the truncate is left.
    let remaining: Vec<String> = c
        .query(
            "SELECT data FROM pg_logical_slot_peek_changes($1, NULL, NULL)",
            &[&slot],
        )
        .expect("peek the slot")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .filter(|d| !d.starts_with("BEGIN") && !d.starts_with("COMMIT"))
        .collect();
    assert_eq!(
        remaining.len(),
        1,
        "the slot must hold ONLY the truncate — anything else still in it is what \
         `pg_replication_slot_advance` would destroy. Got: {remaining:?}"
    );
    assert!(
        remaining[0].contains("TRUNCATE"),
        "and that one thing is the truncate itself: {remaining:?}"
    );
}

/// The metadata warning must come from the WIRE, not from the server's setting.
///
/// `row_metadata_warning` asks `@@global.binlog_row_metadata` at open. The events
/// a run drains replay whatever was in force when they were WRITTEN — so a server
/// switched to FULL yesterday still reads a MINIMAL backlog positionally, and the
/// probe, asked about the present, says nothing.
///
/// MEASURED before the fix: anchor under FULL, one row written under MINIMAL,
/// server back to FULL, then a same-arity `MODIFY .. AFTER` — the parquet came back
/// `a='BBB', b='AAA'`, swapped, `status: success`, and ZERO warnings. The probe was
/// not wrong about the variable; it was answering the wrong question.
///
/// The sink now warns when it takes the nameless arm, once per table, which is the
/// only place that knows for certain.
#[test]
#[ignore = "live: requires docker compose mysql-cdc (SYSTEM_VARIABLES_ADMIN)"]
fn roast_mysql_cdc_warns_on_a_minimal_backlog_a_full_server_would_hide() {
    let _serial = cross_process_serial("mysql_cdc_row_metadata");
    let _meta = RowMetadata::set("FULL");
    let table = unique_name("rivet_cdc_wire").to_lowercase();
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table}(id INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9))"
    ))
    .unwrap();
    let _t = Table(table.clone());

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    write_checkpoint(&mut c, &ckpt);
    let rig = Rig::mysql_cdc(&table).checkpoint_path(ckpt.clone());

    // Written under MINIMAL — the TABLE_MAP for THIS event carries no names.
    c.query_drop("SET GLOBAL binlog_row_metadata=MINIMAL")
        .unwrap();
    c.query_drop(format!("INSERT INTO {table} VALUES (1,'AAA','BBB')"))
        .unwrap();
    // ...and the server is healthy again by the time the run opens, which is
    // exactly what made the open-time probe silent.
    c.query_drop("SET GLOBAL binlog_row_metadata=FULL").unwrap();
    c.query_drop(format!(
        "ALTER TABLE {table} MODIFY COLUMN b VARCHAR(9) AFTER id"
    ))
    .unwrap();
    let now: String = c
        .query_first("SELECT @@GLOBAL.binlog_row_metadata")
        .unwrap()
        .unwrap();
    assert_eq!(
        now, "FULL",
        "the fixture is inert unless the server LOOKS healthy at open — that is the \
         whole point of this cell"
    );

    let said = rig.run_ok_capture();
    assert!(
        said.contains("mapped by POSITION"),
        "a nameless image must be announced from the WIRE — the server's current \
         setting cannot see a backlog written under the old one. Got:\n{said}"
    );
    assert!(
        said.contains("binlog_row_metadata = FULL"),
        "and it must still name the escape: {said}"
    );
}
