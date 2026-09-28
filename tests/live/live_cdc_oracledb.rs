//! Live Oracle CDC (LogMiner, ADR-0037) through the Rig.
//!
//! Gated `#[ignore]`: needs the `oracle` service with the LogMiner prerequisites of
//! `dev/oracle/init/02-logminer.sh` (ARCHIVELOG, minimal supplemental logging, the
//! common user C##RIVETCDC). The independent oracle is the source itself — the ids
//! and values each test wrote, or Oracle's own reading of the table — compared with
//! the parquet the run declared.

use std::path::Path;

use crate::common::*;

/// A `RIVET` table the capture user can read, logged with ALL columns (a whole row per change).
fn cdc_table(prefix: &str, columns: &str) -> OracleTable {
    let t = OracleTable::create(prefix, columns);
    ora_exec(&format!(
        "ALTER TABLE {} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        t.name()
    ));
    ora_exec(&format!("GRANT SELECT ON {} TO c##rivetcdc", t.name()));
    t
}

fn rig(t: &OracleTable, ckpt: &Path, out: &Path) -> Rig {
    std::fs::create_dir_all(out).unwrap();
    Rig::oracle_cdc(t.name())
        .checkpoint_path(ckpt.to_path_buf())
        .dest_path(out.to_path_buf())
}

/// Run several statements in ONE transaction on one connection.
fn ora_tx(stmts: &[String]) {
    let conn = ora_conn();
    for s in stmts {
        conn.execute(s, &[])
            .unwrap_or_else(|e| panic!("{s}: {e:?}"));
    }
    conn.commit().unwrap();
}

fn ops(v: &[(i64, &str)]) -> Vec<(i64, String)> {
    v.iter().map(|(i, o)| (*i, o.to_string())).collect()
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_resume_captures_only_new_changes() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cres", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    ora_exec(&format!("INSERT INTO {} VALUES (1, 10)", t.name()));
    ora_exec(&format!("INSERT INTO {} VALUES (2, 20)", t.name()));
    let out1 = d.path().join("out1");
    rig(&t, &ckpt, &out1).run_ok();
    assert_eq!(cdc_id_ops(&out1), ops(&[(1, "insert"), (2, "insert")]));

    ora_exec(&format!("INSERT INTO {} VALUES (3, 30)", t.name()));
    ora_exec(&format!("UPDATE {} SET v = 11 WHERE id = 1", t.name()));
    let out2 = d.path().join("out2");
    rig(&t, &ckpt, &out2).run_ok();
    assert_eq!(
        cdc_id_ops(&out2),
        ops(&[(1, "update"), (3, "insert")]),
        "run 2 captures only the changes after run 1's checkpoint"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_idle_first_run_then_change_is_captured() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cidle", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    rig(&t, &ckpt, &out1).run_ok();
    assert!(
        cdc_id_ops(&out1).is_empty(),
        "the idle first run captures nothing"
    );
    assert!(ckpt.exists(), "the idle first run leaves an anchor");

    ora_exec(&format!("INSERT INTO {} VALUES (1, 10)", t.name()));
    let out2 = d.path().join("out2");
    rig(&t, &ckpt, &out2).run_ok();
    assert_eq!(
        cdc_id_ops(&out2),
        ops(&[(1, "insert")]),
        "a change after an idle run is captured, never skipped to 'now'"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_transaction_open_across_a_run_keeps_its_early_rows() {
    // ADR-0037 OR9: a transaction that wrote before a run and commits after it must
    // arrive whole on the next run. Mining from the last commit alone loses the
    // early row (measured); the checkpoint's low-water mark is what keeps it.
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cspan", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();

    let open = ora_conn();
    open.execute(&format!("INSERT INTO {} VALUES (1, 10)", t.name()), &[])
        .unwrap();
    ora_exec(&format!("INSERT INTO {} VALUES (2, 20)", t.name()));
    let out1 = d.path().join("out1");
    rig(&t, &ckpt, &out1).run_ok();
    assert_eq!(
        cdc_id_ops(&out1),
        ops(&[(2, "insert")]),
        "only the committed row"
    );

    open.execute(&format!("INSERT INTO {} VALUES (3, 30)", t.name()), &[])
        .unwrap();
    open.commit().unwrap();
    let out2 = d.path().join("out2");
    rig(&t, &ckpt, &out2).run_ok();
    assert_eq!(
        cdc_id_ops(&out2),
        ops(&[(1, "insert"), (3, "insert")]),
        "the spanning transaction arrives whole, its pre-run row included"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_intra_transaction_updates_get_distinct_seq() {
    let _serial = cross_process_serial("oracle_cdc");
    const N: i64 = 50;
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cseq", "id NUMBER(18) PRIMARY KEY, counter NUMBER(18)");
    ora_exec(&format!("INSERT INTO {} VALUES (1, 0)", t.name()));
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    ora_tx(
        &(1..=N)
            .map(|i| format!("UPDATE {} SET counter = {i} WHERE id = 1", t.name()))
            .collect::<Vec<_>>(),
    );
    let out = d.path().join("out");
    rig(&t, &ckpt, &out).run_ok();
    assert_intra_transaction_seq(&out, N);
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_sum_reconciles_across_intra_txn_updates() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table(
        "ora_csum",
        "id NUMBER(18) PRIMARY KEY, v NUMBER(18) NOT NULL",
    );
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    for txn in cdc_sum_workload(t.name()) {
        ora_tx(&txn);
    }
    let source_sum: i64 =
        ora_text_rows(&format!("SELECT TO_CHAR(NVL(SUM(v), 0)) FROM {}", t.name()))[0][0]
            .as_deref()
            .unwrap()
            .parse()
            .unwrap();
    let out = d.path().join("out");
    rig(&t, &ckpt, &out).run_ok();
    let changes = read_cdc_changes(&out);
    assert!(
        intra_txn_multi_change_count(&changes) > 0,
        "the workload must touch one key several times per transaction"
    );
    assert_eq!(
        deduped_current_sum(changes, CdcEngine::Oracle),
        source_sum,
        "deduped by (__pos, __seq), the capture sums to the source"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_crash_after_flush_before_ack() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_ccrash", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    ora_exec(&format!("INSERT INTO {} VALUES (1, 10)", t.name()));
    let out1 = d.path().join("out1");
    rig(&t, &ckpt, &out1).run_ok();

    ora_exec(&format!("INSERT INTO {} VALUES (2, 20)", t.name()));
    let crashed = rig(&t, &ckpt, &d.path().join("crash"))
        .run_with_envs(&[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash fails the run"
    );
    let out2 = d.path().join("out2");
    rig(&t, &ckpt, &out2).run_ok();
    assert_eq!(
        cdc_id_ops(&out2),
        ops(&[(2, "insert")]),
        "a crash before the checkpoint re-reads the un-acked change, and only it"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn roast_oracle_cdc_large_transaction_is_atomic_across_a_mid_flush_crash() {
    // RED against `committed` on every event: at rollover 5 a 12-row transaction
    // would roll and checkpoint at its own commit after 5 rows, and a crash there
    // would let the resume skip the other 7.
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cbig", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    ora_tx(
        &(1..=12)
            .map(|i| format!("INSERT INTO {} VALUES ({i}, {i})", t.name()))
            .collect::<Vec<_>>(),
    );
    let crash = d.path().join("crash");
    let crashed = rig(&t, &ckpt, &crash)
        .cdc("rollover: 5")
        .run_with_envs(&[("RIVET_TEST_PANIC_AT", "cdc_after_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash fails the run"
    );
    let out2 = d.path().join("out2");
    rig(&t, &ckpt, &out2).cdc("rollover: 5").run_ok();
    let mut ids: Vec<i64> = cdc_id_ops(&crash)
        .into_iter()
        .chain(cdc_id_ops(&out2))
        .map(|(i, _)| i)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(
        ids,
        (1..=12).collect::<Vec<_>>(),
        "no row of the transaction is lost"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_update_and_delete_carry_full_types() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table(
        "ora_cupd",
        "id NUMBER(18) PRIMARY KEY, v NUMBER(18), amount NUMBER(12,2), note VARCHAR2(20)",
    );
    ora_exec(&format!(
        "INSERT INTO {} VALUES (1, 10, 1.25, 'keep')",
        t.name()
    ));
    ora_exec(&format!(
        "INSERT INTO {} VALUES (2, 20, 2.50, 'gone')",
        t.name()
    ));
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    ora_exec(&format!("UPDATE {} SET v = 11 WHERE id = 1", t.name()));
    ora_exec(&format!("DELETE FROM {} WHERE id = 2", t.name()));
    let out = d.path().join("out");
    rig(&t, &ckpt, &out).run_ok();
    assert_eq!(cdc_id_ops(&out), ops(&[(1, "update"), (2, "delete")]));
    let rows = duckdb_run_sql_json(&format!(
        "SELECT ID, __op, CAST(AMOUNT AS VARCHAR), NOTE FROM read_parquet('{}/*.parquet') \
         ORDER BY 1",
        stage_for_duckdb(&out)
    ));
    let expect = serde_json::json!([
        ["1", "update", "1.25", "keep"],
        ["2", "delete", "2.50", "gone"]
    ]);
    assert_eq!(
        rows["rows"], expect,
        "an UPDATE of one column still carries the untouched ones, and a DELETE the whole row"
    );
}

/// The types the preview captures, with edge values.
fn type_table() -> OracleTable {
    cdc_table(
        "ora_ctypes",
        "id NUMBER(10) PRIMARY KEY, n_bare NUMBER, n_dec NUMBER(38,10), n_int NUMBER(9), \
         n_big NUMBER(18), bf BINARY_FLOAT, bd BINARY_DOUBLE, d DATE, ts TIMESTAMP(6), \
         ts9 TIMESTAMP(9), tstz TIMESTAMP(6) WITH TIME ZONE, \
         tsltz TIMESTAMP(6) WITH LOCAL TIME ZONE, vc VARCHAR2(100 CHAR), nvc NVARCHAR2(50), \
         ch CHAR(5 CHAR), rw RAW(16)",
    )
}

fn seed_types(table: &str) {
    for row in [
        "1, 123.45, 1.5, 42, 9007199254740993, 1.5, 2.25, \
         TO_DATE('2024-02-29 13:14:15','YYYY-MM-DD HH24:MI:SS'), \
         TO_TIMESTAMP('2024-02-29 13:14:15.123456','YYYY-MM-DD HH24:MI:SS.FF'), \
         TO_TIMESTAMP('2024-02-29 13:14:15.123456789','YYYY-MM-DD HH24:MI:SS.FF'), \
         TO_TIMESTAMP_TZ('2024-02-29 10:00:00.5 +02:00','YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'), \
         TO_TIMESTAMP_TZ('2024-02-29 10:00:00 -03:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'), \
         'it''s, \"q\"', N'unicode ✓', 'ab', HEXTORAW('00FF10')",
        "2, 12345678901234567890.123456789, 1234567890123456789012345678.0123456789, \
         -999999999, -999999999999999999, BINARY_FLOAT_NAN, BINARY_DOUBLE_INFINITY, \
         TO_DATE('0001-01-01','YYYY-MM-DD'), \
         TO_TIMESTAMP('9999-12-31 23:59:59.999999','YYYY-MM-DD HH24:MI:SS.FF'), NULL, \
         TO_TIMESTAMP_TZ('2024-07-01 10:00:00 Europe/Berlin','YYYY-MM-DD HH24:MI:SS TZR'), \
         NULL, '   ', NULL, 'x', HEXTORAW('01')",
        "3, 1E125, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', N'日本語', \
         NULL, NULL",
        "4, -0.000000000000000000000000000000000000001, 0.5, 0, 0, -0.1, 0.1, NULL, NULL, \
         NULL, NULL, NULL, NULL, NULL, NULL, NULL",
    ] {
        ora_exec(&format!("INSERT INTO {table} VALUES ({row})"));
    }
}

/// SQL for every column of the parquet under `dir` (CDC meta columns dropped when `cdc`), by `ID`.
fn cells_sql(dir: &Path, cdc: bool) -> String {
    let cols = if cdc {
        "* EXCLUDE (__op, __pos, __seq)"
    } else {
        "*"
    };
    format!(
        "SELECT {cols} FROM read_parquet('{}/*.parquet') ORDER BY ID",
        stage_for_duckdb(dir)
    )
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_full_type_matrix_matches_batch() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = type_table();
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    seed_types(t.name());
    let cdc_out = d.path().join("cdc");
    rig(&t, &ckpt, &cdc_out).run_ok();
    let batch = Rig::oracle_batch(t.name());
    batch.run_ok();
    let cdc = duckdb_run_sql_json(&cells_sql(&cdc_out, true));
    let batch = duckdb_run_sql_json(&cells_sql(&batch.out_dir(), false));
    assert_eq!(
        cdc["rows"].as_array().map(Vec::len),
        Some(4),
        "every seeded row is captured"
    );
    assert_eq!(
        (&cdc["columns"], &cdc["rows"]),
        (&batch["columns"], &batch["rows"]),
        "each captured value equals the batch export's"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_non_utc_session_matches_batch() {
    // The capture user's own logon sets every rendering knob off its default; the
    // adapter's session pin must win, or dates, numbers and zones decode wrong.
    let _serial = cross_process_serial("oracle_cdc");
    struct Trigger;
    impl Drop for Trigger {
        fn drop(&mut self) {
            let _ = ora_system_conn().execute("DROP TRIGGER system.rivet_cdc_odd_session", &[]);
        }
    }
    ora_system_exec(
        "CREATE OR REPLACE TRIGGER system.rivet_cdc_odd_session AFTER LOGON ON c##rivetcdc.SCHEMA \
         BEGIN \
           EXECUTE IMMEDIATE q'[ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-RR']'; \
           EXECUTE IMMEDIATE q'[ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'DD-MON-RR HH.MI.SSXFF AM']'; \
           EXECUTE IMMEDIATE q'[ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'DD-MON-RR HH.MI.SSXFF AM TZR']'; \
           EXECUTE IMMEDIATE q'[ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ',.']'; \
           EXECUTE IMMEDIATE q'[ALTER SESSION SET TIME_ZONE = 'Asia/Tokyo']'; \
         END;",
    );
    let _trigger = Trigger;
    let d = tempfile::tempdir().unwrap();
    let t = type_table();
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    seed_types(t.name());
    let cdc_out = d.path().join("cdc");
    rig(&t, &ckpt, &cdc_out).run_ok();
    let batch = Rig::oracle_batch(t.name());
    batch.run_ok();
    let cdc = duckdb_run_sql_json(&cells_sql(&cdc_out, true));
    let batch = duckdb_run_sql_json(&cells_sql(&batch.out_dir(), false));
    assert_eq!(
        cdc["rows"].as_array().map(Vec::len),
        Some(4),
        "every seeded row is captured"
    );
    assert_eq!(
        (&cdc["columns"], &cdc["rows"]),
        (&batch["columns"], &batch["rows"]),
        "an odd capture session decodes every value exactly as the batch export"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_initial_snapshot_covers_preexisting_rows() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = cdc_table("ora_csnap", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    ora_exec(&format!("INSERT INTO {} VALUES (1, 10)", t.name()));
    ora_exec(&format!("INSERT INTO {} VALUES (2, 20)", t.name()));
    let rig = Rig::oracle_cdc(t.name()).cdc("initial: snapshot");
    let out = rig.out_dir();
    rig.run_ok();
    assert_eq!(
        duckdb_dir_parquet_id_set(&out.join("snapshot"))
            .into_iter()
            .collect::<Vec<i64>>(),
        vec![1, 2],
        "the snapshot holds exactly the pre-existing rows"
    );
    ora_exec(&format!("INSERT INTO {} VALUES (3, 30)", t.name()));
    rig.run_ok();
    assert_eq!(
        cdc_id_ops(&out),
        ops(&[(3, "insert")]),
        "the post-snapshot change streams"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_mixed_transaction_ending_on_uncaptured_table() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let orders = cdc_table("ora_cmixo", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let audit = cdc_table("ora_cmixa", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    rig(&orders, &ckpt, &d.path().join("anchor")).run_ok();
    ora_tx(&[
        format!("INSERT INTO {} VALUES (1, 10)", orders.name()),
        format!("INSERT INTO {} VALUES (1, 99)", audit.name()),
    ]);
    let out1 = d.path().join("out1");
    rig(&orders, &ckpt, &out1).run_ok();
    assert_eq!(
        cdc_id_ops(&out1),
        ops(&[(1, "insert")]),
        "exactly the orders row"
    );
    let out2 = d.path().join("out2");
    rig(&orders, &ckpt, &out2).run_ok();
    assert!(
        cdc_id_ops(&out2).is_empty(),
        "the mixed transaction is not re-read"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_schema_qualified_table_config_captures_events() {
    // A lower-case `rivet.<table>` resolves like Oracle does (upper-case) and routes.
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cq", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    let lower = format!("rivet.{}", t.name().to_lowercase());
    let r = |out: &Path| rig(&t, &ckpt, out).tables(&[lower.as_str()]);
    r(&d.path().join("anchor")).run_ok();
    ora_exec(&format!("INSERT INTO {} VALUES (7, 70)", t.name()));
    let out = d.path().join("out");
    r(&out).run_ok();
    assert_eq!(cdc_id_ops(&out), ops(&[(7, "insert")]));
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_corrupt_checkpoint_fails_loud_not_silently_absent() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cbad", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::write(&ckpt, "{\"low_water\": \"12").unwrap();
    let err = rig(&t, &ckpt, &d.path().join("out")).run_expect_fail();
    assert!(err.contains("corrupt or truncated"), "{err}");
}

/// The checkpoint with `edit` applied to its JSON.
fn rewrite_checkpoint(ckpt: &Path, edit: impl FnOnce(&mut serde_json::Value)) {
    let mut v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(ckpt).unwrap()).unwrap();
    edit(&mut v);
    std::fs::write(ckpt, v.to_string()).unwrap();
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_resume_past_log_retention_fails_loudly() {
    // A checkpoint whose low-water mark predates every redo log still on disk is
    // the state an archive purge leaves behind: refuse it as a loss, never re-anchor.
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cgone", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    rewrite_checkpoint(&ckpt, |v| v["low_water"] = "1".into());
    let err = rig(&t, &ckpt, &d.path().join("out")).run_expect_fail();
    assert!(err.contains("LOST to this stream"), "{err}");
    assert!(err.contains("anchors afresh FIRST"), "{err}");
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_checkpoint_from_another_database_is_refused() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_cdbid", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    rewrite_checkpoint(&ckpt, |v| v["dbid"] = "1".into());
    let err = rig(&t, &ckpt, &d.path().join("out")).run_expect_fail();
    assert!(err.contains("written against another database"), "{err}");
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_table_without_all_column_logging_is_refused() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = OracleTable::create("ora_cnolog", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    ora_exec(&format!("GRANT SELECT ON {} TO c##rivetcdc", t.name()));
    let err = rig(&t, &d.path().join("cdc.ckpt"), &d.path().join("out")).run_expect_fail();
    assert!(
        err.contains("ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"),
        "the refusal names the statement that fixes it: {err}"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_a_lob_column_is_refused_by_name() {
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_clob", "id NUMBER(18) PRIMARY KEY, body CLOB");
    let err = rig(&t, &d.path().join("cdc.ckpt"), &d.path().join("out")).run_expect_fail();
    assert!(err.contains("BODY CLOB"), "{err}");
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_cli_resolves_the_source_from_env_and_file_alike() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = cdc_table("ora_ccli", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let table = format!("RIVET.{}", t.name());
    let d = tempfile::tempdir().unwrap();
    let url_file = d.path().join("url.txt");
    std::fs::write(&url_file, ORACLE_CDC_URL).unwrap();
    let capture = |form: &[&str], envs: &[(&str, &str)], id: i64| {
        let ck = d.path().join(format!("ck_{id}"));
        let ck = ck.to_str().unwrap().to_string();
        let mut args: Vec<&str> = vec!["cdc"];
        args.extend_from_slice(form);
        args.extend_from_slice(&["--table", &table, "--checkpoint", &ck]);
        let anchor = run_rivet_args_bounded_env(&args, envs, std::time::Duration::from_secs(90));
        assert!(anchor.is_some(), "the anchoring run did not terminate");
        ora_exec(&format!("INSERT INTO {} VALUES ({id}, {id})", t.name()));
        let out = run_rivet_args_bounded_env(&args, envs, std::time::Duration::from_secs(90))
            .unwrap_or_else(|| panic!("`rivet cdc {}` did not terminate", form.join(" ")));
        out.lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter(|v| v.get("table").and_then(|x| x.as_str()) == Some(t.name()))
            .filter_map(|v| v.get("after")?.get(0)?.as_i64())
            .collect::<std::collections::BTreeSet<i64>>()
    };
    let inline = capture(&["--source", ORACLE_CDC_URL], &[], 1);
    assert_eq!(inline, [1].into(), "the inline form captures its change");
    let from_env = capture(
        &["--source-env", "RIVET_TEST_ORA_CDC_URL"],
        &[("RIVET_TEST_ORA_CDC_URL", ORACLE_CDC_URL)],
        2,
    );
    assert_eq!(
        from_env,
        [2].into(),
        "`--source-env` resolves to the same source"
    );
    let from_file = capture(&["--source-file", url_file.to_str().unwrap()], &[], 3);
    assert_eq!(
        from_file,
        [3].into(),
        "`--source-file` resolves to the same source"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_cli_writes_csv_parts_and_stops_at_the_cap_on_a_commit() {
    // `--format csv` is read back by DuckDB's own CSV parser; `--max-events 2` over three
    // single-row commits must deliver two now and the third on the next run, never lose it;
    // `--rollover 1` must cut one part per commit; `--stream` is refused, not silently bounded.
    let _serial = cross_process_serial("oracle_cdc");
    let t = cdc_table("ora_cclicsv", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let table = format!("RIVET.{}", t.name());
    let d = tempfile::tempdir().unwrap();
    let ck = d.path().join("ck").to_str().unwrap().to_string();
    let (host, container) = duckdb_shared_workdir(&unique_name("ora_cclicsv_out"));
    let out = host.to_str().unwrap().to_string();
    let args = |extra: &[&'static str]| {
        let mut a: Vec<String> = ["cdc", "--source", ORACLE_CDC_URL, "--table", &table]
            .iter()
            .map(|s| s.to_string())
            .collect();
        a.extend(["--checkpoint", &ck, "--output", &out, "--format", "csv"].map(String::from));
        a.extend(extra.iter().map(|s| s.to_string()));
        a
    };
    let run = |extra: &[&'static str]| {
        let a = args(extra);
        let refs: Vec<&str> = a.iter().map(String::as_str).collect();
        run_rivet_args_bounded_env(&refs, &[], std::time::Duration::from_secs(90))
    };
    assert!(run(&[]).is_some(), "the anchoring run did not terminate");
    for id in 1..=3 {
        ora_exec(&format!("INSERT INTO {} VALUES ({id}, {id})", t.name()));
    }
    let a = args(&["--stream"]);
    let refs: Vec<&str> = a.iter().map(String::as_str).collect();
    let refused = run_rivet(&refs);
    let err = String::from_utf8_lossy(&refused.stderr);
    assert!(
        !refused.status.success() && err.contains("Oracle CDC is always a bounded drain"),
        "--stream on Oracle must refuse, not run a bounded drain it did not ask for: {err}"
    );
    assert!(
        run(&["--max-events", "2", "--rollover", "1"]).is_some(),
        "a capped run did not terminate"
    );
    let ids = |_: ()| -> Vec<i64> {
        let v = duckdb_run_sql_json(&format!(
            "SELECT DISTINCT CAST(ID AS BIGINT) FROM read_csv_auto('{container}/**/*.csv', \
             header=true) ORDER BY 1"
        ));
        v["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r[0].as_str().unwrap().parse().unwrap())
            .collect()
    };
    assert_eq!(ids(()), vec![1, 2], "the cap stops after two commits");
    assert!(
        files_with_extension(&host, "csv").len() >= 2,
        "--rollover 1 cuts a part per commit"
    );
    assert!(run(&[]).is_some(), "the resuming run did not terminate");
    assert_eq!(
        ids(()),
        vec![1, 2, 3],
        "the capped-off commit arrives on the next run"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_refuses_a_table_it_cannot_resolve_before_the_first_ack() {
    // A typo beside a real table must fail the run, not deliver the real one and call
    // the misspelled one an empty success.
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_ctypo", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    let real = format!("RIVET.{}", t.name());
    let typo = format!("RIVET.{}_NOPE", t.name());
    ora_exec(&format!("INSERT INTO {} VALUES (1, 10)", t.name()));
    let err = rig(&t, &ckpt, &d.path().join("out"))
        .tables(&[real.as_str(), typo.as_str()])
        .run_expect_fail();
    assert!(err.contains("not found or not readable"), "{err}");
    assert!(
        !ckpt.exists(),
        "no anchor is written for a config that cannot run"
    );
}

#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn oracle_cdc_byte_cap_counts_the_first_row_and_defers_not_drops() {
    // RED against a group whose first row is left out of the byte count (the old
    // Oracle-only copy of the cap): a one-row transaction then never reached the check.
    let _serial = cross_process_serial("oracle_cdc");
    let d = tempfile::tempdir().unwrap();
    let t = cdc_table("ora_ccap", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    let ckpt = d.path().join("cdc.ckpt");
    rig(&t, &ckpt, &d.path().join("anchor")).run_ok();
    ora_exec(&format!("INSERT INTO {} VALUES (1, 10)", t.name()));

    let refused =
        rig(&t, &ckpt, &d.path().join("refused")).run_with_envs(&[("RIVET_CDC_MAX_TX_BYTES", "1")]);
    let err = String::from_utf8_lossy(&refused.stderr);
    assert!(
        !refused.status.success()
            && err.contains(
                "oracle cdc: one commit SCN (one or more transactions) needs more than 1 bytes"
            ),
        "a one-row transaction past the byte cap must refuse, naming what it buffered: {err}"
    );

    let out = d.path().join("out");
    rig(&t, &ckpt, &out).run_ok();
    assert_eq!(
        cdc_id_ops(&out),
        ops(&[(1, "insert")]),
        "the refused transaction is re-read on the next run, never skipped"
    );
}
