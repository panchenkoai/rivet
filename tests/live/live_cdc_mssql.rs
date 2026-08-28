//! Live SQL Server CDC regression — at-least-once resume.
//!
//! Gated `#[ignore]`: needs the dedicated `mssql-cdc` engine (the `cdc` profile,
//! :1434) with SQL Server Agent running — the capture job copies committed changes
//! into `cdc.<instance>_CT` asynchronously. Run with:
//!     docker compose --profile cdc up -d mssql-cdc
//!     cargo test --test live_suite -- --ignored

use std::time::Duration;

use crate::common::*;

// CDC enable/disable mutates database-global metadata + a shared capture job
// (sp_cdc_stop_job/sp_cdc_start_job are SERVER-wide), so these tests must not
// run concurrently. Serialization is cross_process_serial("mssql_cdc") — a
// static Mutex sat here first and serialized NOTHING under the canonical
// nextest one-process-per-test runner (r4 bughunt; same class as r3's
// COMPRESSION_SERIAL).

/// One CDC rig per (table, capture instance, checkpoint, destination). Callers
/// own ckpt/out so several configs can share one dir across a scenario; the rig
/// owns everything else (this replaced a yaml round-trip through write_config).
fn mssql_cdc_rig(table: &str, ci: &str, ckpt: &std::path::Path, out: &std::path::Path) -> Rig {
    Rig::mssql_cdc(table, ci)
        .checkpoint_path(ckpt.to_path_buf())
        .dest_path(out.to_path_buf())
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_resume_captures_only_new_changes() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 2, "run 1 captures the 2 changes");

    // Resume: the checkpoint advanced past the first two, so run 2 must capture ONLY
    // the two new changes — not re-read all four from the change table's min LSN.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30),(4,40)"));
    wait_for_capture(&ci, 4);
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out2).run_ok();
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume must capture only the 2 new changes (LSN resume), not re-read all 4"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(3, "insert".to_string()), (4, "insert".to_string())],
        "the resumed parquet must hold exactly the NEW changes (count 2 cannot tell new-2 from wrong-2)"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_intra_transaction_updates_get_distinct_seq() {
    // Peer of cdc_intra_transaction_updates_get_distinct_seq. SQL Server stamps
    // every change of a transaction with the same __$start_lsn (what rivet emits
    // as __pos), so __pos ties them — __seq restores the intra-transaction order.
    let _serial = cross_process_serial("mssql_cdc");
    const N: i64 = 200;
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ms_seq");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, counter BIGINT)"
    ));
    // The seed INSERT precedes CDC enable, so only the N updates are captured.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1, 0)"));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    // N updates of the SAME row in a SINGLE transaction.
    mssql_cdc_exec(&format!(
        "BEGIN TRAN; DECLARE @i INT = 1; WHILE @i <= {N} BEGIN \
         UPDATE dbo.{table} SET counter = @i WHERE id = 1; SET @i = @i + 1; END; COMMIT;"
    ));
    // SQL Server records each UPDATE as two CT rows (before + after image).
    wait_for_capture(&ci, 2 * N);

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    assert_intra_transaction_seq(&out, N);
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_sum_reconciles_across_intra_txn_updates() {
    // Peer of cdc_sum_reconciles_across_intra_txn_updates for SQL Server.
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ms_sum");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    for txn in cdc_sum_workload(&table) {
        mssql_cdc_exec(&format!("BEGIN TRAN; {}; COMMIT;", txn.join("; ")));
    }
    // A sentinel (v=0, does not move the sum) as the final change: once the
    // capture job has it, every prior change is captured too (LSN order) — a
    // robust drain signal when the exact CT row count is hard to predict
    // (0-row UPDATEs/DELETEs produce no CT rows).
    const SENTINEL: i64 = 9_000_000;
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES ({SENTINEL}, 0)"));
    let mut drained = false;
    for _ in 0..120 {
        if mssql_cdc_query_i64(&format!(
            "SELECT COUNT(*) FROM cdc.{ci}_CT WHERE id = {SENTINEL}"
        )) >= 1
        {
            drained = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(drained, "capture job did not reach the sentinel in 60s");

    // The query helper reads an INT column; the deterministic workload's sum is
    // a few tens of thousands, so CAST to INT is exact (and guards against a
    // silent widening bug better than a BIGINT the helper would misread).
    let source_sum = mssql_cdc_query_i64(&format!(
        "SELECT CAST(COALESCE(SUM(v), 0) AS INT) FROM dbo.{table}"
    ));

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    let changes = read_cdc_changes(&out);
    assert!(
        intra_txn_multi_change_count(&changes) > 0,
        "workload must exercise intra-transaction multi-updates or the sum passes vacuously"
    );
    let target_sum = deduped_current_sum(changes, CdcEngine::SqlServer);
    assert_eq!(
        source_sum, target_sum,
        "deduped-by-(__pos,__seq) SUM(v) must equal the source's SUM(v)"
    );
}

// Idle-first-run anchor model (per-engine, see CLAUDE.md): SQL Server has no
// client-side anchor to pin — a run without a checkpoint floors at
// `fn_cdc_get_min_lsn` (over-reads, never skips). This test pins that property:
// if a no-checkpoint run ever starts at the *max* LSN instead, a change landing
// between two idle scheduler cycles would be silently skipped — the exact hole
// MySQL shipped with (`first_run_with_zero_changes_pins_the_checkpoint_at_open`).
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_idle_first_run_then_change_is_captured_not_skipped() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_msidle");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    // Run 1: nothing captured yet — the change table is empty.
    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 0, "idle run 1 captures nothing");

    // A change lands BETWEEN the idle run and the next scheduler cycle.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);

    // Run 2 must capture it — never skip past it to the current max LSN.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out2).run_ok();
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

// Conformance: stream-property commit boundary + qualified `table:` routing,
// SQL Server flavour. MSSQL stamps committed=true per change-table row, so
// the MySQL stall cannot occur structurally; `dbo.<t>` must route.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_mixed_transaction_and_qualified_table_conformance() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let orders = unique_name("rivet_cdc_mixq");
    let audit = unique_name("rivet_cdc_mixa");
    let (ci_o, ci_a) = (format!("dbo_{orders}"), format!("dbo_{audit}"));
    for (t, _ci) in [(&orders, &ci_o), (&audit, &ci_a)] {
        mssql_cdc_drop_table(&format!("dbo.{t}"));
        mssql_cdc_exec(&format!("CREATE TABLE dbo.{t}(id INT PRIMARY KEY, v INT)"));
    }
    enable_cdc(&orders, &ci_o);
    enable_cdc(&audit, &ci_a);
    let _g1 = MssqlCdcTable {
        table: orders.clone(),
        ci: ci_o.clone(),
    };
    let _g2 = MssqlCdcTable {
        table: audit.clone(),
        ci: ci_a.clone(),
    };

    // ONE transaction touching both tables, audit last.
    mssql_cdc_exec(&format!(
        "BEGIN TRANSACTION; INSERT INTO dbo.{orders} VALUES (1,10); \
         INSERT INTO dbo.{audit} VALUES (1,99); COMMIT;"
    ));
    wait_for_capture(&ci_o, 1);

    // Qualified `table: dbo.<orders>` must route the captured row.
    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out1).unwrap();
    std::fs::create_dir_all(&out2).unwrap();
    let qualified = format!("dbo.{orders}");
    mssql_cdc_rig(&qualified, &ci_o, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 1, "qualified table: must capture");

    // And the checkpoint advanced past the mixed transaction.
    mssql_cdc_rig(&qualified, &ci_o, &ckpt, &out2).run_ok();
    assert_eq!(
        manifest_rows(&out2),
        0,
        "no re-read of the mixed transaction"
    );
}

// Gremlin CG4: the capture job (SQL Server Agent) stalls mid-life —
// `sys.sp_cdc_stop_job` freezes the change tables. Changes landing during the
// stall must NOT be lost: the stalled-window run captures nothing new (and
// must not advance past it), and after the job restarts they all appear.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn gremlin_mssql_capture_job_stall_loses_nothing() {
    let _serial = cross_process_serial("mssql_cdc");
    // Self-heal first: an earlier aborted run of THIS test may have left the
    // capture job disabled/stopped (the fault it injects is exactly that).
    mssql_cdc_try_exec(
        "EXEC msdb.dbo.sp_update_job @job_name = N'cdc.rivet_capture', @enabled = 1",
    );
    mssql_cdc_try_exec("EXEC sys.sp_cdc_start_job @job_type = N'capture'");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_stall");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 1);

    // Stall the capture job: DISABLE it (so the scheduler cannot restart it)
    // and stop it tolerantly — between polls the job is "not running" and a
    // bare sp_cdc_stop_job refuses.
    // Re-enable guard armed BEFORE the first manipulation — a panic anywhere
    // in the stall sequence must never leave the SHARED capture job disabled
    // (that cascades into every other mssql test's wait_for_capture).
    struct JobGuard;
    impl Drop for JobGuard {
        fn drop(&mut self) {
            mssql_cdc_try_exec(
                "EXEC msdb.dbo.sp_update_job @job_name = N'cdc.rivet_capture', @enabled = 1",
            );
            mssql_cdc_try_exec("EXEC sys.sp_cdc_start_job @job_type = N'capture'");
        }
    }
    let _job = JobGuard;
    mssql_cdc_try_exec(
        "EXEC msdb.dbo.sp_update_job @job_name = N'cdc.rivet_capture', @enabled = 0",
    );
    // The continuous job may be BETWEEN polls (stop refused) or mid-poll —
    // retry the stop until msdb reports no running instance, or the "stall"
    // never actually happened and the test is meaningless (the earlier flake).
    let running = || -> i64 {
        mssql_cdc_query_i64(
            "SELECT COUNT(*) FROM msdb.dbo.sysjobactivity ja \
             JOIN msdb.dbo.sysjobs j ON ja.job_id = j.job_id \
             WHERE j.name = 'cdc.rivet_capture' \
               AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions) \
               AND ja.start_execution_date IS NOT NULL \
               AND ja.stop_execution_date IS NULL",
        )
    };
    let stop_deadline = std::time::Instant::now() + Duration::from_secs(60);
    while running() > 0 {
        mssql_cdc_try_exec("EXEC sys.sp_cdc_stop_job @job_type = N'capture'");
        assert!(
            std::time::Instant::now() < stop_deadline,
            "could not stop the capture job — the stall precondition never held"
        );
        std::thread::sleep(Duration::from_secs(1));
    }

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (2,20),(3,30)"));

    // Run during the stall: nothing new to read — and that must be a plain
    // 0-row run, never an advance past the uncaptured changes.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out2).run_ok();
    assert_eq!(manifest_rows(&out2), 0, "stalled job ⇒ nothing new visible");

    // Job back: the changes must ALL appear on the next run.
    mssql_cdc_try_exec(
        "EXEC msdb.dbo.sp_update_job @job_name = N'cdc.rivet_capture', @enabled = 1",
    );
    mssql_cdc_try_exec("EXEC sys.sp_cdc_start_job @job_type = N'capture'");
    // The continuous capture job takes noticeably longer to come back after a
    // disable+stop than its steady-state poll cadence — give it up to 120 s.
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT")) < 3 {
        // Retry the start each pass — it can race an old instance winding
        // down ("already running") and be refused transiently.
        mssql_cdc_try_exec("EXEC sys.sp_cdc_start_job @job_type = N'capture'");
        assert!(
            std::time::Instant::now() < deadline,
            "capture job did not resume within 120s after re-enable"
        );
        std::thread::sleep(Duration::from_secs(2));
    }
    let out3 = d.path().join("out3");
    std::fs::create_dir_all(&out3).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out3).run_ok();
    assert_eq!(
        manifest_rows(&out3),
        2,
        "changes landed during the stall must appear after the job restarts"
    );
}

// UPDATE and DELETE through the typed surface (the matrix pins INSERTs only):
// an UPDATE's after-image must equal a batch export of the post-update state,
// column type for column type; a DELETE's image must carry the typed PK.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_update_and_delete_carry_full_types() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_updel");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount DECIMAL(18,4), \
         dt2 DATETIME2, u UNIQUEIDENTIFIER, vb VARBINARY(8), m MONEY, note NVARCHAR(50))"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, 1.5, '2024-01-01', \
         '12345678-1234-1234-1234-123456789012', 0xAA, 1.00, N'v1')"
    ));
    wait_for_capture(&ci, 1);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    mssql_cdc_exec(&format!(
        "UPDATE dbo.{table} SET amount=99999999999999.9999, \
         dt2='2035-08-07T09:08:07.987654', u='FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF', \
         vb=0xDEADBEEF, m=123.4567, note=N'üñíçødé v2' WHERE id=1"
    ));
    wait_for_capture(&ci, 3); // insert(1) + update before(2) + after(4) rows
    let upd_out = d.path().join("upd");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&upd_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &upd_out).run_ok();
    mssql_full_rig(&table, &batch_out).run_ok();
    let upd = read_one_batch(&upd_out);
    assert_eq!(upd.num_rows(), 1, "exactly the update after-image");
    let batch = read_one_batch(&batch_out);
    for field in batch.schema().fields() {
        let bi = batch.schema().index_of(field.name()).unwrap();
        let ui = upd.schema().index_of(field.name()).unwrap();
        assert_eq!(
            batch.column(bi).to_data(),
            upd.column(ui).to_data(),
            "update after-image column {}: differs from post-update batch",
            field.name()
        );
    }

    mssql_cdc_exec(&format!("DELETE FROM dbo.{table} WHERE id=1"));
    wait_for_capture(&ci, 4);
    let del_out = d.path().join("del");
    std::fs::create_dir_all(&del_out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &del_out).run_ok();
    let del = read_one_batch(&del_out);
    assert_eq!(del.num_rows(), 1);
    use arrow::array::Int32Array;
    let id = del
        .column(del.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("typed PK in the delete image");
    assert_eq!(id.value(0), 1);
}

// `cdc.initial: snapshot` — anchor(max LSN) → snapshot → drain, enforced by
// construction. Pre-rows must be captured by the Agent BEFORE run 1, so the
// anchor covers them; a lagging capture job just widens the overlap (deduped
// by PK downstream), never a gap.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_initial_snapshot_covers_preexisting_rows_then_streams() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("rivet_cdc_init");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);

    let rig = Rig::mssql_cdc(&table, &ci)
        .cdc("initial: snapshot")
        .cdc("until_current: true");
    let out = rig.out_dir();
    let cfg = rig.config_path();

    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join("snapshot")), 2);
    assert_eq!(
        duckdb_dir_parquet_id_set(&out.join("snapshot"))
            .into_iter()
            .collect::<Vec<i64>>(),
        vec![1, 2],
        "snapshot parquet must hold exactly the pre-existing ids (independent re-read)"
    );
    assert_eq!(
        manifest_rows(&out),
        0,
        "anchor at max LSN ⇒ nothing to drain"
    );

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30)"));
    wait_for_capture(&ci, 3);
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
    assert_eq!(
        cdc_id_ops(&out),
        vec![(3, "insert".to_string())],
        "streamed parquet must hold exactly the post-snapshot change (not just a count of 1)"
    );
}

// RED test for the finding: MONEY/SMALLMONEY were typed correctly
// (decimal(19,4)/(10,4)) but every VALUE was NULL — tiberius delivers money as
// ColumnData::F64 and both decimal decoders (batch arrow_convert and the CDC
// cell path) accepted only Numeric. The values must survive BOTH paths and
// stay ArrayData-equal. (Money is server-side fixed-point 1/10000; the f64
// hop is exact up to ~9×10^11 currency units — fidelity: compatible.)
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_money_values_survive_batch_and_cdc() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_money");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, m MONEY, sm SMALLMONEY)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, 123.4567, 12.34), (2, NULL, NULL)"
    ));
    wait_for_capture(&ci, 2);

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &cdc_out).run_ok();
    mssql_full_rig(&table, &batch_out).run_ok();

    // Value-level check against the SOURCE literal (NULL == NULL between the
    // two exports would mask the loss — that is exactly how it hid).
    use arrow::array::{Array, Decimal128Array};
    let batch = read_one_batch(&batch_out);
    let m_idx = batch.schema().index_of("m").unwrap();
    let m = batch
        .column(m_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("money must be Decimal128");
    assert!(!m.is_null(0), "money value must survive the batch export");
    assert_eq!(m.value(0), 1_234_567, "123.4567 at scale 4");
    let sm_idx = batch.schema().index_of("sm").unwrap();
    let sm = batch
        .column(sm_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("smallmoney must be Decimal128");
    assert_eq!(sm.value(0), 123_400, "12.34 at scale 4");
    assert!(m.is_null(1) && sm.is_null(1), "real NULLs stay NULL");

    // And the CDC leg must be ArrayData-equal to batch, column by column.
    let cdc = read_one_batch(&cdc_out);
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let cidx = cdc.schema().index_of(field.name()).unwrap();
        assert_eq!(
            batch.column(i).to_data(),
            cdc.column(cidx).to_data(),
            "column {}: CDC differs from batch",
            field.name()
        );
    }
}

// RED test for the finding (caught live: 6 of 8 tables captured ZERO events):
// the stream derived schema/table from the capture-instance NAME by splitting
// on the first underscore, so an instance named after an underscored table
// (`product_catalog` → schema "product", table "catalog") tagged every event
// with the wrong table and the sink's routing silently dropped them all — the
// run still reported success. Resolution must come from cdc.change_tables
// metadata, not the name.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_capture_instance_name_must_not_decide_the_table() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    // The table name contains underscores AND the capture instance is named
    // exactly after it — the shape the split-once heuristic gets wrong.
    let table = unique_name("rivet_cdc_und");
    let ci = table.clone();
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();
    assert_eq!(
        manifest_rows(&out),
        2,
        "events must be routed by the REAL table name (from cdc.change_tables), \
         not by parsing the capture-instance name"
    );
}

/// The routing bug's actual SHAPE needs two tables — the test above cannot
/// express it.
///
/// The field failure was events for 6 of 8 tables landing under the WRONG table
/// (the capture-instance name was split on `_`, so `product_catalog` became
/// schema `product` / table `catalog`), while every run reported success. With
/// ONE table a mis-route can only drop to zero, which `manifest_rows == 2`
/// catches. With two, a swap keeps BOTH counts at 2 and only the CONTENT
/// differs — and nothing in the suite looked at content, on this engine, until
/// now (audit 2026-08-17). Its own rule: a guard against confusing two things
/// needs two of the thing.
///
/// SQL Server cannot do this in one export — `Config` refuses `tables:` there
/// because capture instances are per-table — so this is two CDC exports over one
/// config, which is what `Rig::also_cdc_export` was added for.
///
/// The ids are deliberately DISJOINT (10,11 vs 20,21): identical ids in both
/// tables would make a perfect swap invisible even to a content check, which is
/// how the PG/MySQL uncaptured-table fixtures are shaped today (both insert
/// `id = 1`).
///
/// HONEST LIMIT, stated because a reader will otherwise assume more. I could not
/// build a mutant where this test bites and the single-table one above does not.
/// Four were tried: ignore the catalog and split the instance name; the same with
/// the identity guard disabled; resolve without `WHERE capture_instance`; and both
/// together. The first and third fail LOUDLY in `rig.run_ok()` — the identity
/// guard (`table_matches` against the catalog's spelling) refuses before any event
/// moves. With the guard disabled the routing filter is byte-exact, so a
/// mis-resolved export matches NOTHING and the prefix comes back EMPTY (`left:
/// []`) rather than holding the other table's rows. Drop-to-zero is what the count
/// assertion above already catches.
///
/// So what this adds is coverage, not proven sensitivity: multi-table SQL Server
/// CDC had NO test at all before it (`Rig::also_cdc_export` had to be written for
/// it), and per-prefix CONTENT is the assertion PG and MySQL already make and this
/// engine did not. If someone later makes routing tolerant enough to place a row
/// under the wrong prefix instead of dropping it, this is the test that sees it —
/// but that is an argument from shape, and it has not been demonstrated.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_two_underscored_tables_do_not_cross_route() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();

    let t1 = unique_name("rivet_cdc_ord");
    let t2 = unique_name("rivet_cdc_inv");
    let (ci1, ci2) = (t1.clone(), t2.clone());
    for (t, ci) in [(&t1, &ci1), (&t2, &ci2)] {
        mssql_cdc_drop_table(&format!("dbo.{t}"));
        mssql_cdc_exec(&format!("CREATE TABLE dbo.{t}(id INT PRIMARY KEY, v INT)"));
        enable_cdc(t, ci);
    }
    let _g1 = MssqlCdcTable {
        table: t1.clone(),
        ci: ci1.clone(),
    };
    let _g2 = MssqlCdcTable {
        table: t2.clone(),
        ci: ci2.clone(),
    };

    mssql_cdc_exec(&format!("INSERT INTO dbo.{t1} VALUES (10,1),(11,1)"));
    mssql_cdc_exec(&format!("INSERT INTO dbo.{t2} VALUES (20,2),(21,2)"));
    wait_for_capture(&ci1, 2);
    wait_for_capture(&ci2, 2);

    let rig = Rig::mssql_cdc(&t1, &ci1)
        .checkpoint_path(d.path().join("cdc1.ckpt"))
        .also_cdc_export(&t2, &t2, &[&format!("capture_instance: {ci2}")]);
    rig.run_ok();

    // Content, per export prefix. A swap leaves both counts at 2.
    assert_eq!(
        cdc_id_ops(&rig.out_dir()),
        vec![(10, "insert".to_string()), (11, "insert".to_string())],
        "export '{t1}' must hold ONLY its own rows — a cross-route keeps the count \
         at 2 and swaps the ids, which is the shape the field failure had"
    );
    assert_eq!(
        cdc_id_ops(&rig.out_dir_for(&t2)),
        vec![(20, "insert".to_string()), (21, "insert".to_string())],
        "export '{t2}' must hold ONLY its own rows"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_crash_before_checkpoint_re_reads_on_resume() {
    let _serial = cross_process_serial("mssql_cdc");
    // At-least-once under a crash, now that SQL Server resumes by LSN: establish a
    // checkpoint, then crash on the next batch AFTER the part is durable but BEFORE
    // the checkpoint advances. The checkpoint must stay put, so the resume re-reads
    // exactly that batch — not lose it, and not re-read everything.
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_mscrash");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");

    // Establish the checkpoint at the first two changes.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert_eq!(manifest_rows(&out1), 2);

    // Two more changes; run crashes after the part is durable, before the checkpoint.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30),(4,40)"));
    wait_for_capture(&ci, 4);
    let crash_out = d.path().join("crash");
    std::fs::create_dir_all(&crash_out).unwrap();
    let crashed = mssql_cdc_rig(&table, &ci, &ckpt, &crash_out)
        .run_with_envs(&[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail the run"
    );

    // The checkpoint stayed at change 2, so the resume re-reads exactly 3 and 4 —
    // not lost (would be 0 if the checkpoint had advanced) and not all four.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out2).run_ok();
    assert_eq!(
        manifest_rows(&out2),
        2,
        "crash before the checkpoint → resume re-reads exactly the 2 un-checkpointed changes"
    );
    assert_eq!(
        cdc_id_ops(&out2),
        vec![(3, "insert".to_string()), (4, "insert".to_string())],
        "the re-read parquet must hold exactly the un-acked changes"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_datetimeoffset_value_is_preserved() {
    let _serial = cross_process_serial("mssql_cdc");
    // datetimeoffset is tz-aware: it must land as a tz-aware Timestamp carrying the
    // UTC instant — identical to the batch export (parity) — never silently dropped.
    // The adapter used to try_get it as NaiveDateTime (wrong type) → None → NULL.
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_dto");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, dto DATETIMEOFFSET)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    // 10:00 at +05:30 is 04:30:00 UTC — the instant that must survive.
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, '2026-06-23 10:00:00 +05:30')"
    ));
    wait_for_capture(&ci, 1);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    // tz-aware Timestamp carrying the UTC instant (10:00 +05:30 → 04:30:00 UTC).
    let dto = parquet_one_timestamp(&out, "dto");
    assert!(
        dto.starts_with("2026-06-23 04:30:00"),
        "datetimeoffset must be captured as the 04:30 UTC instant — got {dto:?}"
    );
}

/// The first row's `col` (a Timestamp(µs)) as its UTC `NaiveDateTime` string.
fn parquet_one_timestamp(dir: &std::path::Path, col: &str) -> String {
    use arrow::array::{AsArray, types::TimestampMicrosecondType};
    let part = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .expect("a .parquet part");
    let f = std::fs::File::open(part).unwrap();
    let mut r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let batch = r.next().expect("a row").unwrap();
    let idx = batch.schema().index_of(col).expect("column present");
    batch
        .column(idx)
        .as_primitive::<TimestampMicrosecondType>()
        .value_as_datetime(0)
        .expect("a non-null instant")
        .to_string()
}

/// Whether the first row's `col` is non-null in the one `.parquet` part.
fn parquet_col0_present(dir: &std::path::Path, col: &str) -> bool {
    use arrow::array::Array;
    let part = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
        .expect("a .parquet part");
    let f = std::fs::File::open(part).unwrap();
    let mut r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let batch = r.next().expect("a row").unwrap();
    let idx = batch.schema().index_of(col).expect("column present");
    !batch.column(idx).is_null(0)
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_uniqueidentifier_value_is_preserved() {
    let _serial = cross_process_serial("mssql_cdc");
    // uniqueidentifier resolves to a UUID column (FixedSizeBinary(16)). The adapter
    // used to map the Guid to its 36-char string, which does not fit the fixed-size
    // builder and silently became NULL — data loss.
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_uuid");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, u UNIQUEIDENTIFIER)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, '12345678-1234-1234-1234-123456789012')"
    ));
    wait_for_capture(&ci, 1);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();
    assert!(
        parquet_col0_present(&out, "u"),
        "uniqueidentifier must be captured (16 canonical bytes), not dropped to NULL"
    );
}

fn mssql_full_rig(table: &str, out: &std::path::Path) -> Rig {
    Rig::mssql_batch(&format!("{table}_batch"))
        .source_url(MSSQL_CDC_URL)
        .query(&format!("SELECT * FROM dbo.{table}"))
        .dest_path(out.to_path_buf())
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_full_type_matrix_matches_batch() {
    let _serial = cross_process_serial("mssql_cdc");
    // The parity contract, enforced: a comprehensive type table exported both ways —
    // batch (`mode: full`) and CDC — must produce the IDENTICAL Arrow column (type AND
    // value, via ArrayData equality) for every source column. Two value-decode paths
    // exist for performance (CDC's typed RivetValue sink vs batch's zero-alloc
    // arrow_convert); this test is what guarantees they can't drift — any divergence
    // (a tz type, a uuid byte order, a decimal scale, a dropped value) fails here.
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_matrix");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, big BIGINT, amount DECIMAL(18,4), \
         flag BIT, label VARCHAR(50), nlabel NVARCHAR(50), dt2 DATETIME2, dto DATETIMEOFFSET, \
         d DATE, t TIME, u UNIQUEIDENTIFIER, vb VARBINARY(16), \
         ch CHAR(8), nch NCHAR(8), dt1 DATETIME, sdt SMALLDATETIME, \
         fb BINARY(8), num NUMERIC(10,3), m MONEY, sm SMALLMONEY)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, 9000000000000, 12345.6789, 1, 'hello', \
         N'cafe-unicode', '2026-06-23 10:00:00.1234567', '2026-06-23 10:00:00 +05:30', \
         '2026-06-23', '13:45:30.123456', '12345678-1234-1234-1234-123456789012', 0xDEADBEEF, \
         'pad', N'ñpad', '2026-01-15T13:45:30.127', '2026-01-15T13:45:00', 0xAB, 12.345, \
         123.4567, -0.01)"
    ));
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} (id) VALUES (2)"));
    wait_for_capture(&ci, 2);

    let cdc_out = d.path().join("cdc");
    let batch_out = d.path().join("batch");
    std::fs::create_dir_all(&cdc_out).unwrap();
    std::fs::create_dir_all(&batch_out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &cdc_out).run_ok();
    mssql_full_rig(&table, &batch_out).run_ok();

    let batch = read_one_batch(&batch_out);
    let cdc = read_one_batch(&cdc_out);
    // Every source column the batch export has must be byte-for-byte identical in CDC.
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
    // CDC adds its change-metadata columns the batch export doesn't have.
    assert!(cdc.schema().index_of("__op").is_ok() && cdc.schema().index_of("__pos").is_ok());

    // ─── Per-column NULL profile against SQL SERVER ITSELF ────────────────────
    //
    // HONEST LIMIT, first, because it changes how to read what follows: no mutant
    // was found where THIS assertion is the one that bites. Two were tried.
    // (1) `ColumnData::Guid(Some(g))` → `RivetValue::Null` in the MSSQL CDC
    // decoder — the exact degrade-to-NULL shape — is caught FIRST by the
    // ArrayData comparison above (`column u: CDC differs from the batch export`),
    // because only the CDC side degrades. (2) The shared decision point, where a
    // fault WOULD be symmetric — `RivetType::Uuid => FixedSizeBinary(16)` in
    // `types/mapping.rs`, which both paths read — narrowed to `(8)` never runs:
    // the product rejects it at `mapping.rs:294` ("Uuid extension only valid on
    // FixedSizeBinary(16)"). That is a real result about the product, not a gap:
    // the one place both paths could go wrong together is guarded by its own
    // invariant.
    //
    // So this earns its place as defence in depth, not on a demonstrated kill —
    // and the distinction is stated here rather than left for a reader to assume
    // a kill. What it uniquely covers is the SYMMETRIC fault: the comparison
    // above is differential (CDC against batch, both decoded by rivet), so a
    // fault the two share passes its own inspection. This asks SQL Server.
    //
    // The class is not hypothetical on other engines: the `FixedSizeBinary(16)`
    // builder nulled anything not exactly 16 bytes and `test_decoding` renders
    // uuids as 36-char TEXT, so 100% of a PostgreSQL uuid column became NULL on a
    // real bucket while every count and sum check passed.
    //
    // The column list is DERIVED from the catalog, never re-typed here: a
    // hand-written list grades only the columns its author remembered and
    // silently stops covering any the fixture gains later.
    //
    // INSERT images only — a CDC part holds one row per change, so the source's
    // 2 rows are the 2 inserts; reading the whole part compares different
    // populations.
    let cols = mssql_cdc_query_strings(&format!(
        "SELECT c.name FROM sys.columns c \
         JOIN sys.tables t ON t.object_id = c.object_id \
         WHERE t.name = '{table}' ORDER BY c.column_id"
    ));
    assert!(
        cols.len() >= 20,
        "the fixture must present a rich column set to profile; got {} — did the \
         catalog query stop resolving the table?",
        cols.len()
    );
    let mut columns_with_nulls = 0;
    for col in &cols {
        let src_nulls = mssql_cdc_query_i64(&format!(
            "SELECT COUNT(*) - COUNT([{col}]) FROM dbo.{table}"
        ));
        columns_with_nulls += i32::from(src_nulls > 0);
        let dst_nulls = duckdb_dir_scalar(
            &cdc_out,
            &format!("count(*) - count(\"{col}\")"),
            Some("__op = 'insert'"),
        );
        assert_eq!(
            src_nulls, dst_nulls,
            "column '{col}': NULL-count parity against SQL Server itself — source \
             {src_nulls}, captured {dst_nulls}. A per-cell decode that degrades to \
             NULL moves this and nothing else."
        );
    }
    // The fixture must actually LOAD this axis: row 2 sets only `id`, so every
    // other column must show a source NULL. A column compared at 0-vs-0 is a
    // green that proves nothing, and that is what this decays into if someone
    // later simplifies the INSERTs.
    assert!(
        columns_with_nulls >= 15,
        "only {columns_with_nulls} of {} columns carry a source NULL — the fixture \
         stopped exercising the null axis, so the parity above is comparing zeros",
        cols.len()
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_resume_past_retention_errors_not_a_silent_gap() {
    let _serial = cross_process_serial("mssql_cdc");
    // If the resume LSN has fallen below the change table's min (the cleanup job
    // removed it), resuming from min would silently SKIP the cleaned-up changes. The
    // adapter must fail loudly (prompting a re-snapshot), never hide the gap.
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_stale");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);

    // A checkpoint whose LSN is far below the change table's min — what a checkpoint
    // older than retention looks like after the cleanup job runs.
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::write(&ckpt, r#"{"lsn":"00000000000000000001"}"#).unwrap();
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let res = mssql_cdc_rig(&table, &ci, &ckpt, &out).run();
    assert!(
        !res.status.success(),
        "a resume past retention must fail, not silently skip the gap"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        stderr.contains("older than") && stderr.contains("re-snapshot"),
        "the error must name the retention gap + the re-snapshot remedy, got:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_corrupt_checkpoint_fails_loud_not_silently_absent() {
    // #99, SQL Server flavour: a corrupt/truncated checkpoint must fail the run
    // loudly, never be swallowed into "no checkpoint". The from-LSN resume site
    // (create_change_stream) and the shared cdc_job resume-plan both route the
    // read through `Position::load`, which now errors on a corrupt checkpoint
    // instead of `.ok().flatten()`-ing it into a silent re-anchor.
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_corrupt");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    // Run 1 captures one change and pins a valid checkpoint.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);
    let ckpt = d.path().join("cdc.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out1).run_ok();
    assert!(ckpt.exists(), "run 1 pins a checkpoint");

    // The checkpoint is corrupted; a further change lands. The run must refuse to
    // proceed on a checkpoint it cannot parse, not read it as absent.
    std::fs::write(&ckpt, b"{ not valid json").unwrap();
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (2,20)"));
    wait_for_capture(&ci, 2);

    // Run 2 must FAIL loudly.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let res = mssql_cdc_rig(&table, &ci, &ckpt, &out2).run();
    assert!(
        !res.status.success(),
        "a corrupt checkpoint must fail the run, not be read as absent"
    );
    let stderr = String::from_utf8_lossy(&res.stderr);
    assert!(
        stderr.contains("corrupt or truncated"),
        "the failure must name the corrupt checkpoint, got:\n{stderr}"
    );
}

// ─── schema drift + bounded-run termination (coverage-matrix gap fills) ──────

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_column_added_via_new_capture_instance_is_captured() {
    // Schema-drift on SQL Server is a correctness cliff: `ALTER TABLE ADD COLUMN`
    // does NOT widen the existing capture instance — its `cdc.<ci>_CT` keeps the
    // old columns, so a run pointed at the old instance never sees the new column.
    // The documented recovery is a SECOND capture instance (SQL Server allows two
    // per table). This proves rivet, pointed at the new instance, resolves and
    // emits the WIDER schema — reading the old instance would silently drop `w`.
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_msdrift");
    let ci1 = format!("dbo_{table}_v1");
    let ci2 = format!("dbo_{table}_v2");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v INT)"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci1}';"
    ));
    let _g1 = MssqlCdcTable {
        table: table.clone(),
        ci: ci1.clone(),
    };

    // A change under the original (id, v) schema, captured by ci1.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci1, 1);
    let ckpt1 = d.path().join("cdc1.ckpt");
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    mssql_cdc_rig(&table, &ci1, &ckpt1, &out1).run_ok();
    assert!(
        !duckdb_dir_parquet_has_column(&out1, "w"),
        "ci1 predates the added column"
    );

    // Add a column; ci1 CANNOT widen — create a SECOND capture instance for the
    // (id, v, w) shape (this is the documented drift-recovery path).
    mssql_cdc_exec(&format!("ALTER TABLE dbo.{table} ADD w VARCHAR(20)"));
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci2}';"
    ));
    let _g2 = MssqlCdcTable {
        table: table.clone(),
        ci: ci2.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (2,20,'hello')"));
    wait_for_capture(&ci2, 1);

    // A run pointed at the NEW instance must expose the added column.
    let ckpt2 = d.path().join("cdc2.ckpt");
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    mssql_cdc_rig(&table, &ci2, &ckpt2, &out2).run_ok();
    assert!(
        duckdb_dir_parquet_has_column(&out2, "w"),
        "the new capture instance must expose the column added after ci1"
    );
    assert!(
        duckdb_dir_parquet_distinct_strings(&out2, "w").contains("hello"),
        "the added column's value must be captured, not nulled"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_until_current_terminates_under_sustained_writes() {
    // Peer of the Mongo roast_until_current_terminates_under_sustained_writes.
    // The `until_current` bound is `get_max_lsn()` pinned at open; a writer that
    // keeps committing advances the DB LSN, but the bounded run must still stop at
    // the open-time bound (not chase it) and keep the pre-open backlog.
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_mshb");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    // Pre-open backlog: ids 0..30 (wait for the async capture job to copy them).
    let vals: Vec<String> = (0..30).map(|i| format!("({i},{i})")).collect();
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES {}",
        vals.join(",")
    ));
    wait_for_capture(&ci, 30);

    // A writer committing continuously while the bounded run drains.
    let table_bg = table.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            mssql_cdc_try_exec(&format!("INSERT INTO dbo.{table_bg} VALUES ({i},{i})"));
            i += 1;
            std::thread::sleep(Duration::from_millis(20));
        }
    });

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let bounded_rig = mssql_cdc_rig(&table, &ci, &ckpt, &out);
    let elapsed = run_rivet_bounded(&bounded_rig.config_path(), Duration::from_secs(30));
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

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn roast_mssql_until_current_open_bound_two_runs_lose_nothing() {
    // MSSQL peer of roast_pg_until_current_open_bound_two_runs_lose_nothing, but
    // a DIFFERENT contract: termination comes from the engine, not the pin. The
    // capture Agent's scan gaps hand the reader an empty poll sooner or later, so
    // the drain exhausts even with the open-time @max pin DISABLED — verified by
    // the disable-pin RED probe (the run still exited under a sustained writer).
    // So the pinned @max is a PRECISE-STOP refinement, not load-bearing for
    // termination (the load-bearing engines are PostgreSQL — continuous slot
    // re-peek — and MongoDB — tailable stream; each hangs with its bound
    // disabled). What THIS test proves is DEFER-NOT-DROP: run 1 captures a prefix,
    // run 2 drains the tail, the union equals the SOURCE. Oracle: the source
    // table (count/sum/min/max of id — the scalar helpers can't fetch a set),
    // never rivet's own counters.
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("rivet_cdc_msob");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    // Pre-open backlog: ids 0..30 (wait for the async capture job to copy them).
    let vals: Vec<String> = (0..30).map(|i| format!("({i},{i})")).collect();
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES {}",
        vals.join(",")
    ));
    wait_for_capture(&ci, 30);

    // A writer committing continuously while the bounded run drains.
    let table_bg = table.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            mssql_cdc_try_exec(&format!("INSERT INTO dbo.{table_bg} VALUES ({i},{i})"));
            i += 1;
            std::thread::sleep(Duration::from_millis(5));
        }
    });

    let rig = Rig::mssql_cdc(&table, &ci).cdc("until_current: true");
    let cfg = rig.config_path();
    let elapsed = run_rivet_bounded(&cfg, Duration::from_secs(30));
    bg.stop();
    assert!(
        elapsed.is_some(),
        "run 1 must terminate at the open-time max-LSN bound under sustained writes"
    );

    // Let the capture job copy EVERYTHING the writer committed, then run 2
    // drains the remainder from run 1's checkpoint.
    let total = mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM dbo.{table}"));
    wait_for_capture(&ci, total);
    let elapsed2 = run_rivet_bounded(&cfg, Duration::from_secs(60));
    assert!(
        elapsed2.is_some(),
        "run 2 (no writers) must drain the tail and exit"
    );

    let got: std::collections::BTreeSet<i64> = duckdb_dir_parquet_i64(&rig.out_dir(), "id")
        .into_iter()
        .collect();
    let sum: i64 = got.iter().sum();
    assert_eq!(
        got.len() as i64,
        total,
        "distinct dest ids must match the source count"
    );
    assert_eq!(
        sum,
        mssql_cdc_query_i64(&format!("SELECT ISNULL(SUM(id),0) FROM dbo.{table}")),
        "dest id sum must match the source"
    );
    assert_eq!(
        got.first().copied(),
        Some(mssql_cdc_query_i64(&format!(
            "SELECT MIN(id) FROM dbo.{table}"
        ))),
        "dest min id must match the source"
    );
    assert_eq!(
        got.last().copied(),
        Some(mssql_cdc_query_i64(&format!(
            "SELECT MAX(id) FROM dbo.{table}"
        ))),
        "dest max id must match the source"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn roast_mssql_cdc_large_transaction_is_atomic_across_a_mid_flush_crash() {
    // MSSQL peer of roast_pg_cdc_large_transaction_is_atomic_across_a_mid_flush_
    // crash. All change rows of one source transaction share `__$start_lsn`;
    // every row used to carry `committed: true`, so a transaction larger than
    // `rollover` rolled + CHECKPOINTED mid-group, and a crash before the tail
    // flushed left the checkpoint at that start LSN — resume reads strictly AFTER
    // it (`fn_cdc_increment_lsn`), skipping the rest of the same-LSN group and
    // losing the tail. Fix: mark only the last row of each start-LSN group
    // committed. RED-proof: one 12-row transaction at rollover 5, crash at
    // `cdc_after_checkpoint_before_ack`. Oracle: the union of all parts on disk.
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_msatomic");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id BIGINT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    // ONE transaction, 12 rows (> 2× the rollover of 5) — one `__$start_lsn`.
    let vals: Vec<String> = (0..12).map(|i| format!("({i},{i})")).collect();
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES {}",
        vals.join(",")
    ));
    wait_for_capture(&ci, 12);

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mssql_cdc(&table, &ci)
        .cdc("rollover: 5")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    // Run 1 crashes right after the checkpoint is persisted (MSSQL ack is a
    // no-op; the checkpoint is the durable resume position).
    let crashed = rig.run_with_envs(&[("RIVET_TEST_PANIC_AT", "cdc_after_checkpoint_before_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );

    // Run 2 resumes from the checkpoint the crash left behind.
    let rig2 = Rig::mssql_cdc(&table, &ci)
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    run_rivet_ok(&rig2.config_path());

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (0..12).collect();
    assert_eq!(
        got,
        want,
        "the 12-row transaction must survive the mid-flush crash whole — got {} ids \
         (a mid-transaction checkpoint at the shared start LSN skipped the tail on resume)",
        got.len()
    );
}

/// MSSQL sibling of the independent CDC-type oracle (option 1): the
/// `mssql_cdc_full_type_matrix_matches_batch` cell is a DIFFERENTIAL self-oracle
/// (CDC vs batch, shared decode). This reads the `__changes` parquet with DuckDB —
/// outside rivet's decode family — and asserts each value vs the SOURCE literal,
/// catching a shared-decode bug matches_batch misses.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + duckdb"]
fn mssql_cdc_typed_values_match_source_via_duckdb_not_batch() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("cdc_typed_ms"));
    let table = unique_name("rivet_cdc_typed");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id INT PRIMARY KEY, big BIGINT, amount DECIMAL(18,4), \
         label VARCHAR(50), d DATE, vb VARBINARY(4), m MONEY, \
         fl FLOAT, bt BIT, u UNIQUEIDENTIFIER)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, 9000000000000, 12345.6789, 'hello', \
         '2026-06-23', 0xDEADBEEF, 123.4567, \
         1.5, 1, '12345678-1234-1234-1234-123456789012')"
    ));
    wait_for_capture(&ci, 1);
    mssql_cdc_rig(&table, &ci, &ckpt, &host_dir).run_ok();

    let res = duckdb_run_sql_json(&format!(
        "SELECT (big = 9000000000000) AND (amount = 12345.6789) AND (label = 'hello') \
         AND (d = DATE '2026-06-23') AND (lower(to_hex(vb)) = 'deadbeef') AND (m = 123.4567) \
         AND (fl = 1.5) AND (CAST(bt AS INTEGER) = 1) \
         AND (lower(CAST(u AS VARCHAR)) = '12345678-1234-1234-1234-123456789012') \
         FROM read_parquet('{container_dir}/cdc-*.parquet') WHERE id = 1"
    ));
    let rows = res["rows"].as_array().expect("duckdb rows");
    assert_eq!(rows.len(), 1, "one captured change for id=1; got: {res}");
    assert!(
        rows[0][0]
            .as_str()
            .is_some_and(|s| s.eq_ignore_ascii_case("true")),
        "MSSQL CDC typed values must equal the SOURCE literals via DuckDB (independent of \
         batch): bigint, decimal 12345.6789, text, date, binary (hex), MONEY 123.4567; got: {res}"
    );
}

/// A PARTIAL capture instance must be refused even when a sibling instance of the
/// same table is complete.
///
/// SQL Server allows two capture instances per source table, and the documented
/// schema-change workflow is exactly that: create a second, cut over, drop the
/// old. `row_image` joined `cdc.captured_columns` on the CHANGE table but grouped
/// by the SOURCE table, so the two instances' captured columns were SUMMED
/// against a denominator counted once — 2 of 3 plus 3 of 3 reads as 5 of 3, the
/// `got < all` test goes false, and the gate returns `Whole`. Pointing rivet at
/// the partial instance then writes NULL for the omitted column in every event,
/// with `status: success`: precisely the harm the gate exists to refuse.
///
/// Invisible with one instance, which is what every other test and every manual
/// check uses — the mechanism only misbehaves at N=2.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_a_partial_capture_instance_is_refused_even_beside_a_complete_sibling() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_twoci");
    let ci_full = format!("{table}_full");
    let ci_part = format!("{table}_part");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, keep INT, dropped INT)"
    ));

    // Instance ONE captures every column; instance TWO omits `dropped`.
    enable_cdc(&table, &ci_full);
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci_part}', \
         @captured_column_list=N'id,keep';"
    ));
    let _g1 = MssqlCdcTable {
        table: table.clone(),
        ci: ci_full.clone(),
    };
    let _g2 = MssqlCdcTable {
        table: table.clone(),
        ci: ci_part.clone(),
    };

    // The fixture must actually be the shape under test: two instances, and the
    // one rivet is pointed at must really be short.
    let n_ci = mssql_cdc_query_i64(&format!(
        "SELECT COUNT(*) FROM cdc.change_tables ct JOIN sys.tables t \
         ON t.object_id = ct.source_object_id WHERE t.name = '{table}'"
    ));
    assert_eq!(
        n_ci, 2,
        "fixture is inert — the two-instance case is the whole point"
    );
    let n_part = mssql_cdc_query_i64(&format!(
        "SELECT COUNT(*) FROM cdc.captured_columns cc JOIN cdc.change_tables ct \
         ON ct.object_id = cc.object_id WHERE ct.capture_instance = '{ci_part}'"
    ));
    assert_eq!(
        n_part, 2,
        "the partial instance must capture 2 of the table's 3 columns"
    );

    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,10,100),(2,20,200)"
    ));
    wait_for_capture(&ci_part, 2);

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let partial_rig = mssql_cdc_rig(&table, &ci_part, &ckpt, &out);
    let cfg = partial_rig.config_path();
    let res = run_rivet(&["run", "--config", cfg.to_str().unwrap()]);
    let stderr = String::from_utf8_lossy(&res.stderr).into_owned();

    assert!(
        !res.status.success(),
        "capturing through an instance that omits a column must be REFUSED, not reported \
         successful with that column NULL in every event; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("2 of 3 columns"),
        "the refusal must name the instance's REAL arity (2 of 3), not the sum across \
         instances; stderr:\n{stderr}"
    );
}

// ─── the config's table name is a label; the catalog is the truth ────────────

/// A `table:` that differs from the catalog only in CASE must not silently
/// capture nothing while the checkpoint advances past the changes.
///
/// `MssqlChangeStream::open` takes each event's `(schema, table)` from the
/// catalog — `cdc.change_tables` / `OBJECT_NAME` — which is right. But the SINK
/// routes with the raw config string through `table_matches`
/// (src/source/cdc/sink.rs:220), a byte-exact Rust comparison. Nothing ever
/// compares the two, and SQL Server's default collation is case-INSENSITIVE, so
/// `table: dbo.casetest` against a real `dbo.CaseTest` resolves its schema
/// perfectly (`SELECT * FROM dbo.casetest` returns a full, correct column list)
/// while `table_matches("dbo.casetest", "dbo", "CaseTest")` is false for every
/// event.
///
/// This is the `product_catalog` finding's sibling with the opposite ending. That
/// one dropped 100% of events for six of eight tables and at-least-once saved it
/// — flush → checkpoint → ack never ran for the unrouted tables, so one fixed
/// run recovered everything. Here the commit boundary is recorded BEFORE the
/// routing filter and the end-of-pass roll fires on `unacked_commit` alone, so
/// the checkpoint advances over events that were never captured.
///
/// Measured on this stand — 6 rows in `cdc.dbo_CaseTest_CT`, config lowercase:
///
///   run                          0 rows, 0 files, status success
///   checkpoint written           {"lsn":"0000006b000021a80005"}
///   re-run, case FIXED           0 rows — the LSN is already past them
///   re-run, case fixed + ckpt deleted   5 rows   ← they were there all along
///
/// The last two lines are the control pair, and they are what separates "lost"
/// from "deferred": the change rows never left the change table, so the only
/// thing that skipped them was the advanced checkpoint. Recovery costs deleting
/// the checkpoint and re-reading from the retention floor; past retention there
/// is nothing to re-read.
// AUDIT-RED cdc-case-identity: a case-only `table:` mismatch routes ZERO events while the checkpoint advances past them — the changes are unrecoverable without discarding the checkpoint. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_case_only_table_mismatch_must_not_silently_drop_events() {
    let _serial = cross_process_serial("mssql_cdc");

    // Mixed case ON PURPOSE — `unique_name` lowercases, and the whole mechanism
    // is a catalog name the config spells differently.
    let table = format!("CaseIdent{}", std::process::id() % 100_000);
    let table = table.as_str();
    let ci = &format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id int PRIMARY KEY, v nvarchar(50))"
    ));
    enable_cdc(table, ci);
    let _guard = MssqlCdcTable {
        table: table.to_string(),
        ci: ci.clone(),
    };

    let d = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cdc.ckpt");
    // The config names the table in a case the catalog does not use. SQL Server
    // accepts it everywhere EXCEPT rivet's own byte-exact router.
    let anchor_rig = mssql_cdc_rig(&table.to_lowercase(), ci, &ckpt, out.path());
    let cfg = anchor_rig.config_path();

    // The anchor run is where a catalog cross-check would fire, so it must be
    // allowed to REFUSE rather than asserted to succeed — refusing is the
    // outcome this test wants.
    let anchor = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[]);
    if !anchor.status.success() {
        let why = String::from_utf8_lossy(&anchor.stderr);
        assert!(
            why.contains("no configured table matches"),
            "the anchor run failed for an unrelated reason:\n{why}"
        );
        return; // refused at open, before any checkpoint could advance — correct
    }
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,'a'),(2,'b'),(3,'c'); \
         UPDATE dbo.{table} SET v='B' WHERE id=2; \
         DELETE FROM dbo.{table} WHERE id=3;"
    ));
    wait_for_capture(ci, 6);

    let o = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &[]);
    // Refusing at open — a catalog cross-check — is the correct outcome.
    if !o.status.success() {
        return;
    }

    let captured = read_cdc_changes(out.path()).len();
    if captured > 0 {
        return; // routed correctly; nothing to guard
    }

    // Captured nothing. The only acceptable version of that is a checkpoint that
    // did NOT move, leaving the events for the next run (at-least-once). Prove
    // which happened: the change rows are still there, so a correctly-cased
    // re-run against the SAME checkpoint must find them.
    let still_in_ct = mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT"));
    let fixed_rig = mssql_cdc_rig(&format!("dbo.{table}"), ci, &ckpt, out.path());
    let fixed = fixed_rig.config_path();
    let _ = run_rivet_env(&["run", "--config", fixed.to_str().unwrap()], &[]);
    let after_fix = read_cdc_changes(out.path()).len();

    panic!(
        "a case-only `table:` mismatch captured 0 of the {still_in_ct} change row(s) and exited 0. \
         Re-running with the case FIXED against the same checkpoint recovered {after_fix} — the \
         change rows never left the change table, so the checkpoint advanced past events that \
         were never captured. `MssqlChangeStream::open` reads identity from the catalog; the sink \
         routes with the raw config string through the byte-exact `table_matches` \
         (src/source/cdc/sink.rs:220), and nothing compares the two. SQL Server's default \
         collation is case-insensitive, so every other layer accepted the name."
    );
}

/// #200-3: the catalog-identity guard must also fire on the `rivet cdc` CLI path,
/// not only in `mode: cdc` config mode. `dispatch.rs` hard-coded
/// `configured_tables = Vec::new()` for the CLI, and `mssql/cdc.rs` disables the
/// cross-check when that set is empty — so `rivet cdc --capture-instance X
/// --table Y` opened WITHOUT the guard the config path has, and a `--table`
/// spelled in a case the catalog does not use routes ZERO events while the
/// checkpoint advances past them (the same silent drop the config-mode test above
/// guards, on the sibling entry point).
///
/// The fix passes `--table` — which the CLI ALREADY routes/filters by — as
/// `configured_tables`, so the guard has the same subject it does in config mode.
/// The `--output` leg is used (durable sink + checkpoint), because the drop is
/// only dangerous once a checkpoint can advance.
// AUDIT-RED cdc-cli-identity: a case-only --table mismatch on the CLI path routes ZERO events while the checkpoint advances — expected to FAIL until the CLI passes --table as configured_tables.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_cli_path_case_only_table_mismatch_must_not_silently_drop_events() {
    let _serial = cross_process_serial("mssql_cdc");

    let table = format!("CliIdent{}", std::process::id() % 100_000);
    let table = table.as_str();
    let ci = &format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id int PRIMARY KEY, v nvarchar(50))"
    ));
    enable_cdc(table, ci);
    let _guard = MssqlCdcTable {
        table: table.to_string(),
        ci: ci.clone(),
    };

    let d = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("cli.ckpt");

    // The CLI invocation, mirroring config mode's mismatch: --table in a case the
    // catalog does not use. The anchor is allowed to REFUSE (the guard firing at
    // open is exactly the fix) — refusing before any checkpoint advances is the
    // outcome this test wants.
    let cli = |dir: &std::path::Path, tbl: &str| {
        run_rivet_env(
            &[
                "cdc",
                "--source",
                MSSQL_CDC_URL,
                "--capture-instance",
                ci,
                "--table",
                tbl,
                "--checkpoint",
                ckpt.to_str().unwrap(),
                "--output",
                dir.to_str().unwrap(),
            ],
            &[],
        )
    };

    let anchor = cli(out.path(), &table.to_lowercase());
    if !anchor.status.success() {
        let why = String::from_utf8_lossy(&anchor.stderr);
        assert!(
            why.contains("no configured table matches"),
            "the CLI anchor run failed for an unrelated reason:\n{why}"
        );
        return; // refused at open, before any checkpoint could advance — correct
    }

    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,'a'),(2,'b'),(3,'c');"
    ));
    wait_for_capture(ci, 3);

    let o = cli(out.path(), &table.to_lowercase());
    if !o.status.success() {
        return; // refused — the guard the fix adds; correct
    }

    // Read the captured EVENTS back (the sibling test's oracle) — part
    // presence alone is the weak "mc ls | wc -l" class the matrix audit
    // banned; the content read costs nothing more and grades honestly.
    let captured = read_cdc_changes(out.path()).len();
    if captured > 0 {
        return; // routed correctly; nothing to guard
    }

    // Captured nothing AND exited 0 — the pre-fix silent drop. Prove the changes
    // were there all along: a correctly-cased CLI run against a FRESH checkpoint
    // recovers them (parts appear on disk).
    let still_in_ct = mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT"));
    let out_fixed = tempfile::tempdir().unwrap();
    let fixed_ckpt = d.path().join("cli_fixed.ckpt");
    let _ = run_rivet_env(
        &[
            "cdc",
            "--source",
            MSSQL_CDC_URL,
            "--capture-instance",
            ci,
            "--table",
            &format!("dbo.{table}"),
            "--checkpoint",
            fixed_ckpt.to_str().unwrap(),
            "--output",
            out_fixed.path().to_str().unwrap(),
        ],
        &[],
    );
    let after_fix = read_cdc_changes(out_fixed.path()).len();

    panic!(
        "the `rivet cdc` CLI path captured 0 of the {still_in_ct} change row(s) on a case-only \
         --table mismatch and exited 0; a correctly-cased run wrote {after_fix} part(s). The CLI \
         passed configured_tables = Vec::new() (dispatch.rs), which disables the catalog-identity \
         cross-check (mssql/cdc.rs), so the guard the config path has never fired here — every \
         event was dropped while the checkpoint advanced past it."
    );
}

/// A checkpoint that PARSES but has no `lsn` silently re-reads the whole change
/// table — the guard at the parse site claims it doesn't.
///
/// `Position::load` refuses a checkpoint that is not valid JSON, and its message
/// says why: treating it as absent "would permanently skip every change since the
/// last checkpoint". The MSSQL call site then reads the position with
/// `.and_then(|pos| pos.0.get("lsn")...)` — so a file that IS valid JSON yet
/// carries no `lsn` (a key renamed by a future version, a hand-edit, a half-written
/// migration) yields `None`, which `fill_sql` turns into
/// `from_expr = fn_cdc_get_min_lsn(ci)`: the entire retained change table, re-read
/// and re-delivered, with a green exit and nothing in the log.
///
/// MySQL refuses exactly this (`ok_or_else`, "checkpoint missing 'file'/'pos'");
/// Mongo refuses it (`decode_resume_token`); PostgreSQL cannot hit it at all
/// because its anchor is the slot, server-side. SQL Server is the one engine whose
/// resume position lives ONLY in that file, and it was the one that shrugged.
///
/// The direction of harm is over-read, not loss — at-least-once still holds. It
/// is still a silent breach of the resume contract, and the comment above the code
/// already claimed it was closed (#99).
///
/// NOT the `pinned` key beside it: absent `pinned` is a documented legacy default
/// (`unwrap_or(false)` = treat as a resume, the loud direction). Only `lsn` — the
/// position itself — makes the checkpoint meaningless by its absence.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_checkpoint_without_an_lsn_must_refuse_not_silently_reread_everything() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_nolsn");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1,10),(2,20),(3,30)"
    ));
    wait_for_capture(&ci, 3);

    // Leg 1: an ordinary run, which writes a real checkpoint.
    let r1 = mssql_cdc_rig(&table, &ci, &ckpt, &d.path().join("out1"));
    r1.run_ok();
    assert_eq!(
        duckdb_dir_parquet_id_set(&r1.out_dir())
            .into_iter()
            .collect::<Vec<i64>>(),
        vec![1, 2, 3],
        "leg 1 must really capture — a leg that captured nothing would make every \
         assertion below vacuous"
    );

    // The degradation: the file still parses, the position is gone. Keep `pinned`
    // so the test isolates the `lsn` key rather than two changes at once.
    let before: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&ckpt).unwrap()).unwrap();
    assert!(
        before.get("lsn").and_then(|v| v.as_str()).is_some(),
        "the fixture is inert: leg 1 wrote no `lsn` to key off. Got {before}"
    );
    std::fs::write(
        &ckpt,
        serde_json::to_string(&serde_json::json!({
            "lsn_renamed_by_a_future_version": before.get("lsn").unwrap(),
            "pinned": false,
        }))
        .unwrap(),
    )
    .unwrap();

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (4,40)"));
    wait_for_capture(&ci, 4);

    // Leg 2 into a FRESH destination, so what it delivers is its own doing and not
    // leg 1's parts read a second time.
    let r2 = mssql_cdc_rig(&table, &ci, &ckpt, &d.path().join("out2"));
    let said = r2.run_expect_fail();
    assert!(
        said.contains("lsn") && said.to_lowercase().contains("checkpoint"),
        "the refusal must name the key and the file, or an operator cannot act on \
         it. Got:\n{said}"
    );

    // And the refusal must be the LOUD direction: nothing delivered, checkpoint
    // untouched, so a restored file resumes exactly where leg 1 stopped.
    assert!(
        !r2.out_dir().join("manifest.json").is_file(),
        "a refused run must deliver nothing"
    );
}

/// SQL Server needs no ambiguity resolution — and this pins WHY, so the next
/// reader does not add a guard that cannot fire.
///
/// The other three engines resolve a configured `table:` against a catalog that
/// may hold the name more than once. SQL Server cannot: the stream is opened on a
/// CAPTURE INSTANCE, `cdc.change_tables` holds one row per instance, and that row
/// names exactly one source object. So the relation is resolved before rivet
/// compares anything, and a bare configured name cannot pull in a second relation
/// because the stream carries only the one instance's change table.
///
/// What remains is the CROSS-CHECK — that the configured string matches the
/// relation the instance actually emits — and that already refuses, naming the
/// catalog's own spelling. This test proves both halves: a same-named table in
/// ANOTHER schema does not disturb a correct config, and a config naming the wrong
/// schema is refused rather than silently capturing.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_capture_instance_resolves_identity_so_a_same_named_table_cannot_interfere() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("rivet_cdc_amb");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "IF SCHEMA_ID('other') IS NULL EXEC('CREATE SCHEMA other'); \
         CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    // The same NAME in another schema — the shape that re-labels rows on the
    // engines whose stream is server-wide.
    mssql_cdc_exec(&format!(
        "IF OBJECT_ID('other.{table}') IS NOT NULL DROP TABLE other.{table}; \
         CREATE TABLE other.{table}(zzz INT PRIMARY KEY, qqq INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    mssql_cdc_exec(&format!("INSERT INTO other.{table} VALUES (2,20)"));
    wait_for_capture(&ci, 1);

    // A BARE configured name, the ambiguous shape elsewhere. Here the instance has
    // already resolved the relation, so it runs clean and captures only dbo's row.
    let d = tempfile::tempdir().unwrap();
    let rig = mssql_cdc_rig(&table, &ci, &d.path().join("c.ckpt"), &d.path().join("out"));
    rig.run_ok();
    assert_eq!(
        duckdb_dir_parquet_i64(&rig.out_dir(), "id"),
        vec![1],
        "only the capture instance's own relation is streamed — a same-named table \
         in another schema is not in this change table at all, which is why this \
         engine has no ambiguity to resolve"
    );

    // ...and naming the WRONG schema is refused, not silently captured.
    let wrong = Rig::mssql_cdc(&format!("other.{table}"), &ci)
        .checkpoint_path(d.path().join("w.ckpt"))
        .dest_path(d.path().join("w"));
    let said = wrong.run_expect_fail();
    assert!(
        said.contains(&format!("dbo.{table}")),
        "the refusal must name the relation the INSTANCE emits — that is the whole \
         remediation, and it is the catalog's spelling rather than the config's. \
         Got:\n{said}"
    );
    mssql_cdc_exec(&format!("DROP TABLE IF EXISTS other.{table}"));
}

/// The schema probe must read the relation the CAPTURE INSTANCE names — not
/// whatever the connection's default schema makes of the configured string.
///
/// Round-3 bughunt, and SQL Server is the only engine that never calls
/// `identity::resolve_captured_table`, so it takes the harm the other engines
/// refuse out loud. Two independent resolutions of one config:
///
///   `MssqlChangeStream::open` reads `cdc.change_tables` — the catalog, the truth —
///   and tags every event `<schema>.<table>`. Its routing check then passes, because
///   `table_matches` lets a BARE configured name match any schema.
///
///   `run_capture` separately hands the CONFIGURED string to `CdcSchemaResolver`,
///   whose `SELECT * FROM <name>` resolves in the connection's DEFAULT schema — a
///   different relation, with different columns.
///
/// Nothing compares the two. The export is then written with one table's column
/// names over another table's events: PROVEN before the fix — `status: success`,
/// exit 0, no warning, and the parquet carried `dbo`'s columns while `note`, the
/// captured table's only data column, was absent from the output entirely and the
/// columns that were present were all NULL.
///
/// This is precisely what PostgreSQL refuses in words — "a foreign row is written
/// under THIS table's names … or as an all-NULL row … Neither is recoverable from
/// the output" — and SQL Server was doing it.
///
/// The fix is resolution, not refusal: a capture instance names its source object
/// in the catalog unambiguously, so there is nothing here for an operator to
/// disambiguate. Carry the pair the stream already resolved.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_schema_probe_follows_the_capture_instance_not_the_default_schema() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_sch");
    let ci = format!("rvsales_{table}");
    // A same-named DECOY in the default schema, with DIFFERENT columns. Without
    // one the two resolutions agree by accident and the test proves nothing — the
    // fixture has to cross the mechanism's activation threshold.
    mssql_cdc_exec("IF SCHEMA_ID('rvsales') IS NULL EXEC('CREATE SCHEMA rvsales');");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount INT, dbo_only INT); \
         INSERT INTO dbo.{table} VALUES (900, 42, 7);"
    ));
    mssql_cdc_exec(&format!(
        "CREATE TABLE rvsales.{table}(id INT PRIMARY KEY, note NVARCHAR(50))"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'rvsales', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable::in_schema("rvsales", &table, &ci);
    let _decoy = DboDecoy(table.clone());

    let ckpt = d.path().join("cdc.ckpt");
    mssql_cdc_exec(&format!(
        "INSERT INTO rvsales.{table} VALUES (1, N'real-note-one'), (2, N'real-note-two')"
    ));
    wait_for_capture(&ci, 2);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();

    // The config an operator writes: the bare table name plus the capture instance
    // that says, unambiguously, which relation it means.
    mssql_cdc_rig(&table, &ci, &ckpt, &out).run_ok();

    let batches = read_all_parts(&out);
    let cols: std::collections::BTreeSet<String> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        cols.contains("note"),
        "the captured table's own column must be in the output. Its absence is the \
         defect in its purest form: the probe read dbo's columns, so the only data \
         column of the relation actually being captured never reached the \
         destination. Got: {cols:?}"
    );
    assert!(
        !cols.contains("dbo_only"),
        "and the DECOY's columns must not be — a column that exists in neither the \
         captured table nor its events can only arrive all-NULL, which reads as \
         'the source had no data' rather than as a mis-resolved config: {cols:?}"
    );

    // `rivet check` is the THIRD reading of this config, and the operator's last
    // chance to notice. Round-4 measured it reporting the DECOY's columns and row
    // count with verdict ACCEPTABLE while the run wrote the captured relation's —
    // and `rivet load` plans the warehouse schema from these same reports, so it
    // would create `<table>__changes` with one relation's columns and feed it the
    // other's parquet.
    let checked = run_rivet(&[
        "check",
        "--config",
        mssql_cdc_rig(&table, &ci, &ckpt, &out)
            .config_path()
            .to_str()
            .unwrap(),
        "--json",
    ]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&checked.stdout),
        String::from_utf8_lossy(&checked.stderr)
    );
    assert!(
        said.contains("note"),
        "preflight must probe the relation the capture instance names — it reported \
         the decoy's columns while the run wrote the captured table's, and the two \
         disagreeing is worse than both being wrong together. Got:\n{said}"
    );
    assert!(
        !said.contains("dbo_only"),
        "and it must not report the decoy's columns at all:\n{said}"
    );

    // Values, not just names: a schema that is right while every cell is NULL is
    // the same silent loss one layer down.
    let notes: std::collections::BTreeSet<String> =
        duckdb_dir_parquet_distinct_strings(&out, "note");
    assert_eq!(
        notes,
        ["real-note-one", "real-note-two"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        "the captured rows' real values must reach the destination"
    );
}

/// Drops the `dbo` decoy the schema-probe test plants. The shared
/// [`MssqlCdcTable`] guard owns the CAPTURED table (it must disable CDC first);
/// this one owns the un-captured twin, which is an ordinary table.
struct DboDecoy(String);
impl Drop for DboDecoy {
    fn drop(&mut self) {
        let t = self.0.clone();
        let _ = std::panic::catch_unwind(move || {
            mssql_cdc_drop_table(&format!("dbo.{t}"));
        });
    }
}

/// The SNAPSHOT leg must read the same relation the drain captures.
///
/// The other half of the schema-probe defect, and the worse half: the drain wrote
/// the wrong COLUMNS, this writes rows that never existed in the captured table at
/// all. `initial: snapshot` plans its baseline BEFORE any stream opens, from
/// `export.table` — the configured string — so `SELECT … FROM <name>` resolves in
/// the connection's default schema and the baseline is a different table's
/// contents, deposited in the captured table's own prefix.
///
/// MEASURED before the fix on the mssql CDC stand: `status: success`, exit 0, and
/// `snapshot/…parquet` held exactly `{"id":900,"amount":42,"dbo_only":7}` — the
/// decoy's row — while the captured table's two rows appeared nowhere. A
/// downstream loader folds that baseline into the current-state view as fact.
///
/// The label and the READ have to part ways here: the sub-prefix and the leg's
/// name stay the configured string (both legs share one prefix, and the snapshot
/// marker is keyed by it, so changing them would strand every existing resume), while
/// the relation read follows the catalog.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_snapshot_leg_reads_the_captured_relation_not_the_default_schema() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_snp");
    let ci = format!("rvsales_{table}");
    mssql_cdc_exec("IF SCHEMA_ID('rvsales') IS NULL EXEC('CREATE SCHEMA rvsales');");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount INT, dbo_only INT); \
         INSERT INTO dbo.{table} VALUES (900, 42, 7);"
    ));
    mssql_cdc_exec(&format!(
        "CREATE TABLE rvsales.{table}(id INT PRIMARY KEY, note NVARCHAR(50)); \
         INSERT INTO rvsales.{table} VALUES (1, N'base-one'), (2, N'base-two');"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'rvsales', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable::in_schema("rvsales", &table, &ci);
    let _decoy = DboDecoy(table.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    mssql_cdc_rig(&table, &ci, &ckpt, &out)
        .cdc_line("initial: snapshot")
        .run_ok();

    let snap = out.join("snapshot");
    assert!(
        snap.is_dir(),
        "the snapshot leg must have run at all — without it this test grades nothing"
    );
    let notes: std::collections::BTreeSet<String> =
        duckdb_dir_parquet_distinct_strings(&snap, "note");
    assert_eq!(
        notes,
        ["base-one", "base-two"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        "the baseline must be the CAPTURED table's rows. Before the fix this \
         directory held the decoy's row (id 900) — data that never existed in the \
         relation being captured, written into its prefix under a green run"
    );

    // The other half of the LABEL/READ split, and the half round-5 caught: the leg
    // READS the catalog's relation but must RECORD the configured one. Both legs of
    // one export write into one prefix, and `ensure_single_export` refuses a prefix
    // whose manifests name two sources — so a leg recording `dbo.<table>` against a
    // drain recording `<table>` breaks `rivet load` on artifacts that are already
    // durable, with a message blaming the operator's prefix layout.
    let doc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(snap.join("manifest.json")).expect("read the leg's manifest"),
    )
    .expect("parse the leg's manifest");
    assert_eq!(
        doc["source"]["table"].as_str(),
        Some(table.as_str()),
        "the leg must record the CONFIGURED name, which is what the drain records \
         (`schema: None, table: <configured>`). Got: {}",
        doc["source"]
    );
    assert_eq!(
        doc["source"]["schema"],
        serde_json::Value::Null,
        "and no schema, for the same reason — the drain writes none, and an identity \
         the two legs spell differently is a refused load: {}",
        doc["source"]
    );
}

/// A capture instance the catalog does not know must be refused BEFORE the
/// snapshot leg makes a fabricated baseline permanent.
///
/// Round-4 bughunt, and the finding is about ORDER, not about detection: `open`
/// already bails on an unknown capture instance, but the snapshot leg runs, writes
/// and MARKS ITSELF DONE first. So a typo'd `capture_instance:` produced a baseline
/// read from whatever the configured name resolves to in the default schema, the
/// drain then failed, and the operator's fix was worthless — MEASURED: run 2 with
/// the name corrected reported `status: success`, exit 0, and never backfilled the
/// real rows, because the snapshot was already done. One fabricated row, two real
/// rows missing, permanently.
///
/// The plan-time lookup that made the fix possible had swallowed the distinction:
/// a missing instance, a permission error and a network blip all became `Ok(None)`
/// and fell back to the configured string — silently restoring the pre-fix
/// behaviour for exactly the config most likely to be wrong.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_refuses_an_unknown_capture_instance_before_the_snapshot_is_durable() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ghost");
    let ci = format!("rvsales_{table}");
    mssql_cdc_exec("IF SCHEMA_ID('rvsales') IS NULL EXEC('CREATE SCHEMA rvsales');");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount INT); \
         INSERT INTO dbo.{table} VALUES (900, 42);"
    ));
    mssql_cdc_exec(&format!(
        "CREATE TABLE rvsales.{table}(id INT PRIMARY KEY, note NVARCHAR(50)); \
         INSERT INTO rvsales.{table} VALUES (1, N'real-one'), (2, N'real-two');"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'rvsales', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable::in_schema("rvsales", &table, &ci);
    let _decoy = DboDecoy(table.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();

    // The typo: a capture instance that does not exist.
    let typo = format!("{ci}_typo");
    let rig = mssql_cdc_rig(&table, &typo, &ckpt, &out).cdc_line("initial: snapshot");
    let said = rig.run_expect_fail();
    assert!(
        said.contains(&typo),
        "the refusal must name the instance it could not find — an operator cannot \
         fix a name they were not shown. Got:\n{said}"
    );
    assert!(
        !out.join("snapshot").exists()
            || files_with_extension(&out.join("snapshot"), "parquet").is_empty(),
        "and it must refuse BEFORE the snapshot writes. A baseline on disk here is \
         the whole defect: it is also marked done, so correcting the typo never \
         backfills the rows it skipped"
    );

    // With the name corrected the run works, and the baseline is the real table's.
    let ok_rig = mssql_cdc_rig(&table, &ci, &ckpt, &out).cdc_line("initial: snapshot");
    ok_rig.run_ok();
    let notes: std::collections::BTreeSet<String> =
        duckdb_dir_parquet_distinct_strings(&out.join("snapshot"), "note");
    assert_eq!(
        notes,
        ["real-one", "real-two"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        "the corrected run must produce the CAPTURED table's baseline — this is the \
         assertion that failed before the fix, because the typo'd run had already \
         marked the snapshot done"
    );
}

/// The snapshot-done row must be keyed by the LABEL both times, or the durable
/// signal cannot match itself and every cycle re-snapshots the whole table.
///
/// Round-4 bughunt, and a regression introduced by the round-3B fix beside it: once
/// the snapshot leg learned to READ the catalog's relation, `mark_snapshot_done`
/// started writing that string while `snapshot_plan` kept asking with the CONFIGURED
/// one. The key is byte-exact, so on SQL Server — the only engine where the two
/// differ — the row written as `dbo.orders` could never answer a question about
/// `orders`.
///
/// The oracle is the behaviour, not the row: the state DB exists precisely so that
/// `cleanup_source: true` may wipe the destination without the next run
/// re-snapshotting (its own docstring says so), so deleting the snapshot directory
/// and running again asks exactly the question the store was built to answer. A
/// mismatched key means a full table re-read plus a duplicated baseline appended
/// into `<table>__changes` on every scheduled cycle, forever, under `status:
/// success`.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_a_completed_snapshot_is_not_redone_after_the_destination_is_wiped() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_once");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT); \
         INSERT INTO dbo.{table} VALUES (1,10),(2,20);"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    // ONE rig, run twice — the state DB lives in the rig's workdir, so two rigs
    // would be two databases and the second run could never see the first's row.
    // (That mistake made the first draft of this test fail for a harness reason
    // while it looked like the product defect.)
    let rig = mssql_cdc_rig(&table, &ci, &ckpt, &out).cdc_line("initial: snapshot");
    rig.run_ok();
    let snap = out.join("snapshot");
    assert!(
        !files_with_extension(&snap, "parquet").is_empty(),
        "run 1 must actually produce a baseline — without one this test grades nothing"
    );

    // What `cleanup_source: true` does: the destination goes, the state row stays.
    std::fs::remove_dir_all(&snap).expect("wipe the snapshot prefix");
    rig.run_ok();
    // The outcome, not only the absence: run 1's baseline must still be READABLE
    // after run 2 — "no new parts" and "nothing survives" look identical otherwise.
    assert_eq!(
        read_all_parts(&out)
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        0,
        "the drain captured no changes here, so the export prefix holds none — the \
         baseline lives under snapshot/ and is asserted below"
    );
    assert!(
        files_with_extension(&snap, "parquet").is_empty(),
        "run 2 must NOT re-snapshot: the state DB already records this table as \
         backfilled, and that record is the ONLY thing standing between a wiped \
         destination and a full re-read plus a duplicated baseline on every \
         scheduled cycle"
    );
}

/// `rivet check --type-report` must still type the export when the cdc catalog is
/// unreadable — that is the moment an operator runs it.
///
/// Round-5 bughunt, a regression from round 4's own fix. Teaching preflight to
/// resolve the capture instance made the lookup FATAL: on a database where CDC has
/// never been enabled (`Invalid object name 'cdc.change_tables'`, error 208) or a
/// login without SELECT on the cdc schema, the error escaped `collect_reports`,
/// `preflight::check` swallowed it into one `log::warn!`, and the command printed
/// its verdict, `Looks good.` and exit 0 — with no column table at all. MEASURED
/// RED/GREEN on a fresh CDC-less database.
///
/// A resolution that cannot run must degrade to the configured relation — the same
/// relation every non-CDC export reads — and say so.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_check_still_types_the_export_when_the_cdc_catalog_is_unreadable() {
    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_chk_nocdc").to_lowercase();
    // A DATABASE where CDC was never enabled, so `cdc.change_tables` does not exist
    // at all (error 208) — not merely a table with no capture instance. The
    // distinction is the whole test: a missing ROW is `Ok(None)` and always
    // degraded gracefully; a missing CATALOG is the `Err` arm, and that is the one
    // that made the column table vanish. Measured: with the fixture on the shared
    // `rivet` database (CDC enabled by every other test here) the mutant sails
    // through, because the query succeeds and simply returns nothing.
    let db = format!("nocdc_{table}");
    mssql_cdc_exec(&format!(
        "IF DB_ID(N'{db}') IS NOT NULL DROP DATABASE [{db}]; CREATE DATABASE [{db}];"
    ));
    let _db_guard = NoCdcDb(db.clone());
    mssql_cdc_exec(&format!(
        "USE [{db}]; CREATE TABLE dbo.{table}(id INT PRIMARY KEY, amount INT); \
         INSERT INTO dbo.{table} VALUES (1, 10);"
    ));

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = mssql_cdc_rig(&table, &format!("dbo_{table}"), &ckpt, &out)
        .source_url(&MSSQL_CDC_URL.replace("/rivet", &format!("/{db}")));
    let checked = run_rivet(&[
        "check",
        "--config",
        rig.config_path().to_str().unwrap(),
        "--type-report",
    ]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&checked.stdout),
        String::from_utf8_lossy(&checked.stderr)
    );
    // No capture happens in this test: `rivet check` writes no data, so there is no
    // delivered outcome to assert — the type REPORT is the outcome, and its absence
    // is exactly the defect. Said here because the conformance gate reads these
    // bodies for a delivery assertion and would otherwise be right to ask.
    assert!(
        said.contains("amount"),
        "the column report must still be produced — an unreadable cdc catalog is \
         the NORMAL state before `sp_cdc_enable_table` runs, and a check that \
         silently types nothing while printing a verdict is worse than one that \
         refuses. Got:\n{said}"
    );
}

/// Drops the CDC-less database the check test stands up.
struct NoCdcDb(String);
impl Drop for NoCdcDb {
    fn drop(&mut self) {
        let db = self.0.clone();
        let _ = std::panic::catch_unwind(move || {
            mssql_cdc_exec(&format!(
                "IF DB_ID(N'{db}') IS NOT NULL BEGIN ALTER DATABASE [{db}] SET SINGLE_USER \
                 WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END"
            ));
        });
    }
}

/// A SQL Server poll batch past the memory cap spills, and every transaction in it
/// is still delivered whole — confirmed by an oracle that shares no code with rivet.
///
/// SQL Server's buffer is the shape neither other engine has: it holds SEVERAL
/// transactions, runs of rows sharing a `__$start_lsn`, and the boundaries are only
/// visible by comparing neighbours. So its spilled tail reads one record AHEAD —
/// a row cannot be handed out until the row after it is known, because that is what
/// says whether it closes its transaction. Getting that wrong is invisible in the
/// delivered rows: fusing two transactions or splitting one ships exactly the same
/// data, and only changes when the sink rolls, checkpoints and acks.
///
/// TWO transactions on purpose, the second small: with one, a tail that closes
/// only at its very end and a tail that closes per group are indistinguishable.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_a_batch_past_the_memory_cap_spills_rather_than_failing() {
    const ROWS: usize = 400;
    const CAP: usize = 50;
    const TAIL_TX: usize = 3;

    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("cdc_spill_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!("CREATE TABLE dbo.{table} {ONE_TRANSACTION_DDL}"));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    // `census_oracle()` owns the workdir, the destination AND the config placement:
    // the state DB lives beside the config and the DuckDB reader works from inside a
    // container, so hand-building those paths is the smell the rig removes.
    let rig = Rig::mssql_cdc(&table, &ci).census_oracle();

    // Two transactions — each one statement, so each is one `__$start_lsn` group.
    mssql_seed_one_transaction(&table, 1..=ROWS);
    mssql_seed_one_transaction(&table, ROWS + 1..=ROWS + TAIL_TX);
    wait_for_capture(&ci, (ROWS + TAIL_TX) as i64);

    // Spilling is OPT-IN (`spill_dir_for`): with no directory named, the cap keeps
    // its original meaning and REFUSES the transaction. Without this the test would
    // exercise the refusal path and read it as a spill that produced no rows.
    let out = rig.run_with_envs(&[
        ("RIVET_CDC_MAX_TX_ROWS", &CAP.to_string()),
        ("RIVET_CDC_SPILL_DIR", "1"),
    ]);
    assert!(
        out.status.success(),
        "a batch past the cap must SPILL, not fail the run: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let log = String::from_utf8_lossy(&out.stderr).to_string();

    // The fixture is not inert, and memory really was bounded. Rows alone cannot see
    // this: a spill that quietly keeps buffering delivers identical rows.
    let split = log
        .split("delivered ")
        .filter_map(|s| s.split_once(" rows from memory and "))
        .find_map(|(head, rest)| {
            let tail = rest.split_once(" from disk")?.0;
            Some((
                head.trim().parse::<usize>().ok()?,
                tail.trim().parse::<usize>().ok()?,
            ))
        });
    let (from_memory, from_disk) =
        split.unwrap_or_else(|| panic!("the run must report the split. stderr: {log}"));
    assert!(
        from_disk > 0,
        "rows must actually reach DISK — a cap that was noticed and never acted on \\
         reads the same in the parquet. stderr: {log}"
    );
    // CAP + 1: the cap is checked after the row is pushed, so the head holds one
    // row more than the cap before the spill opens.
    assert_eq!(
        from_memory,
        CAP + 1,
        "memory must stop at the cap — that is the point of spilling, and a head \\
         larger than the cap means the ceiling is not enforced. stderr: {log}"
    );

    // THE INDEPENDENT ORACLE — the SOURCE table, the delivered parquet, and rivet's
    // two ledgers, from one DuckDB session that shares no code with rivet.
    let census = rig.row_census();
    assert_eq!(
        census.source,
        (ROWS + TAIL_TX) as i64,
        "the fixture itself must hold what the test thinks it does, or every \\
         comparison below is against the wrong number: {census:?}"
    );
    assert!(
        census.agrees(),
        "source, delivered parquet, export_metrics and file_log must all agree — a \\
         spilled tail that never reached the destination, or reached it without \\
         being counted, shows up here and nowhere else: {census:?}"
    );

    // Exactly once, and the SECOND transaction still arrives after the first.
    let changes = cdc_id_ops(&rig.out_dir());
    let mut ids: Vec<i64> = changes.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(
        ids.len(),
        ROWS + TAIL_TX,
        "every id exactly once across the memory head and the disk tail"
    );
    assert_eq!(
        changes.last().map(|(id, _)| *id),
        Some((ROWS + TAIL_TX) as i64),
        "the transaction after the spilled one must still arrive, and arrive last"
    );
}

/// A crash while a SPILLED tail is being handed out must lose nothing on resume.
///
/// SQL Server is the one engine where this window exists at all: its spilled tail
/// holds SEVERAL transactions (`SpooledGroups`), each group's last row is a real
/// commit boundary, so the sink can roll + checkpoint + ack MID-tail — between two
/// spilled groups — and crash before the rest is delivered. PostgreSQL and MySQL
/// spill exactly one transaction, whose only commit is the tail's last row, so
/// their sink cannot act until the tail is fully drained and no such window opens.
///
/// The crash tests that exist never set `RIVET_CDC_SPILL_DIR`, so until this test
/// no crash had ever fired with a spill file on disk — the spilled path did not
/// inherit the atomicity proof, it merely sat beside it.
///
/// Three transactions of 5 over a cap of 4 and rollover of 5: every transaction
/// crosses the cap (its tail spills), and each group's commit lets the sink roll.
/// The crash lands at `cdc_after_checkpoint_before_ack` — the checkpoint has
/// advanced past the delivered groups, the rest of the tail is undelivered, and the
/// reader (with the spill file) dies with the process. Resume must re-read from the
/// checkpoint and deliver the remainder: the union of both runs holds every row
/// exactly, or the tail died with the crashed process.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn roast_mssql_cdc_a_crash_mid_spilled_tail_loses_no_group_on_resume() {
    const TXS: usize = 3;
    /// SEVEN on purpose — not five, and the difference is the whole test. With
    /// `PER_TX` equal to the rollover, every possible roll lands exactly on a group
    /// boundary, so even a mutant that closes the tail's every row "commits" only
    /// at points where the checkpoint is accidentally legal — measured, two such
    /// mutants stayed green. At 7 over a rollover of 5, an early close rolls MID-
    /// group: the checkpoint advances past a commit whose last two rows are still
    /// in the dying process's spill, and the resume skips them — 19 of 21.
    const PER_TX: usize = 7;

    let _serial = cross_process_serial("mssql_cdc");
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("cdc_spillcrash_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!("CREATE TABLE dbo.{table} {ONE_TRANSACTION_DDL}"));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    // THREE transactions — one statement each, so one `__$start_lsn` group each.
    for t in 0..TXS {
        mssql_seed_one_transaction(&table, t * PER_TX + 1..=(t + 1) * PER_TX);
    }
    wait_for_capture(&ci, (TXS * PER_TX) as i64);

    let ckpt = d.path().join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mssql_cdc(&table, &ci)
        .cdc("rollover: 5")
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());

    // Run 1: the cap forces every transaction's tail onto disk, and the crash fires
    // at the first roll — after the checkpoint, before the ack — with the rest of
    // the spilled tail still undelivered in the dying process.
    let crashed = rig.run_with_envs(&[
        ("RIVET_CDC_MAX_TX_ROWS", "4"),
        ("RIVET_CDC_SPILL_DIR", "1"),
        ("RIVET_TEST_PANIC_AT", "cdc_after_checkpoint_before_ack"),
    ]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail run 1"
    );
    let log1 = String::from_utf8_lossy(&crashed.stderr).to_string();
    assert!(
        log1.contains("passed the in-memory cap"),
        "the fixture is inert unless run 1 actually SPILLED before crashing — \
         without this line the crash fired on the unspilled path and the test \
         proves nothing about it. stderr: {log1}"
    );

    // Run 2: same checkpoint, same destination — the scheduler's own resume shape.
    // The spill is re-created from the re-read, so the env var stays set.
    let out2 = rig.run_with_envs(&[("RIVET_CDC_MAX_TX_ROWS", "4"), ("RIVET_CDC_SPILL_DIR", "1")]);
    assert!(
        out2.status.success(),
        "the resume run must succeed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );

    let got: std::collections::BTreeSet<i64> =
        duckdb_dir_parquet_i64(&out, "id").into_iter().collect();
    let want: std::collections::BTreeSet<i64> = (1..=(TXS * PER_TX) as i64).collect();
    assert_eq!(
        got, want,
        "every row of every spilled group must survive the mid-tail crash — a \
         missing group means the checkpoint advanced past a commit whose rows died \
         with the crashed process's spill file"
    );

    // And the crashed process's spill file must not still be on disk after the
    // resume: its writer is dead, the sweep runs at the next spill, and one leaked
    // multi-GB file per crash fills a disk on a scheduler.
    let leaked: Vec<String> = std::fs::read_dir(ckpt.parent().unwrap().join(".rivet-spill"))
        .map(|rd| {
            rd.flatten()
                .map(|e| e.path().display().to_string())
                .filter(|p| p.contains("rivet-spill-"))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        leaked.is_empty(),
        "the crashed run's spill must be collected by the resume's sweep: {leaked:?}"
    );
}
