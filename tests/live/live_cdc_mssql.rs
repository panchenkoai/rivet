//! Live SQL Server CDC regression — at-least-once resume.
//!
//! Gated `#[ignore]`: needs the dedicated `mssql-cdc` engine (the `cdc` profile,
//! :1434) with SQL Server Agent running — the capture job copies committed changes
//! into `cdc.<instance>_CT` asynchronously. Run with:
//!     docker compose --profile cdc up -d mssql-cdc
//!     cargo test --test live_suite -- --ignored

use std::time::Duration;

use crate::common::*;

/// CDC enable/disable mutates database-global metadata + a shared capture job, so
/// the two tests must not run concurrently (cargo runs tests in parallel).
static CDC_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Enable CDC on the database (idempotent) + the table, creating capture instance
/// `ci`. The capture job (SQL Server Agent) then populates `cdc.<ci>_CT`.
fn enable_cdc(table: &str, ci: &str) {
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
}

/// Block until the capture job has copied at least `want` rows into the change
/// table — the job runs asynchronously, so the test must wait for it.
fn wait_for_capture(ci: &str, want: i64) {
    for _ in 0..60 {
        if mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT")) >= want {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("capture job did not populate cdc.{ci}_CT to {want} rows in 30s");
}

fn mssql_cdc_config(
    d: &tempfile::TempDir,
    table: &str,
    ci: &str,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = Rig::mssql_cdc(table, ci)
        .checkpoint_path(ckpt.to_path_buf())
        .dest_path(out.to_path_buf())
        .yaml();
    write_config(d, &yaml)
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_resume_captures_only_new_changes() {
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 2, "run 1 captures the 2 changes");

    // Resume: the checkpoint advanced past the first two, so run 2 must capture ONLY
    // the two new changes — not re-read all four from the change table's min LSN.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30),(4,40)"));
    wait_for_capture(&ci, 4);
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume must capture only the 2 new changes (LSN resume), not re-read all 4"
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
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 0, "idle run 1 captures nothing");

    // A change lands BETWEEN the idle run and the next scheduler cycle.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10)"));
    wait_for_capture(&ci, 1);

    // Run 2 must capture it — never skip past it to the current max LSN.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        1,
        "the change between an idle run and the next run must be captured, not skipped"
    );
}

// Conformance: stream-property commit boundary + qualified `table:` routing,
// SQL Server flavour. MSSQL stamps committed=true per change-table row, so
// the MySQL stall cannot occur structurally; `dbo.<t>` must route.
#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_mixed_transaction_and_qualified_table_conformance() {
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &qualified, &ci_o, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 1, "qualified table: must capture");

    // And the checkpoint advanced past the mixed transaction.
    run_rivet_ok(&mssql_cdc_config(&d, &qualified, &ci_o, &ckpt, &out2));
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
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out1));
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out2));
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out3));
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
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out));

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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &upd_out));
    run_rivet_ok(&mssql_full_config(&d, &table, &batch_out));
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &del_out));
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
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let d = tempfile::tempdir().unwrap();
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
    let cfg = write_config(&d, &rig.yaml());

    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out.join("snapshot")), 2);
    assert_eq!(
        manifest_rows(&out),
        0,
        "anchor at max LSN ⇒ nothing to drain"
    );

    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30)"));
    wait_for_capture(&ci, 3);
    run_rivet_ok(&cfg);
    assert_eq!(manifest_rows(&out), 1, "the post-snapshot change streams");
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
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &cdc_out));
    run_rivet_ok(&mssql_full_config(&d, &table, &batch_out));

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
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out));
    assert_eq!(
        manifest_rows(&out),
        2,
        "events must be routed by the REAL table name (from cdc.change_tables), \
         not by parsing the capture-instance name"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_crash_before_checkpoint_re_reads_on_resume() {
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 2);

    // Two more changes; run crashes after the part is durable, before the checkpoint.
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30),(4,40)"));
    wait_for_capture(&ci, 4);
    let crash_out = d.path().join("crash");
    std::fs::create_dir_all(&crash_out).unwrap();
    let crashed = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            mssql_cdc_config(&d, &table, &ci, &ckpt, &crash_out)
                .to_str()
                .unwrap(),
        ])
        .env("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")
        .output()
        .expect("spawn rivet");
    assert!(
        !crashed.status.success(),
        "the injected crash must fail the run"
    );

    // The checkpoint stayed at change 2, so the resume re-reads exactly 3 and 4 —
    // not lost (would be 0 if the checkpoint had advanced) and not all four.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "crash before the checkpoint → resume re-reads exactly the 2 un-checkpointed changes"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_datetimeoffset_value_is_preserved() {
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out));

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
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out));
    assert!(
        parquet_col0_present(&out, "u"),
        "uniqueidentifier must be captured (16 canonical bytes), not dropped to NULL"
    );
}

fn mssql_full_config(
    d: &tempfile::TempDir,
    table: &str,
    out: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = Rig::mssql_batch(&format!("{table}_batch"))
        .source_url(MSSQL_CDC_URL)
        .query(&format!("SELECT * FROM dbo.{table}"))
        .dest_path(out.to_path_buf())
        .yaml();
    write_config(d, &yaml)
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_full_type_matrix_matches_batch() {
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    run_rivet_ok(&mssql_cdc_config(&d, &table, &ci, &ckpt, &cdc_out));
    run_rivet_ok(&mssql_full_config(&d, &table, &batch_out));

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
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_resume_past_retention_errors_not_a_silent_gap() {
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
    let res = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            mssql_cdc_config(&d, &table, &ci, &ckpt, &out)
                .to_str()
                .unwrap(),
        ])
        .output()
        .expect("spawn rivet");
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
