//! Live SQL Server CDC regression — at-least-once resume.
//!
//! Gated `#[ignore]`: needs the docker `mssql` service with SQL Server Agent
//! running (the capture job copies committed changes into `cdc.<instance>_CT`
//! asynchronously) and CDC enabled. Run with:
//!     cargo test --test live_cdc_mssql -- --ignored

mod common;

use std::time::Duration;

use common::*;

/// CDC enable/disable mutates database-global metadata + a shared capture job, so
/// the two tests must not run concurrently (cargo runs tests in parallel).
static CDC_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Enable CDC on the database (idempotent) + the table, creating capture instance
/// `ci`. The capture job (SQL Server Agent) then populates `cdc.<ci>_CT`.
fn enable_cdc(table: &str, ci: &str) {
    mssql_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
}

/// Drops the capture instance + the table on teardown (a CDC-tracked table can't
/// just be dropped — its change table would be orphaned). Panic-safe in `Drop`.
struct CdcTable {
    table: String,
    ci: String,
}
impl Drop for CdcTable {
    fn drop(&mut self) {
        let (table, ci) = (self.table.clone(), self.ci.clone());
        let _ = std::panic::catch_unwind(move || {
            mssql_exec(&format!(
                "IF EXISTS(SELECT 1 FROM cdc.change_tables ct JOIN sys.tables t \
                   ON ct.source_object_id=t.object_id WHERE t.name='{table}') \
                 EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'{table}', \
                 @capture_instance=N'{ci}';"
            ));
            mssql_drop_table(&format!("dbo.{table}"));
        });
    }
}

/// Block until the capture job has copied at least `want` rows into the change
/// table — the job runs asynchronously, so the test must wait for it.
fn wait_for_capture(ci: &str, want: i64) {
    for _ in 0..60 {
        if mssql_query_i64(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT")) >= want {
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
    let yaml = format!(
        r#"source: {{type: mssql, url: "{MSSQL_URL}"}}
exports:
  - name: {table}
    table: {table}
    mode: cdc
    format: parquet
    cdc: {{ capture_instance: {ci}, checkpoint: "{ckpt}" }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
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

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_resume_captures_only_new_changes() {
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_ms");
    let ci = format!("dbo_{table}");
    mssql_drop_table(&format!("dbo.{table}"));
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = CdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let ckpt = d.path().join("cdc.ckpt");
    mssql_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_cdc(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 2, "run 1 captures the 2 changes");

    // Resume: the checkpoint advanced past the first two, so run 2 must capture ONLY
    // the two new changes — not re-read all four from the change table's min LSN.
    mssql_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30),(4,40)"));
    wait_for_capture(&ci, 4);
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    run_cdc(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "resume must capture only the 2 new changes (LSN resume), not re-read all 4"
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
    mssql_drop_table(&format!("dbo.{table}"));
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = CdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");

    // Establish the checkpoint at the first two changes.
    mssql_exec(&format!("INSERT INTO dbo.{table} VALUES (1,10),(2,20)"));
    wait_for_capture(&ci, 2);
    let out1 = d.path().join("out1");
    std::fs::create_dir_all(&out1).unwrap();
    run_cdc(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out1));
    assert_eq!(manifest_rows(&out1), 2);

    // Two more changes; run crashes after the part is durable, before the checkpoint.
    mssql_exec(&format!("INSERT INTO dbo.{table} VALUES (3,30),(4,40)"));
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
    run_cdc(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out2));
    assert_eq!(
        manifest_rows(&out2),
        2,
        "crash before the checkpoint → resume re-reads exactly the 2 un-checkpointed changes"
    );
}

/// The single string value under `col` in the one `.parquet` part.
fn parquet_one_string(dir: &std::path::Path, col: &str) -> String {
    use arrow::array::{Array, StringArray};
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
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column")
        .value(0)
        .to_string()
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC"]
fn mssql_cdc_datetimeoffset_value_is_preserved() {
    let _serial = CDC_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // datetimeoffset is tz-aware: it must be captured (as the UTC instant in text),
    // never silently dropped. The adapter used to try_get it as NaiveDateTime — the
    // wrong type — which returned None → an empty/null value (data loss).
    let d = tempfile::tempdir().unwrap();
    let table = unique_name("rivet_cdc_dto");
    let ci = format!("dbo_{table}");
    mssql_drop_table(&format!("dbo.{table}"));
    mssql_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, dto DATETIMEOFFSET)"
    ));
    enable_cdc(&table, &ci);
    let _guard = CdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let ckpt = d.path().join("cdc.ckpt");
    // 10:00 at +05:30 is 04:30:00 UTC — the instant that must survive.
    mssql_exec(&format!(
        "INSERT INTO dbo.{table} VALUES (1, '2026-06-23 10:00:00 +05:30')"
    ));
    wait_for_capture(&ci, 1);
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_cdc(&mssql_cdc_config(&d, &table, &ci, &ckpt, &out));

    // Lossless: the local time + its offset both survive (RFC3339), so the UTC
    // instant (04:30) is recoverable and the zone is never silently dropped.
    let dto = parquet_one_string(&out, "dto");
    assert!(
        dto.contains("10:00:00+05:30"),
        "datetimeoffset must be captured losslessly with its zone, not dropped — got {dto:?}"
    );
}
