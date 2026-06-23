//! Live CDC regression — locks the invariants the CDC build kept getting wrong:
//! at-least-once resume (no gap, no dup) and the run being recorded in the state
//! DB (metric + journal) like a batch export.
//!
//! Gated `#[ignore]` like the other `live_*` tests — needs the docker compose
//! `mysql` (binlog ROW + a REPLICATION grant for the `rivet` user, which the dev
//! stack already has). Run with:
//!     cargo test --test live_cdc -- --ignored

mod common;

use common::*;
use mysql::prelude::Queryable;

/// A throwaway table dropped on `Drop`, so a panic mid-test still cleans up.
struct Table(String);
impl Drop for Table {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

fn conn() -> mysql::PooledConn {
    mysql::Pool::new(MYSQL_URL)
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
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true }}
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
