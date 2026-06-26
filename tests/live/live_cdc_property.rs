//! Property-based CDC harness — the fundamental CDC correctness invariants,
//! generatively, over random op sequences:
//!
//! 1. `cdc_replay_reconstructs_source` — replaying the captured changes (MERGE by
//!    PK + `__op`) reconstructs the source table's final state.
//! 2. `cdc_replay_reconstructs_after_a_crash` — the same, but the first capture is
//!    crashed mid-flight (before the checkpoint advances); the resume re-reads and
//!    still reconstructs. The at-least-once guarantee, generatively.
//!
//! Gated `#[ignore]`: needs the `cdc` profile's `mysql-cdc` (:3307). Run with:
//!     docker compose --profile cdc up -d mysql-cdc
//!     cargo test --test live_suite -- --ignored

use std::collections::HashMap;

use crate::common::*;
use mysql::prelude::Queryable;
use proptest::prelude::*;

/// A generated operation on the `(id, v)` table.
#[derive(Debug, Clone)]
enum Op {
    Upsert(u32, i32),
    Delete(u32),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // id ∈ 1..8 keeps the key space small so upserts hit existing rows (updates)
    // and deletes hit live rows — exercising all three ops, not just inserts.
    prop_oneof![
        (1u32..8, any::<i32>()).prop_map(|(id, v)| Op::Upsert(id, v)),
        (1u32..8).prop_map(Op::Delete),
    ]
}

fn cdc_config(
    d: &tempfile::TempDir,
    table: &str,
    server_id: u32,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {table}
    table: {table}
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {server_id} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
    );
    write_config(d, &yaml)
}

/// Clear `table`, checkpoint at the current binlog position, then apply `ops` and
/// return the temp dir, the checkpoint path, and the source's expected final state.
fn setup_and_apply(
    table: &str,
    ops: &[Op],
) -> (tempfile::TempDir, std::path::PathBuf, HashMap<u32, i32>) {
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql-cdc pool")
        .get_conn()
        .expect("conn");
    c.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY, v INT)"
    ))
    .unwrap();
    c.query_drop(format!("DELETE FROM {table}")).unwrap();

    // Checkpoint AFTER clearing, so the capture sees only this case's ops.
    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ckpt");
    let row: mysql::Row = c.query_first("SHOW MASTER STATUS").unwrap().unwrap();
    let (file, pos): (String, u64) = (row.get(0).unwrap(), row.get(1).unwrap());
    std::fs::write(&ckpt, format!(r#"{{"file":"{file}","pos":{pos}}}"#)).unwrap();

    let mut expected: HashMap<u32, i32> = HashMap::new();
    for op in ops {
        match *op {
            Op::Upsert(id, v) => {
                c.exec_drop(
                    format!("INSERT INTO {table} (id, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = VALUES(v)"),
                    (id, v),
                )
                .unwrap();
                expected.insert(id, v);
            }
            Op::Delete(id) => {
                c.exec_drop(format!("DELETE FROM {table} WHERE id = ?"), (id,))
                    .unwrap();
                expected.remove(&id);
            }
        }
    }
    (d, ckpt, expected)
}

/// Read the captured changes as `(__op, id, v)` in file order (= `__pos` order).
fn read_cdc_rows(dir: &std::path::Path) -> Vec<(String, i32, Option<i32>)> {
    use arrow::array::{Array, Int32Array, StringArray};
    let Some(part) = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|x| x == "parquet"))
    else {
        return vec![]; // a run that captured nothing writes no part
    };
    let f = std::fs::File::open(part).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = |name: &str| batch.column(batch.schema().index_of(name).unwrap());
        let op = col("__op").as_any().downcast_ref::<StringArray>().unwrap();
        let id = col("id").as_any().downcast_ref::<Int32Array>().unwrap();
        let v = col("v").as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..batch.num_rows() {
            out.push((
                op.value(i).to_string(),
                id.value(i),
                (!v.is_null(i)).then(|| v.value(i)),
            ));
        }
    }
    out
}

/// Replay captured `(__op, id, v)` rows the way a downstream MERGE would.
fn replay(rows: Vec<(String, i32, Option<i32>)>) -> HashMap<u32, i32> {
    let mut m = HashMap::new();
    for (op, id, v) in rows {
        match op.as_str() {
            "insert" | "update" => {
                m.insert(id as u32, v.expect("insert/update carries v"));
            }
            "delete" => {
                m.remove(&(id as u32));
            }
            other => panic!("unexpected __op {other:?}"),
        }
    }
    m
}

fn run_cdc(cfg: &std::path::Path, crash: bool) -> std::process::Output {
    let mut cmd = std::process::Command::new(RIVET_BIN);
    cmd.args(["run", "--config", cfg.to_str().unwrap()]);
    if crash {
        cmd.env("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack");
    }
    cmd.output().expect("spawn rivet")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    #[ignore = "live: requires docker compose --profile cdc mysql-cdc (:3307)"]
    fn cdc_replay_reconstructs_source(ops in prop::collection::vec(op_strategy(), 1..40)) {
        let (d, ckpt, expected) = setup_and_apply("cdc_prop", &ops);
        let out = d.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let res = run_cdc(&cdc_config(&d, "cdc_prop", 9_999, &ckpt, &out), false);
        prop_assert!(res.status.success(), "cdc run failed:\n{}", String::from_utf8_lossy(&res.stderr));
        prop_assert_eq!(
            &replay(read_cdc_rows(&out)), &expected,
            "replaying the captured changes must reconstruct the source's final state; ops = {:?}", ops
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    #[test]
    #[ignore = "live: requires docker compose --profile cdc mysql-cdc (:3307)"]
    fn cdc_replay_reconstructs_after_a_crash(rest in prop::collection::vec(op_strategy(), 0..30)) {
        // Prepend a guaranteed change so the crash point (after the part is durable,
        // before the checkpoint) is actually reached.
        let mut ops = vec![Op::Upsert(1, 0)];
        ops.extend(rest);
        let (d, ckpt, expected) = setup_and_apply("cdc_prop_crash", &ops);

        // Run 1 crashes after the part is durable but before the checkpoint advances.
        let crash_out = d.path().join("crash");
        std::fs::create_dir_all(&crash_out).unwrap();
        let crashed = run_cdc(&cdc_config(&d, "cdc_prop_crash", 9_998, &ckpt, &crash_out), true);
        prop_assert!(!crashed.status.success(), "the injected crash must fail run 1");

        // Run 2 (clean): the checkpoint never advanced, so it re-reads everything —
        // at-least-once, nothing lost — and the replay reconstructs the source.
        let out = d.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let res = run_cdc(&cdc_config(&d, "cdc_prop_crash", 9_998, &ckpt, &out), false);
        prop_assert!(res.status.success(), "resume run failed:\n{}", String::from_utf8_lossy(&res.stderr));
        prop_assert_eq!(
            &replay(read_cdc_rows(&out)), &expected,
            "a crash before the checkpoint must lose nothing — the resume reconstructs the source; ops = {:?}", ops
        );
    }
}
