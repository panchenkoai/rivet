//! Property-based CDC harness — the fundamental CDC correctness invariant:
//! **replaying the captured changes reconstructs the source's final state.**
//!
//! proptest generates a random sequence of upserts/deletes on a small key space
//! (so updates and deletes overlap), applies it to MySQL, captures it via CDC, then
//! replays the `__op` rows (insert/update → upsert, delete → remove) into a model
//! and asserts the model equals the source table. Over many generated sequences
//! this catches whole classes of capture/ordering bugs a single example can't.
//!
//! Gated `#[ignore]`: needs the `cdc` profile's `mysql-cdc` (:3307). Run with:
//!     docker compose --profile cdc up -d mysql-cdc
//!     cargo test --test live_cdc_property -- --ignored

mod common;

use std::collections::HashMap;

use common::*;
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

/// A `server_id` outside `live_cdc.rs`'s `server_id_for` range (10_000..60_000) so
/// this harness's binlog connection never collides with the other CDC tests.
const SERVER_ID: u32 = 9_999;

fn cdc_config(
    d: &tempfile::TempDir,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: cdc_prop
    table: cdc_prop
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {SERVER_ID} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
    );
    write_config(d, &yaml)
}

/// Read the captured changes as `(__op, id, v)`, in file order (= `__pos` order).
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

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    #[ignore = "live: requires docker compose --profile cdc mysql-cdc (:3307)"]
    fn cdc_replay_reconstructs_source(ops in prop::collection::vec(op_strategy(), 1..40)) {
        let pool = mysql::Pool::new(MYSQL_CDC_URL).expect("mysql-cdc pool");
        let mut c = pool.get_conn().expect("conn");
        c.query_drop("CREATE TABLE IF NOT EXISTS cdc_prop (id INT PRIMARY KEY, v INT)").unwrap();
        c.query_drop("DELETE FROM cdc_prop").unwrap();

        // Checkpoint at the current position (AFTER clearing) so the capture sees
        // only this case's ops, not the clear.
        let d = tempfile::tempdir().unwrap();
        let ckpt = d.path().join("ckpt");
        let row: mysql::Row = c.query_first("SHOW MASTER STATUS").unwrap().unwrap();
        let (file, pos): (String, u64) = (row.get(0).unwrap(), row.get(1).unwrap());
        std::fs::write(&ckpt, format!(r#"{{"file":"{file}","pos":{pos}}}"#)).unwrap();

        // Apply the ops, building the expected final state in lockstep.
        let mut expected: HashMap<u32, i32> = HashMap::new();
        for op in &ops {
            match *op {
                Op::Upsert(id, v) => {
                    c.exec_drop(
                        "INSERT INTO cdc_prop (id, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = VALUES(v)",
                        (id, v),
                    ).unwrap();
                    expected.insert(id, v);
                }
                Op::Delete(id) => {
                    c.exec_drop("DELETE FROM cdc_prop WHERE id = ?", (id,)).unwrap();
                    expected.remove(&id);
                }
            }
        }

        // Capture.
        let out = d.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let res = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cdc_config(&d, &ckpt, &out).to_str().unwrap()])
            .output()
            .expect("spawn rivet");
        prop_assert!(
            res.status.success(),
            "cdc run failed:\n{}",
            String::from_utf8_lossy(&res.stderr)
        );

        // Replay the captured changes the way a downstream MERGE would.
        let mut replayed: HashMap<u32, i32> = HashMap::new();
        for (op, id, v) in read_cdc_rows(&out) {
            match op.as_str() {
                "insert" | "update" => { replayed.insert(id as u32, v.expect("insert/update carries v")); }
                "delete" => { replayed.remove(&(id as u32)); }
                other => prop_assert!(false, "unexpected __op {other:?}"),
            }
        }

        prop_assert_eq!(
            &replayed, &expected,
            "replaying the captured changes must reconstruct the source's final state; ops = {:?}",
            ops
        );
    }
}
