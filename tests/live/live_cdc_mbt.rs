//! Model-based test under CONCURRENT writers — the staff-review blind spot #1.
//!
//! Every other CDC test is a single writer issuing sequential transactions;
//! production is N sessions interleaving commits. This test runs 4 writer
//! threads doing randomized inserts/updates/deletes over a SHARED key range
//! (fixed RNG seed per thread — the op stream is deterministic, the
//! INTERLEAVING is whatever the server actually did), then checks the capture
//! against two independent truths:
//!
//! 1. **Conservation**: total captured events per op == Σ affected-rows the
//!    writers observed (MySQL reports affected per statement, so conflicts
//!    and no-op updates are counted exactly, not modeled).
//! 2. **Convergence**: replaying the captured events through the DOCUMENTED
//!    merge (last image per key in part/row order — same-`__pos` events
//!    resolve by order, later positions win) must reproduce the source
//!    table's final state EXACTLY — every surviving row's full image, and
//!    nothing else.
//!
//! If the sink ever reorders same-key images across the commit order, drops
//! a boundary, or routes an event to the wrong image, convergence breaks.

use std::collections::HashMap;

use arrow::array::{Array, Int64Array, StringArray};
use mysql::prelude::Queryable;

use crate::common::*;

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

fn server_id_for(tbl: &str) -> u32 {
    let h = tbl.bytes().fold(2_166_136_261u32, |a, b| {
        (a ^ b as u32).wrapping_mul(16_777_619)
    });
    10_000 + (h % 50_000)
}

/// Tiny deterministic PRNG (xorshift) — no rand dependency, fixed seeds.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq)]
struct OpCounts {
    inserts: i64,
    updates: i64,
    deletes: i64,
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_concurrent_writers_capture_converges_to_source_state() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_mbt");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .expect("pool")
        .get_conn()
        .expect("conn");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, v BIGINT NOT NULL, w VARCHAR(40) NOT NULL)"
    ))
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
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid}, rollover: 128 }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(&tbl),
    );
    let cfg = write_config(&d, &yaml);
    let run = || {
        let st = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(st.success());
    };
    run(); // pin

    // ── 4 concurrent writers, shared key range 1..=60, 250 ops each. ──────
    const KEYS: u64 = 60;
    const OPS: usize = 250;
    let handles: Vec<_> = (0..4u64)
        .map(|t| {
            let tbl = tbl.clone();
            std::thread::spawn(move || -> OpCounts {
                let mut c = mysql::Pool::new(MYSQL_CDC_URL)
                    .expect("pool")
                    .get_conn()
                    .expect("conn");
                let mut rng = Rng(0x9E3779B97F4A7C15 ^ (t + 1));
                let mut counts = OpCounts::default();
                for _ in 0..OPS {
                    let id = rng.below(KEYS) + 1;
                    let val = rng.below(1_000_000) as i64;
                    match rng.below(10) {
                        // 40%: upsert-style insert (fails silently on dup via
                        // IGNORE — affected==0 then, so conservation stays exact)
                        0..=3 => {
                            c.query_drop(format!(
                                "INSERT IGNORE INTO {tbl} VALUES ({id}, {val}, 'w{t}-{val}')"
                            ))
                            .unwrap();
                            counts.inserts += c.affected_rows() as i64;
                        }
                        // 40%: update (affected==0 when the row is absent OR
                        // the values are unchanged — both excluded from counts)
                        4..=7 => {
                            c.query_drop(format!(
                                "UPDATE {tbl} SET v = {val}, w = 'u{t}-{val}' WHERE id = {id}"
                            ))
                            .unwrap();
                            counts.updates += c.affected_rows() as i64;
                        }
                        // 20%: delete
                        _ => {
                            c.query_drop(format!("DELETE FROM {tbl} WHERE id = {id}"))
                                .unwrap();
                            counts.deletes += c.affected_rows() as i64;
                        }
                    }
                }
                counts
            })
        })
        .collect();
    let mut issued = OpCounts::default();
    for h in handles {
        let c = h.join().expect("writer thread");
        issued.inserts += c.inserts;
        issued.updates += c.updates;
        issued.deletes += c.deletes;
    }

    // The truth: the source table's final state.
    let source: HashMap<i64, (i64, String)> = c
        .query_map(
            format!("SELECT id, v, w FROM {tbl} ORDER BY id"),
            |(id, v, w)| (id, (v, w)),
        )
        .unwrap()
        .into_iter()
        .collect();

    run(); // capture the whole interleaved history

    // Replay in (filename, row) order — the documented merge.
    let mut parts: Vec<_> = std::fs::read_dir(&out)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    parts.sort();
    let mut merged: HashMap<i64, (i64, String)> = HashMap::new();
    let mut captured = OpCounts::default();
    for p in parts {
        let f = std::fs::File::open(&p).unwrap();
        let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            let b = b.unwrap();
            let op = b
                .column(b.schema().index_of("__op").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            let id = b
                .column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .clone();
            let v = b
                .column(b.schema().index_of("v").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let w = b
                .column(b.schema().index_of("w").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            for r in 0..b.num_rows() {
                let key = id.value(r) as i64;
                match op.value(r) {
                    "insert" => {
                        captured.inserts += 1;
                        merged.insert(key, (v.value(r), w.value(r).to_string()));
                    }
                    "update" => {
                        captured.updates += 1;
                        merged.insert(key, (v.value(r), w.value(r).to_string()));
                    }
                    "delete" => {
                        captured.deletes += 1;
                        merged.remove(&key);
                    }
                    other => panic!("unexpected op {other}"),
                }
            }
        }
    }

    // 1) Conservation: exactly the affected-row counts the writers observed.
    assert_eq!(
        (captured.inserts, captured.updates, captured.deletes),
        (issued.inserts, issued.updates, issued.deletes),
        "captured event counts must equal the writers' affected-row counts"
    );
    // 2) Convergence: the merge reproduces the source's final state exactly.
    assert_eq!(
        merged.len(),
        source.len(),
        "merged key-set size must match the source"
    );
    for (k, sv) in &source {
        assert_eq!(
            merged.get(k),
            Some(sv),
            "key {k}: merged image must equal the source's final row"
        );
    }
}

// Staff class #2 — the fault-point SWEEP: not five handpicked gremlins but a
// panic at EVERY phase boundary of the CDC path, parametrized. For each point:
// the faulted run must FAIL LOUDLY, and the clean retry must leave the union
// of parts holding every row (overlap allowed — at-least-once — gap never).
// bug_002 lived at exactly the one loop point nobody had picked by hand.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_fault_point_sweep_every_phase_boundary_recovers() {
    const POINTS: &[&str] = &[
        "cdc_after_open",
        "cdc_before_resolve",
        "cdc_after_flush_before_ack",
        "cdc_after_checkpoint_before_ack",
        "cdc_after_ack",
        "cdc_before_manifest",
    ];
    for point in POINTS {
        let d = tempfile::tempdir().unwrap();
        let tbl = unique_name("cdc_sweep");
        let mut c = mysql::Pool::new(MYSQL_CDC_URL)
            .expect("pool")
            .get_conn()
            .expect("conn");
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
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid}, rollover: 10 }}
    destination: {{ type: local, path: "{out}" }}
"#,
            ckpt = ckpt.display(),
            out = out.display(),
            sid = server_id_for(&tbl),
        );
        let cfg = write_config(&d, &yaml);
        // Pin cleanly, then a 30-row backlog (3 parts at rollover 10).
        let st = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(st.success());
        let vals: Vec<String> = (1..=30).map(|i| format!("({i}, {i})")).collect();
        c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
            .unwrap();

        // Faulted run: must crash (loudly), never report success.
        let res = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap()])
            .env("RIVET_TEST_PANIC_AT", point)
            .output()
            .unwrap();
        assert!(
            !res.status.success(),
            "{point}: the faulted run must fail loudly"
        );

        // Clean retries close the gap.
        let mut ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for _ in 0..3 {
            let st = std::process::Command::new(RIVET_BIN)
                .args(["run", "--config", cfg.to_str().unwrap()])
                .status()
                .unwrap();
            assert!(st.success(), "{point}: recovery run must succeed");
            ids = distinct_int_ids(&out);
            if ids.len() >= 30 {
                break;
            }
        }
        assert_eq!(
            ids.len(),
            30,
            "{point}: union of parts must hold every row after recovery"
        );
    }
}

/// Distinct Int32 `id`s across every part under `out`.
fn distinct_int_ids(out: &std::path::Path) -> std::collections::HashSet<i64> {
    let mut ids = std::collections::HashSet::new();
    for e in std::fs::read_dir(out).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_none_or(|x| x != "parquet") {
            continue;
        }
        let f = std::fs::File::open(&p).unwrap();
        let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            let b = b.unwrap();
            let id = b
                .column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .clone();
            for r in 0..b.num_rows() {
                ids.insert(id.value(r) as i64);
            }
        }
    }
    ids
}

// Staff class #7 — OBSERVABILITY contracts: it is not enough that a fault
// state fails loudly at run time; the operator's standing sensor (`rivet
// doctor`) must SEE it before/without a run. One fault state per engine
// sensor family, asserted against doctor --json.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn doctor_sees_the_fault_states_the_gremlins_create() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_obs");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .expect("pool")
        .get_conn()
        .expect("conn");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    // Fault state: a checkpoint pointing at a PURGED binlog file — the
    // stalled/expired-resume condition the retention gremlins create.
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::write(&ckpt, r#"{"file":"binlog.000000","pos":4}"#).unwrap();
    // (binlog.000000 never exists — MySQL numbering starts at 000001 — so
    // this is the purged-past-retention state regardless of instance age;
    // the first attempt used 000001 and doctor CORRECTLY reported it healthy,
    // because the shared instance still retains its first file.)
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
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
        sid = server_id_for(&tbl),
    );
    let cfg = write_config(&d, &yaml);

    let doc = std::process::Command::new(RIVET_BIN)
        .args(["doctor", "--config", cfg.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    let j: serde_json::Value = serde_json::from_slice(&doc.stdout).expect("doctor --json parses");
    let checks = j["checks"].as_array().expect("checks array");
    // The sensor contract: SOME cdc check must be non-ok and name the binlog
    // retention/checkpoint problem — the operator alerts on all_ok=false.
    assert_eq!(
        j["all_ok"], false,
        "doctor must flag a checkpoint behind retention: {j}"
    );
    let flagged = checks.iter().any(|ch| {
        ch["ok"] == false
            && ch["name"]
                .as_str()
                .is_some_and(|n| n.contains("cdc") || n.contains("binlog"))
    });
    assert!(
        flagged,
        "a cdc/binlog-named check must carry the failure: {j}"
    );

    // And the healthy baseline: with a fresh checkpoint doctor is green again
    // (the sensor has no stuck-at-fail failure mode).
    std::fs::remove_file(&ckpt).unwrap();
    let doc = std::process::Command::new(RIVET_BIN)
        .args(["doctor", "--config", cfg.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    let j: serde_json::Value = serde_json::from_slice(&doc.stdout).expect("doctor --json parses");
    assert_eq!(j["all_ok"], true, "healthy state must read green: {j}");
}
