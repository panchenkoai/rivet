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

// Finding #37 — DDL inside the capture window. Binlog row events are
// POSITIONAL and carry no column names; after `DROP COLUMN a` mid-window the
// pre-DDL event's 'a' value landed in column 'b' (observed live, silently,
// status success). The sink now refuses arity drift loudly. Three shapes:
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_mid_window_ddl_fails_loudly_never_misaligns() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_ddl_mid");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, a VARCHAR(10), b VARCHAR(10))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());
    let (cfg, out) = ddl_cfg(&d, &tbl);
    run_ok(&cfg); // pin
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,'AAA','BBB')"))
        .unwrap();
    c.query_drop(format!("ALTER TABLE {tbl} DROP COLUMN a"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} (id,b) VALUES (2,'CCC')"))
        .unwrap();

    let res = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!res.status.success(), "mid-window DDL must fail the run");
    let err = String::from_utf8_lossy(&res.stderr);
    assert!(
        err.contains("WRONG columns"),
        "the failure must explain the misalignment risk: {err}"
    );
    let parts = std::fs::read_dir(&out)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "parquet")
        })
        .count();
    assert_eq!(parts, 0, "no misaligned part may reach the destination");
}

// DDL BETWEEN runs is fine by construction: each run resolves fresh.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_ddl_between_runs_is_captured_with_each_runs_schema() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_ddl_btw");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, a VARCHAR(10), b VARCHAR(10))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());
    let (cfg, out) = ddl_cfg(&d, &tbl);
    run_ok(&cfg); // pin
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,'AAA','BBB')"))
        .unwrap();
    run_ok(&cfg); // captures the 3-column shape
    c.query_drop(format!("ALTER TABLE {tbl} DROP COLUMN a"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} (id,b) VALUES (2,'CCC')"))
        .unwrap();
    run_ok(&cfg); // captures the 2-column shape
    // Both rows present across parts; the post-DDL row is well-formed.
    let ids = distinct_int_ids(&out);
    assert_eq!(ids.len(), 2, "both shapes captured across their runs");
}

// RENAME is positionally safe: same arity, values stay in their columns.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_mid_window_rename_is_positionally_safe() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_ddl_ren");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, a VARCHAR(10))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());
    let (cfg, out) = ddl_cfg(&d, &tbl);
    run_ok(&cfg); // pin
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,'AAA')"))
        .unwrap();
    c.query_drop(format!("ALTER TABLE {tbl} RENAME COLUMN a TO a2"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2,'ZZZ')"))
        .unwrap();
    run_ok(&cfg);
    // Both values land under the RESOLVED (new) name, positions intact.
    use arrow::array::StringArray;
    let mut vals = Vec::new();
    for b in read_all_parts(&out) {
        let a2 = b
            .column(b.schema().index_of("a2").expect("resolved name a2"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for r in 0..b.num_rows() {
            vals.push(a2.value(r).to_string());
        }
    }
    assert_eq!(
        vals,
        vec!["AAA", "ZZZ"],
        "values stay in their column across a rename"
    );
}

fn ddl_cfg(d: &tempfile::TempDir, tbl: &str) -> (std::path::PathBuf, std::path::PathBuf) {
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
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(tbl),
    );
    (write_config(d, &yaml), out)
}

fn run_ok(cfg: &std::path::Path) {
    let st = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(st.success());
}

fn read_all_parts(out: &std::path::Path) -> Vec<arrow::record_batch::RecordBatch> {
    let mut parts: Vec<_> = std::fs::read_dir(out)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    parts.sort();
    let mut out_b = Vec::new();
    for p in parts {
        let f = std::fs::File::open(&p).unwrap();
        let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            out_b.push(b.unwrap());
        }
    }
    out_b
}

// Finding #38 — Form B was a silent no-op for CDC: the manifest recorded
// `column_checksums: None`, so `rivet validate` skipped the value leg on CDC
// prefixes while looking green. The sink already computed the arrow-side sum
// per column per part (Form A); it now XOR-accumulates them into the
// manifest, and validate's re-read must agree — or name the column.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_manifest_records_form_b_checksums_and_validate_verifies_them() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_formb");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, v VARCHAR(20))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());
    let (cfg, out) = ddl_cfg(&d, &tbl);
    run_ok(&cfg); // pin
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1,'aaa'),(2,'bbb')"))
        .unwrap();
    run_ok(&cfg);

    let manifest_path = out.join("manifest.json");
    let m: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    let sums = m["column_checksums"]
        .as_array()
        .expect("CDC manifest must record column_checksums (finding #38)");
    assert!(
        sums.iter().any(|c| c["name"] == "v"),
        "data columns are recorded: {sums:?}"
    );

    // Clean validate passes…
    let ok = std::process::Command::new(RIVET_BIN)
        .args(["validate", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        ok.status.success(),
        "clean validate must pass:\n{}",
        String::from_utf8_lossy(&ok.stderr)
    );

    // …and a tampered recorded sum must fail, naming the column.
    let mut m2 = m.clone();
    for csum in m2["column_checksums"].as_array_mut().unwrap() {
        if csum["name"] == "v" {
            csum["checksum"] = serde_json::Value::String("1".into());
        }
    }
    std::fs::write(&manifest_path, serde_json::to_string_pretty(&m2).unwrap()).unwrap();
    let bad = std::process::Command::new(RIVET_BIN)
        .args(["validate", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        !bad.status.success(),
        "a checksum mismatch must fail validate"
    );
    let err = format!(
        "{}{}",
        String::from_utf8_lossy(&bad.stdout),
        String::from_utf8_lossy(&bad.stderr)
    );
    // Column naming is pinned at the unit level
    // (verify_fails_on_mismatch_naming_the_column); here the contract is:
    // the run fails AND the failure is checksum-shaped.
    assert!(
        err.to_lowercase().contains("checksum") || err.contains("INCONSISTENT"),
        "the failure must be checksum-shaped: {err}"
    );
}

// Composition-of-everything: multi-table × `initial: snapshot` × qualified
// `columns:` override × a CLOUD destination (fake-gcs) — every headline
// feature of the branch enabled at once. The initial-snapshot markers on GCS
// were the one leg never live-tested (head("_SUCCESS") over the emulator).
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + fake-gcs"]
fn cdc_all_features_combined_on_gcs() {
    let d = tempfile::tempdir().unwrap();
    let ta = unique_name("cdc_all_a");
    let tb = unique_name("cdc_all_b");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    for t in [&ta, &tb] {
        c.query_drop(format!("DROP TABLE IF EXISTS {t}")).unwrap();
        c.query_drop(format!(
            "CREATE TABLE {t} (id INT PRIMARY KEY, v BIGINT UNSIGNED)"
        ))
        .unwrap();
    }
    let (_g1, _g2) = (Table(ta.clone()), Table(tb.clone()));
    // Pre-existing rows — the snapshot's job.
    c.query_drop(format!("INSERT INTO {ta} VALUES (1, 10)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tb} VALUES (1, 18446744073709551615)"))
        .unwrap();

    let bucket = "rivet-qa-cdc-gcs";
    ensure_gcs_bucket(bucket);
    let prefix = unique_name("allfeat");
    let ckpt = d.path().join("cdc.ckpt");
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: all_features
    tables: [{ta}, {tb}]
    mode: cdc
    format: parquet
    columns: {{ "{tb}.v": "decimal(20,0)" }}
    cdc: {{ initial: snapshot, checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
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
    let run = || {
        let st = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(st.success());
    };
    run(); // anchor → per-table snapshots on GCS → drain(0)

    let list = || -> Vec<String> {
        let body = reqwest::blocking::get(format!(
            "{FAKE_GCS_ENDPOINT}/storage/v1/b/{bucket}/o?prefix={prefix}"
        ))
        .unwrap()
        .text()
        .unwrap();
        let j: serde_json::Value = serde_json::from_str(&body).unwrap();
        j["items"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|i| i["name"].as_str().unwrap().to_string())
                    .collect()
            })
            .unwrap_or_default()
    };
    let keys = list();
    for t in [&ta, &tb] {
        assert!(
            keys.iter()
                .any(|k| k.contains(&format!("{t}/snapshot/")) && k.ends_with("_SUCCESS")),
            "table {t}: snapshot marker must land on GCS: {keys:?}"
        );
    }

    // Stream a change; run 2 must NOT re-snapshot (marker count stable).
    let markers_before = keys.iter().filter(|k| k.ends_with("_SUCCESS")).count();
    c.query_drop(format!("INSERT INTO {tb} VALUES (2, 7)"))
        .unwrap();
    run();
    let keys2 = list();
    assert!(
        keys2.iter().any(|k| k.contains(&format!("{tb}/cdc-"))),
        "the change streams into tb's own prefix: {keys2:?}"
    );
    let markers_after = keys2
        .iter()
        .filter(|k| k.ends_with("_SUCCESS") && k.contains("/snapshot/"))
        .count();
    assert_eq!(
        markers_after,
        keys.iter()
            .filter(|k| k.ends_with("_SUCCESS") && k.contains("/snapshot/"))
            .count(),
        "run 2 must not re-snapshot (markers stable)"
    );
    let _ = markers_before;
}

// Negative family #5 — corrupt CHECKPOINT files. The probe found the code
// already loud on all four shapes (garbage JSON, truncated, wrong-engine
// format, empty): serde/shape errors surface, no silent re-anchor. Pinned so
// it can never regress into the #28 class (silent re-anchor = silent gap).
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_corrupt_checkpoint_fails_loudly_never_reanchors() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_badckpt");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());
    let (cfg, out) = ddl_cfg(&d, &tbl);
    let ckpt = d.path().join("cdc.ckpt");
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();

    for (name, body) in [
        ("garbage", "{not json at all"),
        ("truncated", "{\"file\":\"binlog.0000"),
        ("wrong_engine", "{\"lsn\":\"0/1A2B3C4D\"}"),
        ("empty", ""),
    ] {
        std::fs::write(&ckpt, body).unwrap();
        let res = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(
            !res.status.success(),
            "{name}: a corrupt checkpoint must fail the run loudly, never \
             silently re-anchor at current"
        );
        let parts = std::fs::read_dir(&out)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "parquet")
            })
            .count();
        assert_eq!(
            parts, 0,
            "{name}: nothing may be captured off a corrupt anchor"
        );
    }
}

// Finding #39b end-to-end: a UNICODE enum label must ride the whole pipe —
// information_schema enrichment → label parse → index→label fix → parquet —
// with no mojibake (the byte-as-char parser garbled every non-ASCII label).
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_unicode_enum_labels_survive_end_to_end() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_uenum");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, st ENUM('привет','мир','it''s'))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());
    let (cfg, out) = ddl_cfg(&d, &tbl);
    run_ok(&cfg); // pin
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1,'привет'),(2,'мир'),(3,'it''s')"
    ))
    .unwrap();
    run_ok(&cfg);

    use arrow::array::StringArray;
    let mut got = Vec::new();
    for b in read_all_parts(&out) {
        let a = b
            .column(b.schema().index_of("st").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for r in 0..b.num_rows() {
            got.push(a.value(r).to_string());
        }
    }
    assert_eq!(
        got,
        vec!["привет", "мир", "it's"],
        "unicode + quoted enum labels must land exactly"
    );
}

// Negative family #4 — the environment revokes rights mid-life: a DBA drops
// the REPLICATION grants between scheduler cycles. The next run must fail
// LOUDLY with the grants hint (never a 0-row success), the checkpoint must
// not move, and restoring the grant must resume with zero loss.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_replication_grant_revoked_mid_life_fails_loud_and_resumes_after_regrant() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_grant");
    let root_url = MYSQL_CDC_URL.replace("rivet:rivet@", "root:rivet@");
    let mut root = mysql::Conn::new(mysql::Opts::from_url(&root_url).unwrap()).unwrap();
    // A dedicated user so revoking cannot break the parallel suite.
    let user = unique_name("cdc_u");
    root.query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
        .unwrap();
    root.query_drop(format!("CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'"))
        .unwrap();
    root.query_drop(format!(
        "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'"
    ))
    .unwrap();
    struct UserGuard(String, String);
    impl Drop for UserGuard {
        fn drop(&mut self) {
            if let Ok(mut c) = mysql::Conn::new(mysql::Opts::from_url(&self.1).unwrap()) {
                use mysql::prelude::Queryable as _;
                let _ = c.query_drop(format!("DROP USER IF EXISTS '{}'@'%'", self.0));
            }
        }
    }
    let _u = UserGuard(user.clone(), root_url.clone());

    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let user_url = MYSQL_CDC_URL.replace("rivet:rivet@", &format!("{user}:pw@"));
    let yaml = format!(
        r#"source: {{type: mysql, url: "{user_url}"}}
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
    run_ok(&cfg); // pin under full rights
    let ckpt_before = std::fs::read_to_string(&ckpt).unwrap();

    // The DBA takes the grants away; a change lands.
    root.query_drop(format!(
        "REVOKE REPLICATION SLAVE, REPLICATION CLIENT ON *.* FROM '{user}'@'%'"
    ))
    .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();

    let res = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        !res.status.success(),
        "revoked grants must fail the run loudly"
    );
    let err = String::from_utf8_lossy(&res.stderr).to_lowercase();
    assert!(
        err.contains("replication") || err.contains("grant") || err.contains("denied"),
        "the failure must point at the grants: {err}"
    );
    assert_eq!(
        std::fs::read_to_string(&ckpt).unwrap(),
        ckpt_before,
        "a failed run must not move the checkpoint"
    );

    // Rights restored ⇒ the change is captured with zero loss.
    root.query_drop(format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'"
    ))
    .unwrap();
    run_ok(&cfg);
    assert_eq!(
        distinct_int_ids(&out).len(),
        1,
        "the change lands after the re-grant — a rights outage is a delay, not a loss"
    );
}

// Negative family #4 — destination disk full (ENOSPC, a different error path
// than the checkpoint's EACCES gremlin). Verified live on a 2 MB volume with
// incompressible payload: the run fails loudly ("No space left on device"),
// the checkpoint does not move, and pointing the export at a roomy
// destination captures the full backlog — a full disk is a delay, not a
// loss. Needs a pre-mounted tiny filesystem, so it is env-gated:
//   macOS: hdiutil create -size 2m -fs APFS ...; hdiutil attach ...
//   Linux: mount -t tmpfs -o size=2m tmpfs <dir>
//   RIVET_TINYFS_DIR=<mountpoint> cargo test ... cdc_destination_disk_full
#[test]
#[ignore = "live: requires mysql-cdc + RIVET_TINYFS_DIR pointing at a ~2MB filesystem"]
fn cdc_destination_disk_full_is_loud_and_lossless() {
    let Some(tiny) = std::env::var_os("RIVET_TINYFS_DIR") else {
        eprintln!("RIVET_TINYFS_DIR not set — skipping (see the test doc for mounting)");
        return;
    };
    let tiny = std::path::PathBuf::from(tiny);
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_enospc");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, pad VARBINARY(255))"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());

    let ckpt = d.path().join("cdc.ckpt");
    let out_tiny = tiny.join(unique_name("out"));
    std::fs::create_dir_all(&out_tiny).unwrap();
    let mk_cfg = |out: &std::path::Path| {
        let yaml = format!(
            r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{o}" }}
"#,
            ckpt = ckpt.display(),
            o = out.display(),
            sid = server_id_for(&tbl),
        );
        write_config(&d, &yaml)
    };
    run_ok(&mk_cfg(&out_tiny)); // pin
    let ckpt_before = std::fs::read_to_string(&ckpt).unwrap();

    // ~2.4 MB of INCOMPRESSIBLE payload (zstd flattens repeats — the first
    // probe of this scenario "passed" because 8 MB of 'x' fit in 200 KB).
    for chunk in 0..20 {
        let vals: Vec<String> = (1..=1000)
            .map(|i| {
                let id = chunk * 1000 + i;
                // Pseudo-random hex from a cheap LCG — incompressible enough.
                let mut x: u64 = 0x9E37 ^ (id as u64 * 2654435761);
                let hex: String = (0..30)
                    .map(|_| {
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                        format!("{:08x}", (x >> 16) as u32)
                    })
                    .collect();
                format!("({id}, UNHEX('{hex}'))")
            })
            .collect();
        c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
            .unwrap();
    }

    let res = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", mk_cfg(&out_tiny).to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!res.status.success(), "ENOSPC must fail the run loudly");
    let err = String::from_utf8_lossy(&res.stderr);
    assert!(
        err.contains("No space left") || err.contains("os error 28"),
        "the failure must name the full disk: {err}"
    );
    assert_eq!(
        std::fs::read_to_string(&ckpt).unwrap(),
        ckpt_before,
        "a failed run must not move the checkpoint"
    );

    // Heal: a roomy destination captures the FULL backlog.
    let out_big = d.path().join("big");
    std::fs::create_dir_all(&out_big).unwrap();
    run_ok(&mk_cfg(&out_big));
    assert_eq!(
        distinct_int_ids(&out_big).len(),
        20_000,
        "a full disk is a delay, not a loss"
    );
}

// Ultrareview-2 bug_012 (finding #41): a PG DELETE under default REPLICA
// IDENTITY carries ONLY the key columns — WITH their names in the wire text
// — but the parser discarded the names and the sink mapped positionally, so
// with a non-first PK the key value landed in column 0 (rendered into a TEXT
// column!) and the PK itself became NULL: a downstream MERGE-by-PK matches
// nothing and the DELETE is silently lost. Every fixture in the campaign had
// `id` first — the shape-of-the-fixture blind spot in person.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_delete_with_non_first_pk_lands_in_the_pk_column() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_pk_mid");
    let slot = unique_name("rivet_pkmid_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    // PK deliberately NOT first; a compound variant covers the PK-in-the-
    // middle shape too.
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (name TEXT, id INT PRIMARY KEY)"
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
        "INSERT INTO {tbl} VALUES ('alice', 42); DELETE FROM {tbl} WHERE id = 42;"
    ))
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_ok(&pg_mbt_cfg(&d, &tbl, &slot, &out));

    use arrow::array::{Int32Array, StringArray};
    let mut checked_delete = false;
    for b in read_all_parts(&out) {
        let op = b
            .column(b.schema().index_of("__op").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let id = b
            .column(b.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        let name = b
            .column(b.schema().index_of("name").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for r in 0..b.num_rows() {
            if op.value(r) != "delete" {
                continue;
            }
            checked_delete = true;
            assert!(
                !id.is_null(r),
                "the DELETE's key must land in the PK column, not vanish"
            );
            assert_eq!(id.value(r), 42, "the typed PK value");
            assert!(
                name.is_null(r),
                "the PK value must NOT bleed into column 0 ('name' got {:?})",
                name.value(r)
            );
        }
    }
    assert!(checked_delete, "a delete event must be captured");
}

struct Slot(String);
impl Drop for Slot {
    fn drop(&mut self) {
        use postgres::NoTls;
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_CDC_URL, NoTls) {
            let _ = c.execute("SELECT pg_drop_replication_slot($1)", &[&self.0]);
        }
    }
}

fn pg_mbt_cfg(
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

// Finding #42 (live): updating the PRIMARY KEY is a legal operation —
// test_decoding renders `old-key: … new-tuple: …`, and gluing the sections
// bricked the stream permanently behind a misleading "DDL" arity failure.
#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_pk_changing_update_captures_and_does_not_brick() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_pkupd");
    let slot = unique_name("rivet_pkupd_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v TEXT)"
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
        "INSERT INTO {tbl} VALUES (1,'a'); UPDATE {tbl} SET id = 2 WHERE id = 1;"
    ))
    .unwrap();

    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    run_ok(&pg_mbt_cfg(&d, &tbl, &slot, &out));

    use arrow::array::{Int32Array, StringArray};
    let mut update_after: Option<(i32, String)> = None;
    for b in read_all_parts(&out) {
        let op = b
            .column(b.schema().index_of("__op").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let id = b
            .column(b.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        let v = b
            .column(b.schema().index_of("v").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for r in 0..b.num_rows() {
            if op.value(r) == "update" {
                update_after = Some((id.value(r), v.value(r).to_string()));
            }
        }
    }
    assert_eq!(
        update_after,
        Some((2, "a".to_string())),
        "the update's after-image is the NEW tuple (id=2), stream not bricked"
    );
}
