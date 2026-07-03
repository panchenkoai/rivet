//! Batch golden suite — the FULL → INCREMENTAL switch, anchored to arithmetic.
//!
//! This is the exact flow a pilot runs (full reload, then flip the same
//! export to `mode: incremental`), and its boundary semantics were verified
//! empirically before pinning: a `mode: full` run does NOT seed the cursor,
//! so the FIRST incremental run re-exports the full set — an OVERLAP the
//! downstream PK-merge absorbs; every following run exports exactly the new
//! increment; an idle run exports zero. Overlap, never a gap — the same
//! at-least-once direction as CDC. Each stage's sum is asserted as a
//! formula-derived literal, per the CDC golden suite's method: if the switch
//! ever starts gapping (cursor falsely seeded) or double-counting increments,
//! these literals move.

use std::collections::HashSet;

use arrow::array::{Array, Decimal128Array, Int64Array};
use mysql::prelude::Queryable;

use crate::common::*;

fn conn() -> mysql::PooledConn {
    mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn")
}

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

/// (row_count, Σ unscaled cents of `amount`, distinct ids) over every part in `dir`.
fn stage_metrics(dir: &std::path::Path) -> (i64, i128, HashSet<i64>) {
    let mut rows = 0i64;
    let mut cents = 0i128;
    let mut ids = HashSet::new();
    for e in std::fs::read_dir(dir).unwrap() {
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
            rows += b.num_rows() as i64;
            let a = b
                .column(b.schema().index_of("amount").unwrap())
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("amount is Decimal128");
            let id = b
                .column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64");
            for r in 0..b.num_rows() {
                if !a.is_null(r) {
                    cents += a.value(r);
                }
                ids.insert(id.value(r));
            }
        }
    }
    (rows, cents, ids)
}

fn run_cfg(cfg: &std::path::Path) {
    let st = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(st.success(), "batch run failed");
}

const T0: &str = "2026-01-15 12:00:00";

// The pilot flow, id cursor: full(1..40) → switch → incr(overlap = the same
// 40) → +10 rows → incr(exactly those 10) → idle incr(0). Union = 1..50.
// amount = i + 0.25 ⇒ Σ cents over 1..40 = 100·820 + 25·40 = 83_000;
// over 41..50 = 100·455 + 25·10 = 45_750.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn batch_full_to_incremental_switch_golden_math() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("bsw_gold");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, amount DECIMAL(12,2) NOT NULL)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());
    let insert = |c: &mut mysql::PooledConn, lo: i64, hi: i64| {
        let vals: Vec<String> = (lo..=hi).map(|i| format!("({i}, {i}.25)")).collect();
        c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
            .unwrap();
    };
    insert(&mut c, 1, 40);

    // Same config DIR (⇒ same state DB / export name) — only mode+dest differ.
    let stage_cfg = |mode: &str, out: &std::path::Path| -> std::path::PathBuf {
        let cursor = if mode == "incremental" {
            "\n    cursor_column: id"
        } else {
            ""
        };
        std::fs::create_dir_all(out).unwrap();
        let yaml = format!(
            r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: {mode}{cursor}
    format: parquet
    destination: {{ type: local, path: "{o}" }}
"#,
            o = out.display(),
        );
        write_config(&d, &yaml)
    };

    let out_full = d.path().join("full");
    run_cfg(&stage_cfg("full", &out_full));
    let (n, cents, ids) = stage_metrics(&out_full);
    assert_eq!((n, cents), (40, 83_000), "full: 40 rows, Σ = 830.00");
    assert_eq!(ids, (1..=40).collect::<HashSet<i64>>());

    // The SWITCH: first incremental re-exports the full set (the cursor was
    // never seeded by `full`) — an overlap with EXACTLY the same sum, and the
    // safe direction: dedupe-by-PK absorbs it; a gap could not.
    let out_i1 = d.path().join("i1");
    run_cfg(&stage_cfg("incremental", &out_i1));
    let (n, cents, ids) = stage_metrics(&out_i1);
    assert_eq!(
        (n, cents),
        (40, 83_000),
        "switch run: the overlap is exactly the full set — never a gap, never a partial"
    );
    assert_eq!(ids, (1..=40).collect::<HashSet<i64>>());

    // New rows → the increment is exactly them.
    insert(&mut c, 41, 50);
    let out_i2 = d.path().join("i2");
    run_cfg(&stage_cfg("incremental", &out_i2));
    let (n, cents, ids) = stage_metrics(&out_i2);
    assert_eq!((n, cents), (10, 45_750), "increment: exactly rows 41..=50");
    assert_eq!(ids, (41..=50).collect::<HashSet<i64>>());

    // Idle increment is empty (skip_empty=false default still writes a
    // manifest; zero parts ⇒ zero rows through stage_metrics).
    let out_i3 = d.path().join("i3");
    run_cfg(&stage_cfg("incremental", &out_i3));
    let (n, cents, _) = stage_metrics(&out_i3);
    assert_eq!((n, cents), (0, 0), "idle increment exports nothing");
}

// The update-capture variant: an id cursor is blind to UPDATEs by design —
// the pilot answer for mutable tables is a datetime cursor (updated_at).
// Golden math: 20 rows at T0; 5 rows later get amount += 100.00 AND a newer
// updated_at; the next incremental exports EXACTLY those 5 with the new
// values: Σ cents = Σ(100i+25, i=1..5) + 5·10_000 = 1_625 + 50_000.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn batch_incremental_datetime_cursor_captures_updates_golden_math() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("bsw_upd");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, amount DECIMAL(12,2) NOT NULL, \
         updated_at DATETIME(6) NOT NULL)"
    ))
    .unwrap();
    let _guard = Table(tbl.clone());
    let vals: Vec<String> = (1..=20)
        .map(|i: i64| format!("({i}, {i}.25, '{T0}')"))
        .collect();
    c.query_drop(format!("INSERT INTO {tbl} VALUES {}", vals.join(",")))
        .unwrap();

    let stage_cfg = |out: &std::path::Path| -> std::path::PathBuf {
        std::fs::create_dir_all(out).unwrap();
        let yaml = format!(
            r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: {{ type: local, path: "{o}" }}
"#,
            o = out.display(),
        );
        write_config(&d, &yaml)
    };

    let out1 = d.path().join("r1");
    run_cfg(&stage_cfg(&out1));
    let (n, cents, _) = stage_metrics(&out1);
    assert_eq!(
        (n, cents),
        (20, 100 * 210 + 25 * 20),
        "seed: Σ(100i+25, 1..20)"
    );

    // Mutate 5 rows with a NEWER updated_at — the datetime cursor must
    // re-export exactly them, with the post-update values.
    c.query_drop(format!(
        "UPDATE {tbl} SET amount = amount + 100.00, \
         updated_at = '2026-01-15 12:00:01' WHERE id <= 5"
    ))
    .unwrap();
    let out2 = d.path().join("r2");
    run_cfg(&stage_cfg(&out2));
    let (n, cents, ids) = stage_metrics(&out2);
    assert_eq!(
        (n, cents),
        (5, 1_625 + 50_000),
        "updated rows re-export with post-update sums"
    );
    assert_eq!(ids, (1..=5).collect::<HashSet<i64>>());

    let out3 = d.path().join("r3");
    run_cfg(&stage_cfg(&out3));
    let (n, _, _) = stage_metrics(&out3);
    assert_eq!(n, 0, "idle run exports nothing");
}
