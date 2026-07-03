//! CDC golden suite — hand-CALCULATED metrics over the batch fixture schemas.
//!
//! The matrix tests prove CDC == batch; this suite anchors CDC to ARITHMETIC:
//! a deterministic workload whose aggregates are computed by formula (sums of
//! series, modular counts, explicit null plans) and asserted against the
//! captured parts as literals. If batch and CDC ever drift together, the
//! matrices stay green — this suite does not.
//!
//! Diversity comes from the REAL batch-export fixture schemas (`CREATE TABLE
//! … LIKE`): decimal money (transactions), char(36) uuids (sessions),
//! AUTO_INCREMENT keys (email_queue), a varchar PRIMARY KEY
//! (product_catalog), doubles (metric_samples), nullable text
//! (logs_archive), fixed-width hex (audit_log), and the nullable-cursor
//! shape (orders_coalesce) — all through ONE multiplexed stream.

use std::collections::HashSet;

use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
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

fn server_id_for(tbl: &str) -> u32 {
    let h = tbl.bytes().fold(2_166_136_261u32, |a, b| {
        (a ^ b as u32).wrapping_mul(16_777_619)
    });
    10_000 + (h % 50_000)
}

/// Every row of every part under `dir`, concatenated.
fn read_all(dir: &std::path::Path) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut parts: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    parts.sort();
    for p in parts {
        let f = std::fs::File::open(&p).unwrap();
        let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            out.push(b.unwrap());
        }
    }
    out
}

/// The captured-part metrics the goldens are computed against.
#[derive(Default, Debug)]
struct Metrics {
    inserts: i64,
    updates: i64,
    deletes: i64,
    /// Unscaled Decimal128 sum of `col` over INSERT images (None if absent).
    decimal_insert_sum: i128,
    /// Unscaled Decimal128 sum of `col` over UPDATE after-images.
    decimal_update_sum: i128,
    /// f64 sum over insert images (exact for dyadic golden values).
    double_insert_sum: f64,
    /// SUM of an Int64 column over UPDATE after-images.
    int_update_sum: i64,
    /// NULL count per probed text column, over INSERT images.
    nulls: i64,
    /// Distinct values of the probed string column, over ALL images.
    distinct_strings: HashSet<String>,
    /// Distinct Int64 ids over DELETE images.
    deleted_ids: HashSet<i64>,
    /// Occurrences of a probed string value over INSERT images.
    string_hits: i64,
    /// Σ(id · unscaled decimal) over INSERT images — the SWAP detector: a
    /// permutation of amounts across rows preserves the plain sum and the
    /// id-set, but moves this (the goldens' answer to keyed checksums).
    keyed_decimal_sum: i128,
    /// (min, max) epoch-µs of the probed timestamp column over INSERT images.
    ts_min_max: Option<(i64, i64)>,
}

/// Fold metrics over the captured parts. `decimal_col`/`double_col`/`int_col`/
/// `null_col`/`string_col` probe those aspects when the column exists.
#[allow(clippy::too_many_arguments)]
fn collect(
    dir: &std::path::Path,
    decimal_col: Option<&str>,
    double_col: Option<&str>,
    int_col: Option<&str>,
    null_col: Option<&str>,
    string_col: Option<&str>,
    string_needle: &str,
    id_col: &str,
) -> Metrics {
    let mut m = Metrics::default();
    for b in read_all(dir) {
        let op = b
            .column(b.schema().index_of("__op").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for r in 0..b.num_rows() {
            match op.value(r) {
                "insert" => m.inserts += 1,
                "update" => m.updates += 1,
                "delete" => m.deletes += 1,
                other => panic!("unexpected op {other}"),
            }
        }
        let col_i128 = |name: &str| -> Option<Decimal128Array> {
            b.schema().index_of(name).ok().and_then(|i| {
                b.column(i)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .cloned()
            })
        };
        if let Some(name) = decimal_col
            && let Some(a) = col_i128(name)
        {
            let ids = b
                .schema()
                .index_of(id_col)
                .ok()
                .and_then(|i| b.column(i).as_any().downcast_ref::<Int64Array>().cloned());
            for r in 0..b.num_rows() {
                if a.is_null(r) {
                    continue;
                }
                match op.value(r) {
                    "insert" => {
                        m.decimal_insert_sum += a.value(r);
                        if let Some(ids) = &ids
                            && !ids.is_null(r)
                        {
                            m.keyed_decimal_sum += ids.value(r) as i128 * a.value(r);
                        }
                    }
                    "update" => m.decimal_update_sum += a.value(r),
                    _ => {}
                }
            }
        }
        // Timestamp probe (M7): the goldens plant a single known instant —
        // min==max==T0 catches any epoch shift the sums cannot see.
        if let Ok(i) = b.schema().index_of("created_at")
            && let Some(a) = b
                .column(i)
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        {
            for r in 0..b.num_rows() {
                if op.value(r) == "insert" && !a.is_null(r) {
                    let v = a.value(r);
                    m.ts_min_max = Some(match m.ts_min_max {
                        None => (v, v),
                        Some((lo, hi)) => (lo.min(v), hi.max(v)),
                    });
                }
            }
        }
        if let Some(name) = double_col
            && let Ok(i) = b.schema().index_of(name)
            && let Some(a) = b.column(i).as_any().downcast_ref::<Float64Array>()
        {
            for r in 0..b.num_rows() {
                if op.value(r) == "insert" && !a.is_null(r) {
                    m.double_insert_sum += a.value(r);
                }
            }
        }
        if let Some(name) = int_col
            && let Ok(i) = b.schema().index_of(name)
        {
            let col = b.column(i);
            for r in 0..b.num_rows() {
                if op.value(r) != "update" || col.is_null(r) {
                    continue;
                }
                if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                    m.int_update_sum += a.value(r);
                } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    m.int_update_sum += a.value(r) as i64;
                }
            }
        }
        if let Some(name) = null_col
            && let Ok(i) = b.schema().index_of(name)
        {
            let a = b.column(i);
            for r in 0..b.num_rows() {
                if op.value(r) == "insert" && a.is_null(r) {
                    m.nulls += 1;
                }
            }
        }
        if let Some(name) = string_col
            && let Ok(i) = b.schema().index_of(name)
            && let Some(a) = b.column(i).as_any().downcast_ref::<StringArray>()
        {
            for r in 0..b.num_rows() {
                if a.is_null(r) {
                    continue;
                }
                m.distinct_strings.insert(a.value(r).to_string());
                if op.value(r) == "insert" && a.value(r) == string_needle {
                    m.string_hits += 1;
                }
            }
        }
        if let Ok(i) = b.schema().index_of(id_col)
            && let Some(a) = b.column(i).as_any().downcast_ref::<Int64Array>()
        {
            for r in 0..b.num_rows() {
                if op.value(r) == "delete" && !a.is_null(r) {
                    m.deleted_ids.insert(a.value(r));
                }
            }
        }
    }
    m
}

fn manifest_rows(out: &std::path::Path) -> i64 {
    let body = std::fs::read_to_string(out.join("manifest.json")).expect("manifest.json");
    let m: serde_json::Value = serde_json::from_str(&body).unwrap();
    m["row_count"].as_i64().expect("row_count")
}

const T0: &str = "2026-01-15 12:00:00";

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_golden_fixture_tables_calculated_metrics() {
    let d = tempfile::tempdir().unwrap();
    let uniq = unique_name("g");
    let mut c = conn();

    // Golden copies of the eight batch fixture schemas.
    let fixtures = [
        "transactions",
        "sessions",
        "email_queue",
        "product_catalog",
        "metric_samples",
        "logs_archive",
        "audit_log",
        "orders_coalesce",
    ];
    let mut names = std::collections::HashMap::new();
    let mut guards = Vec::new();
    for f in fixtures {
        let g = format!("{uniq}_{f}");
        c.query_drop(format!("DROP TABLE IF EXISTS {g}")).unwrap();
        c.query_drop(format!("CREATE TABLE {g} LIKE {f}")).unwrap();
        guards.push(Table(g.clone()));
        names.insert(f, g);
    }

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let tables_list = fixtures
        .iter()
        .map(|f| names[f].clone())
        .collect::<Vec<_>>()
        .join(", ");
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: golden_cdc
    tables: [{tables_list}]
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(&uniq),
    );
    let cfg = write_config(&d, &yaml);
    let run = || {
        let st = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(st.success(), "golden cdc run failed");
    };
    run(); // pin the checkpoint

    // ── The golden workload: every number below is derivable by hand. ──────
    // transactions: i=1..100, amount = i + 0.25 (cents 100i+25), status
    // alternates ok/chargeback; update ids 1..10 (+1000.00); delete 91..100.
    let t = &names["transactions"];
    for chunk in (1..=100).collect::<Vec<i64>>().chunks(50) {
        let vals: Vec<String> = chunk
            .iter()
            .map(|i| {
                let st = if i % 2 == 0 { "chargeback" } else { "ok" };
                format!("({i}, {u}, {i}.25, '{st}', '{T0}', '{T0}')", u = i % 7)
            })
            .collect();
        c.query_drop(format!("INSERT INTO {t} VALUES {}", vals.join(",")))
            .unwrap();
    }
    c.query_drop(format!(
        "UPDATE {t} SET amount = amount + 1000.00 WHERE id BETWEEN 1 AND 10"
    ))
    .unwrap();
    c.query_drop(format!("DELETE FROM {t} WHERE id BETWEEN 91 AND 100"))
        .unwrap();

    // sessions: i=1..50, deterministic uuid, user_id = i (sum = 1275).
    let t = &names["sessions"];
    let vals: Vec<String> = (1..=50)
        .map(|i: i64| format!("({i}, '00000000-0000-4000-8000-{i:012}', {i}, '{T0}', '{T0}')"))
        .collect();
    c.query_drop(format!("INSERT INTO {t} VALUES {}", vals.join(",")))
        .unwrap();

    // email_queue (AUTO_INCREMENT id omitted): 40 rows; status sent when
    // i%4==0 (10 sent / 30 pending); body NULL when i%5==0 (8 NULLs).
    let t = &names["email_queue"];
    let vals: Vec<String> = (1..=40)
        .map(|i: i64| {
            let st = if i % 4 == 0 { "sent" } else { "pending" };
            let body = if i % 5 == 0 {
                "NULL".to_string()
            } else {
                format!("'body {i}'")
            };
            format!("('u{i}@example.com', 'subj {i}', {body}, '{st}', '{T0}')")
        })
        .collect();
    c.query_drop(format!(
        "INSERT INTO {t} (to_email, subject, body, status, scheduled_at) VALUES {}",
        vals.join(",")
    ))
    .unwrap();

    // product_catalog (varchar PK): 30 skus, price cents = 999 + i
    // (sum = 30*999 + 465 = 30_435); update 5 (+5.00 ⇒ after-image cents
    // sum = 5*999 + 15 + 5*500 = 7_510); delete 3.
    let t = &names["product_catalog"];
    let vals: Vec<String> = (1..=30)
        .map(|i: i64| {
            format!(
                "('SKU-{i:04}', 'name {i}', 'cat{c}', {p}.{f:02}, {a}, '{T0}')",
                c = i % 3,
                p = (999 + i) / 100,
                f = (999 + i) % 100,
                a = i32::from(i % 3 != 0)
            )
        })
        .collect();
    c.query_drop(format!("INSERT INTO {t} VALUES {}", vals.join(",")))
        .unwrap();
    c.query_drop(format!(
        "UPDATE {t} SET price = price + 5.00 WHERE sku <= 'SKU-0005'"
    ))
    .unwrap();
    c.query_drop(format!("DELETE FROM {t} WHERE sku > 'SKU-0027'"))
        .unwrap();

    // metric_samples: 60 rows, value = i * 0.5 (dyadic ⇒ exact) — sum = 915.0.
    let t = &names["metric_samples"];
    let vals: Vec<String> = (1..=60)
        .map(|i: i64| {
            format!(
                "('host{h}', 'metric{m}', {v}, '{T0}')",
                h = i % 4,
                m = i % 6,
                v = i as f64 * 0.5
            )
        })
        .collect();
    c.query_drop(format!(
        "INSERT INTO {t} (host, metric_name, value, captured_at) VALUES {}",
        vals.join(",")
    ))
    .unwrap();

    // logs_archive: 50 rows; payload NULL for even i (25 NULLs), unicode
    // payloads otherwise; updated_at NULL always (50 NULLs — via null_col
    // probe on a second collect).
    let t = &names["logs_archive"];
    let vals: Vec<String> = (1..=50)
        .map(|i: i64| {
            let p = if i % 2 == 0 {
                "NULL".to_string()
            } else {
                format!("'журнал → {i}'")
            };
            format!("({i}, {p}, '{T0}', NULL)")
        })
        .collect();
    c.query_drop(format!("INSERT INTO {t} VALUES {}", vals.join(",")))
        .unwrap();

    // audit_log: 50 rows; 32-hex action_hash from i (distinct = 50); action
    // cycles create/update/delete — 'create' hits = 17 (i%3==1 for 1..50).
    let t = &names["audit_log"];
    let vals: Vec<String> = (1..=50)
        .map(|i: i64| {
            let act = ["delete", "create", "update"][(i % 3) as usize];
            format!("({i}, {a}, '{h:032x}', '{act}', '{T0}')", a = i % 9, h = i)
        })
        .collect();
    c.query_drop(format!("INSERT INTO {t} VALUES {}", vals.join(",")))
        .unwrap();

    // orders_coalesce: 50 rows (id explicit 1..50, quantity = i); delete odd
    // (25); double the quantity of the surviving even ids ⇒ update
    // after-image quantity sum = 2*(2+4+…+50) = 2_550... (2+4+..+50 = 650;
    // ×2 = 1_300).
    let t = &names["orders_coalesce"];
    let vals: Vec<String> = (1..=50)
        .map(|i: i64| format!("({i}, 'prod {i}', {i}, {i}.00, NULL, '{T0}')"))
        .collect();
    c.query_drop(format!("INSERT INTO {t} VALUES {}", vals.join(",")))
        .unwrap();
    c.query_drop(format!("DELETE FROM {t} WHERE id % 2 = 1"))
        .unwrap();
    c.query_drop(format!("UPDATE {t} SET quantity = quantity * 2"))
        .unwrap();

    run(); // capture everything through ONE stream

    // ── The golden assertions: literals, not batch comparisons. ────────────
    let dir = |f: &str| out.join(&names[f]);

    // transactions — 120 events; insert Σamount = Σ(100i+25) = 507_500 cents;
    // update after Σ = Σ(i=1..10)(100i+25) + 10*100_000 = 5_750 + 1_000_000.
    let m = collect(
        &dir("transactions"),
        Some("amount"),
        None,
        None,
        None,
        Some("status"),
        "chargeback",
        "id",
    );
    assert_eq!(
        (m.inserts, m.updates, m.deletes),
        (100, 10, 10),
        "transactions ops"
    );
    assert_eq!(m.decimal_insert_sum, 507_500, "Σ insert amount = 5075.00");
    assert_eq!(
        m.decimal_update_sum, 1_005_750,
        "Σ update-after amount = 10057.50"
    );
    assert_eq!(m.string_hits, 50, "50 chargebacks");
    // Swap detector: Σ id·cents = 100·Σi² + 25·Σi over 1..100
    //              = 100·338_350 + 25·5_050 = 33_961_250.
    assert_eq!(
        m.keyed_decimal_sum, 33_961_250,
        "keyed Σ(id·amount-cents) — a cross-row amount swap moves this"
    );
    // Every golden insert carries the same instant: min == max == T0.
    let t0 = chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    assert_eq!(
        m.ts_min_max,
        Some((t0, t0)),
        "created_at pinned to T0 epoch"
    );
    assert_eq!(
        m.deleted_ids,
        (91..=100).collect::<HashSet<i64>>(),
        "deleted ids are exactly 91..=100"
    );
    assert_eq!(manifest_rows(&dir("transactions")), 120);

    // sessions — 50 inserts, 50 distinct uuids.
    let m = collect(
        &dir("sessions"),
        None,
        None,
        None,
        None,
        Some("session_uuid"),
        "",
        "id",
    );
    assert_eq!(
        (m.inserts, m.updates, m.deletes),
        (50, 0, 0),
        "sessions ops"
    );
    assert_eq!(m.distinct_strings.len(), 50, "50 distinct uuids");
    assert!(
        m.distinct_strings
            .contains("00000000-0000-4000-8000-000000000037"),
        "deterministic uuid #37 present"
    );

    // email_queue — 40 inserts; 8 NULL bodies; 10 'sent'.
    let m = collect(
        &dir("email_queue"),
        None,
        None,
        None,
        Some("body"),
        Some("status"),
        "sent",
        "id",
    );
    assert_eq!(
        (m.inserts, m.updates, m.deletes),
        (40, 0, 0),
        "email_queue ops"
    );
    assert_eq!(m.nulls, 8, "8 NULL bodies (every 5th)");
    assert_eq!(m.string_hits, 10, "10 sent (every 4th)");

    // product_catalog — 30/5/3; insert Σprice cents = 30_435; update-after
    // Σ = 7_510.
    let m = collect(
        &dir("product_catalog"),
        Some("price"),
        None,
        None,
        None,
        Some("sku"),
        "",
        "id",
    );
    assert_eq!((m.inserts, m.updates, m.deletes), (30, 5, 3), "catalog ops");
    assert_eq!(m.decimal_insert_sum, 30_435, "Σ insert price = 304.35");
    assert_eq!(m.decimal_update_sum, 7_510, "Σ update-after price = 75.10");
    assert_eq!(
        m.distinct_strings.len(),
        30,
        "30 distinct skus across all images"
    );

    // metric_samples — 60 inserts; Σvalue = 915.0 exactly (dyadic halves).
    let m = collect(
        &dir("metric_samples"),
        None,
        Some("value"),
        None,
        None,
        None,
        "",
        "id",
    );
    assert_eq!((m.inserts, m.updates, m.deletes), (60, 0, 0), "metrics ops");
    assert_eq!(m.double_insert_sum, 915.0, "Σ value = 915.0 (exact)");

    // logs_archive — 50 inserts; 25 NULL payloads; 50 NULL updated_at.
    let m = collect(
        &dir("logs_archive"),
        None,
        None,
        None,
        Some("payload"),
        None,
        "",
        "id",
    );
    assert_eq!((m.inserts, m.updates, m.deletes), (50, 0, 0), "logs ops");
    assert_eq!(m.nulls, 25, "25 NULL payloads (even ids)");
    let m2 = collect(
        &dir("logs_archive"),
        None,
        None,
        None,
        Some("updated_at"),
        None,
        "",
        "id",
    );
    assert_eq!(m2.nulls, 50, "updated_at NULL on every row");

    // audit_log — 50 inserts; 50 distinct 32-hex hashes; 17 'create'.
    let m = collect(
        &dir("audit_log"),
        None,
        None,
        None,
        None,
        Some("action_hash"),
        "",
        "id",
    );
    assert_eq!((m.inserts, m.updates, m.deletes), (50, 0, 0), "audit ops");
    assert_eq!(m.distinct_strings.len(), 50, "50 distinct hashes");
    let m2 = collect(
        &dir("audit_log"),
        None,
        None,
        None,
        None,
        Some("action"),
        "create",
        "id",
    );
    assert_eq!(m2.string_hits, 17, "17 creates (i%3==1 over 1..=50)");

    // orders_coalesce — 50/25/25; update-after Σquantity = 1_300.
    let m = collect(
        &dir("orders_coalesce"),
        None,
        None,
        Some("quantity"),
        None,
        None,
        "",
        "id",
    );
    assert_eq!(
        (m.inserts, m.updates, m.deletes),
        (50, 25, 25),
        "coalesce ops"
    );
    assert_eq!(m.int_update_sum, 1_300, "Σ doubled even quantities = 1300");
    assert_eq!(
        m.deleted_ids,
        (1..=50).filter(|i| i % 2 == 1).collect::<HashSet<i64>>(),
        "deleted ids are exactly the odd 1..=49"
    );

    drop(guards);
}

// M1 — the ORDERING pin, absent everywhere else: downstream MERGE correctness
// rests on (a) parts being seq-ordered by filename, (b) rows within a part
// being commit-ordered, and (c) same-`__pos` events (one transaction) being
// resolvable by intra-part row order. Three updates to ONE row — two inside a
// single transaction (same commit position), one in the next — must read back
// in exactly commit order, so "latest by (__pos, file, row_number)" yields
// the true final image.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_event_order_within_and_across_transactions_is_commit_order() {
    use arrow::array::Int32Array;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_order");
    let mut c = conn();
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
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid} }}
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

    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 0)"))
        .unwrap();
    // ONE transaction, two updates to the same row — same commit __pos.
    c.query_drop("START TRANSACTION").unwrap();
    c.query_drop(format!("UPDATE {tbl} SET v = 1 WHERE id = 1"))
        .unwrap();
    c.query_drop(format!("UPDATE {tbl} SET v = 2 WHERE id = 1"))
        .unwrap();
    c.query_drop("COMMIT").unwrap();
    // A second transaction — a later __pos.
    c.query_drop(format!("UPDATE {tbl} SET v = 3 WHERE id = 1"))
        .unwrap();
    run();

    // Read rows in (filename, row) order; collect (v, __pos) per after-image.
    let mut seq: Vec<(i32, String)> = Vec::new();
    for b in read_all(&out) {
        let v = b
            .column(b.schema().index_of("v").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        let pos = b
            .column(b.schema().index_of("__pos").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for r in 0..b.num_rows() {
            seq.push((v.value(r), pos.value(r).to_string()));
        }
    }
    let vs: Vec<i32> = seq.iter().map(|(v, _)| *v).collect();
    assert_eq!(
        vs,
        vec![0, 1, 2, 3],
        "images must read back in exact commit order (insert, u1, u2, u3)"
    );
    // Same transaction ⇒ same commit position; the NEXT transaction differs —
    // so the documented dedupe (max __pos, then intra-part row order) resolves
    // v=2 over v=1 by order, and v=3 over both by position.
    assert_eq!(
        seq[1].1, seq[2].1,
        "u1 and u2 share their tx's commit __pos"
    );
    assert_ne!(seq[2].1, seq[3].1, "the next transaction has a later __pos");
    let last = seq.last().unwrap();
    assert_eq!(
        last.0, 3,
        "latest-by-(__pos, order) is the true final image"
    );
}
