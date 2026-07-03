//! CDC gremlin tests — REAL faults, not panics.
//!
//! The CDC crash matrix uses `RIVET_TEST_PANIC_AT`, but a panic unwinds and
//! runs `Drop` guards; a SIGKILL, a cut TCP stream, or a failed checkpoint
//! write do not. Each test here injects one real fault class and asserts the
//! at-least-once contract from the outside: after recovery, the union of all
//! parts holds every source row (overlap allowed — dedupe by PK downstream —
//! but never a gap), and failures are loud, never 0-row successes.
//!
//! | ID  | Fault                                                    |
//! |-----|----------------------------------------------------------|
//! | CG1 | SIGKILL mid-drain (destructors never run)                |
//! | CG2 | binlog TCP stream cut after N bytes (toxiproxy)          |
//! | CG3 | GCS upload cut mid-part (toxiproxy → fake-gcs)           |
//! | CG6 | checkpoint write fails (EACCES) after a durable flush    |
//!
//! CG4 (capture-job stall) lives in `live_cdc_mssql.rs` next to its helpers;
//! CG5 (slot invalidation via `max_slot_wal_keep_size`) is a deliberate skip —
//! it needs a WAL-size cap on the shared instance, which endangers every other
//! test's slot (the drop-variant is pinned in `live_cdc.rs`).

use mysql::prelude::Queryable;

use crate::common::CdcTable as Table;
use crate::common::*;

fn conn() -> mysql::PooledConn {
    mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn")
}

/// CDC config against an arbitrary source URL (direct or through toxiproxy),
/// with a small rollover so a big backlog produces many parts (a mid-drain
/// fault then lands between parts, not before the first one).
fn cdc_cfg(
    d: &tempfile::TempDir,
    url: &str,
    tbl: &str,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"source: {{type: mysql, url: "{url}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid}, rollover: 200 }}
    destination: {{ type: local, path: "{out}" }}
"#,
        ckpt = ckpt.display(),
        out = out.display(),
        sid = server_id_for(tbl),
    );
    write_config(d, &yaml)
}

/// Distinct `id`s across every parquet part under `out` — the gap detector.
/// Overlap (the same id in two parts) is FINE (at-least-once + PK dedupe);
/// a missing id is a gap and a failure.
fn distinct_ids(out: &std::path::Path) -> std::collections::HashSet<i64> {
    use arrow::array::{Array, Int32Array, Int64Array};
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
        for batch in r {
            let batch = batch.unwrap();
            let idx = batch.schema().index_of("id").unwrap();
            let col = batch.column(idx);
            if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                (0..a.len()).for_each(|i| {
                    ids.insert(a.value(i) as i64);
                });
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                (0..a.len()).for_each(|i| {
                    ids.insert(a.value(i));
                });
            }
        }
    }
    ids
}

fn seed_backlog(c: &mut mysql::PooledConn, tbl: &str, rows: usize) {
    // One multi-row INSERT per 500 — a handful of big transactions, so the
    // drain spans several parts and commit boundaries.
    for chunk in (0..rows).collect::<Vec<_>>().chunks(500) {
        let values: Vec<String> = chunk
            .iter()
            .map(|i| format!("({i}, repeat('x', 200))"))
            .collect();
        c.query_drop(format!("INSERT INTO {tbl} VALUES {}", values.join(",")))
            .unwrap();
    }
}

// ── CG1: SIGKILL mid-drain ───────────────────────────────────────────────────
// A panic unwinds; kill -9 does not. Whatever parts were committed before the
// kill stay; the checkpoint is wherever the last completed roll left it; the
// re-run must close the gap — union of parts == full backlog.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn gremlin_cdc_sigkill_mid_drain_recovers_without_gap() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("gremlin_kill");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, pad TEXT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let cfg = cdc_cfg(&d, MYSQL_CDC_URL, &tbl, &ckpt, &out);

    // Pin, then a 5k-row backlog (25 parts at rollover: 200).
    let st = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(st.success());
    seed_backlog(&mut c, &tbl, 5_000);

    // Start the drain; SIGKILL as soon as the first part exists.
    let mut child = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut killed_mid_drain = false;
    for _ in 0..600 {
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
        if parts >= 1 {
            let _ = std::process::Command::new("kill")
                .args(["-9", &child.id().to_string()])
                .status();
            killed_mid_drain = true;
            break;
        }
        if let Some(_status) = child.try_wait().unwrap() {
            break; // drained before we could kill — too fast; still assert below
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let _ = child.wait();

    // Recovery run(s): drain whatever the killed run left behind.
    for _ in 0..3 {
        let st = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(st.success(), "recovery run must succeed");
        if distinct_ids(&out).len() >= 5_000 {
            break;
        }
    }
    let ids = distinct_ids(&out);
    assert_eq!(
        ids.len(),
        5_000,
        "union of parts must hold every row (overlap ok, gap never); \
         killed_mid_drain={killed_mid_drain}"
    );
}

// ── CG2: binlog stream cut after N bytes ─────────────────────────────────────
// A real TCP cut mid-replication-stream (toxiproxy limit_data — deterministic,
// unlike timeouts). The run must fail LOUDLY (never a partial silent success
// past unflushed data), and the retry must close the gap.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + toxiproxy"]
fn gremlin_cdc_binlog_cut_mid_drain_fails_loud_then_recovers() {
    let _guard_toxi = toxiproxy_guard();
    ensure_toxi_proxy("mysql_cdc_gremlin", 13307, "mysql-cdc:3306");
    toxi_reset_toxics("mysql_cdc_gremlin");

    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("gremlin_cut");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, pad TEXT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let proxied = "mysql://rivet:rivet@127.0.0.1:13307/rivet";
    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    std::fs::create_dir_all(&out).unwrap();
    let cfg = cdc_cfg(&d, proxied, &tbl, &ckpt, &out);

    // Pin through the healthy proxy, then seed ~1.2 MB of binlog.
    let st = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(st.success(), "pin run through the healthy proxy");
    seed_backlog(&mut c, &tbl, 5_000);

    // Cut the stream after 256 KB — mid-drain, deterministically.
    toxi_add_limit_data("mysql_cdc_gremlin", 256 * 1024, "downstream");
    let out_cut = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        !out_cut.status.success(),
        "a cut mid-drain must fail the run loudly, not succeed partially:\n{}",
        String::from_utf8_lossy(&out_cut.stderr)
    );

    // Heal and retry until the union is complete.
    toxi_reset_toxics("mysql_cdc_gremlin");
    for _ in 0..5 {
        let st = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(st.success(), "post-heal run must succeed");
        if distinct_ids(&out).len() >= 5_000 {
            break;
        }
    }
    assert_eq!(
        distinct_ids(&out).len(),
        5_000,
        "after healing, the union of parts must hold every row"
    );
}

// ── CG6: checkpoint write failure after a durable flush ──────────────────────
// The durable order is flush → checkpoint → ack. If the checkpoint WRITE
// fails (read-only state dir — same error class as ENOSPC), the run must fail
// loudly, and because the ack never ran, the retry re-reads — never loses.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn gremlin_cdc_checkpoint_write_failure_is_loud_and_lossless() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("gremlin_ckpt");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, pad TEXT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let ro_dir = d.path().join("ro");
    std::fs::create_dir_all(&ro_dir).unwrap();
    let ckpt = ro_dir.join("cdc.ckpt");
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let cfg = cdc_cfg(&d, MYSQL_CDC_URL, &tbl, &ckpt, &out);

    // Pin normally (writable), THEN make the checkpoint dir read-only.
    let st = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(st.success());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 'x'), (2, 'y')"))
        .unwrap();
    let mut perms = std::fs::metadata(&ro_dir).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt;
    perms.set_mode(0o555);
    std::fs::set_permissions(&ro_dir, perms.clone()).unwrap();

    let res = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    // Restore writability FIRST so the guard cleanup and the retry work even
    // if the asserts below fail.
    perms.set_mode(0o755);
    std::fs::set_permissions(&ro_dir, perms).unwrap();
    assert!(
        !res.status.success(),
        "a failed checkpoint write must fail the run loudly:\n{}",
        String::from_utf8_lossy(&res.stderr)
    );

    // The ack never ran, so the retry must re-deliver both rows.
    let out2 = d.path().join("out2");
    std::fs::create_dir_all(&out2).unwrap();
    let cfg2 = cdc_cfg(&d, MYSQL_CDC_URL, &tbl, &ckpt, &out2);
    let st = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg2.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(st.success());
    let ids: std::collections::HashSet<i64> = distinct_ids(&out)
        .union(&distinct_ids(&out2))
        .copied()
        .collect();
    assert!(
        ids.contains(&1) && ids.contains(&2),
        "both rows must survive the failed-checkpoint window: {ids:?}"
    );
}

// ── CG3: GCS upload cut mid-part ─────────────────────────────────────────────
// The commit seam under a REAL network fault on the destination side: the
// upload dies partway (toxiproxy limit_data in front of fake-gcs). The run
// must fail loudly; the retry into the SAME prefix must not clobber prior
// parts (run-token names) and must close the gap.
#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + fake-gcs + toxiproxy"]
fn gremlin_cdc_gcs_upload_cut_fails_loud_then_recovers_without_clobber() {
    let _guard_toxi = toxiproxy_guard();
    ensure_toxi_proxy("fake_gcs_gremlin", 14443, "fake-gcs:4443");
    toxi_reset_toxics("fake_gcs_gremlin");

    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("gremlin_gcs");
    let mut c = conn();
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, pad TEXT)"))
        .unwrap();
    let _guard = Table(tbl.clone());

    let bucket = "rivet-qa-cdc-gcs";
    ensure_gcs_bucket(bucket);
    let prefix = unique_name("gremlin");
    let ckpt = d.path().join("cdc.ckpt");
    let cfg_for = |d: &tempfile::TempDir, endpoint: &str| {
        let yaml = format!(
            r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {tbl}
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: {sid}, rollover: 200 }}
    destination:
      type: gcs
      bucket: {bucket}
      prefix: {prefix}
      endpoint: {endpoint}
      allow_anonymous: true
"#,
            ckpt = ckpt.display(),
            sid = server_id_for(&tbl),
        );
        write_config(d, &yaml)
    };

    // Pin through the healthy proxy, seed, then cut uploads after 128 KB.
    let proxied = "http://127.0.0.1:14443";
    let cfg = cfg_for(&d, proxied);
    let st = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(st.success(), "pin run through the healthy proxy");
    seed_backlog(&mut c, &tbl, 3_000);
    // A per-connection limit_data is DEFEATED by the destination retry layer
    // (opendal reopens with a fresh budget and eventually lands the part —
    // observed live). The honest mid-drain destination fault is a hard outage:
    // disable the proxy once the first part is up, so every retry is refused
    // and the run must fail loudly.
    let mut child = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let first_part_up = |prefix: &str| -> bool {
        reqwest::blocking::get(format!(
            "{FAKE_GCS_ENDPOINT}/storage/v1/b/{bucket}/o?prefix={prefix}"
        ))
        .ok()
        .and_then(|r| r.text().ok())
        .map(|b| b.contains(".parquet"))
        .unwrap_or(false)
    };
    for _ in 0..600 {
        if first_part_up(&prefix) {
            toxi_disable("fake_gcs_gremlin");
            break;
        }
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let status = child.wait().unwrap();
    toxi_enable("fake_gcs_gremlin");
    assert!(
        !status.success(),
        "a destination outage mid-drain must fail the run loudly"
    );

    // Heal (direct endpoint) and retry until every id is present in the union
    // of GCS parts; a clobbered part would show up as a GAP in the union.
    toxi_reset_toxics("fake_gcs_gremlin");
    let cfg_direct = cfg_for(&d, FAKE_GCS_ENDPOINT);
    let list_ids = || -> std::collections::HashSet<i64> {
        // Pull every parquet object under the prefix into a temp dir, reuse
        // the local reader.
        let tmp = tempfile::tempdir().unwrap();
        let body = reqwest::blocking::get(format!(
            "{FAKE_GCS_ENDPOINT}/storage/v1/b/{bucket}/o?prefix={prefix}"
        ))
        .expect("gcs list request")
        .text()
        .expect("gcs list body");
        let json: serde_json::Value = serde_json::from_str(&body).expect("gcs list json");
        if let Some(items) = json["items"].as_array() {
            for (i, it) in items.iter().enumerate() {
                let name = it["name"].as_str().unwrap();
                if !name.ends_with(".parquet") {
                    continue;
                }
                // Object names are ASCII with '/' separators — percent-encode
                // just the slash for the alt=media path form.
                let enc = name.replace('/', "%2F");
                let bytes = reqwest::blocking::get(format!(
                    "{FAKE_GCS_ENDPOINT}/storage/v1/b/{bucket}/o/{enc}?alt=media"
                ))
                .expect("gcs get")
                .bytes()
                .expect("gcs body");
                std::fs::write(tmp.path().join(format!("p{i}.parquet")), &bytes).unwrap();
            }
        }
        distinct_ids(tmp.path())
    };
    for _ in 0..5 {
        let st = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg_direct.to_str().unwrap()])
            .status()
            .unwrap();
        assert!(st.success(), "post-heal run must succeed");
        if list_ids().len() >= 3_000 {
            break;
        }
    }
    assert_eq!(
        list_ids().len(),
        3_000,
        "after healing, the union of GCS parts must hold every row (no gap, no clobber)"
    );
}
