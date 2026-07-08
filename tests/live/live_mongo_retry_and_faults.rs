//! Runtime retry + mid-scan fault injection for the MongoDB batch read path —
//! the Mongo twin of `live_retry_and_faults.rs` (PG) and
//! `live_mysql_retry_and_faults.rs`. Requires the `mongo` service + Toxiproxy
//! (`docker compose up -d mongo toxiproxy`).
//!
//! These prove the Mongo batch read path RECOVERS from a mid-scan transport
//! outage via rivet's chunk retry loop — the operational parity the SQL engines
//! have. A dropped socket surfaces as `mongodb::error::ErrorKind::Io`; rivet
//! classifies it TRANSIENT and retries on a fresh connection rather than aborting.
//!
//! Scope note: an `Io` drop is *also* matched by the retry string fallback
//! ("connection reset" / "unexpected eof"), so these tests do not, on their own,
//! isolate the typed `classify_mongo_error` arm — its non-redundant value is the
//! retryable-read COMMAND CODES (replica-set failover: stepdown / election),
//! which Toxiproxy (network faults, not server command errors) cannot induce.
//! See the coverage note on `classify_mongo_error`.
//!
//! Reads cross Toxiproxy (`:27019` → `mongo:27017`); the collection is seeded
//! over the DIRECT `:27017` driver so the fault only ever hits rivet's reads.

use crate::common::*;
use std::time::Duration;

const DIRECT_PORT: u16 = 27017; // seed here; rivet reads through :27019

/// Register the `mongo` toxi proxy (listen :27019 → `mongo:27017`) and wipe any
/// leftover toxics — idempotent, mirrors `reset_mysql_proxy`.
fn reset_mongo_proxy() {
    ensure_toxi_proxy("mongo", 27019, "mongo:27017");
    toxi_reset_toxics("mongo");
    // A prior `..._fails_cleanly..` test disables the proxy and does not restore
    // it; re-enable so each test starts from a healthy link regardless of order.
    toxi_enable("mongo");
}

fn mongo_cfg(dir: &std::path::Path, db: &str, out: &std::path::Path) -> std::path::PathBuf {
    write_mongo_config(
        dir,
        &mongo_toxi_url(db),
        "t",
        out,
        ", mongo: { page_size: 2000 }",
        ", tuning: { max_retries: 4, retry_backoff_ms: 200 }",
    )
}

#[test]
#[ignore = "live: requires docker compose up -d mongo toxiproxy"]
fn mongo_export_survives_transient_latency_added_via_toxiproxy() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Toxiproxy);
    reset_mongo_proxy();
    let db = unique_name("rt_lat");
    let m = MongoTest::connect(DIRECT_PORT, &db);
    m.seed_int_id("t", 8000);

    // A per-read delay slows the scan but must not fail it.
    toxi_add_latency("mongo", 25);

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let r = run_rivet(&[
        "run",
        "-c",
        mongo_cfg(cfg_dir.path(), &db, out.path()).to_str().unwrap(),
    ]);
    assert!(
        r.status.success(),
        "export through added latency must still complete; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );
    assert_eq!(
        dir_parquet_distinct_strings(out.path(), "_id").len(),
        8000,
        "latency must not lose rows"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo toxiproxy"]
fn mongo_export_recovers_after_mid_stream_proxy_disable_then_enable_with_retries() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Toxiproxy);
    reset_mongo_proxy();
    let db = unique_name("rt_mid");
    let m = MongoTest::connect(DIRECT_PORT, &db);
    // Large enough (100K / 2K page = 50 pages) that the scan is still in flight
    // when the proxy drops at 80 ms — so a real mid-scan socket death is retried.
    m.seed_int_id("t", 100_000);

    let bg = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(80));
        toxi_disable("mongo"); // kill in-flight connections → ErrorKind::Io
        std::thread::sleep(Duration::from_millis(600));
        toxi_enable("mongo"); // rivet's retry reconnects here
    });

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let r = run_rivet(&[
        "run",
        "-c",
        mongo_cfg(cfg_dir.path(), &db, out.path()).to_str().unwrap(),
    ]);
    let _ = bg.join();

    assert!(
        r.status.success(),
        "export must RECOVER from a mid-scan proxy outage via the chunk retry \
         loop; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );
    assert_eq!(
        dir_parquet_distinct_strings(out.path(), "_id").len(),
        100_000,
        "recovery must be complete — every row present after the outage"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo toxiproxy"]
fn mongo_export_fails_cleanly_when_proxy_is_disabled_before_run() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Toxiproxy);
    reset_mongo_proxy();
    let db = unique_name("rt_dead");
    let m = MongoTest::connect(DIRECT_PORT, &db);
    m.seed_int_id("t", 100);

    toxi_disable("mongo"); // upstream unreachable for the whole run

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let r = run_rivet(&[
        "run",
        "-c",
        mongo_cfg(cfg_dir.path(), &db, out.path()).to_str().unwrap(),
    ]);
    // A permanently-dead upstream must fail (a non-zero exit), not hang — the
    // retry budget is finite and server-selection failure is terminal after it.
    assert!(
        !r.status.success(),
        "a fully-disabled proxy must fail the run, not hang or silently pass"
    );
}
