//! Runtime retry + mid-scan fault injection for the MongoDB batch read path, on
//! the canonical [`Rig`] — the Mongo twin of `live_retry_and_faults.rs` (PG) and
//! `live_mysql_retry_and_faults.rs`. Requires `mongo` + Toxiproxy
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

/// A batch Rig whose reads cross the toxi proxy, with a bounded retry budget.
fn proxied(db: &str) -> Rig {
    Rig::mongo_batch("t")
        .source_url(&mongo_toxi_url(db))
        .mongo("page_size: 2000")
        .export_line("tuning: { max_retries: 4, retry_backoff_ms: 200 }")
}

#[test]
#[ignore = "live: requires docker compose up -d mongo toxiproxy"]
fn mongo_export_survives_transient_latency_added_via_toxiproxy() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Toxiproxy);
    reset_mongo_proxy();
    let db = unique_name("rt_lat");
    MongoTest::connect(DIRECT_PORT, &db).seed_int_id("t", 8000);

    // A per-read delay slows the scan but must not fail it.
    toxi_add_latency("mongo", 25);

    let rig = proxied(&db);
    rig.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
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
    // Large enough (100K / 2K page = 50 pages) that the scan is still in flight
    // when the proxy drops at 80 ms — so a real mid-scan socket death is retried.
    MongoTest::connect(DIRECT_PORT, &db).seed_int_id("t", 100_000);

    let bg = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(80));
        toxi_disable("mongo"); // kill in-flight connections → ErrorKind::Io
        std::thread::sleep(Duration::from_millis(600));
        toxi_enable("mongo"); // rivet's retry reconnects here
    });

    let rig = proxied(&db);
    rig.run_ok(); // recovers via the chunk retry loop, or panics with the output
    let _ = bg.join();

    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
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
    MongoTest::connect(DIRECT_PORT, &db).seed_int_id("t", 100);

    toxi_disable("mongo"); // upstream unreachable for the whole run

    // A permanently-dead upstream must fail (a non-zero exit), not hang — the
    // retry budget is finite and server-selection failure is terminal after it.
    let _stderr = proxied(&db).run_expect_fail();
}
