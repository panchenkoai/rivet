//! Least-privilege harm-permission for MongoDB, on the canonical [`Rig`] — the
//! Mongo analog of `live_mssql_harm_permission.rs`. Requires
//! `docker compose up -d mongo-auth`.
//!
//! The source-harm counters come from `serverStatus`, which needs the
//! `clusterMonitor` role. A plain `read` login cannot run it. The contract: that
//! denial must **degrade gracefully** — `sample_harm_counters` returns `None`
//! (its `.ok()?` swallows the auth error) and the export completes with every
//! row — never a failed or hung run because the harm probe was refused.

use crate::common::*;

#[test]
#[ignore = "live: requires docker compose up -d mongo-auth"]
fn mongo_readonly_login_exports_despite_denied_harm_probe() {
    require_alive(LiveService::MongoAuth);

    // The `reader` login has `read` on harmdb but NOT `clusterMonitor`, so the
    // Tier-2 source-harm probe (serverStatus) is Unauthorized. The export must
    // ignore that and still read every document.
    let rig = Rig::mongo_batch("t").source_url(&mongo_auth_reader_url());
    rig.run_ok();
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        3,
        "the read-only export must still read every document"
    );
}

/// The refusal half of the auth row: WRONG credentials must fail LOUD naming
/// authentication — never a hung run, a silent empty export, or a partial one.
/// The oracle names the ONE thing that can produce the failure (the exit-status
/// rule): stderr must say auth, and the out dir must hold zero parquet.
#[test]
#[ignore = "live: requires docker compose up -d mongo-auth"]
fn mongo_wrong_password_fails_loud_and_writes_nothing() {
    require_alive(LiveService::MongoAuth);
    let bad =
        "mongodb://reader:WRONGPASS@127.0.0.1:27020/harmdb?authSource=harmdb&directConnection=true";
    let rig = Rig::mongo_batch("t").source_url(bad);
    let err = rig.run_expect_fail().to_lowercase();
    assert!(
        err.contains("auth") || err.contains("scram") || err.contains("unauthorized"),
        "the failure must NAME authentication, not a generic timeout: {err}"
    );
    let parts = std::fs::read_dir(rig.out_dir())
        .map(|d| {
            d.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                .count()
        })
        .unwrap_or(0);
    assert_eq!(parts, 0, "nothing may be exported off a refused login");
}
