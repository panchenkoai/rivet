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
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        3,
        "the read-only export must still read every document"
    );
}
