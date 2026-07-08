//! Least-privilege harm-permission for MongoDB — the Mongo analog of
//! `live_mssql_harm_permission.rs`. Requires `docker compose up -d mongo-auth`.
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
    let url = mongo_auth_reader_url();

    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_mongo_config(cfg_dir.path(), &url, "t", out.path(), "", "");

    // The `reader` login has `read` on harmdb but NOT `clusterMonitor`, so the
    // Tier-2 source-harm probe (serverStatus) is Unauthorized. The export must
    // ignore that and still read every document.
    let r = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        r.status.success(),
        "a read-only login must export despite serverStatus being denied \
         (harm counters just degrade to absent); stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );
    assert_eq!(
        dir_parquet_distinct_strings(out.path(), "_id").len(),
        3,
        "the read-only export must still read every document"
    );
}
