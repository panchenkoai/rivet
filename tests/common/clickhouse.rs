//! ClickHouse validator helpers (ADR-0014 §4).
//!
//! Tests reach the long-running `rivet-clickhouse` container via the HTTP
//! interface on :8123 (see `docker-compose.yaml`). The native protocol on
//! :9002 is published too but no Rust ClickHouse client is used here — an
//! HTTP request with `FORMAT JSON` is enough for read-back validation and
//! avoids pulling a new dep.
//!
//! Path / workdir helpers (`live_container_path`, `live_shared_workdir`)
//! live in [`super::env`] because they are shared with the DuckDB validator
//! — both containers see the same bind mount under `/work`.
//!
//! Parquet files are read with `file('<rel>', 'Parquet')` (ClickHouse's
//! `file()` table function); `user_files_path` is set to `/work` via the
//! config patch in `dev/clickhouse/user_files.xml` so the `<rel>` path is
//! relative to the shared mount.

#![allow(dead_code)]

use std::collections::HashMap;

use super::env::{CLICKHOUSE_DB, CLICKHOUSE_HTTP_URL, CLICKHOUSE_PASSWORD, CLICKHOUSE_USER};

/// Re-exported under the historical `clickhouse_*` names so existing call
/// sites keep working. Implementations live in [`super::env`].
pub use super::env::live_container_path as clickhouse_container_path;
pub use super::env::live_shared_workdir as clickhouse_shared_workdir;

/// Run a ClickHouse query via HTTP and return its JSON body. Always uses
/// `FORMAT JSONEachRow` so each result row is a self-contained object; this
/// keeps types stable across query shapes.
pub fn clickhouse_run_sql_json(sql: &str) -> Vec<serde_json::Value> {
    // rivet writes Parquet as 0600/owner-only; ClickHouse reads as uid 101 and
    // would hit EACCES on Linux. Relax the test-owned files first (no-op on
    // macOS where Docker Desktop virtualises ownership).
    super::env::make_shared_world_readable();

    // Append `FORMAT JSONEachRow` unless the caller already specified a format.
    let lower = sql.to_lowercase();
    let final_sql = if lower.contains(" format ") {
        sql.to_string()
    } else {
        format!("{sql} FORMAT JSONEachRow")
    };

    let resp = reqwest::blocking::Client::new()
        .post(format!("{CLICKHOUSE_HTTP_URL}/?database={CLICKHOUSE_DB}"))
        .basic_auth(CLICKHOUSE_USER, Some(CLICKHOUSE_PASSWORD))
        .body(final_sql)
        .send()
        .expect("post to clickhouse");

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        panic!("clickhouse HTTP {status}: {body}\nSQL: {sql}");
    }
    let text = resp.text().expect("read body");
    text.lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            serde_json::from_str(l).unwrap_or_else(|e| panic!("parse JSONEachRow line {l:?}: {e}"))
        })
        .collect()
}

/// Turn a ClickHouse `DESCRIBE TABLE …` result (as returned by
/// [`clickhouse_run_sql_json`]) into a `column_name → column_type` map.
/// JSONEachRow yields objects with `name` / `type` fields, so the parse is
/// trivial — the helper exists so the call sites can stay one-liners.
pub fn clickhouse_parse_describe(described: Vec<serde_json::Value>) -> HashMap<String, String> {
    described
        .into_iter()
        .map(|r| {
            (
                r["name"]
                    .as_str()
                    .expect("DESCRIBE row has `name`")
                    .to_string(),
                r["type"]
                    .as_str()
                    .expect("DESCRIBE row has `type`")
                    .to_string(),
            )
        })
        .collect()
}
