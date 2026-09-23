//! The stand registry (`dev/stand/registry.yaml`): every name the tests and the gate use.
#![allow(dead_code)]

use std::sync::OnceLock;

const REGISTRY_YAML: &str = include_str!("../../dev/stand/registry.yaml");

/// The parsed registry, compiled into the test binary.
pub fn stand() -> &'static serde_yaml_ng::Value {
    static R: OnceLock<serde_yaml_ng::Value> = OnceLock::new();
    R.get_or_init(|| {
        serde_yaml_ng::from_str(REGISTRY_YAML).expect("dev/stand/registry.yaml parses")
    })
}

fn text(path: &[&str]) -> &'static str {
    path.iter()
        .fold(stand(), |v, k| &v[*k])
        .as_str()
        .unwrap_or_else(|| panic!("dev/stand/registry.yaml has no string at {path:?}"))
}

/// Container name of a stand source (`postgres`, `mongo_rs`, …).
pub fn stand_container(source: &str) -> &'static str {
    text(&["sources", source, "container"])
}

/// Connection URL of a stand source.
pub fn stand_url(source: &str) -> &'static str {
    text(&["sources", source, "url"])
}

/// `host:port` of a stand source, parsed from its URL.
pub fn stand_host_port(source: &str) -> &'static str {
    let url = stand_url(source);
    let rest = url.split("://").nth(1).expect("registry url has a scheme");
    let rest = rest.rsplit('@').next().unwrap_or(rest);
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    &rest[..end]
}

/// The one permanent BigQuery dataset (live tests and the harness).
pub fn stand_bq_e2e() -> &'static str {
    text(&["bigquery", "e2e"])
}

/// The BigQuery location every test dataset is created in.
pub fn stand_bq_location() -> &'static str {
    text(&["bigquery", "location"])
}

/// A disposable BigQuery dataset name — the only kind `make sweep-test-cloud` drops.
pub fn stand_bq_tmp(name: &str) -> String {
    format!("{}{name}", text(&["bigquery", "tmp_prefix"]))
}

/// The seeded 1M-row fixture database the harness reads.
pub fn stand_bench_db() -> &'static str {
    stand()["databases"]
        .as_sequence()
        .and_then(|s| {
            s.iter()
                .filter_map(|v| v.as_str())
                .find(|d| d.ends_with("_bench"))
        })
        .expect("registry lists a *_bench database")
}
