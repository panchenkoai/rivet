//! `LiveService::addr()` hand-types every service's host port, and
//! `docker-compose.yaml` hand-types the same ports as mappings — twin lists in
//! two files, the exact shape that has already produced false "verified" claims
//! twice when a hand-drifted container disagreed with the declared stand
//! (PRs 139/142), and the workflow-stack drift the 08-19 nightly paid for.
//!
//! Derivation direction: every port a `LiveService` arm claims must exist as a
//! HOST port mapping in docker-compose.yaml (or the dev stand's compose). The
//! reverse is deliberately not asserted — compose maps many ports no test
//! probes (admin consoles, replica internals), and requiring an arm per mapping
//! would just breed dead enum variants.

use std::collections::BTreeSet;

/// Host ports mapped by a compose file: the left side of `- "HOST:CONTAINER"`.
fn compose_host_ports(path: &str) -> BTreeSet<u16> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e} (run from the repo root)"));
    text.lines()
        .filter_map(|l| {
            let t = l.trim();
            let t = t.strip_prefix("- \"")?;
            let (host, _rest) = t.split_once(':')?;
            host.parse::<u16>().ok()
        })
        .collect()
}

/// Every `(name, port)` the LiveService enum claims, parsed from the SOURCE of
/// `tests/common/env.rs` — the same read-the-source technique the runner-frame
/// and workflow-stack guards use. Parsing the source rather than linking the
/// enum keeps this an offline test with no live-test feature plumbing; the
/// self-check below pins that the parse is not truncated.
fn live_service_ports() -> Vec<(String, u16)> {
    let text = std::fs::read_to_string("tests/common/env.rs").expect("read tests/common/env.rs");
    let addr_fn = text
        .split("fn addr(self)")
        .nth(1)
        .expect("env.rs must contain LiveService::addr");
    let mut out = Vec::new();
    for arm in addr_fn.split("LiveService::").skip(1) {
        let Some(name) = arm.split(|c: char| !c.is_alphanumeric()).next() else {
            continue;
        };
        // The port is the second tuple field after the host literal.
        if let Some(rest) = arm.split("\"127.0.0.1\",").nth(1)
            && let Some(port) = rest
                .split(|c: char| !c.is_ascii_digit())
                .find(|s| !s.is_empty())
                .and_then(|s| s.parse::<u16>().ok())
        {
            out.push((name.to_string(), port));
        }
        // Stop at the end of the match block — later LiveService:: mentions in
        // the file (require_alive etc.) are not addr arms.
        if arm.contains("\n    }\n") {
            break;
        }
    }
    out
}

#[test]
fn every_live_service_port_is_mapped_by_a_compose_file() {
    let mut mapped = compose_host_ports("docker-compose.yaml");
    // The optional dev stand maps the engine-version matrix ports (pg18 etc.).
    if std::path::Path::new("dev/stand/docker-compose.yaml").exists() {
        mapped.extend(compose_host_ports("dev/stand/docker-compose.yaml"));
    }
    let services = live_service_ports();
    let orphans: Vec<&(String, u16)> = services
        .iter()
        // Port 0 is the documented "not reached via TCP" sentinel: DuckDb is
        // probed through `docker exec` container health (env.rs says so at the
        // require_alive early-return), so there is no host mapping to demand.
        .filter(|(_, p)| *p != 0 && !mapped.contains(p))
        .collect();
    assert!(
        orphans.is_empty(),
        "LiveService arms claim ports no compose file maps: {orphans:?}. Either the \
         service moved in docker-compose.yaml and env.rs was not updated (every \
         require_alive for it now probes a dead port and reports the service down), or \
         the arm was added ahead of the compose service (its tests can never run). \
         Hand-drifted twins here have produced false 'verified' claims before."
    );
}

#[test]
fn the_port_parse_is_not_truncated() {
    // Self-check, same reason as the workflow-stack guard's: a parser that loses
    // arms makes the assertion above vacuously green. 17 arms today; assert a
    // floor rather than equality so adding a service does not touch this test.
    let services = live_service_ports();
    assert!(
        services.len() >= 15,
        "parsed only {} LiveService port arms from env.rs — the guard above is grading a \
         truncated list: {services:?}",
        services.len()
    );
    for known in ["Postgres", "Mysql", "MssqlGovernor", "Minio"] {
        assert!(
            services.iter().any(|(n, _)| n == known),
            "parser lost `{known}`; parsed: {services:?}"
        );
    }
}
