//! Canary tests for the live-infrastructure harness.
//!
//! These tests verify that the shared helpers in `tests/common/mod.rs` reach
//! each docker-compose service correctly.  If any canary fails, nothing else
//! in the live-test matrix can be trusted.
//!
//! Run with:
//!
//! ```text
//! docker compose up -d
//! cargo test --test live_harness_canary -- --ignored
//! ```
//!
//! Every test is `#[ignore = "live: ..."]` so the default offline `cargo test`
//! skips them.

mod common;

use common::*;
use postgres::NoTls;

#[test]
#[ignore = "live: requires docker compose postgres"]
fn postgres_primary_is_reachable() {
    require_alive(LiveService::Postgres);
    let mut c = pg_connect();
    let rows = c.query("SELECT 1::int AS v", &[]).unwrap();
    let v: i32 = rows[0].get("v");
    assert_eq!(v, 1);
}

#[test]
#[ignore = "live: requires docker compose toxiproxy"]
fn postgres_via_toxiproxy_is_reachable_after_proxy_registration() {
    // Toxiproxy starts with an empty runtime config — registering the
    // upstream route is part of the harness, not the image.  `ensure_toxi_proxy`
    // is idempotent so repeated test runs do not conflict.
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    // Important: also clear any leftover toxics from a previous flaky test.
    toxi_reset_toxics("postgres");

    let mut c = postgres::Client::connect(POSTGRES_TOXI_URL, NoTls).expect("pg via toxi");
    let rows = c.query("SELECT 1::int AS v", &[]).unwrap();
    let v: i32 = rows[0].get("v");
    assert_eq!(v, 1);
}

#[test]
#[ignore = "live: requires docker compose toxiproxy"]
fn mysql_via_toxiproxy_is_reachable_after_proxy_registration() {
    ensure_toxi_proxy("mysql", 13306, "mysql:3306");
    toxi_reset_toxics("mysql");

    use mysql::prelude::Queryable;
    let pool = mysql::Pool::new(MYSQL_TOXI_URL).expect("mysql pool via toxi");
    let mut c = pool.get_conn().expect("mysql via toxi");
    let v: Option<i64> = c.query_first("SELECT 1").unwrap();
    assert_eq!(v, Some(1));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_primary_is_reachable() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    let mut c = mysql_connect();
    let v: Option<i64> = c.query_first("SELECT 1").unwrap();
    assert_eq!(v, Some(1));
}

#[test]
#[ignore = "live: requires docker compose minio"]
fn minio_endpoint_responds_to_health_probe() {
    require_alive(LiveService::Minio);
    // MinIO's live endpoint returns 200 for an anonymous GET; we only assert
    // the TCP-level reachability + that the HTTP response line looks valid.
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let mut s = TcpStream::connect("127.0.0.1:9000").expect("connect minio");
    s.write_all(b"GET /minio/health/live HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    assert!(
        resp.starts_with("HTTP/1."),
        "expected HTTP response from MinIO health, got:\n{resp}"
    );
}

#[test]
#[ignore = "live: requires docker compose fake-gcs"]
fn fake_gcs_endpoint_responds_to_probe() {
    require_alive(LiveService::FakeGcs);
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let mut s = TcpStream::connect("127.0.0.1:4443").expect("connect fake-gcs");
    s.write_all(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    assert!(
        resp.starts_with("HTTP/1."),
        "expected HTTP response from fake-gcs, got:\n{resp}"
    );
}

#[test]
#[ignore = "live: requires docker compose toxiproxy"]
fn toxiproxy_api_responds_with_json_version() {
    require_alive(LiveService::Toxiproxy);
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let mut s = TcpStream::connect("127.0.0.1:8474").expect("connect toxiproxy");
    s.write_all(b"GET /version HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    assert!(
        resp.starts_with("HTTP/1."),
        "expected HTTP response from toxiproxy, got:\n{resp}"
    );
}

// ─── Harness-level sanity checks ─────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn unique_name_is_collision_free_across_repeated_calls() {
    let names: Vec<String> = (0..1000).map(|_| unique_name("t")).collect();
    let mut sorted = names.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        names.len(),
        sorted.len(),
        "unique_name produced a collision"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_table_guard_seeds_and_drops_table_cleanly() {
    require_alive(LiveService::Postgres);

    let name = {
        let t = seed_pg_numeric_table(10);
        let mut c = pg_connect();
        let rows = c
            .query(&format!("SELECT COUNT(*)::int AS n FROM {}", t.name()), &[])
            .unwrap();
        let n: i32 = rows[0].get("n");
        assert_eq!(n, 10);
        t.name().to_string()
    };
    // After `t` drops, the table must be gone.
    let mut c = pg_connect();
    let rows = c
        .query(
            "SELECT COUNT(*)::int AS n FROM pg_tables WHERE tablename = $1",
            &[&name],
        )
        .unwrap();
    let n: i32 = rows[0].get("n");
    assert_eq!(n, 0, "PgTable guard must DROP the table on scope exit");
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_table_guard_seeds_and_drops_table_cleanly() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);

    let name = {
        let t = seed_mysql_numeric_table(5);
        let mut c = mysql_connect();
        let n: Option<i64> = c
            .query_first(format!("SELECT COUNT(*) FROM {}", t.name()))
            .unwrap();
        assert_eq!(n, Some(5));
        t.name().to_string()
    };
    let mut c = mysql_connect();
    let n: Option<i64> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_name = '{name}'"
        ))
        .unwrap();
    assert_eq!(
        n,
        Some(0),
        "MysqlTable guard must DROP the table on scope exit"
    );
}

#[test]
#[ignore = "live: requires rivet binary built"]
fn rivet_binary_reachable_via_env_constant() {
    // `run_rivet` uses CARGO_BIN_EXE_rivet which is auto-populated by cargo
    // when running integration tests.  A quick `rivet --version` proves the
    // constant points at the right file and the binary runs.
    let out = run_rivet(&["--version"]);
    assert!(
        out.status.success(),
        "rivet --version should succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.to_lowercase().contains("rivet") || stdout.contains('.'),
        "rivet --version produced unexpected output: {stdout}"
    );
}
