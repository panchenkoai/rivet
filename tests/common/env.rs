//! Live-service endpoints and reachability gate.
//!
//! Every constant in this file mirrors `docker-compose.yaml`.  Tests that need
//! a live service call `require_alive(LiveService::Foo)` at the top; if the
//! stack is down they fail fast with an actionable message rather than waiting
//! for a driver timeout.

#![allow(dead_code)]

use std::net::TcpStream;
use std::time::Duration;

// ─── Live service endpoints (match docker-compose.yaml) ────────────────────

pub const POSTGRES_HOST: &str = "127.0.0.1";
pub const POSTGRES_PORT: u16 = 5432;
pub const POSTGRES_USER: &str = "rivet";
pub const POSTGRES_PASSWORD: &str = "rivet";
pub const POSTGRES_DB: &str = "rivet";
pub const POSTGRES_URL: &str = "postgresql://rivet:rivet@127.0.0.1:5432/rivet";

/// Same Postgres as `POSTGRES_URL` but routed through Toxiproxy on :15432.
/// Use this for retry / chaos tests that need to inject latency, timeouts or
/// RST mid-connection.
pub const POSTGRES_TOXI_URL: &str = "postgresql://rivet:rivet@127.0.0.1:15432/rivet";

/// pgBouncer in transaction mode with pool_size=1, port :6432.
/// Opt in: docker compose --profile pool up -d pgbouncer
pub const PGBOUNCER_URL: &str = "postgresql://rivet:rivet@127.0.0.1:6432/rivet";

pub const MYSQL_URL: &str = "mysql://rivet:rivet@127.0.0.1:3306/rivet";
pub const MYSQL_TOXI_URL: &str = "mysql://rivet:rivet@127.0.0.1:13306/rivet";

/// ProxySQL in transaction-persistent mode, port :6033.
/// Opt in: docker compose --profile pool up -d proxysql
/// Backend forwards to the same `mysql` service used by `MYSQL_URL`.
pub const PROXYSQL_URL: &str = "mysql://rivet:rivet@127.0.0.1:6033/rivet";

pub const MINIO_ENDPOINT: &str = "http://127.0.0.1:9000";
pub const MINIO_ACCESS_KEY: &str = "minioadmin";
pub const MINIO_SECRET_KEY: &str = "minioadmin";

pub const FAKE_GCS_ENDPOINT: &str = "http://127.0.0.1:4443";

pub const TOXIPROXY_API: &str = "http://127.0.0.1:8474";

// ─── Live-gate: fast reachability probe ────────────────────────────────────

/// Attempt a TCP connect with a short timeout.  Returns `true` if reachable.
fn tcp_alive(host: &str, port: u16, timeout: Duration) -> bool {
    let addr = format!("{host}:{port}");
    addr.to_socket_addrs_alive(timeout).is_some()
}

trait ToSocketAddrsAlive {
    fn to_socket_addrs_alive(&self, timeout: Duration) -> Option<()>;
}

impl ToSocketAddrsAlive for String {
    fn to_socket_addrs_alive(&self, timeout: Duration) -> Option<()> {
        let addr: std::net::SocketAddr = self.parse().ok()?;
        TcpStream::connect_timeout(&addr, timeout).ok().map(|_| ())
    }
}

/// Fail fast with an actionable message if the docker-compose stack is not
/// reachable.  Called at the top of every live test so the failure mode is
/// "cannot connect to postgres on :5432 — did you run `docker compose up -d`?"
/// rather than a driver timeout screen-full of noise.
pub fn require_alive(service: LiveService) {
    let (host, port, hint) = service.addr();
    if !tcp_alive(host, port, Duration::from_millis(500)) {
        panic!(
            "live test requires {service:?} reachable on {host}:{port}.\n\
             hint: {hint}\n\
             run `docker compose up -d` in the project root first."
        );
    }
}

#[derive(Debug, Copy, Clone)]
pub enum LiveService {
    Postgres,
    PostgresToxi,
    Mysql,
    MysqlToxi,
    Minio,
    FakeGcs,
    Toxiproxy,
    PgBouncer,
    ProxySql,
}

impl LiveService {
    fn addr(self) -> (&'static str, u16, &'static str) {
        match self {
            LiveService::Postgres => (
                "127.0.0.1",
                5432,
                "service `postgres` in docker-compose.yaml",
            ),
            LiveService::PgBouncer => (
                "127.0.0.1",
                6432,
                "service `pgbouncer` — run: docker compose --profile pool up -d pgbouncer",
            ),
            LiveService::PostgresToxi => (
                "127.0.0.1",
                15432,
                "service `toxiproxy` — proxied Postgres on :15432",
            ),
            LiveService::Mysql => ("127.0.0.1", 3306, "service `mysql` in docker-compose.yaml"),
            LiveService::MysqlToxi => (
                "127.0.0.1",
                13306,
                "service `toxiproxy` — proxied MySQL on :13306",
            ),
            LiveService::Minio => ("127.0.0.1", 9000, "service `minio` in docker-compose.yaml"),
            LiveService::FakeGcs => (
                "127.0.0.1",
                4443,
                "service `fake-gcs` in docker-compose.yaml",
            ),
            LiveService::Toxiproxy => (
                "127.0.0.1",
                8474,
                "service `toxiproxy` HTTP API in docker-compose.yaml",
            ),
            LiveService::ProxySql => (
                "127.0.0.1",
                6033,
                "service `proxysql` — run: docker compose --profile pool up -d proxysql",
            ),
        }
    }
}
