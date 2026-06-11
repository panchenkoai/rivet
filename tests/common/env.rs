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

/// SQL Server (`service: mssql`, port :1433). The `sa` login + dev password
/// match `docker-compose.yaml`; the `rivet` database is seeded by
/// `dev/mssql/init.sql` (piped through sqlcmd by the live-test setup).
pub const MSSQL_URL: &str = "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet";

/// ProxySQL in transaction-persistent mode, port :6033.
/// Opt in: docker compose --profile pool up -d proxysql
/// Backend forwards to the same `mysql` service used by `MYSQL_URL`.
pub const PROXYSQL_URL: &str = "mysql://rivet:rivet@127.0.0.1:6033/rivet";

pub const MINIO_ENDPOINT: &str = "http://127.0.0.1:9000";
pub const MINIO_ACCESS_KEY: &str = "minioadmin";
pub const MINIO_SECRET_KEY: &str = "minioadmin";

pub const FAKE_GCS_ENDPOINT: &str = "http://127.0.0.1:4443";

/// Azurite (Azure Blob emulator) well-known dev account, key, and blob
/// endpoint. The key is the fixed Azurite default — not a secret.
pub const AZURITE_ENDPOINT: &str = "http://127.0.0.1:10000/devstoreaccount1";
pub const AZURITE_ACCOUNT: &str = "devstoreaccount1";
pub const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
/// Connection string for the `az` CLI (container provisioning, read-back).
pub const AZURITE_CONN_STRING: &str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

pub const TOXIPROXY_API: &str = "http://127.0.0.1:8474";

// ─── ClickHouse + DuckDB (Parquet validators — ADR-0014 §4) ────────────────

pub const CLICKHOUSE_HOST: &str = "127.0.0.1";
pub const CLICKHOUSE_PORT_HTTP: u16 = 8123;
pub const CLICKHOUSE_USER: &str = "rivet";
pub const CLICKHOUSE_PASSWORD: &str = "rivet";
pub const CLICKHOUSE_DB: &str = "rivet";
pub const CLICKHOUSE_HTTP_URL: &str = "http://127.0.0.1:8123";

/// Container name of the long-running `python:3.12-slim + duckdb` validator.
/// Tests reach it via `docker exec`, not TCP, so there is no port constant.
pub const DUCKDB_CONTAINER: &str = "rivet-duckdb";
pub const CLICKHOUSE_CONTAINER: &str = "rivet-clickhouse";

/// Host-side path that is bind-mounted as `/work` inside both `rivet-duckdb`
/// and `rivet-clickhouse`. Tests write Parquet here from Rust, then read the
/// same path from inside the container.
pub fn live_shared_tmp_host() -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(".live-tmp");
    std::fs::create_dir_all(&dir).expect("create tests/.live-tmp");
    dir
}

/// In-container view of [`live_shared_tmp_host`]. Used to build paths that
/// `docker exec` invocations should read.
pub const LIVE_SHARED_TMP_CONTAINER: &str = "/work";

/// Translate a host path under `tests/.live-tmp/...` to its in-container
/// view under `/work/...`. Used by both the DuckDB and ClickHouse helpers
/// (the two validator containers share the same bind mount). Panics if
/// `host` is not inside [`live_shared_tmp_host`] — catches path mistakes
/// early rather than handing the container a missing file.
pub fn live_container_path(host: &std::path::Path) -> String {
    let root = live_shared_tmp_host();
    let rel = host.strip_prefix(&root).unwrap_or_else(|_| {
        panic!(
            "validator containers can only see files under {}; got {}",
            root.display(),
            host.display()
        )
    });
    format!("{}/{}", LIVE_SHARED_TMP_CONTAINER, rel.display())
}

/// Create a fresh sub-directory under the shared bind mount and return its
/// `(host_path, container_path)` pair. Tests use the host path to write
/// Parquet / CSV, and pass the container path to the validator helpers.
pub fn live_shared_workdir(label: &str) -> (std::path::PathBuf, String) {
    let host = live_shared_tmp_host().join(label);
    let _ = std::fs::remove_dir_all(&host);
    std::fs::create_dir_all(&host).expect("create live-tmp workdir");
    let container = live_container_path(&host);
    (host, container)
}

/// Make everything the test wrote under the shared bind mount readable by any
/// uid, so reader containers running as a *different* user can open it.
///
/// rivet writes output files as `0600` (inherited from the tempfile it copies
/// from), owned by the test process. The ClickHouse container reads as the
/// `clickhouse` user (uid 101), so on Linux `file()` hits `EACCES` opening the
/// Parquet. macOS Docker Desktop virtualises bind-mount ownership and hides
/// this. Call this from the reader helpers before the container touches
/// `/work`. Best-effort and recursive: the chmod of the mount root itself may
/// fail (ClickHouse chowns it to uid 101 on startup) but `-R` still relaxes the
/// test-owned subdirs and files underneath, which is what the readers open.
pub fn make_shared_world_readable() {
    let root = live_shared_tmp_host();
    let _ = std::process::Command::new("chmod")
        .arg("-R")
        .arg("a+rX")
        .arg(&root)
        .status();
}

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
    // The `duckdb` validator publishes no TCP port — it is reached via
    // `docker exec`. Probe its container health instead.
    if let LiveService::DuckDb = service {
        require_docker_healthy(DUCKDB_CONTAINER, "duckdb");
        return;
    }
    let (host, port, hint) = service.addr();
    if !tcp_alive(host, port, Duration::from_millis(500)) {
        panic!(
            "live test requires {service:?} reachable on {host}:{port}.\n\
             hint: {hint}\n\
             run `docker compose up -d` in the project root first."
        );
    }
}

fn require_docker_healthy(container: &str, compose_service: &str) {
    let output = std::process::Command::new("docker")
        .args(["inspect", "-f", "{{.State.Health.Status}}", container])
        .output();
    let status = match output {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        Ok(o) => format!(
            "docker inspect failed: {}",
            String::from_utf8_lossy(&o.stderr)
        ),
        Err(e) => format!("docker not available: {e}"),
    };
    if status != "healthy" {
        panic!(
            "live test requires `{container}` healthy (got {status:?}).\n\
             hint: run `docker compose up -d {compose_service}` and wait for healthcheck."
        );
    }
}

#[derive(Debug, Copy, Clone)]
pub enum LiveService {
    Postgres,
    PostgresToxi,
    Mysql,
    MysqlToxi,
    /// SQL Server source engine. TCP :1433.
    Mssql,
    Minio,
    FakeGcs,
    /// Azure Blob emulator (Azurite). Blob endpoint :10000.
    Azurite,
    Toxiproxy,
    PgBouncer,
    ProxySql,
    /// ADR-0014 Parquet validator: `python:3.12-slim + duckdb`. No TCP port —
    /// probed via `docker inspect` health.
    DuckDb,
    /// ADR-0014 Parquet validator: `clickhouse/clickhouse-server`. HTTP :8123.
    ClickHouse,
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
            LiveService::Mssql => ("127.0.0.1", 1433, "service `mssql` in docker-compose.yaml"),
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
            LiveService::Azurite => (
                "127.0.0.1",
                10000,
                "service `azurite` in docker-compose.yaml (blob :10000)",
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
            LiveService::ClickHouse => (
                "127.0.0.1",
                CLICKHOUSE_PORT_HTTP,
                "service `clickhouse` in docker-compose.yaml (HTTP :8123)",
            ),
            // Not used (DuckDb is handled before addr() in require_alive),
            // but kept exhaustive so future TCP-style probes do not panic.
            LiveService::DuckDb => (
                "127.0.0.1",
                0,
                "service `duckdb` — probed via docker inspect, not TCP",
            ),
        }
    }
}
