//! Shared test helpers for live-infrastructure integration tests.
//!
//! By Rust convention this file lives at `tests/common/mod.rs` (not
//! `tests/common.rs`) so cargo does NOT compile it as its own test binary.
//! Each integration test file that needs these helpers opts in with
//! `mod common;`.
//!
//! ## Why live tests are gated with `#[ignore]`
//!
//! Live tests require the docker-compose stack (see `docker-compose.yaml`) to
//! be running.  We do *not* silently skip them when infrastructure is
//! unreachable — that would let CI pass even when the live-test matrix is
//! actually broken.  Instead, live tests carry `#[ignore = "live: ..."]` so
//! the default `cargo test` run stays offline, and `cargo test -- --ignored`
//! (or `--include-ignored`) opts into live mode.
//!
//! When live tests run against a non-healthy stack they fail with an actionable
//! message (see `require_alive` below) — not a panic from deep inside the
//! `postgres`/`mysql` driver.
//!
//! ## Isolation
//!
//! Every test must allocate its own unique resource names (table, export name,
//! destination prefix, S3 bucket path) so the suite can run with
//! `--test-threads=N` without false-sharing.  Use `unique_name(prefix)` for
//! that — it combines PID and an atomic counter.

#![allow(dead_code)] // helpers are shared across many integration files; some
// binaries use only a subset.

use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use postgres::{Client as PgClient, NoTls};

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

pub const MYSQL_URL: &str = "mysql://rivet:rivet@127.0.0.1:3306/rivet";
pub const MYSQL_TOXI_URL: &str = "mysql://rivet:rivet@127.0.0.1:13306/rivet";

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
}

impl LiveService {
    fn addr(self) -> (&'static str, u16, &'static str) {
        match self {
            LiveService::Postgres => (
                "127.0.0.1",
                5432,
                "service `postgres` in docker-compose.yaml",
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
        }
    }
}

// ─── Unique resource naming (races-free suite parallelism) ─────────────────

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Build a globally-unique identifier safe to use as a SQL table name or an
/// export name.  Combines process id and an atomic counter so parallel
/// `cargo test --test-threads=N` runs do not collide.
pub fn unique_name(prefix: &str) -> String {
    let c = NAME_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("{prefix}_{pid}_{c}")
}

// ─── Postgres helpers ──────────────────────────────────────────────────────

/// Open a fresh Postgres connection to the primary instance.  Panics on
/// failure with the driver's message — callers should call `require_alive`
/// first to get an actionable error if the stack is down.
pub fn pg_connect() -> PgClient {
    PgClient::connect(POSTGRES_URL, NoTls).expect("connect to postgres")
}

/// RAII handle that drops the table on test exit (panic-safe via `Drop`).
/// Without this, a test that seeds `orders_xyz` and then fails leaves the
/// table behind, polluting the next run.
pub struct PgTable {
    name: String,
    // Holds a connection alive — otherwise `Drop` opens a fresh one which is
    // fine but slower.
}

impl PgTable {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for PgTable {
    fn drop(&mut self) {
        // Best-effort cleanup: if the drop fails we've already caused more
        // damage than this can unwind.  Do NOT panic from Drop.
        if let Ok(mut c) = PgClient::connect(POSTGRES_URL, NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.name), &[]);
        }
    }
}

/// Create a uniquely-named Postgres table populated with `row_count` rows of
/// the canonical `(id BIGINT, name TEXT, amount NUMERIC, created_at TIMESTAMPTZ)`
/// shape used throughout the live-test suite.  Returns a `PgTable` guard
/// plus the table name so the caller can inject it into a rivet YAML config.
pub fn seed_pg_numeric_table(row_count: i64) -> PgTable {
    let name = unique_name("rivet_qa_tbl");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            amount NUMERIC(12,2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );"
    ))
    .expect("create test table");

    // Insert in a single VALUES statement — fast enough for row counts up to
    // a few thousand, which is all live tests need.
    if row_count > 0 {
        let mut sql = format!("INSERT INTO {name} (id, name, amount, created_at) VALUES ");
        for i in 0..row_count {
            if i > 0 {
                sql.push_str(", ");
            }
            // created_at spaced one second apart so cursor-based pagination
            // has something deterministic to walk.
            sql.push_str(&format!(
                "({i}, 'row_{i}', {:.2}, now() - ({} || ' seconds')::interval)",
                (i as f64) * 1.5,
                row_count - i
            ));
        }
        c.batch_execute(&sql).expect("seed rows");
    }

    PgTable { name }
}

// ─── MySQL helpers ─────────────────────────────────────────────────────────

/// Open a fresh MySQL connection to the primary instance.
pub fn mysql_connect() -> mysql::PooledConn {
    let pool = mysql::Pool::new(MYSQL_URL).expect("mysql pool");
    pool.get_conn().expect("connect to mysql")
}

/// RAII handle that drops the MySQL table on test exit.
pub struct MysqlTable {
    name: String,
}

impl MysqlTable {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for MysqlTable {
    fn drop(&mut self) {
        use mysql::prelude::Queryable;
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.name));
        }
    }
}

/// MySQL analogue of `seed_pg_numeric_table` — same logical schema so parity
/// tests can assert identical exports across both dialects.
pub fn seed_mysql_numeric_table(row_count: i64) -> MysqlTable {
    use mysql::prelude::Queryable;

    let name = unique_name("rivet_qa_tbl");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;"
    ))
    .expect("create mysql test table");

    if row_count > 0 {
        let mut sql = format!("INSERT INTO {name} (id, name, amount, created_at) VALUES ");
        for i in 0..row_count {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "({i}, 'row_{i}', {:.2}, DATE_SUB(NOW(), INTERVAL {} SECOND))",
                (i as f64) * 1.5,
                row_count - i
            ));
        }
        c.query_drop(sql).expect("seed mysql rows");
    }
    MysqlTable { name }
}

// ─── Rivet binary runner ───────────────────────────────────────────────────

/// Absolute path to the `rivet` binary built for this integration test.
pub const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");

/// Write `yaml` to `<tmpdir>/rivet.yaml` and return the path.  The tempdir is
/// returned too so the caller can keep it alive for the duration of the run.
pub fn write_config(tmpdir: &tempfile::TempDir, yaml: &str) -> PathBuf {
    let path = tmpdir.path().join("rivet.yaml");
    std::fs::write(&path, yaml).expect("write rivet config");
    path
}

/// Run `rivet <args...>` and capture stdout/stderr.  Panics if the process
/// cannot be spawned (which indicates a build-time problem, not a test
/// failure).
pub fn run_rivet(args: &[&str]) -> Output {
    Command::new(RIVET_BIN)
        .args(args)
        .output()
        .expect("spawn rivet binary")
}

/// Convenience: `rivet run --config <path> --export <name>` and return the
/// captured output.  Caller is responsible for asserting exit code / contents.
pub fn run_rivet_export(config_path: &std::path::Path, export_name: &str) -> Output {
    run_rivet(&[
        "run",
        "--config",
        config_path.to_str().unwrap(),
        "--export",
        export_name,
    ])
}

// ─── Toxiproxy admin client (minimal, HTTP/1.0 over TcpStream) ─────────────
//
// We avoid pulling in `reqwest` for the admin path because (a) it is already a
// dependency but its blocking client would spin up a tokio runtime per call
// and (b) our needs are tiny — create/delete a proxy, add a toxic, remove it.
// A raw HTTP/1.0 writer + one-shot TcpStream read is plenty.

/// Execute a one-shot HTTP/1.0 request against the Toxiproxy admin API and
/// return the (status_code, body) pair.  Panics on IO errors — this is a
/// test harness, not production code.
fn toxi_admin(method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    use std::io::{Read, Write};
    let mut s = TcpStream::connect("127.0.0.1:8474").expect("connect toxiproxy api");
    let body_bytes = body.unwrap_or("").as_bytes();
    let mut req = format!(
        "{method} {path} HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body_bytes.len()
    )
    .into_bytes();
    req.extend_from_slice(body_bytes);
    s.write_all(&req).expect("write toxi request");
    let mut raw = String::new();
    let _ = s.read_to_string(&mut raw);
    // Parse status line + body split at the first blank line.
    let status: u16 = raw
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, rest)| rest)
        .unwrap_or("")
        .to_string();
    (status, body)
}

/// Ensure that Toxiproxy has a proxy named `name` forwarding `listen_port` to
/// `upstream` (e.g. `"postgres:5432"`).  If the proxy already exists it is
/// left in place (idempotent).  Returns when the proxy is ready to accept
/// traffic.
///
/// Toxiproxy starts with an empty runtime configuration; without this helper
/// every live test routed through :15432 / :13306 hits a reset connection.
pub fn ensure_toxi_proxy(name: &str, listen_port: u16, upstream: &str) {
    require_alive(LiveService::Toxiproxy);
    let payload = format!(
        r#"{{"name":"{name}","listen":"0.0.0.0:{listen_port}","upstream":"{upstream}","enabled":true}}"#
    );
    let (code, body) = toxi_admin("POST", "/proxies", Some(&payload));
    // 201 Created = first time. 409 Conflict or 500-with-existing = already
    // present from an earlier test (toxiproxy does not expose a native
    // "create or update", so we treat both as success).
    assert!(
        code == 201 || code == 409 || body.contains("already exists"),
        "toxiproxy POST /proxies for '{name}' returned unexpected status {code}: {body}"
    );
}

/// Remove every toxic from the named proxy (does NOT delete the proxy itself).
/// Use at the start of a test to reset state left by a previous flaky run.
pub fn toxi_reset_toxics(name: &str) {
    require_alive(LiveService::Toxiproxy);
    let (code, body) = toxi_admin("GET", &format!("/proxies/{name}/toxics"), None);
    if code != 200 {
        return;
    }
    // Crude JSON parse: each toxic is preceded by a `"name":"..."` field.
    // Extract those names and DELETE each.
    for toxic_name in body
        .split("\"name\":\"")
        .skip(1)
        .filter_map(|chunk| chunk.split('"').next())
    {
        let _ = toxi_admin(
            "DELETE",
            &format!("/proxies/{name}/toxics/{toxic_name}"),
            None,
        );
    }
}

/// Add a latency toxic to a proxy: every byte in the downstream direction is
/// delayed by `latency_ms` milliseconds.  Returns the toxic name (so tests
/// can remove it explicitly); use `toxi_reset_toxics` for broad cleanup.
pub fn toxi_add_latency(proxy: &str, latency_ms: u64) -> String {
    let toxic_name = unique_name("rivet_latency");
    let payload = format!(
        r#"{{"name":"{toxic_name}","type":"latency","stream":"downstream","toxicity":1.0,"attributes":{{"latency":{latency_ms},"jitter":0}}}}"#
    );
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}/toxics"), Some(&payload));
    assert!(
        code == 200,
        "add latency to {proxy}: status {code}, body:\n{body}"
    );
    toxic_name
}

/// Disable the proxy — every open connection is closed and new ones refused
/// until re-enabled.  Simulates a hard "database went away" event.
pub fn toxi_disable(proxy: &str) {
    let payload = r#"{"enabled":false}"#;
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}"), Some(payload));
    assert!(code == 200, "disable {proxy}: status {code}, body:\n{body}");
}

/// Re-enable a disabled proxy.
pub fn toxi_enable(proxy: &str) {
    let payload = r#"{"enabled":true}"#;
    let (code, body) = toxi_admin("POST", &format!("/proxies/{proxy}"), Some(payload));
    assert!(code == 200, "enable {proxy}: status {code}, body:\n{body}");
}

/// RAII cross-process lock guard for the Toxiproxy admin API.
///
/// Closing the file descriptor (happens automatically on `Drop`) releases the
/// kernel-held `flock(2)` lock.  Works across cargo's parallel integration
/// test binaries — an in-process `Mutex` would not, since each test binary
/// is a separate OS process with its own static state.
pub struct ToxiproxyGuard {
    _file: std::fs::File,
}

/// Acquire the global Toxiproxy serialisation lock.
///
/// The admin API is a shared resource across the entire test suite.  Cargo
/// executes integration test binaries in parallel by default, so a plain
/// in-process mutex would not serialise `toxi_disable` / `toxi_enable` /
/// toxic-lifecycle calls across binaries.  We use an advisory `flock(2)`
/// exclusive lock on a well-known file in the temp dir; the kernel releases
/// it automatically when the file descriptor is closed — even if the test
/// panics mid-run.
///
/// Call this at the top of any test that mutates Toxiproxy state:
///
/// ```ignore
/// #[test]
/// fn my_toxi_test() {
///     let _g = toxiproxy_guard();
///     toxi_add_latency(...);
/// }
/// ```
///
/// Unix-only — live tests are Linux/macOS in CI.  A Windows CI runner would
/// need a different backend (e.g. `LockFileEx`), but the live-test matrix
/// does not target Windows.
pub fn toxiproxy_guard() -> ToxiproxyGuard {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("rivet_qa_toxiproxy.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|e| panic!("open toxiproxy lock {}: {e}", path.display()));
    // SAFETY: `file` owns the fd; `flock` does not transfer ownership.
    // LOCK_EX blocks this thread until the lock is acquired.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        panic!(
            "flock(LOCK_EX) on {} failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    ToxiproxyGuard { _file: file }
}

// ─── Object-storage bucket provisioning ────────────────────────────────────

/// Idempotently create `bucket` in the local MinIO instance via `mc` inside
/// the running container.  Does nothing if the bucket already exists.
///
/// Implementation: `docker compose exec -T minio sh -c "mc alias set ... && mc mb -p local/<bucket>"`.
/// Uses `-T` so cargo does not fight with the container for a TTY.  Panics
/// with an actionable message if `docker` is not on PATH — live tests need
/// it anyway.
pub fn ensure_minio_bucket(bucket: &str) {
    require_alive(LiveService::Minio);
    let script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
         mc mb -p local/{bucket} >/dev/null 2>&1 || true"
    );
    let status = Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c", &script])
        .status()
        .expect(
            "failed to spawn `docker compose exec minio` — \
             live tests for S3/MinIO require docker CLI on PATH",
        );
    assert!(
        status.success(),
        "`mc mb local/{bucket}` inside minio container failed with {status}"
    );
}

/// Idempotently create `bucket` in the fake-gcs server via its HTTP API.
/// The server exposes a create-bucket endpoint that does not require auth.
pub fn ensure_gcs_bucket(bucket: &str) {
    require_alive(LiveService::FakeGcs);
    use std::io::{Read, Write};
    let mut s = TcpStream::connect("127.0.0.1:4443").expect("connect fake-gcs");
    let body = format!(r#"{{"name":"{bucket}"}}"#);
    let req = format!(
        "POST /storage/v1/b?project=test HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).expect("write gcs req");
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    // 200 / 201 (fresh create) or 409 (already exists) — both acceptable.
    let status_ok = resp.starts_with("HTTP/1.0 200")
        || resp.starts_with("HTTP/1.0 201")
        || resp.starts_with("HTTP/1.1 200")
        || resp.starts_with("HTTP/1.1 201")
        || resp.contains(" 409 ");
    assert!(
        status_ok,
        "fake-gcs bucket create returned unexpected response:\n{resp}"
    );
}

// ─── Produced-file discovery ───────────────────────────────────────────────

/// Collect every file with the given extension under `dir` (non-recursive).
/// Useful for locating the timestamped output rivet just wrote.
pub fn files_with_extension(dir: &std::path::Path, ext: &str) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return vec![];
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|e| e == ext))
        .map(|e| e.path())
        .collect()
}
