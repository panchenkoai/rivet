//! The PostgreSQL client dial, shared by the source and the state store.

use postgres::{Client, NoTls};

use super::tls::build_native_tls;
use crate::config::TlsConfig;
use crate::error::Result;

/// Detect whether the connection is going through a transaction-mode pooler
/// (pgBouncer, Odyssey, etc.) by comparing backend PIDs across two implicit
/// transactions. Returns true when PIDs differ — impossible on a direct
/// connection or session-mode pooler where the same physical backend is kept.
///
/// False negatives are the rule behind an idle pooler (both statements land on
/// the same backend), so this feeds a warning and decides nothing.
pub(crate) fn detect_pg_transaction_pooler(client: &mut Client) -> bool {
    let pid1: Option<i32> = client
        .query_one("SELECT pg_backend_pid()", &[])
        .ok()
        .and_then(|r| r.try_get(0).ok());
    let pid2: Option<i32> = client
        .query_one("SELECT pg_backend_pid()", &[])
        .ok()
        .and_then(|r| r.try_get(0).ok());
    matches!((pid1, pid2), (Some(a), Some(b)) if a != b)
}

/// Open a bare `postgres::Client` honoring the configured TLS policy.
///
/// Shared by preflight, doctor, and `rivet init` so every code path that
/// connects to Postgres applies the same transport-security rules. Preflight
/// and doctor pass the YAML `tls:` block; init runs before any YAML exists,
/// so it derives a `TlsConfig` from the URL's `sslmode` parameter (see
/// `crate::init::postgres::connect`). `tls = None` or `mode: disable` falls
/// back to the insecure `NoTls` transport — a warning is logged from
/// `create_source` so operators know TLS is off.
/// Parse `url` into a `postgres::Config` and FORCE `ssl_mode(Require)`.
///
/// The bug this closes: `Client::connect(url, connector)` lets tokio-postgres
/// decide TLS from the URL's own `sslmode` — so `?sslmode=disable`, or the
/// driver's DEFAULT `prefer` against a server that declines TLS, returns a raw
/// PLAINTEXT stream and never touches the connector we built. An operator who
/// wrote `tls: { mode: verify-full }` (or `--tls verify-full`) then ships
/// credentials and every row in cleartext under exit 0. Mongo fixed the
/// identical class by overriding `opts.tls`; this is Postgres's override.
///
/// `ssl_mode(Require)` only forces TLS to be USED; the CONNECTOR
/// (`build_native_tls`) still decides how strictly the cert is checked
/// (require = accept-invalid, verify-ca = chain, verify-full = chain+host), so
/// the four modes keep their meanings — this just stops `disable`/`prefer` in
/// the URL from silently winning over an enforced `tls:` block.
fn pg_config_ssl_forced(url: &str) -> Result<postgres::Config> {
    use std::str::FromStr;
    // Strip the URL's own `sslmode` before parsing: we force Require below, so
    // its value is irrelevant — AND tokio-postgres only accepts disable/prefer/
    // require, rejecting the libpq-valid `verify-ca`/`verify-full` with a parse
    // error (bug hunt 2026-08-08: init derived verify-full from such a URL and
    // then failed to parse it, erroring every time). Dropping it makes any
    // sslmode the operator wrote parseable; the connector decides verification.
    let cleaned = crate::config::url::strip_url_query_key(url, "sslmode");
    let mut config = postgres::Config::from_str(&cleaned).map_err(|e| {
        anyhow::anyhow!("postgres: cannot parse source URL for TLS enforcement: {e}")
    })?;
    config.ssl_mode(postgres::config::SslMode::Require);
    Ok(config)
}

/// The text formats every format-dependent read runs under (UTC, ISO dates, postgres intervals, hex bytea), set for one transaction.
const TXN_FORMATS: &str = "SET LOCAL TimeZone = 'UTC'; SET LOCAL DateStyle = 'ISO, MDY'; \
     SET LOCAL IntervalStyle = 'postgres'; SET LOCAL bytea_output = 'hex'";

/// An open transaction that reads under [`TXN_FORMATS`]; dropped uncommitted, it rolls back.
pub(crate) struct PgTxnGuard<'a> {
    client: &'a mut Client,
    committed: bool,
}

impl<'a> PgTxnGuard<'a> {
    /// `BEGIN` and the formats in one round trip.
    pub(crate) fn begin(client: &'a mut Client) -> std::result::Result<Self, postgres::Error> {
        let txn = Self {
            client,
            committed: false,
        };
        txn.client.batch_execute(&format!("BEGIN; {TXN_FORMATS}"))?;
        Ok(txn)
    }

    pub(crate) fn client_mut(&mut self) -> &mut Client {
        self.client
    }

    pub(crate) fn commit(mut self) -> std::result::Result<(), postgres::Error> {
        self.client.batch_execute("COMMIT")?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PgTxnGuard<'_> {
    fn drop(&mut self) {
        if !self.committed
            && let Err(e) = self.client.batch_execute("ROLLBACK")
        {
            log::warn!("PgTxnGuard: ROLLBACK during drop failed: {e:#}");
        }
    }
}

/// Run `read` in a transaction of its own under [`TXN_FORMATS`], so its text is the same on every server and nothing is left on the session.
pub(crate) fn read_pinned<T>(
    client: &mut Client,
    read: impl FnOnce(&mut Client) -> std::result::Result<T, postgres::Error>,
) -> std::result::Result<T, postgres::Error> {
    let mut txn = PgTxnGuard::begin(client)?;
    let out = read(txn.client_mut())?;
    txn.commit()?;
    Ok(out)
}

/// Dial `url` honoring the TLS policy; no statement is sent, so a pooled server connection is left as it was found.
pub(crate) fn connect_client(url: &str, tls: Option<&TlsConfig>) -> Result<Client> {
    // Refuse remote plaintext (no `tls:` block) before any dial (CWE-319).
    super::require_tls_or_loopback(url, tls)?;
    match tls {
        Some(cfg) if cfg.mode.is_enforced() => {
            let connector = build_native_tls(cfg)?;
            let make_tls = postgres_native_tls::MakeTlsConnector::new(connector);
            // Config::connect, NOT Client::connect(url, …): the forced
            // ssl_mode(Require) overrides the URL's sslmode so the connector is
            // actually used (see pg_config_ssl_forced).
            pg_config_ssl_forced(url)?
                .connect(make_tls)
                .map_err(|e| super::describe_connect_error(url, e.into()))
        }
        _ => Client::connect(url, NoTls).map_err(|e| super::describe_connect_error(url, e.into())),
    }
}

#[cfg(test)]
mod tests {
    use super::pg_config_ssl_forced;

    /// A PostgreSQL server for one client: every simple query is answered (with an error when it says `fail`), and the queries are handed over when the client leaves.
    fn recording_server() -> (String, std::sync::mpsc::Receiver<Vec<String>>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "postgresql://u@127.0.0.1:{}/db",
            listener.local_addr().unwrap().port()
        );
        let (queries_out, queries) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            conn.set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let read_body = |conn: &mut std::net::TcpStream| -> Option<Vec<u8>> {
                let mut len = [0u8; 4];
                conn.read_exact(&mut len).ok()?;
                let mut body = vec![0u8; u32::from_be_bytes(len) as usize - 4];
                conn.read_exact(&mut body).ok()?;
                Some(body)
            };
            let ready = b"Z\0\0\0\x05I";
            read_body(&mut conn).expect("startup");
            conn.write_all(b"R\0\0\0\x08\0\0\0\0").unwrap();
            conn.write_all(ready).unwrap();
            let mut seen = Vec::new();
            let mut tag = [0u8; 1];
            while conn.read_exact(&mut tag).is_ok() && tag[0] == b'Q' {
                let Some(body) = read_body(&mut conn) else {
                    break;
                };
                let sql = String::from_utf8_lossy(&body[..body.len() - 1]).to_string();
                let answer: &[u8] = if sql.contains("fail") {
                    b"E\0\0\0\x1cSERROR\0CXX000\0Mrefused\0\0"
                } else {
                    b"C\0\0\0\x08SET\0"
                };
                seen.push(sql);
                conn.write_all(answer).unwrap();
                conn.write_all(ready).unwrap();
            }
            let _ = queries_out.send(seen);
        });
        (url, queries)
    }

    /// The queries the server was sent, once the client has left.
    fn sent(queries: std::sync::mpsc::Receiver<Vec<String>>) -> Vec<String> {
        queries
            .recv_timeout(std::time::Duration::from_secs(15))
            .expect("the server reports its queries")
    }

    const BEGIN_PINNED: &str = "BEGIN; SET LOCAL TimeZone = 'UTC'; SET LOCAL DateStyle = 'ISO, MDY'; \
         SET LOCAL IntervalStyle = 'postgres'; SET LOCAL bytea_output = 'hex'";

    #[test]
    fn the_dial_sends_no_statement() {
        let (url, queries) = recording_server();
        drop(super::connect_client(&url, None).unwrap());
        assert_eq!(sent(queries), Vec::<String>::new());
    }

    #[test]
    fn a_pinned_read_sets_the_four_formats_for_its_own_transaction_and_commits() {
        let (url, queries) = recording_server();
        let mut client = super::connect_client(&url, None).unwrap();
        let read = super::read_pinned(&mut client, |c| c.batch_execute("SELECT 1").map(|()| 7));
        assert_eq!(read.unwrap(), 7);
        drop(client);
        assert_eq!(sent(queries), [BEGIN_PINNED, "SELECT 1", "COMMIT"]);
    }

    #[test]
    fn a_pinned_read_that_fails_rolls_back_and_returns_the_error() {
        let (url, queries) = recording_server();
        let mut client = super::connect_client(&url, None).unwrap();
        let read = super::read_pinned(&mut client, |c| c.batch_execute("fail"));
        assert_eq!(
            read.unwrap_err().as_db_error().map(|e| e.message()),
            Some("refused")
        );
        drop(client);
        assert_eq!(sent(queries), [BEGIN_PINNED, "fail", "ROLLBACK"]);
    }

    #[test]
    fn a_transaction_dropped_open_is_rolled_back() {
        let (url, queries) = recording_server();
        let mut client = super::connect_client(&url, None).unwrap();
        drop(super::PgTxnGuard::begin(&mut client).unwrap());
        drop(client);
        assert_eq!(sent(queries), [BEGIN_PINNED, "ROLLBACK"]);
    }

    /// The TLS-honesty fix (bug hunt 2026-08-08): under an enforced `tls:`
    /// block the connection's ssl_mode must be forced to Require REGARDLESS of
    /// what the URL's own `sslmode` says — otherwise `?sslmode=disable` (or the
    /// driver's default `prefer` against a TLS-declining server) silently wins
    /// and the connector we built is never used, shipping cleartext under a
    /// `verify-full` claim.
    #[test]
    fn enforced_tls_forces_ssl_mode_require_over_the_urls_sslmode() {
        use postgres::config::SslMode;
        // Every URL sslmode an operator might write — all must come out Require.
        for url in [
            "postgresql://u:p@h/db?sslmode=disable",
            "postgresql://u:p@h/db?sslmode=prefer",
            "postgresql://u:p@h/db", // no param → driver default is `prefer`
            "postgresql://u:p@h/db?sslmode=require",
            // libpq-valid but tokio-postgres-REJECTED values: stripped, not fatal
            "postgresql://u:p@h/db?sslmode=verify-ca",
            "postgresql://u:p@h/db?sslmode=verify-full",
            // sslmode alongside another param — only sslmode is dropped
            "postgresql://u:p@h/db?application_name=x&sslmode=verify-full&connect_timeout=5",
        ] {
            let cfg = pg_config_ssl_forced(url).unwrap_or_else(|e| panic!("parse {url}: {e}"));
            assert_eq!(
                cfg.get_ssl_mode(),
                SslMode::Require,
                "enforced TLS must force Require, but {url} yielded {:?}",
                cfg.get_ssl_mode()
            );
        }
        // A malformed URL is a loud parse error, not a silent plaintext fallback.
        assert!(pg_config_ssl_forced("not a url").is_err());
    }

    /// A `mode: disable` block dials without a connector: the failure is the closed port.
    #[test]
    fn a_disabled_tls_block_dials_in_plaintext() {
        let off = crate::config::TlsConfig {
            mode: crate::config::TlsMode::Disable,
            ca_file: None,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
        };
        let err = super::connect_client("postgresql://u:p@127.0.0.1:1/db", Some(&off))
            .err()
            .expect("nothing listens on port 1");
        let shown = format!("{err:#}");
        assert_eq!(
            shown.split(" \u{2014} ").next(),
            Some("nothing is listening on 127.0.0.1:1")
        );
    }
    /// An enforced `tls:` block opens with the TLS request, not with a plaintext startup.
    #[test]
    fn an_enforced_tls_block_asks_the_server_for_tls_first() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut peer = loop {
                match listener.accept() {
                    Ok((peer, _)) => break peer,
                    Err(_) if std::time::Instant::now() < deadline => {
                        std::thread::sleep(std::time::Duration::from_millis(10))
                    }
                    Err(_) => return None,
                }
            };
            peer.set_nonblocking(false).unwrap();
            peer.set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut first = [0u8; 8];
            peer.read_exact(&mut first).ok()?;
            let _ = peer.write_all(b"N");
            Some(first)
        });
        let on = crate::config::TlsConfig {
            mode: crate::config::TlsMode::Require,
            ca_file: None,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
        };
        let url = format!("postgresql://u:p@127.0.0.1:{port}/db");
        assert!(
            super::connect_client(&url, Some(&on)).is_err(),
            "the server declined TLS"
        );
        assert_eq!(
            server.join().unwrap(),
            Some([0, 0, 0, 8, 4, 210, 22, 47]),
            "the first message is the SSLRequest"
        );
    }
}
