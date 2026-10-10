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
/// False negatives are possible when pool_size = 1 (the same backend is always
/// reused), so this is a best-effort warning rather than a hard guarantee.
fn detect_pg_transaction_pooler(client: &mut Client) -> bool {
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

/// Pin the session's text formats (UTC, ISO dates, postgres intervals, hex bytea) on a fresh
/// connection unless a transaction pooler would leak them to other clients; returns whether one was detected.
pub(crate) fn pin_session_formats(client: &mut Client) -> Result<bool> {
    let transaction_pooler = detect_pg_transaction_pooler(client);
    if !transaction_pooler {
        client.batch_execute(
            "SET TimeZone = 'UTC'; SET DateStyle = 'ISO, MDY'; \
             SET IntervalStyle = 'postgres'; SET bytea_output = 'hex'",
        )?;
    }
    Ok(transaction_pooler)
}

pub(crate) fn connect_client(url: &str, tls: Option<&TlsConfig>) -> Result<Client> {
    let mut client = connect_client_raw(url, tls)?;
    pin_session_formats(&mut client)?;
    Ok(client)
}

/// Dial `url` honoring the TLS policy, with the server's own session defaults.
pub(crate) fn connect_client_raw(url: &str, tls: Option<&TlsConfig>) -> Result<Client> {
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
}
