//! The dial every connection shares: the TLS gate, the connector and the PostgreSQL client.

pub(crate) mod postgres;
pub(crate) mod tls;

use crate::config::TlsConfig;
use crate::config::url::{host_is_loopback, host_port_span};
use crate::error::{Result, TlsHandshakeFailed};

/// True when a rendered driver error names a failed TLS handshake or an untrusted certificate.
pub(crate) fn is_tls_handshake_failure(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    [
        "tls handshake",
        "tlserror",
        "does not support tls",
        "invalid peer certificate",
        "certificate verify failed",
    ]
    .iter()
    .any(|p| t.contains(p))
}

/// Name the `url:` host and port when a connection failed before the server answered —
/// an unresolvable name, a refused port, a timeout — since the driver's text alone
/// does not say which part of the URL is wrong. Other errors pass through unchanged.
pub(crate) fn describe_connect_error(url: &str, err: anyhow::Error) -> anyhow::Error {
    let text = format!("{err:#}").to_ascii_lowercase();
    let (host, port) = url_host_port(url);
    let at = if port.is_empty() {
        host.clone()
    } else {
        format!("{host}:{port}")
    };
    if is_tls_handshake_failure(&text) {
        return TlsHandshakeFailed::wrap(err, Some(&at));
    }
    let hint = if [
        "failed to lookup address",
        "nodename nor servname",
        "name or service not known",
        "no such host",
        "temporary failure in name resolution",
        "dns error",
    ]
    .iter()
    .any(|p| text.contains(p))
    {
        format!("cannot resolve host `{host}` — check the host name in `url:`")
    } else if text.contains("connection refused") {
        format!("nothing is listening on {at} — check the port in `url:` and that the server is up")
    } else if text.contains("timed out") || text.contains("timeout") {
        format!("no answer from {at} — check the host and port in `url:`, and the firewall")
    } else {
        return err;
    };
    anyhow::Error::msg(UnreachableTarget(format!("{hint} (driver: {err:#})")))
}

/// A connect that never reached the server, named by host:port — rivet's own verdict, so no setup hint goes in front.
#[derive(Debug)]
pub(crate) struct UnreachableTarget(String);

impl std::fmt::Display for UnreachableTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The `(host, port)` of a connection URL; the port is empty when the URL has none.
fn url_host_port(url: &str) -> (String, String) {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = rest.split(['/', '?']).next().unwrap_or(rest);
    let hosts = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let first = hosts.split(',').next().unwrap_or(hosts);
    match first.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() && !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            (h.trim_matches(['[', ']']).to_string(), p.to_string())
        }
        _ => (first.trim_matches(['[', ']']).to_string(), String::new()),
    }
}

/// Refuse a URL that carries no host authority (`mysql://`, `postgres:///db`)
/// with a clear parse error, BEFORE any engine-specific setup hint can blanket
/// it (dogfood LOW: `rivet cdc --source mysql://` reported a binlog-grants
/// problem for a host that doesn't exist). No URL echo — the userinfo may hold
/// credentials — and no `user:pass@` pattern in the message (the redactor
/// mangles it).
pub(crate) fn require_url_has_host(url: &str) -> Result<()> {
    if host_port_span(url).is_empty() {
        anyhow::bail!(
            "source: invalid URL — no host found. Expected a URL of the form \
             scheme://host:port/database."
        );
    }
    Ok(())
}

/// Gate plaintext / trust-any-cert connections by host (CWE-319 / CWE-295).
///
/// When no `tls:` block is configured (`tls == None`) **and** the resolved host
/// is not loopback, refuse the connection *before any network I/O* with a
/// TLS-required policy error. This stops the per-engine connect helpers from
/// silently dialing a remote database in cleartext (Postgres/MySQL `NoTls`) or
/// trusting any server certificate (MSSQL `trust_cert`).
///
/// Loopback hosts (docker / local dev) keep today's behaviour — plaintext is
/// allowed there because the bytes never leave the box. An explicit
/// `tls: { mode: disable }` is `Some(..)`, so it is the operator's opt-in to
/// remote plaintext and is **not** refused here.
/// Marker error for the TLS-required policy refusal, so callers whose remedy
/// differs (init: a FLAG, not a config block) can recognize it by type.
#[derive(Debug)]
pub(crate) struct TlsRequiredError;
impl std::fmt::Display for TlsRequiredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TLS required for a remote host")
    }
}
impl std::error::Error for TlsRequiredError {}

pub(crate) fn require_tls_or_loopback(url: &str, tls: Option<&TlsConfig>) -> Result<()> {
    // An explicit `tls: {..}` (including `mode: disable`) is the operator's
    // opt-in and is never refused here — including for a host-LESS URL, which a
    // driver resolves as a LOCAL unix socket (`postgres:///db?host=/var/run/
    // postgresql`). The host-presence check must therefore live INSIDE the
    // no-tls branch: hoisting it above (da7abbf) rejected a valid socket URL that
    // worked on main whenever `tls: { mode: disable }` was set (#16 bughunt).
    if tls.is_none() {
        // A URL with NO host at all (`mysql://`, `postgres:///db`) is not a
        // "remote host" — it is malformed. Prescribing a TLS block there sends the
        // operator chasing a security setting for a host that doesn't exist.
        require_url_has_host(url)?;
    }
    if tls.is_none() && !host_is_loopback(url) {
        // The message must name TLS *and* that it is a policy refusal for a
        // remote host. Emit it at `error` level (→ stderr) as well as returning
        // it: callers like `doctor` print the `Err` to stdout in their own
        // `[FAIL]` style and only re-raise a generic summary, so the log line is
        // what guarantees the TLS-required reason reaches stderr. Deliberately
        // avoids socket-error vocabulary ("could not connect", "timeout", "os
        // error") so it is never mistaken for a connect-time failure.
        let msg = "source: TLS required — refusing to connect to a remote (non-loopback) \
             host without TLS; credentials and every exported row would cross the network \
             in cleartext. Add `source.tls: { mode: verify-full }` (with `ca_file:` for a \
             private CA; not yet supported for Oracle) to enable transport security, or explicitly opt into remote \
             plaintext with `source.tls: { mode: disable }` if this network path is \
             already trusted.";
        log::error!("{msg}");
        // Typed, not just a string: `rivet init` has no config file to add a
        // `tls:` block TO (it generates one), so its dispatch matches on this
        // marker and re-prescribes the `--tls` flag instead — detection by
        // downcast, never by string-matching the message (#146).
        return Err(anyhow::Error::new(TlsRequiredError).context(msg));
    }
    Ok(())
}

#[cfg(test)]
mod connect_error_tests {
    use super::{describe_connect_error, url_host_port};

    fn described(url: &str, driver: &str) -> String {
        format!(
            "{:#}",
            describe_connect_error(url, anyhow::anyhow!("{driver}"))
        )
    }

    #[test]
    fn an_unresolvable_host_is_named_on_every_engine() {
        let cases = [
            (
                "postgresql://u:p@nosuch-host.invalid:5432/db",
                "error connecting to server: failed to lookup address information: nodename nor servname provided, or not known",
            ),
            (
                "mysql://u:p@nosuch-host.invalid:3306/db",
                "DriverError { Could not connect to address `nosuch-host.invalid:3306': failed to lookup address information: nodename nor servname provided, or not known }",
            ),
            (
                "sqlserver://u:p@nosuch-host.invalid:1433/db",
                "mssql: TCP connect failed: failed to lookup address information: nodename nor servname provided, or not known",
            ),
            (
                "mongodb://nosuch-host.invalid:27017/db",
                "Kind: Server selection timeout: No available servers. Topology: { Type: Unknown, Servers: [ { Address: nosuch-host.invalid:27017, Type: Unknown, Error: Kind: I/O error: failed to lookup address information: nodename nor servname provided, or not known, labels: {}, source: None } ] }",
            ),
            (
                "postgresql://u:p@nosuch-host.invalid/db",
                "error connecting to server: Name or service not known",
            ),
        ];
        for (url, driver) in cases {
            let msg = described(url, driver);
            assert!(
                msg.starts_with(
                    "cannot resolve host `nosuch-host.invalid` — check the host name in `url:`"
                ),
                "{url}: {msg}"
            );
            assert!(msg.contains("driver:"), "the driver's text stays: {msg}");
        }
    }

    const MONGO_TLS_EOF: &str = "Kind: Server selection timeout: No available servers. Topology: { Type: Unknown, Servers: [ { Address: 127.0.0.1:27017, Type: Unknown, Error: Kind: I/O error: tls handshake eof, labels: {\"SystemOverloadedError\", \"RetryableError\"}, source: None, server response: None } ] }, labels: {}, source: None, server response: None";
    const MONGO_UNREACHABLE: &str = "Kind: Server selection timeout: No available servers. Topology: { Type: Unknown, Servers: [ { Address: 10.0.0.9:27017, Type: Unknown, Error: Kind: I/O error: connection timed out, labels: {}, source: None } ] }";

    #[test]
    fn a_mongo_tls_handshake_failure_is_permanent_and_says_tls_first() {
        let e = describe_connect_error(
            "mongodb://127.0.0.1:27017/rivet",
            anyhow::anyhow!("{MONGO_TLS_EOF}"),
        );
        assert_eq!(
            crate::pipeline::retry::classify_error(&e),
            crate::pipeline::retry::RetryClass::Permanent
        );
        assert!(
            format!("{e:#}").starts_with("TLS handshake with 127.0.0.1:27017 failed"),
            "{e:#}"
        );
    }

    #[test]
    fn an_unreachable_mongo_host_stays_transient() {
        let e = describe_connect_error(
            "mongodb://10.0.0.9:27017/rivet",
            anyhow::anyhow!("{MONGO_UNREACHABLE}"),
        );
        assert!(crate::pipeline::retry::classify_error(&e).is_transient());
        assert!(format!("{e:#}").starts_with("no answer from 10.0.0.9:27017"));
    }

    #[test]
    fn a_refused_port_and_a_timeout_name_the_endpoint() {
        let refused = described(
            "postgresql://u:p@127.0.0.1:1/db",
            "error connecting to server: Connection refused (os error 61)",
        );
        assert!(
            refused.starts_with("nothing is listening on 127.0.0.1:1"),
            "{refused}"
        );
        let timeout = described(
            "mysql://u:p@10.0.0.9:3306/db",
            "DriverError { Could not connect to address `10.0.0.9:3306': connection timed out }",
        );
        assert!(
            timeout.starts_with("no answer from 10.0.0.9:3306"),
            "{timeout}"
        );
    }

    #[test]
    fn other_errors_pass_through_unchanged() {
        let auth = "MySqlError { ERROR 1045 (28000): Access denied for user 'u'@'%' }";
        assert_eq!(described("mysql://u:p@db.example:3306/db", auth), auth);
    }

    #[test]
    fn host_and_port_come_out_of_any_url_shape() {
        let hp = |u: &str| {
            let (h, p) = url_host_port(u);
            format!("{h}|{p}")
        };
        assert_eq!(
            hp("postgresql://u:p%40ss@db.example:5432/db?sslmode=require"),
            "db.example|5432"
        );
        assert_eq!(hp("mysql://u:p@10.0.0.5/billing"), "10.0.0.5|");
        assert_eq!(
            hp("mongodb://a.example:27017,b.example:27017/db?replicaSet=rs0"),
            "a.example|27017"
        );
        assert_eq!(hp("sqlserver://sa:p@[::1]:1433/db"), "::1|1433");
        assert_eq!(hp("localhost:5432"), "localhost|5432");
    }

    #[test]
    fn a_bracketed_ipv6_host_without_a_port_is_not_split_at_its_colons() {
        assert_eq!(
            url_host_port("postgresql://u:p@[::1]/db"),
            ("::1".to_string(), String::new())
        );
    }
}

#[cfg(test)]
mod tls_gate_tests {
    use super::require_tls_or_loopback;
    use crate::config::{TlsConfig, TlsMode};

    /// The marker must be REACHABLE (downcast finds it on the chain) and must
    /// SAY something (a `Display` stubbed to nothing turns `{:#}` chains into
    /// a trailing colon and empty segment). Both halves lib-side, where the
    /// marker lives — init's flag-naming remedy is tested bin-side.
    #[test]
    fn tls_required_error_is_downcastable_and_self_describing() {
        let err = require_tls_or_loopback("mysql://u:p@203.0.113.9/db", None)
            .expect_err("remote + no tls refuses");
        assert!(
            err.chain()
                .any(|c| c.downcast_ref::<super::TlsRequiredError>().is_some()),
            "the refusal must carry the typed marker"
        );
        let display = format!("{}", super::TlsRequiredError);
        assert!(
            display.contains("TLS required"),
            "the marker's own text must name the policy: {display:?}"
        );
    }

    #[test]
    fn gate_refuses_remote_plaintext_only() {
        let remote = "postgresql://rivet:rivet@10.255.255.1:5432/rivet";
        let loopback = "postgresql://rivet:rivet@127.0.0.1:5432/rivet";
        let disable = TlsConfig {
            mode: TlsMode::Disable,
            ..Default::default()
        };
        let verify = TlsConfig {
            mode: TlsMode::VerifyFull,
            ..Default::default()
        };

        // Remote + no tls block → refused.
        assert!(require_tls_or_loopback(remote, None).is_err());
        // Loopback + no tls block → allowed (docker / dev path).
        assert!(require_tls_or_loopback(loopback, None).is_ok());
        // Explicit `mode: disable` is the remote-plaintext opt-in → allowed.
        assert!(require_tls_or_loopback(remote, Some(&disable)).is_ok());
        // Enforced TLS to a remote host → allowed (the connect path uses TLS).
        assert!(require_tls_or_loopback(remote, Some(&verify)).is_ok());
    }

    #[test]
    fn a_query_host_or_hostaddr_cannot_route_a_loopback_url_off_the_box() {
        for u in [
            "postgresql://u:p@localhost:5432/db?hostaddr=203.0.113.5",
            "postgresql://u:p@127.0.0.1/db?host=db.example.com",
            "postgresql://u:p@localhost/db?sslmode=disable&host=localhost,db.example.com",
        ] {
            assert!(require_tls_or_loopback(u, None).is_err(), "{u}");
        }
        for u in [
            "postgresql://u:p@localhost/db?hostaddr=127.0.0.1",
            "postgresql://u:p@localhost/db?hostaddr=::1",
            "postgresql://u:p@localhost/db?host=/var/run/postgresql",
            "postgresql://u:p@localhost/db?host=%2Fvar%2Frun%2Fpostgresql",
        ] {
            assert!(require_tls_or_loopback(u, None).is_ok(), "{u}");
        }
    }

    #[test]
    fn hostless_url_is_a_parse_error_not_a_tls_refusal() {
        // #dogfood LOW: `mysql://` has NO host, yet the gate reported "remote
        // (non-loopback) host, TLS required" and prescribed a TLS block for a
        // host that doesn't exist. It must be a clear parse error instead.
        for u in ["mysql://", "postgres:///db", "sqlserver://"] {
            let err = require_tls_or_loopback(u, None)
                .expect_err("a host-less URL must error, not connect");
            let msg = err.to_string();
            assert!(
                msg.contains("no host found"),
                "host-less URL must be a parse error: {msg}"
            );
            assert!(
                !msg.contains("TLS required"),
                "host-less URL must NOT prescribe a TLS block: {msg}"
            );
        }
    }

    #[test]
    fn hostless_socket_url_with_explicit_tls_disable_is_allowed() {
        // #16 bughunt: a unix-socket URL has no authority host (the socket path
        // lives in `?host=`), so the host-presence check rejected it. But an
        // explicit `tls: { mode: disable }` is the operator's opt-in for a LOCAL
        // connection — it must connect, as it did on main (where the gate was
        // skipped whenever tls was Some). The check now lives inside the no-tls
        // branch, so tls=Some(disable) is never refused.
        let disable = TlsConfig {
            mode: TlsMode::Disable,
            ..Default::default()
        };
        for u in [
            "postgres:///rivet?host=/var/run/postgresql",
            "mysql://",
            "postgres:///db",
        ] {
            require_tls_or_loopback(u, Some(&disable))
                .expect("an explicit tls: { mode: disable } must not be refused for a socket URL");
        }
    }
}
