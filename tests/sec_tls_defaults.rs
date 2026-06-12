//! SEC-RED — cluster `tls-defaults` (V3 + V4).
//!
//! V3 (Postgres / MySQL plaintext default) and V4 (MSSQL trust-any-cert
//! default): with no `tls:` block, rivet connects to Postgres/MySQL over
//! cleartext (`NoTls`) and `trust_cert()`s any certificate for SQL Server,
//! emitting only a `log::warn!`. A passive sniffer or an active MITM on the
//! network path sees the credentials and every exported row.
//!
//! ## Secure policy (per the audit)
//!
//! Plaintext / trust-any-cert to a **loopback** host (127.0.0.1, ::1,
//! localhost) is fine — that is the dev / docker-compose case and the bytes
//! never leave the box. Plaintext to a **remote** host must be a *loud
//! failure*: rivet must refuse before opening a cleartext connection and tell
//! the operator to enable TLS (or to explicitly opt out with `tls.mode:
//! disable`). The invariant to encode is therefore a contrast:
//!
//!     loopback plaintext  → allowed (quiet)
//!     remote   plaintext  → refused (loud, TLS-required)
//!
//! ## Why these are live / `#[ignore]`d and where the RED bites
//!
//! There is **no policy seam today** that decides TLS posture from
//! (config, host): `crate::source::warn_if_tls_disabled` only logs, and the
//! per-engine connect helpers (`postgres::connect_client`,
//! `mysql::connect_pool`, `mssql::MssqlSource::connect_with_tls`) fall through
//! to `NoTls` / `trust_cert()` regardless of host. So the only stable seam
//! that exists *today* is the `rivet` binary itself (driven via `run_rivet`).
//!
//! `sec_remote_plaintext_pg_is_loud` is the genuine RED: against current code
//! `rivet doctor` on a remote, no-TLS config *attempts* a `NoTls` connect to
//! an unroutable host and fails with a generic connection error — never a
//! TLS-required refusal. The test asserts the secure behaviour (a loud,
//! TLS-required refusal that is distinct from a mere connection error), so it
//! FAILS until the loopback-vs-remote gate is added.
//!
//! `sec_loopback_plaintext_pg_is_allowed` is the guardrail half of the same
//! invariant: it asserts the fix must NOT blanket-reject loopback plaintext
//! (the docker / dev path). It passes today and must keep passing once the
//! gate lands, so it is the regression fence that stops the V3/V4 fix from
//! breaking the loopback (docker) workflow.

mod common;
use common::*;

/// Minimal Postgres export config with NO `tls:` block, pointed at `url`.
/// Inline `url:` (not `url_env:`) so the host the policy gate inspects is
/// fully determined by this string.
fn pg_no_tls_config(tmp: &tempfile::TempDir, url: &str, out_dir: &str) -> std::path::PathBuf {
    let yaml = format!(
        r#"source:
  type: postgres
  url: "{url}"
exports:
  - name: sec_tls_probe
    query: "SELECT 1"
    mode: full
    format: csv
    destination:
      type: local
      path: "{out_dir}"
"#
    );
    write_config(tmp, &yaml)
}

/// Heuristic: does this stderr look like a *generic connection error* rather
/// than a deliberate TLS-required policy refusal? The whole point of the fix
/// is that a remote plaintext config must be refused *before* a connect is
/// attempted, with a message about TLS — not after, with a socket error.
fn looks_like_connection_error(stderr_lower: &str) -> bool {
    [
        "connection refused",
        "could not connect",
        "timed out",
        "timeout",
        "no route to host",
        "network is unreachable",
        "connection reset",
        "host unreachable",
        "i/o error",
        "os error",
    ]
    .iter()
    .any(|needle| stderr_lower.contains(needle))
}

/// Heuristic: does this stderr name a TLS-required *policy* (the secure
/// refusal), as opposed to a TLS *handshake* failure (which is a connect-time
/// error, not a config-time policy refusal)?
fn names_tls_required_policy(stderr_lower: &str) -> bool {
    // Must mention TLS/SSL/encryption AND that it is required / the host is
    // remote / a disable opt-in is needed — i.e. a posture decision, not a
    // handshake diagnostic.
    let mentions_tls = stderr_lower.contains("tls")
        || stderr_lower.contains("ssl")
        || stderr_lower.contains("encrypt");
    let mentions_policy = stderr_lower.contains("required")
        || stderr_lower.contains("refus")
        || stderr_lower.contains("remote")
        || stderr_lower.contains("non-loopback")
        || stderr_lower.contains("mode: disable")
        || stderr_lower.contains("mode disable")
        || stderr_lower.contains("plaintext to a remote")
        || stderr_lower.contains("explicit");
    mentions_tls && mentions_policy
}

// SEC-RED V3: PG/MySQL plaintext default — a no-`tls:` config aimed at a
// REMOTE (non-loopback) host must be a loud, TLS-required refusal, NOT a
// silent `NoTls` connect whose only signal is a warn (and, when the host is
// unreachable, a generic connection error). 10.255.255.1 is an unrouted
// RFC5737-style sink that fails fast; the secure code path must refuse on
// policy grounds *before* it ever dials.
#[test]
#[ignore = "live: none (drives the rivet binary; no DB required — refusal must precede any connect)"]
fn sec_remote_plaintext_pg_is_loud() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    // Non-loopback, unroutable host: the secure path refuses on policy before
    // the socket even matters; the vulnerable path attempts a cleartext dial.
    let remote_url = "postgresql://rivet:rivet@10.255.255.1:5432/rivet";
    let cfg = pg_no_tls_config(&tmp, remote_url, out.to_str().unwrap());

    let result = run_rivet_with_warn_log(&["doctor", "--config", cfg.to_str().unwrap()]);
    let stderr = String::from_utf8_lossy(&result.stderr).to_lowercase();

    // 1) It must fail (today a connection error also fails, so this alone is
    //    not the RED — see assertion 2).
    assert!(
        !result.status.success(),
        "remote plaintext (no `tls:`) must NOT succeed; stderr:\n{stderr}"
    );

    // 2) THE RED: the refusal must be a TLS-required POLICY decision, and must
    //    NOT be merely a connection error. Today rivet dials `NoTls` and either
    //    warns + (eventually) reports a socket error, or warns + connects — in
    //    no current path does it refuse a remote plaintext config on TLS-policy
    //    grounds. This assertion fails until the loopback-vs-remote gate exists.
    assert!(
        names_tls_required_policy(&stderr),
        "remote plaintext must be refused with a TLS-required policy message \
         (name TLS + that it is required for a remote host, or that `tls.mode: disable` \
         is the explicit opt-in); got stderr:\n{stderr}"
    );
    assert!(
        !looks_like_connection_error(&stderr),
        "remote plaintext must be refused on POLICY before any connect — the failure \
         must be distinct from a generic connection error; got a connection-error-shaped \
         stderr:\n{stderr}"
    );
}

// SEC-RED V3: the loopback half of the same invariant — the fix must NOT
// blanket-reject plaintext. A no-`tls:` config aimed at a LOOPBACK host
// (127.0.0.1, the docker / dev path) must remain allowed: `rivet doctor`
// against the seeded docker Postgres must still succeed. This passes today
// and must keep passing once the loopback-vs-remote gate lands — it is the
// regression fence that stops the V3/V4 remote-refusal fix from breaking the
// loopback (docker) workflow. Requires the docker pg stack (loopback,
// plaintext — the stack itself has no TLS, which is exactly the case the
// policy must keep allowing).
#[test]
#[ignore = "live: pg (docker loopback, plaintext)"]
fn sec_loopback_plaintext_pg_is_allowed() {
    require_alive(LiveService::Postgres);

    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    // POSTGRES_URL is the 127.0.0.1 loopback docker DB with no TLS — the dev
    // case the secure policy explicitly permits.
    let cfg = pg_no_tls_config(&tmp, POSTGRES_URL, out.to_str().unwrap());

    let result = run_rivet_with_warn_log(&["doctor", "--config", cfg.to_str().unwrap()]);
    let stderr = String::from_utf8_lossy(&result.stderr);

    assert!(
        result.status.success(),
        "loopback (127.0.0.1) plaintext is the docker/dev path and MUST stay allowed — \
         the remote-refusal fix must not blanket-reject loopback plaintext; \
         exit={:?} stderr:\n{stderr}",
        result.status.code()
    );
    // And it must not be refused on TLS-policy grounds either.
    assert!(
        !names_tls_required_policy(&stderr.to_lowercase()),
        "loopback plaintext must NOT trigger a TLS-required refusal; stderr:\n{stderr}"
    );
}
