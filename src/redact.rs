//! **Layer: Cross-cutting helper** (credential redaction invariant, v0.7.2 P0.3)
//!
//! Single chokepoint for stripping plaintext credential material out of
//! strings that are about to land in operator-visible artifacts: logs,
//! `summary.json` / `summary.md`, the run journal, Slack/webhook payloads,
//! and hard-failure error messages bubbling out of any subcommand.
//!
//! The invariant this module backs:
//!
//! > A credential that the operator passed through `password`,
//! > `*_env`, `*_file`, `credentials_file`, or as an embedded
//! > `user:password@host` URL MUST NOT round-trip into any persisted or
//! > emitted artifact.  When in doubt, redact.
//!
//! Scope:
//! - **Embedded-URL passwords**: `scheme://user:password@host…` →
//!   `scheme://REDACTED@host…`.  This is the only pattern Rivet
//!   round-trips through driver/error context, so it is the single
//!   high-value rewrite.  Patches expand here.
//! - **Known token-shape secrets** (AWS access keys etc.) are *not*
//!   matched on shape today — they shouldn't be in stringified error
//!   context unless the operator passed `--source 'aws_access_key_id=AKIA…'`
//!   by mistake.  If a leak vector is discovered, add it here, write a
//!   regression test, and roll a patch release.
//!
//! What this module does NOT guarantee (documented in [`SECURITY.md`]):
//! - Third-party driver/library output that bypasses our error wrappers.
//! - In-memory secrets — `Zeroizing<String>` is used at the source-config
//!   boundary, but anything copied into a `String` along the way may
//!   linger in process memory until allocator reuse.
//! - Secrets the operator captured *outside* Rivet (shell history, env
//!   var dumps, `ps` snapshots) — out of scope.

/// Replace `user:password@host` userinfo segments in any URL-like
/// substring with `REDACTED@host`.
///
/// Conservative match:
/// - scheme is `[A-Za-z][A-Za-z0-9+.\-]*`
/// - followed by `://`
/// - then a userinfo run of non-whitespace, non-`/`, non-`?`, non-`#`
///   characters containing `:` (i.e. `user:password`)
/// - terminated by `@`
///
/// A bare `user@host` (no `:`) is preserved verbatim — there's no
/// password to redact, and stripping the username makes log lines
/// harder to triage.  Operators wanting full userinfo redaction can
/// continue to rely on `SourceConfig::redact_for_artifact` for the
/// structural path.
///
/// Idempotent: once-redacted strings pass through unchanged.
pub fn redact_url_passwords(s: &str) -> String {
    // Find `scheme://userinfo@` segments.  We don't pull in a regex
    // crate just for this one pattern — a hand-rolled walk is faster
    // and avoids a dep that grows the binary.
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = try_redact_at(bytes, i) {
            // `try_redact_at` already wrote into `out`; advance to `end`.
            // The `out` parameter is mutated via the helper for clarity.
            let (rewritten, advance) = end;
            out.push_str(&rewritten);
            i = advance;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// If `bytes[i..]` starts a `scheme://userinfo@` pattern with a `:` in
/// the userinfo (a password segment), return the rewritten prefix and
/// the new cursor position.  Otherwise return `None`.
fn try_redact_at(bytes: &[u8], i: usize) -> Option<(String, usize)> {
    // scheme: must start with an ASCII letter
    if !bytes.get(i).is_some_and(|b| b.is_ascii_alphabetic()) {
        return None;
    }
    let mut j = i + 1;
    while j < bytes.len() {
        let b = bytes[j];
        if b.is_ascii_alphanumeric() || matches!(b, b'+' | b'.' | b'-') {
            j += 1;
        } else {
            break;
        }
    }
    // `://`
    if !bytes[j..].starts_with(b"://") {
        return None;
    }
    let userinfo_start = j + 3;
    // Walk userinfo until terminator.  We require a `:` (password
    // segment) and an `@` before any path/query/whitespace.
    let mut k = userinfo_start;
    let mut has_colon = false;
    while k < bytes.len() {
        let b = bytes[k];
        if b == b'@' {
            break;
        }
        if matches!(b, b'/' | b'?' | b'#') || b.is_ascii_whitespace() {
            return None;
        }
        if b == b':' {
            has_colon = true;
        }
        k += 1;
    }
    if !has_colon || k >= bytes.len() || bytes[k] != b'@' {
        return None;
    }
    // Slice out `scheme://`, replace userinfo with `REDACTED`.
    let scheme_part = std::str::from_utf8(&bytes[i..userinfo_start]).ok()?;
    Some((format!("{scheme_part}REDACTED"), k))
}

/// Compose every redactor.  Use this at every boundary that turns a
/// driver/library error (or any operator-untrusted string) into a
/// persisted or emitted artifact.
pub fn redact_secrets(s: &str) -> String {
    redact_url_passwords(s)
}

/// Convenience: format an `anyhow::Error` with `{:#}` and redact the
/// result in one call.  Use at the boundary of every error-to-artifact
/// path (`summary.error_message = ...`, `log::error!(... e ...)`).
pub fn redact_error(e: &anyhow::Error) -> String {
    redact_secrets(&format!("{e:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── redact_url_passwords ───────────────────────────────────────────────

    #[test]
    fn rewrites_postgres_userinfo_with_password() {
        let s = "connection failed to postgresql://alice:s3cret@db.prod:5432/orders: timeout";
        let out = redact_url_passwords(s);
        assert!(!out.contains("s3cret"), "password must be stripped: {out}");
        assert!(
            out.contains("postgresql://REDACTED@db.prod:5432/orders"),
            "expected REDACTED@host, got: {out}",
        );
    }

    #[test]
    fn rewrites_mysql_userinfo_with_password() {
        let s = "auth error: mysql://root:hunter2@10.0.0.5:3306/billing";
        let out = redact_url_passwords(s);
        assert!(!out.contains("hunter2"));
        assert!(out.contains("mysql://REDACTED@10.0.0.5"));
    }

    #[test]
    fn preserves_bare_user_at_host_without_password() {
        // `user@host` has no password to strip; rewriting it would lose
        // useful triage signal.  Pin the conservative behaviour.
        let s = "connection: postgresql://alice@db.prod:5432/orders";
        assert_eq!(redact_url_passwords(s), s);
    }

    #[test]
    fn idempotent_on_already_redacted_string() {
        let s = "postgresql://REDACTED@db.prod:5432/orders";
        assert_eq!(redact_url_passwords(s), s);
    }

    #[test]
    fn preserves_non_url_text_with_at_sign() {
        // `email@example.com` is not a URL — must not be rewritten.
        let s = "user alice@example.com reported failure";
        assert_eq!(redact_url_passwords(s), s);
    }

    #[test]
    fn handles_multiple_urls_in_one_string() {
        let s = "primary postgresql://a:b@h1/d failed, retrying mysql://c:d@h2/d";
        let out = redact_url_passwords(s);
        assert!(!out.contains("a:b@"));
        assert!(!out.contains("c:d@"));
        assert!(out.contains("postgresql://REDACTED@h1/d"));
        assert!(out.contains("mysql://REDACTED@h2/d"));
    }

    #[test]
    fn stops_at_whitespace_in_userinfo() {
        // Userinfo cannot contain whitespace — defensive guard against
        // matching wild `://foo bar@…` substrings inside prose.
        let s = "scheme://broken token@host";
        assert_eq!(redact_url_passwords(s), s);
    }

    #[test]
    fn preserves_strings_without_urls() {
        let s = "export 'orders' failed: relation does not exist";
        assert_eq!(redact_url_passwords(s), s);
    }

    // ── redact_error ───────────────────────────────────────────────────────

    #[test]
    fn redact_error_strips_password_from_anyhow_chain() {
        let e = anyhow::anyhow!("connect failed to postgresql://alice:s3cret@db.prod/orders");
        let out = redact_error(&e);
        assert!(!out.contains("s3cret"));
        assert!(out.contains("REDACTED@db.prod"));
    }
}
