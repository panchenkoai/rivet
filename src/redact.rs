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
    //
    // F-NEW-C (0.7.5 audit): the previous version copied non-matching
    // bytes one at a time via `out.push(bytes[i] as char)`, which
    // re-interpreted each UTF-8 byte as a Unicode code point and
    // re-encoded it.  Every multi-byte glyph (em-dash, Cyrillic, …)
    // became double-encoded mojibake (`—` → `â\u{80}\u{94}`) in any
    // error message that hit the redactor.  Correct fix: copy the
    // next UTF-8 codepoint as a whole slice of `s`, not byte-by-byte.
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some((rewritten, advance)) = try_redact_at(bytes, i) {
            out.push_str(&rewritten);
            i = advance;
            continue;
        }
        let b = bytes[i];
        if b.is_ascii() {
            out.push(b as char);
            i += 1;
        } else {
            // Multi-byte UTF-8 codepoint starting at `i`; continuation
            // bytes have the form 10xxxxxx.  Copy the whole codepoint
            // verbatim from the source string.
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i] & 0xC0) == 0x80 {
                i += 1;
            }
            out.push_str(&s[start..i]);
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
    // Walk the authority until the path/query/fragment/whitespace terminator,
    // tracking the LAST `@` we cross. A password may itself contain `@`
    // (`user:p@ssw0rd@host`), so splitting at the FIRST `@` would leak the tail
    // after it; the userinfo terminator is the last `@` before the path. A raw
    // `/` in the password would make the URL ambiguous — which is why a
    // Rivet-constructed URL percent-encodes the userinfo (`build_url_from_fields`),
    // so the password's `/`/`?`/`#` never appears raw here. `has_colon` must reflect
    // a `:` *within the userinfo* (before that last `@`), not a host:port colon
    // after it, so we recompute it from the chosen `@`.
    let mut k = userinfo_start;
    let mut last_at: Option<usize> = None;
    while k < bytes.len() {
        let b = bytes[k];
        if b == b'@' {
            last_at = Some(k);
        } else if matches!(b, b'/' | b'?' | b'#') || b.is_ascii_whitespace() {
            break;
        }
        k += 1;
    }
    let at = last_at?;
    let has_colon = bytes[userinfo_start..at].contains(&b':');
    if !has_colon {
        return None;
    }
    // Slice out `scheme://`, replace userinfo with `REDACTED`.
    let scheme_part = std::str::from_utf8(&bytes[i..userinfo_start]).ok()?;
    Some((format!("{scheme_part}REDACTED"), at))
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

/// Render one log record into a redacted, operator-visible line.
///
/// The module scope names **logs** as a redaction target, but the `log::*`
/// macros bypass the artifact-path redaction that is wired by hand at the
/// error/summary call sites — a `log::warn!("…{e}", e)` whose `e` captured a
/// `scheme://user:password@host` connect error would otherwise print the
/// password to stderr. `main`'s `env_logger` formatter delegates here so the
/// log **sink** itself is the chokepoint: every line, present and future,
/// passes through [`redact_secrets`] with no reliance on each call site
/// remembering to redact. Kept in this module (beside the other redactors) and
/// log-crate-agnostic (`level` is a pre-rendered `&str`) so the wiring is
/// unit-testable without capturing global stderr.
pub fn redacted_log_line(timestamp: &str, level: &str, target: &str, message: &str) -> String {
    redact_secrets(&format!("[{timestamp} {level} {target}] {message}"))
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

    // ── F-NEW-C: multi-byte UTF-8 must round-trip ─────────────────────────────

    #[test]
    fn preserves_em_dash_and_other_multibyte_glyphs() {
        // Before the F-NEW-C fix, the byte-by-byte loop in
        // `redact_url_passwords` double-encoded every non-ASCII codepoint:
        // the em-dash `—` (UTF-8 e2 80 94) came out as `â\u{80}\u{94}`
        // (c3 a2 c2 80 c2 94).  This test pins the round-trip so the
        // regression cannot return silently in any error message.
        let s = "export 'orders': --resume refused — destination prefix has _SUCCESS";
        assert_eq!(
            redact_url_passwords(s),
            s,
            "non-URL text containing an em-dash must pass through unchanged"
        );

        let s2 = "сообщение об ошибке: cannot connect";
        assert_eq!(
            redact_url_passwords(s2),
            s2,
            "Cyrillic text must pass through unchanged"
        );

        // And it must still redact correctly when the string contains
        // BOTH multi-byte glyphs and a redactable URL.
        let s3 = "ошибка — postgresql://u:p@host/db: dropped";
        let out = redact_url_passwords(s3);
        assert!(out.contains("ошибка — postgresql://REDACTED@host/db"));
        assert!(!out.contains("u:p@"));
    }

    // ── SEC-RED: embedded `@` in password must not leak ───────────────────────

    #[test]
    fn sec_redact_url_password_with_at() {
        // SEC-RED V8: redact_url_passwords splits userinfo at the FIRST `@`,
        // leaking the password tail after an embedded `@`. The userinfo walk in
        // `try_redact_at` breaks on the first `@` it sees, so the password
        // `p@ssw0rd` is split: only `p` is treated as the password and the
        // tail `ssw0rd` survives in the output. The userinfo terminator must be
        // the LAST `@` before the path/query (rfind semantics, as already used
        // by redact_pg_url in state/mod.rs).
        let s = "connect failed to postgresql://rivet:p@ssw0rd@db.example.com:5432/orders";
        let out = redact_url_passwords(s);
        // No fragment of the password may survive. `ssw0rd` is the tail that
        // leaks today.
        assert!(
            !out.contains("ssw0rd"),
            "password tail after embedded @ must not leak: {out}"
        );
        assert!(
            !out.contains("p@ssw0rd"),
            "full password must not leak: {out}"
        );
        // Host and path must be retained, redacted to REDACTED@host.
        assert!(
            out.contains("postgresql://REDACTED@db.example.com:5432/orders"),
            "expected REDACTED@host with embedded-@ password stripped, got: {out}"
        );

        // Guard: a normal password (no embedded @) still redacts correctly so
        // this test pins the fix rather than just any change.
        let normal = "connect failed to postgresql://rivet:s3cret@db.example.com:5432/orders";
        let normal_out = redact_url_passwords(normal);
        assert!(
            !normal_out.contains("s3cret"),
            "normal password must still be redacted: {normal_out}"
        );
        assert!(
            normal_out.contains("postgresql://REDACTED@db.example.com:5432/orders"),
            "normal password redaction unchanged: {normal_out}"
        );
    }
}
