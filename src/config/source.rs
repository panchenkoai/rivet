//! Source-database connection config: URL/structured fields, TLS, environment hints.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::resolve::resolve_env_vars;
use crate::tuning::{TuningConfig, TuningProfile};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: SourceType,

    pub url: Option<String>,
    pub url_env: Option<String>,
    pub url_file: Option<String>,

    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub password_env: Option<String>,
    pub database: Option<String>,

    /// Operational profile of the source database.
    ///
    /// Selects the **default** tuning profile when none is explicitly set in
    /// `source.tuning.profile` or `export.tuning.profile`:
    ///
    /// | `environment`           | default profile |
    /// |-------------------------|------------------|
    /// | `production` (default)  | `balanced` (50 ms throttle, 10 k batch, retries) |
    /// | `replica`               | `balanced` |
    /// | `local`                 | `fast` (no throttle, 50 k batch — saves ~30% wall on localhost) |
    ///
    /// Explicit `tuning.profile:` always wins over this hint.
    #[serde(default)]
    pub environment: Option<SourceEnvironment>,

    #[serde(default)]
    pub tuning: Option<TuningConfig>,

    /// Transport security settings (ADR: SecOps). When absent, Rivet connects
    /// without TLS — a warning is emitted so operators are aware. See [`TlsConfig`].
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

/// Operational environment of the source database — drives the default tuning
/// profile when none is explicitly set. Opt-in: existing configs without
/// `environment:` continue to use `balanced` as today.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceEnvironment {
    /// Localhost / Docker compose / read-only container — no throttle by default
    /// (compiles to `fast` profile defaults). Use when DB load is not a concern.
    Local,
    /// Read replica — `balanced` default. Same throttle as production, but free
    /// to dial up `tuning.batch_size`.
    Replica,
    /// Live production primary — `balanced` default. Bias toward source-safety.
    Production,
}

impl SourceEnvironment {
    /// Default tuning profile selected by this environment when the user has
    /// not set `tuning.profile:` explicitly.
    pub fn default_profile(self) -> TuningProfile {
        match self {
            SourceEnvironment::Local => TuningProfile::Fast,
            SourceEnvironment::Replica | SourceEnvironment::Production => TuningProfile::Balanced,
        }
    }
}

/// Transport security for the source database connection.
///
/// Credentials and exported data cross the wire on every connection; without TLS
/// they are visible to anyone on the network path (cloud inter-VPC, cross-AZ, or
/// a compromised upstream). The default for all new connections is
/// [`TlsMode::Require`] when `tls:` is present; setting `tls: { mode: disable }`
/// is explicit opt-out.
///
/// ```yaml
/// source:
///   type: postgres
///   url_env: DATABASE_URL
///   tls:
///     mode: verify-full
///     ca_file: /etc/ssl/certs/rds-ca-2019-root.pem
/// ```
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Enforcement level. See [`TlsMode`].
    #[serde(default)]
    pub mode: TlsMode,
    /// PEM-encoded CA certificate to trust for server verification. Required
    /// for [`TlsMode::VerifyCa`] and [`TlsMode::VerifyFull`] against a private CA.
    pub ca_file: Option<String>,
    /// Accept certificates not chained to a trusted CA. Dangerous — disables
    /// server authentication — and only honored when explicitly `true`.
    #[serde(default)]
    pub accept_invalid_certs: bool,
    /// Accept certificates whose subjectAltName does not match the connection
    /// hostname. Dangerous — disables hostname verification.
    #[serde(default)]
    pub accept_invalid_hostnames: bool,
}

/// TLS enforcement mode, mirroring libpq's `sslmode` semantics where possible.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    /// Plaintext. Use only inside trusted networks (loopback, cgroup-private).
    Disable,
    /// Require a TLS handshake; accept the server certificate without verifying
    /// issuer or hostname. Protects against passive sniffing, not MITM.
    Require,
    /// TLS + verify certificate chains to the configured / system trust store.
    /// Does not check hostname (useful for IP-addressed or internal names).
    VerifyCa,
    /// TLS + verify chain **and** hostname against the server cert's SAN/CN.
    /// Recommended default for production.
    #[default]
    VerifyFull,
}

impl TlsMode {
    pub fn is_enforced(self) -> bool {
        !matches!(self, TlsMode::Disable)
    }
}

impl SourceConfig {
    /// Return a copy of this config with **all plaintext credential material stripped**,
    /// safe to embed in a persisted [`crate::plan::PlanArtifact`] (ADR-0005 PA9).
    ///
    /// Redaction rules:
    /// - `password` → always `None` (plaintext password never leaves the process).
    /// - `url` containing `user[:password]@` → userinfo segment replaced with `"REDACTED"`.
    /// - `url_env`, `url_file`, `password_env` — kept (env var **names** and file paths
    ///   are references, not secrets; `apply` needs them to re-resolve credentials).
    /// - `host`, `port`, `user`, `database` — kept (structured connection metadata).
    ///
    /// If a plaintext `password` or `url` is redacted, callers should surface a warning
    /// to the operator so env/file-based auth is available at apply time.
    pub fn redact_for_artifact(&self) -> (Self, bool) {
        let mut out = self.clone();
        let mut redacted = false;

        if out.password.is_some() {
            out.password = None;
            redacted = true;
        }

        if let Some(ref raw) = out.url
            && let Some((userinfo_end, scheme_end)) = find_userinfo(raw)
        {
            let mut s = String::with_capacity(raw.len());
            s.push_str(&raw[..scheme_end]); // "postgresql://"
            s.push_str("REDACTED");
            s.push_str(&raw[userinfo_end..]); // "@host:port/db…"
            out.url = Some(s);
            redacted = true;
        }

        (out, redacted)
    }

    pub(crate) fn has_structured_fields(&self) -> bool {
        self.host.is_some()
            || self.user.is_some()
            || self.database.is_some()
            || self.password.is_some()
            || self.password_env.is_some()
    }

    pub(crate) fn has_url_fields(&self) -> bool {
        self.url.is_some() || self.url_env.is_some() || self.url_file.is_some()
    }

    fn build_url_from_fields(&self) -> crate::error::Result<String> {
        // First-user-friendly errors: name the missing field, suggest a
        // concrete value, and remind the operator that `url_env` is the
        // alternative path so they don't bounce.  See
        // `docs/getting-started.md` for the full onboarding flow.
        let host = self.host.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "source: structured config is missing 'host'.\n  Hint: add `host: localhost` (or your DB host) under `source:` in rivet.yaml.\n  Or switch to URL-based config: `url_env: DATABASE_URL`."
            )
        })?;
        let user = self.user.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "source: structured config is missing 'user'.\n  Hint: add `user: <username>` under `source:` in rivet.yaml."
            )
        })?;
        let database = self.database.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "source: structured config is missing 'database'.\n  Hint: add `database: <dbname>` under `source:` in rivet.yaml."
            )
        })?;

        // SecOps: keep the plaintext password inside a `Zeroizing<String>` until it
        // is spliced into the final URL, so the standalone password buffer is
        // wiped on drop (the final URL still lives as a plain String but is
        // shorter-lived and dropped by the driver constructor).
        let password: zeroize::Zeroizing<String> =
            zeroize::Zeroizing::new(match (&self.password, &self.password_env) {
                (Some(_), Some(_)) => {
                    anyhow::bail!("source: specify 'password' or 'password_env', not both");
                }
                (Some(p), None) => {
                    static WARNED: std::sync::Once = std::sync::Once::new();
                    WARNED.call_once(|| {
                        log::warn!(
                            "source config contains plaintext password -- consider using password_env"
                        );
                    });
                    resolve_env_vars(p)?
                }
                (None, Some(env)) => std::env::var(env).map_err(|_| {
                    anyhow::anyhow!(
                        "source: env var '{0}' is not set (referenced by password_env).\n  Hint: export the value before running, e.g.\n      export {0}='your-database-password'",
                        env
                    )
                })?,
                (None, None) => String::new(),
            });

        let default_port = match self.source_type {
            SourceType::Postgres => 5432,
            SourceType::Mysql => 3306,
            SourceType::Mssql => 1433,
        };
        let port = self.port.unwrap_or(default_port);

        let scheme = match self.source_type {
            SourceType::Postgres => "postgresql",
            SourceType::Mysql => "mysql",
            SourceType::Mssql => "sqlserver",
        };

        if password.is_empty() {
            Ok(format!(
                "{}://{}@{}:{}/{}",
                scheme, user, host, port, database
            ))
        } else {
            Ok(format!(
                "{}://{}:{}@{}:{}/{}",
                scheme,
                user,
                password.as_str(),
                host,
                port,
                database
            ))
        }
    }

    pub fn resolve_url(&self) -> crate::error::Result<String> {
        if self.has_url_fields() && self.has_structured_fields() {
            anyhow::bail!(
                "source: pick either URL-based config (url/url_env/url_file) OR structured fields (host/user/database/port/password_env), not both.\n  Hint: remove whichever block you don't want; mixing the two is ambiguous."
            );
        }

        if self.has_structured_fields() {
            return self.build_url_from_fields();
        }

        // Capture *where* the URL came from so the password warning below
        // can be specific: scolding an operator who already used
        // `url_env:` (the recommendation!) for "considering url_env" is
        // misleading and trains them to tune out our warnings.
        //
        // The `EnvVar(&str)` / `File(&str)` payloads are retained for
        // future use (e.g. mentioning the env-var name in a richer
        // diagnostic later) — `#[allow(dead_code)]` keeps clippy quiet
        // while we keep the slot open. Renaming the variants to unit
        // would lose the documentation that "this came from <name>".
        #[allow(dead_code)]
        enum UrlSource<'a> {
            InlineYaml,
            EnvVar(&'a str),
            File(&'a str),
        }
        let (raw, source) = match (&self.url, &self.url_env, &self.url_file) {
            (Some(u), None, None) => (u.clone(), UrlSource::InlineYaml),
            (None, Some(env), None) => (
                std::env::var(env).map_err(|_| {
                    anyhow::anyhow!(
                        "source: env var '{0}' is not set (referenced by url_env).\n  Hint: export the value before running, e.g.\n      export {0}='postgresql://user:pass@host:5432/dbname'\n  Or change `url_env: {0}` in your config to a different env var name.",
                        env
                    )
                })?,
                UrlSource::EnvVar(env),
            ),
            (None, None, Some(file)) => (
                std::fs::read_to_string(file)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "source: cannot read url_file '{}': {}.\n  Hint: ensure the file exists and is readable; the file should contain only the URL on a single line.",
                            file,
                            e
                        )
                    })?
                    .trim()
                    .to_string(),
                UrlSource::File(file),
            ),
            _ => anyhow::bail!(
                "source: configure exactly one connection method:\n  url_env: DATABASE_URL                          (URL from env var — recommended)\n  url: 'postgresql://user:pass@host:5432/db'      (inline — not recommended for committed configs)\n  url_file: /etc/rivet/source.url                 (URL from file — rotation-friendly)\n  host/user/database/...                          (structured fields under `source:`)"
            ),
        };

        let resolved = resolve_env_vars(&raw)?;

        if resolved.contains('@')
            && resolved.contains(':')
            && let Some(userinfo) = resolved.split('@').next()
            && userinfo.contains(':')
            && !userinfo.ends_with(':')
        {
            // `resolve_url` is called from many places per run (plan build,
            // doctor, every export, every chunk worker). Fire each variant
            // of this warning exactly once per process so operators see
            // one clean nudge, not 3-4 stacked copies in stderr.
            //
            // Only the InlineYaml case is a real misconfiguration to flag:
            // the password is sitting in a committed file. EnvVar / File
            // sources are explicitly the recommended forms — scolding an
            // operator who already uses them for "considering url_env"
            // would be a false alarm.
            match source {
                UrlSource::InlineYaml => {
                    static WARNED: std::sync::Once = std::sync::Once::new();
                    WARNED.call_once(|| {
                        log::warn!(
                            "source: inline `url:` in YAML contains a plaintext password — \
                             move it to `url_env: DATABASE_URL` (or `url_file:`) to keep \
                             credentials out of committed configs"
                        );
                    });
                }
                UrlSource::EnvVar(_) | UrlSource::File(_) => {
                    // The recommended forms — no warning. Operator hygiene
                    // for shell history / file permissions is out of scope.
                }
            }
        }

        Ok(resolved)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Postgres,
    Mysql,
    Mssql,
}

/// Locate `user[:password]@` userinfo inside a standard URL.
///
/// Returns `(userinfo_end_index, scheme_end_index)` where:
/// - `scheme_end_index` points right after `"://"` (start of userinfo)
/// - `userinfo_end_index` points at the `@` separator (exclusive of `@`)
///
/// Returns `None` if the URL has no userinfo segment.
fn find_userinfo(raw: &str) -> Option<(usize, usize)> {
    let scheme = raw.find("://")? + 3;
    let rest = &raw[scheme..];
    // The authority ends at the first path/query/fragment delimiter; an `@`
    // after that belongs to the path or query (`?foo=a@b`), not the userinfo.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    // Terminate userinfo at the LAST `@` within the authority: a password may
    // itself contain `@` (`user:p@ssw0rd@host`), and splitting at the FIRST
    // `@` would leak the tail after it into the persisted plan artifact.
    // `rfind` mirrors `redact_pg_url` in state/mod.rs, which strips passwords
    // the same way for the same reason.
    let at = rest[..authority_end].rfind('@')?;
    Some((scheme + at, scheme))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── TlsMode::is_enforced ────────────────────────────────────────────────

    #[test]
    fn tls_mode_disable_not_enforced() {
        assert!(!TlsMode::Disable.is_enforced());
    }

    #[test]
    fn tls_mode_require_is_enforced() {
        assert!(TlsMode::Require.is_enforced());
        assert!(TlsMode::VerifyCa.is_enforced());
        assert!(TlsMode::VerifyFull.is_enforced());
    }

    // ── SourceConfig::redact_for_artifact ───────────────────────────────────

    fn make_source(source_type: SourceType) -> SourceConfig {
        SourceConfig {
            source_type,
            url: None,
            url_env: None,
            url_file: None,
            host: None,
            port: None,
            user: None,
            password: None,
            password_env: None,
            database: None,
            environment: None,
            tuning: None,
            tls: None,
        }
    }

    #[test]
    fn redact_plaintext_password() {
        let mut src = make_source(SourceType::Postgres);
        src.password = Some("s3cr3t".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag, "redaction should be flagged");
        assert!(
            redacted.password.is_none(),
            "plaintext password must be stripped"
        );
    }

    #[test]
    fn redact_url_with_password() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://user:hunter2@db.example.com:5432/app".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag, "URL redaction flagged");
        let url = redacted.url.unwrap();
        assert!(!url.contains("hunter2"), "password must not appear: {url}");
        assert!(url.contains("REDACTED"), "placeholder must appear: {url}");
        assert!(url.contains("@db.example.com"), "host retained: {url}");
    }

    #[test]
    fn redact_url_without_at_sign_not_flagged() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://db.example.com:5432/app".into());
        let (_, flag) = src.redact_for_artifact();
        assert!(!flag, "URL with no userinfo must not be flagged");
    }

    #[test]
    fn redact_url_with_user_but_no_password_is_flagged() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://user@db.example.com:5432/app".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag, "bare user@ is still userinfo and gets redacted");
        let url = redacted.url.unwrap();
        assert!(url.contains("REDACTED"), "userinfo replaced: {url}");
        assert!(!url.contains("user@"), "bare username removed: {url}");
    }

    #[test]
    fn redact_env_var_reference_kept_intact() {
        let mut src = make_source(SourceType::Mysql);
        src.url_env = Some("DB_URL".into());
        src.password_env = Some("DB_PASS".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(!flag, "env var references are not secrets");
        assert_eq!(redacted.url_env.as_deref(), Some("DB_URL"));
        assert_eq!(redacted.password_env.as_deref(), Some("DB_PASS"));
    }

    #[test]
    fn redact_mysql_url_with_password() {
        let mut src = make_source(SourceType::Mysql);
        src.url = Some("mysql://root:pass@127.0.0.1:3306/mydb".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag);
        let url = redacted.url.unwrap();
        assert!(url.contains("REDACTED"), "{url}");
        assert!(!url.contains("pass"), "{url}");
    }

    // ── SourceConfig::resolve_url (structured fields) ───────────────────────

    #[test]
    fn resolve_url_from_structured_fields_postgres() {
        let mut src = make_source(SourceType::Postgres);
        src.host = Some("pg.internal".into());
        src.user = Some("alice".into());
        src.database = Some("warehouse".into());
        src.port = Some(5433);
        let url = src.resolve_url().unwrap();
        assert_eq!(url, "postgresql://alice@pg.internal:5433/warehouse");
    }

    #[test]
    fn resolve_url_from_structured_fields_defaults_port() {
        let mut src = make_source(SourceType::Mysql);
        src.host = Some("my.internal".into());
        src.user = Some("bob".into());
        src.database = Some("orders".into());
        let url = src.resolve_url().unwrap();
        assert_eq!(url, "mysql://bob@my.internal:3306/orders");
    }

    #[test]
    fn resolve_url_direct_url_passthrough() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://carol@pg.example.com:5432/db".into());
        let url = src.resolve_url().unwrap();
        assert_eq!(url, "postgresql://carol@pg.example.com:5432/db");
    }

    #[test]
    fn resolve_url_rejects_mixed_url_and_structured() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://carol@pg.example.com/db".into());
        src.host = Some("other".into());
        let err = src.resolve_url().unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("URL-based") || msg.contains("structured"),
            "{msg}"
        );
    }

    #[test]
    fn resolve_url_rejects_missing_host() {
        let mut src = make_source(SourceType::Postgres);
        src.user = Some("alice".into());
        src.database = Some("warehouse".into());
        let err = src.resolve_url().unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("host"), "{msg}");
    }

    // ── find_userinfo ────────────────────────────────────────────────────────

    #[test]
    fn find_userinfo_detects_password_in_url() {
        let url = "postgresql://user:pass@host/db";
        let result = find_userinfo(url);
        assert!(result.is_some(), "should detect user:pass@");
    }

    #[test]
    fn find_userinfo_no_password_no_at_returns_none() {
        assert!(find_userinfo("postgresql://host/db").is_none());
    }

    #[test]
    fn find_userinfo_user_only_at_sign_matches() {
        let url = "postgresql://user@host/db";
        assert!(find_userinfo(url).is_some(), "bare user@ should match");
    }

    #[test]
    fn find_userinfo_no_at_sign_returns_none() {
        assert!(find_userinfo("postgresql://db.example.com:5432/app").is_none());
    }

    // ── SEC-RED: embedded `@` in password must not leak to plan artifact ──────

    #[test]
    fn sec_artifact_redaction_password_with_at() {
        // SEC-RED V7: find_userinfo (used by redact_for_artifact when building
        // the persisted plan JSON) splits userinfo at the FIRST `@` via
        // `rest.find('@')`, leaking the password tail after an embedded `@`.
        // For `postgresql://rivet:p@ssw0rd@host/db` the first `@` sits right
        // after `p`, so `userinfo_end` lands before `ssw0rd@host/db` and the
        // rewrite emits `postgresql://REDACTED@ssw0rd@host/db` — the password
        // tail `ssw0rd` round-trips into the artifact. The terminator must be
        // the LAST `@` before the path (rfind semantics, as already used by
        // redact_pg_url in state/mod.rs:564).
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://rivet:p@ssw0rd@db.example.com:5432/orders".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag, "URL with userinfo must be flagged as redacted");
        let url = redacted.url.expect("url retained after redaction");
        assert!(
            !url.contains("ssw0rd"),
            "password tail after embedded @ must not leak into artifact: {url}"
        );
        assert!(
            !url.contains("p@ssw0rd"),
            "full password must not leak into artifact: {url}"
        );
        assert!(url.contains("REDACTED"), "placeholder must appear: {url}");
        assert!(
            url.contains("@db.example.com:5432/orders"),
            "host and path must be retained: {url}"
        );
    }
}
