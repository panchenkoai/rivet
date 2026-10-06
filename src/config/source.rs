//! Source-database connection config: URL/structured fields, TLS, environment hints.

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::resolve::resolve_env_vars;
use crate::tuning::{TuningConfig, TuningProfile};

/// Chars to percent-encode in a URL userinfo (user / password) component: encode
/// everything except the RFC 3986 unreserved set (ALPHA / DIGIT / `- . _ ~`), so
/// no credential byte can be mistaken for a URL delimiter. Over-encoding is
/// harmless — the driver percent-decodes it back.
const USERINFO: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

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

    /// MongoDB-specific read options (`source.mongo:`). Honoured only when
    /// `type: mongo`; ignored by the SQL engines. See [`MongoConfig`].
    #[serde(default)]
    pub mongo: Option<MongoConfig>,
}

/// MongoDB read-path knobs. All three address completeness/fidelity of a `full`
/// collection export that the SQL engines get for free from SQL semantics.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct MongoConfig {
    /// JSON rendering of the `document` column. `relaxed` (default) keeps common
    /// scalars native (`42`, `"x"`); `canonical` wraps every number
    /// (`{"$numberLong":"…"}`) so Int64/Double round-trip losslessly through a
    /// JSON-number parser that would otherwise clamp values beyond 2^53.
    #[serde(default)]
    pub json: MongoJsonMode,

    /// Read concern for the collection scan. `snapshot` gives a **point-in-time
    /// consistent** full export (no doc missed/double-read under concurrent
    /// writes) — requires MongoDB 5.0+ on a replica set; a standalone rejects it.
    /// Default (`server`) uses the server's default read concern.
    #[serde(default)]
    pub read_concern: MongoReadConcern,

    /// Keep the scan cursor alive past the server's idle timeout (default 10 min)
    /// so a slow destination cannot let the server reap the cursor mid-scan and
    /// silently drop the tail of a large collection. Default: `true`.
    #[serde(default = "default_true")]
    pub no_cursor_timeout: bool,

    /// When set, read the collection with **keyset (seek) pagination** on `_id`
    /// instead of one long-held cursor: each page is a bounded
    /// `find({_id: {$gt: last}}).sort({_id: 1}).limit(page_size)` — an indexed
    /// range scan that becomes one output part file. Bounds longest-query time
    /// (no 35-minute cursor to hit a timeout / snapshot window) and is the base
    /// for parallel `_id`-range reads. Works with any **uniform** `_id` type
    /// (ObjectId — the default — integer, string, date, …); a collection mixing
    /// `_id` type brackets errors with a clear message pointing at the full
    /// ordered scan (Mongo's `$gt` compares only within a type bracket, so a
    /// mixed key would silently drop every bracket but one). Unset ⇒ the
    /// single-cursor full scan.
    #[serde(default)]
    pub page_size: Option<usize>,

    /// With keyset paging (`page_size`), persist the last committed `_id` and
    /// **resume** from it next run — a crashed export continues where it left
    /// off, and a re-run captures only documents inserted since (ObjectId `_id`
    /// is time-ordered), which `rivet load` appends rather than overwrites. For
    /// append-only collections: an update to a document already read is never
    /// re-read, and after a whole re-read (`rivet state reset`) the warehouse view
    /// may serve either copy of a document updated in between. Default `false`
    /// re-reads the whole collection each run (plain `mode: full` semantics). No
    /// effect without `page_size`.
    #[serde(default)]
    pub resume: bool,
}

impl Default for MongoConfig {
    fn default() -> Self {
        Self {
            json: MongoJsonMode::default(),
            read_concern: MongoReadConcern::default(),
            no_cursor_timeout: true,
            page_size: None,
            resume: false,
        }
    }
}

fn default_true() -> bool {
    true
}

/// Extended-JSON rendering mode for the `document` column.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MongoJsonMode {
    /// Native scalars where possible; `{"$oid":…}`/`{"$date":…}` only for exotic
    /// BSON. Friendliest for `PARSE_JSON`, but Int64 renders as a bare number.
    #[default]
    Relaxed,
    /// Every value type-tagged (`{"$numberLong":…}`, `{"$numberInt":…}`) — a
    /// lossless, bulkier form that survives a double-based JSON-number parser.
    Canonical,
}

/// Read concern applied to the collection scan.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MongoReadConcern {
    /// The server's default read concern — no point-in-time guarantee across the
    /// scan (a plain `find`, like today).
    #[default]
    Server,
    /// `snapshot` — a consistent point-in-time read for the whole cursor
    /// (MongoDB 5.0+ replica set only).
    Snapshot,
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
// `clap::ValueEnum` so `rivet init --tls <mode>` accepts exactly this enum — a
// parallel CLI-only enum would be a second definition of the same four words,
// and it would drift on the first variant change (#146). A regular comment,
// not a doc comment: the doc text IS the schema description users see.
#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum,
)]
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

/// The kebab-case name serde/clap already own — ONE rendering for every
/// user-facing surface (#162: three hand matches drifted-in-waiting).
impl std::fmt::Display for TlsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TlsMode::Disable => "disable",
            TlsMode::Require => "require",
            TlsMode::VerifyCa => "verify-ca",
            TlsMode::VerifyFull => "verify-full",
        })
    }
}

impl TlsMode {
    pub fn is_enforced(self) -> bool {
        !matches!(self, TlsMode::Disable)
    }
}

/// An IPv6 literal host in URL form: bracketed, so its colons are not read as a port.
fn bracket_ipv6(host: &str) -> std::borrow::Cow<'_, str> {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]").into()
    } else {
        host.into()
    }
}

impl SourceConfig {
    /// Return a copy of this config with **all plaintext credential material stripped**,
    /// safe to embed in a persisted [`crate::plan::PlanArtifact`] (ADR-0005 PA9).
    ///
    /// Redaction rules:
    /// - `password` → always `None` (plaintext password never leaves the process).
    /// - `url` containing `user[:password]@` → userinfo segment replaced with `"REDACTED"`.
    /// - `url` query secrets (`password=`, `*_token=`, …) → value replaced with `***`.
    /// - `url` in keyword/value form (`host=… password=…`, `…;PWD=…`) → password value replaced with `***`.
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

        if let Some(ref raw) = out.url {
            let masked = crate::redact::redact_keyword_passwords(raw);
            if masked != *raw {
                out.url = Some(masked);
                redacted = true;
            }
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
        if let Some(ref raw) = out.url {
            let scrubbed = crate::redact::redact_query_secrets(raw);
            if scrubbed != *raw {
                out.url = Some(scrubbed);
                redacted = true;
            }
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
            SourceType::Oracle => 1521,
            SourceType::Mongo => 27017,
        };
        let port = self.port.unwrap_or(default_port);

        let scheme = match self.source_type {
            SourceType::Postgres => "postgresql",
            SourceType::Mysql => "mysql",
            SourceType::Mssql => "sqlserver",
            SourceType::Oracle => "oracle",
            SourceType::Mongo => "mongodb",
        };

        // Percent-encode the userinfo so a credential containing `/ @ : ? #` can't
        // make the URL ambiguous (breaking the driver) or defeat redaction.
        let user_enc = utf8_percent_encode(user, USERINFO);
        let host = bracket_ipv6(host);
        if password.is_empty() {
            Ok(format!(
                "{}://{}@{}:{}/{}",
                scheme, user_enc, host, port, database
            ))
        } else {
            let pw_enc = utf8_percent_encode(password.as_str(), USERINFO);
            Ok(format!(
                "{}://{}:{}@{}:{}/{}",
                scheme, user_enc, pw_enc, host, port, database
            ))
        }
    }

    /// The state key of what reads from here (cursor, crash anchor): engine, host, port and
    /// database — no credentials or parameters — so a changed destination keeps its cursor.
    pub fn state_key(&self) -> String {
        self.resolve_url()
            .map(|u| source_state_key(self.source_type, &u))
            .unwrap_or_default()
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

        if let Some(named) = SourceType::from_url_scheme(&resolved)
            && named != self.source_type
        {
            crate::config_bail!(
                crate::error::codes::CONFIG_SOURCE_URL_SCHEME_MISMATCH,
                "source.type: {} but the url scheme names {} — the two must agree, or the run \
                 would anchor and read one engine while typing the stream as another",
                self.source_type.label(),
                named.label()
            );
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
    /// Oracle Database 19c+, read through Oracle's pure-Rust thin driver.
    Oracle,
    /// Document store. Unlike the three SQL engines, MongoDB has no SQL, no
    /// fixed per-collection schema, and no `information_schema` — so the
    /// SQL-shaped read seam (chunked/keyset planning, incremental predicate
    /// building, catalog introspection) does not apply. The OSS source is the
    /// JSON-blob model: each document exports as `_id` + a `document` JSON
    /// column, typing punted downstream. Change streams (CDC) map onto the
    /// canonical `ChangeStream` seam separately.
    Mongo,
}

impl SourceType {
    /// Whether this engine speaks SQL (the relational read seam: `SELECT`
    /// queries, `information_schema` introspection, chunked/keyset/incremental
    /// SQL builders). `false` for document stores like MongoDB, whose adapter
    /// reads via the driver's native query API instead. Used at the match sites
    /// that would otherwise have to special-case every non-SQL engine.
    pub fn is_sql(self) -> bool {
        !matches!(self, SourceType::Mongo)
    }

    /// The engine as `export_metrics.source_type` records it (`postgres`, `mysql`, `mssql`, `mongo`).
    pub fn ledger_label(self) -> String {
        self.label().to_string()
    }

    /// Stable lowercase engine label for metrics, run records and hints.
    pub fn label(self) -> &'static str {
        match self {
            SourceType::Postgres => "postgres",
            SourceType::Mysql => "mysql",
            SourceType::Mssql => "mssql",
            SourceType::Oracle => "oracle",
            SourceType::Mongo => "mongo",
        }
    }

    /// The engine a URL's scheme names (case-insensitive), or `None` for a scheme rivet does not read.
    pub fn from_url_scheme(url: &str) -> Option<SourceType> {
        let (scheme, _) = url.split_once("://")?;
        match scheme.to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" => Some(SourceType::Postgres),
            "mysql" => Some(SourceType::Mysql),
            "sqlserver" | "mssql" => Some(SourceType::Mssql),
            "mongodb" | "mongodb+srv" => Some(SourceType::Mongo),
            "oracle" => Some(SourceType::Oracle),
            _ => None,
        }
    }
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
    // The LAST `@` ends the userinfo when it sits in the authority or a `:` precedes
    // it: a raw `${VAR}` password may hold `/ ? # @`. Only a `:`-free `@` past the
    // authority (`host/db?filter=a@b`) is left in the path/query.
    let at = rest.rfind('@')?;
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    (at < authority_end || rest[..at].contains(':')).then_some((scheme + at, scheme))
}

/// `engine://host:port/database` of `url`, credentials, query and fragment dropped; a keyword/value string keeps its pairs with the password masked.
pub(crate) fn source_state_key(source_type: SourceType, url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let rest = find_userinfo(url).map_or(rest, |(at, _)| &url[at + 1..]);
    let at_host = rest.split(['?', '#']).next().unwrap_or(rest);
    let key = format!("{source_type:?}://{}", at_host.trim_end_matches('/')).to_lowercase();
    crate::redact::redact_keyword_passwords(&key)
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_source_state_key_names_the_server_and_database_without_credentials() {
        use super::{SourceType, source_state_key};
        let k = source_state_key(
            SourceType::Postgres,
            "postgresql://u:secret@Db.host:5432/app?sslmode=require",
        );
        assert_eq!(k, "postgres://db.host:5432/app");
        assert_eq!(
            k,
            source_state_key(
                SourceType::Postgres,
                "postgres://other:pw@db.host:5432/app/"
            )
        );
        assert_ne!(
            k,
            source_state_key(SourceType::Mysql, "mysql://u:secret@db.host:5432/app")
        );
        assert_ne!(
            k,
            source_state_key(SourceType::Postgres, "postgresql://u@db.host:5432/other")
        );
    }

    #[test]
    fn a_raw_query_delimiter_in_the_password_does_not_merge_two_servers_into_one_scope() {
        use super::{SourceType, source_state_key};
        let a = source_state_key(SourceType::Postgres, "postgresql://app:k?9@h1:5432/a");
        let b = source_state_key(SourceType::Postgres, "postgresql://app:k#9@h2:5432/b");
        assert_eq!(a, "postgres://h1:5432/a");
        assert_eq!(b, "postgres://h2:5432/b");
    }

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
            mongo: None,
        }
    }

    /// Whatever credentials and query options a URL carries, the state key never holds them
    /// and does not change with them.
    #[test]
    fn a_source_state_key_never_carries_credentials_or_options() {
        use proptest::prelude::*;
        proptest!(|(user in "[a-z][a-z0-9]{0,8}", pw in "[A-Za-z0-9!%]{1,12}",
                    opt in "[a-z]{1,6}=[a-z0-9]{1,6}")| {
            let bare = source_state_key(SourceType::Postgres, "postgresql://db.h:5432/app");
            let full = source_state_key(
                SourceType::Postgres,
                &format!("postgresql://{user}:{pw}@db.h:5432/app?{opt}"),
            );
            prop_assert_eq!(&full, &bare);
            prop_assert!(!full.contains(&pw) || bare.contains(&pw));
        });
    }

    #[test]
    fn a_source_state_key_is_derived_from_its_resolved_url() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://u:pw@db.host:5432/app".into());
        assert_eq!(src.state_key(), "postgres://db.host:5432/app");
    }

    /// A keyword/value string keeps its pairs in the key, the password masked; a rotated password keeps the key.
    #[test]
    fn a_source_state_key_masks_a_keyword_value_password() {
        let dsn = |pw: &str| format!("host=Db.Host port=5432 user=App password={pw} dbname=app");
        let key = source_state_key(SourceType::Postgres, &dsn("S3cr3tPw"));
        assert_eq!(
            key,
            "postgres://host=db.host port=5432 user=app password=*** dbname=app"
        );
        assert_eq!(key, source_state_key(SourceType::Postgres, &dsn("rotated")));
        let before_the_mask =
            "postgres://host=db.host port=5432 user=app password=s3cr3tpw dbname=app";
        assert_eq!(
            crate::redact::redact_keyword_passwords(before_the_mask),
            key,
            "a key stored before the mask must mask to today's key (the state store adopts on that)"
        );
        assert_eq!(
            source_state_key(
                SourceType::Postgres,
                "host=Db.Host port=5432 user=App dbname=app"
            ),
            "postgres://host=db.host port=5432 user=app dbname=app",
            "a string with no password is keyed as before"
        );
    }

    #[test]
    fn redact_keyword_value_url_masks_the_password_and_keeps_the_rest() {
        for (raw, want) in [
            (
                "host=h port=5432 user=u password=S3cr3tPw dbname=d",
                "host=h port=5432 user=u password=*** dbname=d",
            ),
            (
                "Server=h,1433;Database=d;User Id=u;PWD=S3cr3tPw;",
                "Server=h,1433;Database=d;User Id=u;PWD=***;",
            ),
            (
                "sqlserver://h:1433;databaseName=app;user=sa;password=p@S3cr3tPw",
                "sqlserver://h:1433;databaseName=app;user=sa;password=***",
            ),
        ] {
            let mut src = make_source(SourceType::Postgres);
            src.url = Some(raw.into());
            let (redacted, flag) = src.redact_for_artifact();
            assert!(flag, "a masked password is flagged: {raw}");
            assert_eq!(redacted.url.as_deref(), Some(want), "{raw}");
        }
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("host=/var/run/postgresql user=u dbname=d".into());
        let (kept, flag) = src.redact_for_artifact();
        assert!(!flag, "nothing to mask, nothing flagged");
        assert_eq!(
            kept.url.as_deref(),
            Some("host=/var/run/postgresql user=u dbname=d")
        );
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
    fn from_url_scheme_is_the_one_scheme_parser() {
        for (url, want) in [
            ("postgresql://h/db", Some(SourceType::Postgres)),
            ("postgres://h/db", Some(SourceType::Postgres)),
            ("POSTGRESQL://h/db", Some(SourceType::Postgres)),
            ("mysql://h/db", Some(SourceType::Mysql)),
            ("sqlserver://h/db", Some(SourceType::Mssql)),
            ("mssql://h/db", Some(SourceType::Mssql)),
            ("mongodb://h/db", Some(SourceType::Mongo)),
            ("mongodb+srv://h/db", Some(SourceType::Mongo)),
            ("oracle://h/db", Some(SourceType::Oracle)),
            ("ORACLE://h/db", Some(SourceType::Oracle)),
            ("postgresXYZ://h/db", None),
            ("postgres:h/db", None),
            ("redis://h", None),
        ] {
            assert_eq!(SourceType::from_url_scheme(url), want, "{url}");
        }
    }

    #[test]
    fn label_is_the_ledger_spelling_of_every_engine() {
        for (t, want) in [
            (SourceType::Postgres, "postgres"),
            (SourceType::Mysql, "mysql"),
            (SourceType::Mssql, "mssql"),
            (SourceType::Oracle, "oracle"),
            (SourceType::Mongo, "mongo"),
        ] {
            assert_eq!(t.label(), want);
            assert_eq!(t.ledger_label(), want);
        }
    }

    #[test]
    fn resolve_url_refuses_a_scheme_that_names_another_engine() {
        let mut src = make_source(SourceType::Mysql);
        src.url = Some("postgresql://carol@pg.example.com:5432/db".into());
        let err = src
            .resolve_url()
            .expect_err("type mysql, scheme postgresql");
        assert_eq!(
            err.to_string(),
            "source.type: mysql but the url scheme names postgres — the two must agree, or the \
             run would anchor and read one engine while typing the stream as another"
        );
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH")
        );
    }

    #[test]
    fn resolve_url_admits_a_matching_or_unknown_scheme() {
        let mut src = make_source(SourceType::Mssql);
        src.url = Some("mssql://sa@db:1433/d".into());
        assert!(src.resolve_url().is_ok());
        src.url = Some("jdbc:sqlserver://db:1433".into());
        assert!(
            src.resolve_url().is_ok(),
            "an unknown scheme is the driver's to judge"
        );
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
    fn a_raw_delimiter_or_query_password_never_reaches_the_plan_artifact() {
        for (url, secret) in [
            ("postgresql://app:a/b@db.example.com:5432/prod", "a/b"),
            ("postgresql://app:a?b@db.example.com:5432/prod", "a?b"),
            ("postgresql://app:a#b@db.example.com:5432/prod", "a#b"),
            (
                "postgresql://db.example.com/prod?user=app&password=s3cret",
                "s3cret",
            ),
        ] {
            let mut src = make_source(SourceType::Postgres);
            src.url = Some(url.into());
            let (redacted, flag) = src.redact_for_artifact();
            let out = redacted.url.unwrap();
            assert!(flag, "{url} must be flagged");
            assert!(!out.contains(secret), "{secret} leaked: {out}");
            assert!(out.contains("db.example.com"), "host kept: {out}");
        }
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

    #[test]
    fn a_structured_ipv6_host_is_bracketed() {
        assert_eq!(bracket_ipv6("::1"), "[::1]");
        assert_eq!(bracket_ipv6("[::1]"), "[::1]");
        assert_eq!(bracket_ipv6("db.example"), "db.example");
    }

    #[test]
    fn build_url_percent_encodes_userinfo_so_delimiters_cant_leak() {
        // A2 (audit root fix): a password with URL delimiters (`/ @ : ? #` — `/` is
        // a common base64 char) is percent-encoded when Rivet builds the connection
        // URL from structured fields. Un-encoded, the raw `/`/`@` made the URL
        // ambiguous: it broke the driver AND stopped the password-redaction scan
        // early, leaking the tail. Encoding is the unambiguous root fix (redaction
        // of a well-formed URL is then correct). RED before the encoding.
        let mut src = make_source(SourceType::Postgres);
        src.host = Some("db.example.com".into());
        src.user = Some("rivet".into());
        src.password = Some("pa/s:s@w?rd#x".into());
        src.database = Some("orders".into());
        let url = src.resolve_url().expect("url built from fields");
        assert!(
            !url.contains("pa/s:s@w?rd#x"),
            "raw password with delimiters must not appear un-encoded: {url}"
        );
        assert!(
            url.contains("pa%2Fs%3As%40w%3Frd%23x"),
            "userinfo must be percent-encoded: {url}"
        );
    }

    #[test]
    fn mssql_url_from_fields_roundtrips_through_parse_mssql_url() {
        // Round-2 audit #1: the encoding above is only safe if the consumer
        // decodes it. PG/MySQL/Mongo delegate to a driver URL parser that
        // percent-decodes; MSSQL's hand-rolled `parse_mssql_url` is the outlier
        // that must decode itself. Prove the two halves agree: a password with
        // every URL delimiter survives encode→parse losslessly. RED before the
        // decode landed in `parse_mssql_url` (it echoed the literal %-bytes).
        let mut src = make_source(SourceType::Mssql);
        src.host = Some("db.internal".into());
        src.user = Some(r"dom\svc".into());
        src.password = Some("p:a/s@s?w#d!%x".into());
        src.database = Some("rivet".into());
        let url = src.resolve_url().expect("mssql url built from fields");
        let parsed =
            crate::source::mssql::parse_mssql_url(&url).expect("built mssql url must parse back");
        assert_eq!(parsed.user, r"dom\svc", "user round-trips");
        assert_eq!(
            parsed.password, "p:a/s@s?w#d!%x",
            "every URL-delimiter char must survive the encode→decode round-trip"
        );
        assert_eq!(parsed.database, "rivet");
    }
}
