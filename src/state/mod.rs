use rusqlite::Connection;

use crate::error::Result;

mod cdc_snapshot_store;
mod checkpoint;
mod cursor;
mod file_log;
mod journal_store;
mod keyset_range;
mod load_journal_store;
mod load_lease;
// Named so a caller can HOLD a lease across a scope it owns — the cleanup delete
// needs one that outlives the call that took it. Every other holder infers the type.
pub use load_lease::LoadLease;
mod load_spec_store;
mod metrics;
mod migrations;
use migrations::{migrate, migrate_pg};
mod progression;
mod row;
mod run_aggregate;
mod run_status_store;
mod schema;
mod shape;

// Re-export domain types so callers use `rivet::state::*` unchanged.
// Items below may not be explicitly named by all internal callers (often used
// as inferred return types), but are part of the public integration-test API.
#[allow(unused_imports)]
pub use checkpoint::{ChunkTaskInfo, StrategySnapshot};
#[allow(unused_imports)]
pub use file_log::{DurablePart, FilePart, FileRecord};
#[allow(unused_imports)]
pub use keyset_range::{KeysetRangePart, KeysetRangeRow};
pub use load_journal_store::LoadRecord;
#[allow(unused_imports)]
pub use load_spec_store::{LoadSpec, LoadSpecColumn};
#[allow(unused_imports)]
pub use metrics::ExportMetric;
pub use metrics::MetricRow;
#[allow(unused_imports)]
pub use progression::{Boundary, ExportProgression};
#[allow(unused_imports)]
pub use run_aggregate::{RunAggregate, RunAggregateEntry};
pub use run_status_store::FinishOutcome;
#[allow(unused_imports)]
pub use schema::{SchemaChange, SchemaColumn, arrow_schema_to_columns, schema_fingerprint};
#[allow(unused_imports)]
pub use shape::ShapeWarning;

const STATE_DB_NAME: &str = ".rivet_state.db";

// ─── SQL helpers ──────────────────────────────────────────────────────────────

/// Convert SQLite `?N` placeholders to PostgreSQL `$N` style.
/// `"WHERE x = ?1 AND y = ?2"` → `"WHERE x = $1 AND y = $2"`.
pub(super) fn pg_sql(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'?' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            out.push('$');
        } else {
            out.push(bytes[i] as char);
        }
        i += 1;
    }
    out
}

/// Open a Postgres client for the state backend, honoring the URL's `sslmode`.
///
/// The state backend connects to its store using only a URL (`RIVET_STATE_URL`)
/// — there is no YAML `tls:` block — so the transport-security policy is derived
/// from the URL's `sslmode` query parameter, exactly as `rivet init` does for
/// source connections. The connection itself goes through the shared
/// [`crate::source::postgres::connect_client`] path so the state backend and
/// source connections apply identical TLS rules.
///
/// - missing / `disable` / `prefer` / `allow` / unrecognized → `NoTls`
///   (plaintext), keeping local and dev setups working unchanged.
/// - `require` / `verify-ca` / `verify-full` → negotiate TLS.
///
/// Used by both [`StateStore::open_postgres`] and the parallel chunk-worker
/// reconnection paths in `checkpoint.rs`, so every PG state connection is
/// TLS-aware.
pub(super) fn connect_pg(url: &str) -> Result<postgres::Client> {
    let tls = state_tls_mode_from_url(url).map(|mode| crate::config::TlsConfig {
        mode,
        ..crate::config::TlsConfig::default()
    });
    crate::source::postgres::connect_client(url, tls.as_ref())
        .map_err(|e| anyhow::anyhow!("state(pg): connect to '{}': {:#}", redact_pg_url(url), e))
}

/// Map the state URL's `sslmode` query parameter to a [`crate::config::TlsMode`].
///
/// Mirrors the source-side mapping in `crate::init::postgres`: `require` /
/// `verify-ca` / `verify-full` enforce TLS; everything else — parameter missing,
/// `disable`, `prefer`, `allow`, or an unrecognized value — returns `None`
/// (plaintext `NoTls`). [`crate::config::TlsMode`] has no `prefer` variant, so no
/// try-TLS-then-fallback is attempted. Last occurrence wins, matching libpq.
fn state_tls_mode_from_url(url: &str) -> Option<crate::config::TlsMode> {
    use crate::config::TlsMode;
    let (_, query) = url.split_once('?')?;
    let mut mode = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key != "sslmode" {
            continue;
        }
        mode = match value {
            "require" => Some(TlsMode::Require),
            "verify-ca" => Some(TlsMode::VerifyCa),
            "verify-full" => Some(TlsMode::VerifyFull),
            _ => None,
        };
    }
    mode
}

// ─── Backend connection ────────────────────────────────────────────────────────

/// Internal storage for the active database connection.
pub(super) enum StateConn {
    Sqlite(rusqlite::Connection),
    /// postgres::Client requires `&mut self` for queries; RefCell provides
    /// interior mutability so `StateStore` methods can keep `&self` signatures.
    /// StateStore is not Sync (neither backend is), so RefCell is safe here.
    /// Boxed to keep the enum variant sizes balanced (postgres::Client is ~320 B).
    Postgres(Box<std::cell::RefCell<postgres::Client>>),
}

/// Serialisable reference that identifies a state database without holding a
/// live connection.  Passed to parallel chunk workers so they can open their
/// own connection for atomic `claim_next_chunk_task` operations.
#[derive(Clone)]
pub enum StateRef {
    Sqlite(std::path::PathBuf),
    Postgres(String),
}

/// Redact the password from a PostgreSQL URL for safe use in log/error messages.
/// `postgresql://user:SECRET@host/db` → `postgresql://user:***@host/db`
/// Uses `rfind('@')` so passwords containing `@` are handled correctly.
fn redact_pg_url(url: &str) -> String {
    // Mask the password in `scheme://user:password@host/...`. RIVET_STATE_URL is
    // operator-supplied and may be NON-conforming — a raw password can contain any
    // of `/ ? # @ :` that a well-formed URL would percent-encode. There is no
    // unambiguous parse of such a URL, so a redactor MUST default-deny: never leak,
    // even at the cost of over-redacting a pathological host.
    //
    // Rule (rounds 2/3/4 converged here after the bounded/two-pass forms each leaked
    // a different shape): the userinfo ends at the LAST '@' before whitespace (the
    // URL / log-line terminator), and the user is everything up to the FIRST ':'
    // (the password separator; a ':' inside the password is masked with the rest).
    //   * one '@' (the normal case): the real terminator → host preserved.
    //   * a password with a raw '/','?','#','@' (round-3 `pa/ss`, round-4 `Kp@9x/..`):
    //     the last '@' is still the true terminator → tail masked, no leak.
    //   * a ':'-bearing password (`a:b:c:secret`): FIRST ':' splits → prefix masked.
    //   * a stray '@' in a query (`?opt=a@b`) — vanishingly rare for a connection
    //     URL — over-redacts the host but never leaks (default-deny).
    // Residual limitation (round-4 #4/#5, documented): a raw WHITESPACE in the
    // password terminates the URL scan (whitespace ends the token in a log line), so
    // a password containing a literal space/tab may not be fully masked. This is
    // out of reliable scope — a space in a URL is itself non-conforming (must be
    // %20-encoded), and treating a whitespace-bounded `:`-bearing span as userinfo
    // would mangle every common credential-free `scheme://host:port/db ...` log line.
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after_scheme = &url[scheme_end + 3..];
    let span_end = after_scheme
        .find(char::is_whitespace)
        .unwrap_or(after_scheme.len());
    let span = &after_scheme[..span_end];
    // No '@' → no userinfo to redact.
    let Some(at_rel) = span.rfind('@') else {
        return url.to_string();
    };
    let userinfo = &span[..at_rel];
    // No ':' before the '@' → user-only, no password to mask.
    let Some(colon) = userinfo.find(':') else {
        return url.to_string();
    };
    let user = &userinfo[..colon];
    let at_pos = scheme_end + 3 + at_rel;
    format!(
        "{}://{}:***@{}",
        &url[..scheme_end],
        user,
        &url[at_pos + 1..]
    )
}

// ─── SQLite connection helper ─────────────────────────────────────────────────

pub(crate) const SQLITE_BUSY_TIMEOUT_MS: i64 = 10_000;

pub(crate) fn open_connection(db_path: &std::path::Path) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    if let Err(e) = conn.execute_batch("PRAGMA journal_mode=WAL;") {
        log::warn!(
            "state: WAL journal mode unavailable ({}); \
             running in default mode — concurrent writes may be slower",
            e
        );
    }
    if let Err(e) = conn.execute_batch(&format!(
        "PRAGMA busy_timeout = {};",
        SQLITE_BUSY_TIMEOUT_MS
    )) {
        log::warn!(
            "state: failed to set busy_timeout ({}); \
             concurrent writers may surface SQLITE_BUSY immediately",
            e
        );
    }
    Ok(conn)
}

// ─── StateStore ───────────────────────────────────────────────────────────────

/// Entry point for all persistent state.  Supports two backends:
///
/// - **SQLite** (default) — a single `.rivet_state.db` file next to the
///   config.  Good for local / single-node / dev deployments.
/// - **PostgreSQL** — a shared database addressed by `RIVET_STATE_URL`.
///   Required for stateless container / Kubernetes deployments where the
///   rivet pod is ephemeral or replicated.
///
/// Set the `RIVET_STATE_URL` environment variable to a PostgreSQL URL to
/// activate the Postgres backend:
///
/// ```text
/// RIVET_STATE_URL=postgresql://user:pass@host:5432/rivet_state
/// ```
///
/// When the variable is absent or does not start with `postgres`, SQLite is
/// used and the variable is ignored.
pub struct StateStore {
    pub(super) conn: StateConn,
    /// Serialisable reference for reconnection (parallel chunk workers).
    pub(super) state_ref: StateRef,
}

impl StateStore {
    /// Open the appropriate backend.
    ///
    /// Checks `RIVET_STATE_URL`; falls back to SQLite next to `config_path`.
    pub fn open(config_path: &str) -> Result<Self> {
        if let Ok(url) = std::env::var("RIVET_STATE_URL")
            && url.starts_with("postgres")
        {
            return Self::open_postgres(&url);
        }
        Self::open_sqlite(config_path)
    }

    /// Reopen the store a [`StateRef`] points at.
    ///
    /// The reconnection seam for parallel chunk workers, which cannot share one
    /// `StateStore` across threads. It exists because the alternative — handing a
    /// worker the CONFIG PATH and re-deriving the location — silently writes to
    /// the wrong database whenever that string is not a real config path: `rivet
    /// apply` dispatches its chunk-checkpoint runner with `""`, and
    /// `Path::new("").parent()` is `None`, so the fallback lands on
    /// `./.rivet_state.db` in the process CWD. A `StateRef` carries the resolved
    /// location, so there is nothing left to re-derive.
    pub fn open_at_ref(state_ref: &StateRef) -> Result<Self> {
        match state_ref {
            StateRef::Sqlite(db_path) => {
                let conn = open_connection(db_path)?;
                migrate(&conn)?;
                Ok(Self {
                    conn: StateConn::Sqlite(conn),
                    state_ref: StateRef::Sqlite(db_path.clone()),
                })
            }
            StateRef::Postgres(url) => Self::open_postgres(url),
        }
    }

    fn open_sqlite(config_path: &str) -> Result<Self> {
        // An EMPTY path is never a location — it is a caller that had nothing to
        // give and passed a placeholder. `Path::new("").parent()` is `None`, so
        // the fallback below would silently resolve it to `./.rivet_state.db` in
        // whatever the process CWD happens to be, CREATE that database, migrate
        // it, and return a perfectly usable store pointed at the wrong file.
        //
        // That is not hypothetical: `rivet apply` dispatches its chunk-checkpoint
        // runner with `""` (a display-only hint there), and the worker used it to
        // reopen the ledger. Every durable-part row landed in a stray database
        // while the real state DB got none — invisible on a clean run, and on
        // recovery the resume found the chunks `completed` with no file_log to
        // rehydrate, so it declared a manifest with zero parts over parquet
        // already on the destination. Reopening from a `StateRef`
        // ([`StateStore::open_at_ref`]) is the fix for that caller; refusing the
        // empty path is the fix for the CLASS, so the next one fails loudly at
        // the open instead of quietly at recovery.
        if config_path.is_empty() {
            anyhow::bail!(
                "state: refusing to open a state database from an EMPTY path — \
                 it would resolve to './{STATE_DB_NAME}' in the current working \
                 directory rather than beside the config. Pass the config path, \
                 or reopen from a StateRef with StateStore::open_at_ref()."
            );
        }
        let config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let db_path = config_dir.join(STATE_DB_NAME);
        let conn = open_connection(&db_path)?;
        migrate(&conn)?;
        Ok(Self {
            conn: StateConn::Sqlite(conn),
            state_ref: StateRef::Sqlite(db_path),
        })
    }

    fn open_postgres(url: &str) -> Result<Self> {
        let is_local =
            url.contains("localhost") || url.contains("127.0.0.1") || url.contains("::1");
        if !is_local && state_tls_mode_from_url(url).is_none() {
            log::warn!(
                "state(pg): connecting to a remote host without TLS; \
                 add sslmode=require (or verify-ca / verify-full) to RIVET_STATE_URL \
                 to negotiate TLS for production use"
            );
        }
        let mut client = connect_pg(url)?;
        migrate_pg(&mut client)?;
        Ok(Self {
            conn: StateConn::Postgres(Box::new(std::cell::RefCell::new(client))),
            state_ref: StateRef::Postgres(url.to_string()),
        })
    }

    /// Path to `.rivet_state.db` for SQLite deployments.  Returns the config
    /// directory path for Postgres (not meaningful for connection, only used
    /// by legacy callers — prefer `state_ref()` for new code).
    pub fn state_db_path(config_path: &str) -> std::path::PathBuf {
        let config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        config_dir.join(STATE_DB_NAME)
    }

    /// Serialisable connection reference for parallel chunk workers.
    pub fn state_ref(&self) -> &StateRef {
        &self.state_ref
    }

    /// In-memory SQLite store for unit tests.
    #[allow(dead_code)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        migrate(&conn)?;
        Ok(Self {
            conn: StateConn::Sqlite(conn),
            state_ref: StateRef::Sqlite(std::path::PathBuf::from(":memory:")),
        })
    }

    /// Open a SQLite store at an explicit file path (tests that need
    /// cross-connection access via `claim_next_chunk_task_at_path`).
    #[allow(dead_code)]
    pub fn open_at_path(db_path: &std::path::Path) -> Result<Self> {
        let conn = open_connection(db_path)?;
        migrate(&conn)?;
        Ok(Self {
            conn: StateConn::Sqlite(conn),
            state_ref: StateRef::Sqlite(db_path.to_path_buf()),
        })
    }
}

// ─── Migration tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod empty_state_path_guard {
    use super::*;

    /// An empty config path must FAIL the open, not resolve to the CWD.
    ///
    /// `Path::new("").parent()` is `None`, so the fallback in `open_sqlite`
    /// resolved it to `./.rivet_state.db` — creating, migrating and returning a
    /// usable store pointed at the process working directory. `rivet apply`
    /// dispatches its chunk-checkpoint runner with exactly that placeholder, and
    /// the worker reopened the ledger from it: every durable-part row landed in a
    /// stray database while the real state DB got none. Nothing failed, because
    /// nothing looked — the clean-run manifest is built from the in-memory
    /// summary. It surfaced only on RECOVERY, where the resume found the chunks
    /// `completed` with no file_log to rehydrate and declared a manifest with
    /// zero parts over parquet that was already durable.
    ///
    /// The sibling half of the fix is `open_at_ref`, so a worker never re-derives
    /// a location from a string at all.
    #[test]
    fn an_empty_config_path_is_refused_rather_than_resolved_to_the_cwd() {
        let msg = match StateStore::open("") {
            Ok(_) => panic!(
                "an empty path must not open a store — it silently becomes \
                 ./.rivet_state.db in the process CWD"
            ),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("EMPTY path"),
            "the error must name the cause, not just fail: {msg}"
        );
        assert!(
            msg.contains("open_at_ref"),
            "…and name the seam that replaces it, since every caller that hits \
             this is a worker that already holds a StateRef: {msg}"
        );
    }

    /// A REAL path with no parent component still works — the guard is about
    /// emptiness, not about parentlessness, and `:memory:` / a bare filename are
    /// legitimate callers that must keep opening.
    #[test]
    fn a_bare_filename_still_opens_beside_itself() {
        let d = tempfile::tempdir().unwrap();
        let cfg = d.path().join("rivet.yaml");
        std::fs::write(&cfg, "exports: []").unwrap();
        assert!(
            StateStore::open(cfg.to_str().unwrap()).is_ok(),
            "a real config path must still open"
        );
    }
}

#[cfg(test)]
mod tests {
    /// Round-10 mutants: the NOTADB `||` in the migrate-lock error split was
    /// un-graded — `&&` reverts corrupt-DB reporting to the phantom
    /// "another process is migrating" message the round-8 fix removed. Both
    /// SQLite renderings must route to the CORRUPT branch.
    #[test]
    fn a_garbage_state_db_reports_corruption_not_a_phantom_process() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("r.yaml");
        std::fs::write(&cfg, "x: 1\n").unwrap();
        std::fs::write(dir.path().join(".rivet_state.db"), b"garbage-not-a-db").unwrap();
        let err = match StateStore::open(cfg.to_str().unwrap()) {
            Err(e) => format!("{e:#}"),
            Ok(_) => panic!("garbage bytes must refuse"),
        };
        assert!(
            err.contains("CORRUPT"),
            "the operator must be told the file is corrupt, never to wait on a \
             phantom migrating process: {err}"
        );
        assert!(!err.contains("wait for it to finish"), "{err}");
    }

    use super::*;

    #[test]
    fn pg_sql_converts_placeholders() {
        assert_eq!(
            pg_sql("SELECT ?1, ?2 FROM t WHERE x = ?3"),
            "SELECT $1, $2 FROM t WHERE x = $3"
        );
        assert_eq!(
            pg_sql("INSERT INTO t VALUES (?1, ?2)"),
            "INSERT INTO t VALUES ($1, $2)"
        );
        assert_eq!(pg_sql("no placeholders"), "no placeholders");
        // ?N with two digits
        assert_eq!(pg_sql("?10 AND ?11"), "$10 AND $11");
    }

    #[test]
    fn redact_pg_url_removes_password() {
        assert_eq!(
            redact_pg_url("postgresql://rivet:secret123@localhost:5433/rivet_state"),
            "postgresql://rivet:***@localhost:5433/rivet_state"
        );
        assert_eq!(
            redact_pg_url("postgres://admin:p@ssw0rd@db.prod.example.com/state"),
            "postgres://admin:***@db.prod.example.com/state"
        );
    }

    #[test]
    fn redact_pg_url_no_password_unchanged() {
        // URL without a password should come back as-is.
        let url = "postgresql://rivet@localhost/state";
        assert_eq!(redact_pg_url(url), url);
    }

    #[test]
    fn redact_pg_url_stray_at_in_query_does_not_leak_password() {
        // Round-2 audit #2: an unbounded rfind('@') landed on a '@' in the query and
        // echoed `secret`. The SECURITY property is that the secret never survives.
        // The round-4 default-deny redactor over-redacts this contrived query-'@'
        // shape (masks to the last '@') rather than risk a leak — the secret is gone,
        // which is what matters; a '@' in a connection-URL query is vanishingly rare.
        let out = redact_pg_url("postgresql://u:secret@host:5432/db?opt=a@b");
        assert!(
            !out.contains("secret"),
            "password must not survive redaction with a stray '@' in the query: {out}"
        );
        // A normal single-'@' URL keeps the host visible (no over-redaction).
        assert_eq!(
            redact_pg_url("postgresql://u:secret@host:5432/db"),
            "postgresql://u:***@host:5432/db"
        );
    }

    #[test]
    fn redact_pg_url_common_hostport_url_is_not_mangled() {
        // Round-4 #4/#5 documents that whitespace terminates the scan; the flip side
        // this test PINS is that we must NOT aggressively redact a whitespace-bounded
        // `:`-bearing span — a common credential-free `scheme://host:port/db` URL has
        // exactly that shape and must pass through untouched (no false-positive mangle).
        let url = "postgresql://db.internal:5432/orders";
        assert_eq!(
            redact_pg_url(url),
            url,
            "a credential-free host:port URL is untouched"
        );
        assert_eq!(
            redact_pg_url("connecting to postgresql://db:5432/x then retry"),
            "connecting to postgresql://db:5432/x then retry"
        );
    }

    #[test]
    fn redact_pg_url_at_or_colon_in_password_does_not_leak() {
        // Round-4: the two-pass redactor leaked when the password held a '@' BEFORE a
        // raw '/','?','#' (pass 1 caught the internal '@', skipping the fail-safe), and
        // split the user at the LAST ':' (rfind) so a ':'-bearing password leaked its
        // prefix. The default-deny form (last '@' before whitespace, FIRST ':') closes
        // both. RED before the redesign.
        assert_eq!(
            redact_pg_url("postgresql://rivet:Kp@9x/Lm2z@db.prod:5432/orders"),
            "postgresql://rivet:***@db.prod:5432/orders",
            "'@'-before-'/' password tail must not leak"
        );
        assert_eq!(
            redact_pg_url("postgresql://rivet:a:b:c:secret@host:5432/state"),
            "postgresql://rivet:***@host:5432/state",
            "':'-bearing password prefix must not leak (FIRST-colon split)"
        );
        for u in [
            "postgresql://rivet:Kp@9x/Lm2z@db/orders",
            "postgresql://rivet:a:b:c:secret@host/state",
            "postgresql://u:p@w?rd@host/db",
            "postgresql://u:p@w#rd@host/db",
        ] {
            let out = redact_pg_url(u);
            assert!(
                !out.contains("Lm2z")
                    && !out.contains("a:b:c")
                    && !out.contains("w?rd")
                    && !out.contains("w#rd"),
                "no password fragment may survive: {out}"
            );
        }
    }

    #[test]
    fn redact_pg_url_password_with_raw_delimiters_does_not_leak() {
        // Round-3 regression: the #2 authority-bound `find(['/','?','#'])` truncated
        // BEFORE the real '@' when the password itself contained '/','?', or '#'
        // (base64 secrets routinely contain '/'), so rfind('@') missed, the redactor
        // fell through, and echoed the cleartext password. RED before the fail-safe
        // pass. Each must mask the secret AND keep the user + host visible.
        assert_eq!(
            redact_pg_url("postgresql://u:pa/ss@host/db"),
            "postgresql://u:***@host/db",
            "'/' in password must be redacted, not leaked"
        );
        assert_eq!(
            redact_pg_url("postgresql://u:pa?ss@host/db"),
            "postgresql://u:***@host/db",
            "'?' in password must be redacted"
        );
        assert_eq!(
            redact_pg_url("postgresql://u:pa#ss@host/db"),
            "postgresql://u:***@host/db",
            "'#' in password must be redacted"
        );
        // Belt-and-suspenders: the secret string never survives, whatever the shape.
        for u in [
            "postgresql://rivet:Xy/9Zq@db:5432/state",
            "postgres://admin:p/a?s#s@db.example.com/state",
        ] {
            assert!(
                !redact_pg_url(u).contains("Xy/9Zq") && !redact_pg_url(u).contains("p/a?s#s"),
                "no raw-delimiter password may survive: {}",
                redact_pg_url(u)
            );
        }
    }

    // ── state(pg) sslmode → TlsMode mapping ─────────────────────────────────
    //
    // Pins the decision behind the TLS bug fix: the state backend can no longer
    // hard-code NoTls. We can't drive a live TLS handshake in a unit test, so we
    // assert the *chosen transport policy* — TLS is enforced for require /
    // verify-* and plaintext (NoTls) otherwise — which is what selects the
    // connector inside `connect_pg` -> `connect_client`.
    use crate::config::TlsMode;

    #[test]
    fn state_sslmode_enforced_values_negotiate_tls() {
        for (url, want) in [
            (
                "postgresql://u:p@db.prod:5432/state?sslmode=require",
                TlsMode::Require,
            ),
            (
                "postgresql://u:p@db.prod/state?sslmode=verify-ca",
                TlsMode::VerifyCa,
            ),
            (
                "postgresql://u:p@db.prod/state?sslmode=verify-full",
                TlsMode::VerifyFull,
            ),
        ] {
            let mode = state_tls_mode_from_url(url);
            assert_eq!(mode, Some(want), "url: {url}");
            assert!(
                mode.unwrap().is_enforced(),
                "{want:?} must enforce TLS (not NoTls)"
            );
        }
    }

    #[test]
    fn state_sslmode_plaintext_values_stay_notls() {
        // Missing / disable / prefer / allow / unrecognized / uppercase all keep
        // the original NoTls behavior, so dev + docker setups are unchanged.
        for url in [
            "postgresql://u:p@localhost/state",
            "postgresql://u:p@localhost/state?sslmode=disable",
            "postgresql://u:p@db/state?sslmode=prefer",
            "postgresql://u:p@db/state?sslmode=allow",
            "postgresql://u:p@db/state?sslmode=REQUIRE",
            "postgresql://u:p@db/state?sslmode=garbage",
            "postgresql://u:p@db/state?sslmode",
            "postgresql://u:p@db/state?sslmode=",
        ] {
            assert_eq!(state_tls_mode_from_url(url), None, "url: {url}");
        }
    }

    #[test]
    fn state_sslmode_exact_key_and_last_occurrence_wins() {
        // `xsslmode` is a different parameter; the exact `sslmode` key matters.
        assert_eq!(
            state_tls_mode_from_url("postgresql://u:p@db/state?xsslmode=require"),
            None
        );
        // Found among other params.
        assert_eq!(
            state_tls_mode_from_url(
                "postgresql://u:p@db/state?connect_timeout=10&sslmode=require&application_name=x"
            ),
            Some(TlsMode::Require)
        );
        // Last occurrence wins, matching libpq.
        assert_eq!(
            state_tls_mode_from_url("postgresql://u:p@db/state?sslmode=disable&sslmode=require"),
            Some(TlsMode::Require)
        );
        assert_eq!(
            state_tls_mode_from_url("postgresql://u:p@db/state?sslmode=require&sslmode=disable"),
            None
        );
    }
}
