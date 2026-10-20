use rusqlite::Connection;

use crate::error::Result;

mod cdc_snapshot_store;
mod checkpoint;
pub mod copy;
mod cursor;
mod file_log;
mod journal_store;
mod keyset_range;
mod load_journal_store;
mod load_lease;
// Named so a caller can HOLD a lease across a scope it owns — the cleanup delete
// needs one that outlives the call that took it. Every other holder infers the type.
pub(crate) use cdc_snapshot_store::joins_capture;
pub use cursor::{Accepted, Landing, ProgressClaim, ProgressKey};
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
mod write_turn;

// Re-export domain types so callers use `rivet::state::*` unchanged.
// Items below may not be explicitly named by all internal callers (often used
// as inferred return types), but are part of the public integration-test API.
#[allow(unused_imports)]
pub use checkpoint::{ChunkTaskInfo, StrategySnapshot};
#[allow(unused_imports)]
pub use file_log::{DurablePart, FilePart, FileRecord};
#[allow(unused_imports)]
pub use keyset_range::{KeysetRangePart, KeysetRangeRow};
pub use load_journal_store::{LoadRecord, LoadStatus};
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
/// from the URL's `sslmode` query parameter ([`crate::config::url::url_tls`]), exactly as `rivet init` does for
/// source connections. The connection itself goes through the shared
/// [`crate::connect::postgres::connect_client`] path so the state backend and
/// source connections apply identical TLS rules.
///
/// - `require` / `verify-ca` / `verify-full` → negotiate TLS.
/// - `disable` → plaintext, the operator's explicit opt-out, on any host.
/// - anything else → plaintext to a loopback host; a remote host is refused
///   `RIVET_STATE_URL_TLS_REQUIRED` by [`state_url_tls`].
///
/// Used by both [`StateStore::open_postgres`] and the parallel chunk-worker
/// reconnection paths in `checkpoint.rs`, so every PG state connection is
/// TLS-aware.
pub(super) fn connect_pg(url: &str) -> Result<postgres::Client> {
    let tls = state_url_tls(url)?;
    crate::connect::postgres::connect_client(url, tls.as_ref()).map_err(|e| {
        crate::error::coded(
            crate::error::codes::STATE_BACKEND_FAILED,
            format!("state(pg): connect to '{}': {:#}", redact_pg_url(url), e),
        )
    })
}

/// The last `sslmode` value of a state URL, if it names one.
fn url_sslmode(url: &str) -> Option<&str> {
    let query = url.split_once('?').map_or("", |(_, q)| q);
    query
        .split('&')
        .filter_map(|pair| pair.split_once('=').or(Some((pair, ""))))
        .filter(|(key, _)| *key == "sslmode")
        .map(|(_, value)| value)
        .next_back()
}

/// Whether the state URL opts out of TLS itself with `sslmode=disable`.
fn opts_out_of_tls(url: &str) -> bool {
    url_sslmode(url) == Some("disable")
}

/// The TLS a state URL asks for: its `sslmode`, an explicit plaintext for `sslmode=disable`,
/// and a coded refusal for a remote host whose URL says neither (the source's `tls:` block,
/// which the shared dial names, does not exist for the state).
fn state_url_tls(url: &str) -> Result<Option<crate::config::TlsConfig>> {
    if let Some(tls) = crate::config::url::url_tls(url).1 {
        return Ok(Some(tls));
    }
    if opts_out_of_tls(url) {
        return Ok(Some(crate::config::TlsConfig {
            mode: crate::config::TlsMode::Disable,
            ..Default::default()
        }));
    }
    if !crate::config::url::host_is_loopback(url) {
        let asked = url_sslmode(url).map_or_else(
            || "names no `sslmode`".to_string(),
            |m| format!("says `sslmode={m}`, which rivet cannot negotiate"),
        );
        crate::rivet_bail!(
            crate::error::codes::STATE_URL_TLS_REQUIRED,
            "state(pg): `RIVET_STATE_URL` ('{}') points at a remote host and {asked}, so the \
             state's credentials and cursors would cross the network unencrypted. Add \
             `?sslmode=require` (or `verify-ca` / `verify-full`) with TLS enabled on the state \
             server, or `?sslmode=disable` to accept plaintext on a private network.",
            redact_pg_url(url)
        );
    }
    Ok(None)
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

/// An error with its cause appended: `postgres::Error` displays only "db error", the server's message is its source.
pub(super) fn pg_detail(e: &dyn std::error::Error) -> String {
    match e.source() {
        Some(cause) => format!("{e}: {cause}"),
        None => e.to_string(),
    }
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

/// The SQLite file a `RIVET_STATE_URL` of the form `sqlite:<path>` names: an absolute path as
/// written, a relative one anchored to the config's directory (where the default file lives, so
/// the place does not move with the working directory); `None` for any other value.
fn sqlite_state_path(url: &str, config_path: &str) -> Option<std::path::PathBuf> {
    let path = url.strip_prefix("sqlite:")?;
    let path = path.strip_prefix("//").unwrap_or(path);
    let path = std::path::Path::new(path);
    Some(if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir_of(config_path).join(path)
    })
}

/// The directory a config path's state is anchored to.
fn config_dir_of(config_path: &str) -> &std::path::Path {
    std::path::Path::new(config_path)
        .parent()
        .unwrap_or(std::path::Path::new("."))
}

/// Whether the SQLite file at `path` holds state a run wrote: an export cursor or a run.
fn holds_state(path: &std::path::Path) -> bool {
    if !path.exists() {
        return false;
    }
    Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .and_then(|c| {
            c.query_row(
                "SELECT (SELECT COUNT(*) FROM export_state) + (SELECT COUNT(*) FROM run_status)",
                [],
                |r| r.get::<_, i64>(0),
            )
        })
        .is_ok_and(|n| n > 0)
}

/// Refuse a `sqlite:` target that holds nothing while the file beside the config holds this
/// config's state: the cursors, runs and load journal would be left behind and the next run would
/// start over (a full re-read, a load that finds its own tables foreign).
fn refuse_a_state_left_behind(beside: &std::path::Path, target: &std::path::Path) -> Result<()> {
    let resolved =
        |p: &std::path::Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    let same = |a: &std::path::Path, b: &std::path::Path| resolved(a) == resolved(b);
    if same(beside, target) || !holds_state(beside) || holds_state(target) {
        return Ok(());
    }
    crate::rivet_bail!(
        crate::error::codes::STATE_URL_LEAVES_STATE_BEHIND,
        "RIVET_STATE_URL names `{}`, which holds no state, while `{}` beside the config holds this \
         config's cursors and runs: opening the new file would start every export over and leave \
         the load journal behind. Nothing was opened. Copy the state to the new place with every \
         run stopped (`sqlite3 {} \".backup {}\"`, which takes the `-wal` file with it), or unset \
         RIVET_STATE_URL to keep using the file beside the config.",
        target.display(),
        beside.display(),
        beside.display(),
        target.display()
    )
}

/// The PostgreSQL URL a `RIVET_STATE_URL` value names, `None` for an empty one (SQLite); any other value is refused, never replaced by SQLite.
fn postgres_state_url(url: &str) -> Result<Option<&str>> {
    if url.is_empty() || url.starts_with("sqlite:") {
        return Ok(None);
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        return Ok(Some(url));
    }
    let is_scheme = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
    };
    let what = match url.split_once("://") {
        Some((scheme, _)) if is_scheme(scheme) => format!("has the scheme `{scheme}://`"),
        _ => "is not a URL".to_string(),
    };
    crate::rivet_bail!(
        crate::error::codes::STATE_URL_SCHEME_UNSUPPORTED,
        "RIVET_STATE_URL {what}; the state backends are PostgreSQL and a SQLite file, and a \
         URL rivet cannot use is not replaced by the file beside the config. No state was \
         opened and nothing was read or written. Set RIVET_STATE_URL to a `postgres://` or \
         `postgresql://` URL or to `sqlite:<path>`, or unset it to keep the state in \
         `{STATE_DB_NAME}` beside the config."
    )
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
/// When the variable is absent or empty, SQLite is used. Any other value that is
/// not a `postgres://` / `postgresql://` URL is refused
/// (`RIVET_STATE_URL_SCHEME_UNSUPPORTED`), never replaced by SQLite.
pub struct StateStore {
    pub(super) conn: StateConn,
    /// Serialisable reference for reconnection (parallel chunk workers).
    pub(super) state_ref: StateRef,
    /// Whether a [`StateStore::transaction`] is open on `conn`.
    pub(super) in_tx: std::cell::Cell<bool>,
}

impl StateStore {
    /// Open the appropriate backend.
    ///
    /// `RIVET_STATE_URL` when it is set, else SQLite next to `config_path`.
    pub fn open(config_path: &str) -> Result<Self> {
        if let Ok(url) = std::env::var("RIVET_STATE_URL") {
            if let Some(path) = sqlite_state_path(&url, config_path) {
                refuse_a_state_left_behind(&config_dir_of(config_path).join(STATE_DB_NAME), &path)?;
                return Self::open_at_path(&path);
            }
            if let Some(url) = postgres_state_url(&url)? {
                return Self::open_postgres(url);
            }
        }
        Self::open_sqlite(config_path)
    }

    /// Whether `RIVET_STATE_URL` names a state server, so the state does not live beside the config.
    pub fn names_a_server() -> bool {
        Self::names_a_server_in(std::env::var("RIVET_STATE_URL").ok().as_deref())
    }

    /// [`StateStore::names_a_server`] over the variable's value: anything set and not a SQLite file.
    pub fn names_a_server_in(url: Option<&str>) -> bool {
        url.is_some_and(|u| !u.is_empty() && !u.starts_with("sqlite:"))
    }

    /// Which state `config_path` uses and that it answers, without creating or migrating it: `postgres <redacted url>` or `sqlite <path>`.
    pub fn probe(config_path: &str) -> Result<String> {
        if let Ok(url) = std::env::var("RIVET_STATE_URL")
            && let Some(url) = postgres_state_url(&url)?
        {
            let mut client = connect_pg(url)?;
            client.simple_query("SELECT 1").map_err(|e| {
                crate::error::coded(
                    crate::error::codes::STATE_BACKEND_FAILED,
                    format!(
                        "state(pg): '{}' took the connection and not `SELECT 1`: {}",
                        redact_pg_url(url),
                        pg_detail(&e)
                    ),
                )
            })?;
            return Ok(format!("postgres {}", redact_pg_url(url)));
        }
        let path = Self::state_db_path(config_path);
        refuse_a_state_left_behind(&config_dir_of(config_path).join(STATE_DB_NAME), &path)?;
        if path.exists() {
            let conn =
                Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                    .and_then(|c| {
                        c.query_row("SELECT count(*) FROM sqlite_master", [], |r| {
                            r.get::<_, i64>(0)
                        })
                        .map(|_| c)
                    });
            if let Err(e) = conn {
                crate::rivet_bail!(
                    crate::error::codes::STATE_BACKEND_FAILED,
                    "state(sqlite): `{}` cannot be read: {e}",
                    path.display()
                );
            }
        } else if !path
            .parent()
            .is_some_and(|d| d.as_os_str().is_empty() || d.is_dir())
        {
            crate::rivet_bail!(
                crate::error::codes::STATE_BACKEND_FAILED,
                "state(sqlite): the directory of `{}` does not exist, so no state can be kept there",
                path.display()
            );
        }
        Ok(format!("sqlite {}", path.display()))
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
                    in_tx: Default::default(),
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
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_STATE_INVARIANT,
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
            in_tx: Default::default(),
        })
    }

    fn open_postgres(url: &str) -> Result<Self> {
        static WARNED: std::sync::Once = std::sync::Once::new();
        if is_plaintext_remote(url) && opts_out_of_tls(url) {
            WARNED.call_once(|| {
            log::warn!(
                "state(pg): plaintext to a remote host, as `sslmode=disable` in RIVET_STATE_URL \
                 asks; use `sslmode=require` (or verify-ca / verify-full) outside a private network"
            );
            });
        }
        let mut client = connect_pg(url)?;
        migrate_pg(&mut client)?;
        Ok(Self {
            conn: StateConn::Postgres(Box::new(std::cell::RefCell::new(client))),
            state_ref: StateRef::Postgres(url.to_string()),
            in_tx: Default::default(),
        })
    }

    /// Path to `.rivet_state.db` for SQLite deployments.  Returns the config
    /// directory path for Postgres (not meaningful for connection, only used
    /// by legacy callers — prefer `state_ref()` for new code).
    pub fn state_db_path(config_path: &str) -> std::path::PathBuf {
        std::env::var("RIVET_STATE_URL")
            .ok()
            .and_then(|url| sqlite_state_path(&url, config_path))
            .unwrap_or_else(|| config_dir_of(config_path).join(STATE_DB_NAME))
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
            in_tx: Default::default(),
        })
    }

    /// Rewrite a SQLite state DB without its free pages; `(bytes_before, bytes_after)`, `None` on Postgres.
    pub fn vacuum(&self) -> Result<Option<(i64, i64)>> {
        let StateConn::Sqlite(c) = &self.conn else {
            return Ok(None);
        };
        let size = || -> Result<i64> {
            Ok(c.query_row(
                "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()",
                [],
                |r| r.get(0),
            )?)
        };
        let before = size()?;
        c.execute_batch("VACUUM;")?;
        c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
        Ok(Some((before, size()?)))
    }

    /// Run raw SQL on an in-memory store, to put it in a state no product path reaches.
    #[cfg(test)]
    pub(crate) fn exec_for_test(&self, sql: &str) {
        if let StateConn::Sqlite(c) = &self.conn {
            c.execute_batch(sql).unwrap();
        }
    }

    /// Open a SQLite store at an explicit file path (tests that need
    /// a file-backed store another connection can reopen).
    #[allow(dead_code)]
    pub fn open_at_path(db_path: &std::path::Path) -> Result<Self> {
        let conn = open_connection(db_path)?;
        migrate(&conn)?;
        Ok(Self {
            conn: StateConn::Sqlite(conn),
            state_ref: StateRef::Sqlite(db_path.to_path_buf()),
            in_tx: Default::default(),
        })
    }
}

/// A state URL that reaches a non-loopback host without an `sslmode` asking for TLS.
fn is_plaintext_remote(url: &str) -> bool {
    !crate::config::url::host_is_loopback(url) && crate::config::url::url_tls(url).1.is_none()
}

// ─── Migration tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod vacuum {
    use super::StateStore;

    #[test]
    fn vacuum_returns_the_free_pages_and_keeps_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let s = StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        s.exec_for_test(
            "CREATE TABLE junk (b BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
             INSERT INTO junk SELECT randomblob(1000) FROM n;
             DELETE FROM junk WHERE rowid > 10;",
        );
        let (before, after) = s.vacuum().unwrap().unwrap();
        assert!(
            before > 1_500_000 && after < before / 4,
            "{before} -> {after}"
        );
        let left = s
            .query_opt("SELECT COUNT(*) FROM junk", &[], |r| r.i64(0))
            .unwrap();
        assert_eq!(left, Some(10));
    }
}

#[cfg(test)]
mod plaintext_remote_warning {
    use super::is_plaintext_remote;

    #[test]
    fn only_a_remote_host_without_tls_is_plaintext_remote() {
        assert!(is_plaintext_remote("postgres://u:p@db.example.com/s"));
        assert!(!is_plaintext_remote(
            "postgres://u:p@db.example.com/s?sslmode=require"
        ));
        assert!(!is_plaintext_remote("postgres://u:p@localhost/s"));
        assert!(!is_plaintext_remote("postgres://u:p@127.0.0.1:5432/s"));
        assert!(!is_plaintext_remote("postgres://u:p@[::1]:5432/s"));
        assert!(
            is_plaintext_remote("postgres://u:localhost@db.example.com/s"),
            "a password that spells a loopback name is not a loopback host"
        );
    }
}

#[cfg(test)]
mod state_url_guard {
    use super::*;

    #[test]
    fn a_postgres_url_in_either_spelling_is_the_postgres_backend_and_an_empty_one_is_sqlite() {
        for url in ["postgres://u:p@h:5432/s", "postgresql://u:p@h/s"] {
            assert_eq!(postgres_state_url(url).unwrap(), Some(url));
        }
        assert_eq!(postgres_state_url("").unwrap(), None);
    }

    #[test]
    fn any_other_state_url_is_refused_by_code_with_both_remedies_and_no_secret() {
        let cases = [
            (
                "postgre://u:hunter2@h:5432/s",
                "has the scheme `postgre://`",
            ),
            ("mysql+ssl://u:hunter2@h/s", "has the scheme `mysql+ssl://`"),
            ("postgres", "is not a URL"),
            ("postgresql:/u:hunter2@h/s", "is not a URL"),
            ("u:hunter2@h:5432/s", "is not a URL"),
            ("hunter2 x://h/s", "is not a URL"),
            ("://h/s", "is not a URL"),
            (" ", "is not a URL"),
        ];
        for (url, what) in cases {
            let err = postgres_state_url(url).unwrap_err();
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_STATE_URL_SCHEME_UNSUPPORTED"),
                "{url}"
            );
            assert_eq!(crate::error::classify_exit(&err), 1, "{url}");
            let said = err.to_string();
            assert_eq!(
                said,
                format!(
                    "RIVET_STATE_URL {what}; the state backends are PostgreSQL and a SQLite \
                     file, and a URL rivet cannot use is not replaced by the file beside the \
                     config. No state was opened and nothing was read or written. Set \
                     RIVET_STATE_URL to a `postgres://` or `postgresql://` URL or to \
                     `sqlite:<path>`, or unset it to keep the state in `.rivet_state.db` beside \
                     the config."
                ),
                "{url}"
            );
            assert!(!said.contains("hunter2"), "{said}");
        }
    }
}

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
    fn an_empty_path_and_a_refused_state_connection_carry_their_codes() {
        let err = StateStore::open("")
            .err()
            .expect("an empty path is refused");
        crate::error::assert_coded(&err, "RIVET_INTERNAL_STATE_INVARIANT", 6);
        let err = connect_pg("postgres://u:p@127.0.0.1:1/db")
            .err()
            .expect("nothing listens on port 1");
        crate::error::assert_coded(&err, "RIVET_STATE_BACKEND_FAILED", 2);
    }

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

    /// Only `?` followed by a digit is a placeholder; a bare `?` stays, at the end of the text too.
    #[test]
    fn a_question_mark_without_a_digit_is_not_a_placeholder() {
        assert_eq!(pg_sql("WHERE a ? b"), "WHERE a ? b");
        assert_eq!(pg_sql("WHERE a = ?1 OR b ?"), "WHERE a = $1 OR b ?");
        assert_eq!(pg_sql("?"), "?");
        assert_eq!(pg_sql(""), "");
    }

    /// The SQLite state file sits in the config's own directory, whatever the config is called.
    #[test]
    fn the_sqlite_state_file_lives_beside_the_config() {
        use std::path::Path;
        assert_eq!(
            StateStore::state_db_path("/etc/rivet/prod.yaml"),
            Path::new("/etc/rivet/.rivet_state.db")
        );
        assert_eq!(
            StateStore::state_db_path("configs/nightly.yaml"),
            Path::new("configs/.rivet_state.db")
        );
        assert_eq!(
            StateStore::state_db_path("rivet.yaml"),
            Path::new(".rivet_state.db")
        );

        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("r.yaml");
        let named = StateStore::state_db_path(cfg.to_str().unwrap());
        assert!(!named.exists());
        let store = StateStore::open_sqlite(cfg.to_str().unwrap()).unwrap();
        assert!(
            named.is_file(),
            "the path the CLI names must be the file the store created: {}",
            named.display()
        );
        assert!(
            matches!(store.state_ref(), StateRef::Sqlite(opened) if *opened == named),
            "the store must be the SQLite file at {}",
            named.display()
        );
    }

    #[test]
    fn pg_detail_appends_the_cause_the_error_itself_does_not_print() {
        #[derive(Debug)]
        struct Outer(std::io::Error);
        impl std::fmt::Display for Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("db error")
            }
        }
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let e = Outer(std::io::Error::other(
            "ERROR: column \"columns_json\" does not exist",
        ));
        assert_eq!(
            pg_detail(&e),
            "db error: ERROR: column \"columns_json\" does not exist"
        );
        assert_eq!(pg_detail(&std::io::Error::other("plain")), "plain");
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
}

#[cfg(test)]
mod probe_tests {
    use super::StateStore;

    /// `probe` names the SQLite file beside the config, and refuses one it cannot read or a directory that does not exist, creating nothing.
    #[test]
    fn a_sqlite_probe_names_the_file_and_refuses_what_it_cannot_read() {
        if std::env::var("RIVET_STATE_URL").is_ok_and(|u| !u.is_empty()) {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("c.yaml");
        let cfg = cfg.to_str().unwrap();
        let at = StateStore::probe(cfg).unwrap();
        assert_eq!(
            at,
            format!("sqlite {}", dir.path().join(".rivet_state.db").display())
        );
        assert!(
            !dir.path().join(".rivet_state.db").exists(),
            "the probe creates nothing"
        );

        std::fs::write(
            dir.path().join(".rivet_state.db"),
            b"not a database at all, just text",
        )
        .unwrap();
        let err = StateStore::probe(cfg).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_BACKEND_FAILED"),
            "{err:#}"
        );

        let gone = dir.path().join("missing").join("c.yaml");
        let err = StateStore::probe(gone.to_str().unwrap()).unwrap_err();
        assert!(format!("{err:#}").contains("does not exist"), "{err:#}");

        std::fs::remove_file(dir.path().join(".rivet_state.db")).unwrap();
        StateStore::open(cfg).unwrap();
        assert!(StateStore::probe(cfg).unwrap().starts_with("sqlite "));
    }
}

#[cfg(test)]
mod state_url_tls_tests {
    use super::state_url_tls;
    use crate::config::TlsMode;

    #[test]
    fn a_remote_state_url_without_sslmode_is_refused_with_the_state_remedies() {
        for url in [
            "postgresql://u:p@10.0.0.5:5432/s",
            "postgresql://u:p@db.internal/s?sslmode=prefer",
            "postgresql://u:p@db.internal/s?sslmode=disable&sslmode=allow",
        ] {
            let err = state_url_tls(url).unwrap_err();
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_STATE_URL_TLS_REQUIRED"),
                "{url}: {err:#}"
            );
            assert_eq!(crate::error::classify_exit(&err), 5, "{url}");
            let text = format!("{err:#}");
            assert!(
                text.contains("?sslmode=require") && text.contains("?sslmode=disable"),
                "{text}"
            );
            assert!(
                !text.contains("source.tls") && !text.contains(":p@"),
                "{text}"
            );
        }
    }

    #[test]
    fn sslmode_disable_is_the_operators_opt_out_on_any_host() {
        for url in [
            "postgresql://u:p@10.0.0.5:5432/s?sslmode=disable",
            "postgresql://u:p@db.internal/s?connect_timeout=5&sslmode=disable",
            "postgresql://u:p@db.internal/s?sslmode=require&sslmode=disable",
        ] {
            let tls = state_url_tls(url).unwrap().expect("an explicit plaintext");
            assert_eq!(tls.mode, TlsMode::Disable, "{url}");
            crate::connect::require_tls_or_loopback(url, Some(&tls)).unwrap();
        }
    }

    #[test]
    fn loopback_stays_plaintext_and_an_enforced_sslmode_negotiates() {
        assert!(
            state_url_tls("postgresql://u:p@127.0.0.1:5433/s")
                .unwrap()
                .is_none()
        );
        assert!(
            state_url_tls("postgresql://u:p@localhost/s")
                .unwrap()
                .is_none()
        );
        let tls = state_url_tls("postgresql://u:p@db.internal/s?sslmode=verify-full")
            .unwrap()
            .unwrap();
        assert_eq!(tls.mode, TlsMode::VerifyFull);
    }
}

#[cfg(test)]
mod names_a_server_tests {
    use super::StateStore;

    #[test]
    fn a_server_is_named_by_any_set_url_but_a_sqlite_file() {
        assert!(!StateStore::names_a_server_in(None));
        assert!(!StateStore::names_a_server_in(Some("")));
        assert!(!StateStore::names_a_server_in(Some(
            "sqlite:/var/lib/rivet/state.db"
        )));
        assert!(StateStore::names_a_server_in(Some("postgresql://u@db/s")));
    }

    /// The wrapper reads `RIVET_STATE_URL`: checked here against the value this process has, and,
    /// when it has none, once more in a child of this test binary that has a PostgreSQL URL.
    #[test]
    fn names_a_server_reads_rivet_state_url() {
        let url = std::env::var("RIVET_STATE_URL").ok();
        assert_eq!(
            StateStore::names_a_server(),
            StateStore::names_a_server_in(url.as_deref())
        );
        if url.is_none() {
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "state::names_a_server_tests::names_a_server_reads_rivet_state_url",
                    "--test-threads",
                    "1",
                ])
                .env("RIVET_STATE_URL", "postgresql://u@db/s")
                .output()
                .unwrap();
            assert!(
                child.status.success()
                    && String::from_utf8_lossy(&child.stdout).contains("1 passed"),
                "{}{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr)
            );
        }
    }
}

#[cfg(test)]
mod sqlite_state_url_tests {
    use super::*;

    #[test]
    fn a_sqlite_url_names_its_file_and_a_relative_one_is_anchored_to_the_config() {
        let cfg = "/etc/rivet/app.yaml";
        let p = |u: &str| sqlite_state_path(u, cfg);
        assert_eq!(
            p("sqlite:/var/lib/rivet/state.db"),
            Some("/var/lib/rivet/state.db".into())
        );
        assert_eq!(
            p("sqlite:///var/lib/rivet/state.db"),
            Some("/var/lib/rivet/state.db".into())
        );
        assert_eq!(
            p("sqlite:state/app.db"),
            Some("/etc/rivet/state/app.db".into())
        );
        assert_eq!(p("postgresql://u@h/s"), None);
        assert_eq!(p(""), None);
        assert_eq!(postgres_state_url("sqlite:/x.db").unwrap(), None);
    }

    #[test]
    fn a_sqlite_url_that_leaves_the_state_beside_the_config_behind_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let beside = dir.path().join(".rivet_state.db");
        let target = dir.path().join("other").join("state.db");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        refuse_a_state_left_behind(&beside, &target).unwrap();
        let s = StateStore::open_at_path(&beside).unwrap();
        s.update_legacy("orders", "42").unwrap();
        drop(s);
        let err = refuse_a_state_left_behind(&beside, &target).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_URL_LEAVES_STATE_BEHIND")
        );
        assert_eq!(crate::error::classify_exit(&err), 5);
        refuse_a_state_left_behind(&beside, &beside).unwrap();
        std::fs::copy(&beside, &target).unwrap();
        refuse_a_state_left_behind(&beside, &target).unwrap();
    }

    /// `holds_state` at its threshold: a migrated state with no row holds nothing, one cursor row is state.
    #[test]
    fn a_state_holds_something_from_its_first_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("s.db");
        assert!(!holds_state(&db), "no file");
        let s = StateStore::open_at_path(&db).unwrap();
        assert!(!holds_state(&db), "a migrated state with no row");
        s.update_legacy("orders", "1").unwrap();
        assert!(holds_state(&db), "one cursor row");
    }

    /// Each condition that lets a `sqlite:` target open, alone: the same file under another
    /// spelling; nothing beside the config; state already in the target. Only state beside the
    /// config and none in the target refuses.
    #[test]
    fn each_reason_to_open_a_sqlite_target_is_enough_on_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let beside = dir.path().join(".rivet_state.db");
        let s = StateStore::open_at_path(&beside).unwrap();
        s.update_legacy("orders", "1").unwrap();
        drop(s);
        // The same file, spelled through `..`: equal only once resolved.
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let respelled = sub.join("..").join(".rivet_state.db");
        refuse_a_state_left_behind(&beside, &respelled).unwrap();
        // Nothing beside the config, nothing in the target.
        let empty_dir = tempfile::tempdir().unwrap();
        refuse_a_state_left_behind(
            &empty_dir.path().join(".rivet_state.db"),
            &empty_dir.path().join("t.db"),
        )
        .unwrap();
        // State in both.
        let both = dir.path().join("both.db");
        let t = StateStore::open_at_path(&both).unwrap();
        t.update_legacy("orders", "2").unwrap();
        drop(t);
        refuse_a_state_left_behind(&beside, &both).unwrap();
        // State beside, none in the target.
        let err = refuse_a_state_left_behind(&beside, &dir.path().join("new.db")).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_URL_LEAVES_STATE_BEHIND")
        );
    }
}
