use rusqlite::Connection;

use crate::error::Result;

mod checkpoint;
mod cursor;
mod manifest;
mod metrics;
mod progression;
mod schema;

// Re-export domain types so callers use `rivet::state::*` unchanged.
// Items below may not be explicitly named by all internal callers (often used
// as inferred return types), but are part of the public integration-test API.
#[allow(unused_imports)]
pub use checkpoint::ChunkTaskInfo;
#[allow(unused_imports)]
pub use manifest::FileRecord;
#[allow(unused_imports)]
pub use metrics::ExportMetric;
#[allow(unused_imports)]
pub use progression::{Boundary, ExportProgression};
#[allow(unused_imports)]
pub use schema::{SchemaChange, SchemaColumn};

const STATE_DB_NAME: &str = ".rivet_state.db";

/// Current schema version — always the last entry in `MIGRATIONS`.
/// Adding a migration automatically updates this; no manual bump needed.
/// Used in tests to assert the DB reaches the expected version.
const SCHEMA_VERSION: i64 = MIGRATIONS[MIGRATIONS.len() - 1].0;

/// Each entry is `(version, sql)`.  Applied in order when the DB is behind.
const MIGRATIONS: &[(i64, &str)] = &[
    // v1: core tables
    (
        1,
        "CREATE TABLE IF NOT EXISTS export_state (
            export_name TEXT PRIMARY KEY,
            last_cursor_value TEXT,
            last_run_at TEXT
        );
        CREATE TABLE IF NOT EXISTS export_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            export_name TEXT NOT NULL,
            run_at TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            total_rows INTEGER NOT NULL,
            peak_rss_mb INTEGER,
            status TEXT NOT NULL,
            error_message TEXT,
            tuning_profile TEXT,
            format TEXT,
            mode TEXT,
            files_produced INTEGER DEFAULT 0,
            bytes_written INTEGER DEFAULT 0,
            retries INTEGER DEFAULT 0,
            validated INTEGER,
            schema_changed INTEGER,
            run_id TEXT
        );
        CREATE TABLE IF NOT EXISTS export_schema (
            export_name TEXT PRIMARY KEY,
            columns_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS file_manifest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            format TEXT NOT NULL,
            compression TEXT,
            created_at TEXT NOT NULL
        );",
    ),
    // v2: chunk checkpoint tables
    (
        2,
        "CREATE TABLE IF NOT EXISTS chunk_run (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            plan_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            max_chunk_attempts INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_run_export_status
            ON chunk_run(export_name, status);
        CREATE TABLE IF NOT EXISTS chunk_task (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            start_key TEXT NOT NULL,
            end_key TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            rows_written INTEGER,
            file_name TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(run_id, chunk_index)
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_task_run_status ON chunk_task(run_id, status);",
    ),
    // v3: index on file_manifest for faster per-export lookups
    (
        3,
        "CREATE INDEX IF NOT EXISTS idx_file_manifest_export ON file_manifest(export_name, id DESC);",
    ),
    // v4: committed / verified boundary tracking (ADR-0008, Epic G)
    (
        4,
        "CREATE TABLE IF NOT EXISTS export_progression (
            export_name TEXT PRIMARY KEY,
            last_committed_strategy TEXT,
            last_committed_cursor TEXT,
            last_committed_chunk_index INTEGER,
            last_committed_run_id TEXT,
            last_committed_at TEXT,
            last_verified_strategy TEXT,
            last_verified_cursor TEXT,
            last_verified_chunk_index INTEGER,
            last_verified_run_id TEXT,
            last_verified_at TEXT
        );",
    ),
];

fn ensure_schema_version_table(conn: &Connection) {
    let _ = conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        );",
    );
}

fn get_current_version(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_version",
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

fn migrate(conn: &Connection) -> Result<()> {
    ensure_schema_version_table(conn);

    let current = get_current_version(conn);

    // Detect pre-versioning databases: if export_state exists but version == 0,
    // the DB was created by older code before versioned migrations.
    if current == 0 {
        let has_export_state: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='export_state'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if has_export_state {
            let metrics_cols = [
                "files_produced INTEGER DEFAULT 0",
                "bytes_written INTEGER DEFAULT 0",
                "retries INTEGER DEFAULT 0",
                "validated INTEGER",
                "schema_changed INTEGER",
                "run_id TEXT",
            ];
            for col_def in &metrics_cols {
                let sql = format!("ALTER TABLE export_metrics ADD COLUMN {}", col_def);
                let _ = conn.execute(&sql, []);
            }
        }
    }

    for &(ver, sql) in MIGRATIONS {
        if ver > current {
            log::debug!("state: applying migration v{}", ver);
            // Each migration is atomic: schema DDL + version record commit together.
            // If the process crashes mid-migration, the next startup re-applies the
            // same migration safely (all DDL uses IF NOT EXISTS / IF EXISTS guards).
            let atomic_sql = format!(
                "BEGIN;\n{}\nINSERT INTO schema_version (version) VALUES ({});\nCOMMIT;",
                sql, ver
            );
            conn.execute_batch(&atomic_sql)
                .map_err(|e| anyhow::anyhow!("state: migration v{} failed: {}", ver, e))?;
        }
    }

    // Compact schema_version to a single row.  Older migration implementations
    // inserted one row per migration; keep only the highest version row so the
    // table does not grow unboundedly across database lifetimes.
    let _ = conn.execute(
        "DELETE FROM schema_version WHERE version < (SELECT MAX(version) FROM schema_version)",
        [],
    );

    // Sanity check: confirm we are at the expected version.
    let final_version = get_current_version(conn);
    if final_version != SCHEMA_VERSION {
        anyhow::bail!(
            "state: migration incomplete — expected schema v{} but reached v{}",
            SCHEMA_VERSION,
            final_version
        );
    }

    Ok(())
}

// ─── StateStore ───────────────────────────────────────────────────────────────

/// Entry point for all persistent state.
///
/// A single SQLite database serves five logical stores:
/// - [`cursor`] — incremental cursor positions (`export_state`)
/// - [`checkpoint`] — chunk run/task lifecycle (`chunk_run`, `chunk_task`)
/// - [`metrics`] — run outcome history (`export_metrics`)
/// - [`manifest`] — file manifest (`file_manifest`)
/// - [`schema`] — schema snapshot history (`export_schema`)
///
/// Each store is implemented in its own submodule as an `impl StateStore` block.
/// Physical storage (one SQLite file) and the migration runner live here.
pub struct StateStore {
    /// Shared connection — submodule `impl` blocks borrow this directly.
    pub(super) conn: Connection,
    pub(super) db_path: std::path::PathBuf,
}

impl StateStore {
    /// Open the state DB located next to `config_path`.
    pub fn open(config_path: &str) -> Result<Self> {
        let config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let db_path = config_dir.join(STATE_DB_NAME);
        let db_path_buf = db_path.to_path_buf();
        let conn = Connection::open(db_path)?;
        // Enable WAL for better concurrent read/write performance.
        // Log if unavailable (e.g., network filesystems) rather than silently degrading.
        if let Err(e) = conn.execute_batch("PRAGMA journal_mode=WAL;") {
            log::warn!(
                "state: WAL journal mode unavailable ({}); \
                 running in default mode — concurrent writes may be slower",
                e
            );
        }
        migrate(&conn)?;
        Ok(Self {
            conn,
            db_path: db_path_buf,
        })
    }

    /// Path to `.rivet_state.db` next to the config file (same rules as `open`).
    pub fn state_db_path(config_path: &str) -> std::path::PathBuf {
        let config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        config_dir.join(STATE_DB_NAME)
    }

    pub(crate) fn database_path(&self) -> &std::path::Path {
        self.db_path.as_path()
    }

    /// In-memory store for unit tests that do not require cross-connection access.
    #[allow(dead_code)] // used by integration tests
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        migrate(&conn)?;
        Ok(Self {
            conn,
            db_path: std::path::PathBuf::from(":memory:"),
        })
    }

    /// Open (or create) a state DB at an explicit file path.
    /// Used by tests that need `claim_next_chunk_task`, which opens separate
    /// connections and therefore cannot use an in-memory store.
    #[allow(dead_code)]
    pub fn open_at_path(db_path: &std::path::Path) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        migrate(&conn)?;
        Ok(Self {
            conn,
            db_path: db_path.to_path_buf(),
        })
    }
}

// ─── Migration tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_db_reaches_latest_version() {
        let s = StateStore::open_in_memory().unwrap();
        let ver = get_current_version(&s.conn);
        assert_eq!(ver, SCHEMA_VERSION);
    }

    #[test]
    fn migration_is_idempotent() {
        let s = StateStore::open_in_memory().unwrap();
        migrate(&s.conn).unwrap();
        migrate(&s.conn).unwrap();
        let ver = get_current_version(&s.conn);
        assert_eq!(ver, SCHEMA_VERSION);
    }

    #[test]
    fn legacy_db_gets_upgraded() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE export_state (
                export_name TEXT PRIMARY KEY,
                last_cursor_value TEXT,
                last_run_at TEXT
            );
            CREATE TABLE export_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_name TEXT NOT NULL,
                run_at TEXT NOT NULL,
                duration_ms INTEGER NOT NULL,
                total_rows INTEGER NOT NULL,
                status TEXT NOT NULL
            );",
        )
        .unwrap();

        migrate(&conn).unwrap();

        let ver = get_current_version(&conn);
        assert_eq!(ver, SCHEMA_VERSION);

        let has_chunk_run: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='chunk_run'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_chunk_run);
    }
}
