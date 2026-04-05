use rusqlite::{Connection, TransactionBehavior};

use crate::error::Result;
use crate::types::CursorState;

const STATE_DB_NAME: &str = ".rivet_state.db";

/// Current schema version.  Bump this and add a matching arm in `MIGRATIONS`.
#[cfg(test)]
const SCHEMA_VERSION: i64 = 3;

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

fn migrate(conn: &Connection) {
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
            // Legacy DB — add any columns that older versions may lack.
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
            if let Err(e) = conn.execute_batch(sql) {
                log::error!("state: migration v{} failed: {}", ver, e);
                break;
            }
            let _ = conn.execute("INSERT INTO schema_version (version) VALUES (?1)", [ver]);
        }
    }
}

/// One row from `chunk_task` for display / debugging.
#[derive(Debug, Clone)]
pub struct ChunkTaskInfo {
    pub chunk_index: i64,
    pub start_key: String,
    pub end_key: String,
    pub status: String,
    pub attempts: i64,
    pub last_error: Option<String>,
    pub rows_written: Option<i64>,
    pub file_name: Option<String>,
}

pub struct StateStore {
    conn: Connection,
    db_path: std::path::PathBuf,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ExportMetric {
    pub export_name: String,
    pub run_id: Option<String>,
    pub run_at: String,
    pub duration_ms: i64,
    pub total_rows: i64,
    pub peak_rss_mb: Option<i64>,
    pub status: String,
    pub error_message: Option<String>,
    pub tuning_profile: Option<String>,
    pub format: Option<String>,
    pub mode: Option<String>,
    pub files_produced: i64,
    pub bytes_written: i64,
    pub retries: i64,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FileRecord {
    pub run_id: String,
    pub export_name: String,
    pub file_name: String,
    pub row_count: i64,
    pub bytes: i64,
    pub format: String,
    pub compression: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Debug)]
pub struct SchemaChange {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub type_changed: Vec<(String, String, String)>, // (name, old_type, new_type)
}

impl SchemaChange {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.type_changed.is_empty()
    }
}

impl StateStore {
    pub fn open(config_path: &str) -> Result<Self> {
        let config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let db_path = config_dir.join(STATE_DB_NAME);
        let db_path_buf = db_path.to_path_buf();
        let conn = Connection::open(db_path)?;
        let _ = conn.execute_batch("PRAGMA journal_mode=WAL;");
        migrate(&conn);
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

    // ─── Chunk checkpoint (chunked exports) ───────────────────

    /// Latest `in_progress` chunk run for this export, if any.
    pub fn find_in_progress_chunk_run(
        &self,
        export_name: &str,
    ) -> Result<Option<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, plan_hash FROM chunk_run
             WHERE export_name = ?1 AND status = 'in_progress'
             ORDER BY created_at DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map([export_name], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.next().transpose()?)
    }

    pub fn create_chunk_run(
        &self,
        run_id: &str,
        export_name: &str,
        plan_hash: &str,
        max_chunk_attempts: u32,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO chunk_run (run_id, export_name, plan_hash, status, max_chunk_attempts, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'in_progress', ?4, ?5, ?5)",
            rusqlite::params![run_id, export_name, plan_hash, max_chunk_attempts as i64, now],
        )?;
        Ok(())
    }

    pub fn insert_chunk_tasks(&self, run_id: &str, ranges: &[(i64, i64)]) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let mut stmt = self.conn.prepare(
            "INSERT INTO chunk_task (run_id, chunk_index, start_key, end_key, status, attempts, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5)",
        )?;
        for (i, (start, end)) in ranges.iter().enumerate() {
            stmt.execute(rusqlite::params![
                run_id,
                i as i64,
                start.to_string(),
                end.to_string(),
                now,
            ])?;
        }
        Ok(())
    }

    /// Mark tasks left `running` after a crash as `pending` / `failed` retryable again.
    pub fn reset_stale_running_chunk_tasks(&self, run_id: &str) -> Result<usize> {
        let now = chrono::Utc::now().to_rfc3339();
        let n = self.conn.execute(
            "UPDATE chunk_task SET status = 'pending', updated_at = ?1
             WHERE run_id = ?2 AND status = 'running'",
            rusqlite::params![now, run_id],
        )?;
        Ok(n)
    }

    /// Atomically claim the next pending or retryable failed chunk (single-threaded export).
    pub fn claim_next_chunk_task(&self, run_id: &str) -> Result<Option<(i64, String, String)>> {
        Self::claim_next_chunk_task_at_path(self.db_path.as_path(), run_id)
    }

    fn claim_next_chunk_in_tx(
        tx: &rusqlite::Transaction<'_>,
        now: &str,
        run_id: &str,
    ) -> Result<Option<(i64, String, String)>> {
        let mut stmt = tx.prepare(
            "UPDATE chunk_task
             SET status = 'running', attempts = attempts + 1, updated_at = ?1
             WHERE rowid = (
               SELECT ct.rowid FROM chunk_task ct
               INNER JOIN chunk_run cr ON cr.run_id = ct.run_id
               WHERE ct.run_id = ?2
                 AND cr.status = 'in_progress'
                 AND (
                   ct.status = 'pending'
                   OR (ct.status = 'failed' AND ct.attempts < cr.max_chunk_attempts)
                 )
               ORDER BY ct.chunk_index ASC
               LIMIT 1
             )
             RETURNING chunk_index, start_key, end_key",
        )?;
        let mut rows = stmt.query(rusqlite::params![now, run_id])?;
        let out = match rows.next()? {
            Some(row) => Some((row.get(0)?, row.get(1)?, row.get(2)?)),
            None => None,
        };
        Ok(out)
    }

    /// Same as `claim_next_chunk_task` using a fresh connection (parallel workers).
    pub fn claim_next_chunk_task_at_path(
        db_path: &std::path::Path,
        run_id: &str,
    ) -> Result<Option<(i64, String, String)>> {
        let mut conn = Connection::open(db_path)?;
        let now = chrono::Utc::now().to_rfc3339();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let res = Self::claim_next_chunk_in_tx(&tx, &now, run_id)?;
        tx.commit()?;
        Ok(res)
    }

    pub fn complete_chunk_task(
        &self,
        run_id: &str,
        chunk_index: i64,
        rows_written: i64,
        file_name: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "UPDATE chunk_task
             SET status = 'completed', rows_written = ?1, file_name = ?2, last_error = NULL, updated_at = ?3
             WHERE run_id = ?4 AND chunk_index = ?5",
            rusqlite::params![rows_written, file_name, now, run_id, chunk_index],
        )?;
        Ok(())
    }

    pub fn fail_chunk_task(&self, run_id: &str, chunk_index: i64, err: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "UPDATE chunk_task SET status = 'failed', last_error = ?1, updated_at = ?2
             WHERE run_id = ?3 AND chunk_index = ?4",
            rusqlite::params![err, now, run_id, chunk_index],
        )?;
        Ok(())
    }

    pub fn fail_chunk_task_at_path(
        db_path: &std::path::Path,
        run_id: &str,
        chunk_index: i64,
        err: &str,
    ) -> Result<()> {
        let conn = Connection::open(db_path)?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE chunk_task SET status = 'failed', last_error = ?1, updated_at = ?2
             WHERE run_id = ?3 AND chunk_index = ?4",
            rusqlite::params![err, now, run_id, chunk_index],
        )?;
        Ok(())
    }

    pub fn complete_chunk_task_at_path(
        db_path: &std::path::Path,
        run_id: &str,
        chunk_index: i64,
        rows_written: i64,
        file_name: Option<&str>,
    ) -> Result<()> {
        let conn = Connection::open(db_path)?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE chunk_task
             SET status = 'completed', rows_written = ?1, file_name = ?2, last_error = NULL, updated_at = ?3
             WHERE run_id = ?4 AND chunk_index = ?5",
            rusqlite::params![rows_written, file_name, now, run_id, chunk_index],
        )?;
        Ok(())
    }

    pub fn count_chunk_tasks_not_completed(&self, run_id: &str) -> Result<i64> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM chunk_task WHERE run_id = ?1 AND status != 'completed'",
            [run_id],
            |row| row.get(0),
        )?;
        Ok(n)
    }

    pub fn finalize_chunk_run_completed(&self, run_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "UPDATE chunk_run SET status = 'completed', updated_at = ?1 WHERE run_id = ?2",
            rusqlite::params![now, run_id],
        )?;
        Ok(())
    }

    /// Remove all chunk runs and tasks for an export (abandon resume).
    pub fn reset_chunk_checkpoint(&self, export_name: &str) -> Result<usize> {
        let run_ids: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT run_id FROM chunk_run WHERE export_name = ?1")?;
            let rows = stmt.query_map([export_name], |row| row.get(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for rid in &run_ids {
            let _ = self
                .conn
                .execute("DELETE FROM chunk_task WHERE run_id = ?1", [rid]);
        }
        let deleted = self.conn.execute(
            "DELETE FROM chunk_run WHERE export_name = ?1",
            [export_name],
        )?;
        Ok(deleted)
    }

    /// Latest chunk_run row for an export (any status), for `rivet state chunks`.
    pub fn get_latest_chunk_run(
        &self,
        export_name: &str,
    ) -> Result<Option<(String, String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, plan_hash, status, updated_at FROM chunk_run
             WHERE export_name = ?1 ORDER BY updated_at DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map([export_name], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn list_chunk_tasks_for_run(&self, run_id: &str) -> Result<Vec<ChunkTaskInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT chunk_index, start_key, end_key, status, attempts, last_error, rows_written, file_name
             FROM chunk_task WHERE run_id = ?1 ORDER BY chunk_index ASC",
        )?;
        let rows = stmt.query_map([run_id], |row| {
            Ok(ChunkTaskInfo {
                chunk_index: row.get(0)?,
                start_key: row.get(1)?,
                end_key: row.get(2)?,
                status: row.get(3)?,
                attempts: row.get(4)?,
                last_error: row.get(5)?,
                rows_written: row.get(6)?,
                file_name: row.get(7)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    // ─── Cursor State ────────────────────────────────────────

    pub fn get(&self, export_name: &str) -> Result<CursorState> {
        let mut stmt = self.conn.prepare(
            "SELECT last_cursor_value, last_run_at FROM export_state WHERE export_name = ?1",
        )?;
        let result = stmt.query_row([export_name], |row| {
            Ok(CursorState {
                export_name: export_name.to_string(),
                last_cursor_value: row.get(0)?,
                last_run_at: row.get(1)?,
            })
        });
        match result {
            Ok(state) => Ok(state),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(CursorState {
                export_name: export_name.to_string(),
                last_cursor_value: None,
                last_run_at: None,
            }),
            Err(e) => Err(e.into()),
        }
    }

    pub fn update(&self, export_name: &str, cursor_value: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO export_state (export_name, last_cursor_value, last_run_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(export_name) DO UPDATE SET
                last_cursor_value = excluded.last_cursor_value,
                last_run_at = excluded.last_run_at",
            rusqlite::params![export_name, cursor_value, now],
        )?;
        Ok(())
    }

    pub fn reset(&self, export_name: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM export_state WHERE export_name = ?1",
            [export_name],
        )?;
        Ok(())
    }

    pub fn list_all(&self) -> Result<Vec<CursorState>> {
        let mut stmt = self.conn.prepare(
            "SELECT export_name, last_cursor_value, last_run_at FROM export_state ORDER BY export_name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(CursorState {
                export_name: row.get(0)?,
                last_cursor_value: row.get(1)?,
                last_run_at: row.get(2)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    // ─── Metrics ─────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub fn record_metric(
        &self,
        export_name: &str,
        run_id: &str,
        duration_ms: i64,
        total_rows: i64,
        peak_rss_mb: Option<i64>,
        status: &str,
        error_message: Option<&str>,
        tuning_profile: Option<&str>,
        format: Option<&str>,
        mode: Option<&str>,
        files_produced: i64,
        bytes_written: i64,
        retries: i64,
        validated: Option<bool>,
        schema_changed: Option<bool>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO export_metrics (export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb,
             status, error_message, tuning_profile, format, mode,
             files_produced, bytes_written, retries, validated, schema_changed)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            rusqlite::params![
                export_name, run_id, now, duration_ms, total_rows, peak_rss_mb,
                status, error_message, tuning_profile, format, mode,
                files_produced, bytes_written, retries, validated, schema_changed
            ],
        )?;
        Ok(())
    }

    pub fn get_metrics(
        &self,
        export_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ExportMetric>> {
        let cols = "export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb,
                    status, error_message, tuning_profile, format, mode,
                    files_produced, bytes_written, retries, validated, schema_changed";
        let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(name) =
            export_name
        {
            (
                format!(
                    "SELECT {} FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT {}",
                    cols, limit
                ),
                vec![Box::new(name.to_string())],
            )
        } else {
            (
                format!(
                    "SELECT {} FROM export_metrics ORDER BY id DESC LIMIT {}",
                    cols, limit
                ),
                vec![],
            )
        };

        let mut stmt = self.conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok(ExportMetric {
                export_name: row.get(0)?,
                run_id: row.get(1)?,
                run_at: row.get(2)?,
                duration_ms: row.get(3)?,
                total_rows: row.get(4)?,
                peak_rss_mb: row.get(5)?,
                status: row.get(6)?,
                error_message: row.get(7)?,
                tuning_profile: row.get(8)?,
                format: row.get(9)?,
                mode: row.get(10)?,
                files_produced: row.get::<_, Option<i64>>(11)?.unwrap_or(0),
                bytes_written: row.get::<_, Option<i64>>(12)?.unwrap_or(0),
                retries: row.get::<_, Option<i64>>(13)?.unwrap_or(0),
                validated: row.get(14)?,
                schema_changed: row.get(15)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    // ─── File Manifest ─────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub fn record_file(
        &self,
        run_id: &str,
        export_name: &str,
        file_name: &str,
        row_count: i64,
        bytes: i64,
        format: &str,
        compression: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO file_manifest (run_id, export_name, file_name, row_count, bytes, format, compression, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![run_id, export_name, file_name, row_count, bytes, format, compression, now],
        )?;
        Ok(())
    }

    pub fn get_files(&self, export_name: Option<&str>, limit: usize) -> Result<Vec<FileRecord>> {
        let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(name) =
            export_name
        {
            (
                format!(
                    "SELECT run_id, export_name, file_name, row_count, bytes, format, compression, created_at
                     FROM file_manifest WHERE export_name = ?1 ORDER BY id DESC LIMIT {}",
                    limit
                ),
                vec![Box::new(name.to_string())],
            )
        } else {
            (
                format!(
                    "SELECT run_id, export_name, file_name, row_count, bytes, format, compression, created_at
                     FROM file_manifest ORDER BY id DESC LIMIT {}",
                    limit
                ),
                vec![],
            )
        };

        let mut stmt = self.conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok(FileRecord {
                run_id: row.get(0)?,
                export_name: row.get(1)?,
                file_name: row.get(2)?,
                row_count: row.get(3)?,
                bytes: row.get(4)?,
                format: row.get(5)?,
                compression: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    // ─── Schema Tracking ─────────────────────────────────────

    pub fn get_stored_schema(&self, export_name: &str) -> Result<Option<Vec<SchemaColumn>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT columns_json FROM export_schema WHERE export_name = ?1")?;
        let result = stmt.query_row([export_name], |row| {
            let json_str: String = row.get(0)?;
            Ok(json_str)
        });
        match result {
            Ok(json_str) => {
                let cols: Vec<SchemaColumn> = serde_json::from_str(&json_str)?;
                Ok(Some(cols))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn store_schema(&self, export_name: &str, columns: &[SchemaColumn]) -> Result<()> {
        let json = serde_json::to_string(columns)?;
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO export_schema (export_name, columns_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(export_name) DO UPDATE SET
                columns_json = excluded.columns_json,
                updated_at = excluded.updated_at",
            rusqlite::params![export_name, json, now],
        )?;
        Ok(())
    }

    pub fn detect_schema_change(
        &self,
        export_name: &str,
        current: &[SchemaColumn],
    ) -> Result<Option<SchemaChange>> {
        let stored = match self.get_stored_schema(export_name)? {
            Some(s) => s,
            None => {
                self.store_schema(export_name, current)?;
                return Ok(None);
            }
        };

        let stored_map: std::collections::HashMap<&str, &str> = stored
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect();
        let current_map: std::collections::HashMap<&str, &str> = current
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect();

        let added: Vec<String> = current
            .iter()
            .filter(|c| !stored_map.contains_key(c.name.as_str()))
            .map(|c| format!("{} ({})", c.name, c.data_type))
            .collect();

        let removed: Vec<String> = stored
            .iter()
            .filter(|c| !current_map.contains_key(c.name.as_str()))
            .map(|c| c.name.clone())
            .collect();

        let type_changed: Vec<(String, String, String)> = current
            .iter()
            .filter_map(|c| {
                stored_map.get(c.name.as_str()).and_then(|old_type| {
                    if *old_type != c.data_type.as_str() {
                        Some((c.name.clone(), old_type.to_string(), c.data_type.clone()))
                    } else {
                        None
                    }
                })
            })
            .collect();

        let change = SchemaChange {
            added,
            removed,
            type_changed,
        };

        if !change.is_empty() {
            self.store_schema(export_name, current)?;
            Ok(Some(change))
        } else {
            Ok(None)
        }
    }

    #[allow(dead_code)] // used by integration tests (tests/*.rs)
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        migrate(&conn);
        Ok(Self {
            conn,
            db_path: std::path::PathBuf::from(":memory:"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    // ─── Cursor tests ────────────────────────────────────────

    #[test]
    fn get_unknown_returns_empty_state() {
        let s = store();
        let state = s.get("nonexistent").unwrap();
        assert!(state.last_cursor_value.is_none());
    }

    #[test]
    fn update_then_get_returns_stored_cursor() {
        let s = store();
        s.update("orders", "2024-06-01").unwrap();
        assert_eq!(
            s.get("orders").unwrap().last_cursor_value.as_deref(),
            Some("2024-06-01")
        );
    }

    #[test]
    fn update_overwrites_previous_cursor() {
        let s = store();
        s.update("orders", "100").unwrap();
        s.update("orders", "200").unwrap();
        assert_eq!(
            s.get("orders").unwrap().last_cursor_value.as_deref(),
            Some("200")
        );
    }

    #[test]
    fn reset_clears_cursor_state() {
        let s = store();
        s.update("orders", "100").unwrap();
        s.reset("orders").unwrap();
        assert!(s.get("orders").unwrap().last_cursor_value.is_none());
    }

    #[test]
    fn list_all_on_empty_store_returns_empty() {
        assert!(store().list_all().unwrap().is_empty());
    }

    #[test]
    fn list_all_returns_entries_sorted_by_name() {
        let s = store();
        s.update("gamma", "3").unwrap();
        s.update("alpha", "1").unwrap();
        s.update("beta", "2").unwrap();
        let all = s.list_all().unwrap();
        assert_eq!(all[0].export_name, "alpha");
        assert_eq!(all[2].export_name, "gamma");
    }

    // ─── Metrics tests ───────────────────────────────────────

    #[test]
    fn record_and_query_metrics() {
        let s = store();
        s.record_metric(
            "orders",
            "run_001",
            1200,
            50000,
            Some(142),
            "success",
            None,
            Some("safe"),
            Some("parquet"),
            Some("full"),
            1,
            4096,
            0,
            Some(true),
            Some(false),
        )
        .unwrap();
        s.record_metric(
            "orders",
            "run_002",
            300,
            0,
            Some(30),
            "failed",
            Some("timeout"),
            Some("safe"),
            Some("parquet"),
            Some("full"),
            0,
            0,
            2,
            None,
            None,
        )
        .unwrap();

        let metrics = s.get_metrics(Some("orders"), 10).unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].status, "failed");
        assert_eq!(metrics[0].run_id.as_deref(), Some("run_002"));
        assert_eq!(metrics[0].retries, 2);
        assert_eq!(metrics[1].total_rows, 50000);
        assert_eq!(metrics[1].run_id.as_deref(), Some("run_001"));
        assert_eq!(metrics[1].files_produced, 1);
        assert_eq!(metrics[1].bytes_written, 4096);
        assert_eq!(metrics[1].validated, Some(true));
        assert_eq!(metrics[1].schema_changed, Some(false));
    }

    #[test]
    fn query_metrics_all_exports() {
        let s = store();
        s.record_metric(
            "orders", "r1", 100, 1000, None, "success", None, None, None, None, 1, 500, 0, None,
            None,
        )
        .unwrap();
        s.record_metric(
            "users", "r2", 200, 2000, None, "success", None, None, None, None, 1, 800, 0, None,
            None,
        )
        .unwrap();

        let metrics = s.get_metrics(None, 10).unwrap();
        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn metrics_limit_works() {
        let s = store();
        for i in 0..10 {
            s.record_metric(
                "t",
                &format!("r{}", i),
                i * 100,
                i,
                None,
                "success",
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                None,
                None,
            )
            .unwrap();
        }
        let metrics = s.get_metrics(Some("t"), 3).unwrap();
        assert_eq!(metrics.len(), 3);
    }

    // ─── File manifest tests ─────────────────────────────────

    #[test]
    fn record_and_query_files() {
        let s = store();
        s.record_file(
            "run_001",
            "orders",
            "orders_20260329.parquet",
            50000,
            4096,
            "parquet",
            Some("zstd"),
        )
        .unwrap();
        s.record_file(
            "run_001",
            "orders",
            "orders_20260329_chunk1.parquet",
            25000,
            2048,
            "parquet",
            Some("zstd"),
        )
        .unwrap();
        s.record_file(
            "run_002",
            "users",
            "users_20260329.csv",
            1000,
            500,
            "csv",
            None,
        )
        .unwrap();

        let files = s.get_files(Some("orders"), 10).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].run_id, "run_001");
        assert_eq!(files[0].row_count, 25000);

        let all = s.get_files(None, 10).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn files_limit_works() {
        let s = store();
        for i in 0..10 {
            s.record_file(
                &format!("r{}", i),
                "t",
                &format!("f{}.parquet", i),
                i,
                i * 100,
                "parquet",
                None,
            )
            .unwrap();
        }
        let files = s.get_files(Some("t"), 3).unwrap();
        assert_eq!(files.len(), 3);
    }

    // ─── Schema tracking tests ───────────────────────────────

    #[test]
    fn first_schema_stored_no_change() {
        let s = store();
        let cols = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "name".into(),
                data_type: "Utf8".into(),
            },
        ];
        let change = s.detect_schema_change("orders", &cols).unwrap();
        assert!(change.is_none(), "first run should detect no change");
        assert!(s.get_stored_schema("orders").unwrap().is_some());
    }

    #[test]
    fn same_schema_no_change() {
        let s = store();
        let cols = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        s.detect_schema_change("t", &cols).unwrap();
        let change = s.detect_schema_change("t", &cols).unwrap();
        assert!(change.is_none());
    }

    #[test]
    fn added_column_detected() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "email".into(),
                data_type: "Utf8".into(),
            },
        ];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.added.len(), 1);
        assert!(change.added[0].contains("email"));
    }

    #[test]
    fn removed_column_detected() {
        let s = store();
        let v1 = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "Int64".into(),
            },
            SchemaColumn {
                name: "old_field".into(),
                data_type: "Utf8".into(),
            },
        ];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        }];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.removed, vec!["old_field"]);
    }

    #[test]
    fn type_change_detected() {
        let s = store();
        let v1 = vec![SchemaColumn {
            name: "price".into(),
            data_type: "Float64".into(),
        }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![SchemaColumn {
            name: "price".into(),
            data_type: "Utf8".into(),
        }];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.type_changed.len(), 1);
        assert_eq!(
            change.type_changed[0],
            ("price".into(), "Float64".into(), "Utf8".into())
        );
    }

    // ─── Chunk checkpoint (needs on-disk DB: separate connections share one file) ─

    fn store_on_disk() -> (tempfile::TempDir, StateStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = dir.path().join("rivet.yaml");
        std::fs::write(&cfg, "# test").expect("write cfg");
        let s = StateStore::open(cfg.to_str().unwrap()).expect("open store");
        (dir, s)
    }

    #[test]
    fn chunk_claim_complete_and_finalize() {
        let (_dir, s) = store_on_disk();
        s.create_chunk_run("run_a", "orders", "deadbeef", 2)
            .unwrap();
        s.insert_chunk_tasks("run_a", &[(1, 5), (6, 10)]).unwrap();

        let t0 = s.claim_next_chunk_task("run_a").unwrap().expect("claim 0");
        assert_eq!(t0.0, 0);
        assert_eq!(t0.1, "1");
        assert_eq!(t0.2, "5");

        s.complete_chunk_task("run_a", 0, 3, Some("part0.csv"))
            .unwrap();

        let t1 = s.claim_next_chunk_task("run_a").unwrap().expect("claim 1");
        assert_eq!(t1.0, 1);
        s.complete_chunk_task("run_a", 1, 2, Some("part1.csv"))
            .unwrap();

        assert_eq!(s.count_chunk_tasks_not_completed("run_a").unwrap(), 0);
        s.finalize_chunk_run_completed("run_a").unwrap();
    }

    #[test]
    fn chunk_fail_then_retry_until_max() {
        let (_dir, s) = store_on_disk();
        s.create_chunk_run("run_b", "orders", "ab", 2).unwrap();
        s.insert_chunk_tasks("run_b", &[(1, 2)]).unwrap();

        let t = s.claim_next_chunk_task("run_b").unwrap().unwrap();
        assert_eq!(t.0, 0);
        s.fail_chunk_task("run_b", 0, "boom").unwrap();

        let t2 = s.claim_next_chunk_task("run_b").unwrap().unwrap();
        assert_eq!(t2.0, 0);
        s.fail_chunk_task("run_b", 0, "again").unwrap();

        assert!(s.claim_next_chunk_task("run_b").unwrap().is_none());
        assert_eq!(s.count_chunk_tasks_not_completed("run_b").unwrap(), 1);
    }

    #[test]
    fn reset_chunk_checkpoint_clears_runs() {
        let (_dir, s) = store_on_disk();
        s.create_chunk_run("r1", "e", "h", 1).unwrap();
        s.insert_chunk_tasks("r1", &[(0, 1)]).unwrap();
        assert_eq!(s.reset_chunk_checkpoint("e").unwrap(), 1);
        assert!(s.find_in_progress_chunk_run("e").unwrap().is_none());
    }

    // ─── Schema versioning tests ────────────────────────────

    #[test]
    fn fresh_db_reaches_latest_version() {
        let s = store();
        let ver = get_current_version(&s.conn);
        assert_eq!(ver, SCHEMA_VERSION);
    }

    #[test]
    fn migration_is_idempotent() {
        let s = store();
        migrate(&s.conn);
        migrate(&s.conn);
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

        migrate(&conn);

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
