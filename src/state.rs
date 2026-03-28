use rusqlite::Connection;

use crate::error::Result;
use crate::types::CursorState;

const STATE_DB_NAME: &str = ".rivet_state.db";

const INIT_SQL: &str = "
    CREATE TABLE IF NOT EXISTS export_state (
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
        schema_changed INTEGER
    );
    CREATE TABLE IF NOT EXISTS export_schema (
        export_name TEXT PRIMARY KEY,
        columns_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
";

fn migrate(conn: &Connection) {
    let new_cols = [
        "files_produced INTEGER DEFAULT 0",
        "bytes_written INTEGER DEFAULT 0",
        "retries INTEGER DEFAULT 0",
        "validated INTEGER",
        "schema_changed INTEGER",
    ];
    for col_def in &new_cols {
        let col_name = col_def.split_whitespace().next().unwrap();
        let sql = format!("ALTER TABLE export_metrics ADD COLUMN {}", col_def);
        match conn.execute(&sql, []) {
            Ok(_) => log::debug!("migration: added column {} to export_metrics", col_name),
            Err(_) => {} // column already exists
        }
    }
}

pub struct StateStore {
    conn: Connection,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ExportMetric {
    pub export_name: String,
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
        let conn = Connection::open(db_path)?;
        conn.execute_batch(INIT_SQL)?;
        migrate(&conn);
        Ok(Self { conn })
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
        self.conn.execute("DELETE FROM export_state WHERE export_name = ?1", [export_name])?;
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
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    // ─── Metrics ─────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub fn record_metric(
        &self,
        export_name: &str,
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
            "INSERT INTO export_metrics (export_name, run_at, duration_ms, total_rows, peak_rss_mb,
             status, error_message, tuning_profile, format, mode,
             files_produced, bytes_written, retries, validated, schema_changed)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            rusqlite::params![
                export_name, now, duration_ms, total_rows, peak_rss_mb,
                status, error_message, tuning_profile, format, mode,
                files_produced, bytes_written, retries, validated, schema_changed
            ],
        )?;
        Ok(())
    }

    pub fn get_metrics(&self, export_name: Option<&str>, limit: usize) -> Result<Vec<ExportMetric>> {
        let cols = "export_name, run_at, duration_ms, total_rows, peak_rss_mb,
                    status, error_message, tuning_profile, format, mode,
                    files_produced, bytes_written, retries, validated, schema_changed";
        let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(name) = export_name {
            (
                format!("SELECT {} FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT {}", cols, limit),
                vec![Box::new(name.to_string())],
            )
        } else {
            (
                format!("SELECT {} FROM export_metrics ORDER BY id DESC LIMIT {}", cols, limit),
                vec![],
            )
        };

        let mut stmt = self.conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok(ExportMetric {
                export_name: row.get(0)?,
                run_at: row.get(1)?,
                duration_ms: row.get(2)?,
                total_rows: row.get(3)?,
                peak_rss_mb: row.get(4)?,
                status: row.get(5)?,
                error_message: row.get(6)?,
                tuning_profile: row.get(7)?,
                format: row.get(8)?,
                mode: row.get(9)?,
                files_produced: row.get::<_, Option<i64>>(10)?.unwrap_or(0),
                bytes_written: row.get::<_, Option<i64>>(11)?.unwrap_or(0),
                retries: row.get::<_, Option<i64>>(12)?.unwrap_or(0),
                validated: row.get(13)?,
                schema_changed: row.get(14)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    // ─── Schema Tracking ─────────────────────────────────────

    pub fn get_stored_schema(&self, export_name: &str) -> Result<Option<Vec<SchemaColumn>>> {
        let mut stmt = self.conn.prepare(
            "SELECT columns_json FROM export_schema WHERE export_name = ?1",
        )?;
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

        let stored_map: std::collections::HashMap<&str, &str> =
            stored.iter().map(|c| (c.name.as_str(), c.data_type.as_str())).collect();
        let current_map: std::collections::HashMap<&str, &str> =
            current.iter().map(|c| (c.name.as_str(), c.data_type.as_str())).collect();

        let added: Vec<String> = current.iter()
            .filter(|c| !stored_map.contains_key(c.name.as_str()))
            .map(|c| format!("{} ({})", c.name, c.data_type))
            .collect();

        let removed: Vec<String> = stored.iter()
            .filter(|c| !current_map.contains_key(c.name.as_str()))
            .map(|c| c.name.clone())
            .collect();

        let type_changed: Vec<(String, String, String)> = current.iter()
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

        let change = SchemaChange { added, removed, type_changed };

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
        conn.execute_batch(INIT_SQL)?;
        migrate(&conn);
        Ok(Self { conn })
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
        assert_eq!(s.get("orders").unwrap().last_cursor_value.as_deref(), Some("2024-06-01"));
    }

    #[test]
    fn update_overwrites_previous_cursor() {
        let s = store();
        s.update("orders", "100").unwrap();
        s.update("orders", "200").unwrap();
        assert_eq!(s.get("orders").unwrap().last_cursor_value.as_deref(), Some("200"));
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
        s.record_metric("orders", 1200, 50000, Some(142), "success", None, Some("safe"), Some("parquet"), Some("full"), 1, 4096, 0, Some(true), Some(false)).unwrap();
        s.record_metric("orders", 300, 0, Some(30), "failed", Some("timeout"), Some("safe"), Some("parquet"), Some("full"), 0, 0, 2, None, None).unwrap();

        let metrics = s.get_metrics(Some("orders"), 10).unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].status, "failed"); // most recent first
        assert_eq!(metrics[0].retries, 2);
        assert_eq!(metrics[1].total_rows, 50000);
        assert_eq!(metrics[1].files_produced, 1);
        assert_eq!(metrics[1].bytes_written, 4096);
        assert_eq!(metrics[1].validated, Some(true));
        assert_eq!(metrics[1].schema_changed, Some(false));
    }

    #[test]
    fn query_metrics_all_exports() {
        let s = store();
        s.record_metric("orders", 100, 1000, None, "success", None, None, None, None, 1, 500, 0, None, None).unwrap();
        s.record_metric("users", 200, 2000, None, "success", None, None, None, None, 1, 800, 0, None, None).unwrap();

        let metrics = s.get_metrics(None, 10).unwrap();
        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn metrics_limit_works() {
        let s = store();
        for i in 0..10 {
            s.record_metric("t", i * 100, i, None, "success", None, None, None, None, 0, 0, 0, None, None).unwrap();
        }
        let metrics = s.get_metrics(Some("t"), 3).unwrap();
        assert_eq!(metrics.len(), 3);
    }

    // ─── Schema tracking tests ───────────────────────────────

    #[test]
    fn first_schema_stored_no_change() {
        let s = store();
        let cols = vec![
            SchemaColumn { name: "id".into(), data_type: "Int64".into() },
            SchemaColumn { name: "name".into(), data_type: "Utf8".into() },
        ];
        let change = s.detect_schema_change("orders", &cols).unwrap();
        assert!(change.is_none(), "first run should detect no change");
        assert!(s.get_stored_schema("orders").unwrap().is_some());
    }

    #[test]
    fn same_schema_no_change() {
        let s = store();
        let cols = vec![
            SchemaColumn { name: "id".into(), data_type: "Int64".into() },
        ];
        s.detect_schema_change("t", &cols).unwrap();
        let change = s.detect_schema_change("t", &cols).unwrap();
        assert!(change.is_none());
    }

    #[test]
    fn added_column_detected() {
        let s = store();
        let v1 = vec![SchemaColumn { name: "id".into(), data_type: "Int64".into() }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![
            SchemaColumn { name: "id".into(), data_type: "Int64".into() },
            SchemaColumn { name: "email".into(), data_type: "Utf8".into() },
        ];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.added.len(), 1);
        assert!(change.added[0].contains("email"));
    }

    #[test]
    fn removed_column_detected() {
        let s = store();
        let v1 = vec![
            SchemaColumn { name: "id".into(), data_type: "Int64".into() },
            SchemaColumn { name: "old_field".into(), data_type: "Utf8".into() },
        ];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![SchemaColumn { name: "id".into(), data_type: "Int64".into() }];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.removed, vec!["old_field"]);
    }

    #[test]
    fn type_change_detected() {
        let s = store();
        let v1 = vec![SchemaColumn { name: "price".into(), data_type: "Float64".into() }];
        s.detect_schema_change("t", &v1).unwrap();

        let v2 = vec![SchemaColumn { name: "price".into(), data_type: "Utf8".into() }];
        let change = s.detect_schema_change("t", &v2).unwrap().unwrap();
        assert_eq!(change.type_changed.len(), 1);
        assert_eq!(change.type_changed[0], ("price".into(), "Float64".into(), "Utf8".into()));
    }
}
