use rusqlite::Connection;

use crate::error::Result;
use crate::types::CursorState;

const STATE_DB_NAME: &str = ".rivet_state.db";

pub struct StateStore {
    conn: Connection,
}

impl StateStore {
    pub fn open(config_path: &str) -> Result<Self> {
        let config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let db_path = config_dir.join(STATE_DB_NAME);
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS export_state (
                export_name TEXT PRIMARY KEY,
                last_cursor_value TEXT,
                last_run_at TEXT
            );",
        )?;
        Ok(Self { conn })
    }

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
        let mut states = Vec::new();
        for row in rows {
            states.push(row?);
        }
        Ok(states)
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS export_state (
                export_name TEXT PRIMARY KEY,
                last_cursor_value TEXT,
                last_run_at TEXT
            );",
        )?;
        Ok(Self { conn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store should open")
    }

    #[test]
    fn get_unknown_returns_empty_state() -> anyhow::Result<()> {
        let store = store();
        let state = store.get("nonexistent")?;
        assert_eq!(state.export_name, "nonexistent");
        assert!(state.last_cursor_value.is_none(), "new export should have no cursor");
        assert!(state.last_run_at.is_none(), "new export should have no last_run_at");
        Ok(())
    }

    #[test]
    fn update_then_get_returns_stored_cursor() -> anyhow::Result<()> {
        let store = store();
        store.update("orders", "2024-06-01T00:00:00")?;
        let state = store.get("orders")?;
        assert_eq!(state.export_name, "orders");
        assert_eq!(state.last_cursor_value.as_deref(), Some("2024-06-01T00:00:00"));
        assert!(state.last_run_at.is_some(), "last_run_at should be set after update");
        Ok(())
    }

    #[test]
    fn update_overwrites_previous_cursor() -> anyhow::Result<()> {
        let store = store();
        store.update("orders", "100")?;
        store.update("orders", "200")?;
        let state = store.get("orders")?;
        assert_eq!(state.last_cursor_value.as_deref(), Some("200"));
        Ok(())
    }

    #[test]
    fn reset_clears_cursor_state() -> anyhow::Result<()> {
        let store = store();
        store.update("orders", "100")?;
        store.reset("orders")?;
        let state = store.get("orders")?;
        assert!(state.last_cursor_value.is_none(), "cursor should be cleared after reset");
        Ok(())
    }

    #[test]
    fn list_all_on_empty_store_returns_empty() -> anyhow::Result<()> {
        let store = store();
        let all = store.list_all()?;
        assert!(all.is_empty(), "fresh store should have no entries");
        Ok(())
    }

    #[test]
    fn list_all_returns_entries_sorted_by_name() -> anyhow::Result<()> {
        let store = store();
        store.update("alpha", "1")?;
        store.update("beta", "2")?;
        store.update("gamma", "3")?;
        let all = store.list_all()?;
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].export_name, "alpha");
        assert_eq!(all[1].export_name, "beta");
        assert_eq!(all[2].export_name, "gamma");
        Ok(())
    }
}
