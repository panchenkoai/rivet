use crate::error::Result;
use crate::types::CursorState;

use super::StateStore;

/// Incremental cursor store — reads and writes `export_state`.
///
/// The cursor records the last extracted value so incremental runs can pick up
/// where the previous run left off.  Invariant I3 (Write Before Cursor) governs
/// the ordering of cursor updates relative to destination writes.
impl StateStore {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

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
}
