use crate::error::Result;
use crate::types::CursorState;

use super::{StateConn, StateStore, pg_sql};

/// Incremental cursor store — reads and writes `export_state`.
///
/// The cursor records the last extracted value so incremental runs can pick up
/// where the previous run left off.  Invariant I3 (Write Before Cursor) governs
/// the ordering of cursor updates relative to destination writes.
impl StateStore {
    pub fn get(&self, export_name: &str) -> Result<CursorState> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(
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
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                match c.query_opt(
                    "SELECT last_cursor_value, last_run_at FROM export_state WHERE export_name = $1",
                    &[&export_name],
                )? {
                    Some(row) => Ok(CursorState {
                        export_name: export_name.to_string(),
                        last_cursor_value: row.get(0),
                        last_run_at: row.get(1),
                    }),
                    None => Ok(CursorState {
                        export_name: export_name.to_string(),
                        last_cursor_value: None,
                        last_run_at: None,
                    }),
                }
            }
        }
    }

    pub fn update(&self, export_name: &str, cursor_value: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_state (export_name, last_cursor_value, last_run_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(export_name) DO UPDATE SET
                last_cursor_value = excluded.last_cursor_value,
                last_run_at = excluded.last_run_at";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(sql, rusqlite::params![export_name, cursor_value, now])?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(&pg_sql(sql), &[&export_name, &cursor_value, &now])?;
            }
        }
        Ok(())
    }

    /// Round-5 (keyset checkpoint-resume manifest completeness): persist the
    /// in-progress keyset run_id beside the resume cursor, so a crash+resume reuses
    /// it and reconstructs every committed page's manifest part from file_log. Set on
    /// the first checkpointed run, read on resume, cleared when the run finalizes.
    pub fn set_resume_run_id(&self, export_name: &str, run_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_state (export_name, resume_run_id, last_run_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(export_name) DO UPDATE SET resume_run_id = excluded.resume_run_id";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(sql, rusqlite::params![export_name, run_id, now])?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(&pg_sql(sql), &[&export_name, &run_id, &now])?;
            }
        }
        Ok(())
    }

    /// The persisted in-progress keyset run_id, or None when no run is in progress.
    pub fn get_resume_run_id(&self, export_name: &str) -> Result<Option<String>> {
        let sql = "SELECT resume_run_id FROM export_state WHERE export_name = ?1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
                let mut rows = stmt.query_map([export_name], |r| r.get::<_, Option<String>>(0))?;
                Ok(rows.next().transpose()?.flatten())
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(&pg_sql(sql), &[&export_name])?;
                Ok(rows.first().and_then(|r| r.get::<_, Option<String>>(0)))
            }
        }
    }

    /// Clear the in-progress run_id once a keyset run has finalized its manifest.
    pub fn clear_resume_run_id(&self, export_name: &str) -> Result<()> {
        let sql = "UPDATE export_state SET resume_run_id = NULL WHERE export_name = ?1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(sql, [export_name])?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(&pg_sql(sql), &[&export_name])?;
            }
        }
        Ok(())
    }

    /// Return an export to a "never ran" state.
    ///
    /// Clears the incremental cursor (`export_state`) **and** the committed /
    /// verified boundary (`export_progression`). Both must go: a surviving
    /// progression row would make `rivet state progression` report a stale
    /// committed boundary after `state show` is already empty.
    pub fn reset(&self, export_name: &str) -> Result<()> {
        let sql = "DELETE FROM export_state WHERE export_name = ?1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(sql, [export_name])?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(&pg_sql(sql), &[&export_name])?;
            }
        }
        self.delete_progression(export_name)?;
        Ok(())
    }

    pub fn list_all(&self) -> Result<Vec<CursorState>> {
        let sql = "SELECT export_name, last_cursor_value, last_run_at FROM export_state ORDER BY export_name";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
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
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(sql, &[])?;
                Ok(rows
                    .iter()
                    .map(|row| CursorState {
                        export_name: row.get(0),
                        last_cursor_value: row.get(1),
                        last_run_at: row.get(2),
                    })
                    .collect())
            }
        }
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

    // ─── Cursor round-trip / monotonicity (QA backlog Task 3.1) ─────────────
    //
    // ADR-0001 I3 makes monotonicity a pipeline responsibility, not a storage
    // one.  These tests pin the *value-preservation* contract on the state
    // side — the subset the pipeline relies on when reading the stored cursor
    // back on resume.

    /// Duplicate cursor values across runs are common when the cursor column
    /// is a low-precision timestamp with ties.  The store must return each
    /// written value verbatim.
    #[test]
    fn duplicate_cursor_values_are_stored_as_written() {
        let s = store();
        s.update("orders", "2024-06-01T00:00:00Z").unwrap();
        s.update("orders", "2024-06-01T00:00:00Z").unwrap();
        assert_eq!(
            s.get("orders").unwrap().last_cursor_value.as_deref(),
            Some("2024-06-01T00:00:00Z")
        );
    }

    /// Microsecond/nanosecond precision must not be rounded or truncated on
    /// round-trip — otherwise the pipeline's strict-greater-than boundary
    /// check would re-export rows on the microsecond edge.
    #[test]
    fn high_precision_timestamp_is_preserved_byte_for_byte() {
        let s = store();
        let ts = "2024-06-01T12:34:56.123456789+02:00";
        s.update("events", ts).unwrap();
        assert_eq!(
            s.get("events").unwrap().last_cursor_value.as_deref(),
            Some(ts)
        );
    }

    /// Cursor values can be arbitrary UTF-8: UUID v7, version tokens,
    /// Cyrillic names, multiline strings, the empty string.
    #[test]
    fn unicode_and_binary_like_cursor_values_round_trip() {
        let s = store();
        let values = [
            "2024-06-01",
            "018f1c0b-7a34-7b54-8e16-1c5a9b3f1c2d", // UUID v7
            "ελληνικά 🚀 cursor",
            "v\n\t with whitespace",
            "",
        ];
        for v in values {
            s.update("t", v).unwrap();
            assert_eq!(
                s.get("t").unwrap().last_cursor_value.as_deref(),
                Some(v),
                "cursor value {v:?} must round-trip exactly"
            );
        }
    }

    /// Resume-from-zero tooling depends on `reset` producing a state
    /// indistinguishable from "never ran": both cursor and last_run_at gone.
    #[test]
    fn reset_clears_cursor_state_completely() {
        let s = store();
        s.update("orders", "2024-06-01").unwrap();
        s.reset("orders").unwrap();
        let after = s.get("orders").unwrap();
        assert!(after.last_cursor_value.is_none());
        assert!(
            after.last_run_at.is_none(),
            "reset must clear last_run_at as well"
        );
    }

    /// #22 (0.9.x audit): `reset` left `export_progression` behind, so
    /// `rivet state progression` reported a stale committed boundary after
    /// `state show` was already empty. Reset must clear progression too.
    #[test]
    fn reset_clears_committed_progression() {
        let s = store();
        s.update("orders", "100").unwrap();
        s.record_committed_incremental("orders", "100", "run-1")
            .unwrap();
        // Other exports' progression must survive — reset is per-export.
        s.record_committed_incremental("users", "9", "run-u")
            .unwrap();

        s.reset("orders").unwrap();

        let p = s.get_progression("orders").unwrap();
        assert!(
            p.committed.is_none() && p.verified.is_none(),
            "reset must clear the export's committed/verified boundary"
        );
        assert!(
            s.get_progression("users").unwrap().committed.is_some(),
            "reset must not touch another export's progression"
        );
    }
}
