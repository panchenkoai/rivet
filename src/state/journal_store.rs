use crate::error::Result;
use crate::journal::RunJournal;

use super::{StateConn, StateStore};

impl StateStore {
    /// Persist a completed `RunJournal` to the state DB.
    ///
    /// Called once per export run, after `RunCompleted` has been recorded.
    /// Overwrites any existing row for the same `run_id` (idempotent on retry).
    pub fn store_journal(&self, journal: &RunJournal) -> Result<()> {
        let json = serde_json::to_string(journal)?;
        let now = chrono::Utc::now().to_rfc3339();
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    "INSERT OR REPLACE INTO run_journal (run_id, export_name, finished_at, journal_json)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![journal.run_id, journal.export_name, now, json],
                )?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
                    "INSERT INTO run_journal (run_id, export_name, finished_at, journal_json)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (run_id) DO UPDATE SET
                         export_name  = excluded.export_name,
                         finished_at  = excluded.finished_at,
                         journal_json = excluded.journal_json",
                    &[&journal.run_id, &journal.export_name, &now, &json],
                )?;
            }
        }
        Ok(())
    }

    /// Load a journal by `run_id`.  Returns `None` if the run is not found.
    #[allow(dead_code)]
    pub fn load_journal(&self, run_id: &str) -> Result<Option<RunJournal>> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                let result = c.query_row(
                    "SELECT journal_json FROM run_journal WHERE run_id = ?1",
                    rusqlite::params![run_id],
                    |row| row.get::<_, String>(0),
                );
                match result {
                    Ok(json) => Ok(Some(serde_json::from_str(&json)?)),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                match c.query_opt(
                    "SELECT journal_json FROM run_journal WHERE run_id = $1",
                    &[&run_id],
                )? {
                    Some(row) => {
                        let json: String = row.get(0);
                        Ok(Some(serde_json::from_str(&json)?))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    /// Return the most recent `limit` journal entries for an export, newest first.
    #[allow(dead_code)]
    pub fn recent_journals(&self, export_name: &str, limit: usize) -> Result<Vec<RunJournal>> {
        let sql = "SELECT journal_json FROM run_journal
             WHERE export_name = ?1
             ORDER BY finished_at DESC
             LIMIT ?2";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
                let rows = stmt.query_map(rusqlite::params![export_name, limit as i64], |row| {
                    row.get::<_, String>(0)
                })?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(serde_json::from_str::<RunJournal>(&row?)?);
                }
                Ok(out)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(
                    &format!(
                        "SELECT journal_json FROM run_journal
                         WHERE export_name = $1
                         ORDER BY finished_at DESC
                         LIMIT {}",
                        limit
                    ),
                    &[&export_name],
                )?;
                let mut out = Vec::new();
                for row in rows {
                    let json: String = row.get(0);
                    out.push(serde_json::from_str::<RunJournal>(&json)?);
                }
                Ok(out)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{RunEvent, RunJournal};

    fn make_journal(run_id: &str, export: &str) -> RunJournal {
        let mut j = RunJournal::new(run_id, export);
        j.record(RunEvent::FileWritten {
            file_name: "part0.parquet".into(),
            rows: 1_000,
            bytes: 65_536,
            part_index: 0,
        });
        j.record(RunEvent::RunCompleted {
            status: "success".into(),
            error_message: None,
            duration_ms: 420,
        });
        j
    }

    #[test]
    fn store_and_load_roundtrip() {
        let store = StateStore::open_in_memory().unwrap();
        let j = make_journal("run_abc_001", "orders");
        store.store_journal(&j).unwrap();

        let loaded = store.load_journal("run_abc_001").unwrap().unwrap();
        assert_eq!(loaded.run_id, "run_abc_001");
        assert_eq!(loaded.export_name, "orders");
        assert_eq!(loaded.entries.len(), 2);
        assert!(matches!(
            loaded.entries[0].event,
            RunEvent::FileWritten { rows: 1_000, .. }
        ));
        assert!(matches!(
            loaded.entries[1].event,
            RunEvent::RunCompleted { ref status, .. } if status == "success"
        ));
    }

    #[test]
    fn load_missing_returns_none() {
        let store = StateStore::open_in_memory().unwrap();
        assert!(store.load_journal("nonexistent").unwrap().is_none());
    }

    #[test]
    fn store_is_idempotent_on_same_run_id() {
        let store = StateStore::open_in_memory().unwrap();
        let j = make_journal("run_idem", "payments");
        store.store_journal(&j).unwrap();
        store.store_journal(&j).unwrap();

        let loaded = store.load_journal("run_idem").unwrap().unwrap();
        assert_eq!(loaded.entries.len(), 2);
    }

    #[test]
    fn recent_journals_returns_newest_first() {
        let store = StateStore::open_in_memory().unwrap();
        for i in 1..=3_u32 {
            std::thread::sleep(std::time::Duration::from_millis(2));
            store
                .store_journal(&make_journal(&format!("run_{i:03}"), "events"))
                .unwrap();
        }

        let recent = store.recent_journals("events", 2).unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].run_id, "run_003");
        assert_eq!(recent[1].run_id, "run_002");
    }
}
