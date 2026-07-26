use crate::error::Result;
use crate::journal::RunJournal;

use super::StateStore;

impl StateStore {
    /// Persist a completed `RunJournal` to the state DB.
    ///
    /// Called once per export run, after `RunCompleted` has been recorded.
    /// Overwrites any existing row for the same `run_id` (idempotent on retry).
    pub fn store_journal(&self, journal: &RunJournal) -> Result<()> {
        let json = serde_json::to_string(journal)?;
        let now = chrono::Utc::now().to_rfc3339();
        self.execute(
            "INSERT INTO run_journal (run_id, export_name, finished_at, journal_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (run_id) DO UPDATE SET
                 export_name  = excluded.export_name,
                 finished_at  = excluded.finished_at,
                 journal_json = excluded.journal_json",
            &[
                journal.run_id.as_str().into(),
                journal.export_name.as_str().into(),
                now.into(),
                json.into(),
            ],
        )?;
        Ok(())
    }

    /// Load a journal by `run_id`.  Returns `None` if the run is not found.
    #[allow(dead_code)]
    pub fn load_journal(&self, run_id: &str) -> Result<Option<RunJournal>> {
        let json = self.query_opt(
            "SELECT journal_json FROM run_journal WHERE run_id = ?1",
            &[run_id.into()],
            |r| r.text(0),
        )?;
        Ok(match json {
            Some(s) => Some(serde_json::from_str(&s)?),
            None => None,
        })
    }

    /// Return the most recent `limit` journal entries for an export, newest first.
    #[allow(dead_code)]
    pub fn recent_journals(&self, export_name: &str, limit: usize) -> Result<Vec<RunJournal>> {
        let jsons = self.query(
            "SELECT journal_json FROM run_journal
             WHERE export_name = ?1
             ORDER BY finished_at DESC
             LIMIT ?2",
            &[export_name.into(), (limit as i64).into()],
            |r| r.text(0),
        )?;
        jsons
            .iter()
            .map(|j| serde_json::from_str::<RunJournal>(j).map_err(Into::into))
            .collect()
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
