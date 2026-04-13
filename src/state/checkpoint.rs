use rusqlite::{Connection, TransactionBehavior};

use crate::error::Result;

use super::StateStore;

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

/// Chunk checkpoint store — reads and writes `chunk_run` and `chunk_task`.
///
/// Governs Invariant I5 (Chunk Task Acyclicity): transitions are strictly
/// forward (`pending → running → completed | failed`).  Completed tasks are
/// never re-claimed.  Failed tasks return to `running` only while
/// `attempts < max_chunk_attempts`.
///
/// Some methods accept an explicit `db_path` and open a fresh connection.
/// This is required for parallel workers that cannot share one `Connection`.
impl StateStore {
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

    /// Mark tasks left `running` after a crash as `pending` so they can be retried.
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

    pub fn count_chunk_tasks_total(&self, run_id: &str) -> Result<usize> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM chunk_task WHERE run_id = ?1",
            [run_id],
            |row| row.get(0),
        )?;
        Ok(n as usize)
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
