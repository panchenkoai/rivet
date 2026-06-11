use rusqlite::TransactionBehavior;

use crate::error::Result;

use super::{StateConn, StateRef, StateStore, open_connection, pg_sql};

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
/// Some methods accept an explicit `state_ref` and open a fresh connection.
/// This is required for parallel workers that cannot share one `Connection`.
impl StateStore {
    /// Distinct `export_name` values that currently have at least one
    /// `chunk_run` row with `status = 'in_progress'` (interrupted run — resume or reset).
    pub fn list_export_names_with_in_progress_chunk_runs(&self) -> Result<Vec<String>> {
        let sql = "SELECT DISTINCT export_name FROM chunk_run WHERE status = 'in_progress' ORDER BY export_name ASC";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(&pg_sql(sql), &[])?;
                Ok(rows.iter().map(|row| row.get(0)).collect())
            }
        }
    }

    /// Latest `in_progress` chunk run for this export, if any.
    pub fn find_in_progress_chunk_run(
        &self,
        export_name: &str,
    ) -> Result<Option<(String, String)>> {
        let sql = "SELECT run_id, plan_hash FROM chunk_run
             WHERE export_name = ?1 AND status = 'in_progress'
             ORDER BY created_at DESC LIMIT 1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
                let mut rows =
                    stmt.query_map([export_name], |row| Ok((row.get(0)?, row.get(1)?)))?;
                Ok(rows.next().transpose()?)
            }
            StateConn::Postgres(client) => {
                // Single borrow for the duration of this call; safe because all Postgres
                // operations in StateStore are sequential (no re-entrant borrows).
                let mut c = client.borrow_mut();
                let rows = c.query(&pg_sql(sql), &[&export_name])?;
                Ok(rows.first().map(|row| (row.get(0), row.get(1))))
            }
        }
    }

    pub fn create_chunk_run(
        &self,
        run_id: &str,
        export_name: &str,
        plan_hash: &str,
        max_chunk_attempts: u32,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO chunk_run (run_id, export_name, plan_hash, status, max_chunk_attempts, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'in_progress', ?4, ?5, ?5)";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    sql,
                    rusqlite::params![
                        run_id,
                        export_name,
                        plan_hash,
                        max_chunk_attempts as i64,
                        now
                    ],
                )?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
                    &pg_sql(sql),
                    &[
                        &run_id,
                        &export_name,
                        &plan_hash,
                        &(max_chunk_attempts as i64),
                        &now,
                    ],
                )?;
            }
        }
        Ok(())
    }

    pub fn insert_chunk_tasks(&self, run_id: &str, ranges: &[(i64, i64)]) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now().to_rfc3339();
        match &self.conn {
            StateConn::Sqlite(c) => {
                let tx = c.unchecked_transaction()?;
                {
                    let mut stmt = tx.prepare(
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
                }
                tx.commit()?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let mut tx = c
                    .transaction()
                    .map_err(|e| anyhow::anyhow!("state(pg): begin transaction: {:#}", e))?;
                for (i, (start, end)) in ranges.iter().enumerate() {
                    tx.execute(
                        "INSERT INTO chunk_task (run_id, chunk_index, start_key, end_key, status, attempts, updated_at)
                         VALUES ($1, $2, $3, $4, 'pending', 0, $5)",
                        &[
                            &run_id,
                            &(i as i64),
                            &start.to_string(),
                            &end.to_string(),
                            &now,
                        ],
                    )
                    .map_err(|e| anyhow::anyhow!("state(pg): insert chunk_task: {:#}", e))?;
                }
                tx.commit()
                    .map_err(|e| anyhow::anyhow!("state(pg): commit: {:#}", e))?;
            }
        }
        Ok(())
    }

    /// Mark tasks left `running` after a crash as `pending` so they can be retried.
    pub fn reset_stale_running_chunk_tasks(&self, run_id: &str) -> Result<usize> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "UPDATE chunk_task SET status = 'pending', updated_at = ?1
             WHERE run_id = ?2 AND status = 'running'";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let n = c.execute(sql, rusqlite::params![now, run_id])?;
                Ok(n)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let n = c.execute(&pg_sql(sql), &[&now, &run_id])?;
                Ok(n as usize)
            }
        }
    }

    /// Atomically claim the next pending or retryable failed chunk.
    pub fn claim_next_chunk_task(&self, run_id: &str) -> Result<Option<(i64, String, String)>> {
        Self::claim_next_chunk_task_at_ref(&self.state_ref, run_id)
    }

    fn claim_next_chunk_in_sqlite_tx(
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

    /// Claim next chunk using a fresh connection identified by `state_ref`.
    /// Used by parallel workers that cannot share a single connection.
    pub fn claim_next_chunk_task_at_ref(
        state_ref: &StateRef,
        run_id: &str,
    ) -> Result<Option<(i64, String, String)>> {
        match state_ref {
            StateRef::Sqlite(db_path) => {
                let mut conn = open_connection(db_path)?;
                let now = chrono::Utc::now().to_rfc3339();
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let res = Self::claim_next_chunk_in_sqlite_tx(&tx, &now, run_id)?;
                tx.commit()?;
                Ok(res)
            }
            StateRef::Postgres(url) => {
                let mut client = super::connect_pg(url)?;
                let now = chrono::Utc::now().to_rfc3339();
                // FOR UPDATE SKIP LOCKED ensures concurrent workers each get a distinct task.
                let rows = client
                    .query(
                        "UPDATE chunk_task
                         SET status = 'running', attempts = attempts + 1, updated_at = $1
                         WHERE id = (
                             SELECT ct.id FROM chunk_task ct
                             INNER JOIN chunk_run cr ON cr.run_id = ct.run_id
                             WHERE ct.run_id = $2
                               AND cr.status = 'in_progress'
                               AND (
                                 ct.status = 'pending'
                                 OR (ct.status = 'failed' AND ct.attempts < cr.max_chunk_attempts)
                               )
                             ORDER BY ct.chunk_index ASC
                             LIMIT 1
                             FOR UPDATE SKIP LOCKED
                         )
                         RETURNING chunk_index, start_key, end_key",
                        &[&now, &run_id],
                    )
                    .map_err(|e| anyhow::anyhow!("state(pg): claim chunk: {:#}", e))?;
                Ok(rows.first().map(|row| (row.get(0), row.get(1), row.get(2))))
            }
        }
    }

    pub fn complete_chunk_task(
        &self,
        run_id: &str,
        chunk_index: i64,
        rows_written: i64,
        file_name: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "UPDATE chunk_task
             SET status = 'completed', rows_written = ?1, file_name = ?2, last_error = NULL, updated_at = ?3
             WHERE run_id = ?4 AND chunk_index = ?5";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    sql,
                    rusqlite::params![rows_written, file_name, now, run_id, chunk_index],
                )?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
                    &pg_sql(sql),
                    &[&rows_written, &file_name, &now, &run_id, &chunk_index],
                )?;
            }
        }
        Ok(())
    }

    pub fn fail_chunk_task(&self, run_id: &str, chunk_index: i64, err: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "UPDATE chunk_task SET status = 'failed', last_error = ?1, updated_at = ?2
             WHERE run_id = ?3 AND chunk_index = ?4";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(sql, rusqlite::params![err, now, run_id, chunk_index])?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(&pg_sql(sql), &[&err, &now, &run_id, &chunk_index])?;
            }
        }
        Ok(())
    }

    pub fn fail_chunk_task_at_ref(
        state_ref: &StateRef,
        run_id: &str,
        chunk_index: i64,
        err: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "UPDATE chunk_task SET status = 'failed', last_error = ?1, updated_at = ?2
             WHERE run_id = ?3 AND chunk_index = ?4";
        match state_ref {
            StateRef::Sqlite(db_path) => {
                let conn = open_connection(db_path)?;
                conn.execute(sql, rusqlite::params![err, now, run_id, chunk_index])?;
            }
            StateRef::Postgres(url) => {
                let mut client = super::connect_pg(url)?;
                client.execute(&pg_sql(sql), &[&err, &now, &run_id, &chunk_index])?;
            }
        }
        Ok(())
    }

    pub fn complete_chunk_task_at_ref(
        state_ref: &StateRef,
        run_id: &str,
        chunk_index: i64,
        rows_written: i64,
        file_name: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "UPDATE chunk_task
             SET status = 'completed', rows_written = ?1, file_name = ?2, last_error = NULL, updated_at = ?3
             WHERE run_id = ?4 AND chunk_index = ?5";
        match state_ref {
            StateRef::Sqlite(db_path) => {
                let conn = open_connection(db_path)?;
                conn.execute(
                    sql,
                    rusqlite::params![rows_written, file_name, now, run_id, chunk_index],
                )?;
            }
            StateRef::Postgres(url) => {
                let mut client = super::connect_pg(url)?;
                client.execute(
                    &pg_sql(sql),
                    &[&rows_written, &file_name, &now, &run_id, &chunk_index],
                )?;
            }
        }
        Ok(())
    }

    pub fn count_chunk_tasks_total(&self, run_id: &str) -> Result<usize> {
        let sql = "SELECT COUNT(*) FROM chunk_task WHERE run_id = ?1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let n: i64 = c.query_row(sql, [run_id], |row| row.get(0))?;
                Ok(n as usize)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let row = c.query_one(&pg_sql(sql), &[&run_id])?;
                let n: i64 = row.get(0);
                Ok(n as usize)
            }
        }
    }

    pub fn count_chunk_tasks_not_completed(&self, run_id: &str) -> Result<i64> {
        let sql = "SELECT COUNT(*) FROM chunk_task WHERE run_id = ?1 AND status != 'completed'";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let n: i64 = c.query_row(sql, [run_id], |row| row.get(0))?;
                Ok(n)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let row = c.query_one(&pg_sql(sql), &[&run_id])?;
                Ok(row.get(0))
            }
        }
    }

    pub fn finalize_chunk_run_completed(&self, run_id: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "UPDATE chunk_run SET status = 'completed', updated_at = ?1 WHERE run_id = ?2";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(sql, rusqlite::params![now, run_id])?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(&pg_sql(sql), &[&now, &run_id])?;
            }
        }
        Ok(())
    }

    /// Reset a single completed chunk task back to `pending` so the next
    /// `--resume` run re-exports it.
    ///
    /// Used by ADR-0012 M8 reconciliation: when the destination's manifest
    /// says a part was committed but the actual object is missing or has
    /// drifted (size mismatch), the chunk_task that produced it must run
    /// again.  Without this reset, `claim_next_chunk_task` would skip the
    /// completed row and the destination would stay broken across resumes.
    ///
    /// Distinct from `reset_chunk_checkpoint(export_name)` (which wipes
    /// every run+task for an export — the operator-facing "abandon resume"
    /// nuke) and from `reset_stale_running_chunk_tasks` (which only
    /// rescues tasks left in 'running' after a crash).  This one is
    /// surgical: a single (run_id, chunk_index) goes from completed
    /// back to pending, attempts reset to 0, file_name cleared, and
    /// last_error annotated with the M8 reason so the journal/audit
    /// trail records why.
    ///
    /// Returns the number of rows updated (0 or 1).  Idempotent —
    /// calling it on a non-completed task is a no-op.
    pub fn reset_chunk_task_for_re_export(
        &self,
        run_id: &str,
        chunk_index: i64,
        reason: &str,
    ) -> Result<usize> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "UPDATE chunk_task
             SET status = 'pending',
                 attempts = 0,
                 file_name = NULL,
                 rows_written = NULL,
                 last_error = ?1,
                 updated_at = ?2
             WHERE run_id = ?3
               AND chunk_index = ?4
               AND status = 'completed'";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let n = c.execute(sql, rusqlite::params![reason, now, run_id, chunk_index])?;
                Ok(n)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let n = c.execute(&pg_sql(sql), &[&reason, &now, &run_id, &chunk_index])?;
                Ok(n as usize)
            }
        }
    }

    /// Remove all chunk runs and tasks for an export (abandon resume).
    pub fn reset_chunk_checkpoint(&self, export_name: &str) -> Result<usize> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                let run_ids: Vec<String> = {
                    let mut stmt =
                        c.prepare("SELECT run_id FROM chunk_run WHERE export_name = ?1")?;
                    let rows = stmt.query_map([export_name], |row| row.get(0))?;
                    rows.collect::<std::result::Result<Vec<_>, _>>()?
                };
                for rid in &run_ids {
                    let _ = c.execute("DELETE FROM chunk_task WHERE run_id = ?1", [rid]);
                }
                let deleted = c.execute(
                    "DELETE FROM chunk_run WHERE export_name = ?1",
                    [export_name],
                )?;
                Ok(deleted)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(
                    "SELECT run_id FROM chunk_run WHERE export_name = $1",
                    &[&export_name],
                )?;
                let run_ids: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
                for rid in &run_ids {
                    let _ = c.execute("DELETE FROM chunk_task WHERE run_id = $1", &[rid]);
                }
                let deleted = c.execute(
                    "DELETE FROM chunk_run WHERE export_name = $1",
                    &[&export_name],
                )?;
                Ok(deleted as usize)
            }
        }
    }

    /// Latest chunk_run row for an export (any status), for `rivet state chunks`.
    pub fn get_latest_chunk_run(
        &self,
        export_name: &str,
    ) -> Result<Option<(String, String, String, String)>> {
        let sql = "SELECT run_id, plan_hash, status, updated_at FROM chunk_run
             WHERE export_name = ?1 ORDER BY updated_at DESC LIMIT 1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
                let mut rows = stmt.query_map([export_name], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })?;
                Ok(rows.next().transpose()?)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(&pg_sql(sql), &[&export_name])?;
                Ok(rows
                    .first()
                    .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3))))
            }
        }
    }

    pub fn list_chunk_tasks_for_run(&self, run_id: &str) -> Result<Vec<ChunkTaskInfo>> {
        let sql = "SELECT chunk_index, start_key, end_key, status, attempts, last_error, rows_written, file_name
             FROM chunk_task WHERE run_id = ?1 ORDER BY chunk_index ASC";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
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
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(&pg_sql(sql), &[&run_id])?;
                Ok(rows
                    .iter()
                    .map(|row| ChunkTaskInfo {
                        chunk_index: row.get(0),
                        start_key: row.get(1),
                        end_key: row.get(2),
                        status: row.get(3),
                        attempts: row.get(4),
                        last_error: row.get(5),
                        rows_written: row.get(6),
                        file_name: row.get(7),
                    })
                    .collect())
            }
        }
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

    #[test]
    fn list_in_progress_exports_orders_and_deduplicates() {
        let (_dir, s) = store_on_disk();
        assert!(
            s.list_export_names_with_in_progress_chunk_runs()
                .unwrap()
                .is_empty()
        );
        s.create_chunk_run("r_old", "zebra", "h", 1).unwrap();
        s.finalize_chunk_run_completed("r_old").unwrap();
        s.create_chunk_run("r_a", "alpha", "h1", 1).unwrap();
        s.create_chunk_run("r_b", "beta", "h2", 1).unwrap();
        assert_eq!(
            s.list_export_names_with_in_progress_chunk_runs().unwrap(),
            vec!["alpha".to_string(), "beta".to_string()]
        );
    }
}
