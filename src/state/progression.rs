//! Committed / verified export progression (Epic G — ADR-0008).

use chrono::{DateTime, Utc};

use super::{StateConn, StateStore, pg_sql};
use crate::error::Result;

/// One export's progression record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportProgression {
    pub export_name: String,
    pub committed: Option<Boundary>,
    pub verified: Option<Boundary>,
}

/// A single boundary snapshot (committed or verified).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Boundary {
    pub strategy: String,
    pub run_id: Option<String>,
    pub cursor: Option<String>,
    pub chunk_index: Option<i64>,
    pub at: DateTime<Utc>,
}

impl StateStore {
    /// Record a successful incremental commit: `cursor` is the max value written
    /// to destination in this run.
    ///
    /// Monotonic guard: the boundary only moves forward; a non-advancing commit
    /// leaves the row untouched. The comparison happens in Rust (see
    /// `cursor_advances`) because the column is TEXT and SQL `<` would order
    /// numeric cursors lexicographically ("1000" < "999"), freezing the
    /// boundary. Read-then-write is not atomic, but — like the single-statement
    /// guard it replaces — this is regression protection, not a lock; rivet
    /// never runs two commits for the same export concurrently.
    pub fn record_committed_incremental(
        &self,
        export_name: &str,
        cursor: &str,
        run_id: &str,
    ) -> Result<()> {
        if let Some(stored) = self.committed_cursor(export_name)?
            && !cursor_advances(&stored, cursor)
        {
            return Ok(());
        }
        let now = Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_progression (
                export_name,
                last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                last_committed_run_id, last_committed_at
             ) VALUES (?1, 'incremental', ?2, NULL, ?3, ?4)
             ON CONFLICT(export_name) DO UPDATE SET
                last_committed_strategy = 'incremental',
                last_committed_cursor = excluded.last_committed_cursor,
                last_committed_chunk_index = NULL,
                last_committed_run_id = excluded.last_committed_run_id,
                last_committed_at = excluded.last_committed_at";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(sql, rusqlite::params![export_name, cursor, run_id, now])?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(&pg_sql(sql), &[&export_name, &cursor, &run_id, &now])?;
            }
        }
        Ok(())
    }

    /// Stored committed cursor for `export_name` — `None` when the export has
    /// no progression row or its committed boundary is chunked (cursor NULL).
    fn committed_cursor(&self, export_name: &str) -> Result<Option<String>> {
        let sql = "SELECT last_committed_cursor FROM export_progression WHERE export_name = ?1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                match c.query_row(sql, [export_name], |r| r.get::<_, Option<String>>(0)) {
                    Ok(v) => Ok(v),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                match c.query_opt(&pg_sql(sql), &[&export_name])? {
                    Some(row) => Ok(row.get::<_, Option<String>>(0)),
                    None => Ok(None),
                }
            }
        }
    }

    /// Record a successful chunked-run commit: the highest completed `chunk_index` for this run.
    pub fn record_committed_chunked(
        &self,
        export_name: &str,
        highest_chunk_index: i64,
        run_id: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_progression (
                export_name,
                last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                last_committed_run_id, last_committed_at
             ) VALUES (?1, 'chunked', NULL, ?2, ?3, ?4)
             ON CONFLICT(export_name) DO UPDATE SET
                last_committed_strategy = 'chunked',
                last_committed_cursor = NULL,
                last_committed_chunk_index = excluded.last_committed_chunk_index,
                last_committed_run_id = excluded.last_committed_run_id,
                last_committed_at = excluded.last_committed_at";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    sql,
                    rusqlite::params![export_name, highest_chunk_index, run_id, now],
                )?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
                    &pg_sql(sql),
                    &[&export_name, &highest_chunk_index, &run_id, &now],
                )?;
            }
        }
        Ok(())
    }

    /// Record a successful reconcile: all partitions in `run_id` matched.
    pub fn record_verified_chunked(
        &self,
        export_name: &str,
        highest_chunk_index: i64,
        run_id: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_progression (
                export_name,
                last_verified_strategy, last_verified_cursor, last_verified_chunk_index,
                last_verified_run_id, last_verified_at
             ) VALUES (?1, 'chunked', NULL, ?2, ?3, ?4)
             ON CONFLICT(export_name) DO UPDATE SET
                last_verified_strategy = 'chunked',
                last_verified_cursor = NULL,
                last_verified_chunk_index = excluded.last_verified_chunk_index,
                last_verified_run_id = excluded.last_verified_run_id,
                last_verified_at = excluded.last_verified_at";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    sql,
                    rusqlite::params![export_name, highest_chunk_index, run_id, now],
                )?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
                    &pg_sql(sql),
                    &[&export_name, &highest_chunk_index, &run_id, &now],
                )?;
            }
        }
        Ok(())
    }

    pub fn get_progression(&self, export_name: &str) -> Result<ExportProgression> {
        let sql = "SELECT
                last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                last_committed_run_id, last_committed_at,
                last_verified_strategy, last_verified_cursor, last_verified_chunk_index,
                last_verified_run_id, last_verified_at
             FROM export_progression WHERE export_name = ?1";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
                let row = stmt.query_row([export_name], |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, Option<i64>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Option<String>>(4)?,
                        r.get::<_, Option<String>>(5)?,
                        r.get::<_, Option<String>>(6)?,
                        r.get::<_, Option<i64>>(7)?,
                        r.get::<_, Option<String>>(8)?,
                        r.get::<_, Option<String>>(9)?,
                    ))
                });
                let (c_str, c_cur, c_idx, c_run, c_at, v_str, v_cur, v_idx, v_run, v_at) = match row
                {
                    Ok(t) => t,
                    Err(rusqlite::Error::QueryReturnedNoRows) => {
                        return Ok(ExportProgression {
                            export_name: export_name.to_string(),
                            committed: None,
                            verified: None,
                        });
                    }
                    Err(e) => return Err(e.into()),
                };
                Ok(ExportProgression {
                    export_name: export_name.to_string(),
                    committed: boundary_from_row(c_str, c_cur, c_idx, c_run, c_at),
                    verified: boundary_from_row(v_str, v_cur, v_idx, v_run, v_at),
                })
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                match c.query_opt(&pg_sql(sql), &[&export_name])? {
                    None => Ok(ExportProgression {
                        export_name: export_name.to_string(),
                        committed: None,
                        verified: None,
                    }),
                    Some(row) => {
                        let c_str: Option<String> = row.get(0);
                        let c_cur: Option<String> = row.get(1);
                        let c_idx: Option<i64> = row.get(2);
                        let c_run: Option<String> = row.get(3);
                        let c_at: Option<String> = row.get(4);
                        let v_str: Option<String> = row.get(5);
                        let v_cur: Option<String> = row.get(6);
                        let v_idx: Option<i64> = row.get(7);
                        let v_run: Option<String> = row.get(8);
                        let v_at: Option<String> = row.get(9);
                        Ok(ExportProgression {
                            export_name: export_name.to_string(),
                            committed: boundary_from_row(c_str, c_cur, c_idx, c_run, c_at),
                            verified: boundary_from_row(v_str, v_cur, v_idx, v_run, v_at),
                        })
                    }
                }
            }
        }
    }

    /// Delete the progression row for an export (committed + verified boundary).
    ///
    /// Called from `StateStore::reset` / `reset_chunk_checkpoint` so a reset
    /// returns the export to a "never ran" state across *all* state tables. A
    /// surviving `export_progression` row would make `rivet state progression`
    /// report a stale committed boundary after `state show` is already empty —
    /// a silent inconsistency that masks the reset.
    ///
    /// Returns the number of rows deleted (0 or 1).
    pub fn delete_progression(&self, export_name: &str) -> Result<usize> {
        let sql = "DELETE FROM export_progression WHERE export_name = ?1";
        match &self.conn {
            StateConn::Sqlite(c) => Ok(c.execute(sql, [export_name])?),
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                Ok(c.execute(&pg_sql(sql), &[&export_name])? as usize)
            }
        }
    }

    pub fn list_progression(&self) -> Result<Vec<ExportProgression>> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt =
                    c.prepare("SELECT export_name FROM export_progression ORDER BY export_name")?;
                let names: Vec<String> = stmt
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<std::result::Result<_, _>>()?;
                drop(stmt);
                let mut out = Vec::with_capacity(names.len());
                for n in names {
                    out.push(self.get_progression(&n)?);
                }
                Ok(out)
            }
            StateConn::Postgres(client) => {
                // Single query to avoid nested borrow_mut() calls.
                let mut c = client.borrow_mut();
                let rows = c.query(
                    "SELECT export_name,
                            last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                            last_committed_run_id, last_committed_at,
                            last_verified_strategy, last_verified_cursor, last_verified_chunk_index,
                            last_verified_run_id, last_verified_at
                     FROM export_progression ORDER BY export_name",
                    &[],
                )?;
                Ok(rows
                    .iter()
                    .map(|row| {
                        let export_name: String = row.get(0);
                        let c_str: Option<String> = row.get(1);
                        let c_cur: Option<String> = row.get(2);
                        let c_idx: Option<i64> = row.get(3);
                        let c_run: Option<String> = row.get(4);
                        let c_at: Option<String> = row.get(5);
                        let v_str: Option<String> = row.get(6);
                        let v_cur: Option<String> = row.get(7);
                        let v_idx: Option<i64> = row.get(8);
                        let v_run: Option<String> = row.get(9);
                        let v_at: Option<String> = row.get(10);
                        ExportProgression {
                            export_name,
                            committed: boundary_from_row(c_str, c_cur, c_idx, c_run, c_at),
                            verified: boundary_from_row(v_str, v_cur, v_idx, v_run, v_at),
                        }
                    })
                    .collect())
            }
        }
    }
}

/// True when `new` advances strictly past `stored` under cursor ordering.
///
/// Cursors are stored as TEXT but are often numeric (integer PKs, Float64
/// columns stringified by the sink). Integers compare as i128 first — exact
/// past f64's 2^53 mantissa — then floats as f64; when either side has no
/// numeric reading (or the f64s are unordered, e.g. NaN) the guard falls back
/// to byte-wise string order, which is correct for RFC3339 timestamps,
/// `YYYY-MM-DD` dates, and UUIDv7 keys.
fn cursor_advances(stored: &str, new: &str) -> bool {
    if let (Ok(a), Ok(b)) = (stored.parse::<i128>(), new.parse::<i128>()) {
        return b > a;
    }
    if let (Ok(a), Ok(b)) = (stored.parse::<f64>(), new.parse::<f64>())
        && let Some(ord) = b.partial_cmp(&a)
    {
        return ord.is_gt();
    }
    new > stored
}

fn boundary_from_row(
    strategy: Option<String>,
    cursor: Option<String>,
    chunk_index: Option<i64>,
    run_id: Option<String>,
    at: Option<String>,
) -> Option<Boundary> {
    let strategy = strategy?;
    let at = at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))?;
    Some(Boundary {
        strategy,
        run_id,
        cursor,
        chunk_index,
        at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn progression_unknown_export_returns_empty() {
        let s = store();
        let p = s.get_progression("orders").unwrap();
        assert!(p.committed.is_none());
        assert!(p.verified.is_none());
    }

    #[test]
    fn committed_incremental_records_cursor_and_run() {
        let s = store();
        s.record_committed_incremental("orders", "2024-06-01", "run-1")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.strategy, "incremental");
        assert_eq!(b.cursor.as_deref(), Some("2024-06-01"));
        assert_eq!(b.chunk_index, None);
        assert_eq!(b.run_id.as_deref(), Some("run-1"));
    }

    #[test]
    fn committed_cursor_does_not_regress_lexicographically() {
        let s = store();
        s.record_committed_incremental("orders", "2024-06-10", "run-10")
            .unwrap();
        s.record_committed_incremental("orders", "2024-01-01", "run-01")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("2024-06-10"));
    }

    #[test]
    fn committed_chunked_records_chunk_index() {
        let s = store();
        s.record_committed_chunked("orders", 41, "run-A").unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.strategy, "chunked");
        assert_eq!(b.chunk_index, Some(41));
        assert_eq!(b.cursor, None);
    }

    #[test]
    fn verified_and_committed_are_independent() {
        let s = store();
        s.record_committed_chunked("orders", 10, "run-A").unwrap();
        s.record_verified_chunked("orders", 5, "run-A").unwrap();
        let p = s.get_progression("orders").unwrap();
        assert_eq!(p.committed.as_ref().unwrap().chunk_index, Some(10));
        assert_eq!(p.verified.as_ref().unwrap().chunk_index, Some(5));
    }

    #[test]
    fn switching_strategy_updates_committed_row() {
        let s = store();
        s.record_committed_incremental("orders", "2024-01-01", "inc-1")
            .unwrap();
        s.record_committed_chunked("orders", 7, "chunk-1").unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.strategy, "chunked");
        assert_eq!(b.chunk_index, Some(7));
        assert_eq!(b.cursor, None);
    }

    // ROAST-RED progression-numeric-cursor: committed-boundary guard compares cursors
    // with SQL string '<' on a TEXT column, so numeric cursors regress ("1000" < "999"
    // lexicographically) and the boundary freezes at the shorter value.
    // Asserts CORRECT behavior; expected to FAIL until the fix lands.
    #[test]
    fn roast_committed_numeric_cursor_advances_past_lexicographic_boundary() {
        let s = store();
        s.record_committed_incremental("orders", "999", "run-999")
            .unwrap();
        s.record_committed_incremental("orders", "1000", "run-1000")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(
            b.cursor.as_deref(),
            Some("1000"),
            "numeric cursor must advance from 999 to 1000, but the lexicographic \
             TEXT comparison froze the committed boundary at {:?}",
            b.cursor
        );
    }

    #[test]
    fn committed_numeric_cursor_does_not_regress() {
        let s = store();
        s.record_committed_incremental("orders", "1000", "run-1000")
            .unwrap();
        s.record_committed_incremental("orders", "999", "run-999")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("1000"));
        assert_eq!(
            b.run_id.as_deref(),
            Some("run-1000"),
            "non-advancing commit must leave the boundary row untouched"
        );
    }

    #[test]
    fn committed_float_cursor_advances_across_integer_boundary() {
        // "10" < "9.9" lexicographically; the sink stringifies Float64 cursors
        // as "10" (no trailing .0), so once the i128 parse fails on "9.9" the
        // guard must compare as f64.
        let s = store();
        s.record_committed_incremental("scores", "9.9", "run-1")
            .unwrap();
        s.record_committed_incremental("scores", "10", "run-2")
            .unwrap();
        let b = s.get_progression("scores").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("10"));
        s.record_committed_incremental("scores", "9.95", "run-3")
            .unwrap();
        let b = s.get_progression("scores").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("10"), "9.95 must not regress 10");
    }

    #[test]
    fn committed_equal_cursor_is_a_no_op() {
        let s = store();
        s.record_committed_incremental("orders", "100", "run-1")
            .unwrap();
        s.record_committed_incremental("orders", "100", "run-2")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("100"));
        assert_eq!(
            b.run_id.as_deref(),
            Some("run-1"),
            "an equal cursor does not advance; the row must stay untouched"
        );
    }

    #[test]
    fn committed_rfc3339_cursor_advances_and_does_not_regress() {
        let s = store();
        s.record_committed_incremental("orders", "2024-06-01T00:00:00Z", "run-1")
            .unwrap();
        s.record_committed_incremental("orders", "2024-06-02T00:00:00Z", "run-2")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("2024-06-02T00:00:00Z"));
        s.record_committed_incremental("orders", "2024-05-31T00:00:00Z", "run-3")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("2024-06-02T00:00:00Z"));
    }

    #[test]
    fn committed_mixed_kind_cursor_falls_back_to_string_order() {
        // Old stored cursor is non-numeric, new one is numeric: there is no
        // shared numeric domain, so the guard keeps plain string order —
        // "123" < "abc" byte-wise, so the boundary holds.
        let s = store();
        s.record_committed_incremental("orders", "abc", "run-1")
            .unwrap();
        s.record_committed_incremental("orders", "123", "run-2")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("abc"));
    }

    #[test]
    fn committed_large_integer_cursor_compares_exactly() {
        // 2^53 and 2^53 + 1 collapse to the same f64; the i128 path must
        // compare them exactly so the boundary still advances by one.
        let s = store();
        s.record_committed_incremental("orders", "9007199254740992", "run-1")
            .unwrap();
        s.record_committed_incremental("orders", "9007199254740993", "run-2")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.cursor.as_deref(), Some("9007199254740993"));
    }

    #[test]
    fn switching_chunked_to_incremental_writes_cursor() {
        // The progression row exists but its committed cursor is NULL
        // (chunked); an incremental commit must write unconditionally.
        let s = store();
        s.record_committed_chunked("orders", 7, "chunk-1").unwrap();
        s.record_committed_incremental("orders", "100", "inc-1")
            .unwrap();
        let b = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(b.strategy, "incremental");
        assert_eq!(b.cursor.as_deref(), Some("100"));
        assert_eq!(b.chunk_index, None);
    }

    #[test]
    fn cursor_advances_orders_numbers_strings_and_nan() {
        assert!(cursor_advances("999", "1000"));
        assert!(!cursor_advances("1000", "999"));
        assert!(!cursor_advances("100", "100"));
        assert!(cursor_advances("9.9", "10"));
        assert!(cursor_advances("-5", "-4"));
        assert!(cursor_advances("2024-01-01", "2024-06-10"));
        // NaN has no f64 order; fall back to string order instead of
        // freezing the boundary forever.
        assert!(cursor_advances("NaN", "inf"));
        assert!(!cursor_advances("inf", "NaN"));
    }

    #[test]
    fn delete_progression_removes_only_the_named_export() {
        let s = store();
        s.record_committed_incremental("orders", "100", "run-o")
            .unwrap();
        s.record_committed_incremental("users", "9", "run-u")
            .unwrap();

        assert_eq!(
            s.delete_progression("orders").unwrap(),
            1,
            "deleting an existing progression row reports one row removed"
        );
        assert!(s.get_progression("orders").unwrap().committed.is_none());
        assert!(
            s.get_progression("users").unwrap().committed.is_some(),
            "delete must be scoped to the named export"
        );
        assert_eq!(
            s.delete_progression("orders").unwrap(),
            0,
            "deleting an absent progression row is a no-op (zero rows)"
        );
    }

    #[test]
    fn list_progression_sorted_by_name() {
        let s = store();
        s.record_committed_incremental("gamma", "3", "r").unwrap();
        s.record_committed_incremental("alpha", "1", "r").unwrap();
        s.record_committed_incremental("beta", "2", "r").unwrap();
        let all = s.list_progression().unwrap();
        let names: Vec<_> = all.iter().map(|p| p.export_name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }
}
