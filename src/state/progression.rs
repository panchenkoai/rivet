//! Committed / verified export progression (Epic G — ADR-0008).

use chrono::{DateTime, Utc};

use super::StateStore;
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
        self.execute(
            sql,
            &[export_name.into(), cursor.into(), run_id.into(), now.into()],
        )?;
        Ok(())
    }

    /// Stored committed cursor for `export_name` — `None` when the export has
    /// no progression row or its committed boundary is chunked (cursor NULL).
    fn committed_cursor(&self, export_name: &str) -> Result<Option<String>> {
        let sql = "SELECT last_committed_cursor FROM export_progression WHERE export_name = ?1";
        // query_opt → Option<row>; the row's cursor is itself Option<String> → flatten.
        Ok(self
            .query_opt(sql, &[export_name.into()], |r| r.opt_text(0))?
            .flatten())
    }

    /// Stored chunked boundary index for `column`
    /// (`last_committed_chunk_index` / `last_verified_chunk_index`) — `None` when
    /// no row or the boundary isn't chunked (index NULL). `column` is a fixed
    /// literal chosen by the caller, never user input.
    fn chunk_boundary_index(&self, export_name: &str, column: &str) -> Result<Option<i64>> {
        let sql = format!("SELECT {column} FROM export_progression WHERE export_name = ?1");
        Ok(self
            .query_opt(&sql, &[export_name.into()], |r| r.opt_i64(0))?
            .flatten())
    }

    /// Record a successful chunked-run commit: the highest completed `chunk_index` for this run.
    pub fn record_committed_chunked(
        &self,
        export_name: &str,
        highest_chunk_index: i64,
        run_id: &str,
    ) -> Result<()> {
        // Never REGRESS the committed boundary: a later run that committed FEWER
        // chunks (a smaller re-run) must not lower it — mirror the
        // cursor_advances guard record_committed_cursor uses (bug hunt
        // 2026-08-09; the incremental path guarded, chunked did not).
        //
        // `<`, not `<=`, and the difference is the whole row. `highest_chunk_index`
        // is RUN-RELATIVE: `chunk_index` restarts at 0 every run, so this is a chunk
        // COUNT, not a watermark. A stable table with a stable `chunk_size` yields
        // the SAME number forever — and `<=` discarded every later run whole,
        // `run_id` and `at` included. After N successful runs the row still named
        // run 1, so `rivet state progression` answered "when was this last
        // committed" with the FIRST run's timestamp, permanently, and a clean
        // `rivet reconcile` never refreshed `last_verified_at`. Silent because the
        // function returns `Ok(())` and the caller logs only on `Err`: the row looks
        // fully populated, just from the wrong run. The guard test exercised
        // 40 → 9 → 55 and never the EQUAL case (round-11 bughunt).
        if let Some(stored) =
            self.chunk_boundary_index(export_name, "last_committed_chunk_index")?
            && highest_chunk_index < stored
        {
            return Ok(());
        }
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
        self.execute(
            sql,
            &[
                export_name.into(),
                highest_chunk_index.into(),
                run_id.into(),
                now.into(),
            ],
        )?;
        Ok(())
    }

    /// Record a successful reconcile: all partitions in `run_id` matched.
    pub fn record_verified_chunked(
        &self,
        export_name: &str,
        highest_chunk_index: i64,
        run_id: &str,
    ) -> Result<()> {
        // Same no-regress guard as record_committed_chunked (bug hunt 2026-08-09).
        if let Some(stored) = self.chunk_boundary_index(export_name, "last_verified_chunk_index")?
            && highest_chunk_index < stored
        {
            return Ok(());
        }
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
        self.execute(
            sql,
            &[
                export_name.into(),
                highest_chunk_index.into(),
                run_id.into(),
                now.into(),
            ],
        )?;
        Ok(())
    }

    pub fn get_progression(&self, export_name: &str) -> Result<ExportProgression> {
        let sql = "SELECT
                last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                last_committed_run_id, last_committed_at,
                last_verified_strategy, last_verified_cursor, last_verified_chunk_index,
                last_verified_run_id, last_verified_at
             FROM export_progression WHERE export_name = ?1";
        Ok(self
            .query_opt(sql, &[export_name.into()], |r| ExportProgression {
                export_name: export_name.to_string(),
                committed: boundary_from_row(
                    r.opt_text(0),
                    r.opt_text(1),
                    r.opt_i64(2),
                    r.opt_text(3),
                    r.opt_text(4),
                ),
                verified: boundary_from_row(
                    r.opt_text(5),
                    r.opt_text(6),
                    r.opt_i64(7),
                    r.opt_text(8),
                    r.opt_text(9),
                ),
            })?
            .unwrap_or_else(|| ExportProgression {
                export_name: export_name.to_string(),
                committed: None,
                verified: None,
            }))
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
        self.execute(
            "DELETE FROM export_progression WHERE export_name = ?1",
            &[export_name.into()],
        )
    }

    pub fn list_progression(&self) -> Result<Vec<ExportProgression>> {
        // One query + one projection for both backends (was a per-name loop on
        // SQLite and a duplicated 11-column extraction on Postgres).
        let sql = "SELECT export_name,
                        last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                        last_committed_run_id, last_committed_at,
                        last_verified_strategy, last_verified_cursor, last_verified_chunk_index,
                        last_verified_run_id, last_verified_at
                 FROM export_progression ORDER BY export_name";
        self.query(sql, &[], |r| ExportProgression {
            export_name: r.text(0),
            committed: boundary_from_row(
                r.opt_text(1),
                r.opt_text(2),
                r.opt_i64(3),
                r.opt_text(4),
                r.opt_text(5),
            ),
            verified: boundary_from_row(
                r.opt_text(6),
                r.opt_text(7),
                r.opt_i64(8),
                r.opt_text(9),
                r.opt_text(10),
            ),
        })
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
    fn committed_chunked_boundary_does_not_regress() {
        // A later run that committed FEWER chunks must not lower the boundary —
        // the same no-regress guard the incremental path has (bug hunt
        // 2026-08-09). RED against the unconditional ON CONFLICT overwrite.
        let s = store();
        s.record_committed_chunked("orders", 40, "runA").unwrap();
        s.record_committed_chunked("orders", 9, "runB").unwrap(); // must be ignored
        let p = s.get_progression("orders").unwrap();
        let idx = p
            .committed
            .expect("committed boundary present")
            .chunk_index
            .expect("chunked boundary carries a chunk_index");
        assert_eq!(idx, 40, "boundary must not regress from 40 to 9");
        // a genuine advance still moves it
        s.record_committed_chunked("orders", 55, "runC").unwrap();
        let idx2 = s
            .get_progression("orders")
            .unwrap()
            .committed
            .unwrap()
            .chunk_index
            .unwrap();
        assert_eq!(idx2, 55, "a higher index must advance the boundary");

        // The EQUAL case, which this test never had and which is the ORDINARY one:
        // `highest_chunk_index` is run-relative (`chunk_index` restarts at 0 each
        // run), so a stable table with a stable `chunk_size` reports the SAME count
        // forever. Under `<=` every later run was discarded whole — `run_id` and
        // `at` with it — so the row named the FIRST run permanently and
        // `rivet state progression` answered "last committed" with its timestamp.
        s.record_committed_chunked("orders", 55, "runD").unwrap();
        let c = s.get_progression("orders").unwrap().committed.unwrap();
        assert_eq!(
            c.chunk_index.unwrap(),
            55,
            "the boundary itself is unchanged — this is not an advance"
        );
        assert_eq!(
            c.run_id.as_deref(),
            Some("runD"),
            "...but the run that last committed it MUST refresh, or the answer to \
             `when was this last committed` freezes at the first run that ever hit \
             this count"
        );
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
