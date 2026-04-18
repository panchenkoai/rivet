//! Committed / verified export progression (Epic G — ADR-0008).
//!
//! Separates the three boundaries an operator may ask about:
//!
//! 1. **Observed** — seen in source (row estimates, chunk min/max). Today this
//!    lives in preflight outputs and `ComputedPlanData`; it is *not* a commitment.
//! 2. **Committed** — successfully exported to the destination. For incremental
//!    mode this is the max cursor written; for chunked, the highest `chunk_index`
//!    whose file reached the destination.
//! 3. **Verified** — committed **and** reconciled (Epic F). Partition-level
//!    source/export counts all matched.
//!
//! The `export_progression` table stores committed and verified boundaries.
//! `export_state.last_cursor_value` is untouched: it remains the single cursor
//! string used by execution (the `WHERE cursor > ?` predicate) and by ADR-0005
//! PA4 (apply-time drift check).

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
///
/// For incremental exports `cursor` is set; for chunked exports `chunk_index`
/// is set (highest committed/verified chunk). Mode label in `strategy` makes the
/// row self-describing in `rivet state progression`.
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
    /// Monotonicity is enforced at write time: if the stored committed cursor
    /// compares lexicographically greater than the new one, the existing row
    /// is kept (advisory guard against accidental regressions).
    pub fn record_committed_incremental(
        &self,
        export_name: &str,
        cursor: &str,
        run_id: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO export_progression (
                export_name,
                last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                last_committed_run_id, last_committed_at
             ) VALUES (?1, 'incremental', ?2, NULL, ?3, ?4)
             ON CONFLICT(export_name) DO UPDATE SET
                last_committed_strategy = 'incremental',
                last_committed_cursor = CASE
                    WHEN export_progression.last_committed_cursor IS NULL
                         OR export_progression.last_committed_cursor < excluded.last_committed_cursor
                    THEN excluded.last_committed_cursor
                    ELSE export_progression.last_committed_cursor END,
                last_committed_chunk_index = NULL,
                last_committed_run_id = excluded.last_committed_run_id,
                last_committed_at = excluded.last_committed_at",
            rusqlite::params![export_name, cursor, run_id, now],
        )?;
        Ok(())
    }

    /// Record a successful chunked-run commit: the highest completed `chunk_index` for this run.
    pub fn record_committed_chunked(
        &self,
        export_name: &str,
        highest_chunk_index: i64,
        run_id: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO export_progression (
                export_name,
                last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                last_committed_run_id, last_committed_at
             ) VALUES (?1, 'chunked', NULL, ?2, ?3, ?4)
             ON CONFLICT(export_name) DO UPDATE SET
                last_committed_strategy = 'chunked',
                last_committed_cursor = NULL,
                last_committed_chunk_index = excluded.last_committed_chunk_index,
                last_committed_run_id = excluded.last_committed_run_id,
                last_committed_at = excluded.last_committed_at",
            rusqlite::params![export_name, highest_chunk_index, run_id, now],
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
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO export_progression (
                export_name,
                last_verified_strategy, last_verified_cursor, last_verified_chunk_index,
                last_verified_run_id, last_verified_at
             ) VALUES (?1, 'chunked', NULL, ?2, ?3, ?4)
             ON CONFLICT(export_name) DO UPDATE SET
                last_verified_strategy = 'chunked',
                last_verified_cursor = NULL,
                last_verified_chunk_index = excluded.last_verified_chunk_index,
                last_verified_run_id = excluded.last_verified_run_id,
                last_verified_at = excluded.last_verified_at",
            rusqlite::params![export_name, highest_chunk_index, run_id, now],
        )?;
        Ok(())
    }

    pub fn get_progression(&self, export_name: &str) -> Result<ExportProgression> {
        let mut stmt = self.conn.prepare(
            "SELECT
                last_committed_strategy, last_committed_cursor, last_committed_chunk_index,
                last_committed_run_id, last_committed_at,
                last_verified_strategy, last_verified_cursor, last_verified_chunk_index,
                last_verified_run_id, last_verified_at
             FROM export_progression WHERE export_name = ?1",
        )?;
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

        let (c_str, c_cur, c_idx, c_run, c_at, v_str, v_cur, v_idx, v_run, v_at) = match row {
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

    pub fn list_progression(&self) -> Result<Vec<ExportProgression>> {
        let mut stmt = self
            .conn
            .prepare("SELECT export_name FROM export_progression ORDER BY export_name")?;
        let names: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<_, _>>()?;
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            out.push(self.get_progression(&n)?);
        }
        Ok(out)
    }
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
        // Earlier value — must not overwrite.
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
