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
        Ok(self
            .query_opt(
                "SELECT last_cursor_value, last_run_at, cursor_column FROM export_state \
                 WHERE export_name = ?1",
                &[export_name.into()],
                |r| CursorState {
                    export_name: export_name.to_string(),
                    last_cursor_value: r.opt_text(0),
                    last_run_at: r.opt_text(1),
                    cursor_column: r.opt_text(2),
                },
            )?
            .unwrap_or_else(|| CursorState {
                export_name: export_name.to_string(),
                last_cursor_value: None,
                last_run_at: None,
                cursor_column: None,
            }))
    }

    /// Read the cursor for a run progressing on `expected`, refusing one written for another column.
    pub fn get_owned(&self, export_name: &str, expected: &str) -> Result<CursorState> {
        let state = self.get(export_name)?;
        let Some(value) = state.last_cursor_value.as_deref() else {
            return Ok(state);
        };
        let owner = match &state.cursor_column {
            Some(c) => Some(c.clone()),
            None => self.legacy_cursor_owner(export_name, value)?,
        };
        if let Some(owner) = owner
            && owner != expected
        {
            anyhow::bail!(
                "export '{export_name}': the stored cursor `{value}` was written for `{owner}`, \
                 but this export now progresses on `{expected}` — comparing `{expected}` against \
                 it selects the wrong rows (on MySQL silently none, on every run).\n  \
                 Hint: `rivet state reset --export {export_name}` starts `{expected}` over with a \
                 full pass; or restore the previous cursor (`{owner}`)."
            );
        }
        Ok(state)
    }

    /// Owner of a pre-v26 row: the key of the latest successful keyset run that wrote this value.
    fn legacy_cursor_owner(&self, export_name: &str, value: &str) -> Result<Option<String>> {
        let latest = self.query_opt(
            "SELECT mode, chunk_key, cursor_max FROM export_metrics \
             WHERE export_name = ?1 AND status = 'success' ORDER BY id DESC LIMIT 1",
            &[export_name.into()],
            |r| (r.opt_text(0), r.opt_text(1), r.opt_text(2)),
        )?;
        Ok(match latest {
            Some((Some(mode), Some(key), Some(max))) if mode == "keyset" && max == value => {
                Some(key)
            }
            _ => None,
        })
    }

    /// Advance the cursor and record which column/key it belongs to.
    pub fn update_with_column(
        &self,
        export_name: &str,
        cursor_value: &str,
        cursor_column: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql =
            "INSERT INTO export_state (export_name, last_cursor_value, last_run_at, cursor_column)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(export_name) DO UPDATE SET
                last_cursor_value = excluded.last_cursor_value,
                last_run_at = excluded.last_run_at,
                cursor_column = excluded.cursor_column";
        self.execute(
            sql,
            &[
                export_name.into(),
                cursor_value.into(),
                now.into(),
                cursor_column.into(),
            ],
        )?;
        Ok(())
    }

    /// Advance the cursor value only, leaving any recorded `cursor_column` as is.
    pub fn update(&self, export_name: &str, cursor_value: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_state (export_name, last_cursor_value, last_run_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(export_name) DO UPDATE SET
                last_cursor_value = excluded.last_cursor_value,
                last_run_at = excluded.last_run_at";
        self.execute(sql, &[export_name.into(), cursor_value.into(), now.into()])?;
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
        self.execute(sql, &[export_name.into(), run_id.into(), now.into()])?;
        Ok(())
    }

    /// The persisted in-progress keyset run_id, or None when no run is in progress.
    pub fn get_resume_run_id(&self, export_name: &str) -> Result<Option<String>> {
        let sql = "SELECT resume_run_id FROM export_state WHERE export_name = ?1";
        Ok(self
            .query_opt(sql, &[export_name.into()], |r| r.opt_text(0))?
            .flatten())
    }

    /// Clear the in-progress run_id once a keyset run has finalized its manifest.
    pub fn clear_resume_run_id(&self, export_name: &str) -> Result<()> {
        self.execute(
            "UPDATE export_state SET resume_run_id = NULL WHERE export_name = ?1",
            &[export_name.into()],
        )?;
        Ok(())
    }

    /// Null ONLY the persisted keyset high-water mark (`last_cursor_value`),
    /// leaving `resume_run_id` and the progression boundary intact.
    ///
    /// Crash-recovery-only (non-incremental) keyset needs this at the start of a
    /// FRESH run: `last_cursor_value` is a run-independent persistent field, so a
    /// prior COMPLETED run leaves its final high-water mark behind. If this fresh
    /// run then crashes BEFORE its first page commits, the recovery run would load
    /// that stale mark as this run's "resume point" and skip the whole table
    /// (`WHERE key > <prior-max>` → 0 rows → a successful empty manifest). Clearing
    /// it here ties crash-recovery to THIS run's committed progress only.
    /// Incremental keyset deliberately does NOT clear it — continuing from the
    /// prior high-water mark is the whole point of `keyset_incremental`.
    pub fn clear_cursor_value(&self, export_name: &str) -> Result<()> {
        self.execute(
            "UPDATE export_state SET last_cursor_value = NULL WHERE export_name = ?1",
            &[export_name.into()],
        )?;
        Ok(())
    }

    /// Return an export to a "never ran" state.
    ///
    /// Clears the incremental cursor (`export_state`) **and** the committed /
    /// verified boundary (`export_progression`). Both must go: a surviving
    /// progression row would make `rivet state progression` report a stale
    /// committed boundary after `state show` is already empty.
    pub fn reset(&self, export_name: &str) -> Result<()> {
        self.execute(
            "DELETE FROM export_state WHERE export_name = ?1",
            &[export_name.into()],
        )?;
        self.delete_progression(export_name)?;
        Ok(())
    }

    pub fn list_all(&self) -> Result<Vec<CursorState>> {
        self.query(
            "SELECT export_name, last_cursor_value, last_run_at, cursor_column FROM export_state \
             ORDER BY export_name",
            &[],
            |r| CursorState {
                export_name: r.text(0),
                last_cursor_value: r.opt_text(1),
                last_run_at: r.opt_text(2),
                cursor_column: r.opt_text(3),
            },
        )
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
    fn clear_cursor_value_nulls_the_cursor_but_keeps_the_resume_run_id() {
        // The keyset stale-cursor fix: a fresh non-incremental run must null the
        // prior COMPLETED run's high-water mark so a pre-first-commit crash cannot
        // resume from it (skipping the whole table) — WITHOUT dropping the fresh
        // resume_run_id it is about to set (crash-recovery needs that).
        let s = store();
        s.update("orders", "9000000").unwrap();
        s.set_resume_run_id("orders", "run_2").unwrap();
        s.clear_cursor_value("orders").unwrap();
        assert!(
            s.get("orders").unwrap().last_cursor_value.is_none(),
            "cursor must be nulled"
        );
        assert_eq!(
            s.get_resume_run_id("orders").unwrap().as_deref(),
            Some("run_2"),
            "resume_run_id must survive"
        );
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

    fn metric(s: &StateStore, mode: &str, key: Option<&str>, max: &str) {
        s.execute(
            "INSERT INTO export_metrics \
             (export_name, run_at, duration_ms, total_rows, status, mode, chunk_key, cursor_max) \
             VALUES ('orders', '2026-09-11T00:00:00Z', 1, 1, 'success', ?1, ?2, ?3)",
            &[mode.into(), key.map(str::to_string).into(), max.into()],
        )
        .unwrap();
    }

    #[test]
    fn get_owned_accepts_the_identity_that_wrote_the_cursor() {
        let s = store();
        s.update_with_column("orders", "100", "id").unwrap();
        let c = s.get_owned("orders", "id").unwrap();
        assert_eq!(c.last_cursor_value.as_deref(), Some("100"));
        assert_eq!(c.cursor_column.as_deref(), Some("id"));
    }

    #[test]
    fn get_owned_refuses_a_cursor_written_for_another_column() {
        let s = store();
        s.update_with_column("orders", "3711169", "idvisit")
            .unwrap();
        let msg = format!(
            "{:#}",
            s.get_owned("orders", "visit_last_action_time").unwrap_err()
        );
        assert!(
            msg.contains("idvisit")
                && msg.contains("visit_last_action_time")
                && msg.contains("state reset"),
            "{msg}"
        );
    }

    #[test]
    fn update_keeps_the_recorded_column() {
        let s = store();
        s.update_with_column("orders", "1", "id").unwrap();
        s.update("orders", "2").unwrap();
        assert_eq!(
            s.get("orders").unwrap().cursor_column.as_deref(),
            Some("id")
        );
    }

    #[test]
    fn get_owned_without_a_cursor_value_never_refuses() {
        let s = store();
        s.set_resume_run_id("orders", "r1").unwrap();
        assert!(s.get_owned("orders", "anything").is_ok());
    }

    #[test]
    fn legacy_row_is_owned_by_the_keyset_run_that_wrote_it() {
        let s = store();
        s.update("orders", "3711169").unwrap();
        metric(&s, "keyset", Some("idvisit"), "3711169");
        assert!(s.get_owned("orders", "idvisit").is_ok());
        assert!(s.get_owned("orders", "visit_last_action_time").is_err());
    }

    #[test]
    fn legacy_row_without_a_matching_keyset_run_is_not_refused() {
        let s = store();
        s.update("orders", "2026-09-11 10:00:00").unwrap();
        metric(&s, "keyset", Some("idvisit"), "3711169");
        assert!(s.get_owned("orders", "updated_at").is_ok());

        let t = store();
        t.update("orders", "500").unwrap();
        metric(&t, "incremental", None, "500");
        assert!(t.get_owned("orders", "anything").is_ok());
    }
}
