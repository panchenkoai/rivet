use rusqlite::OptionalExtension;

use crate::error::Result;

use super::{StateConn, StateStore};

/// The `run_status` ledger — the AUTHORITATIVE record of each export run's
/// lifecycle: written `running` at run START (before any part lands) and
/// transitioned to a terminal status at finalize. The bucket manifest is a
/// PROJECTION of it (its status written FROM these rows), so a cross-boundary
/// reader (Airflow over the bucket) and a rivet process over a shared state DB
/// see one truth. Row columns: `run_id` PK, `export_name`, `prefix` (the run's
/// write URI — the key `gc_orphans` matches at-or-under `plan.gcs_prefix`),
/// `status` (`running`|`success`|`failed`|`interrupted`), `started_at`,
/// `finished_at`.
///
/// `gc_orphans` reads it to tell a LIVE extract (a `running` run NOT superseded
/// by a newer run of the same export) from a crash orphan — no wall-clock
/// heuristic. A hard-crashed run leaves a stale `running` row; a later run of the
/// same export SUPERSEDES it (higher `started_at`), so it stops counting as
/// active without any age/lease timer.
impl StateStore {
    /// Record an export run as `running` at its START. Upsert on `run_id` so a
    /// RESUMED run reuses its row and re-arms `running` (clearing any prior
    /// terminal status / `finished_at` from an earlier attempt).
    pub fn begin_run(
        &self,
        run_id: &str,
        export_name: &str,
        prefix: &str,
        started_at: &str,
    ) -> Result<()> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    "INSERT INTO run_status
                       (run_id, export_name, prefix, status, started_at, finished_at)
                     VALUES (?1, ?2, ?3, 'running', ?4, NULL)
                     ON CONFLICT(run_id) DO UPDATE SET
                         export_name = excluded.export_name,
                         prefix      = excluded.prefix,
                         status      = 'running',
                         started_at  = excluded.started_at,
                         finished_at = NULL",
                    rusqlite::params![run_id, export_name, prefix, started_at],
                )?;
            }
            StateConn::Postgres(client) => {
                client.borrow_mut().execute(
                    "INSERT INTO run_status
                       (run_id, export_name, prefix, status, started_at, finished_at)
                     VALUES ($1, $2, $3, 'running', $4, NULL)
                     ON CONFLICT (run_id) DO UPDATE SET
                         export_name = excluded.export_name,
                         prefix      = excluded.prefix,
                         status      = 'running',
                         started_at  = excluded.started_at,
                         finished_at = NULL",
                    &[&run_id, &export_name, &prefix, &started_at],
                )?;
            }
        }
        Ok(())
    }

    /// Transition a run to a terminal status (`success` | `failed` |
    /// `interrupted`) at finalize. A no-op if the row is absent (a run that
    /// never called `begin_run` — e.g. a legacy/in-memory path).
    pub fn finish_run(&self, run_id: &str, status: &str, finished_at: &str) -> Result<()> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    "UPDATE run_status SET status = ?2, finished_at = ?3 WHERE run_id = ?1",
                    rusqlite::params![run_id, status, finished_at],
                )?;
            }
            StateConn::Postgres(client) => {
                client.borrow_mut().execute(
                    "UPDATE run_status SET status = $2, finished_at = $3 WHERE run_id = $1",
                    &[&run_id, &status, &finished_at],
                )?;
            }
        }
        Ok(())
    }

    /// Is a LIVE extract writing `prefix`? True iff some `running` run on that
    /// prefix is NOT superseded by a newer run of the SAME export (a newer
    /// `started_at` means the old run crashed and its successor already re-ran,
    /// so the stale `running` no longer protects anything). Supersession, not a
    /// clock — the reconciliation is record-vs-record, never record-vs-`now`.
    pub fn has_active_run_on_prefix(&self, prefix: &str) -> Result<bool> {
        // A running, non-superseded run AT-OR-UNDER this prefix. A run records
        // its FULL write URI; the load asks about its BASE (truncated at
        // `{partition}`), so a partitioned run's `…/created_at=…/` must still
        // match the base `…/` — hence the boundary-safe `LIKE base/%` in addition
        // to the trailing-slash-agnostic equality. `rtrim(x,'/')` + `||` + `LIKE`
        // are all portable across the SQLite and Postgres backends.
        let sql = "SELECT 1 FROM run_status r
                   WHERE (rtrim(r.prefix, '/') = rtrim(?1, '/')
                          OR r.prefix LIKE rtrim(?1, '/') || '/%')
                     AND r.status = 'running'
                     AND NOT EXISTS (
                         SELECT 1 FROM run_status r2
                         WHERE r2.export_name = r.export_name
                           AND r2.started_at > r.started_at)
                   LIMIT 1";
        match &self.conn {
            StateConn::Sqlite(c) => Ok(c
                .query_row(sql, rusqlite::params![prefix], |_| Ok(()))
                .optional()?
                .is_some()),
            StateConn::Postgres(client) => {
                let pg_sql = sql.replace("?1", "$1");
                Ok(client
                    .borrow_mut()
                    .query_opt(&pg_sql, &[&prefix])?
                    .is_some())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: &str = "gs://b/exports/orders/";

    #[test]
    fn begin_marks_active_then_finish_clears_it() {
        let s = StateStore::open_in_memory().unwrap();
        assert!(
            !s.has_active_run_on_prefix(P).unwrap(),
            "empty → not active"
        );
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "a running run makes the prefix active"
        );
        s.finish_run("r1", "success", "2026-01-01T00:01:00Z")
            .unwrap();
        assert!(
            !s.has_active_run_on_prefix(P).unwrap(),
            "a finished run leaves the prefix inactive"
        );
    }

    #[test]
    fn active_is_scoped_to_the_prefix() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            !s.has_active_run_on_prefix("gs://b/exports/users/").unwrap(),
            "a run on a DIFFERENT prefix does not make this one active"
        );
    }

    #[test]
    fn a_superseded_running_row_no_longer_counts_as_active() {
        // The clock-free staleness contract. r1 hard-crashed (still `running`,
        // never finished). Its successor r2 (newer started_at, SAME export) ran
        // and SUCCEEDED. r1 must stop counting as active — WITHOUT any age timer.
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap(); // crashed, left running
        s.begin_run("r2", "orders", P, "2026-01-01T00:00:05Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "r2 is running and newest → active"
        );
        s.finish_run("r2", "success", "2026-01-01T00:01:00Z")
            .unwrap();
        assert!(
            !s.has_active_run_on_prefix(P).unwrap(),
            "r1 is `running` but SUPERSEDED by the finished r2 → NOT active (no clock)"
        );
    }

    #[test]
    fn a_lone_crashed_running_row_still_counts_as_active() {
        // The documented residual: a hard-crash with NO successor leaves a
        // `running` row that keeps the prefix active — gc defers (safe: never
        // deletes) until the export re-runs and supersedes it.
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "a lone crashed running run keeps the prefix active (deferred, not deleted)"
        );
    }

    #[test]
    fn begin_rearms_running_on_a_resumed_run_id() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", P, "2026-01-01T00:00:00Z")
            .unwrap();
        s.finish_run("r1", "failed", "2026-01-01T00:01:00Z")
            .unwrap();
        assert!(!s.has_active_run_on_prefix(P).unwrap());
        // A resume reuses the run_id → re-arm running (upsert).
        s.begin_run("r1", "orders", P, "2026-01-01T00:02:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "re-begin on the same run_id re-arms running"
        );
    }

    #[test]
    fn finish_on_an_absent_run_is_a_noop() {
        let s = StateStore::open_in_memory().unwrap();
        s.finish_run("ghost", "success", "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(!s.has_active_run_on_prefix(P).unwrap());
    }

    #[test]
    fn active_matches_a_partitioned_child_prefix() {
        // A run records its FULL write URI (partition replaced); the load asks
        // about the BASE (truncated at `{partition}`). The child must still match.
        let s = StateStore::open_in_memory().unwrap();
        let child = "gs://b/exports/orders/created_at=2023-01-01/";
        s.begin_run("r1", "orders", child, "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix("gs://b/exports/orders/")
                .unwrap(),
            "a partitioned child run makes its base prefix active"
        );
    }

    #[test]
    fn active_is_boundary_safe_against_a_string_sibling() {
        // `exports/orders` must NOT match a run under `exports/orders_archive/`
        // (the classic string-prefix-vs-path-boundary trap).
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run(
            "r1",
            "arch",
            "gs://b/exports/orders_archive/p.parquet",
            "2026-01-01T00:00:00Z",
        )
        .unwrap();
        assert!(
            !s.has_active_run_on_prefix("gs://b/exports/orders").unwrap(),
            "a sibling prefix that merely string-starts-with must not count as active"
        );
    }
}
