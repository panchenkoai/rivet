use crate::error::Result;

use std::collections::HashSet;

use super::StateStore;

/// The `run_status` ledger — a best-effort ADVISORY record of each export run's
/// lifecycle: written `running` at run START (before any part lands) and
/// transitioned to a terminal status at finalize. The bucket manifest is a
/// companion PROJECTION (its status written FROM the same lifecycle), so a
/// cross-boundary reader (Airflow over the bucket) and a rivet process over a
/// shared state DB read the same signal. Row columns: `run_id` PK, `export_name`,
/// `prefix` (the run's write URI — the key `gc_orphans` matches at-or-under
/// `plan.gcs_prefix`), `status` (`running`|`success`|`failed`|`interrupted`),
/// `started_at`, `finished_at`.
///
/// NOT authoritative, by design: every write is best-effort (a miss only warns)
/// and `gc_orphans` reads it FAIL-OPEN — an unwritten or unreadable ledger makes
/// gc `active` (spare everything), the same safe behaviour as no ledger at all.
/// It only ever REFINES gc toward deleting a dead orphan; it never risks deleting
/// a live one. `gc_orphans` uses it to tell a LIVE extract (a `running` run NOT
/// superseded by a newer run of the same export) from a crash orphan — no
/// wall-clock heuristic. A hard-crashed run leaves a stale `running` row; a later
/// run of the same export SUPERSEDES it (higher `started_at`), so it stops
/// counting as active without any age/lease timer.
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
        self.execute(
            "INSERT INTO run_status
               (run_id, export_name, prefix, status, started_at, finished_at)
             VALUES (?1, ?2, ?3, 'running', ?4, NULL)
             ON CONFLICT(run_id) DO UPDATE SET
                 export_name = excluded.export_name,
                 prefix      = excluded.prefix,
                 status      = 'running',
                 started_at  = excluded.started_at,
                 finished_at = NULL",
            &[
                run_id.into(),
                export_name.into(),
                prefix.into(),
                started_at.into(),
            ],
        )?;
        Ok(())
    }

    /// Transition a run to a terminal status (`success` | `failed` |
    /// `interrupted`) at finalize. A no-op if the row is absent (a run that
    /// never called `begin_run` — e.g. a legacy/in-memory path).
    pub fn finish_run(&self, run_id: &str, status: &str, finished_at: &str) -> Result<()> {
        self.execute(
            "UPDATE run_status SET status = ?2, finished_at = ?3 WHERE run_id = ?1",
            &[run_id.into(), status.into(), finished_at.into()],
        )?;
        Ok(())
    }

    /// Is a LIVE extract writing `prefix`? True iff some `running` run on that
    /// prefix is NOT superseded by a newer run of the SAME export (a newer
    /// `started_at` means the old run crashed and its successor already re-ran,
    /// so the stale `running` no longer protects anything). Supersession, not a
    /// clock — the reconciliation is record-vs-record, never record-vs-`now`.
    pub fn has_active_run_on_prefix(&self, prefix: &str) -> Result<bool> {
        // A running, non-superseded run whose write prefix OVERLAPS this prefix —
        // either direction, so a query at ANY granularity matches:
        //   * equal (batch: run and load prefix coincide),
        //   * run AT-OR-UNDER query (a partitioned run's `…/created_at=…/` vs the
        //     load's base, truncated at `{partition}`), and
        //   * query AT-OR-UNDER run (a CDC run records its BASE `…/`, but the load
        //     gc's a per-TABLE child `…/<table>/`).
        // Over-matching (a broad run covering an unrelated child) only makes gc
        // SPARE — the safe direction (defer, never wrong-delete). `rtrim(x,'/')` +
        // `||` + `LIKE` are all portable across the SQLite and Postgres backends.
        let sql = "SELECT 1 FROM run_status r
                   WHERE (rtrim(r.prefix, '/') = rtrim(?1, '/')
                          OR r.prefix LIKE rtrim(?1, '/') || '/%'
                          OR rtrim(?1, '/') LIKE rtrim(r.prefix, '/') || '/%')
                     AND r.status = 'running'
                     AND NOT EXISTS (
                         SELECT 1 FROM run_status r2
                         WHERE r2.export_name = r.export_name
                           AND r2.started_at > r.started_at
                           AND (rtrim(r2.prefix, '/') = rtrim(r.prefix, '/')
                                OR r2.prefix LIKE rtrim(r.prefix, '/') || '/%'
                                OR rtrim(r.prefix, '/') LIKE rtrim(r2.prefix, '/') || '/%'))
                   LIMIT 1";
        Ok(self.query_opt(sql, &[prefix.into()], |_| ())?.is_some())
    }

    /// The run_ids of the runs [`has_active_run_on_prefix`] answers `true` for —
    /// same predicate, named rather than counted.
    ///
    /// The load needs the NAMES because a run still writing into the prefix can
    /// still GROW its manifest: the CDC sink rewrites a `Success` superset at
    /// every commit-boundary roll under ONE run_id. Recording such a run as
    /// consumed strands every part it writes afterwards — silently, forever,
    /// since the skip set is keyed on the run_id alone. Excluding exactly the
    /// active runs leaves them retryable while every terminal run is still
    /// recorded, so a completed run is never re-loaded.
    pub fn active_run_ids_on_prefix(&self, prefix: &str) -> Result<HashSet<String>> {
        let sql = "SELECT r.run_id FROM run_status r
                   WHERE (rtrim(r.prefix, '/') = rtrim(?1, '/')
                          OR r.prefix LIKE rtrim(?1, '/') || '/%'
                          OR rtrim(?1, '/') LIKE rtrim(r.prefix, '/') || '/%')
                     AND r.status = 'running'
                     AND NOT EXISTS (
                         SELECT 1 FROM run_status r2
                         WHERE r2.export_name = r.export_name
                           AND r2.started_at > r.started_at
                           AND (rtrim(r2.prefix, '/') = rtrim(r.prefix, '/')
                                OR r2.prefix LIKE rtrim(r.prefix, '/') || '/%'
                                OR rtrim(r.prefix, '/') LIKE rtrim(r2.prefix, '/') || '/%'))";
        Ok(self
            .query(sql, &[prefix.into()], |r| r.text(0))?
            .into_iter()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The load's skip set is keyed on `run_id` alone, and a CDC run's manifest
    /// GROWS under one id (the sink rewrites a `Success` superset at every
    /// commit-boundary roll). So a load that samples an in-flight run must not
    /// record it consumed — this accessor is what tells it which runs those are.
    ///
    /// Pins both halves: an ACTIVE run is named (so it stays retryable), and a
    /// FINISHED one is not (so a completed run is recorded and never re-loaded).
    #[test]
    fn active_run_ids_names_the_in_flight_run_and_forgets_the_finished_one() {
        let dir = tempfile::tempdir().unwrap();
        let st = crate::state::StateStore::open_sqlite(dir.path().join("s.db").to_str().unwrap())
            .unwrap();
        let prefix = "gs://b/exports/orders/";
        st.begin_run("run_a", "orders", prefix, "2026-08-01T10:00:00Z")
            .unwrap();
        st.begin_run("run_b", "customers", prefix, "2026-08-01T10:00:01Z")
            .unwrap();
        let active = st.active_run_ids_on_prefix(prefix).unwrap();
        assert!(
            active.contains("run_a") && active.contains("run_b"),
            "both in-flight runs must be named; got {active:?}"
        );
        st.finish_run("run_a", "success", "2026-08-01T10:05:00Z")
            .unwrap();
        let active = st.active_run_ids_on_prefix(prefix).unwrap();
        assert!(
            !active.contains("run_a"),
            "a finished run is no longer active — it must be recordable as consumed"
        );
        assert!(
            active.contains("run_b"),
            "the still-running sibling stays active"
        );
    }

    const P: &str = "gs://b/exports/orders/";

    /// Two independent StateStore connections to ONE shared Postgres state db behave
    /// like two rivet PROCESSES on a shared-Postgres deployment: a run begun on
    /// connection A is immediately visible to connection B's has_active_run_on_prefix
    /// (the gc_orphans concurrency signal — the whole reason PG state exists over the
    /// per-host SQLite file), and a newer run of the same export SUPERSEDES the older
    /// one CLOCK-FREE. The gate is otherwise single-process; gc_survival only simulates
    /// a running manifest. Env-gated on RIVET_TEST_STATE_URL; unique names so the shared
    /// public schema never collides with a sibling test.
    #[test]
    fn pg_shared_state_cross_connection_visibility_and_supersession() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return;
        };
        if !url.starts_with("postgres") {
            return;
        }
        // Two connections = two processes on the same shared Postgres state.
        unsafe { std::env::set_var("RIVET_STATE_URL", &url) };
        let a = StateStore::open(":memory:").expect("conn A");
        let b = StateStore::open(":memory:").expect("conn B");
        unsafe { std::env::remove_var("RIVET_STATE_URL") };

        let pid = std::process::id();
        let exp = format!("conc_test_{pid}");
        let (r1, r2) = (format!("r1_{pid}"), format!("r2_{pid}"));
        let pa = format!("gs://b/{exp}/runA/");
        let pb = format!("gs://b/{exp}/runB/");

        // A begins run1; B — a SEPARATE connection — must SEE it active.
        a.begin_run(&r1, &exp, &pa, "2026-01-01T00:00:01Z").unwrap();
        assert!(
            b.has_active_run_on_prefix(&pa).unwrap(),
            "conn B must see conn A's active run on the shared Postgres state (multi-process gc signal)"
        );

        // B begins run2 (newer started_at, same export) → supersedes run1 CLOCK-FREE.
        b.begin_run(&r2, &exp, &pb, "2026-01-01T00:00:02Z").unwrap();
        assert!(
            !a.has_active_run_on_prefix(&pa).unwrap(),
            "run1 is superseded by the newer run2 (by started_at, no age timer) → its prefix is no longer active"
        );
        assert!(
            a.has_active_run_on_prefix(&pb).unwrap(),
            "run2 (newest, same export) is the active run"
        );

        // Finishing run2 clears it — a terminal run is never active.
        b.finish_run(&r2, "success", "2026-01-01T00:00:03Z")
            .unwrap();
        assert!(
            !a.has_active_run_on_prefix(&pb).unwrap(),
            "a finished run must not count as active"
        );
        a.finish_run(&r1, "success", "2026-01-01T00:00:04Z").ok();
    }

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
    fn active_matches_a_load_prefix_under_a_cdc_base() {
        // A CDC run records its BASE prefix; the load gc's a per-table CHILD under
        // it. The overlap match (query-under-run direction) must still see it live.
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("cdc1", "cdc", "gs://b/exports/", "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix("gs://b/exports/orders/")
                .unwrap(),
            "a per-table child of a live CDC base prefix counts as active"
        );
        assert!(
            !s.has_active_run_on_prefix("gs://b/other/orders/").unwrap(),
            "an unrelated prefix outside the CDC base is NOT active"
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
