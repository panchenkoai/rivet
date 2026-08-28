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
/// One `run_status` row, as `rivet state runs` prints it.
pub struct RunStatusRow {
    pub run_id: String,
    pub export_name: String,
    pub prefix: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: String,
}

/// What `finish_run_checked` did — the CLI's three honest answers.
#[derive(Debug, PartialEq, Eq)]
pub enum FinishOutcome {
    /// The row was `running` and is now `interrupted`.
    Stamped,
    /// Nothing to do — the row already carries this terminal status.
    AlreadyTerminal(String),
    /// No such run id: refuse loudly, never "success" on a typo.
    NotFound,
}

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
        // SUPERSESSION requires a newer SUCCESS, not merely a newer terminal row.
        // The first cut accepted any non-running successor, and a FAILED one
        // proves nothing: under the documented overlap model (interval overrun,
        // cron double-fire) cycle N+1 can start, fail at open (cycle N holds the
        // slot), and finish terminal while N is still streaming — the inference
        // "newer terminal ⇒ the old run crashed and was re-run" then declared
        // the LIVE run dead, `gc_orphans` collected its just-flushed unmanifested
        // parts in the flush→manifest window, and the ack advanced the source
        // past them: gone from both ends, manifest says Success. A newer SUCCESS
        // over the same export genuinely proves a full pass happened after the
        // stale row; a newer failure only proves somebody tried.
        //
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
                           AND r2.status = 'success')
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
    /// The status of ONE run-status row, or `None` when no row exists — the gc
    /// marker sweep's ledger probe: a `Running` BUCKET marker whose ledger row
    /// is TERMINAL is a dead crash marker even though no Success ever
    /// superseded it (an abandoned export's marker can never be superseded —
    /// round-5, three agents independently). Co-located only; a stateless gc
    /// has no ledger and keeps the conservative supersession-only sweep.
    pub fn run_status_of(&self, run_id: &str) -> Result<Option<String>> {
        let sql = "SELECT status FROM run_status WHERE run_id = ?1";
        Ok(self
            .query(sql, &[run_id.into()], |r| r.text(0))?
            .into_iter()
            .next())
    }

    /// The run-status rows, newest first — `rivet state runs`. `running_only`
    /// narrows to the rows that can freeze a prefix (gc/cleanup read them).
    pub fn recent_run_status(&self, last: usize, running_only: bool) -> Result<Vec<RunStatusRow>> {
        // COALESCE: `begin_run` leaves finished_at NULL until finalize, and the
        // row mapper's text() refuses NULL — the running rows are exactly the
        // ones this listing exists for (caught by the e2e test seeding through
        // the REAL begin_run; a hand-seeded '' fixture hid it).
        let sql = if running_only {
            "SELECT run_id, export_name, prefix, status, started_at,
                    COALESCE(finished_at, '')
             FROM run_status WHERE status = 'running'
             ORDER BY started_at DESC LIMIT ?1"
        } else {
            "SELECT run_id, export_name, prefix, status, started_at,
                    COALESCE(finished_at, '')
             FROM run_status ORDER BY started_at DESC LIMIT ?1"
        };
        self.query(sql, &[(last as i64).into()], |r| RunStatusRow {
            run_id: r.text(0),
            export_name: r.text(1),
            prefix: r.text(2),
            status: r.text(3),
            started_at: r.text(4),
            finished_at: r.text(5),
        })
    }

    /// The `rivet state finish-run` escape hatch: stamp a row `interrupted`
    /// ONLY if it exists and is still `running` — the outcome says which, so
    /// the CLI can refuse loudly instead of "success" on a typo'd id.
    ///
    /// Exists because a hard-crashed run with no SUCCESSFUL successor has no
    /// other exit (round-4): supersession is success-only, nothing ages rows
    /// out, and gc/cleanup/consumed-exclusion all freeze on the stale row —
    /// with every message telling the operator to wait for a run that will
    /// never finish.
    pub fn finish_run_checked(&self, run_id: &str, finished_at: &str) -> Result<FinishOutcome> {
        // ATOMIC: the UPDATE itself carries the status guard, and Stamped is
        // derived from rows-affected — a run finalizing `success` between a
        // SELECT and an unconditional UPDATE would be downgraded to
        // `interrupted`, possibly un-superseding an older stale row the
        // success had just retired (round-5 refuter). The SELECT afterwards
        // only names which of the two remaining answers is true.
        let n = self.execute(
            "UPDATE run_status SET status = 'interrupted', finished_at = ?2
             WHERE run_id = ?1 AND status = 'running'",
            &[run_id.into(), finished_at.into()],
        )?;
        if n > 0 {
            return Ok(FinishOutcome::Stamped);
        }
        let sql = "SELECT status FROM run_status WHERE run_id = ?1";
        let status = self.query(sql, &[run_id.into()], |r| r.text(0))?;
        match status.into_iter().next() {
            Some(status) => Ok(FinishOutcome::AlreadyTerminal(status)),
            None => Ok(FinishOutcome::NotFound),
        }
    }

    /// Terminal-stamp the `running` rows of split units a `--split --resume`
    /// RECONSTRUCTION has ceased to exist — ordinals at/past `kept`.
    ///
    /// A trailing-adjacent crash makes the reconstructed partition SMALLER than
    /// the crashed one (the open tail absorbs the crashed range), so
    /// `{giant}#5..#7` will never run again under this lineage — no SUCCESS of
    /// those names can ever supersede their rows, and an unstamped row wedges
    /// `has_active_run_on_prefix` (and with it gc/cleanup on the whole shared
    /// prefix) FOREVER (round-4). The moment that KNOWS an ordinal ceased is
    /// the reconstruction, so its caller stamps here.
    ///
    /// Returns the stamped export names. Ordinal-scoped, never a blanket LIKE
    /// delete: `#4` of a 5-unit reconstruction is still a live name.
    pub fn interrupt_ceased_split_units(
        &self,
        giant: &str,
        prefix: &str,
        kept: usize,
        finished_at: &str,
    ) -> Result<Vec<String>> {
        // PREFIX-SCOPED (round-5 refuter): on a shared-Postgres state DB two
        // configs can both carry a split giant named `orders` — a name-only
        // stamp would close the OTHER config's live rows. Same containment
        // predicate as `active_run_ids_on_prefix`.
        let sql = "SELECT r.run_id, r.export_name FROM run_status r
                   WHERE r.export_name LIKE ?1 || '#%' AND r.status = 'running'
                     AND (rtrim(r.prefix, '/') = rtrim(?2, '/')
                          OR r.prefix LIKE rtrim(?2, '/') || '/%'
                          OR rtrim(?2, '/') LIKE rtrim(r.prefix, '/') || '/%')";
        let rows = self.query(sql, &[giant.into(), prefix.into()], |r| {
            (r.text(0), r.text(1))
        })?;
        let mut stamped = Vec::new();
        for (run_id, name) in rows {
            let ceased = name
                .strip_prefix(giant)
                .and_then(|s| s.strip_prefix('#'))
                .and_then(|s| s.parse::<usize>().ok())
                .is_some_and(|ord| ord >= kept);
            if !ceased {
                continue;
            }
            self.finish_run(&run_id, "interrupted", finished_at)?;
            stamped.push(name);
        }
        Ok(stamped)
    }

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
                           AND r2.status = 'success')";
        Ok(self
            .query(sql, &[prefix.into()], |r| r.text(0))?
            .into_iter()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The escape hatch's three answers, each decisive: a typo'd id must not
    /// read as success, an already-terminal row must say its status, and only
    /// a `running` row gets stamped. RED against collapsing NotFound into
    /// Stamped (the UPDATE-only shape, which no-ops silently on both).
    #[test]
    fn finish_run_checked_stamps_only_a_live_row_and_says_why_otherwise() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("r1", "orders", "gs://b/p", "2026-08-21T00:00:00Z")
            .unwrap();
        assert_eq!(
            s.finish_run_checked("nope", "2026-08-21T01:00:00Z")
                .unwrap(),
            FinishOutcome::NotFound
        );
        assert_eq!(
            s.finish_run_checked("r1", "2026-08-21T01:00:00Z").unwrap(),
            FinishOutcome::Stamped
        );
        assert!(!s.has_active_run_on_prefix("gs://b/p").unwrap());
        assert_eq!(
            s.finish_run_checked("r1", "2026-08-21T02:00:00Z").unwrap(),
            FinishOutcome::AlreadyTerminal("interrupted".into())
        );
        // And the listing surfaces what the operator needs to find the id.
        let rows = s.recent_run_status(10, false).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].run_id, "r1");
        assert_eq!(rows[0].status, "interrupted");
        assert!(s.recent_run_status(10, true).unwrap().is_empty());
    }

    /// Round-4 split-wedge closure, ledger half: a reconstruction that kept 5
    /// units stamps `#5..` interrupted; `#0..#4` and a FOREIGN export are
    /// untouched, and the prefix stops reading active. RED against `>` in the
    /// ordinal compare (off-by-one keeps `#5` wedged) and against a blanket
    /// stamp (would kill `#4`).
    #[test]
    fn ceased_split_units_are_stamped_and_live_ordinals_survive() {
        let s = StateStore::open_in_memory().unwrap();
        for ord in 0..8 {
            s.begin_run(
                &format!("r{ord}"),
                &format!("orders#{ord}"),
                "gs://b/orders",
                "2026-08-21T00:00:00Z",
            )
            .unwrap();
        }
        s.begin_run("rx", "unrelated", "gs://b/other", "2026-08-21T00:00:00Z")
            .unwrap();
        // A FOREIGN config's same-name giant on ANOTHER prefix must survive.
        s.begin_run(
            "rf",
            "orders#7",
            "gs://other/orders",
            "2026-08-21T00:00:00Z",
        )
        .unwrap();
        let stamped = s
            .interrupt_ceased_split_units("orders", "gs://b/orders", 5, "2026-08-21T01:00:00Z")
            .unwrap();
        let mut names = stamped.clone();
        names.sort();
        assert_eq!(names, ["orders#5", "orders#6", "orders#7"]);
        // #0..#4 still running (their ordinals exist in the reconstruction)…
        let active = s.active_run_ids_on_prefix("gs://b/orders").unwrap();
        for ord in 0..5 {
            assert!(active.contains(&format!("r{ord}")), "r{ord} must stay live");
        }
        // …and the ceased ones no longer wedge the prefix once the rest finish.
        for ord in 0..5 {
            s.finish_run(&format!("r{ord}"), "success", "2026-08-21T02:00:00Z")
                .unwrap();
        }
        assert!(
            !s.has_active_run_on_prefix("gs://b/orders").unwrap(),
            "with every kept unit finished and every ceased one stamped, the \
             prefix must read idle — this is the wedge the stamp exists to close"
        );
        assert!(s.has_active_run_on_prefix("gs://b/other").unwrap());
        assert!(
            s.has_active_run_on_prefix("gs://other/orders").unwrap(),
            "the OTHER config's same-name giant is out of scope — prefix-scoping is the guard"
        );
    }

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
    /// Supersession requires a TERMINAL successor — on the default backend, so it
    /// actually runs.
    ///
    /// The rule lived only in the Postgres test above, which early-returns unless
    /// `RIVET_TEST_STATE_URL` points at one — so on an ordinary `cargo test` it was
    /// SKIPPED, and mutating the status clause away left everything green. Skip is
    /// not a pass, and this file had no other home for the rule.
    ///
    /// What the clause protects: two concurrently LIVE runs of one export are
    /// ordinary — a `{date}` prefix straddling UTC midnight while the next
    /// scheduled cycle starts. Without it the older one reads as INACTIVE, which
    /// un-gates `gc_orphans` against its prefix and deletes parts a live writer has
    /// committed but not yet manifested.
    #[test]
    fn a_running_successor_does_not_supersede_but_a_finished_one_does() {
        // `open_in_memory` EXPLICITLY, not `open`. `open` consults the process-global
        // `RIVET_STATE_URL`, and the Postgres test below SETS that variable mid-run
        // (`unsafe set_var` … `remove_var`) — so when the dev stand supplies
        // `RIVET_TEST_STATE_URL` and both tests run in one process, this store
        // silently opened POSTGRES instead and the first assertion failed. Measured:
        // green alone, red in the full suite. A test that names its backend cannot
        // be moved by another test's environment.
        let st = StateStore::open_in_memory().expect("state");
        let (pa, pb) = ("gs://b/e/runA/", "gs://b/e/runB/");
        st.begin_run("r1", "e", pa, "2026-01-01T00:00:01Z").unwrap();
        assert!(st.has_active_run_on_prefix(pa).unwrap(), "r1 is running");

        // A newer run of the SAME export, itself still running.
        st.begin_run("r2", "e", pb, "2026-01-01T00:00:02Z").unwrap();
        assert!(
            st.has_active_run_on_prefix(pa).unwrap(),
            "a RUNNING successor has re-done nothing, so r1's prefix must stay \
             active — releasing it here is what lets the collector delete a live \
             writer's committed parts"
        );

        // Terminal successor: now the crashed r1 is releasable, clock-free.
        st.finish_run("r2", "success", "2026-01-01T00:00:09Z")
            .unwrap();
        assert!(
            !st.has_active_run_on_prefix(pa).unwrap(),
            "a FINISHED successor supersedes it — no age timer, no second clock"
        );
    }

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

        // B begins run2 (newer started_at, same export). It is still RUNNING, so it
        // supersedes NOTHING — and this assertion is the correction of a spec the
        // test itself used to pin.
        //
        // Supersession answers "did a later run already re-do this crashed one's
        // work"; only a run that REACHED a terminal status has. Two concurrently
        // live runs of one export are ordinary — a `{date}` prefix straddling UTC
        // midnight while the next scheduled cycle starts — and under the old rule
        // the older one read as INACTIVE, which un-gates `gc_orphans` against its
        // prefix and deletes parts a live writer has committed but not yet
        // manifested. That is precisely the harm the three-way classification
        // exists to prevent, and the test asserted the bug.
        b.begin_run(&r2, &exp, &pb, "2026-01-01T00:00:02Z").unwrap();
        assert!(
            a.has_active_run_on_prefix(&pa).unwrap(),
            "run1 is STILL active: a newer run that is itself running has not \
             re-done anything, so it cannot release run1's prefix to the collector"
        );

        // Once the successor is TERMINAL, supersession applies — still clock-free,
        // still no age timer.
        b.finish_run(&r2, "success", "2026-01-01T00:00:09Z")
            .unwrap();
        assert!(
            !a.has_active_run_on_prefix(&pa).unwrap(),
            "a FINISHED successor supersedes the crashed run1 — that is what makes \
             a stale `running` row releasable without comparing two clocks"
        );
        b.begin_run(&r2, &exp, &pb, "2026-01-01T00:00:02Z").unwrap();
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

    /// A newer FAILED run does not supersede a live one — only a SUCCESS does.
    ///
    /// The overlap model makes this load-bearing: cycle N streams while cycle
    /// N+1 starts, fails at open (N holds the slot), and lands terminal. The
    /// old predicate read any newer terminal row as "the old run crashed and
    /// was re-run", declared the LIVE run inactive, and `gc_orphans` then
    /// collected its just-flushed unmanifested parts while the ack advanced the
    /// source past them — gone from both ends under a Success manifest.
    #[test]
    fn a_newer_failed_run_does_not_supersede_a_live_one() {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("old", "orders", P, "2026-08-28T10:00:00Z")
            .unwrap();
        s.begin_run("new", "orders", P, "2026-08-28T10:05:00Z")
            .unwrap();
        s.finish_run("new", "failed", "2026-08-28T10:05:01Z")
            .unwrap();
        assert!(
            s.has_active_run_on_prefix(P).unwrap(),
            "a FAILED successor proves somebody tried, not that the old run is \
             dead — treating it as supersession lets gc delete a live run's \
             just-flushed parts"
        );
        // BOTH predicates — they are one rule in two spellings, and the second
        // (`active_run_ids`) is what the load's consumed-exclusion reads; a fix
        // landing on only one lets the load consume the live run's growing
        // manifest and strand its later parts.
        assert_eq!(
            s.active_run_ids_on_prefix(P).unwrap(),
            std::collections::HashSet::from(["old".to_string()]),
            "the live run must still be NAMED active for the load's exclusion"
        );
        s.begin_run("new2", "orders", P, "2026-08-28T10:10:00Z")
            .unwrap();
        s.finish_run("new2", "success", "2026-08-28T10:11:00Z")
            .unwrap();
        assert!(
            !s.has_active_run_on_prefix(P).unwrap(),
            "a newer SUCCESS is the one honest supersession signal"
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
