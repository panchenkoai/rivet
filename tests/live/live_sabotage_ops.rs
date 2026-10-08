//! Stops (docs/sabotage-matrix.yaml, the `sigkill_*`, `graceful_*` and `dead_owner_*` rows): a run
//! stopped by a signal while it is parked at a point the test can see, and the run after it. The
//! one grade is `Rig::stopped_then_run`: the stop is not a success, and the next plain run delivers
//! the source (the default oracle at the rig's seam). A graceful stop (SIGTERM, SIGINT) is held to
//! the SIGKILL of the same point by `graceful_is_worse`. A lease whose owner cannot be seen dead (it
//! names another host, or a pid that is alive again) must expire and let the next run through.

use super::live_sabotage::{mongo_rig, seeded};
use crate::common::*;

/// An export shape: its mode and export lines.
type Shape = (&'static str, &'static [&'static str]);

const FULL: Shape = ("full", &[]);
const INCREMENTAL: Shape = ("incremental", &["cursor_column: id"]);
const RANGE: Shape = (
    "chunked",
    &[
        "chunk_column: id",
        "chunk_size: 5",
        "chunk_checkpoint: true",
    ],
);
const KEYSET: Shape = (
    "chunked",
    &[
        "chunk_by_key: id",
        "chunk_size: 4",
        "chunk_checkpoint: true",
    ],
);

/// A lease no cell outlives: a takeover inside it is the dead owner's, not the clock's.
const LONG_LEASE: [(&str, &str); 1] = [("RIVET_STATE_LEASE_TTL_S", "600")];

/// A fresh seeded table exported as `shape`, and its drop guard.
fn rig_of(engine: SqlEngine, tag: &str, shape: Shape) -> (Rig, Box<dyn std::any::Any>) {
    let (table, guard) = seeded(engine, tag);
    (engine.staged(engine.rig(&table), shape.0, shape.1), guard)
}

/// Stop `rig`'s run with `how` at `at`; the next run must deliver.
fn stop_and_deliver(rig: &Rig, at: Parked, how: Stop) -> StoppedRun {
    let stopped = rig.stopped_then_run(at, how, &LONG_LEASE);
    if let Survived::Refused(text) = &stopped.next {
        panic!(
            "the run after a {how:?} at {at:?} is refused, and nothing here walks its remedy: {:?}\n{text}",
            stopped.left
        );
    }
    stopped
}

/// A SIGKILLed run is taken over by the next plain run.
fn killed(engine: SqlEngine, shape: Shape, at: Parked) {
    let (rig, _guard) = rig_of(engine, "stop_kill", shape);
    stop_and_deliver(&rig, at, Stop::Kill);
}

/// SIGTERM and SIGINT each leave no worse a state than the SIGKILL of the same point.
fn graceful(engine: SqlEngine, shape: Shape, at: Parked) {
    let (reference, _guard) = rig_of(engine, "stop_ref", shape);
    let by_kill = stop_and_deliver(&reference, at, Stop::Kill);
    for how in [Stop::Term, Stop::Int] {
        let (rig, _guard) = rig_of(engine, "stop_soft", shape);
        let stopped = stop_and_deliver(&rig, at, how);
        if let Some(why) = graceful_is_worse(&stopped, &by_kill) {
            panic!("{how:?} at {at:?} {why}\n{stopped:?}\n--- SIGKILL:\n{by_kill:?}");
        }
    }
}

/// [`killed`] for a MongoDB full export.
fn killed_mongo() {
    let (rig, _guard) = mongo_rig("stop_kill");
    stop_and_deliver(&rig, Parked::FirstPartStaged, Stop::Kill);
}

/// [`graceful`] for a MongoDB full export.
fn graceful_mongo() {
    let (reference, _guard) = mongo_rig("stop_ref");
    let by_kill = stop_and_deliver(&reference, Parked::FirstPartStaged, Stop::Kill);
    for how in [Stop::Term, Stop::Int] {
        let (rig, _guard) = mongo_rig("stop_soft");
        let stopped = stop_and_deliver(&rig, Parked::FirstPartStaged, how);
        if let Some(why) = graceful_is_worse(&stopped, &by_kill) {
            panic!("{how:?} {why}\n{stopped:?}\n--- SIGKILL:\n{by_kill:?}");
        }
    }
}

/// The run lease of a killed range run, rewritten to the holder `holder_sql` yields and kept for a few seconds: the run is refused twice, and waiting it out delivers.
fn a_lease_nobody_can_see_dead(engine: SqlEngine, holder_sql: &str) {
    const KEPT_S: u64 = 20;
    if state_url_under_test().is_none() {
        skip_live(
            "a lease is a row only on a Postgres state: set RIVET_GATE_STATE_URL (SQLite holds it by flock, which dies with its process)",
        );
        return;
    }
    let (mut rig, _guard) = rig_of(engine, "stop_owner", RANGE);
    rig.stopped(Parked::APartCommitted, Stop::Kill, &[]);
    rig.edit_state(
        &format!(
            "UPDATE state_lease SET holder = {holder_sql}, expires_at = now() + interval '{KEPT_S} seconds' \
             WHERE lease_key = 'chunk-run:{{export}}'"
        ),
        1,
    );
    let kept = std::time::Instant::now();
    rig.refuses_twice_then(
        &["run"],
        &[],
        Refused::uncoded_known_defect(
            1,
            "RIVET_STATE_RUN_IN_PROGRESS: a run beside a held run lease is refused with no code (the run-beside-a-run row of docs/operator-contract-matrix.yaml)",
        ),
        vec![Remedy::new(
            "wait for it to finish",
            Then::DeliversTheSource,
            move |_| {
                assert!(
                    kept.elapsed().as_secs() < KEPT_S,
                    "fixture: the lease expired before the refusal was read twice"
                );
                std::thread::sleep(std::time::Duration::from_secs(KEPT_S + 1) - kept.elapsed());
            },
        )],
    );
}

/// [`a_lease_nobody_can_see_dead`] with an owner on a host that is gone.
fn owner_on_another_host(engine: SqlEngine) {
    a_lease_nobody_can_see_dead(engine, "'a-host-that-is-gone:4242:0'");
}

/// [`a_lease_nobody_can_see_dead`] with the dead owner's pid alive again as an unrelated process (this test).
fn owner_pid_reused(engine: SqlEngine) {
    let holder = format!("split_part(holder, ':', 1) || ':{}:0'", std::process::id());
    a_lease_nobody_can_see_dead(engine, &holder);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_killed_full_run_with_first_part_staged_is_taken_over_by_the_next_run_postgres() {
    killed(SqlEngine::Pg, FULL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_killed_full_run_with_first_part_staged_is_taken_over_by_the_next_run_mysql() {
    killed(SqlEngine::Mysql, FULL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_killed_full_run_with_first_part_staged_is_taken_over_by_the_next_run_mssql() {
    killed(SqlEngine::Mssql, FULL, Parked::FirstPartStaged);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_killed_full_run_with_first_part_staged_is_taken_over_by_the_next_run_oracle() {
    killed(SqlEngine::Oracle, FULL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_killed_full_run_with_first_part_staged_is_taken_over_by_the_next_run_mongo() {
    killed_mongo();
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_killed_incremental_run_with_first_part_staged_is_taken_over_by_the_next_run_postgres() {
    killed(SqlEngine::Pg, INCREMENTAL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_killed_incremental_run_with_first_part_staged_is_taken_over_by_the_next_run_mysql() {
    killed(SqlEngine::Mysql, INCREMENTAL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_killed_incremental_run_with_first_part_staged_is_taken_over_by_the_next_run_mssql() {
    killed(SqlEngine::Mssql, INCREMENTAL, Parked::FirstPartStaged);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_killed_incremental_run_with_first_part_staged_is_taken_over_by_the_next_run_oracle() {
    killed(SqlEngine::Oracle, INCREMENTAL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_killed_range_run_with_first_part_staged_is_taken_over_by_the_next_run_postgres() {
    killed(SqlEngine::Pg, RANGE, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_killed_range_run_with_first_part_staged_is_taken_over_by_the_next_run_mysql() {
    killed(SqlEngine::Mysql, RANGE, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_killed_range_run_with_first_part_staged_is_taken_over_by_the_next_run_mssql() {
    killed(SqlEngine::Mssql, RANGE, Parked::FirstPartStaged);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_killed_range_run_with_first_part_staged_is_taken_over_by_the_next_run_oracle() {
    killed(SqlEngine::Oracle, RANGE, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_killed_range_run_with_a_part_committed_is_taken_over_by_the_next_run_postgres() {
    killed(SqlEngine::Pg, RANGE, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_killed_range_run_with_a_part_committed_is_taken_over_by_the_next_run_mysql() {
    killed(SqlEngine::Mysql, RANGE, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_killed_range_run_with_a_part_committed_is_taken_over_by_the_next_run_mssql() {
    killed(SqlEngine::Mssql, RANGE, Parked::APartCommitted);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_killed_range_run_with_a_part_committed_is_taken_over_by_the_next_run_oracle() {
    killed(SqlEngine::Oracle, RANGE, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_killed_keyset_run_with_first_part_staged_is_taken_over_by_the_next_run_postgres() {
    killed(SqlEngine::Pg, KEYSET, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_killed_keyset_run_with_first_part_staged_is_taken_over_by_the_next_run_mysql() {
    killed(SqlEngine::Mysql, KEYSET, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_killed_keyset_run_with_first_part_staged_is_taken_over_by_the_next_run_mssql() {
    killed(SqlEngine::Mssql, KEYSET, Parked::FirstPartStaged);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_killed_keyset_run_with_first_part_staged_is_taken_over_by_the_next_run_oracle() {
    killed(SqlEngine::Oracle, KEYSET, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_killed_keyset_run_with_a_part_committed_is_taken_over_by_the_next_run_postgres() {
    killed(SqlEngine::Pg, KEYSET, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_killed_keyset_run_with_a_part_committed_is_taken_over_by_the_next_run_mysql() {
    killed(SqlEngine::Mysql, KEYSET, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_killed_keyset_run_with_a_part_committed_is_taken_over_by_the_next_run_mssql() {
    killed(SqlEngine::Mssql, KEYSET, Parked::APartCommitted);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_killed_keyset_run_with_a_part_committed_is_taken_over_by_the_next_run_oracle() {
    killed(SqlEngine::Oracle, KEYSET, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_graceful_stop_of_a_full_run_with_first_part_staged_is_no_worse_than_a_kill_postgres() {
    graceful(SqlEngine::Pg, FULL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_graceful_stop_of_a_full_run_with_first_part_staged_is_no_worse_than_a_kill_mysql() {
    graceful(SqlEngine::Mysql, FULL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_graceful_stop_of_a_full_run_with_first_part_staged_is_no_worse_than_a_kill_mssql() {
    graceful(SqlEngine::Mssql, FULL, Parked::FirstPartStaged);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_graceful_stop_of_a_full_run_with_first_part_staged_is_no_worse_than_a_kill_oracle() {
    graceful(SqlEngine::Oracle, FULL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_graceful_stop_of_a_full_run_with_first_part_staged_is_no_worse_than_a_kill_mongo() {
    graceful_mongo();
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_graceful_stop_of_a_incremental_run_with_first_part_staged_is_no_worse_than_a_kill_postgres() {
    graceful(SqlEngine::Pg, INCREMENTAL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_graceful_stop_of_a_incremental_run_with_first_part_staged_is_no_worse_than_a_kill_mysql() {
    graceful(SqlEngine::Mysql, INCREMENTAL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_graceful_stop_of_a_incremental_run_with_first_part_staged_is_no_worse_than_a_kill_mssql() {
    graceful(SqlEngine::Mssql, INCREMENTAL, Parked::FirstPartStaged);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_graceful_stop_of_a_incremental_run_with_first_part_staged_is_no_worse_than_a_kill_oracle() {
    graceful(SqlEngine::Oracle, INCREMENTAL, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_graceful_stop_of_a_range_run_with_first_part_staged_is_no_worse_than_a_kill_postgres() {
    graceful(SqlEngine::Pg, RANGE, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_graceful_stop_of_a_range_run_with_first_part_staged_is_no_worse_than_a_kill_mysql() {
    graceful(SqlEngine::Mysql, RANGE, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_graceful_stop_of_a_range_run_with_first_part_staged_is_no_worse_than_a_kill_mssql() {
    graceful(SqlEngine::Mssql, RANGE, Parked::FirstPartStaged);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_graceful_stop_of_a_range_run_with_first_part_staged_is_no_worse_than_a_kill_oracle() {
    graceful(SqlEngine::Oracle, RANGE, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_graceful_stop_of_a_range_run_with_a_part_committed_is_no_worse_than_a_kill_postgres() {
    graceful(SqlEngine::Pg, RANGE, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_graceful_stop_of_a_range_run_with_a_part_committed_is_no_worse_than_a_kill_mysql() {
    graceful(SqlEngine::Mysql, RANGE, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_graceful_stop_of_a_range_run_with_a_part_committed_is_no_worse_than_a_kill_mssql() {
    graceful(SqlEngine::Mssql, RANGE, Parked::APartCommitted);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_graceful_stop_of_a_range_run_with_a_part_committed_is_no_worse_than_a_kill_oracle() {
    graceful(SqlEngine::Oracle, RANGE, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_graceful_stop_of_a_keyset_run_with_first_part_staged_is_no_worse_than_a_kill_postgres() {
    graceful(SqlEngine::Pg, KEYSET, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_graceful_stop_of_a_keyset_run_with_first_part_staged_is_no_worse_than_a_kill_mysql() {
    graceful(SqlEngine::Mysql, KEYSET, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_graceful_stop_of_a_keyset_run_with_first_part_staged_is_no_worse_than_a_kill_mssql() {
    graceful(SqlEngine::Mssql, KEYSET, Parked::FirstPartStaged);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_graceful_stop_of_a_keyset_run_with_first_part_staged_is_no_worse_than_a_kill_oracle() {
    graceful(SqlEngine::Oracle, KEYSET, Parked::FirstPartStaged);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_graceful_stop_of_a_keyset_run_with_a_part_committed_is_no_worse_than_a_kill_postgres() {
    graceful(SqlEngine::Pg, KEYSET, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_graceful_stop_of_a_keyset_run_with_a_part_committed_is_no_worse_than_a_kill_mysql() {
    graceful(SqlEngine::Mysql, KEYSET, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_graceful_stop_of_a_keyset_run_with_a_part_committed_is_no_worse_than_a_kill_mssql() {
    graceful(SqlEngine::Mssql, KEYSET, Parked::APartCommitted);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_graceful_stop_of_a_keyset_run_with_a_part_committed_is_no_worse_than_a_kill_oracle() {
    graceful(SqlEngine::Oracle, KEYSET, Parked::APartCommitted);
}

#[test]
#[ignore = "live: requires docker compose postgres + postgres-state (RIVET_GATE_STATE_URL)"]
fn a_lease_of_an_owner_on_a_host_that_is_gone_expires_and_the_run_delivers_postgres() {
    owner_on_another_host(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql + postgres-state (RIVET_GATE_STATE_URL)"]
fn a_lease_of_an_owner_on_a_host_that_is_gone_expires_and_the_run_delivers_mysql() {
    owner_on_another_host(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql + postgres-state (RIVET_GATE_STATE_URL)"]
fn a_lease_of_an_owner_on_a_host_that_is_gone_expires_and_the_run_delivers_mssql() {
    owner_on_another_host(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + postgres-state (RIVET_GATE_STATE_URL)"]
fn a_lease_of_an_owner_on_a_host_that_is_gone_expires_and_the_run_delivers_oracle() {
    owner_on_another_host(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres + postgres-state (RIVET_GATE_STATE_URL)"]
fn a_lease_of_a_dead_owner_whose_pid_is_alive_again_expires_and_the_run_delivers_postgres() {
    owner_pid_reused(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql + postgres-state (RIVET_GATE_STATE_URL)"]
fn a_lease_of_a_dead_owner_whose_pid_is_alive_again_expires_and_the_run_delivers_mysql() {
    owner_pid_reused(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql + postgres-state (RIVET_GATE_STATE_URL)"]
fn a_lease_of_a_dead_owner_whose_pid_is_alive_again_expires_and_the_run_delivers_mssql() {
    owner_pid_reused(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle + postgres-state (RIVET_GATE_STATE_URL)"]
fn a_lease_of_a_dead_owner_whose_pid_is_alive_again_expires_and_the_run_delivers_oracle() {
    owner_pid_reused(SqlEngine::Oracle);
}
