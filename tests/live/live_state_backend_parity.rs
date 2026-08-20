//! State-backend PARITY: init (migrate) + run the SAME golden fixture with BOTH
//! state backends — SQLite (the default `.rivet_state.db`) and PostgreSQL (via
//! `RIVET_TEST_STATE_URL`) — and assert identical results.
//!
//! This is the empirical guard the SQLite-only state unit tests can never provide.
//! The state layer speaks a dialect seam (StateRow/StateParam) that ASSUMES the two
//! migration arrays (MIGRATIONS vs PG_MIGRATIONS) are type-compatible. When they drift
//! — the shipped `keyset_range.range_index`/`done` were int4 in PG but bound/read as
//! i64 — the Postgres-state run FAILS or diverges while the SQLite-state run passes.
//! Running each state-exercising fixture on BOTH backends and comparing the output
//! catches that class the moment it reappears (RED against the int4 DDL: the Postgres
//! run aborts at persist_keyset_ranges, so the success assert fails).
//!
//! `RIVET_TEST_STATE_URL=postgresql://user:pass@host:port/db` opts in; absent → skip.
//! The state Postgres is SEPARATE from the source Postgres (source = rivet-postgres,
//! state = a dedicated DB), so a single source engine suffices — the state schema is
//! source-agnostic.

use std::collections::BTreeSet;

use arrow::array::{Array, Int64Array};

use crate::common::*;

/// Read the BIGINT `id` column across every parquet part: (row count, distinct ids).
fn id_count_set(dir: &std::path::Path) -> (usize, BTreeSet<i64>) {
    let mut count = 0usize;
    let mut keys = BTreeSet::new();
    for b in read_all_parts(dir) {
        let id = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        for i in 0..id.len() {
            count += 1;
            keys.insert(id.value(i));
        }
    }
    (count, keys)
}

/// The Postgres STATE backend URL (not the source), or None to skip.
fn pg_state_url() -> Option<String> {
    let u = std::env::var("RIVET_TEST_STATE_URL").ok()?;
    u.starts_with("postgres").then_some(u)
}

/// Seed a fresh `1..=n` BIGINT-keyed source table in the source Postgres and return
/// its fully-qualified name. Dropped by the caller.
fn seed_source(table: &str, n: usize) {
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, {n}) g;"
    ))
    .unwrap();
}

fn drop_source(table: &str) {
    let mut c = pg_connect();
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// Run `build()` once with the default SQLite state and once with RIVET_STATE_URL
/// pointed at Postgres; both must SUCCEED and export byte-identical row sets. `build`
/// yields a FRESH Rig (own tempdir + isolated SQLite state) each call, both reading
/// the same pre-seeded source table.
fn assert_single_run_parity(build: impl Fn() -> Rig, pg_url: &str, expected: usize) {
    // SQLite state — RIVET_STATE_URL pinned EMPTY on the child, not merely
    // absent: the harness only ADDS env (no env_clear), so an ambient
    // RIVET_STATE_URL from the operator's shell would silently switch this
    // leg to Postgres and the parity test would compare Postgres to Postgres
    // (r4 bughunt). Empty fails the starts_with("postgres") check in
    // StateStore::open, forcing the SQLite arm regardless of the shell.
    let s = build();
    let out = s.run_with_env("RIVET_STATE_URL", "");
    assert!(
        out.status.success(),
        "sqlite-state run failed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (sc, sk) = id_count_set(&s.out_dir());
    assert_eq!(sc, expected, "sqlite-state row count must equal the seed");

    // Postgres state — must SUCCEED (the int4 bug aborted at keyset_range persist) and
    // export the SAME rows.
    let p = build();
    let out = p.run_with_env("RIVET_STATE_URL", pg_url);
    assert!(
        out.status.success(),
        "postgres-state run must succeed — a keyset_range schema drift aborts it here.\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (pc, pk) = id_count_set(&p.out_dir());
    assert_eq!(
        pc, sc,
        "postgres-state run must export the SAME row count as sqlite-state"
    );
    assert_eq!(
        pk, sk,
        "the id-sets must be identical across state backends"
    );
}

const N: usize = 3000;

/// Parallel keyset + chunk_checkpoint: the run persists the ranges at open
/// (persist_keyset_ranges) and commits each completed range (commit_keyset_range_at_ref)
/// to the state backend — the two write sites the int4 DDL broke on Postgres. Parity
/// here proves both write paths work identically on SQLite and Postgres state.
fn parallel_checkpoint(rig: Rig) -> Rig {
    rig.mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 500")
}

#[test]
#[ignore = "live: requires docker postgres + RIVET_TEST_STATE_URL (postgres state db)"]
fn state_parity_parallel_keyset_checkpoint() {
    require_alive(LiveService::Postgres);
    let Some(pg) = pg_state_url() else {
        eprintln!("skip: RIVET_TEST_STATE_URL not set");
        return;
    };
    let table = unique_name("sp_ckpt");
    seed_source(&table, N);
    let t = format!("public.{table}");
    assert_single_run_parity(|| parallel_checkpoint(Rig::pg_batch(&t)), &pg, N);
    drop_source(&table);
}

/// Incremental-by-key: the anchor cursor is persisted to the state backend after run 1
/// and read by run 2, which must then add ZERO rows (no keys past the anchor). Exercises
/// the cursor + keyset_range tables on both backends; a Postgres cursor that failed to
/// persist would re-export on run 2 (count doubles), breaking parity.
fn incremental(rig: Rig) -> Rig {
    rig.mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 500")
}

#[test]
#[ignore = "live: requires docker postgres + RIVET_TEST_STATE_URL (postgres state db)"]
fn state_parity_incremental_cursor() {
    require_alive(LiveService::Postgres);
    let Some(pg) = pg_state_url() else {
        eprintln!("skip: RIVET_TEST_STATE_URL not set");
        return;
    };
    let table = unique_name("sp_incr");
    seed_source(&table, N);
    let t = format!("public.{table}");

    // SQLite: run twice; the persisted anchor must make run 2 add zero.
    // RIVET_STATE_URL="" pinned so an ambient value can't switch this "SQLite"
    // leg to Postgres and make the parity vacuous (r6 bughunt; the helper
    // assert_single_run_parity pins it, these hand-rolled twins did not).
    let s = incremental(Rig::pg_batch(&t));
    let sqlite = [("RIVET_STATE_URL", "")];
    assert!(s.run_with_envs(&sqlite).status.success(), "sqlite run 1");
    assert_eq!(id_count_set(&s.out_dir()).0, N, "sqlite run 1 exports all");
    assert!(s.run_with_envs(&sqlite).status.success(), "sqlite run 2");
    let (sc, sk) = id_count_set(&s.out_dir());
    assert_eq!(
        sc, N,
        "sqlite run 2 must add 0 (anchor persisted); {sc} != {N} means the cursor did not persist"
    );

    // Postgres: same sequence, exercising the cursor/keyset_range tables on PG.
    let p = incremental(Rig::pg_batch(&t));
    let r1 = p.run_with_env("RIVET_STATE_URL", &pg);
    assert!(
        r1.status.success(),
        "pg run 1: {}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let r2 = p.run_with_env("RIVET_STATE_URL", &pg);
    assert!(
        r2.status.success(),
        "pg run 2: {}",
        String::from_utf8_lossy(&r2.stderr)
    );
    let (pc, pk) = id_count_set(&p.out_dir());
    assert_eq!(
        pc, sc,
        "postgres incremental must match sqlite (both add 0 on run 2)"
    );
    assert_eq!(
        pk, sk,
        "the id-sets must be identical across state backends"
    );

    drop_source(&table);
}

/// Crash mid-run then resume, on BOTH backends. The crash run persists + commits
/// range 0 to the state backend; the resume run LOADS it (load_keyset_ranges — the
/// int4 bug's panic site) and re-runs the rest. Parity proves crash-recovery recovers
/// the SAME complete row set on SQLite and Postgres state.
#[test]
#[ignore = "live: requires docker postgres + RIVET_TEST_STATE_URL (postgres state db)"]
fn state_parity_parallel_keyset_crash_resume() {
    require_alive(LiveService::Postgres);
    let Some(pg) = pg_state_url() else {
        eprintln!("skip: RIVET_TEST_STATE_URL not set");
        return;
    };
    let table = unique_name("sp_crash");
    seed_source(&table, N);
    let t = format!("public.{table}");
    let panic_at = "keyset_parallel_range_committed:0";

    // SQLite: crash after range 0 commits, then resume. RIVET_STATE_URL=""
    // pinned so an ambient value can't make this the Postgres leg (r6 bughunt).
    let s = parallel_checkpoint(Rig::pg_batch(&t));
    let c1 = s.run_with_envs(&[("RIVET_STATE_URL", ""), ("RIVET_TEST_PANIC_AT", panic_at)]);
    assert!(
        !c1.status.success(),
        "sqlite crash run must fail by injection"
    );
    assert!(
        s.run_with_envs(&[("RIVET_STATE_URL", "")]).status.success(),
        "sqlite resume"
    );
    let (sc, sk) = id_count_set(&s.out_dir());
    assert_eq!(sc, N, "sqlite crash-resume recovers every row");

    // Postgres: crash persists+commits range 0 in PG; resume LOADS it (panic site).
    let p = parallel_checkpoint(Rig::pg_batch(&t));
    let c2 = p.run_with_envs(&[("RIVET_STATE_URL", &pg), ("RIVET_TEST_PANIC_AT", panic_at)]);
    assert!(!c2.status.success(), "pg crash run must fail by injection");
    let r2 = p.run_with_env("RIVET_STATE_URL", &pg);
    assert!(
        r2.status.success(),
        "pg resume must succeed — load_keyset_ranges reads range_index/done.\nstderr: {}",
        String::from_utf8_lossy(&r2.stderr)
    );
    let (pc, pk) = id_count_set(&p.out_dir());
    assert_eq!(
        pc, sc,
        "postgres crash-resume recovers the same rows as sqlite"
    );
    assert_eq!(
        pk, sk,
        "the id-sets must be identical across state backends"
    );

    drop_source(&table);
}
