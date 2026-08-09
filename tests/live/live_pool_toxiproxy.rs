//! The pool scheduler's SEPARATE e2e flow, through toxiproxy (#166 GA).
//!
//! `apply --pool N` on a multi-export config whose source connection runs
//! through a BANDWIDTH-CAPPED proxy — the shared-link shape the field run
//! exposed (60 concurrent exports split one tunnel's ~0.25 MB/s instead of
//! adding throughput). The oracles:
//!
//! 1. CORRECTNESS under the pool: every export's row count is exact — bounded
//!    concurrent slots + the heavy-serialization rule lose nothing on a
//!    congested link.
//! 2. HONESTY of the model: the run prints its predicted makespan up front and
//!    grades itself against the actual at the end (the predicted-vs-actual
//!    line) — the self-correction contract, asserted on the run's own stdout.
//! 3. The heavy-serialization rule holds end-to-end (one non-parallel_safe
//!    export at a time) — asserted structurally by the run COMPLETING with the
//!    heavy primary + safe secondaries mix; the pick rule itself is RED-proven
//!    at the unit level (`pool_next_eligible_serializes_heavies_and_backfills_safe`).

use crate::common::*;

/// ~50 MB of decoded rows for the heavy export, ~1-2 MB per small one, through
/// a 4 MB/s downstream cap: the heavy stream saturates the link long enough
/// that the pool genuinely overlaps work (and the run still finishes in tens of
/// seconds, not minutes).
const HEAVY_ROWS: i64 = 120_000;
const SMALL_ROWS: i64 = 15_000;

#[test]
fn pool_apply_through_a_bandwidth_capped_link_is_exact_and_grades_itself() {
    let _lock = toxiproxy_guard();
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");
    let _bw = toxi_add_bandwidth("postgres", 4_000, "downstream");

    // One heavy primary (parallel_safe ABSENT → not safe: it must serialize
    // with any other heavy) + three cheap safe exports that backfill.
    let rig = Rig::pg_batch("pool_heavy")
        .source_url(POSTGRES_TOXI_URL)
        .query(&format!(
            "SELECT g AS id, md5(g::text) AS a, md5((g*3)::text) AS b, md5((g*7)::text) AS c \
             FROM generate_series(1, {HEAVY_ROWS}) g"
        ))
        .also_export(
            "pool_s1",
            &format!(
                "SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, {SMALL_ROWS}) g"
            ),
        )
        .also_export_line("parallel_safe: true")
        .also_export(
            "pool_s2",
            &format!(
                "SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, {SMALL_ROWS}) g"
            ),
        )
        .also_export_line("parallel_safe: true")
        .also_export(
            "pool_s3",
            &format!(
                "SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, {SMALL_ROWS}) g"
            ),
        )
        .also_export_line("parallel_safe: true");

    let cfg = rig.config_path();
    let out = run_rivet_env(&["apply", cfg.to_str().unwrap(), "--pool", "2"], &[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "pool apply must succeed on a bandwidth-capped link\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // 1. Correctness: exact per-export row counts read back independently.
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()) as i64,
        HEAVY_ROWS,
        "heavy export must be exact under the pool"
    );
    for name in ["pool_s1", "pool_s2", "pool_s3"] {
        assert_eq!(
            duckdb_total_parquet_rows(&rig.out_dir_for(name)) as i64,
            SMALL_ROWS,
            "safe export {name} must be exact under the pool"
        );
    }

    // 2. The model states its claim and grades itself — both lines are part of
    //    the pool's contract, not decoration.
    assert!(
        stdout.contains("predicted makespan"),
        "the pool must state its predicted makespan up front:\n{stdout}"
    );
    assert!(
        stdout.contains("actual makespan"),
        "the pool must grade predicted vs actual at the end:\n{stdout}"
    );

    toxi_reset_toxics("postgres");
}

/// DEFER-NOT-DROP under --resume: a second pool run into the same destinations
/// skips the already-complete exports (their `_SUCCESS` stands) instead of
/// re-exporting — the wave path's resume contract, preserved verbatim by the
/// pool. The union stays exact (no clobber, no double rows).
#[test]
fn pool_apply_resume_skips_complete_exports_and_loses_nothing() {
    let _lock = toxiproxy_guard();
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");

    let rig = Rig::pg_batch("pool_r_heavy")
        .source_url(POSTGRES_TOXI_URL)
        .query("SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, 20000) g")
        .also_export(
            "pool_r_s1",
            "SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, 5000) g",
        )
        .also_export_line("parallel_safe: true");

    let cfg = rig.config_path();
    let first = run_rivet_env(&["apply", cfg.to_str().unwrap(), "--pool", "2"], &[]);
    assert!(
        first.status.success(),
        "first pool run: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let second = run_rivet_env(
        &["apply", cfg.to_str().unwrap(), "--pool", "2", "--resume"],
        &[],
    );
    let s2 = String::from_utf8_lossy(&second.stderr);
    assert!(second.status.success(), "resume pool run: {s2}");
    // Second run skipped both (complete): counts unchanged, no duplication.
    assert_eq!(duckdb_total_parquet_rows(&rig.out_dir()), 20000);
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir_for("pool_r_s1")),
        5000
    );
}
