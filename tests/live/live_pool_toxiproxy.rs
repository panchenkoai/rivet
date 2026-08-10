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
#[ignore = "live: requires docker compose up -d postgres toxiproxy"]
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
#[ignore = "live: requires docker compose up -d postgres toxiproxy"]
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

/// #166 HeavyGuard reset (roast 2026-08-09): two NON-parallel_safe (heavy)
/// exports must BOTH complete through --pool 2 — the second can only be claimed
/// after the first releases heavy_running. A stubbed HeavyGuard::drop or a
/// deleted `!is_parallel_safe` never resets the flag, so the second heavy is
/// never eligible and the run deadlocks (caught here by the union row count,
/// under the test's own wall-clock; the pool has no internal timeout).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pool_apply_two_heavy_exports_both_complete() {
    let _lock = toxiproxy_guard();
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");

    // Two heavy exports (parallel_safe ABSENT → heavy): they must serialize with
    // each other yet both finish. If HeavyGuard never resets, the 2nd starves.
    let rig = Rig::pg_batch("pool_h1")
        .source_url(POSTGRES_TOXI_URL)
        .query("SELECT g AS id, md5(g::text) AS p FROM generate_series(1, 8000) g")
        .also_export(
            "pool_h2",
            "SELECT g AS id, md5(g::text) AS p FROM generate_series(1, 6000) g",
        );
    let cfg = rig.config_path();
    let out = run_rivet_env(&["apply", cfg.to_str().unwrap(), "--pool", "2"], &[]);
    assert!(
        out.status.success(),
        "both heavy exports must complete (a leaked heavy_running deadlocks #2):\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()) as i64,
        8000,
        "heavy #1"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir_for("pool_h2")) as i64,
        6000,
        "heavy #2 must not starve behind the first"
    );
    toxi_reset_toxics("postgres");
}

/// #167 Slice E: `apply --pool --split` realizes the range split and the union
/// reads back exactly — the completeness oracle the issue names (DuckDB over the
/// shared prefix), no gap or dup at the range seams.
///
/// `advise_split` keys off RECORDED durations, so a FRESH run predicts the same
/// default for every export and nothing dominates — the split only fires once the
/// giant has a measured, dominating wall. So the flow mirrors real use: one
/// `--pool` run PRIMES the durations, the outputs are cleared (durations persist
/// in the state DB next to the config), then `--pool --split` runs. The giant now
/// dominates → splits into N range sub-exports over `id`, all writing to ONE
/// shared prefix under the family name; the union is exact.
///
/// Vacuous-oracle guard (this repo's rule): `union == HEAVY` passes whether or
/// not the split fired — a giant run WHOLE is also complete. So the test also
/// asserts the split ACTUALLY happened: the executor's log line AND range-unit
/// part names (`pool_split_heavy#N`) on disk. RED if `--split` silently no-ops,
/// and RED if any seam drops/dups a row (distinct != HEAVY).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pool_split_realizes_the_range_split_and_the_union_is_exact() {
    const HEAVY: i64 = 200_000;
    const SMALL: i64 = 1_000;

    // The giant: chunked over `id` (splittable — a range key, no `table:` needed),
    // parallel_safe so the split units may run concurrently. The small backfills.
    let rig = Rig::pg_batch("pool_split_heavy")
        .query(&format!(
            "SELECT g AS id, md5(g::text) AS a, md5((g*3)::text) AS b \
             FROM generate_series(1, {HEAVY}) g"
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50000")
        .export_line("parallel_safe: true")
        .also_export(
            "pool_split_small",
            &format!("SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, {SMALL}) g"),
        )
        .also_export_line("parallel_safe: true");
    let cfg = rig.config_path();

    // 1. Prime the durations — no split. The giant records a wall that dominates
    //    the small (200k wide rows vs 1k), which is what `advise_split` reads next.
    let prime = run_rivet_env(&["apply", cfg.to_str().unwrap(), "--pool", "2"], &[]);
    assert!(
        prime.status.success(),
        "priming run must succeed:\n{}",
        String::from_utf8_lossy(&prime.stderr)
    );

    // 2. Clear the outputs so the split run writes into clean prefixes. The state
    //    DB (durations) lives next to the config, not under these dirs, so it
    //    survives — the giant still reads as dominating on the next run.
    for d in [rig.out_dir(), rig.out_dir_for("pool_split_small")] {
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
    }

    // 3. The split run.
    let out = run_rivet_env(
        &["apply", cfg.to_str().unwrap(), "--pool", "2", "--split"],
        &[],
    );
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "split run must succeed:\n{log}");

    // The split ACTUALLY fired (else the union oracle below is vacuous): the
    // executor said so, AND range-unit parts landed on disk.
    assert!(
        log.contains("split 'pool_split_heavy' into"),
        "the executor must report realizing the split:\n{log}"
    );
    let giant_range_parts: Vec<_> = files_with_extension(&rig.out_dir(), "parquet")
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains("pool_split_heavy#"))
        })
        .collect();
    assert!(
        !giant_range_parts.is_empty(),
        "range sub-export parts (pool_split_heavy#N) must be on disk — the split reached the writer"
    );

    // 4. The union is exact: every key once, no gap/dup at the range seams.
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()) as i64,
        HEAVY,
        "the split union must read back every row of the giant exactly"
    );
    let ids = duckdb_dir_parquet_id_set(&rig.out_dir());
    assert_eq!(
        ids.len() as i64,
        HEAVY,
        "distinct ids must equal HEAVY — a seam that drops or duplicates a key fails here"
    );
    assert_eq!(
        *ids.iter().next().unwrap(),
        1,
        "the floor key must be present"
    );
    assert_eq!(
        *ids.iter().next_back().unwrap(),
        HEAVY,
        "the ceil key must be present (last window has no upper bound)"
    );

    // 5. Only the DOMINATING export is split — the small one runs whole.
    let small_split: Vec<_> = files_with_extension(&rig.out_dir_for("pool_split_small"), "parquet")
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains('#'))
        })
        .collect();
    assert!(
        small_split.is_empty(),
        "the non-dominating export must not be split"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir_for("pool_split_small")) as i64,
        SMALL,
        "the small export must still be exact"
    );
}
