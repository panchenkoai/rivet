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

/// #167 GA-hardening: manifest coherence under the shared prefix.
///
/// A `--split` prefix holds N concurrent run-unique manifest copies of ONE
/// family, each declaring a DISJOINT part set. Three coherence properties, each
/// measured on the real prefix the split writes (not theorized):
///
/// 1. LOAD view (authoritative): the copies all fold to the parent family and
///    their row_counts sum to the whole (`dir_manifest_copy_total_rows`) — so
///    `rivet load`'s cross-run reconcile reads one table with the full count, not
///    N families or one unit's rows. This is the silent-under-count guard.
/// 2. VALIDATE view: `rivet validate` does NOT flag a sibling unit's parts as
///    untracked — before the fix the canonical manifest (last writer) listed one
///    unit and every other unit's parts read as untracked surplus.
/// 3. SAFETY: a genuinely foreign object IS still flagged — the fix only claims
///    SAME-FAMILY siblings, so a real orphan is not masked by the noise removal.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pool_split_prefix_is_manifest_coherent() {
    const HEAVY: i64 = 200_000;
    const SMALL: i64 = 1_000;

    let rig = Rig::pg_batch("mcoh_giant")
        .query(&format!(
            "SELECT g AS id, md5(g::text) AS a, md5((g*3)::text) AS b \
             FROM generate_series(1, {HEAVY}) g"
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50000")
        .export_line("parallel_safe: true")
        .also_export(
            "mcoh_small",
            &format!("SELECT g AS id, md5(g::text) AS payload FROM generate_series(1, {SMALL}) g"),
        )
        .also_export_line("parallel_safe: true");
    let cfg = rig.config_path();

    // Prime durations, clear, then split (advise_split keys off recorded duration).
    let prime = run_rivet_env(&["apply", cfg.to_str().unwrap(), "--pool", "2"], &[]);
    assert!(
        prime.status.success(),
        "prime: {}",
        String::from_utf8_lossy(&prime.stderr)
    );
    for d in [rig.out_dir(), rig.out_dir_for("mcoh_small")] {
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
    }
    let split = run_rivet_env(
        &["apply", cfg.to_str().unwrap(), "--pool", "2", "--split"],
        &[],
    );
    let slog = format!(
        "{}{}",
        String::from_utf8_lossy(&split.stdout),
        String::from_utf8_lossy(&split.stderr)
    );
    assert!(split.status.success(), "split run must succeed:\n{slog}");
    assert!(
        slog.contains("split 'mcoh_giant' into"),
        "the split must have fired (else this test is vacuous):\n{slog}"
    );

    let giant = rig.out_dir();

    // (1) LOAD coherence: >=2 unit copies, all folding to family "mcoh_giant",
    //     summing to the whole. RED if a unit recorded its own name as the family
    //     (the load would then see N families and REFUSE) or if a unit's rows
    //     were dropped from the sum.
    assert!(
        dir_manifest_copy_count(&giant) >= 2,
        "the split must write >=2 run-unique unit manifests, got {}",
        dir_manifest_copy_count(&giant)
    );
    assert_eq!(
        dir_manifest_copy_total_rows(&giant),
        HEAVY,
        "the run-unique manifest copies must sum to the whole table — the cross-run \
         reconcile the loader runs"
    );
    for entry in std::fs::read_dir(&giant).unwrap() {
        let p = entry.unwrap().path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("manifest-") && name.ends_with(".json") {
            let m: serde_json::Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
            assert_eq!(
                m["export_family"].as_str(),
                Some("mcoh_giant"),
                "every split unit copy must fold to the parent family (load sees one table): {name}"
            );
        }
    }

    // (2) VALIDATE coherence: no sibling unit's parts flagged as untracked.
    let v = run_rivet_env(&["validate", "--config", cfg.to_str().unwrap()], &[]);
    let vlog = format!(
        "{}{}",
        String::from_utf8_lossy(&v.stdout),
        String::from_utf8_lossy(&v.stderr)
    );
    assert!(
        !vlog.contains("untracked object: mcoh_giant#"),
        "a split unit's own parts must not be flagged untracked (the merge-back \
         claims same-family siblings):\n{vlog}"
    );

    // (3) SAFETY: a genuinely foreign object under the prefix IS still flagged —
    //     the fix narrows untracked to non-sibling objects, it does not blind it.
    let orphan = giant.join("mcoh_foreign_orphan.parquet");
    std::fs::write(&orphan, b"not a real rivet part").unwrap();
    let v2 = run_rivet_env(&["validate", "--config", cfg.to_str().unwrap()], &[]);
    let vlog2 = format!(
        "{}{}",
        String::from_utf8_lossy(&v2.stdout),
        String::from_utf8_lossy(&v2.stderr)
    );
    assert!(
        vlog2.contains("mcoh_foreign_orphan.parquet"),
        "a true foreign orphan must STILL be flagged untracked — the fix must not \
         mask a real orphan behind the sibling-claim:\n{vlog2}"
    );
    let _ = std::fs::remove_file(&orphan);
}

/// #167 per-unit resume: `apply --pool --split --resume` re-runs only the
/// INCOMPLETE units of a crashed split — no gap (the unfinished window is
/// re-run), no dup (a completed unit is skipped by its manifest copy; a crashed
/// unit resumes its own checkpoint, reusing its run_id so the partial parts are
/// overwritten in place). The two-run crash→resume proof the resume contract
/// demands.
///
/// Run 1 splits and fails via `RIVET_TEST_ERROR_AT=chunk_export:1` (each ~half
/// unit has 2 chunks at chunk_size 75k, so a unit errors on its 2nd chunk). Run 2
/// resumes with no fault and must complete the whole table exactly once.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pool_split_resume_recovers_a_crashed_partial_with_no_gap_or_dup() {
    require_alive(LiveService::Postgres);
    const N: i64 = 300_000;
    let table = unique_name("pr_resume");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} AS SELECT g AS id, md5(g::text) AS a, md5((g*3)::text) AS b \
           FROM generate_series(1, {N}) g;
         ALTER TABLE {table} ADD PRIMARY KEY (id);"
    ))
    .unwrap();

    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 75000")
        .export_line("parallel_safe: true")
        .also_export(
            "pr_resume_small",
            "SELECT g AS id FROM generate_series(1, 100) g",
        )
        .also_export_line("parallel_safe: true");
    let cfg = rig.config_path();

    // Prime durations, clear, so the split fires on run 1.
    assert!(
        run_rivet_env(&["apply", cfg.to_str().unwrap(), "--pool", "2"], &[])
            .status
            .success(),
        "priming run must succeed"
    );
    for d in [rig.out_dir(), rig.out_dir_for("pr_resume_small")] {
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
    }

    // Run 1: split + crash a unit mid-way. The run FAILS, leaving a partial split.
    let crashed = run_rivet_env(
        &["apply", cfg.to_str().unwrap(), "--pool", "2", "--split"],
        &[("RIVET_TEST_ERROR_AT", "chunk_export:1")],
    );
    assert!(
        !crashed.status.success(),
        "run 1 must fail (a unit errored mid-split):\n{}",
        String::from_utf8_lossy(&crashed.stderr)
    );
    // The fixture is not inert: SOME of the giant's parts made it to disk, but not
    // the whole table (a unit crashed).
    let partial = duckdb_total_parquet_rows(&rig.out_dir()) as i64;
    assert!(
        partial < N,
        "run 1 must leave the split INCOMPLETE (a unit crashed), got {partial} of {N}"
    );

    // Run 2: resume, no fault. Must complete the whole table exactly once.
    let resumed = run_rivet_env(
        &[
            "apply",
            cfg.to_str().unwrap(),
            "--pool",
            "2",
            "--split",
            "--resume",
        ],
        &[],
    );
    assert!(
        resumed.status.success(),
        "resume run must succeed:\n{}",
        String::from_utf8_lossy(&resumed.stderr)
    );

    let rows = duckdb_total_parquet_rows(&rig.out_dir()) as i64;
    let ids = duckdb_dir_parquet_id_set(&rig.out_dir());
    assert_eq!(
        rows, N,
        "after resume the giant must hold every row exactly once — no gap (unfinished \
         window re-run) and no dup (completed units skipped, crashed unit's parts \
         overwritten): got {rows}"
    );
    assert_eq!(
        ids.len() as i64,
        N,
        "distinct ids must equal {N} — a resume that duplicated a completed unit's \
         parts, or re-ran with a fresh run_id beside the survivors, fails here"
    );

    let _ = c.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}
