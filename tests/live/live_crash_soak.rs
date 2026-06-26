//! Crash-soak (Tier-1 trust lever): no fault point loses a row.
//!
//! The C1–C4 tests in `live_chunked_recovery.rs` pin SPECIFIC crash scenarios
//! against the state DB. This sweeps the fault matrix and asserts the one
//! invariant operators care about, by RE-READING THE DESTINATION (not rivet's
//! bookkeeping): after a crash at any point + `--resume`, every source row is
//! present at the destination at least once (a complete superset — no silent
//! loss), with at-least-once as the only allowed surplus.
//!
//! For each fault point we run a fresh chunked export, kill it with
//! `RIVET_TEST_PANIC_AT`, resume, and check `distinct ids == source set` AND
//! `physical rows >= source`. A row dropped on the recovery path — the failure
//! mode the state-DB-only assertions cannot see — fails here.
//!
//! Run: `docker compose up -d postgres && \
//!       cargo test --test live_suite -- --ignored`

use crate::common::*;

const N: i64 = 300; // seed_pg_numeric_table → ids 0..299
const CHUNK: i64 = 50; // → 6 chunks, indices 0..5

/// Fault points spanning the write-path boundaries × early/middle/last chunk.
/// `after_chunk_file:N` = file written + manifest recorded, before
/// `complete_chunk_task` (chunk left 'running' → reset+rerun on resume).
/// `after_chunk_complete:N` = chunk fully done, crash before the next.
const FAULTS: &[&str] = &[
    "after_chunk_file:0",
    "after_chunk_file:3",
    "after_chunk_file:5",
    "after_chunk_complete:0",
    "after_chunk_complete:2",
    "after_chunk_complete:5",
];

fn rivet(
    cfg: &std::path::Path,
    export: &str,
    resume: bool,
    panic_at: Option<&str>,
) -> std::process::Output {
    let mut args = vec!["run", "--config", cfg.to_str().unwrap(), "--export", export];
    if resume {
        args.push("--resume");
    }
    let mut cmd = std::process::Command::new(RIVET_BIN);
    cmd.args(&args);
    if let Some(p) = panic_at {
        cmd.env("RIVET_TEST_PANIC_AT", p);
    }
    cmd.output().expect("spawn rivet")
}

fn run_soak_case(fault: &str) {
    let table = seed_pg_numeric_table(N);
    let export = unique_name("soak");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: {CHUNK}
    chunk_checkpoint: true
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Crash mid-run at the fault point.
    let crash = rivet(&cfg, &export, false, Some(fault));
    assert!(
        !crash.status.success(),
        "[{fault}] crash run must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    // Distinct filename timestamps for any re-run within the same second.
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Resume to completion.
    let resume = rivet(&cfg, &export, true, None);
    assert!(
        resume.status.success(),
        "[{fault}] --resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // The invariant: the destination, re-read independently, is a COMPLETE
    // superset of the source — every id present at least once (no loss), with
    // at-least-once duplication the only allowed surplus.
    let ids = dir_parquet_id_set(out.path());
    let expected: std::collections::BTreeSet<i64> = (0..N).collect();
    let missing: Vec<i64> = expected.difference(&ids).copied().collect();
    assert!(
        missing.is_empty(),
        "[{fault}] DATA LOSS after resume: {} source id(s) absent from the destination \
         (first 20: {:?})",
        missing.len(),
        &missing.iter().take(20).collect::<Vec<_>>()
    );
    let physical = total_parquet_rows(out.path()) as i64;
    assert!(
        physical >= N,
        "[{fault}] at-least-once: physical destination rows ({physical}) must be >= source ({N})"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_at_every_fault_point_recovers_complete_destination() {
    require_alive(LiveService::Postgres);
    for fault in FAULTS {
        run_soak_case(fault);
    }
}
