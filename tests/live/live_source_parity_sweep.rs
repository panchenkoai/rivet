//! Independent source-parity sweep, wrapped as live tests so CI's `--ignored`
//! run enforces it. The oracle (source direct-query vs DuckDB-over-parquet) does
//! NOT trust rivet's own counters, so it catches silent corruption that a
//! self-oracle (re-reading rivet's output, or its row_count) cannot: row loss,
//! null injection, distinct collapse, decimal precision loss — the class the
//! uuid->null field bug belonged to. The actual sweeps live in `dev/sweep/`
//! (also runnable by hand); these tests just invoke them and fail on any mismatch.
//!
//! Requires the full docker stack + the `duckdb` CLI on PATH (batch: postgres/
//! mysql/mssql; CDC: the `cdc` profile with `rivet` seeded on mssql-cdc).

use std::process::Command;

fn run_sweep(script: &str) {
    let root = env!("CARGO_MANIFEST_DIR");
    let out = Command::new("bash")
        .arg(format!("{root}/dev/sweep/{script}"))
        // Use the binary this test run built, not a possibly-stale target/debug/rivet.
        .env("RIVET", env!("CARGO_BIN_EXE_rivet"))
        .current_dir(root)
        .output()
        .expect("spawn sweep script");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    // exit 2 = environment/setup missing (duckdb CLI not on PATH, rivet not built,
    // a service down). That is NOT a corruption signal — skip rather than fail, so
    // this test is inert in jobs that don't provide the sweep's deps (e.g. the E2E
    // matrix has no host `duckdb` CLI). Corruption is exit 1; a clean run is exit 0.
    if out.status.code() == Some(2) {
        eprintln!("SKIP {script}: sweep dependencies unavailable in this environment:\n{stdout}");
        return;
    }
    assert!(
        !stdout.contains("MISMATCH"),
        "source-parity sweep found a column that diverged from the source \
         (silent corruption):\n{stdout}"
    );
    assert!(
        out.status.success(),
        "source-parity sweep failed (exit {:?}) — corruption or a setup error:\n\
         --- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status.code()
    );
}

#[test]
#[ignore = "live: full docker stack (batch engines) + duckdb CLI on PATH"]
fn source_parity_batch_matches_source_independently() {
    run_sweep("source_parity_sweep.sh");
}

#[test]
#[ignore = "live: cdc docker profile + duckdb CLI on PATH"]
fn source_parity_cdc_matches_source_independently() {
    run_sweep("source_parity_cdc.sh");
}
