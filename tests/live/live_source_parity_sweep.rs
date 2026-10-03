//! Independent source-parity sweep, wrapped as live tests so CI's `--ignored`
//! run enforces it. The oracle (source direct-query vs DuckDB-over-parquet) does
//! NOT trust rivet's own counters, so it catches silent corruption that a
//! self-oracle (re-reading rivet's output, or its row_count) cannot: row loss,
//! null injection, distinct collapse, decimal precision loss — the class the
//! uuid->null field bug belonged to. The sweeps themselves are the Python
//! modules named below (also runnable by hand); these tests just invoke them and
//! fail on any mismatch.
//!
//! Requires the full docker stack + `uv` (the sweeps read through the uv-pinned duckdb
//! package; batch: postgres/mysql/mssql; CDC: the `cdc` profile with `rivet` seeded on mssql-cdc).

use std::process::Command;

/// Run one sweep as `uv run python -m <module> source-parity`, on the uv-pinned duckdb.
///
/// Both sweeps honour `$RIVET`, print `*** MISMATCH ***` per diverging column,
/// and reserve exit 2 for an environment gap — the contract this wrapper reads,
/// asserted in each module's own docstring.
fn run_sweep(module: &str) {
    let root = env!("CARGO_MANIFEST_DIR");
    let out = Command::new("uv")
        .args([
            "run",
            "--frozen",
            "--quiet",
            "python",
            "-m",
            module,
            "source-parity",
        ])
        // Use the binary this test run built, not a possibly-stale target/debug/rivet.
        .env("RIVET", env!("CARGO_BIN_EXE_rivet"))
        .current_dir(root)
        .output()
        .expect("spawn `uv run` for the sweep — install uv (the oracle is pinned by uv.lock)");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    // exit 2 = environment/setup missing (rivet not built, a service down): a skip that names the cause from
    // stderr, never a corruption signal. Corruption is exit 1; a clean run is exit 0.
    if out.status.code() == Some(2) {
        crate::common::skip_live(&format!(
            "{module}: sweep dependencies unavailable in this environment: {}{stdout}",
            stderr.trim()
        ));
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
#[ignore = "live: full docker stack (batch engines) + uv"]
fn source_parity_batch_matches_source_independently() {
    run_sweep("dev.pytools.sweep");
}

#[test]
#[ignore = "live: cdc docker profile + uv"]
fn source_parity_cdc_matches_source_independently() {
    run_sweep("dev.pytools.cdc_soak");
}
