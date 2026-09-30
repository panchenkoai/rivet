//! #153: the operator's first two minutes on a new host. A common-mode startup
//! failure (the field: 70+ children, dest creds absent) must NOT be a blank
//! terminal then a flood of identical ✗ cards. Two guarantees, both asserted on
//! the parent's stderr of a batch where EVERY child fails at startup:
//!   1. a heartbeat line appears (spawned N; waiting…) — silence = idle-by-design.
//!   2. ONE representative error excerpt appears before the batch's full dump.

use crate::common::*;

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_apply_common_mode_failure_shows_heartbeat_and_one_representative() {
    require_alive(LiveService::Postgres);
    // Every export points at a NONEXISTENT table → every child fails at startup
    // with the same error class, before any success. 4 exports (>= threshold 3).
    let mut rig = Rig::pg_batch("missing_0").query("SELECT id FROM rivet_nonexistent_0");
    for i in 1..4 {
        rig = rig.also_export(
            &format!("missing_{i}"),
            &format!("SELECT id FROM rivet_nonexistent_{i}"),
        );
    }

    let result = rig.run_args(&["--parallel-export-processes"]);
    let stderr = String::from_utf8_lossy(&result.stderr);

    assert!(
        !result.status.success(),
        "a batch where every child fails must exit non-zero"
    );
    // 1. heartbeat PRESENCE — silence is now idle-by-design, not unknown.
    //    NOTE (roast 2026-08-10): this pins the heartbeat is EMITTED, not the D1
    //    TTY-ordering property (it corrupted the interactive card cursor). This
    //    harness pipes stderr (non-TTY → the linear renderer, no cursor walk), so
    //    it CANNOT reproduce the race. The ordering fix is correct-by-construction
    //    (the eprintln is lexically before the UI-thread spawn); a PTY ordering
    //    assertion is the proper guard and is deferred (needs a pty harness).
    assert!(
        stderr.contains("waiting for the first child event"),
        "#153-1: the parent must emit a spawn heartbeat:\n{stderr}"
    );
    // 2. ONE representative excerpt before the end-of-batch dump.
    assert!(
        stderr.contains("children failed with the same error before any succeeded"),
        "#153-2: a common-mode failure must surface one representative excerpt:\n{stderr}"
    );
}

/// The `▸` progress lines of an unattended run of `rig` at the given interval.
fn progress_lines(rig: &Rig, interval: &str) -> Vec<String> {
    let out = rig.run_with_env("RIVET_PROGRESS_INTERVAL_SECS", interval);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stderr)
        .lines()
        .filter(|l| l.starts_with("▸ "))
        .map(str::to_string)
        .collect()
}

/// A 3000-row table read 100 rows per 100 ms batch, so the export outlives three 1 s intervals.
fn slow_rig(mode: &str, lines: &[&str]) -> (Rig, PgTable) {
    require_alive(LiveService::Postgres);
    let tbl = unique_name("rivet_ux_beat");
    pg_connect()
        .batch_execute(&format!(
            "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT); \
             INSERT INTO {tbl} SELECT g, g FROM generate_series(1, 3000) g"
        ))
        .expect("seed");
    let mut rig = Rig::pg_batch(&tbl)
        .mode(mode)
        .export_line("tuning: {batch_size: 100, throttle_ms: 100}");
    for line in lines {
        rig = rig.export_line(line);
    }
    (rig, PgTable::adopt(tbl))
}

/// A piped run longer than the interval prints a `▸` line carrying a non-zero row count and `label`.
fn assert_progress(rig: &Rig, label: &str) {
    let lines = progress_lines(rig, "1");
    assert!(
        lines
            .iter()
            .any(|l| l.contains(label) && l.contains(" rows") && !l.contains(" 0 rows")),
        "no `▸ … {label} … N rows` line in a run longer than the interval: {lines:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_unattended_run_prints_progress_on_the_single_runner_and_interval_zero_turns_it_off() {
    let (rig, _t) = slow_rig("full", &[]);
    assert_progress(&rig, "streaming");
    assert_eq!(progress_lines(&rig, "0"), Vec::<String>::new());
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_unattended_run_prints_progress_on_the_keyset_runner() {
    let (rig, _t) = slow_rig("chunked", &["chunk_by_key: id", "chunk_size: 1000"]);
    assert_progress(&rig, "streaming");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_unattended_run_prints_progress_on_the_chunked_runner() {
    let (rig, _t) = slow_rig("chunked", &["chunk_column: id", "chunk_size: 1000"]);
    assert_progress(&rig, "chunks");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_unattended_run_prints_progress_on_the_checkpoint_runner() {
    let (rig, _t) = slow_rig(
        "chunked",
        &[
            "chunk_column: id",
            "chunk_size: 1000",
            "chunk_checkpoint: true",
        ],
    );
    assert_progress(&rig, "chunks");
}
