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
