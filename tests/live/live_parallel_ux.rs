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
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut ex = String::new();
    for i in 0..4 {
        ex.push_str(&format!(
            "  - name: missing_{i}\n    query: \"SELECT id FROM rivet_nonexistent_{i}\"\n    \
             mode: full\n    format: parquet\n    destination:\n      type: local\n      \
             path: {}/o{i}\n",
            out.path().display()
        ));
    }
    let yaml = format!("source:\n  type: postgres\n  url: \"{POSTGRES_URL}\"\nexports:\n{ex}");
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--parallel-export-processes",
        ])
        .output()
        .expect("spawn rivet");
    let stderr = String::from_utf8_lossy(&result.stderr);

    assert!(
        !result.status.success(),
        "a batch where every child fails must exit non-zero"
    );
    // 1. heartbeat — silence is now idle-by-design, not unknown.
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
