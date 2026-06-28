//! LIVE: the wave-ordered `rivet apply <config>` executor (Postgres).
//!
//! Tables are independent, so a failing export in an early wave must NOT block
//! later waves: `apply` collects the failure, runs every other export, and exits
//! non-zero — the continue/isolate policy. This proves that end to end.
//!
//! Harness mirrors the other live suites: `use crate::common::*;`,
//! `#[ignore = "live: postgres"]`, drive the real binary, assert on exit + files.

use crate::common::*;

/// A failing export in wave 1 must not stop waves 2 and 3: apply exits non-zero,
/// but both downstream exports still produce their Parquet (continue/isolate).
#[test]
#[ignore = "live: postgres"]
fn wave_failure_isolates_later_waves() {
    require_alive(LiveService::Postgres);

    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let bad = unique_name("bad");
    let good_a = unique_name("orders_w2");
    let good_b = unique_name("users_w3");

    // wave 1 fails (query against a nonexistent table); waves 2/3 are valid
    // tables. Waves are hand-set so apply runs them in order regardless of cost.
    let yaml = format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {bad}
    query: "SELECT id FROM no_such_table_{bad}"
    mode: full
    format: parquet
    wave: 1
    destination: {{ type: local, path: {root}/{bad} }}
  - name: {good_a}
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    wave: 2
    destination: {{ type: local, path: {root}/{good_a} }}
  - name: {good_b}
    query: "SELECT id FROM users"
    mode: full
    format: parquet
    wave: 3
    destination: {{ type: local, path: {root}/{good_b} }}
"#,
        root = out.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let apply = std::process::Command::new(RIVET_BIN)
        .args(["apply", cfg.to_str().unwrap()])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet apply");

    // wave 1 failed → apply exits non-zero...
    assert!(
        !apply.status.success(),
        "apply must exit non-zero when an export fails; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr),
    );

    // ...but the later waves still ran: both downstream exports produced Parquet.
    let a_files = files_with_extension(&out.path().join(&good_a), "parquet");
    let b_files = files_with_extension(&out.path().join(&good_b), "parquet");
    assert!(
        !a_files.is_empty(),
        "wave 2 export '{good_a}' must produce Parquet despite the wave-1 failure \
         (continue/isolate — independent tables). stderr:\n{}",
        String::from_utf8_lossy(&apply.stderr),
    );
    assert!(
        !b_files.is_empty(),
        "wave 3 export '{good_b}' must produce Parquet despite the wave-1 failure \
         (continue/isolate — independent tables).",
    );
}
