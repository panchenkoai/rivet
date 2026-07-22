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

/// `apply --resume` skips an export whose destination already completed
/// (`_SUCCESS`), so a re-run after a partial failure does not redo finished
/// tables. A plain re-run (no `--resume`) DOES re-export — the contrast proves
/// the skip is real, not apply silently never writing.
#[test]
#[ignore = "live: postgres"]
fn resume_skips_completed_exports() {
    require_alive(LiveService::Postgres);

    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let exp = unique_name("orders_done");

    let yaml = format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {exp}
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    wave: 1
    destination: {{ type: local, path: {root} }}
"#,
        root = out.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = |args: &[&str]| {
        std::process::Command::new(RIVET_BIN)
            .arg("apply")
            .arg(cfg.to_str().unwrap())
            .args(args)
            .env("DATABASE_URL", POSTGRES_URL)
            .output()
            .expect("spawn rivet apply")
    };
    let parquet_count = || files_with_extension(out.path(), "parquet").len();

    // Phase 1 — fresh run writes one Parquet + a `_SUCCESS` marker.
    let first = run(&[]);
    assert!(
        first.status.success(),
        "fresh apply must succeed; stderr:\n{}",
        String::from_utf8_lossy(&first.stderr),
    );
    let after_first = parquet_count();
    assert_eq!(after_first, 1, "fresh apply must write exactly one Parquet");

    // Phase 2 — `--resume` sees `_SUCCESS` and SKIPS: no new Parquet, exit 0.
    let resumed = run(&["--resume"]);
    assert!(
        resumed.status.success(),
        "apply --resume must succeed (everything already complete); stderr:\n{}",
        String::from_utf8_lossy(&resumed.stderr),
    );
    assert_eq!(
        parquet_count(),
        after_first,
        "apply --resume must skip the completed export — no new Parquet written",
    );

    // Phase 3 (contrast) — a plain re-run re-exports, appending a second Parquet.
    // Proves the Phase-2 skip is real, not apply simply never writing. The part
    // filename is second-granularity (`<export>_<YYYYMMDD_HHMMSS>.parquet`), so
    // wait past the current second to guarantee a distinct name rather than an
    // overwrite of Phase 1's file (the failure when all three phases land in one
    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).
    let rerun = run(&[]);
    assert!(
        rerun.status.success(),
        "plain re-run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&rerun.stderr),
    );
    assert!(
        parquet_count() > after_first,
        "a plain re-run (no --resume) must re-export, adding a Parquet — \
         confirming --resume's skip was the actual difference",
    );
}

#[test]
#[ignore = "live: postgres"]
fn resume_skips_a_completed_export_with_a_templated_destination() {
    // #4: the apply resume-skip probed the RAW destination, so a templated prefix
    // (`{export}`/`{table}`/`{date}`) never matched a literal `_SUCCESS` path — a
    // completed templated export was never skipped and re-ran into the resume gate
    // (a hard failure / duplicate write). The probe now expands the destination the
    // way `rivet run` does at write time. RED before that expansion.
    require_alive(LiveService::Postgres);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let exp = unique_name("orders_tmpl");
    // Destination path carries the `{export}` token → writes under `<root>/<exp>/`.
    let yaml = format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {exp}
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    wave: 1
    destination: {{ type: local, path: "{root}/{{export}}" }}
"#,
        root = out.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let run = |args: &[&str]| {
        std::process::Command::new(RIVET_BIN)
            .arg("apply")
            .arg(cfg.to_str().unwrap())
            .args(args)
            .env("DATABASE_URL", POSTGRES_URL)
            .output()
            .expect("spawn rivet apply")
    };
    // Detect a skip by the run-unique `manifest-<run_id>.json` copies under the
    // EXPANDED `<root>/<exp>/` prefix: a SKIP writes none, a re-run adds one. This
    // is robust to the shared `orders` fixture's row count (a 0-row run still
    // writes _SUCCESS + a manifest copy, but skip vs re-run is what we assert).
    let dir = out.path().join(&exp);
    let manifest_copies = || {
        std::fs::read_dir(&dir)
            .map(|rd| {
                rd.filter_map(Result::ok)
                    .filter(|e| e.file_name().to_string_lossy().starts_with("manifest-"))
                    .count()
            })
            .unwrap_or(0)
    };

    assert!(
        run(&[]).status.success(),
        "fresh templated apply must succeed"
    );
    assert!(
        dir.join("_SUCCESS").exists(),
        "fresh apply must write _SUCCESS under the expanded <root>/<export>/ prefix"
    );
    let after_first = manifest_copies();
    assert!(
        after_first >= 1,
        "fresh apply must leave a run-unique manifest copy"
    );

    // --resume must SKIP the completed templated export: no new run, exit 0. Pre-fix
    // the raw `{export}` path never matched _SUCCESS, so it re-ran into the resume
    // gate instead of skipping.
    let resumed = run(&["--resume"]);
    assert!(
        resumed.status.success(),
        "apply --resume of a COMPLETED templated export must succeed by skipping it, not re-run \
         into the resume gate; stderr:\n{}",
        String::from_utf8_lossy(&resumed.stderr),
    );
    assert_eq!(
        manifest_copies(),
        after_first,
        "apply --resume must SKIP the completed templated export — no new run-unique manifest copy",
    );

    // Contrast: a plain re-run (no --resume) DOES re-run, adding a manifest copy —
    // proving the skip above was the real difference, not apply never running.
    assert!(run(&[]).status.success(), "plain re-run must succeed");
    assert!(
        manifest_copies() > after_first,
        "a plain re-run (no --resume) must re-export, adding a manifest copy",
    );
}
