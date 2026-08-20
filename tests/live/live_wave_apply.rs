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

    let bad = unique_name("bad");
    let good_a = unique_name("orders_w2");
    let good_b = unique_name("users_w3");

    // wave 1 fails (query against a nonexistent table); waves 2/3 are valid
    // tables. Waves are hand-set so apply runs them in order regardless of cost.
    let rig = Rig::pg_batch(&bad)
        .query(&format!("SELECT id FROM no_such_table_{bad}"))
        .source_url_env("DATABASE_URL")
        .export_line("wave: 1")
        .also_export(&good_a, "SELECT id FROM orders")
        .also_export_line("wave: 2")
        .also_export(&good_b, "SELECT id FROM users")
        .also_export_line("wave: 3");
    let cfg = rig.config_path();

    let apply = run_rivet_env(
        &["apply", cfg.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

    // wave 1 failed → apply exits non-zero...
    assert!(
        !apply.status.success(),
        "apply must exit non-zero when an export fails; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr),
    );

    // ...but the later waves still ran: both downstream exports produced Parquet.
    let a_files = files_with_extension(&rig.out_dir_for(&good_a), "parquet");
    let b_files = files_with_extension(&rig.out_dir_for(&good_b), "parquet");
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

    let exp = unique_name("orders_done");

    let rig = Rig::pg_batch(&exp)
        .query("SELECT id FROM orders")
        .source_url_env("DATABASE_URL")
        .export_line("wave: 1");
    let cfg = rig.config_path();

    let run = |args: &[&str]| {
        let mut all = vec!["apply", cfg.to_str().unwrap()];
        all.extend_from_slice(args);
        run_rivet_env(&all, &[("DATABASE_URL", POSTGRES_URL)])
    };
    let parquet_count = || files_with_extension(&rig.out_dir(), "parquet").len();

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
    let exp = unique_name("orders_tmpl");
    // Destination path carries the `{export}` token → writes under `<root>/<exp>/`.
    // The templated SUBPATH is the fixture's subject, so the rig's dest is
    // overridden with it (a PathBuf carries the literal braces fine).
    let rig = Rig::pg_batch(&exp)
        .query("SELECT id FROM orders")
        .source_url_env("DATABASE_URL")
        .export_line("wave: 1")
        .dest_path(out.path().join("{export}"));
    let cfg = rig.config_path();
    let run = |args: &[&str]| {
        let mut all = vec!["apply", cfg.to_str().unwrap()];
        all.extend_from_slice(args);
        run_rivet_env(&all, &[("DATABASE_URL", POSTGRES_URL)])
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

/// `rivet plan` must NOT rewrite a hand-tuned schedule unless asked, and
/// `--annotate-waves` is the asking.
///
/// This is a regression with an incident behind it, recorded on the flag itself:
/// before 0.24.4 a read-only-LOOKING `rivet plan` replaced the operator's
/// `wave:` values, and a 5-per-wave split became one 76-export wave. The pure
/// annotation logic has unit tests (`wave_annotations_insert_replace_and_preserve`),
/// but nothing ran the FLAG: a correct function reached through a wrong wiring is
/// exactly the shape that costs a production schedule, and the unit test cannot
/// see it because the test supplies the input the CLI is meant to supply.
///
/// So both directions go through the real binary and read the CONFIG FILE back:
/// without the flag the hand-set waves must survive byte-for-byte, with it they
/// must change. Asserting only the first would pass on a `plan` that never
/// annotates at all.
///
/// The load-bearing gate is `fields_to_write(&recs, &config, annotate_waves)` —
/// RED-proven by forcing its third argument to `true`, which rewrites the config
/// with no flag and fires the preservation assertion. The OTHER `annotate_waves`
/// branch a reader meets first (`repack_from_history` at plan_cmd.rs:191) is
/// EQUIVALENT for this property: it changes what the recommendation says, not
/// whether anything is written. Named here so the next person mutating this file
/// does not conclude the test is insensitive after trying the wrong one.
#[test]
#[ignore = "live: postgres"]
fn plan_preserves_hand_tuned_waves_unless_annotate_is_asked() {
    require_alive(LiveService::Postgres);
    let a = unique_name("wave_keep_a");
    let b = unique_name("wave_keep_b");

    // Hand-tuned: two exports the planner would happily put in ONE wave, split
    // across two on purpose. That split is the thing an operator owns.
    let rig = Rig::pg_batch(&a)
        .query("SELECT id FROM orders")
        .export_line("wave: 7")
        .export_line("parallel_safe: false")
        .also_export(&b, "SELECT id FROM orders")
        .also_export_line("wave: 9")
        .also_export_line("parallel_safe: false");
    let cfg = rig.config_path();
    let before = std::fs::read_to_string(&cfg).expect("read config");

    let plain = rig.cli(&["plan"]);
    assert!(
        plain.status.success(),
        "rivet plan must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plain.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(&cfg).expect("re-read config"),
        before,
        "`rivet plan` without --annotate-waves must leave the config BYTE-FOR-BYTE alone — \
         it reads as a read-only command, and silently replacing a hand-tuned `wave:` is \
         the 0.24.4 incident this flag was introduced for"
    );

    // …and the flag must actually do the thing, or the assertion above is
    // satisfied by a `plan` that simply never annotates.
    let annotated = rig.cli(&["plan", "--annotate-waves"]);
    assert!(
        annotated.status.success(),
        "rivet plan --annotate-waves must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&annotated.stderr)
    );
    let after = std::fs::read_to_string(&cfg).expect("re-read config after annotate");
    assert_ne!(
        after, before,
        "--annotate-waves must REWRITE the hand-set schedule (waves 7/9 were chosen so any \
         real plan disagrees); an unchanged file means the flag never reached the annotator \
         and the preservation assertion above proves nothing"
    );
}
