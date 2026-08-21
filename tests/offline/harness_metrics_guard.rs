//! The harness's own THERMOMETER — the numbers must be true even though nothing
//! rides on them.
//!
//! `.github/scripts/harness_metrics.py` publishes one JSON per CI run: how many
//! convention-cop guards this repo holds, how many of them prove their subject
//! is non-empty (`nonvacuity.rs`), how much of each diff `.cargo/mutants.toml`
//! removes, how the offline-reachable and live-only mutant classes are moving,
//! how many tests declare themselves documentation rather than verification.
//! Nobody was writing any of that down, which is how three guards rotted for
//! months with every signal a reader has still reading green.
//!
//! It is not a gate and this file must keep it from becoming one: a metric with
//! teeth is a number people MANAGE (pad the cop count; rename a test
//! `..._documents_...` to duck a red), a metric a human reads is a trend.
//!
//! What is graded here, and why it is worth grading at all:
//!
//! 1. **Unknown is `null`, never `0`.** A run whose mutation job was skipped,
//!    timed out or never started must leave a HOLE in the trend. Published as
//!    `0 missed` it is an unbroken green streak meaning "we measured nothing" —
//!    the same fail-open reading `mutants_classify.py` refuses when a coverage
//!    report is unreadable, one layer further out where nothing fails loudly.
//! 2. **The census is not vacuous.** It is a text scan over `tests/offline/`,
//!    i.e. exactly the fragile class `nonvacuity.rs` exists for; a scan that
//!    stops matching publishes a harness with no guards in it and the chart
//!    just... declines.
//! 3. **The workflow still emits it, and still does not gate on it.**
//!
//! Scope honesty, said plainly: this file cannot run GitHub Actions. It reads
//! the workflow's steps and runs the emitter over fixtures; it cannot prove the
//! artifact upload succeeds on a runner. That failure mode is handled by
//! construction rather than by assertion — the job is `continue-on-error` on
//! top of `if: always()`, so a broken thermometer costs a missing artifact and
//! nothing else.
//!
//! One deviation from the "no subprocess" instinct, stated because it is a real
//! one: the emitter is PYTHON (it must run in a checkout-plus-python3 job with
//! no toolchain — a Rust emitter would make the cheapest job in the file build
//! the whole test tree), so the shaping tests below drive it through `python3`
//! the way `mutation_gate_priority_guard` drives the classifier. What they do
//! NOT do is shell out to `cargo`, `cargo-mutants` or `gh`: every count they
//! grade is a FIXTURE this file wrote.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const CI: &str = ".github/workflows/ci.yml";
const EMITTER: &str = ".github/scripts/harness_metrics.py";
const JOB: &str = "harness-metrics";

fn root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn emitter(args: &[&str]) -> std::process::Output {
    Command::new("python3")
        .arg(EMITTER)
        .args(args)
        .current_dir(root())
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "run `python3 {EMITTER} {}`: {e} — python3 is a prerequisite of every gate \
                 script in this repo, not a reason to skip",
                args.join(" ")
            )
        })
}

/// A scratch directory for the count fixtures this file shapes.
///
/// Hand-rolled rather than `tempfile`, and named by pid+nanos, for the same two
/// reasons `mutation_gate_priority_guard` gives: this suite links once for ~40
/// modules, and parallel runs of the same module must not collide.
fn scratch(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "rivet-harnessmetrics-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Shape one fixture of counts through the emitter's PURE half.
///
/// `shape` reads a JSON of counts and writes the published document: no tree
/// scan, no environment, no clock. So what comes back is a function of the
/// numbers this test wrote and nothing else — the fixture cannot be answered by
/// the state of the repo, which is what makes the expectations below
/// hand-writable.
fn shaped(tag: &str, raw: &str) -> serde_json::Value {
    let dir = scratch(tag);
    let input = dir.join("raw.json");
    let output = dir.join("metrics.json");
    std::fs::write(&input, raw).expect("write the fixture of counts");
    let out = emitter(&[
        "shape",
        "--in",
        input.to_str().unwrap(),
        "--out",
        output.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "`{EMITTER} shape` failed on a fixture it must accept:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let written = std::fs::read_to_string(&output).expect("the shaped document");
    // The document is read from the FILE the emitter wrote (what CI uploads),
    // and the summary from its stdout (what CI prints) — the two artifacts the
    // job actually produces, checked against each other below.
    let doc: serde_json::Value = serde_json::from_str(&written)
        .unwrap_or_else(|e| panic!("`{EMITTER} shape` did not write JSON ({e}):\n{written}"));
    assert_eq!(
        doc["summary"].as_str(),
        Some(String::from_utf8_lossy(&out.stdout).trim()),
        "the line printed into the job log is not the `summary` field of the artifact — the \
         two must be one string, or the log and the trend disagree"
    );
    let _ = std::fs::remove_dir_all(&dir);
    doc
}

/// The published document is exactly the one a human can predict from the
/// counts that went in.
///
/// The oracle is HAND-WRITTEN, not recomputed from the emitter's own arithmetic
/// (CLAUDE.md's self-oracle rule: a test that derives `expected` from the code
/// it guards cannot catch the bug). 2/(2+25) and 13/19 are typed out below
/// because a reader has to be able to check them by eye.
///
/// RED-proven by renaming `subject_proven_rate` in `harness_metrics.py`'s
/// `derived` block: this test fails on the missing key while the emitter's own
/// self-test — which asserts `derived` as a whole — fails too.
#[test]
fn shaping_a_fixture_of_counts_matches_a_hand_written_document() {
    let doc = shaped(
        "full",
        r#"{
          "run": {"repo": "acme/rivet", "run_id": "42", "sha": "deadbeef", "event": "pull_request"},
          "mutants": {"state": "in-budget", "reach": "measured", "report_only_audit": "oracle-holds",
                      "in_scope": 42, "excluded": 8, "graded": 34,
                      "offline_reachable": 27, "live_only": 7,
                      "caught": 25, "missed": 2, "unviable": 0, "timeout": 0},
          "guards": {"guard_files": 40, "convention_cops": 19, "subject_proven_non_empty": 13,
                     "subject_unproven": 6, "documents_only_tests": 6,
                     "files_declaring_blind_spots": 3},
          "tests": {"offline_declared": 285, "lib_declared": 2674}
        }"#,
    );

    assert_eq!(doc["schema"], "rivet-harness-metrics/v1");
    assert_eq!(doc["mutants"]["missed"], 2);
    assert_eq!(doc["mutants"]["live_only"], 7);
    assert_eq!(doc["guards"]["subject_unproven"], 6);
    assert_eq!(doc["tests"]["lib_declared"], 2674);
    // 2 missed of the 27 that reached a verdict; 7 live-only of 34 graded;
    // 13 of 19 cops proving their subject. Rounded to four places by the
    // emitter — the values a chart plots.
    assert_eq!(doc["derived"]["missed_rate"], 0.0741);
    assert_eq!(doc["derived"]["live_only_rate"], 0.2059);
    assert_eq!(doc["derived"]["subject_proven_rate"], 0.6842);
    assert_eq!(
        doc["warnings"].as_array().map(Vec::len),
        Some(0),
        "a self-consistent fixture must produce no warnings; got {:?}",
        doc["warnings"]
    );
    // The one line a human reads in the job log is DERIVED from the document,
    // so the log and the artifact cannot drift apart.
    let summary = doc["summary"].as_str().expect("summary is a string");
    for fragment in [
        "25 caught / 2 missed",
        "13 of 19 convention cops",
        "285 offline + 2674 lib",
    ] {
        assert!(
            summary.contains(fragment),
            "the human summary lost `{fragment}`: {summary}"
        );
    }
}

/// An UNMEASURED count publishes as `null`, and every rate over it as `null`.
///
/// This is the defect the emitter exists to avoid, and it is a fail-OPEN one:
/// a docs-only PR, a skipped mutation job or one killed by its two-hour ceiling
/// all hand this emitter empty strings. Recorded as `0`, they draw a run with
/// zero missed mutants — a perfect score for a run that measured nothing, and
/// on a trend chart the healthiest-looking line in the file. The hole has to
/// stay a hole, in the JSON (`null`) and in the log (`?`).
///
/// RED-proven by making `as_count` return `0` instead of `None` for an unset
/// value in `harness_metrics.py`: this test fails on `mutants.missed` being 0,
/// and the emitter's self-test fails with it.
#[test]
fn an_unmeasured_count_publishes_null_never_zero() {
    let doc = shaped(
        "unknown",
        r#"{
          "mutants": {"state": "", "reach": "", "report_only_audit": "",
                      "in_scope": "", "excluded": "", "graded": "",
                      "offline_reachable": "", "live_only": "",
                      "caught": "", "missed": "", "unviable": "", "timeout": ""},
          "guards": {"guard_files": 40, "convention_cops": 0, "subject_proven_non_empty": 0,
                     "subject_unproven": 0, "documents_only_tests": 6,
                     "files_declaring_blind_spots": 3},
          "tests": {"offline_declared": 285, "lib_declared": 2674}
        }"#,
    );

    for field in [
        "in_scope",
        "excluded",
        "graded",
        "offline_reachable",
        "live_only",
        "caught",
        "missed",
        "unviable",
        "timeout",
        "state",
        "reach",
        "report_only_audit",
    ] {
        assert!(
            doc["mutants"][field].is_null(),
            "`mutants.{field}` came back as {} for a run that measured nothing. Zero is a \
             MEASUREMENT; the trend must show a hole, not a clean sheet.",
            doc["mutants"][field]
        );
    }
    for field in ["missed_rate", "live_only_rate", "subject_proven_rate"] {
        assert!(
            doc["derived"][field].is_null(),
            "`derived.{field}` is {} — a rate over an unknown (or over a zero denominator, as \
             `convention_cops: 0` is here) is not a score, and 0.0 reads as a perfect one",
            doc["derived"][field]
        );
    }
    let summary = doc["summary"].as_str().expect("summary is a string");
    assert!(
        summary.contains("? caught / ? missed") && !summary.contains("0 caught"),
        "the log line must show the hole too: {summary}"
    );
}

/// The emitter's own unit checks — RUN here, so they have a call site outside
/// CI and fail before the push.
///
/// They grade the shaping over fixtures (full document, unknown-is-null, zero
/// denominator, four consistency warnings, five refused input shapes) and the
/// census over a fake tree — including the file that names a repo path only in
/// a COMMENT and must therefore NOT count as a convention cop. That last case
/// is the `runner_frame_gate` defect (a gate satisfiable by a doc comment) one
/// level up, in the thing that counts the gates.
///
/// RED-proven twice against `harness_metrics.py`: making `_uncommented` return
/// its input unchanged (the comment-only file is counted as a cop: 3 != 2), and
/// replacing `census`'s `guards == 0` refusal with a plain return (an empty
/// tree censuses as a harness holding no guards).
#[test]
fn the_emitter_passes_its_own_self_test() {
    let out = emitter(&["--self-test"]);
    assert!(
        out.status.success(),
        "{EMITTER} fails its own self-test:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// The census describes THIS repo, and does not describe a truncated scan of
/// it.
///
/// Floors, not equalities: the numbers are a trend and every ordinary PR moves
/// them. What the floors catch is the scan that stopped matching — which lands
/// near zero, not one short — and `guard_files` is pinned against an
/// INDEPENDENT count taken here, so a census that counts the wrong set of files
/// disagrees with a reader of the same directory.
///
/// RED-proven by dropping the `#[test]` filter from `census` in
/// `harness_metrics.py` (every `.rs` under tests/offline becomes a guard file:
/// 40 counted vs 39 independently counted, and `nonvacuity.rs` — a helper with
/// no test in it — is what the disagreement names).
#[test]
fn the_census_counts_the_guards_this_repo_actually_holds() {
    let out = emitter(&["census"]);
    assert!(
        out.status.success(),
        "`{EMITTER} census` failed on this repo:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let doc: serde_json::Value = serde_json::from_slice(&out.stdout).expect("census JSON");
    let n = |block: &str, field: &str| -> u64 {
        doc[block][field]
            .as_u64()
            .unwrap_or_else(|| panic!("census has no `{block}.{field}`: {doc}"))
    };

    // An independent count of the same subject: every `.rs` under tests/offline
    // that carries a `#[test]`. Taken here rather than read from the census, so
    // the two can disagree — which is the whole point of an oracle.
    let dir = std::path::Path::new(root()).join("tests/offline");
    let mut with_tests = 0;
    for entry in std::fs::read_dir(&dir).expect("read tests/offline") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|e| e == "rs")
            && std::fs::read_to_string(&path)
                .expect("read a guard")
                .contains("#[test]")
        {
            with_tests += 1;
        }
    }
    super::nonvacuity::require_enumerated(
        with_tests,
        25,
        "`#[test]`-bearing files in tests/offline",
        "This test's own oracle read almost nothing — fix the read before believing the \
         comparison below.",
    );
    assert_eq!(
        n("guards", "guard_files"),
        with_tests as u64,
        "the census counts a different set of guard files than a reader of tests/offline \
         does. It is meant to count the files that hold `#[test]`s — a helper module \
         (nonvacuity.rs) is not a guard, and a guard the scan cannot see is a guard missing \
         from every number below."
    );

    // Floors far below today's counts (39 guard files / 19 cops / 13 proven /
    // 285 offline / 2674 lib on 2026-08-21): a scan that lost its subject lands
    // at zero, and these fire there rather than on a normal diff.
    for (block, field, floor) in [
        ("guards", "convention_cops", 8usize),
        ("guards", "subject_proven_non_empty", 5),
        ("tests", "offline_declared", 120),
        ("tests", "lib_declared", 800),
    ] {
        super::nonvacuity::require_enumerated(
            n(block, field) as usize,
            floor,
            &format!("`{block}.{field}` counted by {EMITTER}"),
            "The census scan lost its subject — re-point it at wherever the harness moved to; \
             a metric that silently reports a harness with nothing in it is worse than none.",
        );
    }
    assert!(
        n("guards", "subject_proven_non_empty") <= n("guards", "convention_cops")
            && n("guards", "convention_cops") <= n("guards", "guard_files"),
        "the census contradicts itself: {} proven <= {} cops <= {} guard files does not hold",
        n("guards", "subject_proven_non_empty"),
        n("guards", "convention_cops"),
        n("guards", "guard_files"),
    );
    assert!(
        n("guards", "convention_cops") < n("guards", "guard_files"),
        "EVERY guard file counted as a convention cop — this repo has offline tests that read \
         no checked-in subject at all (config_fuzz, retry_integration, …), so a census that \
         finds a repo path in all of them is matching comments or matching everything"
    );
}

/// The workflow EMITS the metrics — and still does not gate on them.
///
/// Two halves of one property, and both have bitten this repo in other clothes.
/// A script nobody calls is the dead-code-behind-a-green-cell shape
/// (`blessed_path.verify_blessed_path`, registered `test` for four engines with
/// no caller in the tree). A metric that acquires teeth is worse than useless:
/// people optimise the number instead of the harness — a cop count is trivially
/// padded, and `..._documents_...` becomes a way to duck a red rather than an
/// honest label.
///
/// The needles are matched against the job's `run:` shell with comments
/// stripped, because a gate satisfiable by a doc COMMENT is not hypothetical
/// here (`runner_frame_gate` was, and passed while grading nothing).
///
/// RED-proven three times: deleting the `harness_metrics.py emit` step;
/// dropping `continue-on-error: true` from the job; and adding
/// `harness-metrics` to `mutants-verdict`'s `needs:` (the thermometer becomes a
/// dependency of a blocking job, i.e. a gate).
#[test]
fn the_workflow_emits_the_metrics_and_never_gates_on_them() {
    let text = super::nonvacuity::subject_text(CI);
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse {CI}: {e}"));
    let jobs = doc["jobs"].as_mapping().expect("ci.yml has jobs");
    let job = jobs
        .get(serde_yaml_ng::Value::from(JOB))
        .unwrap_or_else(|| {
            panic!(
                "{CI} has no `{JOB}` job — the harness metrics are emitted by nobody, and \
                 every assertion below would grade an absent subject. If the job was renamed, \
                 re-point this guard at it."
            )
        });

    let steps = job["steps"].as_sequence().expect("the job has steps");
    let shell: String = steps
        .iter()
        .filter_map(|s| s.get("run").and_then(|r| r.as_str()))
        .flat_map(|s| s.lines())
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    super::nonvacuity::require_enumerated(
        shell.lines().count(),
        3,
        &format!("non-comment shell lines in ci.yml's `{JOB}` job"),
        "The job parsed but its scripts did not — this guard is reading the workflow wrong, \
         and every needle below would miss against an empty haystack.",
    );
    for (needle, why) in [
        (
            "harness_metrics.py emit",
            "the emitter itself; without it the job publishes nothing and the trend flat-lines \
             at whatever was last uploaded",
        ),
        (
            "::notice::",
            "the one-line human summary in the job log — an artifact nobody opens is a metric \
             nobody reads",
        ),
    ] {
        super::nonvacuity::require_needle(
            &shell,
            &format!("ci.yml's `{JOB}` shell"),
            needle,
            1,
            &format!("The metrics job must run `{needle}` — it is {why}."),
        );
    }
    let uploads = steps.iter().any(|s| {
        s.get("uses")
            .and_then(|u| u.as_str())
            .is_some_and(|u| u.contains("upload-artifact"))
    });
    assert!(
        uploads,
        "ci.yml's `{JOB}` job does not upload its JSON — a metrics file that lives and dies \
         inside one runner is not a trend anybody can read across runs"
    );
    // Its shaping is graded where it is cheap to grade: the gate-scripts job.
    super::nonvacuity::require_needle(
        &text,
        CI,
        "harness_metrics.py --self-test",
        1,
        "The emitter's own unit checks must run in CI too, not only in this suite.",
    );

    // …and the half that keeps it a thermometer.
    assert_eq!(
        job.get("continue-on-error").and_then(|v| v.as_bool()),
        Some(true),
        "ci.yml's `{JOB}` job is not `continue-on-error: true` — a broken thermometer must \
         never hold a merge. The numbers here are a trend, and the moment they can fail a PR \
         people start managing the number instead of the harness."
    );
    let depends_on_metrics: Vec<String> = jobs
        .iter()
        .filter(|(name, _)| name.as_str() != Some(JOB))
        .filter(|(_, spec)| match spec.get("needs") {
            Some(serde_yaml_ng::Value::String(one)) => one == JOB,
            Some(serde_yaml_ng::Value::Sequence(many)) => {
                many.iter().any(|n| n.as_str() == Some(JOB))
            }
            _ => false,
        })
        .map(|(name, _)| name.as_str().unwrap_or("?").to_string())
        .collect();
    assert!(
        depends_on_metrics.is_empty(),
        "{depends_on_metrics:?} depend(s) on `{JOB}`, which makes the thermometer a \
         PRECONDITION of another job. Nothing may block on these numbers: publish them, read \
         them, and gate on the guards themselves."
    );
}
