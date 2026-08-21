//! The mutation gate PRIORITISES its mutants — this guards the half that can
//! quietly stop meaning anything.
//!
//! `.github/workflows/ci.yml`'s `mutants-in-diff` job used to grade one
//! undifferentiated pile against one budget, and went quiet exactly where the
//! risk is highest: past `MUTANTS_DIFF_BUDGET` it graded NOTHING, and inside it
//! every MISSED mutant read alike. Measured on the 2026-08-21 refactor branch:
//! 42 mutants in scope, 30 minutes, 16 missed — ONE a real assertion gap (a
//! pure function whose whole-body stub would have let a FAILED run exit 0), 15
//! live-only bodies `cargo mutants -- --lib --bins` cannot kill at all.
//!
//! `.github/scripts/mutants_classify.py` splits them on a MEASURED signal (the
//! offline suite's own llvm-cov function coverage): mutants in executed code
//! are graded and block, mutants in functions measured at ZERO executions are
//! reported. The split is only honest while two properties hold, and both are
//! invisible to the workflow itself:
//!
//! 1. **Everything unknown is GRADED.** No measurement, an unmentioned file, a
//!    line no extent covers, an unparseable name — all stay in the blocking
//!    class. The moment "we did not measure it" starts meaning "it does not
//!    count", this stops being prioritisation and becomes an excuse. The first
//!    two tests here run the real classifier and assert exactly that.
//! 2. **The workflow still CALLS it, and handles every verdict it can produce.**
//!    A classifier nobody invokes is the dead-code-behind-a-green-cell shape
//!    CLAUDE.md names (`verify_blessed_path` was registered `test` for four
//!    engines with no call site anywhere); a state the workflow does not handle
//!    is a verdict that arrives as an empty string and is treated as "fine".
//!
//! WHAT THESE GUARDS CANNOT SEE, said plainly: none of them runs GitHub
//! Actions. They read the workflow's steps and run the classifier; they cannot
//! prove that `cargo llvm-cov` produces a usable report on the CI runner. That
//! failure mode is handled by construction instead of by assertion — the reach
//! step is `continue-on-error` with its own timeout, and an absent report means
//! every mutant is graded, i.e. the gate's behaviour before this existed.
//!
//! The classifier's own classification logic is graded by its `--self-test`,
//! which the first test RUNS (the `Gate scripts (self-tests)` CI job runs it
//! too; this makes it fail locally, before the push).

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const CI: &str = ".github/workflows/ci.yml";
const CLASSIFY: &str = ".github/scripts/mutants_classify.py";

fn root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn classify(args: &[&str]) -> std::process::Output {
    Command::new("python3")
        .arg(CLASSIFY)
        .args(args)
        .current_dir(root())
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "run `python3 {CLASSIFY} {}`: {e} — python3 is a prerequisite of every gate \
                 script in this repo, not a reason to skip",
                args.join(" ")
            )
        })
}

/// A scratch directory for the fixture lists the classifier reads and writes.
///
/// Hand-rolled rather than `tempfile`: this suite links once for ~40 modules
/// and a dependency added for two files is a dependency the whole binary pays
/// for. Named by pid+nanos so parallel runs of this module cannot collide.
fn scratch(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "rivet-mutprio-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// The `run:` scripts of one CI job, joined, with SHELL COMMENTS stripped.
///
/// Two strippings, and both are load-bearing. The YAML parser drops `#`
/// comments between steps; this drops the ones INSIDE a `run: |` block, where
/// the reasoning for each guard lives and quotes the very commands the guard
/// below looks for. A gate satisfiable by a doc comment is not hypothetical
/// here — `runner_frame_gate` was, and passed while grading nothing.
fn job_shell(job: &str, floor: usize) -> String {
    let text = super::nonvacuity::subject_text(CI);
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse {CI}: {e}"));
    let steps = doc["jobs"][job]["steps"].as_sequence().unwrap_or_else(|| {
        panic!(
            "{CI}: job `{job}` has no steps — this guard grades that job's shell, and with \
                the job renamed it would grade nothing. Re-point it at the new name."
        )
    });
    let shell: String = steps
        .iter()
        .filter_map(|s| s.get("run").and_then(|r| r.as_str()))
        .flat_map(|s| s.lines())
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    super::nonvacuity::require_enumerated(
        shell.lines().count(),
        floor,
        &format!("non-comment shell lines in ci.yml's `{job}` job"),
        "The job's steps parsed but its scripts did not — this guard is reading the workflow \
         wrong, and every needle below would miss against an empty haystack.",
    );
    shell
}

/// The classifier's own unit checks — executed, so they have a call site here
/// and not only in CI.
///
/// It grades the shapes the partition must judge (executed function, untaken
/// branch inside one, function measured at zero, unmentioned file, uncovered
/// line, unparseable name), the no-coverage fallback, the exactness of the
/// generated `--exclude-re` patterns, both directions of the post-exclusion
/// verification, and six coverage-report shapes it must REFUSE rather than read
/// as "nothing is covered". Each case is RED-provable against one named mutant
/// in the script; the mutants are listed in its `self_test` docstring.
#[test]
fn the_mutant_prioritiser_passes_its_own_self_test() {
    let out = classify(&["--self-test"]);
    assert!(
        out.status.success(),
        "{CLASSIFY} fails its own self-test:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// WITHOUT a coverage measurement, every mutant is graded.
///
/// This is the property that keeps the split from becoming "it wasn't covered,
/// so it doesn't count": the report-only class may be entered only on positive
/// evidence (a function llvm-cov OBSERVED at zero executions), and every other
/// answer — including "the measurement did not run at all" — falls back to the
/// gate's pre-prioritisation behaviour, where a missed mutant is red.
///
/// Driven through the real script over a fixture list, so it grades the
/// classifier a CI runner would invoke rather than a re-implementation of its
/// rule in this test.
///
/// RED-proven by inverting `partition`'s no-extents arm in
/// `mutants_classify.py` (`return list(mutants), []` -> `return [], list(mutants)`):
/// this test fails with p2=3 while the script's own self-test fails too.
#[test]
fn an_unmeasured_offline_reach_grades_every_mutant() {
    let dir = scratch("unmeasured");
    let list = dir.join("diff-list.txt");
    std::fs::write(
        &list,
        "src/pipeline/run.rs:99:1: replace run_pool -> Result<()> with Ok(())\n\
         src/manifest.rs:10:5: replace * with + in compute_part_checksums\n\
         src/plan/waves.rs:109:38: replace && with || in pack\n",
    )
    .expect("write fixture list");
    let p1 = dir.join("p1.txt");
    let p2 = dir.join("p2.txt");
    let out = classify(&[
        "partition",
        list.to_str().unwrap(),
        "--p1",
        p1.to_str().unwrap(),
        "--p2",
        p2.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "partition failed: {out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        stdout.contains("reach=unmeasured") && stdout.contains("p1=3") && stdout.contains("p2=0"),
        "with no coverage measurement the classifier must put EVERY mutant in the graded \
         class and say so; it reported: {stdout}"
    );
    assert_eq!(
        std::fs::read_to_string(&p1).expect("p1").lines().count(),
        3,
        "the graded class lost a mutant the gate is supposed to block on"
    );
    assert!(
        std::fs::read_to_string(&p2).expect("p2").trim().is_empty(),
        "a mutant was deprioritised with NO measurement behind it — the report-only class may \
         only be entered on an observed zero coverage count"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// WITH a measurement, only an observed ZERO buys the report-only class.
///
/// The three mutants below are the three answers the classifier can reach on a
/// real report, and only one of them is "not graded": a function measured at
/// zero executions. A never-taken BRANCH inside an executed function stays
/// graded (a test can reach it — "you added a path and tested none of it" is
/// the finding this gate exists for), and so does a file the coverage report
/// never mentions.
///
/// RED-proven twice in `mutants_classify.py`: widening the P2 test to
/// `not containing or max(containing) == 0` moves the unmentioned file into the
/// report-only class (p2=2 here), and `max(containing)` -> `min(containing)`
/// moves the covered branch there.
#[test]
fn only_a_function_measured_at_zero_leaves_the_graded_class() {
    let dir = scratch("measured");
    let list = dir.join("diff-list.txt");
    std::fs::write(
        &list,
        "src/a.rs:12:9: replace < with <= in executed_fn\n\
         src/a.rs:41:5: replace never_run -> usize with 0\n\
         src/b.rs:7:1: replace unmentioned -> usize with 0\n",
    )
    .expect("write fixture list");
    let extents = dir.join("fn-extents.tsv");
    // `file<TAB>start<TAB>end<TAB>exec_count`, as `mutants_classify.py reach`
    // writes it from a real `cargo llvm-cov --lib --bins --json` export.
    // The second row is a CLOSURE record nested inside the executed function
    // (llvm-cov emits one per closure, counted separately) — it is what makes
    // "max over every containing extent" observable here rather than assumed.
    std::fs::write(
        &extents,
        "src/a.rs\t10\t20\t7\nsrc/a.rs\t11\t14\t0\nsrc/a.rs\t40\t50\t0\n",
    )
    .expect("write extents");
    let p1 = dir.join("p1.txt");
    let p2 = dir.join("p2.txt");
    let out = classify(&[
        "partition",
        list.to_str().unwrap(),
        "--extents",
        extents.to_str().unwrap(),
        "--p1",
        p1.to_str().unwrap(),
        "--p2",
        p2.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "partition failed: {out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        stdout.contains("reach=measured") && stdout.contains("p1=2") && stdout.contains("p2=1"),
        "expected exactly the zero-coverage function to be deprioritised; got: {stdout}"
    );
    assert_eq!(
        std::fs::read_to_string(&p2).expect("p2").trim(),
        "src/a.rs:41:5: replace never_run -> usize with 0",
        "the report-only class is not the set of functions measured at zero executions"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The classifier is CALLED by the gate — in the job that grades, and in the
/// job that self-tests it.
///
/// Registration is a claim about behaviour; only a call site is evidence
/// (CLAUDE.md, after `blessed_path.verify_blessed_path` sat behind four green
/// matrix cells with no caller in the tree). The needles are matched against
/// the job's `run:` shell with comments stripped, so the long comment blocks
/// above each command — which name these very invocations — cannot satisfy it.
///
/// RED-proven three times, each with the rest of this file staying green:
/// replacing the MEASURED partition branch with `true` (the workflow still
/// calls the classifier and still grades one undifferentiated pile — caught on
/// `--extents fn-extents.tsv`); turning the post-exclusion `verify` into
/// `if false`; and deleting the `--self-test` step from `gate-scripts`.
#[test]
fn the_mutation_gate_actually_invokes_the_prioritiser() {
    let mutate = job_shell("mutants-in-diff", 40);
    for (needle, why) in [
        (
            "mutants_classify.py reach",
            "the offline-reach measurement — without it nothing is measured and the gate \
             cannot tell an assertion gap from a live-only body",
        ),
        (
            "partition diff-list.txt",
            "the classification of THIS diff's mutants",
        ),
        (
            // The MEASURED branch specifically. `partition` is invoked twice —
            // once with the coverage extents and once without — and only the
            // first one prioritises anything; losing it leaves a workflow that
            // still calls the classifier and still grades one undifferentiated
            // pile, which is the defect wearing the fix's clothes.
            "--extents fn-extents.tsv",
            "the classification's only MEASURED input; without it the partition \
             is the no-coverage fallback and nothing is ever prioritised",
        ),
        (
            "verify p1.txt p1-check.txt",
            "the check that the generated exclusions removed exactly the report-only class; \
             without it an over-broad pattern silently un-grades blocking mutants",
        ),
    ] {
        super::nonvacuity::require_needle(
            &mutate,
            "ci.yml's `mutants-in-diff` shell",
            needle,
            1,
            &format!(
                "The mutation gate must invoke `{needle}` — it is {why}. If the command moved, \
                 re-point this guard at it; a prioritiser nobody calls leaves the workflow \
                 grading the old undifferentiated pile while this file reports green."
            ),
        );
    }
    super::nonvacuity::require_needle(
        // Three one-line `run:` steps, hence the floor of 2: the point of a
        // floor is to catch a job whose scripts stopped PARSING (which lands at
        // zero), not to count them — set at 3 it fires on the deleted step
        // before the needle does, and reports "this guard is reading the
        // workflow wrong" about a workflow the guard read correctly.
        &job_shell("gate-scripts", 2),
        "ci.yml's `gate-scripts` shell",
        "mutants_classify.py --self-test",
        1,
        "The classifier's own unit checks must run in CI, not only in this suite.",
    );
}

/// Every verdict the classifier can publish is HANDLED by the workflow.
///
/// The vocabulary is DERIVED from the script (`--states`), never re-typed here:
/// a hand-written list grades only the states its author already knew, which is
/// the defect (CLAUDE.md, "derive the enumerated dimension, never type it in").
/// A state the workflow neither sets nor reads arrives at the blocking verdict
/// job as an empty string, and an empty string is exactly what that job treats
/// as "nothing to worry about".
///
/// RED-proven by adding a state (`"degraded"`) to `REACH_STATES` in
/// `mutants_classify.py` and leaving ci.yml alone.
#[test]
fn every_state_the_prioritiser_can_publish_is_handled_by_the_workflow() {
    let out = classify(&["--states"]);
    assert!(out.status.success(), "--states failed: {out:?}");
    let states: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    super::nonvacuity::require_enumerated(
        states.len(),
        4,
        "states published by mutants_classify.py --states",
        "The vocabulary is read from the script; an empty read would make this guard grade \
         nothing at all.",
    );

    // Both jobs: the run job SETS these, the verdict job READS them.
    let shell = format!(
        "{}\n{}",
        job_shell("mutants-in-diff", 40),
        job_shell("mutants-verdict", 20)
    );
    let unhandled: Vec<&String> = states.iter().filter(|s| !shell.contains(*s)).collect();
    assert!(
        unhandled.is_empty(),
        "these classifier states appear nowhere in the mutation gate's shell: {unhandled:?}. \
         A state the workflow neither publishes nor reads reaches `Mutants (coverage verdict)` \
         as an empty value, which that job reads as 'no concern' — the fail-open shape this \
         whole gate keeps paying for. Teach ci.yml the state, or delete it from the script."
    );
}
