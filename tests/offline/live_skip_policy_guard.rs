//! ONE skip policy for the live suite across its CI runners.
//!
//! A live test's `#[ignore = "..."]` reason says who runs it: a `gate-only` flag in the
//! prefix (`live+gate-only: ...`) means no CI job holds its topology and the release gate is
//! its only runner; every other live_suite test runs in the nightly (`live-full`, or
//! `mongo-versions` for a test whose path names mongo) and, minus the named E2E-only cuts,
//! in the PR E2E job. The CI filters are substring lists kept by hand in two workflow files;
//! these tests grade them against the markers, in both directions: a gate-only test that a
//! CI filter still selects (the `*_failover_to_the_standby` red run), and a filter term that
//! drops a test CI could run (`--skip mongo` took 14 live_suite tests from every CI job,
//! `--skip validates` took two Postgres audit tests by a name collision).

use std::path::{Path, PathBuf};

const CI: &str = ".github/workflows/ci.yml";
const NIGHTLY: &str = ".github/workflows/nightly-live.yml";

/// Tests the E2E job skips although the nightly runs them, each with its reason.
const E2E_ONLY_SKIPS: &[(&str, &str)] = &[(
    "content_export",
    "the 50K-row content_items fixture takes minutes to seed; the nightly runs it",
)];

struct IgnoredTest {
    path: String,
    reason: String,
}

impl IgnoredTest {
    /// True when the reason's prefix carries the `gate-only` flag (`live+gate-only: ...`).
    fn gate_only(&self) -> bool {
        let prefix = self.reason.split_once(':').map_or("", |(p, _)| p);
        prefix.split('+').any(|f| f == "gate-only")
    }
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Every `#[ignore]`d test fn of one file as `<module>::<fn>` (or a bare `<fn>` without a module).
fn ignored_in(file: &Path, module: Option<&str>) -> Vec<IgnoredTest> {
    let src =
        std::fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
    let mut out = Vec::new();
    let mut reason: Option<String> = None;
    for line in src.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("#[ignore = \"") {
            reason = Some(rest.split('"').next().unwrap_or_default().to_string());
        } else if t == "#[ignore]" {
            reason = Some(String::new());
        } else if let Some(r) = reason.take() {
            let sig = t.strip_prefix("pub ").unwrap_or(t);
            if let Some(name) = sig.strip_prefix("fn ") {
                let name: String = name
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                let path = module.map_or(name.clone(), |m| format!("{m}::{name}"));
                out.push(IgnoredTest { path, reason: r });
            } else {
                reason = Some(r); // another attribute or a doc line sits between
            }
        }
    }
    out
}

/// `*.rs` files directly under `tests/<subdir>`, sorted.
fn rs_files(subdir: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(root().join("tests").join(subdir))
        .unwrap_or_else(|e| panic!("read tests/{subdir}: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.sort();
    files
}

/// Every ignored live_suite test, keyed `<module>::<fn>` (the module is the file stem).
fn live_tests() -> Vec<IgnoredTest> {
    rs_files("live")
        .iter()
        .flat_map(|f| ignored_in(f, Some(f.file_stem().unwrap().to_str().unwrap())))
        .collect()
}

/// Ignored tests of every OTHER test binary, so a filter term aimed at them is not read as stale.
fn other_ignored_paths() -> Vec<String> {
    let mut out: Vec<String> = rs_files("type_roundtrip")
        .iter()
        .flat_map(|f| ignored_in(f, Some(f.file_stem().unwrap().to_str().unwrap())))
        .map(|t| t.path)
        .collect();
    out.extend(
        rs_files("")
            .iter()
            .flat_map(|f| ignored_in(f, None))
            .map(|t| t.path),
    );
    out
}

/// The `run:` text of one step of one job in a workflow file.
fn step_run(workflow: &str, job: &str, step: &str) -> String {
    let text = std::fs::read_to_string(root().join(workflow))
        .unwrap_or_else(|e| panic!("read {workflow}: {e}"));
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse {workflow}: {e}"));
    let steps = doc["jobs"][job]["steps"]
        .as_sequence()
        .unwrap_or_else(|| panic!("{workflow}: no job `{job}` with steps"));
    steps
        .iter()
        .find(|s| s["name"].as_str() == Some(step))
        .unwrap_or_else(|| panic!("{workflow}: job `{job}` lost its `{step}` step"))["run"]
        .as_str()
        .unwrap_or_else(|| panic!("{workflow}: `{step}` has no run:"))
        .to_string()
}

/// True when any `docker compose ... up -d` line of the job's run texts names a `mongo*` service.
fn job_starts_mongo(workflow: &str, job: &str) -> bool {
    let text = std::fs::read_to_string(root().join(workflow)).unwrap();
    let doc: serde_yaml_ng::Value = serde_yaml_ng::from_str(&text).unwrap();
    doc["jobs"][job]["steps"]
        .as_sequence()
        .unwrap_or_else(|| panic!("{workflow}: no job `{job}`"))
        .iter()
        .filter_map(|s| s["run"].as_str())
        .flat_map(|r| r.lines())
        .filter(|l| l.contains("docker compose") && l.contains("up -d"))
        .any(|l| l.split_whitespace().any(|w| w.starts_with("mongo")))
}

/// The substrings after every `--skip` in a libtest command.
fn libtest_skips(run: &str) -> Vec<String> {
    let words: Vec<&str> = run.split_whitespace().collect();
    words
        .windows(2)
        .filter(|w| w[0] == "--skip")
        .map(|w| w[1].to_string())
        .collect()
}

/// The `test(X)` substrings of a nextest `-E` expression, split at its `not`: (selected, excluded).
fn nextest_terms(run: &str) -> (Vec<String>, Vec<String>) {
    let expr = run
        .split_once("-E '")
        .and_then(|(_, rest)| rest.split_once('\''))
        .map(|(e, _)| e)
        .unwrap_or_else(|| panic!("no -E '...' filter in: {run}"));
    let not_at = expr.find("not ").unwrap_or(expr.len());
    let (mut inc, mut exc) = (Vec::new(), Vec::new());
    for (i, _) in expr.match_indices("test(") {
        let term = expr[i + "test(".len()..]
            .split(')')
            .next()
            .unwrap()
            .to_string();
        if i < not_at {
            inc.push(term)
        } else {
            exc.push(term)
        }
    }
    (inc, exc)
}

fn hit(path: &str, terms: &[String]) -> bool {
    terms.iter().any(|t| path.contains(t.as_str()))
}

/// The three CI runners' selection rules, read from the two workflow files.
struct Runners {
    e2e_skips: Vec<String>,
    nightly_excludes: Vec<String>,
    mongo_includes: Vec<String>,
    mongo_excludes: Vec<String>,
}

impl Runners {
    fn load() -> Self {
        let e2e_skips = libtest_skips(&step_run(CI, "e2e", "Run live cargo integration tests"));
        let (nightly_inc, nightly_excludes) =
            nextest_terms(&step_run(NIGHTLY, "live-full", "Run FULL live suite"));
        assert!(
            nightly_inc.is_empty(),
            "live-full selects by exclusion only; got includes {nightly_inc:?}"
        );
        let (mongo_includes, mongo_excludes) =
            nextest_terms(&step_run(NIGHTLY, "mongo-versions", "Run Mongo live tests"));
        assert!(
            e2e_skips.len() >= 10 && nightly_excludes.len() >= 10 && !mongo_includes.is_empty(),
            "the filter parsers read too little: e2e {e2e_skips:?}, nightly {nightly_excludes:?}, mongo {mongo_includes:?}"
        );
        Self {
            e2e_skips,
            nightly_excludes,
            mongo_includes,
            mongo_excludes,
        }
    }
    fn e2e(&self, t: &IgnoredTest) -> bool {
        !hit(&t.path, &self.e2e_skips)
    }
    fn nightly(&self, t: &IgnoredTest) -> bool {
        !hit(&t.path, &self.nightly_excludes)
    }
    fn mongo(&self, t: &IgnoredTest) -> bool {
        hit(&t.path, &self.mongo_includes) && !hit(&t.path, &self.mongo_excludes)
    }
}

fn live_tests_checked() -> Vec<IgnoredTest> {
    let tests = live_tests();
    let gate_only = tests.iter().filter(|t| t.gate_only()).count();
    assert!(
        tests.len() > 1000 && gate_only >= 10,
        "the live-test parser read too little: {} tests, {gate_only} gate-only",
        tests.len()
    );
    tests
}

/// A `gate-only` test is one no CI job can run: every CI filter must exclude it, or CI goes red on topology.
#[test]
fn a_gate_only_live_test_is_skipped_by_every_ci_runner() {
    let r = Runners::load();
    let leaks: Vec<String> = live_tests_checked()
        .iter()
        .filter(|t| t.gate_only())
        .filter_map(|t| {
            let by: Vec<&str> = [
                (r.e2e(t), "ci.yml e2e"),
                (r.nightly(t), "nightly live-full"),
                (r.mongo(t), "nightly mongo-versions"),
            ]
            .into_iter()
            .filter(|(sel, _)| *sel)
            .map(|(_, who)| who)
            .collect();
            (!by.is_empty()).then(|| {
                format!(
                    "{} (reason: {}) is selected by {}",
                    t.path,
                    t.reason,
                    by.join(", ")
                )
            })
        })
        .collect();
    assert!(
        leaks.is_empty(),
        "live tests marked `live+gate-only` that a CI runner still selects — add `--skip <fn>` to the E2E \
         step and `test(<fn>)` to the nightly's exclusion (or `and not` in mongo-versions):\n{}",
        leaks.join("\n")
    );
}

/// Everything CI can hold, the nightly runs: a skipped test without the marker is a silent coverage loss.
#[test]
fn every_other_live_test_runs_in_the_nightly_or_the_mongo_job() {
    let r = Runners::load();
    let dropped: Vec<String> = live_tests_checked()
        .iter()
        .filter(|t| !t.gate_only() && !r.nightly(t) && !r.mongo(t))
        .map(|t| format!("{} (reason: {})", t.path, t.reason))
        .collect();
    assert!(
        dropped.is_empty(),
        "live tests no CI job runs and no marker excuses: either a filter term catches them by a name \
         collision (rename the test or tighten the term), or they need a topology CI cannot hold — then \
         mark them `#[ignore = \"live+gate-only: ...\"]`:\n{}",
        dropped.join("\n")
    );
}

/// The E2E job is the nightly minus the named time cuts; it never drops anything else, and never adds.
#[test]
fn e2e_runs_what_the_nightly_runs_minus_the_named_cuts() {
    let r = Runners::load();
    let cuts: Vec<String> = E2E_ONLY_SKIPS.iter().map(|(s, _)| s.to_string()).collect();
    let drift: Vec<String> = live_tests_checked()
        .iter()
        .filter(|t| !t.gate_only() && r.e2e(t) != r.nightly(t))
        .filter(|t| !(r.nightly(t) && hit(&t.path, &cuts)))
        .map(|t| format!("{}: e2e={} nightly={}", t.path, r.e2e(t), r.nightly(t)))
        .collect();
    assert!(
        drift.is_empty(),
        "the E2E and nightly live filters disagree outside E2E_ONLY_SKIPS {cuts:?}; both lists must \
         name the same tests:\n{}",
        drift.join("\n")
    );
    assert!(
        live_tests_checked().iter().any(|t| hit(&t.path, &cuts)),
        "E2E_ONLY_SKIPS names no live test — a stale cut"
    );
}

/// A stack that starts no mongo service must skip every mongo test; the mongo job must run them.
#[test]
fn a_stack_without_mongo_skips_every_mongo_test() {
    let r = Runners::load();
    let tests = live_tests_checked();
    for (workflow, job, sel) in [
        (CI, "e2e", &r.e2e_skips),
        (NIGHTLY, "live-full", &r.nightly_excludes),
    ] {
        if job_starts_mongo(workflow, job) {
            continue;
        }
        let leaks: Vec<&str> = tests
            .iter()
            .filter(|t| t.path.contains("mongo") && !hit(&t.path, sel))
            .map(|t| t.path.as_str())
            .collect();
        assert!(
            leaks.is_empty(),
            "{workflow} `{job}` starts no mongo service yet selects mongo tests: {leaks:?}"
        );
    }
    let orphans: Vec<String> = tests
        .iter()
        .filter(|t| t.path.contains("mongo") && !t.gate_only() && !r.mongo(t))
        .map(|t| t.path.clone())
        .collect();
    assert!(
        orphans.is_empty(),
        "mongo tests the mongo-versions job does not select (and no marker excuses): {orphans:?}"
    );
}

/// A filter term that matches no ignored test in any binary is a stale line that reads as a decision.
#[test]
fn every_ci_skip_term_still_names_a_test() {
    let r = Runners::load();
    let mut paths: Vec<String> = live_tests_checked().into_iter().map(|t| t.path).collect();
    paths.extend(other_ignored_paths());
    let all: Vec<&String> = [
        &r.e2e_skips,
        &r.nightly_excludes,
        &r.mongo_includes,
        &r.mongo_excludes,
    ]
    .into_iter()
    .flatten()
    .collect();
    let stale: Vec<&String> = all
        .into_iter()
        .filter(|term| !paths.iter().any(|p| p.contains(term.as_str())))
        .collect();
    assert!(
        stale.is_empty(),
        "CI filter terms that match no ignored test under tests/: {stale:?}"
    );
}

/// The parsers against planted text, so a passing policy is a graded one.
#[test]
fn the_filter_parsers_read_what_the_workflows_write() {
    assert_eq!(
        libtest_skips("cargo test -- --ignored --skip a --skip b_c\n  --skip d"),
        ["a", "b_c", "d"]
    );
    let (inc, exc) = nextest_terms("x -E 'not (test(a) or test(b)\n or test(c))'");
    assert!(inc.is_empty() && exc == ["a", "b", "c"], "{inc:?} {exc:?}");
    let (inc, exc) = nextest_terms("-E 'binary(live_suite) and test(mongo) and not (test(z))'");
    assert!(inc == ["mongo"] && exc == ["z"], "{inc:?} {exc:?}");
    let dir = std::env::temp_dir().join(format!("skip_policy_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let f = dir.join("m.rs");
    std::fs::write(&f, "#[test]\n#[ignore = \"live+gate-only: x\"]\n#[should_panic]\nfn a() {}\n/// fn not_me()\n#[test]\n#[ignore]\npub fn b() {}\n#[test]\nfn c() {}\n").unwrap();
    let got = ignored_in(&f, Some("m"));
    let _ = std::fs::remove_dir_all(&dir);
    let seen: Vec<(&str, bool)> = got
        .iter()
        .map(|t| (t.path.as_str(), t.gate_only()))
        .collect();
    assert_eq!(seen, [("m::a", true), ("m::b", false)]);
}
