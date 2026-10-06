//! The semantic gates are STEPS of the `Tests` job, the service-backed suites steps of the E2E
//! `pre` leg and the censuses steps of the `E2E matrix` verdict job, not jobs of their own. A step is easy to drop or to point at the
//! wrong filter in an edit that reads as tidying; this pins each one by its command.

use std::path::Path;

const ALWAYS: &str = "${{ !cancelled() }}";
/// The E2E job is a matrix; these run on its `pre` leg only, so once per check.
const PRE_LEG: &str = "${{ !cancelled() && matrix.leg == 'pre' }}";

/// (job id, step name, one line its `run:` must hold EXACTLY — a substring would accept `recovery_typo`, its `if:`)
const GATES: &[(&str, &str, &str, &str)] = &[
    (
        "test",
        "Invariant tests (semantic gate)",
        "cargo test --test invariants --test journal_invariants",
        ALWAYS,
    ),
    (
        "test",
        "Recovery tests (semantic gate)",
        "cargo test --test recovery",
        ALWAYS,
    ),
    (
        "test",
        "Compatibility matrix tests (semantic gate)",
        "cargo test --lib -- plan::validate::tests",
        ALWAYS,
    ),
    (
        "test",
        "Type mapping contracts (semantic gate)",
        "cargo test --test type_roundtrip contract_",
        ALWAYS,
    ),
    (
        "test",
        "Stability tests — format & row-group goldens (semantic gate)",
        "cargo test --test format_golden",
        ALWAYS,
    ),
    (
        "test",
        "Stability tests — sink unit tests (semantic gate)",
        "cargo test --lib -- pipeline::sink::tests",
        ALWAYS,
    ),
    (
        "test",
        "Generated docs are in sync (docs-as-code)",
        "python3 -m dev.pytools.docgen --check",
        ALWAYS,
    ),
    (
        "e2e",
        "Type-golden tests (semantic gate)",
        "cargo test --test live_type_golden -- --ignored",
        PRE_LEG,
    ),
    (
        "e2e",
        "Type round-trip validators — DuckDB · ClickHouse · pyarrow (semantic gate)",
        "cargo test --test type_roundtrip -- --include-ignored --skip bigquery",
        PRE_LEG,
    ),
    (
        "e2e",
        "Differential correctness at scale (DuckDB vs source)",
        "cargo test --test live_differential -- --ignored",
        PRE_LEG,
    ),
    (
        "e2e",
        "PR regression matrices (cli + cfg + path)",
        "python3 -m dev.pytools.matrices --tier=pr --skip-compose | tee dev/matrices/run.log",
        PRE_LEG,
    ),
    (
        "e2e-matrix",
        "Self-skip census",
        "python3 -m dev.release_oracle.skip_census target/rivet-skips.log --lacking \"$LACKING\"",
        ALWAYS,
    ),
    (
        "e2e-matrix",
        "Rig oracle verdict census",
        "python3 -m dev.release_oracle.skip_census --verdicts target/rivet-oracle.log --lane ci",
        ALWAYS,
    ),
];

fn steps_of(ci: &serde_yaml_ng::Value, job: &str) -> Vec<serde_yaml_ng::Value> {
    ci["jobs"][job]["steps"]
        .as_sequence()
        .unwrap_or_else(|| panic!("ci.yml has no job `{job}` with steps"))
        .clone()
}

/// Every former gate job still runs, under its own name, with its own filter.
#[test]
fn every_semantic_gate_is_a_step_with_its_command() {
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join(".github/workflows/ci.yml"),
    )
    .expect("read ci.yml");
    let ci: serde_yaml_ng::Value = serde_yaml_ng::from_str(&text).expect("parse ci.yml");
    for (job, name, cmd, cond) in GATES {
        let steps = steps_of(&ci, job);
        let step = steps
            .iter()
            .find(|s| s["name"].as_str() == Some(*name))
            .unwrap_or_else(|| panic!("job `{job}` lost its `{name}` step"));
        let run = step["run"].as_str().unwrap_or_default();
        assert!(
            run.lines().any(|l| l.trim() == *cmd),
            "`{name}` in job `{job}` no longer runs `{cmd}` — got `{run}`"
        );
        assert_eq!(
            step["if"].as_str(),
            Some(*cond),
            "`{name}` must run even after an earlier gate fails, or a red run names only the first"
        );
    }
}

/// `Some(n)` when `legs` is `pre` once plus every partition `i/n` of ONE n, each once.
fn partitions_of(legs: &[&str]) -> Option<usize> {
    let parts: Vec<&str> = legs.iter().copied().filter(|l| *l != "pre").collect();
    let n = parts.len();
    let mut want: Vec<String> = (1..=n).map(|i| format!("{i}/{n}")).collect();
    let mut got: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
    want.sort();
    got.sort();
    (n > 0 && legs.len() == n + 1 && got == want).then_some(n)
}

/// A leg list short of one partition drops a third of the live suite and stays green.
#[test]
fn the_e2e_legs_are_pre_plus_every_partition_and_the_verdict_job_needs_them() {
    assert_eq!(partitions_of(&["pre", "1/3", "2/3", "3/3"]), Some(3));
    assert_eq!(partitions_of(&["pre", "1/1"]), Some(1));
    for bad in [
        &["pre", "1/3", "2/3"][..],
        &["pre", "1/3", "2/3", "3/4"],
        &["pre", "1/3", "1/3", "3/3"],
        &["1/2", "2/2"],
        &["pre", "pre", "1/1"],
        &["pre"],
    ] {
        assert_eq!(partitions_of(bad), None, "{bad:?}");
    }
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join(".github/workflows/ci.yml"),
    )
    .expect("read ci.yml");
    let ci: serde_yaml_ng::Value = serde_yaml_ng::from_str(&text).expect("parse ci.yml");
    let legs: Vec<String> = ci["jobs"]["e2e"]["strategy"]["matrix"]["leg"]
        .as_sequence()
        .expect("the e2e job has a `leg` matrix")
        .iter()
        .map(|v| v.as_str().expect("a leg is a string").to_string())
        .collect();
    let legs: Vec<&str> = legs.iter().map(String::as_str).collect();
    assert!(
        partitions_of(&legs).is_some(),
        "the e2e legs {legs:?} are not `pre` plus every `i/N` of one N: a partition nobody runs is a silent cut"
    );
    let sweep = steps_of(&ci, "e2e")
        .into_iter()
        .find(|s| s["name"].as_str() == Some("Run live cargo integration tests"))
        .expect("the e2e job runs the live sweep");
    assert_eq!(
        sweep["if"].as_str(),
        Some("${{ !cancelled() && matrix.leg != 'pre' }}"),
        "every partition leg runs the sweep"
    );
    let run = sweep["run"].as_str().unwrap_or_default();
    for flag in [
        "--partition count:${{ matrix.leg }} ",
        "--run-ignored only ",
        "--profile pr ",
    ] {
        assert!(run.contains(flag), "the live sweep lost `{flag}`: {run}");
    }
    let verdict = &ci["jobs"]["e2e-matrix"];
    assert_eq!(
        verdict["name"].as_str(),
        Some("E2E matrix"),
        "branch protection requires this name"
    );
    assert_eq!(verdict["needs"].as_str(), Some("e2e"));
    assert_eq!(verdict["if"].as_str(), Some("${{ always() }}"));
    let first = &steps_of(&ci, "e2e-matrix")[0];
    assert!(
        first["if"].is_null()
            && first["run"]
                .as_str()
                .unwrap_or_default()
                .lines()
                .any(|l| l.trim() == r#"[ "${{ needs.e2e.result }}" = success ]"#),
        "the verdict job must fail unless every leg succeeded"
    );
}
