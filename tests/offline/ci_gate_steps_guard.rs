//! The semantic gates are STEPS of the `Tests` job, and the service-backed suites steps of
//! the E2E job (one compile, one set of containers), not jobs of their own. A step is easy to drop or to point at the
//! wrong filter in an edit that reads as tidying; this pins each one by its command.

use std::path::Path;

/// (job id, step name, one line its `run:` must hold EXACTLY — a substring would accept `recovery_typo`)
const GATES: &[(&str, &str, &str)] = &[
    (
        "test",
        "Invariant tests (semantic gate)",
        "cargo test --test invariants --test journal_invariants",
    ),
    (
        "test",
        "Recovery tests (semantic gate)",
        "cargo test --test recovery",
    ),
    (
        "test",
        "Compatibility matrix tests (semantic gate)",
        "cargo test --lib -- plan::validate::tests",
    ),
    (
        "test",
        "Type mapping contracts (semantic gate)",
        "cargo test --test type_roundtrip contract_",
    ),
    (
        "test",
        "Stability tests — format & row-group goldens (semantic gate)",
        "cargo test --test format_golden",
    ),
    (
        "test",
        "Stability tests — sink unit tests (semantic gate)",
        "cargo test --lib -- pipeline::sink::tests",
    ),
    (
        "test",
        "Generated docs are in sync (docs-as-code)",
        "python3 -m dev.pytools.docgen --check",
    ),
    (
        "e2e",
        "Type-golden tests (semantic gate)",
        "cargo test --test live_type_golden -- --ignored",
    ),
    (
        "e2e",
        "Type round-trip validators — DuckDB · ClickHouse · pyarrow (semantic gate)",
        "cargo test --test type_roundtrip -- --include-ignored --skip bigquery",
    ),
    (
        "e2e",
        "Differential correctness at scale (DuckDB vs source)",
        "cargo test --test live_differential -- --ignored",
    ),
    (
        "e2e",
        "PR regression matrices (cli + cfg + path)",
        "python3 -m dev.pytools.matrices --tier=pr --skip-compose | tee dev/matrices/run.log",
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
    for (job, name, cmd) in GATES {
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
        {
            assert_eq!(
                step["if"].as_str(),
                Some("${{ !cancelled() }}"),
                "`{name}` must run even after an earlier gate fails, or a red run names only the first"
            );
        }
    }
}
