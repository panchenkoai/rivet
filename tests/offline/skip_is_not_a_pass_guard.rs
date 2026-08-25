//! A runner that cannot run must report `Skipped`, never an empty result.
//!
//! `assert_all_pass` (tests/oracle_fixture_matrix.rs) panics on a `Skipped`
//! outcome — the strength floor — and walks straight through an EMPTY list,
//! because a `for` over nothing executes nothing. So a runner that answers a
//! missing environment with `Ok(Vec::new())` makes every cell that calls it
//! report green while verifying nothing.
//!
//! MEASURED 2026-08-25: all six runners in `tests/harness/mod.rs` did exactly
//! that, and `cargo test --test oracle_fixture_matrix -- --ignored` reported
//! `20 passed` in **0.00s** on a machine with no warehouse credentials — which
//! is every machine, since `RIVET_TEST_GCS_BUCKET` appears in no workflow. The
//! doc comment on `run` had called an empty oracle list a strength-floor failure
//! since it was written.
//!
//! This guard is source-shaped rather than behavioural on purpose: the runners
//! read process environment, and a test that mutates env to observe them races
//! every other test in the binary. What it checks is exactly the shape that
//! failed — an early return of an empty vec from a runner whose result feeds the
//! floor.

use std::path::PathBuf;

fn harness_source() -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/harness/mod.rs");
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The runners are DERIVED from the source, never listed here: a seventh added
/// tomorrow is asked the same question without anyone remembering to add a row.
fn runner_bodies(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = src;
    while let Some(i) = rest.find("    pub fn run") {
        let after = &rest[i + 4..];
        let name: String = after["pub fn ".len()..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        // Body = up to the next `    pub fn ` (or end): enough to see an early
        // return, which is all this guard is about.
        let body_start = i + 4;
        let body_end = after[4..]
            .find("\n    pub fn ")
            .map(|j| body_start + 4 + j)
            .unwrap_or(rest.len());
        out.push((name, rest[body_start..body_end].to_string()));
        rest = &rest[body_end..];
    }
    assert!(
        out.len() >= 6,
        "derived only {} runners from tests/harness/mod.rs — the derivation is broken, \
         and a broken derivation grades nothing while looking green",
        out.len()
    );
    out
}

#[test]
fn no_harness_runner_answers_a_missing_environment_with_an_empty_result() {
    let src = harness_source();
    let mut offenders = Vec::new();
    for (name, body) in runner_bodies(&src) {
        // Only runners whose result feeds the strength floor: the ones returning a
        // list of outcomes. A helper returning something else is not this class.
        if !body.contains("OracleOutcome") {
            continue;
        }
        if body.contains("Ok(Vec::new())") {
            offenders.push(name);
        }
    }
    assert!(
        offenders.is_empty(),
        "these runners return an EMPTY result instead of a `Skipped` outcome: {offenders:?}. \
         `assert_all_pass` panics on Skipped and walks straight through an empty list, so \
         every cell calling one of these reports green while verifying nothing — measured \
         at `20 passed in 0.00s` before this was closed. Return \
         `Ok(vec![(name, OracleOutcome::Skipped {{ why: SKIP_WHY.to_string() }})])`."
    );
}

/// The positive control: the thing this guard looks for must be findable. A grep
/// that matches nothing reads identically to a grep that found no problem — which
/// is the same defect one layer up.
#[test]
fn the_guard_can_actually_see_a_runner_body() {
    let src = harness_source();
    let bodies = runner_bodies(&src);
    let with_outcomes = bodies
        .iter()
        .filter(|(_, b)| b.contains("OracleOutcome"))
        .count();
    assert!(
        with_outcomes >= 6,
        "only {with_outcomes} of {} derived runners mention OracleOutcome — the parse is \
         reading the wrong thing, and the guard above would pass over any input",
        bodies.len()
    );
    assert!(
        bodies.iter().any(|(_, b)| b.contains("SKIP_WHY")),
        "no runner references SKIP_WHY — either the skip path was removed (fine, say so \
         here) or the parse is not reading bodies at all (not fine)"
    );
}
