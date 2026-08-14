//! The mutation gate's own exclusion list must not disable the gate.
//!
//! `.cargo/mutants.toml`'s `exclude_re` entries are REGEXES matched against a
//! mutant's name. A single unescaped `|` turns an entry into an alternation
//! with an EMPTY branch, and an empty branch matches every mutant name — so one
//! typo silently excludes the entire corpus while every signal an operator has
//! keeps saying green:
//!
//! ```text
//! "replace && with || in overlay_measured_rows"
//!   => "replace && with " | "" | " in overlay_measured_rows"
//! ```
//!
//! Measured on the tree that shipped it (2026-08-14): 18,670 mutants without
//! the entry, 102 with it — 99.5% of the corpus gone, including EVERY mutant in
//! `src/pipeline/run.rs`, `pool.rs`, `governor.rs` and `src/tuning/adaptive.rs`.
//! The PR gate ran `cargo mutants --in-diff` against a 2,265-line diff, printed
//! `INFO No mutants to filter`, and exited 0 in 23 seconds on all eight CI runs
//! of #227/#229/#230. It is `continue-on-error: true`, so it could not have
//! failed even if it had noticed. The repo's own rule is that a green test which
//! was never RED is unverified; here the machine that checks that rule was the
//! thing running vacuously.
//!
//! This guard is offline and cheap: it never runs cargo-mutants, it only asks
//! whether the exclusion patterns are sane.

use std::path::Path;

fn exclude_patterns() -> Vec<String> {
    let cfg = Path::new(env!("CARGO_MANIFEST_DIR")).join(".cargo/mutants.toml");
    let text = std::fs::read_to_string(&cfg).expect("read .cargo/mutants.toml");
    let mut out = Vec::new();
    let mut in_list = false;
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with("exclude_re") {
            in_list = true;
            continue;
        }
        if in_list {
            if t.starts_with(']') {
                break;
            }
            if let Some(start) = t.find('"')
                && let Some(end) = t.rfind('"')
                && end > start
            {
                out.push(t[start + 1..end].to_string());
            }
        }
    }
    assert!(
        !out.is_empty(),
        "no exclude_re entries parsed — did the config's shape change? This guard \
         must not silently pass by reading nothing."
    );
    out
}

/// No exclusion may match a mutant it was not written for.
///
/// Each pattern is applied to a set of canonical mutant names drawn from
/// unrelated modules. A correctly-scoped entry names one function and matches
/// none of them; an over-broad one (an empty regex alternative, a bare `.*`, a
/// stray `|`) matches them all.
///
/// RED-proven by restoring the unescaped `||` form: this test fails with the
/// offending pattern named, instead of the gate going quietly vacuous.
#[test]
fn exclude_patterns_are_not_over_broad() {
    // Names shaped exactly like cargo-mutants output, from modules no entry in
    // the list should have any opinion about.
    const UNRELATED: &[&str] = &[
        "src/pipeline/run.rs:100:1: replace next_eligible -> Option<usize> with None",
        "src/pipeline/pool.rs:100:1: replace predict_secs -> PredictedFrom with Default::default()",
        "src/tuning/adaptive.rs:100:1: replace GovernorState::observe -> Option<usize> with None",
        "src/pipeline/governor.rs:100:1: replace blind_signal_warning -> Option<String> with None",
        "src/types/target.rs:100:1: replace decimal -> Resolved with Default::default()",
    ];

    let mut offenders = Vec::new();
    for pat in exclude_patterns() {
        let re = match regex::Regex::new(&pat) {
            Ok(re) => re,
            Err(e) => {
                offenders.push(format!("{pat}  — not a valid regex: {e}"));
                continue;
            }
        };
        // An entry that matches the empty string matches every mutant name.
        if re.is_match("") {
            offenders.push(format!(
                "{pat}  — matches the EMPTY string, so it excludes every mutant \
                 (an unescaped `|` is the usual cause: escape it as `\\|`)"
            ));
            continue;
        }
        for name in UNRELATED {
            if re.is_match(name) {
                offenders.push(format!("{pat}  — also matches an unrelated mutant: {name}"));
                break;
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "an exclusion in .cargo/mutants.toml is over-broad — it silently removes \
         mutants it was not written for, and the PR gate then reports green having \
         graded nothing:\n{}",
        offenders.join("\n")
    );
}
