//! The two CI workflows each hand-maintain a `docker compose … up -d` service
//! list, and hand-maintained twins drift: `mssql-governor` was added to
//! ci.yml's E2E stack on 2026-08-17 and missed in nightly-live.yml, so both
//! governor canaries failed the 08-19 nightly with "requires MssqlGovernor
//! reachable on :1435" — the runner-bypass class, in workflow YAML.
//!
//! The invariant is directional, not equality: the NIGHTLY runs the full live
//! suite (a superset of the E2E matrix), so every service the E2E stack starts
//! must be started by the nightly too. The nightly may start MORE (loaders,
//! mongo variants); the E2E matrix may not need those.

use std::collections::BTreeSet;

/// Every service named across all `docker compose … up -d …` lines of one
/// workflow file — flags (`--wait`, `--profile x`) stripped, the union taken
/// because both files start services across several steps/jobs.
fn compose_services(path: &str) -> BTreeSet<String> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e} (run from the repo root)"));
    let mut out = BTreeSet::new();
    for line in text.lines() {
        let Some(idx) = line.find("up -d") else {
            continue;
        };
        if !line.contains("docker compose") {
            continue;
        }
        let mut words = line[idx + "up -d".len()..].split_whitespace().peekable();
        while let Some(w) = words.next() {
            if w == "--wait" {
                continue;
            }
            if w == "--profile" {
                words.next(); // the profile name is not a service
                continue;
            }
            if w.starts_with("--") || w.starts_with('#') {
                continue;
            }
            out.insert(w.trim_end_matches(&['"', '\''][..]).to_string());
        }
    }
    assert!(
        !out.is_empty(),
        "{path}: no `docker compose … up -d` service lists found — if the startup moved to \
         a script, point this guard at it rather than letting it pass on an empty set"
    );
    out
}

#[test]
fn the_nightly_stack_covers_every_service_the_e2e_stack_starts() {
    let e2e = compose_services(".github/workflows/ci.yml");
    let nightly = compose_services(".github/workflows/nightly-live.yml");
    let missing: Vec<&String> = e2e.difference(&nightly).collect();
    assert!(
        missing.is_empty(),
        "nightly-live.yml does not start {missing:?}, which ci.yml's E2E stack does. The \
         nightly runs the FULL live suite — a superset of the E2E matrix — so a service the \
         E2E tests need is a service the nightly needs too. Add it to the nightly's \
         `docker compose … up -d` line (and its wait/seed steps if ci.yml has them); this \
         exact drift is how both governor canaries failed the 2026-08-19 nightly."
    );
}

#[test]
fn the_guard_reads_real_lists_not_an_empty_parse() {
    // Self-check: the parser must actually extract the services this repo is
    // known to start, or the subset assertion above is vacuously green.
    let e2e = compose_services(".github/workflows/ci.yml");
    for known in ["postgres", "mysql", "mssql", "mssql-governor", "minio"] {
        assert!(
            e2e.contains(known),
            "parser lost `{known}` from ci.yml — the subset check above is grading a \
             truncated list; parsed: {e2e:?}"
        );
    }
}
