//! ENVIRONMENT HYGIENE: the harness neither mutates its own process environment
//! nor lets the gate drop a variable the harness reads.
//!
//! Hunt H4 (2026-10-02) measured both halves. `pg_store()` in `live_pg_state.rs`
//! wrapped `StateStore::open` in a process-wide `set_var("RIVET_STATE_URL")`;
//! under libtest's threads a rivet spawned by ANOTHER test in that window
//! inherited the Postgres URL and failed on an empty SQLite file. And the gate
//! merged the whole shell into every cell, so a leftover `RIVET_TEST_PANIC_AT`
//! read as a product panic. The fix strips every inherited `RIVET_*` and keeps an
//! allow-list of the harness's own; this file keeps that list honest.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Every `.rs` file under `dir`, at any depth.
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = vec![];
    for e in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(rust_files(&p));
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out
}

/// Helpers allowed to set a process-wide variable. Empty: a test passes env to the CHILD.
const SET_VAR_ALLOWED: &[&str] = &[];

#[test]
fn no_live_or_common_test_sets_process_env() {
    let mut hits = vec![];
    for dir in ["tests/live", "tests/common"] {
        for f in rust_files(&root().join(dir)) {
            let rel = f.strip_prefix(root()).unwrap().display().to_string();
            if SET_VAR_ALLOWED.contains(&rel.as_str()) {
                continue;
            }
            for (i, line) in std::fs::read_to_string(&f).unwrap().lines().enumerate() {
                if line.contains("set_var(") && !line.trim_start().starts_with("//") {
                    hits.push(format!("{rel}:{}: {}", i + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        hits.is_empty(),
        "a process-wide set_var leaks into every rivet another thread spawns; pass the value to \
         the child (`envs`) or open the store at a StateRef instead:\n{}",
        hits.join("\n")
    );
}

/// No lib test moves `StateStore::open` of every concurrent test onto Postgres by setting `RIVET_STATE_URL`.
#[test]
fn no_lib_test_sets_the_state_url_for_the_process() {
    let hits: Vec<String> = rust_files(&root().join("src"))
        .iter()
        .flat_map(|f| {
            let rel = f.strip_prefix(root()).unwrap().display().to_string();
            std::fs::read_to_string(f)
                .unwrap()
                .lines()
                .enumerate()
                .filter(|(_, l)| l.contains("set_var(\"RIVET_STATE_URL\""))
                .map(|(i, l)| format!("{rel}:{}: {}", i + 1, l.trim()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        hits.is_empty(),
        "open the store at StateRef::Postgres(url) instead; a process-wide RIVET_STATE_URL turned \
         a_garbage_state_db_reports_corruption_not_a_phantom_process red whenever RIVET_TEST_STATE_URL was set:\n{}",
        hits.join("\n")
    );
}

/// The `RIVET_*` names in `HARNESS_ENV` in core.py.
fn gate_allow_list(core: &str) -> BTreeSet<String> {
    let start = core
        .find("HARNESS_ENV: frozenset[str] = frozenset((")
        .expect("HARNESS_ENV in core.py");
    let body = &core[start..];
    let end = body.find("))").expect("HARNESS_ENV closes");
    regex::Regex::new(r#""(RIVET_[A-Z0-9_]+)""#)
        .unwrap()
        .captures_iter(&body[..end])
        .map(|c| c[1].to_string())
        .collect()
}

/// Every `RIVET_*` the gate, the Rust harness and the Makefile's GATE_ENV read from the shell.
fn harness_reads() -> BTreeSet<(String, String)> {
    let py =
        regex::Regex::new(r#"(?:environ(?:\.get)?\(?\[?|getenv\()"(RIVET_[A-Z0-9_]+)""#).unwrap();
    let rs = regex::Regex::new(r#"env::var(?:_os)?\("(RIVET_[A-Z0-9_]+)""#).unwrap();
    let mk = regex::Regex::new(r"(?m)^\s*(RIVET_[A-Z0-9_]+)='").unwrap();
    let mut reads = BTreeSet::new();
    let mut scan = |re: &regex::Regex, f: &Path| {
        let rel = f.strip_prefix(root()).unwrap().display().to_string();
        for c in re.captures_iter(&std::fs::read_to_string(f).unwrap()) {
            reads.insert((c[1].to_string(), rel.clone()));
        }
    };
    for e in std::fs::read_dir(root().join("dev/release_oracle")).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == "py") {
            scan(&py, &p);
        }
    }
    for dir in ["tests/live", "tests/common"] {
        for f in rust_files(&root().join(dir)) {
            scan(&rs, &f);
        }
    }
    scan(&mk, &root().join("Makefile"));
    reads
}

#[test]
fn the_gate_keeps_every_variable_the_harness_reads() {
    let core = std::fs::read_to_string(root().join("dev/release_oracle/core.py")).unwrap();
    let allowed = gate_allow_list(&core);
    let family = regex::Regex::new(r"^RIVET_(CDC|ORACLE)_[A-Z0-9]+_URL$").unwrap();
    assert!(
        core.contains(r#"HARNESS_ENV_FAMILY = re.compile(r"^RIVET_(CDC|ORACLE)_[A-Z0-9]+_URL$")"#)
    );
    let reads = harness_reads();
    assert!(
        reads.len() > 40,
        "the scan found only {} reads; the patterns drifted",
        reads.len()
    );
    let missing: Vec<String> = reads
        .iter()
        // The gate sets RIVET_STATE_URL itself before any cell reads it; a shell copy is the leak.
        .filter(|(name, _)| name != "RIVET_STATE_URL")
        .filter(|(name, _)| !allowed.contains(name) && !family.is_match(name))
        .map(|(name, file)| format!("{name} (read in {file})"))
        .collect();
    assert!(
        missing.is_empty(),
        "scrub_inherited_rivet_env() would drop a variable the harness reads; add it to \
         HARNESS_ENV in dev/release_oracle/core.py:\n{}",
        missing.join("\n")
    );
}
