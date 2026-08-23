//! The release oracle must read what a run DECLARED, not what a bucket HOLDS.
//!
//! Every completeness cell compares a DuckDB read to the SOURCE. Read with a
//! glob, that comparison is satisfied by a run that under-declares its own
//! delivery: the parquet is on disk, the source agrees, and `rivet load` —
//! which is manifest-authoritative — reads short. Measured 2026-08-23 against a
//! product mutant that drops the last part from the manifest while leaving
//! `part_count` and the `_SUCCESS` fingerprint coherent: source 1000, glob 1000
//! (PASSED), manifest-scoped 800 (caught), on local, s3 and gcs alike.
//!
//! So the glob is not banned — it is made a DECISION. The set of glob reads is
//! DERIVED from the code every run; the allowlist below carries only the reason
//! each survivor is one, in the shape the sibling ledgers use.
//!
//! What this guard deliberately does NOT see, said out loud rather than implied:
//!
//! * a glob assembled elsewhere and passed IN as a string — `duckdb_allnull_
//!   columns(path_glob)` reads `read_parquet('{path_glob}')`, whose literal
//!   holds no `*`. That function is allowlisted anyway (a null profile is a
//!   property of the decode path, which every part shares), but a NEW helper
//!   taking a glob parameter would slip past this detector.
//! * a read built by string concatenation across lines.
//!
//! Both are the honest limit of a text scan. It grades the shape that has
//! actually regressed twice, not every shape imaginable.

use std::collections::BTreeSet;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// `(file, enclosing fn, why this one reads the destination rather than the manifest)`
const GLOB_ALLOWLIST: &[(&str, &str, &str)] = &[
    (
        "blessed_path.py",
        "_parquet_rows_and_files",
        "the FILE count is compared to the manifest's `part_count`, so one side must come \
         from the disk or the comparison is the manifest against itself: disk > declared is \
         an undeclared orphan, disk < declared a part never written, and only a disk-side \
         count sees either. The ROW count in the same function IS manifest-scoped.",
    ),
    (
        "rowhash.py",
        "_rows",
        "a VALUE claim (can two different inputs canonicalize to one hash), graded over a \
         prefix this module exports fresh — declared and present coincide, and narrowing \
         the read would only narrow what the injectivity check can see.",
    ),
    (
        "scenarios.py",
        "duckdb_allnull_cloud",
        "an all-null column is a property of the DECODE path and every part a run wrote \
         went through the same decode, so an undeclared orphan is a copy of the same fault \
         and cannot flip the verdict either way. A completeness count is the opposite: \
         there the undeclared part IS the difference between held and delivered.",
    ),
];

/// Every `read_parquet('…*…')` / `read_csv('…*…')` in the release oracle, as
/// `(file, enclosing fn, line)`. The enclosing fn is the nearest preceding
/// `def NAME(` at any indentation — good enough for a flat module, and a miss
/// fails toward reporting `<module>`, which no allowlist entry names.
fn glob_reads() -> Vec<(String, String, usize)> {
    let dir = repo_root().join("dev/release_oracle");
    let mut out = Vec::new();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read dev/release_oracle")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("py"))
        .collect();
    files.sort();
    for path in files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path).expect("read oracle module");
        let mut current = "<module>".to_string();
        for (i, line) in text.lines().enumerate() {
            let t = line.trim_start();
            if let Some(rest) = t.strip_prefix("def ")
                && let Some(paren) = rest.find('(')
            {
                current = rest[..paren].to_string();
            }
            for needle in ["read_parquet('", "read_csv('"] {
                if let Some(at) = line.find(needle) {
                    let tail = &line[at + needle.len()..];
                    if let Some(end) = tail.find('\'')
                        && tail[..end].contains('*')
                    {
                        out.push((name.clone(), current.clone(), i + 1));
                    }
                }
            }
        }
    }
    out
}

#[test]
fn every_glob_read_in_the_release_oracle_is_allowlisted_with_a_reason() {
    let allowed: BTreeSet<(&str, &str)> = GLOB_ALLOWLIST.iter().map(|(f, n, _)| (*f, *n)).collect();
    let mut unlisted = Vec::new();
    for (file, func, line) in glob_reads() {
        if !allowed.contains(&(file.as_str(), func.as_str())) {
            unlisted.push(format!("{file}:{line} in `{func}`"));
        }
    }
    assert!(
        unlisted.is_empty(),
        "these release-oracle reads use a GLOB and are not allowlisted:\n  {}\n\n\
         A glob reads what the destination HOLDS. If the number is compared to the SOURCE \
         (a delivery/completeness claim), scope it to the manifest with \
         `scenarios._declared_read(root, suffix)` — that is the defect this guard exists \
         for. If the read genuinely wants the destination (a decode property, a value \
         claim, an orphan check), add it to GLOB_ALLOWLIST with the reason, the way the \
         three entries there do.",
        unlisted.join("\n  ")
    );
}

#[test]
fn the_glob_allowlist_carries_no_entry_whose_site_is_gone() {
    let present: BTreeSet<(String, String)> =
        glob_reads().into_iter().map(|(f, n, _)| (f, n)).collect();
    let stale: Vec<&str> = GLOB_ALLOWLIST
        .iter()
        .filter(|(f, n, _)| !present.contains(&(f.to_string(), n.to_string())))
        .map(|(f, _, _)| *f)
        .collect();
    assert!(
        stale.is_empty(),
        "GLOB_ALLOWLIST names sites that no longer glob: {stale:?}. An exception that \
         outlived its site is a reason nobody can check — delete the entry (and if the \
         read was scoped to the manifest, that is a win worth banking here, not carrying \
         as a permanent exception)."
    );
}

#[test]
fn every_allowlist_entry_states_a_reason_not_a_label() {
    for (file, func, why) in GLOB_ALLOWLIST {
        assert!(
            why.len() > 80,
            "{file}:{func} is allowlisted with `{why}` — an exception is only honest while \
             it says WHY the destination, not the manifest, is the right subject. A label \
             (\"intentional\", \"value check\") is the thing a reviewer cannot grade."
        );
    }
}

#[test]
fn the_manifest_scoped_helper_is_called_not_merely_defined() {
    // The repo's own rule, paid for by `verify_blessed_path`: registration is a
    // claim about behaviour, only a CALL SITE is evidence. `_declared_read` could
    // sit perfectly written and unused while every cell globbed on.
    let dir = repo_root().join("dev/release_oracle");
    let mut callers = BTreeSet::new();
    for entry in std::fs::read_dir(&dir).expect("read dev/release_oracle") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("py") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path).expect("read oracle module");
        for line in text.lines() {
            let t = line.trim_start();
            if t.starts_with("def _declared_read(") || t.starts_with('#') {
                continue;
            }
            if line.contains("_declared_read(") {
                callers.insert(name.clone());
            }
        }
    }
    for module in ["scenarios.py", "blessed_path.py", "regression.py"] {
        assert!(
            callers.contains(module),
            "{module} claims delivery somewhere but never calls `_declared_read` — either \
             its completeness read went back to a glob, or the helper was renamed and this \
             module was left behind. Callers found: {callers:?}"
        );
    }
}
