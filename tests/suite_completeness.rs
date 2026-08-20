//! Guard against the silent-skip footgun of the consolidated test layout.
//!
//! The offline/live suites are single binaries that `#[path]`-include their
//! members from `tests/offline/` and `tests/live/`. A file dropped into one of
//! those directories but NOT registered in its `*_suite.rs` entry compiles and
//! runs as *nothing* — a silent loss of coverage. (Before consolidation cargo
//! auto-discovered every `tests/*.rs`; the entries traded that for a manual
//! `#[path]` line.)
//!
//! This test re-introduces the discovery as an explicit check: the directory
//! listing and the entry's `#[path]` includes must match EXACTLY, so a forgotten
//! include fails loudly here instead of quietly dropping tests. It is a
//! top-level (auto-discovered) test on purpose — so the guard itself can never
//! be the thing that gets silently dropped.

use std::collections::BTreeSet;
use std::fs;

/// `*.rs` file names directly under `tests/<subdir>/`.
fn dir_rs_files(subdir: &str) -> BTreeSet<String> {
    let dir = format!("{}/tests/{subdir}", env!("CARGO_MANIFEST_DIR"));
    let mut out = BTreeSet::new();
    for e in fs::read_dir(&dir).unwrap_or_else(|e| panic!("read dir {dir}: {e}")) {
        let e = e.expect("dir entry");
        let name = e.file_name().to_string_lossy().into_owned();
        // A SUBDIRECTORY here is a layout error, not a thing to skip: nothing
        // under it is ever compiled (cargo builds only what the entry
        // #[path]-includes, and the includes are flat), yet a planted
        // tests/live/<sub>/probe.rs with #[test] fns passed every matrix
        // guard while never running (r4 bughunt, RED-proven live).
        if e.file_type().expect("file type").is_dir() {
            // Data-only subdirs (fixtures/) are fine; a subdir holding .rs is
            // the trap — those files never compile.
            let has_rs = walk_has_rs(&e.path());
            assert!(
                !has_rs,
                "tests/{subdir}/{name}/ contains .rs files — the consolidated \
                 layout is FLAT; files under a subdirectory compile and run as \
                 NOTHING while text-scanning guards still see them"
            );
            continue;
        }
        if name.ends_with(".rs") {
            out.insert(name);
        }
    }
    out
}

/// Any `.rs` anywhere under `dir` (recursive) — the compile-and-run-as-nothing trap.
fn walk_has_rs(dir: &std::path::Path) -> bool {
    let Ok(rd) = fs::read_dir(dir) else {
        return false;
    };
    for e in rd.filter_map(|e| e.ok()) {
        let p = e.path();
        if p.is_dir() {
            if walk_has_rs(&p) {
                return true;
            }
        } else if p.extension().is_some_and(|x| x == "rs") {
            return true;
        }
    }
    false
}

/// File names referenced by `#[path = "<subdir>/<file>.rs"]` lines in the entry.
fn entry_path_includes(entry: &str, subdir: &str) -> BTreeSet<String> {
    let path = format!("{}/tests/{entry}", env!("CARGO_MANIFEST_DIR"));
    let src = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let open = format!("\"{subdir}/");
    src.lines()
        .filter_map(|line| {
            let start = line.find(&open)? + open.len();
            let rest = &line[start..];
            let end = rest.find(".rs\"")? + ".rs".len();
            Some(rest[..end].to_string())
        })
        .collect()
}

fn assert_suite_complete(subdir: &str, entry: &str) {
    let files = dir_rs_files(subdir);
    let included = entry_path_includes(entry, subdir);

    let missing: Vec<&String> = files.difference(&included).collect();
    assert!(
        missing.is_empty(),
        "tests/{subdir}/ has file(s) NOT registered in tests/{entry} — they compile and run as \
         NOTHING (silent coverage loss). Add `#[path = \"{subdir}/<file>\"] mod <name>;`: {missing:?}"
    );

    let dangling: Vec<&String> = included.difference(&files).collect();
    assert!(
        dangling.is_empty(),
        "tests/{entry} #[path]-includes file(s) missing from tests/{subdir}/: {dangling:?}"
    );
}

#[test]
fn offline_suite_registers_every_file() {
    assert_suite_complete("offline", "offline_suite.rs");
}

#[test]
fn live_suite_registers_every_file() {
    assert_suite_complete("live", "live_suite.rs");
}

/// The third consolidated suite — it had NO registration guard (r4 bughunt):
/// a new tests/type_roundtrip/*.rs file compiled and ran as nothing, exactly
/// the footgun this file's header describes.
#[test]
fn type_roundtrip_suite_registers_every_file() {
    assert_suite_complete("type_roundtrip", "type_roundtrip.rs");
}
