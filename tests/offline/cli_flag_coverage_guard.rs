//! Every long CLI flag must be exercised somewhere in the test tree — or carry
//! a named exemption with the reason.
//!
//! This was a ONE-OFF sweep on 2026-08-18: deriving the flag list from
//! `src/cli/args.rs` (instead of the list anyone remembered) found EIGHT flags
//! with zero references anywhere, among them `--json-errors` (the machine
//! contract a wrapper parses), `--rollover` (part naming on the CDC CLI, the
//! clobber class), and four init cloud-destination flags. A sweep that found
//! that much is a gate, not a script; this is the sweep, permanent.
//!
//! The exemptions are judgments, not coverage — each carries WHY, and the guard
//! fails if an exempt flag disappears from args.rs (a stale exemption is a list
//! drifting in the other direction).

use std::collections::BTreeSet;

/// Flags deliberately not referenced by the test tree, each with its reason.
const EXEMPT: &[(&str, &str)] = &[
    (
        "tls",
        "needs a TLS-enabled source to mean anything — passing it at a plaintext server \
         proves only that clap accepted the argument (recorded 2026-08-18)",
    ),
    (
        "tls-ca",
        "same fixture gap as --tls: a CA file against a plaintext server exercises nothing",
    ),
    (
        "rivet-bin",
        "contract pinned by `rivet_bin_defaults_to_this_executable_never_a_path_lookup` \
         (src/cli/dispatch.rs, bin crate) on `resolve_rivet_bin` directly; the literal \
         flag only reaches code on a real warehouse load",
    ),
];

/// Every long flag `src/cli/args.rs` declares: `long = "name"` when named,
/// else the kebab-cased field under a bare `long`.
fn declared_long_flags() -> BTreeSet<String> {
    let src = std::fs::read_to_string("src/cli/args.rs").expect("read src/cli/args.rs");
    let mut flags = BTreeSet::new();
    for (start, _) in src.match_indices("#[arg(") {
        let Some(close) = src[start..].find(")]") else {
            continue;
        };
        let attrs = &src[start..start + close];
        if !attrs.contains("long") {
            continue;
        }
        let after = &src[start + close..];
        let name = if let Some(named) = attrs.split("long = \"").nth(1) {
            named.split('"').next().unwrap_or_default().to_string()
        } else {
            // The field name is the first `ident:` after the attribute.
            after
                .lines()
                .map(str::trim)
                .find_map(|l| {
                    let l = l.strip_prefix("pub ").unwrap_or(l);
                    let (ident, _) = l.split_once(':')?;
                    ident
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit())
                        .then(|| ident.replace('_', "-"))
                })
                .unwrap_or_default()
        };
        if !name.is_empty() {
            flags.insert(name);
        }
    }
    flags
}

/// Does `--flag` occur, with a boundary, anywhere under `tests/`? Boundary
/// matters: `--source` must not be satisfied by `--source-env`.
fn referenced_in_tests(haystack: &str, flag: &str) -> bool {
    let needle = format!("--{flag}");
    haystack.match_indices(&needle).any(|(i, _)| {
        haystack[i + needle.len()..]
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '-'))
    })
}

fn test_tree_text() -> String {
    let mut out = String::new();
    let mut stack = vec![std::path::PathBuf::from("tests")];
    while let Some(dir) = stack.pop() {
        if dir.ends_with(".live-tmp") {
            continue;
        }
        for e in std::fs::read_dir(&dir).expect("read tests dir").flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push_str(&std::fs::read_to_string(&p).unwrap_or_default());
            }
        }
    }
    out
}

#[test]
fn every_declared_long_flag_is_tested_or_exempted_with_a_reason() {
    let flags = declared_long_flags();
    assert!(
        flags.len() >= 40,
        "parsed only {} long flags from args.rs — the sweep below is grading a truncated \
         list: {flags:?}",
        flags.len()
    );
    let tests = test_tree_text();
    let exempt: BTreeSet<&str> = EXEMPT.iter().map(|(f, _)| *f).collect();

    let unreferenced: Vec<&String> = flags
        .iter()
        .filter(|f| !exempt.contains(f.as_str()) && !referenced_in_tests(&tests, f))
        .collect();
    assert!(
        unreferenced.is_empty(),
        "long flags with no reference anywhere under tests/ and no exemption: \
         {unreferenced:?}. Either add a test (the 2026-08-18 sweep found a wedge, a \
         clobber and a broken machine contract behind exactly such flags) or add an \
         EXEMPT entry with the reason a test cannot be written."
    );

    // Stale exemptions are the same defect pointed the other way.
    let stale: Vec<&&str> = exempt.iter().filter(|f| !flags.contains(**f)).collect();
    assert!(
        stale.is_empty(),
        "EXEMPT names flags args.rs no longer declares: {stale:?} — delete the entries"
    );
}
