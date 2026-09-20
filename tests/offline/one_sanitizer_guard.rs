//! The file-name sanitizer must exist ONCE — a SOURCE lint, because no test of
//! the function's output can see a second implementation of it.
//!
//! History this gate exists for: `src/source/cdc/sink.rs` carried `run_token`, a
//! byte-identical copy of `manifest::file_token`, from the day CDC part naming
//! landed until 2026-09-21. Part names (`cdc-<token>-NNNN`) and the manifest
//! sidecar beside them must sanitize identically, so the two copies agreeing was
//! load-bearing — and nothing graded the pair. `file_token_is_the_one_sidecar_
//! sanitizer` asserts file_token's OUTPUT and is structurally blind to a rival in
//! another file, so its name promised what its body could not check. This lint
//! checks the thing the name claims.

use std::path::Path;

/// The rule, normalised: anything outside `[A-Za-z0-9._-]` folds to `-`.
/// Matched on whitespace-collapsed source so rustfmt cannot hide a copy.
const CHAR_RULE: &str = "is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')";

#[test]
fn the_filename_sanitizer_has_exactly_one_implementation() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    // file -> why it may hold the rule
    let allowed: &[(&str, &str)] = &[("manifest.rs", "file_token IS the sanitizer")];

    let mut found: Vec<String> = Vec::new();
    let mut stack = vec![src.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().is_none_or(|x| x != "rs") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&p) else {
                continue;
            };
            let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
            if flat.contains(CHAR_RULE) {
                let rel = p
                    .strip_prefix(&src)
                    .unwrap_or(&p)
                    .to_string_lossy()
                    .into_owned();
                if !allowed.iter().any(|(f, _)| rel.ends_with(f)) {
                    found.push(rel);
                }
            }
        }
    }

    assert!(
        found.is_empty(),
        "the file-name sanitizer is implemented outside manifest.rs: {found:?}\n\
         Call `crate::manifest::file_token` instead. A second copy agrees with the \
         first only until one of them is edited, and part names must match the \
         sidecar names written beside them.\n\
         If a new file legitimately owns this rule, add it to `allowed` WITH the \
         reason — an unexplained entry is how the last copy survived."
    );
}

/// The lint must be able to FAIL: if `CHAR_RULE` ever stops matching the real
/// implementation (a rustfmt change, an edit to file_token), the test above goes
/// permanently green while guarding nothing — the vacuous-guard shape.
#[test]
fn the_lint_actually_matches_the_canonical_implementation() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/manifest.rs");
    let text = std::fs::read_to_string(&manifest).expect("read src/manifest.rs");
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        flat.contains(CHAR_RULE),
        "CHAR_RULE no longer matches file_token in src/manifest.rs, so the \
         one-implementation lint is grading nothing. Update CHAR_RULE to the \
         current spelling."
    );
}
