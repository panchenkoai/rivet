//! One rule, one place: a guard must FAIL when its SUBJECT is missing.
//!
//! Every gate in this directory that resolves its subject by NAME — a path, a
//! needle, a YAML key — has two ways to go wrong, and only one of them is
//! visible. It can find the subject and disagree with it (loud: an ordinary
//! failure naming the drift), or it can fail to FIND the subject and grade the
//! empty set instead — at which point every assertion below is vacuously true
//! and the guard reports green while checking nothing.
//!
//! The second is not hypothetical here. `destructive_delete_gate` keyed on an
//! expression shape, the construction moved into a helper, and the gate went on
//! passing over ZERO delete sites; it survives only because it asserts "parsed
//! only {} fn bodies from dispatch.rs — this guard is reading it wrong" before
//! grading anything. `runner_frame_gate` was satisfiable by a doc-COMMENT
//! mention of the call it demanded. An inline census in `src/pipeline/run.rs`
//! stopped pinning what it claimed to. Five multi-agent bughunts over the
//! 2026-08 refactors found ZERO defects in the refactored code and SEVEN in the
//! guards those refactors had to touch — text-matching guards are fragile in
//! exactly this direction, and always in the safe-looking one.
//!
//! So the rule is written ONCE, here, rather than re-typed per guard in
//! whatever wording its author reached for (this repo already had it phrased
//! three different ways in three files). Read the subject through
//! [`subject_text`]; state the floor the code justifies through
//! [`require_enumerated`] / [`require_needle`]. A guard whose subject moves then
//! fails ON THE MISSING SUBJECT, naming what it can no longer find, instead of
//! on some downstream assertion that still happens to hold over nothing.
//!
//! Scope honesty: this makes a guard's BLINDNESS loud. It does not make the
//! guard right — a floor that is met by a subject the guard misreads is still a
//! guard misreading its subject. The floors below are therefore stated with the
//! count the repo holds today, so a truncating parser (which lands somewhere
//! near zero, not one short) is what they catch.

use std::path::PathBuf;

/// The repo root — `CARGO_MANIFEST_DIR` of this test crate.
///
/// Guards in this directory are written to run from the root, and most read
/// bare relative paths. Resolving through here instead makes the read
/// independent of the runner's cwd, which is one fewer way for a subject to go
/// "missing" for a reason that is not drift.
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Read a repo-relative subject file, failing loudly and by NAME when it is
/// gone or empty.
///
/// The `unwrap_or_default()` this replaces is the whole defect in one call: a
/// moved file becomes an empty string, an empty string matches no needle, and a
/// "no offenders" assertion passes.
pub fn subject_text(rel: &str) -> String {
    let path = repo_root().join(rel);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "NON-VACUITY: subject `{rel}` cannot be read ({e}). This guard grades that file; \
             with the file gone it would grade nothing at all. Re-point the guard at wherever \
             the subject moved to — do not delete the assertion."
        )
    });
    assert!(
        !text.trim().is_empty(),
        "NON-VACUITY: subject `{rel}` is empty — every needle below would miss, and every \
         `no offenders` assertion would pass over an empty haystack."
    );
    text
}

/// The set a guard is about to grade must clear a floor its own subject
/// justifies.
///
/// `what` names the set in the subject's terms ("`scenarios:` rows of
/// docs/perf-matrix.yaml"), `hint` says what to do — re-point the parser, not
/// lower the floor. Floors are set far below today's count on purpose: they
/// catch a parser that lost the SECTION (landing at or near zero), not one row
/// added or removed in a normal diff.
pub fn require_enumerated(found: usize, floor: usize, what: &str, hint: &str) {
    assert!(
        found >= floor,
        "NON-VACUITY: only {found} {what} (floor {floor}) — this guard is about to grade a \
         truncated or empty set, which passes every assertion below while checking nothing. \
         {hint}"
    );
}

/// The needle a source lint keys on must still OCCUR in the text it reads.
///
/// A lint over `foo(` says nothing once `foo` is renamed: there are no
/// occurrences, so there are no offenders, so it is green. This asserts the
/// subject of the lint exists before its verdict is believed.
pub fn require_needle(text: &str, subject: &str, needle: &str, floor: usize, hint: &str) {
    let found = text.matches(needle).count();
    assert!(
        found >= floor,
        "NON-VACUITY: `{subject}` contains {found} occurrence(s) of `{needle}` (floor {floor}) \
         — the thing this guard lints for is not there, so its verdict is about an empty \
         search, not about the code. {hint}"
    );
}
