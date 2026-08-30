//! One `## Unreleased` heading, at H2 — the release path depends on it.
//!
//! The release workflow awk-extracts notes from `^## <ver>` to the next
//! `^## [0-9]`, and `dev/pytools/release.py` checks only that a `## <version>`
//! section exists. A second Unreleased at `###` (measured: one arrived stacked
//! ABOVE the H2 one) sits above the version heading at release time, is never
//! extracted into the release body, and the stranded heading survives into the
//! released changelog — the 0.16.x class, a gap only the release path exercises.

#[test]
fn exactly_one_unreleased_heading_and_it_is_h2() {
    let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/CHANGELOG.md"))
        .expect("read CHANGELOG.md");
    let h2 = text
        .lines()
        .filter(|l| l.trim_end() == "## Unreleased")
        .count();
    let wrong_level = text
        .lines()
        .filter(|l| {
            let t = l.trim_end();
            (t.starts_with('#') && t.ends_with("Unreleased")) && t != "## Unreleased"
        })
        .count();
    assert_eq!(
        (h2, wrong_level),
        (1, 0),
        "CHANGELOG must carry exactly ONE `## Unreleased` heading at H2 — found \
         {h2} at H2 and {wrong_level} at other levels. The release extraction \
         reads `^## ` boundaries, so any other shape strands its entries above \
         the version heading and ships them in no release notes."
    );
}
