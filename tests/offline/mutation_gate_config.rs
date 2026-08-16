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
//!
//! # What this file CANNOT check, and who does
//!
//! Two questions need the real corpus, which only cargo-mutants can produce:
//!
//! * does an entry match at least ONE real mutant? (a DEAD entry is a false
//!   triage claim — it says "this mutant is equivalent, ignore it" about
//!   nothing, and the mutant it meant to triage is still graded);
//! * does an entry match mutants at more than one SITE — `(file, function)`,
//!   not the bare name (`density_probe` is defined in both src/init/mysql.rs
//!   and src/init/postgres.rs, so a name-keyed answer says "one function" for
//!   an entry reaching two modules)? Entries that legitimately span files are
//!   declared in that script's `MULTI_SITE_OK`, which NAMES THE FILES the entry
//!   may reach along with the reason one triage argument covers them: a bare
//!   "this one may span sites" let a third engine's `density_probe` inherit an
//!   argument written about two other modules, and the checker reported
//!   `ok  4 mutant(s) … across 4 declared sites`.
//!
//! Both are checked by `.github/scripts/mutants_exclusions.py`, run in the
//! BLOCKING `Mutants (gate sanity)` CI job against `cargo mutants --list
//! --no-config`. That the offline half was blind to the first question is not
//! theoretical: round-4 triaged `Governor::decision_cause` by writing
//! `"replace < with <= in decision_cause"`, which matches zero real mutants
//! (cargo-mutants qualifies impl methods as `Type::method`) — every guard in
//! this file stayed green over an exclusion that excluded nothing.

use std::path::Path;

fn config_text() -> String {
    let cfg = Path::new(env!("CARGO_MANIFEST_DIR")).join(".cargo/mutants.toml");
    std::fs::read_to_string(&cfg).expect("read .cargo/mutants.toml")
}

/// Read a top-level array-of-strings key out of `.cargo/mutants.toml` the way
/// cargo-mutants' own TOML parser reads it.
///
/// Hand-written rather than `toml`-derived because the workspace carries no
/// `toml` dependency, but FAITHFUL where the previous line/quote scraper was
/// not — and it PANICS on anything it cannot decode exactly (a literal string,
/// a multi-line string, an unsupported escape) instead of grading a mangled
/// approximation. Two measured defects in the scraper motivated this:
///
/// 1. it took the text between the first and last `"` of every line in the
///    block, comments included, so the reason comment at `mutants.toml`'s
///    `overlay_measured_rows` entry was graded as a 23rd "pattern"; the moment
///    a comment quotes an example of the very defect this file guards (the
///    natural thing to write there) the suite goes RED against a correct config;
/// 2. it never decoded TOML escapes, so the shipped entry
///    `"replace \\* with \\+ in compute_part_checksums"` was compiled as the
///    regex `\\*` (a literal-backslash repetition, 0 matches) while
///    cargo-mutants compiles `\*` (2 matches) — the guard validated a different
///    regex from the one that ships, in exactly the escaping class it exists to
///    police.
fn toml_string_array(text: &str, key: &str) -> Option<Vec<String>> {
    // mutants.toml is flat top-level TOML: a line-anchored key search is exact.
    let (key_at, _) = text
        .match_indices(key)
        .find(|(i, _)| *i == 0 || text.as_bytes()[i - 1] == b'\n')?;
    let chars: Vec<char> = text[key_at + key.len()..].chars().collect();
    let mut i = 0;
    while i < chars.len() && (chars[i] == ' ' || chars[i] == '\t') {
        i += 1;
    }
    assert_eq!(
        chars.get(i),
        Some(&'='),
        "{key} is not a `key = [..]` array"
    );
    i += 1;
    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }
    assert_eq!(chars.get(i), Some(&'['), "{key} is not an array");
    i += 1;

    let mut out = Vec::new();
    loop {
        match chars.get(i) {
            None => panic!("{key}: unterminated array"),
            Some(']') => break,
            Some('#') => {
                // A COMMENT, never a pattern — the scraper's first defect.
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            Some(c) if c.is_whitespace() || *c == ',' => i += 1,
            Some('\'') => panic!(
                "{key}: literal ('single-quoted') strings are not decoded by this guard — \
                 use a basic \"double-quoted\" string so the guard grades the same regex \
                 cargo-mutants compiles"
            ),
            Some('"') => {
                assert_ne!(
                    (chars.get(i + 1), chars.get(i + 2)),
                    (Some(&'"'), Some(&'"')),
                    "{key}: multi-line strings are not decoded by this guard"
                );
                i += 1;
                let mut s = String::new();
                loop {
                    match chars.get(i) {
                        None | Some('\n') => panic!("{key}: unterminated string"),
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some('\\') => {
                            let esc = chars.get(i + 1).copied().unwrap_or('\0');
                            let decoded = match esc {
                                '\\' => '\\',
                                '"' => '"',
                                'n' => '\n',
                                't' => '\t',
                                'r' => '\r',
                                other => panic!(
                                    "{key}: escape `\\{other}` is not decoded by this guard — \
                                     it would grade a different regex than cargo-mutants \
                                     compiles; teach the guard or avoid the escape"
                                ),
                            };
                            s.push(decoded);
                            i += 2;
                        }
                        Some(c) => {
                            s.push(*c);
                            i += 1;
                        }
                    }
                }
                out.push(s);
            }
            Some(c) => panic!("{key}: unexpected character `{c}` in the array"),
        }
    }
    Some(out)
}

fn exclude_patterns() -> Vec<String> {
    let out = toml_string_array(&config_text(), "exclude_re")
        .expect("`exclude_re` not found in .cargo/mutants.toml — did the config's shape change?");
    assert!(
        !out.is_empty(),
        "no exclude_re entries parsed — this guard must not silently pass by reading nothing."
    );
    out
}

/// The array reader must decode TOML, not scrape quotes.
///
/// Both halves are the measured defects of the previous scraper, and both are
/// graded against a hand-written expected value (never against the file's own
/// text): a comment inside the block is not an entry, and `\\` decodes to a
/// single backslash so the guard compiles the regex cargo-mutants compiles.
///
/// RED-proven against the scraper it replaces (take-text-between-first-and-last-
/// quote, no comment skip, no unescaping): it returns four "patterns" for the
/// synthetic document's two entries, and leaves `\\*` doubled.
#[test]
fn exclude_re_is_decoded_as_toml_not_scraped_for_quotes() {
    let doc = r#"
exclude_globs = ["src/fuzz.rs"]

exclude_re = [
    # A reason comment that QUOTES the defect it explains: written "a||b",
    # this entry would match every mutant name.
    "replace \\* with \\+ in compute_part_checksums",
    # trailing comment "with quotes"
    "replace && with \\|\\| in overlay_measured_rows",
]
"#;
    // Hand-written expectations: two entries, backslash escapes collapsed once.
    assert_eq!(
        toml_string_array(doc, "exclude_re").unwrap(),
        vec![
            r"replace \* with \+ in compute_part_checksums".to_string(),
            r"replace && with \|\| in overlay_measured_rows".to_string(),
        ]
    );
    assert_eq!(
        toml_string_array(doc, "exclude_globs").unwrap(),
        vec!["src/fuzz.rs".to_string()]
    );

    // …and the same on the REAL file: the shipped checksum entry decodes to
    // single backslashes, and no comment text is graded as a pattern.
    let real = exclude_patterns();
    assert!(
        real.contains(&r"replace \* with \+ in compute_part_checksums".to_string()),
        "the shipped compute_part_checksums entry is not decoded as cargo-mutants \
         compiles it: {real:#?}"
    );
    assert!(
        !real.iter().any(|p| p.contains(" OR ")),
        "a REASON COMMENT was harvested as a pattern: {real:#?}"
    );
}

/// No exclusion may match a mutant it was not written for.
///
/// Each pattern is applied to a set of canonical mutant names drawn from
/// unrelated modules. A correctly-scoped entry names one function and matches
/// none of them; an over-broad one (an empty regex alternative, a bare `.*`, a
/// stray `|`, an UNANCHORED function name) matches one or more.
///
/// The UNRELATED corpus is deliberately mixed-shape, and the reason is a real
/// miss: while it held only function-STUB names (`replace <fn> -> <ty> with
/// <val>`) it could not see an over-broad OPERATOR exclusion at all, and the
/// shipped entry `"replace == with != in check"` — whose comment claims the
/// three `preflight/mod.rs::check` sites — was silently also excluding 13
/// mutants in six other files (`quality.rs::check_null_ratios`,
/// `check_uniqueness`, `resource.rs::check_memory`,
/// `cli/dispatch.rs::check_export_selection`, …), all of which have offline
/// tests and are exactly what the gate exists to grade. Every name below is a
/// REAL name from `cargo mutants --list --no-config` on this tree, so a match
/// here is a mutant genuinely lost from the corpus.
///
/// RED-proven twice: with the unescaped `||` form restored (the empty-branch
/// bug), and with `"replace == with != in check"` un-anchored (the prefix bug —
/// it matches `check_null_ratios` and `check_memory` below).
#[test]
fn exclude_patterns_are_not_over_broad() {
    // Names shaped exactly like cargo-mutants output, from modules no entry in
    // the list should have any opinion about. Operator-, delete- and stub-shaped
    // — an exclusion list is written in all three shapes.
    const UNRELATED: &[&str] = &[
        // Operator mutants whose function name merely SHARES A PREFIX with an
        // excluded one: `in check` must not mean `in check_*`.
        "src/quality.rs:365:40: replace == with != in check_null_ratios",
        "src/quality.rs:436:40: replace == with != in check_uniqueness",
        "src/resource.rs:103:21: replace == with != in check_memory",
        "src/cli/dispatch.rs:35:45: replace == with != in check_export_selection",
        "src/preflight/analysis.rs:410:13: replace == with != in check_sparse_range",
        "src/pipeline/run.rs:642:14: replace == with != in run_waves",
        // Other operator shapes: a bare `replace && with `, `replace < with <=`
        // or `replace || with &&` fragment matches these.
        "src/plan/waves.rs:109:38: replace && with || in pack",
        "src/manifest.rs:119:52: replace && with || in identity_family",
        "src/pipeline/run.rs:651:33: replace || with && in run_waves",
        "src/tuning/adaptive.rs:251:31: replace < with <= in GovernorState::observe",
        "src/plan/waves.rs:112:21: replace += with *= in pack",
        // Delete-expression and delete-match-arm shapes.
        "src/manifest.rs:114:8: delete ! in identity_family",
        "src/plan/waves.rs:109:20: delete ! in pack",
        "src/bin/seed/insert.rs:693:9: delete match arm \"page_view\" in gen_event_payload",
        // Function-stub shapes.
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

/// `exclude_re` is not the only key that can empty the corpus.
///
/// `exclude_globs` drops whole FILES, and `examine_re`/`examine_globs` invert
/// the question — they restrict the corpus to what they name, so adding one as
/// "triage" silently un-grades everything else. The previous guard looked at no
/// key but `exclude_re`, so any of those could have been added without a single
/// test noticing.
///
/// Files may be excluded (today: `src/fuzz.rs`, whose entry points are graded by
/// cargo-fuzz instead), but only ONE AT A TIME and by name: a `**` wildcard
/// under `src/` removes a whole subtree, which is the file-level shape of the
/// same defect.
#[test]
fn no_wildcard_file_exclusion_and_no_narrowing_examine_keys() {
    let text = config_text();
    for key in ["examine_re", "examine_globs"] {
        assert!(
            toml_string_array(&text, key).is_none(),
            "`{key}` RESTRICTS the mutation corpus to what it names — everything \
             else stops being graded. If one is genuinely wanted, this guard has \
             to learn why; it must not appear silently."
        );
    }
    for glob in toml_string_array(&text, "exclude_globs").unwrap_or_default() {
        assert!(
            !glob.contains('*'),
            "exclude_globs entry `{glob}` is a WILDCARD — it drops every file it \
             matches, present and future, out of the graded corpus. Name the file."
        );
    }
}
