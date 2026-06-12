//! Lint passes layered on top of serde's parse errors (v0.7.3 P1.2).
//!
//! `#[serde(deny_unknown_fields)]` on the `Config` type tree turns
//! every typo into an error like:
//!
//! ```text
//! unknown field `acccess_key_env`, expected one of `type`, `bucket`, ... at line 12 column 5
//! ```
//!
//! That's actionable but verbose.  This module re-shapes the error so
//! the operator gets a one-line, single-suggestion hint:
//!
//! ```text
//! unknown field `acccess_key_env` at line 12 column 5
//!   Did you mean `access_key_env`?
//! ```
//!
//! The full expected-fields list is preserved in a follow-up line so
//! editor copy/paste still works.

/// Wrap a serde_yaml_ng parse error with a "Did you mean ...?" hint
/// when the unknown field has a close-enough lexical match in the
/// expected-fields list embedded in the error message.
///
/// The function inspects the *string form* of the error (serde does
/// not expose the unknown field name through a structured API).  If
/// the message doesn't match the `unknown field …` shape, the
/// original error is returned unchanged.
pub fn enhance_parse_error(err: serde_yaml_ng::Error) -> anyhow::Error {
    let raw = err.to_string();
    if let Some(hint) = did_you_mean_from_message(&raw) {
        // Keep the original error's `Display` shape — VS Code, CI,
        // and humans all parse the leading `unknown field` token —
        // and just append the suggestion as a new line.
        return anyhow::anyhow!("{raw}\n  Did you mean `{hint}`?");
    }
    if let Some(hint) = missing_field_hint(&raw) {
        // serde's bare `missing field \`x\`` says WHAT is missing but not how to
        // fix it. Append a remediation line, preserving the verbatim leading text.
        return anyhow::anyhow!("{raw}\n  Hint: {hint}");
    }
    err.into()
}

/// A remediation line for serde's `missing field \`x\`` errors on the few
/// top-level fields a newcomer is most likely to omit. `None` for any other
/// field, so it never over-fires on a missing nested/optional field.
fn missing_field_hint(msg: &str) -> Option<&'static str> {
    let i = msg.find("missing field `")? + "missing field `".len();
    let field = msg[i..].split('`').next()?;
    match field {
        "source" => Some(
            "every config needs a `source:` block (type + url/url_env). Run `rivet init` to scaffold one.",
        ),
        "exports" => Some(
            "every config needs an `exports:` list with at least one export. Run `rivet init` to scaffold one.",
        ),
        "name" => Some("each export needs a `name:` (used in output filenames and state keys)."),
        "query" => {
            Some("each export needs a `query:` — or the `table:` shortcut — to select rows.")
        }
        _ => None,
    }
}

/// Parse `"unknown field `xxx`, expected one of `a`, `b`, ..."` and
/// return the best lexical match for `xxx` from the expected list, or
/// `None` if the message doesn't fit the shape or nothing is close
/// enough.
fn did_you_mean_from_message(msg: &str) -> Option<String> {
    let (unknown, expected) = parse_unknown_field_message(msg)?;
    closest_match(&unknown, &expected)
}

/// Extract `(unknown_field, expected_fields)` from a serde unknown-
/// field error string.  Returns `None` if the message shape doesn't
/// match — the caller should pass the original error through.
fn parse_unknown_field_message(msg: &str) -> Option<(String, Vec<String>)> {
    // Format: "unknown field `xxx`, expected one of `a`, `b`, `c`, ..."
    // Also (single expected): "unknown field `xxx`, expected `a`"
    let start = msg.find("unknown field `")? + "unknown field `".len();
    let rest = &msg[start..];
    let unknown_end = rest.find('`')?;
    let unknown = rest[..unknown_end].to_string();

    let after_unknown = &rest[unknown_end + 1..];
    // Look for "expected one of " or "expected ".
    let expect_idx = after_unknown.find("expected ")?;
    let after_expected = &after_unknown[expect_idx + "expected ".len()..];
    let after_expected = after_expected
        .strip_prefix("one of ")
        .unwrap_or(after_expected);

    // Collect every backtick-wrapped token until we hit the trailing
    // `at line ...` / end-of-string.
    let mut expected: Vec<String> = Vec::new();
    let mut cursor = after_expected;
    while let Some(open) = cursor.find('`') {
        let after_open = &cursor[open + 1..];
        let close = after_open.find('`')?;
        expected.push(after_open[..close].to_string());
        cursor = &after_open[close + 1..];
    }
    if expected.is_empty() {
        return None;
    }
    Some((unknown, expected))
}

/// Find the closest match for `needle` in `candidates` by
/// case-insensitive Levenshtein distance, with one shortcut: when a
/// candidate appears as a substring of the needle (or vice versa), it
/// wins regardless of strict distance.  That handles the common
/// "append a noun" mistake (`database_name` → `database`,
/// `bucket_name` → `bucket`) without loosening the threshold for
/// genuinely unrelated typos.
///
/// Distance threshold: `≤ 2` for short keys, scaling to `longer/3`
/// for longer ones.  Tightness over a longer distance avoids
/// embarrassing suggestions like "did you mean `tls`?" for a typo of
/// `xxxxxxxxxxxx`.
fn closest_match(needle: &str, candidates: &[String]) -> Option<String> {
    let needle_lower = needle.to_ascii_lowercase();

    // Edit-distance FIRST: a near-exact typo must win over a substring match to
    // a longer, unrelated field. `export` is one edit from `exports`, but it is
    // ALSO a substring of `parallel_export_processes` — running substring first
    // (longest wins) suggested the advanced flag instead of the obvious field.
    let mut best: Option<(usize, &String)> = None;
    for c in candidates {
        let d = levenshtein(&needle_lower, &c.to_ascii_lowercase());
        match best {
            Some((bd, _)) if d >= bd => {}
            _ => best = Some((d, c)),
        }
    }
    if let Some((dist, hit)) = best {
        let longer = needle.len().max(hit.len());
        let threshold = (longer / 3).max(2);
        if dist <= threshold {
            return Some(hit.clone());
        }
    }

    // Substring fallback: `bucket_name` → `bucket` is meaningful even though the
    // edit distance (5) is too far. Pick the longest qualifying candidate.
    // Floor at 3 chars: single-letter candidates produce nonsense suggestions
    // and no real Config field is that short.
    const SUBSTR_MIN_LEN: usize = 3;
    let mut substring_hit: Option<&String> = None;
    for c in candidates {
        if c.len() < SUBSTR_MIN_LEN {
            continue;
        }
        let cl = c.to_ascii_lowercase();
        if needle_lower.contains(&cl) || cl.contains(&needle_lower) {
            substring_hit = match substring_hit {
                Some(prev) if prev.len() >= c.len() => Some(prev),
                _ => Some(c),
            };
        }
    }
    substring_hit.cloned()
}

/// Classic Wagner–Fischer Levenshtein distance over ASCII / byte-
/// aligned strings.  The Config-grammar field names are all ASCII so
/// a byte-level comparison is correct and avoids the unicode
/// segmentation overhead.
fn levenshtein(a: &str, b: &str) -> usize {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr: Vec<usize> = vec![0; b.len() + 1];
    for (i, &ai) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &bj) in b.iter().enumerate() {
            let cost = if ai == bj { 0 } else { 1 };
            curr[j + 1] = (curr[j] + 1).min(prev[j + 1] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── levenshtein() ──────────────────────────────────────────────────────

    #[test]
    fn levenshtein_identical_is_zero() {
        assert_eq!(levenshtein("password", "password"), 0);
    }

    #[test]
    fn levenshtein_single_typo_is_one() {
        assert_eq!(levenshtein("paswword", "password"), 1);
    }

    #[test]
    fn levenshtein_insert_is_one() {
        assert_eq!(levenshtein("acccess_key_env", "access_key_env"), 1);
    }

    // ── closest_match() ────────────────────────────────────────────────────

    #[test]
    fn closest_match_picks_obvious_typo() {
        let candidates = vec![
            "access_key_env".into(),
            "secret_key_env".into(),
            "bucket".into(),
        ];
        assert_eq!(
            closest_match("acccess_key_env", &candidates).as_deref(),
            Some("access_key_env"),
        );
    }

    #[test]
    fn closest_match_returns_none_when_too_far() {
        let candidates = vec!["a".into(), "b".into(), "c".into()];
        // 15-char input, 1-char candidates — distance is far past the
        // threshold; no suggestion better than silence.
        assert!(closest_match("totally_unrelated", &candidates).is_none());
    }

    #[test]
    fn closest_match_is_case_insensitive() {
        let candidates = vec!["bucket".into()];
        assert_eq!(
            closest_match("BUCKET", &candidates).as_deref(),
            Some("bucket"),
        );
    }

    #[test]
    fn closest_match_prefers_near_typo_over_longer_substring() {
        // `export` is one edit from `exports`, but ALSO a substring of
        // `parallel_export_processes`. The near typo must win — the old
        // substring-first rule suggested the advanced flag.
        let candidates = vec![
            "source".into(),
            "exports".into(),
            "parallel_exports".into(),
            "parallel_export_processes".into(),
        ];
        assert_eq!(
            closest_match("export", &candidates).as_deref(),
            Some("exports"),
        );
    }

    #[test]
    fn closest_match_substring_fallback_still_works() {
        // No near typo, but `bucket_name` meaningfully contains `bucket`.
        let candidates = vec!["bucket".into(), "prefix".into()];
        assert_eq!(
            closest_match("bucket_name", &candidates).as_deref(),
            Some("bucket"),
        );
    }

    // ── parse_unknown_field_message() ──────────────────────────────────────

    #[test]
    fn parses_serde_unknown_field_shape() {
        let msg = "unknown field `azure_container`, expected one of `bucket`, `prefix`, `path`";
        let (unknown, expected) = parse_unknown_field_message(msg).unwrap();
        assert_eq!(unknown, "azure_container");
        assert_eq!(expected, vec!["bucket", "prefix", "path"]);
    }

    #[test]
    fn parses_with_trailing_location() {
        let msg = "unknown field `foo`, expected one of `a`, `b` at line 12 column 5";
        let (unknown, expected) = parse_unknown_field_message(msg).unwrap();
        assert_eq!(unknown, "foo");
        assert_eq!(expected, vec!["a", "b"]);
    }

    #[test]
    fn returns_none_for_non_matching_message() {
        let msg = "invalid type: integer `42`, expected a string at line 3 column 5";
        assert!(parse_unknown_field_message(msg).is_none());
    }

    // ── did_you_mean_from_message() ────────────────────────────────────────

    #[test]
    fn end_to_end_typo_suggestion() {
        let msg = "unknown field `acccess_key_env`, expected one of `bucket`, `access_key_env`, `secret_key_env` at line 14 column 5";
        assert_eq!(
            did_you_mean_from_message(msg).as_deref(),
            Some("access_key_env"),
        );
    }

    #[test]
    fn no_suggestion_when_nothing_is_close() {
        let msg =
            "unknown field `flux_capacitor`, expected one of `bucket`, `prefix` at line 1 column 1";
        assert!(did_you_mean_from_message(msg).is_none());
    }
}
