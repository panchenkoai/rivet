//! Tests for placeholder/env-var resolution and `parse_file_size`.

use super::*;

// ─── resolve_vars regression tests ───────────────────────────

#[test]
fn resolve_vars_multiple_vars_in_one_string() {
    let mut params = std::collections::HashMap::new();
    params.insert("A".into(), "1".into());
    params.insert("B".into(), "2".into());
    let resolved = resolve_vars("${A}-${B}-end", Some(&params)).unwrap();
    assert_eq!(resolved, "1-2-end");
}

#[test]
fn resolve_vars_missing_closing_brace_stops() {
    let resolved = resolve_vars("before${NO_CLOSE after", None).unwrap();
    assert_eq!(resolved, "before${NO_CLOSE after");
}

#[test]
fn resolve_vars_empty_var_name() {
    let resolved = resolve_vars("x=${}y", None).unwrap();
    assert_eq!(resolved, "x=y");
}

#[test]
fn resolve_vars_no_vars_passthrough() {
    let resolved = resolve_vars("plain text, no dollars", None).unwrap();
    assert_eq!(resolved, "plain text, no dollars");
}

#[test]
fn resolve_vars_adjacent_vars() {
    let mut params = std::collections::HashMap::new();
    params.insert("X".into(), "hello".into());
    params.insert("Y".into(), "world".into());
    let resolved = resolve_vars("${X}${Y}", Some(&params)).unwrap();
    assert_eq!(resolved, "helloworld");
}

#[test]
fn resolve_vars_dollar_without_brace_ignored() {
    let resolved = resolve_vars("price is $5", None).unwrap();
    assert_eq!(resolved, "price is $5");
}

#[test]
fn resolve_vars_value_containing_dollar_does_not_recurse() {
    // Regression: after replacing `${VAR}` with a value that itself contains `${`,
    // the loop must not re-interpret the expansion. `search_from` advance guards this.
    let mut params = std::collections::HashMap::new();
    params.insert("RAW".into(), "${NOT_A_VAR}".into());
    let resolved = resolve_vars("x=${RAW}", Some(&params)).unwrap();
    assert_eq!(resolved, "x=${NOT_A_VAR}");
}

// ─── parse_file_size regression tests ────────────────────────

#[test]
fn parse_file_size_fractional() {
    assert_eq!(
        parse_file_size("1.5GB").unwrap(),
        (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
    );
    assert_eq!(
        parse_file_size("0.5MB").unwrap(),
        (0.5 * 1024.0 * 1024.0) as u64
    );
}

#[test]
fn parse_file_size_whitespace() {
    assert_eq!(parse_file_size("  512 MB  ").unwrap(), 512 * 1024 * 1024);
}

#[test]
fn parse_file_size_lowercase() {
    assert_eq!(parse_file_size("256mb").unwrap(), 256 * 1024 * 1024);
    assert_eq!(parse_file_size("1gb").unwrap(), 1024 * 1024 * 1024);
}

#[test]
fn parse_file_size_invalid_errors() {
    assert!(parse_file_size("abc").is_err());
    assert!(parse_file_size("MB").is_err());
}

#[test]
fn parse_file_size_zero() {
    assert_eq!(parse_file_size("0MB").unwrap(), 0);
    assert_eq!(parse_file_size("0").unwrap(), 0);
}

// =============================================================================
// Placeholder / parameter resolution — QA backlog Task 5.2 (edge cases)
// =============================================================================

#[test]
fn resolve_vars_unicode_value_preserved_exactly() {
    let mut params = std::collections::HashMap::new();
    params.insert("GREETING".into(), "Γεια σου 🌍".into());
    let got = resolve_vars("msg=${GREETING}!", Some(&params)).unwrap();
    assert_eq!(got, "msg=Γεια σου 🌍!");
}

#[test]
fn resolve_vars_value_with_quotes_newlines_braces_passes_through() {
    // These characters often break naive substitution (they look "structural"
    // in YAML and SQL).  `resolve_vars` is a literal text substitution;
    // downstream escaping is the caller's responsibility.  Values must land
    // verbatim.
    let mut params = std::collections::HashMap::new();
    params.insert(
        "NASTY".into(),
        "a \"quote\" and a\nnewline and a {brace}".into(),
    );
    let got = resolve_vars("v=[${NASTY}]", Some(&params)).unwrap();
    assert_eq!(got, "v=[a \"quote\" and a\nnewline and a {brace}]");
}

#[test]
fn resolve_vars_long_value_terminates_quickly() {
    // 1 MiB value exercises any accidental O(n²) substitution.
    let big = "a".repeat(1024 * 1024);
    let mut params = std::collections::HashMap::new();
    params.insert("BIG".into(), big);

    let t0 = std::time::Instant::now();
    let got = resolve_vars("x=${BIG}y", Some(&params)).unwrap();
    let dur = t0.elapsed();
    assert_eq!(got.len(), 2 + 1024 * 1024 + 1);
    assert!(got.starts_with("x=") && got.ends_with('y'));
    assert!(
        dur.as_secs() < 5,
        "expected sub-5s resolution of a 1MiB value, took {dur:?}"
    );
}

#[test]
fn resolve_vars_repeated_placeholder_uses_value_each_time() {
    let mut params = std::collections::HashMap::new();
    params.insert("A".into(), "alpha".into());
    params.insert("B".into(), "beta".into());
    let got = resolve_vars("${A}${A}-${B}-${A}", Some(&params)).unwrap();
    assert_eq!(got, "alphaalpha-beta-alpha");
}

// =============================================================================
// parse_file_size boundaries + fuzz — QA backlog Task 2.3
// =============================================================================

#[test]
fn parse_file_size_rejects_empty_and_garbage() {
    assert!(parse_file_size("").is_err());
    assert!(parse_file_size("not-a-size").is_err());
    assert!(parse_file_size("MB").is_err());
    assert!(parse_file_size("GB 1").is_err());
}

#[test]
fn parse_file_size_boundary_exact_kb() {
    assert_eq!(parse_file_size("1023").unwrap(), 1023);
    assert_eq!(parse_file_size("1024").unwrap(), 1024);
    assert_eq!(parse_file_size("1KB").unwrap(), 1024);
}

#[test]
fn parse_file_size_does_not_panic_on_huge_inputs_and_is_deterministic() {
    // Absurd inputs either parse (possibly saturating) or return Err — but
    // never panic.  Same input must produce the same Ok/Err verdict.
    let inputs = [
        "1000000000000000000000000GB",
        "1e308GB",
        "999999999999999999999999999B",
        "1.7976931348623157e308MB",
    ];
    for s in inputs {
        let a = parse_file_size(s).is_ok();
        let b = parse_file_size(s).is_ok();
        assert_eq!(a, b, "deterministic for input {s:?}");
    }
}

#[test]
fn parse_file_size_negative_values_are_not_huge_positive() {
    // A negative file size is nonsense; callers rely on max_file_size being
    // a usable upper bound.  Assert the behaviour is either "error" or "zero",
    // never a huge positive number that would silently disable splitting.
    match parse_file_size("-1GB").ok() {
        None => {}
        Some(v) => assert_eq!(v, 0, "negative must coerce to 0 if accepted, got {v}"),
    }
}
