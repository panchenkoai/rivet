/// Replaces `${VAR}` patterns with values from `params` (if provided) or environment variables.
/// Params take precedence over env vars.
///
/// **Strict mode (default, SecOps):** if a `${VAR}` reference resolves to neither a
/// param nor an env var, an error is returned rather than silently substituting an
/// empty string. A missing `DB_PASS` turning `postgres://u:${DB_PASS}@h/d` into
/// `postgres://u:@h/d` is an auth-bypass footgun — we fail fast instead.
///
/// A literal empty value is still accepted (`export VAR=""`) — only completely
/// unset variables fail.
///
/// Empty placeholders (`${}`) are left as-is for backwards compatibility.
pub fn resolve_vars(
    input: &str,
    params: Option<&std::collections::HashMap<String, String>>,
) -> crate::error::Result<String> {
    let mut result = input.to_string();
    let mut search_from = 0;
    while let Some(rel_start) = result[search_from..].find("${") {
        let start = search_from + rel_start;
        let Some(rel_end) = result[start..].find('}') else {
            break;
        };
        let end = start + rel_end;
        let var_name = &result[start + 2..end];

        let value = if var_name.is_empty() {
            // Preserve legacy behavior: `${}` expands to the empty string. No secret
            // is involved, so there's nothing to protect against.
            String::new()
        } else if let Some(v) = params.and_then(|p| p.get(var_name)) {
            v.clone()
        } else {
            match std::env::var(var_name) {
                Ok(v) => v,
                Err(_) => anyhow::bail!(
                    "environment variable '{}' referenced in config is not set \
                     (a missing secret silently becomes an empty string — refusing)",
                    var_name
                ),
            }
        };

        result = format!("{}{}{}", &result[..start], value, &result[end + 1..]);
        search_from = start + value.len();
    }
    Ok(result)
}

/// Convenience wrapper: resolve `${VAR}` from environment only.
pub fn resolve_env_vars(input: &str) -> crate::error::Result<String> {
    resolve_vars(input, None)
}

/// Return the names of `--param key=value` entries whose `${key}` placeholder
/// does not appear in `haystack`. Sorted for deterministic warning order.
///
/// Used by [`warn_unused_params`]; exposed separately so tests can assert the
/// set of unused keys without needing to capture log output.
pub fn find_unused_params(
    haystack: &str,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Vec<String> {
    let Some(p) = params else {
        return Vec::new();
    };
    let mut unused: Vec<String> = p
        .keys()
        .filter(|k| !haystack.contains(&format!("${{{k}}}")))
        .cloned()
        .collect();
    unused.sort();
    unused
}

/// F10 (0.7.5 audit): warn loudly when `--param key=value` was passed but
/// `${key}` never appears anywhere the resolver searched.  A common typo
/// (`--param maxid=…` vs `${max_id}`) is otherwise silently ignored and the
/// operator gets unexpected results.
///
/// Decoupled from `resolve_vars` because the same params object flows through
/// the YAML body resolve AND each `ExportConfig::resolve_query` call — emitting
/// the warning inside `resolve_vars` itself fired it N+1 times per `--param`.
/// Call this exactly once per CLI invocation, passing the original (un-resolved)
/// YAML text as the haystack so placeholders are still present.
pub fn warn_unused_params(
    haystack: &str,
    params: Option<&std::collections::HashMap<String, String>>,
) {
    for key in find_unused_params(haystack, params) {
        log::warn!(
            "--param '{}' was not referenced by any `${{{}}}` placeholder in the config — \
             check the parameter name (case-sensitive) or remove the unused --param",
            key,
            key
        );
    }
}

/// Parse a human-readable file size like "512MB", "1GB", "100KB" into bytes.
pub fn parse_file_size(s: &str) -> crate::error::Result<u64> {
    let s = s.trim().to_uppercase();
    let (num, multiplier) = if let Some(n) = s.strip_suffix("GB") {
        (n.trim(), 1024u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MB") {
        (n.trim(), 1024u64 * 1024)
    } else if let Some(n) = s.strip_suffix("KB") {
        (n.trim(), 1024u64)
    } else if let Some(n) = s.strip_suffix('B') {
        (n.trim(), 1u64)
    } else {
        (s.as_str(), 1u64)
    };
    let value: f64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid file size: '{}'", s))?;
    Ok((value * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ── resolve_vars — no substitution ──────────────────────────────────────

    #[test]
    fn no_placeholders_returned_verbatim() {
        assert_eq!(resolve_vars("SELECT 1", None).unwrap(), "SELECT 1");
    }

    #[test]
    fn empty_string_returned_verbatim() {
        assert_eq!(resolve_vars("", None).unwrap(), "");
    }

    // ── resolve_vars — param substitution ───────────────────────────────────

    #[test]
    fn param_substitutes_placeholder() {
        let mut p = HashMap::new();
        p.insert("TABLE".into(), "orders".into());
        let result = resolve_vars("SELECT * FROM ${TABLE}", Some(&p)).unwrap();
        assert_eq!(result, "SELECT * FROM orders");
    }

    #[test]
    fn param_takes_precedence_over_env() {
        // Set an env var with the same name but different value.
        unsafe { std::env::set_var("RIVET_TEST_OVERRIDE_VAR", "from_env") };
        let mut p = HashMap::new();
        p.insert("RIVET_TEST_OVERRIDE_VAR".into(), "from_param".into());
        let result = resolve_vars("${RIVET_TEST_OVERRIDE_VAR}", Some(&p)).unwrap();
        unsafe { std::env::remove_var("RIVET_TEST_OVERRIDE_VAR") };
        assert_eq!(result, "from_param");
    }

    #[test]
    fn multiple_placeholders_all_substituted() {
        let mut p = HashMap::new();
        p.insert("A".into(), "hello".into());
        p.insert("B".into(), "world".into());
        let result = resolve_vars("${A} ${B}", Some(&p)).unwrap();
        assert_eq!(result, "hello world");
    }

    // ── resolve_vars — env var substitution ─────────────────────────────────

    #[test]
    fn env_var_substituted_when_set() {
        unsafe { std::env::set_var("RIVET_TEST_RESOLVE_VAR", "secret123") };
        let result = resolve_vars("pass=${RIVET_TEST_RESOLVE_VAR}", None).unwrap();
        unsafe { std::env::remove_var("RIVET_TEST_RESOLVE_VAR") };
        assert_eq!(result, "pass=secret123");
    }

    #[test]
    fn missing_env_var_returns_error() {
        unsafe { std::env::remove_var("RIVET_DEFINITELY_NOT_SET_VAR_XYZ") };
        let err = resolve_vars("${RIVET_DEFINITELY_NOT_SET_VAR_XYZ}", None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("RIVET_DEFINITELY_NOT_SET_VAR_XYZ"),
            "got: {msg}"
        );
    }

    // ── resolve_vars — empty placeholder ────────────────────────────────────

    #[test]
    fn empty_placeholder_expands_to_empty_string() {
        let result = resolve_vars("pre${}post", None).unwrap();
        assert_eq!(result, "prepost");
    }

    // ── resolve_vars — unclosed placeholder ─────────────────────────────────

    #[test]
    fn unclosed_placeholder_left_as_is() {
        let result = resolve_vars("${UNCLOSED", None).unwrap();
        assert_eq!(result, "${UNCLOSED");
    }

    // ── find_unused_params — regression: F-NEW after 0.7.5 audit ────────────
    //
    // Before splitting the warning out of `resolve_vars`, the unused-param
    // warning was emitted N+1 times per `--param` (once at YAML resolve,
    // once per `ExportConfig::resolve_query` call), AND every `--param` was
    // wrongly flagged unused at the resolve_query stage because the YAML
    // pass had already substituted the placeholders out. These tests pin
    // the new behavior: `find_unused_params` flags only genuinely-unused
    // keys, against an un-resolved (placeholder-bearing) haystack.

    #[test]
    fn find_unused_params_returns_empty_when_no_params() {
        assert!(find_unused_params("SELECT 1", None).is_empty());
    }

    #[test]
    fn find_unused_params_used_key_not_flagged() {
        let mut p = HashMap::new();
        p.insert("max_id".into(), "20".into());
        let unused = find_unused_params("SELECT * FROM t WHERE id <= ${max_id}", Some(&p));
        assert!(unused.is_empty(), "got: {unused:?}");
    }

    #[test]
    fn find_unused_params_unused_key_flagged_once() {
        let mut p = HashMap::new();
        p.insert("typo_id".into(), "20".into());
        let unused = find_unused_params("SELECT * FROM t WHERE id <= ${max_id}", Some(&p));
        assert_eq!(unused, vec!["typo_id".to_string()]);
    }

    #[test]
    fn find_unused_params_mixed_used_and_unused() {
        let mut p = HashMap::new();
        p.insert("col".into(), "id".into());
        p.insert("typo".into(), "x".into());
        let unused = find_unused_params("SELECT ${col} FROM t", Some(&p));
        assert_eq!(unused, vec!["typo".to_string()]);
    }

    #[test]
    fn find_unused_params_partial_match_does_not_count() {
        // A param named `max` is NOT used by a `${max_id}` placeholder —
        // substring overlap must not satisfy the check.
        let mut p = HashMap::new();
        p.insert("max".into(), "20".into());
        let unused = find_unused_params("SELECT * FROM t WHERE id <= ${max_id}", Some(&p));
        assert_eq!(unused, vec!["max".to_string()]);
    }

    // ── resolve_env_vars wrapper ─────────────────────────────────────────────

    #[test]
    fn resolve_env_vars_reads_env() {
        unsafe { std::env::set_var("RIVET_TEST_ENV_WRAPPER", "wrapped") };
        let result = resolve_env_vars("v=${RIVET_TEST_ENV_WRAPPER}").unwrap();
        unsafe { std::env::remove_var("RIVET_TEST_ENV_WRAPPER") };
        assert_eq!(result, "v=wrapped");
    }

    // ── parse_file_size ──────────────────────────────────────────────────────

    #[test]
    fn parse_1gb() {
        assert_eq!(parse_file_size("1GB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_512mb() {
        assert_eq!(parse_file_size("512MB").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn parse_100kb() {
        assert_eq!(parse_file_size("100KB").unwrap(), 100 * 1024);
    }

    #[test]
    fn parse_bytes_suffix() {
        assert_eq!(parse_file_size("2048B").unwrap(), 2048);
    }

    #[test]
    fn parse_no_suffix_treated_as_bytes() {
        assert_eq!(parse_file_size("4096").unwrap(), 4096);
    }

    #[test]
    fn parse_whitespace_trimmed() {
        assert_eq!(parse_file_size("  256MB  ").unwrap(), 256 * 1024 * 1024);
    }

    #[test]
    fn parse_lowercase_accepted() {
        assert_eq!(parse_file_size("1gb").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_invalid_returns_error() {
        assert!(parse_file_size("notanumber").is_err());
    }
}
