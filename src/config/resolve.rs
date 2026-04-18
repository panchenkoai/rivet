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
