/// Replaces `${VAR}` patterns with values from `params` (if provided) or environment variables.
/// Params take precedence over env vars.
pub fn resolve_vars(
    input: &str,
    params: Option<&std::collections::HashMap<String, String>>,
) -> String {
    let mut result = input.to_string();
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            let var_name = &result[start + 2..start + end];
            let value = params
                .and_then(|p| p.get(var_name).cloned())
                .unwrap_or_else(|| std::env::var(var_name).unwrap_or_default());
            result = format!(
                "{}{}{}",
                &result[..start],
                value,
                &result[start + end + 1..]
            );
        } else {
            break;
        }
    }
    result
}

/// Convenience wrapper: resolve `${VAR}` from environment only.
pub fn resolve_env_vars(input: &str) -> String {
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
