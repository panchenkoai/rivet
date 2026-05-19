//! Helpers that translate CLI strings into the shapes pipeline code wants.
//!
//! [`parse_params`] turns repeated `--param KEY=VALUE` flags into a
//! `HashMap<String, String>` used for `${var}` substitution in YAML queries.
//! [`resolve_init_source`] picks the one of three source-spec inputs that
//! `rivet init` was invoked with, reading env or file when needed so the DB
//! URL never has to appear on the command line.

use std::collections::HashMap;

use crate::error::Result;

/// Collect repeated `--param KEY=VALUE` strings into a map.
///
/// Items without `=` are silently dropped (clap already validates form); the
/// first `=` wins so values may themselves contain `=` (e.g. SQL fragments).
/// Duplicate keys follow last-wins ordering, matching shell expectations.
pub(super) fn parse_params(raw: &[String]) -> HashMap<String, String> {
    raw.iter()
        .filter_map(|s| s.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Resolve the DB URL for `rivet init` from exactly one of `--source`,
/// `--source-env`, or `--source-file`.
///
/// Clap's argument group already enforces mutual exclusion and
/// at-least-one-required. This function only performs the side-effect of
/// reading env / file so that credentials never have to appear on the command
/// line (which would leak into shell history, `ps`, and `/proc/<pid>/cmdline`).
pub(super) fn resolve_init_source(
    source: Option<String>,
    source_env: Option<String>,
    source_file: Option<String>,
) -> Result<String> {
    if let Some(url) = source {
        return Ok(url);
    }
    if let Some(var) = source_env {
        return std::env::var(&var)
            .map_err(|_| anyhow::anyhow!("--source-env '{}' is not set in the environment", var));
    }
    if let Some(path) = source_file {
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("cannot read --source-file '{}': {}", path, e))?;
        return Ok(raw.trim().to_string());
    }
    // Unreachable: clap ArgGroup enforces `required = true`.
    anyhow::bail!("--source, --source-env, or --source-file is required")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_params_basic() {
        let input = vec!["KEY=value".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.get("KEY").unwrap(), "value");
    }

    #[test]
    fn parse_params_equals_in_value() {
        let input = vec!["FILTER=x=1 AND y=2".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.get("FILTER").unwrap(), "x=1 AND y=2");
    }

    #[test]
    fn parse_params_multiple() {
        let input = vec!["A=1".to_string(), "B=2".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("A").unwrap(), "1");
        assert_eq!(result.get("B").unwrap(), "2");
    }

    #[test]
    fn parse_params_empty_input() {
        let result = parse_params(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_params_no_equals_skipped() {
        let input = vec!["NO_EQUALS_HERE".to_string()];
        let result = parse_params(&input);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_params_empty_value() {
        let input = vec!["KEY=".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.get("KEY").unwrap(), "");
    }

    #[test]
    fn parse_params_duplicate_last_wins() {
        let input = vec!["K=first".to_string(), "K=second".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.get("K").unwrap(), "second");
    }

    #[test]
    fn json_error_output_is_valid_json() {
        let err = anyhow::anyhow!("connection refused");
        let output = serde_json::json!({"error": format!("{err:#}")}).to_string();
        let parsed: serde_json::Value = serde_json::from_str(&output).expect("must be valid JSON");
        assert_eq!(parsed["error"], "connection refused");
    }
}
