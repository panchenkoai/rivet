//! SQL expressions for incremental cursor preflight (ORDER BY / MIN / MAX).

use crate::config::{ExportConfig, ExportMode, IncrementalCursorMode, SourceType};
use crate::sql::quote_ident;

/// Expression used to order incremental explain queries and min/max range probes.
pub(crate) fn incremental_key_expr(
    export: &ExportConfig,
    source_type: SourceType,
) -> Option<String> {
    if export.mode != ExportMode::Incremental {
        return None;
    }
    let primary = export.cursor_column.as_ref()?;
    match export.incremental_cursor_mode {
        IncrementalCursorMode::SingleColumn => Some(quote_ident(source_type, primary)),
        IncrementalCursorMode::Coalesce => {
            let f = export.cursor_fallback_column.as_ref()?;
            let p = quote_ident(source_type, primary);
            let fb = quote_ident(source_type, f);
            Some(format!("COALESCE({p}, {fb})"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExportConfig;

    fn cfg(extra_yaml: &str) -> ExportConfig {
        let yaml = format!(
            "name: test\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n{extra_yaml}"
        );
        serde_yaml_ng::from_str(&yaml).expect("parse ExportConfig")
    }

    #[test]
    fn non_incremental_mode_returns_none() {
        let e = cfg("mode: full\n");
        assert!(incremental_key_expr(&e, SourceType::Postgres).is_none());
    }

    #[test]
    fn incremental_no_cursor_column_returns_none() {
        let e = cfg("mode: incremental\n"); // cursor_column not set
        assert!(incremental_key_expr(&e, SourceType::Postgres).is_none());
    }

    #[test]
    fn incremental_single_column_postgres_double_quoted() {
        let e = cfg("mode: incremental\ncursor_column: updated_at\n");
        let expr = incremental_key_expr(&e, SourceType::Postgres).unwrap();
        assert_eq!(expr, r#""updated_at""#);
    }

    #[test]
    fn incremental_single_column_mysql_backtick_quoted() {
        let e = cfg("mode: incremental\ncursor_column: updated_at\n");
        let expr = incremental_key_expr(&e, SourceType::Mysql).unwrap();
        assert_eq!(expr, "`updated_at`");
    }

    #[test]
    fn incremental_coalesce_postgres_returns_coalesce_expr() {
        let e = cfg(
            "mode: incremental\ncursor_column: updated_at\ncursor_fallback_column: created_at\nincremental_cursor_mode: coalesce\n",
        );
        let expr = incremental_key_expr(&e, SourceType::Postgres).unwrap();
        assert_eq!(expr, r#"COALESCE("updated_at", "created_at")"#);
    }

    #[test]
    fn incremental_coalesce_missing_fallback_returns_none() {
        // Coalesce mode but no fallback column → None (returns None from the let f = ... ?)
        let e = cfg(
            "mode: incremental\ncursor_column: updated_at\nincremental_cursor_mode: coalesce\n",
        );
        // cursor_fallback_column is None → match arm returns None
        assert!(incremental_key_expr(&e, SourceType::Postgres).is_none());
    }
}
