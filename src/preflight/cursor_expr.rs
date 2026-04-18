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
