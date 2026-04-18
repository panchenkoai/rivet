//! Incremental cursor configuration (Epic D — cursor policy).
//!
//! `cursor_column` remains the primary progression column. Optional `cursor_fallback_column`
//! plus [`IncrementalCursorMode::Coalesce`] uses `COALESCE(primary, fallback)` for the
//! incremental predicate and stored cursor value (single scalar string in state).

use serde::{Deserialize, Serialize};

/// How the primary (and optional fallback) column(s) participate in incremental extraction.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum IncrementalCursorMode {
    /// `WHERE primary > last ORDER BY primary` — optional fallback column is ignored for execution.
    #[default]
    SingleColumn,
    /// `WHERE COALESCE(primary, fallback) > last` with a synthetic result column for cursor extraction.
    Coalesce,
}
