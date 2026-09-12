/// Cursor state for incremental exports.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CursorState {
    pub export_name: String,
    pub last_cursor_value: Option<String>,
    pub last_run_at: Option<String>,
    /// Column/key the value belongs to; `None` for rows written before it was recorded.
    pub cursor_column: Option<String>,
}
