/// Cursor state for incremental exports.
#[derive(Debug, Clone)]
pub struct CursorState {
    pub export_name: String,
    pub last_cursor_value: Option<String>,
    pub last_run_at: Option<String>,
}
