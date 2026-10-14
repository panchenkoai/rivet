//! Snowflake read through `dev/release_oracle/sf_read.py` (DuckDB's `snowflake` extension), shared by the rig cells and the warehouse harness.

/// Every row of `sql`, each a list in SELECT order; `Err` carries the reader's own words.
pub fn snowflake_rows(
    connection: &str,
    database: &str,
    warehouse: &str,
    sql: &str,
) -> Result<Vec<Vec<serde_json::Value>>, String> {
    use std::io::Write as _;
    let spec = serde_json::json!({
        "connection": connection, "database": database, "warehouse": warehouse, "sql": sql,
    });
    let mut child = std::process::Command::new("uv")
        .args([
            "run",
            "--frozen",
            "--quiet",
            "python",
            "-m",
            "dev.release_oracle.sf_read",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn `uv run` for the Snowflake reader: {e}"))?;
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(spec.to_string().as_bytes())
        .map_err(|e| format!("write the Snowflake reader's spec: {e}"))?;
    let out = child
        .wait_with_output()
        .map_err(|e| format!("wait for the Snowflake reader: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "Snowflake reader failed ({}) on `{sql}`: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let start = text
        .find('[')
        .ok_or_else(|| format!("Snowflake reader printed no rows for `{sql}`: {text}"))?;
    serde_json::from_str(&text[start..])
        .map_err(|e| format!("parse the Snowflake reader's answer to `{sql}`: {e}: {text}"))
}
