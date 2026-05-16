//! MCP (Model Context Protocol) server — read-only DB introspection tools.
//!
//! Entry point: `rivet mcp --stdio [--pg-url URL] [--mysql-url URL]`
//!
//! The server speaks JSON-RPC 2.0 over stdin/stdout (one object per line).
//! All tools are read-only — no writes, no DDL.
//!
//! ## Claude Desktop integration
//!
//! Add to `~/Library/Application\ Support/Claude/claude_desktop_config.json`:
//! ```json
//! {
//!   "mcpServers": {
//!     "rivet": {
//!       "command": "rivet",
//!       "args": ["mcp", "--stdio"],
//!       "env": { "DATABASE_URL": "postgresql://user:pass@localhost/db" }
//!     }
//!   }
//! }
//! ```
//!
//! ## Claude Code integration
//!
//! ```bash
//! claude mcp add rivet -- rivet mcp --stdio
//! # then set DATABASE_URL in your shell
//! ```

use serde_json::{Value, json};
use std::io::{BufRead, Write};

// ─── Public entry point ────────────────────────────────────────────────────

/// Run the MCP server loop on stdin/stdout until EOF.
///
/// `pg_url` and `mysql_url` are used for tool calls against each database.
/// Either (or both) may be `None`; tools for a missing database return an error.
pub fn run_stdio(pg_url: Option<&str>, mysql_url: Option<&str>) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Notifications have no `id` and require no response.
        let id = msg.get("id").cloned();
        let Some(id) = id else { continue };

        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let envelope = match dispatch(method, &msg, pg_url, mysql_url) {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(e) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32_000, "message": e.to_string() }
            }),
        };

        writeln!(stdout, "{}", serde_json::to_string(&envelope)?)?;
        stdout.flush()?;
    }
    Ok(())
}

// ─── Dispatcher ────────────────────────────────────────────────────────────

fn dispatch(
    method: &str,
    msg: &Value,
    pg_url: Option<&str>,
    mysql_url: Option<&str>,
) -> anyhow::Result<Value> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": {} },
            "serverInfo": {
                "name": "rivet-mcp",
                "version": env!("CARGO_PKG_VERSION")
            }
        })),

        "tools/list" => Ok(json!({ "tools": tools_list() })),

        "tools/call" => {
            let params = msg
                .get("params")
                .ok_or_else(|| anyhow::anyhow!("missing params"))?;
            let name = params
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing tool name"))?;
            let args = params.get("arguments").unwrap_or(&Value::Null);
            // Per MCP spec, tool execution errors are text content, not JSON-RPC errors.
            Ok(match call_tool(name, args, pg_url, mysql_url) {
                Ok(v) => v,
                Err(e) => json!({
                    "content": [{ "type": "text", "text": format!("error: {e}") }],
                    "isError": true
                }),
            })
        }

        _ => Err(anyhow::anyhow!("unknown method: {method}")),
    }
}

// ─── Tool registry ─────────────────────────────────────────────────────────

fn tools_list() -> Value {
    json!([
        {
            "name": "pg_active_sessions",
            "description": "Show non-idle Postgres sessions: pid, state, wait event, query snippet, user, application. Useful to spot blocked or long-running queries during an export.",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "pg_checkpoint_pressure",
            "description": "Show pg_stat_bgwriter counters: checkpoints_timed, checkpoints_req (write-pressure indicator), write/sync times, and buffer stats. Rivet adaptive mode reacts to checkpoints_req delta.",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "pg_table_stats",
            "description": "Top 20 Postgres tables by live row count: n_live_tup, n_dead_tup, dead ratio, last vacuum/analyze timestamps.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Restrict to a specific schema (default: all user schemas)"
                    }
                },
                "required": []
            }
        },
        {
            "name": "pg_locks",
            "description": "Show relation-level Postgres locks: pid, relation, mode, granted. Useful to diagnose lock contention during an export.",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "pg_top_queries_by_io",
            "description": "Top 10 queries by total I/O wait time from pg_stat_statements. Requires the pg_stat_statements extension; returns a clear error if unavailable.",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "mysql_processlist",
            "description": "Show MySQL SHOW PROCESSLIST: id, user, db, command, time, state, query snippet.",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "mysql_key_metrics",
            "description": "Key MySQL global status counters: Innodb_log_waits, Threads_running, Queries, Slow_queries, Innodb_row_lock_waits, Connections.",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "mysql_table_stats",
            "description": "Top 20 MySQL InnoDB tables by row count from information_schema.TABLES.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Restrict to a specific schema/database (default: all non-system schemas)"
                    }
                },
                "required": []
            }
        },
        {
            "name": "pgbouncer_pools",
            "description": "Show pgBouncer pool stats (SHOW POOLS) via the pgBouncer admin interface. Requires PGBOUNCER_ADMIN_URL env var (e.g. postgresql://pgbouncer@127.0.0.1:6432/pgbouncer).",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        },
        {
            "name": "pgbouncer_stats",
            "description": "Show pgBouncer per-database stats (SHOW STATS). Requires PGBOUNCER_ADMIN_URL env var.",
            "inputSchema": { "type": "object", "properties": {}, "required": [] }
        }
    ])
}

// ─── Tool dispatch ──────────────────────────────────────────────────────────

fn call_tool(
    name: &str,
    args: &Value,
    pg_url: Option<&str>,
    mysql_url: Option<&str>,
) -> anyhow::Result<Value> {
    match name {
        "pg_active_sessions" => text(pg_active_sessions(require_pg(pg_url)?)),
        "pg_checkpoint_pressure" => text(pg_checkpoint_pressure(require_pg(pg_url)?)),
        "pg_table_stats" => {
            let schema = args.get("schema").and_then(|v| v.as_str());
            text(pg_table_stats(require_pg(pg_url)?, schema))
        }
        "pg_locks" => text(pg_locks(require_pg(pg_url)?)),
        "pg_top_queries_by_io" => text(pg_top_queries_by_io(require_pg(pg_url)?)),
        "mysql_processlist" => text(mysql_processlist(require_mysql(mysql_url)?)),
        "mysql_key_metrics" => text(mysql_key_metrics(require_mysql(mysql_url)?)),
        "mysql_table_stats" => {
            let schema = args.get("schema").and_then(|v| v.as_str());
            text(mysql_table_stats(require_mysql(mysql_url)?, schema))
        }
        "pgbouncer_pools" => text(pgbouncer_query("SHOW POOLS")),
        "pgbouncer_stats" => text(pgbouncer_query("SHOW STATS")),
        other => Err(anyhow::anyhow!("unknown tool: {other}")),
    }
}

fn require_pg(url: Option<&str>) -> anyhow::Result<&str> {
    url.ok_or_else(|| {
        anyhow::anyhow!("no Postgres URL configured — pass --pg-url or set DATABASE_URL")
    })
}

fn require_mysql(url: Option<&str>) -> anyhow::Result<&str> {
    url.ok_or_else(|| {
        anyhow::anyhow!("no MySQL URL configured — pass --mysql-url or set DATABASE_URL")
    })
}

fn text(result: anyhow::Result<String>) -> anyhow::Result<Value> {
    let body = result.unwrap_or_else(|e| format!("error: {e}"));
    Ok(json!({ "content": [{ "type": "text", "text": body }] }))
}

// ─── Postgres tools ────────────────────────────────────────────────────────

fn pg_connect(url: &str) -> anyhow::Result<postgres::Client> {
    use postgres::NoTls;
    Ok(postgres::Client::connect(url, NoTls)?)
}

/// Convert a Postgres row cell to a displayable string.
fn pg_val(row: &postgres::Row, idx: usize) -> String {
    // Try common types in priority order; most pg_stat* columns are int8/float8/text.
    if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
        return v.unwrap_or_else(|| "NULL".into());
    }
    if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
        return v.map(|n| n.to_string()).unwrap_or_else(|| "NULL".into());
    }
    if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
        return v.map(|n| n.to_string()).unwrap_or_else(|| "NULL".into());
    }
    if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
        return v
            .map(|n| format!("{n:.2}"))
            .unwrap_or_else(|| "NULL".into());
    }
    if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
        return v.map(|b| b.to_string()).unwrap_or_else(|| "NULL".into());
    }
    if let Ok(v) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
        return v
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "NULL".into());
    }
    if let Ok(v) = row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
        return v
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "NULL".into());
    }
    "?".into()
}

fn pg_rows_to_table(rows: &[postgres::Row]) -> String {
    if rows.is_empty() {
        return "(no rows)".into();
    }
    let headers: Vec<String> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let data: Vec<Vec<String>> = rows
        .iter()
        .map(|row| (0..headers.len()).map(|i| pg_val(row, i)).collect())
        .collect();
    ascii_table(&headers, &data)
}

fn pg_active_sessions(url: &str) -> anyhow::Result<String> {
    let mut client = pg_connect(url)?;
    let rows = client.query(
        "SELECT pid::text, state, COALESCE(wait_event_type,'') AS wait_type,
                COALESCE(wait_event,'') AS wait_event,
                LEFT(COALESCE(query,''),100) AS query_snippet,
                usename, application_name
         FROM pg_stat_activity
         WHERE state IS DISTINCT FROM 'idle'
         ORDER BY state, pid",
        &[],
    )?;
    Ok(format!(
        "Active sessions ({})\n\n{}",
        rows.len(),
        pg_rows_to_table(&rows)
    ))
}

fn pg_checkpoint_pressure(url: &str) -> anyhow::Result<String> {
    let mut client = pg_connect(url)?;
    let rows = client.query(
        "SELECT checkpoints_timed, checkpoints_req,
                ROUND(checkpoint_write_time) AS write_ms,
                ROUND(checkpoint_sync_time) AS sync_ms,
                buffers_checkpoint, buffers_clean, buffers_backend,
                maxwritten_clean
         FROM pg_stat_bgwriter",
        &[],
    )?;
    Ok(format!("pg_stat_bgwriter\n\n{}", pg_rows_to_table(&rows)))
}

fn pg_table_stats(url: &str, schema: Option<&str>) -> anyhow::Result<String> {
    let mut client = pg_connect(url)?;
    let schema_filter = match schema {
        Some(s) => format!("AND schemaname = '{s}'"),
        None => "AND schemaname NOT IN ('pg_catalog','information_schema','pg_toast')".into(),
    };
    let sql = format!(
        "SELECT schemaname, relname AS tablename, n_live_tup, n_dead_tup,
                (n_dead_tup * 100 / NULLIF(n_live_tup + n_dead_tup, 0)) AS dead_pct,
                COALESCE(to_char(last_vacuum, 'YYYY-MM-DD HH24:MI'), '-') AS last_vacuum,
                COALESCE(to_char(last_analyze, 'YYYY-MM-DD HH24:MI'), '-') AS last_analyze
         FROM pg_stat_user_tables
         WHERE TRUE {schema_filter}
         ORDER BY n_live_tup DESC
         LIMIT 20"
    );
    let rows = client.query(&sql, &[])?;
    Ok(format!(
        "Table stats (top 20)\n\n{}",
        pg_rows_to_table(&rows)
    ))
}

fn pg_locks(url: &str) -> anyhow::Result<String> {
    let mut client = pg_connect(url)?;
    let rows = client.query(
        "SELECT l.pid::text, c.relname AS relation, l.mode, l.granted::text
         FROM pg_locks l
         LEFT JOIN pg_class c ON c.oid = l.relation
         WHERE l.relation IS NOT NULL
         ORDER BY l.granted, l.pid",
        &[],
    )?;
    if rows.is_empty() {
        return Ok("No relation-level locks held.".into());
    }
    Ok(format!(
        "Relation locks ({})\n\n{}",
        rows.len(),
        pg_rows_to_table(&rows)
    ))
}

fn pg_top_queries_by_io(url: &str) -> anyhow::Result<String> {
    let mut client = pg_connect(url)?;
    // Check that pg_stat_statements is available.
    let available: bool = client
        .query_one(
            "SELECT COUNT(*) > 0 FROM pg_extension WHERE extname = 'pg_stat_statements'",
            &[],
        )
        .ok()
        .and_then(|r| r.try_get::<_, bool>(0).ok())
        .unwrap_or(false);
    if !available {
        return Ok("pg_stat_statements extension is not installed. \
             Run: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
            .into());
    }
    let rows = client.query(
        "SELECT LEFT(query, 80) AS query, calls,
                ROUND(blk_read_time + blk_write_time) AS io_ms,
                ROUND(total_exec_time) AS total_exec_ms
         FROM pg_stat_statements
         ORDER BY blk_read_time + blk_write_time DESC
         LIMIT 10",
        &[],
    )?;
    Ok(format!(
        "Top 10 queries by I/O time\n\n{}",
        pg_rows_to_table(&rows)
    ))
}

// ─── MySQL tools ───────────────────────────────────────────────────────────

fn mysql_pool(url: &str) -> anyhow::Result<mysql::Pool> {
    use mysql::{Opts, OptsBuilder, PoolConstraints, PoolOpts};
    let opts = Opts::from(
        OptsBuilder::from_opts(Opts::from_url(url)?).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(1, 1).expect("valid pool constraints")),
        ),
    );
    Ok(mysql::Pool::new(opts)?)
}

fn mysql_rows_to_table(rows: &[Vec<String>], headers: &[String]) -> String {
    if rows.is_empty() {
        return "(no rows)".into();
    }
    ascii_table(headers, rows)
}

fn mysql_processlist(url: &str) -> anyhow::Result<String> {
    use mysql::prelude::*;
    let pool = mysql_pool(url)?;
    let mut conn = pool.get_conn()?;
    let mut result = conn.exec_iter("SHOW PROCESSLIST", ())?;
    let cols: Vec<String> = result
        .columns()
        .as_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let row_set = result
        .iter()
        .ok_or_else(|| anyhow::anyhow!("no result set"))?;
    let rows: Vec<Vec<String>> = row_set
        .filter_map(|r| r.ok())
        .map(|row| {
            (0..cols.len())
                .map(|i| match row.as_ref(i) {
                    Some(mysql::Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
                    Some(mysql::Value::Int(n)) => n.to_string(),
                    Some(mysql::Value::UInt(n)) => n.to_string(),
                    Some(mysql::Value::NULL) | None => "NULL".into(),
                    _ => "?".into(),
                })
                .collect()
        })
        .collect();
    Ok(format!(
        "SHOW PROCESSLIST ({})\n\n{}",
        rows.len(),
        mysql_rows_to_table(&rows, &cols)
    ))
}

fn mysql_key_metrics(url: &str) -> anyhow::Result<String> {
    use mysql::prelude::*;
    let pool = mysql_pool(url)?;
    let mut conn = pool.get_conn()?;
    let metrics = [
        "Innodb_log_waits",
        "Innodb_row_lock_waits",
        "Innodb_row_lock_time_avg",
        "Threads_running",
        "Threads_connected",
        "Queries",
        "Slow_queries",
        "Connections",
        "Aborted_connects",
    ];
    let in_clause = metrics
        .iter()
        .map(|m| format!("'{m}'"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT variable_name, variable_value \
         FROM information_schema.global_status \
         WHERE variable_name IN ({in_clause})"
    );
    let rows: Vec<(String, String)> = conn.query(sql)?;
    if rows.is_empty() {
        return Ok("(no metrics returned)".into());
    }
    let headers = vec!["metric".to_string(), "value".to_string()];
    let data: Vec<Vec<String>> = rows.into_iter().map(|(k, v)| vec![k, v]).collect();
    Ok(format!(
        "MySQL key metrics\n\n{}",
        ascii_table(&headers, &data)
    ))
}

fn mysql_table_stats(url: &str, schema: Option<&str>) -> anyhow::Result<String> {
    use mysql::prelude::*;
    let pool = mysql_pool(url)?;
    let mut conn = pool.get_conn()?;
    let schema_filter = match schema {
        Some(s) => format!("AND table_schema = '{s}'"),
        None => "AND table_schema NOT IN ('information_schema','performance_schema','mysql','sys')"
            .into(),
    };
    let sql = format!(
        "SELECT table_schema, table_name, table_rows, \
                data_length, index_length, engine \
         FROM information_schema.TABLES \
         WHERE table_type = 'BASE TABLE' {schema_filter} \
         ORDER BY table_rows DESC \
         LIMIT 20"
    );
    let mut result = conn.exec_iter(&sql, ())?;
    let cols: Vec<String> = result
        .columns()
        .as_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let row_set = result
        .iter()
        .ok_or_else(|| anyhow::anyhow!("no result set"))?;
    let rows: Vec<Vec<String>> = row_set
        .filter_map(|r| r.ok())
        .map(|row| {
            (0..cols.len())
                .map(|i| match row.as_ref(i) {
                    Some(mysql::Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
                    Some(mysql::Value::Int(n)) => n.to_string(),
                    Some(mysql::Value::UInt(n)) => n.to_string(),
                    Some(mysql::Value::NULL) | None => "NULL".into(),
                    _ => "?".into(),
                })
                .collect()
        })
        .collect();
    Ok(format!(
        "Table stats (top 20)\n\n{}",
        mysql_rows_to_table(&rows, &cols)
    ))
}

// ─── pgBouncer tools ───────────────────────────────────────────────────────

fn pgbouncer_query(sql: &str) -> anyhow::Result<String> {
    let admin_url = std::env::var("PGBOUNCER_ADMIN_URL").map_err(|_| {
        anyhow::anyhow!(
            "PGBOUNCER_ADMIN_URL not set. \
             Example: postgresql://pgbouncer@127.0.0.1:6432/pgbouncer"
        )
    })?;
    use postgres::NoTls;
    let mut client = postgres::Client::connect(&admin_url, NoTls)?;
    let rows = client.query(sql, &[])?;
    Ok(pg_rows_to_table(&rows))
}

// ─── ASCII table formatter ─────────────────────────────────────────────────

fn ascii_table(headers: &[impl AsRef<str>], rows: &[Vec<String>]) -> String {
    let ncols = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.as_ref().len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < ncols {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let fmt_row = |cells: &[String]| -> String {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:<width$}", c, width = widths.get(i).copied().unwrap_or(0)))
            .collect::<Vec<_>>()
            .join(" | ")
    };

    let header: Vec<String> = headers.iter().map(|h| h.as_ref().to_string()).collect();
    let separator = widths
        .iter()
        .map(|w| "-".repeat(*w))
        .collect::<Vec<_>>()
        .join("-+-");
    let body = rows
        .iter()
        .map(|r| fmt_row(r))
        .collect::<Vec<_>>()
        .join("\n");

    if body.is_empty() {
        format!("{}\n{}", fmt_row(&header), separator)
    } else {
        format!("{}\n{}\n{}", fmt_row(&header), separator, body)
    }
}
