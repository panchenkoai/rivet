//! MCP server contract tests — verify the JSON-RPC 2.0 protocol surface
//! without a live database.
//!
//! These tests run the `rivet-mcp --stdio` binary as a subprocess and drive it
//! via its stdin pipe.  They assert the protocol structure (initialize,
//! tools/list, error responses) but NOT actual query results — those require
//! live infrastructure and are covered by live_* tests.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use common::RIVET_MCP_BIN;
use serde_json::{Value, json};

struct McpProcess {
    child: std::process::Child,
    reader: BufReader<std::process::ChildStdout>,
    writer: std::process::ChildStdin,
}

impl McpProcess {
    fn start() -> Self {
        let mut child = Command::new(RIVET_MCP_BIN)
            .args(["--stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn rivet-mcp");
        let stdout = child.stdout.take().unwrap();
        let writer = child.stdin.take().unwrap();
        Self {
            child,
            reader: BufReader::new(stdout),
            writer,
        }
    }

    fn send(&mut self, msg: &Value) {
        let line = serde_json::to_string(msg).unwrap() + "\n";
        self.writer.write_all(line.as_bytes()).unwrap();
        self.writer.flush().unwrap();
    }

    fn recv(&mut self) -> Value {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).expect("valid JSON from MCP server")
    }

    fn close(mut self) {
        drop(self.writer); // close stdin → EOF → server exits
        let _ = self.child.wait();
    }
}

// ─── Protocol tests ────────────────────────────────────────────────────────

#[test]
fn mcp_initialize_returns_correct_protocol_version() {
    let mut proc = McpProcess::start();
    proc.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "test", "version": "0" }
        }
    }));
    let resp = proc.recv();
    proc.close();

    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    assert_eq!(resp["result"]["protocolVersion"], "2024-11-05");
    assert_eq!(resp["result"]["serverInfo"]["name"], "rivet-mcp");
    assert!(resp["result"]["capabilities"]["tools"].is_object());
}

#[test]
fn mcp_tools_list_contains_expected_tools() {
    let mut proc = McpProcess::start();
    // Initialize first (required by spec).
    proc.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}));
    proc.recv();

    proc.send(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}));
    let resp = proc.recv();
    proc.close();

    let tools = resp["result"]["tools"].as_array().expect("tools is array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();

    for expected in &[
        "pg_active_sessions",
        "pg_checkpoint_pressure",
        "pg_table_stats",
        "pg_locks",
        "pg_top_queries_by_io",
        "mysql_processlist",
        "mysql_key_metrics",
        "mysql_table_stats",
        "pgbouncer_pools",
        "pgbouncer_stats",
    ] {
        assert!(
            names.contains(expected),
            "expected tool {expected} not in tools/list: {names:?}"
        );
    }
}

#[test]
fn mcp_tool_call_unknown_tool_returns_error_in_result() {
    let mut proc = McpProcess::start();
    proc.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}));
    proc.recv();

    // Unknown tool — the server returns a text result with "error: ..." content
    // (tools/call errors are surfaced as text, not as JSON-RPC errors).
    proc.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": { "name": "nonexistent_tool", "arguments": {} }
    }));
    let resp = proc.recv();
    proc.close();

    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .expect("content[0].text");
    assert!(
        text.starts_with("error:"),
        "expected error prefix, got: {text}"
    );
}

#[test]
fn mcp_unknown_method_returns_json_rpc_error() {
    let mut proc = McpProcess::start();
    proc.send(&json!({"jsonrpc":"2.0","id":99,"method":"unknown/method","params":{}}));
    let resp = proc.recv();
    proc.close();

    assert_eq!(resp["id"], 99);
    assert!(resp["error"].is_object(), "expected JSON-RPC error object");
    assert_eq!(resp["error"]["code"], -32_000);
}

#[test]
fn mcp_tool_call_no_pg_url_returns_error_content() {
    let mut proc = McpProcess::start();
    proc.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}));
    proc.recv();

    // No DATABASE_URL set — pg tools should return an error in the text content.
    proc.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": { "name": "pg_checkpoint_pressure", "arguments": {} }
    }));
    let resp = proc.recv();
    proc.close();

    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .expect("content[0].text");
    assert!(
        text.starts_with("error:"),
        "expected error when no pg_url, got: {text}"
    );
}

#[test]
fn mcp_notifications_without_id_are_silently_ignored() {
    let mut proc = McpProcess::start();
    proc.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}));
    proc.recv();

    // Notification (no id) — must NOT produce a response line.
    proc.send(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}));

    // Immediately send a real request to verify the server is still alive.
    proc.send(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}));
    let resp = proc.recv();
    proc.close();

    // We must receive the tools/list response (id=2), not an accidental response to the notification.
    assert_eq!(resp["id"], 2);
    assert!(resp["result"]["tools"].is_array());
}
