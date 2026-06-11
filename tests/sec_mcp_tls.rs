//! SEC-RED tests for the `rivet-mcp` server (`src/mcp.rs`).
//!
//! Cluster `mcp-tls` covers three audited issues in the MCP diagnostics
//! server, which talks JSON-RPC 2.0 over stdio to a prompt-injectable client
//! (an LLM such as Claude Desktop / Claude Code):
//!
//!   * V10 (MEDIUM) `src/mcp.rs:400` `mysql_pool` builds `Opts` from the URL
//!     with NO `ssl_opts`, so MySQL MCP diagnostics never use TLS even when the
//!     URL asks for it.
//!   * V18 (LOW)    `src/mcp.rs:219` `pg_connect` (and `pgbouncer_query`)
//!     hardcode `postgres::NoTls`, ignoring `sslmode=require`.
//!   * V11 (LOW)    `src/mcp.rs:43` / `:86` / `:213` error paths return the raw
//!     driver error to the client (`e.to_string()` / `format!("error: {e}")`)
//!     with NO pass through `rivet::redact::redact_secrets`.
//!
//! ## Why these are NOT cleanly unit-testable from `tests/` (feasible = false)
//!
//! The two TLS issues live in *private* connection builders — `mcp::mysql_pool`
//! and `mcp::pg_connect` are `fn`, not `pub fn`, so they are unreachable from an
//! integration-test crate. The only public seam is `rivet::mcp::run_stdio`,
//! which is driven here through the `rivet-mcp` binary. But `run_stdio` opens a
//! *live* connection inside each tool call (the builders are not pure
//! constructors — `mysql::Pool::new` / `postgres::Client::connect` dial the
//! socket), so "TLS was negotiated" can only be observed against a
//! TLS-*capable* server. The docker stack is plaintext 127.0.0.1 loopback (the
//! task forbids assuming docker TLS), so there is no environment in which these
//! assertions can be made RED today.
//!
//! For V11, an empirical probe of the built binary (every error shape:
//! connect-refused, URL parse error, bad scheme, bad port, TLS-handshake
//! failure, access-denied) showed neither the `postgres` nor the `mysql`
//! driver ever echoes the password into its error string. rivet itself never
//! interpolates the URL into an mcp error either. So a live "no leak" assertion
//! through `run_stdio` would pass against *today's* vulnerable code — it is not
//! reliably RED, because the (absent) leak depends on third-party driver output
//! that happens to be well-behaved in every reproducible case. The
//! deterministic V11 assertion ("mcp error formatting routes through
//! `redact_secrets`") can only be made once the private `dispatch`/`text`
//! helpers are refactored to a redacting chokepoint — i.e. in the fix wave.
//!
//! ## Fix direction (for the fix wave)
//!
//!   * V10 / V18: route mcp connection construction through the already
//!     TLS-aware source helpers — `crate::source::mysql::connect_pool(url, tls)`
//!     and `crate::source::postgres::connect_with_tls(url, tls)` (which derive a
//!     `TlsConfig` from `sslmode` / `ssl` URL params) — instead of the bare
//!     `Opts::from_url` + `NoTls` builders in `mcp.rs`. Then the existing
//!     `tests/live_pool_safety.rs` / source-level TLS tests cover the path, and
//!     a unit test on the (now shared) builder can assert `ssl_opts.is_some()`
//!     when the URL enforces TLS.
//!   * V11: wrap every mcp error-to-client boundary
//!     (`run_stdio`'s `e.to_string()`, `dispatch`'s `format!("error: {e}")`,
//!     `text`'s `format!("error: {e}")`) in `rivet::redact::redact_secrets`,
//!     then unit-test that a credentialed URL embedded in an error is redacted
//!     (the seam `rivet::redact::redact_secrets` is already `pub`).
//!
//! The tests below are written against the stable seams that DO exist today
//! (the `rivet-mcp` binary + the `pub` `rivet::redact::redact_secrets`) so they
//! compile now and document the SECURE behavior. They carry `#[ignore]` because
//! they cannot be made reliably-RED until the production seam above exists; the
//! fix wave converts them into true assertions on the shared TLS-aware builder.

mod common;

use common::*;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{Value, json};

/// Minimal JSON-RPC-over-stdio driver for the `rivet-mcp` binary, mirroring the
/// harness in `tests/mcp_contract.rs`. Spawned with caller-supplied DB-URL args
/// so each SEC test can point the server at a deliberately hostile URL.
struct McpProc {
    child: Child,
    reader: BufReader<ChildStdout>,
    writer: ChildStdin,
}

impl McpProc {
    fn start(extra_args: &[&str]) -> Self {
        let mut args = vec!["--stdio"];
        args.extend_from_slice(extra_args);
        let mut child = Command::new(RIVET_MCP_BIN)
            .args(&args)
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

/// Initialize the server, call one tool, and return the full JSON-RPC response.
fn call_tool(proc: &mut McpProc, tool: &str) -> Value {
    proc.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}));
    proc.recv();
    proc.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": { "name": tool, "arguments": {} }
    }));
    proc.recv()
}

/// Flatten the whole response (every text field + the raw envelope) into one
/// string so a leak anywhere in the payload is caught.
fn full_payload(resp: &Value) -> String {
    serde_json::to_string(resp).unwrap_or_default()
}

// SEC-RED V11: mcp error paths must redact credentialed URLs before returning
// them to the prompt-injectable client. Drives the binary with a URL whose
// userinfo carries a unique password and asserts the password never appears in
// the JSON-RPC error body. Asserts SECURE behavior; expected to FAIL until the
// mcp error boundary routes through `rivet::redact::redact_secrets`.
//
// NOTE (honesty): against today's well-behaved drivers this may pass even on
// vulnerable code because the driver does not echo the password — see the file
// header. The reliably-RED form is the unit assertion enabled by the fix.
#[test]
#[ignore = "live: pg (and not reliably RED until mcp routes errors through redact_secrets)"]
fn sec_mcp_error_is_redacted_pg() {
    require_alive(LiveService::Postgres);
    let secret = unique_name("leakpwpg");
    // Point at the live PG host but a non-existent database so the connect/auth
    // path errors with a credentialed URL in scope.
    let url = format!(
        "postgresql://rivet:{secret}@127.0.0.1:5432/{}",
        unique_name("nodb")
    );
    let mut proc = McpProc::start(&["--pg-url", &url]);
    let resp = call_tool(&mut proc, "pg_checkpoint_pressure");
    proc.close();

    let payload = full_payload(&resp);
    assert!(
        !payload.contains(&secret),
        "mcp must redact the URL password before returning the error to the client; \
         leaked secret {secret} in: {payload}"
    );
    // And the rivet redactor (the chokepoint the fix must use) does strip it,
    // proving the assertion is about wiring, not about the redactor itself.
    assert!(
        !rivet::redact::redact_secrets(&payload).contains(&secret),
        "redact_secrets must strip the embedded URL password"
    );
}

// SEC-RED V11: same invariant for the MySQL tool path.
#[test]
#[ignore = "live: mysql (and not reliably RED until mcp routes errors through redact_secrets)"]
fn sec_mcp_error_is_redacted_mysql() {
    require_alive(LiveService::Mysql);
    let secret = unique_name("leakpwmy");
    let url = format!(
        "mysql://rivet:{secret}@127.0.0.1:3306/{}",
        unique_name("nodb")
    );
    let mut proc = McpProc::start(&["--mysql-url", &url]);
    let resp = call_tool(&mut proc, "mysql_processlist");
    proc.close();

    let payload = full_payload(&resp);
    assert!(
        !payload.contains(&secret),
        "mcp must redact the URL password before returning the error to the client; \
         leaked secret {secret} in: {payload}"
    );
    assert!(
        !rivet::redact::redact_secrets(&payload).contains(&secret),
        "redact_secrets must strip the embedded URL password"
    );
}

// SEC-RED V18: a Postgres MCP URL that requests TLS (`sslmode=require`) must NOT
// fall through to the hardcoded `NoTls` transport. The secure server either
// negotiates TLS or refuses; it must never silently connect in plaintext.
//
// This cannot be made RED against the docker stack (plaintext loopback, no TLS
// listener), so it is gated and documents intent only. The fix routes mcp
// through `crate::source::postgres::connect_with_tls`, after which a
// TLS-capable fixture would let this assert a real handshake.
#[test]
#[ignore = "live: pg with TLS listener — docker pg is plaintext; fix routes mcp through source::postgres::connect_with_tls"]
fn sec_mcp_pg_honors_sslmode() {
    require_alive(LiveService::Postgres);
    // sslmode=require against a server that cannot do TLS must error, not
    // silently downgrade to NoTls. (Docker PG is plaintext, so today this hits
    // the hardcoded NoTls path and connects regardless of sslmode — the vuln.)
    let url = "postgresql://rivet:rivet@127.0.0.1:5432/rivet?sslmode=require";
    let mut proc = McpProc::start(&["--pg-url", url]);
    let resp = call_tool(&mut proc, "pg_checkpoint_pressure");
    proc.close();

    let payload = full_payload(&resp);
    let is_error =
        resp["result"]["isError"].as_bool().unwrap_or(false) || payload.contains("error:");
    assert!(
        is_error,
        "pg MCP tool with sslmode=require against a non-TLS server must refuse, \
         not silently connect over NoTls; got: {payload}"
    );
}

// SEC-RED V10: a MySQL MCP URL that requests TLS (`ssl-mode=REQUIRED`) must NOT
// be built without `ssl_opts`. The secure server must configure TLS from the
// URL rather than always connecting in plaintext.
//
// Same gating rationale as V18: docker MySQL is plaintext, so this documents
// intent. The fix routes mcp through `crate::source::mysql::connect_pool`
// (already TLS-aware via `build_mysql_ssl_opts`).
#[test]
#[ignore = "live: mysql with TLS listener — docker mysql is plaintext; fix routes mcp through source::mysql::connect_pool"]
fn sec_mcp_mysql_pool_sets_ssl_opts() {
    require_alive(LiveService::Mysql);
    // The mysql driver spells enforced TLS as the `ssl-mode=REQUIRED` URL param.
    let url = "mysql://rivet:rivet@127.0.0.1:3306/rivet?ssl-mode=REQUIRED";
    let mut proc = McpProc::start(&["--mysql-url", url]);
    let resp = call_tool(&mut proc, "mysql_processlist");
    proc.close();

    let payload = full_payload(&resp);
    let is_error =
        resp["result"]["isError"].as_bool().unwrap_or(false) || payload.contains("error:");
    assert!(
        is_error,
        "mysql MCP tool with ssl-mode=REQUIRED against a non-TLS server must \
         refuse, not silently connect without ssl_opts; got: {payload}"
    );
}
