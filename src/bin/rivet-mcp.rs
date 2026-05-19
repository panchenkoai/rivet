//! `rivet-mcp` — MCP (Model Context Protocol) server for read-only DB introspection.
//!
//! Speaks JSON-RPC 2.0 over stdin/stdout (one object per line). Integrates with
//! Claude Desktop and Claude Code. See the `rivet::mcp` module for the tool list
//! and protocol details.
//!
//! ## Claude Desktop config
//!
//! Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:
//! ```json
//! {
//!   "mcpServers": {
//!     "rivet": {
//!       "command": "rivet-mcp",
//!       "args": ["--stdio"],
//!       "env": { "DATABASE_URL": "postgresql://user:pass@localhost/db" }
//!     }
//!   }
//! }
//! ```
//!
//! ## Claude Code
//!
//! ```bash
//! claude mcp add rivet -- rivet-mcp --stdio
//! ```

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "rivet-mcp",
    version,
    about = "MCP (Model Context Protocol) server: read-only PG/MySQL/pgBouncer diagnostics over JSON-RPC stdio"
)]
struct Args {
    /// Use stdio transport (the only supported transport; flag kept for symmetry with other MCP servers).
    #[arg(long, default_value_t = true)]
    stdio: bool,

    /// Postgres connection URL (overrides DATABASE_URL env var).
    #[arg(long, env = "DATABASE_URL", value_name = "URL")]
    pg_url: Option<String>,

    /// MySQL connection URL.
    #[arg(long, value_name = "URL")]
    mysql_url: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    rivet::mcp::run_stdio(args.pg_url.as_deref(), args.mysql_url.as_deref())
}
