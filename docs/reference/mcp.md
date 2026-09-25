# `rivet-mcp` — database health for AI agents

`rivet-mcp` is a [Model Context Protocol](https://modelcontextprotocol.io/) server binary that lets an AI agent answer *"is this database healthy enough to extract from right now?"* — before any rows are touched.

Exposed read-only surfaces:

- **PostgreSQL** — `pg_stat_activity` (active queries, lock waits, idle-in-transaction), `pg_stat_statements` top I/O, checkpoint pressure (`pg_stat_bgwriter`), pgBouncer pool saturation and client wait time
- **MySQL** — `SHOW PROCESSLIST` (running queries and duration)

Works with [Claude Desktop](https://claude.ai/), [Claude Code](https://claude.ai/code), and any MCP-compatible client. It runs as a separate binary and never needs write access to the source database.

```bash
export DATABASE_URL="postgresql://..."
rivet-mcp        # reads DATABASE_URL from the environment
```

Add to your MCP client config:

```json
{
  "mcpServers": {
    "rivet": {
      "command": "rivet-mcp",
      "env": { "DATABASE_URL": "postgresql://..." }
    }
  }
}
```
