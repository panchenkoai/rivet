- **`rivet run` on a PostgreSQL source crashed when the process had no file descriptor left for the
  driver's runtime.** Under a low `RLIMIT_NOFILE` the PostgreSQL client's own runtime build hit
  `Too many open files` and the process ended with a panic (exit 101, no `Error:` line), while
  MySQL, SQL Server, Oracle and MongoDB failed loudly under the same cap (0.31.0 and earlier).
  **Now** every PostgreSQL dial (the source, CDC, the Postgres state, `init`, `doctor`, `check`, the
  MCP server) first proves the process can start a runtime and is refused
  `RIVET_SOURCE_RUNTIME_UNAVAILABLE` (an environment stop) when it cannot. **What to do:** raise the
  open-files limit of the process that runs rivet.
