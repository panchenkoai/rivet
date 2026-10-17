- **An export whose source table had been dropped failed with the raw driver error and no code.**
  `rivet run` over a table dropped since the config was generated ended with the engine's own words
  as its `Error:` line and no `RIVET_*` code on MySQL and PostgreSQL (`Error: MySqlError { ERROR
  1146 (42S02): Table 'rivet.drop_probe_b' doesn't exist }`, exit 1), or `RIVET_SOURCE_QUERY_FAILED`
  on SQL Server and Oracle; `rivet check` refused the same table as `RIVET_SOURCE_QUERY_FAILED`
  (0.30.0 and 0.31.0). **Now** one function recognises each engine's own verdict (MySQL 1146,
  PostgreSQL SQLSTATE 42P01, SQL Server 208, Oracle ORA-00942) on every path that reads the table
  (the batch read, the parallel runners, the chunk planner's probe, the CDC snapshot leg, `check`
  and `plan`), and the export stops as `Error: [RIVET_SOURCE_TABLE_NOT_FOUND] export 'drop_probe_b':
  the table it reads does not exist (`drop_probe_b`, read by its `query:`). Correct the export's
  `table:` or `query:`, or remove the export from `exports:` if the table was dropped on purpose:
  MySqlError { ERROR 1146 (42S02): Table 'rivet.drop_probe_b' doesn't exist }` (exit 1). The exit
  stays 1 and the other exports of the same run still deliver (`1 success · 1 failed`), so alerting
  on the exit code sees no change; `rivet plan` keeps `RIVET_PLAN_SOURCE_UNREADABLE` as its outer
  code, as it does for a missing MongoDB collection, with this refusal as the cause. **What to do:**
  remove the export from `exports:` if the table was dropped on purpose, or correct its `table:`.
