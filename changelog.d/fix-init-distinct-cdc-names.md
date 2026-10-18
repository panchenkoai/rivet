- **`rivet init` gave a table's batch and CDC exports one name and nested the CDC prefix in the
  batch one.** Both scaffolds named the export after the table, and the CDC one wrote
  `exports/<t>/cdc/` inside `exports/<t>/` (`./output/<t>/cdc/` locally), so the two exports shared
  a load spec in a shared state and a load of the batch prefix listed the stream's runs (0.32.0
  and earlier). **Now** a CDC scaffold names its export `<t>_cdc` and writes to the sibling
  `exports/<t>_cdc/` (`./output/<t>_cdc/`). **What to do:** nothing for new scaffolds; a config
  generated earlier keeps working, and `rivet load` already skips a nested CDC export's runs.
- **A schema-wide `rivet init` on SQL Server scaffolded the system tables SQL Server ships.** On a
  database with CDC enabled `dbo.systranschemas` became an export (0.32.0 and earlier). **Now**
  tables and views marked `IsMSShipped` are left out. **What to do:** delete such an export from a
  config generated earlier.
- **A SQL Server login without `VIEW SERVER PERFORMANCE STATE` filled the log with permission
  errors.** Each run sampled the server counters (`sys.dm_os_performance_counters` and the wait
  and query statistics) and the driver logged the server's refusal at ERROR, twice per run, on a
  run that otherwise succeeded (0.32.0 and earlier). **Now** rivet asks once per connection
  whether the login may read them and skips them quietly when it may not. **What to do:**
  nothing; grant `VIEW SERVER PERFORMANCE STATE` (2022) or `VIEW SERVER STATE` to keep the
  harm counters and the governor's pressure signal.
