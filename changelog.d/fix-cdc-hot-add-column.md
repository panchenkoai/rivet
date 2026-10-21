- **A column added to a captured table while a CDC run was open lost its values, exit 0.** A
  continuous run (`until_current: false`) froze its schema when it opened, so a hot `ALTER TABLE …
  ADD COLUMN` left the new column out of every change the run read after it, and the run acked them
  (#644; PostgreSQL, MySQL, SQL Server). **Now** the first change that names the new column closes
  the table's part under the old schema, the table's columns are read again and judged by
  `on_schema_drift` (`fail` stops before the change is acknowledged), and the run continues in a
  new part with the column and its values; `rivet load` takes both schemas into one change log.
  SQL Server's capture instance cannot carry a column added after it was created, so a run that
  sees its table outgrow the instance now stops with the new code `RIVET_SOURCE_CDC_SCHEMA_CHANGED`
  (exit 5) instead of ending clean. **What to do:** on SQL Server, create a capture instance that
  covers the column and re-baseline the table; elsewhere nothing.
