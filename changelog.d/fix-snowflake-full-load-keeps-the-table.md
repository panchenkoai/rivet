- **Behaviour change: a Snowflake full load emptied its table before the COPY.** The load ran
  `CREATE OR REPLACE TABLE` and then `COPY` as one script, so a COPY that failed (an unreadable
  part, a lost client, a killed `snow`) left the table with 0 rows until a later load succeeded
  (0.31.0 and earlier; measured: 20 rows before, 0 after a COPY of a corrupted part). **Now** the
  parts land in a new table `<table>__rivet_load_<16 hex>`, the landed row count is checked against
  the runs, and one `CREATE OR REPLACE TABLE <table> CLONE <staging> COPY GRANTS` puts it in place;
  any failure before that statement leaves the table as it was and drops the staging table. A table
  that existed before the upgrade is replaced like any other. Grants made on the table now survive a
  full load (`COPY GRANTS`); before, every full load dropped them. **What to do:** nothing; a load
  killed between the COPY and the clone can leave a `<table>__rivet_load_*` table, which no view or
  load reads: drop it.
