- **The `rivet load` / `rivet compact` document said how many rows, not what the warehouse did with
  them or what it cost.** **Now** each `per_table[]` entry also carries `rows_inserted`, `rows_updated`,
  `rows_deleted`, `rows_merged`, `base_rows_after`, `files`, `runs`, `warehouse_job_ids`,
  `bytes_processed` and `bytes_billed` (ADR-0039 D3). Every count is what the warehouse or the ledger
  returned: a compact's inserted / updated / deleted rows are its BigQuery MERGE jobs' `dmlStats` (a
  script's from its child jobs), the bytes are the jobs' `totalBytesProcessed` / `totalBytesBilled`, the
  job ids are BigQuery job ids or ClickHouse query ids, `files` / `runs` are what the load consumed from
  the ledger. A value nobody returned is `null`, never 0: the bytes on ClickHouse and Snowflake, the
  job ids on Snowflake, a load's updates and deletes, the deletes of a compact under `load.delete_flag`
  (BigQuery counts a flagged delete as an update). The only added read is one `jobs.list` per compact
  script (metadata, not billed). **What to do:** nothing; the keys are new.
