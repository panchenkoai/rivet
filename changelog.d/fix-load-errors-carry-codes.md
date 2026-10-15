- **Most `rivet load` and `rivet compact` errors carried no `RIVET_*` code.** A refused warehouse
  object, an unsafe identifier, a missing key, an incomplete run under the prefix, a failed
  BigQuery job and the other stops raised under `src/load/` printed only their text, and most exited
  1 whatever their kind (0.31.0 and earlier). **Now** every error raised there carries a registered
  code, with the message text unchanged: `RIVET_LOAD_IDENTIFIER_UNSAFE`,
  `RIVET_LOAD_KEY_COLUMN_INVALID`, `RIVET_LOAD_PRIMARY_KEY_MISSING`, `RIVET_LOAD_RESERVED_COLUMN`,
  `RIVET_LOAD_LOOKALIKE_COLUMN`, `RIVET_LOAD_TARGET_COLLISION`, `RIVET_LOAD_STAGING_PREFIX_INVALID`,
  `RIVET_LOAD_DESTINATION_TYPE_UNSUPPORTED`, `RIVET_LOAD_PARTITION_BUDGET_EXCEEDED`,
  `RIVET_LOAD_FILE_LIMIT_EXCEEDED`, `RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED`, `RIVET_LOAD_TLS_CA_INVALID`
  (exit 1); `RIVET_LOAD_EMPTY_RUN_UNSUPPORTED`, `RIVET_LOAD_RUN_NOT_LOADABLE`,
  `RIVET_LOAD_STAGING_OBJECT_UNSAFE`, `RIVET_LOAD_SOURCES_SHARED`, `RIVET_LOAD_COLUMN_RESPELLED`,
  `RIVET_LOAD_TARGET_OBJECT_CONFLICT`, `RIVET_LOAD_COMPACT_BASE_MISSING`,
  `RIVET_LOAD_CHANGELOG_PARTITION_CHANGED`, `RIVET_LOAD_BUFFER_NOT_COMPACTED`,
  `RIVET_LOAD_REBASELINE_OVER_CHANGES`, `RIVET_LOAD_TABLE_BUSY` (refusals, exit 5);
  `RIVET_LOAD_MANIFEST_INCONSISTENT`, `RIVET_LOAD_SOURCE_FILE_MISMATCH`,
  `RIVET_LOAD_COPY_COUNT_MISMATCH` (integrity, exit 3); `RIVET_LOAD_LEDGER_UNREADABLE`,
  `RIVET_LOAD_RUN_LANDED_DURING_LOAD`, `RIVET_LOAD_WAREHOUSE_AUTH_FAILED`,
  `RIVET_LOAD_WAREHOUSE_ACCESS_DENIED`, `RIVET_LOAD_WAREHOUSE_OBJECT_NOT_FOUND`,
  `RIVET_LOAD_WAREHOUSE_REJECTED`, `RIVET_LOAD_WAREHOUSE_UNAVAILABLE` (environment, exit 2 when
  transient, else 1); `RIVET_LOAD_WAREHOUSE_ANSWER_UNREADABLE` and `RIVET_INTERNAL_LOAD_INVARIANT`
  (internal, exit 6). Some existing codes now also cover load sites (`RIVET_CONFIG_NO_EXPORTS`,
  `RIVET_CONFIG_MODE_SETTING_INVALID`, `RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED`,
  `RIVET_CONFIG_LOAD_PARTITION_UNSUPPORTED`, `RIVET_LOAD_TARGET_NOT_RIVETS`). A Snowflake or
  ClickHouse stop before the first statement (an empty newest run, more than 1,000 files, an unsafe
  `cluster_by`, a CDC log without a key) is now journaled `refused`, not `failed`, so it no longer
  counts as rivet having loaded the table. See `docs/reference/errors.md`. **What to do:** nothing;
  a script matching error text keeps working. A scheduler that keys on the exit code sees 5, 3 or 6
  where these stops used to exit 1.
