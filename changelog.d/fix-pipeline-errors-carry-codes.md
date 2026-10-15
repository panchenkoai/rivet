- **Most errors raised while running, planning, applying, reconciling, repairing and validating
  an export carried no `RIVET_*` code.** An unknown `--export`, a stale plan artifact, a NULL
  chunk key, an unfinished chunk checkpoint run, a committed part gone from the destination, a
  damaged `manifest.json`, failed chunks or keyset ranges, and the other stops raised under
  `src/pipeline/` printed only their text, and most exited 1 whatever their kind (0.31.0 and
  earlier). **Now** every error raised there carries a registered code, with the message text
  unchanged: `RIVET_CLI_EXPORT_UNKNOWN`, `RIVET_CLI_RUN_ID_EXPORT_MISMATCH`,
  `RIVET_CLI_OPTION_UNSUPPORTED`, `RIVET_CLI_REPORT_INVALID`, `RIVET_PLAN_NOTHING_TO_PLAN`,
  `RIVET_PLAN_ARTIFACT_NO_CREDENTIALS`, `RIVET_PLAN_PARTITION_TOKEN_UNEXPANDED`,
  `RIVET_PLAN_REJECTED`, `RIVET_PLAN_CHUNK_KEY_SPARSE`, `RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER`,
  `RIVET_CONFIG_SETTLE_COLUMN_NOT_TEMPORAL`, `RIVET_CONFIG_SPLIT_PREFIX_UNSTABLE`,
  `RIVET_CONFIG_EXPORT_NAME_RESERVED`, `RIVET_CONFIG_BATCH_MEMORY_EXCEEDED`,
  `RIVET_CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH`, `RIVET_SOURCE_VALUE_TOO_LARGE`,
  `RIVET_STATE_CHUNK_RUN_MISSING`, `RIVET_DEST_MANIFEST_MISSING` (usage, exit 1);
  `RIVET_PLAN_ARTIFACT_STALE`, `RIVET_PLAN_CHUNK_KEY_NULL`, `RIVET_SOURCE_KEYSET_KEY_COARSE`,
  `RIVET_STATE_CHUNK_RUN_UNFINISHED`, `RIVET_DEST_COMMITTED_PART_GONE`,
  `RIVET_DEST_CONTROL_ARTIFACT_INVALID` (refusals, exit 5); `RIVET_DEST_PART_ROWS_MISMATCH`
  (integrity, exit 3); `RIVET_RUN_UNITS_FAILED`, `RIVET_RUN_RECONCILE_UNVERIFIED`,
  `RIVET_DEST_MANIFEST_NOT_WRITTEN`, `RIVET_DEST_OBJECT_NOT_FOUND`, `RIVET_DEST_UPLOAD_CORRUPTED`,
  `RIVET_DEST_PREFIX_UNLISTABLE`, `RIVET_CLI_OUTPUT_UNWRITABLE`, `RIVET_CLI_INPUT_UNREADABLE`,
  `RIVET_CLI_EXECUTABLE_UNRESOLVED`, `RIVET_CLI_TEST_FAULT_INJECTED` (environment, exit 2 when
  transient, else 1); `RIVET_INTERNAL_RUN_INVARIANT` (internal, exit 6). Some existing codes now
  also cover pipeline sites (`RIVET_SOURCE_PROBE_UNREADABLE`, `RIVET_STATE_CHUNK_WINDOWS_BROKEN`,
  `RIVET_SOURCE_CDC_PREREQUISITE`, `RIVET_CONFIG_MODE_SETTING_INVALID`,
  `RIVET_CONFIG_CDC_SHAPE_INVALID`, `RIVET_LOAD_KEY_COLUMN_INVALID`, `RIVET_CONFIG_NO_EXPORTS`,
  `RIVET_CONFIG_FILE_UNREADABLE`, `RIVET_VALIDATE_UNVERIFIED`, `RIVET_STATE_NOT_WRITABLE`). An
  error rebuilt around another (the chunked planner's probe around `resolve_url`, a worker's
  state-database open, `state reset` / `state accept` around the source) keeps the inner error's
  code instead of dropping it. See `docs/reference/errors.md`. **What to do:** nothing; a script
  matching error text keeps working. A scheduler that keys on the exit code sees 5 where these
  stops used to exit 1: a plan artifact older than 24 h or with a drifted cursor applied without
  `--force`; NULL values in a range, date or `--pool --split` chunk key; a keyset page that ends
  on the key it started after; a plain run meeting an unfinished chunk checkpoint run; a
  committed part gone from the destination on resume; a `manifest.json` that does not parse or
  exceeds the read cap; a min/max/partition bound the planner cannot read, or a keyset key value
  it cannot read from a page's last row (`RIVET_SOURCE_PROBE_UNREADABLE`); a stored chunk window
  bound that is not an integer (`RIVET_STATE_CHUNK_WINDOWS_BROKEN`). It sees 3 where a part
  written under `validate: true` holds fewer rows than counted, and 6 where a run invariant
  broke (a `COUNT(*)` with no row, a chunk run recorded with no tasks, a reconcile over a
  non-chunked plan, an encode worker that stopped or panicked, a retry loop with no error to
  report).
