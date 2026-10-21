- **`rivet load` and `rivet compact` told a scheduler what happened to each table only in prose.** Through
  0.32.1 a per-table outcome was a `LOAD OK` / `COMPACT SKIP` line, and both commands always took every
  table of the config. **Now** both take `--export <name>` and `--table <table>` to work on a subset, and
  `--json` (stdout) / `--summary-output <file>` print one object with `run_id` and `per_table[]`: each
  table's `export`, `table`, `status` (`loaded` / `compacted`, `skipped`, `failed`), `skip_reason`
  (`up_to_date`; `no_buffer`, `full_load`, `warehouse_never_compacts`, `log_view`), `rows` and, for a
  failed table, the error object (`code`, `kind`, `class`, `exit_code`, `retryable`, `action`, `message`;
  ADR-0039 D1/D3). Under `--json` the result lines go to stderr. A name the config does not load is
  refused, `RIVET_CLI_EXPORT_UNKNOWN` or the new `RIVET_CLI_TABLE_UNKNOWN`, exit 1. Two detail lines of
  `rivet load` (`run(s) produced no files`, `gc-orphans: removed`) moved from stdout to stderr.
  **What to do:** nothing; a scheduler can read the object instead of the lines.
