- **Most config errors carried no `RIVET_*` code.** A missing config file, broken YAML, an
  incomplete `source:` block, an unset `url_env`, a mode without its setting and the other
  refusals raised while a config is read printed only their text, so a scheduler or a script could
  match them by wording alone (0.31.0 and earlier). **Now** every error raised under `src/config/`
  carries a registered code, with the message text unchanged: `RIVET_CONFIG_FILE_UNREADABLE`,
  `RIVET_CONFIG_YAML_INVALID`, `RIVET_CONFIG_SOURCE_CONNECTION_INVALID`, `RIVET_CONFIG_SECRET_UNSET`,
  `RIVET_CONFIG_EXPORT_SOURCE_INVALID`, `RIVET_CONFIG_MODE_SETTING_INVALID`,
  `RIVET_CONFIG_DESTINATION_AUTH_INVALID`, `RIVET_CONFIG_DESTINATION_ENDPOINT_UNSAFE`,
  `RIVET_CONFIG_VALUE_OUT_OF_RANGE`, `RIVET_CONFIG_CDC_SHAPE_INVALID`,
  `RIVET_CONFIG_CSV_COMPRESSION_UNSUPPORTED`, `RIVET_CONFIG_TLS_VERIFICATION_DOWNGRADE`,
  `RIVET_CONFIG_LOAD_BLOCK_INVALID`, `RIVET_CONFIG_PATH_UNSAFE`, `RIVET_CONFIG_ROW_HASH_INVALID` and
  `RIVET_CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN` (see `docs/reference/errors.md`). The exit code stays
  1. **What to do:** nothing; a script matching config error text keeps working.
