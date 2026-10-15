- **Most errors raised while reading a source carried no `RIVET_*` code.** A malformed source
  URL, a CDC `table:` that names no single base table, a value the column type cannot hold, a
  SQL Server statement the server refused, a MySQL server with no binlog, a spill log that
  would not flush, a pgoutput message in a shape the reader never produces, and the other stops
  raised under `src/source/` (PostgreSQL, MySQL, SQL Server, Oracle, MongoDB and the shared CDC,
  spill, checksum and validate modules) printed only their text, and most exited 1 whatever
  their kind (0.31.0 and earlier). **Now** every error raised there carries a registered code,
  with the message text unchanged: `RIVET_CONFIG_SOURCE_URL_INVALID`,
  `RIVET_CONFIG_SOURCE_OPTION_UNSUPPORTED`, `RIVET_CONFIG_SOURCE_FEATURE_MISSING`,
  `RIVET_SOURCE_CDC_TABLE_UNRESOLVED`, `RIVET_SOURCE_CDC_TABLE_UNSUPPORTED`,
  `RIVET_SOURCE_CDC_TRANSACTION_TOO_LARGE`, `RIVET_SOURCE_QUERY_COLUMN_UNREFERENCEABLE` (usage,
  exit 1); `RIVET_SOURCE_KEYSET_KEY_UNORDERED` (refusal, exit 5); `RIVET_SOURCE_RUNTIME_UNAVAILABLE`,
  `RIVET_SOURCE_CONNECT_FAILED`, `RIVET_SOURCE_QUERY_FAILED`, `RIVET_SOURCE_CDC_SPILL_UNLOCKABLE`,
  `RIVET_SOURCE_CDC_SPILL_UNWRITABLE` (environment, exit 2 when transient, else 1);
  `RIVET_INTERNAL_SOURCE_INVARIANT` (internal, exit 6). Existing codes now also cover source
  sites (`RIVET_TYPE_UNSAFE_MAPPING`, `RIVET_SOURCE_VALUE_UNREPRESENTABLE`,
  `RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH`, `RIVET_SOURCE_VALUE_TOO_LARGE`,
  `RIVET_SOURCE_COLUMNS_CHANGED_UNDER_READ`, `RIVET_SOURCE_CDC_PREREQUISITE`,
  `RIVET_SOURCE_CDC_CELL_UNSUPPORTED`, `RIVET_SOURCE_CDC_UNDECODABLE`, `RIVET_SOURCE_CDC_TRUNCATED`,
  `RIVET_SOURCE_CDC_CHECKPOINT_INVALID`, `RIVET_CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH`,
  `RIVET_CONFIG_MODE_SETTING_INVALID`, `RIVET_CONFIG_EXPORT_SOURCE_INVALID`,
  `RIVET_CONFIG_VALUE_OUT_OF_RANGE`, `RIVET_LOAD_RESERVED_COLUMN`, `RIVET_STATE_CURSOR_INCOMPARABLE`,
  `RIVET_DEST_CONTROL_ARTIFACT_INVALID`, `RIVET_VALIDATE_UNVERIFIED`, `RIVET_INTERNAL_SPILL`,
  `RIVET_INTERNAL_TYPE_BUILDER`, `RIVET_INTERNAL_VALUE_CONVERTER`). An Oracle driver error is
  coded by phase (connect against statement) with the server's words kept; a MySQL anchor refused
  because the identity query failed keeps that query's own code; a pgoutput transaction poisoned
  by a refusal re-raises the original cause with its code. See `docs/reference/errors.md`.
  **What to do:** nothing; a script matching error text keeps working. A scheduler that keys on
  the exit code sees 5 where these stops used to exit 1: a batch value the column's type cannot
  hold (a PostgreSQL multi-dimensional array or an array of temporal/uuid/bytea elements, a
  NUMERIC NaN or infinity under a decimal target, a DECIMAL that does not fit the declared
  scale on PostgreSQL or MySQL, a SQL Server MONEY that is non-finite or past 2^53, a SQL Server
  decimal that overflows on rescale, an Oracle date, time or NUMBER outside its column type); a
  CDC cell the decoder has no faithful reading for (a deferred PostgreSQL infinity or
  unchanged-TOAST cell, a pgoutput column type with no binary decoder or no known OID, an
  Oracle value the miner cannot decode); a MySQL XA PREPARE or statement-logged DML on a
  captured table, an Oracle row written without ALL COLUMNS supplemental logging, and a MongoDB
  event past 16 MB (`RIVET_SOURCE_CDC_UNDECODABLE`); a MongoDB change stream invalidated
  (`RIVET_SOURCE_CDC_TRUNCATED`); a malformed SQL Server resume LSN or an unrecognised MongoDB
  resume-token checkpoint (`RIVET_SOURCE_CDC_CHECKPOINT_INVALID`); a MongoDB `_id` span a seek
  cannot page over, and a stored MongoDB keyset cursor token that no longer decodes
  (`RIVET_STATE_CURSOR_INCOMPARABLE`); a manifest column checksum that is not a number
  (`RIVET_DEST_CONTROL_ARTIFACT_INVALID`, under `rivet validate`). It sees 6 where a source
  reader invariant broke: a truncated or unexpected pgoutput message, a row before its Relation,
  a row image whose arity differs from its relation, a text-form cell on a binary slot, a
  spilled row that decodes to no change, a malformed slot LSN or position, a SQL Server open-time
  LSN or `sys.databases` answer in a shape the server never gives, a MySQL query with no result
  set, an Oracle SCN the server did not return or a mined row of an unexpected table, a MongoDB
  resume token that is not a document or a non-row operation reaching the row decoder, a column
  builder given a type it cannot build (`RIVET_INTERNAL_TYPE_BUILDER`), a value checksum that
  changed between read and Arrow build or a MONEY render that did not parse
  (`RIVET_INTERNAL_VALUE_CONVERTER`), and a spill frame that is truncated, oversized, nested too
  deep, not UTF-8, not a date-time, not JSON or followed by trailing bytes
  (`RIVET_INTERNAL_SPILL`). Environment stops keep their exit: 2 when the failure is transient,
  else 1, as before.
