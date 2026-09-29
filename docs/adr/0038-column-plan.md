# ADR-0038: The Column Plan — One Type Decision for Check, Batch and CDC

- **Status:** Proposed
- **Date:** 2026-09-29
- **Context:** A survey of every source engine in both modes (2026-09-29) found three places that each decide what a column becomes: `rivet check` (the engine's `type_mappings()` plus `TypePolicy`), the batch row decoder (a `match` on the Arrow type inside each engine's `arrow_convert.rs`), and the CDC sink (`value::is_buildable` plus the shared `value::build_column`). None of them asks the others. Thirteen disagreements were proven in code, in four classes:
  1. **Check accepts, the run fails.** A `columns:` override the engine's decoder has no arm for (Oracle `date`/`int2`/`uuid`/`decimal(>38)`/`timestamp_ns`, MySQL `timestamp_ns`, SQL Server `decimal(>38)`), PostgreSQL arrays of date/uuid/json.
  2. **Silent loss.** The CDC sink turns any column it cannot build, including every `Unsupported` one, into an unlabelled `Utf8`; the CDC builder turned a cell that does not fit its column into NULL; MySQL batch turns unparsable integers, floats and dates into NULL.
  3. **Batch and CDC disagree on the same type.** `timestamp_ns`, PostgreSQL `uuid` read as text, MySQL invalid UTF-8, decimal scale 0, MySQL `TIME` outside one day.
  4. **An override is ignored or crashes.** MongoDB ignores `columns:`; a PostgreSQL integer override of another width panicked.

  The root: `TypePolicy` runs only in preflight, and a mapping's fidelity is derived from the `RivetType` alone, never from what the engine can actually decode.

  A second study (the type-delivery research, 2026-09-29, primary sources only) established, per type, whether the whole chain source driver → Arrow → Parquet → warehouse can hold a value exactly, and, where it cannot, which text form every engine can produce losslessly and every warehouse can recover.

---

## Decision

| ID | Name | Statement |
|----|------|-----------|
| **CP1** | The rule | A value reaches Parquet either as a native type that holds it exactly, or as text in the one canonical form its type is assigned (CP5). A NULL is written only for a source NULL. A value is never rounded, clamped or dropped without a refusal. A column is refused only when no lossless text exists for its type, and the refusal names the column and the way out. |
| **CP2** | The plan is `TypeMapping` | `TypeMapping` gains one field, `delivery: Delivery`, where `Delivery` is `Native` or `Text(TextForm)`. It is decided once, by the engine's `type_mappings()`, and every consumer reads it: the type report and `check`, the load spec, the batch schema and decoder, the CDC schema and builder. No consumer derives an Arrow type or a fallback of its own. `Text(_)` columns are `Utf8` with fidelity `LogicalString` and field metadata `rivet.text_form = <form>`. |
| **CP3** | Capability is declared, not implied | Each engine answers, per mode, one question: for this native column and this requested `RivetType`, which of MY decoders produces it exactly? The answer is an engine-local, closed enum, and the engine's decoder dispatches on that enum with an exhaustive `match`. A mapping the decoder has no arm for is therefore unrepresentable: if the engine has no exact decoder, the planner asks it for the column's text form (CP5); if there is none, the column is `Unsupported` and refused at `check`. |
| **CP4** | CDC reads the same plan | The CDC resolver keeps calling `type_mappings()`, with the mode set to CDC so the engine answers for its change-event converter. The sink builds exactly the planned type; `is_buildable` stops being a silent fallback and becomes an assertion the planner satisfies. A change event whose value does not fit its planned column fails the flush naming the column, before any checkpoint or ack. |
| **CP5** | Canonical text forms | `TextForm` is a closed enum; each variant fixes one rendering and names how each engine produces it and how each warehouse recovers it. Initial set: `DecimalPlain` (no exponent, no grouping), `Iso8601Duration`, `IsoTimestampNanos`, `TimeOfDayOffset`, `TimeBeyondDay` (`[-]hhh:mm:ss.ffffff`), `Uuid36`, `Json` (RFC 8259; MySQL typed scalars and Oracle OSON through their EXTENDED serializations), `HexWkb` (SRID carried separately), `HexBytes`, `BitString`, `InetText`, `RangeText`, `XmlText`, `ServerText` (the engine's own exact type output, for types with no portable form: PostgreSQL composites, multi-dimensional arrays, hstore, tsvector; SQL Server `hierarchyid`; `sql_variant` with its base type name). The server's default rendering is often lossy (SQL Server `CAST(float)` keeps 6 digits and `money` 2 decimals; Oracle `DATE` under NLS defaults drops the time; MySQL `VECTOR_TO_STRING` keeps 6 digits), so each engine's rendering is pinned per form and tested against the server's own value. |
| **CP6** | Policy runs in the run | `TypePolicy` is evaluated over the plan at the start of every batch and CDC run, with the same verdicts as `check`. `LogicalString` is accepted by strict policy (the value is preserved); `Lossy` and `Unsupported` are not. |
| **CP7** | Overrides are requests | A `columns:` entry asks for a `RivetType`. The planner grants it natively when the engine declares an exact decoder for that native column, grants text when the request is a string type, and otherwise refuses it at `check`, naming the column, the request and what the engine can deliver. A narrowing request (a wider source into a narrower type) stays `Lossy` and is refused under strict policy. MongoDB, whose exports are a document blob, refuses `columns:` by name instead of ignoring it. |
| **CP8** | Warehouses recover from the form | The load reads `rivet.text_form` from the load spec and, where the target can hold the value natively, creates the native column through the target's own conversion (for example `ST_GEOGFROMWKB`, `TO_UUID`, `PARSE_JSON`, `CAST AS BIGNUMERIC`); otherwise the column stays text and the type report prints the recovery expression through the existing `cast_sql` mechanism (ADR-0014 L5). |
| **CP9** | Evidence | `docs/type-capability-matrix.yaml` lists engine × mode × native type → delivery; its guard derives the engine, mode, `RivetType` and `TextForm` dimensions from the code and fails when a variant has no row. Each engine gets one live test that runs `check` and then the run over the full override matrix and asserts the verdicts are the same, with values compared to the source through DuckDB. |

## Why this shape

- **One field on an existing record.** `TypeMapping` already travels to every consumer (type report, load spec, schema-drift check, CDC resolver). Adding `delivery` there reaches all of them without a new type or a new call path.
- **Engine-local enums, not one shared decoder enum.** The engines' decoders differ in what they read (tokio-postgres wire types, `mysql::Value`, tiberius `ColumnData`, Oracle `OraKind`); a single cross-engine enum would be a union nobody implements fully. What is shared is the contract: the enum an engine returns is the enum its decoder matches on.
- **Text is a delivery, not a degradation.** `LogicalString` already means "value preserved as text"; a canonical form makes that claim checkable and gives every warehouse a recovery path.
- **Rejected: a separate capability predicate beside the decoders.** It would leave the predicate and the decoder's `match` as two lists that drift at the next new type — the defect this ADR removes.
- **Rejected: route batch through `RivetValue` and the CDC builder.** It unifies the builders but adds a per-cell enum allocation to the batch hot path (the export benchmark runs about 300k rows/s) and a migration of every decoder at once.

## Consequences

- Batch no longer refuses types it can deliver as text (PostgreSQL bare `numeric`, `money`, `inet`, ranges; MySQL `GEOMETRY`, `VECTOR`; SQL Server `sql_variant`, `hierarchyid`, spatial types). CDC no longer writes unlabelled text: the same columns carry the same form and metadata in both modes.
- A load spec written before this ADR has no `delivery`; it reads as `Native` for every column whose Arrow type is not `Utf8`, and as `Text(ServerText)` for `Utf8` columns whose `RivetType` is not a string type, which is what those runs wrote.
- The migration is per engine, each behind its own live check-equals-run test: Oracle, PostgreSQL, MySQL, SQL Server, MongoDB.
- Source-side limits that no mapping can fix are reported by `rivet doctor` / `check` as named findings, not discovered in the data: Oracle LogMiner ignoring tables with identity columns, BFILE or nested tables; SQL Server CDC writing NULL for unchanged `max` types in update-before and for computed columns; MySQL `binlog_row_image` other than `FULL`, `PARTIAL_JSON`, and `binlog_row_metadata=MINIMAL`; PostgreSQL unchanged TOAST values without `REPLICA IDENTITY FULL`.

## Sources

- PostgreSQL data types and output functions: https://www.postgresql.org/docs/current/datatype.html
- MySQL data types, vector functions, spatial formats: https://dev.mysql.com/doc/refman/8.4/en/data-types.html, https://dev.mysql.com/doc/refman/9.4/en/vector-functions.html, https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html
- SQL Server CAST and CONVERT styles: https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql
- Oracle data types, JSON serialization, LogMiner supported types: https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html, https://docs.oracle.com/en/database/oracle/oracle-database/23/adjsn/json-data-type.html, https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html
- MongoDB Extended JSON v2: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
- Parquet logical types: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
- Warehouse recovery functions: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions, https://docs.snowflake.com/en/sql-reference/functions/to_geography, https://clickhouse.com/docs/sql-reference/functions/geo/geometry, https://duckdb.org/docs/current/core_extensions/spatial/functions
