# Rivet type mapping contract

This document describes how Rivet maps source column types to logical
[`RivetType`](../src/types/rivet_type.rs) values and Arrow/Parquet/CSV
representations. It is aligned with the automated suite in
[`tests/type_roundtrip/`](../tests/type_roundtrip/) and
[`tests/live_type_golden.rs`](../tests/live_type_golden.rs).

## Guarantees (v0.7.8)

- **DECIMAL / NUMERIC** are never silently converted to float. They export as
  Arrow `Decimal128` / `Decimal256` in Parquet when precision and scale are known
  (column override, catalog hint, or PostgreSQL wire metadata).
- **Binary** (`BYTEA`, `BLOB`, `BINARY`/`VARBINARY` with charset 63) stays
  Arrow `Binary` in Parquet.
- **JSON / JSONB** is valid JSON text in the file, paired with the Arrow
  `arrow.json` canonical extension type so parquet-rs emits native
  `LogicalType::Json` in the Parquet footer. The Rivet field metadata
  (`rivet.logical_type=json`) stays for Rivet-aware consumers.
- **UUID** exports as canonical 16-byte `FixedSizeBinary(16)` paired with the
  Arrow `arrow.uuid` canonical extension; parquet-rs emits native
  `LogicalType::Uuid`. Downstream Parquet readers (DuckDB, ClickHouse 25.x+,
  pyarrow, BigQuery autodetect) recover the UUID type without a cast.
- **Timestamps**: `TIMESTAMPTZ` and MySQL `TIMESTAMP` use UTC semantics
  (`timezone: Some("UTC")`); naive `TIMESTAMP` / `DATETIME` have no timezone.
- **Nullability** is preserved in Arrow schema and export.

### What `rivet init` auto-detects (and what needs an override)

`rivet init` reads the source database's catalog / wire-protocol metadata to
build the initial `rivet.yaml`. Whether a column lands as a native logical
type in the resulting Parquet depends on whether the source server
*advertises* the semantic — Rivet never guesses from column names or sample
values, by design (a wrong guess is silent corruption; an honest "I don't
know" is a config knob).

| Semantic | PostgreSQL | MySQL |
|----------|------------|-------|
| `JSON` / `JSONB` | **auto** — `Type::JSON` (OID 114) and `Type::JSONB` (OID 3802) are native PG wire types. | **auto** — `MYSQL_TYPE_JSON` is native since MySQL 5.7. |
| `UUID` | **auto** — `Type::UUID` (OID 2950) is a native PG type. | **manual override required.** MySQL has no native UUID; they are stored in `VARCHAR(36)` or `BINARY(16)`. The catalog reports only `varchar`/`binary` — semantic UUID information is gone before the driver ever sees the column. Operators add an explicit override (see below). |
| `DECIMAL(p,s)` | **auto when declared** — PG's catalog returns `numeric_precision`/`numeric_scale` for table-qualified queries; ad-hoc `numeric` expressions need an override. | **manual override required** for ad-hoc queries — the mysql wire protocol does not expose precision/scale on the column descriptor. Required for any column the user wants exact-decimal for. |

Adding overrides looks like:

```yaml
exports:
  - name: users
    query: "SELECT id, uid, amount FROM users"
    columns:
      uid: uuid              # MySQL VARCHAR(36) → UUID semantic
      amount: decimal(18,2)  # also valid for MySQL DECIMAL without catalog
```

The override path is safe: a `uid: uuid` declaration tries to parse each
cell — either 16 raw bytes (BINARY(16) layout) or a 36-char canonical text
form. **Parse failures emit `NULL`, never silent garbage.** Same convention
as `decimal` overrides whose values do not parse cleanly.

`rivet init` does **not** apply heuristics ("column is 36 chars wide and
named `uid_*` → probably UUID") — guessing risks misclassifying a non-UUID
varchar and silently producing wrong-shape Parquet. Operators who want UUID
semantics on a MySQL column add the override explicitly.

## Test commands

```bash
make test-types              # offline mapping contracts (PR-fast)
make test-types-live         # full matrix; requires docker compose
make test-types-validators   # PG/MySQL → Parquet → {DuckDB, ClickHouse} round-trip
```

`test-types-validators` re-runs the same canonical PG / MySQL type matrix used
by `test-types-live`, but writes the Parquet into the shared bind-mount under
`tests/.live-tmp/` and feeds it through **three independent readers**: DuckDB,
ClickHouse (both from `docker-compose.yaml`), and pyarrow (installed alongside
duckdb in the same container). Each reader catches what the others cannot:

| Reader | What it pins |
|--------|--------------|
| DuckDB | Autoload physical types (`DECIMAL`, `TIMESTAMPTZ`, `INTEGER[]`, `BLOB`, …), decimal sums, JSON validity, UUID parseability, byte-exact BLOB, list lengths (including empty), null-bitmap propagation |
| ClickHouse | Independent confirmation of the above through a second decoder; native `UInt64` round-trip for `BIGINT UNSIGNED`; tz-aware timestamps as `DateTime64(6, 'UTC')` |
| pyarrow | Arrow field metadata (`rivet.*` keys) reaches the Parquet footer; row-group statistics (`min`/`max`/`null_count`) are correct; Decimal256 (precision > 38) round-trips exactly where DuckDB downgrades to DOUBLE |

A fourth reader — **BigQuery** — is exercised by a separate harness
(`docs/bench/harness/type_bench_bq.sh`) that requires a Google Cloud
project + `bq` CLI auth. It loads each Rivet-produced Parquet via
`bq load --autodetect --source_format=PARQUET`, captures the
BigQuery-inferred schema, and asserts decimal sums round-trip through
`bq query`. See [`docs/bench/reports/REPORT_types_bigquery.md`](bench/reports/REPORT_types_bigquery.md)
for the per-column table and BigQuery-specific findings (notably:
`LogicalType::Json` does **not** autoload as native BQ `JSON` — it
falls back to `BYTES`/`STRING`; values are valid JSON but operators
need an explicit `--schema='attrs:JSON,...'` to query the structure).

In addition, two structural-only tests pin the Parquet layout itself —
[`parquet_schema.rs`](../tests/type_roundtrip/parquet_schema.rs) for
physical + logical types per column, and
[`parquet_metadata.rs`](../tests/type_roundtrip/parquet_metadata.rs) for the
`rivet.native_type` / `rivet.fidelity` / `rivet.logical_type` field metadata.

Coverage extensions live in dedicated files:
[`compression_matrix.rs`](../tests/type_roundtrip/compression_matrix.rs)
re-runs the export under `zstd` / `snappy` / `gzip` / `none` and asserts
value parity across all four codecs;
[`csv_load.rs`](../tests/type_roundtrip/csv_load.rs) replays the matrix
through DuckDB `read_csv_auto`;
[`pg_edge_cases.rs`](../tests/type_roundtrip/pg_edge_cases.rs) covers
decimal precision boundaries (38, 39 / Decimal128↔Decimal256), tz timestamps
pre-epoch and far-future, JSON deep nesting + unicode keys + i64 edges,
arrays with NULL elements, large single-cell strings.

### v0.7.8 breaking change: UUID export layout

UUID columns (PG native `uuid` type and MySQL columns explicitly overridden
as `columns: { col: uuid }`) now export as `FixedSizeBinary(16)` instead of
hyphenated `Utf8`. This is what lets parquet-rs emit native
`LogicalType::Uuid` so downstream readers (DuckDB, ClickHouse 25.x+,
pyarrow, BigQuery autodetect) recover the UUID type without a cast.

What this changes:

- **Parquet schema**: `BYTE_ARRAY + LogicalType::String` → `FIXED_LEN_BYTE_ARRAY + LogicalType::Uuid`.
- **On-disk bytes**: 36-char canonical ASCII (`a0eebc99-9c0b-…`) → 16 raw bytes (the same UUID, compact encoding).
- **CSV**: unchanged — the CSV writer still emits hyphenated lowercase text.
- **DuckDB autoload**: `VARCHAR` → `UUID` native type. A view like
  `SELECT uid::VARCHAR FROM read_parquet(...)` keeps working because
  DuckDB's `UUID::VARCHAR` cast yields the canonical form.
- **ClickHouse 24.8 autoload**: `String` → `FixedString(16)` (the bytes,
  not the text). To get back the canonical string, use
  `lower(hex(uid))` and reformat, or upgrade to ClickHouse 25.x which
  decodes `LogicalType::Uuid` directly.

Consumers reading the old Utf8-shaped Parquet files keep working — only
files produced by Rivet ≥ v0.7.8 carry the new layout.

### Findings & fixes from triangulating against external readers

Driving the matrix through DuckDB + ClickHouse + pyarrow exposed three real
defects in the PG / MySQL drivers; all have been fixed in v0.7.8:

* **PG arrays with NULL elements were silently lost.** The driver decoded
  `ARRAY[1, NULL, 3]` via `try_get::<Vec<i32>>`, which errors on a NULL
  element; the error was swallowed and a whole-row NULL was written. Fixed
  by deserializing as `Vec<Option<T>>` and pushing nulls through
  `ListBuilder::append_null` ([`src/source/postgres/arrow_convert.rs`](../src/source/postgres/arrow_convert.rs)).
* **MySQL ENUM / SET were misclassified as `String`.** They arrive on the
  wire as `MYSQL_TYPE_STRING` / `MYSQL_TYPE_VAR_STRING` with the
  `ENUM_FLAG` / `SET_FLAG` set, not as `MYSQL_TYPE_ENUM`. The mapper now
  checks the flag and emits `RivetType::Enum` so the
  `rivet.logical_type=enum` Parquet metadata is preserved.
* **MySQL `native_type` lost precision.** `tinyint unsigned`, `tinyint(1)`,
  `bit(1)`, `char` vs `varchar`, `binary` vs `varbinary` all collapsed to a
  single label. The mapper now distinguishes them via `column_type()` +
  `flags()` + `character_set`.

### Known limitations (pinned by `*currently_fails*` tests)

* PG `numeric(p, -s)` (negative scale) cannot be written to Parquet — the
  spec requires non-negative DECIMAL scale. Test
  `pg_edge_decimal_negative_scale_currently_fails_at_parquet_write`
  pins the failure with a friendly error message so any future workaround
  must update the test deliberately.
* DuckDB's `DECIMAL` is capped at precision 38 (HUGEINT-backed). For
  Parquet files with `precision > 38` DuckDB silently returns DOUBLE.
  Our file still carries `LogicalType::Decimal(p, s)` correctly — pyarrow
  decodes it as `Decimal256`. Verified in
  `pg_edge_decimal_boundaries_round_trip`.

## PostgreSQL

| Source type | Rivet logical | Parquet (Arrow) | CSV | Notes | Tested |
|-------------|---------------|-----------------|-----|-------|--------|
| `smallint` | `int16` | `Int16` | integer text | | golden |
| `integer` | `int32` | `Int32` | integer text | | golden |
| `bigint` | `int64` | `Int64` | integer text | | golden |
| `numeric(p,s)` | `decimal(p,s)` | `DECIMAL(p,s)` | exact decimal text | override if unbounded in query | contract + live matrix |
| `real` | `float32` | `Float32` | float text | | golden |
| `double precision` | `float64` | `Float64` | float text | | golden |
| `date` | `date` | `Date32` | ISO date | | golden |
| `time` | `time(microsecond)` | `Time64(µs)` | time text | | partial |
| `timestamp` | `timestamp(microsecond)` | `Timestamp(µs, None)` | datetime text | naive wall clock | live matrix |
| `timestamptz` | `timestamp_tz(µs, UTC)` | `Timestamp(µs, UTC)` | datetime text | | live matrix |
| `text` / `varchar` | `string` | `Utf8` | escaped UTF-8 | newlines/quotes escaped | live matrix |
| `bytea` | `binary` | `Binary` | hex or base64 per writer | | live matrix |
| `json` / `jsonb` | `json` | `Utf8` + Parquet `LogicalType::Json` (via `arrow.json` extension) | JSON string | | live matrix |
| `uuid` | `uuid` | `FixedSizeBinary(16)` + Parquet `LogicalType::Uuid` (via `arrow.uuid` extension) | canonical UUID text | downstream readers autoload as native UUID type | golden |
| `boolean` | `bool` | `Boolean` | true/false | | type_roundtrip |
| `numeric(10,2)` | `decimal(10,2)` | `DECIMAL(10,2)` | exact decimal text | second precision tier | type_roundtrip |
| `char` / `bpchar` | `string` | `Utf8` | escaped UTF-8 | padded char | type_roundtrip |
| `interval` | `interval` | `Utf8` (ISO 8601) | duration text | not Parquet Interval type | type_roundtrip |
| `enum` | `enum` | `Utf8` + logical=enum | label text | custom PG enum | type_roundtrip |
| `text[]` | `list<string>` | `List<Utf8>` | — | 1-D arrays | type_roundtrip |
| `integer[]` | `list<int32>` | `List<Int32>` | — | 1-D arrays | type_roundtrip |
| nullable / all-null | — | null bitmap preserved | empty cells | `note_nullable`, `note_all_null` | type_roundtrip |
| large `text` | `string` | `Utf8` | escaped | 2k–5k chars | type_roundtrip |

## MySQL

| Source type | Rivet logical | Parquet (Arrow) | CSV | Notes | Tested |
|-------------|---------------|-----------------|-----|-------|--------|
| `tinyint` (not width 1) | `int16` | `Int16` | integer text | widened signed | golden |
| `tinyint(1)` | `bool` | `Boolean` | 0/1 | MySQL boolean convention | golden |
| `smallint` | `int16` | `Int16` | integer text | | golden |
| `int` | `int32` | `Int32` | integer text | | golden |
| `bigint` | `int64` | `Int64` | integer text | signed | golden |
| `bigint unsigned` | `u_int64` → `UInt64` | `UInt64` | integer text | values &gt; i64::MAX | type_roundtrip |
| `decimal(p,s)` | `decimal(p,s)` | `DECIMAL(p,s)` | exact decimal text | requires `columns:` override | live matrix |
| `float` / `double` | `float32` / `float64` | `Float32` / `Float64` | float text | | golden |
| `date` | `date` | `Date32` | ISO date | | golden |
| `datetime` | `timestamp(µs, none)` | `Timestamp(µs, None)` | datetime text | naive | live matrix |
| `timestamp` | `timestamp_tz(µs, UTC)` | `Timestamp(µs, UTC)` | datetime text | `SET time_zone = '+00:00'` | live matrix |
| `time` | `time(µs)` | `Time64(µs)` | time text | | partial |
| `varchar` / `text` | `string` / `text` | `Utf8` | escaped UTF-8 | | live matrix |
| `json` | `json` | `Utf8` + Parquet `LogicalType::Json` (via `arrow.json` extension) | JSON string | | live matrix |
| `binary` / `varbinary` / `blob` | `binary` | `Binary` | hex in CSV | charset 63 / binary payload | type_roundtrip |
| `bit(1)` | `bool` | `Boolean` | | | golden |
| `bit(n>1)` | `int64` | `Int64` | | avoids silent truncation | type_roundtrip |
| `tinyint unsigned` | `int16` | `Int16` | integer text | 0–255 | type_roundtrip |
| `smallint unsigned` | `int32` | `Int32` | integer text | up to 65535 | type_roundtrip |
| `int unsigned` | `int64` | `Int64` | integer text | up to 4294967295 | type_roundtrip |
| `decimal(10,2)` | `decimal(10,2)` | `DECIMAL(10,2)` | exact decimal text | `columns:` override | type_roundtrip |
| `char` | `string` | `Utf8` | escaped | fixed `CHAR(n)` | type_roundtrip |
| `mediumtext` / `longtext` | `text` | `Utf8` | escaped | large payloads | type_roundtrip |
| `enum` / `set` | `enum` | `Utf8` + logical=enum | label text | SET comma-separated | type_roundtrip |
| `year` | `int16` | `Int16` | integer text | calendar year | type_roundtrip |
| `boolean` (native) | `bool` | `Boolean` | true/false | not only `TINYINT(1)` | type_roundtrip |
| nullable / all-null | — | preserved | empty cells | edge columns | type_roundtrip |

## Known gaps (tracked)

1. **MySQL `DECIMAL` without override**: autodetect yields `Unsupported` until
   `columns:` or `type_policy.decimal.unbounded` is set — by design (no float fallback).
2. **Nested arrays, ranges, inet, PostGIS**: not in v0.7.8 matrix (see roadmap).
3. **CSV `List` columns**: not serialized yet (Parquet only for array types).

## Fidelity labels

| Label | Meaning |
|-------|---------|
| `exact` | Value and type semantics preserved |
| `compatible` | Value preserved; physical type differs (e.g. UUID as Utf8) |
| `logical_string` | Valid text; native JSON tree semantics not enforced in Arrow |
| `lossy` | Rejected in strict mode |
| `unsupported` | Requires policy override |

See [`src/types/fidelity.rs`](../src/types/fidelity.rs).

## Downstream targets (autoload vs native)

Parquet/CSV preserve values and Arrow physical types. Warehouse engines infer types from
physical schema on autoload (e.g. JSON columns appear as `STRING` / `VARCHAR`). Native
target types (`JSON`, `VARIANT`, `UUID`, …) require a **materialization** step (cast SQL,
load schema, or typed view).

See [ADR-0014: Target type materialization](adr/0014-target-type-materialization.md) for
the full matrix (DuckDB, BigQuery, Snowflake, ClickHouse) and planned
`rivet check --type-report --target <engine>` extensions.

### Verified physical autoload (DuckDB + ClickHouse, v0.7.8)

`make test-types-validators` re-reads every PG / MySQL matrix column through
two independent engines and pins the autoload type. The full matrix:

| RivetType (PG / MySQL source) | DuckDB `DESCRIBE` | ClickHouse `DESCRIBE TABLE file()` |
|-------------------------------|--------------------|------------------------------------|
| `int16` / `smallint`          | `SMALLINT`         | `Nullable(Int16)`                  |
| `int32` / `integer`           | `INTEGER`          | `Nullable(Int32)`                  |
| `int64` / `bigint`            | `BIGINT`           | `Nullable(Int64)`                  |
| `u_int64` (MySQL `BIGINT UNSIGNED`) | `UBIGINT`    | `Nullable(UInt64)`                 |
| `decimal(p,s)`                | `DECIMAL(p,s)`     | `Nullable(Decimal(p, s))`          |
| `float32` / `real`            | `FLOAT`            | `Nullable(Float32)`                |
| `float64` / `double precision`| `DOUBLE`           | `Nullable(Float64)`                |
| `date`                        | `DATE`             | `Nullable(Date32)`                 |
| `time(µs)`                    | `TIME`             | `Nullable(DateTime64(6))`          |
| `timestamp(µs)` (naive)       | `TIMESTAMP`        | `Nullable(DateTime64(6))`          |
| `timestamp_tz(µs, UTC)`       | `TIMESTAMP WITH TIME ZONE` | `Nullable(DateTime64(6, 'UTC'))` |
| `string` / `text` / `enum` / `interval` | `VARCHAR` | `Nullable(String)` |
| `json` (PG `JSON`/`JSONB`, MySQL `JSON`) | `JSON` | `Nullable(String)` (ClickHouse 24.8) — DuckDB autoloads as native `JSON` |
| `uuid` (PG native; MySQL via override) | `UUID` | `Nullable(FixedString(16))` (ClickHouse 24.8) — DuckDB autoloads as native `UUID` |
| `binary` (`bytea` / `BLOB`)   | `BLOB`             | `Nullable(String)` (raw bytes)     |
| `bool`                        | `BOOLEAN`          | `Nullable(Bool)`                   |
| `list<inner>`                 | `inner[]`          | `Array(Nullable(inner))`           |

Round-trip values that the suite pins per row: `sum(decimal * 10^scale)`
matches the in-process Arrow check; `json_valid` / `isValidJSON` returns true
for every row; `TRY_CAST(uid AS UUID)` / `toUUID(uid)` parses every row; BLOB
columns compare byte-for-byte (`hex(...)`); the empty list survives; null
bitmaps propagate. The MySQL `BIGINT UNSIGNED` max value (`2^64 − 1`) is the
load-bearing assertion that exact-width unsigned ints are not silently
overflowed to i64.

### BigQuery autoload & recovery (verified live)

BigQuery's Parquet loader is weaker than DuckDB's: it ignores several Parquet
logical types on autoload and — critically — **will not coerce a column to a
different declared type on load**. A `bq load` into a table that declares
`JSON`/`DATETIME` is *rejected* (`Field x has changed type from JSON to BYTES`),
so native types are recovered with a **post-load transform, not a load schema**.

| RivetType | BigQuery autoload | Native | Recovery (post-load) |
|-----------|-------------------|--------|----------------------|
| `json`    | `BYTES`           | `JSON` | `PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING(col))` |
| `uuid`    | `BYTES` (16 raw)  | `STRING` | `TO_HEX(col)` |
| `timestamp` (naive) | `TIMESTAMP` (instant) | `DATETIME` | `DATETIME(col)` |
| `list<inner>` | `RECORD{item}` | `REPEATED inner` | load staging with `--parquet_enable_list_inference`, then `ARRAY(SELECT el.item FROM UNNEST(col) AS el)` |
| `u_int64` | `INT64` (overflows > 2^63−1) | `NUMERIC` | none post-load — fix at source: `columns: { c: decimal(20,0) }` |
| `timestamp_tz`, `decimal`, `string`, `binary`, `bool`, ints | native | same | — |

Rivet writes the Parquet list element as `item` (arrow-rs default, not the
spec's `element`), so even `--parquet_enable_list_inference` yields
`REPEATED RECORD{item}` rather than a clean `REPEATED <scalar>` — the `UNNEST`
flatten above is required.

`rivet check --type-report --target bigquery` prints the per-column autoload
type, the native type, and a ready-to-run recovery `CREATE TABLE … AS SELECT`
over the autoloaded `<table>__staging`. Set `exports[].target: bigquery` to get
it without the CLI flag. DuckDB needs none of this — it autoloads every logical
type natively.
