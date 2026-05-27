# Loading rivet Parquet into Snowflake

Snowflake does not yet exist as a rivet `destination` (see
[ADR-0014](../adr/0014-target-type-materialization.md) §6 for the planned
resolver work). Operators today land Parquet locally (or in cloud object
storage) and load it into Snowflake themselves. This recipe is the canonical
sequence for that load — built around Snowsight's web Worksheets, since the
`snowsql` CLI requires MFA enrollment that many new accounts do not have
configured.

It documents the **specific Snowflake autoload quirks** that bite rivet's
type matrix (binary, JSON, UUID, microsecond Time / Timestamp, UINT64) and the
minimal set of overrides that recovers fidelity.

> Verified end-to-end against the type-matrix Parquet files produced by
> `tests/type_roundtrip/fixtures/{postgres,mysql}_*.sql`. All 28 PG columns and
> 38 MySQL columns roundtrip with values intact (including microsecond
> precision, `u64::MAX`, raw binary bytes, canonical UUID, multi-byte UTF-8).

## Prerequisites

1. A rivet export produced with format `parquet`. For MySQL columns that may
   carry `BIGINT UNSIGNED` values above `2^63-1`, add a column override to
   ride that field as exact decimal — Snowflake's Parquet reader rejects raw
   UINT64 above that bound:

   ```yaml
   exports:
     - name: my_export
       columns:
         c_bigint_u: decimal(20,0)
   ```

   (`string` also works as an alternative if you prefer to cast on the
   warehouse side.)

2. A Snowflake account with at least one warehouse, database, and schema you
   can write to. The examples below assume:

   ```
   WAREHOUSE = COMPUTE_WH
   DATABASE  = RIVET_DATA_TOOL
   SCHEMA    = PUBLIC
   STAGE     = RIVET_STG (created in step 3)
   ```

3. Access to [Snowsight](https://app.snowflake.com) with a role that can
   `CREATE STAGE`, `CREATE TABLE`, and upload files.

## The load

### 1. Create the stage

Open a Worksheet and run:

```sql
USE WAREHOUSE COMPUTE_WH;
USE DATABASE  RIVET_DATA_TOOL;
USE SCHEMA    PUBLIC;

CREATE STAGE IF NOT EXISTS RIVET_STG
  FILE_FORMAT = (TYPE = PARQUET);
```

### 2. Upload the Parquet files

In the Snowsight left nav: **Data → Databases → RIVET_DATA_TOOL → PUBLIC →
Stages → RIVET_STG → "+ Files"**. Drop the `.parquet` files there. No
subdirectory needed.

### 3. Run the load script

Pick `pg` or `mysql` below and run it in a Worksheet. Replace the filename in
the `FROM @RIVET_STG/...` clause with the actual file you uploaded.

The script uses a **two-step pattern** — `COPY` into a *staging* table with
"forgiving" types (NUMBER for raw int64 µs, VARCHAR for JSON text, BINARY for
raw bytes) followed by a `CREATE TABLE AS SELECT` that applies the necessary
transforms (`TIME_FROM_PARTS`, `TO_TIMESTAMP_NTZ`, `PARSE_JSON`, canonical
UUID formatting). This is the only shape we found that survives all the
autoload caveats listed below.

#### Postgres matrix

```sql
USE WAREHOUSE COMPUTE_WH;
USE DATABASE  RIVET_DATA_TOOL;
USE SCHEMA    PUBLIC;

-- Snowflake autoload for Parquet `Timestamp(MICROSECOND, isAdjustedToUTC=true)`
-- into TIMESTAMP_TZ uses the *session* offset as the recorded TZ, which shifts
-- the absolute instant by that offset. Pinning the session to UTC for the
-- duration of the load keeps (wall_clock, offset) = (UTC, +00:00) and preserves
-- the original instant.
ALTER SESSION SET TIMEZONE = 'UTC';

CREATE OR REPLACE TABLE PG_STAGE (
    id            NUMBER(38,0),
    c_smallint    NUMBER(38,0),
    c_integer     NUMBER(38,0),
    c_bigint      NUMBER(38,0),
    amount        NUMBER(18,2),
    fee           NUMBER(20,6),
    price         NUMBER(10,2),
    c_real        FLOAT,
    c_double      FLOAT,
    c_date        DATE,
    c_time        NUMBER(38,0),       -- µs of day (Parquet Time64)
    created_at    NUMBER(38,0),       -- µs of epoch (no-tz)
    created_at_tz TIMESTAMP_TZ(6),
    label         VARCHAR,
    c_varchar     VARCHAR,
    c_bpchar      VARCHAR,
    raw_bytes     BINARY,
    uid           BINARY,             -- 16-byte UUID payload
    attrs         VARCHAR,            -- JSON text
    attrs_json    VARCHAR,
    c_bool        BOOLEAN,
    interval_col  VARCHAR,
    enum_col      VARCHAR,
    tags          ARRAY,
    nums          ARRAY,
    large_text    VARCHAR,
    note_nullable VARCHAR,
    note_all_null VARCHAR
);

COPY INTO PG_STAGE
FROM @RIVET_STG/<your_pg_file>.parquet
FILE_FORMAT = (TYPE = PARQUET BINARY_AS_TEXT = FALSE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE TABLE PG AS
SELECT
    id, c_smallint, c_integer, c_bigint,
    amount, fee, price, c_real, c_double, c_date,
    TIME_FROM_PARTS(0, 0, 0, c_time * 1000)                         AS c_time,
    TO_TIMESTAMP_NTZ(created_at, 6)                                 AS created_at,
    created_at_tz,
    label, c_varchar, c_bpchar,
    raw_bytes,
    REGEXP_REPLACE(LOWER(HEX_ENCODE(uid)),
        '^(.{8})(.{4})(.{4})(.{4})(.{12})$', '\\1-\\2-\\3-\\4-\\5')   AS uid,
    PARSE_JSON(attrs)        AS attrs,
    PARSE_JSON(attrs_json)   AS attrs_json,
    c_bool, interval_col, enum_col, tags, nums,
    large_text, note_nullable, note_all_null
FROM PG_STAGE;

DROP TABLE PG_STAGE;
```

#### MySQL matrix

```sql
USE WAREHOUSE COMPUTE_WH;
USE DATABASE  RIVET_DATA_TOOL;
USE SCHEMA    PUBLIC;

ALTER SESSION SET TIMEZONE = 'UTC';

CREATE OR REPLACE TABLE MS_STAGE (
    id            NUMBER(38,0),
    c_tinyint     NUMBER(38,0),
    c_tinyint_u   NUMBER(38,0),
    c_bool        BOOLEAN,
    c_boolean     BOOLEAN,
    c_smallint    NUMBER(38,0),
    c_smallint_u  NUMBER(38,0),
    c_int         NUMBER(38,0),
    c_int_u       NUMBER(38,0),
    c_bigint      NUMBER(38,0),
    c_bigint_u    NUMBER(20,0),       -- export with `c_bigint_u: decimal(20,0)` override
    amount        NUMBER(18,2),
    fee           NUMBER(20,6),
    price         NUMBER(10,2),
    c_float       FLOAT,
    c_double      FLOAT,
    c_date        DATE,
    c_time        NUMBER(38,0),
    created_at_dt NUMBER(38,0),
    created_at_ts TIMESTAMP_TZ(6),
    label         VARCHAR,
    c_char        VARCHAR,
    c_text        VARCHAR,
    c_varchar     VARCHAR,
    long_text     VARCHAR,
    medium_text   VARCHAR,
    raw_bytes     BINARY,
    var_bytes     BINARY,
    blob_bytes    BINARY,
    uid           VARCHAR(36),
    extras        VARCHAR,
    enum_col      VARCHAR,
    set_col       VARCHAR,
    year_col      NUMBER(4,0),
    c_bit1        BOOLEAN,
    c_bit8        NUMBER(38,0),
    note_nullable VARCHAR,
    note_all_null VARCHAR
);

COPY INTO MS_STAGE
FROM @RIVET_STG/<your_mysql_file>.parquet
FILE_FORMAT = (TYPE = PARQUET BINARY_AS_TEXT = FALSE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE TABLE MS AS
SELECT
    id, c_tinyint, c_tinyint_u, c_bool, c_boolean,
    c_smallint, c_smallint_u, c_int, c_int_u, c_bigint, c_bigint_u,
    amount, fee, price, c_float, c_double, c_date,
    TIME_FROM_PARTS(0, 0, 0, c_time * 1000)               AS c_time,
    TO_TIMESTAMP_NTZ(created_at_dt, 6)                    AS created_at_dt,
    created_at_ts,
    label, c_char, c_text, c_varchar, long_text, medium_text,
    raw_bytes, var_bytes, blob_bytes,
    uid,
    PARSE_JSON(extras)                                    AS extras,
    enum_col, set_col, year_col, c_bit1, c_bit8,
    note_nullable, note_all_null
FROM MS_STAGE;

DROP TABLE MS_STAGE;
```

## Why these specific options

The non-obvious choices, in order of how surprising they were:

1. **`BINARY_AS_TEXT = FALSE`** is required in the `FILE_FORMAT`. Snowflake's
   default treats Parquet `BYTE_ARRAY` *without a logical type* as UTF-8 text,
   so binary columns containing `0xFF` byte fail with `Invalid UTF8 detected
   while decoding`. Text and JSON columns carry their own `LogicalType` and
   are unaffected.
2. **Two-step (stage → final)** instead of `COPY INTO final FROM (SELECT ...)`.
   The `$1:col::varchar` syntax that the single-step pattern needs also hits
   the UTF-8 decode on raw-binary BYTE_ARRAY columns — even when the target
   cast is `::binary`. `MATCH_BY_COLUMN_NAME` on a stage table with `BINARY`
   columns avoids that path.
3. **`ALTER SESSION SET TIMEZONE = 'UTC'`** before the load. Snowflake's
   autoload of `Timestamp(MICROSECOND, isAdjustedToUTC=true)` into
   `TIMESTAMP_TZ` records the *session* offset as the column's TZ, which
   shifts the absolute instant by that offset. Pinning the session to UTC
   makes the recorded offset `+00:00` so the instant survives. (BigQuery does
   not have this issue.)
4. **`NUMBER(38,0)` for `c_time`, `created_at`, `created_at_dt`** in staging.
   Snowflake's autoload does not recognize Arrow `Time64(MICROSECOND)` or
   `Timestamp(MICROSECOND, isAdjustedToUTC=false)` as `TIME`/`TIMESTAMP_NTZ`;
   it surfaces the raw `int64` µs. We accept the raw value into NUMBER and
   convert with `TIME_FROM_PARTS` / `TO_TIMESTAMP_NTZ` in the final SELECT.
5. **`HEX_ENCODE(uid)` + regex** because `LogicalType::Uuid` arrives as
   `BINARY` (16 bytes); the canonical `8-4-4-4-12` hyphenated form is rebuilt
   by SQL.
6. **`PARSE_JSON(...)` for `attrs` / `attrs_json` / `extras`** because
   `LogicalType::Json` arrives as `VARCHAR`, not `VARIANT`. The bytes are
   already valid UTF-8 JSON so the parse is cheap.

## Sanity checks

After the final tables are populated, the following queries should all return
the original source values:

```sql
-- microseconds preserved on Time / Timestamp
SELECT id,
       TO_VARCHAR(c_time,        'HH24:MI:SS.FF6')                        AS c_time_us,
       TO_VARCHAR(created_at,    'YYYY-MM-DD HH24:MI:SS.FF6')             AS created_at_us,
       TO_VARCHAR(created_at_tz, 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')     AS created_at_tz_iso
FROM PG ORDER BY id;

-- Multi-byte UTF-8 survives
SELECT note_nullable, HEX_ENCODE(note_nullable) FROM MS WHERE id = 4;
-- expected hex: 756E69636F64653A20E697A5E69CACE8AA9E20F09F9A80
--                                    ^^^^^^^^^^^^^^^^^^^^^^^^ '日本語 🚀'

-- UINT64 max round-trips through the decimal override
SELECT c_bigint_u FROM MS WHERE id = 1;
-- expected: 18446744073709551615

-- Binary bytes recovered (no hex string substitution)
SELECT id, HEX_ENCODE(raw_bytes) FROM PG ORDER BY id;
-- expected: 00FF012345, DEADBEEF, CAFE, 00
```

## Autoload fidelity table

For reference — what Snowflake's `MATCH_BY_COLUMN_NAME` does *out of the box*
versus what we need:

| Parquet column                              | Snowflake autoload          | What we want   | Recovered via                  |
| ------------------------------------------- | --------------------------- | -------------- | ------------------------------ |
| `BYTE_ARRAY` (no logical type)              | VARCHAR (fails on non-UTF8) | BINARY         | `BINARY_AS_TEXT = FALSE`       |
| `BYTE_ARRAY` + `LogicalType::String`        | VARCHAR                     | VARCHAR        | (unchanged)                    |
| `BYTE_ARRAY` + `LogicalType::Json`          | VARCHAR                     | VARIANT        | `PARSE_JSON(col)` in final     |
| `FixedSizeBinary(16)` + `LogicalType::Uuid` | BINARY                      | canonical UUID | `HEX_ENCODE` + regex in final  |
| `Time64(MICROSECOND)`                       | (fails to bind to TIME)     | TIME(6)        | stage as NUMBER + `TIME_FROM_PARTS` |
| `Timestamp(MICROSECOND, no-tz)`             | (fails to bind to NTZ)      | TIMESTAMP_NTZ  | stage as NUMBER + `TO_TIMESTAMP_NTZ` |
| `Timestamp(MICROSECOND, UTC)`               | TIMESTAMP_TZ shifted        | TIMESTAMP_TZ   | `ALTER SESSION SET TIMEZONE = 'UTC'` |
| `UINT64` > 2^63-1                           | overflow error              | NUMBER(20,0)   | rivet `c_bigint_u: decimal(20,0)` override |
| `List<X>`                                   | ARRAY                       | ARRAY          | (unchanged)                    |

BigQuery's `bq load` (with `--parquet_enable_list_inference`) handles items 4–7
natively. Snowflake autoload behavior may improve in future releases; treat
this recipe as a snapshot.
