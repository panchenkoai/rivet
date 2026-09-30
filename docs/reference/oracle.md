# Oracle Database (source)

> **Status: Preview.** Live-tested against Oracle AI Database 26ai Free
> (release 23.26.3, `gvenzl/oracle-free:23-slim-faststart`, the stand's `oracle` compose service).
> `mode: cdc` reads the redo logs through LogMiner (preview; see the Oracle section of
> [cdc.md](cdc.md) for the prerequisites). The driver is Oracle's pure-Rust thin
> driver `oracledb 26.0.0-beta.4`; no Oracle client install is needed.

## Connecting

```yaml
source:
  type: oracle
  url_env: ORACLE_URL   # oracle://user:password@host:1521/SERVICE
```

- The URL path is the **service name** (Oracle Free's pluggable database is
  `FREEPDB1`), not a SID. Port defaults to 1521. Percent-encode `@`, `:` and `/` in
  the password.
- `rivet init --source-env ORACLE_URL` scaffolds a config from the catalog of the
  connecting user's schema (`--schema OWNER` for another one).
- Unquoted names are upper-case in Oracle. `table: orders` reads `ORDERS`; a
  quoted mixed-case table (`"MixedCase"`) needs a `query:` — `rivet init` writes one.
  Strategy columns (`chunk_column`, `chunk_by_key`, `cursor_column`) match the
  catalog name exactly; `rivet check` names the real spelling when they do not.

### TLS

`tls.mode` other than `disable` connects with `tcps://`. The driver verifies the
server certificate against the **public CA bundle compiled into it**, not the
system trust store, for every enforced mode. `tls.ca_file` is refused: a server
certificate issued by a private CA cannot be verified yet.

### Privileges

| Grant | Needed for |
|---|---|
| `CREATE SESSION` + `SELECT` on the exported tables | every export |
| `SELECT_CATALOG_ROLE` (or `SELECT` on `V_$SYSSTAT`, `V_$SYSTEM_EVENT`) | source-harm metrics and governor pressure; without it they are absent and `rivet doctor` says so |

Row estimates come from `ALL_TABLES.NUM_ROWS` and need no extra grant.

## Session state

rivet pins its own session so values never depend on database or client
defaults: `TIME_ZONE = '+00:00'`, `NLS_CALENDAR = GREGORIAN`,
`NLS_NUMERIC_CHARACTERS = '.,'`, ISO `NLS_DATE_FORMAT` / `NLS_TIMESTAMP_FORMAT` /
`NLS_TIMESTAMP_TZ_FORMAT`, and `NLS_SORT = NLS_COMP = BINARY` (so a keyset or
cursor seek compares keys the way `ORDER BY` sorts them, even under a logon
trigger that makes the session linguistic).

## Modes

| Mode | Oracle notes |
|---|---|
| `full` | any table, view or `query:` |
| `incremental` | cursor bound as text through the pinned masks |
| `chunked` range | `chunk_column` must be an integer `NUMBER(p ≤ 18, 0)` |
| `chunked` keyset (`chunk_by_key`) | single-column unique NOT NULL key: integer `NUMBER` of any precision, bare `NUMBER`, `VARCHAR2`/`CHAR`, `DATE`, `TIMESTAMP(0..6)`; also `parallel > 1` |
| `time_window`, `partition_by` | ANSI `TIMESTAMP '…'` / `DATE '…'` bounds |

`TIMESTAMP(7..9)` is not a keyset key (rivet reads it at microseconds, so a page
could not advance), and it is refused as an incremental cursor
(`RIVET_SOURCE_CURSOR_FINER_THAN_MICROSECOND`): the saved cursor would fall below
its own row and every run would export that row again. Read it as
`CAST(col AS TIMESTAMP(6))` in a `query:`, or pick a cursor with at most 6
fractional digits. `rivet init` never scaffolds one as a cursor or a keyset key,
nor a `BINARY_FLOAT`/`BINARY_DOUBLE` or zoned `TIMESTAMP` keyset key.

`columns:` override keys match the result's column names exactly; a key that
matches one only when case is ignored (`created_at` for `CREATED_AT`) is refused
(`RIVET_CONFIG_COLUMN_OVERRIDE_CASE`) by `rivet check` and by the run, rather than
silently skipped.

### Column overrides

A `columns:` override reads the Oracle column into the declared type:

| Override | Oracle columns it reads |
|---|---|
| `int2`, `int4`, `int8`, `decimal(p ≤ 38, s)` | `NUMBER` |
| `float4` | `NUMBER`, `BINARY_FLOAT` |
| `float8` | `NUMBER`, `BINARY_FLOAT` (widened exactly), `BINARY_DOUBLE` |
| `bool` | `BOOLEAN` |
| `date` | `DATE`, `TIMESTAMP` holding midnight; a value with a time of day fails the run with the column named |
| `timestamp`, `timestamp_tz` | `DATE`, `TIMESTAMP` (zoned ones through their UTC instant) |
| `timestamp_ns`, `timestamp_tz_ns` | the same, keeping all nine fractional digits; a value outside 1677-09-21..2262-04-11 fails the run |
| `binary` | `RAW`, `LONG RAW`, `BLOB` |
| `uuid` | a 16-byte `RAW`/`BLOB`; any other length fails the run |
| `text` | any exported column: numbers as exact text, `DATE`/`TIMESTAMP` as `SYYYY-MM-DDTHH24:MI:SS.FF6` (`FF9` when a value has sub-microsecond digits), binary floats as shortest round-trip text, `BOOLEAN` as `true`/`false`, `RAW`/`BLOB` as upper-case hex (`RAWTOHEX`) |

Any other pairing (a number type on a character column, `decimal(p > 38, s)`)
fails the run with the column named; `rivet check` does not refuse it yet.

`tuning.statement_timeout_s` is enforced on the server: the driver's call timeout
stops the query at the budget.

## Types

| Oracle | Arrow / Parquet |
|---|---|
| `NUMBER(1..9, 0)` / `NUMBER(10..18, 0)` | Int32 / Int64 |
| `NUMBER(p, s)` otherwise | Decimal128(p, s); `s > p` widens to (s, s), negative `s` to (p − s, 0) |
| bare `NUMBER`, `FLOAT` | exact decimal text (Utf8) with a warning — declare `columns:` to load it as a number |
| `BINARY_FLOAT` / `BINARY_DOUBLE` | Float32 / Float64 (NaN, ±Inf kept) |
| `BOOLEAN` (23ai and later) | Boolean |
| `DATE`, `TIMESTAMP(0..6)` | Timestamp(µs) |
| `TIMESTAMP(7..9)` | Timestamp(µs), sub-microsecond digits truncated (reported Lossy) |
| `TIMESTAMP WITH [LOCAL] TIME ZONE` | Timestamp(µs, UTC) — converted on the server (`SYS_EXTRACT_UTC`) |
| `INTERVAL YEAR TO MONTH` / `DAY TO SECOND` | ISO 8601 duration text |
| `VARCHAR2`, `NVARCHAR2`, `CHAR`, `NCHAR`, `CLOB`, `NCLOB`, `LONG` | Utf8 (a zero-length LOB stays `''`, not NULL) |
| `RAW`, `LONG RAW`, `BLOB` | Binary |
| `JSON`, `XMLTYPE`, `VECTOR`, `ROWID` | text, serialized on the server |

Refused with the column named (live-tested on a VARRAY): user-defined object
types, collections and `ANYDATA` — select their attributes in a `query:`. An
unaliased `ROWID` in a `query:` is refused too — alias it.

BC dates keep their calendar fields (Oracle `-0001-06-15` is `0001-06-15 BC` in
Parquet); dates before 1582-10-15 are not converted from Oracle's Julian calendar.

## Known limits

- The driver is a **beta**: `oracledb =26.0.0-beta.4`, pinned exactly, behind the
  `oracle` cargo feature, which is **on by default**.
- TLS verifies the server against the public CA bundle compiled into the driver
  (`webpki-roots`) only: no system trust store, no `tls.ca_file` (refused), no
  Oracle wallet.
- `TIMESTAMP(7..9)` values are truncated to microseconds (reported Lossy, with a
  warning); such a column is refused as an incremental cursor.
- CDC (LogMiner) is a preview and runs only as a bounded drain to files, to the SCN
  current at open; continuous CDC (`until_current: false`, `rivet cdc --stream`) is refused at config load
  (`RIVET_CONFIG_CDC_CONTINUOUS_UNSUPPORTED`), and an Oracle CDC export cannot feed a
  `load:` block. A `TRUNCATE` (table, partition or subpartition) of a captured table
  is refused after the changes before it are delivered and checkpointed, and every
  re-run stops there until you re-anchor and re-snapshot. It captures NUMBER, FLOAT, BINARY_FLOAT/DOUBLE, DATE, TIMESTAMP
  (every zone form), VARCHAR2/NVARCHAR2/CHAR/NCHAR and RAW columns and refuses a table
  with any other type by name.
- Graded only against Oracle AI Database 23ai/26ai Free. 21c XE (amd64-only) does
  not start under the arm64 stand's emulation (ORA-00442), and 19c has no freely
  pullable image, so both are untested.
- A non-Unicode database character set is live-tested on one: a WE8ISO8859P1 PDB
  (national character set AL16UTF16). Latin-1 text in `VARCHAR2`/`CHAR`/`CLOB` and
  non-Latin text (Cyrillic, CJK, a supplementary character) in
  `NVARCHAR2`/`NCHAR`/`NCLOB` arrive byte-exact. Other single-byte and multi-byte
  character sets are untested.
- A table of exactly 1000 columns that has LOBs: the server-side empty-value flags
  would exceed Oracle's 1000-column select list, so zero-length LOBs read as NULL,
  with a warning.
- Each chunk or page reads its own statement-level snapshot; there is no
  cross-chunk consistency.
