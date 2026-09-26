# Oracle Database (source)

> **Status: Preview — batch only.** Live-tested against Oracle AI Database 26ai Free
> (release 23.26.3, `gvenzl/oracle-free:23-slim-faststart`, the stand's `oracle` compose service).
> `mode: cdc` is refused at config load. The driver is Oracle's pure-Rust thin
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
could not advance); as an incremental cursor it re-exports the rows sharing the
last microsecond on each run (duplicates, never loss).

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

- No CDC yet (LogMiner is the planned mechanism).
- A table of exactly 1000 columns that has LOBs: the server-side empty-value flags
  would exceed Oracle's 1000-column select list, so zero-length LOBs read as NULL,
  with a warning.
- Each chunk or page reads its own statement-level snapshot; there is no
  cross-chunk consistency (`chunk_dense` is the most exposed — prefer `chunk_by_key`).
