# Research: Oracle Database as a rivet source (batch + CDC)

Status: research, 2026-09-25, on `main` d8854e1a. Nothing is implemented.
Tags: **[V]** verified against the cited primary source; **[I]** inference;
**[U]** unverified — needs a live measurement before it goes into an ADR.

## 1. Decisions this research proposes

| # | Decision | Why |
|---|---|---|
| D1 | Driver: Oracle's pure-Rust thin driver `oracledb`, pinned to an exact version, behind a cargo feature `oracle` | No Instant Client; sync like rivet's `Source`; same arrow 59 major; rustls 0.23 already in the lockfile |
| D2 | rivet builds Arrow itself from driver values; never uses the driver's `query_arrow` | Its mapping is lossy (see §3) |
| D3 | Bare `NUMBER` needs a declared product choice (see open question Q1) — never a silent `Float64` | No lossless fixed-scale `Decimal128` exists for it |
| D4 | Pin session state on every connection: `TIME_ZONE='+00:00'`, NLS date/timestamp/tz formats, `NLS_NUMERIC_CHARACTERS='.,'`, `NLS_CALENDAR='GREGORIAN'`; cursors as typed binds | Driver defaults the session zone to the host's local zone [V] |
| D5 | CDC mechanism: LogMiner over SQL polling, **without** `COMMITTED_DATA_ONLY`, values via `MINE_VALUE` + `COLUMN_PRESENT`, online-catalog dictionary, refuse on mid-window DDL | Only redo-based path with no extra license; bounded memory; no SQL-text parser |
| D6 | CDC anchor model: **client-side** SCN coordinates `{restart_scn, commit_scn}` + server identity; retention is reader-independent (no pin) | No server object remembers a LogMiner reader |
| D7 | Engine identifier in code/tests must not be `oracle` alone | "oracle" already means *independent test oracle* across the repo |

## 2. Driver and distribution

| crate | latest | client libs | notes |
|---|---|---|---|
| `oracledb` | 26.0.0-beta.4 (2026-09-23) [V] | none, pure Rust [V] | Oracle-maintained (beta published by the python-oracledb maintainer) [V]; UPL-1.0 OR Apache-2.0 [V]; optional `arrow-array`/`arrow-schema` ^59.1 [V]; `rustls` ^0.23 [V]; TLS/mTLS yes, NNE/Kerberos/SEPS wallet **no**; tested 19c/21c/26ai |
| `oracle` (ODPI-C) | 0.6.3 (2025-01-02) [V] | Instant Client at runtime | covers 11.2+; no release in ~20 months |
| `sibyl` | 0.7.1 | OCI at build **and** runtime | — |

- The crate name `oracledb` changed hands: 0.9.1 (MIT) was published by a different
  author; Oracle's line starts at 26.0.0-beta.1 (2026-08-06) [V]. Pin exact versions.
- Instant Client's license allows free redistribution only with an executed end-user
  agreement and distribution records [V] — incompatible with a public Docker image or
  Homebrew bottle [I]. D1 avoids it. An ODPI-C fallback, if ever needed (NNE/Kerberos
  sites, DB < 19c), must be user-supplied, never bundled.
- Sources: crates.io API (`/api/v1/crates/oracledb`, `/oracle`, `/sibyl`);
  https://docs.rs/oracledb ; https://www.oracle.com/downloads/licenses/instant-client-lic.html

## 3. Batch: types, session, chunking

**Types** (https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html):
- `NUMBER(p,s)`: p 1–38 → `Decimal128(p,s)`; `p≤18,s=0` may be an int. Bare `NUMBER` is
  floating with a 10⁻¹³⁰…10¹²⁶ range [V] → no lossless fixed-scale mapping [I].
- `DATE` carries time to the second, range 4712 BC–9999 AD [V]; BC does not fit
  BigQuery [U].
- `TIMESTAMP(9)` is nanosecond [V]; `WITH TIME ZONE` stores region or offset [V];
  `WITH LOCAL TIME ZONE` renders in the session zone [V].
- Zero-length `VARCHAR2` **is NULL** [V] — rivet can never deliver `''` from Oracle;
  the independent oracle's null profile must expect it.
- 23ai/26ai: `BOOLEAN`, `JSON` (21c+), `VECTOR` [V].
- The driver's Arrow path: bare NUMBER → Float64, all datetimes →
  `Timestamp(µs, None)` (zone dropped, ns truncated), CLOB/INTERVAL/JSON/VECTOR/ROWID
  → error [V from the crate's `doc/arrow.md`].

**Chunking / consistency**
- Keyset on PK; `OFFSET … FETCH` for `nth_row` [V].
- `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID` **writes and commits** [V] → not for
  a read-only exporter. ROWID ranges from `DBA_EXTENTS` need dictionary grants.
- Consistent parallel snapshot: `AS OF SCN` needs the FLASHBACK privilege and is
  bounded by undo (`UNDO_RETENTION` default 900 s, ORA-01555) [V]. Default to a
  single-session `SET TRANSACTION READ ONLY`; SCN snapshots opt-in [I].
- `ALL_TABLES.NUM_ROWS` exists only after `DBMS_STATS` [V] → may be NULL; the
  sparse-chunk warning must use the count-only regime.
- Privileges: SELECT/READ per table; `ALL_*` views need nothing extra [V].

## 4. CDC

**Mechanisms** (licensing: https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html):
- LogMiner — "available for use with all Oracle AI Database offerings", Y in every
  edition column including Free [V]. **But** with GoldenGate supplemental logging on,
  a GoldenGate license is required "regardless of which APIs are used … LogMiner,
  XStream or 3rd party tools" [V] → preflight must refuse
  `ENABLE_GOLDENGATE_REPLICATION=TRUE`.
- XStream — requires a GoldenGate license and EE [V]. Excluded.
- `CONTINUOUS_MINE` desupported in 19c [V] → rivet registers log files itself
  (`V$ARCHIVED_LOG`, `V$LOG`) or uses per-PDB auto-registration (19c RU10+/21c+).
- `ORA_ROWSCN` polling cannot see deletes [I]; triggers are invasive. Neither is CDC
  in rivet's sense.

**LogMiner facts that shape the design**
(https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html,
https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-LOGMNR_CONTENTS.html)
- `COMMITTED_DATA_ONLY` stages each transaction in memory and can fail "Out of
  Memory" [V] → rivet frames transactions itself by XID (`START`/`COMMIT`/`ROLLBACK`
  rows), spilling through `cdc/spill.rs`, `committed` only on the last event.
- Several events can share one SCN [V]; `(RS_ID, SSN)` identifies a logical change [V].
- Values: `SQL_REDO` text or `MINE_VALUE` (VARCHAR2) + `COLUMN_PRESENT` (NULL vs
  absent) [V]; neither covers LONG/LOB/ADT [V]. No typed binary value API.
- **Silent whole-table exclusion**: a table with identity columns, BFILE, nested
  tables, temporal validity, … "is ignored by LogMiner" [V]; table/column names must
  not exceed 30 characters [V]. → preflight refusal per captured table.
- LOB/LONG/ADT cannot be supplementally logged [V] (unchanged-LOB class, like PG TOAST).
- A missing log does **not** stop mining unless an option needs it [V] → rivet must
  refuse gaps itself: restart SCN below the oldest contiguous available log,
  `V$LOGMNR_LOGS.STATUS=4`, or any `MISSING_SCN` row.
- RAC: all threads' logs must be added or results are partial, silently [V].
- Mining cost is proportional to total redo, not captured rows [V] → the
  uncaptured-traffic starvation class applies.

**Anchor + identity** (https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-DATABASE.html)
- Checkpoint = `{restart_scn, commit_scn}` [I]: resume mines from the oldest start SCN
  of transactions open at the last emitted commit, drops commits below `commit_scn`.
  Checkpointing one SCN loses the early DML of a transaction that straddles it [I].
- Identity = `DBID`, `DB_UNIQUE_NAME`, `RESETLOGS_CHANGE#`/`RESETLOGS_TIME` (a new
  incarnation after `OPEN RESETLOGS`), PDB `CON_UID` [V fields; I choice]. Refuse a
  mismatch; warn (not refuse) on a checkpoint without identity.
- No retention pin for a LogMiner client (RMAN keeps logs only for Data Guard or a
  licensed capture process) [V] → `ack` is a no-op; the gap refusal is load-bearing.
- Idle first run: persist the anchor at open (the MySQL model).
- Bounded drain: open-time `CURRENT_SCN`; end at the first COMMIT past it; with async
  commit, terminate only once mined-through SCN ≥ bound [I/U]. Expected load-bearing
  (LogMiner re-reads like PG's peek) — must be proven by disabling it.
- Initial snapshot: `AS OF SCN S`, stream anchored at `min(V$TRANSACTION.START_SCN)`
  active at S, not S [I].

## 5. Stand and independent oracle

- Image `container-registry.oracle.com/database/free` — amd64 **and** arm64 [V,
  registry manifest]; 2 cores / 2 GB RAM / 12 GB data caps [V]; free for development
  and testing [V]. Full image, not `-lite`, until LogMiner in lite is confirmed [U].
- ARCHIVELOG needs `SHUTDOWN` → `STARTUP MOUNT` inside the container [U] — likely a
  setup script baked into the stand.
- Not coverable on Free: RAC multi-thread, Data Guard.
- Independent reader: DuckDB community extension `oracle_scanner` (own TNS/TTC
  implementation in C++, no Oracle client) [V from its README; young]; second choice
  python-oracledb **thick** mode (a decode path not shared with the thin drivers) [I].

## 6. Integration cost in rivet (verified sites)

- Two `_ =>` arms would route Oracle down the Mongo path with no compile error:
  `src/init/mod.rs:525-533` (`source_type_of`) and `src/source/cdc/mod.rs:1154-1164`
  (`ensure_anchor`). Both need explicit arms.
- 43 production lines build `… AS _rivet…` derived tables; Oracle rejects `AS` before
  a table alias and unquoted identifiers starting with `_` → one dialect helper for
  derived-table aliases, then mechanical.
- Oracle folds unquoted identifiers to UPPERCASE → `quote_ident` and init's
  case-fold rule are design work.
- `src/source/cdc/validate.rs::PosKey` has no numeric SCN variant — a healthy Oracle
  output would read as "unparseable `__pos`".
- `tests/offline/chunking_matrix_guard.rs:809` hard-asserts 4 source engines; 18
  `docs/*-matrix.yaml` ledgers carry `engines: [postgres, mysql, mssql, mongo]`; the
  CDC conformance gate has 25 rows × 4 engines.
- No engine is behind a cargo feature today; Oracle would be the first.

Totals (from the walk): ~60 production match/dialect sites + ~25 alias wrappers, 1
`Source` impl, ~20 CDC arms + an adapter, ~45 test-infra sites, 18 ledgers,
~8 packaging, ~12 docs.

## 7. Phased plan

1. **Batch v1** — full/chunked/keyset/incremental on 19c+ via `oracledb`; own Arrow
   builder; session pin; dialect helper; preflight refusals; `rivet init`; stand +
   DuckDB `oracle_scanner` oracle; type-fidelity matrix incl. a flipped-NLS/TZ test.
2. **CDC v1** — LogMiner as in D5/D6 with the contract tests below; single instance,
   non-RAC; LOB/LONG columns refused at preflight.
3. Later — RAC threads, per-PDB mining choice, LOB capture, ODPI-C fallback.

**CDC contract tests needing an Oracle variant**: large-transaction atomicity across a
mid-flush crash; two-run resume + idle first run + flush/ack fault hooks; a
transaction straddling a checkpoint (proves `restart_scn ≠ commit_scn`); foreign
DBID and same DBID after `OPEN RESETLOGS` refused; gap refusal (deleted archive log,
`MISSING_SCN`); open-bound two-run no-loss + disabled-bound termination; uncaptured
large transaction ahead of the backlog; flipped NLS/TZ session; per-column null
profile vs source; preflight refusals (identity column, >30-char name, unsupported
type, `ENABLE_GOLDENGATE_REPLICATION`, NOARCHIVELOG, no supplemental logging);
mid-window DDL refused; partial and full rollback; PK-update representation.

## 7a. Live spike results (2026-09-25)

Spike: `dev/spikes/oracle-driver` (standalone crate, not part of rivet) against
`gvenzl/oracle-free:23-slim-faststart` (Oracle 23ai Free, arm64). It reads a 21-column
table through `oracledb =26.0.0-beta.4`, builds Arrow itself, writes Parquet, and is
compared against (a) Oracle's own server rendering (`TO_CHAR`, `SYS_EXTRACT_UTC`,
`RAWTOHEX`, `DBMS_LOB.GETLENGTH`, `JSON_SERIALIZE`) and (b) DuckDB `oracle_scanner`
0.2.2, a reader with its own protocol implementation.

**Exact through our own Arrow builder**: bare `NUMBER` (1E125, 1E-130, 29 significant
digits, via `OracleNumber` text → Utf8), `NUMBER(38,10)`, `NUMBER(18)`, NaN/Inf,
`BOOLEAN`, `DATE` incl. year −4712 and 0001, `TIMESTAMP(9)` (ns), `TSTZ` as a UTC
instant, `TSLTZ`, unicode/emoji, `CHAR` padding, 100 KB `CLOB`, `RAW`, `''` → NULL.
oracle_scanner agreed on 66 of 70 cells; the 4 differences are representation
(`'1'` vs true), its own TSTZ text (UTC fields with the source offset appended — the
same misleading form the driver's `Display` prints; server `SYS_EXTRACT_UTC` confirms
our Parquet), and one real driver defect (below).

**Driver defects found (oracledb 26.0.0-beta.4)**
1. `TIMESTAMP WITH TIME ZONE` holding a **region name** (`Europe/Berlin`) **panics**
   (`todo!()` in `ora_type/timestamp.rs:236`). rivet must `catch_unwind` or fetch such
   columns as `SYS_EXTRACT_UTC(col)` / `TO_CHAR(...)`; report upstream.
2. `EMPTY_BLOB()` (length 0) arrives as **NULL**; the independent reader returns `b''`.
   Empty ≠ NULL is lost for BLOBs.
3. `oracledb::Error` implements only `Debug` (no `Display`, no `std::error::Error`) —
   an adapter is needed for anyhow.
4. `OracleTimestamp`'s `Display` of a TSTZ prints UTC fields with the original offset
   (a different instant); negative `INTERVAL DAY TO SECOND` `Display` is garbled
   (`P-1DT0H0M0.-00001000S`). Build from fields, never from `Display`.
5. CLOB/BLOB are described as `DB_TYPE_LONG` / `DB_TYPE_LONG_RAW` in fetch metadata;
   `get::<String>` refuses NUMBER; `JsonValue` has no text serializer (use
   `JSON_SERIALIZE` server-side).
6. The crate pulls `aws-lc-sys` (a C/cmake build) as rustls' crypto provider — check
   against rivet's cross builds.

**rivet-side mapping findings**: `TIMESTAMP(9)` in ns overflows i64 past 2262 (the
row with 9999-12-31 had to be nulled) → µs (truncating ns) or a declared overflow
policy; Parquet has no `Timestamp(Second)` (DuckDB reads it as BIGINT) → map `DATE`
to µs.

**LogMiner through the thin driver**: works — `DBMS_LOGMNR.ADD_LOGFILE` /
`START_LOGMNR` PL/SQL calls and `V$LOGMNR_CONTENTS` + `MINE_VALUE` over the online
logs, from the CDB root. One transaction's inserts share one XID. Per-type isolation
on 23ai Free:

| column type | LogMiner |
|---|---|
| VARCHAR2 / NUMBER | INSERT/UPDATE decoded |
| native `JSON` (23ai) | **UNSUPPORTED** |
| `BOOLEAN` (23ai) | **UNSUPPORTED** |
| CLOB | INSERT with `EMPTY_CLOB()` + a separate UPDATE carrying the value |
| TSTZ / DATE in `SQL_REDO` | rendered by session NLS: `'29-FEB-24 10.00.00.000000 AM +02:00'` (two-digit year) |
| UPDATE without PK supplemental logging | only the changed column + ROWID |

Consequences: preflight must refuse CDC for tables with native JSON / BOOLEAN; LOB
writes must be framed within their transaction; `MINE_VALUE` text (e.g. `1.0…E+125`)
needs a parser and pinned NLS; PK supplemental logging is a prerequisite. Tables
dropped and recreated appear as `UNKNOWN.OBJ#` with the online-catalog dictionary.

Not yet exercised: ARCHIVELOG + archived-log registration, resume across a log
switch, gap detection, RAC — they need the full image (`container-registry.oracle.com`
is ~0.6 MB/s from here; its pull is paused).

## 8. Open questions for the owner

- **Q1** Bare `NUMBER`: string, `Decimal256` with a declared scale, or refuse without a
  `columns:` override?
- **Q2** Minimum Oracle version: 19c+ (thin driver's tested floor) acceptable?
- **Q3** Ship on a beta driver behind a feature flag, or wait for `oracledb` 26.0.0?
- **Q4** CDC mining scope: CDB root with a `C##` common user, or per-PDB (19c RU10+)?
- **Q5** Stand weight: an Oracle container (2 GB RAM cap) in the per-PR E2E job, or
  nightly only?
