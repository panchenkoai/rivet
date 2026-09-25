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

## 8. Open questions for the owner

- **Q1** Bare `NUMBER`: string, `Decimal256` with a declared scale, or refuse without a
  `columns:` override?
- **Q2** Minimum Oracle version: 19c+ (thin driver's tested floor) acceptable?
- **Q3** Ship on a beta driver behind a feature flag, or wait for `oracledb` 26.0.0?
- **Q4** CDC mining scope: CDB root with a `C##` common user, or per-PDB (19c RU10+)?
- **Q5** Stand weight: an Oracle container (2 GB RAM cap) in the per-PR E2E job, or
  nightly only?
