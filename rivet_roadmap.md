# Rivet Consolidated Pain-Driven Roadmap

This document is the **single source of truth** for Rivet planning. It consolidates:

- the current product direction and validated user pains
- the **numbered** strategic epics (§5, P0–P3)
- **execution status**: lettered epics (A–O, M), phase completion, task ✅/⏳, and Definition of Done (§9)
- packaging, security, trust, release, and external-adoption work that used to live in a separate roadmap

The purpose is to keep Rivet focused on recurring problems in database-first extraction while tracking what is shipped vs open.

---

# 0. Why Rivet exists

Most data teams that extract from PostgreSQL or MySQL end up writing a script. It starts simple — a `COPY` or `SELECT *` piped to a file — and then it grows:

- someone adds retry logic after a production incident
- someone adds batching because the table hit 50M rows
- someone adds a cursor column so the script does not re-read everything
- someone adds a memory guard after OOM on a wide table
- someone adds schema tracking after a column rename broke the warehouse

Within a year the script is 2,000 lines, understood by one person, and quietly critical to the business.

**Rivet replaces that script.**

It is a single binary that extracts data from relational databases into Parquet or CSV files — locally, to S3, or to GCS — with the safety mechanisms that production workloads eventually demand: preflight checks, tuning profiles, retry with error classification, incremental cursors, chunked parallelism, validation, reconciliation, and a crash-recovery model.

Rivet is deliberately **not** a CDC tool, not an ELT platform, and not a SaaS connector marketplace. It solves one problem well: **getting data out of a fragile database safely, predictably, and repeatedly.**

---

# 1. Product focus

Rivet should remain focused on:

**a source-aware, self-hosted extraction engine for fragile database-first systems**

Rivet is for teams that need to extract data from operational databases into analytics-friendly files/storage **safely, predictably, and with clear operational visibility**.

## Rivet is not currently optimizing to become:
- a Kafka replacement
- a CDC platform first
- an all-in-one ELT platform
- a SaaS connector marketplace
- a universal warehouse merge/load orchestrator
- a distributed execution platform

---

# 2. Validated pain themes

## Pain A — Fragile source databases
Repeated user pain:
- bad extraction queries can hurt production or replicas
- missing indexes make extraction dangerous
- parallelism can overload weak sources
- DB connection limits are often lower than requested parallelism
- billion-row tables require more careful strategies than generic JDBC partitioning

## Pain B — Weak incremental foundations
Repeated user pain:
- clean delta keys are often missing
- teams re-read large windows to avoid data loss
- late-arriving data forces replay/overlap strategies
- date-based slicing is often needed, but tools assume timestamp/unix-style partitioning
- sparse keys and weak partition columns make chunking unreliable or expensive

## Pain C — Poor visibility into change
Repeated user pain:
- schema drift is noticed too late
- widening text/string/json payloads break downstream assumptions without schema changes
- uniqueness/cardinality assumptions change and break downstream merge logic
- users want stronger visibility into what changed and when

## Pain D — Operational unpredictability
Repeated user pain:
- users do not know if extraction is safe until they run it
- users do not know if source-side migration/index changes are required
- users do not know what happened after interrupted runs
- users do not know what was exported, retried, or partially published

## Pain E — Self-built pipelines become hard to maintain
Repeated user pain:
- custom scripts grow into fragile systems
- behavior is understood by only one or two people
- guarantees are unclear
- retries, memory pressure, and late-data logic become messy over time

## Pain F — Connectivity and deployment friction
Repeated user pain:
- SSH / jump host / bastion access is common
- driver/URL handling is brittle and frustrating
- local SQLite state is not enough for stateless containerized deployments
- install/tryout friction matters for adoption

## Pain G — Event-driven alternatives exist, but are not the default reality
Observed market boundary:
- Kafka/CDC ecosystems solve a different class of problems
- many teams still live in a database-first world
- snapshot, replay, and backfill remain critical even where streaming exists

---

# 3. Roadmap principles

1. **Trust first, expansion second**  
   Before adding big new surface area, Rivet should become more trustworthy and explainable.

2. **Safety before throughput**  
   Parallelism and performance matter, but not before source safety and predictable behavior.

3. **One problem layer at a time**  
   Extraction trust layer first, then extraction intelligence, then adoption polish, then expansion.

4. **Solve recurring pains, not every theoretical use case**  
   New work should map back to validated user pain.

---

# 4. Priority tiers

Priority legend:
- **P0** = immediate / next milestone
- **P1** = strong next layer after P0
- **P2** = adoption/polish after core trust is solid
- **P3** = later expansion after the product is trusted

---

# 5. Consolidated roadmap

## P0 — Trust, Safety, and Explainability

These are the most validated and strategically important problems.

---

## Epic 1 — Preflight Planner & Source Safety
**Priority:** P0  
**Status:** ✅ DONE — lettered epic B; `rivet check` with strategy output, verdicts, sparse/dense warnings, connection limit warnings, profile recommendations  
**Pain coverage:** Pain A, Pain B, Pain D

### Goal
Turn Rivet into a tool that helps users understand whether extraction is safe before they run it.

### Deliverables
- stronger `rivet check`
- selected extraction strategy output
- tuning profile recommendation
- sparse range warnings
- connection pressure / parallelism warnings
- migration/index-required guidance
- degraded vs unsafe distinction
- initial recommended parallelism output

### Why this matters
Users repeatedly describe not wanting to find out too late that:
- a table needs an index
- a query will seq scan
- a chosen parallelism level is unrealistic
- source limits make the chosen settings invalid

---

## Epic 2 — Auditability, Manifest & Reconciliation
**Priority:** P0  
**Status:** ✅ DONE — lettered epics D, F; run summary, file manifest, `rivet metrics`, `rivet state files`, `--reconcile`  
**Pain coverage:** Pain C, Pain D, Pain E

### Goal
Make every export inspectable, attributable, and trustworthy.

### Deliverables
- run summary
- file manifest
- per-file row counts
- rows read / written / validated accounting
- reconciliation summary
- optional source-vs-output count checks for bounded modes

### Why this matters
After extract-to-bucket works, users immediately ask:
- what exactly was exported?
- which files belong to which run?
- how many rows were written?
- can I trust this result?

---

## Epic 3 — Recovery & Interrupted Run Semantics
**Priority:** P0  
**Status:** ✅ DONE — lettered epics C, H; crash matrix, lifecycle docs, E2E recovery paths, `tests/recovery.rs` (10 tests across 5 failure boundaries)  
**Pain coverage:** Pain D, Pain E

### Goal
Make restarts, partial runs, retries, and reruns predictable.

### Deliverables
- crash matrix
- chunk/file/run status model
- rerun semantics documentation
- failure injection tests
- recovery integration tests
- duplicate semantics documentation

### Why this matters
Without explicit recovery semantics, any interrupted run becomes:
- a debugging session
- a manual cleanup problem
- a trust issue

---

## Epic 4 — Durable State Backend
**Priority:** P0  
**Status:** ✅ DONE — `StateConn` enum supports SQLite and PostgreSQL backends; activated via `RIVET_STATE_URL=postgresql://...`; auto-migration on connect; `StateRef` for parallel chunk workers; documented in `docs/reference/cli.md`; `docker-compose.yaml` includes dedicated `postgres-state` service  
**Pain coverage:** Pain D, Pain F

### Goal
Support both local/dev workflows and durable production/container deployments.

### Deliverables
- state backend abstraction
- SQLite backend for local/dev
- PostgreSQL backend for durable/prod/k8s use
- init/migration command
- docs for deployment modes
- clear guidance: SQLite for local/dev, external backend for durable deployment

### Why this matters
SQLite is good for local and single-node workflows, but not enough for stateless workers in durable environments.

---

## P1 — Better Extraction Control and Change Visibility

These strengthen Rivet's core differentiation once the trust layer is solid.

---

## Epic 5 — Real Batch / Fetch / Write Control
**Priority:** P1  
**Status:** ✅ DONE — lettered epic M; `batch_size`, `batch_size_memory_mb`, `max_file_size`, `throttle_ms`, streaming uploads, per-export tuning overrides  
**Pain coverage:** Pain A, Pain B, Pain E

### Goal
Give users real control over how extraction interacts with:
- source load
- memory
- file generation
- runtime behavior

### Deliverables
- separate controls for:
  - logical chunk size
  - DB fetch size
  - writer flush size
  - parquet row group size
  - file rotation threshold
- docs explaining each
- planner hints for dangerous combinations

### Why this matters
Users repeatedly complain that existing tooling exposes “batch size” in unclear or misleading ways.

---

## Epic 6 — Date / Timestamp / Range Partition Intelligence
**Priority:** P1  
**Status:** ✅ DONE — `chunked` mode with `chunk_by_days`, date-native `>= / <` semantics, sparse range warnings, planner awareness  
**Pain coverage:** Pain A, Pain B, Pain D

### Goal
Improve support for large-table extraction by making partitioning more explicit and more realistic.

### Deliverables
- first-class support for date-based slicing
- timestamp-based slicing
- numeric range slicing
- explicit boundary semantics
- planner awareness of date-vs-timestamp tradeoffs
- docs/examples for large-table partition strategies

### Why this matters
Users repeatedly hit partitioning pain on 100M+ tables and often need date-based rather than unix-timestamp semantics.

---

## Epic 7 — Schema Drift Visibility & Policy
**Priority:** P1  
**Status:** ✅ DONE — column add/remove/type-change tracking in run summary + `on_schema_drift: warn|continue|fail` policy hook in YAML config  
**Pain coverage:** Pain C, Pain E

### Goal
Make structural source changes visible early and operationally understandable.

### Deliverables
- stronger schema drift warnings
- run summary schema flags
- documented behavior for add/remove/type-change
- future policy hooks: warn / fail / continue

### Why this matters
Users often discover schema drift only after downstream damage is already done.

---

## Epic 8 — Data Shape Drift Detection
**Priority:** P1  
**Status:** ✅ DONE — per-column max byte length tracked across runs; `shape_drift_warn_factor` YAML config; warns when growth exceeds threshold  
**Pain coverage:** Pain C

### Goal
Detect meaningful data-shape changes even when schema itself has not changed.

### Deliverables
- optional observed max-length tracking for text/string/json columns
- compare current run vs previous runs
- width-growth warnings
- docs for structural schema drift vs shape drift

### Why this matters
Widening text/string payloads are a real downstream failure source and are not captured by structural schema checks alone.

---

## Epic 9 — Data Contract Checks
**Priority:** P1  
**Status:** ✅ DONE — `quality:` YAML block; row count bounds, null ratio thresholds, uniqueness assertions; chunked aggregate quality gate  
**Pain coverage:** Pain C, Pain E

### Goal
Expose optional checks for downstream assumptions such as uniqueness and cardinality.

### Deliverables
- optional uniqueness probe for declared keys
- duplicate warnings
- docs for cost and usage
- foundation for future row-count bounds / anomaly checks

### Why this matters
A lot of downstream breakage comes not from extraction failure, but from drifting source assumptions.

---

## P2 — UX, Packaging, and Adoption Improvements

These matter for adoption and polish, but should not outrank trust/safety work.

---

## Epic 10 — Config & Connection UX Improvements
**Priority:** P2  
**Pain coverage:** Pain F

### Goal
Reduce friction around configuration and connectivity.

### Deliverables
- query vs table auto-detection
- better structured connection config
- explicit driver override
- clearer connection validation and errors
- better docs/examples for connection parameters

### Why this matters
These are real annoyances, but they are secondary to safety and trust.

---

## Epic 11 — Installation & Packaging
**Priority:** P2  
**Status:** ✅ DONE for current distribution baseline — GitHub release binaries, Docker GHCR image, Homebrew tap, crates.io package  
**Pain coverage:** Pain F

### Goal
Make trying and adopting Rivet easier.

### Deliverables
- ✅ GitHub release binaries
- ✅ Homebrew tap support
- ✅ Docker image
- ✅ crates.io package (`rivet-cli`)
- ✅ improved local quickstart and pilot docs

### Why this matters
Install friction is real, but only worth optimizing after product direction is clear.

---

## Epic 12 — Deployment Modes Guidance
**Priority:** P2  
**Status:** ✅ Partial — pilot guide, production checklist, Docker/Compose docs, and state backend guidance exist; k8s/Helm remains future  
**Pain coverage:** Pain F

### Goal
Help users choose the right deployment model.

### Deliverables
- docs for local/dev vs durable/prod modes
- Docker/Compose examples
- ⏳ k8s/Helm guidance
- state backend recommendations

### Why this matters
Many reliability problems come from running the tool in a mode that does not match the environment.

---

## P3 — Later Expansion

These are valuable, but should come only after the trust layer is mature and pilot feedback confirms demand.

---

## Epic 13 — SSH / Jump Host Access
**Priority:** P3  
**Pain coverage:** Pain F

### Goal
Support or document constrained connectivity environments.

### Deliverables
- documented external tunnel workflow first
- possible later native SSH tunnel support

### Why this matters
Real pain, but infrastructure/security complexity is non-trivial.

---

## Epic 14 — Narrow Warehouse Load Layer
**Priority:** P3 → **P1 (in progress)**  
**Status:** ✅ Partial — type system, type report, strict mode, BigQuery compat layer, complex types (M1–M6 complete); **v0.7.8 Type Roundtrip Proof shipped** (Phase 1 of the type/verify/UX track — 4 independent reader validators + native Parquet UUID/JSON logical types + type-fidelity benchmark); load path (Parquet → BQ) = future  
**Pain coverage:** Pain C, Pain E

### Goal
Add a narrow, compatibility-aware path from extracted files into selected warehouse targets.

### Deliverables
- ✅ canonical type system (`src/types/`: `RivetType`, `TypeMapping`, `TypeFidelity`)
- ✅ target schema adapters (`ExportTarget::BigQuery`, Arrow→BQ mapping with NUMERIC/BIGNUMERIC/REPEATED/etc.)
- ✅ compatibility report (`rivet check --type-report --target bigquery`)
- ✅ strict/permissive mapping policy (`TypePolicy`, `--strict` flag)
- ✅ column type overrides (`columns:` YAML block)
- ✅ complex types: Enum, Interval, List (Postgres); Enum/SET, TIME, arrays (MySQL)
- ✅ **type roundtrip proof (v0.7.8)** — PG/MySQL → Parquet/CSV matrix tested through 4 independent readers (DuckDB, ClickHouse, pyarrow, BigQuery), 31 live tests in `tests/type_roundtrip/`, `make test-types-validators`. Documented in [`docs/type-mapping.md`](docs/type-mapping.md), [ADR-0014](docs/adr/0014-target-type-materialization.md), and per-tool benchmark reports under [`docs/bench/reports/REPORT_types*.md`](docs/bench/reports/)
- ✅ **native Parquet logical types (v0.7.8)** — UUID → `FixedSizeBinary(16) + LogicalType::Uuid` via `arrow.uuid` extension; JSON → `LogicalType::Json` via `arrow.json` extension. Downstream engines (DuckDB, ClickHouse 25.x+, pyarrow) recognise UUID/JSON natively without a cast; BigQuery autoload promotes UUID→BYTES exact (one documented gap: BQ does not lift `LogicalType::Json` to native `JSON` without an explicit `--schema=attrs:JSON`)
- ✅ **MySQL driver fidelity fixes (v0.7.8)** — PG arrays preserve NULL elements; MySQL `ENUM`/`SET` flag detection so `rivet.logical_type=enum` survives; `native_type` distinguishes UNSIGNED, `TINYINT(1)`, `BIT(1)`, CHAR vs VARCHAR, BINARY vs VARBINARY
- ⏳ direct load path (write Parquet files directly into BigQuery) = future

### Why this matters
Useful, but this opens a new product layer and should follow extraction trust.

---

## Epic 15 — WAL/Binlog CDC to Files
**Priority:** P3
**Pain coverage:** Pain B, Pain G
**Status:** ✅ **Shipped in 0.14.0**

### Goal
Extend Rivet from snapshot extraction into logical change extraction after the trust layer is mature.

### Deliverables
- ✅ logical change extraction — all three engines (MySQL binlog, PostgreSQL logical slot, SQL Server change tables)
- ✅ insert/update/delete event model (`__op`) with a `__pos` continuity marker
- ✅ durable LSN/binlog position state — checkpoint file (MySQL/SQL Server) + slot advance (PostgreSQL); at-least-once, crash-tested per engine
- ✅ changelog file format — typed Parquet/CSV through the same commit seam as a batch export (full type parity, byte-for-byte, validated via DuckDB/ClickHouse/BigQuery)
- ✅ snapshot + changelog workflow — `mode: full` + `mode: cdc` of the same table run in parallel via `rivet run`

### Why this matters
A natural evolution, but only after auditability, recovery, and data quality are solid.

### Scope note
This ships **CDC to files** — a bounded, resumable capture that lands typed files in the same
bucket through the same commit seam (destination + content-MD5 + manifest + `_SUCCESS`) the
batch path uses. It is **not** a streaming CDC *platform* (Kafka, real-time connectors, a SaaS
marketplace) — that boundary, stated throughout this roadmap, still holds.

---

## Epic 16 — Automatic Parallelism
**Priority:** P3  
**Pain coverage:** Pain A, Pain D
**Status:** ⚠️ Partial — `recommend_parallelism` (`src/preflight/analysis.rs:332`)
ships an advisory recommendation in `check`/`plan` for all three engines; the
"later auto-selected parallelism bounded by safety rules" deliverable (auto-apply
at `run`) is the remaining open piece (verified in-tree 2026-06-29).

### Goal
Move from warning-based guidance to controlled automatic parallelism selection.

### Deliverables
- recommended parallelism first
- later auto-selected parallelism bounded by safety rules

### Why this matters
Parallelism is valuable, but automation should come after better planner guidance and safety semantics.

## Epic 17 — MySQL Source Parity (COM_STMT_FETCH)
**Priority:** P1
**Pain coverage:** Pain A, Pain B
**Status:** ✅ RESOLVED — superseded by **Epic 18** (which re-derived and shipped
this work). The premise below (~9s longest statement; a server-side cursor as the
fix) was re-measured and is obsolete:
- **Re-measured 2026-06-29** (live MySQL `content_items`, current main, build A):
  at the planner's default `chunk_size` (100k) the longest chunk is **~2.0s**, not
  9s — the memory-adaptive batching + sane default already closed it. The ~9s only
  appears at a *manually set* large `chunk_size` (600k rows → 5.9s; ~500k → ~9s),
  i.e. a misconfiguration. Per-statement time scales linearly (~20µs/row).
- **Phase A (adaptive time-feedback) — WON'T BUILD:** `chunk_size` is already the
  lever; sub-paging measured as a +25% wall / 10×-QPS regression (Epic 18 A1-impl).
- **Phase B (COM_STMT_FETCH cursor) — WON'T BUILD:** proven expensive (the crate
  hardcodes `CURSOR_TYPE_NO_CURSOR`, `mysql_common 0.37.2 packets/mod.rs:2696`;
  raw `write_command` private) *and* ineffective (MySQL materialises the result to
  temp tables at cursor open, so it does not shorten the longest statement — Epic
  18 Phase D, `dev/spikes/mysql_cursor_efficacy.c`).

The shipped parity work lives in Epic 18 (session guards, pressure proxies, MSSQL
keyset, `chunk_size` documented as the statement-duration lever). The original
Epic 17 plan is kept below for history.

### Goal

Close the source-pressure gap between MySQL and PostgreSQL.

PostgreSQL issues a single `DECLARE cursor FOR SELECT …` and then repeats `FETCH N` — every round-trip returns a small batch, and the longest single SQL statement on a 2M-row wide table is **0.19s**.

MySQL uses chunked `SELECT … WHERE pk BETWEEN start AND end` queries, one per chunk.  The longest single statement on the same fixture is **~9s** — still 15–23× better than sling/dlt defaults, but far from PostgreSQL parity and potentially above a DBA's `max_execution_time`.

### Root cause

MySQL exposes server-side cursors via the binary protocol: `COM_STMT_EXECUTE` with flag `CURSOR_TYPE_READ_ONLY = 0x01`, followed by `COM_STMT_FETCH N`.  The `mysql` Rust crate v28 does not surface this flag in its public API — `exec_iter()` opens a fresh query per call with no persistent cursor.

### Deliverables (two-phase)

**Phase A — Adaptive time-feedback chunking** (1–2 weeks, no protocol changes)

- After each chunk, measure wall time.  If last chunk exceeded a configurable `chunk_max_statement_s` (default: 2s), halve `effective_bs` for the next chunk.
- Cap: do not shrink below `chunk_min_rows` (default: 1000) to avoid pathological overhead.
- Add `RIVET_MYSQL_CHUNK_TIME_FEEDBACK=1` env flag to enable the adaptive path explicitly during benchmarking.
- Expected result: longest single query on 2M-row table drops from ~9s to ~1–2s without protocol changes.
- Update benchmark tables in README to reflect new numbers once measured.

**Phase B — COM_STMT_FETCH server-side cursor** (4–8 weeks, protocol-level)

- Investigate `mysql` crate internals: determine whether `CURSOR_TYPE_READ_ONLY` can be set via an existing low-level API or requires a fork/PR.
- If crate supports it: implement a single-cursor extraction path for MySQL that mirrors the PostgreSQL `DECLARE … FETCH N` loop.  Path: prepare statement → execute with `CURSOR_TYPE_READ_ONLY` → `FETCH chunk_size` in a loop → `COM_STMT_CLOSE`.
- If crate does not support it: open upstream PR to `blackbeard/mysql_rust` exposing `CursorType` on `StatementParams`; track until merged; vendor if needed for near-term delivery.
- Known MySQL cursor limitations to test: `ORDER BY` in cursor context, temp-table creation on large result sets, `max_execution_time` interaction.
- Expected result: longest single query drops to sub-second on wide tables, matching PostgreSQL story.

### Definition of done

- [ ] Phase A: adaptive chunk feedback implemented and unit-tested; `chunk_max_statement_s` config field documented.
- [ ] Phase A: new benchmark numbers measured and README benchmark tables updated.
- [ ] Phase B: `mysql` crate cursor path investigated; upstream PR opened or workaround documented.
- [ ] Phase B: integration test covering the `FETCH N` loop against a live MySQL fixture.
- [ ] README note "MySQL parity roadmap" replaced with concrete timing numbers.

### Why this matters

The current README is honest about the 9s MySQL number, but it creates an asymmetric product story: "sub-second on Postgres, 9s on MySQL."  Many teams run MySQL exclusively.  Closing this gap removes the biggest technical objection for MySQL-first users and makes the benchmark headline symmetric.

---

## Epic 18 — Source parity (MySQL / MSSQL → PostgreSQL gold standard)

**Status:** ✅ DONE — all phases shipped & live-validated (2026-06-07/08);
re-verified in-tree 2026-06-29: `MysqlSessionGuard` (`src/source/mysql/mod.rs:438`,
`Drop` at `:468`), MSSQL `impl Drop` LOCK_TIMEOUT reset (`src/source/mssql/mod.rs:61`),
C1 pressure proxy `Created_tmp_disk_tables` + `Innodb_buffer_pool_wait_free`
(`mysql/mod.rs:71`), C2 Workfiles/Worktables (`mssql/mod.rs:760`), A2 MSSQL keyset,
and the efficacy spike `dev/spikes/mysql_cursor_efficacy.c`. The MySQL cursor
(Phase D) is closed with proof on both cost and efficacy. This epic subsumes
Epic 17.

**Goal:** PostgreSQL is the reference source. Close the per-engine gap on the
two axes that make it the gold standard — (1) longest-single-query under a
DBA `statement_timeout`, and (2) pooler-safe session state — without assuming
the gap requires a server-side cursor.

**Verified baseline (code, not README — re-verified against the tree at
roadmap-write time; file paths resolved, line refs current):**

| Mechanism | PG | MySQL | MSSQL |
|---|---|---|---|
| Server-side cursor (`DECLARE … FETCH N`, capped `work_mem×0.7`) | ✅ `src/source/postgres/mod.rs:405,427,467` | ❌ keyset emulation | ⚠️ `OFFSET 0 FETCH NEXT` (keyset) |
| Governor pressure proxy | ✅ `temp_bytes` + `checkpoints_req` + `work_mem` | ⚠️ `Innodb_log_waits` (write-pressure) | ⚠️ `Log Flush Waits` (write-pressure) |
| Keyset chunking (OPT-4) | ✅ any unique NOT NULL index `src/source/postgres/mod.rs:314-340` | ✅ unique NOT NULL index `src/source/mysql/mod.rs:303-335` | ✅ every single-column NOT NULL UNIQUE index (PK + unique constraints), PK-first `src/source/mssql/mod.rs:955-981` |
| Pooler-safe session | ✅ RAII `PgTxnGuard` + `SET LOCAL` `src/source/postgres/mod.rs:121-156` | ⚠️ session `SET max_execution_time` `src/source/mysql/mod.rs:519` (end-of-fn reset at `:543`, not `Drop`-safe) | ⚠️ none |

> Note: governor parity already exists — all three engines implement
> `sample_pressure`. The real remaining gaps are cursor/keyset page size,
> session-state leakage behind a pooler, and MSSQL keyset key selection.

---

### Phase A — measure-first, highest leverage (P0)

- [x] **A1 (measurement) — DONE 2026-06-07. The gating experiment is
      conclusive: the long MySQL query is the oversized page, not the missing
      cursor.** On live MySQL `content_items` (524 288 wide rows, ~3.9 KB/row),
      wall time of one chunk statement `SELECT * … WHERE id BETWEEN 1 AND N`:

      | page (rows) | one statement (wall) |
      |---|---|
      | 1 000 | 0.44 s |
      | 10 000 | **0.56 s** |
      | 100 000 (`default_chunk_size`) | **3.15 s** (1.36 s server + ~1.8 s transfer) |

      Per-statement time scales with `rows × row_width`, independent of table
      size (each chunk statement touches only `chunk_size` rows). At the bench's
      wider ~12 KB rows the 100 k page is the README's ~9 s; a ~10 k page lands
      at PG `FETCH N` levels (~0.5 s). **→ Phase D (MySQL server-side cursor) is
      gated OUT** — small index-backed pages close the gap; the upstream-crate
      cursor cost is not justified.
- [x] **A1 (implementation) — WON'T BUILD (2026-06-07). The existing
      `chunk_size` knob already is the lever; sub-paging would only regress.**
      The idea was to sub-page a range chunk via keyset
      (`WHERE id > last ORDER BY id LIMIT page`) into the same part file so the
      DBA sees ~0.5 s statements, not one 3–9 s. Measured, it is **not free**:

      | | total wall | statements | longest stmt |
      |---|---|---|---|
      | one 100k `BETWEEN` (today) | 3.40 s | 1 | 3.40 s |
      | 10× 10k keyset pages | **4.25 s (+25%)** | 10 (**×10 QPS**) | **0.40 s (÷8.5)** |

      So it trades ~25% throughput + 10× query count for shorter statements —
      it *shifts* DBA-harm from duration to QPS rather than removing it (10× the
      queries can load a busy source *more*). A free version needs a real
      server-side cursor (no re-seek) = the gated-out Phase D.

      **The decisive point: `chunk_size` already does this today.** A user whose
      chunk query trips a `statement_timeout` sets `chunk_size: 10000` →
      ~0.5 s statements, no new code. Sub-paging's *only* unique addition over
      that is keeping large output files while shortening the query — a niche
      "short query **and** large files" combination most users don't need.
      Resolution: don't build the `statement_page_rows` knob; document
      `chunk_size` as the statement-duration lever
      ([`reference/tuning.md` → Choosing `chunk_size`](docs/reference/tuning.md)).
      Revisit only if a real user hits the niche case.
- [x] **A2. MSSQL keyset on single-column unique-index keys (parity with PG) — DONE.**
      The key-selection logic from `src/source/postgres/mod.rs:314-340` (every
      single-column unique NOT NULL index) is now ported to
      `src/source/mssql/mod.rs:955-981`: it discovers every single-column NOT NULL
      UNIQUE index (PK + unique constraints), PK-first and de-duplicated, so
      surrogate-unique-key tables use the safe keyset path. (Composite-PK keyset
      remains a follow-up.)

### Phase B — pooler-safe session parity (P1)

- [x] **B1. MySQL session-reset RAII guard — DONE 2026-06-07.** Added
      `MysqlSessionGuard` (analogue of `postgres::PgTxnGuard`): it applies the
      two per-connection SETs (`time_zone = '+00:00'` always; `max_execution_time`
      when a statement timeout is configured) and resets them on `Drop`, so a
      pooled connection — returned to the mysql-crate pool or reused behind
      ProxySQL / MaxScale — is always clean before the next checkout. Closes two
      gaps the old end-of-function reset missed: (1) a **panic** mid-export
      unwinding past the reset, and (2) an **early `?`** on the
      `SET max_execution_time` itself (MariaDB spells it `max_statement_time`, so
      that SET errors — and `time_zone`, already set, leaked). The guard is
      constructed immediately after the `time_zone` SET so the reset is armed
      before any later failure. Live-validated: the reset fires on teardown
      (probe), is correctly gated (`max_exec` reset only when armed;
      `statement_timeout_s: 0` skips it), and the `live_mysql_chunked` suite
      (every run resets `time_zone`) stays green.
- [x] **B2. MSSQL session/transaction hygiene — DONE 2026-06-07.** Verified the
      source never opens a transaction (no `BEGIN TRAN`/`COMMIT`/`ROLLBACK`
      anywhere — every read is an autocommit `SELECT`), so the `block_on` bridge
      (ADR-0011) leaves nothing dangling. The only session-mutating SET is
      `SET LOCK_TIMEOUT` (`src/source/mssql/mod.rs:213`); added `impl Drop for
      MssqlSource` that resets it to the SQL Server default (`-1`) before the
      connection closes, so a *multiplexed* pooler that keeps the backend alive
      can't hand our non-default `LOCK_TIMEOUT` to the next session.
      Best-effort + 2 s time-boxed so `Drop` can never hang; gated on a flag so
      it fires only when the SET was issued (lock_timeout > 0; default profile is
      30 s, `lock_timeout_s: 0` skips it). Live-validated: reset executes on
      teardown (probe: `inner_ok=true`), is skipped when no SET ran, and the
      whole `live_mssql_chunked` suite stays green.

### Phase C — pressure-proxy fidelity (P2)

- [x] **C1. MySQL extraction-pressure proxy — DONE 2026-06-07.** Replaced the
      old `Innodb_log_waits` (redo-**write** pressure, near-flat during a read)
      with `mysql_sample_extraction_pressure` = the sum of
      `Created_tmp_disk_tables` + `Innodb_buffer_pool_wait_free` — sort/temp-table
      spill **and** buffer-pool memory pressure, the PG `temp_bytes` analogue.
      Both are monotonic globals so their sum is too (governor `cur > prev`
      unchanged). The sum is deliberately robust to MySQL 8.0's `TempTable`
      engine, where a spill may not bump `Created_tmp_disk_tables`
      (live-confirmed delta=0) — `Innodb_buffer_pool_wait_free` carries the signal
      then. Live-validated end-to-end via an adaptive export.
- [x] **C2. MSSQL tempdb-pressure proxy — DONE 2026-06-07.** The Epic's
      suggested `sys.dm_db_task_space_usage` / Page Life Expectancy are **gauges**
      (non-monotonic; PLE is also inverted), which don't fit the governor's
      monotonic `cur > prev` contract — so instead sample cumulative
      `Workfiles Created` + `Worktables Created` (`sys.dm_os_performance_counters`,
      Access Methods): a workfile/worktable is created when a sort or hash spills
      to **tempdb** — the SQL Server analogue of `temp_bytes` /
      `Created_tmp_disk_tables`, and monotonic. Replaced `Log Flush Waits`;
      dropped the now-unused per-database `MssqlSource::database` field.
      Live-validated via an adaptive export.

### Phase D — MySQL server-side cursor (P3) — WON'T BUILD, now with proof on both axes

- [x] **D1. CLOSED 2026-06-08 — proven expensive AND measured ineffective.**
      Two separate kills, each verified rather than asserted:

      **Cost — proven by reading the client code.** `mysql_common 0.37.2`
      hardcodes the cursor flag at the *type* level:
      `flags: Const::new(CursorType::CURSOR_TYPE_NO_CURSOR)`
      (`packets/mod.rs:2696`) — no runtime setter — and the `mysql 28.0` crate's
      `write_command` / raw-packet APIs are private (no escape hatch). Neither
      `mysql_async` (same `mysql_common`) nor `sqlx` exposes a read-only cursor
      either. So a Rust cursor requires forking `mysql_common` + patching the
      `mysql` crate's exec/FETCH state machine — a real, ongoing fork cost.

      **Efficacy — MEASURED (an earlier *asserted* claim, now proven).** A
      standalone `libmysqlclient` probe
      ([`dev/spikes/mysql_cursor_efficacy.c`](dev/spikes/mysql_cursor_efficacy.c))
      opened a real `CURSOR_TYPE_READ_ONLY` cursor on a chunk-shaped `BETWEEN`
      query and timed the open:

      | | result |
      |---|---|
      | cursor OPEN time (3 runs) | 0.78–1.82 s — **the longest single statement** |
      | `Created_tmp_tables` delta at open | **3, every run** |
      | fetches after open | cheap (~0.15 s / 10k) |

      So MySQL's read-only cursor **materialises the whole result into temp
      tables at open**, then fetches cheaply — the *opposite* of PG's streaming
      `FETCH N` (which pulls incrementally from a live scan, no temp table). The
      cursor therefore does **not** shorten the longest statement: it moves the
      cost into a long materialising open **and** adds tempdb pressure the
      keyset / `chunk_size` path never causes. Even after paying the fork, D
      would be a **regression** vs the free `chunk_size` lever (~0.5 s pages, no
      temp tables).

      > Honesty note: the materialisation behaviour was first stated from
      > recollection (not in the client code we read) and overstated as fact;
      > it is now backed by the measurement above. Phase D stays closed on
      > **proven** grounds, not a hunch.

---

**Crosswalk update (§9.1):** Epic 17 (MySQL parity) and Epic 16
(Auto-parallel) feed into this; the former Phase B "COM_STMT_FETCH" becomes
Epic 18 Phase D, now explicitly gated on the Phase A measurement.

**Exit criteria (revised after the A1 measurement):**
- **Longest-single-query** — *resolved by tuning, not by a new default.* At
  defaults MySQL/MSSQL run one statement per chunk, so the gap vs PG's cursor is
  inherent; closing it "at defaults" would need either the sub-paging regression
  (A1-impl, won't-build) or the gated Phase D cursor. Instead `chunk_size` is
  documented as the statement-duration lever (`reference/tuning.md`): a user
  under a strict `statement_timeout` sets `chunk_size: 10000` for ~0.5 s
  statements. ✅
- **No session-state leak behind a pooler** — B1 (MySQL RAII guard) + B2 (MSSQL
  `Drop` reset) shipped and live-validated. ✅
- **MSSQL keyset coverage matches PG's index eligibility** — A2 shipped: every
  single-column unique NOT NULL index, not just the PK. ✅

---

# 5.1 Packaging, Trust, and External Adoption Track

This track replaces the former standalone `rivet_packaging_trust_roadmap.md`.
Its purpose is not to add extraction surface area; it makes the existing product
credible to a team evaluating Rivet without help from the author.

## Product trust message

Rivet should be positioned as:

> Safe, resumable, observable database extraction under failure and resource constraints.

Not as:

> PostgreSQL/MySQL to Parquet/CSV exporter.

The stronger message is backed by explicit docs and code paths: plan/apply,
preflight diagnostics, tuning profiles, state, journal, manifest, reconciliation,
recovery semantics, quality gates, resource controls, and reliability coverage.

## Completed trust hygiene

| Item | Status | Evidence |
|---|---|---|
| Version consistency discipline | ✅ Active | release workflow, `rivet --version`, release tags, changelog/release docs |
| Security policy | ✅ Done | `SECURITY.md` documents access, artifacts, credentials, TLS, supply chain, reporting |
| Sensitive artifact hygiene | ✅ Done | README/security docs warn about `.rivet_state.db`, plans, journals, configs, outputs |
| README top positioning | ✅ Done | README leads with source-safe extraction, scope, non-goals, core promise |
| Fit / non-fit boundaries | ✅ Done | README + semantics exclude SaaS marketplace, k8s platform, loading, and **continuous/live-streaming replication** — and now correctly distinguish that from **CDC-to-files** (`mode: cdc`, shipped 0.14.0): `README.md:195` + `docs/semantics.md:187` reworded to "CDC captured to files, not a continuously-running replication sink" |
| Execution semantics contract | ✅ Done | `docs/semantics.md` covers retry, crash, resume, repair, reconcile, non-guarantees |
| Reliability matrix | ✅ Done | `docs/reliability-matrix.md` separates PR CI, nightly, and manual coverage |
| Compatibility matrix | ✅ Done | `docs/reference/compatibility.md` covers PG 12-16, MySQL 5.7/8.0, and SQL Server 2022 (Beta) |
| Pilot kit | ✅ Done | `docs/pilot/README.md`, quickstarts, demo, walkthrough, production checklist |
| Benchmark methodology | ✅ Done | `docs/bench/` and best-practices methodology docs capture DB-side signals |

## Remaining trust work

| Work | Priority | Status | Definition of done |
|---|---|---|---|
| Release checksums (`SHA256SUMS`) | P1 | ✅ Done | `release.yml` publishes `SHA256SUMS.txt` per release; verification documented in README + SECURITY.md |
| Signed releases / attestations | P2 | ✅ Done | `SHA256SUMS.txt` signed with cosign keyless (Sigstore + GitHub OIDC) → `SHA256SUMS.txt.cosign.bundle`; `cosign verify-blob` documented in SECURITY.md |
| SBOM | P2 | ✅ Done | Release publishes an SPDX SBOM (`rivet-<tag>.spdx.json`, syft) of the source + Cargo graph; its hash rides in the signed `SHA256SUMS.txt`; SECURITY.md SBOM row → Active |
| 24h+ soak tests | P2 | ⏳ Open | Long-horizon extraction run documented in reliability matrix |
| Real-cloud destination release smoke | P2 | ✅ Done | `docs/cloud-smoke-tests.md` (dated S3/GCS/Azure record) + `docs/release-checklist.md` §3 (per-tag smoke gate before publish) |
| k8s/Helm deployment guidance | P3 | ✅ doc-guidance done | `docs/who-is-this-for.md` (Job/CronJob supported; operator/Helm chart an explicit non-fit). An actual Helm chart / operator remains a deliberate non-goal / future |

## Demo/adoption priorities

The current GIFs and pilot docs cover the minimum adoption story:

1. `docs/gifs/basic.gif` — init, doctor, check, run, state.
2. `docs/gifs/plan-apply.gif` — sealed plan/apply with credential redaction.
3. `docs/gifs/reconcile-repair.gif` — chunked reconcile and targeted repair.
4. `docs/pilot/demo-quickstart.md` — scripted 14-table pilot fixture.

Next demos should focus on proof under pressure rather than happy-path export:

1. **Wide-table memory protection** — show RSS before/after batch caps and `work_mem`-aware FETCH sizing.
2. **Source-pressure run** — show export under concurrent OLTP writes with DB-side signals.
3. **Release verification** — once checksums/signing ship, document a clean install verification flow.

---

# 5.2 Production Feedback, v0.5 Stabilization, and Performance Track

This track consolidates the former `rivet_reddit_feedback_roadmap.md`,
`rivet_v0_5_stabilization_roadmap.md`, and `rivet_performance_roadmap.md`.
It keeps the production-viability feedback visible without fragmenting planning
across multiple roadmap files.

## Production feedback status

| Feedback item | Status | Current answer |
|---|---|---|
| pgBouncer / pooler safety | ✅ Done | Postgres transaction-pooler detection, `SET LOCAL` scoped inside guarded transactions, pgBouncer live tests, pool-safety coverage |
| MySQL pooler / multiplexer detection | ✅ Done | `MysqlProxyKind` (Direct, ProxySql, MaxScale, Multiplexed); 4-signal classifier (`PROXYSQL INTERNAL SESSION`, `@@version_comment`, `@@proxy_version`, `CONNECTION_ID()` drift) with 13 unit tests + ProxySQL live tests (`docker compose --profile pool up -d proxysql`) |
| MySQL live-test symmetry with PG | ✅ Done | 6 paired suites (`live_mysql_crash_recovery`, `live_mysql_chunked_recovery`, `live_mysql_resume`, `live_mysql_schema_drift`, `live_mysql_retry_and_faults`, `live_mysql_reconcile_repair`) — ~31 MySQL tests mirror the PG matrix 1:1 for crash points / chunked recovery / resume / schema drift / retry+toxiproxy / reconcile+repair |
| Session-state cleanup | ✅ Done | Postgres RAII transaction guard; MySQL session variables reset on all paths; seed FK checks cleaned up |
| OLTP load tests | ✅ Done / ongoing | `live_oltp_load` plus `live_content_load` exercise concurrent writes, checkpoint pressure, MVCC snapshot behavior |
| Adaptive runtime feedback | ✅ Partial | `tuning.adaptive` reacts to Postgres `checkpoints_req` and MySQL `Innodb_log_waits`; still no `pg_stat_io` / tablespace-level controller |
| MCP operational visibility | ✅ Done | `rivet-mcp --stdio` (separate binary) exposes read-only Postgres, MySQL, and pgBouncer diagnostics |
| README/product boundaries | ✅ Done | README states no CDC, no SaaS marketplace, no k8s platform, no loading/transformation |

Open edge: feedback-loop guarantees are intentionally bounded. Rivet is still a
batch SELECT extractor with adaptive guardrails, not CDC, Arrow Flight SQL, or a
DB-resource governor.

## v0.5.x stabilization status

| Area | Status | Notes |
|---|---|---|
| Batch memory policies | ✅ Done | `max_batch_memory_mb` with `warn`, `fail`, `auto_shrink`; tests and best-practices docs |
| Row group tuning | ✅ Done | `parquet.row_group_strategy`, target/max row group settings, docs and golden tests |
| Compression profiles | ✅ Done | `none`, `fast`, `balanced`, `compact`; docs explain CPU/size trade-offs |
| Quality memory control | ✅ Partial | typed hashing and `unique_max_entries`; future: approximate distinct / default caps |
| Resume semantics | ✅ Done | stricter `--resume`, recovery docs, chunk checkpoint tests |
| Resource estimates | ✅ Partial | `rivet plan` shows advisory estimates; future: optional sampling (`plan --sample N`) |
| Benchmark evidence | ✅ Partial | `docs/bench/` cross-tool harness and DB-side signals; future: release-gated perf budget |

## Performance/resource-control status

| Area | Status | Current implementation / next step |
|---|---|---|
| Benchmark matrix | ✅ Partial | `docs/bench/harness/` captures wall/RSS/CPU/output and DB-side signals; keep manual before release |
| Adaptive batch by row width | ✅ Done | `batch_size_memory_mb`, first-batch observation, Postgres `work_mem` cap, MySQL row buffer cap |
| Hard Arrow batch cap | ✅ Done | actual Arrow buffer accounting via `get_array_memory_size()` |
| Quality unique hot path | ✅ Done | typed xxHash3-64 hashing; cap support remains opt-in |
| Compression presets | ✅ Done | profile-to-codec mapping shipped |
| Parquet row group tuning | ✅ Done | auto/fixed rows/fixed memory modes |
| PostgreSQL state optimization | ✅ Done | chunk task/state paths use structured stores and transactional writes |
| Direct multipart upload | ⏳ Future | optional complexity; current S3/GCS paths use streaming writes |

Near-term resource-control priorities:

1. Add optional `rivet plan --sample N` to replace purely heuristic memory estimates when users can afford a small source probe.
2. Add a warning when `quality.unique_columns` is configured without `unique_max_entries`.
3. Promote the DB-signal benchmark harness into the release checklist so source-friendliness regressions are visible before publishing.
4. Keep `pg_stat_io` / tablespace / IO-concurrency adaptation as future DBA-grade work, not a v0.x guarantee.

---

# 6. Suggested execution order

## Phase 1 — Make Rivet trustworthy
1. Epic 1 — Preflight Planner & Source Safety
2. Epic 2 — Auditability, Manifest & Reconciliation
3. Epic 3 — Recovery & Interrupted Run Semantics
4. Epic 4 — Durable State Backend

## Phase 2 — Make Rivet more source-aware
5. Epic 5 — Real Batch / Fetch / Write Control
6. Epic 6 — Date / Timestamp / Range Partition Intelligence
7. Epic 7 — Schema Drift Visibility & Policy
8. Epic 8 — Data Shape Drift Detection
9. Epic 9 — Data Contract Checks

## Phase 3 — Improve adoption and usability
10. Epic 10 — Config & Connection UX Improvements
11. Epic 11 — Installation & Packaging
12. Epic 12 — Deployment Modes Guidance

## Phase 3.5 — Source-engine parity
17. Epic 17 — MySQL Source Parity (COM_STMT_FETCH)
18. Epic 18 — Source parity (MySQL / MSSQL → PostgreSQL gold standard)

## Phase 4 — Expand carefully
13. Epic 13 — SSH / Jump Host Access
14. Epic 14 — Narrow Warehouse Load Layer
15. Epic 15 — WAL/Binlog CDC to Files — ✅ **shipped in 0.14.0**
16. Epic 16 — Automatic Parallelism

---

# 7. Strongest near-term niche

The best near-term niche remains:

**small-to-mid data teams extracting from PostgreSQL/MySQL and similar database-first operational sources into analytics-friendly files/storage, where current custom pipelines are painful and heavy ingestion platforms are not a good fit.**

---

# 8. Final summary

The strongest and most defensible direction for Rivet remains:

**safe, predictable, source-aware extraction from fragile database-first systems**

The roadmap should continue to optimize for:
- trust
- safety
- explainability
- auditability
- recovery
- drift visibility
- controllable performance

before broadening into:
- warehouse loading
- CDC
- broader platform ambitions

---

# 9. Execution status (lettered epics & phases)

This section merges the former `rivet_roadmap_v3.md` task tracker. **Strategic priorities** remain the numbered epics in §5; **delivery tracking** uses the lettered epics below.

## 9.1 Crosswalk: numbered (§5) ↔ lettered (§9)

| §5 epic (pain roadmap) | Lettered execution epics | Notes |
|------------------------|--------------------------|--------|
| Epic 1 — Preflight | **B** | Strategy output, profile recommendation, sparse warnings |
| Epic 2 — Auditability | **D**, **F** | Run summary, manifest, metrics, `--reconcile` |
| Epic 3 — Recovery | **C**, **H** | Lifecycle semantics, crash matrix, E2E recovery |
| Epic 4 — Durable state backend | `src/state/mod.rs` | `StateConn::Postgres` via `RIVET_STATE_URL`; auto-migration; `StateRef` for workers ✅ |
| Epic 5 — Batch/fetch/write control | **M**, tuning | `batch_size`, `max_file_size`, streaming; not full split of row-group vs fetch |
| Epic 6 — Partition intelligence | **B**, modes | `chunked`, `time_window`, dense surrogate guidance; `chunk_by_days` date-native partitioning ✅ |
| Epic 7 — Schema drift | **D** (tracking) | `on_schema_drift: warn\|continue\|fail` ✅ |
| Epic 8 — Shape drift | **N2** (Phase 3 table) | `export_shape` + `shape_drift_warn_factor` ✅ |
| Epic 9 — Data contracts | `quality:` YAML | Row bounds, null ratio, uniqueness; chunked aggregate rules |
| Epic 10 — Config UX | **E**, **J**, misplaced-field validation | Docs + errors |
| Epic 11 — Packaging | **L** | Release workflow, binaries, Docker GHCR, Homebrew, crates.io ✅ |
| Epic 12 — Deployment | docs, `docker-compose`, state backend | pilot/production checklist + durable state ✅; k8s/Helm = future |
| Epic 13 — SSH | **O** | External tunnel docs first; native SSH = future |
| Epic 14 — Warehouse load | `src/types/`, M1–M6 | Type system + type report + BQ compat ✅; direct load path = future |
| Epic 15 — CDC | **N** | WAL/binlog CDC to files **shipped 0.14.0** ✅ (3 engines: `src/source/{postgres,mysql,mssql}/cdc.rs`) |
| Epic 16 — Auto-parallel | *(none)* | Auto-parallelism = future |
| Epic 17 — MySQL parity | *(none yet)* | Phase A: adaptive chunk timing; Phase B (COM_STMT_FETCH) → re-homed as **Epic 18 Phase D**, now gated on the Epic 18 Phase A measurement |
| Epic 18 — Source parity | `src/source/{postgres,mysql,mssql}/mod.rs` | Close MySQL/MSSQL longest-single-query + pooler-safe session gap vs the PG gold standard. A1 (keyset page size) gates D (MySQL server-side cursor); A2 = MSSQL keyset on unique indexes; B1/B2 = session-reset hygiene |

**Auth and connectivity (lettered A)** underpin all runs and map across Epics 1–2 and §7 (niche).

---

## 9.2 Current state (0.6.0)

Rivet core is **feature-complete for stable extraction with type safety, resource controls, and external trust documentation**. All Wave 1–3 stabilisation epics are shipped; Epic 14 type safety layer (M1–M6) complete; observability layer includes persistent RunJournal; packaging/trust docs now cover security, semantics, reliability, compatibility, pilots, and benchmarks. 0.6.0 adds the `table:` config shortcut, MySQL chunking/memory parity with Postgres, `work_mem`-aware `FETCH` capping, ProxySQL / MaxScale detection, the first published cross-tool benchmark harness (defaults + steelman), and ships the MCP server as a separate `rivet-mcp` binary.

### Extraction

- PostgreSQL and MySQL streaming extraction
- Parquet and CSV (zstd default; snappy, gzip, lz4, none)
- Local, S3, GCS, **stdout** (streaming uploads)
- Modes: `full`, `incremental`, `chunked`, `time_window`
- `max_file_size`, `--param` / `${VAR}`, `skip_empty`, shell completions
- `batch_size_memory_mb`, **jemalloc** (default feature), misplaced `tuning` validation
- `rivet check`, `rivet doctor`, SQLite state + migrations, metrics, file manifest, chunk checkpoints
- Schema tracking, typed retries (SQLSTATE / MySQL codes), `--validate`, `--reconcile`
- Tuning profiles, Slack, data **quality** checks, meta columns
- Date-native chunking (`chunk_by_days`), connection limit warnings, `rivet init`
- **Plan/Apply workflow** — sealed execution artifacts (`rivet plan` / `rivet apply`, ADR-0005)
- **Parallel exports** — `--parallel-exports` (threads) and `--parallel-export-processes` (OS processes) with live cards UI

### Type safety (M1–M6, Epic 14)

- Canonical type system: `RivetType`, `TypeMapping`, `TypeFidelity`, `ColumnOverrides`
- `TypePolicy` with Fail/Warn/Allow per fidelity level; `--strict` CLI gate
- `rivet check --type-report [--json] [--target bigquery]`
- BigQuery compatibility layer: NUMERIC, BIGNUMERIC, TIMESTAMP, DATETIME, REPEATED, overflow warnings
- Per-column overrides: `columns: { col: decimal(p,s) }` inline string syntax; `rivet init` auto-generates from `information_schema` precision/scale
- Complex types: Enum, Interval→Utf8 ISO 8601 (Postgres), List/Array (Postgres), MySQL TIME/TIME2, MySQL ENUM/SET
- Unsupported-column errors now report **all** unmappable columns in one message (not just the first)
- **Golden E2E trust proof** — [`tests/live_type_golden.rs`](tests/live_type_golden.rs): Postgres *and* MySQL; DB → `rivet run` → Parquet → Arrow read-back; decimal sums, timestamp `tz=None` vs `UTC`, binary, UUID/VARCHAR text, INTERVAL ISO 8601 values. Dedicated `test-type-golden` CI gate (Postgres + MySQL only, faster than full `e2e` job).

### Observability

- `RunJournal` domain model: 14 `RunEvent` variants (plan snapshot, files, retries, chunks, quality issues, schema changes, outcome)
- **Persistent** — serialized to `run_journal` SQLite table (migration v7) at end of every run; `store_journal` / `load_journal` / `recent_journals` APIs
- **`rivet journal --config <file> --export <name>`** — inspect last N runs: per-run header (status, duration, run_id), files/rows/bytes summary, retries, quality issues, schema changes, error first line
- `--run-id <id>` flag to inspect a specific run
- **MCP server** — read-only DB introspection tools for Postgres/MySQL/pgBouncer (`pg_stat_activity`, checkpoint pressure, table stats, locks, `pg_stat_statements` IO, processlist, key metrics)

### Architecture (stabilisation plan — all complete)

- `ResolvedRunPlan` planning layer; `RawConfig` / `ValidatedConfig` separation (Epics 1, 3)
- Centralized `PlanValidator` with compatibility diagnostics (Epic 2)
- State update invariants + `tests/invariants.rs` (Epic 4, ADR-0001)
- CLI-product boundary locked, module visibility hardened (Epic 5, ADR-0002)
- Domain state stores: `CursorStore`, `ManifestStore`, `MetricsStore`, `SchemaHistoryStore`, `JournalStore` (Epics 6, 10)
- `ExtractionStrategy` explicit types; Planning / Execution / Persistence layers (Epics 7, 8, ADR-0003)
- `DestinationCapabilities`, `WriteCommitProtocol` per backend (Epic 9, ADR-0004)
- Semantic release gates — `test-invariants`, `test-recovery`, `test-compatibility`, **`test-type-golden`** required before build (Epics 11–14/arch)

### Release & distribution

- **CI:** rustfmt, clippy, tests, release build, **E2E** (Docker Compose), cargo audit, semantic gates + `test-type-golden`
- **Release:** Linux x86_64/arm64, macOS arm64/Intel binaries; Docker GHCR; Homebrew tap
- **Published:** `rivet-cli` on crates.io
- **Trust docs:** `SECURITY.md`, `docs/semantics.md`, `docs/reliability-matrix.md`, `docs/reference/compatibility.md`, `docs/pilot/`, `docs/bench/`

---

## 9.3 Phase 1 — Pilot alpha stabilization ✅ COMPLETE

### Epic A — Auth and connectivity ✅

| Task | Status | Notes |
|------|--------|-------|
| A1. Credential precedence matrix | ✅ | README §Credential precedence |
| A2. GCS ADC support | ✅ | ADC / `credentials_file` |
| A3. GCS explicit JSON credentials | ✅ | Validated at load |
| A4. DB credential normalization | ✅ | URL vs structured, mutual exclusion |
| A5. Auth diagnostics | ✅ | `rivet doctor` |

### Epic B — Preflight and planner 2.0 ✅

| Task | Status | Notes |
|------|--------|-------|
| B1–B6 | ✅ | Strategy, profile, sparse/dense warnings, parallel hints, verdict suggestions |
| B-conn | ✅ | Connection limit warning: `parallel >= max_connections` warns with exact numbers; skipped gracefully if fetch fails |

### Epic C — Execution semantics ✅

| Task | Status | Notes |
|------|--------|-------|
| C1–C5 | ✅ | Lifecycle, cursor timing, duplicates, retries, validation scope — documented |

### Epic D — Observability and run summary ✅

| Task | Status | Notes |
|------|--------|-------|
| D1–D4 | ✅ | Summary, `rivet metrics`, `rivet state files`, aligned IDs |

### Epic E — Documentation ✅

| Task | Status | Notes |
|------|--------|-------|
| E1–E5 | ✅ | README, USER_GUIDE modes/profiles/auth/limitations |

### Epic M — Output and CLI

| Task | Status | Notes |
|------|--------|-------|
| M1–M7 | ✅ | Compression, skip empty, splits, memory batch, completions, stdout, params |
| M8. Per-column Parquet encoding | ⏳ | Not started |

### Bonus (not in original alpha list)

| Item | Notes |
|------|--------|
| Streaming cloud uploads | `std::io::copy` to S3/GCS/stdout |
| Misplaced tuning detection | Clear errors + hints |
| Versioned SQLite migrations | `schema_version` |
| `aws_profile` for S3 | Sets `AWS_PROFILE` for OpenDAL chain |
| Chunked quality gate | Row-count bounds after all chunks; warn on null/unique in chunked mode |
| QA live-test harness | `tests/common/mod.rs` + **285** ignored live tests across **51** `tests/live_*.rs` binaries mapped in [docs/reference/testing.md](docs/reference/testing.md); full offline suite (**~1833** `#[test]` in `src/`, ≈2200 with integration tests) runs on every PR via `cargo test`. Includes **golden type round-trip** ([`live_type_golden.rs`](tests/live_type_golden.rs)). |

---

## 9.4 Phase 2 — Pilot readiness and battle testing

### Epic F — Auditability and correctness

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| F1 | ✅ Partial | P1 | Metrics + reconcile; formal “audit mode” TBD |
| F2 | ✅ | P1 | `--reconcile` |
| F3 | ✅ | — | Per-file row counts in state |
| F4 | ✅ | P1 | MATCH/MISMATCH in summary |
| F5 | ✅ | P2 | Reconcile vs validate tradeoff table added to cli.md |

### Epic G — Real-world test harness

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| G1 | ✅ | P1 | MinIO + E2E |
| G2 | ✅ | P2 | Toxiproxy wired into `docker-compose.yaml`, registered via `tests/common/mod.rs::ensure_toxi_proxy`, exercised by `tests/live_retry_and_faults.rs` + `tests/live_chaos.rs`; cross-process flock guard prevents suite races |
| G3 | ✅ Partial | P1 | `seed` inserts; limited mutations |
| G4 | ✅ Partial | P1 | `dev/` configs; edge fixtures TBD |
| G5 | ✅ | P0 | E2E matrix in CI — `ci.yml::e2e` runs both `dev/e2e/run_e2e.sh` **and** `cargo test --release -- --ignored` (**285** ignored live tests across **51** `tests/live_*.rs` binaries, including **Trust golden** parity on Postgres + MySQL in `live_type_golden.rs`). See [docs/reference/testing.md](docs/reference/testing.md). |

### Epic H — Crash and recovery

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| H1 | ✅ | P1 | `dev/CRASH_MATRIX.md` |
| H2 | ✅ | P2 | Env-var-driven fault-injection hook in `src/test_hook.rs` (`RIVET_TEST_PANIC_AT`); four fault points across the write cycle (`after_source_read`, `after_file_write`, `after_manifest_update`, `after_cursor_commit`); crash-point recovery matrix in `tests/live_crash_recovery.rs`. Zero overhead when env var is unset (one relaxed atomic load per call). |
| H3 | ✅ | P1 | E2E recovery paths |
| H4 | ✅ Partial | P1 | Link CRASH_MATRIX from USER_GUIDE if missing |

### Epic I — Performance envelope

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| I1 | ✅ Partial | P1 | Manual runs; standardized datasets TBD |
| I2 | ✅ | P1 | `cargo bench` + `dev/scripts/bench.sh` save/compare; column_scan + shape_tracking groups |
| I3 | ✅ | — | USER_GUIDE defaults |
| I4 | ✅ | — | Check warnings |
| I5 | ✅ | P2 | Capacity/memory planning section in tuning.md (peak RSS formula, table width rules) |

### Epic J — Product UX

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| J1 | ✅ | P1 | `docs/` + `examples/` |
| J2–J4 | ✅ | — | Errors, troubleshooting, `doctor` |

### Epic K — First pilot rollout

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| K1 | ✅ | — | Pilot tables exercised |
| K2 | ✅ Partial | P1 | Multi-day automation TBD |
| K3 | ⏳ | P2 | Feedback template |
| K4 | ⏳ | P2 | Findings doc |

---

## 9.5 Phase 3 — Release engineering and ecosystem

### Epic L — Release and distribution *(lettered; not Epic 4 durable state)*

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| L1 | P0 | ✅ | Release workflow + matrix (Linux + macOS); green on v0.2.0-beta.2 |
| L2 | P0 | ✅ | GitHub Release assets published at v0.2.0-beta.2 |
| L3 | P0 | ✅ | `cargo publish` — rivet-cli v0.2.0-beta.2 published to crates.io |
| L4 | P1 | ✅ | Docker image via Dockerfile + GHCR (ghcr.io/panchenkoai/rivet) |
| L5 | P2 | ✅ | Homebrew tap panchenkoai/homebrew-rivet; auto-updated on release |

### Epic N — Advanced features (post-pilot)

| Task | Priority |
|------|----------|
| N1–N6 | P2–P3 as in prior roadmap (encoding, shape drift, strict YAML, webhook, rate limits) |

### Epic O — Future vision

| Task | Priority |
|------|----------|
| O1–O10 | P3 — Iceberg/Delta, multi-source, encryption, Prometheus, plugins, UI, sources, Flight, serverless (CDC shipped 0.14.0 — no longer "future") |

---

## 9.6 Next priorities (rolling)

Prioritize by stabilization before distribution polish:

**Completed (v0.2.0-beta.2 → v0.3.5):**

1. ✅ **Green GitHub Release** — v0.2.0-beta.2 published (binaries, Docker, Homebrew tap).
2. ✅ **L3** — `cargo publish` — rivet-cli on crates.io.
3. ✅ **Connection limit warning** — `rivet check` warns when `parallel >= max_connections`.
4. ✅ **Date-native chunking** — `chunk_by_days` with `>= date AND < date` semantics, parallel support, E2E tested.
5. ✅ **Stabilisation plan Waves 1–3** — all 14 arch epics shipped (invariant/recovery/compatibility tests, semantic release gates).
6. ✅ **Parallel export processes** — `--parallel-export-processes` with live cards UI (v0.3.4).
7. ✅ **Cards UI for `--parallel-exports`** — unified cards renderer + compact summaries (v0.3.5).
8. ✅ **Type safety layer M1–M6** — `rivet check --type-report`, `TypePolicy`, BigQuery compat, complex types (Enum/Interval/List).
9. ✅ **Type Roundtrip Proof (v0.7.8)** — PG/MySQL → Parquet/CSV preserved through 4 independent reader validators (DuckDB, ClickHouse, pyarrow, BigQuery); native Parquet `LogicalType::Uuid` / `LogicalType::Json` via `arrow.uuid` / `arrow.json` extension types; 31 live tests; cross-tool fidelity benchmark. Three real driver bugs fixed along the way: PG arrays losing NULL elements, MySQL ENUM/SET misclassified as String, MySQL `native_type` collapsing UNSIGNED / `TINYINT(1)` / `BIT(1)` / CHAR / VARCHAR variants.

**Remaining open items (P1 first):**

1. ✅ **Epic 7 schema drift policy** — `on_schema_drift: warn|continue|fail` YAML hook shipped.
2. ✅ **Epic 8 data shape drift** — `export_shape` SQLite table; `shape_drift_warn_factor` YAML config; warns on `N×` growth.
3. ✅ **F5 + I5** — reconcile/validate tradeoffs (cli.md); capacity/memory planning (tuning.md).
4. ✅ **I2** — `cargo bench` + `dev/scripts/bench.sh` save/compare harness; column_scan + shape_tracking groups.
5. ✅ **Epic 4 (§5)** — external/durable state backend: `RIVET_STATE_URL` PostgreSQL backend shipped.
6. ✅ **Verify / Validation Layer (shipped 0.15.0 + graded depth/codes)** — *"are produced files + manifest + state + summary internally consistent, AND is the data itself intact?"*. **Value-integrity depth SHIPPED** (v0.15.0): an always-on per-column `xxh3` value checksum — **Form A** cross-checks source↔Arrow in-process (catches the `build_array` converter mid-run) and **Form B** records per-column checksums in the manifest so `rivet validate` re-reads the Parquet and compares (catches an Arrow→Parquet encode / post-write fault). This **answers the design-open question toward extending `rivet validate`** (ADR-0013 "no new flags", NOT a new `rivet verify` command) and delivers the deepest "full file scan" level. Scope is honest: *decoded-value → file*, covered types only (uncovered logged, never a silent zero), probabilistic (xxh3-64), the DB-wire decode trusted; live round-trip proven (export → validate pass → flip a byte → validate fails). Cost vs v0.14.1: +2.8% wall / +11.3% CPU. **Now SHIPPED too** (`feat/verify-and-ux`): the graded `rivet validate --depth light|sample|full` UX, stable `RIVET_VERIFY_*` codes (one per failure variant — in JSON `code` + the pretty `[CODE]` prefix), and JSON output, over the artifact-consistency checks that already lived in Epic 2's `--validate` (missing/partial/orphan, size mismatch, `_SUCCESS` fingerprint). The Verify Layer is feature-complete; only a manifest↔state cross-divergence check remains a future nicety.
7. ✅ **Operator UX & Diagnostics (v0.8.0) — DONE** — structured diagnostics with stable codes, severity, `--json` everywhere, strategy explanation, a `doctor` report. **Shipped so far** (`feat/verify-and-ux`): `rivet check --json` now emits the per-export verdict / strategy / warnings / recommended profile+parallel / capabilities (not just the type report); `rivet plan` now **explains the strategy** (`plan::explain` — why this mode, chunk geometry, and parallelism, the RSS-budget fit, plus the resumability + memory risk); `RIVET_VERIFY_*` codes shipped (validate). **Done since** (`feat/operator-config-source-codes`, 2026-06-29): stable `RIVET_CONFIG_*` / `RIVET_SOURCE_*` codes via a `CodedError` typed marker (surfaced as a JSON `code` field + a `[CODE]` text prefix on the top-level error; `RIVET_SOURCE_STATEMENT_TIMEOUT` also recognised on the existing timeout marker); `--json` added to `metrics`, `state files`, `state show`, and `state chunks` (the last via an inline `serde_json::json!` composite — run header + per-chunk `tasks[]`, e2e-proven on a crash-hook checkpoint). **The `--json`-everywhere line item is DONE:** re-checking the tree, `reconcile` / `repair` / `validate` already shipped `--format json` / `--output`, so only those four inspect commands were actually missing it — now all have it. **Per-warning severity DONE** — re-scoping found a chokepoint (`analysis::collect_warnings` assembles the 5 operator-facing `check` warnings, NOT the 6 unrelated `Vec<String>` streams I first counted): a `Severity` (low/medium/high/blocking) + `Warning { severity, message }` are tagged at that one site, so `check --json` emits `warnings: [{severity, message}]` and the text path prefixes `[SEVERITY]` (the 5 check fns + their unit tests stay untouched). **`doctor --json` DONE** — `doctor()` is ~115 lines (not the 1015 the file is), refactored to collect each probe into a `DoctorCheck` Vec; `--json` serializes `{config_path, all_ok, checks[]}`, the text path is byte-for-byte unchanged (the `println!`s gated on `!json`). **Item 7 is complete.**

### 9.6.1 UX hardening backlog (v0.7.8 walk-through findings)

The fast-track + pilot blessed-path walk in the v0.7.8 session found three
P1-class bugs (already fixed and folded into item 9 above) plus a longer
list of P2/P3 friction worth addressing while polishing for v0.7.9 /
v0.8.0. Each line is one focused change; pick off in order or interleave
with verify-layer work as bandwidth allows.

**Fixed in v0.7.8 session** (evidence in [`src/preflight/{postgres,mysql,analysis}.rs`](src/preflight/), [`src/config/source.rs`](src/config/source.rs), [`src/pipeline/cli.rs`](src/pipeline/cli.rs), [`src/pipeline/chunked/{sequential,parallel}_checkpoint.rs`](src/pipeline/chunked/)):

- ✅ `rivet check` no longer reports "No index detected" for indexed `chunk_column` / `cursor_column` — catalog-based btree probe overrides the EXPLAIN-of-base-query heuristic; verdict thresholds relaxed so indexed > 10 M rows is ACCEPTABLE not DEGRADED.
- ✅ `WARN: source URL contains plaintext password` no longer fires when the user already chose `url_env:` / `url_file:` — only inline `url:` (the misconfig case) triggers it.
- ✅ `rivet state show` after chunked-only runs no longer says "No export state recorded yet" — distinguishes "never ran" from "ran chunked, look at metrics / state files" and prints the right next-step pointer.
- ✅ `summary.retries` now actually increments in chunked exports (sequential + parallel paths) — was silently stuck at 0, masking flaky-link runs that only worked because backoff covered for them. Visible in console summary card, `rivet metrics`, and `export_metrics.retries`.

**P2 — friction, not bugs** (tackle before v0.7.9 release if time permits):

- ✅ **`check` verdict pessimism vs actual run — FIXED.** `compute_verdict` now feeds the catalog `avg_row_bytes` + `parallel` into the bench-validated peak-RSS model (`tuning::memory::estimate_peak_rss_mb`, single-sourced with the scaffold) and downgrades UNSAFE→DEGRADED for an unindexed large scan whose predicted peak fits the 2 GB budget; it stays UNSAFE only when the width is unknown (e.g. MySQL) or the prediction breaches the budget. The 117 MB / 10 M-row case is now DEGRADED, not UNSAFE.
- ✅ **`destination is not retry-safe` WARN does NOT fire for local — DONE.** `src/destination/local.rs:119` reports `retry_safe: true` (temp-then-rename leaves nothing at the final key on failure), so the gate `!retry_safe && max_retries > 0` (`destination/mod.rs:214`) is false for `local` *and* `cloud`. The WARN fires only for `stdout` (`retry_safe: false`), whose stream genuinely can't be reverted — which is correct, not spam.
- ✅ **TLS warning now surfaces in `doctor`/`check`** (v0.7.8) — preflight calls `warn_if_tls_disabled` ([src/preflight/doctor.rs](src/preflight/doctor.rs), [src/preflight/mod.rs](src/preflight/mod.rs)), so the missing-`tls:` warning fires before a real extract, not only in `run`.
- ✅ **`rivet init --schema X` test-table filtering — DONE.** Repeatable `--include` / `--exclude '<glob>'` flags (`*`/`?` globs; `--exclude` wins over `--include`) — `TableFilter` + an in-tree `glob_match` (`src/init/mod.rs`), wired in `src/cli/dispatch.rs:474`, 4 passing unit tests (`table_filter_*`). Whole-schema init now drops `bench_*` / `tmp_*` / `*_temp` on demand.

**P3 — polish & doc clarity** (good v0.8.0 fodder):

- ✅ **`rivet init` explains *why* it picked a mode — DONE.** `src/init/yaml_scaffold.rs:333` renders `# {mode_rationale}` above `mode:`; `TableInfo::mode_rationale` (`init/mod.rs:85`) emits e.g. "auto: ~Nk rows ≥ 100K threshold and chunk column 'id' available. NOTE: chunked re-reads the whole table — for scheduled re-runs use `mode: incremental` on '<cursor>'".
- ✅ **`rivet journal` shows retry events — DONE.** `RunEvent::RetryAttempted` is journaled and the command prints `retries: N` (`src/pipeline/cli.rs:517`); `RunJournal::retries()` exposes the entries for post-mortem.
- ✅ **100 files per 10 M-row chunked export — FIXED.** `rivet init` now scales `chunk_size` by the row estimate (`TableInfo::suggest_chunk_size` → `src/init/yaml_scaffold.rs:340`); a 10 M-row export that used to render 100 files now lands at ~10. Operators override the rendered line for different geometry.
- ✅ **`status: skipped` now names the cursor** (v0.7.8) — shows `(no new rows since cursor '<col>')` ([src/pipeline/single.rs](src/pipeline/single.rs)) so the operator doesn't have to guess.
- ✅ **Doc note added: re-running `chunked` re-extracts everything** — [`docs/modes/chunked.md`](docs/modes/chunked.md) § "Clean re-runs are NOT idempotent" spells out that `--resume` only skips completed chunks after a crash.
- ✅ **Doc note added: `time_window` re-runs duplicate output.** `docs/modes/time-window.md` documents that rolling-window mode does not persist "this window already done", so frequent re-runs duplicate files.
- ✅ **Retry / I3 (Write Before Cursor) at-least-once dupe scenario — COVERED** (2026-06-29 re-check, verified live). `crash_after_file_write_leaves_file_but_no_manifest_or_cursor` (`tests/live/live_crash_recovery.rs`, with per-engine `mysql_…`/`mssql_…` siblings) injects `RIVET_TEST_PANIC_AT=after_file_write` (file durable on disk, manifest = 0, cursor = None), re-runs without the injection, and asserts the dupe end-to-end: **≥ 2 files** (orphaned pre-crash file + recovery file), **every source id present** (re-reads the Parquet, not the state DB — a missing id would be row LOSS), and **physical rows ≥ source** (the at-least-once surplus). Passed live on PostgreSQL + MySQL at re-check (MSSQL sibling present but env-blocked — `mssql` service not up on :1433). The original "SIGKILL-between-write-and-commit" wording is satisfied by the panic injection: for I3 the panic-vs-SIGKILL distinction is immaterial — neither writes the cursor, and the file is already durable (temp→rename) *before* that boundary, so the post-crash state and the dupe outcome are identical. The SIGKILL *commit-window* atomicity (no committed file mid-rename) is separately proven by OPT-6's `sigkill_in_commit_window_leaves_no_committed_file`. The "not yet covered" note was stale.
- ⏳ **Stale roadmap items inherited from earlier sessions:** "2–3 pilot tables repeated on a schedule" in §9.7 (organizational; optional automation K2) is now the only unchecked §9.7 item. *(Release checksums shipped v0.7.8; SBOM + signed-release attestations shipped v0.15.0 — both now checked in §9.7.)*

---

## 9.7 Definition of done — stable v0.5.x

- [x] Auth predictable and documented
- [x] `rivet check` actionable strategy and safety guidance
- [x] Execution semantics frozen and documented
- [x] Run summary + reconciliation (`--reconcile`)
- [x] Crash/recovery tested (matrix + E2E)
- [x] Local battle lab (MinIO + compose + E2E)
- [x] Docs for real scenarios (`docs/` canonical; `USER_GUIDE.md` deprecated, navigation consolidated)
- [x] Architecture stabilisation — Waves 1–3 complete (14 epics, invariant/recovery/compatibility tests, semantic release gates)
- [x] Plan/Apply workflow — sealed execution artifacts, ADR-0005
- [x] Parallel exports — `--parallel-exports` + `--parallel-export-processes` with live cards UI
- [x] Type safety layer — `--type-report`, TypePolicy, BigQuery compat, complex types (M1–M6)
- [x] **Type roundtrip proof (v0.7.8, since expanded)** — **PG / MySQL / MSSQL** × Parquet/CSV validated through DuckDB + ClickHouse + pyarrow + BigQuery **+ Snowflake** (all live-verified together); native Parquet `LogicalType::Uuid` / `LogicalType::Json`; `make test-types-validators`; per-tool fidelity benchmark in [`docs/bench/reports/`](docs/bench/reports/). Now **guarded by the always-on value checksum** — A==B byte-identical across every matrix type on all 3 engines (the permanent matrix-guard regression oracle that would have caught the PG-enum side-A gap before ship)
- [x] Schema drift policy hooks — `on_schema_drift: warn|continue|fail` (Epic 7)
- [x] Data shape drift detection — string/text width tracking (Epic 8)
- [ ] 2–3 pilot tables repeated on a schedule *(organizational; optional automation K2)*
- [x] Cross-platform release binaries **published** (v0.3.5: Linux x86_64/arm64, macOS arm64/Intel, Docker GHCR, Homebrew tap)
- [x] E2E matrix in CI
- [x] Published to crates.io (rivet-cli)
- [x] Security policy and sensitive-artifact guidance
- [x] Execution semantics and known non-guarantees documented
- [x] Reliability and compatibility matrices published
- [x] Pilot and production-readiness docs published
- [x] Release checksums *(v0.7.8 — `SHA256SUMS.txt` published per release; verify with `sha256sum -c`)*
- [x] Signed releases / attestations
- [x] SBOM

---

# 10. Engineering Optimization Backlog (v0.7.8 release + code audit)

This section captures findings from a code-level audit conducted against the
v0.7.8 release (tag, release notes, artifacts) and the live source tree. Unlike
§5–§9 (feature epics), these are **hardening / optimization releases**: each one
closes a gap between a documented promise and what the engine actually
guarantees, or removes a structural risk. They are ordered by
**impact ÷ uniqueness** — OPT-1 is foundational debt under the headline claim;
OPT-2 is the most differentiating new capability.

The audit also confirmed what is *already sound* and should not be re-litigated:
the I1–I8 invariant model + failure-point map (ADR-0001), SQLite `WAL` +
`busy_timeout` (`src/state/mod.rs:537-549`), the single-held-runtime async bridge
for OpenDAL destinations (`src/destination/gcs.rs:51-76`, no `block_on`-per-call),
and the two-engine separation with explicit revisit triggers (ADR-0010).

## 10.1 Findings summary

> **Validated against the source — see
> [`docs/planning/optimization-backlog-validation.md`](docs/planning/optimization-backlog-validation.md)**
> for per-item verdicts, file:line evidence, and implementation plans. A
> line-by-line pass corrected OPT-4 and OPT-5 substantially (a deterministic
> per-part content hash already exists; MySQL hard-refuses rather than silently
> degrading) and narrowed OPT-1/OPT-2. The validation doc is authoritative over
> the prose below.

| ID | Area | Priority | Status | One-line |
|---|---|---|---|---|
| OPT-1 | Memory safety | P2 | ✅ Done (core) | Per-value ceiling `RIVET_VALUE_TOO_LARGE` shipped (189d915, default 256 MB). Follow-ups: probe-batch warmup shrink, check-predictor feedback |
| OPT-2 | Adaptive concurrency | P1 | ✅ Done | Adaptive parallelism governor shipped (141bf33) — resizes worker count in `[min, parallel]` under source pressure (in-process engine). Follow-ups: subprocess engine (after OPT-6), richer `rivet-mcp` signals |
| OPT-3 | Type fidelity | P1 | ✅ Done | proptest type round-trip shipped (`tests/type_roundtrip/property.rs`, `make test-types-property`); covers random *values* over a fixed MySQL schema — random *schemas* + Postgres remain documented follow-ups |
| OPT-4 | MySQL parity | P1 | ✅ Done | MySQL keyset (seek) pagination shipped (40433a0) — single-column unique key, sequential, index range scan (documented in code comments; no EXPLAIN assertion in the suite). Follow-ups: composite keys, parallel keyset, resume |
| OPT-5 | Dedup ergonomics | P2 | ✅ Partial | Deterministic per-part `content_fingerprint` exposed in every manifest (`manifest.rs:198`) + documented as the dedup key; remaining gap = a dedicated SIGKILL-between-write-and-commit dedup-collision test |
| OPT-6 | Engine debt | P2 | ✅ Done | Crash-matrix symmetry pass on `feat/opt6-crash-matrix`: SIGTERM/SIGINT child reaper (`parallel_children::child_reaper`), SIGKILL-mid-write proof (temp+rename), subprocess panic recovery across all 4 write-cycle boundaries. See `dev/CRASH_MATRIX.md`. Residuals: object-store `FinalizeOnClose`, single-child external-signal accounting |
| OPT-7 | Doc/roadmap drift | P1 | ✅ Done | Checksums documented as shipped (SECURITY.md + README + §5.1/§9.7); 3 §9.6.1 items struck. *Of those 3: ALL in fact shipped — `init chunk_size scaling` (`yaml_scaffold.rs:340`), the `init` mode-selection comment (`yaml_scaffold.rs:333` + `mode_rationale`), and the `local-retry WARN` (suppressed for local/cloud by `local.rs:119` `retry_safe: true`; fires only for stdout, correctly). OPT-7's "not shipped" claim was stale on all three.* |
| OPT-8 | Test build/infra | P2 | ✅ Done | 109 integration-test binaries → 13 (`tests/{offline,live}_suite.rs` `#[path]`-consolidation) + cargo-nextest per-test isolation + split pre-push hook → test-LINK build 229s→29s (8×). `tests/suite_completeness.rs` guard closes the silent-skip footgun; nextest requirement documented; cargo-chef Dockerfile caches the dep layer. Rejected after measurement: sccache (incremental → 0% Rust hits), lld (macOS ld-prime already fast), dep-dedup (transitive majors), feature-trim (all live) |

---

## OPT-1 — Memory-bound hardening (residual gaps on an existing adaptive cap)

**Priority: P2** — *corrected after reading the source.* An earlier draft of this
item claimed there was "no hard byte budget, only reactive sampling." **That was
wrong.** A row-width-adaptive byte-budget cap already exists on both engines and
genuinely backs the headline claim. This item is now narrow edge-case hardening,
not foundational debt.

**What already exists (do not re-build):**
- **MySQL** (`src/source/mysql/mod.rs:385-440`): a 500-row `PROBE_BATCH_SIZE`
  first batch measures real Arrow bytes/row (`SourceTuning::batch_memory_bytes`),
  then caps `effective_bs` so each flush fits `tuning.batch_size_memory_mb`
  (default `MYSQL_BATCH_TARGET_MB_DEFAULT = 64` MB), never exceeding the
  configured `batch_size`.
- **PostgreSQL** (`src/source/postgres/mod.rs:380-440`): a 500-row
  `PROBE_FETCH_SIZE` first FETCH, then `FETCH N` is capped under
  `work_mem × 0.7` (`pg_fetch_work_mem_bytes`) so the server-side cursor never
  spills to `pgsql_tmp/`. The in-code comment explicitly anticipates the
  "single huge FETCH triggers spill" case — that is *why* the probe exists.
- Plus a reactive RSS sampler (`src/resource.rs`) and `memory_threshold_mb`
  pause-gate (`src/pipeline/chunked/exec.rs:66,269,271`) as a backstop.

**Residual gaps (the actual scope of this item):**
1. **Probe-batch warmup is uncapped.** The first 500 rows are buffered before
   bytes/row is known. The code assumes "500 × ~4 KB ≈ 2 MB"; 500 genuinely
   huge rows (e.g. 500 × 1 MB) overshoot before the cap is computed.
2. **Single-outlier value.** The cap is based on *average* bytes/row in a batch.
   One 200 MB JSONB/`bytea` cell among normal rows still lands in a single
   batch; an average-based cap doesn't bound a lone giant value. There is no
   hard per-value ceiling with a typed error.
3. **Soft target × threads.** The cap is a per-batch *target* (~64 MB), and with
   `parallel` threads peak is ×N. It is not a hard process-level ceiling.

**Proposed change.**
- A hard per-value size limit with a typed error (`RIVET_VALUE_TOO_LARGE`) so a
  single fat cell fails cleanly instead of risking OOM.
- Optionally shrink the probe batch when `avg_row_bytes` (already estimated in
  preflight) is large, so warmup respects the budget too.
- Feed the measured row width back into the `check` predictor (tightens the
  pessimistic UNSAFE/DEGRADED verdict, §9.6.1).

**Definition of done.** A fixture with a deliberately fat value column
(e.g. 256 MB JSONB rows) either exports under the configured budget or fails
with `RIVET_VALUE_TOO_LARGE` — never OOMs; regression test in the soak matrix.

---

## OPT-2 — Adaptive concurrency governor (extend existing adaptation to parallelism)

**Priority: P1** — the differentiating item. *Scope corrected after reading the
source:* batch-size adaptation under source pressure **already exists** — this
item is the next step, not a greenfield build.

**What already exists (do not re-build):** `tuning.adaptive` already samples a
source-side pressure proxy each `ADAPTIVE_SAMPLE_INTERVAL` batches and resizes
the batch via `next_adaptive_batch_size` — PG via `pg_sample_checkpoints_req`
(`src/source/postgres/mod.rs`), MySQL via `mysql_sample_innodb_log_waits`
(`src/source/mysql/mod.rs:444-464`). So the control loop exists; it adjusts
*batch size* off *one* proxy signal.

**The actual gap (two dimensions the current loop doesn't cover):**
1. **It governs batch size, not parallelism / connection count.**
   The default `parallel` is **1** (`src/config/export.rs:319`, conservative),
   but each chunk worker opens its *own* source connection inside `s.spawn`
   (`src/pipeline/chunked/exec.rs:305`, gated by `Semaphore::new(parallel)` at
   `exec.rs:246`) — `Source: Send` not `Sync` (ADR-0011). So a user who dials
   `parallel: N` holds up to N connections against exactly the fragile,
   low-`max_connections` source the tool protects. Preflight only *warns*
   (`src/preflight/analysis.rs:164-182`); the count is static at runtime — the
   governor never backs it off. Extend the loop to adjust the semaphore permit
   count (and `throttle_ms`) within `[min, max]`, not just batch size.
2. **It reads one proxy signal, not the richer `rivet-mcp` set.** `rivet-mcp`
   already exposes `pg_stat_activity` (active / lock-wait / idle-in-txn),
   pgBouncer saturation, and MySQL `SHOW PROCESSLIST`. Feed those into the same
   governor so back-off reacts to real contention (lock waits, replication lag,
   active-query count), not just checkpoint/log-wait pressure. One pressure
   model shared between `rivet-mcp` and the run loop.

**Proposed change.**
- Promote the existing adaptive loop from "resize batch" to a governor that also
  resizes the active permit count + throttle.
- Reuse the `rivet-mcp` read-only query surface as the pressure source.
- Surface governor decisions in the run journal ("backed off parallel 16→8 at
  T+45s: lock_waits=12").

**Definition of done.** Under a synthetic concurrent-OLTP load fixture, an
adaptive run keeps a source-side pressure metric below a threshold that a
static `parallel=N` run breaches; decisions visible in `rivet journal`.

---

## OPT-3 — Property-based type round-trip (proof by construction)

**Priority: P1** — converts type rigor from "many tests" to "provable".

**Problem.** The four-reader round-trip matrix (`tests/type_roundtrip/`) is
excellent but proves only the **enumerated** types. `UNSIGNED BIGINT → Decimal128`
was found *by example* in 0.7.8 — a symptom that integer-width / precision
overflow is a *class* found one funeral at a time. The long tail
(`numeric(1000,…)`, arrays-of-composite, domains, ranges, `citext`/`hstore`/
PostGIS, MySQL `ZEROFILL`/`BIT`/`SET` edge cases) is where silent corruption
lives.

**Proposed change.** A property-based harness that generates random schemas +
values for the supported type universe, exports, reads back through ≥1
independent reader, and asserts value + metadata equality. Shrink failing cases
to a minimal reproducer.

**Definition of done.** `make test-types-property` runs N generated schemas in CI
(PR tier: small N; nightly: large N) and any discovered mismatch fails the gate
with a minimized fixture checked into `tests/type_roundtrip/`.

---

## OPT-4 — MySQL safety parity (or explicit degraded mode)

**Priority: P1** — the headline "source-safe" property is asymmetric across engines.

**Problem.** PostgreSQL gets `DECLARE CURSOR` + `work_mem`-aware `FETCH N`
(0.19 s longest query). MySQL has no widely-supported server-side cursor in the
current client stack, so safety rests on PK-range chunking (9 s), which *requires*
a clean monotonic numeric PK. On a MySQL table with composite / UUID / no PK the
"don't hold a long query, don't OOM" guarantee silently degrades to a buffered
read or an expensive global sort. The weaker engine is also the more common
"fragile shared prod" case in the SMB segment.

**Proposed change.**
- Either: pursue a streaming read shape on MySQL that bounds memory without a
  clean PK (e.g. keyset pagination on the best available index).
- Or (cheaper first step): make the unsafe shape an explicit `opt-in` —
  `rivet check` / `rivet run` refuses the buffered/sort path on MySQL unless the
  operator acknowledges the degraded guarantee, instead of falling into it.

**Definition of done.** A MySQL table with no usable PK either exports under a
bounded memory budget, or fails preflight with a clear degraded-mode
acknowledgement requirement — never silently buffers the whole table.

---

## OPT-5 — Deterministic dedup token in the manifest

**Priority: P2** — makes "boring" boring all the way downstream.

**Problem.** I3 (Write-Before-Cursor) is correct, but a crash between write and
cursor advance produces a duplicate file, and the manifest carries no run-scoped
idempotency key / content hash a downstream `MERGE` can dedup on deterministically.
"Boring extraction" that needs a hand-written dedup recipe
(`recipes/idempotent-warehouse-load.md`) isn't fully boring.

**Proposed change.** Stamp each manifest entry (and optionally the filename) with
a deterministic content hash + `(chunk_id, cursor_range)` so downstream dedup is
mechanical, not convention-based. Pairs naturally with the existing xxHash3 used
in quality gates.

**Definition of done.** Two runs that re-extract the same window after a simulated
SIGKILL produce files whose manifest dedup keys collide, so a consumer can drop
the duplicate by key alone. Closes the §9.6.1 open *"I3 at-least-once dupe
scenario not covered by tests"* with an end-to-end SIGKILL-between-write-and-commit
test.

---

## OPT-6 — Two-engine maintenance debt + crash-matrix symmetry

**Priority: P2** — accepted debt (ADR-0010), but it compounds.

**Problem.** ADR-0010 honestly lists the cost: two retry loops, two progress UIs,
two error-aggregation paths. Every cross-cutting feature (graceful shutdown,
tracing, OPT-2 governor, OPT-5 dedup token) must be built twice or it drifts. The
highest-risk corner is SIGTERM / parquet-footer finalization in the subprocess
engine — the classic "killed mid-run leaves a corrupt parquet" regression site.

**Proposed change.**
- Audit `dev/CRASH_MATRIX.md` for **symmetry**: every crash scenario should run
  against both the in-process thread engine (`src/pipeline/chunked/exec.rs`) and
  the subprocess fan-out engine (`src/pipeline/parallel_children.rs`).
- Add the OPT-2/OPT-5 cross-cutting features to a shared layer where possible so
  they are not implemented twice.

**Definition of done.** The crash matrix asserts no corrupt/footerless output
file under SIGTERM/SIGKILL for *both* engines, and the gap (if any) is documented
as a known non-guarantee.

**Status (2026-06-14): Done** — `feat/opt6-crash-matrix`.

- Audit corrected: the subprocess engine had *partial* panic coverage, not zero
  (the "zero crash tests" claim above was inaccurate). `dev/CRASH_MATRIX.md` now
  carries the verified two-engine coverage matrix + the panic-vs-signal rationale.
- **SIGTERM/SIGINT reaper** (`parallel_children::child_reaper`): a targeted kill
  of the parent no longer orphans children holding source connections. RED→GREEN:
  `parallel_processes_sigterm_reaps_children_no_orphans`.
- **SIGKILL-mid-commit proof**: `sigkill_in_commit_window_leaves_no_committed_file`
  shows temp+rename (not `Drop`) carries the no-corrupt-output guarantee under a
  non-unwinding signal.
- **Subprocess panic recovery** across all 4 write-cycle boundaries:
  `parallel_processes_recovers_from_child_crash_at_each_boundary`.

Residual non-guarantees (documented in the crash matrix, not in this slice):
object-store `FinalizeOnClose` under SIGKILL (needs a live cloud target); a
single *child* killed by an external signal (parent ranking is unit-tested).

---

## OPT-7 — Doc / roadmap drift sync (cheap, do first)

**Priority: P1** — near-zero effort, removes a credibility leak, and prevents
re-planning work that is already done.

**Findings.**
- **Checksums already ship, docs say otherwise.** `release.yml:153` generates
  `SHA256SUMS.txt`, and the v0.7.8 release publishes it as an asset — yet
  `SECURITY.md:165-169` still tells users *"until checksums and signatures are
  published, verify by rebuilding from source… `git checkout v0.6.0`"* (also a
  stale tag), and §5.1 (line ~579) marks "Release checksums" as `⏳ Open / P1`.
  The v0.7.8 release notes don't mention the checksums at all. → Add verification
  instructions to README/SECURITY, mention checksums in release notes, flip
  §5.1 + §9.7 to done.
- **Several §9.6.1 ⏳ items were already fixed in 0.7.8 but not struck:** retry-safe
  WARN demoted to DEBUG for local (line ~996), TLS warning now fires from
  `doctor`/`check` (~997), `rivet init` explains its mode choice inline (~1002),
  `chunk_size` scaled to row estimate (~1004), `status: skipped (no new rows since
  cursor X)` (~1005). The 0.7.8 release notes' `polish(ux)` bullet is the evidence.
  → Strike these so the backlog stops overstating remaining work.

**Definition of done.** §5.1, §9.6.1, and §9.7 reflect the actual shipped state of
v0.7.8; README/SECURITY document checksum verification against the published
`SHA256SUMS.txt`.

**Status (2026-06-29): Done** — applied and verified. §5.1 marks Release checksums
`✅ Done` (line ~808); the three §9.6.1 items are struck `✅` (retry-safe local WARN
via `local.rs:119 retry_safe: true`, TLS warning from `doctor`/`check`, `init` mode
rationale); §9.7 checks checksums + SBOM; OPT-7 is `✅ Done` in the §5 table.
`SECURITY.md:165-169` + `README.md:281-285` document `SHA256SUMS.txt` (+ cosign)
verification — the stale "until checksums are published" wording is gone. The
*Findings* above are the historical before-state, now resolved.

---

## OPT-8 — Test build/infra consolidation (link-count + isolation)

**Priority: P2** — pure developer-velocity / CI-cost debt; no product-surface change.

**Problem.** cargo builds one test binary per `tests/*.rs` file, and each links the whole crate. The repo
had ~109 such files, so `cargo test --tests` / the pre-push hook / CI linked rivet 109 times — the link
phase, not compilation, dominated the offline test build (~229s warm). The only thing that link cost
bought was per-test isolation *as a side effect* (one binary = one process) — an implicit property
nothing named or could reason about.

**Change (shipped on `feat/verify-and-ux`).**
- **Consolidation 109 → 13 binaries.** Self-contained offline tests moved under `tests/offline/`, the live
  (`#[ignore]`, docker) suites under `tests/live/`; two entry files (`tests/offline_suite.rs`,
  `tests/live_suite.rs`) `#[path]`-include them so each set LINKS ONCE. A few targets named individually
  by CI (`type_roundtrip`, `live_differential`, `live_type_golden`) stay separate.
- **cargo-nextest** (`.config/nextest.toml`) for process-per-test isolation, so a crash / SIGKILL /
  global-state test can't poison its siblings inside a consolidated binary — restoring *explicitly* the
  isolation the old layout gave implicitly.
- **Split pre-push hook**: unit tests threaded (`cargo test --lib --bins`), only the integration suites
  under nextest (`-E 'kind(test)'`) — avoids ~1700 process spawns for the pure unit tests.
- **Both footguns the consolidation introduced, closed:** silent-skip (a file added to the subdir but not
  registered in the entry runs as nothing) → `tests/suite_completeness.rs` guard (auto-discovered; proven
  RED→GREEN); runner-dependent isolation (`cargo test` ≠ nextest) → documented in both entry headers, the
  hook fallback, and a README "Running tests" section.
- **cargo-chef Dockerfile**: the dependency build is its own layer keyed on `recipe.json`, so a
  source-only change recompiles just rivet, not the ~500 dep crates.

**Measured (honest).** Test-LINK build (warm lib): **229s → 29s, 8×, −200s** — link count was the whole
cost. Hook run phase **~67s → ~32s** (units threaded vs process-per-test). This is a *test-build* win, NOT
a cold-build one: a from-scratch dep compile is ~60s (arrow/parquet/tokio), not the once-assumed "30
minutes" — that figure never matched a real build.

**Rejected after measurement (not assumed):**
- **sccache** — committed then reverted: with dev `incremental=true` it scores **0% Rust cache hits**
  (sccache skips incremental Rust); `CARGO_INCREMENTAL=0` gives only 60s→35s (1.7×) on an already-60s
  build while hurting the warm edit-loop. A CI-only `incremental=0` setup remains optional.
- **lld linker** — macOS already ships the fast `ld-prime` (Xcode 17, ld v1230); lld doesn't beat it here.
- **dependency dedup** — the 57 duplicate-version crates are transitive major splits (`base64 0.21 ←
  tiberius`, `rand 0.8 ← sqlx`); not unifiable.
- **feature trim** — the parquet codecs (zstd/lz4/gzip) and the arrow canonical extension types
  (`src/types/mapping.rs:24`) are all live.

**Definition of done.** Offline suite + hook green under nextest at 13 binaries; the `suite_completeness`
guard enforces registration; the nextest requirement is documented where a contributor meets it. ✅

---

## 10.2 Suggested execution order

By impact ÷ effort (revised after source audit — the memory cap already exists,
so OPT-1 drops from the top spot):

1. **OPT-7** (doc/roadmap sync) — hours, removes a trust leak, unblocks accurate planning.
2. **OPT-3** (property-based types) — most autonomous; immediately strengthens the trust story.
3. **OPT-2** (adaptive governor → parallelism) — the differentiating bet; builds on the existing adaptive loop.
4. **OPT-4** (MySQL parity / explicit degrade) — closes the engine asymmetry.
5. **OPT-1** (memory residual: per-value cap + probe warmup) — edge-case hardening on an existing cap.
6. **OPT-5 / OPT-6** — ergonomics + debt paydown, interleave as bandwidth allows.
