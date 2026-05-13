# Rivet Consolidated Pain-Driven Roadmap

This document is the **single source of truth** for Rivet planning. It consolidates:

- the current product direction and validated user pains
- the **numbered** strategic epics (§5, P0–P3)
- **execution status**: lettered epics (A–O, M), phase completion, task ✅/⏳, and Definition of Done (§9)

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
**Status:** ⏳ OPEN — SQLite only; external/Postgres backend deferred; domain store split complete (stabilisation Epic 6)  
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
**Pain coverage:** Pain F

### Goal
Make trying and adopting Rivet easier.

### Deliverables
- GitHub release binaries
- Homebrew tap support
- Docker image
- improved local quickstart

### Why this matters
Install friction is real, but only worth optimizing after product direction is clear.

---

## Epic 12 — Deployment Modes Guidance
**Priority:** P2  
**Pain coverage:** Pain F

### Goal
Help users choose the right deployment model.

### Deliverables
- docs for local/dev vs durable/prod modes
- Docker/Compose examples
- k8s/Helm guidance
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
**Status:** ✅ Partial — type system, type report, strict mode, BigQuery compat layer, complex types (M1–M6 complete); load path (Parquet → BQ) = future  
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
- ⏳ direct load path (write Parquet files directly into BigQuery) = future

### Why this matters
Useful, but this opens a new product layer and should follow extraction trust.

---

## Epic 15 — WAL/Binlog CDC to Files
**Priority:** P3  
**Pain coverage:** Pain B, Pain G

### Goal
Extend Rivet from snapshot extraction into logical change extraction after the trust layer is mature.

### Deliverables
- logical change extraction
- insert/update/delete event model
- durable LSN/binlog position state
- changelog file format
- snapshot + changelog workflow

### Why this matters
A natural evolution, but only after auditability, recovery, and data quality are solid.

---

## Epic 16 — Automatic Parallelism
**Priority:** P3  
**Pain coverage:** Pain A, Pain D

### Goal
Move from warning-based guidance to controlled automatic parallelism selection.

### Deliverables
- recommended parallelism first
- later auto-selected parallelism bounded by safety rules

### Why this matters
Parallelism is valuable, but automation should come after better planner guidance and safety semantics.

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

## Phase 4 — Expand carefully
13. Epic 13 — SSH / Jump Host Access
14. Epic 14 — Narrow Warehouse Load Layer
15. Epic 15 — WAL/Binlog CDC to Files
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
| Epic 4 — Durable state backend | *(none yet)* | Today: SQLite only. Postgres/external state = future |
| Epic 5 — Batch/fetch/write control | **M**, tuning | `batch_size`, `max_file_size`, streaming; not full split of row-group vs fetch |
| Epic 6 — Partition intelligence | **B**, modes | `chunked`, `time_window`, dense surrogate guidance; `chunk_by_days` date-native partitioning ✅ |
| Epic 7 — Schema drift | **D** (tracking) | `on_schema_drift: warn\|continue\|fail` ✅ |
| Epic 8 — Shape drift | **N2** (Phase 3 table) | Not started |
| Epic 9 — Data contracts | `quality:` YAML | Row bounds, null ratio, uniqueness; chunked aggregate rules |
| Epic 10 — Config UX | **E**, **J**, misplaced-field validation | Docs + errors |
| Epic 11 — Packaging | **L** | Release workflow, binaries; Homebrew/Docker = open |
| Epic 12 — Deployment | docs, `docker-compose` | k8s/Helm, durable state = open |
| Epic 13 — SSH | **O** | External tunnel docs first; native SSH = future |
| Epic 14 — Warehouse load | `src/types/`, M1–M6 | Type system + type report + BQ compat ✅; direct load path = future |
| Epic 15 — CDC | **N** | WAL/binlog = future |
| Epic 16 — Auto-parallel | *(none)* | Auto-parallelism = future |

**Auth and connectivity (lettered A)** underpin all runs and map across Epics 1–2 and §7 (niche).

---

## 9.2 Current state (post-v0.3.5)

Rivet core is **feature-complete for stable extraction with type safety**. All Wave 1–3 stabilisation epics are shipped; Epic 14 type safety layer (M1–M6) complete; observability layer enhanced with persistent RunJournal.

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

- `RunJournal` domain model: 13 `RunEvent` variants (plan snapshot, files, retries, chunks, quality issues, schema changes, outcome)
- **Persistent** — serialized to `run_journal` SQLite table (migration v7) at end of every run; `store_journal` / `load_journal` / `recent_journals` APIs
- **`rivet journal --config <file> --export <name>`** — inspect last N runs: per-run header (status, duration, run_id), files/rows/bytes summary, retries, quality issues, schema changes, error first line
- `--run-id <id>` flag to inspect a specific run

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
| QA live-test harness | `tests/common/mod.rs` + **~55** ignored live tests across **11** `tests/live_*.rs` binaries mapped in [docs/reference/testing.md](docs/reference/testing.md); full offline suite (**~1345** tests) runs on every PR via `cargo test`. Includes **golden type round-trip** ([`live_type_golden.rs`](tests/live_type_golden.rs)). |

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
| G5 | ✅ | P0 | E2E matrix in CI — `ci.yml::e2e` runs both `dev/e2e/run_e2e.sh` **and** `cargo test --release -- --ignored` (**~55** ignored live tests across **11** `tests/live_*.rs` binaries, including **Trust golden** parity on Postgres + MySQL in `live_type_golden.rs`). See [docs/reference/testing.md](docs/reference/testing.md). |

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
| O1–O10 | P3 — CDC, Iceberg/Delta, multi-source, encryption, Prometheus, plugins, UI, sources, Flight, serverless |

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

**Remaining open items (P1 first):**

1. ✅ **Epic 7 schema drift policy** — `on_schema_drift: warn|continue|fail` YAML hook shipped.
2. ✅ **Epic 8 data shape drift** — `export_shape` SQLite table; `shape_drift_warn_factor` YAML config; warns on `N×` growth.
3. ✅ **F5 + I5** — reconcile/validate tradeoffs (cli.md); capacity/memory planning (tuning.md).
4. ✅ **I2** — `cargo bench` + `dev/scripts/bench.sh` save/compare harness; column_scan + shape_tracking groups.
5. **Epic 4 (§5)** — external/durable state backend when pilots need multi-replica or stateless workers.

---

## 9.7 Definition of done — stable v0.3.x

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
- [x] Schema drift policy hooks — `on_schema_drift: warn|continue|fail` (Epic 7)
- [x] Data shape drift detection — string/text width tracking (Epic 8)
- [ ] 2–3 pilot tables repeated on a schedule *(organizational; optional automation K2)*
- [x] Cross-platform release binaries **published** (v0.3.5: Linux x86_64/arm64, macOS arm64/Intel, Docker GHCR, Homebrew tap)
- [x] E2E matrix in CI
- [x] Published to crates.io (rivet-cli)
