# Rivet Roadmap and Task Breakdown

## Goal

Shift the next phase from “building the extractor core” to **making Rivet predictable, auditable, and ready for real pilot usage**.

This roadmap is based on the current Rivet state:

- PostgreSQL and MySQL streaming extraction
- Parquet and CSV output
- Local, S3, and GCS destinations
- full / incremental / chunked / time_window modes
- preflight check
- SQLite state
- metrics history
- schema tracking
- retry with reconnect
- validation
- safe / balanced / fast tuning profiles

---

## Phase 1 — 1 Week

## Milestone W1: Pilot Alpha Stabilization

### Expected outcome

By the end of the week, Rivet should have:

- finished auth and credentials flows
- stronger preflight and planning output
- frozen execution semantics
- clear run/export summary
- scenario-based docs for first pilot users

---

## Epic A — Auth and Connectivity

### Objective

Finish authentication and credential flows so Rivet can be used predictably in real environments.

#### Tasks

- **A1. Credential precedence matrix**
  - Define source priority order for credentials:
    - config
    - environment variables
    - ADC
    - file-based credentials
  - **Acceptance criteria**
    - One documented precedence order for DB and cloud credentials
    - No ambiguous fallback behavior
- **A2. GCS ADC support**
  - Implement Google Cloud ADC flow
  - **Acceptance criteria**
    - Works with `gcloud auth application-default login`
    - No JSON key file required in ADC mode
- **A3. GCS explicit JSON credentials**
  - Support explicit service account file path
  - **Acceptance criteria**
    - User can provide JSON credentials file in config or env
    - Clear error if file missing or invalid
- **A4. DB credential normalization**
  - Unify DB auth model for PostgreSQL/MySQL
  - Support URL and field-based config
  - **Acceptance criteria**
    - Both URL and structured config supported
    - Errors clearly show missing/invalid fields
- **A5. Auth diagnostics command**
  - Extend `rivet check` or add `rivet doctor`
  - **Acceptance criteria**
    - Can verify source auth
    - Can verify destination auth
    - Failure reason is explicit and non-ambiguous

---

## Epic B — Preflight and Planner 2.0

### Objective

Turn `rivet check` into a real decision tool, not just an EXPLAIN wrapper.

#### Tasks

- **B1. Selected strategy output**
  - Show which extraction strategy Rivet will use
  - Examples:
    - `incremental_updated_at`
    - `append_id`
    - `chunked_range`
    - `time_window`
  - **Acceptance criteria**
    - Each export shows selected strategy in check output
- **B2. Profile recommendation**
  - Recommend `safe`, `balanced`, or `fast`
  - **Acceptance criteria**
    - Check output includes recommended tuning profile with reason
- **B3. Sparse range warning**
  - Detect likely sparse-key chunking issues
  - **Acceptance criteria**
    - Warning shown when `max-min` is disproportionately larger than estimated row count
- **B4. Dense surrogate / sort cost warning**
  - Warn when dense chunk workaround implies sort or index-ordered scan cost
  - **Acceptance criteria**
    - Check output explains tradeoff, not just recommendation
- **B5. Parallel safety hints**
  - Warn when parallel mode is risky for wide rows or high memory pressure
  - **Acceptance criteria**
    - Check output includes memory/concurrency caution where applicable
- **B6. Better verdict suggestions**
  - Improve guidance for `DEGRADED` and `UNSAFE`
  - **Acceptance criteria**
    - Each degraded verdict includes 1–3 actionable next steps

---

## Epic C — Execution Semantics Freeze

### Objective

Clearly define what success means, when state is updated, and what guarantees Rivet provides.

#### Tasks

- **C1. Export lifecycle spec**
  - Describe:
    - query start
    - batch read
    - file write
    - upload
    - validation
    - state update
    - run finalize
  - **Acceptance criteria**
    - Lifecycle documented in one canonical place
- **C2. State update point freeze**
  - Decide and document when cursor/state moves forward
  - **Acceptance criteria**
    - Code and docs match
    - No ambiguity around successful export checkpoint
- **C3. Duplicate semantics**
  - Document where duplicates may appear
  - **Acceptance criteria**
    - Overlap/time_window duplicate behavior clearly documented
    - No implied exactly-once guarantee
- **C4. Retry semantics**
  - Document retryable vs permanent errors and reconnect behavior
  - **Acceptance criteria**
    - User can understand when Rivet retries and when it fails fast
- **C5. Validation semantics**
  - Clarify what `--validate` proves and what it does not prove
  - **Acceptance criteria**
    - Row-count validation clearly separated from full correctness guarantees

---

## Epic D — Observability and Run Summary

### Objective

Expose a clear operational summary for every run.

#### Tasks

- **D1. Run summary schema**
  - Define fields:
    - export name
    - rows written
    - files produced
    - bytes written
    - duration
    - peak RSS
    - retries count
    - validation result
    - schema changed yes/no
    - final verdict
  - **Acceptance criteria**
    - Single agreed summary schema exists
- **D2. End-of-run summary output**
  - Print one human-readable summary per export run
  - **Acceptance criteria**
    - Summary visible in CLI/logs after each run
- **D3. Manifest-style accounting**
  - Store export/file accounting details per run
  - **Acceptance criteria**
    - Possible to inspect what files were produced by a given run
- **D4. Metrics-summary alignment**
  - Align end-of-run summary with `rivet metrics`
  - **Acceptance criteria**
    - Same run identifiers and totals line up across both views

---

## Epic E — Documentation Rewrite for Real Scenarios

### Objective

Rewrite docs around real usage scenarios instead of just feature lists.

#### Tasks

- **E1. README repositioning**
  - Refocus on:
    - lightweight
    - source-safe
    - predictable
    - extract-only
  - **Acceptance criteria**
    - README opening explains what Rivet is and is not
- **E2. Choosing a mode guide**
  - Explain when to use:
    - full
    - incremental
    - chunked
    - time_window
  - **Acceptance criteria**
    - One dedicated doc with practical decision rules
- **E3. Choosing a profile guide**
  - Explain when to use:
    - safe
    - balanced
    - fast
  - **Acceptance criteria**
    - Clear production vs replica vs dedicated-source examples
- **E4. Auth guide**
  - Cover:
    - GCS ADC
    - GCS JSON
    - DB creds
    - env vars
  - **Acceptance criteria**
    - New user can configure auth without reading code
- **E5. Guarantees and limitations**
  - Document:
    - extract-only scope
    - no CDC
    - no load/merge
    - duplicate semantics
    - no exactly-once promise
  - **Acceptance criteria**
    - One explicit document for guarantees and non-goals

---

## Epic M — Output & CLI Improvements

### Objective

Improve output format flexibility, CLI ergonomics, and pipeline integration. Informed by competitive analysis of odbc2parquet and similar tools.

#### P0 — Quick wins (this week)

- **M1. Configurable Parquet compression**
  - Replace hardcoded Snappy with configurable compression
  - Default to **zstd** (better ratio at comparable decompression speed)
  - Config field: `compression: zstd | snappy | gzip | lz4 | none`
  - Optional `compression_level` for zstd/gzip/brotli
  - **Acceptance criteria**
    - Zstd is the new default for Parquet output
    - Users can override compression per export
    - Existing Snappy outputs continue to be readable
- **M2. Skip empty exports**
  - Add `skip_empty: true` config option per export
  - When query returns 0 rows, do not create output file
  - Incremental mode with no new data is the primary use case
  - **Acceptance criteria**
    - No file created when skip_empty is true and 0 rows returned
    - State is not advanced on empty result
    - Run summary shows "skipped (0 rows)" instead of error or empty file

#### P1 — High-value improvements (this week or next)

- **M3. File size splitting for full/incremental modes**
  - Add `max_file_size` config option (e.g. `max_file_size: 512MB`)
  - Split output into multiple files when threshold reached
  - Files named `{export}_{timestamp}_part{N}.{ext}`
  - Currently only chunked mode produces multiple files
  - **Acceptance criteria**
    - Full and incremental exports can produce multiple bounded-size files
    - Downstream tools (Spark, Athena, DuckDB) work well with split output
    - Default behavior unchanged (single file) when option not set
- **M4. Memory-based batch sizing**
  - Add `batch_size_memory` tuning parameter (e.g. `batch_size_memory: 256MB`)
  - Calculate max rows per batch based on estimated row size
  - If both `batch_size` and `batch_size_memory` set, use the smaller limit
  - **Acceptance criteria**
    - Memory-based limit produces predictable RSS regardless of row width
    - Works alongside existing row-based batch_size
    - Logged in run summary: effective batch size used
- **M5. Shell completions**
  - Generate completions for bash, zsh, fish, powershell via `clap_complete`
  - New subcommand: `rivet completions <shell>`
  - **Acceptance criteria**
    - Completions generated for all four shells
    - Installation instructions in README

#### P2 — Pipeline integration (Phase 2)

- **M6. Stdout destination**
  - Support `destination: stdout` or `--stdout` flag for ad-hoc use
  - Enables pipe workflows: `rivet run ... --stdout | duckdb`
  - Single-export only (multi-export to stdout is ambiguous)
  - **Acceptance criteria**
    - Parquet/CSV written to stdout when configured
    - Works with shell pipes
    - Error/log output goes to stderr (already true)
- **M7. Parameterized queries**
  - Support environment variable substitution in queries: `${ENV_VAR}`
  - Support CLI parameters: `--param start_date=2024-01-01`
  - Enables dynamic exports without editing YAML
  - **Acceptance criteria**
    - Env vars expanded in query and query_file content
    - CLI params override env vars
    - Missing required param produces clear error

#### P3 — Advanced (later)

- **M8. Per-column Parquet encoding**
  - Allow column-specific encoding hints in config
  - Encodings: PLAIN, RLE, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY
  - Config format: `column_encodings: { description: delta_length_byte_array }`
  - **Acceptance criteria**
    - Column encoding applied when specified
    - Invalid encoding names produce clear error
    - Default encoding unchanged when not specified

---

## Suggested Week-1 Execution Order

1. M1 (zstd compression) + M2 (skip empty) — quick wins, immediate value
2. A1 → A5
3. B1 → B6
4. C1 → C5
5. D1 → D4
6. E1 → E5
7. M5 (shell completions) — low effort, nice polish

---

## Phase 2 — 1 Month

## Milestone M1: Pilot Readiness and Battle Testing

### Expected outcome

By the end of the month, Rivet should be ready for 2–3 real pilot use cases with clear operational expectations.

---

## Epic F — Auditability and Correctness Confidence

### Objective

Improve confidence that exported data matches the intended source slice.

#### Tasks

- **F1. Export audit model**
  - Define what Rivet can reconcile:
    - rows read
    - rows written
    - rows validated
    - expected chunk/window count
  - **Acceptance criteria**
    - Audit model documented and reflected in code paths
- **F2. Bounded source count verification**
  - Add optional `COUNT(*)` for bounded chunk/window scopes where practical
  - **Acceptance criteria**
    - Optional source-vs-output count comparison available for bounded modes
- **F3. Per-file row counts**
  - Persist row counts at file level
  - **Acceptance criteria**
    - Every exported file has row_count recorded
- **F4. Export reconciliation summary**
  - Add summary view:
    - rows seen
    - rows written
    - rows validated
    - mismatch if any
  - **Acceptance criteria**
    - Reconciliation output available post-run or via CLI
- **F5. Audit mode tradeoff docs**
  - Document strict vs cheap verification modes
  - **Acceptance criteria**
    - User understands cost of stronger checks

---

## Epic G — Real-World Test Harness

### Objective

Build a local battle-testing lab around Rivet.

#### Tasks

- **G1. MinIO in dev environment**
  - Add S3-compatible local object storage
  - **Acceptance criteria**
    - Full local upload flow works without real AWS
- **G2. Network fault injection**
  - Add Toxiproxy or equivalent
  - **Acceptance criteria**
    - Can simulate timeout, reset, latency, intermittent failure
- **G3. Mutation runner**
  - Generate:
    - inserts
    - updates to old rows
    - late-arriving data
    - sparse IDs
    - wide text payloads
  - **Acceptance criteria**
    - Reusable mutation script/tool exists
- **G4. Bad source fixtures**
  - Add scenarios:
    - no cursor index
    - seq scan
    - huge text rows
    - connection churn
    - lock contention
  - **Acceptance criteria**
    - These scenarios are reproducible locally
- **G5. Automated E2E matrix**
  - Run core scenarios against PG/MySQL + local/MinIO
  - **Acceptance criteria**
    - One command triggers repeatable E2E matrix

---

## Epic H — Crash and Recovery Battle Tests

### Objective

Make restart and rerun behavior explicit and tested.

#### Tasks

- **H1. Crash matrix**
  - Enumerate failure stages:
    - during read
    - during write
    - before upload
    - during upload
    - before state update
    - after validation before finalize
  - **Acceptance criteria**
    - Crash matrix documented
- **H2. Failure injection hooks**
  - Add hooks to force failures at chosen stages
  - **Acceptance criteria**
    - Can intentionally crash/abort at selected lifecycle points
- **H3. Recovery integration tests**
  - Verify rerun behavior after forced crash
  - **Acceptance criteria**
    - Recovery tests exist and pass for key modes
- **H4. Rerun behavior documentation**
  - Explain what user should expect after interrupted runs
  - **Acceptance criteria**
    - Duplicate/rerun semantics clearly documented

---

## Epic I — Performance and Capacity Envelope

### Objective

Turn benchmark results into operational guidance.

#### Tasks

- **I1. Benchmark datasets**
  - Standardize datasets:
    - narrow table
    - wide text table
    - sparse ID table
    - hot mutable table
  - **Acceptance criteria**
    - Benchmark fixtures are versioned and repeatable
- **I2. Benchmark automation**
  - One command to run benchmark suite
  - **Acceptance criteria**
    - Benchmark process reproducible across environments
- **I3. Recommended defaults**
  - Produce operational guidance for:
    - safe
    - balanced
    - fast
    - parallel on/off
  - **Acceptance criteria**
    - Docs include practical defaults by scenario
- **I4. Memory-heavy warnings**
  - Surface warnings for dangerous parallel + wide-row combinations
  - **Acceptance criteria**
    - Check/docs/logs warn clearly about memory risk
- **I5. Capacity notes**
  - Capture real limits and observations
  - **Acceptance criteria**
    - Docs include practical capacity envelope notes

---

## Epic J — Product UX Polish

### Objective

Make Rivet easier to adopt and operate without reading the source code.

#### Tasks

- **J1. Example configs by scenario**
  - Provide configs for:
    - prod-safe incremental
    - bad table chunked
    - late-data time window
    - local → GCS
    - local → S3 / MinIO
  - **Acceptance criteria**
    - Scenario examples are runnable and documented
- **J2. Better error messages**
  - Improve fatal/retryable error text
  - **Acceptance criteria**
    - Auth/permanent errors do not look retryable
    - Retryable errors explain next attempt behavior
- **J3. “Next action” failure hints**
  - Add fix suggestions to common failures
  - **Acceptance criteria**
    - User sees suggested next step for common failures
- **J4. `rivet doctor` or extended check mode**
  - Centralize sanity checks
  - **Acceptance criteria**
    - One command helps diagnose auth + source + destination + config issues

---

## Epic K — First Pilot Rollout

### Objective

Run Rivet against a small number of real candidate tables and capture feedback.

#### Tasks

- **K1. Select pilot tables**
  - Choose at least:
    - one mutable table with `updated_at`
    - one bad table needing chunked/range
    - one wide/text-heavy table
  - **Acceptance criteria**
    - 2–3 pilot candidates selected and documented
- **K2. Repeated pilot runs**
  - Run for several days
  - **Acceptance criteria**
    - Repeated exports completed and tracked
- **K3. Feedback template**
  - Capture:
    - source load acceptable?
    - result trustworthy?
    - config understandable?
    - failures understandable?
  - **Acceptance criteria**
    - Standard pilot feedback form exists
- **K4. Pilot findings document**
  - Summarize:
    - top pain points
    - top wins
    - unsupported patterns
    - next fixes
  - **Acceptance criteria**
    - Findings doc produced after first pilot cycle

---

## Suggested Month Execution Order

### Week 2

- M3 (file size splitting) + M4 (memory-based batch sizing)
- F1 → F5
- G1 → G3

### Week 3

- M6 (stdout destination) + M7 (parameterized queries)
- G4 → G5
- H1 → H4

### Week 4

- I1 → I5
- J1 → J4
- K1 → K4

---

## Priorities

### P0 — Start now

- Zstd compression (M1) — immediate file size win
- Skip empty exports (M2) — immediate UX win
- Auth and credentials
- Preflight 2.0
- Execution semantics
- Run summary and accounting
- Docs rewrite
- Crash/recovery foundation

### P1 — Next layer

- File size splitting (M3)
- Memory-based batch sizing (M4)
- Shell completions (M5)
- Auditability and reconciliation
- MinIO and fault injection
- Mutation runner
- Benchmark harness
- Product UX polish

### P2 — After first pilots

- Stdout destination (M6)
- Parameterized queries (M7)
- Notifications
- Data quality checks
- More advanced audit modes
- Planner intelligence improvements

### P3 — Future

- Per-column Parquet encoding (M8)
- Insert / load (Parquet → DB)

---

### New Story — Recommended parallelism guidance

**User story**  
As an operator, I want Rivet to suggest a safe and useful parallelism level, so I can improve throughput without guessing and without overloading the source.

**Why it matters**
Real-world pipelines often get meaningful gains from better parallelism defaults alone. Safe performance tuning is valuable, especially when users do not want to hand-tune every export.

**Tasks**

- Derive recommended parallelism from mode, source, profile, and table shape
- Surface parallelism recommendation in `rivet check`
- Distinguish:
  - parallelism not recommended
  - conservative parallelism
  - performance-oriented parallelism
- Document that gains depend on source health, index quality, and row width

### Future Story — Auto parallel mode

**User story**  
As a user, I want Rivet to auto-select a reasonable parallelism level for chunked/range exports, so I do not have to tune thread counts manually.

**Tasks**

- Design heuristic for auto parallelism
- Bound chosen parallelism by tuning profile
- Respect memory thresholds and wide-row risk
- Expose final chosen concurrency in logs and run summary

---

## Definition of Done for Pilot-Ready

Rivet is considered ready for first battle tests when:

- Auth is predictable and documented
- `rivet check` gives actionable strategy and safety guidance
- Execution semantics are frozen and documented
- Run summary and basic reconciliation exist
- Crash/recovery behavior is tested
- Local battle lab exists (MinIO + faults + mutation scenarios)
- Docs explain real scenarios, not only features
- 2–3 pilot tables have been run repeatedly for multiple days

---

## First Tasks to Start Today

- M1. Configurable Parquet compression (zstd default) — fastest win
- M2. Skip empty exports — quick UX improvement
- A1. Credential precedence matrix
- A2. GCS ADC support
- A3. GCS explicit JSON credentials
- B1. Selected strategy output in `rivet check`
- B2. Profile recommendation in `rivet check`
- C1. Export lifecycle spec
- C2. State update point freeze
- D1. Run summary schema
- D2. End-of-run summary output
- E5. Guarantees and limitations document
- M5. Shell completions

### New Story — Data shape drift detection for growing text columns

**User story**  
As an operator, I want Rivet to detect when text/string columns grow significantly in observed length, so downstream schemas and load jobs do not fail unexpectedly.

**Why it matters**
This is not classic schema drift, but it is still a real operational risk:

- downstream targets may have narrower varchar/text assumptions
- row width and memory pressure can grow sharply
- file size and load behavior can degrade without any visible schema change

**Tasks**

- Add optional observed max length tracking for text/string/json columns
- Compare observed max length with previous runs
- Emit warning on significant width growth
- Surface width-growth warning in run summary / schema summary
- Document difference between structural schema drift and data shape drift

---

