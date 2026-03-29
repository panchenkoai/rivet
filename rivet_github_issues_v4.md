# Rivet GitHub Issues Breakdown

This document decomposes the roadmap into a GitHub-friendly hierarchy:

- **Epic** = large outcome-oriented workstream
- **Story** = user-visible or system-visible slice of value
- **Task** = concrete implementation step

Suggested labels:
- `epic`
- `story`
- `task`
- `pilot-alpha`
- `pilot-readiness`
- `auth`
- `planner`
- `reliability`
- `docs`
- `observability`
- `testing`
- `performance`
- `ux`

---

## Validated pain map

The following themes are repeatedly validated by user conversations and field experience. They are the problem space Rivet is prioritizing.

### 1. Fragile source databases
- bad extraction queries can hurt production or replicas
- parallelism is valuable but dangerous without guidance
- missing or weak indexes make extraction risky

### 2. Weak incremental foundations
- good delta keys are often missing
- teams re-read large windows to avoid data loss
- late-arriving data forces overlap/replay strategies

### 3. Poor change visibility
- schema drift is noticed too late
- data shape drift happens without schema changes, especially widening text/string payloads
- downstream assumptions about uniqueness/cardinality can silently break

### 4. Operational unpredictability
- teams often do not know whether extraction is safe, degraded, or unrealistic without source-side changes
- self-built tools work, but become hard to maintain as volume and edge cases grow

### 5. Alternative architecture boundary
- Kafka/event-driven delivery validates a different class of solution
- Rivet should stay focused on database-first extraction, bootstrap, replay, and backfill problems

---

## Product focus

Rivet should be treated as:

**a source-aware, self-hosted extraction engine for database-first systems**

Not as:
- a Kafka replacement
- a CDC platform
- an all-in-one ELT platform
- a SaaS connector marketplace
- a near-term warehouse merge/load orchestrator

---

## Pain-to-epic mapping

| Validated pain | Primary issue area |
|---|---|
| Fragile source DB cannot tolerate bad queries | Epic B — Preflight & Planner 2.0; Epic C — Reliability Semantics |
| Missing usable index / bad key | Epic B — Preflight & Planner 2.0; Epic L — Schema Visibility & Data Contracts |
| Re-reading large windows to avoid data loss | Epic L — Schema Visibility & Data Contracts; Epic H — Crash & Recovery |
| Schema changes go unnoticed | Epic L — Schema Visibility & Data Contracts |
| String/text payload grows unexpectedly | Epic L — Schema Visibility & Data Contracts |
| Parallelism is risky but valuable | Epic B — Preflight & Planner 2.0; Epic I — Performance Envelope |
| Downstream merge breaks because uniqueness changed | Epic L — Schema Visibility & Data Contracts |
| Self-built tools are hard to maintain at scale | Epic D — Observability & Run Summary; Epic E — Documentation for Real Scenarios; Epic J — Product UX Polish |

---


# Phase 1 — Pilot Alpha Stabilization

## Epic A — Auth & Connectivity
**Labels:** `epic`, `pilot-alpha`, `auth`

### Goal
Make authentication and credential behavior predictable for database and cloud destinations.

### Story A1 — Define credential resolution rules
**Labels:** `story`, `auth`, `pilot-alpha`

**User story**  
As an operator, I want Rivet to resolve credentials predictably so I know which auth source is being used.

**Acceptance criteria**
- Credential precedence is documented
- Source and destination precedence are explicit
- Rivet does not silently choose an unexpected auth source

#### Tasks
- [ ] Document DB credential precedence
- [ ] Document GCS credential precedence
- [ ] Document S3 credential precedence
- [ ] Add config comments/examples for auth precedence
- [ ] Add tests for precedence resolution

---

### Story A2 — Support GCS authentication via ADC
**Labels:** `story`, `auth`, `pilot-alpha`

**User story**  
As a GCP user, I want Rivet to use Application Default Credentials so I can run exports without managing a JSON key file.

**Acceptance criteria**
- Works with local ADC login
- Works in GCP runtime environments using ADC
- Failure mode is explicit if ADC unavailable

#### Tasks
- [ ] Implement ADC auth path for GCS destination
- [ ] Add integration test for ADC resolution path
- [ ] Add explicit error text for missing ADC credentials
- [ ] Add README example for ADC-based GCS config

---

### Story A3 — Support explicit GCS JSON credentials
**Labels:** `story`, `auth`, `pilot-alpha`

**User story**  
As an operator, I want to provide a service account JSON file explicitly when ADC is not available.

**Acceptance criteria**
- Config/env-based JSON credential path supported
- Clear error when file missing or invalid
- Docs explain when to prefer ADC vs JSON

#### Tasks
- [ ] Add config field for GCS credentials file
- [ ] Add env var support for GCS credentials file
- [ ] Validate file existence and parseability
- [ ] Add unit tests for bad/missing file
- [ ] Add docs section for explicit JSON auth

---

### Story A4 — Normalize DB credentials
**Labels:** `story`, `auth`, `pilot-alpha`

**User story**  
As a user, I want to configure PostgreSQL/MySQL either with a URL or structured fields so auth is consistent.

**Acceptance criteria**
- URL config supported
- Field-based config supported
- Validation errors are clear and actionable

#### Tasks
- [ ] Define canonical DB credential config schema
- [ ] Support URL + structured fields for PG
- [ ] Support URL + structured fields for MySQL
- [ ] Add config validation errors for missing fields
- [ ] Add example configs for both styles

---

### Story A5 — Add auth diagnostics
**Labels:** `story`, `auth`, `ux`, `pilot-alpha`

**User story**  
As an operator, I want a quick way to verify source and destination auth before running exports.

**Acceptance criteria**
- Rivet can validate source auth
- Rivet can validate destination auth
- Error output clearly distinguishes auth vs connectivity vs permission issues

#### Tasks
- [ ] Extend `rivet check` or add `rivet doctor`
- [ ] Add source auth check
- [ ] Add destination auth check
- [ ] Add explicit auth failure categorization
- [ ] Add docs for pre-run auth verification

---

## Epic B — Preflight & Planner 2.0
**Labels:** `epic`, `pilot-alpha`, `planner`

### Goal
Turn preflight into a planning and safety recommendation tool.

### Story B1 — Show selected extraction strategy
**Labels:** `story`, `planner`, `pilot-alpha`

**User story**  
As a user, I want Rivet to tell me which extraction strategy it plans to use so behavior is understandable before execution.

**Acceptance criteria**
- Each export shows strategy name in check output
- Strategy is derived from config/mode/planner logic
- Output is stable and user-friendly

#### Tasks
- [ ] Define strategy names for display
- [ ] Add strategy selection output to preflight
- [ ] Add tests for strategy rendering
- [ ] Document strategy names in docs

---

### Story B2 — Recommend tuning profile
**Labels:** `story`, `planner`, `pilot-alpha`

**User story**  
As an operator, I want Rivet to suggest safe/balanced/fast so I do not overload a source by accident.

**Acceptance criteria**
- Check output includes recommended profile
- Recommendation explains why
- Recommendation reflects hot/wide/large-table risks when detectable

#### Tasks
- [ ] Define profile recommendation heuristics
- [ ] Add recommendation output to check
- [ ] Add tests for safe/balanced/fast recommendation
- [ ] Document recommendation rules

---

### Story B3 — Detect sparse range risk
**Labels:** `story`, `planner`, `pilot-alpha`

**User story**  
As a user of chunked mode, I want Rivet to warn me when sparse keys will create many empty range windows.

**Acceptance criteria**
- Sparse key risk warning appears when applicable
- Warning includes mitigation ideas
- Docs explain tradeoff

#### Tasks
- [ ] Define sparse-range heuristic
- [ ] Add sparse-range warning to check
- [ ] Add warning tests
- [ ] Link warning to docs/example fixes

---

### Story B4 — Warn about dense surrogate sort cost
**Labels:** `story`, `planner`, `pilot-alpha`

**User story**  
As a user, I want Rivet to warn me that dense surrogate chunking may require sorting or index-ordered scanning.

**Acceptance criteria**
- Warning explains cost tradeoff
- Does not present dense surrogate as a universally good solution
- Appears only when relevant

#### Tasks
- [ ] Add dense-surrogate warning text
- [ ] Trigger warning for known workaround patterns
- [ ] Add docs section for dense surrogate tradeoffs
- [ ] Add tests for warning emission

---

### Story B5 — Warn about parallel memory risk
**Labels:** `story`, `planner`, `observability`, `pilot-alpha`

**User story**  
As an operator, I want Rivet to warn me when parallel mode plus wide rows may cause dangerous memory pressure.

**Acceptance criteria**
- Warning shown for likely memory-heavy scenarios
- Warning recommends safer alternatives
- Docs mention parallel cost for wide-row tables

#### Tasks
- [ ] Define memory-risk heuristic for preflight
- [ ] Add parallel risk warning
- [ ] Add docs for parallel tradeoffs
- [ ] Add tests for warning paths

---

### Story B6 — Improve degraded/unsafe suggestions
**Labels:** `story`, `planner`, `pilot-alpha`

**User story**  
As a user, I want actionable next steps when Rivet marks a query as degraded or unsafe.

**Acceptance criteria**
- DEGRADED and UNSAFE verdicts include concrete actions
- Suggestions are mode-aware
- Suggestions are not generic boilerplate

#### Tasks
- [ ] Create suggestion catalog by failure/verdict type
- [ ] Wire suggestions into check output
- [ ] Add tests for verdict suggestions
- [ ] Review wording for clarity and brevity

---

## Epic C — Reliability Semantics
**Labels:** `epic`, `pilot-alpha`, `reliability`

### Goal
Define and document exact execution semantics, retries, duplicates, and validation meaning.

### Story C1 — Document export lifecycle
**Labels:** `story`, `reliability`, `docs`, `pilot-alpha`

**User story**  
As a user, I want to understand Rivet's execution lifecycle so I know what happens during an export.

**Acceptance criteria**
- One canonical lifecycle document exists
- Lifecycle matches implementation
- Lifecycle covers read/write/upload/validate/state/finalize

#### Tasks
- [ ] Write lifecycle spec
- [ ] Review lifecycle against implementation
- [ ] Add lifecycle diagram to docs
- [ ] Add lifecycle references from README/PRODUCT docs

---

### Story C2 — Freeze state update semantics
**Labels:** `story`, `reliability`, `pilot-alpha`

**User story**  
As an operator, I want to know exactly when Rivet advances cursor/state so I can reason about reruns and failures.

**Acceptance criteria**
- State update point is explicit
- Code and docs match
- Review confirms no ambiguous checkpoint movement

#### Tasks
- [ ] Define checkpoint advancement rule
- [ ] Audit implementation against rule
- [ ] Add tests for state advancement timing
- [ ] Document state update semantics

---

### Story C3 — Document duplicate semantics
**Labels:** `story`, `reliability`, `docs`, `pilot-alpha`

**User story**  
As a user, I want to know where duplicates may happen so I can design downstream handling safely.

**Acceptance criteria**
- Duplicate behavior documented for overlap/time_window/rerun
- No misleading exactly-once implication
- Docs include recommended downstream handling assumptions

#### Tasks
- [ ] Document duplicate-prone scenarios
- [ ] Add overlap/time_window duplicate notes
- [ ] Add rerun duplicate notes
- [ ] Link duplicate semantics from mode docs

---

### Story C4 — Document retry semantics
**Labels:** `story`, `reliability`, `pilot-alpha`

**User story**  
As an operator, I want retry behavior to be understandable so I can trust failures and recovery.

**Acceptance criteria**
- Retryable vs permanent errors clearly documented
- Reconnect behavior documented
- CLI messaging aligns with documentation

#### Tasks
- [ ] Document retry categories
- [ ] Review retry text in CLI/logging
- [ ] Add examples of retry vs fail-fast
- [ ] Add tests for retry messaging if applicable

---

### Story C5 — Clarify validation semantics
**Labels:** `story`, `reliability`, `docs`, `pilot-alpha`

**User story**  
As a user, I want to know what `--validate` proves and what it does not prove.

**Acceptance criteria**
- Validation meaning is clearly documented
- Row-count validation is not overstated
- Docs distinguish file integrity vs source/output equivalence

#### Tasks
- [ ] Write validation semantics doc section
- [ ] Update README wording around validate
- [ ] Add examples of what validate catches
- [ ] Add examples of what validate does not catch

---

## Epic D — Observability & Run Summary
**Labels:** `epic`, `pilot-alpha`, `observability`

### Goal
Give operators a concise operational summary for every run and a traceable accounting record.

### Story D1 — Define run summary schema
**Labels:** `story`, `observability`, `pilot-alpha`

**User story**  
As an operator, I want one consistent summary format so I can quickly inspect export outcomes.

**Acceptance criteria**
- Summary field list finalized
- Same schema used across CLI/logs/metrics where possible

#### Tasks
- [ ] Define summary fields
- [ ] Review field usefulness with current metrics
- [ ] Add schema to docs/spec
- [ ] Add tests for summary serialization/rendering if needed

---

### Story D2 — Print end-of-run summary
**Labels:** `story`, `observability`, `pilot-alpha`

**User story**  
As an operator, I want a clear summary after each run so I can see what happened without digging through logs.

**Acceptance criteria**
- Summary printed after each export run
- Summary is readable in CLI and logs
- Summary includes validation/retry/schema-change information

#### Tasks
- [ ] Implement summary rendering
- [ ] Include summary in CLI output
- [ ] Ensure summary also appears in log stream
- [ ] Add output tests/golden tests

---

### Story D3 — Add manifest-style accounting
**Labels:** `story`, `observability`, `reliability`, `pilot-alpha`

**User story**  
As a user, I want to inspect which files were created by a run so I can reconcile outputs.

**Acceptance criteria**
- Run-to-file accounting exists
- File-level metadata can be inspected after a run
- Accounting is stable enough for pilot operations

#### Tasks
- [ ] Define file accounting schema
- [ ] Persist file-level output metadata
- [ ] Add CLI or state inspection path
- [ ] Add tests for file accounting persistence

---

### Story D4 — Align metrics and run summary
**Labels:** `story`, `observability`, `pilot-alpha`

**User story**  
As an operator, I want `rivet metrics` and end-of-run output to agree on totals and identifiers.

**Acceptance criteria**
- Same run identity used in both places
- Totals match
- No confusing mismatch between summary and metrics history

#### Tasks
- [ ] Define canonical run identifier usage
- [ ] Align summary and metrics storage/output
- [ ] Add consistency tests
- [ ] Update docs/examples

---

## Epic E — Documentation for Real Scenarios
**Labels:** `epic`, `pilot-alpha`, `docs`

### Goal
Make docs scenario-driven and practical for first pilot users.

### Story E1 — Reposition README
**Labels:** `story`, `docs`, `pilot-alpha`

**User story**  
As a new user, I want the README to quickly tell me what Rivet is, when to use it, and what it does not do.

**Acceptance criteria**
- README clearly states scope and non-goals
- Opening emphasizes lightweight, source-safe, predictable extract-only behavior

#### Tasks
- [ ] Rewrite README introduction
- [ ] Add “What Rivet is / is not” section
- [ ] Add tighter problem statement
- [ ] Review docs for consistency with README positioning

---

### Story E2 — Write “Choosing a mode” guide
**Labels:** `story`, `docs`, `pilot-alpha`

**User story**  
As a user, I want practical guidance on choosing full, incremental, chunked, or time_window mode.

**Acceptance criteria**
- One dedicated mode-selection guide exists
- Includes warnings and tradeoffs, not only happy-path examples

#### Tasks
- [ ] Write mode decision matrix
- [ ] Add examples per mode
- [ ] Add failure/anti-pattern notes
- [ ] Link from README/config docs

---

### Story E3 — Write “Choosing a profile” guide
**Labels:** `story`, `docs`, `pilot-alpha`

**User story**  
As an operator, I want practical guidance on when to use safe, balanced, or fast.

**Acceptance criteria**
- Profile guide includes real environment examples
- Explicitly warns against using fast in fragile environments

#### Tasks
- [ ] Write profile guide
- [ ] Add prod vs replica vs dedicated-source examples
- [ ] Add wide-row/parallel caution notes
- [ ] Link from README and check output docs

---

### Story E4 — Write auth guide
**Labels:** `story`, `docs`, `auth`, `pilot-alpha`

**User story**  
As a user, I want one auth guide so I can configure Rivet without reading source code.

**Acceptance criteria**
- Covers DB auth, GCS ADC, GCS JSON, env vars
- Contains runnable examples

#### Tasks
- [ ] Write DB auth section
- [ ] Write GCS ADC section
- [ ] Write GCS JSON section
- [ ] Add auth troubleshooting tips
- [ ] Link from README

---

### Story E5 — Write guarantees and limitations doc
**Labels:** `story`, `docs`, `reliability`, `pilot-alpha`

**User story**  
As a user, I want a plain-language statement of guarantees and limitations so I know what to trust.

**Acceptance criteria**
- One dedicated guarantees/non-goals doc exists
- Covers extract-only scope, no CDC, no merge/load, duplicate semantics, no exactly-once promise

#### Tasks
- [ ] Draft guarantees/limitations document
- [ ] Review against current implementation
- [ ] Add links from README and PRODUCT docs
- [ ] Keep wording precise and non-marketing

---

# Phase 2 — Pilot Readiness & Battle Testing

## Epic F — Auditability & Correctness Confidence
**Labels:** `epic`, `pilot-readiness`, `reliability`

### Goal
Improve trust that exported files correspond to the intended source slice.

### Story F1 — Define export audit model
**Labels:** `story`, `reliability`, `pilot-readiness`

**User story**  
As an operator, I want Rivet to expose audit-friendly counts so I can reason about whether an export completed as intended.

**Acceptance criteria**
- Audit model is documented
- Audit model maps to actual stored data or output summary

#### Tasks
- [ ] Define audit concepts and fields
- [ ] Map audit model to current lifecycle
- [ ] Review cost implications
- [ ] Add docs section for audit model

---

### Story F2 — Add bounded source count verification
**Labels:** `story`, `reliability`, `pilot-readiness`

**User story**  
As a user, I want optional source-side counts for bounded chunks/windows so I can compare source rows to written rows.

**Acceptance criteria**
- Optional verification works for bounded scopes where practical
- Performance tradeoff documented

#### Tasks
- [ ] Design optional source count path
- [ ] Implement count verification for bounded modes
- [ ] Add summary/reconciliation output
- [ ] Add tests for verified counts
- [ ] Document cost tradeoff

---

### Story F3 — Persist per-file row counts
**Labels:** `story`, `observability`, `pilot-readiness`

**User story**  
As an operator, I want each exported file to have a row count so I can reconcile outputs later.

**Acceptance criteria**
- File row counts are persisted and inspectable

#### Tasks
- [ ] Extend file accounting schema
- [ ] Persist row counts per file
- [ ] Expose row counts through CLI or state query
- [ ] Add tests

---

### Story F4 — Add reconciliation summary
**Labels:** `story`, `reliability`, `observability`, `pilot-readiness`

**User story**  
As a user, I want a reconciliation summary so I can see rows seen, written, and validated in one place.

**Acceptance criteria**
- Post-run or CLI reconciliation view exists
- Mismatch is visible and explicit

#### Tasks
- [ ] Design reconciliation output
- [ ] Implement summary generation
- [ ] Integrate with run summary or metrics
- [ ] Add tests/golden outputs

---

### Story F5 — Document strict vs cheap audit modes
**Labels:** `story`, `docs`, `pilot-readiness`

**User story**  
As a user, I want to understand the cost of stronger verification before enabling it.

**Acceptance criteria**
- Docs explain cheap vs strict verification modes
- Guidance included for large/hot tables

#### Tasks
- [ ] Write audit mode tradeoff doc
- [ ] Add examples of recommended usage
- [ ] Link from validation and mode docs

---

## Epic G — Battle Test Lab
**Labels:** `epic`, `pilot-readiness`, `testing`

### Goal
Create a local lab that simulates realistic storage, source failures, and data mutation patterns.

### Story G1 — Add MinIO for local object storage
**Labels:** `story`, `testing`, `pilot-readiness`

**User story**  
As a developer, I want a local S3-compatible environment so I can test upload behavior without AWS.

**Acceptance criteria**
- MinIO integrated into dev environment
- End-to-end export to MinIO works

#### Tasks
- [ ] Add MinIO to docker compose
- [ ] Add sample MinIO config
- [ ] Add local upload test
- [ ] Document MinIO quickstart

---

### Story G2 — Add network fault injection
**Labels:** `story`, `testing`, `pilot-readiness`

**User story**  
As a developer, I want to simulate flaky networks so I can validate retry and reconnect behavior.

**Acceptance criteria**
- Can inject timeout/reset/latency faults
- Fault tests are repeatable

#### Tasks
- [ ] Choose fault injection tool
- [ ] Add tool to dev environment
- [ ] Create timeout scenario
- [ ] Create connection reset scenario
- [ ] Create intermittent latency scenario
- [ ] Document usage

---

### Story G3 — Build mutation runner
**Labels:** `story`, `testing`, `pilot-readiness`

**User story**  
As a developer, I want repeatable mutation scenarios so I can test overlap, late data, and changing tables.

**Acceptance criteria**
- Mutation runner can generate inserts, updates, sparse IDs, late rows, and wide text payloads

#### Tasks
- [ ] Define mutation scenario catalog
- [ ] Implement insert scenario
- [ ] Implement update-old-row scenario
- [ ] Implement late-arrival scenario
- [ ] Implement sparse-key scenario
- [ ] Implement wide-text scenario
- [ ] Add docs/examples

---

### Story G4 — Add bad source fixtures
**Labels:** `story`, `testing`, `pilot-readiness`

**User story**  
As a developer, I want reproducible “bad source” conditions so planner and safe mode can be tested honestly.

**Acceptance criteria**
- Bad source fixtures exist for common degraded scenarios

#### Tasks
- [ ] Add no-index cursor fixture
- [ ] Add seq-scan fixture
- [ ] Add huge-text fixture
- [ ] Add connection churn fixture
- [ ] Add lock-contention fixture
- [ ] Add docs describing each scenario

---

### Story G5 — Automate E2E matrix
**Labels:** `story`, `testing`, `pilot-readiness`

**User story**  
As a maintainer, I want one command to run the core battle-test matrix.

**Acceptance criteria**
- E2E matrix runs across supported sources and destinations
- Key scenarios are automated

#### Tasks
- [ ] Define E2E matrix
- [ ] Add runner script/command
- [ ] Integrate PG scenarios
- [ ] Integrate MySQL scenarios
- [ ] Integrate local destination scenarios
- [ ] Integrate MinIO scenarios
- [ ] Add reporting output

---

## Epic H — Crash & Recovery
**Labels:** `epic`, `pilot-readiness`, `reliability`

### Goal
Make crash behavior and reruns explicit, testable, and trustworthy.

### Story H1 — Create crash matrix
**Labels:** `story`, `reliability`, `pilot-readiness`

**User story**  
As a maintainer, I want a complete crash matrix so failure semantics are explicit and test coverage is intentional.

**Acceptance criteria**
- Crash matrix enumerates lifecycle failure points
- Matrix is documented and reviewed

#### Tasks
- [ ] Define lifecycle failure stages
- [ ] Create crash matrix document
- [ ] Review matrix against implementation lifecycle
- [ ] Link matrix from reliability docs

---

### Story H2 — Add failure injection hooks
**Labels:** `story`, `testing`, `pilot-readiness`

**User story**  
As a developer, I want to force failures at chosen points so recovery paths can be tested deterministically.

**Acceptance criteria**
- Selected lifecycle stages can be forcibly failed in tests/dev runs

#### Tasks
- [ ] Design failure hook mechanism
- [ ] Add hook before upload
- [ ] Add hook after write
- [ ] Add hook before state update
- [ ] Add hook after validation
- [ ] Add test-only control path

---

### Story H3 — Write recovery integration tests
**Labels:** `story`, `testing`, `reliability`, `pilot-readiness`

**User story**  
As a maintainer, I want rerun behavior after crashes to be covered by integration tests.

**Acceptance criteria**
- Recovery tests exist for key modes
- Expected duplicate/rerun behavior asserted

#### Tasks
- [ ] Add recovery test for incremental mode
- [ ] Add recovery test for chunked mode
- [ ] Add recovery test for time_window mode
- [ ] Add assertions for state behavior
- [ ] Add assertions for file/output behavior

---

### Story H4 — Document rerun semantics
**Labels:** `story`, `docs`, `reliability`, `pilot-readiness`

**User story**  
As a user, I want to know what rerunning after failure may do to outputs and duplicates.

**Acceptance criteria**
- Rerun semantics are clearly documented
- Docs reflect real crash/recovery behavior

#### Tasks
- [ ] Write rerun semantics doc section
- [ ] Add examples of interrupted run outcomes
- [ ] Link from guarantees/limitations doc

---

## Epic I — Performance Envelope
**Labels:** `epic`, `pilot-readiness`, `performance`

### Goal
Turn benchmarks into practical operating guidance.

### Story I1 — Standardize benchmark datasets
**Labels:** `story`, `performance`, `pilot-readiness`

**User story**  
As a maintainer, I want stable benchmark datasets so performance comparisons are meaningful.

**Acceptance criteria**
- Benchmark fixtures are repeatable and versioned

#### Tasks
- [ ] Define dataset catalog
- [ ] Create narrow dataset fixture
- [ ] Create wide-text dataset fixture
- [ ] Create sparse-ID dataset fixture
- [ ] Create hot mutable dataset fixture

---

### Story I2 — Automate benchmark suite
**Labels:** `story`, `performance`, `pilot-readiness`

**User story**  
As a maintainer, I want one command to run the benchmark suite so capacity testing is repeatable.

**Acceptance criteria**
- Benchmark suite is runnable with one command
- Outputs are captured in a consistent format

#### Tasks
- [ ] Create benchmark runner
- [ ] Add output format for benchmark results
- [ ] Add docs for benchmark execution
- [ ] Add CI/manual execution guidance

---

### Story I3 — Publish recommended defaults
**Labels:** `story`, `performance`, `docs`, `pilot-readiness`

**User story**  
As a user, I want practical defaults by scenario so I do not guess at tuning.

**Acceptance criteria**
- Docs provide recommended defaults by environment and workload shape

#### Tasks
- [ ] Summarize benchmark-based defaults
- [ ] Publish profile recommendations by scenario
- [ ] Add parallel on/off recommendations
- [ ] Add mode-specific caution notes

---

### Story I4 — Surface memory-heavy warnings
**Labels:** `story`, `performance`, `observability`, `pilot-readiness`

**User story**  
As an operator, I want Rivet to surface dangerous memory combinations before they surprise me.

**Acceptance criteria**
- Warnings appear in check/docs/logs for risky combinations

#### Tasks
- [ ] Define memory-heavy thresholds/heuristics
- [ ] Add warnings to check
- [ ] Add warnings to docs
- [ ] Add tests for warning logic

---

### Story I5 — Write capacity notes
**Labels:** `story`, `performance`, `docs`, `pilot-readiness`

**User story**  
As a user, I want honest capacity notes so I know what sizes and profiles are realistic.

**Acceptance criteria**
- Capacity notes reflect measured behavior
- Notes include caveats for wide rows and parallelism

#### Tasks
- [ ] Draft capacity notes doc
- [ ] Add real benchmark observations
- [ ] Review wording for precision
- [ ] Link from README/performance docs

---

## Epic J — Product UX Polish
**Labels:** `epic`, `pilot-readiness`, `ux`

### Goal
Make Rivet easier to adopt without source-code reading.

### Story J1 — Provide scenario-based example configs
**Labels:** `story`, `ux`, `docs`, `pilot-readiness`

**User story**  
As a new user, I want ready examples so I can start from a working pattern.

**Acceptance criteria**
- Examples exist for core scenarios
- Examples are runnable and documented

#### Tasks
- [ ] Add prod-safe incremental example
- [ ] Add bad-table chunked example
- [ ] Add late-data time_window example
- [ ] Add local-to-GCS example
- [ ] Add local-to-MinIO/S3 example

---

### Story J2 — Improve error messages
**Labels:** `story`, `ux`, `pilot-readiness`

**User story**  
As a user, I want errors to clearly indicate whether I should retry, fix config, or stop.

**Acceptance criteria**
- Auth/permanent errors do not look retryable
- Retryable errors explain next attempt behavior

#### Tasks
- [ ] Audit current error message quality
- [ ] Improve auth failure wording
- [ ] Improve permanent failure wording
- [ ] Improve retryable failure wording
- [ ] Add tests for representative messages if feasible

---

### Story J3 — Add “next action” hints
**Labels:** `story`, `ux`, `pilot-readiness`

**User story**  
As a user, I want suggested next steps when Rivet fails so I can recover faster.

**Acceptance criteria**
- Common failures include next-step hints
- Hints are actionable and concise

#### Tasks
- [ ] Define common failure-to-hint mapping
- [ ] Add hints for auth failures
- [ ] Add hints for unsafe/degraded query failures
- [ ] Add hints for destination failures
- [ ] Add tests/docs

---

### Story J4 — Add `rivet doctor` or extended check mode
**Labels:** `story`, `ux`, `pilot-readiness`

**User story**  
As an operator, I want a single command for sanity checks so I can verify setup before running exports.

**Acceptance criteria**
- One command checks config/auth/connectivity/basic readiness
- Output is user-oriented

#### Tasks
- [ ] Decide between `doctor` and extended `check`
- [ ] Define diagnostic scope
- [ ] Implement config validation output
- [ ] Implement auth/connectivity output
- [ ] Add docs/examples

---

## Epic K — First Pilot Rollout
**Labels:** `epic`, `pilot-readiness`, `pilot`

### Goal
Run Rivet against real candidate tables and capture operational feedback.

### Story K1 — Select pilot tables
**Labels:** `story`, `pilot`, `pilot-readiness`

**User story**  
As a team, we want representative pilot tables so we can validate Rivet against real workload shapes.

**Acceptance criteria**
- At least 2–3 pilot tables selected
- Tables represent different workload patterns

#### Tasks
- [ ] Select mutable `updated_at` table
- [ ] Select bad-table/chunked candidate
- [ ] Select wide/text-heavy table
- [ ] Document pilot table rationale

---

### Story K2 — Run repeated pilots
**Labels:** `story`, `pilot`, `pilot-readiness`

**User story**  
As a team, we want repeated pilot runs so we can observe operational stability over time.

**Acceptance criteria**
- Repeated runs completed across multiple days
- Outcomes tracked consistently

#### Tasks
- [ ] Define pilot schedule
- [ ] Run pilot exports repeatedly
- [ ] Capture outcomes in shared log/doc
- [ ] Track source impact and failures

---

### Story K3 — Create feedback template
**Labels:** `story`, `pilot`, `pilot-readiness`

**User story**  
As a team, we want structured pilot feedback so findings are comparable across users and tables.

**Acceptance criteria**
- One reusable feedback template exists

#### Tasks
- [ ] Draft feedback questions
- [ ] Add prompts for source load/trust/config/errors
- [ ] Share template with pilot participants

---

### Story K4 — Write pilot findings document
**Labels:** `story`, `pilot`, `pilot-readiness`

**User story**  
As a team, we want a pilot findings document so next priorities come from evidence.

**Acceptance criteria**
- Findings doc summarizes wins, failures, unsupported patterns, and next actions

#### Tasks
- [ ] Collect pilot findings
- [ ] Summarize top wins
- [ ] Summarize top pain points
- [ ] Summarize unsupported patterns
- [ ] Propose next priorities

---

# Suggested Initial GitHub Issue Creation Order

## First 10 issues to create
1. Story A1 — Define credential resolution rules
2. Story A2 — Support GCS authentication via ADC
3. Story A3 — Support explicit GCS JSON credentials
4. Story B1 — Show selected extraction strategy
5. Story B2 — Recommend tuning profile
6. Story C1 — Document export lifecycle
7. Story C2 — Freeze state update semantics
8. Story D1 — Define run summary schema
9. Story D2 — Print end-of-run summary
10. Story E5 — Write guarantees and limitations doc

## Next wave
11. Story B3 — Detect sparse range risk
12. Story B6 — Improve degraded/unsafe suggestions
13. Story A5 — Add auth diagnostics
14. Story D3 — Add manifest-style accounting
15. Story J1 — Provide scenario-based example configs
16. Story H1 — Create crash matrix
17. Story G1 — Add MinIO for local object storage
18. Story G2 — Add network fault injection

---

# Suggested Milestones

## Milestone: Pilot Alpha Stabilization
Include:
- Epic A
- Epic B
- Epic C
- Epic D
- Epic E

## Milestone: Pilot Readiness & Battle Testing
Include:
- Epic F
- Epic G
- Epic H
- Epic I
- Epic J
- Epic K



### Story L5 — Detect data shape drift for growing text columns
**Labels:** `story`, `reliability`, `observability`, `pilot-readiness`

**User story**  
As an operator, I want Rivet to detect when text/string columns grow significantly in observed length, so downstream schemas and load jobs do not fail unexpectedly.

**Acceptance criteria**
- Optional observed max length tracking exists for text/string/json columns
- Current run can be compared to previous runs
- Warning appears when growth crosses configured or documented thresholds
- Docs explain the difference between structural schema drift and data shape drift

#### Tasks
- [ ] Design optional observed-length tracking for text/string/json columns
- [ ] Persist per-run max observed length for tracked columns
- [ ] Compare current run vs previous run values
- [ ] Emit width-growth warning in run summary
- [ ] Add docs for structural schema drift vs data shape drift
- [ ] Add tests for width-growth detection and warning emission

---


### Pilot Readiness & Battle Testing
Also include:
- Story L7 — Auto parallel mode
- Story L5 — Detect data shape drift for growing text columns



### Story L6 — Recommend parallelism level
**Labels:** `story`, `planner`, `performance`, `pilot-readiness`

**User story**  
As an operator, I want Rivet to suggest a safe and useful parallelism level, so I can improve throughput without guessing and without overloading the source.

**Acceptance criteria**
- `rivet check` can suggest a parallelism level or recommend against parallelism
- Recommendation reflects mode, profile, and likely source/table shape risk
- Docs explain that gains depend on source health, index quality, and row width

#### Tasks
- [ ] Define heuristics for recommended parallelism
- [ ] Add recommendation to `rivet check`
- [ ] Distinguish no-parallel / conservative / performance-oriented recommendations
- [ ] Add docs for parallelism recommendations
- [ ] Add tests for recommendation paths

---

### Story L7 — Auto parallel mode
**Labels:** `story`, `planner`, `performance`, `pilot-readiness`

**User story**  
As a user, I want Rivet to auto-select a reasonable parallelism level for chunked/range exports, so I do not have to tune thread counts manually.

**Acceptance criteria**
- Rivet can choose concurrency automatically for eligible modes
- Auto choice is bounded by tuning profile and memory safety rules
- Final chosen concurrency is visible in logs and run summary

#### Tasks
- [ ] Design auto-parallel heuristic
- [ ] Bound auto parallelism by tuning profile
- [ ] Respect memory thresholds and wide-row risk
- [ ] Surface chosen concurrency in logs and summary
- [ ] Add tests for auto-parallel selection

---


### Pilot Alpha Stabilization
Also include:
- Story L6 — Recommend parallelism level
