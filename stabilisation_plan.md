# Rivet — Architecture Execution Plan

## Context

Rivet has already moved beyond the shape of a “simple extractor.”  
Based on the repository structure, documentation, workflows, and product surface, it is evolving into an extract-only operational CLI with:

- incremental state
- preflight and doctor flows
- metrics history
- schema tracking
- file manifest
- quality checks
- multiple extraction and destination scenarios

At this stage, the main risk is no longer missing features.  
The main risk is losing control of the core architecture as the execution surface expands.

---

# Guiding Principles

Every architectural change should strengthen at least one of these three pillars:

1. **Source-safe extraction**
2. **Deterministic resumability**
3. **Operational observability**

If a new feature does not clearly improve one of these three, it should be questioned before entering the roadmap.

---

# Wave 1 — Core Stabilization

## Epic 1. Introduce `ResolvedRunPlan`

### Problem
The current product surface is already too broad for the pipeline to depend on raw config and CLI flags directly.  
As more modes are added, execution semantics become harder to reason about and easier to break.

### Goal
Create a single internal contract representing a fully normalized execution decision.

### Deliverables
- `RawConfig`
- `ValidatedConfig`
- `ResolvedRunPlan`
- dedicated planning phase:
  - parse
  - validate
  - normalize
  - resolve defaults
  - resolve strategy and destination constraints

### Expected Model
`ResolvedRunPlan` should explicitly contain:
- source contract
- extraction mode
- batching/chunking strategy
- destination write plan
- state policy
- validation/reconcile policy
- retry/backoff policy
- observability hooks

### First Practical Step
Implement:
- `plan(config, cli_args) -> ResolvedRunPlan`

Then make pipeline consume only `ResolvedRunPlan`.

### Dependencies
- config parsing layer
- validation layer
- initial strategy typing

### Risks
- temporary duplication during migration
- friction while untangling runtime modules from config structs

### Definition of Done
- no runtime module reads raw YAML structures directly
- no pipeline logic depends on raw CLI flags
- all execution decisions are derived before the run starts

### Priority
**P0**

---

## Epic 2. Add Centralized Compatibility Validation

### Problem
Rivet already has many mode combinations, and future failures will increasingly come from invalid or degraded combinations rather than isolated bugs.

### Goal
Turn hidden compatibility rules into explicit product logic.

### Deliverables
A centralized validator over `ResolvedRunPlan` that classifies combinations as:
- supported
- rejected
- supported with warning
- degraded semantics

### Example Rule Areas
- `stdout + manifest`
- `resume + split files`
- `quality checks + chunked export`
- `incremental + reconcile`
- `parallel chunking + checkpointing`

### First Practical Step
Create a `PlanValidator` that returns structured diagnostics.

### Dependencies
- `ResolvedRunPlan`

### Risks
- surfacing hidden inconsistencies already present in the product
- initial rule set may be incomplete

### Definition of Done
- all mode conflicts are detected before extraction starts
- warnings and rejections are produced consistently from one place

### Priority
**P0**

---

## Epic 3. Separate User Config Model from Internal Domain Model

### Problem
User-facing config evolves for UX. Runtime domain types evolve for correctness.  
These should not be the same thing.

### Goal
Prevent YAML/serde shapes from leaking into core execution.

### Deliverables
Three distinct layers:
- `RawConfig`
- `ValidatedConfig`
- `ResolvedRunPlan`

### First Practical Step
Audit runtime modules and remove dependencies on serde-friendly config structs.

### Dependencies
- `ResolvedRunPlan`
- validation layer

### Risks
- refactor overhead across module boundaries
- short-term complexity while transitional types coexist

### Definition of Done
- changes in YAML structure do not cascade across execution code
- runtime logic depends only on validated internal models

### Priority
**P0**

---

## Epic 4. Define State Update Invariants

### Problem
The system already supports retries, resumability, manifests, and at-least-once semantics.  
The hardest bugs will come from partial failures and ambiguous commit ordering.

### Goal
Make recovery semantics explicit and testable.

### Deliverables
A written invariant set covering:
- cursor advancement order
- file finalization order
- manifest write order
- final run verdict persistence
- retry monotonicity rules

### Suggested Invariants
- cursor must not advance before write finalization
- manifest must not be written before write success
- final run status must not be committed before final artifacts persist
- retries must not violate state monotonicity

### First Practical Step
Write an ADR with 5–7 system invariants and map each to at least one test.

### Dependencies
- current state flow understanding
- destination write lifecycle

### Risks
- may expose current ambiguity in failure handling
- may force rework in resume logic

### Definition of Done
- recovery behavior can be explained for every major failure point
- invariants are encoded in tests, not only documentation

### Priority
**P0**

---

## Epic 5. Decide: CLI Product vs Engine/Library

### Problem
The project currently presents both library and binary structure, but the strategic intent is not explicit.

### Goal
Make a clear architectural decision:
- CLI-first product, library internal
- or public embeddable engine

### Deliverables
An ADR stating:
- intended product boundary
- API stability expectations
- visibility and ownership rules for internal modules

### First Practical Step
Document the decision and align module visibility accordingly.

### Dependencies
- none

### Risks
- delaying this decision will create accidental public API
- premature library exposure increases maintenance cost

### Definition of Done
- module boundaries reflect the chosen strategy
- internal interfaces no longer look accidentally public

### Priority
**P0**

---

# Wave 2 — Decomposition of Core Responsibilities

## Epic 6. Split State into Domain Stores

### Problem
“State” is no longer a single concern.  
SQLite is currently serving multiple product responsibilities:
- incremental cursors
- checkpoints
- metrics history
- file manifests
- schema history

### Goal
Separate logical ownership even if physical storage remains one SQLite database.

### Deliverables
Introduce store boundaries such as:
- `CursorStore`
- `CheckpointStore`
- `ManifestStore`
- `MetricsStore`
- `SchemaHistoryStore`
- `MigrationRunner`

### First Practical Step
Refactor code ownership first. Keep the same SQLite schema initially.

### Dependencies
- state invariants
- better planning/runtime separation

### Risks
- temporary adapter complexity
- hidden coupling may surface during split

### Definition of Done
- each state concern is accessed through a dedicated interface
- each store is testable independently
- storage backend remains swappable at the logical boundary

### Priority
**P1**

---

## Epic 7. Make Extraction Strategies Explicit Types

### Problem
Execution modes should not emerge from combinations of booleans and scattered conditions.

### Goal
Represent extraction behavior through a small set of explicit strategies.

### Deliverables
Introduce strategy-level abstractions such as:
- `SnapshotStrategy`
- `IncrementalCursorStrategy`
- `ChunkedRangeStrategy`
- `WindowedTimeStrategy`

Each strategy should define:
- query/batch planning
- required state
- resume semantics
- compatibility constraints

### First Practical Step
Identify current mode combinations and map them into a bounded strategy taxonomy.

### Dependencies
- `ResolvedRunPlan`
- compatibility validator

### Risks
- existing code may rely on implicit hybrid behavior
- migration may require untangling special cases

### Definition of Done
- adding a new extraction mode does not require changing half the pipeline
- strategy behavior is declared through explicit types/contracts

### Priority
**P1**

---

## Epic 8. Separate Product Semantics from Execution Mechanics

### Problem
Runtime code tends to mix:
- what the run means
- how the run is executed
- what is persisted during execution

### Goal
Establish three architectural layers:
- `Planning`
- `Execution`
- `Persistence/Observability`

### Deliverables
A module-level refactor that makes these responsibilities explicit.

### First Practical Step
Review major modules and classify them:
- decision layer
- execution layer
- persistence/observability layer

Any mixed module becomes a split candidate.

### Dependencies
- `ResolvedRunPlan`
- state store split

### Risks
- module churn
- temporary indirection overhead

### Definition of Done
- pipeline does not decide semantics while running
- execution modules only execute resolved decisions
- persistence modules do not own runtime decision logic

### Priority
**P1**

---

## Epic 9. Formalize Destination Write Contracts

### Problem
Local disk, S3, GCS, and stdout are not operationally identical.  
Their failure modes, commit boundaries, and write guarantees differ.

### Goal
Make destination semantics explicit so planning and recovery logic can reason about them safely.

### Deliverables
For each destination, define:
- atomicity boundary
- overwrite semantics
- retry behavior
- partial write behavior
- finalize/commit moment
- relationship to manifest/state updates

Introduce:
- `DestinationCapabilities`
- `WriteCommitProtocol`

### First Practical Step
Document current destination semantics and extract common contract vocabulary.

### Dependencies
- state invariants
- planning layer

### Risks
- destination abstraction may currently hide inconsistent semantics
- some backends may need different recovery behavior

### Definition of Done
- pipeline knows when an output is considered committed
- state/manifest updates are aligned with destination commit semantics

### Priority
**P1**

---

## Epic 10. Introduce `RunJournal` as a First-Class Concept

### Problem
Metrics, manifests, warnings, schema observations, and run summaries can easily stay as side effects instead of forming one coherent execution record.

### Goal
Create a unified model of what happened during a run.

### Deliverables
A `RunJournal` domain model covering:
- run metadata
- resolved plan snapshot
- chunk/file events
- retry events
- warnings
- schema observations
- validation results
- final verdict

### First Practical Step
Unify domain event shapes before changing storage.

### Dependencies
- `ResolvedRunPlan`
- state store split
- observability boundaries

### Risks
- event model may grow too broad without discipline
- legacy summary outputs may need adaptation

### Definition of Done
- for any run, the system can answer:
  - what was planned
  - what happened
  - what succeeded
  - what degraded
  - what final outcome was recorded

### Priority
**P1**

---

# Wave 3 — Semantic Quality Gates

## Epic 11. Build Invariant Test Suite

### Problem
Compilation, linting, and happy-path E2E are not enough to protect a system whose main risk is semantic drift.

### Goal
Protect the system through tests on guarantees, not only on behavior samples.

### Deliverables
Test suites for:
- state monotonicity
- commit ordering
- resume correctness
- retry safety
- validation/reconcile semantics

### First Practical Step
Map each invariant from the ADR to at least one automated test.

### Dependencies
- state invariants
- destination contracts

### Risks
- tests may reveal ambiguous semantics that need product decisions first
- some tests may initially be difficult without more decomposition

### Definition of Done
- architecture invariants are enforced in CI
- regressions in recovery semantics fail the build

### Priority
**P1**

---

## Epic 12. Build Compatibility Matrix Test Suite

### Problem
Mode combinations are a major source of regression risk.

### Goal
Make compatibility behavior executable and testable.

### Deliverables
A scenario matrix covering critical combinations:
- accepted
- rejected
- warning-producing
- degraded

### First Practical Step
Create a table of 15–20 critical scenarios and encode them as plan-validation tests.

### Dependencies
- compatibility validator
- `ResolvedRunPlan`

### Risks
- may require formalizing previously implicit behavior
- matrix size can grow without prioritization

### Definition of Done
- critical combinations are locked by automated tests
- new modes must extend matrix coverage

### Priority
**P1**

---

## Epic 13. Add Recovery and Partial Failure Tests

### Problem
The most expensive defects will happen around interrupted runs, partial writes, resumed exports, and retries after partial progress.

### Goal
Verify behavior under real failure conditions, not only happy-path execution.

### Deliverables
Recovery-focused tests for:
- failure before file finalization
- failure after file finalization but before state update
- failure after manifest update but before final verdict
- retry after partial chunk progress
- resume after interrupted run

### First Practical Step
Select 5 critical failure points in the pipeline and write deterministic tests around them.

### Dependencies
- state invariants
- destination contracts
- run lifecycle clarity

### Risks
- current code may not expose enough hooks for deterministic failure injection
- may require light refactoring for testability

### Definition of Done
- recovery semantics are tested at known interruption boundaries
- release confidence no longer depends only on happy-path E2E

### Priority
**P1**

---

## Epic 14. Promote Semantic Release Gates

### Problem
A successful build does not prove the architecture still preserves its guarantees.

### Goal
Make release confidence depend on semantic guarantees as well as build health.

### Deliverables
Release gates that require:
- invariant tests
- compatibility matrix tests
- recovery tests

### First Practical Step
Classify the most important semantic suites as required checks in CI.

### Dependencies
- invariant tests
- compatibility tests
- recovery tests

### Risks
- CI time may increase
- initially brittle tests may need stabilization

### Definition of Done
- releases are blocked by semantic regressions, not just compilation failures

### Priority
**P2**

---

# Cross-Cutting Architecture Filters

Every future feature should be evaluated against the following questions:

## Feature Filter
Does this feature strengthen:
- source safety?
- deterministic resumability?
- observability/operability?

If not, it should likely not be implemented.

## Complexity Filter
Does this feature:
- require a new strategy?
- require new compatibility rules?
- alter commit ordering or state monotonicity?
- alter destination semantics?

If yes, it must not bypass planning and invariant review.

## Ownership Filter
Which layer owns this change?
- planning
- execution
- persistence/observability

If ownership is unclear, architecture is not ready.

---

# Recommended Execution Order

## Phase 1
1. `ResolvedRunPlan`
2. compatibility validator
3. config/domain model split
4. CLI vs engine ADR
5. state invariants ADR

## Phase 2
6. state store split
7. explicit extraction strategies
8. planning/execution/persistence boundary cleanup
9. destination capability contracts
10. run journal model

## Phase 3
11. invariant tests
12. compatibility matrix tests
13. recovery tests
14. semantic release gates

---

# Recommended First Epic

## Planning Layer Refactor

### Why start here
This has the highest architectural leverage per unit of effort.

### Scope
- introduce `RawConfig -> ValidatedConfig -> ResolvedRunPlan`
- add centralized compatibility validation
- make pipeline consume only resolved plans

### Expected Impact
- reduces hidden coupling
- reduces combinatorial complexity
- creates a stable base for future decomposition
- makes future features cheaper to add safely

### Success Criteria
- execution semantics are visible before runtime starts
- invalid combinations fail early
- core runtime stops depending on raw config shapes

---

# Success Criteria for the Whole Plan

The architecture execution plan can be considered successful when:

- execution semantics are resolved before runtime
- compatibility rules are explicit and testable
- state concerns are decomposed by domain
- destination guarantees are formalized
- recovery semantics are documented and enforced
- semantic invariants are release-gated
- new features can be added without increasing hidden coupling disproportionately

---

# Final Strategic Note

The main risk for Rivet is not that it is immature.  
The main risk is that it is already good enough to grow, while its internal semantics are not yet fully formalized.

This plan is designed to prevent that growth from becoming expensive.