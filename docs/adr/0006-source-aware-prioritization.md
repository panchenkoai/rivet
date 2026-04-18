# ADR-0006: Source-Aware Extraction Prioritization

- **Status:** Accepted
- **Date:** 2026-04-15 (proposed) · 2026-04-18 (accepted after Epics A/B/C/D/E/I landed)
- **Owners:** Rivet maintainers

## Context

Rivet already supports:
- extract-only workflows
- preflight diagnostics
- plan/apply
- snapshot / incremental / chunked / time-window execution
- state, metrics, manifest, and operational visibility

In real production environments, operators often face a different problem:

> not only how to extract, but what to extract first, what to delay, what to isolate, and what should not run together on the same source host or replica.

This matters when:
- many exports share one source replica
- tables differ greatly in size
- cursor quality varies by table
- some tables are weak-cursor or reconcile-prone
- extraction windows are limited
- source pressure matters more than raw throughput

## Decision

Rivet will introduce an **advisory prioritization layer** inside the planning subsystem.

This layer will:
- consume metadata and planning signals
- classify exports by cost/risk/freshness value
- emit explainable recommendations
- optionally consider shared source groups
- recommend ordering and execution waves for multi-export campaigns

This feature is:
- **advisory**
- **planning-time**
- **explainable**

It is not:
- a scheduler
- a queue manager
- an orchestration engine
- a runtime reordering mechanism

## Why

This comes directly from real production pain:
- 100+ tables
- shared replicas
- weak or inconsistent cursors
- sparse huge tables
- mixed strategies
- limited windows
- manual prioritization outside the product

Rivet already has many useful signals:
- row estimates
- chunking information
- warnings
- profile recommendations
- sparse-range diagnostics
- plan artifacts

But they are not yet combined into a clear recommendation layer.

## Principles

### 1. Advisory, not authoritative
Recommendations guide users and external orchestrators.
They do not silently control execution in v1.

### 2. Explainability first
Every recommendation must include structured reasons.

### 3. Metadata is signal, not truth
Metadata can be used for:
- discovery
- hints
- ranking
- risk classification
- freshness signals

Metadata must not alone be used for:
- committed export progression
- correctness proof
- reconcile success
- exact incremental truth

### 4. Planning-layer ownership
This feature belongs first to:
- `plan/`
- `preflight/`
- `plan` CLI output

It should not start inside runtime scheduling or execution control.

### 5. Graceful degradation
Weak metadata must result in weaker recommendations and explicit caveats.

## Definitions

### Metadata
Information about tables/exports that is not the exported data itself.
Examples:
- table size
- estimated row count
- candidate cursor fields
- source freshness hint
- source group

### Export recommendation
An explainable recommendation for one export.

### Campaign recommendation
A recommendation for a set of exports, including:
- ordering
- waves
- source-group warnings
- isolation hints

### Source group
A logical grouping for exports that share one source host, replica, or capacity boundary.

## Scope

### In scope for v1
- per-export scoring and classification
- recommendation reasons
- wave recommendation
- optional source-group hints
- campaign-level advisory output
- integration into `plan`

### Out of scope for v1
- automatic scheduling
- queue daemon
- internal job runner
- dynamic runtime balancing
- historical learning
- execution-time throttling control

## Recommendation model

### Per-export output
Each export may receive:
- `priority_score`
- `priority_class`
- `cost_class`
- `risk_class`
- `recommended_wave`
- `reasons[]`
- optional `isolate_on_source`

### Campaign output
For a group of exports:
- ordered exports
- grouped waves
- source-group warnings
- heavy export isolation hints
- advisory concurrency hints

## Inputs

### Metadata inputs
Expected signals may include:
- estimated table size
- estimated row count
- min/max numeric range
- sparse-range suspicion
- cursor candidates
- cursor quality
- suggested strategy
- source freshness hint
- reconcile-required flag
- source group

### Historical inputs
Deferred:
- previous runtime duration
- retry count
- failure rate
- observed throughput

## Metadata policy

### Allowed uses
Metadata may be used to:
- suggest strategies
- rank exports
- infer cost/risk
- generate warnings
- bootstrap export registry

### Forbidden uses
Metadata must not alone:
- advance committed export boundaries
- define correctness of continuation
- define reconcile success
- prove source-target consistency

## Cursor policy interaction

This feature depends on cursor semantics and requires a stable cursor quality classification such as:
- strong monotonic cursor
- weak time cursor
- weak multi-candidate cursor
- fallback-only cursor
- no usable cursor

This ADR does not fully define cursor policy design, but prioritization must consume its outputs.

## Source group interaction

Exports may optionally belong to a `source_group`.

This allows recommendations such as:
- do not run these together
- isolate this export
- only one heavy export at a time on this group

These are advisory only in v1.

## Scoring approach

### Initial approach
Use a deterministic rule-based scoring model.

### Requirement
Recommendations must never be score-only.
They must always include:
- class
- score or rank
- reasons
- warnings where relevant

## Architecture fit

### Reuse current modules
- `src/plan/` — recommendation models and campaign logic
- `src/preflight/` — advisory inputs
- `src/pipeline/` — CLI rendering
- `src/config/` — optional future metadata fields

### Likely new modules
- `src/plan/recommend.rs`
- `src/plan/campaign.rs`

### Modules mostly untouched in v1
- destinations
- file writing
- retry logic
- apply contracts
- runtime scheduling internals

## Rollout

### Phase 0
- define schemas
- define scoring dimensions
- define reason taxonomy
- define source-group model

### Phase 1
- per-export recommendation MVP
- plan output integration

### Phase 2
- source-group hints
- isolation suggestions

### Phase 3
- campaign view
- wave grouping

### Phase 4
- pilot validation and tuning

### Phase 5
- historical refinement — **landed** as Epic I ([ADR-0008 interaction](0008-export-progression.md), bounded contribution from `export_metrics`; see `plan::history::HistorySnapshot`)

## Risks

### Product sprawl
This may drift toward orchestration.

Mitigation:
- advisory only
- no runtime scheduling
- no internal queue

### Weak metadata quality
Recommendations may be misleading.

Mitigation:
- explicit reasons
- confidence-aware output
- clear warnings

### Beginner overload
Not all users need campaign planning.

Mitigation:
- expose primarily through `plan`
- keep first-run path simple

## Alternatives considered

### Do nothing
Rejected because users already solve this manually outside the product.

### Build a scheduler
Rejected because it expands product scope too far.

### Push all logic to external orchestrators
Rejected because Rivet has richer planning context than generic orchestrators.

## Consequences

### Positive
- stronger differentiation
- better operator experience
- stronger source-safety story
- strong pilot/demo value

### Negative
- more planning complexity
- more heuristics to maintain
- future temptation to over-automate

## Summary

Rivet will add **Source-Aware Extraction Prioritization** as an explainable planning feature.

It will:
- reuse current planning and preflight layers
- classify and rank exports
- recommend execution waves
- emit source-aware warnings

It will not become a scheduler in v1.
