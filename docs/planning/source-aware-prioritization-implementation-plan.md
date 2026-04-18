# Implementation Plan: Source-Aware Extraction Prioritization

## Goal

Implement source-aware prioritization with minimal architectural disruption by extending the planning layer.

## Success criteria

The first useful version is successful when:
- every export can receive a recommendation
- recommendations are explainable
- `rivet plan` can show ordered exports and wave hints
- the feature remains advisory
- no runtime execution path is silently altered

---

## Stage 0 — Contracts and schemas

### Objectives
Define stable internal models before coding scoring logic.

### Deliverables
- `RecommendationReason`
- `ExportRecommendation`
- `CampaignRecommendation`
- `PrioritizationInputs`
- `SourceGroupInfo`

### Suggested Rust types

```rust
pub enum RecommendationReasonKind {
    SmallTable,
    LargeTable,
    HugeTable,
    StrongCursor,
    WeakCursor,
    NoTimeCursor,
    SparseRangeRisk,
    ChunkingHeavy,
    ReconcileRequired,
    FreshnessHintRecent,
    SharedSourceHeavyConflict,
    LowConfidenceMetadata,
}

pub struct RecommendationReason {
    pub kind: RecommendationReasonKind,
    pub message: String,
}

pub enum CostClass {
    Low,
    Medium,
    High,
    VeryHigh,
}

pub enum RiskClass {
    Low,
    Medium,
    High,
}

pub enum PriorityClass {
    Low,
    Medium,
    High,
}

pub struct ExportRecommendation {
    pub export_name: String,
    pub priority_score: i32,
    pub priority_class: PriorityClass,
    pub cost_class: CostClass,
    pub risk_class: RiskClass,
    pub recommended_wave: u32,
    pub isolate_on_source: bool,
    pub reasons: Vec<RecommendationReason>,
}

pub struct CampaignRecommendation {
    pub ordered_exports: Vec<ExportRecommendation>,
    pub waves: Vec<RecommendedWave>,
    pub source_group_warnings: Vec<String>,
}

pub struct RecommendedWave {
    pub wave: u32,
    pub exports: Vec<String>,
}
```

### Touchpoints
- `src/plan/`

### Tasks
- add new types
- define serialization shape for JSON output
- define pretty-print output requirements

---

## Stage 1 — Prioritization input model

### Objectives
Build a normalized input model from existing planning and preflight signals.

### Reuse from current codebase
Reuse existing signals where possible:
- row estimate
- strategy
- chunk count
- warnings
- sparse-range diagnostics
- recommended profile
- source config

### New input model
Introduce:

```rust
pub struct PrioritizationInputs {
    pub export_name: String,
    pub source_group: Option<String>,
    pub strategy: StrategyKind,
    pub estimated_rows: Option<u64>,
    pub estimated_size_bytes: Option<u64>,
    pub chunk_count: Option<u32>,
    pub sparse_range_risk: bool,
    pub cursor_quality: CursorQuality,
    pub reconcile_required: bool,
    pub source_freshness_hint: Option<SourceFreshnessHint>,
}
```

### Needed supporting enums
- `CursorQuality`
- `SourceFreshnessHint`

### Touchpoints
- `src/plan/`
- `src/preflight/`

### Tasks
- derive prioritization inputs from current plan/preflight outputs
- represent missing metadata explicitly
- avoid reading runtime state here in v1

---

## Stage 2 — Per-export scoring engine

### Objectives
Produce deterministic recommendations for one export.

### Scoring dimensions
Initial rule-based dimensions:
- freshness value
- extraction cost
- source pressure risk
- cursor reliability risk
- sparse-range penalty
- reconcile urgency

### Example rule ideas
- small full export + strong cursor → early wave
- huge sparse chunked export → later wave
- weak cursor + large table → higher risk
- reconcile-required export → later or isolated unless explicitly high value
- recent freshness hint + low cost → early wave

### Deliverables
- score calculator
- class assignment
- reason generation

### New module
- `src/plan/recommend.rs`

### Tasks
- implement `recommend_export(inputs) -> ExportRecommendation`
- encode reasons explicitly
- add unit tests for known patterns

### Tests to add
- tiny snapshot table
- medium incremental table
- huge sparse chunked table
- weak cursor table
- reconcile-required table
- low-confidence metadata table

---

## Stage 3 — Source group hints

### Objectives
Allow the engine to emit advice for shared sources.

### Product boundary
Still advisory only.
No scheduling, no runtime coordination.

### New config support
Optional field:
```yaml
source_group: replica_a
```

### Deliverables
- detect exports sharing one source group
- identify heavy exports on same group
- emit warnings:
  - avoid running together
  - isolate this export
  - heavy-on-shared-source warning

### Touchpoints
- `src/config/`
- `src/plan/`
- `src/preflight/`

### Tasks
- add optional `source_group` to config model
- pass source-group data into prioritization inputs
- implement simple same-group conflict heuristics

---

## Stage 4 — Campaign recommendation layer

### Objectives
Turn per-export recommendations into campaign-level output.

### Deliverables
- ordered export list
- wave assignment
- source-group summary
- heavy export isolation hints

### New module
- `src/plan/campaign.rs`

### Suggested API
```rust
pub fn recommend_campaign(
    exports: Vec<ExportRecommendation>
) -> CampaignRecommendation
```

### Example wave rules
- Wave 1: cheap / high-value / low-risk
- Wave 2: standard medium-cost
- Wave 3: heavy or sparse
- Wave 4: isolate / reconcile-heavy / weak-source risky

### Touchpoints
- `src/plan/`
- `src/pipeline/`

### Tasks
- sort exports by priority score
- assign waves
- emit source-group warnings
- define stable pretty output

---

## Stage 5 — CLI integration

### Objectives
Make this visible in normal planning flow.

### Deliverables
- prioritization section in `rivet plan`
- optional machine-readable JSON fields
- optional campaign view when planning multiple exports

### CLI behavior
Recommended:
- keep current `plan` behavior intact
- add recommendation block when available
- if multiple exports are planned, show grouped wave summary

### Touchpoints
- `src/pipeline/`
- maybe command rendering helpers

### Tasks
- integrate recommendation rendering into plan output
- integrate JSON serialization into plan artifact or plan result output
- add snapshot tests / golden tests for CLI output

---

## Stage 6 — Docs and rollout

### Deliverables
- update roadmap
- add user docs for prioritization
- document limits:
  - advisory only
  - metadata confidence matters
  - not a scheduler

### Docs to update
- `rivet_roadmap.md`
- `docs/getting-started.md` (light mention only)
- `docs/reference/cli.md`
- new doc: `docs/planning/prioritization.md`

### Messaging
Key message:
> Rivet helps decide what to run first and what to isolate on shared sources.

---

## Stage 7 — Pilot validation

### Objectives
Test whether the recommendations are actually useful.

### What to validate
- do users agree with ordering?
- do users trust the reasons?
- do source-group hints feel realistic?
- do operators change behavior based on output?

### Pilot scenarios
- 20-export mixed workload
- 50-export mixed workload
- shared source host
- sparse huge table present
- mixed cursor quality

### Exit criteria
- recommendation logic helps at least some real campaigns
- false positives and false negatives are documented
- top tuning adjustments are identified

---

## Deferred work

### Not in v1
- historical runtime-based scoring
- automatic run reordering
- internal queueing
- dynamic source throttling
- business criticality overrides
- targeted repair integration
- stronger state-based campaign refinement

---

## Module impact summary

### Low impact
- `src/destination/`
- `src/format/`
- file writing
- retry logic
- apply execution

### Medium impact
- `src/plan/`
- `src/preflight/`
- `src/pipeline/`
- `src/config/`

### High-sensitivity, later only
- `src/state/`

---

## Recommended implementation order

1. contracts and types
2. prioritization input derivation
3. per-export scoring
4. source-group hints
5. campaign recommendation
6. CLI integration
7. docs
8. pilot tuning
