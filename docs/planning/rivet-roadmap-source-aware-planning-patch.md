# Roadmap Patch: Source-Aware Planning Features

## Epic A — Source-Aware Extraction Prioritization
**Priority:** P1

### Goal
Recommend what to extract first, what to delay, and what to isolate when many exports share one source.

### Deliverables
- per-export priority score
- cost/risk classes
- reasons
- recommended wave
- source-group warnings
- heavy-export isolation hints
- plan output integration

### Main modules
- `src/plan/`
- `src/preflight/`
- `src/pipeline/`

### Disruption
Low to Medium

---

## Epic B — Metadata-Driven Discovery and Catalog Bootstrap
**Priority:** P1

### Goal
Use source metadata to discover tables, infer cursor candidates, suggest strategies, and bootstrap export registry.

### Deliverables
- discovery artifact
- cursor candidates
- suggested mode
- table-scale metadata
- generated export skeletons

### Main modules
- `src/init/`
- `src/preflight/`
- `src/config/`
- `src/plan/`

### Disruption
Low to Medium

---

## Epic C — Weak-Source Awareness
**Priority:** P1

### Goal
Classify tables/exports by source trust level and surface weak-source warnings.

### Deliverables
- weak-source classification
- weak-cursor warnings
- reconcile-required hint
- confidence-aware recommendation inputs

### Main modules
- `src/plan/`
- `src/preflight/`
- `src/config/`

### Disruption
Low to Medium

---

## Epic D — Cursor Policy Model
**Priority:** P2

### Goal
Move from single cursor field to richer cursor policy semantics.

### Deliverables
- primary/fallback cursor support
- cursor quality classification
- richer config model
- planning integration

### Main modules
- `src/config/`
- `src/plan/`
- `src/source/`
- `src/preflight/`

### Disruption
Medium

---

## Epic E — Campaign-Level Planning
**Priority:** P2

### Goal
Turn many exports into one explainable extraction campaign.

### Deliverables
- ordered export list
- grouped waves
- source-group summaries
- campaign-level warnings

### Main modules
- `src/plan/`
- `src/pipeline/`

### Disruption
Medium

---

## Epic F — Partition/Window Reconciliation
**Priority:** P2

### Goal
Support count/hash verification per partition or time window.

### Deliverables
- partition-level compare
- mismatch reports
- repair candidates

### Main modules
- `src/pipeline/`
- `src/plan/`
- `src/state/`
- `src/source/`

### Disruption
Medium

---

## Epic G — Committed Export Boundary Semantics
**Priority:** P2

### Goal
Separate source observation from committed export progression truth.

### Deliverables
- stronger state semantics
- last committed boundary
- last verified boundary
- clearer progression reporting

### Main modules
- `src/state/`
- `src/plan/`
- `src/pipeline/`

### Disruption
Medium to High

---

## Epic H — Targeted Repair Workflow
**Priority:** P3

### Goal
Repair only affected partitions/windows instead of rerunning everything.

### Deliverables
- repair plan
- partition/window re-export
- repair-oriented output

### Main modules
- `src/pipeline/`
- `src/state/`
- `src/plan/`

### Disruption
Medium to High

---

## Epic I — Historical Recommendation Refinement
**Priority:** P3

### Goal
Use real runtime history to improve planning recommendations.

### Deliverables
- duration-based refinements
- retry/failure-aware tuning
- throughput-informed hints

### Main modules
- `src/state/`
- `src/plan/`

### Disruption
Medium
