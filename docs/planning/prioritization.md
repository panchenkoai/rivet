# Source-Aware Extraction Prioritization

> See also: [ADR-0006 — Source-Aware Extraction Prioritization](../adr/0006-source-aware-prioritization.md).

Rivet helps decide **what to extract first**, **what to delay**, and **what to isolate** on shared source hosts. This is an **advisory** planning feature — it does not schedule runs, change execution, or throttle workers.

---

## What you get

For each export, `rivet plan` computes and embeds:

- `priority_score` (0..100) — deterministic rule-based rank.
- `priority_class` — `low` / `medium` / `high`.
- `cost_class` — `low` / `medium` / `high` / `very_high`.
- `risk_class` — `low` / `medium` / `high`.
- `recommended_wave` — integer 1..4 grouping exports by urgency + cost.
- `reasons[]` — structured, explainable reasons (`small_table`, `weak_cursor`, `sparse_range_risk`, `reconcile_required`, …).
- `isolate_on_source` — set when a shared `source_group` has several heavy exports.

For a multi-export `rivet plan` invocation the same artifact also contains a campaign view:

- `ordered_exports` — sorted by `priority_score` (descending), tie-broken by name.
- `waves[]` — exports grouped by `recommended_wave`.
- `source_group_warnings[]` — human-readable warnings about shared-source collisions.

---

## Inputs (how the score is built)

| Signal | Source |
|---|---|
| Row estimate | Preflight `EXPLAIN` |
| Chunk count | Computed at plan time for chunked exports |
| Strategy | Resolved `ExtractionStrategy` |
| Cursor quality | Preflight index use + min/max range; see [ADR-0007](../adr/0007-cursor-policy-contracts.md) |
| Sparse-range risk | Preflight warnings |
| Reconcile required | `reconcile` CLI flag or `reconcile_required: true` in config |
| Source freshness | Heuristic for short `time_window` exports |
| Source group | `source_group:` in `exports[]` config |
| **History** (Epic I) | Last ~20 rows of `export_metrics` — retry rate, recent failure, avg duration |

Missing signals (e.g. preflight failed) lower confidence explicitly — the recommendation is never silently "strong" on weak data.

### Historical refinement (Epic I)

When prior runs exist, `rivet plan` folds them into the score with **bounded contribution** (at most ~15 points across all three historical reasons combined) so history cannot override preflight:

| Condition | Penalty | Reason |
|---|---|---|
| Most recent run failed | −8 | `recent_failure_history` |
| Retry rate > 0.3 over ≥3 runs | −5 | `high_retry_rate_history` |
| Average duration ≥ 5 min over ≥3 runs | −4 | `slow_history` |

History is ignored when sample size is too small or when `state.get_metrics` is unavailable — the advisory never becomes louder than the data allows.

---

## Enabling shared-source awareness

Mark exports that share a single replica/host with `source_group`:

```yaml
exports:
  - name: orders
    source_group: replica_a
    mode: incremental
    cursor_column: updated_at
    ...

  - name: events
    source_group: replica_a
    mode: chunked
    chunk_column: id
    ...

  - name: users_dim
    mode: full          # no source_group — won't participate in group warnings
    ...
```

If two or more members of the same group land in the **heavy** cost classes, Rivet flags the group and marks each heavy member as `isolate_on_source: true` in the artifact.

---

## Viewing the output

**Pretty (default)** — `rivet plan --config ...` prints a `Priority` block per export and a `Campaign` block when multiple exports are planned:

```
  Priority   : score 72 — High (wave 2)
  Prioritize :
    • [large_table] Medium/large estimated row count (~8000000).
    • [chunking_heavy] Chunked extraction (12 chunk windows) — higher source load and runtime.
    • [shared_source_heavy_conflict] Shared source group 'replica_a' has multiple heavy exports — run this export alone on that source.
  Campaign   :
    • Source group 'replica_a': 2 heavy-cost exports (orders, events) — avoid running them concurrently; stagger or isolate.
```

**JSON artifact** — `rivet plan --format json --output plan.json ...` embeds the full `prioritization` object (per-export recommendation plus the campaign view).

---

## Design principles

1. **Advisory, not authoritative.** Recommendations guide operators and external orchestrators; they do not change what Rivet executes.
2. **Explainability first.** Every recommendation carries a list of structured reasons; nothing is score-only.
3. **Metadata is signal, not truth.** When preflight fails or metadata is missing, the `low_confidence_metadata` reason is attached and scores move toward neutral.
4. **Graceful degradation.** Weaker inputs → weaker (never stronger) recommendations.

---

## Out of scope for v1

- Runtime scheduling / queueing / throttling.
- Historical runtime-informed refinement (duration, retries, throughput).
- Automatic execution reordering based on the campaign view.
- Business-criticality overrides.

See the planning documents for the full roadmap:

- [rivet-roadmap-source-aware-planning-patch.md](rivet-roadmap-source-aware-planning-patch.md)
- [source-aware-planning-work-order.md](source-aware-planning-work-order.md)
- [source-aware-prioritization-implementation-plan.md](source-aware-prioritization-implementation-plan.md)
