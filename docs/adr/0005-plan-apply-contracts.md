# ADR-0005: Plan/Apply Contracts

**Status**: Accepted  
**Date**: 2026-04  
**Context**: Rivet implements a Terraform-style plan/apply workflow for data extracts. `rivet plan` generates a sealed `PlanArtifact` that captures the full execution intent at a point in time. `rivet apply` consumes the artifact and executes it. Because plan time and apply time are separated, the contracts between them must be explicit to ensure predictable, auditable execution.

---

## Problem

Separating planning from execution introduces a temporal gap between analysis and action:

- Data in the source may change between plan and apply.
- The cursor may advance (another incremental run completed).
- The config file may be edited.
- The artifact may be arbitrarily old.

Without explicit contracts, `rivet apply` cannot reason about whether the artifact is still valid to execute, and operators cannot reason about what guarantees the system provides.

---

## Contracts

### PA1 — Artifact Is the Communication Channel (ACC)

> A `PlanArtifact` is the sole input to `rivet apply`. Apply does not re-read the config file, re-run preflight queries, or re-compute chunk boundaries. Everything needed for execution is embedded in the artifact.

**Rationale**: Decoupling apply from config re-parsing enables apply to work from a sealed, auditable snapshot. The artifact can be stored as a CI artifact, committed to a PR, or reviewed before execution.

**Consequence**: Any change to config, queries, or chunk boundaries after `rivet plan` requires regenerating the artifact with a new `rivet plan` invocation. Applying a stale artifact against a changed config is detected by staleness checks (PA3) and fingerprint logging (PA6), not by a config re-parse.

---

### PA2 — Artifact Immutability (AI)

> A `PlanArtifact` file must not be modified after it is written by `rivet plan`. `rivet apply` treats the artifact as a sealed, read-only input.

**Rationale**: The artifact's `plan_id` and `created_at` field are set at plan time. Any post-generation modification breaks the audit trail and the staleness check. The artifact is a point-in-time snapshot, not a mutable config.

**Current implementation**: `PlanArtifact` is deserialized from the file at apply time. No write-back or in-place mutation occurs. The file on disk is never opened for writing by `apply_cmd`.

---

### PA3 — Staleness Boundary (SB)

> `rivet apply` enforces a maximum age between plan time and apply time:
>
> - **Age < 1 hour**: apply proceeds silently.
> - **1 hour ≤ age < 24 hours**: apply emits a `WARN` log and proceeds.
> - **Age ≥ 24 hours**: apply rejects the artifact with an error unless `--force` is passed.

**Rationale**: The value of a pre-computed plan degrades as the source drifts. An artifact that is hours old may describe chunk boundaries that are no longer representative of the current data distribution. The hard error threshold prevents accidentally applying a week-old plan that was forgotten in a directory.

**Current implementation**: `PlanArtifact::staleness(warn_after, error_after)` computes the age from `created_at` to `Utc::now()`. `apply_cmd::run_apply_command` calls this and either warns, bails, or proceeds. The `expires_at` field in the artifact JSON provides the deadline in a human-readable form.

**Test coverage**: `test staleness_fresh`, `test staleness_expired_artifact` in `plan/artifact.rs`.

---

### PA4 — Cursor Snapshot Integrity (CSI)

> For `Incremental` exports, `rivet apply` verifies that the cursor value in `StateStore` at apply time equals the cursor value captured in `ComputedPlanData.cursor_snapshot` at plan time. If they differ, apply rejects the artifact.

**Rationale**: If another `rivet run` completed between plan and apply, the cursor has advanced. Applying the artifact would re-extract rows that were already exported and written to the destination, producing duplicate output. The cursor snapshot check makes this divergence explicit rather than silently producing duplicates.

**Failure mode**: If cursor drift is detected and `--force` is not passed, apply exits with:
```
plan 'orders': cursor has drifted since plan was generated
  (plan snapshot: "2026-04-14T09:00:00Z", current: "2026-04-14T11:00:00Z")
Regenerate with `rivet plan` or pass --force to skip this check.
```

**Scope**: This check applies only to `Incremental` exports (where `cursor_snapshot` is non-`None`). Snapshot, TimeWindow, and Chunked exports have no cursor and are not subject to this check.

**Current implementation**: `apply_cmd::run_apply_command` calls `PlanArtifact::cursor_matches(current)`. `cursor_matches` returns `true` when `cursor_snapshot` is `None` (all non-incremental strategies).

**Test coverage**: `test cursor_matches_none_snapshot`, `test cursor_matches_incremental` in `plan/artifact.rs`.

---

### PA5 — Chunk Range Monotonicity (CRM)

> Chunk ranges stored in `ComputedPlanData.chunk_ranges` must satisfy:
>
> 1. Each range `(start, end)` satisfies `start ≤ end`.
> 2. Consecutive ranges `(s1, e1)` and `(s2, e2)` satisfy `s2 = e1 + 1` (no gaps, no overlaps).
>
> An empty `chunk_ranges` is valid for non-Chunked strategies.

**Rationale**: These invariants mirror `generate_chunks` output guarantees (see `math.rs`). At apply time, ranges are replayed as `ChunkSource::Precomputed` and passed directly to `build_chunk_query_sql` without re-validation. A non-monotonic or overlapping range would produce incorrect `WHERE` predicates.

**Current implementation**: `detect_and_generate_chunks` produces ranges via `generate_chunks` which guarantees monotonicity by construction. The artifact round-trips ranges through JSON without mutation.

**Future**: An explicit `PlanArtifact::validate_chunk_ranges()` method that enforces this invariant before execution is a candidate for a future hardening pass.

---

### PA6 — Fingerprint Stability (FS)

> For Chunked exports, the `plan_fingerprint` field is computed at plan time from `(base_query, chunk_column, chunk_size, dense, by_days)` via `chunk_plan_fingerprint`. This fingerprint is embedded in the artifact and logged at apply time for operator visibility.

**Rationale**: If the config changes between plan and apply (query rewritten, chunk_size changed), the fingerprint computed from the new config would differ from the artifact's fingerprint. This mismatch is an operator signal that the artifact may no longer represent the intended extraction, even if apply can technically proceed.

**Current behavior**: The fingerprint is logged but not enforced as a hard gate. The operator is responsible for regenerating the plan when the config changes.

**Alignment with ADR-0001 I5**: The chunk checkpoint system (`chunk_run.plan_hash`) enforces fingerprint matching at resume time. The plan artifact fingerprint is a complementary audit signal, not a resume gate.

---

### PA7 — State Writes Unchanged (SWU)

> `rivet apply` uses the same state persistence paths as `rivet run`. File manifest, cursor updates, and run metrics are written via `StateStore` using the same invariants defined in ADR-0001.

**Rationale**: The artifact replaces chunk boundary detection only. It does not change the semantics of state persistence. All ADR-0001 invariants (I1–I7) apply unchanged to an `apply` run.

**Consequence**: The `StateStore` file (`.rivet_state.db`) used by apply is resolved from the directory containing the plan file. Operators should ensure the plan file is placed adjacent to (or in the directory of) the original config so the same state database is used.

---

### PA8 — Diagnostics Are Advisory (DAA)

> Preflight diagnostics embedded in `PlanDiagnostics` — `verdict`, `warnings`, `recommended_profile` — are advisory metadata captured at plan time. They do not gate execution at apply time. A plan with `verdict: "Unsafe"` can be applied; the verdict is preserved for auditability.

**Rationale**: The decision to apply a plan is the operator's. A degraded verdict is information, not a veto. Vetoing at apply time would be surprising because the operator already reviewed the plan before deciding to apply.

**Contrast with plan validation**: `validate_plan(&plan)` (ADR-0003) is re-run at apply time with `Rejected` diagnostics as hard gates. Plan validation checks structural constraints (e.g., stdout + chunked). Preflight diagnostics check operational health (e.g., missing index). Only the former is enforced at apply time.

---

## Contract Summary Table

| ID | Name | Enforced? | On Violation |
|----|------|-----------|-------------|
| PA1 | Artifact Is the Communication Channel | yes | apply cannot run without an artifact |
| PA2 | Artifact Immutability | operator | undefined behavior if modified |
| PA3 | Staleness Boundary | yes (hard at 24 h) | `bail!` unless `--force` |
| PA4 | Cursor Snapshot Integrity | yes (Incremental only) | `bail!` — regenerate plan |
| PA5 | Chunk Range Monotonicity | by construction | no explicit runtime gate |
| PA6 | Fingerprint Stability | advisory (logged) | operator responsibility |
| PA7 | State Writes Unchanged | yes (ADR-0001) | same failure modes as `rivet run` |
| PA8 | Diagnostics Are Advisory | explicit non-enforcement | verdict visible in artifact; no gate |

---

## Interaction with Existing ADRs

| ADR | Interaction |
|-----|------------|
| ADR-0001 (State Update Invariants) | PA7 — state write ordering (I1–I7) applies unchanged to apply runs |
| ADR-0003 (Layer Classification) | `plan_cmd.rs` and `apply_cmd.rs` are coordinator-layer modules; they bridge plan, execution, and persistence exactly like `pipeline/mod.rs:run_export_job` |
| ADR-0004 (Destination Write Contracts) | PA7 — destination commit protocols apply unchanged; apply does not change write semantics |

---

## Failure Point Map

| Scenario | PA3 | PA4 | State after |
|----------|-----|-----|-------------|
| Plan generated, applied < 1h later | Fresh | Matches (if no other run) | Normal success |
| Plan generated, applied 2h later | Warn | Matches (if no other run) | Normal success with warning logged |
| Plan generated, other incremental run completed, apply attempted | Fresh | Drift detected → bail | No state written (apply never ran) |
| Plan generated, applied 25h later | Error | not checked | bail (unless --force) |
| Plan generated, applied with --force after 25h | Warn override | Checked | Proceeds; operator takes responsibility |

---

## Test Coverage

`plan/artifact.rs` tests: `round_trip_json`, `round_trip_chunked`, `staleness_fresh`, `staleness_expired_artifact`, `cursor_matches_none_snapshot`, `cursor_matches_incremental`.

PA5 structural coverage is provided by `pipeline/chunked/math.rs` tests (`test_generate_chunks`, `test_generate_chunks_exact`, `test_generate_chunks_empty`).
