# ADR-0008: Export Progression Boundaries (Committed / Verified)

- **Status:** Accepted
- **Date:** 2026-04-18
- **Context:** Epic G separates three distinct boundaries an operator may ask about: what was **observed** in the source, what was **committed** to the destination, and what was **verified** against the source after the fact. Earlier versions of Rivet conflated the first two under a single `export_state.last_cursor_value`. Epic F added reconcile reports but no persistent "verified" marker.

---

## Boundaries

| Boundary | Meaning | Source of truth |
|---|---|---|
| **Observed** | Seen in source during preflight / plan (row estimates, chunk min/max) | `ComputedPlanData` in `PlanArtifact`, `ExportDiagnostic` |
| **Committed** | Successfully exported to the destination (file durably written, manifest recorded) | `export_progression.last_committed_*` |
| **Verified** | Committed **and** reconciled — per-partition source/export counts all matched | `export_progression.last_verified_*` |

Observed is ephemeral (rebuilt each `rivet plan`); committed and verified are persistent.

---

## Contract matrix

| ID | Name | Statement | Enforced by |
|----|------|-----------|-------------|
| **PG1** | Cursor table unchanged | `export_state.last_cursor_value` remains the single execution cursor used by the `WHERE cursor > ?` predicate and by ADR-0005 PA4 (apply-time drift check). Epic G does **not** rename or repurpose it. | `state::cursor`, `apply_cmd` |
| **PG2** | Committed after destination write | Committed boundary is written **after** the existing state writes (manifest, cursor, schema) complete — never before. Failure of the progression update is logged and does not fail the pipeline. | `single.rs`, `chunked::record_chunked_commit` |
| **PG3** | Committed monotonicity (incremental) | When an incremental commit arrives with a cursor value lexicographically less than the stored committed cursor, the stored row is kept. Guards against accidental regressions from a stale worker or manual state edit. | `record_committed_incremental` (SQL `CASE`) |
| **PG4** | Committed per strategy | The committed row stores **either** `last_committed_cursor` (incremental) **or** `last_committed_chunk_index` (chunked), with `last_committed_strategy` as the discriminant. Switching modes replaces the row rather than mixing fields. | `record_committed_incremental`, `record_committed_chunked` |
| **PG5** | Verified ⇒ all partitions match | Verified is recorded **only** when `summary.mismatches == 0 && summary.unknown == 0` in a `ReconcileReport`. A partially-verified run does not advance `last_verified_*`. | `reconcile_cmd::reconcile_chunked` |
| **PG6** | Verified ≤ Committed | Verified is derived from the reconcile of a specific `run_id`, which can only advance after that run's commit. Reconcile refuses to run when `chunk_run` is missing (Epic F CC). | `reconcile_cmd`, by construction |
| **PG7** | Advisory only | Progression fields are **observational**. They do not gate `rivet run`, `rivet apply`, or `rivet reconcile`. Consumers: `rivet state progression`, external monitoring. | No execution paths consult `export_progression` |
| **PG8** | Progression survives schema changes | `export_progression` is keyed by `export_name`; column additions to the schema or changes to query SQL do not clear it. Operators drop progression explicitly via a state reset (future `rivet state reset-progression`). | DB key design |

---

## Schema (v4 migration)

```sql
CREATE TABLE export_progression (
    export_name TEXT PRIMARY KEY,
    last_committed_strategy TEXT,
    last_committed_cursor TEXT,
    last_committed_chunk_index INTEGER,
    last_committed_run_id TEXT,
    last_committed_at TEXT,
    last_verified_strategy TEXT,
    last_verified_cursor TEXT,
    last_verified_chunk_index INTEGER,
    last_verified_run_id TEXT,
    last_verified_at TEXT
);
```

Migration is additive (no drops, no data movement); pre-existing state DBs upgrade transparently via the existing versioned migration runner.

---

## Write points (v1)

| Strategy | Committed | Verified |
|---|---|---|
| Incremental (`single.rs`) | After `state.update(cursor)` succeeds, `record_committed_incremental` with `summary.run_id` | — (no partition model; reconcile is whole-export) |
| Chunked, `chunk_checkpoint: true` (sequential and parallel) | After `finalize_chunk_run_completed`, `record_committed_chunked` with `max(chunk_index) WHERE status='completed'` | `rivet reconcile` writes `record_verified_chunked` when the report has zero mismatches and zero unknowns |
| Chunked, no checkpoint | — in v1 | — in v1 |
| Snapshot / TimeWindow | — in v1 | — in v1 |

Chunked without checkpoint has no per-partition state to key progression on; adding it requires Epic F-style partition tracking (future work).

---

## Out of scope (v1)

- Partition-level committed boundary for **non-checkpoint** chunked runs.
- Snapshot / TimeWindow progression (no natural partitions).
- Programmatic monotonicity check for chunked commits across reruns (tracked by `run_id`; new runs overwrite).
- `rivet state reset-progression` subcommand (workaround: `sqlite3 .rivet_state.db 'DELETE FROM export_progression WHERE export_name = …'`).

---

## Observability

`rivet state progression [--export <name>]` prints a table with columns:

```
EXPORT         COMM MODE    COMMITTED                    COMMITTED AT             VERI MODE    VERIFIED
orders         chunked      chunk #41                    2026-04-18 12:20:15 UTC  chunked      chunk #41
events         incremental  2026-04-17T23:59:59Z         2026-04-18 00:02:11 UTC  -            -
```

JSON output for monitoring integrations is tracked as a follow-up; current consumers can parse the table or read the SQLite directly.

---

## Relation to other ADRs

- **ADR-0001** (state invariants) — progression writes happen after the existing ordered state writes, preserving I1–I4.
- **ADR-0005** (plan/apply) — PA4 cursor-drift check still uses `export_state.last_cursor_value`, not the progression table.
- **ADR-0006** (prioritization) — progression is a future input to Epic I (historical refinement), not used in v1 scoring.
- **ADR-0007** (cursor policy) — the single `cursor` string stored in the committed boundary carries the same semantics as the execution cursor; for `coalesce` mode it is `COALESCE(primary, fallback)`.
