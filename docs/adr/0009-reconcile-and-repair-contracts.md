# ADR-0009: Reconcile and Targeted Repair Contracts

- **Status:** Accepted
- **Date:** 2026-04-18
- **Context:** Epic F introduces partition-level reconciliation (`rivet reconcile`), Epic H introduces targeted repair (`rivet repair`). Both operate on completed chunked runs, produce structured reports, and interact with the progression table (ADR-0008) and the plan/apply channel (ADR-0005). The contracts below make the workflow auditable and composable with external tooling.

---

## Scope

| Command | Input | Output | Side effects |
|---|---|---|---|
| `rivet reconcile -c -e` | Latest `chunk_run` + committed files (manifest) | `ReconcileReport` (pretty or JSON) | May advance `last_verified_*` (ADR-0008 PG5) |
| `rivet repair -c -e [--report … \| --auto] [--execute]` | `ReconcileReport` (file or fresh) | `RepairPlan` (without `--execute`) or `RepairReport` (with `--execute`) | With `--execute`: new output files; manifest entries; no cursor/commit changes |

---

## Contract matrix

| ID | Name | Statement | Enforced by |
|----|------|-----------|-------------|
| **RC1** | Reconcile requires a committed chunk run | `rivet reconcile` bails when no `chunk_run` exists for the export. Operator must run the chunked export with `chunk_checkpoint: true` first. | `reconcile_chunked_inner` — `get_latest_chunk_run` |
| **RC2** | Partition SQL parity with extraction | The per-partition source `COUNT(*)` is built from the exact same `build_chunk_query_sql` shape used during extraction — same `WHERE`, same dense/range/by-days branch, same identifier quoting. | `reconcile_chunked_tasks` |
| **RC3** | Per-partition classification | Each `chunk_task` is classified as `match` (counts equal), `mismatch` (both counts known and differ), or `unknown` (either count missing). Unknown is always a repair candidate. | `PartitionResult::classify` |
| **RC4** | Reconcile scope v1 | Only **chunked** exports. `time_window` bails with a clear "use `chunk_by_days`" message; snapshot / incremental receive "use `rivet run --reconcile`". | `reconcile_cmd::run_reconcile_command` |
| **RC5** | Report shape stability | `ReconcileReport` JSON fields (`export_name`, `run_id`, `strategy`, `partitions[]`, `summary`) form a stable schema; new fields are additive only. | `plan::reconcile`, serde defaults |
| **RC6** | Verified advances only on full match | Verified boundary (ADR-0008 PG5) is written **iff** `summary.mismatches == 0 && summary.unknown == 0`. | `reconcile_cmd::reconcile_chunked` |
| **RR1** | Repair derives only from reconcile | `RepairPlan::from_reconcile` is the **single** path that produces repair actions — no direct config paths, no operator-typed ranges. | `plan::repair::RepairPlan::from_reconcile` |
| **RR2** | Plan before execute | Without `--execute`, `rivet repair` prints the plan and exits; no source queries are issued, no files are written. | `repair_cmd::run_repair_command` |
| **RR3** | Repair SQL parity | Repair chunk queries use the **same** `build_chunk_query_sql` as extraction and reconcile — repair is apples-to-apples with the original run. | `run_chunked_sequential(ChunkSource::Precomputed)` |
| **RR4** | Committed boundary not moved by repair | Repair re-exports chunks already covered by committed progression; `last_committed_*` is **not** re-stamped by repair. Operator advances verified by running `rivet reconcile` afterwards. | `repair_cmd::execute_repair` (no `record_committed_*` call) |
| **RR5** | Destination files are additive | Repair writes **new** files alongside originals with the same `<export>_<ts>_chunk<idx>.<ext>` naming; Rivet does not delete or overwrite prior files. Downstream dedup is the operator's responsibility. | `run_chunked_sequential` file naming |
| **RR6** | Unparseable identifiers are skipped | Partitions whose identifier does not match `"chunk N [start..end]"` with parseable `i64` bounds are recorded in `skipped[]` — never silently dropped, never executed. | `RepairAction::from_identifier`, `execute_repair` |
| **RR7** | Strategy scope v1 | Repair requires `mode: chunked`. Other modes bail with a clear error (same policy as reconcile scope). | `repair_cmd::run_repair_command` |
| **RR8** | Report shape stability | `RepairPlan` / `RepairReport` JSON is a stable additive schema (same policy as RC5). | `plan::repair`, serde defaults |

---

## Interaction with other ADRs

| ADR | Interaction |
|---|---|
| **ADR-0001** (state invariants) | Reconcile does **not** write to state beyond progression (ADR-0008). Repair runs `run_chunked_sequential` which honors I1–I4 for the files it produces. |
| **ADR-0005** (plan/apply) | A reconcile or repair-report JSON is a peer of `PlanArtifact`: sealed, reviewable, auditable. No staleness check — reports are snapshots, not execution gates. |
| **ADR-0006** (prioritization) | Reconcile outcomes are not (yet) fed into prioritization; Epic I uses `export_metrics`, not reconcile reports. |
| **ADR-0007** (cursor policy) | Reconcile is chunked-only in v1 and does not touch incremental cursors. Coalesce mode is unaffected. |
| **ADR-0008** (progression) | RC6 is the sole writer of `last_verified_*`; RR4 documents that repair leaves `last_committed_*` untouched. |

---

## Failure map

| Scenario | Reconcile behavior | Repair behavior |
|---|---|---|
| No `chunk_run` for export | Bails (RC1) | Bails (RC1 via fresh reconcile) or error loading report |
| Non-chunked export | Bails (RC4) | Bails (RR7) |
| Source unreachable | Bails at first `query_scalar` | Bails at `source::create_source` before any chunk runs |
| Partition count mismatch | Partition marked `mismatch`; verified not advanced (PG5) | Repair action generated; user decides to `--execute` |
| Chunk task never completed | Partition marked `unknown`; verified not advanced | Repair action generated |
| Unparseable chunk keys | Partition marked `unknown` (source count not attempted) | Skipped with note (RR6) |

---

## Workflow example

```bash
# 1. Run chunked export with checkpoint
rivet run -c cfg.yaml

# 2. Reconcile — advances `last_verified_*` if all match
rivet reconcile -c cfg.yaml -e orders --format json -o reconcile.json

# 3. Repair plan (dry-run)
rivet repair -c cfg.yaml -e orders --report reconcile.json

# 4. Execute repair
rivet repair -c cfg.yaml -e orders --report reconcile.json --execute

# 5. Reconcile again to advance `last_verified_*`
rivet reconcile -c cfg.yaml -e orders
```

---

## Out of scope (v1)

- `time_window` and `incremental` per-partition reconcile.
- Automatic repair execution (always opt-in via `--execute`).
- Repair that rewrites or deletes prior destination files.
- Hash-based partition verification (current v1 is `COUNT(*)` only).
- Repair advancing `last_committed_*` (documented non-goal: commit = first successful extraction; repair is corrective, not commitment).

---

## Test coverage

- `plan::reconcile::tests` — classification, summary, round-trip JSON.
- `plan::repair::tests` — identifier parsing, action derivation, summary counts.
- `pipeline::reconcile_cmd::tests` — stubbed source closure exercises the full `reconcile_chunked_tasks` path without a DB.
- `pipeline::repair_cmd::tests` — smoke test for the reconcile → plan derivation path.
