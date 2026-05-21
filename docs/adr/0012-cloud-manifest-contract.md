# ADR-0012: Cloud Manifest Contract

**Status**: Proposed
**Date**: 2026-05-21
**Context**: Rivet 0.7.0 introduces a public JSON manifest as the trust contract for cloud-output runs (local / S3 / GCS). The manifest is the operator-visible record of what was written, and the input to resume, validation, and reconciliation. Its invariants must be locked before the writer, the resume logic, and the verification extensions are coded — otherwise we will rewrite them.

This ADR defines those invariants. The shipping target is Rivet 0.7.0. Schema v8 has already reclaimed the `manifest` name by renaming the internal SQLite ledger to `file_log` (ADR refs: see CHANGELOG 0.6.1).

---

## Goals

1. Resume-aware cloud output: a re-run can decide, per part, whether to skip, rewrite, or quarantine.
2. Trust verdict: `--validate` and `--reconcile` can give an unambiguous pass/fail by inspecting only the manifest and the destination (no local state required).
3. Legacy compatibility: pre-0.7.0 runs work as-is, with no migration of in-flight state. See M6.
4. Backend portability: identical semantics on local FS, S3-compatible, and GCS.

## Non-goals

1. Cross-engine column-level encryption metadata (a separate ADR if/when encryption ships).
2. Schema evolution between successive runs of the same export (out of scope — schema_changed/fingerprint live in the run report, not in the manifest contract).
3. A new `rivet verify` subcommand — explicitly rejected. The verdict is surfaced via the existing `--validate` / `--reconcile` / `--report` flags.

---

## Artifacts

For every export run targeting a cloud or local-file destination, Rivet writes:

```
<destination_uri>/<export_layout>/
  part-000001.<format>
  part-000002.<format>
  ...
  manifest.json
  _SUCCESS                     # only if the run completed cleanly
```

`<export_layout>` is the destination-config-provided layout, typically `<schema>.<table>/` and optionally namespaced by `run_id`. Layout policy is destination-config concern, not part of this ADR.

`manifest.json` is the authoritative record of the run for this export. Its schema is versioned (see "Versioning") and stable across patch releases.

`_SUCCESS` is a zero-byte marker. Its only meaning is "the manifest at this prefix represents a fully-committed run". Its existence implies the manifest exists, and every part the manifest references also exists at the recorded byte length.

---

## Invariants

These extend [ADR-0001](0001-state-update-invariants.md) (I1–I7) into the cloud-destination plane.

### M1 — Parts Before Manifest (PBM)

> The manifest is written only after every part it references has been committed to the destination.

Rationale: A manifest pointing at a part that was never uploaded is a phantom record — worse than no manifest, because resume logic would skip work that wasn't actually done.

Failure mode if violated: resume skips real work; `--validate` falsely reports completeness.

Recovery: a process killed between part-upload and manifest-write leaves the destination without a manifest. Resume detects "parts present, manifest absent" and re-derives the run state by listing parts (see M6 / M8).

### M2 — Manifest Before SUCCESS (MBS)

> `_SUCCESS` is written only after the manifest has been written and is readable at its destination URI.

Rationale: `_SUCCESS` is the single observable signal an external orchestrator (Airflow, Dagster, CI) can poll to decide "data is ready". If `_SUCCESS` could appear before the manifest, downstream consumers reading the manifest would race.

Recovery: a process killed between manifest-write and `_SUCCESS`-write leaves the prefix with a manifest but no `_SUCCESS`. Resume treats this as "candidate complete; re-verify before finalizing". Re-verification reads the manifest, checks each part, and writes `_SUCCESS` only if all checks pass.

### M3 — Part Identity Triple (PIT)

> Every part referenced by the manifest is uniquely identified by `(path, size_bytes, content_fingerprint)`.

The triple is recorded for each part. On resume, a part is considered the same as the manifested part if and only if all three components match. A part whose path matches but whose size or fingerprint differs is treated as corrupt or stale and quarantined (M9).

`content_fingerprint` is xxh3_64 over the part body. xxh3 was chosen because the codebase already depends on `xxhash-rust` and it is fast enough to compute alongside the write.

For 0.7.0, fingerprint is mandatory for new manifests. Pre-0.7.0 runs have no fingerprint and fall under M6.

### M4 — Manifest Is Append-Only Per Run

> A given `run_id` produces exactly one manifest. The manifest is never amended in place — a resumed run that completes additional parts writes a fresh manifest atomically (write-then-rename on local; atomic PUT on S3/GCS).

Rationale: partially-written manifests must be impossible to observe. Object stores give per-object atomicity for PUT/upload; local FS gets the same via `write-temp-then-rename`.

Resume across multiple interruptions does not produce multiple manifests for the same run — the latest write supersedes.

### M5 — SUCCESS Implies Verifiability

> If `_SUCCESS` exists, then for every part listed in the manifest, the part is present at the destination at the recorded byte length.

This is the contract `--validate` checks on the metadata-only path: it lists the prefix, reads the manifest, and verifies M5 part-by-part. It does **not** require fingerprint re-check — that is the heavier-weight `--validate --deep` (future flag, not in 0.7.0).

`--reconcile` adds: row counts in the manifest sum to the source `COUNT(*)` for the export's row range.

### M6 — Legacy Output Is Labeled, Not Migrated

> Runs that completed before 0.7.0 (no manifest at the destination prefix) are not migrated. Operations on legacy prefixes succeed with reduced guarantees and **must** emit an explicit `legacy_run: true` label in operator-facing output.

Per the project decision documented in
[project-roadmap-0-7-0-legacy-runs](../../.claude/projects/-Users-andriipanchenko-rivet/memory/project_roadmap_0_7_0_legacy_runs.md):

- `--resume` on a legacy prefix uses the pre-0.7.0 file-log-based logic; no manifest-aware skip.
- `--validate` on a legacy prefix falls back to local-file row-count checks; manifest/M5 checks are skipped and reported as such.
- `--reconcile` on a legacy prefix uses source-COUNT vs file-log only; the "manifest part-count match" line is omitted.

Silent fallback is forbidden. Every reduced check must say so in the report.

### M7 — Manifest Atomicity

> The manifest write is observable atomically: a reader either sees the previous state (manifest absent, or older manifest from a prior superseded run) or the new complete manifest. A partially-written manifest is unreachable to readers.

Local FS: write `manifest.json.tmp` then `rename` to `manifest.json` (POSIX `rename(2)` is atomic on the same filesystem).

S3 / GCS: write `manifest.json` as a single PUT / upload. Object stores guarantee write-completes-or-fails-with-no-trace.

Multipart uploads MUST NOT be used for the manifest itself — only the data parts. The manifest stays small enough (KB to single MB) that single-PUT suffices and side-steps the multipart abort/cleanup story.

### M8 — Resume Decisions Are Deterministic

> Given the same destination prefix and the same source/cursor state, `--resume` makes identical decisions on every run.

Decision matrix per part name:

| Manifest entry | Object present | Size matches | Fingerprint matches | Decision |
|---|---|---|---|---|
| yes | yes | yes | yes | skip (committed) |
| yes | yes | yes | no  | quarantine (M9) |
| yes | yes | no  | —   | quarantine (M9) |
| yes | no  | —   | —   | rewrite (lost) |
| no  | yes | —   | —   | quarantine (M9) — untracked artifact |
| no  | no  | —   | —   | new — write |

`_SUCCESS` present + no `--force` → refuse to start (operator must opt in to overwrite a successful run).

### M9 — Untracked / Corrupt Parts Are Quarantined, Not Deleted

> When resume finds an unknown or fingerprint-mismatch part, it is moved to a quarantine prefix and a warning is emitted. Rivet does not delete unknown objects.

Quarantine layout: `<prefix>/_quarantine/<run_id>/<original-name>`.

Rationale: defensive. The unknown part may be the operator's own intentional artifact, or evidence of a bug. Either way, Rivet preserves it and shifts the cost of cleanup to the operator's decision.

If the destination backend does not support cheap move (S3 cross-prefix copy + delete is two operations), the operation is best-effort: failure to move is logged but does not block the run. The unknown object stays in place and the run still proceeds, but the warning escalates.

---

## Manifest schema (v1)

Field additions are backwards-compatible (consumers ignore unknowns). Field removals or type changes require a `manifest_version` bump.

```json
{
  "manifest_version": 1,
  "run_id": "orders_20260521T120000.000",
  "export_name": "public.orders",
  "started_at": "2026-05-21T12:00:00.000Z",
  "finished_at": "2026-05-21T12:14:33.412Z",
  "status": "success",
  "source": {
    "engine": "postgres",
    "schema": "public",
    "table": "orders"
  },
  "destination": {
    "kind": "gcs",
    "uri": "gs://rivet-exports/public.orders/run_20260521T120000/"
  },
  "format": "parquet",
  "compression": "zstd",
  "schema_fingerprint": "xxh3:7f3a91be...",
  "row_count": 2001291,
  "part_count": 41,
  "parts": [
    {
      "part_id": 1,
      "path": "part-000001.parquet",
      "rows": 50000,
      "size_bytes": 123456789,
      "content_fingerprint": "xxh3:8a44e2c1...",
      "status": "committed"
    }
  ]
}
```

`path` is relative to the destination prefix so the manifest is portable across copies of the same dataset.

`source.schema` / `source.table` capture the logical name; the resolved SQL is *not* embedded — the manifest is about the output, not the extraction strategy. The run report (`.rivet/runs/<run_id>/summary.json`) carries the plan-side details.

`schema_fingerprint` is xxh3_64 over a canonical serialization of `[{name, type}]` from the existing `state::SchemaColumn` array. The fingerprint format prefix (`xxh3:`) is reserved so future fingerprint algorithms can coexist.

`status` per part: `committed` (in this manifest) or `quarantined` (the part listed in a prior superseded manifest that resume found corrupted; retained for audit).

---

## What this does NOT define

- Per-column encryption metadata.
- Per-row provenance / lineage fingerprints.
- Cross-run incremental cursor state (lives in `export_state`, surfaced by `rivet state` / `rivet metrics`).
- Quotas, retention, or bucket policy.

These are intentionally outside the manifest. A manifest that tries to be a catalog will lose its trust-verdict role.

---

## Open questions deferred to implementation

1. **`_SUCCESS` body**: empty file (matches Hadoop/Spark convention) vs single line with the manifest fingerprint (lets a polling consumer detect manifest changes without re-reading). Lean toward empty for portability; revisit if a concrete consumer asks for the fingerprint.
2. **Per-table-per-run prefixing**: today the destination layout is config-driven. The manifest's `destination.uri` records what was used; layout policy stays in destination config, not in this ADR.
3. **Quarantine TTL**: Rivet does not delete quarantined objects. Operators may want a cleanup helper (`rivet state remote --gc`) — out of scope for 0.7.0.

---

## Test coverage plan

| Invariant | Test |
|---|---|
| M1 | kill after N parts, before manifest → resume re-derives state |
| M2 | kill after manifest, before `_SUCCESS` → resume reverifies and finalizes |
| M3 | corrupt one part (different fingerprint) → resume quarantines |
| M4 | resume followed by a second resume → exactly one manifest, latest run_id |
| M5 | `_SUCCESS` present but one part deleted → `--validate` FAILS |
| M6 | legacy prefix (no manifest) → operations succeed with `legacy_run: true` |
| M7 | concurrent read during manifest rename → never observes partial JSON |
| M8 | re-run after `_SUCCESS` without `--force` → refuses; with `--force` → restarts |
| M9 | unknown part in prefix → moved to `_quarantine/`, run proceeds |

Each invariant lands with at least one unit test (local FS, fast) and one integration test (S3-compat via MinIO or GCS-compat; nightly).
