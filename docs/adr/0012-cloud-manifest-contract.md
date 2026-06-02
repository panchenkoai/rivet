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

`_SUCCESS` body: a single line `xxh3:<16-hex>\n` carrying the manifest's content fingerprint (xxh3_64 over the exact bytes of `manifest.json`). This lets a polling consumer detect manifest changes (rerun, resume, repair) with a cheap `GET _SUCCESS` instead of re-reading the full manifest. The Hadoop empty-marker convention is **not** followed — Rivet does not target the Hadoop ecosystem and the fingerprint pays for itself the first time an Airflow sensor needs to distinguish "same successful run" from "new successful run at the same prefix".

### M3 — Part Identity Triple (PIT)

> Every part referenced by the manifest is uniquely identified by `(path, size_bytes, content_fingerprint)`.

The triple is recorded for each part. On resume, a part is considered the same as the manifested part if and only if all three components match. A part whose path matches but whose size or fingerprint differs is treated as corrupt or stale and quarantined (M9).

`content_fingerprint` is `xxh3_64` over the part body, formatted `"xxh3:<16-hex>"`. xxh3 was chosen because (a) the codebase already depends on `xxhash-rust`, (b) it streams at ~2 GB/s so the per-part cost is negligible against destination upload latency, and (c) **the manifest is a trust contract for integrity, not for security** — cryptographic hashes (sha256, blake3) are explicitly out of scope. The encryption / tamper-evidence track is deferred to a separate ADR if and when needed; until then, the `xxh3:` prefix in the on-wire format reserves the syntactic slot so a future cryptographic hasher can coexist without a schema break.

For 0.7.0, fingerprint is mandatory for new manifests. Pre-0.7.0 runs have no fingerprint and fall under M6.

### M4 — Manifest Is Append-Only Per Run

> A given `run_id` produces exactly one manifest. The manifest is never amended in place — a resumed run that completes additional parts writes a fresh manifest atomically (write-then-rename on local; atomic PUT on S3/GCS).

Rationale: partially-written manifests must be impossible to observe. Object stores give per-object atomicity for PUT/upload; local FS gets the same via `write-temp-then-rename`.

Resume across multiple interruptions does not produce multiple manifests for the same run — the latest write supersedes.

### M5 — SUCCESS Implies Verifiability

> If `_SUCCESS` exists, then for every part listed in the manifest, the part is present at the destination at the recorded byte length.

This is the contract `--validate` checks on the metadata-only path: it lists the prefix, reads the manifest, and verifies M5 part-by-part. The listing also carries each object's content MD5 (GCS `md5Hash`, S3/Azure single-PUT ETag), so `--validate` confirms **content**, not just size, with **no download** — the original "re-download to re-fingerprint" idea (`--validate --deep`) was rejected as wasteful. A part whose store gives no checksum (streamed multipart, local FS) verifies size-only; `exports[].verify: content` makes that a failure.

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

### M9 — Untracked / Corrupt Parts Are Quarantined Best-Effort, Never Deleted

> When resume finds an unknown or fingerprint-mismatch part, Rivet attempts to move it to a quarantine prefix and emits a warning. The move is **best-effort**: if it fails, the run still proceeds, the warning escalates, and the object stays where it was. Rivet never deletes unknown objects.

Quarantine layout: `<prefix>/_quarantine/<run_id>/<original-name>`.

Rationale — defensive: the unknown part may be the operator's own intentional artifact, or evidence of a bug. Either way, Rivet preserves it and shifts the cost of cleanup to the operator.

Rationale — best-effort: on S3 / GCS the move decomposes into copy + delete, two non-atomic operations. A partial failure (copy succeeds, delete fails; or copy fails outright on a permissions issue) must not abort an otherwise-recoverable run. The reported warning carries enough detail (source path, destination quarantine path, failure reason) for the operator to finish the move manually. If the move never happens, the untracked part remains in place and re-trips M9 on the next resume — that is acceptable; an unmovable artifact is not a correctness problem, just a clutter problem.

Local FS gets the same best-effort behaviour: `rename(2)` is atomic but can still fail (different mount point, permissions, file-in-use on Windows). The semantics are uniform across backends — never bail on a quarantine failure.

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

## Decisions locked at ADR review

These items were open in the first draft of this ADR; they are now decided.

1. **`_SUCCESS` body** — *decided: carries the manifest fingerprint.* See M2. A polling orchestrator can detect manifest changes between two successful runs (a rerun, a resume that completed, a repair) by reading the `_SUCCESS` body alone, without re-fetching the manifest. The Hadoop empty-marker convention is rejected — Rivet does not target the Hadoop ecosystem.
2. **Run-id segmentation in the destination prefix** — *decided: no automatic segmentation.* Rivet writes parts, manifest, and `_SUCCESS` directly under the operator-configured destination prefix. Two successive runs against the same prefix produce one observable dataset whose manifest reflects the latest run; the prior run's parts are reused (M8 skip), rewritten, or quarantined (M9) as the matrix dictates. Operators who want time-segregated historical runs include `{run_id}` (or `{date}`) in their destination URI themselves — that policy lives in the destination config, not in the manifest contract. Resume across overwrite is handled by the `_SUCCESS` gate plus `--force` (M8).
3. **Cryptographic / encryption-aware fingerprinting** — *decided: out of scope for 0.7.0.* See M3. The `xxh3:` prefix reserves the slot.

## Open questions deferred to implementation

1. **Quarantine TTL**: Rivet does not delete quarantined objects. Operators may want a cleanup helper (`rivet state remote --gc`) — out of scope for 0.7.0.

---

## Test coverage plan

| Invariant | Status (2026-05-21) | Test |
|---|---|---|
| M1 | ✅ writer side covered | manifest writer commits parts before manifest (`pipeline::manifest_writer`); kill-mid-write integration test deferred to Phase C-γ |
| M2 | ✅ writer side covered | `_SUCCESS` written iff status==Success; body = `xxh3(manifest.json bytes)`; covered by `success_marker_*` tests + `tests/trust_artifacts_integration.rs` §4 |
| M3 | ✅ write side + no-download content verify | per-part `content_fingerprint` (xxh3) and `content_md5` recorded at write in one pass; `--validate` confirms content by comparing `content_md5` to the store's listing checksum (no download); resume still trusts size for skip decisions (quarantine on size drift) — covered by `pipeline::resume_decisions::tests` and `pipeline::manifest_reconcile::tests` |
| M4 | ✅ | `tests/trust_artifacts_integration.rs §6 — writing_manifest_twice_replaces_the_previous_artifact` |
| M5 | ✅ | `pipeline::validate_manifest` + `tests/trust_artifacts_integration.rs` §22 (manifest read, part presence, size match) |
| M6 | ✅ | `legacy_run: true` label surfaced by `verify_at_destination` when no manifest present; covered in `validate_manifest` unit + integration tests |
| M7 | ✅ writer relies on `Destination::write` atomicity | local: `fs::copy`; S3/GCS: single PUT (opendal); covered by destination capability tests |
| M8 | partial: gate ✅, decision matrix ✅ pure logic, ⚠️ chunked-resume executor wiring deferred to Phase C-γ | `--resume` against `_SUCCESS` refuses without `--force` (covered §26); pure matrix tested per row in `pipeline::resume_decisions::tests` and end-to-end against real Destination listing in §27 |
| M9 | ⚠️ deferred to Phase C-δ | best-effort move on resume; `UntrackedDecision::Quarantine` already produced by `build_resume_plan`; executor side not yet wired |

Each invariant lands with at least one unit test (local FS, fast) and one integration test (S3-compat via MinIO or GCS-compat; nightly).
