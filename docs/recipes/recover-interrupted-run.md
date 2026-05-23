# Recipe: Recover from an Interrupted Run

A short, action-first cookbook for the most common Rivet recovery
scenarios.  For the full execution-semantics contract, see
[docs/semantics.md](../semantics.md).  For deeper concepts, see
[docs/best-practices/recovery-and-resume.md](../best-practices/recovery-and-resume.md).

## Mental model in one line

```text
written → manifested → committed → validated → reconciled
```

| Command | Question it answers |
|---|---|
| `rivet run` | Write parts and `manifest.json`, then `_SUCCESS`. |
| `rivet validate` | Do the files Rivet *says* it wrote still exist and match the manifest? |
| `rivet reconcile` | Does the destination row count match the source row count per chunk? |
| `rivet repair` | Re-export the chunks reconcile flagged. |

`validate` reads only the destination.  `reconcile` reads both source and
destination.  `repair` writes new parts; it never deletes or overwrites.

---

## Scenario 1 — Job was killed mid-export (chunked)

Symptoms: `rivet run` exited non-zero, `_SUCCESS` is missing, some
parts exist at the destination prefix.

```bash
# Continue from the last completed chunk
rivet run --config rivet.yaml --export big_table --resume

# Confirm the run finished cleanly
rivet validate --config rivet.yaml --export big_table
```

What `--resume` does:

- Walks the destination prefix, cross-checks against the state DB and
  the prior manifest, and decides per-chunk: **skip** (already
  committed), **rewrite** (in-progress / missing part), or **quarantine**
  (untracked or corrupt object — moved under `_quarantine/<run_id>/`).
- Refused with an actionable error if the prior run already finished
  cleanly (`_SUCCESS` present + chunks complete).  Use `rivet run`
  without `--resume` to start a new run.

`--resume` is meaningful only for `chunked` mode.  For `full` and
`incremental`, just re-run — `incremental` picks up from the persisted
cursor.

---

## Scenario 2 — Files exist, but `validate` fails

Symptoms: `_SUCCESS` is present, but `rivet validate` reports a missing
or short part, or a manifest fingerprint mismatch.

```bash
# 1. Inspect what the verifier saw
rivet validate --config rivet.yaml --export big_table --format json

# 2. Look at recorded state for the export (chunks, manifest, last verified)
rivet state show --config rivet.yaml
rivet state files --config rivet.yaml --export big_table --last 5

# 3. Drill into per-chunk completion
rivet state chunks --config rivet.yaml --export big_table

# 4. Compare against the source per chunk
rivet reconcile --config rivet.yaml --export big_table
```

If `reconcile` reports `match` everywhere but `validate` still fails,
the destination object set diverged after the export — typically
**something else wrote into the prefix** (different tool, manual
cleanup, lifecycle policy).  Quarantine the prefix and re-run.

If `reconcile` reports `mismatch` or `unknown` chunks, proceed to
Scenario 3.

---

## Scenario 3 — Reconcile flagged some chunks; rewrite them

Symptoms: per-chunk source counts disagree with destination counts on
specific ranges.  This is what `rivet repair` was built for.

```bash
# 1. Capture a reconcile report
rivet reconcile --config rivet.yaml --export big_table --format json --output reconcile.json

# 2. Dry-run the repair plan (default — nothing is written)
rivet repair --config rivet.yaml --export big_table --report reconcile.json

# 3. Execute the plan — re-exports only the flagged chunk ranges
rivet repair --config rivet.yaml --export big_table --report reconcile.json --execute

# 4. Re-reconcile to confirm the gap closed
rivet reconcile --config rivet.yaml --export big_table

# 5. Re-validate the destination contract
rivet validate --config rivet.yaml --export big_table
```

What `repair --execute` does and does not:

- Re-runs only the flagged chunk ranges via `ChunkSource::Precomputed`
  — same SQL shape as extraction and reconcile (RR3).
- Writes **new** files alongside originals with the
  `<export>_<ts>_chunk<idx>.<ext>` naming scheme (RR5).
- Does **not** delete or overwrite prior files.  Downstream
  deduplication or a versioned output prefix is the operator's job.
- Leaves `last_committed_*` untouched (RR4).  `last_verified_*`
  re-advances only on a subsequent clean reconcile (zero mismatches,
  zero unknowns).  See
  [`rivet state progression`](../reference/cli.md#rivet-state-progression).

---

## Scenario 4 — State DB is stuck (chunks never advance)

Symptoms: `rivet run --resume` exits with "in-progress export not
found" or "chunks stuck in checkpoint state"; the state DB contains
checkpoints from a process that no longer exists.

```bash
# Inspect the stuck records
rivet state show --config rivet.yaml
rivet state chunks --config rivet.yaml --export big_table

# Drop only stuck-checkpoint chunk rows (preserves manifest + cursor)
rivet state reset-chunks --config rivet.yaml --export big_table --stuck-checkpoints

# Or: drop only failed-chunk rows
rivet state reset-chunks --config rivet.yaml --export big_table --failed

# Then resume
rivet run --config rivet.yaml --export big_table --resume
```

`rivet state reset-chunks` is targeted on purpose: it does **not** wipe
manifests, cursors, or run journals.  See
[`rivet state reset-chunks`](../reference/cli.md#rivet-state-reset-chunks)
for the flag matrix.

---

## What this recipe does *not* cover

- **Cross-process race against the state DB.**  Two `rivet run` against
  the same `.rivet_state.db` is not supported; the state layer enforces
  a single-writer invariant via SQLite locking.  See
  [ADR-0001](../adr/0001-state-update-invariants.md).
- **Recovering after the destination prefix was deleted by something
  else.**  Rivet detects this on next resume but cannot reconstruct
  data that no longer exists; re-run from scratch.
- **Recovering after the source schema drifted.**  See `on_schema_drift`
  in [docs/reference/config.md](../reference/config.md) and the
  `live_schema_drift` test suite for the policy hook.
- **Multi-export campaign recovery.**  `rivet apply --plan plan.json`
  re-runs the full sealed plan idempotently; per-export recovery falls
  back to the recipes above.

---

## See also

- [docs/semantics.md](../semantics.md) — full crash / retry / resume
  contract, including the non-guarantees.
- [docs/best-practices/recovery-and-resume.md](../best-practices/recovery-and-resume.md)
  — deeper notes on `--resume` semantics and state-DB layout.
- [ADR-0008 — Export progression](../adr/0008-export-progression.md) —
  formal `committed` / `verified` boundaries (PG1–PG8).
- [ADR-0009 — Reconcile and repair](../adr/0009-reconcile-and-repair-contracts.md)
  — formal RR1–RR8 invariants.
