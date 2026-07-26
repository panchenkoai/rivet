# Parallel keyset — iteration 2: per-range crash-recovery

Iteration 1 (shipped) fans N ROW-percentile-range workers with NO crash-recovery:
a crash re-reads the whole table on a clean re-run. Iteration 2 adds **per-range
(coarse) crash-recovery** behind `chunk_checkpoint: true` — a crashed run resumes,
re-reading at most the ranges that hadn't finished.

## Model: the recovery unit is a whole range

- **Fresh run** (`chunk_checkpoint`, no in-progress anchor): sample the N ranges
  once, PERSIST their boundaries (`keyset_range`, keyed by `run_id`, all `done=0`),
  then set `resume_run_id`. Workers page as in iteration 1; at completion each
  worker atomically records its parts to `file_log` AND flips its `keyset_range`
  row `done=1` (`commit_keyset_range_at_ref`, one transaction).
- **Resume** (in-progress `resume_run_id`): reuse that run_id, RELOAD the persisted
  ranges. Skip `done` ranges; re-run the rest from their `lo`. Post-join, rehydrate
  the done ranges' parts from `file_log` into the manifest.
- **Clean finalize**: `finalize_keyset_anchor` clears `resume_run_id` +
  `keyset_range` (a no-op for sequential keyset).

## The three invariants that make it correct

1. **Boundaries survive the crash.** Re-sampling a changed table would move the
   percentile boundaries and leave a gap between a done range and a re-run range.
   So boundaries are persisted at open and RELOADED on resume, never re-sampled.

2. **Part names are stable across resume.** Parts key off the `run_id`
   (`{export}_{run_id}_pk_w{range}_{page}`), which is unique per fresh run AND
   reused on resume. A re-run range's parts therefore OVERWRITE its crashed
   partial parts (idempotent) instead of accumulating duplicates. (Iteration 1
   used a wall-clock stamp; iteration 2 switched to run_id for this reason.)

3. **The checkpoint is atomic.** A range is `done` only when its parts are BOTH
   durable on disk AND recorded to `file_log` AND its row flipped — all in one
   transaction. A crash before that commit → range `done=0`, no `file_log` rows →
   re-read on resume (its disk parts overwritten). A crash after → range `done=1`,
   parts in `file_log` → skipped + rehydrated on resume. No partial state.

Because an incomplete range writes NO `file_log` rows (it never reached the
commit), `rehydrate_manifest_parts_from_file_log(run_id)` naturally pulls ONLY the
done ranges — no filtering needed. And rehydrate dedupes against the parts a
re-run just recorded, so a fresh run (every range re-run this pass) is a no-op.

## Storage: `keyset_range` (state DB, migration v19)

`(export_name, run_id, range_index, lo, hi, done, updated_at)`, PK `(export_name,
range_index)`. Each worker updates only its OWN row → disjoint keys → no
cross-worker write contention (workers reconnect per the ADR-0011 `*_at_ref`
pattern; the live `StateStore` is not `Sync`).

## Scope boundaries

- `keyset_incremental` (append-only continue-on-clean-rerun) is NOT parallel-aware
  yet — the planner disables it under `parallel > 1` with a warning (iteration 3).
- `cursor_high` on a resumed run reflects the RE-RUN ranges only (a done range is
  skipped, its max not re-observed). Acceptable: parallel keyset is a full
  snapshot, not an incremental anchor — its cursor range is descriptive.

## Tests (RED-proven by mutating the product)

- `keyset_parallel_crash_resume_writes_a_complete_destination_manifest`
  (`tests/live/live_keyset.rs`): crash after range 0 commits (a HARD-EXIT hook —
  a panic would defer to the scope join, by which point every worker has
  finished), resume, assert the DESTINATION manifest declares all N rows. RED
  against removing the resume rehydrate (done range orphaned → row_count < N) —
  which is ALSO the proof the done range was SKIPPED, not re-read (if it were
  re-read, rehydrate would be immaterial).
- `state::keyset_range` unit tests: persist/load round-trip, run_id isolation,
  atomic commit marks only its own row, clear.
