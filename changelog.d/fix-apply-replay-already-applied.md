- **Behaviour change: a replay of a sealed plan that already applied re-ran it, and an incremental plan could be applied only once.**
  `rivet apply plan.json` run again after that plan's apply had exited 0 (a scheduler retry whose
  task process died after rivet finished) extracted the whole table a second time for `mode: full`,
  chunked and keyset plans. An incremental plan made after a run had stored a cursor was refused
  `RIVET_PLAN_ARTIFACT_STALE` (exit 5, "cursor has drifted") on every apply after its first, by its
  own cursor commit, and any plan older than 24 h was refused (0.31.0 and earlier). **Now** a
  full, chunked or keyset plan is a point-in-time snapshot: the run that applies it to exit 0
  records the plan's `plan_id` (state schema v37, `run_status.plan_id`), and a replay of it exits 0
  without extracting, printing `apply: plan <plan_id> of export '<export>' was already applied by
  run <run_id> (...)` with that run's rows, files and bytes; it writes no run, manifest or metrics
  row. An incremental plan (`mode: incremental`, `keyset_incremental`) is a reusable description:
  every apply of it, a retry included, reads the cursor from state and delivers what arrived since,
  new rows or none; a cursor moved by an earlier apply of the same plan is not drift, and the 24 h
  limit no longer applies to it. A cursor moved by another run is still refused
  `RIVET_PLAN_ARTIFACT_STALE`, and a plan whose apply failed, was interrupted or died still
  replays. **What to do:** nothing for a retry. To extract a full, chunked or keyset plan again,
  make a new plan with `rivet plan`, or pass `--force`. Plans applied before the upgrade recorded no
  `plan_id`, so a replay of one behaves as before.
