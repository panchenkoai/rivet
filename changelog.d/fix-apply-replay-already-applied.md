- **Behaviour change: a replay of a sealed plan that already applied re-ran it or refused it.**
  `rivet apply plan.json` run again after that plan's apply had exited 0 (a scheduler retry whose
  task process died after rivet finished) extracted the whole table a second time for `mode: full`,
  chunked and keyset plans, and wrote an empty second run for an incremental plan with no stored
  cursor; an incremental plan with a stored cursor was refused `RIVET_PLAN_ARTIFACT_STALE` (exit 5,
  "cursor has drifted") by its own cursor commit (0.31.0 and earlier). **Now** the run that applies
  a plan to exit 0 records the plan's `plan_id` (state schema v37, `run_status.plan_id`), and a
  replay of that artifact exits 0 without extracting, printing `apply: plan <plan_id> of export
  '<export>' was already applied by run <run_id> (...)` with that run's rows, files and bytes; it
  writes no run, manifest or metrics row, so `rivet metrics --last 1` names the run that applied the
  plan. A plan whose apply failed, was interrupted or died still replays, and a cursor moved by
  another run is still refused `RIVET_PLAN_ARTIFACT_STALE`. **What to do:** nothing for a retry. To
  extract again, make a new plan with `rivet plan`, or pass `--force` to run the same artifact again.
  Plans applied before the upgrade recorded no `plan_id`, so their replays behave as before.
