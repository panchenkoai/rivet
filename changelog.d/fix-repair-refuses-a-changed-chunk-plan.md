- **`reconcile` and `repair` read a stored chunk run on an edited `chunk_column`.** Neither
  command compared the chunk plan the stored run was planned under with the config, so after a
  `chunk_column` (or `query`, `chunk_size`, `chunk_count`, `chunk_by_days`) edit they re-read the
  stored windows on the new column, and a `repair --execute` superseded parts: the manifest listed
  39 rows for 40 source ids, exit 0 (P-21). **Now** both commands run the check `rivet run
  --resume` already runs and refuse with `RIVET_STATE_CHUNK_PLAN_CHANGED` (exit 5) before any window
  is read, naming the remedy. **What to do:** restore the chunk settings the run had to reconcile or
  repair it, or `rivet state reset-chunks -c <config> --export <name>` abandons it and the next run
  starts over. Exit codes: a `reconcile` or `repair` under a changed chunk plan exited 0 or 2 and
  exits 5 now.
