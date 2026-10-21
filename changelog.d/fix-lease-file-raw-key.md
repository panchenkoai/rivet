- **Two exports whose names differ only in characters a file name cannot hold shared one run lease
  on a SQLite state.** Through 0.32.1 the lease file was named by a lossy token of its key, so
  `orders#1` and `orders-1` (or `orders x` and `orders-x`, or two Cyrillic names) locked one file, and
  with `--parallel-exports` one of them was refused `RIVET_STATE_RUN_IN_PROGRESS` beside no live run of
  its own. **Now** the file name carries a hash of the whole key after the readable part
  (`.rivet_state.db.lease-<token>-<hash>`). A PostgreSQL state was not affected. **What to do:** nothing;
  do not run a 0.32.1 and a 0.32.2 binary on one SQLite state at the same moment, as they name the
  lease file differently.
