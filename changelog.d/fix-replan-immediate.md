- **A state write beside a sibling writer in the same process could fail `database is locked`.**
  SQLite's busy wait has no queue: a writer that committed back to back (chunk workers, the lease
  keeper, a run's own store on one SQLite state file) took the lock in every gap, and the other
  store's `BEGIN IMMEDIATE` was refused after 15 s, past the 10 s busy timeout (0.32.0 and
  earlier; seen as a 1-in-1000 CI failure of a replan beside a writer). **Now** writers of one
  state file in one process take turns at its write lock, first come first served; another
  process still meets SQLite's busy wait. **What to do:** nothing.
