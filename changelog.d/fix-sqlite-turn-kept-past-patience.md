- **A SQLite state write could still be refused `database is locked` beside a busy sibling.** 0.32.1
  made the writers of one state file in one process take turns, but a writer that waited longer than
  its patience for a turn (a slow or descheduled holder) left the queue for SQLite's own busy wait,
  where a sibling committing back to back took the lock in every gap again: refused after the busy
  timeout, exit 1 (seen once under CI load). **Now** a writer that ran out of patience keeps its
  place: no later writer of the process is served before it has written. **What to do:** nothing.
