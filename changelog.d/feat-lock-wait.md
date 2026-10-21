- **A second run of an export was refused at once while the first held its run lease.** Through 0.32.1
  two triggers of one schedule made one of them a failed task (`RIVET_STATE_RUN_IN_PROGRESS`; a load
  beside a load or compact, `RIVET_LOAD_TABLE_BUSY`). **Now** the run lease and a load's table lease
  are waited on for up to `--lock-wait <seconds>` (a global flag, or `RIVET_LOCK_WAIT`, default **600**, passed on to process
  children), with a log line naming the lease and its holder (pid and host on a PostgreSQL state); when
  the wait runs out the command stops with the new `RIVET_STATE_LOCK_TIMEOUT`, exit 2. `--lock-wait 0`
  keeps the immediate refusal. The staging cleanup still skips a busy prefix and never waits (ADR-0039
  D7). **What to do:** nothing; pass `--lock-wait 0` where a busy export must fail at once.
