- **The `dest retries` line of a `--parallel-export-processes` run counted a CDC child's retries
  more than once.** Each child sends its process's running total of destination retries with every
  run it finishes, and a CDC child with `initial: snapshot` finishes one run per snapshot leg and then
  its own; the parent added every one of those totals, so a run whose leg retried R times and whose
  drain retried D times printed 2R + D, and more with more legs (0.31.0 and earlier with two or more
  legs; with one leg since the snapshot-leg summary change in this release). **Now** the parent adds
  only what each child's total grew by since that child's previous report. **What to do:** nothing.
