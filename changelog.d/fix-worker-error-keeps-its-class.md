- **A coded stop met by one parallel worker lost its code and its exit class.** A keyset export
  with `parallel:` (`chunk_by_key`), a range-chunked export with `parallel:` (`chunk_column`,
  without `chunk_checkpoint`) and a MongoDB export with `parallel:` kept each worker's failure as
  text only, so the run always printed `RIVET_RUN_UNITS_FAILED` and exited 1, or 2 when any
  worker's text read as transient; a scheduler then retried a refusal it should have stopped on.
  Present through 0.31.0. **Now** the run prints the code of the most stop-worthy coded worker
  failure and exits by its class, as the same export does with one worker; the message still
  lists every failed unit, the deciding one last. The exits that change: a refusal in a worker
  (e.g. `RIVET_SOURCE_VALUE_UNREPRESENTABLE`) 1 or 2 → 5; an integrity stop (e.g.
  `RIVET_DEST_PART_ROWS_MISMATCH`) → 3; an internal one → 6; a usage one (e.g.
  `RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH`, `RIVET_SOURCE_VALUE_TOO_LARGE`) 2 → 1 when a sibling's
  text read as transient; an environment one keeps its own code and exits 2 only when its own
  text is transient. Worker failures that carry no code still exit as `RIVET_RUN_UNITS_FAILED`.
  **What to do:** a job that matched `RIVET_RUN_UNITS_FAILED` or exit 1/2 for these stops matches
  the stop's own code and exit class instead.
