- **A coded stop met by one chunk of a checkpointed export lost its code and its exit class.** A
  range-chunked export with `chunk_checkpoint: true` (with or without `parallel:`) recorded a
  failed chunk's cause as text in `chunk_task` only, so the run always ended as the incomplete run
  `RIVET_RUN_UNITS_FAILED` with exit 1 and a `--resume` hint, and a `--resume` met the same stop and
  ended the same way, so a scheduler resumed a refusal forever. Present through 0.31.0. **Now** the
  run prints the code of the most stop-worthy coded chunk failure and exits by its class, as the
  same export does without `chunk_checkpoint`; the message lists every failed chunk under the resume
  remedy, and a `--resume` that meets the stop again refuses by the same code. The exits that
  change: a refusal in a chunk (e.g. `RIVET_SOURCE_VALUE_UNREPRESENTABLE`) 1 → 5; an integrity
  stop (e.g. `RIVET_DEST_PART_ROWS_MISMATCH`) 1 → 3; an internal one 1 → 6; a coded usage or
  environment stop 1 or 2 → its own code's exit. A chunk whose stored window does not parse now
  stops as `RIVET_STATE_CHUNK_WINDOWS_BROKEN` (exit 5) on the parallel checkpoint runner too. Chunk
  failures that carry no code still exit as `RIVET_RUN_UNITS_FAILED`. The sequential runner's
  message changed from `chunk checkpoint incomplete (N tasks not completed); fix errors and ...` to
  `N chunk task(s) of export '...' not completed; fix the cause, then ... --resume, or inspect
  rivet state chunks ...`, followed by each failed chunk's cause. **What to do:** a job that matched
  `RIVET_RUN_UNITS_FAILED` or exit 1 for these stops matches the stop's own code and exit class
  instead.
