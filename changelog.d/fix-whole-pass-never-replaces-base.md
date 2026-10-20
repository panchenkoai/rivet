- **A whole-table pass replaced an existing base-and-buffer base, and every row only the base held was lost.**
  Under `load.layout: base_buffer` (BigQuery), an incremental run with no cursor to resume from (the first
  run after `rivet state reset`, a lost state DB) and a CDC re-baseline (the remedy every CDC data-loss
  refusal prints) landed their whole pass OVER the base, exit 0: rows the source had since deleted, the
  tombstones, and any history copied into the base were gone (measured 3334 -> 2178 and 16681 -> 10772
  rows). 0.32.1 and earlier. **Now** the pass over an existing base is appended to `<table>__changes` and
  `rivet compact` merges it by key: a row the pass does not hold stays in the base. After a CDC
  re-baseline, a live base row the new baseline does not hold is flagged `__is_deleted = TRUE` (the
  baseline read the whole table after the stream's anchor, so the source no longer has it); a base
  without the delete flag keeps such rows as they are. Only an absent base is still created from the
  pass. A buffer that still holds older changes is refused (`RIVET_LOAD_BUFFER_NOT_COMPACTED`) before the
  pass joins it. **What to do:** nothing; a base replaced by 0.32.1 can be restored with BigQuery time
  travel within 7 days.
