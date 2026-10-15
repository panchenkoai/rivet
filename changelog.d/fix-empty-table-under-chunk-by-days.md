- **An empty table under `chunk_by_days` stopped the run instead of exporting zero rows.** The date
  planner read the NULL `min` of a table with no row as an unreadable bound and stopped with
  `min(<column>) returned NULL — table may be empty` (since date-native chunking in 0.2.0-beta.3, through
  0.31.0; exit 5 `RIVET_SOURCE_PROBE_UNREADABLE` once the stop carried a code), while the same table
  range-chunked on an integer key exported zero rows with exit 0. **Now** both planners read the key's
  span through one function: a table with no row plans one empty window, so the run exits 0 and writes
  `_SUCCESS` and a manifest of 0 rows, as the range run does; a bound absent over a table that has rows
  is still refused `RIVET_SOURCE_PROBE_UNREADABLE`, and a NULL key is still refused
  `RIVET_PLAN_CHUNK_KEY_NULL`. **What to do:** nothing.
