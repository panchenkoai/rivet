- **A batch `rivet load` read the runs of a CDC export nested inside its prefix.** `rivet init`
  puts a table's CDC export at `exports/<t>/cdc/` inside its batch export `exports/<t>/`, under
  the same export name, and the load lists its prefix recursively: the batch table received the
  stream's change rows, so a row deleted at the source stayed in it (a full load landed 7 rows
  for 5, an incremental one kept the deleted key), exit 0 (0.32.0 and earlier). **Now** a full or
  incremental load skips the CDC runs of its export that sit in a sub-directory holding none of
  its batch runs, and says in one line which directories it skipped; the CDC export's own load
  still loads them. **What to do:** nothing; reload a batch table that was loaded from such a
  prefix (`mode: full` replaces it on the next load).
