- **A PostgreSQL CDC config written by `rivet init --mode cdc` was refused when another schema held a
  table of the same name.** `init` writes a `public` table unqualified, and every run stopped with
  `RIVET_SOURCE_CDC_TABLE_UNRESOLVED` ("could mean 2 different relations"); a `TRUNCATE` of the other
  schema's table was also taken for this stream's (0.32.2 and earlier). **Now** a bare name means the
  relation the connection's `search_path` resolves, as every statement of the run reads it, when that
  relation can carry a change; the other schema's changes and truncates are not this stream's. A
  bare name the search_path resolves to a view, or to nothing, is elected as before. **What to do:**
  nothing; a config that was refused for this runs as generated.
