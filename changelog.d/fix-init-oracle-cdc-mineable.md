- **`rivet init --mode cdc` on Oracle wrote CDC exports the run refuses.** A table with a column LogMiner
  never decodes (BLOB, CLOB, …), a name over 30 bytes, a virtual column, an identity column before 23,
  or more than 300 columns got a `mode: cdc` export, and the first `rivet run` stopped with
  `RIVET_SOURCE_CDC_TABLE_UNSUPPORTED`. **Now** init asks the capture's own check (one predicate,
  `oracle::cdc::unmineable`, over the same catalog rows) and leaves such a table out with a `# SKIPPED
  <table>: <reason>` line and the stderr list of left-out tables, the way it already leaves out tables
  it cannot scaffold; the tables it can capture are scaffolded as before. **What to do:** export a
  left-out table in batch.
