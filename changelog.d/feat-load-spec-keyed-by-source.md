- **Configs of one export name on one shared state loaded by each other's key.** The load spec
  (`export_load_spec`: columns and the key `rivet init` recorded) was one row per export name, so
  on a PostgreSQL state shared by configs reading different databases the last writer won: a
  `query:` export loaded by another source's key and failed `RIVET_LOAD_KEY_COLUMN_INVALID`, or
  keyed its dedup by the wrong column (0.32.1 and earlier). **Now** the spec is keyed by export
  name, table and source (state schema v38); a spec written before v38 stays the spec of any
  source of that name until the source records its own. **What to do:** nothing; the first run of
  this release migrates the state and leaves the v37 copy for the way back
  (`docs/reference/state.md`, "Rolling back an upgrade").
