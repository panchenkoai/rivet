- **Two exports of one name on a shared state refused each other even when they read different
  sources.** Since every run took the export's run lease (keyed by the export name), the
  shared-state deployment the state docs describe (several configs, one export name, one PostgreSQL
  state, different sources) ran one export at a time and refused the rest with
  `RIVET_STATE_RUN_IN_PROGRESS`. **Now** the run lease is keyed by the export and its source, as the
  cursor is: runs of one name on different sources run side by side, a second run on the same source
  is still refused. A run under its lease closes only its own source's dead `running` rows; state
  schema v36 records the source on each `run_status` row (rows from before v36 have none and are
  closed by any run of the export). **What to do:** nothing.
