- **A rivet with a newer state schema, started beside a live run of an older release, migrated the
  state under that run, even when it was then refused.** Every command opened and migrated the state
  before it took its lease, so a `rivet run` or `rivet load` refused as busy (`is writing`,
  `RIVET_STATE_RUN_IN_PROGRESS`) had already moved the state to the new schema, and the next run of
  the older release on that state was refused `RIVET_STATE_SCHEMA_NEWER` (every release with a state
  migration; found by the 0.32.2 pre-release gate on the v37 → v38 migration). **Now** a migration
  first asks whether another live process holds a lease on the state (a locked lease file beside a
  SQLite state; an unexpired `state_lease` row of a live holder on a PostgreSQL state). While one
  does, it waits up to `--lock-wait` and then stops with the new `RIVET_STATE_UPGRADE_BESIDE_LIVE_RUN`
  (busy, exit 2), the state not migrated and nothing written. **Behaviour change:** the first command
  of a release with a newer state schema, started while a run of the older release holds any lease
  on the same state, waits for that run or stops by this code instead of migrating the state beside
  it; a state no live run holds is migrated as before, and state written before the upgrade is
  unchanged. **What to do:** nothing; a scheduler that retries exit 2 runs the upgrade once the older
  run ends.
