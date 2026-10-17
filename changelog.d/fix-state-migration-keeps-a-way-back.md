- **An upgrade could not be rolled back: the first run of a newer rivet migrated the state schema in
  place, and the previous release then refused it (`RIVET_STATE_SCHEMA_NEWER`, exit 5) with no copy
  of what it had written.** Release 0.31.0 knows schema v32; this release moves it to v37 (0.31.0
  and earlier). **Now** a migration first writes a copy of the state as the previous release left it,
  inside the migration lock and before its first statement: on SQLite the file
  `<state>.v<old>.<UTC time>.bak` beside the state (`.rivet_state.db.v32.20261017T101530Z.bak`), on
  PostgreSQL the schema `rivet_state_backup_v<old>` in the state database, every state table with its
  rows, keys, indexes and defaults, in the migration's own transaction. One log line names the copy.
  A copy that cannot be written (disk full, a read-only directory, a role without `CREATE` on the
  database) stops the migration with `RIVET_STATE_BACKUP_FAILED` (exit 1); the state is untouched and
  the previous release still opens it. **What to do:** nothing to upgrade. To roll back, follow
  [Rolling back an upgrade](docs/reference/state.md#rolling-back-an-upgrade): stop the schedulers,
  install the previous release, put the copy back (SQLite: replace the file; PostgreSQL: the SQL
  block there) and run once. Progress written after the upgrade is lost, so the rows delivered since
  are delivered again, at least once; what that means per mode and for `rivet load` is written there.
