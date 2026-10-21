- **A SQLite state could only reach PostgreSQL by hand.** Through 0.32.1 the only way to move a config to
  a shared PostgreSQL state was a hand-written table copy; pointing `RIVET_STATE_URL` at an empty
  database started every export over and left the load journal behind. **Now** `rivet state copy -c
  <config> [--to postgresql://…]` copies every table of the config's SQLite state into an empty
  PostgreSQL state in one transaction, checks each table's row count before the commit, and only
  reads the SQLite file (a state of an older release, v31 of 0.30.0 included, is migrated on an
  in-memory copy). The next run continues its cursors and CDC streams, and `rivet load` keeps the
  tables earlier loads wrote. A target holding state is refused (`RIVET_STATE_COPY_TARGET_NOT_EMPTY`,
  exit 5). A CDC `cdc.checkpoint` file is not part of the state: it stays where the config names it,
  and the copy prints its path. **What to do:** to move a config, stop its runs, run `rivet state copy`,
  then set `RIVET_STATE_URL` for every command of it.
