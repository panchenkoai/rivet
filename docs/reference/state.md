# State database

rivet keeps its progress (incremental cursors, chunk checkpoints, the ledger of parts and loads,
CDC checkpoints) in a state database: the SQLite file `.rivet_state.db` beside the config, or the
PostgreSQL database `RIVET_STATE_URL` names. The state has a schema version that moves with
releases. A newer rivet migrates an older state the first time it opens it; an older rivet refuses
a newer state with `RIVET_STATE_SCHEMA_NEWER` (exit 5) and never rewrites it.

## Where the state lives

- `RIVET_STATE_URL` unset or empty: the SQLite file `.rivet_state.db` beside the config.
- `RIVET_STATE_URL=postgres://...` / `postgresql://...`: that PostgreSQL database (see the TLS rule
  in [cli.md](cli.md#security)).
- `RIVET_STATE_URL=sqlite:<path>`: that SQLite file, for every command (`init`, `run`, `load`,
  `state ...`). An absolute path is used as written (`sqlite:/var/lib/rivet/app.db`, or
  `sqlite:///var/lib/rivet/app.db`); a relative one is anchored to the config's directory, like the
  default file, so it does not move with the working directory. Nothing is created beside the
  config.

Moving a config's state to a `sqlite:` path is refused, `RIVET_STATE_URL_LEAVES_STATE_BEHIND`
(exit 5), while the file beside the config holds its cursors or runs and the new path holds none:
copy the state first, with every run stopped (`sqlite3 .rivet_state.db ".backup <new path>"`, which
takes the `-wal` file with it), or unset the variable.

## Moving a SQLite state to PostgreSQL (`rivet state copy`)

```sh
rivet state copy -c rivet.yaml --to 'postgresql://rivet@db:5432/rivet_app?sslmode=require'
```

With every run of the config stopped, `rivet state copy` reads the config's SQLite state (the
file beside the config, or the `sqlite:` path `RIVET_STATE_URL` names) and writes every table of it
into the PostgreSQL state `--to` names (or a PostgreSQL `RIVET_STATE_URL`): cursors, chunk
checkpoints, the CDC snapshot records, the load journal and load specs, metrics and run history.

- A state of an older release is brought to this release's schema on an in-memory copy; the file
  itself is only read, and keeps its schema version. It is the way back: unset `RIVET_STATE_URL`
  to use it again (it does not see what the runs did after the copy).
- The copy is one transaction. Each table's row count is compared with the source before the
  commit (`RIVET_STATE_COPY_COUNT_MISMATCH`, exit 3, commits nothing).
- The target must hold no rivet state: a database, or a schema reached through `search_path`
  (`?options=-csearch_path%3D<schema>`), of its own. One with state rows is refused,
  `RIVET_STATE_COPY_TARGET_NOT_EMPTY` (exit 5), naming the tables that hold them.
- After the copy, run every command of the config with `RIVET_STATE_URL` set to the PostgreSQL
  state. The next run continues each cursor and each CDC stream; `rivet load` and `rivet compact`
  find the tables earlier loads wrote in the journal they now read, so they do not refuse them as
  someone else's.
- **A CDC checkpoint is a file, not a state row.** `cdc.checkpoint` stays where the config names
  it (resolved from the config's directory), and the copy prints that path. The runs keep reading
  and writing it there, so it must stay on a disk the scheduler keeps (not an ephemeral pod
  volume). A PostgreSQL slot's position is the server's and needs nothing.

## Load specs on a shared state (v38)

The columns and key a run records for `rivet load` (`export_load_spec`) are kept per export name,
table and source since v38, so configs of one export name reading different databases on one
state each load by their own spec and their own `rivet init` key. A row written before v38 has no
source and stays the spec of any source of that name until the source records its own; the v38
migration rebuilds the table, and the copy it leaves behind restores the v37 shape.

## An upgrade beside a live run of the older release

A migration changes the schema under every process that has the state open, and the older release
refuses a newer state (`RIVET_STATE_SCHEMA_NEWER`). So before it migrates, rivet asks whether another
live process holds a lease on the state: on SQLite, a locked `<state>.lease-*` file beside it; on
PostgreSQL, an unexpired `state_lease` row whose holder is not a dead pid on this host. While one
does, rivet waits up to `--lock-wait` seconds, then stops with `RIVET_STATE_UPGRADE_BESIDE_LIVE_RUN`
(exit 2, retryable), naming the lease (the export or table whose run or load holds it) and its
holder (pid and host on PostgreSQL; on SQLite an `flock` records no pid, and neither records the
holder's release): nothing is migrated and nothing is written, so the older release keeps running on
its state. Stop or wait for that run of the older release, then run again.

## The copy a migration leaves behind

Before a migration runs its first statement, inside the migration lock, rivet writes a copy of the
state as the previous release left it, and prints one line naming it:

- **SQLite**: the file `<state>.v<old version>.<UTC time>.bak` beside the state, for example
  `.rivet_state.db.v32.20261017T101530Z.bak`.
- **PostgreSQL**: the schema `rivet_state_backup_v<old version>` in the state database, for example
  `rivet_state_backup_v32`, holding every state table with its rows, keys, indexes and defaults
  (a serial column gets a sequence of its own, at the same position). The copy is written in the
  migration's transaction. A schema of that name that already exists (a second migration attempt
  from the same version) is kept as it is, and the line says so.

If the copy cannot be written (no disk space, a read-only directory, a role without `CREATE` on the
database), the migration does not run: rivet stops with `RIVET_STATE_BACKUP_FAILED` (exit 1), the
state stays as the previous release left it, and the previous release still opens it.

rivet never deletes a copy. Delete it yourself once the upgrade is accepted.

## Rolling back an upgrade

1. Stop every scheduler, cron entry and operator session that runs rivet against this state.
2. With the newer release still installed, write down the warehouse tables its loads know, for
   every config that loads (step 5 needs it):

   ```sh
   rivet state loads -c <config> --last 1000000 | awk 'NR > 1 { print $NF }' | sort -u > targets-after.txt
   ```

3. Install the previous release.
4. Put the copy back (below), then list the tables the restored ledger knows, the same way:

   ```sh
   rivet state loads -c <config> --last 1000000 | awk 'NR > 1 { print $NF }' | sort -u > targets-before.txt
   comm -23 targets-after.txt targets-before.txt   # tables the newer release created
   ```

5. Each table `comm` prints was created by a load of the newer release, and the restored ledger
   has no record of loading it: the previous release refuses it on every load ("no record of rivet
   loading it", exit 1). Drop it (and its `__changes` log and view, when it has them). Its rows
   come back: the runs it was loaded from are still under the export's prefix and the restored
   ledger counts none of them as loaded, so the previous release's next load lands them again.
   Under `load.cleanup_source: true` those parts were deleted after the load: rename the table
   aside instead of dropping it, and copy into the new table the rows only the renamed one holds.
6. Run the previous release once by hand: `rivet run`, then `rivet load`.

### SQLite

In the directory that holds the config and its `.rivet_state.db`:

```sh
COPY="$(ls -1 .rivet_state.db.v*.bak | tail -n 1)"   # the newest copy: the state as it was before the last migration
cp -p "$COPY" .rivet_state.db
rm -f .rivet_state.db-wal .rivet_state.db-shm
```

### PostgreSQL

Connect exactly as rivet does (`psql "$RIVET_STATE_URL" -v ON_ERROR_STOP=1`): the same database
and the same `search_path`, so that `current_schema()` below is the schema rivet's tables live in.
The block drops every table the newer release migrated and moves the copies back; the newest
`rivet_state_backup_v*` schema is the one the last migration wrote.

```sql
BEGIN;
DO $$
DECLARE
  copy text := (SELECT nspname FROM pg_namespace WHERE nspname LIKE 'rivet_state_backup_v%'
                ORDER BY substr(nspname, 21)::int DESC LIMIT 1);
  t text;
  n int := 0;
BEGIN
  IF copy IS NULL THEN
    RAISE EXCEPTION 'no rivet_state_backup_v* schema in this database: nothing to roll back to';
  END IF;
  FOR t IN SELECT tablename FROM pg_tables WHERE schemaname = copy ORDER BY 1 LOOP
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', current_schema(), t);
    EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I', copy, t, current_schema());
    n := n + 1;
  END LOOP;
  EXECUTE format('DROP SCHEMA %I', copy);
  RAISE NOTICE 'restored % tables from %', n, copy;
END $$;
COMMIT;
```

A table the newer release created and the copy does not hold (none between v32 and v38) is left in
place; the previous release ignores it. Drop it before upgrading again.

### What it costs

Everything rivet wrote to the state after the upgrade is gone: cursors, checkpoints, the ledger of
parts and loads. What rivet delivered to the destination after the upgrade stays there. The
previous release resumes from where its own state left off, so the rows delivered between the
upgrade and the rollback are delivered again: at least once, never lost. Per mode:

- **incremental**, **keyset_incremental**: the rows above the restored cursor or key are exported
  again into new parts under the same prefix. A reader of the whole prefix sees them twice; the
  latest value per key is right in both.
- **full**, **chunked**: a run that finished after the upgrade is forgotten and the next run is a
  full pass again. A checkpointed run the newer release left unfinished is not resumed by the
  previous release: it starts over.
- **cdc**: the checkpoint returns to the previous release's. MySQL, SQL Server, MongoDB and Oracle
  capture again the changes made since it, as long as the server still holds them (a purged
  position is refused as a log gap, see [Failure modes & recovery](cdc-failure-modes.md)).
  PostgreSQL continues from the replication slot, whose position is the server's and does not move
  back: nothing is captured twice.
- **`rivet load`**: the parts loaded after the upgrade are no longer in the load ledger, and the next
  load loads them again. A table the newer release created is unknown to the restored ledger and
  is refused until step 5 drops it. Into a table rivet appends to (an incremental or continued-key export)
  the rows land a second time; a full export's load overwrites, and a `compact` merges by key, so
  nothing doubles there.
