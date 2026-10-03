# ADR-0030: A primary-key UPDATE emits `update(new)` — and the destination cannot converge

Status: **accepted** 2026-10-03 — option (3), split into delete + insert. See the
amendment at the end for what was built and measured.

## Context

`UPDATE t SET id = 9 WHERE id = 1` changes the identity of a row. rivet emits ONE
event, `update`, carrying the NEW tuple. Debezium emits TWO — `delete(old)` then
`insert(new)` — and does so identically on PostgreSQL and MySQL. The differential
oracle (`dev/cdc-oracle`) reports this as the only disagreement in its matrix:
15 agreements, one difference, one `na`.

SQL Server agrees with Debezium for a reason that is not a design choice: the
engine itself splits a PK update into a delete and an insert in the change table,
so rivet has nothing to decide there. MongoDB's `_id` is immutable, so the
scenario cannot be expressed. **The question is live on exactly two engines.**

## What was measured, 2026-08-25

Counts reconcile on both sides, which is why every source-vs-destination oracle
missed this for as long as it existed. What the destination holds does not.

rivet's parquet for the scenario above, read back with DuckDB:

    __op     __pos                  __seq   id   v
    insert   {"lsn":"0/E6A7B500"}   0       1    a
    update   {"lsn":"0/E6A7B5C8"}   0       9    a

Five columns. **The value `1` appears nowhere in the update row.** A consumer
applying the documented latest-image-per-key MERGE therefore ends with TWO live
rows where the source has ONE, and nothing in the output tells it which of the
two to remove — or that a removal is owed at all.

This is not an engine limitation. PostgreSQL hands rivet the old key:

    table public.ku: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:9 v[text]:'a'

and rivet PARSES it (`src/source/postgres/cdc.rs`, the `old-key:` / `new-tuple:`
split) and carries it into the event, with a comment saying so — *"A PK-changing
UPDATE carries its old key too … The old key rides `before`."* MySQL's binlog
UPDATE_ROWS event carries a full before-image and the adapter populates `before`
from it likewise.

**The information survives the wire, the parser and the event, and is dropped at
the sink**, which writes the after-image into the data columns and reads `before`
only for deletes. There is no column for it to land in.

## The decision this ADR does not make

The proposal that prompted this was to keep `update(new)` on the grounds that it
is the LAST ACTION and therefore the truth about the row. The first half is
correct: `update(new)` is an accurate statement about the row that now exists.
The second does not follow, and the measurement is why — an accurate statement
about the new row is not a complete statement about the CHANGE, because the
disappearance of the old key is part of what happened and is unrepresentable in
the current output.

So the "keep it as-is" option cannot be written down as a guarantee. It can be
written down as a documented limitation, which is a different claim and carries a
different obligation (say it in the load docs, next to the MERGE that breaks).

Three ways forward, in increasing order of how much they change:

1. **Document the limitation.** Cheapest, honest, and leaves every consumer of a
   mutable-PK table with a destination that silently diverges. Acceptable only if
   the load path also refuses or warns on a table whose PK it has seen change.
2. **Expose the old key.** Keep one event, add the old key to the output (a
   `__old_key` meta column, or a before-image the sink writes for updates as it
   does for deletes). The consumer's MERGE can then delete the old row. Smallest
   change that makes convergence POSSIBLE — but it makes it the consumer's job,
   and every existing consumer keeps diverging until it is updated.
3. **Split into delete + insert**, matching Debezium and what SQL Server's engine
   already does. Convergence needs no consumer change at all: the tombstone is an
   ordinary delete the MERGE already handles. Costs an event-count change that
   any test asserting "one update" will notice, and the two events must not be
   split across parts (the `committed` framing already guarantees that).

The author's recommendation is (3), with (2) as the fallback if the event-count
change is judged too disruptive: it is the only option under which a destination
converges without every downstream consumer being told to change, and it puts
rivet's representation where two of the four engines already are.

## What the implementation actually needs — measured 2026-08-26

The decision is (3), split into delete + insert. These measurements were taken
before writing any of it, because they change the shape of the work.

**The old key reaches the sink on both engines.** `rivet cdc` NDJSON for
`UPDATE t SET id=9 WHERE id=1`:

    postgres   update | before=[1]        | after=[9,'a']
    mysql      update | before=[1,'a']    | after=[9,'a']

PostgreSQL sends the key alone (REPLICA IDENTITY DEFAULT); MySQL sends the whole
old row. A delete needs only the key, so both are sufficient. Nothing has to be
added to the wire, the adapters or the checkpoints — the split is a SINK change.

**The two engines need DIFFERENT detection, and this is the part that was not
obvious.** Measured with an ordinary update and a PK update side by side:

    postgres   ordinary  before=None      ← `before` appears ONLY on a PK change
    postgres   pk        before=[1]
    mysql      ordinary  before=[1,'a']   ← binlog always carries a before-image
    mysql      pk        before=[1,'b']

So on PostgreSQL the rule is simply "`before` is present". On MySQL that rule would
split EVERY update, turning every value change into a delete+insert pair — the
detection there must compare the KEY columns, and `TypeMapping` carries no
primary-key flag. MySQL therefore needs PK metadata plumbed from
`information_schema` into the schema resolution first; PostgreSQL does not.

**Debezium's shape, measured rather than assumed** (`dev/cdc-oracle`, postgres,
`key-update`):

    debezium-only   delete   k=1   v=<null>
    debezium-only   insert   k=9   v=a
    rivet-only      update   k=9   v=a

The delete carries the KEY ONLY — no field values — which is exactly what rivet
has. Debezium emits the delete FIRST, and the order is load-bearing rather than
cosmetic: if the new key collides with another row's key, applying insert before
delete loses that row. The split must therefore emit `delete(before)` at the same
`__pos` with the LOWER `__seq`.

**Reachability is per-table, and one path needs no `UPDATE` at all.** A surrogate
auto-increment or UUID key nobody touches never hits this. A natural key does —
email, SKU, contract number, country code — and so does a composite key with a
mutable part. And `ON UPDATE CASCADE` produces it in CHILD tables with nothing
written against them; measured on the pg stand, updating only the parent:

    csc_child   update | before=['AA',1] | after=['BB',1]

## Consequences of leaving this open

The differential gate row (`cdc_differential`, wired 2026-08-25) EXCLUDES the
`key-update` scenario, and the reason is written at its call site: a gate cell
that expects a difference goes green the day someone fixes it, which is the wrong
direction for a ratchet. So this divergence is currently guarded by nothing — it
is recorded here and in the harness README, and by no test.

Whichever option is chosen, the scenario re-enters the gate as an ordinary cell.

## Amendment 2026-10-03: option (3), built in the shared sink

**What was built.** One rule for every engine, applied where every CDC table's
events pass (`src/source/cdc/partition_guard.rs::split_move`, called by
`sink::run_to_files`): an UPDATE that changes its KEY, or its partition on a
base-and-buffer table, is written as `delete(before)` at the event's `__seq` and
`insert(after)` at the next one, at the same `__pos`.

- **The key is a type now.** It is the export's declared `load.pk:` when one is
  declared, otherwise the table's primary key, read once per table when the run
  starts (`Source::primary_key`, the query `init` and `load` already use). A
  catalog error there fails the run.
- **A move is decided by NAME, on cells the old image actually carried.**
  `key_moves` looks each key column up by name (exact, then ASCII
  case-insensitive) in the old image and in the new one. It is a move only when
  EVERY key column is in both, and one of them differs. An image whose cells are
  not all named (a positional partial image) never counts. This is what MySQL
  needed: its binlog carries a before-image on every UPDATE, so "`before` is
  present" would split every update. It is also what makes a key that is every
  column (a junction table) work.
- **Both roots of the 2026-08 revert are gone, and absent is not NULL.**
  - The PostgreSQL text reader keeps an UPDATE's `old-key:` cells under their
    OWN names (`ChangeEvent.before_names`). A column the old image does not carry
    is absent, not a NULL.
  - The split's delete is named by those cells. It is exactly the key-only
    delete PostgreSQL itself sends under `REPLICA IDENTITY DEFAULT`.
  - The revert's measurement (`v text, id int PRIMARY KEY`, old key written into
    `v`) is the fixture of `pg_cdc_pk_changing_update_captures_and_does_not_brick`.
  - A first cut of this amendment padded the old image to the row's width with
    NULL. Review then measured what that does when the logged columns are not the
    merge key (live, before the fix): `REPLICA IDENTITY USING INDEX` on a unique
    `email`, with `UPDATE SET email = 'b@x'`, gave `delete (id NULL)` +
    `insert (1)`. A declared `load.pk: [code]` with `UPDATE SET id = 2` gave a
    delete with `code` NULL.
  - Both cells, `pg_cdc_an_update_of_a_replica_identity_index_that_is_not_the_key_stays_one_update`
    and `pg_cdc_a_declared_key_absent_from_the_old_key_does_not_split`, now see
    one `update`.
  - An old cell PostgreSQL cannot decode (`infinity`, a BC date, 24:00) is
    refused like the same cell in a new row, never written as the NULL of a
    delete (`pg_cdc_an_undecodable_old_cell_in_a_key_move_is_refused_not_nulled`).
- **A statement that renumbers keys.** Oracle checks uniqueness per statement, so
  `UPDATE t SET id = id + 1` is logged as `1 -> 2`, then `2 -> 3`. Split naively,
  the delete of key 2 lands after the insert that moved row 1 into 2, and that
  row is lost (measured below). `MovedIn` keeps, per open transaction and open
  part, the keys a key move gave a row. A delete of such a key that belongs to
  ANOTHER row is the row that held the key before the statement, so it swaps
  `__seq` and buffer slot with the moved-in insert. The part stays in `__seq`
  order. "Another row" is decided by `another_row`: by the engine's row identity
  when both events carry one, otherwise by comparing images.
- **The row identity is Oracle's ROWID, and only where it is stable.**
  `ChangeEvent.row_id` is an optional field only the Oracle adapter fills; it is
  not written to any output column. Sources, all primary:
  - Oracle Database Reference, `V$LOGMNR_CONTENTS`: `ROW_ID VARCHAR2(18)`, "Row ID
    of the row modified by the change (only meaningful if the change pertains to
    a DML). This will be NULL if the redo record is not associated with a DML."
  - Oracle Database Concepts, "Rowids of Row Pieces": "Every row in a
    heap-organized table has a rowid unique to this table that corresponds to the
    physical address of a row piece." Rivet relies on nothing about rowid
    stability beyond what the measurements below show.
  - Measured on the stand (Oracle 23.26, LogMiner, online catalog), 2026-10-03:
    `UPDATE t SET id = id + 1` over three heap rows logged three UPDATEs, each
    with its own row's ROWID (`…Qk7AAA`, `…Qk7AAB`, `…Qk7AAC`). On a
    range-partitioned table with `ENABLE ROW MOVEMENT`, `SET id = 7, p = 20`
    (crossing partitions) logged the row's OLD ROWID (`AAAVrp…Q3fAAA`), and the
    next statement on the same row its NEW one (`AAAVrq…RHfAAA`); the internal
    delete and insert of the move are not in `V$LOGMNR_CONTENTS`. On an
    index-organized table every change carried `AAAVrnAAAAAAAAAAAA`: the object
    number and zeros. A direct-path insert (`INSERT /*+ APPEND */`) carried each
    row's own ROWID, under `OPERATION = 'DIRECT INSERT'`. A key move of a
    280-column row (stored as more than one row piece) carried its ROWID, and
    `oracle_cdc_a_key_move_of_a_row_wider_than_255_columns_is_captured_not_refused`
    runs it. A table with a LOB column cannot reach this code: the preview refuses
    it at open (`resolve_tables`).
  - So the adapter reads `ALL_TABLES` at open, and a table is "stable" when
    `ROW_MOVEMENT = 'DISABLED'` and `IOT_TYPE IS NULL`. On a stable table a change
    carries its ROWID.
  - A change there with a NULL or placeholder `ROW_ID` is REFUSED
    (`RIVET_SOURCE_CDC_UNDECODABLE`, `row_identity`). The reference does not
    promise a ROW_ID for every DML: it says only that the value is meaningful
    for DML and NULL for non-DML. Every DML shape measured above carried one.
    Pairing without it would be a guess, so the refusal names the table rather
    than falling back to images on a table that promised identities.
  - On any other table the change carries no row identity and the image rule
    applies. This is decided per table at open, never per event.

**Measured on 2026-10-03, on the branch head.** Each cell below is green on the
fix and RED under the seed `pk-move-without-delete` for its engine
(`make seeded-recall`), graded by the rig's independent oracle (source vs
delivered rows in DuckDB):

| engine | cell | RED under the seed |
|---|---|---|
| PostgreSQL | `pg_cdc_pk_changing_update_captures_and_does_not_brick` | source 1, delivered 2 |
| PostgreSQL | `a_postgres_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source` | source 6, delivered 7 |
| MySQL | `mysql_cdc_a_composite_key_move_is_a_delete_then_an_insert` | source 2, delivered 3 |
| MySQL | `a_mysql_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source` | source 6, delivered 7 |
| SQL Server | `a_sql_server_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source` | source 6, delivered 7 (seed: drop the change table's delete) |
| MySQL, PostgreSQL | `a_key_move_leaves_one_live_row_per_key_in_a_partitioned_base_{mysql,postgres}` (BigQuery base+buffer, day partitions, a key move and a key move into the next day) | rig oracle after the load: PostgreSQL source 2, delivered 4; MySQL source 2, delivered 3 |
| PostgreSQL | `an_update_of_a_replica_identity_index_reaches_the_base_as_one_update_postgres` (BigQuery; seed `absent-old-cell-read-as-moved-key`) | `the buffer holds the update alone`: `left: ("1", "1")` (one delete, one NULL `id`) |
| Oracle | `oracle_cdc_a_primary_key_update_is_a_delete_then_an_insert` | source 3, delivered 4 |
| Oracle | `oracle_cdc_a_renumber_over_rows_with_equal_images_keeps_every_row` | source 4, delivered 1 (seed `renumber-equal-images`: the adapter drops the ROWID) |

With the renumber ordering disabled (`MovedIn` never swapping), the Oracle cell
reads source 3, delivered 2. With the image rule disabled (`same_row` always
true), `pg_cdc_a_renumber_under_a_deferrable_key_keeps_every_row` (a
`DEFERRABLE` key under `REPLICA IDENTITY FULL`, no row identity) reads source 3,
delivered 1. The unit test `a_statement_that_renumbers_keys_keeps_every_moved_row`
then holds `[(4, 30)]` where the source holds `[(2, 10), (3, 20), (4, 30)]`. With the PostgreSQL reader's old
layout restored (the old-key cells with no names of their own), the PostgreSQL
cell reads source 1, delivered 2. The Debezium differential (`dev/cdc-oracle/run.py
--scenario key-update`) now reports AGREE on postgres, mysql and mssql, so the
scenario is back in the gate's `cdc_differential` row on every engine but
MongoDB.

**Known limitations.**

- **A renumber over rows whose images are EQUAL, with no row identity, loses a
  row.** On an Oracle index-organized table or a table with row movement, and
  on PostgreSQL with a `DEFERRABLE` primary key under `REPLICA IDENTITY FULL`
  (test_decoding carries no row identity), "the row that held 2 before the
  statement" and "the row just moved into 2" carry the same image when the rows
  differ only in the key, and a renumber reads as one row moved twice. The
  destination then holds `(3, 10)` where the source holds `(2, 10), (3, 10)`.
  Pinned by `a_renumber_over_equal_images_without_a_row_id_documents_the_lost_row`.
  An immediate uniqueness check (PostgreSQL's default, MySQL) never lets a row
  move onto a key another row still holds, so the delete of a key is always
  logged before a row moves into it, and there is nothing to pair. SQL Server
  logs each key's delete before its insert at one `__$seqval` (measured), with
  no row identity.
- **A PostgreSQL table whose changes carry no old image.** Three tables are
  affected: one under `REPLICA IDENTITY NOTHING`, one under `DEFAULT` with no
  primary key, and one whose primary key is `DEFERRABLE` (a deferrable key is not
  used as the replica identity).
  - Measured with `test_decoding`: the UPDATE carries no `old-key:` section, and
    a DELETE reads `(no-tuple-data)`.
  - So no split happens, and the old key stays live. That is the pre-ADR
    behaviour.
  - Measured through `rivet run` on all three, `INSERT 1; DELETE 1` delivers
    `insert (1, 'a')`, `delete (NULL, NULL)`, so a DELETE retracts nothing. That
    predates this ADR and is its own defect.
  - The capture warns at open, once per such table, naming it schema-qualified,
    with the cause, the loss and the remedy (`no_old_key_warning`, decided by
    `old_image` over `pg_class.relreplident` and `pg_index.indimmediate`,
    resolved by `to_regclass`). The key-only-delete warning names what each other
    non-FULL table's DELETE carries (`row_image_verdict`).
  - Every remedy was measured from the degraded state.
    - `REPLICA IDENTITY FULL` makes the UPDATE carry the whole old row.
    - `REPLICA IDENTITY USING INDEX` on a new non-deferrable unique index of
      `id` makes it carry `old-key: id[integer]:2`, but that index then rejects
      `SET id = id + 1` (`duplicate key value violates unique constraint`), which
      the warning says.
    - `REPLICA IDENTITY DEFAULT` after `NOTHING`, and a primary key added to a
      keyless table, each give `old-key: id[integer]:1` on the UPDATE and
      `DELETE: id[integer]:2`.
  - Under `REPLICA IDENTITY FULL` a deferrable key's renumber is logged as on
    Oracle (measured: `old-key: id:1 v:'a' new-tuple: id:2 v:'a'`, then
    `2 -> 3`, then `3 -> 4`, each with the whole old row), so the split and the
    renumber ordering apply.
- **A key column the replica identity does not log.** Under
  `REPLICA IDENTITY USING INDEX` on an index that is not the key, or with a
  declared `load.pk:` that is not the primary key, an UPDATE that changes such a
  column carries no old value for it. It stays one `update`, and that old key
  stays live. `REPLICA IDENTITY FULL` is the remedy, and docs/reference/cdc.md
  says so.
- `rivet cdc` NDJSON stdout does not split. Its `before` is the old image as the
  engine logged it, which is unchanged from before this ADR. A line whose old
  image is not the row's columns also carries `before_columns`, naming those
  cells (`ndjson_line`).
