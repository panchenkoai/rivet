# ADR-0030: A primary-key UPDATE emits `update(new)` — and the destination cannot converge

Status: **proposed** — the measurement below is settled; the choice is not.

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

## Consequences of leaving this open

The differential gate row (`cdc_differential`, wired 2026-08-25) EXCLUDES the
`key-update` scenario, and the reason is written at its call site: a gate cell
that expects a difference goes green the day someone fixes it, which is the wrong
direction for a ratchet. So this divergence is currently guarded by nothing — it
is recorded here and in the harness README, and by no test.

Whichever option is chosen, the scenario re-enters the gate as an ordinary cell.
