# CDC oracle: Debezium as an independent reference

## Why

rivet's existing CDC oracles compare **the destination to the source** — DuckDB over
the manifest-declared parts against what the database holds. That catches loss and
duplication, and it cannot catch a case where rivet and the test agree on a wrong
answer: the "test and code agree on a wrong spec" blind spot the mutation layer is
also blind to (see CLAUDE.md, "Scope honesty").

Debezium is a third point. It reads the same log, on the same server, at the same
time, and it is the reference implementation every other tool is measured against.
Where rivet and Debezium disagree about a change, one of them is wrong and the
disagreement is the finding — no oracle in this repo can currently produce that
signal.

This is deliberately NOT a conformance suite (none exists publicly; Debezium's own
tests are not portable). It is a differential harness.

## Shape

    source DB ──┬── rivet  (mode: cdc)      → parquet, read via manifest-declared parts
                └── Debezium Server         → HTTP sink → debezium.jsonl

Both read the SAME log. `compare.py` normalises each side to a set of
`(op, table, key, after-values)` and reports the symmetric difference.

Debezium Server's `http` sink is used rather than Kafka so the harness needs no
broker; `sink.py` is a ~30-line receiver whose only job is to append raw events.
Normalisation lives in the comparison step so the capture stays a faithful record
of what the reference emitted.

## A harness limitation, stated because it looks like a finding

**MongoDB deletes cannot be compared by key — and this was PURSUED, not assumed.**

The Mongo connector puts the deleted document's `_id` in the message KEY, and the
http sink forwards only the value. Adding `ExtractNewDocumentState` (the documented
remedy) fixed inserts and updates — those now arrive flattened with `_id` beside
`__op`, which is why Mongo's crud cell compares three of four ops for real. A
delete still arrives as:

    {"__deleted": true, "__op": "d"}

with no key at all. Three configurations were tried and MEASURED, not assumed:

    add.headers=op                     -> header X-DEBEZIUM-__OP only, no key
    delete.tombstone.handling.mode=rewrite -> `__deleted: true`, still no key
    add.fields=op,id                   -> the field appears as `__id: null`

The last one is the clearest evidence: Debezium adds the field and has nothing to
put in it, because the key never enters the value for a Mongo delete. It lives in
the message KEY, and the http sink transmits value plus headers only.

So `delete` is excluded for MongoDB BY FLAG, with the reason here and at the call
site. It is the harness's blind spot, not a difference between the tools, and
closing it needs a sink that records the message key alongside the value.

## Known asymmetries (these are NOT findings)

The two tools legitimately differ, and the comparison must account for it or it
will report noise forever:

- **Plugin.** Debezium uses `pgoutput`; rivet uses `test_decoding` (ADR-0024).
  Debezium therefore sees a partitioned parent under its root name where rivet sees
  the leaf — that difference is the ADR's subject, not a bug to file per-run.
- **TRUNCATE.** Debezium skips it by default (`truncate.handling.mode=skip`);
  rivet refuses. Compare with truncate excluded, or set `include` on both sides.
- **Snapshot.** Debezium's `initial` emits `op=r` (read) rows for existing data;
  rivet's `mode: cdc` alone does not snapshot. Use `snapshot.mode=no_data` for a
  streaming-only comparison.
- **Shape.** Debezium emits before/after envelopes with a schema; rivet writes a
  flat row plus `__op`/`__pos`/`__seq`. Normalisation is where that is reconciled,
  and it must be written to fail loudly on an unrecognised shape rather than
  silently dropping a field it does not understand — a lenient normaliser would
  reintroduce exactly the class this harness exists to catch.

## Comparison runs in DuckDB, not Python

`compare.py` hands BOTH captures to DuckDB — `read_json_auto` for Debezium's
JSONL, `read_parquet` for rivet's parts — and the normalisation plus the symmetric
difference are one SQL statement. The comparison engine is then independent of both
tools being compared, which is the same reason every other oracle here uses DuckDB:
a bug in a hand-rolled Python loop would be a bug in the oracle itself.

rivet's side is read through the parts the MANIFEST DECLARES, never a glob.

The Debezium normaliser handles the enveloped and the `transforms=unwrap` shapes,
and an event matching NEITHER surfaces as `UNKNOWN-SHAPE` rather than being
dropped — a lenient normaliser would reintroduce the exact class this exists to
catch.

## Status (2026-08-25)

    engine      crud    key-update   wide-txn      mid-stream-table
    postgres    AGREE   DIFFERS      AGREE 75/75   AGREE
    mysql       AGREE   DIFFERS      AGREE 75/75   AGREE
    mssql       AGREE   AGREE        AGREE 99ev    AGREE
    mongo       AGREE*  na           AGREE 75/75   AGREE     (* delete excluded, see below)

**Every cell above compares VALUES, not just (op, key)** — re-measured 2026-08-25
after `--value` was fixed and turned on by default. The earlier version of this
table was (op, key) only, which is blind to cell corruption: a parquet with 100%
of a column NULLed reported AGREE. That is now RED-proven to fail, and a run
WITHOUT `--value` says so in its own output rather than claiming a comparison it
did not make.

All sixteen cells settled: fifteen agreements, one difference, one `na`.

**The key-update finding**, and it SURVIVED the harness being fixed — which is the
only reason to trust it. `UPDATE t SET id=9 WHERE id=1` gives `delete(1)+insert(9)`
from Debezium and `update(9)` from rivet, identically on PostgreSQL and MySQL, so
it is rivet's representation choice rather than a plugin artefact. A consumer
applying the documented latest-image MERGE keeps the old key in the destination
forever. Counts reconcile on both sides, which is why the source-vs-destination
oracles never saw it.

SQL Server agrees because the engine itself splits a PK update into delete+insert
in its change table — the same defect is INVISIBLE on one engine of three, which
is the argument for a per-engine matrix over one representative test.

`key-update` is **na** on MongoDB: `_id` is immutable, so the scenario cannot be
expressed. A scenario an engine cannot express is not a passing cell.

## Seven artefacts the harness produced before it produced a finding

Recorded because each one wore the costume of a defect, and most were written
down as "undiagnosed" before being traced. The first four were found on
2026-08-24, the last three on 2026-08-25 while closing the release-gate
preconditions — which is the point: this list is not finished, and a gate that
reports its own races as release blockers gets muted.

1. **Shared work dir across engines** — MySQL read a PostgreSQL `offsets.dat`.
2. **Shared work dir across scenarios** — one run's capture accumulated into the
   next, surfacing as a `delete` in a scenario that has no delete. Recorded as
   fixed on 2026-08-24; it was not. The fix covered the cross-ENGINE leak only,
   and the work dir was `work/engine/table` with the table name identical for
   every scenario. Running crud then wide-txn reported wide-txn DISAGREE with
   crud's four events listed as debezium-only.
3. **Shared work dir across RUNS** — the dir was reused and the sink APPENDS, so
   running one scenario twice compared the second capture against both. The dir
   is cleared at the start of every run now.
4. **A fixed flush wait** instead of the engine's own readiness signal.
5. **A lossy HTTP sink** — 30, 39, 57 events of 75 across identical runs, which
   read as "the reference falls short". HTTP/1.0 was closing the connection
   mid-batch.
6. **A container name without the scenario** — `srv-<table>` collided with the
   previous scenario's leftover. Worse than the crash it caused: a container left
   RUNNING keeps capturing into whatever it has mounted, which is how wide-txn's
   25 updates turned up inside a crud comparison.
7. **Column ORDER between the two views** — `EXCEPT` compares by POSITION, and the
   two views projected the key and the values in opposite orders. With `--value`
   on, every row of a four-row scenario reported as both-sides-only. This is the
   one that made `--value` look broken on the delete path and kept it opt-in.

Two more that were defects in the COMPARISON rather than the harness plumbing,
listed here because they present identically:

8. **A DELETE has no `after`** — its image is in `before`, and rivet writes that
   image into the same columns, so rivet's before-image was compared against NULL.
   Under REPLICA IDENTITY DEFAULT both sides carry only the key and agree by
   accident, which is why it survived.
9. **JSON null vs the STRING "null"** — `json_extract_string` returns the same
   4-char `'null'` for both (measured in DuckDB), so every NULL cell disagreed.
   The lazy fix would make a cell holding the TEXT "null" indistinguishable from an
   absent one: the degrade-to-null silent-loss class, reintroduced inside the
   oracle meant to catch it. A `jstr` macro driven by `json_type` maps a JSON null
   to SQL NULL and leaves the string alone.

## Wiring this into the release gate (plan item 7) — what stopped it

The gate already HAS a CDC leg: `cdc.verify_cdc_e2e` compares the destination to
the source (per-column null profile, distinct counts, over the cloud parts). It
catches loss and degrade-to-null. It is structurally blind to rivet and its own
oracle agreeing on a wrong SPEC, which is what this harness exists to catch.

Adding a `cdc_differential_vs_debezium` row was ATTEMPTED and REVERTED, because
`release_gate_matrix_guard` refused it twice, correctly:

    gate matrix scenario `cdc_differential_vs_debezium` has no
    sc_cdc_differential_vs_debezium() in the gate modules

    release-gate-matrix has 14 gaps > ratchet 10 — you cannot ADD a gap;
    wire the check and LOWER the ratchet

Both are the right verdict. That ledger grades the CALL SITE, not the row — a
lesson it learned from a `verify_blessed_path` registered `test` on four engines
with no caller anywhere in the tree. A row describing intent is exactly what it
exists to reject.

Three things had to be true before the row belonged there. All three are done as
of 2026-08-25, and the row is wired:

1. **The gate must stand up the reference.** DONE — `verify_cdc_differential`
   (dev/release_oracle/cdc.py), called from `__main__`, row `cdc_differential`
   in docs/release-gate-matrix.yaml marked `test`. The harness already pulled
   `quay.io/debezium/server`, put a receiver on the stand network and waited on
   each engine's OWN readiness signal, so the gate did not have to learn any of
   it. The gap ratchet is untouched at 10 because nothing was added to it — the
   two earlier refusals were both correct, and the fix was to WIRE the check
   rather than to describe it.
2. **`--value` must be on.** DONE — on by default, RED-proven to catch a fully
   NULLed column, AGREE on all four engines including the delete path. It was
   never a delete-path problem: three defects in the comparison itself (artefacts
   7, 8, 9 above) produced that symptom.
3. **The harness must stay quiet when nothing is wrong.** DONE for the shapes
   found so far — `postgres crud` three times in a row AGREES three times, where
   the un-cleared work dir made run 2 disagree. Said with the caveat the artefact
   list earns: nine defects in two days means the next one is likely, so the gate
   grades a harness that did not complete as SKIP with the tail attached, never a
   silent pass.

Two grading decisions live at the call site rather than here, because they decide
what the gate MEANS:

- An AGREE that compared only `(op, key)` is a FAILURE, not a pass. The row claims
  a value-level check, the comparison names its own scope in its output, and that
  is checked rather than assumed.
- `key-update` is excluded. rivet emits `update(new)` where Debezium emits
  `delete(old)+insert(new)`, identically on PostgreSQL and MySQL — a representation
  decision that belongs in an ADR. A gate cell EXPECTING a difference would go
  green the day someone fixes it, which is the wrong direction for a ratchet.
