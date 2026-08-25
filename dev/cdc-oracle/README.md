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

**MongoDB deletes carry no key in the event BODY.** Debezium puts the deleted
document's `_id` in the message KEY, and the http sink forwards only the value —
so `{after: null, before: null, op: "d"}` is all that arrives. The comparison
sees a keyless delete on the reference side and a keyed one from rivet, which
renders as a disagreement and is not one.

This is the harness's own blind spot, not a difference between the tools. Fixing
it means capturing the message key (a sink that records both, or `transforms`
that lift `documentKey` into the value). Until then, MongoDB's `delete` is
excluded by `--exclude-op delete` and the exclusion is recorded HERE rather than
applied silently — an excluded op that nobody wrote down becomes an op nobody
checks.

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
    mssql       AGREE   AGREE        AGREE 75/75   AGREE
    mongo       AGREE   na           AGREE 75/75   AGREE

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

## Four artefacts the harness produced before it produced a finding

Recorded because each one wore the costume of a defect, and three were written
down as "undiagnosed" before being traced:

1. **Shared work dir across engines** — MySQL read a PostgreSQL `offsets.dat`.
2. **Shared work dir across scenarios** — one run's capture accumulated into the
   next, surfacing as a `delete` in a scenario that has no delete.
3. **A fixed flush wait** — reported a race as a shortfall; replaced by waiting
   for the reference to quiesce.
4. **The sink dropped batches** — HTTP/1.0 closing a connection Debezium's client
   kept alive, plus a single thread and a 5-deep listen queue. Delivered 30/39/57
   of 75 across identical runs and read as "the reference falls short". This one
   cost the most, because it accused the wrong tool in our favour.

The rule that came out of it: **when a differential harness says the REFERENCE
fell short, suspect the harness first.** The reference has years of production
use; the transport between them was written yesterday.

## A harness limitation, stated because it looks like a finding

**MongoDB deletes carry no key in the event BODY.** Debezium puts the deleted
document's `_id` in the message KEY, and the http sink forwards only the value —
so `{after: null, before: null, op: "d"}` is all that arrives. The comparison
sees a keyless delete on the reference side and a keyed one from rivet, which
renders as a disagreement and is not one.

This is the harness's own blind spot, not a difference between the tools. Fixing
it means capturing the message key (a sink that records both, or `transforms`
that lift `documentKey` into the value). Until then, MongoDB's `delete` is
excluded by `--exclude-op delete` and the exclusion is recorded HERE rather than
applied silently — an excluded op that nobody wrote down becomes an op nobody
checks.

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
    mssql       AGREE   AGREE        undiagnosed   AGREE
    mongo       AGREE   na           AGREE 75/75   AGREE

Fourteen of sixteen cells settled; all four engines run.

**The key-update finding.** `UPDATE t SET id=9 WHERE id=1` gives
`delete(1)+insert(9)` from Debezium and `update(9)` from rivet, identically on
PostgreSQL and MySQL — so it is rivet's representation choice, not a plugin
artefact. A consumer applying the documented latest-image MERGE keeps the old key
in the destination forever. Counts reconcile on both sides, which is why the
source-vs-destination oracles never saw it. SQL Server agrees because the engine
itself splits a PK update into delete+insert in its change table, so the same
defect is INVISIBLE on one engine of three — the argument for a per-engine matrix
rather than one representative test.

`key-update` is **na** on MongoDB: `_id` is immutable, so the scenario cannot be
expressed. A scenario an engine cannot express is not a passing cell.

**MSSQL wide-txn — measured, undiagnosed, and NOT a claim about rivet.**
The 50-insert + 25-update scenario puts 100 rows in `cdc.dbo_<t>_CT` (SQL Server
writes two CT rows per UPDATE). rivet captures 75 change events, which is the
correct count. Debezium delivers 39 and then STOPS — it quiesces there across
three consecutive checks, so this is its steady state rather than a flush race:
an earlier fixed 8s wait gave 30, and waiting for quiescence gave 39.

The 55 resulting rows all sit on the `rivet-only` side, i.e. the REFERENCE is
short. Candidate causes not yet separated: a batch-size limit on the connector, a
poll interval interacting with the capture job's own batching, or something about
`snapshot.mode=no_data` on this engine. Recorded with the numbers rather than
guessed at — a difference whose cause is unknown is not evidence about either
tool, and calling this a rivet defect would be the harness lying in our favour.

Original note follows.

**MSSQL wide-txn** shows `rivet-only` rows — i.e. the REFERENCE lagged, not rivet.
Not yet diagnosed and NOT claimed as a finding: SQL Server's capture job is
asynchronous on both sides and the wait may still be too short for 75 changes.

**MongoDB is NOT running yet.** The connector starts but never records progress
within the readiness window. Two obstacles are already solved and worth keeping:
the Mongo stands live in `stand_default` (not `rivet_default`), and the replica
set advertises itself as `127.0.0.1:27017`, so a driver inside another container
dials itself — `directConnection=true` alongside `replicaSet=rs0` gets past that.
What remains is the readiness signal: `offsets.dat` never appears, so either the
connector needs longer or it is failing after validation.
