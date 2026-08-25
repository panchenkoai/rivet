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
    mssql       AGREE   AGREE        AGREE 75/75   AGREE
    mongo       AGREE*  na           AGREE 75/75   AGREE     (* delete excluded, see below)

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
    mongo       AGREE*  na           AGREE 75/75   AGREE     (* delete excluded, see below)

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


## The harness compares KEYS, not values — and that was a real blind spot

An adversarial pass RAN the check I had not: it took a real capture, rewrote the
parquet with `NULL::VARCHAR AS v` — 100% of the value column destroyed — and
`compare.py` printed a byte-identical **AGREE**. Every "15 of 16 agree" result in
this file was therefore weaker than it read: it proves the two tools saw the same
CHANGES, not that rivet wrote the same VALUES.

`--value <col>` (repeatable) now compares cells as text on both sides, and it is
verified to catch that mutation: the same NULLed parquet goes from AGREE to four
differing rows.

It is **not on by default yet**, deliberately. The DELETE path still compares
unequal, because the reference carries no `after` document for a delete while the
value extract reads only that side. Turning it on now would mean a false alarm on
every run, and a harness that cries wolf gets muted — which is exactly how this
blind spot survived in the first place.

## The gap this harness does NOT cover: a degrading source LINK

Measured 2026-08-25 while looking for negative scenarios: `toxi_` appears **zero**
times in `live_cdc.rs`, `live_cdc_mssql.rs` and `live_cdc_mongo.rs`. Every fault
test in the suite either kills the PROCESS (`RIVET_TEST_PANIC_AT`) or aims at the
DESTINATION upload. A degrading source link is untested on all four engines.

It is a different shape from a crash, which is why the crash coverage does not
stand in for it: the process stays ALIVE holding a half-read stream, and that is
where a reader can mistake "the connection ended" for "the log ended" — a bounded
run then writes `_SUCCESS` over a short capture.

Four rows belong in `cdc-evidence-matrix.yaml` when there is something to put in
them:

    cdc_source_link_drops_mid_capture              toxi_disable
    cdc_source_link_stalls_under_a_large_txn       toxi_add_bandwidth
    cdc_link_cut_after_flush_before_ack            toxi_disable, timed
    cdc_latency_must_not_shorten_a_bounded_run     toxi_add_latency

They are NOT in that matrix yet, deliberately. Its ratchet is shrink-only and adding
four unproven cells would have pushed 22 past a ceiling of 23 — the ledger refuses
to let a new gap be declared, which is the behaviour it exists for. The rows go in
when a test goes in, and the note lives here meanwhile so the surface is not
forgotten rather than being laundered into a ledger as "known".

Per engine, the reason each is worth running:

- **postgres** — the slot peek is non-consuming, so the ARGUMENT is that a lost
  connection re-reads from the un-acked position. That argument has never been run,
  and this engine's `until_current` bound is load-bearing for termination.
- **mysql** — a long-lived dump connection plus a file checkpoint: a drop between
  read and part-commit is the window the idle-first-run anchor rule was written for.
- **mssql** — reads are ordinary SELECTs, so a drop should surface as a query error;
  whether the run reports failure or an EMPTY SUCCESS is untested.
- **mongo** — the stream is tailable and the driver retries internally, which is
  precisely why a silent short read is plausible here rather than a loud one.


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

Three things must be true before the row belongs there, and none is yet:

1. **The gate must stand up the reference.** Pull `quay.io/debezium/server`, put a
   receiver on the stand network, wait on the engine's own readiness signal. Every
   config trap is in this README and each presents as "started and captured
   nothing".
2. **`--value` must be on.** The comparison projects to (op, key) today. `--value`
   is proven to catch a fully NULLed column, but the DELETE path still compares
   unequal, so it is opt-in. Wiring the harness in without it wires in a check that
   cannot see cell corruption — the exact blind spot an adversarial pass found here.
3. **The harness must stay quiet when nothing is wrong.** It produced four false
   findings from its own defects (shared work dirs, a fixed flush wait, a lossy
   HTTP sink) before producing a real one. A gate that reports its own races as
   release blockers gets bypassed.
