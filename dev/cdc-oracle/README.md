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

## Status

Scaffolding only: sink + this contract. The compose service, the compare step and
the first differential test are the next commits.
