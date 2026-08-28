# ADR-0031 — `pgoutput` replaces `test_decoding` for PostgreSQL CDC

Status: **accepted**, partially implemented
Date: 2026-08-27

## Context

rivet's PostgreSQL CDC adapter consumes a `test_decoding` slot through
`pg_logical_slot_peek_changes()`. `test_decoding` is documented by PostgreSQL as an
**example** output plugin; its output format is explicitly not an API and carries no
compatibility guarantee. Building on it costs a 278-line hand-written text parser —
26% of `src/source/postgres/cdc.rs` — and inherits a defect class that cannot exist
on a binary protocol. Three of those defects were measured in this repository:

* `datestyle='German, DMY'` nulled every timestamp;
* `bytea_output='escape'` corrupted every bytea;
* a `timestamptz` rendered in the polling session's zone corrupted every value by
  the offset, and could not parse a negative one at all.

A fourth is structural rather than incidental: `unchanged-toast-datum` and `null`
are ordinary TEXT in that format, so a column whose real value is that text is
indistinguishable from the marker. rivet has now paid for that twice.

## Decision

Move to `pgoutput`, PostgreSQL's own binary logical-decoding plugin, read in
`binary` mode. One reader, no flag.

## What was measured before deciding (2026-08-27, pg-cdc stand, PostgreSQL 16.14)

**`pgoutput` does NOT require the streaming protocol.** It reads through the same
SQL poll rivet already uses —
`pg_logical_slot_peek_binary_changes(slot, NULL, NULL, 'proto_version','1',
'publication_names', pub[, 'binary','true'])`. The module doc's reasoning ("no
mature streaming crate") is correct about crates and does not constrain the plugin.

| | `test_decoding` | `pgoutput` |
| --- | --- | --- |
| NULL | the string `null` | its own cell tag `n` |
| unchanged TOAST | the string `unchanged-toast-datum` | its own cell tag `u` |
| column types | inferred from `[integer]` in the text | a type OID per column |
| key columns | not reported | a flag per column |
| session state | `datestyle` / `bytea_output` / TimeZone shape the text | none in binary mode |
| format stability | none promised | `proto_version`, versioned |

`pgoutput`'s TEXT mode is still session-shaped (measured: `01.03.2026` under
`German, DMY`), so **binary mode is the load-bearing half**, not the plugin alone.

### The two crates were probed, and neither is taken

* `pg_walstream` 0.8.1 — its parser DOES accept the SQL-polled bytes directly (no
  XLogData wrapper; my first reading of its API was wrong), and with
  `default-features = false` it pulls only bytes/chrono/futures-core/memchr/serde/
  smallvec/tracing — no tokio, no rustls, no `pq-sys`.
* `pgwire-replication` 0.4.0 — connects and streams, but is a TRANSPORT: it hands
  `Relation`/`Insert` back as raw bytes, so a decoder is needed either way, and it
  brings tokio + tokio-postgres + rustls + SCRAM.

Both are under a year old with one maintainer each. The decoder is ~300 lines we
must understand to debug either of them, so it is written here.

## Consequences

**Gained.** The whole session-state class becomes structurally impossible. The
278-line text parser goes, including `parse_pg_array_literal` (142 lines — arrays
arrive as elements). Column types and replica identity come from the wire instead
of a catalog round-trip.

**Gained, and bigger than the parser:** a publication filters SERVER-SIDE. Today
the slot decodes the whole database, which is why `check_configured_tables_are_
routable` + `classify_routing` are 164 lines, why the commit boundary must be
recorded before the routing filter, why the sink needs a re-drain loop against
uncaptured-table starvation, and why an uncaptured table's poison must be deferred.
Those classes shrink or disappear together.

**Cost.** `pgoutput` needs a PUBLICATION, and creating one needs CREATE on the
database. `test_decoding` needed no such privilege. rivet must either create the
publication or refuse and name the one command — refusing is the safer default,
since a publication is server state a tool should not invent silently.

**Migration.** The plugin is fixed at slot creation and there is NO `ALTER` for it
(verified: an existing `test_decoding` slot rejects `proto_version`, and the only
slot functions are create/drop/copy). An existing deployment must drain the old slot
to empty, create the new one, confirm overlap, then drop the old — anchor first,
then cut over, the same ordering the recovery hints were corrected to in round 17.
Nothing else in the tree depends on this: at the time of writing there is no
released CDC and no persistent slot on any stand.

## Implementation status

Done, and each piece pure and offline-graded against REAL wire bytes:

* `pgoutput.rs::decode` — the message decoder. 9 mutants RED
  (`tests/fixtures/pgoutput/messages.hex`);
* `…::value_from_binary` — per-OID value decoding through `postgres_types::FromSql`,
  the same implementations the batch export already trusts. 6 mutants RED
  (`…/binary_values.hex`);
* `…::Assembler` — the relation cache and `Message` → `ChangeEvent` framing, with
  `committed` on the true commit boundary. 11 mutants RED;
* the SCENARIOS live in the rig (`tests/common/pg.rs`: `pg_hard_types_ddl`,
  `pg_all_null_row`, `in_one_transaction`, `incompressible_text_sql`), and one live
  test renders them to the fixture. They were a scratch script first, which is the
  bespoke-harness shape the rig exists to refuse.

Three protocol facts the move corrected, each measured rather than reasoned:

* **a key-only image is not NARROWER.** A tuple always carries as many cells as the
  relation has columns; the replica identity decides which hold a VALUE and pads the
  rest with the NULL tag. This is why `decode_tuple`'s arity check is an equality;
* **`REPLICA IDENTITY FULL` does not stop an unchanged TOAST in the NEW tuple** —
  identity shapes the OLD one. Refusing on it would refuse most UPDATEs on any table
  with a large column, and the obvious advice ("set FULL") would not have helped.
  The assembler recovers from the pre-image, as the text reader does;
* **a refusal mid-transaction must poison the rest of it.** A caller that logs the
  error and keeps feeding otherwise gets a PARTIAL transaction — measured: a DELETE
  without its UPDATE. At-least-once is transaction-atomic, so half of one is worse
  than none, and remembering that is not the caller's job.

Remaining, all of it glue around those three:

1. slot creation with `pgoutput`; publication resolution (create or refuse);
2. the read query and its options (`binary=true` is the load-bearing half);
3. delete the text parser, and remove the `allow(dead_code)` the decoder carries
   with its own expiry note;
4. simplify routing once server-side filtering is in force — MEASURE which classes
   it actually removes rather than assuming all of them;
5. the live PG CDC suite: the rig still creates `test_decoding` slots.

### Two fixture thresholds this cost, recorded so they are not rediscovered

* the unchanged-TOAST marker only appears for a value stored OUT OF LINE. A
  compressible 9 KB column stays inline and never produces `u` — an earlier probe
  saw no marker and nearly concluded the protocol does not send one. Use
  incompressible data (concatenated md5s);
* a NULL INSIDE an array value is not a NULL CELL. The first capture had only the
  former and `Cell::Null -> Cell::Text` survived it.
* and a third, from the assembler: a transaction of ONE row makes `committed` on
  the last, on the first, and on every event the same answer. Three rows separate
  all three.
