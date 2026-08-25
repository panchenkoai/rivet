# ADR-0024: Migrate PostgreSQL CDC from `test_decoding` to `pgoutput`

Status: **accepted (roadmap)** — not scheduled; criteria below gate the start.

## Context

The PostgreSQL CDC adapter polls a logical slot through the `test_decoding`
output plugin and **parses its human-readable text rendering** back into typed
values (`src/source/postgres/cdc.rs`). The 2026-07 reliability campaign found
27 defects across the CDC surface; **6 of them existed only because of this
text hop** — each one a case of the rendering carrying less, or
differently-shaped, information than the wire value:

1. UUID rendered as 36-char text → nulled by the 16-byte builder.
2. `bytea` rendered as `\x`-hex → carried as text instead of bytes.
3. `TIME` rendered as text the timestamp-prefix check missed → nulled.
4. `INTERVAL` rendered as PG prose ("1 year 2 mons") vs batch's ISO 8601.
5. Arrays rendered as the `{…}` literal → text column instead of `List`.
6. `timestamptz` rendered **in the polling session's timezone** → the offset
   was dropped, corrupting every value by the zone delta at any non-UTC
   session, and silently nulling at negative offsets (finding #24).

Each was fixed with a parser; the *class* remains: any session state that
shapes the rendering (timezone, `DateStyle`, `bytea_output`,
`extra_float_digits`) is a latent parity bug, discovered only when a
deployment's session differs from the test stack's.

`pgoutput` — the logical replication protocol's native output plugin — emits
**binary tuple data with per-column type OIDs**, no session-dependent
rendering at all. The entire bug class is unrepresentable.

## Why not now

- `pgoutput` requires the **streaming replication protocol**
  (`START_REPLICATION`, keepalive/feedback messages), not plain SQL polling.
  The sync `postgres` crate rivet uses does not speak it; the ecosystem
  crates for it were judged immature at the original design point, and the
  poll model (`pg_logical_slot_peek_changes`) deliberately reuses the
  existing dependency + the peek→flush→ack at-least-once seam.
- It needs a `PUBLICATION` object per captured table set — a new server-side
  resource with its own lifecycle (validation, doctor checks, drop-on-teardown).
- The text-parse fixes above are **live-pinned** (full-type matrix, non-UTC
  session tests, hostile-value tests), so the remaining risk is bounded to
  renderings not yet enumerated, at session states not yet tested.

## Decision

Migrate when ANY of these fires:

1. A seventh text-rendering defect class surfaces (`DateStyle`, `bytea_output`,
   locale-dependent anything) — i.e. the pin set proves insufficient again.
2. CDC throughput on a hot table becomes parse-bound (profile first: the
   text parse is per-cell; pgoutput decode is per-tuple binary).
3. A maintained, audit-clean streaming-replication crate reaches maturity
   (re-evaluate every dependency-refresh cycle).

Migration shape: a second `ChangeStream` impl (`PgOutputStream`) behind the
same trait + the same commit/ack seam; `test_decoding` stays as the fallback
until the live matrix + non-UTC + hostile suites pass against both, then
becomes the compatibility path for one release before removal. The per-engine
anchor-model contract is unchanged — PG still pins server-side at slot creation
(the slot is still the anchor); the contract is documented in the
`ensure_anchor` doc comment (`src/source/cdc/mod.rs`).

## Consequences

- Until migration, every new PG type mapping MUST add its `test_decoding`
  rendering to the parser AND a matrix row (existing process rule).
- The non-UTC session test (`pg_cdc_non_utc_database_timezone_matches_batch`)
  is the canary for this ADR — it fails first if a new session-shaped
  rendering appears.

## Amendment 2026-08-24: a SECOND text class — identity, not values

The six defects above are all about the rendering of a *value*. A hostile
verification pass found a distinct class the pin set never covered: the rendering
of an **identity**. `test_decoding` names relations as TEXT, and every routing
decision compares that text byte-exact, so the same hop corrupts *which table a
change belongs to* rather than what it contains.

Four measured this session, each a silent loss past an advancing slot:

1. **Partitioned parent.** `test_decoding` names the PARTITION a row landed in.
   A config naming the logical parent captured 0 of 2 rows, reported
   `status: success`, advanced the slot, and the corrected re-run recovered
   nothing (the WAL was already freed).
2. **A folded twin.** The schema probe interpolates the config into
   `SELECT * FROM {table}` (PostgreSQL FOLDS it); the router compares byte-exact.
   With `"MixedCase"` and `mixedcase` both present, rows were written under the
   WRONG table's schema — the real column absent entirely, exit 0.
3. **A 3-part name.** `to_regclass` accepts `db.schema.table`; `table_matches`
   splits on the first dot. Resolves, never routes.
4. **TRUNCATE.** Arrives as a line of prose naming a comma-separated LIST of
   relations, which had to be re-parsed (twice — the first fix anchored on the
   wrong separator and matched nothing).

`pgoutput` makes all four unrepresentable for the same structural reason the
value class disappears: it carries a **relation OID plus a Relation message**,
not a name to parse, and TRUNCATE is a typed message carrying an ARRAY of
relation OIDs rather than text. Partition identity is a publication-level
setting — `publish_via_partition_root`, which Debezium 3.2 exposes as
`publish.via.partition.root` — and it does not exist for `test_decoding` at all,
because publications are a pgoutput concept.

External corroboration worth recording: **Debezium supports only `decoderbufs`
and `pgoutput`** — `test_decoding` is not a supported plugin, and PostgreSQL's
own docs call it "meant for testing that replication works rather than for
building robust production apps". The guards this session added are the correct
minimum FOR `test_decoding` (they turn each silent loss into a loud refusal),
but each is a parser standing in for a mechanism the protocol would provide.

### Effect on the decision

Trigger 1 is widened: it now fires on a seventh text-rendering defect class **or
a text-IDENTITY defect class**. The identity class has now fired — four
instances in one day — so by the ADR's own terms this is no longer "not
scheduled" but a candidate whose gate has been met. The blockers in "Why not
now" (the sync `postgres` crate does not speak streaming replication; a
PUBLICATION is a new server-side resource with its own lifecycle) are unchanged
and still real; what has changed is the cost of NOT migrating, which is now
measured rather than projected.

Recommended next step, and deliberately scoped smaller than the migration: a
spike that answers (a) which streaming-replication crate is audit-clean today,
and (b) whether a PUBLICATION can be made optional — capture without DDL rights
was the original reason for this plugin, and if `pgoutput` cannot preserve it,
the migration is a dual-mode adapter rather than a replacement.

### What the rest of the ecosystem does (surveyed 2026-08-24)

Checked because "are we reinventing the wheel" is the right question to ask
before building the seventh parser. Answer: on the plugin choice, yes.

- **Debezium** supports `decoderbufs` and `pgoutput` only. `test_decoding` is not
  a supported plugin at all.
- **PeerDB** takes the parent table name and nothing else — "you don't need to
  specify the names of each partition" — via a publication with
  `publish_via_partition_root` (PG 13-16; on PG 12, a publication FOR ALL TABLES).
- **Sequin's** plugin guide states test_decoding's "output format is not designed
  for production parsing" and calls it "mainly useful for understanding how
  logical decoding works or for quick debugging".
- PostgreSQL's own docs: "meant for testing that replication works rather than
  for building robust production apps".

Two design answers worth stealing regardless of which way this ADR goes:

1. **TRUNCATE is a MODE, not a verdict.** Debezium's `truncate.handling.mode`
   defaults to `skip` and can be set to `include`. rivet now REFUSES, which is
   right for a file/warehouse sink (there is no consumer to interpret a truncate
   event, and the divergence is permanent) — but it is a stricter answer than the
   ecosystem's, and the difference should be a documented choice rather than an
   accident of what we happened to implement.
2. **Partition drop is documented, not detected.** PeerDB explicitly does NOT
   propagate a dropped partition: "we don't delete data matching that partition."
   That is the same class as the `SWITCH PARTITION` finding this session left
   open on SQL Server — rows leaving the source with no events. The mature answer
   is a stated contract, not a detector, and our ledger should record it that way
   rather than carrying it as an unfilled gap forever.
