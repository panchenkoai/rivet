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
becomes the compatibility path for one release before removal. The
`AnchorModel::ServerSide` contract is unchanged (the slot is still the anchor).

## Consequences

- Until migration, every new PG type mapping MUST add its `test_decoding`
  rendering to the parser AND a matrix row (existing process rule).
- The non-UTC session test (`pg_cdc_non_utc_database_timezone_matches_batch`)
  is the canary for this ADR — it fails first if a new session-shaped
  rendering appears.
