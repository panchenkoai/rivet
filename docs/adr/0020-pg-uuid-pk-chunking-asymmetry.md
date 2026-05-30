# ADR-0020: PostgreSQL UUID-PK Tables — Chunking Asymmetry vs MySQL

**Status**: Accepted (partial: layer 2 closed; layer 1 deferred)
**Date**: 2026-05-30

---

## Context

Production tables with non-integer primary keys (most commonly UUID) are
the canonical case where rivet's range-chunked execution path
(`SELECT … WHERE id BETWEEN low AND high`) does not apply: there is no
total ordering on UUID values that maps cleanly to integer ranges. OPT-4
shipped keyset (seek) pagination for exactly this case
(`WHERE id > '<last>' ORDER BY id LIMIT n`), which works on any
single-column, NOT NULL, UNIQUE key regardless of underlying type.

A live-test sweep added in this session
(`tests/live_keyset.rs::keyset_pg_uuid_pk_via_explicit_chunk_by_key_roundtrips_full_set`
and friends) surfaced **two distinct layers** of why PG UUID-PK
chunking was not working end-to-end:

### Layer 1 — Planner: PG does not auto-keyset on non-int PK

`src/plan/build.rs::resolve_chunked_strategy` deliberately scopes the
auto-keyset fallback to MySQL only:

```rust
if config.source.source_type == crate::config::SourceType::Mysql
    && let Some(key) = introspection.auto_keyset_key()
{
    // … auto-select Keyset strategy
    return Ok(ExtractionStrategy::Keyset(KeysetPlan { … }));
}
anyhow::bail!("no safe shape … use mode: full");
```

The rationale in the comment:
> "MySQL has no server-side cursor, so a non-int-PK table has no safe
> range-chunk shape… PG keeps refusing — its `DECLARE CURSOR` snapshot
> is already bounded, so `mode: full` is the safe answer there."

This is **partially true**. PG `DECLARE CURSOR` bounds client-side RAM
(rows do not materialise into the client all at once), which is the
property that makes `mode: full` "safe" in the OOM sense. It does *not*
bound:

- **Wall time of the long-open query**: a single cursor over a 100M-row
  UUID-PK table holds a transaction open for tens of minutes — every
  network blip, every server restart, every long-running operator
  session dies on that snapshot.
- **Server-side resource hold**: an open cursor pins a transaction
  snapshot, blocking vacuum / freeze on the underlying tuples; on a
  busy OLTP source this lengthens `xmin` horizon and can spook DBAs.
- **Network latency cost on hand-offs**: long-running session over a
  bouncer / proxy layer is more likely to die mid-fetch than many short
  keyset pages would be.

So `mode: full` is RAM-safe but not durability-safe for the *large*
UUID-PK table the operator most needs chunking for.

### Layer 2 — Sink runtime: `FixedSizeBinary(16)` was unsupported

Even when an operator opted in explicitly via
`chunk_by_key: <uuid_col>` (the documented escape hatch from layer 1),
the keyset runtime failed at page 0:

```
export 'X': keyset could not read the 'id' value from the last row
of page 0 (NULL or unsupported type) — cannot advance safely.
The key must be NOT NULL and one of: integer, float, string,
timestamp, date.
```

`src/pipeline/sink/cursor.rs::extract_last_cursor_value` had arms for
`Int{16,32,64}`, `Float64`, `Utf8`, `Timestamp(µs)`, and `Date32`. PG
`uuid` maps to Arrow `FixedSizeBinary(16)` (per ADR-0014 §"UUID":
parquet-rs emits native `LogicalType::Uuid` via the `arrow.uuid`
extension type, and the 16-byte canonical encoding is the inter-engine
contract). With no `FixedSizeBinary(16)` arm, the helper returned
`None`, the keyset runner bailed with "unsupported type", and even the
explicit-key path was dead.

## Decision

### Layer 2 (this commit): close the sink-runtime gap

Add a `DataType::FixedSizeBinary(16)` arm to
`extract_last_cursor_value` that decodes the 16 bytes into the
canonical `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` form. The keyset query
builder's PG path (`cursor_rhs(SourceType::Postgres, …)`) already emits
`E'<value>'` literals that PG implicitly casts to the column's type, so
no separate UUID-cast logic is needed in the query builder — the
server-side cast `id > E'<uuid>'` resolves to `id > '<uuid>'::uuid`
because `id`'s type is known.

Update the error-message supported-types list to include `uuid`. Add
two unit tests (`cursor_fixed_size_binary_16_decodes_to_canonical_uuid_string`,
`cursor_fixed_size_binary_wrong_length_returns_none` — defensive
against a non-16 array landing where 16 was expected) and a live
round-trip test
(`keyset_pg_uuid_pk_via_explicit_chunk_by_key_roundtrips_full_set`)
that proves the full operator path works.

### Layer 1 (deferred): planner auto-resolution stays MySQL-only

Do **not** flip the `if SourceType::Mysql` guard in
`resolve_chunked_strategy` to also auto-keyset PG. Reason: this is a
behaviour-changing default that affects every PG-UUID-PK export run by
every existing config, and the comment's "DECLARE CURSOR is bounded"
rationale, while incomplete, is not *wrong* for small-to-medium
tables. An operator who knows their PG UUID table is small can prefer
the simpler `mode: full` path; an operator who knows it is huge has the
escape hatch (`chunk_by_key: <col>`) and now (layer 2 closed) it works.

Promote layer 1 to a decision after a real operator request, not on
the strength of "we technically can".

## Operator UX as of this ADR

| Source | PK type | Supported paths |
|---|---|---|
| PG | `bigint` / `int` PK | `mode: chunked` auto-selects range chunking ✓ |
| PG | `UUID` PK | `mode: full` (snapshot) ✓ <br> `mode: chunked` + explicit `chunk_by_key: <col>` ✓ (since this ADR) <br> `mode: chunked` without explicit key — bails with "no safe shape" actionable error ✗ |
| PG | `TEXT` / `VARCHAR` unique key | Same as UUID — explicit `chunk_by_key:` ✓ |
| MySQL | `bigint` / `int` PK | `mode: chunked` auto-selects range chunking ✓ |
| MySQL | `CHAR(36)` UUID PK | `mode: chunked` auto-selects keyset on the UUID column (OPT-4) ✓ |
| MySQL | `VARCHAR` unique PK | Same — auto-keyset ✓ |

The PG row has the largest column-spread: this ADR documents why and
which path to use when.

## Consequences

**Positive**

- The PG UUID-PK chunking path is now functional end-to-end via the
  explicit `chunk_by_key:` option. Operators with 100M-row UUID tables
  can opt in and avoid the long-cursor `mode: full` risk.
- The sink seam's supported-types list is correct: live error messages
  match the actual code arms.
- Layer 1's planner asymmetry is now documented, not implicit. A
  future operator request to flip the default can be evaluated against
  this paper trail.

**Negative**

- Operators who write configs against a UUID-PK PG table without
  reading this ADR will hit the "no safe shape" actionable error and
  need to choose between `mode: full` and `chunk_by_key:` explicitly.
  The error message points at the choice, but it is an extra step
  versus MySQL's auto-resolution.
- Layer 1 remains an inconsistency between engines — the rivet
  invariant of "same YAML, same outcome on either engine" does not
  hold for the UUID-PK case.

## Trigger for revisiting layer 1

Open this ADR back up to "Proposed" when **any** of the following
holds:

1. An operator reports a real production case where `mode: full` over
   PG `DECLARE CURSOR` hit a wall-time or vacuum-horizon failure on a
   large UUID-PK table.
2. A second non-int PG keyset request lands (e.g., `TEXT` PK or
   composite key — once two real cases exist, the inconsistency
   becomes harder to defend on "small table assumption" grounds alone).
3. The MySQL keyset path acquires non-trivial behaviour the PG path
   lacks (e.g., explicit timestamp keyset, hash-partitioned source) —
   keeping the two engines symmetric becomes the cheaper option.

## References

- `src/pipeline/sink/cursor.rs::extract_last_cursor_value` —
  the `FixedSizeBinary(16)` arm + the two new unit tests.
- `src/pipeline/keyset.rs:146` — error message with the updated
  supported-types list (adds `uuid`).
- `src/plan/build.rs::resolve_chunked_strategy` lines 364-394 —
  the `if SourceType::Mysql` guard that constrains layer 1.
- `src/source/query.rs::cursor_rhs` — the `E'…'` literal-with-cast
  pattern that makes the PG keyset-WHERE path UUID-aware without
  separate cast logic.
- `tests/live_keyset.rs` — four live tests covering the four paths in
  the operator-UX table above:
  - `keyset_varchar_pk_roundtrips_full_keyset_across_pages` (MySQL)
  - `keyset_mysql_uuid_pk_roundtrips_full_keyset_across_pages`
  - `snapshot_pg_uuid_pk_roundtrips_full_uuid_set` (PG, `mode: full`)
  - `keyset_pg_uuid_pk_via_explicit_chunk_by_key_roundtrips_full_set`
    (PG, explicit `chunk_by_key:` — closes layer 2 of the gap).
- ADR-0014 — target type materialization (PG `uuid` →
  `FixedSizeBinary(16)` + `arrow.uuid` extension type).
- ADR-0016 — nullability propagation deferred (related precedent:
  documents an asymmetric type-faithfulness gap with a deferred
  trigger).
- ADR-0017 — per-runner durability ordering map (similar shape:
  acknowledged asymmetry with explicit operator-facing UX).
