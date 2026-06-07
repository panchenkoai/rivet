# ADR-0015: Source Introspection is a Data-Shape Seam, Not a Trait

**Status**: Accepted
**Date**: 2026-05-30

---

## Context

Chunked-mode planning needs four facts about a source table before it can
resolve the extraction strategy: the single-column integer PK (if any),
the set of usable keyset keys (single-column UNIQUE NOT NULL indexes),
the row estimate, and the average row width in bytes. PostgreSQL and
MySQL each expose this data through their own catalog, and the plan
layer needs to ask "either source" for the same shape of answer.

The current arrangement (since OPT-4 shipped, commit `40433a0`):

- `src/source/mod.rs::TableIntrospection` — shared struct holding all
  four facts plus the derived `auto_keyset_key()` and
  `is_usable_keyset_key()` helpers.
- `src/source/postgres/mod.rs::introspect_pg_table_for_chunking(url, tls,
  qualified_table) -> Result<TableIntrospection>`.
- `src/source/mysql/mod.rs::introspect_mysql_table_for_chunking(url, tls,
  qualified_table) -> Result<TableIntrospection>`.
- `src/source/mssql/mod.rs::introspect_mssql_table_for_chunking(url, tls,
  qualified_table) -> Result<TableIntrospection>` (added with the SQL Server
  engine; probes `sys.*` catalog views).
- `src/plan/build.rs::resolve_chunked_strategy` dispatches by
  `match config.source.source_type` to the right free function.

Architecture-review walks have re-suggested unifying these into a
`trait Introspector` with one impl per engine, citing "code drift" /
"parallel modules with no shared abstraction" between the (now three)
introspection functions. Each suggestion has reached the implementation
stage, been examined against the actual code, and been rejected for the
reasons in this ADR.

---

## Decision

**The introspection seam lives at the data shape (`TableIntrospection`),
not at a trait.** The three per-engine functions remain free functions,
dispatched by `match source_type` at the one call site in `plan/build.rs`.
No `trait Introspector` is introduced. (The deletion-test rationale below
scales unchanged to N engines: the functions share a data shape, not logic.)

---

## Why a trait would not deepen the seam

A `trait Introspector { fn introspect_table(url, tls, qualified_table)
-> Result<TableIntrospection>; }` would add:

- the trait definition,
- one impl per engine (still hand-written, since each engine queries a
  different catalog with a different client crate),
- a factory function `fn introspector_for(source_type: SourceType) ->
  Box<dyn Introspector>` — which is itself the same `match` the call
  site has today.

It would remove: nothing. The `match` doesn't disappear; it moves up
into the factory.

The bodies share no extractable implementation logic:

| Concern | Postgres | MySQL |
|---|---|---|
| Row estimate source | `pg_class.reltuples` | `information_schema.TABLES.TABLE_ROWS` |
| Avg row width | `pg_relation_size(c.oid) / reltuples` | `AVG_ROW_LENGTH` with `correct_innodb_avg_row_length` overflow correction |
| Single int PK probe | `pg_index` JOIN `pg_attribute` JOIN `pg_type`, filtered to `int2/int4/int8` | `information_schema.STATISTICS` filtered to `INDEX_NAME='PRIMARY'` + `SEQ_IN_INDEX=1` + composite check |
| Keyset-key probe | `pg_index.indisunique` + `attnotnull` | `information_schema.STATISTICS.NON_UNIQUE=0` + nullability join |
| Client crate | `postgres` | `mysql` |
| SQL dialect | PG (`$1`/`$2`, `regclass`) | MySQL (`?`, no `regclass`) |

Per the [deletion test](../../.claude/skills/improve-codebase-architecture/LANGUAGE.md):
deleting the hypothetical trait concentrates no complexity — the two
free functions remain, the shared struct remains, the dispatch match
remains. The trait was pure ceremony around two functions whose only
shared property is "produce the same data shape."

The two-adapters-for-a-real-seam guideline (also in LANGUAGE.md)
expects the adapters to share implementation logic the seam can hide.
Here the adapters share no implementation logic, only their result
type — which is the actual seam, and it is already in place.

---

## Consequences

**Positive**

- Adding a third engine (DuckDB, ClickHouse, etc.) means writing one
  free function returning `TableIntrospection` plus one `match` arm in
  `resolve_chunked_strategy`. No trait surface to satisfy; no factory
  to register against; no orphan-impl problem with external crates.
- Bug fixes to one engine's catalog query stay scoped to that engine.
  A change to PG's `int2/int4/int8` whitelist does not have a parallel
  in the MySQL function (MySQL does its own type check via
  `arrow_convert::rivet_type_for_mysql_column`); the trait would not
  prevent this asymmetry, and the free-function arrangement makes the
  asymmetry visible at the call site.
- Future architecture-review walks see the doc-comment on
  `TableIntrospection` (and this ADR) before re-suggesting the
  refactor.

**Negative / trade-offs**

- Callers cannot pass an `Introspector` parameter generically; they
  must accept the concrete `SourceType` enum and dispatch. In practice
  this is a single line in `plan/build.rs` and a non-issue.
- The parallel shape ("two functions with identical signatures
  returning the same type") looks like duplication on a first read.
  Documented at the seam in `src/source/mod.rs::TableIntrospection` so
  the first read shows the rationale.

---

## Alternatives considered

**`trait Introspector` per engine.** Rejected on the deletion-test
grounds above: adds ceremony without consolidating implementation
logic.

**Single function with internal `match`** — `fn introspect(source_type,
url, tls, table)` that internally selects PG vs MySQL. Rejected for
weaker encapsulation: it widens the dependency surface of a single
function to *both* engine modules, and the PG function would carry a
`pub(crate)` lifetime even when only MySQL is used. Current arrangement
isolates each engine module's surface.

**Move both introspection functions into `source::introspect`
sub-module.** Considered. The functions already live in the engine
modules where their catalog queries belong; moving them to a
cross-engine sub-module would invert the locality (catalog queries are
engine-specific). Rejected as a re-arrangement without locality gain.

---

## When this decision should be revisited

If the introspection surface grows to a third method that *does* share
non-trivial implementation logic across engines (e.g., a normalized
"column statistics" query that both engines can build on top of an
existing catalog probe), the trait becomes a real deepening — the
shared default method would carry the leverage. The next
architecture-review walk that proposes the trait must point at the
shared logic that would live behind a default impl, not at the
parallel signatures alone.

Until then: this ADR exists so future agents see the prior reasoning
and can short-circuit the re-suggestion.

---

## References

- `src/source/mod.rs::TableIntrospection` — the seam, with doc-comment
  pointing at this ADR.
- `src/source/postgres/mod.rs::introspect_pg_table_for_chunking`
- `src/source/mysql/mod.rs::introspect_mysql_table_for_chunking`
- `src/plan/build.rs::resolve_chunked_strategy` — the one call site
  and dispatch `match`.
- Commit `40433a0` — original OPT-4 keyset work that introduced the
  shared `TableIntrospection` struct.
- ADR-0010 — two parallel *execution* engines (in-process chunked vs
  subprocess fan-out). Note: that ADR is about execution-layer
  parallelism, not the planning-layer introspection seam this ADR
  addresses.
- ADR-0011 — `Source: Send` not `Sync`. Note: that ADR governs the
  `Source` trait at the *execution* layer; introspection is a
  *planning*-layer concern and intentionally not on that trait.
