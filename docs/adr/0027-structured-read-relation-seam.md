# ADR-0027: A structured read-relation on the export request seam

**Status**: Accepted
**Date**: 2026-07
**Relates to**: ADR-0011 (Source trait), ADR-0020 (catalog-hint query)

---

## Context

`Source::export` receives an `ExportRequest` whose `query` is an
**already-materialized SQL string** (`resolve_query` turns a `table: orders`
shortcut into `SELECT * FROM orders`). This is the right currency for the three
SQL engines. It is the wrong currency for a **document store**: MongoDB has no
SQL, so the Mongo adapter was forced to *un-parse the SQL back into intent* —
`collection_from_query` stripped `SELECT * FROM ` to recover the collection
name, reinventing what `sql::strip_select_star_from` already does for the
PostgreSQL catalog-hint path (ADR-0020). The reconcile count added a **second**
un-parser (`last_from_identifier`) for `SELECT COUNT(*) FROM (SELECT * FROM
<coll>) …`. Intent → serialise to SQL → parse SQL back to intent, in two places.

The architecture review flagged this (candidate A): the seam leaks "SQL is the
universal read currency", and every non-SQL adapter pays to undo it.

## Decision

**Carry the bare source relation structurally on `ExportRequest`, alongside the
SQL string — additive, not a replacement.**

`ExportRequest` gains `base_relation: Option<&str>` — the `[schema.]table`
identifier when the export is a `table:` shortcut, `None` for a hand-written
`query:` or any filtered/wrapped form. It is computed **once**, inside the
`unwrapped`/`wrapped` constructors, via the existing
`sql::strip_select_star_from` — so every runner populates it for free with no
call-site churn.

- SQL engines **ignore** `base_relation` and run `query` exactly as before —
  zero behavioural change, zero risk.
- The document adapter reads `request.base_relation` directly: a `Some` is the
  collection to scan; a `None` is an actionable "MongoDB needs a `table:`
  shortcut" error. `collection_from_query` is deleted.

## Rationale

- **Delete a reinvention.** `collection_from_query` duplicated
  `strip_select_star_from`. The relation is now extracted once, by the shared
  helper, at request construction — not re-derived per non-SQL adapter.
- **Additive = low-risk.** No SQL-engine signature or behaviour changes; the new
  field is optional and SQL engines never read it. This is deliberately *not*
  the full "reshape the seam" refactor — that would touch all three engines for
  a larger, riskier change. We take the honest slice that removes the batch-path
  un-parser now.
- **One consumer today, real seam tomorrow.** MongoDB is the first non-SQL
  adapter; `base_relation` is the seam the next one (or Mongo CDC's schema
  resolve, which also builds `SELECT * FROM {table}`) reads instead of parsing.

## Consequences

- The reconcile row count still un-parses SQL (`last_from_identifier`), because
  `Source::query_scalar` receives a bare SQL string with no structured
  counterpart. Giving the reconcile count a typed request is the **remaining
  tail** of this ADR — deferred until it earns its churn (it touches the
  `query_scalar` seam across all engines).
- `base_relation` is populated for SQL engines too. A future step could let the
  PostgreSQL catalog-hint path read it instead of re-parsing `catalog_hint_query`
  — folding another SQL un-parse into the same seam. Not done here.
