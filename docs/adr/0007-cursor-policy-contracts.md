# ADR-0007: Cursor Policy Contracts (Incremental)

- **Status:** Accepted
- **Date:** 2026-04-15
- **Context:** Epic D introduces an explicit incremental cursor policy: primary column, optional fallback, and progression mode. Execution, plan artifacts, preflight, and apply must agree on what "cursor" means so operators can reason about ordering, state, and safety (see also ADR-0005 PA4).

---

## Definitions

| Term | Meaning |
|------|---------|
| **Primary column** | User-configured `cursor_column` — main monotonic progression key. |
| **Fallback column** | Optional `cursor_fallback_column` — only used when `incremental_cursor_mode: coalesce`. |
| **`SingleColumn` mode** | Predicate and ordering use the primary column only; fallback must not be set. |
| **`Coalesce` mode** | Predicate and ordering use `COALESCE(primary, fallback)`; one scalar cursor string is stored in state (max coalesced value from the last batch via a synthetic result column). |
| **Synthetic cursor column** | Reserved alias `_rivet_coalesced_cursor` appended to every `Coalesce` query; stripped before Parquet/CSV write. |

---

## Contract matrix

| ID | Name | Statement | Enforced by |
|----|------|-----------|-------------|
| **CC1** | Config consistency | `cursor_fallback_column` is valid iff `incremental_cursor_mode: coalesce`; coalesce requires a fallback. | `Config::validate` |
| **CC2** | Plan embeds policy | For incremental exports `ResolvedRunPlan.strategy = Incremental(IncrementalCursorPlan { primary_column, fallback_column, mode })` and is serialized in `PlanArtifact.resolved_plan`. | `build_plan`, serde |
| **CC3** | Apply uses artifact only | `rivet apply` does not re-read cursor policy from YAML; it uses the embedded `IncrementalCursorPlan` (same channel as ADR-0005 PA1). | `apply_cmd` + artifact |
| **CC4** | Single-column state key | `SingleColumn` stored cursor equals the primary column's last row value. | `ExtractionStrategy::cursor_extract_column`, `single.rs` |
| **CC5** | Coalesce state key | `Coalesce` stored cursor equals the last row's `_rivet_coalesced_cursor`; that column is stripped before Parquet/CSV write. | `ExportSink::strip_internal_column`, `extract_last_cursor_value` via `cursor_extract_column` |
| **CC6** | Incremental SQL shape | Incremental queries are **single-level** and end with `ORDER BY` so the final Arrow batch carries the maximum progression value. See table below. | `source::query::build_incremental_query` |
| **CC7** | Preflight alignment | EXPLAIN and MIN/MAX range probes use the same key expression as execution. | `preflight/cursor_expr::incremental_key_expr` |
| **CC8** | PA4 unchanged | Cursor snapshot integrity (ADR-0005 PA4) still compares one opaque string in `StateStore` to `cursor_snapshot`; the semantics of that string are mode-dependent but the check is unchanged. | `PlanArtifact::cursor_matches` |
| **CC9** | Identifier quoting | Both primary and fallback columns, and the synthetic cursor alias, are quoted via `sql::quote_ident` (`"…"` for Postgres, `` `…` `` for MySQL). | `source::query::build_incremental_query` |
| **CC10** | Cursor value escaping | Cursor values are emitted as SQL string literals with single quotes doubled (`'` → `''`). | `source::query::escape_sql_string` |

---

## SQL shape (CC6)

### `SingleColumn`

Subsequent run:
```sql
SELECT * FROM (<base>) AS _rivet
WHERE <P> > '<cursor>'
ORDER BY <P>
```

First run (no stored cursor) omits the `WHERE`.

### `Coalesce`

Single-level wrapper — the outer `ORDER BY` is what guarantees the last Arrow batch holds the maximum `COALESCE` value:

```sql
SELECT _rivet.*, COALESCE(_rivet.<P>, _rivet.<F>) AS "_rivet_coalesced_cursor"
FROM (<base>) AS _rivet
WHERE COALESCE(_rivet.<P>, _rivet.<F>) > '<cursor>'
ORDER BY COALESCE(_rivet.<P>, _rivet.<F>), _rivet.<P>, _rivet.<F>
```

First run omits `WHERE`. `<P>` and `<F>` are `quote_ident`-quoted; `<cursor>` is escaped per CC10.

An earlier two-level shape (`SELECT _i.* FROM (ORDER BY ...)` plus an outer projection) was rejected because SQL does not preserve inner ordering through an outer `SELECT`: the last batch could miss the max coalesced value and stored cursor could go backwards.

---

## Prioritization interaction

`CursorQuality` in planning consumes resolved `IncrementalCursorPlan.mode` (`plan/inputs.rs`): `Coalesce` maps to weaker tiers than a fully indexed single column even when both preflight hints (index usage, observed range) are positive, reflecting the higher uncertainty of multi-column progression.

---

## Out of scope (v1)

- Lexicographic **pair** cursors `(a, b)` with two stored values
- Runtime `NULL`-aware progression without `COALESCE` in SQL
- Changing the SQLite state schema beyond a single `last_cursor_value` text field
- More than one fallback column

---

## Summary

Incremental exports now have an explicit **cursor policy** in config and a resolved **`IncrementalCursorPlan`** in the execution plan. `Coalesce` uses one stored cursor string, a single-level SQL wrapper with an outer `ORDER BY`, and a synthetic column that is stripped before write — preserving ADR-0005 apply semantics and keeping destinations clean.
