# ADR-0016: Nullability Propagation Deferred to v0.8 Phase A

**Status**: Accepted (deferred)
**Date**: 2026-05-30

---

## Context

Rivet's source drivers (PostgreSQL, MySQL) construct a `SourceColumn`
per result column when building the type-mapping pipeline. The
`SourceColumn::nullable` field is intended to carry the source schema's
nullability declaration so it can flow through `TypeMapping` →
`build_arrow_field` → `arrow::Field` and ultimately end up in the
Parquet schema's per-column `repetition` (`OPTIONAL` for nullable,
`REQUIRED` for `NOT NULL`).

The current implementation hardcodes `nullable: true` at every
`SourceColumn` construction site:

```
src/source/postgres/arrow_convert.rs:217   SourceColumn::simple(name, native, true)
src/source/postgres/mod.rs:636             SourceColumn::simple(name, native, true)
src/source/mysql/arrow_convert.rs:244     SourceColumn::simple(name, native, true)
src/source/mysql/mod.rs:617                SourceColumn::simple(..,           true)
```

The first round of the type-roundtrip work (v0.7.8) accepted this as a
conservative default. The Gap #5 invariant audit ("nullable values must
remain nullable") was *technically* satisfied — a source column that
allows NULL maps to a Parquet column that allows NULL. But the
**directional** invariant the audit also implies ("source `NOT NULL`
constraints survive the round-trip") is **not** satisfied: every
column in every output Parquet file is `OPTIONAL`, regardless of the
source's `NOT NULL` declarations.

## Problem

Downstream catalog tools (BigQuery LOAD, ClickHouse `file()` table
function, Snowflake `COPY INTO ... PATTERN`, DuckDB catalog views,
generic Parquet schema viewers) read the Parquet schema to infer the
target table's column nullability. They cannot distinguish:

- "the source column is `NOT NULL` and Rivet exported it faithfully" —
  catalog should mark the target column `NOT NULL`,
- "the source column allows NULL but happened to have no NULLs in this
  run" — catalog must allow NULL,
- "the source column allows NULL and some rows are NULL" — catalog
  must allow NULL.

All three cases produce identical Parquet schemas under the current
implementation: every column marked `OPTIONAL`. Information that was
present in the source schema is lost at the seam.

For an extraction tool that brands itself as type-faithful (ADR-0014:
"Decimal precision/scale must not be silently degraded; timestamp
semantics must be explicit; unsupported types must fail or be
explicitly mapped"), losing source `NOT NULL` is an asymmetric gap
relative to those other type fidelities.

## Why this is not fixed in this release

Per-column nullability is only fully resolvable for the
"single-table SELECT" shape:

```sql
SELECT a, b, c FROM users WHERE …
```

Here every result column maps to a source column with a known
`information_schema.columns.is_nullable` (MySQL) or
`pg_attribute.attnotnull` (PostgreSQL) value. PostgreSQL's wire protocol
makes this easy: `RowDescription` carries `table_oid + column_attnum`
per result column, directly indexable into `pg_attribute`.

For non-trivial query shapes the mapping is partial or absent:

| Query shape | Per-column source nullability available? |
|---|---|
| `SELECT cols FROM table` | Yes — direct mapping |
| `SELECT cols FROM a JOIN b ON …` (INNER JOIN) | Yes if column origins resolve uniquely |
| `LEFT JOIN` outer side | **No** — outer-join columns are nullable regardless of source declaration |
| `SELECT col, COUNT(*), expression(...) FROM …` | Only `col` resolvable; computed columns are not in any source catalog |
| `SELECT * FROM (subquery) AS x` | Subquery-specific; would require recursive resolution |
| `WITH cte AS (…) SELECT FROM cte` | CTEs need the same recursive resolution |

A partial fix ("propagate `NOT NULL` only when the query is a simple
single-table SELECT, fall back to `nullable=true` otherwise") would be
correct but introduces a heuristic the operator cannot trivially
predict from the YAML. A full fix requires either query parsing or a
configuration knob.

The other axis is **MySQL**'s weaker introspection surface:
`information_schema.STATISTICS` carries the data, but MySQL's wire
protocol does not give per-result-column `table_oid + column_attnum`
the way PostgreSQL does — driver would need to parse the query or
require an explicit table hint in the YAML.

The combined work (PG protocol-level lookup + MySQL query parsing +
operator UX for the partial-fix gap + tests across `LEFT JOIN` /
computed-column / CTE shapes) is estimated at 200-400 lines per engine
plus operator-facing documentation. It is the right work for the
**v0.8 Phase A type-report extension** already declared in
ADR-0014 (`## CLI and manifest integration → Phase A`), which adds
per-column type provenance to `rivet check` output. Nullability fits
that surface naturally — the type-report would gain a `nullability`
column with values `from_catalog: NOT NULL`, `from_catalog: NULL`, or
`assumed: NULL (computed / LEFT JOIN / CTE)`.

## Decision

**Nullability propagation is explicitly deferred to v0.8 Phase A.**
The current `nullable=true` hardcode at the four `SourceColumn::simple`
call sites is acknowledged as a known limitation, not a design choice.

The deferral is paper-trailed at the `SourceColumn::nullable` field
documentation (`src/types/source_column.rs`) so the next contributor
who reads the struct sees the limitation before the call sites.

## Operator workaround until v0.8 Phase A

The existing `exports[].columns:` mechanism already accepts per-column
type overrides in the YAML (used today for explicit decimal precision:
`columns: { amount: "decimal(18,2)" }`). The same mechanism could be
extended to accept a nullability hint
(`columns: { amount: { type: "decimal(18,2)", nullable: false } }`).
This is the operator's escape hatch for cases where the source schema
is known and the target catalog needs the constraint.

This extension is **not implemented in this release** — it requires
the YAML schema change and the override threading through
`ColumnOverrides`. Operators who need source `NOT NULL` constraints
in their target catalog must currently fix it downstream (e.g., add
`NOT NULL` in the target `CREATE TABLE` statement).

## Trigger for revisiting

Pull this ADR out of deferred status when **any** of the following
ships:

1. v0.8 Phase A type-report extension (per ADR-0014) — direct
   parent work.
2. A specific operator request citing a catalog-tool downstream that
   refuses or mis-handles the all-nullable schema.
3. A query-parser dependency lands in the crate for unrelated reasons
   (e.g., for `chunk_by_key` validation), making the LEFT JOIN /
   computed-column detection cheap.

## Consequences

**Positive (during deferral):**

- Conservative `nullable=true` write-path: any source value passes
  through, no false `RIVET_VALUE_TOO_NULL` errors on data that the
  source happens to contain.
- Source-engine introspection layer stays simple — no per-query
  `RowDescription` walk or query parsing.
- The four `SourceColumn::simple(…, true)` call sites are uniform
  across engines, easy to audit.

**Negative (during deferral):**

- Information loss at the Parquet schema layer for `NOT NULL` source
  columns.
- Downstream catalog tools cannot infer target-table `NOT NULL`
  constraints from Rivet's Parquet output.
- Operators who need this must add constraints in the target manually.

**Reversal cost:**

- ~80 lines per engine for the catalog probe (PG: wire-protocol
  `table_oid + attnum` → `pg_attribute.attnotnull` lookup; MySQL:
  `information_schema.STATISTICS` query keyed on table-name + column-
  name when origin is resolvable).
- ~50 lines for `ColumnOverrides` nullability threading and YAML
  schema bump.
- Live tests per engine: "non-null column round-trips with Parquet
  `repetition = REQUIRED`" + "LEFT JOIN outer side stays
  `OPTIONAL`".

## References

- `src/types/source_column.rs::SourceColumn::nullable` — field with
  the limitation doc and a back-pointer to this ADR.
- `src/source/postgres/arrow_convert.rs:217`,
  `src/source/postgres/mod.rs:636`,
  `src/source/mysql/arrow_convert.rs:244`,
  `src/source/mysql/mod.rs:617` — the four `nullable=true` hardcode
  sites.
- ADR-0014 — target type materialization, including the Phase A
  type-report this work joins.
- The session that surfaced this gap during its invariant audit:
  closing commits 2cedd3a (gap #2/#3) and 73d5be8 (gaps #1/#4) shipped
  CI gates for the four other audit invariants; gap #5 (this one) was
  dismissed at the time as "design choice" — this ADR is the honest
  paper trail replacing that dismissal.
