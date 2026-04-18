# Incremental — Composite Cursor (`coalesce` mode)

> See also: [ADR-0007 — Cursor Policy Contracts](../adr/0007-cursor-policy-contracts.md).

## When to use

Use `incremental_cursor_mode: coalesce` when a single column is not a reliable monotonic key, but combining **two** columns with `COALESCE(primary, fallback)` is.

Typical situations:

- `updated_at` is **nullable** for some rows — progression must fall back to `created_at`.
- A partial migration left some rows with only a `created_at`, others with both.
- You intentionally write `updated_at` only on changes and need a baseline for new rows.

If your primary column is reliably non-null and monotonic, stay with plain [`incremental`](incremental.md) — it is simpler, faster, and has fewer moving parts.

## Required fields

| Field | Value |
|---|---|
| `mode` | `incremental` |
| `cursor_column` | primary progression column (e.g. `updated_at`) |
| `cursor_fallback_column` | fallback column used when primary is `NULL` (e.g. `created_at`) |
| `incremental_cursor_mode` | `coalesce` |

Config validation rejects:
- `coalesce` without `cursor_fallback_column`
- `cursor_fallback_column` set for any mode other than `coalesce`

## Minimal config

```yaml
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: orders_coalesce
    query: "SELECT id, product, quantity, price, updated_at, created_at FROM orders"
    mode: incremental
    cursor_column: updated_at
    cursor_fallback_column: created_at
    incremental_cursor_mode: coalesce
    format: parquet
    destination:
      type: local
      path: ./output
```

## What Rivet runs

A single-level wrapper with an outer `ORDER BY` on the coalesced expression (so the last Arrow batch carries the maximum progression value; ADR-0007 CC6):

```sql
SELECT _rivet.*,
       COALESCE(_rivet."updated_at", _rivet."created_at") AS "_rivet_coalesced_cursor"
FROM (<your query>) AS _rivet
WHERE COALESCE(_rivet."updated_at", _rivet."created_at") > '<last cursor>'
ORDER BY COALESCE(_rivet."updated_at", _rivet."created_at"),
         _rivet."updated_at",
         _rivet."created_at"
```

On the first run (no stored cursor) the `WHERE` is omitted.

## State and output

- **Stored cursor** — a single scalar string, the max value of `COALESCE(primary, fallback)` from the last exported batch (ADR-0007 CC5; same `StateStore` shape as regular `incremental`).
- **Output files** — the synthetic `_rivet_coalesced_cursor` column is **stripped** before writing Parquet/CSV. Your output contains only your selected columns.

## Apply semantics

Apply uses the cursor snapshot embedded in the plan artifact (ADR-0005 PA4). The comparison is string-wise and mode-agnostic — `coalesce` does not change the check, only the meaning of the opaque string.

## Quoting and escaping

Rivet quotes `cursor_column` and `cursor_fallback_column` using the source dialect (`"…"` for Postgres, `` `…` `` for MySQL). Cursor values are emitted as SQL string literals with `'` doubled (`O'Brien` → `'O''Brien'`). No additional escaping is required on your side.

## Caveats

- **`COALESCE` is not indexed** — on very large tables the planner may not use the indexes on `updated_at` or `created_at`. Preflight (`rivet plan`) will flag this in diagnostics.
- **Monotonicity is best-effort** — if new rows can appear with a `created_at` strictly less than the latest `COALESCE(...)` already seen, they will be skipped. Prefer setting `updated_at` on insert when feasible.
- **One fallback only** — two-level lexicographic cursors `(a, b)` and more than one fallback are out of scope for v1 (ADR-0007).

## Trying it locally

The dev seed (`cargo run --bin seed`) populates a fixture table `orders_coalesce` in both Postgres and MySQL with a configurable NULL ratio for `updated_at`:

```bash
# Uses dev/{postgres,mysql}/init.sql + seed defaults (~35% NULL updated_at).
cargo run --bin seed -- --target both --coalesce-rows 2000 --coalesce-null-ratio 0.35

# Then:
rivet plan --config dev/pg_incremental_coalesce.yaml
rivet run  --config dev/pg_incremental_coalesce.yaml
```

## Troubleshooting

**Stored cursor goes backwards** — a row was inserted with `created_at` older than the last seen `COALESCE` value. Options: set `updated_at` at insert time, or reset cursor via `rivet state reset`.

**Empty output on second run but new data exists** — check that the predicate expression matches the data distribution. `rivet plan` shows the exact SQL form.

**`cursor_fallback_column` rejected** — you need `incremental_cursor_mode: coalesce` to enable the fallback. Without it, only the primary is used (see [incremental.md](incremental.md)).
