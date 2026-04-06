# Incremental Export Mode

## When to use

Use `mode: incremental` when you only want to export rows that are new or updated since the last run. Best for:

- Append-only tables (events, logs, audit trails)
- Tables with a reliable `updated_at` timestamp
- Tables with a monotonically increasing ID
- Daily/hourly syncs where re-exporting everything is wasteful

## Required fields

- `cursor_column` -- the column used to track progress (must be monotonically increasing)

## Minimal config

```yaml
source:
  type: postgres
  url: "postgresql://user:pass@host:5432/dbname"

exports:
  - name: orders_incremental
    query: "SELECT id, user_id, product, price, status, updated_at FROM orders"
    mode: incremental
    cursor_column: updated_at       # tracks last exported value
    format: parquet
    destination:
      type: local
      path: ./output
```

## Run it

```bash
# First run — exports all rows (no cursor yet)
rivet run --config orders.yaml --validate --reconcile

# Second run — only exports rows with updated_at > last cursor
rivet run --config orders.yaml --validate

# Check current cursor position
rivet state show --config orders.yaml

# Reset cursor to re-export everything
rivet state reset --config orders.yaml --export orders_incremental
```

## What happens

1. First run: no cursor exists, so all rows matching the query are exported
2. Rivet records the maximum value of `cursor_column` as the cursor
3. Subsequent runs: Rivet appends `WHERE updated_at > $cursor` to your query
4. Only new/updated rows are exported; cursor advances after successful write

```
Run 1 (no cursor):  SELECT ... FROM orders
                     → 5000 rows, cursor saved: 2026-04-05 23:59:59

Run 2 (with cursor): SELECT ... FROM orders WHERE updated_at > '2026-04-05 23:59:59'
                      → 47 rows (only changes since last run)
```

## Cursor column tips

| Column type | Example | Notes |
|-------------|---------|-------|
| `TIMESTAMP` / `DATETIME` | `updated_at` | Most common; ensure it updates on every change |
| `BIGINT` / `SERIAL` | `id` | Works for append-only tables |
| `TIMESTAMPTZ` | `created_at` | Good for event streams |

The cursor column must be:
- Present in the SELECT clause
- Monotonically increasing (new rows always have a larger value)
- Not NULL for rows you want exported

## Batch size and tuning

Even in incremental mode, Rivet fetches rows in batches (not all at once). The `batch_size` from `tuning:` controls how many rows are fetched per `FETCH` call:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    batch_size: 5000            # rows per fetch (default: 10,000 for balanced)

exports:
  - name: orders_incremental
    query: "SELECT id, user_id, product, price, updated_at FROM orders"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination:
      type: local
      path: ./output
    tuning:
      batch_size: 2000          # per-export override (takes precedence)
```

On the first incremental run (no cursor yet), **all rows** are exported. If the table has millions of rows, this first run behaves like a full export — so `batch_size` directly impacts memory and source load. Use a smaller `batch_size` (1,000-5,000) for wide tables or production databases.

See [reference/tuning.md](../reference/tuning.md) for all tuning parameters.

## Common options

```yaml
exports:
  - name: orders_incremental
    query: "SELECT id, user_id, product, price, updated_at FROM orders"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    skip_empty: true            # don't create file if no new rows
    meta_columns:
      exported_at: true         # add _rivet_exported_at for dedup downstream
    destination:
      type: local
      path: ./output
```

## Troubleshooting

**No new rows but export still runs** -- Add `skip_empty: true` to avoid empty files.

**Data appears duplicated across runs** -- Ensure `cursor_column` updates when rows are modified. If rows are updated without changing `updated_at`, they will be missed.

**Need to re-export all data** -- `rivet state reset --config ... --export <name>` clears the cursor.
