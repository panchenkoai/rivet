# Time-Window Export Mode

## When to use

Use `mode: time_window` to export only rows within a rolling N-day window from the current timestamp. Best for:

- Event tables where you only need the last 7/30/90 days
- Periodic refresh of a "recent activity" dataset
- When `incremental` is not suitable because you need overlapping windows

## Required fields

- `time_column` -- the timestamp column to filter on
- `days_window` -- how many days back from now to include

## Minimal config

```yaml
source:
  type: postgres
  url: "postgresql://user:pass@host:5432/dbname"

exports:
  - name: recent_events
    query: "SELECT id, user_id, event_type, payload, created_at FROM events"
    mode: time_window
    time_column: created_at         # timestamp column to filter
    days_window: 30                 # include rows from the last 30 days
    format: parquet
    destination:
      type: local
      path: ./output
```

## Run it

```bash
rivet check --config events.yaml
rivet run --config events.yaml --validate
```

## What happens

1. Rivet calculates the cutoff: `NOW() - 30 days`
2. Appends `WHERE created_at >= '2026-03-07 00:00:00'` to your query
3. Exports all matching rows as a fresh file
4. No cursor is stored -- each run re-evaluates the window

Unlike `incremental`, this mode produces overlapping data across runs (the last 30 days always overlap with yesterday's last 30 days).

## Time column types

By default, Rivet assumes a `TIMESTAMP`/`DATETIME` column. For Unix epoch integers, set `time_column_type`:

```yaml
exports:
  - name: recent_events
    query: "SELECT id, user_id, event_type, created_at_epoch FROM events"
    mode: time_window
    time_column: created_at_epoch
    time_column_type: unix          # column stores Unix epoch (seconds)
    days_window: 7
    format: csv
    destination:
      type: local
      path: ./output
```

| `time_column_type` | Column type | Filter generated |
|-------------------|-------------|-----------------|
| `timestamp` (default) | `TIMESTAMP` / `DATETIME` | `WHERE col >= '2026-03-07 00:00:00'` |
| `unix` | `INT` / `BIGINT` | `WHERE col >= 1741305600` |

## Common options

```yaml
exports:
  - name: recent_events
    query: "SELECT id, user_id, event_type, created_at FROM events"
    mode: time_window
    time_column: created_at
    days_window: 30
    format: parquet
    compression: zstd
    skip_empty: true
    destination:
      type: local
      path: ./output
```

## Troubleshooting

**0 rows exported but the table has data** -- Check that `days_window` is large enough. Events older than the window are excluded. Also check timezones.

**Duplicates across runs** -- This is by design. Each run exports the full window. Downstream consumers should deduplicate by primary key.

**Need non-overlapping exports** -- Use `mode: incremental` with `cursor_column` instead.
