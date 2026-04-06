# Full Export Mode

## When to use

Use `mode: full` when you want a complete snapshot of the query result set every time. Each run re-exports all rows from scratch. Best for:

- Small-to-medium tables (up to a few million rows)
- Reference/dimension tables that need a fresh copy each day
- One-time data migrations

## Minimal config

```yaml
source:
  type: postgres                                    # postgres or mysql
  url: "postgresql://user:pass@host:5432/dbname"

exports:
  - name: users_daily                               # unique export name
    query: "SELECT id, name, email, created_at FROM users"
    mode: full                                      # re-export everything each run
    format: parquet                                 # parquet or csv
    destination:
      type: local
      path: ./output                                # directory for output files
```

Output file: `./output/users_daily_20260406_120000.parquet`

## Run it

```bash
# 1. Verify config and connectivity
rivet check --config users.yaml
rivet doctor --config users.yaml

# 2. Run with validation
rivet run --config users.yaml --validate --reconcile

# 3. Check results
rivet metrics --config users.yaml --last 5
```

## What happens

1. Rivet connects to the source database
2. Executes `SELECT id, name, email, created_at FROM users`
3. Fetches rows in batches controlled by `tuning.batch_size` (default 10,000 with `balanced` profile)
4. Writes a timestamped output file
5. Records metrics in the state database

No cursor is stored. Each run produces a new file with all rows.

`batch_size` directly controls memory usage and source load. For wide tables (many columns, TEXT/JSONB fields), reduce it to 1,000-5,000. See [reference/tuning.md](../reference/tuning.md).

## Common options

```yaml
exports:
  - name: users_daily
    query: "SELECT * FROM users"
    mode: full
    format: parquet
    compression: zstd           # zstd (default), snappy, gzip, lz4, none
    skip_empty: true            # don't create file if query returns 0 rows
    max_file_size: "512MB"      # split into multiple files if output exceeds this
    meta_columns:
      exported_at: true         # add _rivet_exported_at column
    destination:
      type: local
      path: ./output
    tuning:
      profile: safe             # safe/balanced/fast — controls batch size, timeouts
```

## Troubleshooting

**Export is slow on a large table** -- Switch to `mode: chunked` with `parallel: 4` for tables over 1M rows. See [chunked.md](chunked.md).

**Output file is too large** -- Add `max_file_size: "256MB"` to split into parts.

**Query returns 0 rows but file is still created** -- Add `skip_empty: true`.
