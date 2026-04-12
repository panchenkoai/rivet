# Quickstart: PostgreSQL

Export your first table in 5 minutes.

## Prerequisites

- A running PostgreSQL database you can connect to
- Rivet installed (`rivet --version` shows a version)

## Step 1: Create a config file

Create `my_first_export.yaml`:

```yaml
source:
  type: postgres
  url: "postgresql://myuser:mypassword@localhost:5432/mydb"
  # Better: use url_env to avoid plaintext passwords
  # url_env: DATABASE_URL

exports:
  - name: users_full
    query: "SELECT id, name, email, created_at FROM users"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
```

Replace the connection URL with your actual database credentials.

**Optional:** scaffold a config from a live table or whole schema with [`rivet init`](../reference/init.md) (e.g. `rivet init --source "$DATABASE_URL" --table users -o my_first_export.yaml`).

## Step 2: Verify connectivity

```bash
rivet doctor --config my_first_export.yaml
```

Expected output:

```
[OK] Source postgres://localhost:5432/mydb — connected, version 16.x
[OK] Destination './output' — directory writable
```

If you see `[FAIL]`, check your connection string and that the database is reachable.

## Step 3: Preflight check

```bash
rivet check --config my_first_export.yaml
```

This shows: table existence, estimated row count, index information, and recommended tuning profile.

## Step 4: Run the export

```bash
rivet run --config my_first_export.yaml --validate --reconcile
```

- `--validate` verifies the output file row count matches what was exported
- `--reconcile` runs `COUNT(*)` on the source query and compares with exported rows

Expected output:

```
[users_full] mode=full format=parquet compression=zstd
[users_full] Exported 5,432 rows in 1.2s
[users_full] File: ./output/users_full_20260406_120000.parquet (847 KB)
[users_full] Reconciliation: MATCH (source=5432, exported=5432)
```

## Step 5: Inspect the output

```bash
# List exported files
ls -la ./output/

# View with DuckDB (if installed)
duckdb -c "SELECT * FROM read_parquet('./output/users_full_*.parquet') LIMIT 10"

# Or check metrics
rivet metrics --config my_first_export.yaml
```

## Step 6: Switch to incremental (optional)

To only export new rows on subsequent runs, switch to incremental mode:

```yaml
exports:
  - name: users_incremental
    query: "SELECT id, name, email, created_at, updated_at FROM users"
    mode: incremental
    cursor_column: updated_at       # must be monotonically increasing
    format: parquet
    skip_empty: true                # no file if no new rows
    destination:
      type: local
      path: ./output
```

```bash
# First run: exports everything
rivet run --config my_first_export.yaml --validate

# Second run: only exports rows with updated_at > last cursor
rivet run --config my_first_export.yaml --validate

# Check cursor
rivet state show --config my_first_export.yaml
```

## Step 7: Automate with cron

```bash
# Run every day at 2 AM
0 2 * * * cd /path/to/project && rivet run -c my_first_export.yaml --validate >> /var/log/rivet.log 2>&1
```

## Next steps

- Export to S3/GCS: [destinations/s3.md](../destinations/s3.md), [destinations/gcs.md](../destinations/gcs.md)
- Tune for large tables: [reference/tuning.md](../reference/tuning.md)
- Use chunked mode for millions of rows: [modes/chunked.md](../modes/chunked.md)
- Production checklist: [production-checklist.md](production-checklist.md)
