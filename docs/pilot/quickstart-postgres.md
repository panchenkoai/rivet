# Quickstart: PostgreSQL

Export your first table in 5 minutes.

![End-to-end PostgreSQL export: doctor -> check -> run](../gifs/basic.gif)

The commands and outputs shown above are exactly what the steps below walk through.

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

**Optional:** scaffold a config from a live table or whole schema with [`rivet init`](../reference/init.md) (e.g. `rivet init --source-env DATABASE_URL --table users -o my_first_export.yaml`). Using `--source-env` keeps the URL out of shell history / `ps`.

## Step 2: Verify connectivity

```bash
rivet doctor --config my_first_export.yaml
```

Expected output:

```
rivet doctor: verifying auth for config 'my_first_export.yaml'

[OK]  Config parsed successfully
[OK]  Source auth (Postgres)
[OK]  Destination Local(./output)

All checks passed.
```

If you see `[FAIL]`, check your connection string and that the database is reachable.

## Step 3: Preflight check

```bash
rivet check --config my_first_export.yaml
```

This shows strategy, row estimate, scan type (indexed vs sequential), verdict, recommended tuning profile, and a mode-aware `Suggestion:` line when the verdict is `DEGRADED` / `UNSAFE`. Example for a small table without an indexed cursor column:

```
Export: users_full
  Strategy:     full-scan
  Mode:         full
  Row estimate: ~5K
  Scan type:    Seq Scan on users  (cost=0.00..45.51 rows=5432 width=73)
  Verdict:      DEGRADED
  Recommended:  tuning.profile: balanced
  Parallelism:  1 (only chunked mode benefits from parallelism)
  Suggestion:   No index detected -- full table scan. Add an indexed cursor column and switch to incremental mode. Use 'safe' tuning profile to limit database impact.
```

## Step 4: Run the export

```bash
rivet run --config my_first_export.yaml --validate --reconcile
```

- `--validate` reads the output file back and verifies its row count matches the written count
- `--reconcile` runs `SELECT COUNT(*)` on the source query and compares with exported rows; the result appears as the last `reconcile:` line of the summary

Expected output:

```
── users_full ──
  run_id:      users_full_20260419T120000.123
  status:      success
  tuning:      profile=balanced (default), batch_size=10000
  rows:        5432
  files:       1
  bytes:       847 KB
  duration:    1.2s
  peak RSS:    15MB (sampled during run)
  validated:   pass
  schema:      unchanged
  reconcile:   MATCH (5432/5432)
```

**Optional: preview before executing.** Use `rivet plan` to inspect the execution plan without exporting any data:

```bash
rivet plan --config my_first_export.yaml
```

To save a sealed plan artifact and apply it separately (useful for CI/CD and auditable workflows):

```bash
rivet plan --config my_first_export.yaml --format json --output plan.json
rivet apply plan.json
```

See [CLI reference — rivet plan/apply](../reference/cli.md) for details.

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
