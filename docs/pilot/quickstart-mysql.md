# Quickstart: MySQL

Export your first table in 5 minutes.

![End-to-end export: doctor -> check -> run (recorded on Postgres; MySQL syntax is identical)](../gifs/basic.gif)

The commands and outputs shown above are exactly what the steps below walk through; only `type: postgres` becomes `type: mysql` and the URL scheme changes.

## Prerequisites

- A running MySQL database you can connect to
- Rivet installed (`rivet --version` shows a version)

## Step 1: Create a config file

Create `my_first_export.yaml`:

```yaml
source:
  type: mysql
  url: "mysql://myuser:mypassword@localhost:3306/mydb"
  # Better: use url_env to avoid plaintext passwords
  # url_env: DATABASE_URL

exports:
  - name: orders_full
    query: "SELECT id, user_id, product, price, status, created_at FROM orders"
    mode: full
    format: csv
    destination:
      type: local
      path: ./output
```

Replace the connection URL with your actual database credentials.

**Optional:** scaffold a config with [`rivet init`](../reference/init.md) (e.g. `rivet init --source-env DATABASE_URL --table orders -o my_first_export.yaml`). Using `--source-env` keeps the URL out of shell history / `ps`.

## Step 2: Verify connectivity

```bash
rivet doctor --config my_first_export.yaml
```

Expected output:

```
rivet doctor: verifying auth for config 'my_first_export.yaml'

[OK]  Config parsed successfully
[OK]  Source auth (MySQL)
[OK]  Destination Local(./output)

All checks passed.
```

If you see `[FAIL]`, check your connection string and that the database is reachable.

## Step 3: Preflight check

```bash
rivet check --config my_first_export.yaml
```

This shows strategy, row estimate, scan type, verdict, recommended tuning profile, and a mode-aware `Suggestion:` when the verdict is `DEGRADED` / `UNSAFE`. Example for a non-indexed full scan:

```
Export: orders_full
  Strategy:     full-scan
  Mode:         full
  Row estimate: ~12K
  Scan type:    ALL
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
── orders_full ──
  run_id:      orders_full_20260419T120000.123
  status:      success
  tuning:      profile=balanced (default), batch_size=10000
  rows:        12340
  files:       1
  bytes:       1.2 MB
  duration:    2.1s
  peak RSS:    18MB (sampled during run)
  validated:   pass
  schema:      unchanged
  reconcile:   MATCH (12340/12340)
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

# Decompress and preview (if zstd compressed)
zstd -d ./output/orders_full_*.csv.zst --stdout | head -20

# Or export without compression for easy viewing:
# compression: none → produces .csv
```

## Step 6: Switch to incremental (optional)

```yaml
exports:
  - name: orders_incremental
    query: "SELECT id, user_id, product, price, status, created_at, updated_at FROM orders"
    mode: incremental
    cursor_column: updated_at
    format: csv
    compression: none               # plain CSV for easy inspection
    skip_empty: true
    destination:
      type: local
      path: ./output
```

```bash
# First run: exports everything
rivet run --config my_first_export.yaml --validate

# Second run: only new/updated rows
rivet run --config my_first_export.yaml --validate

# Check cursor
rivet state show --config my_first_export.yaml
```

## Step 7: Automate with cron

```bash
0 2 * * * cd /path/to/project && rivet run -c my_first_export.yaml --validate >> /var/log/rivet.log 2>&1
```

## MySQL-specific notes

- **Connection URL format**: `mysql://user:password@host:port/database`
- **Default port**: 3306
- **Structured connection** (alternative to URL):

```yaml
source:
  type: mysql
  host: db.example.com
  port: 3306
  user: rivet_reader
  password_env: MYSQL_PASSWORD
  database: production
```

- **Performance tip**: for large InnoDB tables, use `mode: chunked` with the primary key to avoid long-running queries that block replication.

## Next steps

- Export to S3/GCS: [destinations/s3.md](../destinations/s3.md), [destinations/gcs.md](../destinations/gcs.md)
- Tune for large tables: [reference/tuning.md](../reference/tuning.md)
- Use chunked mode for millions of rows: [modes/chunked.md](../modes/chunked.md)
- Production checklist: [production-checklist.md](production-checklist.md)
