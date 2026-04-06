# Production Checklist

Complete this checklist before running Rivet against a production database.

## Database access

- [ ] **Read-only user**: create a dedicated database user with `SELECT`-only privileges
- [ ] **Credential management**: use `url_env` or `password_env` — never hardcode passwords in YAML
- [ ] **Read replica**: if available, point Rivet at the replica to avoid load on the primary
- [ ] **Connection limits**: confirm the database connection pool has room for Rivet's connections (1 per export, +1 for chunked parallel workers)

## Connectivity

- [ ] **`rivet doctor`** passes for all destinations
- [ ] **Network**: Rivet host can reach the database and destination (S3/GCS) endpoints
- [ ] **Firewall / security groups**: ports are open (5432 for Postgres, 3306 for MySQL, 443 for S3/GCS)

## Configuration

- [ ] **`rivet check`** passes for all exports
- [ ] **Tuning profile**: use `safe` for production OLTP; `balanced` for moderate load; `fast` only on read replicas
- [ ] **`batch_size`**: start conservative (1,000-5,000) for wide tables; increase after monitoring memory
- [ ] **`statement_timeout_s`**: set to prevent runaway queries (recommended: 60-300s)
- [ ] **`lock_timeout_s`**: set to prevent lock contention (recommended: 10-30s)
- [ ] **`throttle_ms`**: add 50-500ms between batches for busy databases

## Export design

- [ ] **Mode selection**: choose the right mode for each table:
  | Table type | Recommended mode |
  |-----------|-----------------|
  | Small reference table | `full` |
  | Append-only events | `incremental` |
  | Large table, initial load | `chunked` |
  | Rolling window analytics | `time_window` |
- [ ] **Query optimization**: test your queries with `EXPLAIN ANALYZE` first
- [ ] **Indexes**: ensure `cursor_column`, `chunk_column`, and `time_column` are indexed
- [ ] **`skip_empty: true`**: avoid creating empty files on incremental runs with no new data
- [ ] **`max_file_size`**: set for large exports to keep output files manageable

## Destination

- [ ] **Bucket/directory exists**: Rivet does not create S3/GCS buckets
- [ ] **IAM permissions**: write access confirmed (`s3:PutObject` / `storage.objects.create`)
- [ ] **Storage lifecycle**: configure retention policies on S3/GCS buckets to manage costs
- [ ] **Disk space**: for local destinations, ensure sufficient disk space

## Quality gates

- [ ] **`--validate`**: always run with `--validate` to verify output row counts
- [ ] **`--reconcile`**: use on critical exports to verify source `COUNT(*)` matches
- [ ] **Quality rules**: set `row_count_min` / `null_ratio_max` for critical data

## Monitoring and alerting

- [ ] **Slack notifications**: configure `notifications.slack.on: [failure, degraded]`
- [ ] **Cron scheduling**: set up cron with logging (`>> /var/log/rivet.log 2>&1`)
- [ ] **Metrics review**: periodically check `rivet metrics` for duration/size trends
- [ ] **Exit codes**: your scheduler should alert on non-zero exit codes

## First production run

1. Run `rivet doctor` -- verify all connections
2. Run `rivet check` -- review preflight analysis
3. Run `rivet run --validate --reconcile` -- first real export
4. Inspect output files -- verify data correctness
5. Check `rivet metrics` -- confirm timing and row counts
6. Run again to test incremental/chunked behavior
7. Set up cron / scheduler

## Memory budgeting

| batch_size | Approximate RSS (narrow table) | Approximate RSS (wide table) |
|-----------|-------------------------------|------------------------------|
| 1,000 | 50-100 MB | 100-500 MB |
| 5,000 | 100-300 MB | 300 MB - 1.5 GB |
| 10,000 | 200-500 MB | 500 MB - 3 GB |
| 50,000 | 500 MB - 2 GB | 2-10 GB |

For memory-constrained environments, use `profile: safe` and consider building with `--features jemalloc`.
