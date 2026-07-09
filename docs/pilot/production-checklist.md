# Production Checklist

For pilot ordering (discovery → run → reconcile → sign-off), use the [Pilot guide](README.md) and [Pilot walkthrough](pilot-walkthrough.md); this page is the **readiness gate** before touching production systems.

Complete this checklist before running Rivet against a production database.

## Database access

- [ ] **Read-only user**: create a dedicated database user with `SELECT`-only privileges
- [ ] **Credential management**: use `url_env` or `password_env` — never hardcode passwords in YAML
- [ ] **Read replica**: if available, point Rivet at the replica to avoid load on the primary
- [ ] **Connection limits**: confirm the database connection pool has room for Rivet's connections (1 per export, +1 for chunked parallel workers)
- [ ] **Connection pooler / proxy awareness**: if traffic is routed through pgBouncer, Odyssey, ProxySQL, MaxScale, or HAProxy, read the **Connection poolers and proxies** section below before the first run

## Connectivity

- [ ] **`rivet doctor`** passes for all destinations
- [ ] **Network**: Rivet host can reach the database and destination (S3/GCS) endpoints
- [ ] **Firewall / security groups**: ports are open (5432 for Postgres, 3306 for MySQL, 1433 for SQL Server, 27017 for MongoDB, 443 for S3/GCS/Azure)

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
  | Continuous low-latency replication | `cdc` |
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

## Auditable extraction (plan/apply)

For CI/CD pipelines, GitOps workflows, or any run that requires a pre-execution review before data is touched:

1. **Generate a plan artifact** — preflight analysis + chunk boundaries pre-computed, no data exported:
   ```bash
   rivet plan -c rivet.yaml --format json --output plan.json
   ```
2. **Review the plan** — inspect `verdict`, `warnings`, chunk count, row estimate. Commit `plan.json` to a PR or store as a CI artifact.
3. **Apply the sealed artifact** — executes exactly the pre-computed plan:
   ```bash
   rivet apply plan.json
   ```

Key guarantees:
- `apply` never re-reads the config file or re-runs preflight queries
- Plans older than 1 hour emit a warning; older than 24 hours require `--force`
- For incremental exports, `apply` rejects the artifact if the cursor has advanced since plan time (another run completed in between)

**Many tables in one run.** For a config with several exports, `rivet plan -c rivet.yaml` also writes a `wave:` and `parallel_safe:` onto each export, and `rivet apply rivet.yaml` runs them **wave by wave** (lowest first), with a barrier between waves. Tables are independent, so a failed export does not block its wave-mates — apply collects failures, runs the rest, and exits non-zero. Add `--parallel-export-processes` to run the cheap (`parallel_safe`) exports within a wave concurrently. See [getting-started § 5](../getting-started.md#5--many-tables-plan-once-apply-by-waves).

> **Security note:** `plan.json` embeds the resolved source connection config. Plaintext `password:` values and `scheme://user:pass@` userinfo are stripped by ADR-0005 PA9; references (`password_env:` / `url_env:` / `url_file:`) are preserved so the apply environment can re-resolve them. Plans still contain query SQL, schema and cursor state — treat them as sensitive.

See [CLI reference](../reference/cli.md) and [ADR-0005](../adr/0005-plan-apply-contracts.md) for the full contract specification.

## Connection poolers and proxies

Many production stacks place pgBouncer / Odyssey / ProxySQL / MaxScale / HAProxy in front of the database. Rivet detects the connection shape at startup and emits a one-line warning if a pooler or multiplexing proxy is involved. Acting on that warning is the operator's call — the export still runs.

![Direct MySQL vs ProxySQL: connect-time warning fires only behind the proxy](../gifs/pool-detect.gif)

### What Rivet detects, and what it does about it

| Stack in front of the DB | Rivet's classification | What still works | What may silently NOT work |
|---|---|---|---|
| Direct connection | Postgres: no warning · MySQL: `MysqlProxyKind::Direct` | Everything | — |
| pgBouncer / Odyssey (transaction mode) | Postgres: "transaction-mode connection pooler detected" | `SET LOCAL` inside our `BEGIN` … `COMMIT` (each export wraps its work in a txn); destination write; cursor/manifest writes | `LISTEN`/`NOTIFY`, advisory locks, prepared statements that outlive a transaction |
| pgBouncer / Odyssey (session mode) | No warning (PIDs stay stable) | Everything direct works | — |
| ProxySQL (default config) | MySQL: `MysqlProxyKind::ProxySql` | Session vars per statement when `transaction_persistent=1` is on the user (we set it in our dev fixture) | Long-lived prepared statements; assumptions that two consecutive queries hit the same backend |
| MariaDB MaxScale | MySQL: `MysqlProxyKind::MaxScale` | Read-write splitting under `readwritesplit` router | Queries the router decides to reject or rewrite; backend-side statement timeouts may diverge from what `tuning.statement_timeout_s` sets |
| HAProxy MySQL mode, in-house balancers | MySQL: `MysqlProxyKind::Multiplexed` | Per-statement behaviour | Anything session-scoped |

### Recommended posture for production

1. **Read the startup log line.** A warning like `transaction-mode connection pooler detected (pgBouncer/Odyssey)` or `MySQL proxy multiplexer detected (ProxySQL)` is intentional, one-time per source connect.
2. **Prefer session mode (Postgres) or `transaction_persistent=1` (ProxySQL)** for any Rivet user that needs `statement_timeout`, `lock_timeout`, or `time_zone` to actually take effect for the full export.
3. **If you must run through transaction mode**, do not assume per-export tuning that depends on session state is enforced for anything outside Rivet's own `BEGIN ... COMMIT` block. The destination commit and state writes are unaffected — `live_pool_safety.rs` exercises both paths against pgBouncer (`pool_size=1`) and ProxySQL nightly.
4. **Connection budget**: chunked exports with `parallel: N` open N concurrent backends. Multiply by the number of exports running simultaneously. Verify your pooler / backend `max_connections` headroom — see [ADR-0011](../adr/0011-source-trait-send-not-sync.md) for why we don't share a connection across workers.

The full coverage table is in [docs/reliability-matrix.md § Pool and load pressure](../reliability-matrix.md#pool-and-load-pressure), and the detection internals are described in [docs/architecture.md § Connection pooler / proxy detection](../architecture.md#connection-pooler--proxy-detection).

---

## Memory budgeting

| batch_size | Approximate RSS (narrow table) | Approximate RSS (wide table) |
|-----------|-------------------------------|------------------------------|
| 1,000 | 50-100 MB | 100-500 MB |
| 5,000 | 100-300 MB | 300 MB - 1.5 GB |
| 10,000 | 200-500 MB | 500 MB - 3 GB |
| 50,000 | 500 MB - 2 GB | 2-10 GB |

For memory-constrained environments, use `profile: safe` and consider building with `--features jemalloc`.
