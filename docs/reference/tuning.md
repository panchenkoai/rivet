# Tuning Reference

Tuning controls how Rivet queries the source database: batch sizes, timeouts, throttling, and retries.

## Where to place tuning

Tuning can be set at two levels:

1. **Global** (`source.tuning`) -- applies to all exports
2. **Per-export** (`exports[].tuning`) -- overrides global for that export

Per-export values take precedence. Unset per-export fields fall back to the global value.

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: balanced               # global default
    batch_size: 10000

exports:
  - name: small_table
    query: "SELECT * FROM users"
    format: parquet
    destination: { type: local, path: ./out }
    # inherits global tuning (balanced, batch_size=10000)

  - name: huge_table
    query: "SELECT * FROM events"
    format: parquet
    destination: { type: local, path: ./out }
    tuning:
      profile: safe                 # override for this export only
      batch_size: 2000
```

> **Common mistake:** placing `batch_size` directly under `source:` or in the export root instead of under `tuning:`. Rivet will reject such configs with a clear error message.

## Profiles

A profile sets sensible defaults for all tuning parameters. Individual fields override the profile.

| Parameter | `fast` | `balanced` (default) | `safe` |
|-----------|--------|---------------------|--------|
| `batch_size` | 50,000 | 10,000 | 2,000 |
| `throttle_ms` | 0 | 50 | 500 |
| `statement_timeout_s` | 0 (none) | 300 | 120 |
| `max_retries` | 1 | 3 | 5 |
| `retry_backoff_ms` | 1,000 | 2,000 | 5,000 |
| `lock_timeout_s` | 0 (none) | 30 | 10 |
| `memory_threshold_mb` | 0 (none) | 4,096 | 2,048 |

### When to use each profile

| Profile | Use case |
|---------|----------|
| **fast** | Dedicated read replica, off-peak hours, small tables |
| **balanced** | General purpose, shared database, production reads |
| **safe** | Busy production database, OLTP systems, wide tables with large rows |

## All tuning parameters

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `profile` | `fast` \| `balanced` \| `safe` | `balanced` | Base profile (sets defaults for all other fields) |
| `batch_size` | integer | profile default | Number of rows fetched per query batch |
| `batch_size_memory_mb` | integer | — | Target memory per batch in MB (adaptive sizing; mutually exclusive with `batch_size`) |
| `throttle_ms` | integer | profile default | Delay in ms between batches (reduces source load) |
| `statement_timeout_s` | integer | profile default | Database statement timeout in seconds (0 = no timeout) |
| `max_retries` | integer | profile default | Max retry attempts for transient errors |
| `retry_backoff_ms` | integer | profile default | Base delay between retries in ms (exponential backoff) |
| `lock_timeout_s` | integer | profile default | Database lock timeout in seconds (0 = no timeout) |
| `memory_threshold_mb` | integer | profile default | RSS threshold in MB; pauses fetching if exceeded (0 = disabled). `balanced` defaults to 4096, `safe` to 2048, `fast` to 0 (no limit). |
| `max_batch_memory_mb` | integer | — | Hard cap on a single Arrow batch in MB. When exceeded, `on_batch_memory_exceeded` determines the response. |
| `on_batch_memory_exceeded` | `warn` \| `fail` \| `auto_shrink` | `warn` | Policy applied when a batch exceeds `max_batch_memory_mb`. |
| `max_value_mb` | integer | `256` | Hard ceiling on a **single cell** (text/JSON/blob) in MB. A value larger than this aborts the run with `RIVET_VALUE_TOO_LARGE`. Guards against one giant cell OOM-ing the process — the batch cap is average-based and can't bound a lone outlier. Set `0` to disable. See [Per-value ceiling](#per-value-ceiling-max_value_mb). |
| `adaptive` | boolean | `false` | Sample source write-pressure at runtime and react: shrink/restore the fetch batch size, and — in chunked mode with `parallel > 1` — drive the [concurrency governor](#adaptive-concurrency-governor). |
| `min_parallel` | integer | `1` | Floor for the concurrency governor: the fewest workers it will back down to under pressure. Ceiling is the export's `parallel`. Only consulted when `adaptive` is on and `parallel > 1`. |

### Batch memory cap (`max_batch_memory_mb`)

`memory_threshold_mb` is a process-level RSS guard — it fires after the OS has already committed memory. `max_batch_memory_mb` is an earlier, batch-level guard: it measures the actual Arrow buffer footprint of each batch before it is written.

```yaml
tuning:
  max_batch_memory_mb: 128
  on_batch_memory_exceeded: warn   # warn | fail | auto_shrink
```

| Policy | Behaviour |
|---|---|
| `warn` | **(default)** Log a warning with the actual size, the limit, and a suggested `batch_size`. Continue the export. |
| `fail` | Return an error immediately. The export stops. Use in strict pipelines where oversized batches indicate a configuration problem. |
| `auto_shrink` | Split the oversized batch in half recursively until each sub-batch fits within the limit, then write the sub-batches individually. Transparent to the rest of the pipeline — total row count and output are identical. |

The warning and error messages include a suggested `batch_size`:

```
batch memory 184 MB exceeds max_batch_memory_mb=128 MB (5000 rows).
Consider lowering batch_size to ~3478.
```

Use `auto_shrink` when you want protection against accidental wide-table OOM without needing to tune `batch_size` manually. Use `fail` in CI pipelines where any oversized batch should block the run.

### Per-value ceiling (`max_value_mb`)

`max_batch_memory_mb` and the adaptive byte budget are **average-based** — they size a batch from its mean row width. Neither bounds a *single* pathological cell: one 300 MB JSONB document or `bytea` blob among otherwise-small rows still lands whole in memory and can OOM the process (and the `auto_shrink` splitter can't divide a single oversized value).

`max_value_mb` is a hard per-value ceiling. Before a batch is split or encoded, Rivet checks every variable-length cell (text / JSON / binary — fixed-width types can't be individually huge); a value over the limit aborts the run:

```
RIVET_VALUE_TOO_LARGE: column 'body' has a single value of 301.2 MB, exceeding the
per-value ceiling of 256 MB. ...Raise `tuning.max_value_mb` (or set it to 0 to
disable the guard) if this value is expected.
```

It is **on by default at 256 MB** — high enough to never trip on realistic data, low enough to catch a runaway cell before it OOMs. Raise it for tables that legitimately store large blobs, or set `max_value_mb: 0` to disable the guard entirely.

## Choosing `batch_size`

`batch_size` is the most impactful parameter for both performance and memory usage.

| batch_size | Memory per batch (narrow table) | Memory per batch (wide table) | Best for |
|------------|-------------------------------|-------------------------------|----------|
| 1,000 | ~1-5 MB | ~20-100 MB | Wide tables, low-memory environments |
| 5,000 | ~5-25 MB | ~100-500 MB | Medium tables, shared databases |
| 10,000 | ~10-50 MB | ~200 MB - 1 GB | General purpose (default balanced) |
| 50,000 | ~50-250 MB | ~1-5 GB | Read replicas, fast profile |

For wide tables (50+ columns, TEXT/JSONB fields), start with `batch_size: 1000-2000`.

## Adaptive batch sizing

Instead of a fixed row count, let Rivet adjust batch size based on memory:

```yaml
tuning:
  batch_size_memory_mb: 64          # target ~64 MB per batch
```

Rivet samples the first batch to estimate row size, then adjusts subsequent batches. Cannot be used together with `batch_size`.

## Adaptive concurrency governor

In **chunked mode with `parallel > 1`**, setting `adaptive: true` arms a governor that adjusts how many chunk workers (and therefore source connections) run concurrently, in response to source write-pressure. It backs parallelism down when the source is under load and recovers it when the load eases, staying within `[min_parallel, parallel]`.

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    adaptive: true        # arm batch-size adaptation + the governor
    min_parallel: 2       # never drop below 2 workers (default 1)

exports:
  - name: orders
    mode: chunked
    chunk_column: id
    parallel: 8           # ceiling — governor varies the live count in [2, 8]
```

**How it decides.** A dedicated monitoring connection polls a source write-pressure counter every ~1.5 s and compares it to the previous reading. A *rising* counter means pressure is climbing, so the governor sheds one worker; a flat/falling counter lets it recover one. The counter is:

| Engine | Pressure proxy | Read via |
|--------|----------------|----------|
| PostgreSQL | `pg_stat_bgwriter.checkpoints_req` | `SELECT checkpoints_req FROM pg_stat_bgwriter` (preceded by `pg_stat_clear_snapshot()`) |
| MySQL | global `Innodb_log_waits` | `SHOW GLOBAL STATUS LIKE 'Innodb_log_waits'` |

It is the **same proxy** the adaptive batch loop uses — enabling the governor adds no new query beyond what `adaptive: true` already runs.

### Required privileges (read-only is enough)

The governor needs **no elevated privileges**. A plain read-only role can run every query it issues — verified against PostgreSQL 16 and MySQL 8:

- **PostgreSQL** — a role with only `CONNECT` + `USAGE ON SCHEMA` + `SELECT ON TABLES` can read `pg_stat_bgwriter` and call `pg_stat_clear_snapshot()` (both are available to `PUBLIC`). No `pg_read_all_stats`, no superuser.

  ```sql
  CREATE ROLE rivet_ro LOGIN PASSWORD '…';
  GRANT CONNECT ON DATABASE mydb TO rivet_ro;
  GRANT USAGE ON SCHEMA public TO rivet_ro;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO rivet_ro;
  ```

- **MySQL** — a user with only `SELECT` on the target schema can run `SHOW GLOBAL STATUS`; it needs no `PROCESS` or other global privilege.

  ```sql
  CREATE USER 'rivet_ro'@'%' IDENTIFIED BY '…';
  GRANT SELECT ON mydb.* TO 'rivet_ro'@'%';
  ```

**Graceful degradation.** If the pressure query ever fails or returns nothing (locked-down role, unsupported engine view), the governor *holds parallelism flat* rather than failing the run — the export proceeds at the static `parallel` count. A failed monitoring connection logs a warning and disables the governor for that run; it never aborts the export.

> **Note on richer signals.** A future iteration may read lock waits / `idle in transaction` from `pg_stat_activity` or `SHOW PROCESSLIST`. Those *do* require elevated privileges (`pg_read_all_stats` on PostgreSQL; the `PROCESS` privilege on MySQL) to observe sessions other than your own. The current proxy was chosen specifically so the default least-privilege, read-only setup keeps working. When the richer signals land, this section will document the additional grants.

**Visibility.** Each adjustment is recorded in the run journal as a `ParallelismAdjusted` event (`from`, `to`, `reason`) and logged at `info` (`governor parallelism 8 → 7 (source pressure rising: backed off)`).

## Memory optimization tips

1. **Reduce `batch_size`** -- the single most effective knob
2. **Use `safe` profile** for wide tables on production databases
3. **Enable jemalloc** -- build with `--features jemalloc` for 20-40% lower RSS
4. **Set `memory_threshold_mb`** -- Rivet pauses fetching when RSS exceeds this

## Examples

### Minimal (use defaults)

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  # No tuning block → balanced profile with all defaults
```

### Aggressive (read replica)

```yaml
source:
  type: postgres
  url_env: REPLICA_URL
  tuning:
    profile: fast
    batch_size: 100000
    throttle_ms: 0
```

### Conservative (production OLTP)

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: safe
    batch_size: 1000
    throttle_ms: 1000
    statement_timeout_s: 60
    memory_threshold_mb: 512
```

---

## Capacity and memory planning

### Peak RSS formula

```
peak_rss ≈ batch_size × avg_row_bytes × parallel_workers
```

Add ~50–150 MB overhead for the Tokio runtime, the source connection pool, jemalloc bookkeeping, and the OS page cache on the temp file.

### Rule of thumb by table width

| Table type | Avg row bytes | Recommended batch_size | Expected peak RSS |
|---|---|---|---|
| Narrow (IDs, timestamps, small text) | ~100 B | 50 000–100 000 | ~50–200 MB |
| Medium (mixed text, JSON) | ~1 KB | 10 000–25 000 | ~50–250 MB |
| Wide (TEXT/JSONB payloads ≥ 10 KB avg) | ~10 KB | 500–2 000 | ~50–200 MB |

Use the `safe` profile for wide tables — it caps `batch_size` at 500 automatically.

### How `memory_threshold_mb` works

When `tuning.memory_threshold_mb` is set, Rivet samples RSS after each batch (via `mach_task_basic_info` on macOS, `/proc/self/statm` on Linux). If RSS exceeds the threshold, fetching pauses until RSS drops below 80 % of the limit. This prevents OOM on tables with highly variable row widths.

```yaml
source:
  tuning:
    memory_threshold_mb: 1024   # pause fetching above 1 GB RSS
```

The guard adds ~1–2 ms of overhead per batch from the RSS syscall. It is enabled by default on `balanced` (4096 MB) and `safe` (2048 MB) profiles; set `memory_threshold_mb: 0` to disable it.

### Parallelism and source capacity

Each parallel chunk worker opens its own source connection. Postgres `max_connections` is typically 100–200 for shared instances and 20–50 for read replicas. `rivet check` warns when `parallel >= max_connections`.

Safe upper bound: `parallel ≤ max_connections / 4` to leave headroom for application traffic.

### Per-export memory isolation

`--parallel-export-processes` spawns one OS process per export — each export has its own allocator and heap, so peak RSS is per-export rather than aggregate. Use this mode when running many wide-table exports at once on memory-constrained hosts.
