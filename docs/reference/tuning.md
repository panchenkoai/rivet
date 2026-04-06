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
| `memory_threshold_mb` | 0 (none) | 0 (none) | 0 (none) |

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
| `memory_threshold_mb` | integer | 0 | RSS threshold in MB; pauses fetching if exceeded (0 = disabled) |

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
