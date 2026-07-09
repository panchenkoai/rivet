# Best Practices

Practical guidance for using Rivet's resource-aware extraction capabilities.
These guides go beyond the reference documentation to explain *why* settings
matter and *when* to use them.

The tuning, quality, and compression settings shown here apply to every Rivet
source (PostgreSQL, MySQL, SQL Server, MongoDB) and every mode (`full`,
`incremental`, `chunked`, `time_window`, `cdc`) — the quick-start examples below
use PostgreSQL + `incremental` only for concreteness.

| Guide | What it covers |
|---|---|
| [Resource-aware extraction](resource-aware-extraction.md) | Memory budgets, batch cap policies (`warn`/`fail`/`auto_shrink`), RSS formula |
| [Parquet tuning](parquet-tuning.md) | Row group strategies, target sizes, downstream read implications |
| [Compression profiles](compression-profiles.md) | Profile-to-codec mapping, CPU/size trade-offs, when to use each |
| [Quality checks](quality-checks.md) | Row count gates, null ratio, uniqueness tracking, `unique_max_entries` cap |
| [Low-memory runners](low-memory-runners.md) | Settings for 512 MB–4 GB hosts; `auto_shrink` guarantees and caveats |
| [Gentle SQL Server extraction](mssql-gentle-extraction.md) | Easy on the source DB *and* the worker; why `chunk_size` (not `chunk_size_memory_mb`) on MSSQL — config: [`rivet_mssql_gentle.yaml`](rivet_mssql_gentle.yaml) |
| [Recovery and resume](recovery-and-resume.md) | `--resume` semantics, crash recovery, state inspection |
| [Benchmark methodology](benchmark-methodology.md) | How to run E2E and Criterion benchmarks, interpret results, compare versions |

## Quick-start recipes

### Safe production export

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: balanced

exports:
  - name: orders
    query: "SELECT * FROM orders"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    compression_profile: balanced
    destination:
      type: local
      path: ./out
    parquet:
      row_group_strategy: auto
      target_row_group_mb: 128
    quality:
      row_count_min: 1
      unique_columns: [id]
      unique_max_entries: 1000000
    tuning:
      max_batch_memory_mb: 256
      on_batch_memory_exceeded: warn
```

### Low-memory runner (≤ 512 MB RAM)

```yaml
tuning:
  profile: safe
  max_batch_memory_mb: 64
  on_batch_memory_exceeded: auto_shrink
parquet:
  row_group_strategy: auto
  target_row_group_mb: 32
  max_row_group_mb: 64
compression_profile: fast
```

### CI strict mode

```yaml
tuning:
  max_batch_memory_mb: 128
  on_batch_memory_exceeded: fail
quality:
  row_count_min: 100
  unique_columns: [id]
  unique_max_entries: 500000
```
