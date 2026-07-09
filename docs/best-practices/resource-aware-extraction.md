# Resource-Aware Extraction

Rivet gives you explicit controls over how much memory a single export is
allowed to use. This guide explains the mental model, the available knobs, and
the recommended defaults for common scenarios.

---

## The two memory budgets

Rivet operates with two independent memory boundaries:

| Boundary | Config key | What it measures | When it fires |
|---|---|---|---|
| Process RSS guard | `tuning.memory_threshold_mb` | OS-reported resident set size | After each batch, if RSS ≥ threshold → pause fetching |
| Batch footprint cap | `tuning.max_batch_memory_mb` | Arrow in-memory buffer size | Before writing, if batch bytes > cap → apply policy |

They are complementary, not redundant:

- **RSS guard** (`memory_threshold_mb`) is a late, coarse signal — the OS has already committed the memory.
- **Batch cap** (`max_batch_memory_mb`) is an early, precise signal — it measures the Arrow buffer before I/O.

For predictable memory use, set both.

---

## Batch memory cap policies

When a batch exceeds `max_batch_memory_mb`, the `on_batch_memory_exceeded`
policy determines what happens:

| Policy | Effect | When to use |
|---|---|---|
| `warn` (default) | Log the overage with a suggested `batch_size`, continue. | Development, observability without blocking. |
| `fail` | Exit non-zero immediately. | CI pipelines where oversized batches indicate a config error. |
| `auto_shrink` | Recursively split the batch in half until each sub-batch fits, then write sub-batches. Row count and output are identical. | Low-memory runners where you cannot predict row width in advance. |

`auto_shrink` is the safest choice for wide or skewed tables. It adds CPU
overhead from the extra Arrow slicing, but total output is always correct.

---

## Recommended configurations

### Production default — shared database

```yaml
tuning:
  profile: balanced
  max_batch_memory_mb: 256
  on_batch_memory_exceeded: warn
```

### Strict CI pipeline

```yaml
tuning:
  max_batch_memory_mb: 128
  on_batch_memory_exceeded: fail
```

Any batch that would exceed 128 MB is a sign that `batch_size` is too large for
the table width. The pipeline fails fast rather than silently consuming memory.

### Low-memory runner (≤ 512 MB RAM)

```yaml
tuning:
  profile: safe
  max_batch_memory_mb: 64
  on_batch_memory_exceeded: auto_shrink
```

`auto_shrink` automatically adapts to unexpected wide rows without operator
intervention.

### High-throughput read replica

```yaml
tuning:
  profile: fast
  batch_size: 100000
  max_batch_memory_mb: 512
  on_batch_memory_exceeded: warn
```

---

## Understanding `batch_size` vs `max_batch_memory_mb`

`batch_size` is a row count. `max_batch_memory_mb` is a byte budget. They
interact:

```
actual_batch_bytes ≈ batch_size × avg_row_bytes
```

For a narrow numeric table (avg row ~100 B), `batch_size: 50000` is
~5 MB — well within any reasonable cap.

For a wide text table (avg row ~10 KB), the same `batch_size: 50000` is
~500 MB — a common source of OOM surprises.

**Rule of thumb:** set `max_batch_memory_mb` to your desired per-batch budget,
and let `auto_shrink` handle any table that exceeds it. You only need to tune
`batch_size` manually when you want to optimise throughput.

---

## `auto_shrink` guarantees

When `auto_shrink` splits a batch:

- **Row count is preserved.** Total rows exported equals source rows queried.
- **No duplicate rows.** Each row appears in exactly one sub-batch.
- **Cursor correctness.** For incremental exports, the cursor advances to the
  last row of the *original* batch, not the last sub-batch. This ensures the
  second run does not re-export rows.
- **File splitting is unaffected.** `max_file_size` boundaries are computed
  per sub-batch write, so file splits still occur at roughly the configured
  size.
- **Quality checks run on sub-batches.** Row count, null ratio, and uniqueness
  checks accumulate correctly across all sub-batches.

---

## Peak RSS formula

```
peak_rss ≈ max_batch_memory_mb + parquet_writer_buffer + rivet_overhead
```

`parquet_writer_buffer` is typically 1–2× the batch footprint during encoding.
`rivet_overhead` is ~50–150 MB (runtime, connection pool, temp file page cache).

Practical rule: provision at least `3 × max_batch_memory_mb + 256 MB` of
available RAM.

---

## See also

- [Tuning reference](../reference/tuning.md) — full parameter list
- [Parquet tuning](parquet-tuning.md) — row group size and its memory implications
- [Compression profiles](compression-profiles.md) — CPU/size trade-offs
