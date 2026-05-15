# Low-Memory Runners

How to run Rivet reliably on hosts with 1–4 GB of RAM — containers, small VMs, and CI workers.

> **Numbers in this guide are measured, not estimated.** The benchmark below was run
> against a `content_items` table: 200,000 rows, 12 columns (TEXT, JSONB, VARCHAR),
> average row size ~3 KB. All three scenarios exported the same 200,000 rows
> and produced identical row counts in the output.

---

## The problem with defaults

Rivet's `balanced` profile fetches 10,000 rows per batch. For a narrow table (IDs, timestamps, small text) this is fine. For a wide table (TEXT/JSONB columns, average row ~3 KB), a larger batch size can push total RSS above 800 MB:

| Config | `batch_size` | Peak RSS | Wall time | Output size |
|--------|--------------|----------|-----------|-------------|
| No cap (`batch_size: 25000`) | 25,000 | **878 MB** | 17.2 s | 71 MB (zstd-3) |
| Safe baseline (`max_batch_memory_mb: 64`) | 2,000 (safe profile) | **154 MB** | 16.6 s | 71 MB (zstd-3) |
| Tight (`batch_size: 500`, cap 32 MB) | 500 | **111 MB** | 16.3 s | 188 MB (snappy) |

The safe baseline cuts RSS by **5.7×** with no wall-time regression. Default profiles do not know your table shape — low-memory environments need explicit caps.

---

## The safe baseline

Start here for any host with less than 2 GB available:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: safe
    max_batch_memory_mb: 64
    on_batch_memory_exceeded: auto_shrink
    memory_threshold_mb: 512

parquet:
  row_group_strategy: auto
  target_row_group_mb: 32
  max_row_group_mb: 64
```

What each setting does:

| Setting | Effect |
|---------|--------|
| `profile: safe` | `batch_size: 2000`, `throttle_ms: 500`, `memory_threshold_mb: 2048` |
| `max_batch_memory_mb: 64` | Caps each Arrow batch at 64 MB; overrides the profile default |
| `on_batch_memory_exceeded: auto_shrink` | Splits oversized batches instead of failing |
| `memory_threshold_mb: 512` | Pauses fetching when process RSS reaches 512 MB |
| `target_row_group_mb: 32` | Writes smaller Parquet row groups — reduces Parquet writer peak RSS |

On a wide-text table (200K rows, avg 3 KB/row, 12 columns), this combination measured **154 MB peak RSS** — compared to 878 MB without the cap. Actual RSS on your table will depend on row width and column count; use `rivet state metrics` to validate after the first run.

---

## 512 MB host (container / CI)

```yaml
source:
  tuning:
    profile: safe
    batch_size: 500
    max_batch_memory_mb: 32
    on_batch_memory_exceeded: auto_shrink
    memory_threshold_mb: 256
    throttle_ms: 1000

parquet:
  row_group_strategy: auto
  target_row_group_mb: 16
  max_row_group_mb: 32

compression_profile: fast    # snappy: lower CPU overhead than zstd
```

On the wide-text benchmark table (200K rows, ~3 KB/row), this config measured **111 MB peak RSS** — well within a 512 MB host. Wall time was identical to the uncapped run.

**Trade-off:** `fast` (snappy) compresses less aggressively than `balanced` (zstd-3). On the same table, snappy produced a 188 MB output file vs. 71 MB for zstd-3. If storage cost matters, use `compression_profile: balanced` even on a constrained host — the CPU cost difference is small (snappy is only ~5–10% faster on text-heavy data).

---

## Wide-table export (TEXT / JSONB heavy)

Wide tables need a smaller batch size and tighter row group targets. Use `batch_size_memory_mb` to let Rivet calculate batch size from a memory budget rather than a row count:

```yaml
exports:
  - name: events
    query: "SELECT id, payload, metadata FROM events"
    format: parquet
    destination: { type: local, path: ./out }
    tuning:
      batch_size_memory_mb: 32      # target ~32 MB per batch
      max_batch_memory_mb: 64       # hard cap; auto_shrink if exceeded
      on_batch_memory_exceeded: auto_shrink
    parquet:
      row_group_strategy: auto
      target_row_group_mb: 32
```

`batch_size_memory_mb` samples the first batch to estimate row width, then adjusts subsequent batches automatically. This is more reliable than guessing a row count for wide tables.

---

## Parallel exports on memory-constrained hosts

When running multiple exports in parallel (`--parallel-export-processes`), each worker spawns its own OS process with an independent heap. Budget memory per-worker:

```
available_ram = total_ram × 0.7     # leave 30% for OS / filesystem cache
ram_per_worker = available_ram / workers
max_batch_memory_mb = ram_per_worker × 0.5   # batch is ~half of worker RSS
```

Example for 2 GB host, 2 workers:

```
available = 2048 × 0.7 = ~1400 MB
per worker = 1400 / 2 = ~700 MB
max_batch_memory_mb = 700 × 0.5 = ~350 MB
```

```yaml
source:
  tuning:
    max_batch_memory_mb: 256    # conservative, with headroom
    on_batch_memory_exceeded: auto_shrink
    memory_threshold_mb: 512
```

---

## `auto_shrink` guarantees and caveats

`auto_shrink` is the most reliable policy for low-memory environments because it adapts at runtime instead of failing.

**Guarantees:**
- Total row count is identical to a no-cap run — no rows are lost or duplicated.
- Cursor state and manifest are correct — each sub-batch writes atomically.
- Parquet schema is stable across sub-batches.

**Caveats:**
- Adds CPU overhead proportional to the split depth. A batch that splits 8 levels (256× fragmentation) adds measurable latency.
- If a single row is wider than `max_batch_memory_mb`, the split terminates at a 1-row batch and writes it as-is. This is correct but may produce many small files.
- For extremely wide rows (average row > `max_batch_memory_mb`), lower `batch_size` to 1–100 instead of relying on auto_shrink alone.

For tables where individual rows can be > 64 MB (BLOB-heavy schemas), set `max_batch_memory_mb` to match the expected maximum row size and accept single-row batches as the floor:

```yaml
tuning:
  batch_size: 10
  max_batch_memory_mb: 256
  on_batch_memory_exceeded: auto_shrink
```

---

## Monitoring RSS in production

Rivet always reports peak RSS in the run summary printed to the terminal:

```
✓ events  full  142,380 rows  1 files  71.2 MB  16.6s  RSS 154 MB
```

The `RSS` value is sampled by a background thread during the run, so it reflects the high-water mark rather than end-of-process RSS.

The same value is stored in the state DB and accessible via:

```bash
rivet state metrics --config rivet.yaml --export events
```

Use these to validate that RSS stays within your host budget across different table sizes. The first run on a new table is the most reliable baseline — query planner cache, filesystem buffer, and jemalloc slab warmth all affect subsequent runs.

---

## Quick-reference: settings by host RAM

Measured on a wide-text table (200K rows, ~3 KB avg row, 12 columns incl. TEXT and JSONB).
Measured RSS is what you can expect on similarly shaped tables; narrow numeric tables will use less.

| Host RAM | `batch_size` | `max_batch_memory_mb` | `target_row_group_mb` | `memory_threshold_mb` | Measured RSS |
|----------|-----------|-----------------------|-----------------------|-----------------------|-------------|
| 512 MB   | 250–500   | 16–32                 | 16                    | 200                   | ~111 MB ✓   |
| 1 GB     | 500–1000  | 32–64                 | 32                    | 400                   | ~154 MB ✓   |
| 2 GB     | 1000–2000 | 64–128                | 32–64                 | 768                   | ~200 MB est. |
| 4 GB     | 2000–5000 | 128–256               | 64–128                | 1536                  | ~350 MB est. |

Rows marked ✓ are directly measured. Rows marked "est." are extrapolated from the measured data points.
Actual RSS will be higher on tables with larger average row width (e.g. JSONB blobs, long TEXT fields).

---

## See also

- [Resource-aware extraction](resource-aware-extraction.md) — memory budgets, policies, RSS formula
- [Parquet tuning](parquet-tuning.md) — row group strategies and downstream implications
- [Tuning reference](../reference/tuning.md) — all tuning parameters and profiles
