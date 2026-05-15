# Rivet v0.5.x Benchmark Report

Measured on: 2026-05-15  
Binary: `target/release/rivet` (v0.5.0)  
Host: macOS Darwin 25.4.0, Apple Silicon  
Database: PostgreSQL 16 (local docker-compose)

---

## §4.3 Compression Profiles

**Dataset:** `bench_narrow` — 500,000 rows, 5 numeric/timestamp columns, avg ~40 B/row

| Profile | Codec | Wall (s) | User (s) | RSS (MB) | Output (MB) |
|---|---|---:|---:|---:|---:|
| `none` | uncompressed | 0.93 | 0.11 | 46.4 | 13 |
| `fast` | Snappy | 0.92 | 0.11 | 49.5 | 10 |
| `balanced` | Zstd-3 | 0.94 | 0.14 | 50.2 | **5** |
| `compact` | Zstd-9 | 1.04 | **0.28** | **82.2** | **5** |

**Key findings:**

- `balanced` (Zstd-3) compresses 2.6× better than `none` with **identical wall time** and only 4 MB more RSS. This is the recommended production default.
- `fast` (Snappy) is slightly faster than `balanced` but produces 2× larger files. Use for large backfills where storage cost is secondary.
- `compact` (Zstd-9) offers no additional compression benefit over `balanced` on numeric data, while using **64% more memory** and **2× more CPU**. Only use when storage/network cost dominates.
- Wall time is dominated by source query and Parquet serialization, not compression. All profiles are within 12% of each other.

---

## §4.4 Row Group Targets

**Dataset:** `bench_wide` — 100,000 rows, 10 TEXT columns, 200 chars each (~2 KB/row)

| Target | Row group strategy | Wall (s) | RSS (MB) | Output (MB) |
|---|---|---:|---:|---:|
| 32 MB | `auto` | 2.26 | **75.9** | 1 |
| 64 MB | `auto` | 2.25 | 79.5 | 1 |
| 128 MB | `auto` | 2.26 | 82.7 | 1 |
| 256 MB | `auto` | 2.25 | 82.3 | 1 |

**Key findings:**

- Smaller row group targets reduce peak RSS with **no wall-time penalty** — latency is identical across all targets.
- `32 MB` target saves ~7 MB RSS vs `256 MB` on bench_wide (2 KB rows). On wider tables the difference is larger — see the content_items benchmark in [low-memory-runners.md](best-practices/low-memory-runners.md) where it contributes to a 5.7× RSS reduction.
- `auto` strategy chooses row count dynamically from Arrow schema widths. Users don't need to tune a row count — they specify a memory budget and the writer adapts.
- Output size is identical across targets (compression ratio is not affected by row group boundaries).

---

## §4.5 Batch Memory Policies

**Dataset:** `bench_wide` — 100,000 rows, 10 TEXT columns, 200 chars each (~2 KB/row)  
**Cap:** 64 MB, `batch_size: 10,000` (estimated batch ~20 MB — cap does **not** trigger on this dataset)

| Policy | Wall (s) | RSS (MB) | Output (MB) |
|---|---:|---:|---:|
| `warn` (cap=64 MB) | 2.25 | 82.2 | 1 |
| `auto_shrink` (cap=64 MB) | 2.26 | 81.1 | 1 |
| no cap (warn, 4 GB) | 2.26 | 83.7 | 1 |

**Key findings:**

- When batches stay under the cap, `warn` and `auto_shrink` add **zero measurable overhead** vs no cap. The policy check is a single memory comparison per batch.
- All three policies produce identical output and identical wall time, confirming no correctness regression from the cap mechanism.
- On bench_wide at `batch_size: 10,000`, each batch is ~20 MB in Arrow — below the 64 MB cap. To observe RSS reduction from `auto_shrink`, use wider tables or larger batch sizes.

**Content_items reference** (200,000 rows, avg ~3 KB/row, 12 columns including TEXT/JSONB):

| Config | `batch_size` | Peak RSS | Wall (s) |
|---|---|---:|---:|
| No cap (`batch_size: 25,000`) | 25,000 | 878 MB | 17.2 |
| Safe baseline (`max_batch_memory_mb: 64`) | ~2,000 | **154 MB** | 16.6 |
| Tight (`batch_size: 500`, cap 32 MB) | 500 | **111 MB** | 16.3 |

The 5.7× RSS reduction holds for wide real-world tables. See [low-memory-runners.md](best-practices/low-memory-runners.md) for the full methodology.

---

## §4.6 Quality Uniqueness

**Dataset:** `bench_hc` — 200,000 rows, UUID + email columns (high cardinality)

| Config | Wall (s) | RSS (MB) | Output (MB) |
|---|---:|---:|---:|
| `unique_max_entries: 50,000` (capped) | 1.54 | 41.0 | 5 |
| no cap (200,000 unique values tracked) | 1.54 | 41.8 | 5 |

**Key findings:**

- At 200,000 rows, uncapped uniqueness tracking adds only ~0.8 MB RSS above the capped baseline. xxHash3-64 stores u64 hashes (8 bytes), so 200K × 2 columns × 8 bytes = ~3.2 MB of hash sets — negligible against total process RSS.
- Wall time is identical: hash-based tracking is O(1) per row.
- `unique_max_entries` is still recommended for high-cardinality tables as a **defensive cap**, not because the overhead is large. Without it, a runaway uniqueness tracking on a billion-row table could grow to hundreds of MB.
- The hash-based approach (xxHash3-64, typed) is a quality signal, not an exact distinct count.

---

## Summary

| Claim | Evidence |
|---|---|
| `balanced` compression: same speed as `none`, 2.6× smaller files | §4.3 ✓ |
| `compact` adds no compression benefit over `balanced` on numeric data | §4.3 ✓ |
| Smaller row group targets reduce RSS with zero wall-time cost | §4.4 ✓ |
| Memory policies add zero overhead when cap doesn't trigger | §4.5 ✓ |
| `auto_shrink` reduces RSS 5.7× on wide real-world tables | §4.5 content_items ✓ |
| xxHash3-64 uniqueness tracking overhead is < 1 MB on 200K rows | §4.6 ✓ |
| `unique_max_entries` cap recommended as defensive limit | §4.6 ✓ |
