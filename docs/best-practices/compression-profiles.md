# Compression Profiles

Rivet's `compression_profile` is a high-level, intent-based way to choose a
compression codec without knowing the specific codec name or level.

---

## Profile-to-codec mapping

| Profile | Codec | Level | Best for |
|---|---|---|---|
| `none` | Uncompressed | — | Debug / scratch, temporary files, downstream re-compression |
| `fast` | Snappy | — | Fast backfills, high-throughput pipelines, read replicas |
| `balanced` | Zstd | 3 | Production default — good compression, predictable CPU |
| `compact` | Zstd | 9 | Storage-sensitive archives, cold storage, network-constrained uploads |

---

## Choosing a profile

### `none` — uncompressed

Use when:
- You are debugging output format or schema issues and want to open the file quickly.
- Downstream tooling re-compresses the file (e.g. S3 server-side compression).
- The file is temporary and will be deleted immediately after processing.

**Avoid** in production: uncompressed Parquet files are 3–10× larger than
Zstd-3 on typical tabular data, increasing storage cost and upload time.

### `fast` — Snappy

Use when:
- Throughput matters more than output size (large backfills, bulk loads).
- The extraction runs on a shared database or low-CPU runner where Zstd overhead
  is unwanted.
- Downstream query engines read the file frequently and benefit from fast
  decompression (Snappy is ~2–3× faster to decompress than Zstd).

Snappy produces files ~20–30% larger than Zstd-3 on typical tabular data.

### `balanced` — Zstd level 3 (recommended default)

Use when:
- You want a sensible production default without thinking about the trade-off.
- Extraction runs on dedicated infrastructure (not shared OLTP database).
- Files are stored in S3/GCS and you want reasonable storage costs.

Zstd level 3 delivers ~60–70% compression ratio on typical tabular data with
~2–3× the CPU cost of Snappy. This is the right default for most pipelines.

### `compact` — Zstd level 9

Use when:
- Storage cost or network transfer cost is a primary constraint.
- The pipeline runs infrequently (nightly, weekly) and has CPU to spare.
- Files are cold-stored and rarely read.

Zstd level 9 can deliver 5–15% better compression than level 3, at 3–5× the
CPU cost. It is rarely worth using in real-time or latency-sensitive pipelines.

---

## Precedence

`compression_profile` takes priority over the lower-level `compression` and
`compression_level` fields. If you set `compression_profile`, any explicit
`compression` or `compression_level` values on the same export are ignored.

```yaml
# compression_profile wins — compression: snappy is ignored
compression_profile: compact
compression: snappy        # ignored
```

This ensures profiles are self-contained: once you pick a profile, you do not
need to audit individual codec settings.

To use a codec not covered by the four profiles (e.g. Gzip, LZ4), omit
`compression_profile` and set `compression` directly:

```yaml
compression: gzip
compression_level: 6
```

---

## CSV output

Compression profiles apply only to Parquet format. For CSV exports, the
`compression_profile` field is parsed but silently ignored — CSV files are
written uncompressed (you can compress them after export with `gzip`, `zstd`,
etc.).

---

## Configuration examples

### Production default

```yaml
format: parquet
compression_profile: balanced
```

### Fast backfill from read replica

```yaml
format: parquet
compression_profile: fast
tuning:
  profile: fast
  batch_size: 50000
```

### Cold storage archive

```yaml
format: parquet
compression_profile: compact
parquet:
  row_group_strategy: auto
  target_row_group_mb: 256
```

---

## Benchmark expectations

Based on typical tabular data (mixed integer, text, timestamp columns):

| Profile | Relative wall time | Relative output size |
|---|---:|---:|
| `none` | 1.0× (baseline) | 1.0× (largest) |
| `fast` (Snappy) | 1.1–1.3× | 0.3–0.5× |
| `balanced` (Zstd-3) | 1.3–2.0× | 0.2–0.4× |
| `compact` (Zstd-9) | 3–6× | 0.18–0.35× |

Actual numbers depend heavily on data entropy. High-entropy data (UUIDs,
hashes, random text) compresses poorly regardless of level. Low-entropy data
(repeated values, sequential IDs, timestamps) compresses exceptionally well
even at level 3.

Run `./dev/bench/run_bench.sh compression` against your own tables for concrete
numbers.

---

## See also

- [Config reference — `exports[].compression_profile`](../reference/config.md)
- [Parquet tuning](parquet-tuning.md)
- [Resource-aware extraction](resource-aware-extraction.md)
