# Benchmark Methodology

How to run, interpret, and compare Rivet's benchmark suites.

Rivet has two benchmark layers:

| Layer | Tool | Purpose |
|-------|------|---------|
| **Micro-benchmarks** | Criterion (`benches/`) | Hot-path throughput, compilation check, per-function regression gate |
| **E2E benchmarks** | Shell runner (`dev/bench/`) | Wall time, peak RSS, output size for full pipeline runs |

---

## E2E benchmark runner

The E2E runner measures complete pipeline runs — source query, batching, writing, file output — against a live Postgres instance seeded with representative datasets.

### Prerequisites

```bash
# 1. Start Postgres
docker compose up -d postgres

# 2. Seed benchmark datasets
PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -f dev/bench/seed_bench_pg.sql

# 3. Build release binary
cargo build --release

# 4. Run all suites
RIVET=./target/release/rivet POSTGRES_URL=postgres://rivet:rivet@localhost:5432/rivet \
  bash dev/bench/run_bench.sh all | tee bench_report.md
```

To run a single suite:

```bash
bash dev/bench/run_bench.sh compression
bash dev/bench/run_bench.sh row_group
bash dev/bench/run_bench.sh batch_memory
bash dev/bench/run_bench.sh quality
```

### Datasets

| Table | Rows | Description |
|-------|------|-------------|
| `bench_narrow` | 500,000 | INT / BIGINT / TIMESTAMPTZ — baseline throughput |
| `bench_wide` | 100,000 | 10 TEXT columns × ~200 B each — memory pressure |
| `bench_json` | 50,000 | JSONB payload column — CPU/string pressure |
| `bench_hc` | 200,000 | UUID + email + phone — quality uniqueness RAM risk |
| `bench_decimal` | 200,000 | NUMERIC(12,2) columns — type conversion pressure |
| `bench_sparse` | 10,000 | Many nullable columns — low-density export |

### Output format

The runner emits a Markdown table:

```
## Compression profiles — bench_narrow

| Profile | Compression | Wall(s) | User(s) | Sys(s) | RSS(MB) | Files | Size(MB) |
|---------|-------------|---------|---------|--------|---------|-------|----------|
| none    | uncompressed | 4.2   |   3.8   |  0.4   |  210    |  1    |  38.2    |
| fast    | snappy       | 4.5   |   4.1   |  0.4   |  215    |  1    |  21.4    |
| balanced| zstd-3       | 5.1   |   4.7   |  0.4   |  218    |  1    |  14.8    |
| compact | zstd-9       | 8.3   |   7.9   |  0.4   |  219    |  1    |  13.1    |
```

### Metrics

| Metric | Meaning | Notes |
|--------|---------|-------|
| `Wall(s)` | Total elapsed seconds | Most user-facing metric; includes all phases |
| `User(s)` | User-space CPU seconds | Compression, Arrow serialization, hashing |
| `Sys(s)` | Kernel CPU seconds | File I/O, syscalls |
| `RSS(MB)` | Peak resident set size | Captured via GNU `time -v` or `gtime -v` |
| `Files` | Output file count | > 1 means file splitting occurred |
| `Size(MB)` | Total output file size | Compressed size on disk |

**RSS note:** RSS is captured by the shell runner via the `/usr/bin/time -v` (Linux) or `gtime -v` (macOS) wrapper. It reflects the high-water mark for the entire process, including the Parquet writer buffer, the Arrow batch in flight, the source connection pool, and the OS heap. It is **not** the same as the Arrow `get_array_memory_size()` value that `max_batch_memory_mb` checks — expect RSS to be 50–200 MB higher.

---

## How to compare two versions

To compare `v0.4.0` vs `v0.5.0` (or any two builds):

```bash
# Build the reference binary
git checkout v0.4.0
cargo build --release
cp target/release/rivet /tmp/rivet_v040

# Build the current binary
git checkout main
cargo build --release
cp target/release/rivet /tmp/rivet_v050

# Run both against the same seed
RIVET=/tmp/rivet_v040 bash dev/bench/run_bench.sh all > bench_v040.md
RIVET=/tmp/rivet_v050 bash dev/bench/run_bench.sh all > bench_v050.md
```

Then diff the wall time and RSS columns. Key claims to validate:

| Claim | Suite | Metric to compare |
|-------|-------|-------------------|
| Row group auto-tuning reduces RSS on wide tables | `row_group` on `bench_wide` | `RSS(MB)` |
| `balanced` profile compression is good default | `compression` on `bench_narrow` | `Wall(s)`, `Size(MB)` |
| `auto_shrink` has tolerable overhead | `batch_memory` on `bench_wide` | `Wall(s)` vs no-cap baseline |
| Typed hashing reduces quality check overhead | `quality` on `bench_hc` | `User(s)` |

---

## Micro-benchmarks (Criterion)

Criterion benchmarks live in `benches/` and measure specific hot paths in isolation.

```bash
# Run all benchmarks (full Criterion measurement)
cargo bench

# Run a specific group
cargo bench --bench hot_paths
cargo bench --bench resource_aware

# Compile and smoke-check (1 sample, no regression gate)
cargo bench --bench hot_paths -- --warm-up-time 1 --measurement-time 1 --sample-size 10

# Compare to a saved baseline
cargo bench --bench hot_paths -- --save-baseline main
# ... make changes ...
cargo bench --bench hot_paths -- --baseline main
```

### Available benchmarks

| Binary | Group | What it measures |
|--------|-------|-----------------|
| `hot_paths` | `arrow_cast` | Arrow type-cast throughput for each SQL type |
| `hot_paths` | `parquet_write` | Parquet writer throughput for narrow / wide batches |
| `hot_paths` | `quality_hash` | Quality uniqueness tracking throughput |
| `resource_aware` | `auto_shrink` | Split overhead at different cap levels |
| `resource_aware` | `compression_profiles` | Per-codec wall time for a 10,000-row batch |
| `resource_aware` | `row_group_computation` | Row group target computation for narrow / wide schemas |
| `resource_aware` | `quality_uniqueness_cap` | Uniqueness tracking throughput with and without a cap |

Criterion saves HTML reports to `target/criterion/`. Open `target/criterion/index.html` in a browser for violin plots and per-sample distribution.

---

## CI integration

The nightly workflow runs both layers automatically:

```yaml
# .github/workflows/nightly.yml
bench-smoke:    # Criterion: compile + 1 sample (not a regression gate)
bench-e2e:      # run_bench.sh all → uploads bench_report.md as artifact
```

The `bench-smoke` job verifies that benchmarks compile and do not panic — it is not a regression gate. The `bench-e2e` job captures a full report and uploads it as a GitHub Actions artifact named `bench-report-<run_id>`. Download it from the Actions UI to inspect current numbers.

To add a regression gate in the future, save a `--baseline` from a release tag and add a Criterion comparison step to `ci.yml`.

---

## Interpreting results

### Normal variance

E2E runs on shared CI (or a busy laptop) can show ±10–20% variance in wall time and ±5% in RSS depending on filesystem cache warmth and page cache pressure. Run each suite 3 times and take the median for a stable comparison.

### When numbers look wrong

| Symptom | Likely cause |
|---------|-------------|
| RSS much higher than expected | Filesystem cache not warm; first run always higher |
| Wall time much higher than expected | Postgres query planner chose a sequential scan; check indexes on bench tables |
| `Files > 1` unexpectedly | File splitting triggered; check `max_split_size_mb` in the config |
| `Size(MB)` unexpectedly large | Wrong compression profile in the config template |

### Relating E2E numbers to `rivet plan` output

`rivet plan` shows a narrow–wide memory range for each export:

```
Batch memory : ~2 MB (narrow) – ~95 MB (wide)
```

The narrow bound assumes ~200 B/row; the wide bound assumes ~10 KB/row. The E2E benchmarks let you validate which bound your real table shape falls closer to by running with the same config and comparing the measured `RSS(MB)` against the plan estimate.

---

## See also

- [Resource-aware extraction](resource-aware-extraction.md) — memory budgets and policies
- [Parquet tuning](parquet-tuning.md) — row group targets and downstream read implications
- [Compression profiles](compression-profiles.md) — codec mapping and trade-offs
- [Low-memory runners](low-memory-runners.md) — settings for constrained environments
- [`dev/bench/run_bench.sh`](../../dev/bench/run_bench.sh) — the E2E runner script
- [`benches/`](../../benches/) — Criterion benchmark sources
