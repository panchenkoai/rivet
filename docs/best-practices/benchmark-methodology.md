# Benchmark Methodology

How to run, interpret, and compare Rivet's benchmark suites.

Rivet has two benchmark layers:

| Layer | Tool | Purpose |
|-------|------|---------|
| **Micro-benchmarks** | Criterion (`benches/`) | Hot-path throughput, compilation check, per-function regression gate |
| **Cross-tool / cross-engine E2E** | `dev/bench/smoke.py` + `docs/bench/matrix.yaml` | rivet vs 7 other tools on Postgres / MySQL / SQL Server / MongoDB — throughput, peak RSS, source-harm, type fidelity |

---

## Cross-tool / cross-engine E2E harness

The E2E harness compares rivet to duckdb, clickhouse-local, sling, ingestr, dlt,
and odbc2parquet exporting the same fixture to Parquet, and captures what each
tool does to the source (a co-running OLTP probe, longest query/txn, locks,
native engine counters). It is a **single source of truth**: everything is
driven from [`docs/bench/matrix.yaml`](../bench/matrix.yaml) by the runner
[`dev/bench/smoke.py`](../../dev/bench/smoke.py), which fails if the yaml declares
a metric the code doesn't capture. See [`docs/bench/README.md`](../bench/README.md)
for prerequisites and [`docs/bench/report.html`](../bench/report.html) for the
rendered results.

```bash
# system python has PyYAML + dlt; the homebrew pythons ship a broken pyexpat
/usr/bin/python3 dev/bench/smoke.py --engine postgres --table content_items
/usr/bin/python3 dev/bench/smoke.py --engine mysql --table content_items
/usr/bin/python3 dev/bench/smoke.py --engine mssql --table orders
```

Fixtures are seeded into a dedicated `rivet_bench` per engine (via the Rust
`seed` tool; sizes in `matrix.yaml`) so the live-test fixtures are untouched.
Three matrices print per run: benchmark, harm, type-loss.

**Comparing rivet versions** is a special case of the same harness — point
`RIVET_BIN` (or `$PATH` `rivet`) at each build and re-run; the benchmark matrix's
`rows_s` / `peak_mb` columns are the comparison. rivet's own steelman
(`mode: full, tuning.profile: fast, zstd`) was chosen this way — a measured +24 %
rows/s from dropping the `balanced` 50 ms/batch throttle.

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

Only the **Criterion micro-benchmark** layer belongs in CI — it compiles and
smoke-samples the Rust hot paths (a compile/panic check, not a regression gate).
The cross-tool E2E harness is **run manually**: it needs four live database
engines and per-engine vendor drivers, so it is not a CI job — reproduce it from
[`docs/bench/README.md`](../bench/README.md) and publish
[`docs/bench/report.html`](../bench/report.html).

To add a micro-bench regression gate in the future, save a Criterion `--baseline`
from a release tag and add a comparison step to `ci.yml`.

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
- [`dev/bench/smoke.py`](../../dev/bench/smoke.py) + [`docs/bench/matrix.yaml`](../bench/matrix.yaml) — the unified cross-tool / cross-engine harness
- [`docs/bench/report.html`](../bench/report.html) — the rendered results
- [`benches/`](../../benches/) — Criterion micro-benchmark sources (Rust hot paths, separate layer)
