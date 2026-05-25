# Soak / load regression matrix

Fifth matrix in the family. Where path_matrix pins **layout and row
accounting**, this one pins **order-of-magnitude perf and memory**
regressions on a 10_000-row PG table.

## Why

A refactor can keep exit codes, messages, layout, and row counts green
while making exports 50× slower or 10× more memory-hungry. Soak scenarios
capture `duration_ms`, `peak_rss_mb`, and `total_rows` from `summary.json`
and compare against generous per-scenario thresholds.

## Scenarios

| id | mode | what it pins |
|---|---|---|
| s01 | full | baseline full export throughput + RSS |
| s02 | chunked | chunked export with chunk_size=1000 |
| s03 | incremental | cursor-based incremental export |

## Running

```bash
docker compose up -d postgres
cargo build --bin rivet --release
bash dev/matrices/setup_links.sh   # if not done yet
bash dev/soak_matrix/gen_fixtures.sh
bash dev/matrices/_common/seed_pa_soak.sh
cp target/release/rivet dev/soak_matrix/rivet
cd dev/soak_matrix && ./matrix.sh
```

Or via the orchestrator:

```bash
dev/matrices/run.sh --matrix=soak --build
```

## Thresholds

Per-scenario files in `expected/<id>.thresholds`:

```
total_rows_min=10000
duration_ms_max=5000
peak_rss_mb_max=200
```

Thresholds are intentionally generous (2–3× over a healthy local run) to
avoid CI flapping. They catch order-of-magnitude regressions, not micro-tuning.

## Updating baselines

When a perf change is intentional:

1. Run `./matrix.sh` — NEW scenarios print suggested metrics.
2. Copy `logs/<id>/metrics` values into `expected/<id>.thresholds`.
3. Document in CHANGELOG.
