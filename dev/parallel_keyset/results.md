# Parallel keyset — prototype + bench (MySQL)

Branch `feat/parallel-keyset`. Question: can keyset (seek) pagination — today
**sequential** because each page's `WHERE key > last` depends on the previous
page's max key — be parallelised **without losing its sparse-key immunity**?

Reproduce: `dev/parallel_keyset/fixture.sh` (1M-row skewed-key MySQL table),
then `cargo run --release --example parallel_keyset_probe -- <url> big id <N> 10000`.

## Fixture (the sparse footgun, made concrete)

1,000,000 rows, `id` BIGINT PK, **skewed**: 95% dense in `[1..950k]`, 5% sparse
out to ~501M. `span_per_row = 501` (key span 501× the row count) — exactly the
shape where value-based chunking blows up.

## Finding 1 — the split MUST be row-balanced, not value-balanced

For N=4:

| split | id boundaries | rows per worker | skew |
|---|---|---|---|
| **VALUE** (`min..max/4`, what range-chunk does) | 125M / 250M / 375M | **962425 / 12525 / 12525 / 12525** | **77×** |
| **ROW-percentile** (id at 25/50/75% of *rows*) | 250001 / 500001 / 750001 | 250001 / 250000 / 250000 / 249999 | **1.00×** |

A value split hands worker 1 **96% of the rows** → no parallelism. The
row-percentile split is perfectly balanced. This is the whole point: keyset is
valuable *because* it pages by rows, immune to a sparse/gappy key — a parallel
keyset must preserve that by splitting on **row rank**, not key value.

## Finding 2 — real wall-clock speedup, row-count parity held

`page = 10000`, local single-container MySQL 8.4:

| N | per-worker rows | skew | sequential | parallel | speedup |
|---|---|---|---|---|---|
| 2 | 500k / 500k | 1.00× | 1716 ms | 1061 ms | **1.62×** |
| 4 | 250k × 4 | 1.00× | 1486 ms | 473 ms | **3.14×** |
| 8 | 125k × 8 | 1.00× | 1458 ms | 447 ms | **3.26×** |

Parallel reads **every** row (1,000,000 = 1,000,000, asserted). Speedup is
near-linear to N=4, then plateaus — a single local container saturates ~4
concurrent index scans (CPU/IO of one mysqld). A real server (more cores/IO) or
a high-latency link — the field-run's 520K rows over an SSM tunnel took 31 min,
round-trip-bound — is where N workers amortising round-trips wins most.

## Finding 3 — boundary sampling: OFFSET is cheap here, NTILE is not

Computing the 3 row-percentile boundaries (N=4), excluded from the table above:

| method | cost |
|---|---|
| `... ORDER BY id LIMIT 1 OFFSET k` ×3 (index-only skip) | **110 ms** |
| `NTILE(4) OVER (ORDER BY id)` (full window sort) | 3210 ms |

OFFSET is an index-only scan — ~110 ms for 1.5M skipped entries. Net N=4 speedup
*including* boundary cost: `1486 / (473 + 110) = 2.55×`. NTILE is 30× worse —
rejected. **Caveat:** OFFSET is O(offset); on a billion-row table `OFFSET 500M`
is expensive → production should SAMPLE (`TABLESAMPLE` / a sampled histogram)
rather than OFFSET. This probe used OFFSET for prototype simplicity.

## Design implications (for the real implementation)

- Split on **row rank** (percentile), never key value — `KeysetPlan` gains a
  `parallel: usize` + a boundary list; the planner samples boundaries.
- Boundary sampling must be **cheap and reader-independent** per engine: OFFSET
  is fine to ~10M rows; beyond that, sample the key column (`TABLESAMPLE` on
  PG/MSSQL, a sampled scan on MySQL) and take quantiles. Bench the crossover.
- Each worker is the existing keyset seek loop, bounded `WHERE key > cursor AND
  key <= hi` — reuse `chunked/parallel_checkpoint.rs`'s worker/thread-scope +
  per-worker checkpoint + run-unique part names.
- Row-count parity is the load-bearing invariant — every union of ranges must
  read the whole table exactly once (the probe asserts it; the live test must too).
