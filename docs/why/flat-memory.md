# Flat memory at any scale

Rivet's memory is bounded by how much you buffer before a flush — **not** by how
big the table is. It streams rows into Arrow batches, and every time a batch
fills it is written to a Parquet part and dropped. A 10-thousand-row table and a
500-million-row table run at the same resident set size.

## The number that matters

In the cross-tool benchmark, peak resident memory was:

| Tool           | Peak RSS |
|----------------|---------:|
| **rivet**      | **57 MB** |
| sling          |   129 MB |
| clickhouse-local |   820 MB |
| dlt            | 1 735 MB |
| duckdb         | 2 067 MB |
| odbc2parquet   | 3 579 MB |

Rivet is 2× to 63× smaller than the field — and crucially, that 57 MB is *flat*.
The tools that buffer the result set (or materialise it in an embedded engine)
scale their memory with the data; Rivet does not.

## Proven at production scale

The benchmark table is small enough to fit in RAM, which is exactly why the
flat-memory property is invisible there for the buffering tools. It stops being
invisible on a real table. A field run pulled a **454-million-row** table to
Parquet in about **two hours with flat memory and no OOM** — the same table on
which Airbyte OOM'd. The mechanism the benchmark measures (bounded Arrow batches
→ Parquet parts) is the mechanism that survives at 454 M rows.

This is what lets Rivet run on a 512 MB–4 GB host, in a small Kubernetes Job, or
alongside other work on a shared box. See
[low-memory runners](../best-practices/low-memory-runners.md) for the exact
settings and the RSS budget formula.

## CDC memory is bounded by `rollover`, not the backlog

The same property holds for change-data-capture. A CDC drain's peak RSS is
O(`rollover`) — the part-size at which it flushes and checkpoints — not
O(backlog). A soak test grows the drain *interval* 12× (10 → 120 minutes of
accumulated changes) and peak RSS stays flat: the run reads, flushes at
`rollover`, checkpoints, and acks in a loop, so a larger backlog just means more
loops, not more memory. The harness self-asserts both the flat RSS and that
every churned row was captured; details in the
[performance ledger](https://github.com/panchenkoai/rivet/blob/main/docs/perf-matrix.yaml).
