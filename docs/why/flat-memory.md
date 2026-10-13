# Flat memory at any scale

Rivet's memory is bounded by how much you buffer before a flush — **not** by how
big the table is. It streams rows into Arrow batches, and every time a batch
fills it is written to a Parquet part and dropped. From a million rows on, peak
resident memory does not depend on the table's size in `mode: full` and
`mode: incremental` once the Parquet row group is capped
(`parquet.target_row_group_mb`, or fixed-row groups); in `mode: chunked` it is
set by the page (`chunk_size`), not by the table.

What is buffered decides the level:

- **One open Parquet row group.** With the 128 MB group `rivet init` writes, a
  narrow table buffers one group — about 5 million rows of a three-column
  table — before memory levels off.
- **One page in `mode: chunked`.** Each page is its own part. `rivet init` picks
  a larger page for a larger table (250,000 rows from 1 to 10 million,
  1,000,000 up to 100 million), so the level follows the page.
- **A table too small to fill one batch uses less**: 22–29 MiB, the floor of
  the process.

Measured peak RSS in MiB at 10 thousand / 1 million / 5 million / 20 million
rows of a three-column table (two integers and a timestamp), release build,
one run per cell:

| Engine | `full`, init default | `full`, `target_row_group_mb: 16` | `chunked`, init default | `incremental`, init default |
|---|---|---|---|---|
| PostgreSQL | 23 / 50 / 92 / 106 | 23 / 53 / 54 / 54 | 22 / 55 / 53 / 54 | 23 / 48 / 86 / 84 |
| MySQL | 23 / 74 / 130 / 146 | 24 / 82 / 88 / 87 | 24 / 58 / 73 / 78 | 23 / 74 / 121 / 128 |
| SQL Server | 28 / 91 / 144 / – | 29 / 97 / 102 / – | 29 / 87 / 92 / – | 29 / 91 / 134 / – |
| Oracle | 24 / 102 / 142 / – | 23 / 99 / 106 / – | 24 / 92 / 94 / – | 23 / 102 / 132 / – |
| MongoDB | 26 / 109 / 141 / – | 26 / 100 / 121 / – | – | – |

Fixed 100,000-row groups level `full` off the same way (PostgreSQL 42 / 42,
MySQL 76 / 75 / 79 from 1 million rows on). On this narrow table the batch caps
(`batch_size_memory_mb`, `max_batch_memory_mb`, `memory_threshold_mb`) changed
nothing: one batch is 3 MB. MongoDB `mode: full` still grew from 1 million to
5 million rows under every setting tried; the cause is not known.

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
