# Postgres benchmark — 6 tools × 22 tables

Wall / RSS / output measured via `gtime -v`; DB signals from `pg_stat_database`
+ `pg_stat_bgwriter` + `pg_stat_io` deltas plus `pg_stat_activity` sampler @ 50 ms.
All five non-Rivet tools run with their defaults; Rivet uses `table:` shortcut,
`environment: local`, `mode: chunked` with `chunk_size_memory_mb: 256` and the
auto-PK + work_mem-aware FETCH cap.

> **The 22-table aggregate below is the original multi-table run.** The
> **2026-05-31 re-run** (rivet 0.7.9; tool versions in
> [`REPORT_steelman.md`](REPORT_steelman.md)) re-measured the hardest table,
> `content_items` (2 M × 20 wide cols), and added the DBA-harm signals
> (longest *open transaction*, xmin-horizon age, write-pressure dead tuples)
> and a `parallel:` scaling sweep — both new sections are at the bottom of this
> file.

## Total wall, CPU, RSS, output (22 tables)

| Tool | Wall (s) | User CPU (s) | Peak RSS | Output | Failures |
|---|---:|---:|---:|---:|---:|
| **rivet** | 194.5 | 32.2 | 443 MB | 2.1GB | 0 |
| **sling** | 191.1 | 132.4 | 6004 MB | 2.8GB | 0 |
| **dlt** | 323.4 | 150.1 | 1370 MB | 2.2GB | 1 |
| **duckdb** | 149.8 | 30.8 | 18910 MB | 2.2GB | 1 |
| **clickhouse** | 145.1 | 44.3 | 1639 MB | 2.2GB | 0 |
| **odbc2parquet** | 187.6 | 70.3 | 29061 MB | 972.2MB | 1 |

## DB-side signals — content_items (2 M × 20 wide cols)

| Tool | Longest single query | Peak active backends | xact_commit | temp_bytes | temp_files |
|---|---:|---:|---:|---:|---:|
| rivet | 0.19s | 1 | 4699 | 0 | 0 |
| sling | 134.05s | 1 | 1929 | 0 | 0 |
| dlt | 1.20s | 1 | 3759 | 3.2GB | 200 |
| duckdb | 70.63s | 12 | 692 | 0 | 0 |
| clickhouse | 132.86s | 1 | 1913 | 0 | 0 |
| odbc2parquet | 136.26s | 1 | 2636 | 0 | 0 |

## Sample SQL templates observed (content_items)

### `rivet`
- `FETCH 142 FROM _rive`
- `FETCH 500 FROM _rive`
- `SELECT COUNT(*) FROM public.content_items`
- `SELECT column_name::text, data_type::text, numeric_precision, numeric_scale FROM`

### `sling`
- `select * from "public"."content_items"`

### `dlt`
- `FETCH FORWARD 10000 FROM "c_10df2cb50_1"`
- `FETCH FORWARD 9999 FROM "c_10df2cb50_1"`

### `duckdb`
- `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;`

### `clickhouse`
- `COPY (SELECT "id", "title", "body", "raw_html", "metadata", "tags", "author_name`

### `odbc2parquet`
- `SELECT * FROM public."content_items"`

---

## DBA-harm signals — content_items (2026-05-31 re-run, rivet 0.7.9)

The `longest single query` above is a *proxy* for source pressure. These three
measure the harm directly (sampler @ 50 ms, with the Tier-2 write-pressure probe
on — a concurrent `UPDATE` loop whose un-reclaimable dead tuples are counted):

- **longest open txn** — how long one snapshot stays open (a single big
  `SELECT *` holds it for the whole run; short per-chunk FETCHes do not).
- **max xmin age** — how far back (transactions) the oldest `backend_xmin` pins
  the cleanup horizon. While held, `VACUUM` cannot reclaim newer dead tuples.
- **dead tuples stranded** — dead rows the concurrent writer produced that
  `VACUUM` could *not* reclaim because the extractor's snapshot was still open.

| Tool | longest open txn | max xmin age | dead tuples stranded |
|---|---:|---:|---:|
| **rivet** | **12.3 s** | **274** | **0** |
| clickhouse | 87.9 s | 1 975 | 1 383 |
| duckdb | 97.6 s | 737 | 0 |
| dlt | 215.3 s | 8 388 | 4 843 |
| sling | DNF (one `SELECT *` → 23 GB → swap) | — | — |
| odbc2parquet | DNF (killed at 10 min cap) | — | — |

**Rivet holds its snapshot 12.3 s; every other completing tool holds one open
for 88–215 s** — 7–17× longer — because Rivet fetches in short per-chunk
transactions while the rest run one long `SELECT *`. dlt is worst: 215 s open,
horizon pinned 8 388 transactions back, **4 843 dead tuples left un-reclaimable**
on the source during the run. This is the axis a DBA on a busy primary feels
first, and it is Rivet's widest margin in the suite.

---

## Parallel scaling — `parallel: N` (2026-05-31 re-run, rivet 0.7.9, 10-core host)

Rivet's chunk-level parallelism (separate connection per worker) is off by
default (`parallel: 1`) but `rivet init` scaffolds a row-count-scaled value
(`suggest_parallel`: <500 K → 1, <5 M → 2, ≥5 M → 4). Measured speedup on two
shapes, same `chunk_size` per shape:

**content_items (2 M, WIDE 20 cols):**

| parallel | wall | speedup | peak RSS |
|---:|---:|---:|---:|
| 1 | 2:11 | 1.00× | 245 MB |
| 2 | 1:36 | **1.37×** | 410 MB |
| 4 | 1:33 | 1.41× | 656 MB |

**page_views (2 M, NARROW):**

| parallel | wall | speedup | peak RSS |
|---:|---:|---:|---:|
| 1 | 13.6 s | 1.00× | 66 MB |
| 2 | 4.7 s | 2.90× | 115 MB |
| 4 | 3.2 s | **4.19×** | 211 MB |

**Two findings:** (1) **wide tables plateau at `parallel: 2`** (thick rows
saturate the wire fast — 2→4 buys 4 % for +245 MB); **narrow tables scale to 4+**
(near-linear, thin rows stay round-trip-bound). The `init` heuristic's
row-count bands track this well. (2) Head-to-head on content_items, **rivet
`parallel: 2` (1:36, 410 MB) beats duckdb default (1:44, 5 305 MB)** — faster
*and* 13× less memory. The default-config 22-table run above measured rivet at
`parallel: 1`, its slowest setting.
