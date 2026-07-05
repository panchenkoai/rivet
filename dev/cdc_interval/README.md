# CDC drain-interval experiment

**Question:** if you drain a CDC export *less often*, does anything degrade —
data loss, unbounded memory, or divergence — or is the drain interval a free
operational parameter?

**Answer (measured):** on MySQL the interval is bounded by binlog **retention**
(days), not by minutes. Drain wall-time grows linearly with the backlog;
memory stays flat (streaming rollover); every row converges. The interval is
free to stretch — the only cost accrues in the *number of output files*, not in
correctness or RSS.

## What it does

One multi-table CDC export over 10 tables spanning a heat spectrum, with a
continuous loader, verified at growing drain intervals:

| Class | Tables | Load |
|-------|--------|------|
| hot | 2 | ~50 rows/s inserts + tail updates + periodic **purge-deletes** |
| warm | 3 | ~10 rows/s |
| cold | 3 | ~0.6 rows/min |
| frozen | 2 | 1 row / 5 min (often **zero** events in a short interval) |

At each interval it runs the **drain-measure-drain** protocol from
[`../../docs/pilot/reconcile-runbook.md`](../../docs/pilot/reconcile-runbook.md):
pause the loader, drain to current, snapshot the source aggregates, drain again
(must return 0 — proves the source was quiesced), then per table assert:

- **convergence** — source `COUNT(*)` and `BIT_XOR` of a stored per-row hash
  equal the destination merge of `snapshot ∪ events`, winner per id by
  `(binlog_file, pos)` (the lexical `__pos` string is NOT a valid order across
  a binlog rotation — parse the file+pos);
- **conservation** — every id ever inserted (including rows later purged)
  appears as an insert event or in the snapshot.

## Run it

```bash
docker compose --profile cdc up -d mysql-cdc
cargo build --release --bin rivet
RIVET_BIN=target/release/rivet dev/cdc_interval/run.sh          # 10/20/30-min phases
# custom schedule (minutes reps, ';'-separated):
PHASES="10 2;20 2;30 2;60 1;120 1" dev/cdc_interval/run.sh
```

Needs `duckdb` on PATH (destination-side merge oracle). Everything lands under
`/tmp/rivet-cdc-interval/` (override with `WORK=`). `SKIP_SETUP=1` resumes
against existing tables/checkpoint/snapshots.

## Results — 2026-07-04, MySQL 8.0 → local parquet

Continuous ~230 rows/s across the spectrum (inserts + hot-table updates +
purge-deletes). Every interval: **10/10 tables converged, control drain 0**.

| Interval | Drain backlog (events) | Wall time | Drain RSS |
|---------:|-----------------------:|----------:|----------:|
| 10 min | 135,118 | 1 s | 76 MB |
| 10 min | 177,622 | 1 s | 79 MB |
| 20 min | 377,002 | 1 s | 79 MB |
| 20 min | 377,002 | 1 s | 77 MB |
| 30 min | 850,447¹ | 2 s | 86 MB |
| 30 min | 564,884 | 1 s | 78 MB |
| 60 min | 1,127,294 | 3 s | 80 MB |
| 120 min | 2,252,112 | 5 s | 82 MB |

¹ absorbed an orchestrator-restart gap — a larger backlog than a clean 30 min.

**Reading the curve:** drain time ≈ **2.2 s per million events** (~450k
events/s peak); RSS is flat 76–86 MB across a **17×** backlog range
(135k → 2.25M). Memory does not track the backlog — the rollover pipeline
streams parts out; the only in-memory bound is a single transaction
(documented `O(largest transaction)`).

**Interruption = an unplanned crash test, passed.** The run was killed at the
start of the 3-hour phase with an undrained hot-table tail. The post-kill
cross-engine reconcile (merge vs live source, `BIT_XOR` of row hashes) matched
on **all 10 tables** — no loss, no divergence. Stopping the extractor is a
delay, not a loss.

**Source cost during the whole run:** the mysql-cdc container held 0.4–1.5 %
CPU and steady ~824 MiB while carrying the loader's ~230 rows/s; the extractor
itself does not exist between drains (bounded mode). At the 4-hour mark: binlog
1,397 MB total, destination 219 MB parquet.

## Operational note — file count

The only quantity that grew with the interval is the **part count** (1,517 by
the 4-hour mark) — an artifact of the deliberately small `rollover: 50000` used
here to stress the part machinery. In production, size `rollover` to the
expected per-drain backlog for one file per active table per drain; a
too-small rollover on a multi-table stream also emits micro-parts for cold
tables (roll_all flushes all buffers at one consistent frontier — the price of
a single checkpoint). See `docs/reference/tuning.md`.
