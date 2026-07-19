# CDC test stand

One command to stand up the four CDC engines and run everything we guard about
change-data-capture: correctness scenarios, capture throughput, and steady-state
memory + completeness. The stand IS the shared `cdc` docker profile
(`postgres-cdc:5434` / `mysql-cdc:3307` / `mssql-cdc:1434` / `mongo-rs:27018`),
whose ports are isolated from the batch stack (5432/3306/1433/27017) so a CDC run
never contends with batch/smoke work.

```sh
dev/cdc/stand.sh up          # bring the cdc profile up, create the mssql `rivet` DB, verify readiness
dev/cdc/stand.sh verify      # re-check each engine is CDC-ready
dev/cdc/stand.sh scenarios   # the live_cdc scenario suite (serial — shared DBs race in parallel)
dev/cdc/stand.sh perf        # capture-throughput matrix, all 4 engines (dev/cdc/perf.sh)
dev/cdc/stand.sh soak pg     # steady-state memory + completeness soak (dev/cdc_interval/soak_all.sh)
dev/cdc/stand.sh standby     # opt-in primary+replica pair; bounded-CDC-on-a-standby fails loud
dev/cdc/stand.sh all         # up + verify + scenarios + perf
dev/cdc/stand.sh down        # stop the cdc profile
```

Prereqs: a release `rivet` for representative perf/soak numbers
(`cargo build --release --bin rivet`); `duckdb` on PATH for the soak/perf
completeness re-reads. `mssql-cdc` has no init hook, so `up` creates the `rivet`
database itself. `up` also starts `minio` + `fake-gcs` — a couple of scenarios
capture CDC straight to a cloud destination and skip loudly without them.

## What the three layers check

- **`scenarios`** — the `live_cdc*` suite, one test per (scenario × engine) cell
  of `docs/cdc-matrix.yaml`. Correctness: capture, resume, defer-not-drop,
  crash-atomicity, open-bound termination. Run serial — the tests share the CDC
  DBs and race under parallelism.
- **`perf`** — bulk capture throughput: seed a backlog, one bounded
  `until_current` drain, measure wall / rows / rows-per-second / peak RSS. This
  is the BULK profile; RSS reflects the whole backlog buffered before the roll.
- **`soak`** — steady-state: continuous churn drained at growing intervals
  (10/20/30/60/120 min), asserting peak RSS stays FLAT as the per-interval
  backlog grows 12× (the O(rollover) contract) and every row is captured. This
  is where an O(backlog) memory regression or a slow leak shows up.

## Coverage map (`docs/cdc-matrix.yaml`, drift-guarded, 0 gaps)

21 scenarios × 4 engines. "gaps=0" means every cell is a test OR a justified
`na` — it does NOT mean every bug is caught (two adversarial reviews found six
real bugs under a green matrix). Load-bearing termination is PostgreSQL +
MongoDB (re-reading slot / tailable stream); MySQL + SQL Server terminate
natively, so their open-time bound is a precise-stop refinement (each verified
by a disable-bound probe).

| Scenario | PG | MySQL | MSSQL | Mongo |
|---|---|---|---|---|
| initial snapshot → stream | ✓ | ✓ | ✓ | ✓ |
| insert / update / delete capture | ✓ | ✓ | ✓ | ✓ |
| resume only-new / idle-first-run | ✓ | ✓ | ✓ | ✓ |
| crash at-least-once | ✓ | ✓ | ✓ | ✓ |
| schema drift | ✓ | na | ✓ | na |
| whole-db multiplex | ✓ | ✓ | ✓ | ✓ |
| `__seq` total order | ✓ | ✓ | ✓ | na |
| until_current bounded (termination) | ✓ (load-bearing) | ✓ | ✓ | ✓ (load-bearing) |
| open-bound two-run defer-not-drop | ✓ | ✓ | ✓ | na (joint) |
| NDJSON bounded | ✓ | na | na | na |
| empty-transaction churn | ✓ | na | na | na |
| reach open bound past a foreign span | ✓ | na | na | na |
| large-transaction atomic across crash | ✓ | ✓ | ✓ | na |
| type fidelity | ✓ | ✓ | ✓ | ✓ |
| silent-update captured (the CDC value prop) | ✓ | na | na | na |
| bounded-on-a-standby fails loud | ✓ | na | na | na |
| oversized-transaction bails loud (per-adapter cap) | ✓ | ✓ | ✓ | na |
| drain releases pinned WAL (slot harm) | ✓ | na | na | na |

## Honest thin spots (not matrix gaps, but real)

- **Daemon / `Continuous` path** — the re-drain loop and commit-marking also run
  when `until_current: false`; every test scopes `until_current`. Nobody has
  checked whether an unbounded PG stream spins extra ack+re-peek cycles.
- **Harm under load** — replication lag and co-running OLTP p99 have zero tests;
  slot WAL growth + `confirmed_flush` advance is now covered (drain-releases-
  pinned-WAL), but the *under-concurrent-load* harm still needs the smoke.py
  machinery ported to CDC.
- **Multi-pass crash** is proven on PG only; MySQL/Mongo have single-pass crash
  tests but not the re-drain multi-pass window.
- **Steady-state on Mongo** — `soak_all.sh mongo` now exists but has had less
  mileage than the SQL engines.
- **Mutation sensitivity** of the CDC integration code is largely invisible to
  `cargo mutants --lib` (it is live-covered) — that layer is closed by the live
  + soak + independent-oracle suites, not by `--lib`.
