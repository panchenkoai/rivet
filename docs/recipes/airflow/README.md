# Run Rivet on Apache Airflow

Rivet extracts your tables; Airflow schedules and watches them. This recipe makes
Airflow's graph **be** Rivet's extraction plan — small tables parallelised, heavy
tables run one at a time, a barrier between waves — with per-table retries, logs,
and alerting for free, and a real `rivet` binary doing the work.

```
docs/recipes/airflow/
├── Dockerfile                 # apache/airflow + the rivet release binary baked in
├── docker-compose.2.10.yaml   # official Airflow 2.10 stack, adapted
├── docker-compose.3.0.yaml    # official Airflow 3.0 stack, adapted
└── dags/
    ├── rivet_waves_dag.py      # the DAG — builds the wave graph from plan.json
    ├── rivet.yaml              # the config you edit  ← this is yours
    └── plan.json               # rivet plan output the DAG reads (auto-refreshed)
```

---

## Try it locally

These are the **official** Apache Airflow docker-compose stacks (Postgres metadata
+ Redis + CeleryExecutor — not SQLite/Sequential), adapted three ways: example
DAGs are off, the worker image has `rivet` baked in, and a dedicated
TLS-enabled Postgres holds Rivet's durable run state.

```bash
cd docs/recipes/airflow

docker compose -f docker-compose.2.10.yaml up --build      # Airflow 2.10.5
#   …or
docker compose -f docker-compose.3.0.yaml up --build       # Airflow 3.0.3
```

First boot builds the image and migrates the metadata DB (~2-3 min). Then open
<http://localhost:8080> (login **`airflow`** / **`airflow`**), un-pause and trigger
the **`rivet_waves`** DAG, and open **Graph**:

```
plan ──> wave_2 ──────────> wave_3 ───────────────> wave_4
         [11 small tables    [bench_decimal →        [bench_narrow →
          in parallel]        bench_hc →              content_items]
                              orders → bench_wide]    (one at a time)
```

The demo runs against the project's Postgres fixture on the host
(`host.docker.internal:5432`), so `docker compose up` in the rivet repo first.
`docker compose -f … down -v` tears everything down (`-v` drops the state too).

The same DAG runs on both majors — it imports `BashOperator` from the bundled
`standard` provider on Airflow 3.x and from core on 2.x.

---

## What the graph does

**`plan` → waves.** `rivet plan` scores every export (size, cursor quality, chunk
geometry, risk) and groups them into waves. The DAG runs the waves lowest-first
with a **barrier** between them — wave N+1 starts only after every task in wave N
succeeds.

**Cheap parallel, heavy serial — within a wave.** This is the part that matters:
only the cheap exports (planner `cost_class: low`) run in parallel. The heavier
ones run **one at a time** (a sequential chain). The whole reason the planner
defers big tables into a late wave is to *not* pile several large scans onto the
source at once — so running them in parallel would defeat the point. (Same split
as the in-engine `rivet apply --parallel-export-processes`.)

**Each task is a real `rivet run`.** A wave task is `rivet run --config rivet.yaml
--export <table>` — a single table, with `--reconcile` (source `COUNT(*)` vs
exported rows) on full/chunked exports. The task log is the actual rivet output:

```
✓ orders         incremental  250,000 rows  1 files  3.4 MB  2.7s  RSS 64 MB
```

A failing reconcile (or any error) exits non-zero with a stable `[RIVET_*]` code,
so the task fails loudly and the wave barrier stops everything downstream — before
a half-extracted table reaches a warehouse load.

---

## Config → plan → graph (no manual steps)

The graph is generated from Rivet's planner, and the `plan` task keeps it fresh:

1. **`dags/rivet.yaml`** is the config you edit. `rivet init --source <url>`
   scaffolds one from your live schema (use `--exclude '<glob>'` to drop test /
   junk tables); the checked-in sample is the PG fixture trimmed to a clean set.
2. The DAG's first task, **`plan`**, runs `rivet plan --format json` and
   **atomically** rewrites `dags/plan.json` (temp file, swapped in only if rivet
   succeeded *and* the output is valid JSON — a failed plan can't truncate the
   graph source and break the DAG).
3. The DAG reads `plan.json` at **parse time** to lay out the waves. So editing
   the config and re-running the DAG re-shapes the graph on the next parse — no
   hand-run CLI, no committing `plan.json` by yourself. The checked-in sample
   makes the DAG work on the very first boot.

### Skip tables — you don't have to extract everything

Set an env var on the workers (or in the compose `environment:`):

| Variable        | Effect                                                            |
|-----------------|------------------------------------------------------------------|
| `RIVET_EXCLUDE` | Comma-separated tables to drop. A wave left empty disappears.     |
| `RIVET_ONLY`    | Comma-separated allow-list — run only these.                     |

---

## Architecture

- **`rivet` binary** — baked into the worker image from the published release
  ([`Dockerfile`](Dockerfile), arch-aware: pulls the `x86_64` or `aarch64` linux
  build). To pin a version, set `RIVET_VERSION` in the compose `build.args`.
- **Durable state** — `rivet-state-db`, a dedicated TLS Postgres service.
  `RIVET_STATE_URL` points the workers at it with `sslmode=require`. SQLite state
  in a bind-mount corrupts under parallel writers; Rivet refuses to send state
  credentials in cleartext to a non-loopback host (CWE-319), so the service speaks
  TLS (a self-signed cert generated at startup — fine for an in-cluster service).
- **Source credentials** — `RIVET_PG_URL` in the compose env for the demo. In
  production, put the URL in an Airflow Connection / Variable and export it into
  the task env, matching `source: { url_env: RIVET_PG_URL }` in the config —
  never inline it in the DAG. The demo opts into plaintext to the fixture with
  `source.tls: { mode: disable }`; a real remote DB uses `mode: verify-full`.

---

## Why this shape

Rivet's planner is deliberately **advisory** ([ADR-0006](../../adr/0006-source-aware-prioritization.md)):
it scores and groups, it does not schedule. That's what lets a real scheduler own
execution — retries, backfills, SLAs, alerting — while Rivet owns *source safety*
(which tables to defer, which to run serially, how hard to push the database).
This recipe is the seam between the two: the planner's `waves` and cost classes
become Airflow's graph, and nothing about your database or its credentials leaves
your environment.
