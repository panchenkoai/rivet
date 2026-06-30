# Run Rivet on Apache Airflow

Rivet extracts your tables; Airflow schedules and watches them. This recipe makes
Airflow's graph **be** Rivet's extraction plan — small tables parallelised, heavy
tables run one at a time, a barrier between waves — with per-table retries, logs,
and alerting for free, and a real `rivet` binary doing the work. It builds **one
DAG per source database** (PostgreSQL, MySQL, SQL Server) from a single factory.

```
docs/recipes/airflow/
├── Dockerfile                 # apache/airflow + the rivet release binary baked in
├── docker-compose.2.10.yaml   # official Airflow 2.10 stack, adapted
├── docker-compose.3.0.yaml    # official Airflow 3.0 stack, adapted
└── dags/
    ├── rivet_waves_dag.py      # factory → rivet_waves_{postgres,mysql,mssql}
    ├── postgres.yaml / mysql.yaml / mssql.yaml      # the configs you edit  ← yours
    └── postgres.plan.json / mysql.plan.json / …     # rivet plan output (auto-refreshed)
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
<http://localhost:8080> (login **`airflow`** / **`airflow`**). There are three DAGs
— **`rivet_waves_postgres`**, **`rivet_waves_mysql`**, **`rivet_waves_mssql`** —
one per source. Un-pause and trigger one, and open **Graph**:

```
plan ──> wave_2 ──────────> wave_3 ───────────────> wave_4
         [small tables       [bench_decimal →        [bench_narrow →
          in parallel]        bench_hc →              content_items]
                              orders → bench_wide]    (one at a time)
```

The demo runs against the project's fixtures on the host (Postgres :5432, MySQL
:3306, SQL Server :1433), so `docker compose up` in the rivet repo first.
`docker compose -f … down -v` tears everything down (`-v` drops the state too).

Each DAG runs on both Airflow majors — it imports `BashOperator` from the bundled
`standard` provider on 3.x and from core on 2.x.

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

**Each task is a real `rivet run`.** A wave task is `rivet run --config
<source>.yaml --export <table>` — a single table, with `--reconcile` (source
`COUNT(*)` vs exported rows) on a fresh full/chunked export. The task log is the
actual rivet output:

```
✓ orders         incremental  250,000 rows  1 files  3.4 MB  2.7s  RSS 64 MB
```

A failing reconcile (or any error) exits non-zero with a stable `[RIVET_*]` code,
so the task fails loudly and the wave barrier stops everything downstream — before
a half-extracted table reaches a warehouse load.

---

## Config → plan → graph (no manual steps)

The graph is generated from Rivet's planner, and each DAG's `plan` task keeps it
fresh:

1. **`dags/<source>.yaml`** is the config you edit (`postgres.yaml` etc).
   `rivet init --source <url>` scaffolds one from your live schema (use
   `--exclude '<glob>'` to drop test / junk tables); the checked-in samples are
   the fixtures trimmed to a clean set.
2. The DAG's first task, **`plan`**, runs `rivet plan --format json` and
   **atomically** rewrites `<source>.plan.json` (temp file, swapped in only if
   rivet succeeded *and* the output is valid JSON — a failed plan can't truncate
   the graph source and break the DAG).
3. The DAG reads `<source>.plan.json` at **parse time** to lay out the waves. So
   editing the config and re-running the DAG re-shapes the graph on the next parse
   — no hand-run CLI, no committing the plan by yourself. The checked-in sample
   makes the DAG work on the very first boot.

Same-named tables across engines never collide: each source writes to its own
`./output/<engine>/<table>/` and keeps its own state database, so a `bench_hc` in
Postgres and one in MySQL don't share cursor / shape / file-log state.

### Skip tables — you don't have to extract everything

Set an env var on the workers (or in the compose `environment:`):

| Variable        | Effect                                                            |
|-----------------|------------------------------------------------------------------|
| `RIVET_EXCLUDE` | Comma-separated tables to drop. A wave left empty disappears.     |
| `RIVET_ONLY`    | Comma-separated allow-list — run only these.                     |

---

## Many sources → one bucket

For a shared S3 / GCS data lake, point each source's config at the **same bucket
with a per-source prefix** so same-named tables across engines never collide:

```yaml
# postgres.s3.yaml
destination:
  type: s3
  bucket: my-data-lake                 # ← one bucket for every source
  prefix: rivet/postgres/{export}/     # ← namespaced by source; {export} = table
# mysql.s3.yaml  → prefix: rivet/mysql/{export}/
# mssql.s3.yaml  → prefix: rivet/mssql/{export}/
```

A `bench_hc` then lands at `s3://my-data-lake/rivet/postgres/bench_hc/`, the MySQL
one at `…/rivet/mysql/bench_hc/` — separate objects. The namespace is **required**,
not cosmetic: different databases are different data, and the *same* logical table
yields different Arrow types per engine (`uuid_col` is `FixedSizeBinary(16)` on
PG/MSSQL but `Utf8` on MySQL; MySQL drops the timestamp's UTC tz) — merging them
under one prefix would write a dataset with incompatible schemas.

The checked-in `*.s3.yaml` / `postgres.gcs.yaml` configs build the cloud DAGs
(`rivet_waves_<source>_s3`, `rivet_waves_postgres_gcs`); the demo writes to the
project's MinIO / fake-gcs emulators (worker env supplies `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`). A real bucket drops the `endpoint` + `allow_anonymous`
and uses normal cloud credentials.

---

## Recovery — all from Airflow, no shell

A chunked export checkpoints per chunk, so a kill mid-chunk is recoverable. The
whole recovery story is operable from the Airflow UI:

| Situation | What to do |
|-----------|------------|
| **Transient crash mid-chunk** (worker killed, timeout, retry) | **Nothing — automatic.** The task tries a clean run; if rivet reports the checkpoint is still in progress, it `--resume`s from the last good chunk. (Keyed on the checkpoint *state*, not the retry count — idempotent: a fresh run, a mid-chunk crash, and a finished export each do the right thing.) |
| **Unresumable checkpoint** (chunk params changed, or you want a clean re-extract) | **Admin → Variables → `rivet_reset`** = comma-list of tables → trigger the DAG. Each listed table's checkpoint is wiped (`rivet state reset-chunks`) before its run. Clear the Variable afterwards. |
| **Anything else** | the normal task **Clear / re-trigger** in the UI. |

A resumed run writes only the chunks left after the crash, so the reconcile gate
is dropped on the resume path (a full-table `COUNT(*)` would false-mismatch the
remainder) — the same reasoning as skipping reconcile on incremental exports.

---

## Architecture

- **`rivet` binary** — baked into the worker image from the published release
  ([`Dockerfile`](Dockerfile), arch-aware: pulls the `x86_64` or `aarch64` linux
  build). To pin a version, set `RIVET_VERSION` in the compose `build.args`.
- **Durable state** — `rivet-state-db`, a dedicated TLS Postgres service, with a
  **separate database per source** (`rivet_state_postgres` / `_mysql` / `_mssql`);
  the factory passes each DAG its own `RIVET_STATE_URL` with `sslmode=require`.
  SQLite state in a bind-mount corrupts under parallel writers; Rivet refuses to
  send state credentials in cleartext to a non-loopback host (CWE-319), so the
  service speaks TLS (a self-signed cert generated at startup — fine in-cluster).
- **Source credentials** — `RIVET_PG_URL` / `RIVET_MY_URL` / `RIVET_MS_URL` in the
  compose env for the demo. In production, put each URL in an Airflow Connection /
  Variable and export it into the task env, matching `source: { url_env: … }` in
  the config — never inline it in the DAG. The demo opts into plaintext to the
  fixtures with `source.tls: { mode: disable }`; a real remote DB uses
  `mode: verify-full`.

---

## Why this shape

Rivet's planner is deliberately **advisory** ([ADR-0006](../../adr/0006-source-aware-prioritization.md)):
it scores and groups, it does not schedule. That's what lets a real scheduler own
execution — retries, backfills, SLAs, alerting — while Rivet owns *source safety*
(which tables to defer, which to run serially, how hard to push the database).
This recipe is the seam between the two: the planner's `waves` and cost classes
become Airflow's graph, and nothing about your database or its credentials leaves
your environment.
