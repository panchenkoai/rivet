# Run Rivet on Apache Airflow

Rivet extracts your tables; Airflow schedules and watches them. This recipe wires
the two together so Airflow's graph **is** Rivet's extraction plan — heavy tables
isolated, light ones parallelised, a barrier between waves — with per-table
retries, logs, and alerting for free.

There are two levels. Start at level 1; graduate to level 2 when you want
per-table visibility in the Airflow UI.

---

## Level 1 — one task (5 minutes)

`rivet apply` already runs a config's exports **wave by wave**, lowest first, with
a barrier between waves. So the simplest possible DAG is a single command:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="rivet_apply",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rivet"],
) as dag:
    BashOperator(
        task_id="rivet_apply",
        bash_command="rivet apply /opt/rivet/rivet.yaml --resume",
    )
```

`--resume` makes a re-run after a partial failure skip already-finished tables and
continue incomplete chunked ones from their checkpoints — so an Airflow retry is
safe and doesn't redo finished work. One task, Rivet does the wave ordering
internally. You lose per-table visibility in the UI; that's what level 2 adds.

---

## Level 2 — wave-aware DAG (per-table tasks)

[`rivet_waves_dag.py`](rivet_waves_dag.py) reads Rivet's plan and builds one
TaskGroup per wave, one task per table, with a barrier between waves:

```
wave_1  ──>  wave_2  ──>  wave_3  ──────────>  wave_4
[orders]     [bench_json]  [bench_wide,         [bench_narrow,
                            bench_decimal]        content_items]   ← very_high cost, isolated
```

Each box is `rivet run --config rivet.yaml --export <table> --reconcile` — a single
table with a row-count audit (source `COUNT(*)` vs exported rows) that fails the
task on drift. Tables **within** a wave run in parallel; wave N+1 starts only after
every task in wave N succeeds.

This graph is generated, not hand-written — it comes straight from the planner.

### Setup

1. **Generate the plan artifact** (the DAG reads this at parse time, so the
   scheduler never touches your database just to build the graph):

   ```bash
   rivet plan --config rivet.yaml --format json > docs/recipes/airflow/plan.json
   ```

   Commit `plan.json`. Regenerate + re-commit it whenever your table set or sizes
   change materially — the DAG re-shapes itself on the next parse. (A sample is
   checked in here so the DAG parses out of the box.)

2. **Drop the files** into your Airflow `dags/` folder (or point `DAG_FOLDER` at
   this directory): `rivet_waves_dag.py` + `plan.json`.

3. **Give the workers Rivet + credentials.** The binary must be on `PATH` (or use
   the Docker variant below). Source/destination secrets come from the task env —
   never hard-code them in the DAG. For example, export a Postgres URL from an
   Airflow Connection:

   ```python
   # in the DAG, or via the worker environment
   env={"RIVET_PG_URL": "{{ conn.rivet_pg.get_uri() }}"}
   ```

   and reference it in your `rivet.yaml` as `source: { type: postgres, url_env: RIVET_PG_URL }`.

### Knobs (env or Airflow Variables)

| Variable        | Default               | Meaning                          |
|-----------------|-----------------------|----------------------------------|
| `RIVET_BIN`     | `rivet`               | Path to the binary on the worker |
| `RIVET_CONFIG`  | `/opt/rivet/rivet.yaml` | Config the tasks run against     |

---

## Running Rivet in a container (DockerOperator)

If your workers don't have the binary, run the published image instead. Swap the
`BashOperator` for:

```python
from airflow.providers.docker.operators.docker import DockerOperator

DockerOperator(
    task_id=name,
    image="ghcr.io/panchenkoai/rivet:latest",
    command=f"run --config /work/rivet.yaml --export {name} --reconcile",
    mounts=[...],          # mount your config + a writable output/state dir
    environment={"RIVET_PG_URL": "{{ conn.rivet_pg.get_uri() }}"},
    auto_remove=True,
)
```

Mount the config and the state/output directories so checkpoints survive across
retries (Rivet keeps resumable state in `.rivet_state.db` next to the config).

---

## What a task logs

Each `rivet run` task ends with a single, parseable summary line — the shape your
Airflow logs and alerts key off:

```
✓ orders         incremental  250,000 rows  1 files  3.4 MB  5.8s  RSS 59 MB
```

rows written · file parts · bytes · wall time · peak memory. A failing reconcile
(or any error) exits non-zero with a stable `[RIVET_*]` code on the line, so the
task fails loudly and the wave barrier stops everything downstream — exactly what
you want before a half-extracted table reaches a warehouse load.

---

## Why this shape

Rivet's planner is deliberately **advisory** ([ADR-0006](../../adr/0006-source-aware-prioritization.md)):
it scores and groups, it does not schedule. That's what lets a real scheduler own
execution — retries, backfills, SLAs, alerting — while Rivet owns *source safety*
(which tables to defer, which to isolate, how hard to push the database). This
recipe is the seam between the two: the planner's `waves` become Airflow's graph,
and nothing about your database or its credentials leaves your environment.
