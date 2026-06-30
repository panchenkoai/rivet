"""Rivet wave-ordered extraction as an Airflow DAG.

`rivet plan` scores every export by size / cursor quality / risk and groups them
into **waves** — small, well-shaped tables in an early wave (run together), heavy
tables deferred and isolated into later waves so they don't pile load onto the
source at once (see ADR-0006). Rivet's planner is *advisory*: it recommends the
waves; an external scheduler executes them. This DAG is that scheduler.

It turns the planner's recommendation into an Airflow graph:

    wave_1  ──>  wave_2  ──>  wave_3  ──>  wave_4
    [orders]     [json]       [wide,        [narrow,
                               decimal]       content_items]   <- heavy, isolated

* a **TaskGroup per wave**, with a **barrier between waves** (wave N+1 starts only
  after every task in wave N succeeds — the same guarantee `rivet apply` gives);
* **one task per export inside a wave**, so the tables in a wave run in parallel
  and each gets its own Airflow retries, logs, and alerting;
* each task is `rivet run --export <name> --reconcile` — a single table, with a
  row-count audit (source COUNT(*) vs exported rows) that fails the task on drift.

The wave layout is read at parse time from a **committed plan artifact**
(`plan.json`), so the scheduler never touches the database just to build the DAG.
Regenerate it whenever the table set or sizes change materially:

    rivet plan --config rivet.yaml --format json > docs/recipes/airflow/plan.json

Then commit the result. The DAG re-shapes itself on the next parse.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# ── Configuration (override via Airflow Variables or the process env) ──────────
# Path to the rivet binary on the worker (or use the DockerOperator variant — see
# README). Credentials (RIVET_PG_URL, cloud keys) come from an Airflow Connection
# / Variable exported into the task env, NOT hard-coded here.
RIVET_BIN = os.environ.get("RIVET_BIN", "rivet")
RIVET_CONFIG = os.environ.get("RIVET_CONFIG", "/opt/rivet/rivet.yaml")
PLAN_PATH = Path(__file__).with_name("plan.json")

# ── Parse the committed plan once, at DAG-parse time (no DB hit) ───────────────
_plan = json.loads(PLAN_PATH.read_text())
# `rivet plan --format json` emits one object per export; every object carries the
# same campaign recommendation, so the first one is enough.
_campaign = _plan[0]["prioritization"]["campaign"]
_waves = _campaign["waves"]
_meta = {e["export_name"]: e for e in _campaign["ordered_exports"]}


def _run_cmd(export_name: str) -> str:
    # `run` a single export with a row-count reconcile gate. Quote the name so a
    # table with odd characters can't break out of the command.
    return f"{RIVET_BIN} run --config {RIVET_CONFIG} --export {export_name!r} --reconcile"


with DAG(
    dag_id="rivet_waves",
    description="Wave-ordered Rivet extraction (planner-recommended, source-safe).",
    schedule="0 2 * * *",  # nightly 02:00 — adjust to your freshness needs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=8,  # cap fan-out within a wave; the planner already isolates heavy ones
    tags=["rivet", "extract", "elt"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:
    previous_wave = None
    for wave in _waves:
        with TaskGroup(group_id=f"wave_{wave['wave']}") as wave_group:
            for name in wave["exports"]:
                meta = _meta.get(name, {})
                BashOperator(
                    task_id=name,
                    bash_command=_run_cmd(name),
                    # Surface the planner's reasoning in the Airflow UI tooltip.
                    doc_md=(
                        f"**{name}** — cost: `{meta.get('cost_class', '?')}`, "
                        f"risk: `{meta.get('risk_class', '?')}`, "
                        f"priority: `{meta.get('priority_class', '?')}`"
                        + ("  \n_isolated on source_" if meta.get("isolate_on_source") else "")
                    ),
                )
        if previous_wave is not None:
            previous_wave >> wave_group  # barrier: this wave waits for the previous one
        previous_wave = wave_group
