"""Rivet wave-ordered extraction as an Airflow DAG.

`rivet plan` scores every export by size / cursor quality / risk and groups them
into **waves** — small tables in an early wave, heavy tables deferred into later
waves so they don't pile load onto the source at once (see ADR-0006). Rivet's
planner is *advisory*: it recommends; an external scheduler executes. This is it.

The graph:

    plan ──> wave_2 ──────────> wave_3 ──────────────> wave_4
             [small tables       [bench_decimal →       [bench_narrow →
              in parallel]        bench_hc → …]          content_items]

* a `plan` task that refreshes `plan.json` from the config (atomically), then a
  **TaskGroup per wave** with a **barrier between waves**;
* **within a wave**, cheap exports (`cost_class: low`) run in parallel; heavier
  ones run **one at a time** — the planner defers big tables precisely so several
  large scans don't hit the source together, so parallelising them would defeat it;
* each task is a real `rivet run --export <name>` (with `--reconcile` on
  full/chunked exports — a source COUNT(*) audit that fails the task on drift).

The wave layout is read at parse time from `plan.json`, so the scheduler never
touches the database to build the DAG; the `plan` task keeps that file current.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

try:  # Airflow 3.x — BashOperator moved into the bundled `standard` provider
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x — core
    from airflow.operators.bash import BashOperator

from airflow.utils.task_group import TaskGroup

# ── Configuration (override via the process env / Airflow Variables) ───────────
# RIVET_CONFIG points at the config the tasks run against — the docker-compose
# setup mounts `dags/rivet.yaml` and sets RIVET_CONFIG to it, so the file you edit
# sits right next to this DAG (see README "Config → plan → graph"). Credentials
# (RIVET_PG_URL, cloud keys) come from the task env, never hard-coded here.
RIVET_BIN = os.environ.get("RIVET_BIN", "rivet")
RIVET_CONFIG = os.environ.get("RIVET_CONFIG", str(Path(__file__).with_name("rivet.yaml")))
PLAN_PATH = Path(os.environ.get("RIVET_PLAN", Path(__file__).with_name("plan.json")))

# Optionally skip tables — you don't have to extract everything every run.
# `RIVET_EXCLUDE=bench_narrow,content_items` drops the two heavy tables from the
# graph; their wave disappears if it ends up empty. (For an include-list instead,
# set RIVET_ONLY.)
_EXCLUDE = {t.strip() for t in os.environ.get("RIVET_EXCLUDE", "").split(",") if t.strip()}
_ONLY = {t.strip() for t in os.environ.get("RIVET_ONLY", "").split(",") if t.strip()}


def _included(name: str) -> bool:
    if _ONLY:
        return name in _ONLY
    return name not in _EXCLUDE


# ── Parse the committed plan once, at DAG-parse time (no DB hit) ───────────────
# The plan is produced by the `plan` task below (`rivet plan --format json`). The
# DAG reads it here to lay out the waves — it never touches the database just to
# build itself. Edit the config + re-run the `plan` task to re-shape the graph.
try:
    _plan = json.loads(PLAN_PATH.read_text())
    # `rivet plan --format json` emits one object per export; every object carries
    # the same campaign recommendation, so the first one is enough.
    _campaign = _plan[0]["prioritization"]["campaign"]
except (FileNotFoundError, json.JSONDecodeError, KeyError, IndexError) as exc:
    raise RuntimeError(
        f"{PLAN_PATH} is missing or is not valid `rivet plan --format json` output "
        f"({exc!r}). Regenerate and commit it:\n"
        f"    rivet plan --config <your-config> --format json > {PLAN_PATH}"
    ) from exc
# Apply the include/exclude filter, then drop any wave left with no tables.
_waves = [
    {"wave": w["wave"], "exports": [e for e in w["exports"] if _included(e)]}
    for w in _campaign["waves"]
]
_waves = [w for w in _waves if w["exports"]]
_meta = {e["export_name"]: e for e in _campaign["ordered_exports"]}
# Per-table strategy, to decide where a reconcile gate makes sense.
_strategy = {e["export_name"]: e.get("strategy", "full") for e in _plan if "export_name" in e}
# Per-table plan detail (row estimate / verdict) for the operator tooltip.
_detail = {e["export_name"]: e for e in _plan if "export_name" in e}
# The config carries the operator-useful keys (chunk column, cursor column, chunk
# size) that the plan campaign doesn't — read them for the tooltip. Best-effort:
# a missing/unreadable config just means a leaner tooltip, never a broken DAG.
try:
    import yaml

    _cfg_exports = {
        e["name"]: e
        for e in (yaml.safe_load(Path(RIVET_CONFIG).read_text()) or {}).get("exports", [])
    }
except Exception:  # noqa: BLE001 — tooltip enrichment must never break DAG parsing
    _cfg_exports = {}


def _task_doc(name: str) -> str:
    """Operator tooltip: mode + the keys that matter (chunk / cursor), size, risk."""
    m = _meta.get(name, {})
    rows = (_detail.get(name, {}).get("computed") or {}).get("row_estimate")
    verdict = (_detail.get(name, {}).get("diagnostics") or {}).get("verdict")
    cfg = _cfg_exports.get(name, {})
    mode = cfg.get("mode") or _strategy.get(name, "?")
    # The keys an operator actually needs to reason about a slow / stuck export.
    if mode == "chunked" and cfg.get("chunk_by_days"):
        keys = f" · date chunks on `{cfg.get('chunk_column', '?')}` ({cfg['chunk_by_days']}d)"
    elif mode == "chunked":
        keys = f" · chunk key `{cfg.get('chunk_column', '?')}`, size {cfg.get('chunk_size', 100000):,}"
    elif mode == "incremental":
        keys = f" · cursor `{cfg.get('cursor_column', '?')}`"
    elif mode == "time_window":
        keys = f" · time column `{cfg.get('time_column', '?')}`, {cfg.get('days_window', '?')}d"
    else:
        keys = ""
    rows_s = f"~{rows:,}" if isinstance(rows, int) else "?"
    return (
        f"**{name}** — `{mode}`{keys}\n\n"
        f"{rows_s} rows · cost `{m.get('cost_class', '?')}` · risk `{m.get('risk_class', '?')}`"
        + (f" · verdict `{verdict}`" if verdict else "")
        + ("  \n_isolated on source — runs alone_" if m.get("isolate_on_source") else "")
    )


def _run_cmd(export_name: str) -> str:
    # One table. Quote the name so a table with odd characters can't break out of
    # the command.
    cmd = f"{RIVET_BIN} run --config {RIVET_CONFIG} --export {export_name!r}"
    # A reconcile gate (source COUNT(*) vs exported rows) only makes sense for a
    # full-table export. An incremental export writes only rows past the cursor,
    # so a full-table count always mismatches after the first run — rivet itself
    # warns about exactly this — so skip the gate there.
    if not str(_strategy.get(export_name, "")).startswith("incremental"):
        cmd += " --reconcile"
    return cmd


def _plan_cmd() -> str:
    # First task in the DAG: refresh the wave plan from the CURRENT config against
    # the real database — no manual "generate plan.json" step, no committing it by
    # hand. The new layout takes effect on the next DAG parse (Airflow builds the
    # graph at parse time, so a run can't reshape its own graph mid-flight); the
    # checked-in sample plan.json makes the DAG work on the very first boot.
    #
    # The write is ATOMIC and guarded: rivet writes to a temp file, which is
    # swapped in only if rivet succeeded AND the output is valid JSON. A failed or
    # empty plan can therefore never truncate the graph source and break the DAG
    # (the bug that bit the naive `> plan.json`). Then it prints the human summary.
    tmp = f"{PLAN_PATH}.tmp"
    return (
        f"{RIVET_BIN} plan --config {RIVET_CONFIG} --format json > {tmp} "
        f"&& test -s {tmp} "
        f"""&& python3 -c "import json; json.load(open('{tmp}'))" """
        f"&& mv {tmp} {PLAN_PATH} "
        f"&& echo '[plan] plan.json refreshed — the wave layout updates on the next DAG parse' "
        f"&& {RIVET_BIN} plan --config {RIVET_CONFIG}"
    )


with DAG(
    dag_id="rivet_waves",
    description="Wave-ordered Rivet extraction (planner-recommended, source-safe).",
    schedule=None,  # manual-only — trigger from the UI / CLI. Set a cron (e.g.
    # "0 2 * * *") for nightly runs once you're happy with it.
    start_date=datetime(2026, 1, 1),
    catchup=False,  # never backfill missed intervals — one run per trigger
    max_active_tasks=8,  # cap fan-out within a wave; the planner already isolates heavy ones
    tags=["rivet", "extract", "elt"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:
    # Step 1: refresh the plan from the config, then run the waves. The waves all
    # depend on this, so the graph reads init/config → plan → wave_1 → … in order.
    plan_step = BashOperator(
        task_id="plan",
        bash_command=_plan_cmd(),
        doc_md="`rivet plan` — score every export (size, cursor, risk) and group "
        "them into waves. Writes `plan.json`, which shapes the waves below.",
    )
    previous_wave = plan_step

    def _make_task(name: str) -> BashOperator:
        return BashOperator(
            task_id=name,
            bash_command=_run_cmd(name),
            doc_md=_task_doc(name),  # mode + chunk/cursor keys + size + risk
        )

    for wave in _waves:
        with TaskGroup(group_id=f"wave_{wave['wave']}") as wave_group:
            # Within a wave: only the CHEAP exports (cost_class "low") run in
            # parallel. The heavier ones run ONE AT A TIME (a sequential chain) —
            # the whole reason the planner defers big tables into a late wave is to
            # NOT pile several heavy scans onto the source at once, so running them
            # in parallel would defeat the point. Same split as the in-engine
            # `rivet apply --parallel-export-processes` (cheap concurrent, heavy serial).
            cheap = [n for n in wave["exports"] if _meta.get(n, {}).get("cost_class") == "low"]
            heavy = [n for n in wave["exports"] if _meta.get(n, {}).get("cost_class") != "low"]
            for name in cheap:
                _make_task(name)  # independent → run in parallel
            prev_heavy = None
            for name in heavy:
                task = _make_task(name)
                if prev_heavy is not None:
                    prev_heavy >> task  # serialize the heavy exports within the wave
                prev_heavy = task
        previous_wave >> wave_group  # barrier: this wave waits for the previous one
        previous_wave = wave_group
