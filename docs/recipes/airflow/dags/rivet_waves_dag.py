"""Rivet wave-ordered extraction as Airflow DAGs — one per source database.

`rivet plan` scores every export by size / cursor quality / risk and groups them
into **waves** — small tables in an early wave, heavy tables deferred into later
waves so they don't pile load onto the source at once (see ADR-0006). Rivet's
planner is *advisory*: it recommends; an external scheduler executes. This is it.

`build_wave_dag()` turns one source's plan into one DAG; the bottom of the file
builds three — PostgreSQL, MySQL, SQL Server — each off its own config + plan.

Each DAG's graph:

    plan ──> wave_2 ──────────> wave_3 ──────────────> wave_4
             [small tables       [bench_decimal →       [bench_narrow →
              in parallel]        bench_hc → …]          content_items]

* a `plan` task that refreshes that source's `plan.json` from its config
  (atomically), then a **TaskGroup per wave** with a **barrier between waves**;
* **within a wave**, cheap exports (`cost_class: low`) run in parallel; heavier
  ones run **one at a time** — the planner defers big tables precisely so several
  large scans don't hit the source together, so parallelising them would defeat it;
* each task is a real `rivet run --export <name>` (with `--reconcile` on
  full/chunked exports — a source COUNT(*) audit that fails the task on drift).

The wave layout is read at parse time from `<source>.plan.json`, so the scheduler
never touches the database to build the DAG; the `plan` task keeps it current.
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

RIVET_BIN = os.environ.get("RIVET_BIN", "rivet")
_HERE = Path(__file__).parent

# Optionally skip tables — applies to every DAG. `RIVET_EXCLUDE=bench_narrow,…`
# drops those tables from the graph (a wave left empty disappears); `RIVET_ONLY`
# is an allow-list instead.
_EXCLUDE = {t.strip() for t in os.environ.get("RIVET_EXCLUDE", "").split(",") if t.strip()}
_ONLY = {t.strip() for t in os.environ.get("RIVET_ONLY", "").split(",") if t.strip()}


def _included(name: str) -> bool:
    return name in _ONLY if _ONLY else name not in _EXCLUDE


def build_wave_dag(
    dag_id: str, config_path: str, plan_path: str, *, tags: list[str], state_url: str
) -> DAG:
    """Build one wave-ordered DAG for the source described by `config_path`.

    The wave layout comes from `plan_path` (a `rivet plan --format json` artifact)
    read at parse time; the DAG's own `plan` task keeps that file fresh.

    `state_url` is this source's own state database — each source gets its own so
    that same-named tables across engines (a `bench_hc` in Postgres and in MySQL)
    don't collide on cursor / shape / file-log state.
    """
    plan_file = Path(plan_path)
    # ── Parse the plan at DAG-parse time (no DB hit) ──────────────────────────
    try:
        plan = json.loads(plan_file.read_text())
        # Every per-export object carries the same campaign, so the first is enough.
        campaign = plan[0]["prioritization"]["campaign"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError, IndexError) as exc:
        raise RuntimeError(
            f"{plan_file} is missing or is not valid `rivet plan --format json` output "
            f"({exc!r}). Regenerate and commit it:\n"
            f"    rivet plan --config {config_path} --format json > {plan_file}"
        ) from exc

    waves = [
        {"wave": w["wave"], "exports": [e for e in w["exports"] if _included(e)]}
        for w in campaign["waves"]
    ]
    waves = [w for w in waves if w["exports"]]
    meta = {e["export_name"]: e for e in campaign["ordered_exports"]}
    strategy = {e["export_name"]: e.get("strategy", "full") for e in plan if "export_name" in e}
    detail = {e["export_name"]: e for e in plan if "export_name" in e}
    # The config carries the operator-useful keys (chunk / cursor column, chunk
    # size) the plan campaign doesn't. Best-effort: a missing config just means a
    # leaner tooltip, never a broken DAG.
    try:
        import yaml

        cfg_exports = {
            e["name"]: e
            for e in (yaml.safe_load(Path(config_path).read_text()) or {}).get("exports", [])
        }
    except Exception:  # noqa: BLE001 — tooltip enrichment must never break parsing
        cfg_exports = {}

    def task_doc(name: str) -> str:
        """Operator tooltip: mode + the keys that matter (chunk / cursor), size, risk."""
        m = meta.get(name, {})
        rows = (detail.get(name, {}).get("computed") or {}).get("row_estimate")
        verdict = (detail.get(name, {}).get("diagnostics") or {}).get("verdict")
        cfg = cfg_exports.get(name, {})
        mode = cfg.get("mode") or strategy.get(name, "?")
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

    def run_cmd(name: str) -> str:
        # A reconcile gate (source COUNT(*) vs exported rows) only makes sense for a
        # full-table export; an incremental one writes only rows past the cursor, so
        # a full-table count always mismatches after the first run (rivet warns).
        cmd = f"RIVET_STATE_URL='{state_url}' {RIVET_BIN} run --config {config_path} --export {name!r}"
        if not str(strategy.get(name, "")).startswith("incremental"):
            cmd += " --reconcile"
        return cmd

    def plan_cmd() -> str:
        # First task: refresh the plan from the current config against the real DB —
        # no manual "generate plan.json" step. ATOMIC + guarded: rivet writes a temp
        # file, swapped in only if it succeeded AND the output is valid JSON, so a
        # failed plan can't truncate the graph source and break the DAG.
        tmp = f"{plan_file}.tmp"
        return (
            f"{RIVET_BIN} plan --config {config_path} --format json > {tmp} "
            f"&& test -s {tmp} "
            f"""&& python3 -c "import json; json.load(open('{tmp}'))" """
            f"&& mv {tmp} {plan_file} "
            f"&& echo '[plan] {plan_file.name} refreshed — wave layout updates on the next parse' "
            f"&& {RIVET_BIN} plan --config {config_path}"
        )

    with DAG(
        dag_id=dag_id,
        description=f"Wave-ordered Rivet extraction from {config_path} (source-safe).",
        schedule=None,  # manual-only — set a cron once you're happy with it.
        start_date=datetime(2026, 1, 1),
        catchup=False,  # never backfill missed intervals — one run per trigger
        max_active_tasks=8,
        tags=tags,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    ) as dag:
        plan_step = BashOperator(
            task_id="plan",
            bash_command=plan_cmd(),
            doc_md="`rivet plan` — score every export (size, cursor, risk), group into "
            "waves, refresh the plan file the graph below is built from.",
        )
        previous_wave = plan_step

        def make_task(name: str) -> BashOperator:
            return BashOperator(task_id=name, bash_command=run_cmd(name), doc_md=task_doc(name))

        for wave in waves:
            with TaskGroup(group_id=f"wave_{wave['wave']}") as wave_group:
                # Cheap exports (cost_class "low") run in parallel; heavier ones run
                # ONE AT A TIME — the planner defers big tables into a late wave so
                # several large scans don't hit the source together, and running them
                # in parallel would defeat that. (Same split as the in-engine
                # `rivet apply --parallel-export-processes`.)
                cheap = [n for n in wave["exports"] if meta.get(n, {}).get("cost_class") == "low"]
                heavy = [n for n in wave["exports"] if meta.get(n, {}).get("cost_class") != "low"]
                for name in cheap:
                    make_task(name)  # independent → parallel
                prev_heavy = None
                for name in heavy:
                    task = make_task(name)
                    if prev_heavy is not None:
                        prev_heavy >> task  # serialize the heavy exports within the wave
                    prev_heavy = task
            previous_wave >> wave_group  # barrier between waves
            previous_wave = wave_group

    return dag


# ── One DAG per source database ────────────────────────────────────────────────
# Each reads its own config + plan; the worker env supplies the matching source
# URL (RIVET_PG_URL / RIVET_MY_URL / RIVET_MS_URL). Each also gets its OWN state
# database so same-named tables across engines don't collide on cursor / shape.
_STATE_HOST = "rivet-state-db:5432"
for _id, _cfg, _plan, _tag, _state in (
    ("rivet_waves_postgres", "postgres.yaml", "postgres.plan.json", "postgres", "rivet_state_postgres"),
    ("rivet_waves_mysql", "mysql.yaml", "mysql.plan.json", "mysql", "rivet_state_mysql"),
    ("rivet_waves_mssql", "mssql.yaml", "mssql.plan.json", "sqlserver", "rivet_state_mssql"),
):
    globals()[_id] = build_wave_dag(
        _id,
        str(_HERE / _cfg),
        str(_HERE / _plan),
        tags=["rivet", "extract", _tag],
        state_url=f"postgresql://rivet:rivet@{_STATE_HOST}/{_state}?sslmode=require",
    )
