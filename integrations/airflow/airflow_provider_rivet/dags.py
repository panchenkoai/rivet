"""DAG builders: TaskGroups are the picture, dependencies run per table."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from ._compat import DAG, TaskGroup
from .operators import (
    RivetApplyOperator,
    RivetCdcRunOperator,
    RivetCompactOperator,
    RivetLoadOperator,
    RivetPlanOperator,
    RivetRunOperator,
    RivetWaveBarrier,
)

ORDER_ONLY = "all_done"
DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=5)}


def read_plan_layout(plan_file: str) -> list[dict[str, Any]]:
    """Waves from a `rivet plan --format json` file: `[{"wave", "exports", "heavy"}]`, lowest wave first."""
    try:
        plan = json.loads(Path(plan_file).read_text())
        campaign = plan[0]["prioritization"]["campaign"]
    except (OSError, ValueError, KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(
            f"{plan_file} is missing or is not `rivet plan --format json` output ({type(exc).__name__}); "
            f"write it with: rivet plan --config <config> --format json > {plan_file}"
        ) from exc
    cost = {e["export_name"]: e.get("cost_class") for e in campaign.get("ordered_exports", [])}
    waves = sorted(campaign["waves"], key=lambda w: w["wave"])
    return [
        {"wave": w["wave"], "exports": list(w["exports"]), "heavy": [n for n in w["exports"] if cost.get(n) != "low"]}
        for w in waves
        if w["exports"]
    ]


def _dag(dag_id: str, tags: Sequence[str], dag_kwargs: Mapping[str, Any]) -> DAG:
    """A DAG with the overlap limits ADR-0039's lease will later make redundant."""
    settings: dict[str, Any] = {
        "schedule": None,
        "start_date": datetime(2026, 1, 1),
        "catchup": False,
        "max_active_runs": 1,
        "tags": list(tags),
        "default_args": dict(DEFAULT_ARGS),
    }
    settings.update(dag_kwargs)
    return DAG(dag_id=dag_id, **settings)


def build_batch_dag(
    dag_id: str,
    *,
    config: str,
    state_dir: Optional[str] = None,
    plan_file: Optional[str] = None,
    exports: Optional[Sequence[str]] = None,
    refresh_plan: bool = True,
    extract: str = "apply",
    load: bool = True,
    compact_exports: Sequence[str] = (),
    operator_kwargs: Optional[Mapping[str, Any]] = None,
    **dag_kwargs: Any,
) -> DAG:
    """plan -> apply / load / compact groups, chained per export: apply.X >> load.X >> compact.X."""
    if (plan_file is None) == (exports is None):
        raise ValueError("give exactly one of `plan_file` (waves from rivet plan) or `exports` (one wave)")
    if extract not in ("apply", "run"):
        raise ValueError("`extract` is 'apply' or 'run'")
    waves = read_plan_layout(plan_file) if plan_file else [{"wave": 1, "exports": list(exports or ()), "heavy": []}]
    names = [n for w in waves for n in w["exports"]]
    unknown = sorted(set(compact_exports) - set(names))
    if unknown:
        raise ValueError(f"compact_exports names exports the plan does not have: {unknown}")
    if compact_exports and not load:
        raise ValueError("compact_exports needs load=True: a compaction merges what the load buffered")
    common = {"config": config, "state_dir": state_dir, "max_active_tis_per_dag": 1, **(operator_kwargs or {})}
    extract_cls = RivetApplyOperator if extract == "apply" else RivetRunOperator

    with _dag(dag_id, ("rivet", "batch"), dag_kwargs) as dag:
        apply_tasks: dict[str, Any] = {}
        with TaskGroup(group_id="apply"):
            previous: Optional[Any] = None
            for index, wave in enumerate(waves):
                chained: Optional[Any] = None
                for name in wave["exports"]:
                    ordered = previous is not None or (name in wave["heavy"] and chained is not None)
                    task = extract_cls(
                        task_id=name,
                        export=name,
                        trigger_rule=ORDER_ONLY if ordered or (plan_file and refresh_plan) else "all_success",
                        **common,
                    )
                    apply_tasks[name] = task
                    if previous is not None:
                        previous >> task
                    if name in wave["heavy"]:
                        if chained is not None:
                            chained >> task
                        chained = task
                if index < len(waves) - 1:
                    barrier = RivetWaveBarrier(task_id=f"wave_{wave['wave']}_done", trigger_rule=ORDER_ONLY)
                    for name in wave["exports"]:
                        apply_tasks[name] >> barrier
                    previous = barrier
        if plan_file and refresh_plan:
            plan = RivetPlanOperator(task_id="plan", plan_file=plan_file, **common)
            for name in waves[0]["exports"]:
                plan >> apply_tasks[name]
        load_tasks: dict[str, Any] = {}
        if load:
            with TaskGroup(group_id="load"):
                for name in names:
                    load_tasks[name] = RivetLoadOperator(task_id=name, export=name, **common)
                    apply_tasks[name] >> load_tasks[name]
        if compact_exports:
            with TaskGroup(group_id="compact"):
                for name in names:
                    if name in compact_exports:
                        load_tasks[name] >> RivetCompactOperator(task_id=name, export=name, **common)
    return dag


def build_cdc_dag(
    dag_id: str,
    *,
    config: str,
    export: str,
    tables: Sequence[str],
    state_dir: Optional[str] = None,
    load: bool = True,
    compact: bool = True,
    operator_kwargs: Optional[Mapping[str, Any]] = None,
    **dag_kwargs: Any,
) -> DAG:
    """One `run` task for the stream, then load.T >> compact.T per captured table."""
    if not tables and load:
        raise ValueError("`tables` lists the captured tables the load and compact tasks are built for")
    common = {"config": config, "state_dir": state_dir, "max_active_tis_per_dag": 1, **(operator_kwargs or {})}
    with _dag(dag_id, ("rivet", "cdc"), dag_kwargs) as dag:
        run = RivetCdcRunOperator(task_id="run", export=export, **common)
        if load:
            loads = {}
            with TaskGroup(group_id="load"):
                for table in tables:
                    loads[table] = RivetLoadOperator(
                        task_id=table, export=export, table=table, trigger_rule=ORDER_ONLY, **common
                    )
                    run >> loads[table]
            if compact:
                with TaskGroup(group_id="compact"):
                    for table in tables:
                        loads[table] >> RivetCompactOperator(task_id=table, export=export, table=table, **common)
    return dag
