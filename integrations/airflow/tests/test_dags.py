"""The two DAG builders: task ids, TaskGroups and the edges between them."""

from __future__ import annotations

import json
import re
from importlib import metadata
from pathlib import Path

import pytest

from airflow_provider_rivet import __version__, get_provider_info
from airflow_provider_rivet.callbacks import format_failure
from airflow_provider_rivet.dags import build_batch_dag, build_cdc_dag, read_plan_layout
from airflow_provider_rivet.operators import RivetApplyOperator, RivetRunOperator, RivetRunWatcher, RivetWaveBarrier

from conftest import fixture


def edges(dag: object) -> set[tuple[str, str]]:
    """Every (upstream, downstream) pair of a DAG."""
    return {(t.task_id, d) for t in dag.tasks for d in t.downstream_task_ids}


def upstream_closure(dag: object, task_id: str) -> set[str]:
    """Every task a task depends on, directly or not."""
    seen: set[str] = set()
    todo = [task_id]
    while todo:
        for up in dag.get_task(todo.pop()).upstream_task_ids:
            if up not in seen:
                seen.add(up)
                todo.append(up)
    return seen


@pytest.fixture
def plan_file(tmp_path: Path) -> str:
    """A plan layout with two waves; `big` and `huge` are not cheap."""
    campaign = {
        "waves": [{"wave": 3, "exports": ["big", "huge"]}, {"wave": 2, "exports": ["a", "b"]}],
        "ordered_exports": [
            {"export_name": "a", "cost_class": "low"},
            {"export_name": "b", "cost_class": "low"},
            {"export_name": "big", "cost_class": "high"},
            {"export_name": "huge", "cost_class": "high"},
        ],
    }
    path = tmp_path / "pg.plan.json"
    path.write_text(json.dumps([{"export_name": "a", "prioritization": {"campaign": campaign}}]))
    return str(path)


def test_the_plan_layout_orders_waves_and_marks_the_heavy_exports(plan_file: str) -> None:
    assert read_plan_layout(plan_file) == [
        {"wave": 2, "exports": ["a", "b"], "heavy": []},
        {"wave": 3, "exports": ["big", "huge"], "heavy": ["big", "huge"]},
    ]
    with pytest.raises(RuntimeError, match="rivet plan --config"):
        read_plan_layout(plan_file + ".absent")


def test_batch_dag_has_three_groups_and_per_table_chains(plan_file: str) -> None:
    dag = build_batch_dag("b", config="/c/pg.yaml", state_dir="/state", plan_file=plan_file, compact_exports=["big"])
    assert sorted(dag.task_ids) == sorted(
        ["plan", "apply.a", "apply.b", "apply.wave_2_done", "apply.big", "apply.after_big", "apply.huge",
         "load.a", "load.b", "load.big", "load.huge", "compact.big", "watcher"]
    )
    assert set(dag.task_group.children) == {"plan", "apply", "load", "compact", "watcher"}
    assert edges(dag) == {
        ("plan", "apply.a"), ("plan", "apply.b"), ("plan", "apply.big"), ("plan", "apply.huge"),
        ("apply.a", "apply.wave_2_done"), ("apply.b", "apply.wave_2_done"),
        ("apply.wave_2_done", "apply.big"), ("apply.wave_2_done", "apply.huge"),
        ("apply.big", "apply.after_big"), ("apply.after_big", "apply.huge"),
        ("apply.a", "load.a"), ("apply.b", "load.b"), ("apply.big", "load.big"), ("apply.huge", "load.huge"),
        ("load.big", "compact.big"),
    } | {(t, "watcher") for t in dag.task_ids if t != "watcher"}


def test_a_failed_table_does_not_gate_another_tables_load(plan_file: str) -> None:
    dag = build_batch_dag("b", config="/c/pg.yaml", state_dir="/state", plan_file=plan_file, compact_exports=["big"])
    assert dag.get_task("load.b").upstream_task_ids == {"apply.b"}
    assert "apply.a" not in upstream_closure(dag, "load.b") and "load.a" not in upstream_closure(dag, "load.b")
    assert dag.get_task("compact.big").upstream_task_ids == {"load.big"}
    for ordering in ("apply.wave_2_done", "apply.after_big"):
        assert dag.get_task(ordering).trigger_rule == "all_done", f"{ordering}: an ordering point waits, it does not gate"
        assert isinstance(dag.get_task(ordering), RivetWaveBarrier)
    for gated in ("apply.a", "apply.big", "apply.huge", "load.b", "compact.big"):
        assert dag.get_task(gated).trigger_rule == "all_success", gated
    assert dag.get_task("apply.huge").upstream_task_ids == {"plan", "apply.wave_2_done", "apply.after_big"}
    assert "apply.a" in upstream_closure(dag, "load.big"), "a later wave is ordered after the earlier one"
    watcher = dag.get_task("watcher")
    assert isinstance(watcher, RivetRunWatcher) and watcher.trigger_rule == "one_failed" and watcher.retries == 0
    assert watcher.upstream_task_ids == set(dag.task_ids) - {"watcher"}


def test_batch_dag_limits_overlap_and_passes_one_state_directory_to_every_task(plan_file: str) -> None:
    dag = build_batch_dag("b", config="/c/pg.yaml", state_dir="/state", plan_file=plan_file)
    assert dag.max_active_runs == 1 and dag.catchup is False
    rivet_tasks = [t for t in dag.tasks if hasattr(t, "state_dir")]
    assert len(rivet_tasks) == 9 and {t.state_dir for t in rivet_tasks} == {"/state"}
    assert {t.max_active_tis_per_dag for t in rivet_tasks} == {1}
    assert isinstance(dag.get_task("apply.a"), RivetApplyOperator) and dag.get_task("apply.a").retries == 2
    assert "compact" not in dag.task_group.children, "no compact task is created unless the author names the export"


def test_batch_dag_from_an_explicit_list_and_with_rivet_run() -> None:
    dag = build_batch_dag("b", config="/c/pg.yaml", state_dir="/s", exports=["x", "y"], extract="run", load=False)
    assert sorted(dag.task_ids) == ["apply.x", "apply.y", "watcher"]
    assert edges(dag) == {("apply.x", "watcher"), ("apply.y", "watcher")}
    assert isinstance(dag.get_task("apply.x"), RivetRunOperator) and dag.get_task("apply.x").export == "x"
    assert dag.get_task("apply.x").trigger_rule == "all_success"


def test_batch_dag_refuses_contradictory_arguments(plan_file: str) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        build_batch_dag("b", config="c", plan_file=plan_file, exports=["a"])
    with pytest.raises(ValueError, match="does not have"):
        build_batch_dag("b", config="c", exports=["a"], compact_exports=["zzz"])
    with pytest.raises(ValueError, match="needs load=True"):
        build_batch_dag("b", config="c", exports=["a"], compact_exports=["a"], load=False)
    with pytest.raises(ValueError, match="`exports` is empty"):
        build_batch_dag("b", config="c", exports=[])
    with pytest.raises(ValueError, match="`tables` lists"):
        build_cdc_dag("c", config="c", export="e", tables=[])


def no_callback(task: object) -> bool:
    """True when a task has no failure callback (Airflow 2 keeps `None`, Airflow 3 an empty list)."""
    return not task.on_failure_callback


def alert(context: object) -> None:
    """A failure callback that does nothing."""


def test_the_watcher_carries_no_failure_callback_from_default_args() -> None:
    args = {"retries": 2, "on_failure_callback": alert}
    batch = build_batch_dag("b", config="c", exports=["a"], default_args=args)
    cdc = build_cdc_dag("c", config="c", export="e", tables=["t"], default_args=args)
    for dag in (batch, cdc):
        assert no_callback(dag.get_task("watcher")), "the watcher would send a second message with no rivet result in it"
        others = [t for t in dag.tasks if t.task_id != "watcher"]
        assert others and all(not no_callback(t) for t in others)
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    assert "The `watcher` task carries no failure callback" in readme


def test_cdc_dag_is_one_run_then_load_and_compact_per_table() -> None:
    dag = build_cdc_dag("c", config="/c/cdc.yaml", export="app_cdc", tables=["orders", "users"], state_dir="/state")
    assert sorted(dag.task_ids) == ["compact.orders", "compact.users", "load.orders", "load.users", "run", "watcher"]
    assert set(dag.task_group.children) == {"run", "load", "compact", "watcher"}
    assert edges(dag) == {
        ("run", "load.orders"), ("run", "load.users"),
        ("load.orders", "compact.orders"), ("load.users", "compact.users"),
    } | {(t, "watcher") for t in dag.task_ids if t != "watcher"}
    assert dag.get_task("watcher").trigger_rule == "one_failed"
    for table in ("orders", "users"):
        load = dag.get_task(f"load.{table}")
        assert load.trigger_rule == "all_success", "nothing is loaded after a drain that did not succeed"
        assert (load.export, load.table) == ("app_cdc", table)
        assert dag.get_task(f"compact.{table}").trigger_rule == "all_success"
    assert "load.orders" not in upstream_closure(dag, "compact.users")
    assert dag.max_active_runs == 1 and dag.get_task("run").max_active_tis_per_dag == 1


def test_cdc_dag_without_compact_or_load() -> None:
    assert sorted(build_cdc_dag("c", config="c", export="e", tables=["t"], compact=False).task_ids) == ["load.t", "run", "watcher"]
    assert sorted(build_cdc_dag("c2", config="c", export="e", tables=[], load=False).task_ids) == ["run", "watcher"]


def test_the_provider_entry_point_names_the_distribution_and_the_version_has_one_source() -> None:
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-rivet" and info["versions"] == [__version__]
    pyproject = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
    assert 'dynamic = ["version"]' in pyproject and 'path = "airflow_provider_rivet/__init__.py"' in pyproject
    assert not re.search(r"^version\s*=", pyproject, re.M), "the version is written in __init__.py only"
    assert metadata.version("airflow-provider-rivet") == __version__


def test_the_slack_text_has_code_class_action_and_no_error_message() -> None:
    payload = {"units": [fixture("xcom_unit.json"), fixture("xcom_unit_load.json")], "stderr_path": "/state/logs/x.log"}
    text = format_failure(payload, dag_id="d", task_id="apply.orders_cdc", run_id="r1", log_url="http://af/log")
    assert "*d.apply.orders_cdc* failed" in text and "`RIVET_SOURCE_CDC_LOG_GAP` `refusal` (not retryable)" in text
    assert "action: restore the missing log" in text and "/state/logs/x.log" in text and "<http://af/log|task log>" in text
    assert "no longer in the source log" not in text and text.count("•") == 1
    refused = format_failure({"preflight_refusal": "RIVET_AIRFLOW_CDC_ON_POD", "units": []}, dag_id="d", task_id="t", run_id="r")
    assert "RIVET_AIRFLOW_CDC_ON_POD" in refused
    assert "no rivet result" in format_failure(None, dag_id="d", task_id="t", run_id="r")
