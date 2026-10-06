"""Real DAG runs through `dag.test()`: per-task states and the run state for every way a run can fail."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from conftest import NEW_FLAGS, SECRET, DagRig

REFUSED = {"exit": 5, "line": {"error": "refused: row 4111-1111", "exit_class": 5, "code": "RIVET_SOURCE_CDC_LOG_GAP"}}
BATCH_CONFIG = """\
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - {name: a, query: SELECT 1, mode: full}
  - {name: b, query: SELECT 1, mode: full}
  - {name: big, query: SELECT 1, mode: incremental}
  - {name: huge, query: SELECT 1, mode: incremental}
"""
CDC_CONFIG = """\
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - {name: orders_cdc, mode: cdc}
"""
OK, FAILED, BLOCKED, SKIPPED = "success", "failed", "upstream_failed", "skipped"


def plan(dag_rig: DagRig, *waves: list[str]) -> str:
    """A plan layout file; every export of a wave after the first is heavy. The fake prints the same list."""
    heavy = {n for w in waves[1:] for n in w} if len(waves) > 1 else set(waves[0])
    campaign = {
        "waves": [{"wave": i + 1, "exports": list(w)} for i, w in enumerate(waves)],
        "ordered_exports": [{"export_name": n, "cost_class": "high" if n in heavy else "low"} for w in waves for n in w],
    }
    doc = [{"export_name": waves[0][0], "prioritization": {"campaign": campaign}}]
    path = dag_rig.tmp / "pg.plan.json"
    path.write_text(json.dumps(doc))
    dag_rig.scenario(**{**dag_rig.spec, "plan_list": doc})
    return str(path)


@pytest.fixture
def batch(dag_rig: DagRig) -> DagRig:
    """A rig whose config has the four exports of the two-wave plan."""
    dag_rig.config.write_text(BATCH_CONFIG)
    return dag_rig


@pytest.fixture
def cdc(dag_rig: DagRig) -> DagRig:
    """A rig whose config is one CDC stream."""
    dag_rig.config.write_text(CDC_CONFIG)
    return dag_rig


def two_waves(rig: DagRig, dag_id: str, **kwargs: object) -> tuple[str, dict[str, str]]:
    """plan -> wave 1 [a, b] -> wave 2 [big -> huge], with load and compact.big unless told otherwise."""
    kwargs.setdefault("compact_exports", ["big"])
    return rig.run_dag("build_batch_dag", dag_id, plan_file=plan(rig, ["a", "b"], ["big", "huge"]), **kwargs)


def test_a_two_wave_batch_run_where_rivet_succeeds_everywhere_is_a_success(batch: DagRig) -> None:
    state, tasks = two_waves(batch, "run_ok")
    assert tasks.pop("watcher") == SKIPPED
    assert set(tasks.values()) == {OK}, tasks
    assert sorted(tasks) == sorted([
        "plan", "apply.a", "apply.b", "apply.wave_1_done", "apply.big", "apply.after_big", "apply.huge",
        "load.a", "load.b", "load.big", "load.huge", "compact.big",
    ])
    assert state == OK
    order = [c for c in batch.ran() if c[0] == "apply"]
    assert order.index(("apply", "big")) < order.index(("apply", "huge")), "heavy exports run one after another"
    assert max(order.index(("apply", n)) for n in "ab") < order.index(("apply", "big")), "wave 2 starts after wave 1"


def test_a_failed_plan_blocks_everything_downstream_and_fails_the_run(batch: DagRig) -> None:
    batch.scenario(**{"plan:": {"exit": 1, "line": {"error": "bad config", "exit_class": 1}}})
    state, tasks = two_waves(batch, "plan_fails")
    assert tasks["plan"] == FAILED and tasks["watcher"] == FAILED and state == FAILED
    rivet = {t: s for t, s in tasks.items() if t.split(".")[0] in ("apply", "load", "compact") and "_done" not in t and "after_" not in t}
    assert len(rivet) == 9 and set(rivet.values()) == {BLOCKED}, rivet
    assert batch.ran() == [("plan", "")], "nothing but the plan reached rivet"


@pytest.mark.parametrize("load", [True, False])
def test_a_failed_plan_fails_a_one_wave_run_with_and_without_load(batch: DagRig, load: bool) -> None:
    batch.scenario(**{"plan:": {"exit": 1, "line": {"error": "bad config", "exit_class": 1}}})
    state, tasks = batch.run_dag("build_batch_dag", f"plan_fails_one_wave_{load}", plan_file=plan(batch, ["a", "b"]), load=load)
    assert (tasks["plan"], tasks["apply.a"], tasks["apply.b"], tasks["watcher"]) == (FAILED, BLOCKED, BLOCKED, FAILED)
    assert state == FAILED and batch.ran() == [("plan", "")]
    assert all(tasks[f"load.{n}"] == BLOCKED for n in "ab") if load else not [t for t in tasks if t.startswith("load.")]


def test_a_failed_apply_in_the_middle_of_a_heavy_chain_blocks_only_its_own_table(batch: DagRig) -> None:
    batch.scenario(**{"apply:big": REFUSED})
    state, tasks = two_waves(batch, "apply_fails")
    assert (tasks["apply.big"], tasks["load.big"], tasks["compact.big"]) == (FAILED, BLOCKED, BLOCKED)
    assert (tasks["apply.huge"], tasks["load.huge"]) == (OK, OK), "the next table of the chain still runs and loads"
    assert all(tasks[t] == OK for t in ("plan", "apply.a", "apply.b", "load.a", "load.b", "apply.wave_1_done", "apply.after_big"))
    assert tasks["watcher"] == FAILED and state == FAILED
    assert ("load", "big") not in batch.ran() and ("compact", "big") not in batch.ran()


def test_a_failed_load_blocks_its_compaction_and_fails_the_run(batch: DagRig) -> None:
    batch.scenario(**{"load:big": REFUSED})
    state, tasks = two_waves(batch, "load_fails")
    assert (tasks["load.big"], tasks["compact.big"]) == (FAILED, BLOCKED)
    others = {t: s for t, s in tasks.items() if t not in ("load.big", "compact.big", "watcher")}
    assert set(others.values()) == {OK}, others
    assert tasks["watcher"] == FAILED and state == FAILED and ("compact", "big") not in batch.ran()


def test_without_load_a_failed_apply_that_is_not_the_last_of_its_chain_fails_the_run(batch: DagRig) -> None:
    batch.scenario(**{"apply:big": REFUSED})
    state, tasks = batch.run_dag(
        "build_batch_dag", "noload_chain", plan_file=plan(batch, ["big", "huge"]), load=False, refresh_plan=False
    )
    assert tasks == {"apply.big": FAILED, "apply.after_big": OK, "apply.huge": OK, "watcher": FAILED}
    assert state == FAILED


def test_without_load_a_failed_apply_of_the_first_wave_fails_the_run_and_the_next_wave_runs(batch: DagRig) -> None:
    batch.scenario(**{"apply:a": REFUSED})
    state, tasks = two_waves(batch, "noload_waves", load=False, compact_exports=[])
    assert tasks["apply.a"] == FAILED and tasks["watcher"] == FAILED and state == FAILED
    assert all(tasks[t] == OK for t in ("plan", "apply.b", "apply.wave_1_done", "apply.big", "apply.huge"))


def test_a_cdc_run_that_succeeds_loads_and_compacts_every_table(cdc: DagRig) -> None:
    state, tasks = cdc.run_dag("build_cdc_dag", "cdc_ok", export="orders_cdc", tables=["orders", "users"])
    assert tasks.pop("watcher") == SKIPPED
    assert tasks == {t: OK for t in ("run", "load.orders", "load.users", "compact.orders", "compact.users")}
    assert state == OK


@pytest.mark.parametrize("shape", [{}, {"compact": False}, {"load": False}])
def test_a_refused_cdc_run_blocks_every_load_and_compact_and_fails_the_run(cdc: DagRig, shape: dict) -> None:
    cdc.scenario(run=REFUSED)
    name = "cdc_run_fails_" + "_".join(shape) if shape else "cdc_run_fails"
    state, tasks = cdc.run_dag("build_cdc_dag", name, export="orders_cdc", tables=["orders", "users"], **shape)
    assert tasks.pop("run") == FAILED and tasks.pop("watcher") == FAILED and state == FAILED
    assert set(tasks.values()) <= {BLOCKED}, tasks
    assert len(tasks) == {"": 4, "compact": 2, "load": 0}["".join(shape)]
    assert [c[0] for c in cdc.ran()] == ["run"], "no load and no compaction follows a refused drain"


def test_a_failed_cdc_load_blocks_its_own_compaction_only(cdc: DagRig) -> None:
    cdc.scenario(flags=NEW_FLAGS, **{"load:orders_cdc:orders": REFUSED})
    state, tasks = cdc.run_dag("build_cdc_dag", "cdc_load_fails", export="orders_cdc", tables=["orders", "users"])
    assert (tasks["load.orders"], tasks["compact.orders"]) == (FAILED, BLOCKED)
    assert (tasks["run"], tasks["load.users"], tasks["compact.users"]) == (OK, OK, OK)
    assert tasks["watcher"] == FAILED and state == FAILED


def scan(root: Path, needle: bytes) -> list[str]:
    """Every file under a directory that contains the bytes."""
    return [str(p) for p in root.rglob("*") if p.is_file() and needle in p.read_bytes()]


def test_no_password_reaches_the_metadata_database_the_logs_or_an_exception(
    batch: DagRig, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture
) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_RIVET_PG", "postgresql://rivet:c0nn-s3cr3t@db.internal:5432/app")
    batch.scenario(record_env=["RIVET_PG_URL", "RIVET_EXTRA_URL"], leak_env=["RIVET_PG_URL", "RIVET_EXTRA_URL"], **{"apply:b": REFUSED})
    caplog.set_level(logging.DEBUG)
    state, tasks = batch.run_dag(
        "build_batch_dag",
        "secrecy",
        exports=["a", "b"],
        env={"RIVET_EXTRA_URL": SECRET},
        operator_kwargs={"env_from_connections": {"RIVET_PG_URL": "rivet_pg"}},
    )
    assert (tasks["apply.a"], tasks["apply.b"], state) == (OK, FAILED, FAILED)
    seen = batch.calls()[-1]["env"]
    assert seen == {"RIVET_PG_URL": "postgresql://rivet:c0nn-s3cr3t@db.internal:5432/app", "RIVET_EXTRA_URL": SECRET}
    assert "AIRFLOW_CONN_RIVET_PG" not in batch.calls()[-1]["env_keys"]
    captured = capfd.readouterr()
    text = captured.out + captured.err + "\n".join(r.getMessage() + str(r.exc_info or "") for r in caplog.records)
    for secret in (b"c0nn-s3cr3t", b"s3cr3t-pw", b"4111-1111"):
        assert scan(batch.home, secret) == [], f"{secret!r} is in a file of the Airflow home (metadata DB or task log)"
        assert secret.decode() not in text, f"{secret!r} reached a log record or the process's stdout / stderr"
        assert scan(batch.state_dir / "logs", secret), "the fake did print it to its stderr file"
    assert scan(batch.home, str(batch.state_dir).encode()), "the scan sees the rendered fields: state_dir is in them"
    assert "rivet.result" in text, "the scan sees the task's log records"
