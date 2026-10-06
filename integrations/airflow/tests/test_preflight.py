"""The preflight: binary, state location by deployment, config, the notifications warning."""

from __future__ import annotations

import os

import pytest
from airflow.exceptions import AirflowFailException
from conftest import NEW_FLAGS, FakeTI, Rig

from airflow_provider_rivet import _yaml
from airflow_provider_rivet.capabilities import probe
from airflow_provider_rivet.operators import RivetApplyOperator, RivetCdcRunOperator, RivetLoadOperator, RivetRunOperator
from airflow_provider_rivet.preflight import detect_deployment

PG_STATE = {"RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"}


def refused(rig: Rig, op: object, ti: FakeTI | None = None) -> str:
    """Run an operator that must be refused before any rivet process; return the refusal code."""
    exc, ti = rig.run(op, ti)
    assert isinstance(exc, AirflowFailException), exc
    assert not rig.calls(), "a refused task must not start rivet"
    return ti.store[(ti.task_id, "return_value")]["preflight_refusal"]


def test_kubernetes_is_detected_from_the_service_host_and_can_be_overridden() -> None:
    assert detect_deployment({"KUBERNETES_SERVICE_HOST": "10.0.0.1"}) == "pod"
    assert detect_deployment({}) == "local"
    assert detect_deployment({"KUBERNETES_SERVICE_HOST": "10.0.0.1"}, "local") == "local"


def test_a_pod_refuses_sqlite_state(rig: Rig) -> None:
    op = rig.operator(RivetApplyOperator, export="orders", deployment="auto", env={"KUBERNETES_SERVICE_HOST": "10.0.0.1"})
    assert refused(rig, op) == "RIVET_AIRFLOW_STATE_SQLITE_ON_POD"
    assert not (rig.state_dir / ".rivet_airflow_marker").exists()


def test_a_pod_refuses_cdc_even_with_postgres_state(rig: Rig) -> None:
    op = rig.operator(RivetCdcRunOperator, export="orders_cdc", deployment="pod", env=PG_STATE)
    assert refused(rig, op) == "RIVET_AIRFLOW_CDC_ON_POD"
    assert "checkpoint" in rig.events("rivet.refused")[0]["text"]
    plain = rig.operator(RivetRunOperator, export="orders_cdc", deployment="pod", env=PG_STATE)
    assert refused(rig, plain) == "RIVET_AIRFLOW_CDC_ON_POD", "a CDC export is refused whichever operator runs it"


def test_a_pod_with_postgres_state_runs_a_batch_export_and_writes_nothing_to_the_state_directory(rig: Rig) -> None:
    op = rig.operator(RivetRunOperator, export="orders", deployment="pod", env=PG_STATE)
    payload, _ = rig.run(op)
    assert payload["decision"] == "success" and payload["stderr_path"] is None and payload["state_marker"] is None
    assert rig.argv("run")[2] == str(rig.config.resolve()), "the config is run in place"
    assert list(rig.state_dir.iterdir()) == []
    assert "state_url_sqlite" not in payload["degraded"]
    assert rig.calls()[-1]["env"]["RIVET_STATE_URL"] == PG_STATE["RIVET_STATE_URL"]


def test_a_local_worker_needs_an_existing_writable_state_directory(rig: Rig) -> None:
    assert refused(rig, rig.operator(RivetApplyOperator, export="orders", state_dir=None)) == "RIVET_AIRFLOW_STATE_DIR_REQUIRED"
    missing = str(rig.tmp / "never-created")
    assert refused(rig, rig.operator(RivetApplyOperator, export="orders", state_dir=missing)) == "RIVET_AIRFLOW_STATE_DIR_MISSING"
    assert not os.path.exists(missing), "the directory is never created by a task"
    assert refused(rig, rig.operator(RivetApplyOperator, export="orders", state_dir="relative/dir")) == "RIVET_AIRFLOW_STATE_DIR_MISSING"
    locked = rig.tmp / "locked"
    locked.mkdir(mode=0o500)
    assert refused(rig, rig.operator(RivetApplyOperator, export="orders", state_dir=str(locked))) == "RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE"


def test_a_task_that_sees_another_state_directory_than_its_upstream_is_refused(rig: Rig) -> None:
    store: dict = {}
    first = rig.operator(RivetApplyOperator, task_id="apply.orders", export="orders")
    payload, _ = rig.run(first, FakeTI("apply.orders", store=store))
    store[("apply.orders", "return_value")] = payload
    other = rig.tmp / "other-state"
    other.mkdir()
    load = rig.operator(RivetLoadOperator, task_id="load.orders", export="orders", state_dir=str(other))
    load.upstream_task_ids.add("apply.orders")
    before = len(rig.calls())
    exc, ti = rig.run(load, FakeTI("load.orders", store=store))
    assert isinstance(exc, AirflowFailException) and len(rig.calls()) == before
    assert store[("load.orders", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_STATE_DIR_DIFFERS"
    same = rig.operator(RivetLoadOperator, task_id="load.orders", export="orders")
    same.upstream_task_ids.add("apply.orders")
    again, _ = rig.run(same, FakeTI("load.orders", store=store))
    assert again["state_marker"] == payload["state_marker"]


def test_a_binary_below_the_minimum_is_refused_with_both_versions(rig: Rig) -> None:
    rig.scenario(version="0.30.9")
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(exc, AirflowFailException) and "0.30.9" in str(exc) and "0.31.0" in str(exc)
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_BINARY_TOO_OLD"


def test_a_missing_binary_config_or_export_is_refused(rig: Rig) -> None:
    op = RivetApplyOperator(task_id="task", config=str(rig.config), export="orders", rivet_bin="rivet-not-installed")
    exc, _ = rig.run(op)
    assert isinstance(exc, AirflowFailException) and "RIVET_AIRFLOW_BINARY_MISSING" in str(exc)
    assert refused(rig, rig.operator(RivetApplyOperator, export="orders", config=str(rig.tmp / "nope.yaml"))) == "RIVET_AIRFLOW_CONFIG_UNREADABLE"
    assert refused(rig, rig.operator(RivetApplyOperator, export="not_in_config")) == "RIVET_AIRFLOW_EXPORT_UNKNOWN"


def test_a_notifications_block_is_warned_about_until_the_binary_has_no_notify(rig: Rig) -> None:
    rig.config.write_text(rig.config.read_text() + "notifications:\n  slack:\n    on: [failure]\n")
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert [w["warning"] for w in rig.events("rivet.warning")] == ["sqlite_single_host", "notifications_block"]
    assert "--no-notify" not in rig.argv("run")


def test_with_no_notify_the_flag_is_passed_and_the_warning_is_gone(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS)
    rig.config.write_text(rig.config.read_text() + "notifications:\n  slack:\n    on: [failure]\n")
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert [w["warning"] for w in rig.events("rivet.warning")] == ["sqlite_single_host"]
    assert "--no-notify" in rig.argv("run")


def test_postgres_state_on_a_local_worker_does_not_warn_about_sqlite(rig: Rig) -> None:
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", env=PG_STATE))
    assert not rig.events("rivet.warning") and "state_url_sqlite" not in payload["degraded"]


def test_flags_are_probed_from_help_as_whole_words(rig: Rig) -> None:
    rig.scenario(flags={"load": ["--export-all"]})
    caps = probe(str(rig.bin / "rivet"))
    assert caps.version == (0, 31, 0)
    assert caps.has_flag("run", "--export") and not caps.has_flag("load", "--export")
    assert not caps.has_flag("run", "--no-notify")


def test_a_relative_query_file_is_made_absolute_in_the_materialised_config(rig: Rig) -> None:
    rig.config.write_text(rig.config.read_text().replace("query: SELECT * FROM orders", "query_file: sql/orders.sql"))
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    copy = _yaml.load((rig.state_dir / "pg.yaml").read_text())
    assert copy["exports"][0]["query_file"] == str(rig.config_dir.resolve() / "sql/orders.sql")


def test_the_config_reader_keeps_yaml_1_1_words_as_strings() -> None:
    doc = _yaml.load("notifications:\n  on: [failure]\nat: 12:30:00\nday: 2026-01-01\nn: 100000\nflag: true\n")
    assert doc == {"notifications": {"on": ["failure"]}, "at": "12:30:00", "day": "2026-01-01", "n": 100000, "flag": True}
    assert _yaml.load(_yaml.dump(doc)) == doc
