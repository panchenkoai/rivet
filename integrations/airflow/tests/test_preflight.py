"""The preflight: binary, state location by deployment, config, the notifications warning."""

from __future__ import annotations

import ast
import os
import subprocess
from pathlib import Path

import pytest
from airflow.exceptions import AirflowFailException
from conftest import FIXTURES, NEW_FLAGS, FakeTI, Rig

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
    assert payload["decision"] == "success" and payload["state_marker"] is None
    assert not Path(payload["stderr_path"]).is_relative_to(rig.state_dir)
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


def with_query_file(rig: Rig, ref: str = "sql/orders.sql") -> None:
    """Point the `orders` export at a query file."""
    rig.config.write_text(rig.config.read_text().replace("query: SELECT * FROM orders", f"query_file: {ref}"))


def test_a_relative_query_file_stays_relative_and_is_copied_beside_the_materialised_config(rig: Rig) -> None:
    (rig.config_dir / "sql").mkdir()
    (rig.config_dir / "sql" / "orders.sql").write_text("SELECT 42")
    with_query_file(rig)
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert payload["decision"] == "success", rig.calls()
    assert rig.copy().read_text() == rig.config.read_text(), "the copy is the config as written, comments and all"
    assert _yaml.load(rig.copy().read_text())["exports"][0]["query_file"] == "sql/orders.sql"
    assert rig.calls()[-1]["sql"] == {"orders": "SELECT 42"}, "rivet read the SQL through the relative path"
    (rig.config_dir / "sql" / "orders.sql").write_text("SELECT 43")
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert rig.calls()[-1]["sql"] == {"orders": "SELECT 43"}, "an edit of the original reaches the next task"
    rig.run(rig.operator(RivetLoadOperator, export="orders"))
    assert _yaml.load(rig.copy("orders").read_text())["exports"][0]["query_file"] == "sql/orders.sql"


def test_the_fake_binary_refuses_a_query_file_by_rivets_own_rule(rig: Rig) -> None:
    source = (FIXTURES.parents[2] / "src" / "config" / "export.rs").read_text()
    tree = ast.parse((Path(__file__).parent / "fake_rivet.py").read_text())
    rules = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign) and n.targets[0].id == "QUERY_FILE_RULES")
    assert len(rules) == 3 and all(rule in source for rule in rules), "the fake's refusal texts are rivet's"
    outside = rig.tmp / "outside.sql"
    outside.write_text("SELECT 1")
    (rig.config_dir / "link.sql").symlink_to(outside)
    for ref, rule in ((str(outside), rules[0]), ("../outside.sql", rules[1]), ("link.sql", rules[2])):
        rig.config.write_text(rig.config.read_text().replace("query: SELECT * FROM orders", f"query_file: {ref}"))
        done = subprocess.run([str(rig.bin / "rivet"), "run", "--config", str(rig.config)], capture_output=True, text=True)
        assert done.returncode == 1 and rule in done.stderr, ref
        rig.config.write_text(rig.config.read_text().replace(f"query_file: {ref}", "query: SELECT * FROM orders"))


def test_a_query_file_rivet_would_not_read_is_refused_before_rivet_starts(rig: Rig) -> None:
    outside = rig.tmp / "outside.sql"
    outside.write_text("SELECT 1")
    (rig.config_dir / "link.sql").symlink_to(outside)
    (rig.config_dir / "sql").mkdir()
    (rig.config_dir / "inside.sql").write_text("SELECT 1")
    for ref in (str(outside), "../outside.sql", "sql/../inside.sql", "link.sql", "sql/missing.sql"):
        with_query_file(rig, ref)
        assert refused(rig, rig.operator(RivetRunOperator, export="orders")) == "RIVET_AIRFLOW_QUERY_FILE", ref
        assert ref in rig.events("rivet.refused")[-1]["text"]
        rig.config.write_text(rig.config.read_text().replace(f"query_file: {ref}", "query: SELECT * FROM orders"))
    with_query_file(rig, "sql/missing.sql")
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="users"))
    assert payload["decision"] == "success", "a missing file of another export is that export's problem"


def test_two_configs_with_one_basename_get_their_own_copies(rig: Rig) -> None:
    other = rig.tmp / "cfg2" / "pg.yaml"
    other.parent.mkdir()
    other.write_text(rig.config.read_text().replace("SELECT * FROM orders", "SELECT id FROM orders"))
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    rig.run(rig.operator(RivetRunOperator, export="orders", config=str(other)))
    first, second = rig.calls()[-2]["argv"][2], rig.calls()[-1]["argv"][2]
    assert (first, second) == (str(rig.copy()), str(rig.copy(config=other))) and first != second
    assert Path(first).read_text() == rig.config.read_text() and Path(second).read_text() == other.read_text()


def test_two_config_directories_cannot_put_different_files_at_one_relative_path_of_a_state_directory(rig: Rig) -> None:
    other = rig.tmp / "cfg2" / "other.yaml"
    for folder, sql in ((rig.config_dir, "SELECT 1"), (other.parent, "SELECT 2")):
        (folder / "sql").mkdir(parents=True)
        (folder / "sql" / "orders.sql").write_text(sql)
    with_query_file(rig)
    other.write_text(rig.config.read_text())
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders", config=str(other)))
    assert isinstance(exc, AirflowFailException) and len(rig.calls()) == 1
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_QUERY_FILE"
    assert str(rig.config_dir.resolve()) in str(exc) and "separate state directories" in str(exc)
    assert (rig.state_dir / "sql" / "orders.sql").read_text() == "SELECT 1"
    sibling = rig.config_dir / "second.yaml"
    sibling.write_text(rig.config.read_text())
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", config=str(sibling)))
    assert payload["decision"] == "success", "two configs of one directory share their query files"


def test_a_state_database_left_beside_the_original_config_is_refused_until_it_is_moved(rig: Rig) -> None:
    (rig.config_dir / ".rivet_state.db").write_bytes(b"state")
    for _cycle in (1, 2):
        assert refused(rig, rig.operator(RivetRunOperator, export="orders")) == "RIVET_AIRFLOW_STATE_BESIDE_CONFIG"
    text = rig.events("rivet.refused")[-1]["text"]
    assert str(rig.config_dir.resolve() / ".rivet_state.db") in text and str(rig.state_dir) in text and "move" in text
    assert not (rig.state_dir / ".rivet_state.db").exists(), "a refusal writes nothing that would lift it"
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", env=PG_STATE))
    assert payload["decision"] == "success", "PostgreSQL state does not read the file"
    (rig.config_dir / ".rivet_state.db").rename(rig.state_dir / ".rivet_state.db")
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert payload["decision"] == "success"


def test_a_cdc_checkpoint_left_beside_the_original_config_is_refused_until_it_is_moved(rig: Rig) -> None:
    rig.config.write_text(rig.config.read_text().replace("    mode: cdc\n", "    mode: cdc\n    cdc:\n      checkpoint: ckpt/orders.ckpt\n"))
    (rig.config_dir / "ckpt").mkdir()
    (rig.config_dir / "ckpt" / "orders.ckpt").write_text("lsn")
    for _cycle in (1, 2):
        assert refused(rig, rig.operator(RivetCdcRunOperator, export="orders_cdc")) == "RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG"
    assert str(rig.state_dir / "ckpt" / "orders.ckpt") in rig.events("rivet.refused")[-1]["text"]
    assert not (rig.state_dir / "ckpt").exists()
    batch, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert batch["decision"] == "success", "an export that is not the stream is not held by its checkpoint"
    (rig.state_dir / "ckpt").mkdir()
    (rig.config_dir / "ckpt" / "orders.ckpt").rename(rig.state_dir / "ckpt" / "orders.ckpt")
    payload, _ = rig.run(rig.operator(RivetCdcRunOperator, export="orders_cdc"))
    assert payload["decision"] == "success"
    assert "checkpoint: ckpt/orders.ckpt" in rig.copy().read_text(), "the path stays relative: it resolves in the state directory"


def test_a_relative_destination_path_is_left_as_written_and_resolves_against_the_working_directory(rig: Rig) -> None:
    rig.config.write_text(rig.config.read_text().replace("    mode: full\n", "    mode: full\n    destination: {type: local, path: ./out/}\n", 1))
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert "path: ./out/" in rig.copy().read_text() and rig.calls()[-1]["cwd"] == str(rig.config_dir.resolve())
    rig.run(rig.operator(RivetRunOperator, export="orders", cwd=str(rig.tmp.resolve())))
    assert rig.calls()[-1]["cwd"] == str(rig.tmp.resolve())


def test_the_config_reader_keeps_yaml_1_1_words_as_strings() -> None:
    doc = _yaml.load("notifications:\n  on: [failure]\nat: 12:30:00\nday: 2026-01-01\nn: 100000\nflag: true\n")
    assert doc == {"notifications": {"on": ["failure"]}, "at": "12:30:00", "day": "2026-01-01", "n": 100000, "flag": True}
    assert _yaml.load(_yaml.dump(doc)) == doc
