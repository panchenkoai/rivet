"""Each scenario of the fake binary: argv, env, exception type, XCom payload, degradations, secrecy."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from conftest import NEW_FLAGS, SECRET, FakeTI, Rig, fixture

from airflow_provider_rivet.invocation import crashed_retry_allowed
from airflow_provider_rivet.operators import (
    RivetApplyOperator,
    RivetCdcRunOperator,
    RivetCompactOperator,
    RivetLoadOperator,
    RivetPlanOperator,
    RivetRunOperator,
    connection_url,
)

OLD_BATCH = ["apply_summary", "lock_wait", "no_notify", "state_url_sqlite"]
METRICS = [{"export_name": "orders", "run_id": "orders_1", "total_rows": 500, "files_produced": 1}]


def entry(name: str, status: str = "success", **more: object) -> dict:
    """One `per_export` entry as today's binary writes it."""
    base = {"export_name": name, "status": status, "run_id": f"{name}_1", "rows": 0, "files": 0, "mode": "full"}
    return {**base, "error_message": "cell value 4111-1111" if status == "failed" else None, **more}


def assert_no_secret(rig: Rig, payload: object) -> None:
    """Neither the URL nor rivet's error text reaches XCom or the structured log."""
    blob = json.dumps(payload) + json.dumps(rig.records)
    assert SECRET not in blob and "s3cr3t" not in blob and "4111-1111" not in blob


def test_apply_success_on_todays_binary(rig: Rig) -> None:
    rig.scenario(metrics={"rows": METRICS})
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    cfg = str(rig.state_dir / "pg.yaml")
    plan = rig.argv("plan")
    artifact = plan[plan.index("--output") + 1]
    assert plan == ["plan", "--config", cfg, "--export", "orders", "--format", "json", "--output", artifact, "--json-errors"]
    assert rig.argv("apply") == ["apply", artifact, "--json-errors"]
    assert rig.argv("metrics") == ["metrics", "--config", cfg, "--export", "orders", "--last", "1", "--json"]
    assert Path(artifact).is_relative_to(rig.state_dir)
    assert Path(cfg).read_text() == rig.config.read_text()
    assert all(c["env"]["RIVET_PG_URL"] == SECRET for c in rig.calls())
    assert all(c["cwd"] == str(rig.config_dir.resolve()) for c in rig.calls())
    assert payload["decision"] == "success" and payload["degraded"] == OLD_BATCH
    assert payload["units"] == [
        {"export": "orders", "table": None, "status": "success", "run_id": "orders_1", "rows": 500, "files": 1,
         "stop_reason": None, "error": None}
    ]
    assert Path(payload["stderr_path"]).is_relative_to(rig.state_dir / "logs")
    assert SECRET in Path(payload["stderr_path"]).read_text()
    assert [w["warning"] for w in rig.events("rivet.warning")] == ["sqlite_single_host"]
    assert_no_secret(rig, payload)


def test_apply_on_a_binary_with_the_contract_flags_passes_them_and_degrades_nothing(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, apply={"summary": {"per_export": [entry("orders", rows=7, files=1, error=None, stop_reason=None, tables=None)]}})
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders", lock_wait=30))
    apply = rig.argv("apply")
    assert apply[2:] == ["--summary-output", apply[3], "--json-errors", "--no-notify", "--lock-wait", "30"]
    assert not any(c["argv"][0] == "metrics" for c in rig.calls())
    assert payload["degraded"] == ["state_url_sqlite"] and payload["units"][0]["rows"] == 7


def test_a_retry_reuses_the_sealed_plan_artifact(rig: Rig) -> None:
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op)
    rig.run(op, FakeTI(try_number=2))
    assert [c["argv"][0] for c in rig.calls()].count("plan") == 1
    assert [c["argv"][0] for c in rig.calls()].count("apply") == 2


def test_a_coded_refusal_fails_without_retry_and_keeps_the_message_out(rig: Rig) -> None:
    line = {"error": "export 'orders': row 4111-1111 refused", "exit_class": 5, "code": "RIVET_SOURCE_CDC_LOG_GAP"}
    rig.scenario(apply={"exit": 5, "line": line})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(exc, AirflowFailException)
    assert str(exc) == "[RIVET_SOURCE_CDC_LOG_GAP] refusal (orders)"
    payload = ti.store[("task", "return_value")]
    assert payload["decision"] == "fail" and "error_object" in payload["degraded"]
    assert payload["units"][0]["error"] == {
        "code": "RIVET_SOURCE_CDC_LOG_GAP", "kind": None, "class": "refusal", "exit_code": 5, "retryable": False, "action": None,
    }
    assert rig.events("rivet.unit")[0]["class"] == "refusal"
    assert_no_secret(rig, payload)


def test_a_contract_refusal_carries_its_action_into_the_exception(rig: Rig) -> None:
    line = fixture("json_errors.json")
    rig.scenario(flags=NEW_FLAGS, run={"exit": 5, "line": line, "summary": {"per_export": [fixture("run_entry_failed.json"), entry("events", "failed")]}})
    exc, ti = rig.run(rig.operator(RivetRunOperator))
    assert isinstance(exc, AirflowFailException) and "restore the missing log" in str(exc)
    payload = ti.store[("task", "return_value")]
    assert [u["error"]["class"] for u in payload["units"]] == ["refusal", "retryable"]
    assert payload["units"][0] == fixture("xcom_unit.json")
    assert "per_unit_error" not in payload["degraded"] and "error_object" not in payload["degraded"]


def test_exit_2_with_an_object_is_retried(rig: Rig) -> None:
    rig.scenario(apply={"exit": 2, "line": {"error": "connection reset by peer", "exit_class": 2}})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowException
    payload = ti.store[("task", "return_value")]
    assert payload["decision"] == "retry" and payload["error"]["retryable"] is True


def test_exit_2_without_an_object_is_not_retried(rig: Rig) -> None:
    rig.scenario(apply={"exit": 2, "stderr": "error: unexpected argument '--bogus' found"})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders", extra_args=["--bogus"]))
    assert isinstance(exc, AirflowFailException)
    assert "--bogus" in rig.argv("apply")
    error = ti.store[("task", "return_value")]["error"]
    assert (error["class"], error["exit_code"], error["retryable"]) == ("generic", 1, False)


def test_exit_101_is_internal_and_not_retried(rig: Rig) -> None:
    rig.scenario(apply={"exit": 101, "stderr": "thread 'main' panicked"})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(exc, AirflowFailException)
    assert ti.store[("task", "return_value")]["error"]["class"] == "internal"


@pytest.mark.parametrize("status", [1, 3, 4, 6])
def test_exits_1_3_4_6_fail_without_retry(rig: Rig, status: int) -> None:
    rig.scenario(apply={"exit": status, "line": {"error": "boom", "exit_class": status}})
    exc, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(exc, AirflowFailException)


def test_a_kill_is_retried_once_under_its_own_budget_across_tries(rig: Rig) -> None:
    rig.scenario(apply={"signal": "SIGKILL"})
    op = rig.operator(RivetApplyOperator, export="orders")
    first, ti1 = rig.run(op, FakeTI(try_number=1, max_tries=2))
    second, ti2 = rig.run(op, FakeTI(try_number=2, max_tries=2))
    assert type(first) is AirflowException and isinstance(second, AirflowFailException)
    one, two = ti1.store[("task", "return_value")], ti2.store[("task", "return_value")]
    assert (one["signal"], one["exit_status"], one["error"]["class"], one["crashed_retry"]) == (9, None, "crashed", True)
    assert two["crashed_retry"] is False and two["error"]["retryable"] is True
    assert [e["retry"] for e in rig.events("rivet.crashed")] == [True, False]
    cleared, _ = rig.run(op, FakeTI(try_number=3, max_tries=4))
    assert type(cleared) is AirflowException, "a manual clear starts a new series with a fresh crashed budget"


def test_a_transient_failure_does_not_spend_the_crashed_budget(rig: Rig) -> None:
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.scenario(apply={"exit": 2, "line": {"error": "reset", "exit_class": 2}})
    rig.run(op, FakeTI(try_number=1))
    rig.scenario(apply={"signal": "SIGKILL"})
    crashed, _ = rig.run(op, FakeTI(try_number=2))
    assert type(crashed) is AirflowException


def test_the_crashed_budget_without_a_state_directory_counts_tries(tmp_path: Path) -> None:
    assert crashed_retry_allowed(None, 1, 2, 2, 1) is True
    assert crashed_retry_allowed(None, 2, 2, 2, 1) is False
    assert crashed_retry_allowed(None, 3, 4, 2, 1) is True
    assert crashed_retry_allowed(None, 1, 2, 2, 0) is False
    ledger = tmp_path / "x" / "ledger.json"
    assert [crashed_retry_allowed(ledger, t, 5, 5, 2) for t in (1, 2, 3)] == [True, True, False]


def test_exit_137_reported_as_a_number_is_crashed(rig: Rig) -> None:
    rig.scenario(apply={"exit": 137})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowException and ti.store[("task", "return_value")]["error"]["class"] == "crashed"


def test_one_failed_and_one_successful_export_in_one_summary(rig: Rig) -> None:
    summary = {"per_export": [entry("users", rows=500, files=1), entry("orders", "failed")]}
    rig.scenario(run={"exit": 1, "line": {"error": "db error: relation missing", "exit_class": 1}, "summary": summary})
    exc, ti = rig.run(rig.operator(RivetRunOperator, extra_args=["--reconcile"]))
    run = rig.argv("run")
    assert run[:3] == ["run", "--config", str(rig.state_dir / "pg.yaml")] and "--export" not in run
    assert run[3] == "--summary-output" and run[5:] == ["--reconcile", "--json-errors"]
    assert not Path(run[4]).exists(), "the summary file is removed after it is read"
    assert isinstance(exc, AirflowFailException) and str(exc) == "generic (orders)"
    payload = ti.store[("task", "return_value")]
    assert [(u["export"], u["status"], u["rows"]) for u in payload["units"]] == [("users", "success", 500), ("orders", "failed", 0)]
    assert payload["units"][0]["error"] is None and payload["units"][1]["error"]["class"] == "generic"
    assert len(rig.events("rivet.unit")) == 2
    assert_no_secret(rig, payload)


def test_two_failed_exports_share_the_process_object_and_say_so(rig: Rig) -> None:
    summary = {"per_export": [entry("users", "failed"), entry("orders", "failed")]}
    rig.scenario(run={"exit": 2, "line": {"error": "2 export(s) failed", "exit_class": 2}, "summary": summary})
    _, ti = rig.run(rig.operator(RivetRunOperator))
    assert "per_unit_error" in ti.store[("task", "return_value")]["degraded"]


def test_cdc_max_events_succeeds_and_is_reported(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": [fixture("run_entry.json")]}})
    payload, _ = rig.run(rig.operator(RivetCdcRunOperator, export="orders_cdc"))
    assert payload["decision"] == "success" and payload["units"][0]["stop_reason"] == "max_events"
    assert rig.events("rivet.cdc.max_events")[0]["exports"] == ["orders_cdc"]
    assert len([c for c in rig.calls() if c["argv"][0] == "run"]) == 1, "the task does not loop"
    assert "stop_reason" not in payload["degraded"]


def test_cdc_on_todays_binary_cannot_tell_the_stop_reason_and_says_so(rig: Rig) -> None:
    rig.scenario(run={"summary": {"per_export": [entry("orders_cdc", mode="cdc", rows=1200, files=3)]}})
    payload, _ = rig.run(rig.operator(RivetCdcRunOperator, export="orders_cdc"))
    assert payload["units"][0]["stop_reason"] is None and "stop_reason" in payload["degraded"]
    assert not rig.events("rivet.cdc.max_events")


def test_load_on_todays_binary_runs_a_single_export_copy_of_the_config(rig: Rig) -> None:
    payload, _ = rig.run(rig.operator(RivetLoadOperator, export="users", table="users"))
    narrowed = rig.state_dir / "pg--users.yaml"
    assert rig.argv("load") == ["load", "--config", str(narrowed), "--json-errors"]
    text = narrowed.read_text()
    assert "name: users" in text and "name: orders" not in text and "url_env: RIVET_PG_URL" in text
    assert payload["units"] == [
        {"export": "users", "table": "users", "status": "loaded", "run_id": None, "rows": None, "files": None,
         "stop_reason": None, "error": None}
    ]
    assert payload["degraded"] == ["load_filter", "load_result", "load_table_filter", "lock_wait", "no_notify", "state_url_sqlite"]
    assert (rig.state_dir / "airflow" / "locks" / "users.lock").exists()


def test_load_on_a_contract_binary_filters_by_flag_and_reads_the_result(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, load={"exit": 2, "line": fixture("json_errors_load.json"), "summary": fixture("load_result.json")})
    exc, ti = rig.run(rig.operator(RivetLoadOperator, export="events", table="events"))
    load = rig.argv("load")
    assert load[:7] == ["load", "--config", str(rig.state_dir / "pg.yaml"), "--export", "events", "--table", "events"]
    assert load[7] == "--summary-output" and load[9:] == ["--json-errors", "--no-notify", "--lock-wait", "600"]
    assert type(exc) is AirflowException
    payload = ti.store[("task", "return_value")]
    assert [(u["table"], u["status"], u["error"]["class"]) for u in payload["units"]] == [("events", "failed", "retryable")]
    assert payload["degraded"] == ["state_url_sqlite"]


def test_a_load_that_is_up_to_date_is_a_success_so_compact_still_runs(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, load={"summary": fixture("load_result.json")})
    payload, _ = rig.run(rig.operator(RivetLoadOperator, export="countries", table="countries"))
    assert payload["units"][0]["status"] == "skipped" and payload["skip_reason"] == "up_to_date"


def test_a_compaction_rivet_reports_as_skipped_is_an_airflow_skip(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, compact={"summary": fixture("compact_result.json")})
    exc, ti = rig.run(rig.operator(RivetCompactOperator, export="countries", table="countries"))
    assert isinstance(exc, AirflowSkipException) and "full_load" in str(exc)
    assert ti.store[("task", "return_value")]["skip_reason"] == "full_load"
    done, _ = rig.run(rig.operator(RivetCompactOperator, export="orders_cdc", table="orders"))
    assert done["units"][0]["status"] == "compacted" and done["units"][0]["rows"] == 1200


def test_plan_swaps_the_layout_file_in_only_when_it_is_a_plan_list(rig: Rig) -> None:
    target = rig.tmp / "pg.plan.json"
    target.write_text("[\"old\"]")
    rig.scenario(plan_list=[{"export_name": "orders"}])
    payload, _ = rig.run(rig.operator(RivetPlanOperator, plan_file=str(target)))
    assert payload["decision"] == "success" and json.loads(target.read_text()) == [{"export_name": "orders"}]
    rig.scenario(plan={"exit": 1, "line": {"error": "bad config", "exit_class": 1}})
    exc, _ = rig.run(rig.operator(RivetPlanOperator, plan_file=str(target)))
    assert isinstance(exc, AirflowFailException) and json.loads(target.read_text()) == [{"export_name": "orders"}]


def test_stderr_reaches_the_task_log_only_on_request(rig: Rig) -> None:
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert not rig.events("rivet.stderr")
    rig.run(rig.operator(RivetRunOperator, export="orders", stream_stderr=True))
    assert any(SECRET in e["line"] for e in rig.events("rivet.stderr"))


def test_a_connection_becomes_an_env_url_and_is_never_a_rendered_field(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = SimpleNamespace(conn_type="postgres", login="rivet", password="p@ss/w:rd", host="db", port=5432, schema="app",
                           extra_dejson={"rivet_params": "sslmode=require"})
    assert connection_url(conn) == "postgresql://rivet:p%40ss%2Fw%3Ard@db:5432/app?sslmode=require"
    assert connection_url(SimpleNamespace(conn_type="mssql", login=None, password=None, host="h", port=None, schema="d",
                                          extra_dejson={})) == "sqlserver://h/d"
    generic = SimpleNamespace(conn_id="rivet_pg", conn_type="generic", login="u", password="p", host="h", port=1, schema="d", extra_dejson={})
    assert connection_url(generic, "postgresql") == "postgresql://u:p@h:1/d"
    monkeypatch.setattr("airflow_provider_rivet.operators.BaseHook.get_connection", lambda conn_id: generic)
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders", env={}, env_from_connections={"RIVET_PG_URL": "rivet_pg"}))
    assert isinstance(exc, AirflowFailException) and not rig.calls(), "a connection type that names no rivet scheme is refused"
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_CONNECTION_SCHEME"
    explicit = rig.operator(RivetRunOperator, export="orders", env={}, env_from_connections={"RIVET_PG_URL": ("rivet_pg", "postgresql")})
    rig.run(explicit)
    assert rig.calls()[-1]["env"]["RIVET_PG_URL"] == "postgresql://u:p@h:1/d"
    monkeypatch.setattr("airflow_provider_rivet.operators.BaseHook.get_connection", lambda conn_id: conn)
    op = rig.operator(RivetRunOperator, export="orders", env={}, env_from_connections={"RIVET_PG_URL": "rivet_pg"})
    payload, _ = rig.run(op)
    assert rig.calls()[-1]["env"]["RIVET_PG_URL"].startswith("postgresql://rivet:p%40ss")
    assert "env_from_connections" not in op.template_fields
    assert "p%40ss" not in json.dumps(payload) + json.dumps(rig.records)


def test_an_operator_that_needs_an_export_refuses_to_be_built_without_one() -> None:
    with pytest.raises(ValueError, match="needs `export`"):
        RivetApplyOperator(task_id="t", config="c.yaml")
