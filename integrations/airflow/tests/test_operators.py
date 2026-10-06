"""Each scenario of the fake binary: argv, env, exception type, XCom payload, degradations, secrecy."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from conftest import NEW_FLAGS, SECRET, FakeTI, Rig, fixture

from airflow_provider_rivet import invocation
from airflow_provider_rivet.invocation import ENV_ALLOWLIST, crashed_retry_allowed
from airflow_provider_rivet.dags import read_plan_layout
from airflow_provider_rivet.operators import (
    RivetApplyOperator,
    RivetBaseOperator,
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


def mode(path: object) -> int:
    """The permission bits of a file or directory."""
    return stat.S_IMODE(os.stat(str(path)).st_mode)


def assert_no_secret(rig: Rig, payload: object) -> None:
    """Neither the URL nor rivet's error text reaches XCom or the structured log."""
    blob = json.dumps(payload) + json.dumps(rig.records)
    assert SECRET not in blob and "s3cr3t" not in blob and "4111-1111" not in blob


def test_apply_success_on_todays_binary(rig: Rig) -> None:
    rig.scenario(metrics={"rows": METRICS})
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    cfg = str(rig.copy())
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
    assert mode(payload["stderr_path"]) == 0o600 and mode(rig.copy()) == 0o600
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


def test_the_crashed_budget_without_a_state_directory_counts_the_first_tries_only() -> None:
    assert crashed_retry_allowed(None, 1, 2, 1) == (True, None)
    assert crashed_retry_allowed(None, 2, 2, 1) == (False, None)
    assert crashed_retry_allowed(None, 3, 4, 1) == (False, None), "with no ledger a clear gets no fresh crashed budget"
    assert crashed_retry_allowed(None, 1, 2, 0) == (False, None)


def test_the_crashed_ledger_counts_one_series_and_every_failure_of_it_fails_closed(tmp_path: Path) -> None:
    ledger = tmp_path / "x" / "ledger.json"
    assert [crashed_retry_allowed(ledger, t, 5, 2) for t in (1, 2, 3)] == [(True, None), (True, None), (False, None)]
    assert crashed_retry_allowed(ledger, 5, 7, 2) == (True, None), "a clear moves max_tries and starts a new series"
    for junk in ("{not json", "[1, 2]", '[[1, "x"]]', '{"a": 1}', ""):
        ledger.write_text(junk)
        first, second = crashed_retry_allowed(ledger, 1, 9, 2), crashed_retry_allowed(ledger, 2, 9, 2)
        assert first[0] is False and second[0] is False and "unreadable" in first[1] and first[1] == second[1], junk
        assert ledger.read_text() == junk, "a ledger that cannot be read is left for a person to look at"
    blocked = tmp_path / "file"
    blocked.write_text("")
    allowed, problem = crashed_retry_allowed(blocked / "crashed" / "ledger.json", 1, 2, 1)
    assert allowed is False and "cannot be written" in problem


def test_a_crashed_ledger_that_cannot_be_written_grants_no_retry_and_still_publishes(rig: Rig) -> None:
    rig.scenario(apply={"signal": "SIGKILL"})
    (rig.state_dir / "airflow" / "dag").mkdir(parents=True)
    (rig.state_dir / "airflow" / "dag" / "crashed").write_text("not a directory")
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(exc, AirflowFailException), exc
    payload = ti.store[("task", "return_value")]
    assert payload["crashed_retry"] is False and "crashed_ledger" in payload["degraded"]
    assert "cannot be written" in rig.events("rivet.crashed")[0]["ledger_problem"]


def test_lowering_retries_in_the_middle_of_a_series_does_not_reset_the_crashed_budget(rig: Rig) -> None:
    rig.scenario(apply={"signal": "SIGKILL"})
    op = rig.operator(RivetApplyOperator, export="orders")
    op.retries = 5
    first, _ = rig.run(op, FakeTI(try_number=1, max_tries=5))
    op.retries = 1
    second, _ = rig.run(op, FakeTI(try_number=2, max_tries=5))
    assert type(first) is AirflowException and isinstance(second, AirflowFailException)


def test_a_crash_on_a_worker_without_a_state_directory_says_the_budget_is_not_tracked(rig: Rig) -> None:
    rig.scenario(run={"signal": "SIGKILL"})
    pg = {"RIVET_PG_URL": SECRET, "RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"}
    op = rig.operator(RivetRunOperator, export="orders", state_dir=None, env=pg)
    first, ti = rig.run(op, FakeTI(try_number=1))
    second, _ = rig.run(op, FakeTI(try_number=2))
    assert type(first) is AirflowException and isinstance(second, AirflowFailException)
    assert "crashed_ledger" in ti.store[("task", "return_value")]["degraded"]


def test_exit_137_reported_as_a_number_is_crashed(rig: Rig) -> None:
    rig.scenario(apply={"exit": 137})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowException and ti.store[("task", "return_value")]["error"]["class"] == "crashed"


def test_one_failed_and_one_successful_export_in_one_summary(rig: Rig) -> None:
    summary = {"per_export": [entry("users", rows=500, files=1), entry("orders", "failed")]}
    rig.scenario(run={"exit": 1, "line": {"error": "db error: relation missing", "exit_class": 1}, "summary": summary})
    exc, ti = rig.run(rig.operator(RivetRunOperator, extra_args=["--reconcile"]))
    run = rig.argv("run")
    assert run[:3] == ["run", "--config", str(rig.copy())] and "--export" not in run
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
    narrowed = rig.copy("users")
    assert rig.argv("load") == ["load", "--config", str(narrowed), "--json-errors"]
    text = narrowed.read_text()
    assert "name: users" in text and "name: orders" not in text and "url_env: RIVET_PG_URL" in text
    assert payload["units"] == [
        {"export": "users", "table": "users", "status": "loaded", "run_id": None, "rows": None, "files": None,
         "stop_reason": None, "error": None}
    ]
    assert payload["degraded"] == ["load_filter", "load_result", "load_table_filter", "lock_wait", "no_notify", "state_url_sqlite"]
    assert (rig.state_dir / "airflow" / "locks" / "users.lock").exists()


HOLD_LOCK = """
import fcntl, sys, time, pathlib
with open(sys.argv[1], "w") as held:
    fcntl.flock(held, fcntl.LOCK_EX)
    print("held", flush=True)
    time.sleep(1.0)
    pathlib.Path(sys.argv[2]).write_text("released")
"""


def test_the_per_table_tasks_of_one_export_wait_for_the_export_lock(rig: Rig) -> None:
    locks = rig.state_dir / "airflow" / "locks"
    locks.mkdir(parents=True)
    released = rig.tmp / "released"
    holder = subprocess.Popen([sys.executable, "-c", HOLD_LOCK, str(locks / "users.lock"), str(released)], stdout=subprocess.PIPE)
    assert holder.stdout.readline().strip() == b"held"
    payload, _ = rig.run(rig.operator(RivetLoadOperator, export="users", table="users"))
    assert released.exists(), "the load must wait while another task holds the export's lock"
    assert holder.wait() == 0 and payload["decision"] == "success" and rig.argv("load")


def test_load_on_a_contract_binary_filters_by_flag_and_reads_the_result(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, load={"exit": 2, "line": fixture("json_errors_load.json"), "summary": fixture("load_result.json")})
    exc, ti = rig.run(rig.operator(RivetLoadOperator, export="events", table="events"))
    load = rig.argv("load")
    assert load[:7] == ["load", "--config", str(rig.copy()), "--export", "events", "--table", "events"]
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


def test_plan_swaps_the_layout_file_in_only_when_the_builder_can_read_it(rig: Rig) -> None:
    target = rig.tmp / "pg.plan.json"
    target.write_text("[\"old\"]")
    target.chmod(0o640)
    campaign = {"waves": [{"wave": 1, "exports": ["orders"]}], "ordered_exports": [{"export_name": "orders", "cost_class": "low"}]}
    layout = [{"export_name": "orders", "prioritization": {"campaign": campaign}}]
    for unusable in ([], [{"export_name": "orders"}], {"not": "a list"}, [{"prioritization": {"campaign": {"waves": "x"}}}]):
        rig.scenario(plan_list=unusable)
        exc, ti = rig.run(rig.operator(RivetPlanOperator, plan_file=str(target)))
        assert isinstance(exc, AirflowFailException) and json.loads(target.read_text()) == ["old"], unusable
        assert ti.store[("task", "return_value")]["error"]["class"] == "internal"
    rig.scenario(plan_list=layout)
    payload, _ = rig.run(rig.operator(RivetPlanOperator, plan_file=str(target)))
    assert payload["decision"] == "success" and json.loads(target.read_text()) == layout
    assert read_plan_layout(str(target)) == [{"wave": 1, "exports": ["orders"], "heavy": []}]
    assert mode(target) == 0o640, "the layout file keeps the mode the DAG processor reads it with"
    rig.scenario(plan={"exit": 1, "line": {"error": "bad config", "exit_class": 1}})
    exc, _ = rig.run(rig.operator(RivetPlanOperator, plan_file=str(target)))
    assert isinstance(exc, AirflowFailException) and json.loads(target.read_text()) == layout


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


def test_an_upstream_that_published_no_xcom_does_not_break_the_task(rig: Rig) -> None:
    op = rig.operator(RivetApplyOperator, export="orders")
    op.upstream_task_ids.add("apply.wave_1_done")
    payload, _ = rig.run(op)
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


def test_without_a_state_directory_stderr_goes_to_a_private_file_and_never_to_the_task_log(
    rig: Rig, capfd: pytest.CaptureFixture, own_temp_dir: Path
) -> None:
    pg = {"RIVET_PG_URL": SECRET, "RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"}
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", state_dir=None, env=pg))
    streams = capfd.readouterr()
    assert SECRET not in streams.err + streams.out, "raw stderr must not reach the worker process's own stderr"
    path = Path(payload["stderr_path"])
    assert path.is_relative_to(own_temp_dir / f"rivet-airflow-{os.getuid()}" / "logs" / "dag" / invocation.hashed_name("task"))
    assert SECRET in path.read_text() and mode(path) == 0o600
    assert all(mode(d) == 0o700 for d in (path.parent, own_temp_dir / f"rivet-airflow-{os.getuid()}"))
    assert "stderr_temp" in payload["degraded"]
    assert rig.events("rivet.start")[0]["stderr_path"] == str(path)
    assert_no_secret(rig, payload)
    kept, _ = rig.run(rig.operator(RivetRunOperator, export="orders", env=pg, stderr_dir=str(rig.tmp / "mine")))
    assert Path(kept["stderr_path"]).is_relative_to(rig.tmp / "mine") and "stderr_temp" not in kept["degraded"]
    (own_temp_dir / f"rivet-airflow-{os.getuid()}").chmod(0o755)
    wary, _ = rig.run(rig.operator(RivetRunOperator, export="orders", state_dir=None, env=pg))
    elsewhere = Path(wary["stderr_path"])
    assert not elsewhere.is_relative_to(own_temp_dir / f"rivet-airflow-{os.getuid()}"), "a directory others can read is not used"
    assert mode(elsewhere) == 0o600 and mode(elsewhere.parents[3]) == 0o700


def test_log_files_are_private_and_only_the_last_tries_of_a_task_are_kept(rig: Rig) -> None:
    op = rig.operator(RivetApplyOperator, export="orders", log_keep=2)
    paths = [Path(rig.run(op, FakeTI(try_number=n))[0]["stderr_path"]) for n in (1, 2, 3)]
    assert [p.exists() for p in paths] == [False, True, True]
    folder = paths[0].parent
    assert folder == rig.state_dir / "logs" / "dag" / invocation.hashed_name("task") and folder.name == "task-9aae63ebb719"
    assert not list(folder.glob("*try1.*")) and len(list(folder.glob("*try3.*"))) >= 3, "a try's files go together"
    assert all(mode(f) == 0o600 for f in folder.iterdir())
    assert all(mode(d) == 0o700 for d in (folder, rig.state_dir / "logs", rig.state_dir / "airflow", rig.state_dir / "airflow" / "dag"))
    other = rig.operator(RivetRunOperator, task_id="other", export="users", log_keep=2)
    rig.run(other, FakeTI(task_id="other"))
    assert all(p.exists() for p in paths[1:]), "another task's files do not count against this one"
    everything = rig.operator(RivetRunOperator, task_id="all", export="users", log_keep=None)
    kept = [Path(rig.run(everything, FakeTI(task_id="all", try_number=n))[0]["stderr_path"]) for n in range(1, 13)]
    assert all(p.exists() for p in kept)
    default = rig.operator(RivetRunOperator, task_id="ten", export="users")
    tens = [Path(rig.run(default, FakeTI(task_id="ten", try_number=n))[0]["stderr_path"]) for n in range(1, 13)]
    assert [p.exists() for p in tens] == [False, False] + [True] * 10


def test_the_subprocess_gets_the_allow_list_and_what_the_operator_declares_and_nothing_else(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    secrets = {"AIRFLOW__CORE__FERNET_KEY": "k", "MY_DB_PASSWORD": "p", "AWS_SECRET_ACCESS_KEY": "a",
               "GOOGLE_APPLICATION_CREDENTIALS": "/g.json", "AZURE_CLIENT_SECRET": "z", "RIVET_OTHER_URL": "u",
               "HOMEBREW_GITHUB_API_TOKEN": "h", "LANGSMITH_API_KEY": "l", "PATH_SECRET": "s", "TZ_TOKEN": "t"}
    allowed = {"TZ": "UTC", "LC_ALL": "C", "HTTPS_PROXY": "http://proxy:3128", "SSL_CERT_FILE": "/ca.pem"}
    for name, value in {**secrets, **allowed}.items():
        monkeypatch.setenv(name, value)
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    keys = set(rig.calls()[-1]["env_keys"])
    assert {"PATH", "HOME", "RIVET_PG_URL", *allowed} <= keys
    assert not keys & set(secrets), "a variable of the worker that is not on the allow-list must not reach rivet"
    wide = rig.operator(RivetRunOperator, export="orders", env_passthrough=["RIVET_OTHER_*", "MY_DB_PASSWORD"], cloud_credentials=["aws", "gcp"])
    rig.run(wide)
    keys = set(rig.calls()[-1]["env_keys"])
    assert {"RIVET_OTHER_URL", "MY_DB_PASSWORD", "AWS_SECRET_ACCESS_KEY", "GOOGLE_APPLICATION_CREDENTIALS"} <= keys
    assert not keys & {"AZURE_CLIENT_SECRET", "AIRFLOW__CORE__FERNET_KEY", "HOMEBREW_GITHUB_API_TOKEN", "PATH_SECRET"}
    with pytest.raises(ValueError, match="cloud_credentials"):
        RivetRunOperator(task_id="t", config="c.yaml", cloud_credentials=["gcs"])
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    assert all(f"`{name}`" in readme for name in ENV_ALLOWLIST), "README lists the allow-list"


def test_env_is_not_a_templated_field() -> None:
    assert "env" not in RivetBaseOperator.template_fields and "env_from_connections" not in RivetBaseOperator.template_fields


def test_exit_0_with_a_failed_unit_in_the_summary_is_a_failed_task(rig: Rig) -> None:
    rig.scenario(run={"exit": 0, "summary": {"per_export": [entry("users", rows=5), entry("orders", "failed")]}})
    exc, ti = rig.run(rig.operator(RivetRunOperator))
    assert isinstance(exc, AirflowFailException) and str(exc) == "generic (orders)"
    payload = ti.store[("task", "return_value")]
    assert payload["decision"] == "fail" and "exit_zero_failed_unit" in payload["degraded"]
    assert payload["error"] == payload["units"][1]["error"] and payload["error"]["class"] == "generic"
    assert rig.events("rivet.result")[0]["decision"] == "fail"


def test_a_task_with_several_failed_units_and_exit_0_is_classified_by_the_worst(rig: Rig) -> None:
    retry = {"class": "retryable", "retryable": True, "exit_code": 2, "code": None, "kind": None, "action": None, "message": "m"}
    refusal = {**retry, "class": "refusal", "retryable": False, "exit_code": 5, "code": "RIVET_X"}
    mixed = [entry("users", "failed", error=retry), entry("orders", "failed", error=refusal), entry("events", "failed", error=retry)]
    rig.scenario(flags=NEW_FLAGS, run={"exit": 0, "summary": {"per_export": mixed}})
    exc, ti = rig.run(rig.operator(RivetRunOperator))
    assert isinstance(exc, AirflowFailException) and ti.store[("task", "return_value")]["error"]["class"] == "refusal"
    rig.scenario(flags=NEW_FLAGS, run={"exit": 0, "summary": {"per_export": [mixed[0], mixed[2]]}})
    exc, ti = rig.run(rig.operator(RivetRunOperator))
    assert type(exc) is AirflowException and ti.store[("task", "return_value")]["decision"] == "retry"
    rig.scenario(flags=NEW_FLAGS, load={"exit": 0, "summary": {"run_id": "r", "per_table": [
        {"export": "users", "table": "users", "status": "failed", "rows": None, "error": refusal, "skip_reason": None}]}})
    exc, _ = rig.run(rig.operator(RivetLoadOperator, export="users", table="users"))
    assert isinstance(exc, AirflowFailException)


def test_a_failures_entry_without_retryable_and_junk_in_the_summary_do_not_raise(rig: Rig) -> None:
    line = {**fixture("json_errors.json"), "failures": [{"export": "orders", "table": None, "class": "refusal"}, "junk", None]}
    summary = {"per_export": ["junk", entry("orders", "failed", error={"class": "refusal"}), entry("users", 7), None]}
    rig.scenario(flags=NEW_FLAGS, run={"exit": 5, "line": line, "summary": summary})
    exc, ti = rig.run(rig.operator(RivetRunOperator))
    assert isinstance(exc, AirflowFailException), exc
    payload = ti.store[("task", "return_value")]
    assert "result_malformed" in payload["degraded"] and payload["error"]["class"] == "refusal"
    assert [(u["export"], u["status"], u["error"]["class"]) for u in payload["units"]] == [("orders", "failed", "refusal"), ("users", "failed", "refusal")]


@pytest.mark.parametrize(
    "line",
    [
        {"error": "x", "exit_class": 2, "class": "retryable"},
        {"error": "x", "exit_class": 2, "class": "retryable", "retryable": "true", "exit_code": 2},
        {"error": "x", "exit_class": 2, "class": "bogus", "retryable": True, "exit_code": 2},
        {"error": "x", "exit_class": 2, "class": "retryable", "retryable": True, "exit_code": "2"},
        {"error": "x", "exit_class": "two"},
        {"error": "x", "exit_class": 5},
    ],
    ids=["no-retryable", "retryable-as-text", "unknown-class", "exit-code-as-text", "exit-class-as-text", "exit-class-differs"],
)
def test_a_malformed_error_object_falls_back_to_the_exit_table_and_says_so(rig: Rig, line: dict) -> None:
    rig.scenario(apply={"exit": 2, "line": line})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(exc, AirflowFailException), exc
    payload = ti.store[("task", "return_value")]
    assert (payload["error"]["class"], payload["error"]["exit_code"], payload["error"]["retryable"]) == ("generic", 1, False)
    assert "result_malformed" in payload["degraded"]


def test_an_expired_sealed_plan_is_planned_again(rig: Rig) -> None:
    rig.scenario(plan_expires_hours=-1)
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op)
    rig.run(op, FakeTI(try_number=2))
    assert [c["argv"][0] for c in rig.calls()].count("plan") == 2


def test_the_error_object_is_found_behind_trailing_stderr_lines(rig: Rig) -> None:
    rig.scenario(apply={"exit": 2, "line": {"error": "connection reset by peer", "exit_class": 2}, "stderr_after": 300})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowException and ti.store[("task", "return_value")]["decision"] == "retry"


def test_connection_url_keeps_a_lone_password_brackets_ipv6_and_reads_the_oracle_service_name() -> None:
    def conn(**fields: object) -> SimpleNamespace:
        base = {"conn_id": "c", "conn_type": "mysql", "login": None, "password": None, "host": "h", "port": None,
                "schema": "d", "extra_dejson": {}}
        return SimpleNamespace(**{**base, **fields})

    assert connection_url(conn(password="p@ss")) == "mysql://:p%40ss@h/d"
    assert connection_url(conn(host="::1", port=3306, schema="my db#1")) == "mysql://[::1]:3306/my%20db%231"
    assert connection_url(conn(host="[fe80::1]", port=3306)) == "mysql://[fe80::1]:3306/d"
    oracle = {"conn_type": "oracle", "login": "u", "password": "p", "port": 1521, "schema": "HR"}
    assert connection_url(conn(**oracle, extra_dejson={"service_name": "ORCLPDB1"})) == "oracle://u:p@h:1521/ORCLPDB1"
    assert connection_url(conn(**oracle)) == "oracle://u:p@h:1521/HR"


def test_a_declared_env_value_wins_over_the_workers_value_for_the_same_name(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TZ", "UTC")
    monkeypatch.setenv("RIVET_PG_URL", "postgresql://worker-value/db")
    rig.scenario(record_env=["RIVET_PG_URL", "TZ"])
    op = rig.operator(RivetRunOperator, export="orders", env={"RIVET_PG_URL": SECRET, "TZ": "Asia/Tokyo"}, env_passthrough=["RIVET_PG_URL"])
    rig.run(op)
    assert rig.calls()[-1]["env"] == {"RIVET_PG_URL": SECRET, "TZ": "Asia/Tokyo"}


def test_an_older_wider_log_file_and_directory_are_made_private(rig: Rig) -> None:
    for name in ("logs", "airflow"):
        (rig.state_dir / name).mkdir()
        (rig.state_dir / name).chmod(0o777)
    op = rig.operator(RivetRunOperator, export="orders")
    first = Path(rig.run(op)[0]["stderr_path"])
    for wide in (first, first.parent, first.parent.parent):
        wide.chmod(0o777 if wide.is_dir() else 0o666)
    again = Path(rig.run(op)[0]["stderr_path"])
    assert again == first and mode(first) == 0o600, "a file of the same name left wider by an earlier writer is tightened"
    assert [mode(d) for d in (first.parent, first.parent.parent, rig.state_dir / "logs", rig.state_dir / "airflow")] == [0o700] * 4


def test_a_path_name_is_readable_and_never_shared_by_two_different_names() -> None:
    name = invocation.path_name
    assert [name(n) for n in ("orders", "apply.orders_cdc", "manual__2026-10-06", "9x")] == ["orders", "apply.orders_cdc", "manual__2026-10-06", "9x"]
    cyrillic = ("\u0437\u0430\u043a\u0430\u0437\u044b", "\u043a\u043b\u0438\u0435\u043d\u0442\u044b")
    hostile = ["a:b", "a+b", "a_b", "a b", "A_b", "", ".", "..", ".hidden", "-x", "x" * 300, "x" * 301, *cyrillic]
    hostile += [name(n) for n in ("a:b", *cyrillic)] + ["a/../../b", "a\x00b", "a\nb"]
    made = [name(n) for n in hostile]
    assert len({m.lower() for m in made}) == len(hostile), "also on a file system that ignores case"
    assert all(invocation._SAFE.sub("_", m) == m and m not in (".", "..") and 0 < len(m.encode()) < 120 for m in made)
    assert name("a:b").startswith("a_b-") and name("a_b") == "a_b"
    assert name("run", "task") != name("run__task") and name("a__b", "c") != name("a", "b__c")
    assert name("load", "1") != name("load-1") and name("orders") == name("orders")
    assert name("a_", "_b") != name("a__", "b"), "the hash is over the list of parts, not over their concatenation"


COLLIDING = [
    ("\u0437\u0430\u043a\u0430\u0437\u044b", "\u043a\u043b\u0438\u0435\u043d\u0442\u044b"),
    ("a:b", "a+b"),
    ("a b", "a_b"),
    ("Orders", "orders"),
]


@pytest.mark.parametrize("names", COLLIDING, ids=["non-ascii", "punctuation", "space", "case"])
def test_two_exports_whose_names_sanitise_to_one_get_their_own_plan_and_are_both_extracted(rig: Rig, names: tuple) -> None:
    one, two = names
    exports = "".join(f"  - {{name: {json.dumps(n)}, query: SELECT 1, mode: full}}\n" for n in names)
    rig.config.write_text(f"source:\n  type: postgres\n  url_env: RIVET_PG_URL\nexports:\n{exports}")
    for n in names:
        payload, _ = rig.run(rig.operator(RivetApplyOperator, task_id="t", export=n))
        assert payload["decision"] == "success" and [u["export"] for u in payload["units"]] == [n]
    steps = [(c["argv"][0], c["export"]) for c in rig.calls() if c["argv"][0] != "metrics"]
    assert steps == [("plan", one), ("apply", one), ("plan", two), ("apply", two)], "each export is planned and applies its own plan"
    artifacts = [c["argv"][1] for c in rig.calls() if c["argv"][0] == "apply"]
    assert artifacts[0].lower() != artifacts[1].lower()
    assert [json.loads(Path(a).read_text())["export_name"] for a in artifacts] == [one, two]
    for n in names:
        rig.run(rig.operator(RivetLoadOperator, task_id="t", export=n))
    narrowed = [c["argv"][2] for c in rig.calls() if c["argv"][0] == "load"]
    assert narrowed[0].lower() != narrowed[1].lower(), "the single-export copies of the config are separate files"
    assert [c["export"] for c in rig.calls() if c["argv"][0] == "load"] == [one, two]


@pytest.mark.parametrize(
    "ids",
    [
        [{"dag_id": "\u043f\u0440\u043e\u0434\u0430\u0436\u0438"}, {"dag_id": "\u0441\u043a\u043b\u0430\u0434"}],
        [{"dag_id": "Sales"}, {"dag_id": "sales"}],
        [{"task_id": "load.\u0437\u0430\u043a\u0430\u0437\u044b"}, {"task_id": "load.\u043a\u043b\u0438\u0435\u043d\u0442\u044b"}],
        [{"task_id": "load", "map_index": 1}, {"task_id": "load-1"}],
        [{"task_id": "load", "map_index": 1}, {"task_id": "load.1"}],
        [{"task_id": "load", "map_index": 1}, {"task_id": "load_1"}],
        [{"task_id": "load", "map_index": 1}, {"task_id": "load", "map_index": 11}],
        [{"task_id": "apply_a"}, {"task_id": "apply_b"}],
        [{"run_id": "manual__2026-10-06T00:00:00+00:00"}, {"run_id": "manual__2026-10-06T00_00_00_00_00"}],
    ],
    ids=["dag-non-ascii", "dag-case", "task-non-ascii", "mapped-task", "mapped-task-dot", "mapped-task-underscore",
         "mapped-indexes", "two-tasks", "run-id"],
)
def test_two_dags_tasks_or_runs_whose_ids_sanitise_to_one_share_no_artifact_log_or_ledger(rig: Rig, ids: list) -> None:
    def ti(which: int, try_number: int = 1) -> FakeTI:
        made = FakeTI(try_number=try_number)
        for field, value in ids[which].items():
            setattr(made, field, value)
        return made

    keep = None if "run_id" in ids[0] else 1
    op = rig.operator(RivetApplyOperator, export="orders", log_keep=keep)
    first, _ = rig.run(op, ti(0))
    second, _ = rig.run(op, ti(1))
    retried, _ = rig.run(op, ti(1, try_number=2))
    artifacts = [c["argv"][1] for c in rig.calls() if c["argv"][0] == "apply"]
    assert [c["argv"][0] for c in rig.calls()].count("plan") == 2, "the second id plans for itself; only its own retry replays"
    assert artifacts[0].lower() != artifacts[1].lower() and artifacts[1] == artifacts[2]
    logs = [Path(p["stderr_path"]) for p in (first, second, retried)]
    assert str(logs[0]).lower() != str(logs[1]).lower()
    assert logs[0].exists(), "pruning the second id's tries leaves the first id's files alone"
    assert logs[1].exists() is (keep is None) and logs[2].exists(), "retention counts the tries of one task of one DAG"
    rig.scenario(apply={"signal": "SIGKILL"})
    crashed = [rig.run(op, ti(0, try_number=3))[0], rig.run(op, ti(1, try_number=4))[0]]
    assert [type(c) for c in crashed] == [AirflowException, AirflowException], "each id has its own crashed budget"


def test_a_sealed_plan_made_for_another_export_is_refused_and_refused_again_on_the_retry(rig: Rig) -> None:
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op)
    artifact = Path(rig.argv("apply")[1])
    foreign = json.dumps({**json.loads(artifact.read_text()), "export_name": "users"})
    artifact.write_text(foreign)
    before = len(rig.calls())
    outcomes = [rig.run(op, FakeTI(try_number=n)) for n in (2, 3)]
    for exc, ti in outcomes:
        assert isinstance(exc, AirflowFailException), exc
        assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN"
        assert "`users`" in str(exc) and "`orders`" in str(exc)
    assert str(outcomes[0][0]) == str(outcomes[1][0]), "the second cycle refuses for the same reason"
    assert len(rig.calls()) == before, "neither a plan nor an apply ran"
    assert artifact.read_text() == foreign, "the refusal leaves the artifact as it found it"
    artifact.unlink()
    payload, _ = rig.run(op, FakeTI(try_number=4))
    assert payload["decision"] == "success" and json.loads(artifact.read_text())["export_name"] == "orders"


def test_a_plan_that_writes_an_artifact_for_another_export_is_not_applied(rig: Rig) -> None:
    script = rig.bin / "rivet"
    script.write_text(script.read_text().replace('{"export_name": value_of(argv, "--export"),', '{"export_name": "users",'))
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(exc, AirflowFailException), exc
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN"
    assert [c["argv"][0] for c in rig.calls()] == ["plan"]


STATE_URL = "postgresql://state:st4te-pw@state-db/rivet_state"


def test_a_worker_state_url_the_task_does_not_pass_on_is_refused_before_rivet_starts(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("RIVET_STATE_URL", STATE_URL)
    op = rig.operator(RivetRunOperator, export="orders")
    outcomes = [rig.run(op, FakeTI(try_number=n)) for n in (1, 2)]
    for exc, ti in outcomes:
        assert isinstance(exc, AirflowFailException), exc
        assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_STATE_ENV_NOT_PASSED"
        text = str(exc)
        assert "RIVET_STATE_URL" in text and 'env_passthrough=["RIVET_STATE_URL"]' in text and "env_from_connections" in text
        blob = text + json.dumps(ti.store[("task", "return_value")]) + json.dumps(rig.records) + caplog.text
        assert "st4te-pw" not in blob and "state-db" not in blob, "the value is never shown"
    assert not rig.calls(), "rivet is not started on an empty SQLite state"
    assert not (rig.state_dir / "logs").exists() and not list(rig.state_dir.glob("*.yaml"))
    passed = rig.operator(RivetRunOperator, export="orders", env_passthrough=["RIVET_STATE_URL"])
    payload, _ = rig.run(passed)
    assert payload["decision"] == "success" and "state_url_sqlite" not in payload["degraded"]
    declared = rig.operator(RivetRunOperator, export="orders", env={"RIVET_PG_URL": SECRET, "RIVET_STATE_URL": "postgresql://other/db"})
    rig.run(declared)
    assert [c["env"]["RIVET_STATE_URL"] for c in rig.calls()] == [STATE_URL, "postgresql://other/db"]
    monkeypatch.setenv("RIVET_STATE_URL", STATE_URL)
    opted_out, _ = rig.run(rig.operator(RivetRunOperator, export="orders", env={"RIVET_PG_URL": SECRET, "RIVET_STATE_URL": ""}))
    assert opted_out["decision"] == "success" and "state_url_sqlite" in opted_out["degraded"], "a declared empty value is a decision"
    assert rig.calls()[-1]["env"]["RIVET_STATE_URL"] == ""
    for ignored in ("", " ", "sqlite:///tmp/x.db", "mysql://state-db/x", "Postgres://state-db/x", " postgresql://state-db/x"):
        monkeypatch.setenv("RIVET_STATE_URL", ignored)
        payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
        assert isinstance(payload, dict) and payload["decision"] == "success", f"rivet does not use {ignored!r} as its state: {payload}"
        assert "state_url_sqlite" in payload["degraded"]
    for used in ("postgres://state-db/x", "postgresql://state-db/x"):
        monkeypatch.setenv("RIVET_STATE_URL", used)
        exc, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
        assert isinstance(exc, AirflowFailException) and "RIVET_AIRFLOW_STATE_ENV_NOT_PASSED" in str(exc), used
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    assert "RIVET_AIRFLOW_STATE_ENV_NOT_PASSED" in readme and "RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN" in readme


def tenant_tasks(rig: Rig) -> list:
    """Two apply tasks of one DAG run: two configs of one file name, each with an export called `orders`."""
    tasks = []
    for tenant in ("tenant_a", "tenant_b"):
        (rig.tmp / tenant).mkdir()
        config = rig.tmp / tenant / "pg.yaml"
        config.write_text(
            "source:\n  type: postgres\n  url_env: RIVET_PG_URL\nexports:\n"
            f"  - {{name: orders, query: SELECT * FROM {tenant}_orders, mode: full}}\n"
        )
        tasks.append(rig.operator(RivetApplyOperator, task_id=f"apply_{tenant}", config=str(config), export="orders"))
    return tasks


def files_of(folder: Path) -> dict:
    """Every file below a directory with its bytes."""
    return {str(p.relative_to(folder)): p.read_bytes() for p in sorted(folder.rglob("*")) if p.is_file() and "logs" not in p.parts}


def refused_twice(rig: Rig, op: object, artifact: Path, *said: str) -> None:
    """Tries 2 and 3 refuse the artifact for one reason, start no rivet, and leave the state directory as it was."""
    kept, before, calls = artifact.read_text(), files_of(rig.state_dir), len(rig.calls())
    outcomes = [rig.run(op, FakeTI(task_id=op.task_id, try_number=n)) for n in (2, 3)]
    for exc, ti in outcomes:
        assert isinstance(exc, AirflowFailException), exc
        assert ti.store[(op.task_id, "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN"
        assert all(text in str(exc) for text in said), exc
    assert str(outcomes[0][0]) == str(outcomes[1][0]), "the second cycle refuses for the same reason"
    assert len(rig.calls()) == calls, "neither a plan nor an apply ran"
    assert artifact.read_text() == kept, "the refusal leaves the artifact as it found it"
    assert files_of(rig.state_dir) == before, "a refusal records nothing that a later try could read as permission"


def test_two_configs_with_one_export_name_in_one_run_each_plan_and_apply_their_own_plan(rig: Rig) -> None:
    one, two = tenant_tasks(rig)
    for op in (one, two):
        payload, _ = rig.run(op)
        assert payload["decision"] == "success"
    steps = [c["argv"] for c in rig.calls() if c["argv"][0] in ("plan", "apply")]
    assert [s[0] for s in steps] == ["plan", "apply", "plan", "apply"], "the second config is planned, not served the first one's plan"
    configs = [s[s.index("--config") + 1] for s in steps if s[0] == "plan"]
    artifacts = [Path(s[1]) for s in steps if s[0] == "apply"]
    assert configs == [str(rig.copy(config=Path(op.config))) for op in (one, two)] and configs[0] != configs[1]
    assert artifacts[0] != artifacts[1]
    assert [json.loads(a.read_text())["config_path"] for a in artifacts] == [str(Path(c).resolve()) for c in configs]
    assert [mode(d) for d in (artifacts[0].parent, artifacts[0].parent.parent)] == [0o700, 0o700], "plans/ and plans/<run>/ are private"
    rig.run(two, FakeTI(task_id=two.task_id, try_number=2))
    steps = [c["argv"] for c in rig.calls() if c["argv"][0] in ("plan", "apply")]
    assert [s[0] for s in steps][4:] == ["apply"] and steps[4][1] == str(artifacts[1]), "a retry replays its own task's plan"


def test_a_config_or_query_file_edited_between_two_tries_is_planned_again(rig: Rig) -> None:
    (rig.config_dir / "orders.sql").write_text("SELECT 1")
    rig.config.write_text(CONFIG_WITH_QUERY_FILE)
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op)
    rig.run(op, FakeTI(try_number=2))
    assert [c["argv"][0] for c in rig.calls()].count("plan") == 1, "nothing changed: the retry replays the sealed plan"
    (rig.config_dir / "orders.sql").write_text("SELECT 2")
    rig.run(op, FakeTI(try_number=3))
    rig.config.write_text(CONFIG_WITH_QUERY_FILE + "# edited\n")
    rig.run(op, FakeTI(try_number=4))
    artifacts = [c["argv"][1] for c in rig.calls() if c["argv"][0] == "apply"]
    assert [c["argv"][0] for c in rig.calls()].count("plan") == 3, "the plan of the old query or config is not applied"
    assert artifacts[0] == artifacts[1] and len(set(artifacts)) == 3
    for config_tail, sql in (("#S", "ELECT 1"), ("#", "SELECT 1")):
        rig.config.write_text(CONFIG_WITH_QUERY_FILE + config_tail)
        (rig.config_dir / "orders.sql").write_text(sql)
        rig.run(op, FakeTI(try_number=5))
    assert [c["argv"][0] for c in rig.calls()].count("plan") == 5, "the digest is over each input, not over their concatenation"


def test_one_task_id_over_two_config_paths_with_the_same_bytes_shares_no_plan(rig: Rig) -> None:
    (rig.tmp / "other").mkdir()
    twin = rig.tmp / "other" / "pg.yaml"
    twin.write_text(rig.config.read_text())
    for config in (rig.config, twin):
        payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders", config=str(config)))
        assert payload["decision"] == "success"
    steps = [c["argv"] for c in rig.calls() if c["argv"][0] in ("plan", "apply")]
    assert [s[0] for s in steps] == ["plan", "apply", "plan", "apply"] and steps[1][1] != steps[3][1]


CONFIG_WITH_QUERY_FILE = """\
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - {name: orders, query_file: orders.sql, mode: full}
  - {name: users, query: SELECT 1, mode: full}
"""
EXPIRED = "2000-01-01T00:00:00+00:00"


def test_a_sealed_plan_of_another_config_is_refused_and_refused_again_on_the_retry(rig: Rig) -> None:
    one, two = tenant_tasks(rig)
    rig.run(one)
    rig.run(two)
    theirs, mine = [Path(c["argv"][1]) for c in rig.calls() if c["argv"][0] == "apply"]
    mine.write_text(theirs.read_text())
    refused_twice(rig, two, mine, str(rig.copy(config=Path(one.config)).resolve()), str(rig.copy(config=Path(two.config)).resolve()))
    mine.unlink()
    payload, _ = rig.run(two, FakeTI(task_id=two.task_id, try_number=4))
    assert payload["decision"] == "success" and json.loads(mine.read_text())["config_path"] != json.loads(theirs.read_text())["config_path"]


@pytest.mark.parametrize(
    "change",
    [
        {"export_name": "users"},
        {"export_name": "Orders"},
        {"export_name": None},
        {"export_name": "DROP"},
        {"config_path": "/elsewhere/pg.yaml"},
        {"config_path": None},
        {"config_path": ""},
        {"config_path": 7},
        {"config_path": "DROP"},
        {"export_name": "DROP", "config_path": "DROP"},
    ],
    ids=["export-other", "export-case", "export-null", "export-absent", "config-other", "config-null", "config-empty",
         "config-not-text", "config-absent", "both-absent"],
)
@pytest.mark.parametrize("expired", [False, True], ids=["unexpired", "expired"])
def test_a_sealed_plan_with_an_identity_field_foreign_or_missing_is_refused_twice_expired_or_not(
    rig: Rig, change: dict, expired: bool
) -> None:
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op)
    artifact = Path(rig.argv("apply")[1])
    doc = {**json.loads(artifact.read_text()), **change, **({"expires_at": EXPIRED} if expired else {})}
    artifact.write_text(json.dumps({k: v for k, v in doc.items() if v != "DROP"}))
    refused_twice(rig, op, artifact, str(artifact))


@pytest.mark.parametrize("text", ["", "{not json", "[1, 2]", '"orders"'], ids=["empty", "garbage", "list", "string"])
def test_a_file_at_the_artifact_path_that_is_no_plan_object_is_planned_over(rig: Rig, text: str) -> None:
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op)
    artifact = Path(rig.argv("apply")[1])
    artifact.write_text(text)
    payload, _ = rig.run(op, FakeTI(try_number=2))
    assert payload["decision"] == "success" and [c["argv"][0] for c in rig.calls()].count("plan") == 2
    assert json.loads(artifact.read_text())["export_name"] == "orders"


def test_the_scratch_files_of_two_tries_tasks_or_runs_are_separate_files(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS)
    tis = [FakeTI(), FakeTI(try_number=2), FakeTI(task_id="other"), FakeTI()]
    tis[3].run_id = "manual__2026-10-07"
    for cls in (RivetRunOperator, RivetApplyOperator, RivetLoadOperator):
        op = rig.operator(cls, export="orders")
        for ti in tis:
            rig.run(op, ti)
    for sub in ("run", "apply", "load"):
        scratch = [c["argv"][c["argv"].index("--summary-output") + 1] for c in rig.calls() if c["argv"][0] == sub]
        assert len(scratch) == 4 and len({s.lower() for s in scratch}) == 4, f"{sub}: a zombie try and its retry would share a result file"
        assert not any(Path(s).exists() for s in scratch)


def test_the_plan_scratch_file_is_not_shared_by_two_dags_refreshing_one_layout_file(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened, real = [], invocation._open_private
    monkeypatch.setattr(invocation, "_open_private", lambda path: (opened.append(Path(path)), real(path))[1])
    campaign = {"waves": [{"wave": 1, "exports": ["orders"]}], "ordered_exports": []}
    rig.scenario(plan_list=[{"export_name": "orders", "prioritization": {"campaign": campaign}}])
    target = rig.tmp / "pg.plan.json"
    op = rig.operator(RivetPlanOperator, plan_file=str(target))
    tis = [FakeTI(), FakeTI(), FakeTI(try_number=2)]
    tis[1].dag_id = "other_dag"
    for ti in tis:
        payload, _ = rig.run(op, ti)
        assert payload["decision"] == "success"
    scratch = [p for p in opened if p.parent == target.parent]
    assert len(scratch) == 3 and len({str(p).lower() for p in scratch}) == 3 and target not in scratch
    assert sorted(p.name for p in target.parent.iterdir() if p.is_file()) == ["pg.plan.json"], "no scratch file is left behind"


def test_the_lock_of_a_per_table_task_is_named_by_its_exact_export(rig: Rig) -> None:
    names = ("a:b", "a+b", "Orders", "orders")
    exports = "".join(f"  - {{name: {json.dumps(n)}, query: SELECT 1, mode: full}}\n" for n in names)
    rig.config.write_text(f"source:\n  type: postgres\n  url_env: RIVET_PG_URL\nexports:\n{exports}")
    for n in names:
        rig.run(rig.operator(RivetLoadOperator, export=n, table="t"))
    locks = sorted(p.name for p in (rig.state_dir / "airflow" / "locks").iterdir())
    assert locks == sorted(f"{invocation.path_name(n)}.lock" for n in names) and len({name.lower() for name in locks}) == 4
    assert "orders.lock" in locks and mode(rig.state_dir / "airflow" / "locks") == 0o700


OLD_LAYOUT = ("manual__2026-10-06__try1.run.stderr.log", "scheduled__2026-10-06T00_00_00_00_00__try1.apply.stdout.log")


@pytest.mark.parametrize("stderr_dir", [False, True], ids=["state-dir", "stderr-dir"])
def test_pruning_removes_only_this_tasks_own_files_and_never_an_older_layouts(rig: Rig, stderr_dir: bool) -> None:
    base = rig.tmp / "mine" if stderr_dir else rig.state_dir / "logs"
    old = [base / dag / task / name for dag in ("Sales", "sales") for task in ("Task", "task") for name in OLD_LAYOUT]
    for path in old:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"written for {path.parent}")
        os.utime(path, (1, 1))
    op = rig.operator(RivetRunOperator, export="orders", log_keep=1, **({"stderr_dir": str(base)} if stderr_dir else {}))
    mine = []
    for dag in ("sales", "Sales"):
        for n in (1, 2, 3):
            ti = FakeTI(try_number=n)
            ti.dag_id = dag
            mine.append(Path(rig.run(op, ti)[0]["stderr_path"]))
    assert [p.exists() for p in mine] == [False, False, True] * 2, "retention still counts this task's own tries"
    assert all(p.exists() and p.read_text().startswith("written for") for p in old), "a file of the older layout is left alone"
    assert not any(p.parent in {m.parent for m in mine} for p in old), "the new layout's folder is never an older layout's folder"
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    assert "Upgrading a state directory" in readme
