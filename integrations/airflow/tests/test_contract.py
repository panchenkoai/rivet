"""The package's result model against the fixtures the Rust contract test pins (ADR-0039)."""

from __future__ import annotations

import dataclasses

import pytest
from conftest import FIXTURES, fixture

from airflow_provider_rivet.capabilities import DEGRADATIONS
from airflow_provider_rivet.result import CLASSES, ErrorObject, UnitResult, error_from_exit, error_from_line


def full(error: ErrorObject) -> dict:
    """The error object with every contract key, `message` included."""
    return {**error.to_xcom(), "message": error.message}


def test_the_fixture_directory_is_the_repo_contract() -> None:
    assert (FIXTURES / "xcom_unit.json").is_file(), f"contract fixtures not found at {FIXTURES}"


def test_a_failed_run_entry_becomes_the_pinned_xcom_unit() -> None:
    entry = fixture("run_entry_failed.json")
    unit = UnitResult(
        export=entry["export_name"],
        status=entry["status"],
        run_id=entry["run_id"],
        rows=entry["rows"],
        files=entry["files"],
        stop_reason=entry["stop_reason"],
        error=ErrorObject.from_contract(entry["error"]),
    )
    assert unit.to_xcom() == fixture("xcom_unit.json")
    assert "message" not in unit.to_xcom()["error"]


def test_a_loaded_table_becomes_the_pinned_xcom_load_unit() -> None:
    doc = fixture("load_result.json")
    row = doc["per_table"][0]
    unit = UnitResult(export=row["export"], table=row["table"], status=row["status"], run_id=doc["run_id"], rows=row["rows"])
    assert unit.to_xcom() == fixture("xcom_unit_load.json")


def test_the_unit_shape_has_exactly_the_contract_keys() -> None:
    for name in ("xcom_unit.json", "xcom_unit_load.json"):
        assert set(UnitResult("x").to_xcom()) == set(fixture(name))
    error_keys = set(fixture("xcom_unit.json")["error"])
    assert set(ErrorObject(None, None, "generic", 1, False, None).to_xcom()) == error_keys
    assert {f.name.rstrip("_") for f in dataclasses.fields(ErrorObject)} == error_keys | {"message"}


@pytest.mark.parametrize("row", fixture("exit_without_object.json"), ids=lambda r: f"exit{r['exit_status']}-sig{r['signal']}")
def test_an_exit_with_no_object_is_built_by_the_contract_table(row: dict) -> None:
    built = error_from_exit(row["exit_status"], row["signal"])
    assert full(built) == row["object"]
    assert built.class_ in CLASSES


def test_a_kill_seen_directly_is_the_pinned_crashed_object() -> None:
    assert full(error_from_exit(None, 9)) == fixture("error_object_crashed.json")


def test_no_built_object_is_retryable_unless_it_is_crashed() -> None:
    for status in range(1, 256):
        built = error_from_exit(status, None)
        assert built.retryable == (built.class_ == "crashed"), status
        assert (built.class_ == "crashed") == (129 <= status <= 255), status


@pytest.mark.parametrize(
    "name",
    ["json_errors.json", "json_errors_uncoded.json", "json_errors_crashed.json", "json_errors_mixed.json", "json_errors_load.json"],
)
def test_a_contract_line_is_read_and_nothing_is_derived(name: str) -> None:
    line = fixture(name)
    error, derived = error_from_line(line)
    assert not derived
    assert (error.class_, error.retryable, error.exit_code) == (line["class"], line["retryable"], line["exit_code"])
    assert error.code == line.get("code")
    assert error.message == line["error"]


def test_todays_line_is_read_by_its_integer_exit_class_and_says_so() -> None:
    error, derived = error_from_line({"error": "connection reset", "exit_class": 2})
    assert derived and error.retryable and error.class_ == "retryable" and error.code is None
    error, derived = error_from_line({"error": "x", "exit_class": 5, "code": "RIVET_SOURCE_CDC_LOG_GAP"})
    assert derived and not error.retryable and error.class_ == "refusal" and error.code == "RIVET_SOURCE_CDC_LOG_GAP"


def test_the_exception_text_has_code_class_action_and_unit_and_no_message() -> None:
    error = ErrorObject.from_contract(fixture("error_object.json"))
    text = error.describe("orders_cdc")
    assert text.startswith("[RIVET_SOURCE_CDC_LOG_GAP] refusal: restore the missing log")
    assert text.endswith("(orders_cdc)")
    assert error.message not in text


def test_every_degradation_is_documented_in_the_readme() -> None:
    readme = (FIXTURES.parents[2] / "integrations" / "airflow" / "README.md").read_text()
    for key in DEGRADATIONS:
        assert f"`{key}`" in readme, f"README has no row for degradation `{key}`"
