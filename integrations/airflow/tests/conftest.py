"""Test harness: a throwaway AIRFLOW_HOME, a fake `rivet` on PATH and a task instance that is a plain object."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="rivet-provider-airflow-home-")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

import pytest  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "scheduler"
SECRET = "postgresql://rivet:s3cr3t-pw@db.internal:5432/app"
NEW_FLAGS = {
    "*": ["--no-notify", "--lock-wait"],
    "apply": ["--summary-output"],
    "load": ["--export", "--table", "--summary-output"],
    "compact": ["--export", "--table", "--summary-output"],
}
CONFIG = """\
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - name: orders
    query: SELECT * FROM orders
    mode: full
  - name: users
    query: SELECT * FROM users
    mode: incremental
  - name: orders_cdc
    mode: cdc
  - name: events
    mode: incremental
  - name: countries
    mode: full
"""


def fixture(name: str) -> Any:
    """One pinned contract fixture of the Rust repo."""
    return json.loads((FIXTURES / name).read_text())


class FakeTI:
    """The task-instance surface the operators use."""

    def __init__(self, task_id: str = "task", try_number: int = 1, max_tries: int = 2, store: dict | None = None) -> None:
        self.dag_id = "dag"
        self.task_id = task_id
        self.run_id = "manual__2026-10-06"
        self.try_number = try_number
        self.max_tries = max_tries
        self.map_index = -1
        self.store = store if store is not None else {}

    def xcom_push(self, key: str, value: Any) -> None:
        """Keep the value under (task, key)."""
        json.dumps(value)
        self.store[(self.task_id, key)] = value

    def xcom_pull(self, task_ids: Any = None, key: str = "return_value") -> Any:
        """One value for a task id, a list for several."""
        if isinstance(task_ids, str):
            return self.store.get((task_ids, key))
        return [self.store[(t, key)] for t in task_ids if (t, key) in self.store]


class Rig:
    """A fake binary, a config, a state directory, and the calls and log records of one test."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.bin = tmp / "bin"
        self.bin.mkdir()
        body = (Path(__file__).parent / "fake_rivet.py").read_text()
        script = self.bin / "rivet"
        script.write_text(f"#!{sys.executable}\n{body}")
        script.chmod(0o755)
        self.state_dir = tmp / "state"
        self.state_dir.mkdir()
        self.config_dir = tmp / "cfg"
        self.config_dir.mkdir()
        self.config = self.config_dir / "pg.yaml"
        self.config.write_text(CONFIG)
        self.records: list[dict] = []
        self.spec: dict = {}
        self.scenario()

    def scenario(self, **spec: Any) -> None:
        """Set what the fake binary does next."""
        self.spec = {"record_env": ["RIVET_PG_URL", "RIVET_STATE_URL"], "leak_env": ["RIVET_PG_URL"], **spec}
        (self.bin / "scenario.json").write_text(json.dumps(self.spec))

    def calls(self) -> list[dict]:
        """Every non-probe call the fake binary received."""
        log = self.bin / "calls.jsonl"
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def argv(self, sub: str) -> list[str]:
        """The argv of the last call of one subcommand."""
        return [c["argv"] for c in self.calls() if c["argv"][0] == sub][-1]

    def operator(self, cls: Any, **kwargs: Any) -> Any:
        """An operator wired to the fake binary, recording its structured log lines."""
        kwargs.setdefault("task_id", "task")
        kwargs.setdefault("config", str(self.config))
        kwargs.setdefault("state_dir", str(self.state_dir))
        kwargs.setdefault("env", {"RIVET_PG_URL": SECRET})
        kwargs.setdefault("deployment", "local")
        op = cls(rivet_bin=str(self.bin / "rivet"), retries=2, **kwargs)
        original = op._emit

        def record(entry: dict) -> None:
            self.records.append(entry)
            original(entry)

        op._emit = record
        return op

    def run(self, op: Any, ti: FakeTI | None = None) -> tuple[Any, FakeTI]:
        """Execute an operator; return (payload or raised exception, the task instance)."""
        ti = ti or FakeTI(task_id=op.task_id)
        try:
            return op.execute({"ti": ti, "run_id": ti.run_id}), ti
        except Exception as exc:  # noqa: BLE001 - the exception type is what the tests assert
            return exc, ti

    def events(self, name: str) -> list[dict]:
        """Log records of one event name."""
        return [r for r in self.records if r.get("event") == name]


@pytest.fixture
def rig(tmp_path: Path) -> Rig:
    """A fresh rig per test."""
    return Rig(tmp_path)
