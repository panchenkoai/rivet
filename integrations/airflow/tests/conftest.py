"""Test harness: a throwaway AIRFLOW_HOME, a fake `rivet`, a plain task instance, and real DAG runs through `dag.test()`."""

from __future__ import annotations

import hashlib
import importlib.util
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
        """One value for a task id; for several, a sequence with Airflow 2.10's truth-value trap."""
        if isinstance(task_ids, str):
            return self.store.get((task_ids, key))
        return LazyPull([self.store[(t, key)] for t in task_ids if (t, key) in self.store])


class LazyPull:
    """`LazyXComSelectSequence` as Airflow 2.10.5 behaves: iterable, and `bool()` of an empty one raises."""

    def __init__(self, values: list) -> None:
        self.values = values

    def __iter__(self) -> Any:
        return iter(self.values)

    def __bool__(self) -> bool:
        if not self.values:
            raise TypeError("__bool__ should return bool, returned NoneType")
        return True


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

    def copy(self, export: str | None = None, config: Path | None = None) -> Path:
        """Where the package materialises a config in the state directory: named after the source path's hash."""
        source = (config or self.config).resolve()
        digest = hashlib.sha256(str(source).encode()).hexdigest()[:8]
        suffix = f"--{export}" if export else ""
        return self.state_dir / f"{source.stem}.{digest}{suffix}{source.suffix}"

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


@pytest.fixture(autouse=True)
def own_temp_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Each test gets its own `tempfile.gettempdir()`, so a stateless worker's files stay inside the test."""
    path = tmp_path / "tmp"
    path.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(path))
    return path


@pytest.fixture
def rig(tmp_path: Path) -> Rig:
    """A fresh rig per test."""
    return Rig(tmp_path)


@pytest.fixture(scope="session")
def airflow_db() -> str:
    """A migrated SQLite metadata database in the throwaway AIRFLOW_HOME; the path of that home."""
    from airflow.utils import db

    db.resetdb()
    return os.environ["AIRFLOW_HOME"]


class DagRig(Rig):
    """A rig whose DAGs are written to the dags folder and run by `dag.test()` with real task instances."""

    def __init__(self, tmp: Path, home: str) -> None:
        super().__init__(tmp)
        self.home = Path(home)
        (self.home / "dags").mkdir(exist_ok=True)

    def run_dag(self, builder: str, dag_id: str, env: dict | None = None, **kwargs: Any) -> tuple[str, dict[str, str]]:
        """Build a DAG with one of the package's builders and run it; (run state, state by task id)."""
        kwargs.setdefault("config", str(self.config))
        kwargs.setdefault("state_dir", str(self.state_dir))
        kwargs.setdefault("default_args", {"retries": 0})
        op = {"rivet_bin": str(self.bin / "rivet"), "deployment": "local"}
        kwargs["operator_kwargs"] = {**op, **kwargs.get("operator_kwargs", {})}
        env = env or {"RIVET_PG_URL": SECRET}
        path = self.home / "dags" / f"{dag_id}.py"
        path.write_text(
            "import os\n"
            f"from airflow_provider_rivet.dags import {builder}\n"
            f"kwargs = {kwargs!r}\n"
            f"kwargs['operator_kwargs']['env'] = {{name: os.environ[name] for name in {sorted(env)!r}}}\n"
            f"dag = {builder}({dag_id!r}, **kwargs)\n"
        )
        before = {name: os.environ.get(name) for name in env}
        os.environ.update(env)
        try:
            spec = importlib.util.spec_from_file_location(dag_id, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            run = module.dag.test()
            states = {ti.task_id: str(getattr(ti.state, "value", ti.state)) for ti in run.get_task_instances()}
            return str(getattr(run.state, "value", run.state)), states
        finally:
            path.unlink()
            for name, value in before.items():
                os.environ.pop(name, None) if value is None else os.environ.__setitem__(name, value)

    def ran(self) -> list[tuple[str, str]]:
        """(subcommand, export) of every call the fake binary received."""
        return [(c["argv"][0], c["export"]) for c in self.calls()]


@pytest.fixture
def dag_rig(tmp_path: Path, airflow_db: str) -> DagRig:
    """A fresh rig per test over the session's metadata database."""
    return DagRig(tmp_path, airflow_db)
