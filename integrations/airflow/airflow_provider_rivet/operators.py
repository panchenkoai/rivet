"""Airflow operators: thin adapters over `invocation.invoke`; none of them builds argv."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import quote

from ._compat import AirflowException, AirflowFailException, AirflowSkipException, BaseHook, BaseOperator
from .invocation import _SAFE, Outcome, Request, _stop, crashed_retry_allowed, invoke
from .preflight import PreflightRefusal

XCOM_KEY = "return_value"
_SCHEMES = {"postgres": "postgresql", "mssql": "sqlserver", "mongo": "mongodb"}


def connection_url(conn: Any) -> str:
    """A rivet URL from an Airflow Connection's fields, in rivet's scheme, credentials percent-encoded."""
    extra = getattr(conn, "extra_dejson", None) or {}
    scheme = extra.get("rivet_scheme") or _SCHEMES.get(conn.conn_type, conn.conn_type)
    auth = ""
    if conn.login:
        auth = quote(conn.login, safe="")
        if conn.password:
            auth += ":" + quote(conn.password, safe="")
        auth += "@"
    port = f":{conn.port}" if conn.port else ""
    params = f"?{extra['rivet_params']}" if extra.get("rivet_params") else ""
    return f"{scheme}://{auth}{conn.host or ''}{port}/{conn.schema or ''}{params}"


class RivetBaseOperator(BaseOperator):
    """Runs one rivet command on the worker and turns its ADR-0039 result into an Airflow outcome."""

    command = ""
    needs_export = False
    template_fields: Sequence[str] = (
        "config",
        "export",
        "table",
        "state_dir",
        "plan_file",
        "extra_args",
        "env",
        "stderr_dir",
        "cwd",
    )
    ui_color = "#e8f0fe"

    def __init__(
        self,
        *,
        config: str,
        export: Optional[str] = None,
        table: Optional[str] = None,
        state_dir: Optional[str] = None,
        plan_file: Optional[str] = None,
        rivet_bin: str = "rivet",
        env: Optional[Mapping[str, str]] = None,
        env_from_connections: Optional[Mapping[str, str]] = None,
        deployment: str = "auto",
        extra_args: Optional[Sequence[str]] = None,
        lock_wait: int = 600,
        crashed_retries: int = 1,
        stderr_dir: Optional[str] = None,
        stream_stderr: bool = False,
        cwd: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if self.needs_export and not export:
            raise ValueError(f"{type(self).__name__} needs `export`")
        self.config = config
        self.export = export
        self.table = table
        self.state_dir = state_dir
        self.plan_file = plan_file
        self.rivet_bin = rivet_bin
        self.env = env
        self.env_from_connections = env_from_connections
        self.deployment = deployment
        self.extra_args = extra_args
        self.lock_wait = lock_wait
        self.crashed_retries = crashed_retries
        self.stderr_dir = stderr_dir
        self.stream_stderr = stream_stderr
        self.cwd = cwd
        self._proc: Any = None

    def _environment(self) -> dict[str, str]:
        """The subprocess environment: the worker's, the declared names, and URLs from Connections."""
        env = dict(os.environ)
        env.update({str(k): str(v) for k, v in (self.env or {}).items()})
        for name, conn_id in (self.env_from_connections or {}).items():
            env[name] = connection_url(BaseHook.get_connection(conn_id))
        return env

    def _emit(self, record: dict[str, Any]) -> None:
        """Write one structured line to the task log."""
        line = json.dumps(record, sort_keys=True, default=str)
        if record.get("event") == "rivet.warning":
            self.log.warning("%s", line)
        else:
            self.log.info("%s", line)

    def _hold(self, proc: Any) -> None:
        """Remember the running process so `on_kill` can stop it."""
        self._proc = proc

    def on_kill(self) -> None:
        """Stop rivet when Airflow kills the task."""
        if self._proc is not None:
            _stop(self._proc)

    def _upstream_markers(self, ti: Any) -> list[str]:
        """State-directory markers the direct upstream rivet tasks of this run reported."""
        upstream = sorted(self.upstream_task_ids)
        if not upstream:
            return []
        pulled = ti.xcom_pull(task_ids=upstream, key=XCOM_KEY) or []
        return [p["state_marker"] for p in pulled if isinstance(p, dict) and p.get("state_marker")]

    def _fail(self, ti: Any, payload: dict[str, Any], text: str, retry: bool) -> None:
        """Publish the payload, then raise the exception Airflow retries or the one it does not."""
        ti.xcom_push(key=XCOM_KEY, value=payload)
        raise (AirflowException if retry else AirflowFailException)(text)

    def execute(self, context: Any) -> dict[str, Any]:
        """Run the command; return the XCom payload or raise by the contract's retry table."""
        ti = context["ti"]
        run_id = str(context.get("run_id") or getattr(ti, "run_id", "run"))
        map_index = getattr(ti, "map_index", -1)
        task_part = ti.task_id if map_index in (-1, None) else f"{ti.task_id}-{map_index}"
        request = Request(
            command=self.command,
            config=self.config,
            env=self._environment(),
            export=self.export,
            table=self.table,
            rivet_bin=self.rivet_bin,
            state_dir=self.state_dir,
            deployment=self.deployment,
            extra_args=list(self.extra_args or ()),
            lock_wait=self.lock_wait,
            plan_file=self.plan_file,
            cwd=self.cwd,
            stderr_dir=self.stderr_dir,
            stream_stderr=self.stream_stderr,
            label=(ti.dag_id, run_id, task_part, f"try{ti.try_number}"),
            upstream_markers=self._upstream_markers(ti),
        )
        try:
            outcome = invoke(request, self._emit, self._hold)
        except PreflightRefusal as refusal:
            self._emit({"event": "rivet.refused", "code": refusal.code, "text": str(refusal)})
            payload = {"command": self.command, "decision": "fail", "preflight_refusal": refusal.code, "units": []}
            self._fail(ti, payload, str(refusal), retry=False)
        finally:
            self._proc = None
        return self._settle(ti, run_id, task_part, outcome)

    def _settle(self, ti: Any, run_id: str, task_part: str, outcome: Outcome) -> dict[str, Any]:
        """Map the outcome's decision onto success, skip, retry or fail."""
        payload = outcome.to_xcom()
        decision = outcome.decision
        if decision == "success":
            if self.command == "compact" and outcome.units and all(u.status == "skipped" for u in outcome.units):
                ti.xcom_push(key=XCOM_KEY, value=payload)
                raise AirflowSkipException(f"compact skipped: {outcome.skip_reason}")
            stops = [u.export for u in outcome.units if u.stop_reason == "max_events"]
            if stops:
                self._emit({"event": "rivet.cdc.max_events", "exports": stops, "text": "stopped on max_events; backlog remains"})
            return payload
        assert outcome.error is not None
        failed = [u.export for u in outcome.units if u.status == "failed"]
        text = outcome.error.describe(failed[0] if len(failed) == 1 else self.export)
        if decision == "retry_crashed":
            ledger = None
            if outcome.marker is not None and self.state_dir:
                name = f"{_SAFE.sub('_', run_id)}__{_SAFE.sub('_', task_part)}.json"
                ledger = Path(self.state_dir) / "airflow" / _SAFE.sub("_", ti.dag_id) / "crashed" / name
            allowed = crashed_retry_allowed(ledger, ti.try_number, ti.max_tries, self.retries or 0, self.crashed_retries)
            payload["crashed_retry"] = allowed
            self._emit(
                {
                    "event": "rivet.crashed",
                    "retry": allowed,
                    "budget": self.crashed_retries,
                    "try_number": ti.try_number,
                    "airflow_tries_left": ti.try_number <= ti.max_tries,
                }
            )
            self._fail(ti, payload, text, retry=allowed)
        self._fail(ti, payload, text, retry=decision == "retry")
        return payload


class RivetRunOperator(RivetBaseOperator):
    """`rivet run`, for one export or the whole config."""

    command = "run"


class RivetApplyOperator(RivetBaseOperator):
    """`rivet plan -e <export>` into a sealed artifact, then `rivet apply` of it; a retry replays the artifact."""

    command = "apply"
    needs_export = True


class RivetCdcRunOperator(RivetBaseOperator):
    """One bounded CDC drain of a stream; succeeds on `max_events` and reports it."""

    command = "cdc_run"
    needs_export = True
    ui_color = "#e6f4ea"


class RivetLoadOperator(RivetBaseOperator):
    """`rivet load` for one export, and one table where the binary can narrow to it."""

    command = "load"
    ui_color = "#fef7e0"


class RivetCompactOperator(RivetBaseOperator):
    """`rivet compact` for one export; a compaction rivet reports as skipped is an Airflow skip."""

    command = "compact"
    ui_color = "#fce8e6"


class RivetPlanOperator(RivetBaseOperator):
    """`rivet plan --format json` for the whole config, written atomically to `plan_file`."""

    command = "plan"

    def __init__(self, *, plan_file: str, **kwargs: Any) -> None:
        super().__init__(plan_file=plan_file, **kwargs)


class RivetWaveBarrier(BaseOperator):
    """Does nothing: an ordering point between two waves, run when the wave before it has finished."""

    ui_color = "#f1f3f4"

    def execute(self, context: Any) -> None:
        """Nothing to run."""
        return None
