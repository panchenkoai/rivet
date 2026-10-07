"""Failure alerting through Airflow: the error object formatted for the Slack provider's webhook hook."""

from __future__ import annotations

from typing import Any, Callable, Mapping, Optional

from .operators import XCOM_KEY

MAX_UNITS = 10


def format_failure(payload: Optional[Mapping[str, Any]], *, dag_id: str, task_id: str, run_id: str, log_url: str = "") -> str:
    """A Slack message from the XCom payload: code, class, action and unit; never rivet's error text."""
    lines = [f":x: *{dag_id}.{task_id}* failed (run `{run_id}`)"]
    payload = payload or {}
    if payload.get("preflight_refusal"):
        lines.append(f"refused before rivet started: `{payload['preflight_refusal']}`")
    failed = [u for u in payload.get("units") or [] if u.get("error")]
    if not failed and payload.get("error"):
        failed = [{"export": None, "table": None, "error": payload["error"]}]
    for unit in failed[:MAX_UNITS]:
        error = unit["error"]
        name = unit.get("export") or "(process)"
        if unit.get("table"):
            name += f" / {unit['table']}"
        code = f"`{error['code']}` " if error.get("code") else ""
        retry = "retryable" if error.get("retryable") else "not retryable"
        lines.append(f"• *{name}*: {code}`{error.get('class')}` ({retry})")
        if error.get("action"):
            lines.append(f"    action: {error['action']}")
    if len(failed) > MAX_UNITS:
        lines.append(f"… and {len(failed) - MAX_UNITS} more failed units")
    if not payload:
        lines.append("no rivet result was published for this try (the task died outside the operator)")
    if payload.get("stderr_path"):
        lines.append(f"stderr on the worker: `{payload['stderr_path']}`")
    if log_url:
        lines.append(f"<{log_url}|task log>")
    return "\n".join(lines)


def slack_failure_callback(slack_webhook_conn_id: str = "slack_default", **hook_kwargs: Any) -> Callable[[Any], None]:
    """An `on_failure_callback` that posts the formatted failure through `SlackWebhookHook`."""

    def notify(context: Any) -> None:
        """Format this task instance's rivet result and send it."""
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        ti = context["ti"]
        try:
            payload = ti.xcom_pull(task_ids=ti.task_id, key=XCOM_KEY)
        except Exception:  # noqa: BLE001 - an alert must still go out when XCom cannot be read
            payload = None
        text = format_failure(
            payload if isinstance(payload, dict) else None,
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=str(context.get("run_id") or getattr(ti, "run_id", "")),
            log_url=getattr(ti, "log_url", "") or "",
        )
        SlackWebhookHook(slack_webhook_conn_id=slack_webhook_conn_id, **hook_kwargs).send(text=text)

    return notify
