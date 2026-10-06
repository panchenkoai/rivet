"""The ADR-0039 error object and unit result, and the one place a class is derived from an exit."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

CLASS_BY_EXIT = {
    1: "generic",
    2: "retryable",
    3: "data_integrity",
    4: "schema_drift",
    5: "refusal",
    6: "internal",
}
CLASSES = frozenset(CLASS_BY_EXIT.values()) | {"crashed"}


@dataclass(frozen=True)
class ErrorObject:
    """The D1 error object; `message` never leaves the worker."""

    code: Optional[str]
    kind: Optional[str]
    class_: str
    exit_code: Optional[int]
    retryable: bool
    action: Optional[str]
    message: Optional[str] = None

    def to_xcom(self) -> dict[str, Any]:
        """The object as XCom may carry it: every key but `message`."""
        return {
            "code": self.code,
            "kind": self.kind,
            "class": self.class_,
            "exit_code": self.exit_code,
            "retryable": self.retryable,
            "action": self.action,
        }

    @classmethod
    def from_contract(cls, obj: dict[str, Any], message_key: str = "message") -> "ErrorObject":
        """Read an object rivet printed with the contract's keys."""
        return cls(
            code=obj.get("code"),
            kind=obj.get("kind"),
            class_=obj["class"],
            exit_code=obj.get("exit_code"),
            retryable=bool(obj["retryable"]),
            action=obj.get("action"),
            message=obj.get(message_key),
        )

    def describe(self, export: Optional[str]) -> str:
        """The exception text: `[code] class: action (export)`, with no error message in it."""
        code = f"[{self.code}] " if self.code else ""
        action = f": {self.action}" if self.action else ""
        unit = f" ({export})" if export else ""
        return f"{code}{self.class_}{action}{unit}"


def error_from_exit(exit_status: Optional[int], signal: Optional[int]) -> ErrorObject:
    """Build the object for a process that printed none, by ADR-0039 D8's table and nothing else."""
    if signal is not None:
        return ErrorObject(None, None, "crashed", None, True, None, f"rivet was killed by signal {signal}")
    raw = f"rivet exited {exit_status} and printed no error object"
    if exit_status in (1, 3, 4, 5, 6):
        return ErrorObject(None, None, CLASS_BY_EXIT[exit_status], exit_status, False, None, None)
    if exit_status == 101:
        return ErrorObject(None, None, "internal", 6, False, None, raw)
    if exit_status is not None and 129 <= exit_status <= 255:
        return ErrorObject(None, None, "crashed", None, True, None, raw)
    return ErrorObject(None, None, "generic", 1, False, None, raw)


def error_from_line(line: dict[str, Any]) -> tuple[ErrorObject, bool]:
    """Read the `--json-errors` line; the flag says the line lacked `class` and was read by `exit_class`."""
    if "class" in line and "retryable" in line:
        return ErrorObject.from_contract(line, message_key="error"), False
    exit_class = line.get("exit_class")
    known = exit_class if exit_class in CLASS_BY_EXIT else 1
    return (
        ErrorObject(
            code=line.get("code"),
            kind=None,
            class_=CLASS_BY_EXIT[known],
            exit_code=known,
            retryable=known == 2,
            action=None,
            message=line.get("error"),
        ),
        True,
    )


@dataclass
class UnitResult:
    """One unit a scheduler reports: a run / apply export, or a load / compact table."""

    export: Optional[str]
    table: Optional[str] = None
    status: str = "success"
    run_id: Optional[str] = None
    rows: Optional[int] = None
    files: Optional[int] = None
    stop_reason: Optional[str] = None
    error: Optional[ErrorObject] = None
    skip_reason: Optional[str] = field(default=None, compare=False)

    def to_xcom(self) -> dict[str, Any]:
        """The contract's XCom unit shape (tests/fixtures/scheduler/xcom_unit*.json)."""
        return {
            "export": self.export,
            "table": self.table,
            "status": self.status,
            "run_id": self.run_id,
            "rows": self.rows,
            "files": self.files,
            "stop_reason": self.stop_reason,
            "error": self.error.to_xcom() if self.error else None,
        }
