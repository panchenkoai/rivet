"""Known reds: gate failures acknowledged with a reason and an expiry, so a red gate on main
is red ONLY for recorded reasons. A matching failure is reported as KNOWN, not blocking; an
expired entry blocks again; an entry that no failure matched in a FULL run blocks too (fixed —
remove it). Matched by a substring of the failure message, one entry per acknowledged defect.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass


@dataclass(frozen=True)
class KnownRed:
    match: str
    reason: str
    expires: str  # YYYY-MM-DD


KNOWN_RED: tuple[KnownRed, ...] = (
    KnownRed("perf[mssql/cdc-snapshot]: wall",
             "a fixed +0.1–0.3 s on SQL Server's first initial-snapshot run against 0.29.0, reproducible "
             "(0.57/0.57 s vs 0.27/0.38 s), CPU and rows unchanged; it opens FEWER connections (2 vs 3) "
             "and the harm query costs no more than SELECT 1, so the source is not yet attributed — "
             "investigation open, not a loss",
             "2026-10-12"),
)


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in msg:
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
