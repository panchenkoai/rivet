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
    KnownRed("verdicts DIVERGED: EXT_ORDER_KEYED",
             "Oracle init no longer picks keyset for EXT_ORDER_KEYED (UNIQUE NOT NULL NUMBER(19); "
             "MySQL keysets it) and picks incremental for EXT_REF_ID_HISTORY — suspected Oracle init "
             "defect, under the Oracle preview; the golden is NOT re-blessed for these two",
             "2026-10-11"),
)


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in msg:
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
