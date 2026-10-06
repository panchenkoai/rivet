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
    KnownRed('upgrade[oracle/cdc-load]: cycle1/load: prev exit 1: Error: config has no top-level `load:` block',
             "Oracle CDC does not load yet (ADR-0037; the Oracle GA work). The previous release's `rivet init --mode cdc` writes no `load:` block, and this cell still drives the previous release through `load`, which refuses. `upgrade[oracle/cdc-load/init=this]` grades what the scaffold does promise (the baseline Parquet equals the source). The cycle stays unsupported until Oracle CDC loads",
             "2026-10-31"),
    KnownRed('upgrade[oracle/cdc-load/tz=Asia/Tokyo]: cycle1/load: prev exit 1: Error: config has no top-level `load:` block',
             "Oracle CDC does not load yet (ADR-0037; the Oracle GA work). The previous release's `rivet init --mode cdc` writes no `load:` block, and this cell still drives the previous release through `load`, which refuses. `upgrade[oracle/cdc-load/init=this]` grades what the scaffold does promise (the baseline Parquet equals the source). The cycle stays unsupported until Oracle CDC loads",
             "2026-10-31"),
    KnownRed('sentinels[rivet_sent_ts9/full]: SUCCEEDED WITH A CHANGED VALUE: `V` differs',
             "Oracle TIMESTAMP(9) is delivered at microseconds today: docs/type-capability-matrix.yaml's oracle TIMESTAMP(9) row is a known_defect (exact native is Timestamp(ns), ADR-0038 CP1, Oracle engine step), so the 1 ns sentinel lands truncated",
             "2026-10-31"),
    KnownRed('sentinels[rivet_sent_ts9/keyset]: SUCCEEDED WITH A CHANGED VALUE: `V` differs',
             "Oracle TIMESTAMP(9) is delivered at microseconds today: docs/type-capability-matrix.yaml's oracle TIMESTAMP(9) row is a known_defect (exact native is Timestamp(ns), ADR-0038 CP1, Oracle engine step), so the 1 ns sentinel lands truncated",
             "2026-10-31"),
)


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in msg:
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
