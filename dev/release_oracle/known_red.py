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
    KnownRed('upgrade[mssql/cdc-load]: cycle1/compact: prev exit 1: never loaded',
             "`rivet init --mode cdc` on SQL Server writes per-table streams with no baseline and a base_buffer `load:` block, and prints load + compact as the next steps; the first compact refuses 'never loaded' (both 0.30.0 and this tree)",
             "2026-10-31"),
    KnownRed('upgrade[mssql/cdc-load/tz=+09:00]: cycle1/compact: prev exit 1: never loaded',
             "`rivet init --mode cdc` on SQL Server writes per-table streams with no baseline and a base_buffer `load:` block, and prints load + compact as the next steps; the first compact refuses 'never loaded' (both 0.30.0 and this tree)",
             "2026-10-31"),
    KnownRed('upgrade[mongo/cdc-load]: cycle1/compact: prev exit 1: never loaded',
             "`rivet init --mode cdc` on MongoDB writes per-table streams with no baseline and a base_buffer `load:` block, and prints load + compact as the next steps; the first compact refuses 'never loaded' (both 0.30.0 and this tree)",
             "2026-10-31"),
    KnownRed('upgrade[oracle/cdc-load]: anchor: prev exit 1 [RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED]',
             '`rivet init --mode cdc` on Oracle writes a `load:` block that every command refuses (RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED: Oracle CDC load is not supported, ADR-0037), in 0.30.0 and this tree',
             "2026-10-31"),
    KnownRed('upgrade[oracle/cdc-load/tz=Asia/Tokyo]: anchor: prev exit 1 [RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED]',
             '`rivet init --mode cdc` on Oracle writes a `load:` block that every command refuses (RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED: Oracle CDC load is not supported, ADR-0037), in 0.30.0 and this tree',
             "2026-10-31"),
)


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in msg:
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
