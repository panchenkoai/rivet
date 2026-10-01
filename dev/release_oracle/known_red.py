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
             "0.30.0's `rivet init --mode cdc` on SQL Server wrote a per-table stream with no baseline under a base_buffer `load:` block, so the first compact refuses 'never loaded'. Fixed in this tree: init writes `cdc.initial: snapshot` and `upgrade[mssql/cdc-load/init=this]` grades it green. This cell starts from the PREVIOUS release's init, so it stays red until a release carries the fix",
             "2026-10-31"),
    KnownRed('upgrade[mssql/cdc-load/tz=+09:00]: cycle1/compact: prev exit 1: never loaded',
             "0.30.0's `rivet init --mode cdc` on SQL Server wrote a per-table stream with no baseline under a base_buffer `load:` block, so the first compact refuses 'never loaded'. Fixed in this tree: init writes `cdc.initial: snapshot` and `upgrade[mssql/cdc-load/init=this]` grades it green. This cell starts from the PREVIOUS release's init, so it stays red until a release carries the fix",
             "2026-10-31"),
    KnownRed('upgrade[mongo/cdc-load]: cycle1/compact: prev exit 1: never loaded',
             "0.30.0's `rivet init --mode cdc` on MongoDB wrote a per-table stream with no baseline under a base_buffer `load:` block, so the first compact refuses 'never loaded'. Fixed in this tree: init writes `cdc.initial: snapshot` and `upgrade[mongo/cdc-load/init=this]` grades it green. This cell starts from the PREVIOUS release's init, so it stays red until a release carries the fix",
             "2026-10-31"),
    KnownRed('upgrade[oracle/cdc-load]: anchor: prev exit 1 [RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED]',
             "0.30.0's `rivet init --mode cdc` on Oracle wrote a `load:` block the loader refuses (RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED, ADR-0037). This tree's init writes none and says why (`upgrade[oracle/cdc-load/init=this]` grades that); loading Oracle CDC is the Oracle GA work, so this load cycle stays unsupported until then",
             "2026-10-31"),
    KnownRed('upgrade[oracle/cdc-load/tz=Asia/Tokyo]: anchor: prev exit 1 [RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED]',
             "0.30.0's `rivet init --mode cdc` on Oracle wrote a `load:` block the loader refuses (RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED, ADR-0037). This tree's init writes none and says why (`upgrade[oracle/cdc-load/init=this]` grades that); loading Oracle CDC is the Oracle GA work, so this load cycle stays unsupported until then",
             "2026-10-31"),
)


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in msg:
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
