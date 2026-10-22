"""Known reds: gate failures acknowledged with a reason and an expiry, so a red gate on main
is red ONLY for recorded reasons. A matching failure is reported as KNOWN, not blocking; an
expired entry blocks again; an entry that no failure matched in a FULL run blocks too (fixed —
remove it). Matched by a substring of the failure message, one entry per acknowledged defect.

An open product defect is pinned by a live_suite cell that asserts the CORRECT behaviour and so
fails until the fix lands: the cell is named `open_defect_*` and marked `live+gate-only` (CI has no
such ledger and would only go red), and its entry here is `<module>::<fn> — <its first panic line>`,
the defect's own symptom. The fix PR deletes the entry; the cell stays as the regression guard.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass


@dataclass(frozen=True)
class KnownRed:
    match: str
    reason: str
    expires: str  # YYYY-MM-DD


_EXPIRES = "2026-10-31"
def _open(cell: str, symptom: str, reason: str) -> KnownRed:
    """The entry of one `open_defect_*` live cell: its test path and the first line of its panic."""
    return KnownRed(f"{cell} — {symptom}", reason, _EXPIRES)


_PERF_MYSQL_SETTINGS_SCAN = (
    'MySQL CDC reads its server settings with two `SHOW GLOBAL VARIABLES` per run since the ProxySQL-safe reads (#661, #684); each scans performance_schema.global_variables (631 rows), so Handler_read_rnd_next counts about 1280 rows where the previous release counted 13. No user table is read and the wall time is unchanged. Tracked in issue #694 (read the settings once, or grade this counter without the schema scan)'
)


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
    KnownRed('perf[mysql/cdc]: mysql_handler_read_rnd_next ',
             _PERF_MYSQL_SETTINGS_SCAN,
             "2026-10-31"),
    KnownRed('perf[mysql/cdc-spill]: mysql_handler_read_rnd_next ',
             _PERF_MYSQL_SETTINGS_SCAN,
             "2026-10-31"),
    KnownRed('perf[mysql/cdc-resume]: mysql_handler_read_rnd_next ',
             _PERF_MYSQL_SETTINGS_SCAN,
             "2026-10-31"),
    KnownRed('perf[mysql/cdc-snapshot]: mysql_handler_read_rnd_next ',
             _PERF_MYSQL_SETTINGS_SCAN,
             "2026-10-31"),
)


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in msg:
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
