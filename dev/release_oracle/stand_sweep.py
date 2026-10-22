"""Drop replication slots a dead gate left on the stand's PostgreSQL servers, before the gate starts.

A killed run leaves its slot behind (`upg_po_19909`, 1.2 GB of WAL on 2026-10-22), and every
later `rivet doctor` on that server fails on "inactive slot(s) are pinning WAL" — a red with no
product cause. A slot is stale when it is inactive, carries a gate prefix, and either names a
process id that is no longer alive or has been seen inactive for `MAX_AGE_H` hours (PostgreSQL 16
keeps no slot age, so the age is the sweep's own record).

    python3 -m dev.release_oracle.stand_sweep --self-test
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

from .core import ROOT

__all__ = ["stale_slots", "sweep"]

PREFIX = re.compile(r"^(?:upg|rivet)_")
PID = re.compile(r"_(\d{2,7})$")
MAX_AGE_H = 24
SEEN = ROOT / "target" / "stand-slots-seen.json"
URLS = ("RIVET_CDC_POSTGRES_URL", "RIVET_ORACLE_POSTGRES_URL")


def alive(pid: int) -> bool:
    """Whether a process with this id exists on this machine."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def stale_slots(slots: list[tuple[str, bool]], seen: dict[str, float], now: float,
                is_alive=alive, max_age_h: float = MAX_AGE_H) -> tuple[list[str], dict[str, float]]:
    """(slots to drop, the inactive gate slots' first-seen times to keep) from (name, active) rows."""
    drop, keep = [], {}
    for name, active in slots:
        if active or not PREFIX.match(name):
            continue
        first = seen.get(name, now)
        pid = PID.search(name)
        if (pid and not is_alive(int(pid.group(1)))) or now - first >= max_age_h * 3600:
            drop.append(name)
        else:
            keep[name] = first
    return drop, keep


def sweep() -> list[str]:
    """Drop the stale slots on each stand PostgreSQL the gate names; one line per server and per drop."""
    from .cdc import _psql

    try:
        seen = json.loads(SEEN.read_text())
    except (OSError, ValueError):
        seen = {}
    said, kept, now = [], {}, time.time()
    for var in URLS:
        url = os.environ.get(var, "")
        if not url:
            continue
        p = _psql(url, "-Atc", "SELECT slot_name, active FROM pg_replication_slots ORDER BY slot_name")
        if not p.ok:
            said.append(f"slot sweep: {var} unreadable ({(p.stderr or p.stdout or '').strip()[:120]})")
            continue
        rows = [(n, a == "t") for n, _, a in (ln.partition("|") for ln in p.out.splitlines() if "|" in ln)]
        drop, keep = stale_slots(rows, {k.split(" ", 1)[1]: v for k, v in seen.items() if k.startswith(var + " ")}, now)
        kept.update({f"{var} {k}": v for k, v in keep.items()})
        for name in drop:
            q = _psql(url, "-Atc", f"SELECT pg_drop_replication_slot('{name}') FROM pg_replication_slots "
                                   f"WHERE slot_name = '{name}' AND NOT active")
            said.append(f"slot sweep: {var} dropped stale slot {name}" if q.ok
                        else f"slot sweep: {var} could not drop {name}: {(q.stderr or '').strip()[:120]}")
    SEEN.parent.mkdir(parents=True, exist_ok=True)
    SEEN.write_text(json.dumps(kept, sort_keys=True))
    return said


def self_test() -> None:
    """Only inactive gate slots go: a dead pid at once, any other after its age; an active or foreign slot stays."""
    now = 1_000_000.0
    rows = [("upg_po_19909", False), ("upg_po_4242", False), ("upg_po_19910", True), ("rivet_rivet_public_cdc", False),
            ("rivet_old", False), ("app_slot", False), ("pg_extra_11", False)]
    seen = {"rivet_old": now - 25 * 3600, "rivet_rivet_public_cdc": now - 3600}
    drop, keep = stale_slots(rows, seen, now, is_alive=lambda pid: pid == 4242)
    assert drop == ["upg_po_19909", "rivet_old"], drop
    assert keep == {"upg_po_4242": now, "rivet_rivet_public_cdc": now - 3600}, keep
    assert stale_slots([("rivet_x", False)], {}, now)[0] == [], "a slot first seen now is kept"
    print("stand_sweep self-test: ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        print("\n".join(sweep()) or "slot sweep: nothing stale")
