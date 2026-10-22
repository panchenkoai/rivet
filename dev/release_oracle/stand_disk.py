"""Free disk on the host and on the docker VM's data disk: a floor before the gate starts, a watchdog during it.

On 2026-10-22 a full gate filled the host disk; the docker VM's data disk shares it, so InnoDB
stopped on "Disk is full", the Postgres state server crashed, and ~150 cells went red with no
product cause. Below the start floor the gate does not begin; under the watchdog floor mid-run the
run is STAND-INVALID, a verdict above any product red, because its reds no longer grade the product.

    python3 -m dev.release_oracle.stand_disk --self-test
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

from .core import ROOT

__all__ = ["START_FLOOR_GIB", "WATCH_FLOOR_GIB", "free_gib", "start_refusal", "low", "Watchdog"]

START_FLOOR_GIB = 150  # ratchet-pin: gate-start-free-disk-floor
WATCH_FLOOR_GIB = 20  # ratchet-pin: gate-watchdog-free-disk-floor
GIB = 1024 ** 3


def host_free_gib(path: Path = ROOT) -> float | None:
    """Free GiB on the file system holding the checkout."""
    try:
        return shutil.disk_usage(path).free / GIB
    except OSError:
        return None


def vm_free_gib() -> float | None:
    """Free GiB on the docker daemon's data disk, read inside a throwaway container; None when unreadable."""
    try:
        p = subprocess.run(["docker", "run", "--rm", "--pull", "never", "alpine:3", "df", "-Pk", "/"],
                           capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return parse_df(p.stdout) if p.returncode == 0 else None


def parse_df(text: str) -> float | None:
    """Free GiB from `df -Pk` output (the Available column of its one data row)."""
    rows = [ln.split() for ln in text.splitlines()[1:] if ln.strip()]
    try:
        return int(rows[0][3]) * 1024 / GIB
    except (IndexError, ValueError):
        return None


def free_gib() -> dict[str, float | None]:
    """Both readings, named."""
    return {"host": host_free_gib(), "docker VM": vm_free_gib()}


def shown(free: dict[str, float | None]) -> str:
    """`host 201 GiB, docker VM 177 GiB` (`unreadable` for a reading that failed)."""
    return ", ".join(f"{k} {'unreadable' if v is None else f'{v:.0f} GiB'}" for k, v in free.items())


def start_refusal(free: dict[str, float | None], floor: float = START_FLOOR_GIB) -> str:
    """Why the gate must not start ("" = it may); an unreadable disk is refused like a full one."""
    short = [k for k, v in free.items() if v is None or v < floor]
    if not short:
        return ""
    return (f"DISK-LOW: {', '.join(short)} below the {floor:.0f} GiB start floor ({shown(free)}); "
            "a full gate writes ~170 GiB and a full disk crashes the stand's servers mid-run")


def low(free: dict[str, float | None], floor: float = WATCH_FLOOR_GIB) -> str:
    """Why the running gate's stand is no longer valid ("" = it still is); an unreadable reading is not a verdict."""
    short = [k for k, v in free.items() if v is not None and v < floor]
    return f"{', '.join(short)} under {floor:.0f} GiB free ({shown(free)})" if short else ""


class Watchdog:
    """Reads both disks every `every` seconds; the first reading under the floor is kept as `reason`."""

    def __init__(self, read: Callable[[], dict[str, float | None]] = free_gib, every: float = 60.0,
                 floor: float = WATCH_FLOOR_GIB) -> None:
        self.reason = ""
        self._read, self._every, self._floor = read, every, floor
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> Watchdog:
        self._thread.start()
        return self

    def _loop(self) -> None:
        while not self._stop.is_set():
            why = low(self._read(), self._floor)
            if why and not self.reason:
                self.reason = why
                print(f"  ✗ STAND-INVALID: {why}; the reds from here on are the stand's, not the product's",
                      flush=True)
            self._stop.wait(self._every)

    def stop(self) -> str:
        """Stop, after one last reading; the reason (empty when the disks held)."""
        self._stop.set()
        self._thread.join(timeout=5)
        why = low(self._read(), self._floor)
        if why and not self.reason:
            self.reason = why
        return self.reason


def self_test() -> None:
    """The floors decide on both disks, an unreadable one refuses the start, and the watchdog keeps the first low reading."""
    assert parse_df("Filesystem 1024-blocks Used Available Capacity Mounted on\n"
                    "/dev/vdb1 412614656 226799616 185815040 55% /\n") == 185815040 * 1024 / GIB
    assert parse_df("") is None and parse_df("h\nx y z notanumber\n") is None
    assert start_refusal({"host": 201.0, "docker VM": 177.0}) == ""
    said = start_refusal({"host": 201.0, "docker VM": 19.0})
    assert said.startswith("DISK-LOW: docker VM below") and "host 201 GiB, docker VM 19 GiB" in said, said
    assert "host below" in start_refusal({"host": 149.9, "docker VM": 177.0})
    assert "docker VM below" in start_refusal({"host": 201.0, "docker VM": None}), "an unreadable VM disk refuses"
    assert low({"host": 21.0, "docker VM": 177.0}) == ""
    assert low({"host": 201.0, "docker VM": None}) == "", "an unreadable reading mid-run is not a verdict"
    assert low({"host": 19.0, "docker VM": 177.0}).startswith("host under 20 GiB")
    readings = iter([{"host": 200.0, "docker VM": 100.0}, {"host": 200.0, "docker VM": 12.0},
                     {"host": 5.0, "docker VM": 3.0}])
    last = {"host": 200.0, "docker VM": 100.0}

    def read() -> dict[str, float | None]:
        nonlocal last
        last = next(readings, last)
        return last

    dog = Watchdog(read, every=0.01).start()
    while not dog.reason:
        time.sleep(0.001)
    assert dog.stop().startswith("docker VM under 20 GiB free (host 200 GiB, docker VM 12 GiB)"), dog.reason
    held = Watchdog(lambda: {"host": 200.0, "docker VM": 100.0}, every=0.01).start()
    assert held.stop() == ""
    print("stand_disk self-test: ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        f = free_gib()
        print(start_refusal(f) or f"disk ok: {shown(f)}")
