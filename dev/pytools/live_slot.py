#!/usr/bin/env python3
"""Run a command in one of N machine-wide live-test slots (OS advisory locks), or in all of them.

    live_slot.py [--slots N] [--all] -- CMD ...   take a slot (or every slot), then exec CMD
    live_slot.py --self-test

The lock lives on the open file, which CMD inherits across exec: the slot frees when the
last process holding it exits, however it dies (SIGKILL included). No pid files.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DEFAULT_DIR = Path.home() / ".cache" / "rivet" / "live-slots"


def try_lock(path: Path) -> int | None:
    """An fd holding an exclusive lock on `path`, or None when another process holds it."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None


def acquire(slots: int, take_all: bool, root: Path) -> tuple[list[int], str]:
    """Lock one free slot (polling), or every slot in order; return the fds and which slots."""
    root.mkdir(parents=True, exist_ok=True)
    paths = [root / f"slot-{i}.lock" for i in range(slots)]
    if take_all:
        paths = sorted(set(paths) | set(root.glob("slot-*.lock")))
        fds = []
        for p in paths:
            fd = os.open(p, os.O_RDWR | os.O_CREAT, 0o644)
            fcntl.flock(fd, fcntl.LOCK_EX)
            fds.append(fd)
        return fds, f"all {len(paths)} slots"
    while True:
        for i, p in enumerate(paths):
            fd = try_lock(p)
            if fd is not None:
                return [fd], f"slot {i + 1}/{slots}"
        time.sleep(1)


def run(argv: list[str]) -> int:
    """Take the slot(s), say how long that took, and exec the command."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--slots", type=int, default=2, help="machine-wide live slots (default 2)")
    ap.add_argument("--all", action="store_true", help="take every slot (the timed release gate)")
    ap.add_argument("--dir", type=Path, default=DEFAULT_DIR, help=argparse.SUPPRESS)
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    a = ap.parse_args(argv)
    cmd = a.cmd[1:] if a.cmd[:1] == ["--"] else a.cmd
    if not cmd or a.slots < 1:
        ap.error("usage: live_slot.py [--slots N] [--all] -- CMD ...")
    t0 = time.monotonic()
    fds, which = acquire(a.slots, a.all, a.dir)
    print(f"live-slot: holding {which} after waiting {time.monotonic() - t0:.1f}s: {' '.join(cmd)}",
          file=sys.stderr, flush=True)
    for fd in fds:
        os.set_inheritable(fd, True)
    os.execvp(cmd[0], cmd)


def self_test() -> int:
    """Two holders block a third; SIGKILL of a holder frees its slot; --all waits for every slot."""
    me = [sys.executable, str(Path(__file__).resolve())]
    procs: list[subprocess.Popen] = []
    with tempfile.TemporaryDirectory() as d:
        def start(*extra: str) -> subprocess.Popen:
            p = subprocess.Popen([*me, "--slots", "2", "--dir", d, *extra], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            procs.append(p)
            return p
        try:
            a, b = start("--", "sleep", "60"), start("--", "sleep", "60")
            time.sleep(1.5)
            third = start("--", "true")
            time.sleep(2.5)
            assert third.poll() is None, "a third holder ran while two slots were taken"
            os.kill(a.pid, signal.SIGKILL)
            a.wait()
            assert third.wait(timeout=10) == 0, "killing a holder did not free its slot"
            gate = start("--all", "--", "true")
            time.sleep(2)
            assert gate.poll() is None, "--all ran while a slot was held"
            b.kill()
            b.wait()
            assert gate.wait(timeout=10) == 0, "--all did not run once every slot was free"
        finally:
            for p in procs:
                p.kill()
                p.wait()
    print("live_slot self-test: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(self_test() if sys.argv[1:] == ["--self-test"] else run(sys.argv[1:]))
