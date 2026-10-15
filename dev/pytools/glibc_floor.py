"""The newest glibc symbol version a Linux binary needs, refused past a floor.

    python3 -m dev.pytools.glibc_floor --max 2.28 BIN [BIN ...]
    python3 -m dev.pytools.glibc_floor --self-test

A binary links the glibc of the machine that built it; one built on Ubuntu 24.04 needs
GLIBC_2.39 and does not start on Debian 12, RHEL 8/9 or the official Airflow images.
The release builds the `-glibc2.28` assets in manylinux_2_28 and checks them here.
"""

from __future__ import annotations

import re
import subprocess
import sys

_VERSION = re.compile(r"\bGLIBC_(\d+(?:\.\d+)+)\b")


def _key(version: str) -> tuple[int, ...]:
    """`2.28` -> (2, 28), ordered numerically."""
    return tuple(int(p) for p in version.split("."))


def newest(dynamic_symbols: str) -> str | None:
    """The newest `GLIBC_x.y` named in `objdump -T` output, or None when there is none."""
    found = {m.group(1) for m in _VERSION.finditer(dynamic_symbols)}
    return max(found, key=_key) if found else None


def check(path: str, floor: str) -> str | None:
    """None when `path` needs nothing newer than `floor`, else the line saying what it needs."""
    out = subprocess.run(["objdump", "-T", path], capture_output=True, text=True, check=True).stdout
    need = newest(out)
    if need is None:
        return f"{path}: names no GLIBC_ symbol version (static, or not a glibc binary)"
    print(f"{path}: needs GLIBC_{need} (floor {floor})")
    return None if _key(need) <= _key(floor) else f"{path}: needs GLIBC_{need}, newer than the floor GLIBC_{floor}"


def self_test() -> int:
    """`newest` orders numerically (2.9 < 2.28 < 2.39) and ignores other version tags."""
    sample = "0 DF *UND* GLIBC_2.17 memcpy\n0 DF *UND* GLIBC_2.28 fcntl64\n0 DF *UND* GLIBC_2.9 pipe2\n0 DF *UND* GCC_3.0 _Unwind\n"
    assert newest(sample) == "2.28", newest(sample)
    assert newest(sample + "0 DF *UND* GLIBC_2.39 __isoc23_strtol\n") == "2.39"
    assert newest("0 DF *UND* GCC_4.2.0 x\n") is None
    assert _key("2.28") > _key("2.9")
    print("glibc_floor self-test ok")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) < 3 or argv[0] != "--max":
        print(__doc__, file=sys.stderr)
        return 2
    floor, paths = argv[1], argv[2:]
    problems = [p for p in (check(path, floor) for path in paths) if p]
    for p in problems:
        print(f"::error::{p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
