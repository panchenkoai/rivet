"""Platform wheels of `rivet-cli` from the release's own tarballs: the binaries, not a rebuild.

    python3 -m dev.pytools.pypi_wheels --tag v0.31.0 --dist artifacts --out wheels
    python3 -m dev.pytools.pypi_wheels one --tarball T.tar.gz --platform manylinux_2_28_x86_64 --tag pr-601 --out wheels
    python3 -m dev.pytools.pypi_wheels --self-test

Each wheel is `rivet_cli-<version>-py3-none-<platform>.whl`: `rivet` and `rivet-mcp` as scripts
(pip puts them in the environment's bin directory) and a `rivet_cli` module whose `path()` finds
them there, for a worker whose bin directory is not on PATH. Only the assets a platform tag can
honestly name are packed: the `-glibc2.28` Linux builds (manylinux_2_28, checked by glibc228.yml)
and the macOS builds; the default Linux assets need glibc 2.39 and are not a manylinux wheel.
"""

from __future__ import annotations

import base64
import hashlib
import io
import re
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

NAME = "rivet-cli"
DIST = "rivet_cli"

# Release asset target -> wheel platform tag. macOS minimums are rustc's defaults for each arch.
PLATFORMS = {
    "x86_64-unknown-linux-gnu-glibc2.28": "manylinux_2_28_x86_64",
    "aarch64-unknown-linux-gnu-glibc2.28": "manylinux_2_28_aarch64",
    "aarch64-apple-darwin": "macosx_11_0_arm64",
    "x86_64-apple-darwin": "macosx_10_12_x86_64",
}
BINARIES = ("rivet", "rivet-mcp")

MODULE = '''"""The rivet binaries this wheel installed."""

import os
import shutil
import sysconfig

__version__ = "{version}"


def path(name: str = "rivet") -> str:
    """Absolute path of `name` from this wheel: the environment's scripts directory, else PATH."""
    candidate = os.path.join(sysconfig.get_path("scripts"), name)
    if os.access(candidate, os.X_OK):
        return candidate
    found = shutil.which(name)
    if found is None:
        raise FileNotFoundError(f"{{name}} is not in {{sysconfig.get_path('scripts')}} nor on PATH")
    return found
'''

METADATA = """Metadata-Version: 2.1
Name: {name}
Version: {version}
Summary: The rivet binary: PostgreSQL / MySQL / SQL Server / MongoDB / Oracle to Parquet or CSV, with CDC.
Home-page: https://github.com/rivet-lab/rivet
License: MIT
Requires-Python: >=3.8
Description-Content-Type: text/markdown

# rivet-cli

The `rivet` and `rivet-mcp` binaries as a wheel, the same builds as the GitHub release assets.
`pip install rivet-cli` puts them in the environment's `bin` directory; `rivet_cli.path()`
returns the installed `rivet` for a process whose PATH does not include that directory.

Linux wheels are `manylinux_2_28` (any glibc 2.28 or newer: RHEL 8+, Debian 10+, Ubuntu 20.04+,
the Apache Airflow images); macOS wheels cover Apple Silicon and Intel.
"""


def pep440(tag: str) -> str:
    """`v0.31.0` -> `0.31.0`; `v0.2.0-beta.1` -> `0.2.0b1`; `-alpha.N` / `-rc.N` likewise; a non-release
    name (a pull request's `pr-601`) -> `0.0.0.dev0`, never published."""
    if not tag.startswith("v"):
        return "0.0.0.dev0"
    version = tag[1:] if tag.startswith("v") else tag
    m = re.fullmatch(r"(\d+\.\d+\.\d+)(?:-(alpha|beta|rc)\.?(\d+))?", version)
    if not m:
        raise ValueError(f"tag {tag!r} is not vX.Y.Z or vX.Y.Z-(alpha|beta|rc).N")
    base, pre, n = m.groups()
    return base + ({"alpha": "a", "beta": "b", "rc": "rc"}[pre] + n if pre else "")


def _digest(data: bytes) -> str:
    """The RECORD hash form: sha256, urlsafe base64, no padding."""
    return "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


def _binaries(tarball: Path) -> dict[str, bytes]:
    """The `rivet` and `rivet-mcp` files of one release tarball, by name."""
    found: dict[str, bytes] = {}
    with tarfile.open(tarball) as tar:
        for member in tar.getmembers():
            name = Path(member.name).name
            if member.isfile() and name in BINARIES:
                found[name] = tar.extractfile(member).read()
    missing = [b for b in BINARIES if b not in found]
    if missing:
        raise ValueError(f"{tarball.name}: no {missing} in the tarball")
    return found


def build_wheel(tarball: Path, platform: str, version: str, out: Path) -> Path:
    """Write one wheel for `platform` from `tarball` into `out`; its path."""
    files: list[tuple[str, bytes, int]] = []
    for name, data in sorted(_binaries(tarball).items()):
        files.append((f"{DIST}-{version}.data/scripts/{name}", data, 0o755))
    files.append((f"{DIST}/__init__.py", MODULE.format(version=version).encode(), 0o644))
    info = f"{DIST}-{version}.dist-info"
    files.append((f"{info}/METADATA", METADATA.format(name=NAME, version=version).encode(), 0o644))
    wheel_meta = f"Wheel-Version: 1.0\nGenerator: rivet dev/pytools/pypi_wheels.py\nRoot-Is-Purelib: false\nTag: py3-none-{platform}\n"
    files.append((f"{info}/WHEEL", wheel_meta.encode(), 0o644))
    files.append((f"{info}/top_level.txt", f"{DIST}\n".encode(), 0o644))
    record = "".join(f"{p},{_digest(d)},{len(d)}\n" for p, d, _ in files) + f"{info}/RECORD,,\n"
    files.append((f"{info}/RECORD", record.encode(), 0o644))
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{DIST}-{version}-py3-none-{platform}.whl"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as whl:
        for name, data, mode in files:
            entry = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            entry.external_attr = (0o100000 | mode) << 16
            entry.compress_type = zipfile.ZIP_DEFLATED
            whl.writestr(entry, data)
    return path


def build_all(tag: str, dist: Path, out: Path) -> list[Path]:
    """One wheel per platform in PLATFORMS whose tarball `rivet-<tag>-<target>.tar.gz` is in `dist`."""
    version = pep440(tag)
    built = []
    for target, platform in PLATFORMS.items():
        tarball = dist / f"rivet-{tag}-{target}.tar.gz"
        if not tarball.is_file():
            raise FileNotFoundError(f"{tarball}: the release asset for {platform} is missing")
        built.append(build_wheel(tarball, platform, version, out))
    return built


def self_test() -> int:
    """A wheel built from a fake tarball names its tag, marks the scripts executable and records every file's hash."""
    assert pep440("v0.31.0") == "0.31.0"
    assert pep440("v0.2.0-beta.1") == "0.2.0b1"
    assert pep440("v1.0.0-rc.2") == "1.0.0rc2"
    assert pep440("pr-601") == "0.0.0.dev0"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        tarball = root / "rivet-v0.31.0-x86_64-unknown-linux-gnu-glibc2.28.tar.gz"
        with tarfile.open(tarball, "w:gz") as tar:
            for name in BINARIES:
                data = f"#!/bin/sh\necho {name}\n".encode()
                member = tarfile.TarInfo(f"rivet-v0.31.0-x86_64-unknown-linux-gnu-glibc2.28/{name}")
                member.size, member.mode = len(data), 0o755
                tar.addfile(member, io.BytesIO(data))
        whl = build_wheel(tarball, "manylinux_2_28_x86_64", "0.31.0", root / "out")
        assert whl.name == "rivet_cli-0.31.0-py3-none-manylinux_2_28_x86_64.whl", whl.name
        with zipfile.ZipFile(whl) as z:
            names = z.namelist()
            assert "rivet_cli-0.31.0.data/scripts/rivet" in names and "rivet_cli/__init__.py" in names, names
            assert (z.getinfo("rivet_cli-0.31.0.data/scripts/rivet").external_attr >> 16) & 0o111, "scripts are executable"
            record = z.read("rivet_cli-0.31.0.dist-info/RECORD").decode()
            for name in names:
                if not name.endswith("RECORD"):
                    line = next(l for l in record.splitlines() if l.startswith(name + ","))
                    assert line.split(",")[1] == _digest(z.read(name)), name
            assert "Tag: py3-none-manylinux_2_28_x86_64" in z.read("rivet_cli-0.31.0.dist-info/WHEEL").decode()
        try:
            build_all("v0.31.0", root, root / "all")
        except FileNotFoundError as e:
            assert "missing" in str(e)
        else:
            raise AssertionError("a missing platform asset must stop the build")
    print("pypi_wheels self-test ok")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if argv[:1] == ["one"]:
        args = dict(zip(argv[1::2], argv[2::2]))
        if set(args) != {"--tarball", "--platform", "--tag", "--out"}:
            print(__doc__, file=sys.stderr)
            return 2
        print(build_wheel(Path(args["--tarball"]), args["--platform"], pep440(args["--tag"]), Path(args["--out"])))
        return 0
    args = dict(zip(argv[::2], argv[1::2]))
    if set(args) != {"--tag", "--dist", "--out"}:
        print(__doc__, file=sys.stderr)
        return 2
    for path in build_all(args["--tag"], Path(args["--dist"]), Path(args["--out"])):
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
