"""The lib tests that grade the Postgres state backend: ONE selection, run by the release gate's
state-migration parity stage and by CI's E2E job. The selection is DERIVED: every `src/` module
whose source reads RIVET_TEST_STATE_URL is a libtest filter, so a reader in any module is selected
(load::orchestrate's PG ledger test self-skipped ungraded for a cycle under a fixed `state::`).

    RIVET_TEST_STATE_URL=postgresql://… python3 -m dev.release_oracle.state_lib
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

try:  # importable both as a package module and as a plain sibling file
    from .core import ROOT, self_skipped
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import ROOT, self_skipped  # type: ignore

#: The source expression a lib test reads the state URL through.
READER = re.compile(r'var\(\s*"RIVET_TEST_STATE_URL"\s*\)')


def filters(src: Path = ROOT / "src") -> list[str]:
    """libtest substring filters, one per module whose source reads RIVET_TEST_STATE_URL (`a::b::`)."""
    found = set()
    for f in sorted(src.rglob("*.rs")):
        if READER.search(f.read_text(encoding="utf-8")):
            parts = f.relative_to(src).with_suffix("").parts
            if parts[-1] in ("mod", "lib"):
                parts = parts[:-1]
            found.add("::".join(parts) + "::" if parts else "")
    return sorted(found)


def argv() -> list[str]:
    """The one cargo invocation that runs the state lib tests, the URL reaching them only as RIVET_TEST_STATE_URL."""
    return ["env", "-u", "RIVET_STATE_URL", "-u", "RIVET_GATE_STATE_URL", "cargo", "test", "--manifest-path", str(ROOT / "Cargo.toml"), "--lib", "--", *filters()]


def selected(test: str) -> bool:
    """Whether `argv()` runs `test` (a `module::fn` libtest name)."""
    return any(f in test for f in filters())


def vacuous(out: str, skips: dict[str, str]) -> list[str]:
    """Why a state-lib run graded nothing: no test passed, or a test self-skipped."""
    passed = sum(int(n) for n in re.findall(r"^test result: \w+\. (\d+) passed", out, re.M))
    return ([] if passed else [f"no lib test under {', '.join(filters())} passed"]) + [f"{k} — {v}" for k, v in skips.items()]


def main() -> int:
    """Run the selection, echo its output, and fail when it failed or graded nothing."""
    skip_log = Path(tempfile.mkdtemp(prefix="rivet-state-lib-")) / "skips"
    p = subprocess.run(argv(), env={**os.environ, "RIVET_SKIP_LOG": str(skip_log)},
                       capture_output=True, text=True)
    sys.stdout.write(p.stdout)
    sys.stderr.write(p.stderr)
    bad = vacuous(p.stdout, self_skipped(skip_log))
    for why in bad:
        print(f"::error::a state lib test graded nothing: {why}")
    return p.returncode or (1 if bad else 0)


if __name__ == "__main__":
    sys.exit(main())
