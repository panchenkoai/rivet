"""The lib tests that grade the Postgres state backend: ONE selection, run by the release gate's
state-migration parity stage and by CI's E2E job. A lib test that reads RIVET_TEST_STATE_URL is
selected by living under a `state::` module path; one that does not is a self-skip no stage grades,
and the gate's offline battery fails on it.

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

#: libtest's substring filter: every lib test whose path contains it runs with the state URL.
FILTER = "state::"


def argv() -> list[str]:
    """The one cargo invocation that runs the state lib tests, the URL reaching them only as RIVET_TEST_STATE_URL."""
    return ["env", "-u", "RIVET_STATE_URL", "-u", "RIVET_GATE_STATE_URL", "cargo", "test", "--manifest-path", str(ROOT / "Cargo.toml"), "--lib", "--", FILTER]


def selected(test: str) -> bool:
    """Whether `argv()` runs `test` (a `module::fn` libtest name)."""
    return FILTER in test


def vacuous(out: str, skips: dict[str, str]) -> list[str]:
    """Why a state-lib run graded nothing: no test passed, or a test self-skipped."""
    passed = sum(int(n) for n in re.findall(r"^test result: \w+\. (\d+) passed", out, re.M))
    return ([] if passed else [f"no `{FILTER}` lib test passed"]) + [f"{k} — {v}" for k, v in skips.items()]


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
