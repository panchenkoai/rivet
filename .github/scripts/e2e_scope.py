#!/usr/bin/env python3
"""Which E2E a pull request needs: `skip` when every changed path is one the E2E lane never reads, else `full`.

    e2e_scope.py --base SHA [--head SHA]   print `scope=full|skip` and `reason=…` (for $GITHUB_OUTPUT)
    e2e_scope.py --self-test

Conservative by construction: a path is skippable only when it is documentation (`*.md`,
`changelog.d/`, `docs/gifs/`) that no test or source file reads, or harness code under a
prefix the E2E lane never imports or calls (`UNUSED_BY_E2E`). Anything else, an unclassified
path included, and an empty diff, selects the full E2E. Which docs files are read is derived
from the tree on every run (string literals and `include_str!` under `src/` and `tests/`), never
from a hand list.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: Harness prefixes the E2E lane neither imports nor calls (the self-test greps the lane and the tests for them).
UNUSED_BY_E2E = ("dev/seeded/", ".github/scripts/", "dev/spikes/")
#: Files under those prefixes that still decide E2E: this script itself.
ALWAYS_FULL = (".github/scripts/e2e_scope.py",)
#: A string literal or `include_str!` that names a file under docs/ (or a top-level *.md).
READ_DOC = re.compile(r'"((?:\.\./)*(?:docs/[A-Za-z0-9_./-]+|[A-Za-z0-9_-]+\.md))"')


def read_docs(root: Path = ROOT) -> set[str]:
    """Every docs path a test or source file names: such a file is read by some build or test, so it is never skippable."""
    found: set[str] = set()
    for base in ("src", "tests"):
        for f in (root / base).rglob("*.rs"):
            for m in READ_DOC.finditer(f.read_text(errors="ignore")):
                name = m.group(1)
                while name.startswith("../"):
                    name = name[3:]
                found.add(name)
    return found


def classify(path: str, read: set[str]) -> str:
    """`docs`, `unused` (harness the E2E lane never touches) or `full`."""
    if path in ALWAYS_FULL or path in read:
        return "full"
    if path.startswith(UNUSED_BY_E2E):
        return "unused"
    if path.startswith("docs/gifs/") or path.startswith("changelog.d/") or path == "CHANGELOG.md":
        return "docs"
    if path.endswith(".md"):
        return "docs"
    return "full"


def scope(paths: list[str], read: set[str]) -> tuple[str, str]:
    """`(scope, reason)` for a diff of `paths`."""
    if not paths:
        return "full", "no changed path was read: the full E2E runs"
    kinds = {p: classify(p, read) for p in paths}
    full = [p for p, k in kinds.items() if k == "full"]
    if full:
        return "full", f"`{full[0]}` can change what E2E runs"
    if all(k == "docs" for k in kinds.values()):
        return "skip", "docs-only"
    return "skip", "docs and harness the E2E lane does not use"


def changed(base: str, head: str) -> list[str]:
    """The paths that differ between the merge base of `base`..`head` and `head`; a failed diff raises."""
    out = subprocess.run(["git", "-C", str(ROOT), "diff", "--name-only", f"{base}...{head}"],
                         capture_output=True, text=True, check=True).stdout
    return [line for line in out.splitlines() if line.strip()]


def self_test() -> int:
    """The verdicts the workflow relies on, and the proof that the skippable harness is unused by E2E."""
    read = read_docs()
    assert scope(["src/load/mod.rs"], read)[0] == "full"
    assert scope(["tests/live/live_cdc.rs"], read)[0] == "full"
    assert scope(["docs/a.md", "src/main.rs"], read)[0] == "full", "one code path makes it full"
    assert scope(["some/new/place.txt"], read)[0] == "full", "an unclassified path is full"
    assert scope(["Cargo.toml"], read)[0] == "full" and scope(["docker-compose.yaml"], read)[0] == "full"
    assert scope([".github/workflows/ci.yml"], read)[0] == "full"
    assert scope([".github/scripts/e2e_scope.py"], read)[0] == "full", "this script decides E2E"
    assert scope([], read)[0] == "full", "an empty diff proves nothing"
    assert scope(["docs/concepts.md", "docs/how-to/new-page.md", "changelog.d/x.md", "CHANGELOG.md", "docs/gifs/a.gif"], read) \
        == ("skip", "docs-only")
    assert scope(["dev/seeded/seeds.yaml", "docs/a.md"], read)[0] == "skip"
    assert scope(["docs/type-capability-matrix.yaml"], read)[0] == "full", "a yaml under docs is not skipped"
    # Every docs file a test or source file names is full: derived, so a new reader is covered at once.
    assert scope(["README.md"], read)[0] == "full", "a test reads README.md"
    assert "docs/reference/errors.md" in read, "src/error.rs includes the generated errors reference"
    for path in read:
        assert classify(path, read) == "full", path
    # The skippable harness is not used by the E2E lane: no test, source file or E2E step names it.
    ci = (ROOT / ".github/workflows/ci.yml").read_text()
    lane = ci[ci.index("  e2e-build:"):]
    # What the lane runs: its own steps, the source, and every test file that holds an ignored test
    # (E2E runs `--run-ignored only`; the offline guards run in `Tests`).
    rs = [f.read_text(errors="ignore") for f in (ROOT / "tests").rglob("*.rs")]
    texts = [lane] + [t for t in rs if "#[ignore" in t] + [f.read_text(errors="ignore") for f in (ROOT / "src").rglob("*.rs")]
    for prefix in UNUSED_BY_E2E:
        module = prefix.rstrip("/").replace("/", ".")
        hits = [t for t in texts if prefix in t or f"-m {module}" in t]
        assert not hits, f"{prefix} is used by the E2E lane or a test, so it cannot be skippable"
    print(f"self-test ok: e2e scope — {len(read)} docs files read by the build or tests stay full")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) not in (2, 4) or argv[0] != "--base" or (len(argv) == 4 and argv[2] != "--head"):
        print(__doc__, file=sys.stderr)
        return 2
    head = argv[3] if len(argv) == 4 else "HEAD"
    verdict, reason = scope(changed(argv[1], head), read_docs())
    print(f"scope={verdict}")
    print(f"reason={reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
