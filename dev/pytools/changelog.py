"""Changelog fragments: `changelog.d/*.md` entries that `assemble` folds into CHANGELOG.md's `## Unreleased`.

    python3 dev/pytools/changelog.py check       # every fragment is well formed
    python3 dev/pytools/changelog.py assemble    # move the fragments into CHANGELOG.md and delete them
    python3 dev/pytools/changelog.py --self-test
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FRAGMENTS = "changelog.d"
README = "README.md"


def fragments(root: Path) -> list[Path]:
    """The fragment files under `root`, newest first by the commit that added them (untracked first), then by name."""
    d = root / FRAGMENTS
    if not d.is_dir():
        return []
    files = sorted(p for p in d.glob("*.md") if p.name != README)

    def added(p: Path) -> tuple[int, str]:
        out = subprocess.run(
            ["git", "log", "--diff-filter=A", "--format=%ct", "--", str(p.relative_to(root))],
            cwd=root, capture_output=True, text=True,
        ).stdout.split()
        return (-(int(out[-1]) if out else sys.maxsize), p.name)

    return sorted(files, key=added)


def problems(text: str) -> list[str]:
    """What is wrong with one fragment's text; empty when it is a list of CHANGELOG entries."""
    lines = text.rstrip("\n").split("\n")
    if not text.strip():
        return ["empty"]
    if not lines[0].startswith("- **"):
        return ["the first line does not start an entry with `- **`"]
    bad = []
    for n, line in enumerate(lines, 1):
        if line and not (line.startswith("- **") or line.startswith("  ")):
            bad.append(f"line {n} is neither an entry (`- **`) nor a continuation (two spaces)")
        if line.startswith("#"):
            bad.append(f"line {n} is a heading; a fragment holds entries only")
    return bad


def assemble_text(changelog: str, entries: list[str]) -> str:
    """`changelog` with `entries` placed first under `## Unreleased`, one blank line between entries."""
    m = re.search(r"^## Unreleased[ \t]*\n\n?", changelog, re.M)
    if not m:
        raise ValueError("CHANGELOG.md has no `## Unreleased` section")
    block = "".join(e.strip("\n") + "\n\n" for e in entries)
    return changelog[: m.end()] + block + changelog[m.end():]


def check(root: Path) -> int:
    bad = 0
    for p in fragments(root):
        for why in problems(p.read_text()):
            print(f"{p.relative_to(root)}: {why}")
            bad += 1
    return 1 if bad else 0


def assemble(root: Path) -> int:
    files = fragments(root)
    if not files:
        print("no fragments to assemble")
        return 0
    if check(root):
        return 1
    log = root / "CHANGELOG.md"
    log.write_text(assemble_text(log.read_text(), [p.read_text() for p in files]))
    for p in files:
        p.unlink()
    print(f"assembled {len(files)} fragment(s) into CHANGELOG.md")
    return 0


def self_test() -> int:
    assert problems("- **A.** text\n  more\n") == []
    assert problems("") == ["empty"]
    assert problems("text\n")[0].startswith("the first line")
    assert problems("- **A.** x\nloose line\n") == ["line 2 is neither an entry (`- **`) nor a continuation (two spaces)"]
    log = "# Changelog\n\n## Unreleased\n\n- **Old.** x\n\n## 0.31.0\n"
    assert assemble_text(log, ["- **New.** y\n", "- **Newer.** z"]) == (
        "# Changelog\n\n## Unreleased\n\n- **New.** y\n\n- **Newer.** z\n\n- **Old.** x\n\n## 0.31.0\n"
    )
    assert assemble_text("# C\n\n## Unreleased\n\n## 0.1.0\n", ["- **A.** a"]) == "# C\n\n## Unreleased\n\n- **A.** a\n\n## 0.1.0\n"
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        (root / FRAGMENTS).mkdir()
        (root / FRAGMENTS / README).write_text("# not a fragment\n")
        (root / FRAGMENTS / "b.md").write_text("- **B.** b\n")
        (root / FRAGMENTS / "a.md").write_text("- **A.** a\n")
        (root / "CHANGELOG.md").write_text(log)
        assert [p.name for p in fragments(root)] == ["a.md", "b.md"]
        assert assemble(root) == 0
        assert not (root / FRAGMENTS / "a.md").exists() and (root / FRAGMENTS / README).exists()
        assert (root / "CHANGELOG.md").read_text().startswith("# Changelog\n\n## Unreleased\n\n- **A.** a\n\n- **B.** b\n\n- **Old.**")
        (root / FRAGMENTS / "bad.md").write_text("## heading\n")
        assert check(root) == 1
    print("changelog self-test: ok")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", nargs="?", choices=["check", "assemble"])
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    if a.cmd == "check":
        return check(ROOT)
    if a.cmd == "assemble":
        return assemble(ROOT)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
