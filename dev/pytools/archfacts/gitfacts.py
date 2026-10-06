"""The index-free half of the facts: git churn and co-change, the ADR index, the CONTEXT.md glossary."""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
from collections import Counter
from itertools import combinations
from pathlib import Path

GIT = "/usr/bin/git" if Path("/usr/bin/git").exists() else (shutil.which("git") or "git")
ADR_TITLE = re.compile(r"^#\s*(?:ADR[- ]?)?(\d+)?[:. -]*\s*(.+)$", re.M)
ADR_STATUS = re.compile(r"\*\*Status\*\*:?\s*([^\n]+?)\s*$|^Status:\s*([^\n]+?)\s*$", re.M)
PATH_IN_TEXT = re.compile(r"(?<![\w/.-])((?:src|tests|dev|docs|benches|\.github)/[\w./-]*\w|[a-z_]+(?:/[a-z_0-9]+)*\.rs)")
TERM = re.compile(r"^\*\*([^*]+)\*\*:\s*$")


def git(root: Path, *args: str) -> str:
    """Stdout of one git command in `root`."""
    return subprocess.run([GIT, "-C", str(root), *args], capture_output=True, text=True, check=True).stdout


def head(root: Path) -> str:
    """The commit the tree is on."""
    return git(root, "rev-parse", "HEAD").strip()


def dirty_hash(root: Path, scopes: tuple[str, ...]) -> str:
    """A hash of the uncommitted changes under `scopes`; empty when the tree is clean there."""
    diff = git(root, "diff", "HEAD", "--", *scopes)
    untracked = git(root, "ls-files", "--others", "--exclude-standard", "--", *scopes)
    if not diff and not untracked.strip():
        return ""
    h = hashlib.sha1(diff.encode())
    for name in sorted(untracked.splitlines()):
        h.update(name.encode())
        path = root / name
        if path.is_file():
            h.update(path.read_bytes())
    return h.hexdigest()[:12]


def parse_log(log: str) -> list[tuple[str, list[tuple[str, int, int]]]]:
    """(sha, [(path, added, deleted)]) per commit of a `--numstat --format=@%H` log."""
    commits: list[tuple[str, list[tuple[str, int, int]]]] = []
    for line in log.splitlines():
        if line.startswith("@"):
            commits.append((line[1:], []))
        elif line.strip() and commits:
            parts = line.split("\t")
            if len(parts) == 3:
                add, rem, path = parts
                if " => " in path:
                    path = re.sub(r"\{[^}]* => ([^}]*)\}", r"\1", path).replace("//", "/")
                    path = path.split(" => ")[-1]
                commits[-1][1].append((path, int(add) if add.isdigit() else 0, int(rem) if rem.isdigit() else 0))
    return commits


def churn_and_cochange(commits: list, max_files: int, min_together: int) -> dict:
    """Per-file churn and co-change pairs; commits wider than `max_files` add churn but no pairs."""
    touched: Counter[str] = Counter()
    lines: Counter[str] = Counter()
    together: Counter[tuple[str, str]] = Counter()
    skipped = 0
    for _, files in commits:
        names = sorted({f for f, _, _ in files})
        for f, add, rem in files:
            lines[f] += add + rem
        touched.update(names)
        if len(names) > max_files:
            skipped += 1
            continue
        together.update(combinations(names, 2))
    churn = [{"path": p, "commits": n, "lines": lines[p]} for p, n in sorted(touched.items(), key=lambda kv: (-kv[1], -lines[kv[0]], kv[0]))]
    pairs = [
        {"a": a, "b": b, "together": n, "a_commits": touched[a], "b_commits": touched[b], "confidence": round(n / min(touched[a], touched[b]), 2)}
        for (a, b), n in together.items() if n >= min_together
    ]
    pairs.sort(key=lambda p: (-p["together"], -p["confidence"], p["a"], p["b"]))
    return {"commits": len(commits), "wide_commits_skipped": skipped, "max_files": max_files, "churn": churn, "cochange": pairs}


def collect_git(root: Path, scope: str, window: int, max_files: int, min_together: int) -> dict:
    """Churn and co-change over the last `window` commits touching `scope`."""
    log = git(root, "log", f"-{window}", "--numstat", "--format=@%H", "--no-renames", "--", scope)
    return churn_and_cochange(parse_log(log), max_files, min_together)


def adr_index(root: Path, adr_dir: Path) -> list[dict]:
    """Number, title, status and mentioned repo paths of every ADR."""
    out = []
    if not adr_dir.is_dir():
        return out
    for path in sorted(adr_dir.glob("*.md")):
        text = path.read_text(errors="replace")
        m = ADR_TITLE.search(text)
        s = ADR_STATUS.search(text)
        paths = set()
        for hit in PATH_IN_TEXT.findall(text):
            hit = hit.rstrip(".")
            for cand in (hit, f"src/{hit}"):
                if (root / cand).exists():
                    paths.add(cand)
                    break
        num = re.match(r"(\d+)", path.name)
        out.append({
            "file": str(path.relative_to(root)),
            "number": num.group(1) if num else "",
            "title": m.group(2).strip() if m else path.stem,
            "status": ((s.group(1) or s.group(2)).strip() if s else ""),
            "paths": sorted(paths),
        })
    return out


def context_terms(path: Path) -> list[dict]:
    """Glossary terms of CONTEXT.md with their section and the words to avoid."""
    if not path.is_file():
        return []
    terms, section = [], ""
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("## "):
            section = line[3:].strip()
        elif m := TERM.match(line):
            terms.append({"term": m.group(1), "section": section, "avoid": ""})
        elif line.startswith("_Avoid_:") and terms:
            terms[-1]["avoid"] = line.split(":", 1)[1].strip()
    return terms
