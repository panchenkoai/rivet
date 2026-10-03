#!/usr/bin/env python3
"""The mechanical PR rules: text hygiene, raised ratchets, CHANGELOG, and the local `make pr-ready` run.

    pr_ready.py run [--fast] [--base REV] [--body FILE]   everything a PR must pass; prints the summary block
    pr_ready.py ci --base SHA --event PATH [--body FILE]  CI: commits, PR text, raised ratchets, CHANGELOG
    pr_ready.py text --message FILE | --staged | --commits RANGE   the hooks
    pr_ready.py --self-test

Stdlib only, so the hooks and CI run it on a bare `python3`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GIT = "/usr/bin/git" if Path("/usr/bin/git").exists() else (shutil.which("git") or "git")
CARGO = str(Path.home() / ".cargo/bin/cargo")
if not Path(CARGO).exists():
    CARGO = shutil.which("cargo") or "cargo"

CYRILLIC = re.compile(r"[\u0400-\u04ff]")
CLOSING = re.compile(r"\b(fix|fixe[sd]|close[sd]?|resolve[sd]?):? +#[0-9]+", re.I)
TRAILER = re.compile(r"^\s*(co-authored-by|claude-session):", re.I)
BINARY = re.compile(r"\.(gif|png|jpg|jpeg|parquet|ico|pdf)$")
MARK = re.compile(r"ratchet-pin:\s*([\w.-]+)(?:\s+(sum|strings|lines))?(?:\s+(min))?")
STRING = re.compile(r'"(?:[^"\\]|\\.)*"')
INT = re.compile(r"\b\d+\b")
# Definitions that look like a pin by name; each must carry a `ratchet-pin:` marker.
PIN_NAME = re.compile(
    r"^\s*(?:pub(?:\([a-z]+\))?\s+)?(?:const\s+|static\s+)?"
    r"([A-Za-z0-9_]*(?:CEILING|RATCHET|BASELINE|FLOOR|ceiling|ratchet)|PIN_[A-Z0-9_]+|KNOWN_DEFECTS)\b"
    r"\s*(?::[^=\n]*)?[:=]\s*[\[{(\d&]"
)
GUARD_HOMES = ("tests/", "dev/pytools/", "dev/release_oracle/", ".github/", "docs/")
NO_CHANGE = re.compile(r"no user-visible change", re.I)
RATCHET_HEADING = re.compile(r"^\s*(?:#+\s*|\*\*)ratchets raised", re.I)
HEADING = re.compile(r"^\s*(?:#+\s|\*\*[^*]+\*\*\s*$)")


def git(*args: str, check: bool = True) -> str:
    """Run git in the repo and return stdout."""
    p = subprocess.run([GIT, *args], cwd=ROOT, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise SystemExit(f"git {' '.join(args)} failed: {p.stderr.strip()}")
    return p.stdout


# ── text hygiene ────────────────────────────────────────────────────────────


def text_violations(text: str, where: str, *, message: bool = False) -> list[str]:
    """Cyrillic, a closing keyword before `#N`, or an attribution trailer in commit or PR text."""
    out = []
    for i, line in enumerate(text.splitlines(), 1):
        if message and line.startswith("#"):
            continue
        if CYRILLIC.search(line):
            out.append(f"{where}:{i}: Cyrillic (English only): {line.strip()[:100]}")
        m = CLOSING.search(line)
        if m:
            out.append(f"{where}:{i}: closing keyword `{m.group(0)}` (write `see #N`): {line.strip()[:100]}")
        if TRAILER.match(line):
            out.append(f"{where}:{i}: attribution trailer: {line.strip()[:100]}")
    return out


def added_line_violations(diff: str) -> list[str]:
    """Cyrillic in the lines a unified diff ADDS; a line marked `allow-cyrillic` is test data."""
    out, path = [], None
    for line in diff.splitlines():
        if line.startswith("+++ "):
            path = line[6:] if line.startswith("+++ b/") else None
        elif line.startswith("+") and path and not BINARY.search(path):
            if CYRILLIC.search(line) and "allow-cyrillic" not in line:
                out.append(f"{path}: Cyrillic in an added line: {line[1:].strip()[:100]}")
    return out


def commit_violations(rng: str) -> list[str]:
    """Text violations in every non-merge commit message of `rng`."""
    out = []
    for sha in git("rev-list", "--no-merges", rng).split():
        out += text_violations(git("log", "-1", "--format=%B", sha), f"commit {sha[:10]}")
    return out


# ── ratchet pins ────────────────────────────────────────────────────────────


def _code(line: str) -> tuple[str, int]:
    """The line with string literals blanked and any trailing comment cut, plus its string count."""
    n = len(STRING.findall(line))
    code = STRING.sub('""', line)
    for opener in ("//", "#"):
        if opener in code:
            code = code[: code.index(opener)]
    return code, n


def pins(text: str, path: str) -> dict[str, tuple[int, bool]]:
    """Every `ratchet-pin:` in a file: name → (measured value, is_floor). A shape it cannot read is an error."""
    lines = text.splitlines()
    out: dict[str, tuple[int, bool]] = {}
    for i, line in enumerate(lines):
        m = MARK.search(line)
        if not m or m.group(1) == "end":
            continue
        name, measure, floor = m.groups()
        if name in out:
            raise ValueError(f"{path}:{i + 1}: ratchet-pin {name} declared twice")
        if measure is None:
            ints = INT.findall(_code(line[: m.start()])[0])
            if not ints:
                raise ValueError(f"{path}:{i + 1}: ratchet-pin {name} has no integer on its line")
            value = int(ints[-1])
        else:
            body, closed = [], False
            for later in lines[i + 1 :]:
                if MARK.search(later) and MARK.search(later).group(1) == "end":
                    closed = True
                    break
                body.append(later)
            if not closed and measure != "lines":
                raise ValueError(f"{path}:{i + 1}: ratchet-pin {name} {measure} has no `ratchet-pin: end`")
            if measure == "lines":
                value = sum(1 for b in body if b.strip() and not b.strip().startswith(("#", "//")))
            elif measure == "strings":
                value = sum(_code(b)[1] for b in body if not b.strip().startswith(("#", "//")))
            else:
                value = sum(int(x) for b in body for x in INT.findall(_code(b)[0]))
        out[name] = (value, bool(floor))
    return out


def pin_files(rev: str) -> dict[str, str]:
    """Path → text of every tracked file holding a `ratchet-pin:` marker at `rev`."""
    out = {}
    for hit in git("grep", "-l", "-e", "ratchet-pin:", rev, check=False).splitlines():
        path = hit.split(":", 1)[1]
        if path != "dev/pytools/pr_ready.py":
            out[path] = git("show", f"{rev}:{path}")
    return out


def all_pins(rev: str) -> dict[str, tuple[int, bool]]:
    """`path:NAME` → (value, is_floor) over the tree at `rev`."""
    return {f"{p}:{n}": v for p, t in pin_files(rev).items() for n, v in pins(t, p).items()}


def raised(base: dict, head: dict) -> list[str]:
    """Pins whose bound got looser since `base`: a ceiling that grew, a floor that fell, a pin removed."""
    out = [f"{key} {old} -> removed" for key, (old, _) in sorted(base.items()) if key not in head]
    for key, (new, floor) in sorted(head.items()):
        if key in base:
            old = base[key][0]
            if (new < old) if floor else (new > old):
                out.append(f"{key} {old} -> {new}")
    return out


def unmarked_pins() -> list[str]:
    """Definitions named like a pin (CEILING/RATCHET/BASELINE/FLOOR/PIN_) that carry no `ratchet-pin:` marker."""
    out = []
    for path in git("ls-files", *GUARD_HOMES).split():
        if not path.endswith((".rs", ".py", ".yml", ".yaml")) or path.startswith("tests/live/") or path.endswith("pr_ready.py"):
            continue
        lines = (ROOT / path).read_text(errors="ignore").splitlines() + [""]
        for i, line in enumerate(lines[:-1]):
            # rustfmt moves a trailing comment after `= &[` onto the next line.
            marked = "ratchet-pin:" in line or lines[i + 1].lstrip().startswith("// ratchet-pin:")
            if PIN_NAME.match(line) and not marked and not line.lstrip().startswith(("//", "#")):
                out.append(f"{path}:{i + 1}: {line.strip()[:90]}")
    return out


def declared(body: str) -> str:
    """The text of the PR body's "Ratchets raised" section (empty when absent)."""
    lines, on, out = body.splitlines(), False, []
    for line in lines:
        if RATCHET_HEADING.match(line):
            on = True
            continue
        if on and HEADING.match(line):
            break
        if on:
            out.append(line)
    return "\n".join(out)


def undeclared(raises: list[str], body: str) -> list[str]:
    """Raised pins whose NAME the body's "Ratchets raised" section does not mention."""
    section = declared(body)
    return [r for r in raises if r.split(" ")[0].rsplit(":", 1)[1] not in section]


# ── CHANGELOG ───────────────────────────────────────────────────────────────


def unreleased(text: str) -> str:
    """The body of CHANGELOG's `## Unreleased` section."""
    m = re.search(r"^## Unreleased[ \t]*\n(.*?)(?=^## )", text, re.M | re.S)
    if not m:
        raise ValueError("CHANGELOG.md has no `## Unreleased` section ending at a `## ` heading")
    return m.group(1)


def changelog_verdict(changed: list[str], base_log: str, head_log: str, body: str | None) -> tuple[str, str]:
    """(verdict, line): a src/ change needs an Unreleased entry or an explicit "No user-visible change"."""
    src = [p for p in changed if p.startswith("src/")]
    if not src:
        return "N/A", "no change under src/"
    if unreleased(base_log) != unreleased(head_log):
        return "PASS", f"{len(src)} src/ file(s) changed; CHANGELOG Unreleased has an entry"
    if body is not None and NO_CHANGE.search(body):
        return "PASS", f"{len(src)} src/ file(s) changed; PR body declares No user-visible change"
    if body is None:
        return "DECLARE", f"{len(src)} src/ file(s) changed and no CHANGELOG Unreleased entry: add one, or write 'No user-visible change' in the PR body"
    return "FAIL", f"{len(src)} src/ file(s) changed, no CHANGELOG Unreleased entry and no 'No user-visible change' line in the PR body"


# ── the branch checks (CI and local) ────────────────────────────────────────


def branch_rows(base: str, body: str | None) -> list[tuple[str, str, str]]:
    """(check, verdict, line) for every rule graded off git alone."""
    mb = git("merge-base", base, "HEAD").strip()
    rows = []
    bad = commit_violations(f"{mb}..HEAD")
    n = len(git("rev-list", "--no-merges", f"{mb}..HEAD").split())
    rows.append(("commit messages", "FAIL" if bad else "PASS", "; ".join(bad) or f"{n} commit(s): no Cyrillic, closing keyword or trailer"))
    if body is not None:
        bad = text_violations(body, "PR text")
        rows.append(("PR title/body", "FAIL" if bad else "PASS", "; ".join(bad) or "no Cyrillic, closing keyword or trailer"))
    diff = git("diff", "-U0", "--no-color", "--no-ext-diff", f"{mb}...HEAD")
    bad = added_line_violations(diff)
    added = sum(1 for d in diff.splitlines() if d.startswith("+") and not d.startswith("+++ "))
    rows.append(("Cyrillic in added lines", "FAIL" if bad else "PASS", "; ".join(bad[:5]) or f"none in {added} added lines"))
    loose = unmarked_pins()
    rows.append(("pins carry a marker", "FAIL" if loose else "PASS", "; ".join(loose[:5]) or f"{len(all_pins('HEAD'))} ratchet pins discovered"))
    up = raised(all_pins(mb), all_pins("HEAD"))
    if not up:
        rows.append(("ratchets raised", "PASS", "none"))
    elif body is None:
        rows.append(("ratchets raised", "DECLARE", "declare under 'Ratchets raised' in the PR body: " + "; ".join(up)))
    else:
        missing = undeclared(up, body)
        rows.append(("ratchets raised", "FAIL" if missing else "PASS", ("not declared in the PR body: " + "; ".join(missing)) if missing else "declared: " + "; ".join(up)))
    changed = git("diff", "--name-only", f"{mb}...HEAD").split()
    head_log = git("show", "HEAD:CHANGELOG.md")
    base_log = git("show", f"{mb}:CHANGELOG.md")
    rows.append(("CHANGELOG", *changelog_verdict(changed, base_log, head_log, body)))
    return rows


def print_rows(title: str, rows: list[tuple[str, str, str]]) -> None:
    """The summary block, one line per check, ready to paste into a PR body."""
    print("```")
    print(title)
    for name, verdict, line in rows:
        print(f"{name:<26} {verdict:<8} {line}")
    print("```")


# ── local steps (cargo) ─────────────────────────────────────────────────────


def target_dir() -> Path:
    """The cargo target dir this run builds into."""
    return Path(os.environ.get("CARGO_TARGET_DIR") or ROOT / "target")


def run_logged(cmd: list[str], log: Path, env: dict | None = None) -> int:
    """Run `cmd` with output to `log`; return its exit code."""
    with log.open("w") as fh:
        return subprocess.run(cmd, cwd=ROOT, stdout=fh, stderr=subprocess.STDOUT, env=env).returncode


def offline_suite(out: Path) -> tuple[str, str]:
    """Lib/bin unit tests (threaded) and the integration suites under nextest, as pre-push runs them."""
    unit = out / "unit.log"
    rc1 = run_logged([CARGO, "test", "--lib", "--bins"], unit)
    results = re.findall(r"^test result: (\w+)\. (\d+) passed; (\d+) failed", unit.read_text(errors="ignore"), re.M)
    passed = sum(int(p) for _, p, _ in results)
    integ = out / "nextest.log"
    rc2 = run_logged([CARGO, "nextest", "run", "--no-fail-fast", "-E", "kind(test)"], integ)
    summary = re.findall(r"^\s*Summary \[.*$", integ.read_text(errors="ignore"), re.M)
    m = re.search(r"(\d+) tests? run: (\d+) passed", summary[-1]) if summary else None
    line = f"unit: {passed} passed in {len(results)} binaries; integration: {summary[-1].strip() if summary else 'NO Summary line'}"
    ok = rc1 == 0 and rc2 == 0 and passed > 0 and results and all(r[0] == "ok" for r in results) and m and int(m.group(2)) > 0
    return ("PASS" if ok else "FAIL"), line + ("" if ok else f" (exit {rc1}/{rc2}; logs {out})")


def clippy(out: Path, label: str, extra: list[str]) -> tuple[str, str]:
    """`cargo clippy --all-targets -D warnings` with `extra` feature flags."""
    log = out / f"clippy-{label}.log"
    rc = run_logged([CARGO, "clippy", "--all-targets", *extra, "--", "-D", "warnings"], log)
    text = log.read_text(errors="ignore")
    fin = re.findall(r"^\s*Finished .*$", text, re.M)
    errs = len(re.findall(r"^error", text, re.M))
    if rc == 0 and fin:
        return "PASS", fin[-1].strip()
    return "FAIL", f"exit {rc}, {errs} error line(s); log {log}"


COMPILED = re.compile(r"^\s*Compiling rivet-cli\b", re.M)
DONE = re.compile(r"(\d+) mutants tested in [^:]*: (.*)")


def mutant_verdict(mdir: Path, rc: int, log_text: str) -> tuple[str, str]:
    """Grade a finished `cargo mutants` output dir: every mutant compiled, none missed, none timed out."""
    if rc not in (0, 2, 3):
        return "FAIL", f"cargo-mutants exited {rc}: the tool or the tree broke, not a verdict"
    done = DONE.search(log_text)
    if not done:
        return "FAIL", "no `N mutants tested` summary line: the run has no verdict"
    # The test phase legitimately prints `Fresh` after the build phase compiled the mutant; a log with no `Compiling` was never built.
    fresh = [p.name for p in sorted((mdir / "log").glob("*.log")) if p.name != "baseline.log" and not COMPILED.search(p.read_text(errors="ignore"))]
    if fresh:
        return "FAIL", f"{len(fresh)} mutant build(s) were Fresh (never compiled): {', '.join(fresh[:3])}"
    missed = [m for m in (mdir / "missed.txt").read_text().splitlines() if m] if (mdir / "missed.txt").exists() else []
    timeout = [m for m in (mdir / "timeout.txt").read_text().splitlines() if m] if (mdir / "timeout.txt").exists() else []
    if missed or timeout:
        return "FAIL", f"{done.group(0)} | missed: {'; '.join(missed[:5])} {('| timeout: ' + '; '.join(timeout[:3])) if timeout else ''}".strip()
    return "PASS", done.group(0)


def mutants(out: Path, base: str, jobs: int) -> tuple[str, str]:
    """`cargo mutants --in-diff` over the branch diff, `-- --lib --bins` as CI runs it, never on a shared target dir."""
    mb = git("merge-base", base, "HEAD").strip()
    diff = git("diff", f"{mb}...HEAD")
    if not re.search(r"^\+\+\+ b/.*\.rs$", diff, re.M):
        return "N/A", "no .rs file in the diff"
    (out / "pr.diff").write_text(diff)
    env = {k: v for k, v in os.environ.items() if k != "CARGO_TARGET_DIR"}
    listing = out / "mutants-list.log"
    rc = subprocess.run([CARGO, "mutants", "--in-diff", str(out / "pr.diff"), "--list", "--colors=never"],
                        cwd=ROOT, env=env, stdout=listing.open("w"), stderr=subprocess.STDOUT).returncode
    if rc != 0:
        return "FAIL", f"cargo mutants --list exited {rc}: cannot tell 'no mutants' from a broken tool; log {listing}"
    count = sum(1 for line in listing.read_text().splitlines() if line.strip())
    budget = int(re.search(r"MUTANTS_DIFF_BUDGET:\s*(\d+)", (ROOT / ".github/workflows/ci.yml").read_text()).group(1))
    if count == 0:
        return "PASS", "0 mutants in the diff"
    if count > budget:
        return "NOT RUN", f"{count} mutants > CI budget {budget}: CI does not grade this diff either; split the PR"
    log = out / "mutants.log"
    rc = run_logged([CARGO, "mutants", "--in-diff", str(out / "pr.diff"), "-o", str(out), "-j", str(jobs),
                     "--colors=never", "--", "--lib", "--bins"], log, env=env)
    verdict, line = mutant_verdict(out / "mutants.out", rc, log.read_text(errors="ignore"))
    return verdict, f"{line} (whole diff; CI may excuse a miss in a function the offline suite never executes)"


def run_local(args: argparse.Namespace) -> int:
    """`make pr-ready`: every check, then one summary block."""
    out = Path(tempfile.mkdtemp(prefix="pr-ready-"))
    shutil.rmtree(target_dir() / "package", ignore_errors=True)
    body = Path(args.body).read_text() if args.body else None
    rows = []
    for name, step in (
        ("offline suite", lambda: offline_suite(out)),
        ("clippy default", lambda: clippy(out, "default", [])),
        ("clippy jemalloc", lambda: clippy(out, "jemalloc", ["--no-default-features", "--features", "jemalloc"])),
    ):
        print(f"pr-ready: {name} ...", file=sys.stderr, flush=True)
        rows.append((name, *step()))
    if args.fast:
        rows.append(("mutants in-diff", "NOT RUN", "--fast: mutation testing skipped; CI will grade it"))
    else:
        print("pr-ready: mutants in-diff ...", file=sys.stderr, flush=True)
        rows.append(("mutants in-diff", *mutants(out, args.base, args.jobs)))
    rows += branch_rows(args.base, body)
    sha = git("rev-parse", "--short", "HEAD").strip()
    print_rows(f"make pr-ready @ {sha} (base {args.base}){' --fast' if args.fast else ''}; logs {out}", rows)
    return 1 if any(v == "FAIL" for _, v, _ in rows) else 0


def run_ci(args: argparse.Namespace) -> int:
    """The PR-rules CI job: every branch rule, graded against the PR's own title and body."""
    if args.body:
        body = Path(args.body).read_text()
    else:
        pr = json.loads(Path(args.event).read_text())["pull_request"]
        body = f"{pr.get('title') or ''}\n\n{pr.get('body') or ''}"
    rows = branch_rows(args.base, body)
    print_rows(f"PR rules (base {args.base})", rows)
    return 1 if any(v in ("FAIL", "DECLARE") for _, v, _ in rows) else 0


def run_text(args: argparse.Namespace) -> int:
    """The hooks: a commit message, the staged added lines, or a commit range."""
    if args.message:
        bad = text_violations(Path(args.message).read_text(), "commit message", message=True)
    elif args.staged:
        bad = added_line_violations(git("diff", "--cached", "-U0", "--no-color", "--no-ext-diff"))
    else:
        bad = commit_violations(args.commits)
    for b in bad:
        print(f"REFUSING: {b}", file=sys.stderr)
    if bad:
        print("Commits and PR text are English, carry no attribution trailer, and say `see #N`, never `fixes #N`."
              " Cyrillic TEST DATA stays with `allow-cyrillic` on its line.", file=sys.stderr)
    return 1 if bad else 0


# ── self-test ───────────────────────────────────────────────────────────────


def self_test() -> int:
    """Each rule goes RED on the violation it exists for and stays green on its near miss."""
    cyr = "\u041f\u0440\u0438\u0432\u0435\u0442"
    assert text_violations(f"feat: x\n\n{cyr}\n", "m"), "Cyrillic in a message must fail"
    assert text_violations("feat: x\n\nThis fixes #413.\n", "m"), "closing keyword must fail"
    assert text_violations("x\n\nResolved: #9\n", "m"), "closing keyword with a colon must fail"
    assert text_violations("x\n\nCo-Authored-By: A <a@b>\n", "m"), "trailer must fail"
    assert text_violations("x\n\nClaude-Session: https://x\n", "m"), "trailer must fail"
    assert not text_violations("x\n\nSee #413; hotfixes #3; the fixture #4.\n", "m"), "a keyword inside a word is not one"
    assert not text_violations("x\n\nSee #413. Mentions Co-Authored-By: inline.\n", "m"), "near miss must pass"
    assert not text_violations(f"x\n# {cyr} comment\n", "m", message=True), "git comment lines are not the message"
    d = f"+++ b/tests/a.rs\n+let t = \"{cyr}\"; // allow-cyrillic\n+++ b/docs/b.md\n+{cyr}\n+++ b/x.png\n+{cyr}\n"
    assert added_line_violations(d) == [f"docs/b.md: Cyrillic in an added line: {cyr}"], added_line_violations(d)

    rs = (
        "const CEILING: usize = 39; // ratchet-pin: silent\n"
        "const PIN_I: usize = 106; // ratchet-pin: independent min\n"
        "const T: &[(&str, usize)] = &[ // ratchet-pin: table sum\n"
        '    ("a 7", 2), // 2026\n    ("b", 3),\n'
        "]; // ratchet-pin: end\n"
        "const S: &[&str] = &[ // ratchet-pin: set strings\n"
        '    "x", "y",\n    // "z"\n'
        "]; // ratchet-pin: end\n"
    )
    p = pins(rs, "t.rs")
    assert p == {"silent": (39, False), "independent": (106, True), "table": (5, False), "set": (2, False)}, p
    assert pins("# head\n# ratchet-pin: rows lines\nsrc/a.rs:1: x\n\nsrc/b.rs:2: y\n", "b.txt") == {"rows": (2, False)}
    assert pins("  FIRST_RUN_CEILING: 304  # +14 on #411 ratchet-pin: first-run\n", "ci.yml") == {"first-run": (304, False)}
    for broken in ("const X: usize = N; // ratchet-pin: x\n", "const T: &[u8] = &[ // ratchet-pin: t sum\n 1,\n"):
        try:
            pins(broken, "b.rs")
            raise AssertionError(f"an unreadable pin must be an error: {broken!r}")
        except ValueError:
            pass
    base = {"f:silent": (39, False), "f:independent": (106, True), "f:gone": (1, False)}
    keep = {"f:gone": (1, False)}
    assert raised(base, {"f:silent": (40, False), "f:independent": (106, True), **keep}) == ["f:silent 39 -> 40"]
    assert raised(base, {"f:silent": (38, False), "f:independent": (107, True), "f:new": (9, False), **keep}) == []
    assert raised(base, {"f:silent": (39, False), "f:independent": (105, True), **keep}) == ["f:independent 106 -> 105"]
    assert raised(base, {"f:silent": (39, False), "f:independent": (106, True)}) == ["f:gone 1 -> removed"]
    up = ["tests/offline/s.rs:silent 39 -> 40"]
    assert undeclared(up, "## Why\nx\n") == up, "an undeclared raise must fail"
    assert undeclared(up, "## Ratchets raised\n- silent 39 -> 40: why\n## Evidence\n") == []
    assert undeclared(up, "## Ratchets raised\nnone\n## Evidence\nsilent\n") == up, "a mention outside the section does not declare"
    assert PIN_NAME.match("const NO_ORACLE_CEILING: usize = 21;") and PIN_NAME.match("gap_ratchet: 2")
    assert PIN_NAME.match("const BASELINE: &[(&str, usize)] = &[") and PIN_NAME.match("CONN_CEILING = {")
    assert not PIN_NAME.match("    let ceiling = x;") and not PIN_NAME.match("fn ratchet() {")
    assert not PIN_NAME.match("BASELINE_TARGETS = {") and PIN_NAME.match("const KNOWN_DEFECTS: &[&str] = &[")

    log = "# C\n\n## Unreleased\n\n### Fixed\n- a\n\n## 0.30.0\n- old\n"
    new = log.replace("- a\n", "- a\n- b\n")
    assert changelog_verdict(["src/a.rs"], log, log, "## Why\n")[0] == "FAIL"
    assert changelog_verdict(["src/a.rs"], log, new, "## Why\n")[0] == "PASS"
    assert changelog_verdict(["src/a.rs"], log, log, "No user-visible change: a refactor.")[0] == "PASS"
    assert changelog_verdict(["tests/a.rs"], log, log, "")[0] == "N/A"
    assert changelog_verdict(["src/a.rs"], log, log, None)[0] == "DECLARE"

    with tempfile.TemporaryDirectory() as t:
        m = Path(t) / "mutants.out"
        (m / "log").mkdir(parents=True)
        (m / "log/baseline.log").write_text("       Fresh rivet-cli v1 (/x)\n")
        (m / "log/src_a.rs_line_3.log").write_text("   Compiling rivet-cli v1 (/x)\n*** cargo test\n       Fresh rivet-cli v1 (/x)\n")
        (m / "missed.txt").write_text("")
        ok = "3 mutants tested in 1m: 3 caught"
        assert mutant_verdict(m, 0, ok)[0] == "PASS", mutant_verdict(m, 0, ok)
        (m / "missed.txt").write_text("src/a.rs:3:5: replace f -> bool with true\n")
        assert mutant_verdict(m, 2, "3 mutants tested in 1m: 1 missed, 2 caught")[0] == "FAIL"
        (m / "missed.txt").write_text("")
        (m / "log/src_a.rs_line_4.log").write_text("       Fresh rivet-cli v1 (/x)\n")
        assert "Fresh" in mutant_verdict(m, 0, ok)[1], "a Fresh mutant build must fail"
        assert mutant_verdict(m, 0, "")[0] == "FAIL", "no summary line is no verdict"
        assert mutant_verdict(m, 1, ok)[0] == "FAIL", "a tool error is no verdict"
    print("pr_ready self-test: ok")
    return 0


def main() -> int:
    """Dispatch the subcommands."""
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--fast", action="store_true", help="skip mutation testing (reported NOT RUN)")
    r.add_argument("--base", default="origin/main")
    r.add_argument("--body", help="the PR body draft, to grade the declarations against")
    r.add_argument("--jobs", type=int, default=2, help="cargo-mutants -j")
    c = sub.add_parser("ci")
    c.add_argument("--base", required=True)
    c.add_argument("--event", help="the pull_request event payload ($GITHUB_EVENT_PATH)")
    c.add_argument("--body", help="a PR body file instead of the event payload")
    t = sub.add_parser("text")
    g = t.add_mutually_exclusive_group(required=True)
    g.add_argument("--message")
    g.add_argument("--staged", action="store_true")
    g.add_argument("--commits")
    args = ap.parse_args()
    if args.cmd == "ci" and not (args.event or args.body):
        ap.error("ci needs --event or --body")
    return {"run": run_local, "ci": run_ci, "text": run_text}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
