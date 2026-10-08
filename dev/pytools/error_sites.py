"""Which early-exit sites of `src/` has anything ever executed?

A site is a `bail!` (any `*bail!`), `ensure!`, `anyhow!`, `CodedError::new` or
`return Err` in product code. Its count is read at its own line AND column from
`cargo llvm-cov ... --json` (region segments), once for the offline battery and
once for the live suite: an `anyhow!` inside a closure on an executed line is not
executed until the closure runs. Each site is `live`, `offline-only`, `never`, or
`unmeasured` (no counted region covers it; never read as either).

    python3 dev/pytools/error_sites.py measure                 # both coverage runs (long; needs the stand)
    python3 dev/pytools/error_sites.py report                  # write docs/error-sites.md from them
    python3 dev/pytools/error_sites.py check                   # the committed report: generated, and under its ceilings
    python3 dev/pytools/error_sites.py --self-test
"""

from __future__ import annotations

import bisect
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs" / "error-sites.md"
COV = ROOT / "target" / "llvm-cov"

NEVER_CEILING = 314  # ratchet-pin: error-sites-never-executed
OFFLINE_ONLY_CEILING = 361  # ratchet-pin: error-sites-offline-only
LIVE_FLOOR = 201  # ratchet-pin: error-sites-executed-live min

SCRUB = re.compile(
    r"//[^\n]*|/\*.*?\*/"
    r'|b?r(#*)".*?"\1|b?"(?:\\.|[^"\\])*"'
    r"|b?'(?:\\(?:u\{[0-9a-fA-F]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])'",
    re.S,
)
SITE = re.compile(r"\b(\w*bail|ensure|anyhow)!\s*\(|\b(CodedError::new)\(|\b(return\s+Err)\(")
TEST_MOD = re.compile(r"#\[cfg\(test\)\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{")
#: A never-executed site here decides a write or a resume.
WRITE_OR_RESUME = re.compile(r"^src/(state|load)/|manifest|checkpoint|journal|run_store|commit|resume|ledger|cdc/sink")
STATUSES = ("live", "offline-only", "never", "unmeasured")


def blank(src: str) -> str:
    """The source with comments and literals replaced by spaces, every offset kept."""
    return SCRUB.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), src)


def product(src: str) -> str:
    """`blank(src)` with every `#[cfg(test)] mod … { … }` blanked too."""
    text = blank(src)
    while m := TEST_MOD.search(text):
        depth, end = 0, len(text)
        for j in range(m.end() - 1, len(text)):
            depth += (text[j] == "{") - (text[j] == "}")
            if depth == 0:
                end = j + 1
                break
        text = text[: m.start()] + re.sub(r"[^\n]", " ", text[m.start() : end]) + text[end:]
    return text


def sites(src: str) -> list[tuple[int, int, str]]:
    """(line, byte column, kind) of the first early exit on each product line; both 1-based."""
    out = []
    for n, (line, raw) in enumerate(zip(product(src).split("\n"), src.split("\n")), 1):
        if m := SITE.search(line):
            kind = m.group(1) + "!" if m.group(1) else re.sub(r"\s+", " ", m.group(2) or m.group(3))
            out.append((n, len(raw[: m.start()].encode()) + 1, kind))
    return out


def is_test_file(path: str) -> bool:
    """A file that is test code as a whole."""
    return "/tests/" in path or path.endswith(("/tests.rs", "_tests.rs"))


def segments(export: dict, root: str) -> dict[str, list[list]]:
    """Repo-relative path → llvm-cov segments `[line, col, count, has_count, ...]`; a shape it cannot read is an error."""
    files = {}
    for f in export["data"][0]["files"]:
        name = f["filename"]
        if name.startswith(root):
            files[name[len(root) :].lstrip("/")] = f["segments"]
    if not files:
        raise ValueError(f"the coverage export names no file under {root}")
    return files


def count_at(segs: list[list] | None, line: int, col: int) -> int | None:
    """Executions of the region covering (line, col), or None when no counted region does."""
    if not segs:
        return None
    i = bisect.bisect_right(segs, [line, col, float("inf")]) - 1
    if i >= 0 and segs[i][3]:
        return int(segs[i][2])
    # `return Err(x)`: the region starts at `Err`, after an uncounted gap that holds `return`.
    after = segs[i + 1] if i + 1 < len(segs) else None
    return int(after[2]) if after and after[0] == line and after[3] else None


def status(live: int | None, offline: int | None) -> str:
    """Where a site was executed."""
    if live is None and offline is None:
        return "unmeasured"
    return "live" if live else "offline-only" if offline else "never"


def table(sources: dict[str, str], offline: dict[str, list[list]], live: dict[str, list[list]]) -> list[tuple[str, int, str, str, str]]:
    """(path, line, kind, status, source line) for every site of every product file."""
    rows = []
    for path, src in sorted(sources.items()):
        if is_test_file(path):
            continue
        lines = src.split("\n")
        for line, col, kind in sites(src):
            st = status(count_at(live.get(path), line, col), count_at(offline.get(path), line, col))
            rows.append((path, line, kind, st, " ".join(lines[line - 1].split())[:110]))
    return rows


def counts(rows: list) -> dict[str, int]:
    """Sites per status."""
    return {s: sum(r[3] == s for r in rows) for s in STATUSES}


def module(path: str) -> str:
    """`src/state/x.rs` → `state`; `src/enrich.rs` → `enrich`."""
    return path.split("/")[1].removesuffix(".rs")


def render(rows: list, sha: str) -> str:
    """The report; its first line carries the digest of everything after it."""
    c = counts(rows)
    out = [
        "# Early-exit sites of `src/`: what executes them",
        "",
        "Generated by `python3 dev/pytools/error_sites.py report`. Do not edit: `check` recomputes the digest above.",
        "",
        f"Measured at `{sha}`: one offline battery and one live suite under `cargo llvm-cov`.",
        "",
        f"- sites: {len(rows)}",
        f"- never executed: {c['never']}",
        f"- executed only by the offline battery (unit and offline tests): {c['offline-only']}",
        f"- executed by the live suite: {c['live']}",
        f"- unmeasured (no counted region; not read as either): {c['unmeasured']}",
        "",
        "## Never executed, and deciding a write or a resume",
        "",
        "The work list for new cells: state, manifest, checkpoint, journal, commit and load.",
        "",
    ]
    out += [f"- `{p}:{n}` `{k}` — `{text}`" for p, n, k, st, text in rows if st == "never" and WRITE_OR_RESUME.search(p)]
    out += ["", "## By module", "", "| module | sites | live | offline-only | never | unmeasured |", "|---|---|---|---|---|---|"]
    mods = sorted({module(r[0]) for r in rows})
    for m in mods:
        mine = [r for r in rows if module(r[0]) == m]
        mc = counts(mine)
        out.append(f"| {m} | {len(mine)} | {mc['live']} | {mc['offline-only']} | {mc['never']} | {mc['unmeasured']} |")
    for m in mods:
        out += ["", f"### {m}", "", "| site | kind | status |", "|---|---|---|"]
        out += [f"| `{p}:{n}` | `{k}` | {st} |" for p, n, k, st, _ in rows if module(p) == m]
    body = "\n".join(out) + "\n"
    return f"<!-- sha256 {hashlib.sha256(body.encode()).hexdigest()} -->\n{body}"


def read_report(text: str) -> dict[str, int]:
    """The numbers of a report whose digest holds; anything else is an error."""
    head, _, body = text.partition("\n")
    m = re.fullmatch(r"<!-- sha256 ([0-9a-f]{64}) -->", head)
    if not m or hashlib.sha256(body.encode()).hexdigest() != m.group(1):
        raise ValueError("docs/error-sites.md was edited by hand or cut: regenerate it with `error_sites.py report`")
    got = {}
    for key, label in (("sites", "sites"), ("never", "never executed"), ("offline-only", "executed only by the offline battery (unit and offline tests)"),
                       ("live", "executed by the live suite"), ("unmeasured", "unmeasured (no counted region; not read as either)")):
        n = re.search(rf"^- {re.escape(label)}: (\d+)$", body, re.M)
        if not n:
            raise ValueError(f"docs/error-sites.md has no `{label}` line")
        got[key] = int(n.group(1))
    listed = {s: len(re.findall(rf"^\| `[^`]+` \| `[^`]+` \| {s} \|$", body, re.M)) for s in STATUSES}
    if any(listed[s] != got[s] for s in STATUSES) or sum(listed.values()) != got["sites"]:
        raise ValueError(f"docs/error-sites.md: its header says {got} and its tables hold {listed}")
    return got


def grade(got: dict[str, int], never: int, offline_only: int, live_floor: int) -> list[str]:
    """Every way the report's numbers break the pins; empty when they match exactly."""
    out = []
    for key, pin, name in (("never", never, "NEVER_CEILING"), ("offline-only", offline_only, "OFFLINE_ONLY_CEILING")):
        if got[key] > pin:
            out.append(f"{got[key]} sites {key}, ceiling {pin}: a new early exit needs a test that reaches it")
        elif got[key] < pin:
            out.append(f"{got[key]} sites {key}, below the ceiling {pin}: lower {name} to {got[key]} to bank it")
    if got["live"] < live_floor:
        out.append(f"{got['live']} sites executed live, floor {live_floor}: a live cell stopped reaching an early exit")
    elif got["live"] > live_floor:
        out.append(f"{got['live']} sites executed live, above the floor {live_floor}: raise LIVE_FLOOR to {got['live']} to bank it")
    return out


def measure() -> int:
    """Both coverage runs, each timed; the live one inside a live slot."""
    COV.mkdir(parents=True, exist_ok=True)
    env = {k: v for k, v in os.environ.items() if k not in ("RIVET_BIN_OVERRIDE", "RIVET_STATE_URL", "RIVET_GATE_STATE_URL", "RIVET_TEST_STATE_URL")}
    cov = ["cargo", "llvm-cov", "nextest", "--ignore-run-fail", "--json"]
    for name, argv in (
        ("offline", [*cov, "--output-path", str(COV / "offline.json")]),
        ("live", [sys.executable, str(ROOT / "dev/pytools/live_slot.py"), "--", *cov, "--output-path", str(COV / "live.json"),
                  "--test", "live_suite", "--run-ignored", "only", "--retries", "0",
                  "--failure-output", "never", "--success-output", "never", "--status-level", "fail"]),
    ):
        t = time.monotonic()
        rc = subprocess.run(argv, cwd=ROOT, env=env).returncode
        print(f"error_sites: {name} coverage run took {(time.monotonic() - t) / 60:.1f} min (exit {rc})", flush=True)
        if not (COV / f"{name}.json").exists():
            return 1
    return 0


def report() -> int:
    """Write docs/error-sites.md from the two exports of `measure`."""
    sources = {p.relative_to(ROOT).as_posix(): p.read_text() for p in sorted((ROOT / "src").rglob("*.rs"))}
    cov = [segments(json.loads((COV / f"{n}.json").read_text()), str(ROOT)) for n in ("offline", "live")]
    rows = table(sources, *cov)
    sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    REPORT.write_text(render(rows, sha))
    print(f"error_sites: {len(rows)} sites: {counts(rows)} -> {REPORT.relative_to(ROOT)}")
    return 0


def check() -> int:
    """The committed report is the generated one and its numbers are the pins."""
    try:
        got = read_report(REPORT.read_text())
    except (OSError, ValueError) as e:
        print(f"ERROR-SITES: {e}")
        return 1
    problems = grade(got, NEVER_CEILING, OFFLINE_ONLY_CEILING, LIVE_FLOOR)
    print("\n".join(f"ERROR-SITES: {p}" for p in problems))
    print(f"error_sites: {got['sites']} sites, {got['never']} never executed, {got['offline-only']} offline-only, {got['live']} live, {got['unmeasured']} unmeasured")
    return 1 if problems else 0


def self_test() -> int:
    """A tiny file and hand-written segments: each site lands where a reader would put it."""
    src = (
        "fn f(x: u8) -> Result<u8> {\n"                                    # 1
        "    if x == 0 {\n"                                                # 2
        "        bail!(\"zero\");\n"                                       # 3  live
        "    }\n"                                                          # 4
        "    let y = g(x).ok_or_else(|| anyhow!(\"none\"))?;\n"            # 5  closure never runs
        "    if x == 9 { return Err(CodedError::new(E, \"nine\").into()); }\n"  # 6  offline-only
        "    // bail!(\"in a comment\")\n"                                 # 7
        "    let s = \"ensure!(in a string)\";\n"                          # 8
        "    rivet_bail!(E, \"always\");\n"                                # 9  never
        "}\n"                                                              # 10
        "const C: X = X { e: || anyhow!(\"const\") };\n"                   # 11 unmeasured
        "#[cfg(test)]\n"
        "mod tests {\n"
        "    fn t() { bail!(\"test code\"); }\n"
        "}\n"
        "fn after() { ensure!(ok, \"after the test module\"); }\n"         # 16 never
    )
    got = sites(src)
    assert [(n, k) for n, _, k in got] == [(3, "bail!"), (5, "anyhow!"), (6, "return Err"), (9, "rivet_bail!"), (11, "anyhow!"), (16, "ensure!")], got
    assert got[1][1] == 32 and got[0][1] == 9, got
    #          line col count has_count
    live = {"src/a.rs": [[1, 27, 7, True], [2, 15, 2, True], [4, 6, 7, True], [5, 32, 0, True], [5, 48, 7, True], [6, 15, 0, False], [6, 24, 0, True], [6, 66, 7, True], [9, 5, 0, True], [10, 2, 0, False], [16, 14, 0, True]]}
    offline = {"src/a.rs": [[1, 27, 3, True], [2, 15, 0, True], [4, 6, 3, True], [5, 32, 0, True], [5, 48, 3, True], [6, 15, 0, False], [6, 24, 1, True], [6, 66, 2, True], [9, 5, 0, True], [10, 2, 0, False], [16, 14, 0, True]]}
    rows = table({"src/a.rs": src, "src/x/tests/t.rs": "fn t() { bail!(\"x\"); }\n"}, offline, live)
    assert [(r[1], r[3]) for r in rows] == [(3, "live"), (5, "never"), (6, "offline-only"), (9, "never"), (11, "unmeasured"), (16, "never")], rows
    assert counts(rows) == {"live": 1, "offline-only": 1, "never": 3, "unmeasured": 1}
    assert count_at(None, 3, 9) is None and status(None, None) == "unmeasured" and status(None, 0) == "never" and status(0, 2) == "offline-only"
    text = render(rows, "abc1234")
    assert read_report(text) == {"sites": 6, "never": 3, "offline-only": 1, "live": 1, "unmeasured": 1}
    for edited in (text.replace("| never |", "| live |", 1), text.replace("never executed: 3", "never executed: 2"), text[:-40]):
        try:
            read_report(edited)
        except ValueError:
            continue
        raise AssertionError("an edited report was read")
    assert grade(read_report(text), 3, 1, 1) == []
    assert any("ceiling 2" in p for p in grade(read_report(text), 2, 1, 1)) and any("bank it" in p for p in grade(read_report(text), 4, 1, 1))
    assert any("ceiling 0" in p for p in grade(read_report(text), 3, 0, 1)) and any("floor 2" in p for p in grade(read_report(text), 3, 1, 2))
    assert segments({"data": [{"files": [{"filename": "/r/src/a.rs", "segments": [[1, 1, 1, True]]}, {"filename": "/else/b.rs", "segments": []}]}]}, "/r") == {"src/a.rs": [[1, 1, 1, True]]}
    try:
        segments({"data": [{"files": [{"filename": "/else/b.rs", "segments": []}]}]}, "/r")
    except ValueError:
        pass
    else:
        raise AssertionError("an export of another tree was read as no coverage")
    assert "- `src/a.rs:9`" not in text and WRITE_OR_RESUME.search("src/state/x.rs") and WRITE_OR_RESUME.search("src/pipeline/commit.rs") and not WRITE_OR_RESUME.search("src/preflight/mod.rs")
    print("error_sites self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    cmd = argv[1] if len(argv) > 1 else ""
    if cmd not in ("--self-test", "measure", "report", "check"):
        print(__doc__)
        return 2
    return {"--self-test": self_test, "measure": measure, "report": report, "check": check}[cmd]()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
