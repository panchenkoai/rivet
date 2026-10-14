#!/usr/bin/env python3
"""When was each live cell last green? A ledger of evidence beside the coverage ledgers.

A `test:` cell in docs/*-matrix.yaml says a test EXISTS; the drift guard checks the fn is
there. Nothing said when it last ran green: the Snowflake loader was broken for six
releases behind cells that existed. This tool keeps docs/evidence/last-green.yaml, one line
per live test fn (`#[ignore]` under tests/live/) with the date, the source and the commit of
its newest green run, and reports the live cells whose evidence is older than N days or
absent. It is a REPORT, not a gate: nothing here fails a build until the owner says so.

    python3 dev/pytools/evidence.py record [--junit F ...] [--nextest-log F ...]
                                           [--by local|gate|nightly|ci] [--at SHA] [--on DATE]
    python3 dev/pytools/evidence.py report [--days N] [--strict] [--json F] [--list-never]
    python3 dev/pytools/evidence.py --self-test

`record` reads nextest junit files (target/nextest/<profile>/junit.xml, written on every run)
or nextest's own log lines (`PASS [ … ] rivet-cli::live_suite module::fn`), keeps the newest
date per fn, and rewrites the ledger sorted. `report` joins the ledger with every coverage
ledger: a live `test:` cell is FRESH (green within N days), STALE (older) or NEVER (no
evidence); offline cells run on every PR and are not listed.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "docs" / "evidence" / "last-green.yaml"
DEFAULT_DAYS = 7
SOURCES = ("local", "gate", "nightly", "ci")

HEADER = """# Last green run of every live cell: fn -> { green: date, by: local|gate|nightly|ci, at: commit, in: module }.
# Written by dev/pytools/evidence.py record (from nextest junit or logs); read by `report`,
# which lists the live cells of docs/*-matrix.yaml with no green run in the last `days`.
# A line here is EVIDENCE, not coverage: the matrices say what must be tested, this says when
# each live test last proved it. Hand edits are not evidence; record a run instead.
"""

PASS_LINE = re.compile(r"^\s*(?:PASS|LEAK)\s+\[[^\]]*\]\s+\(\s*\d+/\d+\)\s+\S+\s+([A-Za-z0-9_]+)::([A-Za-z0-9_]+)\s*$")


def parse_ledger(path: Path = LEDGER) -> tuple[int, dict[str, dict]]:
    """`(days, cells)` of the ledger; a missing file is an empty ledger with the default threshold."""
    if not path.is_file():
        return DEFAULT_DAYS, {}
    import yaml

    doc = yaml.safe_load(path.read_text()) or {}
    cells = doc.get("cells") or {}
    return int(doc.get("days") or DEFAULT_DAYS), {str(k): {str(a): str(b) for a, b in v.items()} for k, v in cells.items()}


def write_ledger(days: int, cells: dict[str, dict], path: Path = LEDGER) -> None:
    """The ledger, sorted by fn, one line per cell."""
    lines = [HEADER, f"days: {days}", "", "cells:"]
    for fn in sorted(cells):
        c = cells[fn]
        lines.append(f"  {fn}: {{ green: '{c['green']}', by: {c['by']}, at: {c['at']}, in: {c['in']} }}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def parse_junit(path: Path) -> list[tuple[str, str, bool, str]]:
    """`(fn, module, passed, date)` of every testcase in a nextest junit file."""
    out = []
    for case in ET.parse(path).getroot().iter("testcase"):
        name = case.get("name") or ""
        module, _, fn = name.rpartition("::")
        if not fn:
            continue
        passed = case.find("failure") is None and case.find("error") is None and case.find("skipped") is None
        stamp = (case.get("timestamp") or "")[:10]
        out.append((fn, module, passed, stamp))
    return out


def parse_nextest_log(path: Path) -> list[tuple[str, str]]:
    """`(fn, module)` of every PASS (or LEAK, which passed) line of a nextest log."""
    out = []
    for line in path.read_text(errors="replace").splitlines():
        m = PASS_LINE.match(line)
        if m:
            out.append((m.group(2), m.group(1)))
    return out


def merged(cells: dict[str, dict], greens: list[tuple[str, str, str]], by: str, at: str) -> dict[str, dict]:
    """`cells` with each `(fn, module, date)` green run recorded where it is newer than what is there."""
    out = dict(cells)
    for fn, module, on in greens:
        have = out.get(fn)
        if have is None or str(have.get("green", "")) < on:
            out[fn] = {"green": on, "by": by, "at": at, "in": module}
    return out


def record_greens(names, by: str, at: str | None = None, on: str | None = None, path: Path = LEDGER) -> int:
    """Record `module::fn` names that ran green (a harness calling in): the ledger gains the newer dates; how many were recorded. Never raises on a bad name: evidence must not break the run it describes."""
    greens = []
    for name in names:
        module, _, fn = str(name).rpartition("::")
        if module and fn:
            greens.append((fn, module, on or dt.date.today().isoformat()))
    if not greens:
        return 0
    days, cells = parse_ledger(path)
    write_ledger(days, merged(cells, greens, by, at or head_sha()), path)
    return len(greens)


def live_fns(root: Path = ROOT) -> set[str]:
    """Every `fn` defined under tests/live/: the tests only a stand can run."""
    out: set[str] = set()
    for p in (root / "tests" / "live").glob("*.rs"):
        out.update(re.findall(r"^\s*(?:pub(?:\(crate\))?\s+)?fn\s+([A-Za-z0-9_]+)\s*\(", p.read_text(), re.M))
    return out


def matrix_cells(root: Path = ROOT) -> list[tuple[str, str, str, str]]:
    """`(matrix, scenario, column, fn)` of every `test:` cell of every coverage ledger."""
    import yaml

    out = []
    for path in sorted((root / "docs").glob("*-matrix.yaml")):
        doc = yaml.safe_load(path.read_text()) or {}
        for sc in doc.get("scenarios") or []:
            for col, cell in sc.items():
                if isinstance(cell, dict) and cell.get("test"):
                    out.append((path.name, str(sc.get("id")), str(col), str(cell["test"])))
    return out


def classify(cells: dict[str, dict], live: set[str], matrix: list[tuple[str, str, str, str]], days: int, today: dt.date) -> dict:
    """The report: per matrix, the live `test:` cells that are fresh, stale or never green."""
    floor = today - dt.timedelta(days=days)
    per: dict[str, dict] = {}
    seen_fns: set[str] = set()
    for m, sc, col, fn in matrix:
        if fn not in live:
            continue
        row = per.setdefault(m, {"fresh": 0, "stale": [], "never": []})
        ev = cells.get(fn)
        if ev is None:
            row["never"].append(f"{sc}/{col}: {fn}")
        elif dt.date.fromisoformat(str(ev["green"])) < floor:
            row["stale"].append(f"{sc}/{col}: {fn} (last green {ev['green']}, {ev['by']})")
        else:
            row["fresh"] += 1
        seen_fns.add(fn)
    totals = {
        "fresh": sum(r["fresh"] for r in per.values()),
        "stale": sum(len(r["stale"]) for r in per.values()),
        "never": sum(len(r["never"]) for r in per.values()),
        "uncited_evidence": sorted(fn for fn in cells if fn not in seen_fns),
        "days": days,
        "today": today.isoformat(),
    }
    return {"matrices": per, "totals": totals}


def render(report: dict, list_never: bool = False) -> str:
    """The human report: one line per matrix, then every stale cell (and every never-green one on request)."""
    t = report["totals"]
    lines = [f"live cells green within {t['days']} days of {t['today']}: fresh {t['fresh']}, stale {t['stale']}, never {t['never']}"]
    for m, row in sorted(report["matrices"].items()):
        lines.append(f"  {m}: fresh {row['fresh']}, stale {len(row['stale'])}, never {len(row['never'])}")
    for m, row in sorted(report["matrices"].items()):
        for what in ("stale", "never") if list_never else ("stale",):
            for item in row[what]:
                lines.append(f"    {what.upper():5} {m} {item}")
    if t["uncited_evidence"]:
        lines.append(f"  evidence for {len(t['uncited_evidence'])} fn(s) no ledger cites (fine: not every live test is a cell)")
    return "\n".join(lines)


def head_sha(root: Path = ROOT) -> str:
    out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=root, capture_output=True, text=True)
    return out.stdout.strip() or "unknown"


def cmd_record(argv: list[str]) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="evidence.py record")
    ap.add_argument("--junit", nargs="*", default=[], type=Path)
    ap.add_argument("--nextest-log", nargs="*", default=[], type=Path)
    ap.add_argument("--by", choices=SOURCES, default="local")
    ap.add_argument("--at", default=None)
    ap.add_argument("--on", default=None, help="the run's date for log files (junit carries its own)")
    ns = ap.parse_args(argv)
    if not ns.junit and not ns.nextest_log:
        ap.error("name at least one --junit or --nextest-log file")
    today = ns.on or dt.date.today().isoformat()
    greens: list[tuple[str, str, str]] = []
    failed = 0
    for j in ns.junit:
        for fn, module, passed, stamp in parse_junit(j):
            if passed:
                greens.append((fn, module, stamp or today))
            else:
                failed += 1
    for log in ns.nextest_log:
        greens.extend((fn, module, today) for fn, module in parse_nextest_log(log))
    days, cells = parse_ledger()
    before = len(cells)
    cells = merged(cells, greens, ns.by, ns.at or head_sha())
    write_ledger(days, cells)
    print(f"recorded {len(greens)} green run(s) ({failed} not green, not recorded): {len(cells)} fn(s) in the ledger, {len(cells) - before} new")
    return 0


def cmd_report(argv: list[str]) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="evidence.py report")
    ap.add_argument("--days", type=int, default=None)
    ap.add_argument("--strict", action="store_true", help="exit 1 when any live cell is stale or never green")
    ap.add_argument("--json", type=Path, default=None)
    ap.add_argument("--today", default=None)
    ap.add_argument("--list-never", action="store_true", help="list every live cell with no evidence at all, not only the counts")
    ns = ap.parse_args(argv)
    ledger_days, cells = parse_ledger()
    days = ns.days or ledger_days
    today = dt.date.fromisoformat(ns.today) if ns.today else dt.date.today()
    report = classify(cells, live_fns(), matrix_cells(), days, today)
    print(render(report, ns.list_never))
    if ns.json:
        ns.json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    t = report["totals"]
    return 1 if ns.strict and (t["stale"] or t["never"]) else 0


JUNIT_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="3" failures="1">
  <testsuite name="rivet-cli::live_suite" tests="3" failures="1">
    <testcase name="live_a::a_green_cell" classname="rivet-cli::live_suite" timestamp="2026-10-14T17:50:49.731+02:00" time="1"></testcase>
    <testcase name="live_a::a_red_cell" classname="rivet-cli::live_suite" timestamp="2026-10-14T17:50:49.731+02:00" time="1">
      <failure message="assertion failed">boom</failure>
    </testcase>
    <testcase name="live_a::an_older_cell" classname="rivet-cli::live_suite" timestamp="2026-10-01T10:00:00.000+02:00" time="1"></testcase>
  </testsuite>
</testsuites>
"""

LOG_FIXTURE = """        PASS [   7.272s] ( 7/20) rivet-cli::live_suite live_a::a_logged_cell
        FAIL [   1.829s] ( 9/20) rivet-cli::live_suite live_a::a_failed_logged_cell
        LEAK [  29.947s] ( 1/19) rivet-cli::live_suite live_a::a_leaky_cell
 FAIL + LEAK [ 131.707s] ( 8/17) rivet-cli::live_suite live_a::a_failed_leaky_cell
"""


def _self_test() -> None:
    junit = parse_junit_text(JUNIT_FIXTURE)
    assert [(f, p, d) for f, _, p, d in junit] == [
        ("a_green_cell", True, "2026-10-14"),
        ("a_red_cell", False, "2026-10-14"),
        ("an_older_cell", True, "2026-10-01"),
    ], junit
    with tempfile.TemporaryDirectory() as d:
        log = Path(d, "run.log")
        log.write_text(LOG_FIXTURE)
        assert parse_nextest_log(log) == [("a_logged_cell", "live_a"), ("a_leaky_cell", "live_a")], parse_nextest_log(log)
    # The newest date wins; an older run never overwrites a newer one.
    cells = merged({}, [("x", "m", "2026-10-10")], "local", "aaa")
    cells = merged(cells, [("x", "m", "2026-10-01")], "gate", "bbb")
    assert cells["x"] == {"green": "2026-10-10", "by": "local", "at": "aaa", "in": "m"}, cells
    cells = merged(cells, [("x", "m", "2026-10-12")], "nightly", "ccc")
    assert cells["x"]["green"] == "2026-10-12" and cells["x"]["by"] == "nightly", cells
    # The report: fresh within the window, stale past it, never without evidence; offline cells are not listed.
    live = {"fresh_fn", "stale_fn", "never_fn"}
    matrix = [
        ("a-matrix.yaml", "s1", "postgres", "fresh_fn"),
        ("a-matrix.yaml", "s1", "mysql", "stale_fn"),
        ("a-matrix.yaml", "s2", "postgres", "never_fn"),
        ("a-matrix.yaml", "s2", "mysql", "offline_fn"),
    ]
    evidence = {
        "fresh_fn": {"green": "2026-10-12", "by": "local", "at": "a", "in": "m"},
        "stale_fn": {"green": "2026-09-01", "by": "gate", "at": "b", "in": "m"},
        "unrelated_fn": {"green": "2026-10-12", "by": "local", "at": "a", "in": "m"},
    }
    rep = classify(evidence, live, matrix, 7, dt.date(2026, 10, 14))
    row = rep["matrices"]["a-matrix.yaml"]
    assert row["fresh"] == 1 and row["stale"] == ["s1/mysql: stale_fn (last green 2026-09-01, gate)"] and row["never"] == ["s2/postgres: never_fn"], rep
    assert rep["totals"]["uncited_evidence"] == ["unrelated_fn"], rep
    assert "STALE a-matrix.yaml s1/mysql: stale_fn" in render(rep) and "NEVER" not in render(rep), render(rep)
    assert "NEVER a-matrix.yaml s2/postgres: never_fn" in render(rep, True), render(rep, True)
    # The window is inclusive of its floor day.
    rep = classify(evidence, live, matrix, 2, dt.date(2026, 10, 14))
    assert rep["matrices"]["a-matrix.yaml"]["fresh"] == 1, rep
    rep = classify(evidence, live, matrix, 1, dt.date(2026, 10, 14))
    assert rep["matrices"]["a-matrix.yaml"]["fresh"] == 0, rep
    # The ledger round-trips, and a harness records by `module::fn` into it.
    with tempfile.TemporaryDirectory() as d:
        path = Path(d, "last-green.yaml")
        write_ledger(9, evidence, path)
        assert parse_ledger(path) == (9, evidence), parse_ledger(path)
        assert record_greens(["m::fresh_fn", "m::new_fn", "no_module"], "gate", "g1", "2026-10-13", path) == 2
        _, after = parse_ledger(path)
        assert after["new_fn"] == {"green": "2026-10-13", "by": "gate", "at": "g1", "in": "m"}, after
        assert after["fresh_fn"]["green"] == "2026-10-13", after
        assert after["stale_fn"]["green"] == "2026-09-01", after
    print("self-test ok: junit and log parsing, newest-wins merging, fresh/stale/never, ledger round trip")


def parse_junit_text(text: str) -> list[tuple[str, str, bool, str]]:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d, "junit.xml")
        p.write_text(text)
        return parse_junit(p)


def main(argv: list[str]) -> int:
    if argv[:1] == ["--self-test"]:
        _self_test()
        return 0
    if argv[:1] == ["record"]:
        return cmd_record(argv[1:])
    if argv[:1] == ["report"]:
        return cmd_report(argv[1:])
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
