#!/usr/bin/env python3
"""Split the E2E live sweep over its legs by measured duration, not by count.

    e2e_legs.py --list LIST.json --junit DIR --legs N --out LEGS.json   one nextest expression per leg
    e2e_legs.py --self-test

LIST.json is `cargo nextest list --message-format json` of the tests the legs run; DIR holds the
`junit.xml` files of the last full main run (any depth). A test with no measured duration counts
as the median of the measured ones. Every listed test lands in exactly one leg (checked before the
file is written); a list or a timing file that cannot be read writes nothing, and the legs fall
back to nextest's count partition.
"""

from __future__ import annotations

import json
import statistics
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def listed(list_json: dict) -> list[tuple[str, str]]:
    """`(binary_id, test name)` of every test the list selects to run, in list order."""
    out = []
    for binary_id, suite in list_json.get("rust-suites", {}).items():
        for name, case in suite.get("testcases", {}).items():
            if case.get("filter-match", {}).get("status") == "matches":
                out.append((binary_id, name))
    return out


def durations(junit_dir: Path) -> dict[tuple[str, str], float]:
    """Seconds per `(binary_id, test name)` from every junit.xml under `junit_dir`."""
    out: dict[tuple[str, str], float] = {}
    for f in junit_dir.rglob("*.xml"):
        for suite in ET.parse(f).getroot().iter("testsuite"):
            binary_id = suite.get("name", "")
            for case in suite.iter("testcase"):
                try:
                    out[(binary_id, case.get("name", ""))] = float(case.get("time", "0"))
                except ValueError:
                    continue
    return out


def pack(tests: list[tuple[str, str]], secs: dict[tuple[str, str], float], legs: int) -> list[list[tuple[str, str]]]:
    """Longest test first into the lightest leg (LPT): every test in exactly one leg."""
    known = [secs[t] for t in tests if t in secs]
    fallback = statistics.median(known) if known else 1.0
    bins: list[list[tuple[str, str]]] = [[] for _ in range(legs)]
    load = [0.0] * legs
    for t in sorted(tests, key=lambda t: (-secs.get(t, fallback), t)):
        i = min(range(legs), key=lambda k: (load[k], k))
        bins[i].append(t)
        load[i] += secs.get(t, fallback)
    return bins


def expression(tests: list[tuple[str, str]]) -> str:
    """The nextest expression that selects exactly `tests`, grouped by binary."""
    by: dict[str, list[str]] = {}
    for binary_id, name in tests:
        by.setdefault(binary_id, []).append(name)
    parts = [f"(binary_id({b}) & (" + " | ".join(f"test(={n})" for n in sorted(names)) + "))"
             for b, names in sorted(by.items())]
    return " | ".join(parts) if parts else "none()"


def check_exact(tests: list[tuple[str, str]], bins: list[list[tuple[str, str]]]) -> None:
    """Every selected test in exactly one leg, and no other test in any."""
    seen = [t for b in bins for t in b]
    assert len(seen) == len(set(seen)), "a test landed in two legs"
    assert set(seen) == set(tests), "a selected test landed in no leg, or a leg holds a test nobody selected"


def self_test() -> int:
    tests = [("rivet-cli::live_suite", f"m::t{i}") for i in range(10)] + [("rivet-cli::type_roundtrip", "x")]
    secs = {t: float(i + 1) for i, t in enumerate(tests[:8])}
    bins = pack(tests, secs, 3)
    check_exact(tests, bins)
    fallback = statistics.median(secs.values())
    loads = [sum(secs.get(t, fallback) for t in b) for b in bins]
    assert max(loads) - min(loads) <= max(secs.values()), loads
    try:
        check_exact(tests, [bins[0], bins[1]])
    except AssertionError:
        pass
    else:
        raise AssertionError("a leg dropped must fail the check")
    try:
        check_exact(tests, [bins[0] + bins[0][:1], bins[1], bins[2]])
    except AssertionError:
        pass
    else:
        raise AssertionError("a test in two legs must fail the check")
    e = expression([("b1", "m::a"), ("b1", "m::b"), ("b2", "x")])
    assert e == "(binary_id(b1) & (test(=m::a) | test(=m::b))) | (binary_id(b2) & (test(=x)))", e
    assert expression([]) == "none()"
    lst = {"rust-suites": {"b1": {"testcases": {"a": {"filter-match": {"status": "matches"}},
                                                "b": {"filter-match": {"status": "mismatch"}}}}}}
    assert listed(lst) == [("b1", "a")]
    print("self-test ok: e2e legs - every selected test in exactly one leg, longest first into the lightest")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    args = dict(zip(argv[::2], argv[1::2]))
    if set(args) != {"--list", "--junit", "--legs", "--out"}:
        print(__doc__, file=sys.stderr)
        return 2
    tests = listed(json.loads(Path(args["--list"]).read_text()))
    secs = durations(Path(args["--junit"]))
    if not secs:
        print("no measured durations: the legs keep nextest's count partition")
        return 0
    n = int(args["--legs"])
    bins = pack(tests, secs, n)
    check_exact(tests, bins)
    fallback = statistics.median([secs[t] for t in tests if t in secs] or [1.0])
    for i, b in enumerate(bins, 1):
        print(f"leg {i}/{n}: {len(b)} tests, {sum(secs.get(t, fallback) for t in b) / 60:.1f} test-minutes")
    Path(args["--out"]).write_text(json.dumps({f"{i}/{n}": expression(b) for i, b in enumerate(bins, 1)}))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
