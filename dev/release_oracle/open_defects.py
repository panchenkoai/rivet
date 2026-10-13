"""Open defects against their ledger, outside the release gate.

An `open_defect_*` live cell asserts the correct behaviour and so fails until the fix lands;
dev/release_oracle/known_red.py records each with the first line of its panic. CI does not run
these cells, so a harness or fixture change that breaks one for ANOTHER reason is seen by nobody
until the full gate runs: the cell is red before and red after. This command runs them (or reads
a saved nextest log) and holds each to its ledger line, with the gate's own parser and matcher.

    make open-defects                      # run every open_defect_ cell on this stand
    python3 -m dev.release_oracle.open_defects LOG   # grade a saved `cargo nextest` output

A cell is one of: `as recorded` (red with its ledger line), `OTHER REASON` (red with another
first line: something else broke it), `FIXED` (green: remove the entry), `NO ENTRY` (an
`open_defect_` cell the ledger does not know), `not run` (in the ledger, absent from the log: a
stand this machine lacks). The first three kinds of surprise exit 1; `not run` is listed only.
"""

from __future__ import annotations

import subprocess
import sys

from . import known_red
from .core import nextest_outcomes, nextest_panics

OPEN = "open_defect_"
RUN = ["cargo", "nextest", "run", "--test", "live_suite", "--run-ignored", "only", "--no-fail-fast",
       "-E", f"test(/{OPEN}/)"]


def recorded() -> dict[str, known_red.KnownRed]:
    """The ledger's open-defect entries by cell (`module::fn`)."""
    return {k.match.split(" — ", 1)[0]: k for k in known_red.KNOWN_RED if f"::{OPEN}" in k.match}


def grade(log: str, ledger: dict[str, known_red.KnownRed]) -> dict[str, list[str]]:
    """Each open-defect cell of `ledger` and of `log` under its kind, as one printable line."""
    outcomes, panics = nextest_outcomes(log), nextest_panics(log)
    kinds: dict[str, list[str]] = {k: [] for k in ("as recorded", "OTHER REASON", "FIXED", "NO ENTRY", "not run")}
    for cell, entry in sorted(ledger.items()):
        status = outcomes.get(cell)
        if status is None:
            kinds["not run"].append(cell)
        elif status in ("PASS", "LEAK"):
            kinds["FIXED"].append(f"{cell}: green; remove its entry from dev/release_oracle/known_red.py")
        elif entry.match in f"{cell} — {panics.get(cell, '')}":
            kinds["as recorded"].append(cell)
        else:
            want = entry.match.split(" — ", 1)[1]
            kinds["OTHER REASON"].append(f"{cell}\n      recorded: {want}\n      now:      {panics.get(cell, '(no panic line read)')}")
    for cell in sorted(outcomes):
        if f"::{OPEN}" in cell and cell not in ledger:
            kinds["NO ENTRY"].append(f"{cell}: {outcomes[cell]}, and the ledger has no entry for it")
    return kinds


def report(kinds: dict[str, list[str]]) -> int:
    """Print the verdict; 1 when a cell is red for another reason, green, or unknown to the ledger."""
    for kind in ("OTHER REASON", "FIXED", "NO ENTRY"):
        for line in kinds[kind]:
            print(f"  {kind}  {line}")
    if kinds["not run"]:
        print(f"  not run on this stand ({len(kinds['not run'])}):")
        for cell in kinds["not run"]:
            print(f"    {cell}")
    bad = sum(len(kinds[k]) for k in ("OTHER REASON", "FIXED", "NO ENTRY"))
    print(f"open defects: {len(kinds['as recorded'])} as recorded, {len(kinds['OTHER REASON'])} red for another reason, "
          f"{len(kinds['FIXED'])} fixed, {len(kinds['NO ENTRY'])} without an entry, {len(kinds['not run'])} not run")
    return 1 if bad else 0


_SAMPLE = """
        FAIL [   1.0s] (1/5) rivet-cli::live_suite m::open_defect_as_recorded
        FAIL [   1.0s] (2/5) rivet-cli::live_suite m::open_defect_broken_by_a_fixture
        PASS [   1.0s] (3/5) rivet-cli::live_suite m::open_defect_fixed
        FAIL [   1.0s] (4/5) rivet-cli::live_suite m::open_defect_nobody_recorded
        PASS [   1.0s] (5/5) rivet-cli::live_suite m::a_regular_cell
    thread 'm::open_defect_as_recorded' (1) panicked at tests/x.rs:1:1:
    rig oracle: export 'e_1' disagrees with its source:
      - COUNT(*): source 40, delivered 35
    thread 'm::open_defect_broken_by_a_fixture' (2) panicked at tests/x.rs:2:2:
    mysql exec: Unknown column 'label' in 'field list'
    thread 'm::open_defect_nobody_recorded' (4) panicked at tests/x.rs:4:4:
    anything
"""


def _self_test() -> None:
    def entry(cell: str, line: str) -> known_red.KnownRed:
        return known_red.KnownRed(f"m::{cell} — {line}", "self-test", "2099-01-01")

    ledger = {f"m::{c}": entry(c, line) for c, line in [
        ("open_defect_as_recorded", "rig oracle: COUNT(*): source 40, delivered 35"),
        ("open_defect_broken_by_a_fixture", "rig oracle: COUNT(*): source 43, delivered 41"),
        ("open_defect_fixed", "any line"),
        ("open_defect_on_another_stand", "any line"),
    ]}
    kinds = grade(_SAMPLE, ledger)
    assert kinds["as recorded"] == ["m::open_defect_as_recorded"], kinds
    assert len(kinds["OTHER REASON"]) == 1 and "Unknown column 'label'" in kinds["OTHER REASON"][0], kinds
    assert len(kinds["FIXED"]) == 1 and kinds["FIXED"][0].startswith("m::open_defect_fixed"), kinds
    assert len(kinds["NO ENTRY"]) == 1 and kinds["NO ENTRY"][0].startswith("m::open_defect_nobody_recorded"), kinds
    assert kinds["not run"] == ["m::open_defect_on_another_stand"], kinds
    assert not any("a_regular_cell" in line for lines in kinds.values() for line in lines), kinds
    all_recorded = grade(_SAMPLE, {"m::open_defect_as_recorded": ledger["m::open_defect_as_recorded"]})
    assert all_recorded["OTHER REASON"] == [] and len(all_recorded["NO ENTRY"]) == 3
    assert all(f"::{OPEN}" in cell for cell in recorded()), "the real ledger's open cells are named open_defect_"
    print("self-test ok: a cell red for another reason, a fixed one and one without an entry each fail; a cell not run is listed")


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        _self_test()
        return 0
    if argv:
        with open(argv[0], encoding="utf-8", errors="replace") as f:
            log = f.read()
    else:
        ran = subprocess.run(RUN, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace")
        log = ran.stdout
        if "Starting " not in log:
            print(log[-3000:])
            print("open defects: nextest did not start; nothing was graded")
            return 2
    return report(grade(log, recorded()))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
