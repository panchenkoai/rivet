"""Every live_suite module the gate has not already run, through the gate's release binary.

The gate used to name its live tests by hand, cell by cell, so a regression test written for
a defect found later (a keyset anchor resumed by the wrong runner shape, a split re-run, a
ClickHouse idle load) never entered it. This cell DERIVES the set: every `mod` in
tests/live_suite.rs, minus the modules and tests the dedicated cells already ran this gate
(they register what they run), minus the named exclusions below — each with its reason.
It runs last so the registry is complete.
"""

from __future__ import annotations

import os
import re

from .core import RAN_LIVE_MODULES, RAN_LIVE_TESTS, ROOT, Ledger
from .scenarios import _run_live_modules

__all__ = ["verify_live_modules", "live_suite_modules", "exclusive_tests", "EXCLUDED"]

#: Modules the derived run skips, each for a reason (a stale entry fails the offline guard).
EXCLUDED = {
    "common": "the shared test helpers — no tests of its own",
}


def live_suite_modules() -> list[str]:
    """Every module tests/live_suite.rs declares."""
    text = (ROOT / "tests" / "live_suite.rs").read_text()
    return re.findall(r"^mod ([a-z0-9_]+);", text, re.M)


def exclusive_tests() -> list[str]:
    """Bare names of live tests marked `live+exclusive`: each needs the stand to itself."""
    names = []
    for f in sorted((ROOT / "tests" / "live").glob("*.rs")):
        names += re.findall(r'#\[ignore = "live\+exclusive[^\n]*\n(?:\s*#\[[^\n]*\n)*\s*(?:pub )?fn ([a-z0-9_]+)',
                            f.read_text())
    return names


def derived_filter(modules: list[str], ran_modules: set[str], ran_tests: set[str]) -> tuple[list[str], str]:
    """The modules left to run and the nextest filter over them, minus tests already run."""
    left = [m for m in modules if m not in EXCLUDED and m not in ran_modules]
    expr = " | ".join(f"test(/^{m}::/)" for m in left)
    if ran_tests:
        names = "|".join(sorted(re.escape(t) for t in ran_tests))
        expr = f"({expr}) - test(/::({names})$/)"
    return left, expr


def verify_live_modules(led: Ledger) -> None:
    """Run every live_suite module and test no dedicated cell ran; one ledger row per test."""
    left, expr = derived_filter(live_suite_modules(), set(RAN_LIVE_MODULES), set(RAN_LIVE_TESTS))
    excl = [n for n in exclusive_tests() if n not in RAN_LIVE_TESTS]
    # The environment the Rig cells get: a warehouse project and bucket for the cloud tests,
    # a Postgres state URL for the ones that share one — and NO ambient RIVET_STATE_URL, which
    # would move every SQLite-reading test onto Postgres (the batch_resume lesson).
    state = os.environ.get("RIVET_CDC_STATE_URL") or os.environ.get("RIVET_GATE_STATE_URL") or ""
    env = {"RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": "", "RIVET_TEST_STATE_URL": state}
    # A test that grades the Postgres state alone says so by its self-skip; it runs again on the gate's.
    again = ("RIVET_GATE_STATE_URL", {"RIVET_STATE_URL": state, "RIVET_GATE_STATE_URL": state}) if state.startswith("postgres") else None
    _run_live_modules(led, "live", "live modules",
                      f"every other live_suite module ({len(left)}), derived from tests/live_suite.rs",
                      left, env=env, again=again,
                      expr=f"({expr}) - test(/::({'|'.join(excl)})$/)" if excl else expr)
    if excl:
        # They restart a shared engine or hold a slot their neighbours flag: one at a time,
        # with nothing else running, which is the only way they grade anything.
        _run_live_modules(led, "live_modules", "live modules (exclusive)",
                          f"tests that need the stand to themselves ({len(excl)}), one at a time",
                          left, env={**env, "RIVET_TEST_EXCLUSIVE": "1"}, threads=1, again=again,
                          expr=" | ".join(f"test(/::{n}$/)" for n in excl))
