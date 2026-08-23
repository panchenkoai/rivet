#!/usr/bin/env python3
"""Replay mutation outcomes that were already measured for THIS EXACT code.

Why a cache is sound only under a whole-world key
-------------------------------------------------
Whether a mutant is caught depends on the entire tree, not on the function it
mutates: a test in another file may be the only thing that kills it, and a
change three modules away can stop that test from reaching it. So there is no
honest per-file key. The cache is therefore keyed — by the workflow, through
`hashFiles` — on `src/**`, `tests/**`, `Cargo.lock` and `.cargo/mutants.toml`
together. A hit means "the same code, graded by the same suite, under the same
exclusions", and nothing weaker may be treated as a hit.

What that buys, stated honestly: it does NOT make an ordinary PR cheaper. It
removes the RE-runs — a re-run of the same commit after an infra flake, a push
that touches only docs or the workflow itself, a rebase onto a main whose
changes did not land in `src/` or `tests/`. When main does touch the code, the
world changes and every mutant is measured again, which is the correct answer
rather than a fast one.

ALL-OR-NOTHING on purpose. Reusing SOME outcomes and running the rest would
change the counts (`count`, `p1`, `p2`) that `Mutants (coverage verdict)`
audits against its own independent measurement, and a gate whose two halves
disagree about how much was graded is the exact failure this repo's mutation
plumbing exists to prevent. Either every mutant in the graded set is known, or
the run happens in full.
"""
from __future__ import annotations

import json
import sys


def _load(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}
    return doc if isinstance(doc, dict) else {}


def _lines(path: str) -> list[str]:
    try:
        with open(path, encoding="utf-8") as fh:
            return [ln.strip() for ln in fh if ln.strip()]
    except OSError:
        return []


def replay(cache_path: str, wanted_path: str) -> int:
    """Print the rc the run WOULD have produced, or refuse.

    Exit 0 + `0`/`2` on stdout when every wanted mutant is known; exit 1 (and
    nothing on stdout) otherwise, which the caller reads as "run it".
    """
    cache = _load(cache_path).get("outcomes", {})
    wanted = _lines(wanted_path)
    if not wanted:
        # Nothing to grade is not a cache hit — let the run reach its own
        # "no mutants in this diff" conclusion rather than inventing one.
        return 1
    outcomes = []
    for m in wanted:
        if m not in cache:
            return 1
        outcomes.append(cache[m])
    # `missed` is the gate's failure (cargo-mutants exit 2); a timeout is NOT
    # replayable as either — it says the measurement did not finish, so a run
    # that timed out once must be re-run rather than frozen into a verdict.
    if any(o == "timeout" for o in outcomes):
        return 1
    print(2 if any(o == "missed" for o in outcomes) else 0)
    return 0


def record(cache_path: str, caught_path: str, missed_path: str, timeout_path: str) -> int:
    doc = _load(cache_path)
    outcomes = doc.get("outcomes", {})
    if not isinstance(outcomes, dict):
        outcomes = {}
    for path, label in ((caught_path, "caught"), (missed_path, "missed"), (timeout_path, "timeout")):
        for m in _lines(path):
            outcomes[m] = label
    doc["outcomes"] = outcomes
    with open(cache_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=1, sort_keys=True)
    print(len(outcomes))
    return 0


def self_test() -> int:
    import tempfile
    import os

    failures = []

    def check(name: str, got, want):
        if got != want:
            failures.append(f"{name}: got {got!r}, want {want!r}")

    with tempfile.TemporaryDirectory() as d:
        cache = os.path.join(d, "c.json")
        w = os.path.join(d, "want.txt")
        caught = os.path.join(d, "caught.txt")
        missed = os.path.join(d, "missed.txt")
        tmo = os.path.join(d, "timeout.txt")

        open(caught, "w").write("src/a.rs:1:1: replace f -> ()\n")
        open(missed, "w").write("src/b.rs:2:2: replace g -> ()\n")
        open(tmo, "w").write("src/c.rs:3:3: replace h -> ()\n")
        import contextlib as _c
        import io as _io

        with _c.redirect_stdout(_io.StringIO()):   # `record` prints its tally
            record(cache, caught, missed, tmo)
        check("recorded", len(_load(cache)["outcomes"]), 3)

        # a mutant nobody measured must NOT be answered from cache
        open(w, "w").write("src/z.rs:9:9: replace q -> ()\n")
        check("unknown mutant refuses", replay(cache, w), 1)

        # The rc a hit REPLAYS is the whole safety property: a cache that
        # answers "pass" for a set containing a missed mutant is a gate that
        # lies, and it would lie silently. Capture stdout, do not just check
        # that a hit happened.
        import contextlib
        import io

        def replayed(path: str):
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = replay(cache, path)
            return code, buf.getvalue().strip()

        open(w, "w").write("src/a.rs:1:1: replace f -> ()\n")
        check("all caught -> hit, rc 0", replayed(w), (0, "0"))

        # one missed among known mutants replays the FAILING rc, never 0
        open(w, "w").write("src/a.rs:1:1: replace f -> ()\nsrc/b.rs:2:2: replace g -> ()\n")
        check("a missed mutant -> hit, rc 2", replayed(w), (0, "2"))

        # a timeout is not a verdict — it must force a real run
        open(w, "w").write("src/c.rs:3:3: replace h -> ()\n")
        check("timeout refuses", replay(cache, w), 1)

        # an empty wanted set is not a hit
        open(w, "w").write("")
        check("empty set refuses", replay(cache, w), 1)

        # a corrupt cache is a MISS, never an answer
        open(cache, "w").write("{not json")
        open(w, "w").write("src/a.rs:1:1: replace f -> ()\n")
        check("corrupt cache refuses", replay(cache, w), 1)

    if failures:
        for f in failures:
            print(f"FAIL {f}", file=sys.stderr)
        return 1
    print("mutants_cache.py self-test: ok")
    return 0


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    if len(sys.argv) >= 2 and sys.argv[1] == "replay":
        return replay(sys.argv[2], sys.argv[3])
    if len(sys.argv) >= 2 and sys.argv[1] == "record":
        return record(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
