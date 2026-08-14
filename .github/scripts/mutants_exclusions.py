#!/usr/bin/env python3
"""Grade `.cargo/mutants.toml`'s exclusions against the REAL mutant corpus.

`tests/offline/mutation_gate_config.rs` asks whether an exclusion pattern is
*shaped* wrong (matches the empty string, matches a hand-picked unrelated name).
Two questions it structurally cannot answer need the corpus, which only
cargo-mutants can produce:

  1. does the entry match at least ONE real mutant? A DEAD entry is a false
     triage claim: it says "this mutant is equivalent, ignore it" about nothing,
     while the survivor it was written for is still graded. Round-4 shipped
     exactly that — `"replace < with <= in decision_cause"` matches nothing,
     because cargo-mutants prints an impl method as `Governor::decision_cause` —
     and every offline guard stayed green over it.

  2. does the entry reach more than one FUNCTION? An unanchored function name is
     a prefix: `in check` also excluded `check_null_ratios`, `check_uniqueness`,
     `check_memory`, `check_export_selection`, … — 13 mutants in six files that
     the offline guard's sample corpus happened not to contain.

Usage:  mutants_exclusions.py <corpus-file>
where <corpus-file> is the output of `cargo mutants --list --no-config`
(the UNFILTERED corpus — filtering it with the config under test would let a
broken entry hide by removing its own evidence).

Exits 1 with a `::error::` annotation per offending entry.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

CONFIG = Path(__file__).resolve().parents[2] / ".cargo" / "mutants.toml"


def function_of(name: str) -> str:
    """The function a cargo-mutants mutant name belongs to.

    Names are `<path>:<line>:<col>: <description>`; descriptions end in
    `... in <fn>` (operator/delete mutants) or start with `replace <fn> ->` /
    `replace <fn> with` (whole-function stubs).
    """
    desc = name.split(": ", 1)[1] if ": " in name else name
    if " in " in desc:
        return desc.rsplit(" in ", 1)[1]
    if desc.startswith("replace "):
        return re.split(r" (?:->|with) ", desc[len("replace ") :], maxsplit=1)[0]
    return desc


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    corpus = [
        line.strip()
        for line in Path(sys.argv[1]).read_text().splitlines()
        if line.strip()
    ]
    if len(corpus) < 1000:
        print(
            f"::error::the mutant corpus has only {len(corpus)} entries — "
            "`cargo mutants --list --no-config` did not produce the full corpus, "
            "so this check would grade nothing",
            file=sys.stderr,
        )
        return 1

    patterns = tomllib.loads(CONFIG.read_text()).get("exclude_re", [])
    if not patterns:
        print("::error::no exclude_re entries found — has the config moved?")
        return 1

    failed = False
    for pat in patterns:
        try:
            rx = re.compile(pat)
        except re.error as e:
            print(f"::error::exclude_re `{pat}` is not a valid regex: {e}")
            failed = True
            continue
        hits = [n for n in corpus if rx.search(n)]
        if not hits:
            print(
                f"::error::exclude_re `{pat}` matches NO mutant in the corpus. A dead "
                "exclusion triages nothing: either the mutant it was written for is "
                "still being graded (check the exact name — cargo-mutants qualifies "
                "impl methods as `Type::method`), or the code moved and the entry "
                "should be deleted."
            )
            failed = True
            continue
        funcs = sorted({function_of(n) for n in hits})
        if len(funcs) > 1:
            sample = "\n  ".join(sorted(set(hits))[:8])
            print(
                f"::error::exclude_re `{pat}` reaches {len(funcs)} different "
                f"functions ({', '.join(funcs)}) — an unanchored name is a PREFIX, so "
                "it is silently removing mutants it was not written for. Anchor it "
                f"(`... in fn$`) or split it into one entry per function.\n  {sample}"
            )
            failed = True
            continue
        print(f"ok  {len(hits):5d} mutant(s) in {funcs[0]}  <-  {pat}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
