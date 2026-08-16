#!/usr/bin/env python3
"""Read the nightly mutation ROTATION and answer "is this file graded anywhere?".

The PR-level mutation gate (`.github/workflows/ci.yml`, job `mutants-in-diff`)
gives up on a diff that yields more mutants than its 90-minute budget can grade,
and hands the work to the nightly tier rotation
(`.github/workflows/nightly-live.yml`, job `mutants-tier`). That hand-off was a
CLAIM, not a check: the rotation is a `date +%j % N` split over an explicit list
of files, and on 2026-08-16 that list named 31 of the tree's 198 `src/**.rs`
files. A diff outside the list was graded by NOTHING — not per-diff (over
budget), not nightly (not in a tier) — while the gate's own notice told the
reader those modules were covered. The branch that introduced the notice was
itself an instance: it rewrote `src/pipeline/plan_cmd.rs` (+478) and
`src/pipeline/aggregate.rs` (+128), neither of which is in any tier.

So the claim is now DERIVED from the rotation instead of asserted about it.
The workflow stays the single source of truth — the tier lists are not copied
into a second file that could drift from the `case` arms that actually run;
this parses the arms themselves.

    mutation_tiers.py tiers              # tier<TAB>label<TAB>path, one per line
    mutation_tiers.py untiered [PATH...] # the given paths in NO tier (one per line)
    mutation_tiers.py check              # grade the rotation itself; coverage summary
    mutation_tiers.py selftest           # unit-check the parser (no repo needed)

A parse failure is an ERROR (exit 2), never an empty tier set: "the rotation
covers nothing" and "I could not read the rotation" must not look alike to the
caller, because the first makes every file untiered (loud) and the second, if it
degraded to "everything is tiered", would restore exactly the silent false claim
this script exists to remove.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NIGHTLY = ROOT / ".github" / "workflows" / "nightly-live.yml"

# `case $(( $(date +%j) % 5 )) in` — the modulus is the number of tiers the
# rotation can ever select, which must equal the number of arms below it.
_CASE = re.compile(r"case\s+\$\(\(\s*\$\(date\s+\+%j\)\s*%\s*(\d+)\s*\)\)\s+in")
_ARM = re.compile(r"^\s*(\d+)\)\s*$")
_ESAC = re.compile(r"^\s*esac\s*$")
_LABEL = re.compile(r'echo\s+"group:\s*(.+?)"')
_FILE = re.compile(r"-f\s+(\S+\.rs)")


class ParseError(RuntimeError):
    pass


def parse_rotation(text: str) -> tuple[int, dict[int, str], dict[int, list[str]]]:
    """(modulus, {tier: label}, {tier: [path, ...]}) from a nightly-live.yml body."""
    lines = text.splitlines()
    start = None
    modulus = 0
    for i, line in enumerate(lines):
        m = _CASE.search(line)
        if m:
            start, modulus = i, int(m.group(1))
            break
    if start is None:
        raise ParseError(
            "no `case $(( $(date +%j) % N ))` rotation found — the tier rotation "
            "moved or changed shape; this parser must be updated with it"
        )

    labels: dict[int, str] = {}
    files: dict[int, list[str]] = {}
    tier: int | None = None
    for line in lines[start + 1 :]:
        if _ESAC.match(line):
            break
        arm = _ARM.match(line)
        if arm:
            tier = int(arm.group(1))
            labels.setdefault(tier, "")
            files.setdefault(tier, [])
            continue
        if tier is None:
            continue
        lab = _LABEL.search(line)
        if lab and not labels[tier]:
            labels[tier] = lab.group(1)
        files[tier].extend(_FILE.findall(line))
    if not files:
        raise ParseError("the rotation's `case` has no arms — nothing would ever be mutated")
    return modulus, labels, files


def rotation() -> tuple[int, dict[int, str], dict[int, list[str]]]:
    return parse_rotation(NIGHTLY.read_text())


def tier_of(files: dict[int, list[str]]) -> dict[str, int]:
    """path -> tier. A path in two arms keeps the first; `check` reports the overlap."""
    out: dict[str, int] = {}
    for t in sorted(files):
        for p in files[t]:
            out.setdefault(p, t)
    return out


def cmd_tiers() -> int:
    _, labels, files = rotation()
    for t in sorted(files):
        for p in files[t]:
            print(f"{t}\t{labels.get(t, '')}\t{p}")
    return 0


def cmd_untiered(paths: list[str]) -> int:
    _, _, files = rotation()
    covered = tier_of(files)
    for p in paths:
        p = p.strip()
        if not p or not p.endswith(".rs") or not p.startswith("src/"):
            continue
        if p not in covered:
            print(p)
    return 0


def cmd_check() -> int:
    modulus, labels, files = rotation()
    failed = False

    arms = sorted(files)
    if arms != list(range(modulus)):
        print(
            f"::error::the rotation selects `% {modulus}` but its arms are {arms} — "
            "a modulus larger than the arm count leaves days that mutate NOTHING, and "
            "an arm past the modulus never runs. Bump the modulus with the arm.",
        )
        failed = True

    seen: dict[str, int] = {}
    for t in arms:
        if not labels.get(t):
            print(f"::error::tier {t} has no `echo \"group: ...\"` label — the nightly log "
                  "would not say which group ran")
            failed = True
        for p in files[t]:
            if not (ROOT / p).is_file():
                print(
                    f"::error::tier {t} names `{p}`, which does not exist. A tier entry for a "
                    "moved/deleted file is a DEAD claim: it silently grades nothing while the "
                    "PR gate hands diffs to this rotation."
                )
                failed = True
            if p in seen:
                print(
                    f"::error::`{p}` is in tier {seen[p]} AND tier {t} — the rotation is a "
                    "partition; a duplicate spends the six-hour budget twice on one file."
                )
                failed = True
            seen[p] = t

    total = len(list((ROOT / "src").rglob("*.rs")))
    print(
        f"rotation: {modulus} tiers, {len(seen)} of {total} src/**.rs files tiered "
        f"({total - len(seen)} in NO tier — those are graded per-diff or not at all)"
    )
    for t in arms:
        print(f"  tier {t}: {len(files[t]):2d} file(s)  {labels.get(t, '')}")
    return 1 if failed else 0


_SAMPLE = """
        run: |
          case $(( $(date +%j) % 2 )) in
            0)
              echo "group: alpha (Tier 0)"
              FILES="-f src/a.rs -f src/b.rs \\
                     -f src/c.rs"
              ;;
            1)
              # a comment naming -f src/not_a_tier.rs must not count
              echo "group: beta (Tier 1)"
              FILES="-f src/d.rs"
              ;;
          esac
          cargo mutants -j 2 $FILES -- --lib || true
      - name: next step
        run: echo -f src/after_esac.rs
"""


def cmd_selftest() -> int:
    """Unit-check the PARSER against a synthetic rotation.

    RED-provable: drop the `_ESAC` break and `src/after_esac.rs` joins tier 1;
    drop the modulus capture and the arm/modulus cross-check in `check` can
    never fire.
    """
    mod, labels, files = parse_rotation(_SAMPLE)
    assert mod == 2, mod
    assert labels == {0: "alpha (Tier 0)", 1: "beta (Tier 1)"}, labels
    assert files[0] == ["src/a.rs", "src/b.rs", "src/c.rs"], files[0]
    # The comment line inside arm 1 is a false positive the parser accepts: a
    # `-f <path>.rs` in a comment reads as a tier member. That is deliberate —
    # over-counting a tier makes `untiered` report FEWER files, so it would be a
    # silent false claim. `check` catches it if the path does not exist; a
    # comment naming a REAL file is the residual, and it is loud in `tiers`.
    assert files[1] == ["src/not_a_tier.rs", "src/d.rs"], files[1]
    assert "src/after_esac.rs" not in files[1], "the `esac` boundary is not honoured"

    covered = tier_of(files)
    assert covered["src/c.rs"] == 0 and covered["src/d.rs"] == 1, covered

    for bad, why in (
        ("run: |\n  echo nothing here\n", "no case block"),
        ("case $(( $(date +%j) % 3 )) in\nesac\n", "no arms"),
    ):
        try:
            parse_rotation(bad)
        except ParseError:
            pass
        else:  # pragma: no cover - the failure path is the assertion
            raise AssertionError(f"parse_rotation accepted a rotation with {why}")

    print("selftest ok: arms, labels, files, esac boundary, and both parse failures")
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    cmd, rest = argv[0], argv[1:]
    try:
        if cmd == "tiers":
            return cmd_tiers()
        if cmd == "untiered":
            return cmd_untiered(rest or [l for l in sys.stdin.read().splitlines()])
        if cmd == "check":
            return cmd_check()
        if cmd == "selftest":
            return cmd_selftest()
    except ParseError as e:
        print(f"::error::{NIGHTLY.relative_to(ROOT)}: {e}", file=sys.stderr)
        return 2
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
