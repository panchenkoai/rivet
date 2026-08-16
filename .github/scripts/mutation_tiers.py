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
this script exists to remove. The docstring used to say that while
`parse_rotation` only raised on a `case` with no ARMS: arms holding no `-f` at
all produced `{0: [], 1: []}`, which is truthy, so `check` printed
`0 of 198 src/**.rs files tiered` and exited 0. An empty tier SET is now the
parse error the docstring always promised, and a single empty ARM is a `check`
error (that night mutates nothing, or — worse, see below — everything).

# What is graded: the variable the rotation SPENDS

`-f <path>.rs` occurrences are collected only from assignments to the variable
the `cargo mutants` command after `esac` actually expands. Grading the `-f`
DEFINITIONS instead let a rename walk straight past this script: an arm rewritten
to `FILES_4="-f ..."` while the command still spends `$FILES` keeps `check`
green, keeps `untiered` reporting those files as covered, and keeps the PR gate
telling authors "every changed source file IS in a nightly tier" — while the arm
runs `cargo mutants -j 2 -- --lib` with NO `-f`, mutating the whole
18,859-mutant crate and dying at the 360-minute ceiling with no verdict. The
same rule as the coverage-ledger one in CLAUDE.md: grade the CALL SITE, not the
definition.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NIGHTLY = ROOT / ".github" / "workflows" / "nightly-live.yml"

# `case $(( 10#$(date +%j) % 5 )) in` — the modulus is the number of tiers the
# rotation can ever select, which must equal the number of arms below it. The
# day expression is captured whole (not hard-coded to `$(date +%j)`) so `check`
# can grade its BASE: `date +%j` is zero-padded and bash arithmetic reads a
# leading-zero literal as OCTAL, so a bare `$(( $(date +%j) % 5 ))` aborts the
# step on days 008/009/018/019/…/079 and every day of 080-099 (36 nights a year,
# `value too great for base`), and silently re-interprets 010-077 in base 8.
_CASE = re.compile(
    r"case\s+\$\(\(\s*(?P<day>[^)]*\$\(date\s+\+%[A-Za-z]\)[^)]*?)\s*%\s*(?P<mod>\d+)\s*\)\)\s+in"
)
_ARM = re.compile(r"^\s*(\d+)\)\s*$")
_ESAC = re.compile(r"^\s*esac\s*$")
_LABEL = re.compile(r'echo\s+"group:\s*(.+?)"')
_FILE = re.compile(r"-f\s+(\S+\.rs)")
_ASSIGN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)=")
_MUTANTS_CMD = re.compile(r"\bcargo\s+mutants\b")
_STEP_BOUNDARY = re.compile(r"^\s*-\s+(name|uses|run):")
_VAR_USE = re.compile(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?")
# A `$(date +%X)` substitution used in arithmetic, and whether it is forced to
# base 10. `10#` is the only prefix bash accepts for that.
_DATE_SUB = re.compile(r"(?P<base>10#\s*)?\$\(\s*date\s+\+%[A-Za-z]+\s*\)")


class ParseError(RuntimeError):
    pass


@dataclass(frozen=True)
class Rotation:
    """What the nightly rotation actually does, read off its own `case`."""

    modulus: int
    day_expr: str
    command: str
    spent: tuple[str, ...]
    labels: dict[int, str] = field(default_factory=dict)
    files: dict[int, list[str]] = field(default_factory=dict)
    assigned: dict[int, list[str]] = field(default_factory=dict)


def unsafe_date_terms(expr: str) -> list[str]:
    """`$(date +%X)` terms in an arithmetic expression NOT forced to base 10.

    `date +%j` / `+%d` / `+%H` / `+%M` / `+%S` are all ZERO-PADDED, and bash
    arithmetic treats a leading-zero literal as octal: `$(( 008 % 5 ))` is a
    fatal `value too great for base` (the step dies before it assigns anything)
    and `$(( 010 % 5 ))` quietly evaluates to 3, not 0. Only `10#$(date +%j)`
    is safe.
    """
    return [m.group(0) for m in _DATE_SUB.finditer(expr) if not m.group("base")]


def _command_after(lines: list[str], esac_at: int) -> str:
    """The `cargo mutants` invocation the rotation's arms feed, with its continuations.

    Bounded to the SAME workflow step: a `cargo mutants` line in a later step is
    a different command with a different `-f` list, and matching it would grade
    the wrong call site.
    """
    for i in range(esac_at + 1, len(lines)):
        if _STEP_BOUNDARY.match(lines[i]):
            break
        if _MUTANTS_CMD.search(lines[i]):
            cmd = lines[i]
            j = i
            while cmd.rstrip().endswith("\\") and j + 1 < len(lines):
                j += 1
                cmd = cmd.rstrip()[:-1] + " " + lines[j]
            return cmd
    raise ParseError(
        "no `cargo mutants` command found after the rotation's `esac` — this script "
        "grades the `-f` list the rotation SPENDS, so it must see the command that "
        "spends it; the rotation moved or changed shape"
    )


def parse_rotation(text: str) -> Rotation:
    """Read the rotation out of a nightly-live.yml body."""
    lines = text.splitlines()
    start = None
    modulus = 0
    day_expr = ""
    for i, line in enumerate(lines):
        m = _CASE.search(line)
        if m:
            start, modulus, day_expr = i, int(m.group("mod")), m.group("day").strip()
            break
    if start is None:
        raise ParseError(
            "no `case $(( ... $(date +%j) ... % N ))` rotation found — the tier "
            "rotation moved or changed shape; this parser must be updated with it"
        )

    esac_at = None
    for i in range(start + 1, len(lines)):
        if _ESAC.match(lines[i]):
            esac_at = i
            break
    if esac_at is None:
        raise ParseError("the rotation's `case` is never closed by an `esac`")

    command = _command_after(lines, esac_at)
    spent = tuple(dict.fromkeys(_VAR_USE.findall(command)))
    if not spent:
        raise ParseError(
            f"the mutants command spends no shell variable (`{command.strip()}`) — with no "
            "`-f` list it mutates the WHOLE crate and dies at the job's timeout with no "
            "verdict, while every arm above it looks like it selected a tier"
        )

    labels: dict[int, str] = {}
    files: dict[int, list[str]] = {}
    assigned: dict[int, list[str]] = {}
    tier: int | None = None
    cur_var: str | None = None
    in_quote = False
    for line in lines[start + 1 : esac_at]:
        arm = _ARM.match(line)
        if arm and not in_quote:
            tier = int(arm.group(1))
            labels.setdefault(tier, "")
            files.setdefault(tier, [])
            assigned.setdefault(tier, [])
            cur_var = None
            continue
        if tier is None:
            continue
        lab = _LABEL.search(line)
        if lab and not in_quote and not labels[tier]:
            labels[tier] = lab.group(1)
        if not in_quote:
            m = _ASSIGN.match(line)
            if m:
                cur_var = m.group(1)
                if cur_var not in assigned[tier]:
                    assigned[tier].append(cur_var)
        # A `-f` counts only while we are inside an assignment to a variable the
        # mutants command expands. Anywhere else (a comment, an echo, an arm that
        # assigns a variable nothing spends) it grades a DEFINITION, not the call
        # site — which is how a renamed arm variable stayed invisible here.
        if cur_var in spent:
            files[tier].extend(_FILE.findall(line))
        if line.count('"') % 2:
            in_quote = not in_quote
        if not in_quote and not line.rstrip().endswith("\\"):
            cur_var = None

    if not files:
        raise ParseError("the rotation's `case` has no arms — nothing would ever be mutated")
    if not any(files.values()):
        raise ParseError(
            "the rotation's arms name no file at all: every arm would run the mutants "
            "command with an EMPTY `-f` list. `check` must not report `0 of N files "
            "tiered` and exit 0 — an empty tier set is a parse error, never a pass. "
            f"(the command spends {', '.join('$' + v for v in spent)}; the arms assign "
            f"{', '.join(sorted({v for vs in assigned.values() for v in vs})) or 'nothing'})"
        )
    return Rotation(
        modulus=modulus,
        day_expr=day_expr,
        command=command.strip(),
        spent=spent,
        labels=labels,
        files=files,
        assigned=assigned,
    )


def rotation() -> Rotation:
    return parse_rotation(NIGHTLY.read_text())


def tier_of(files: dict[int, list[str]]) -> dict[str, int]:
    """path -> tier. A path in two arms keeps the first; `check` reports the overlap."""
    out: dict[str, int] = {}
    for t in sorted(files):
        for p in files[t]:
            out.setdefault(p, t)
    return out


def cmd_tiers() -> int:
    rot = rotation()
    for t in sorted(rot.files):
        for p in rot.files[t]:
            print(f"{t}\t{rot.labels.get(t, '')}\t{p}")
    return 0


def cmd_untiered(paths: list[str]) -> int:
    covered = tier_of(rotation().files)
    for p in paths:
        p = p.strip()
        if not p or not p.endswith(".rs") or not p.startswith("src/"):
            continue
        if p not in covered:
            print(p)
    return 0


def cmd_check() -> int:
    rot = rotation()
    failed = False

    arms = sorted(rot.files)
    if arms != list(range(rot.modulus)):
        print(
            f"::error::the rotation selects `% {rot.modulus}` but its arms are {arms} — "
            "a modulus larger than the arm count leaves days that mutate NOTHING, and "
            "an arm past the modulus never runs. Bump the modulus with the arm.",
        )
        failed = True

    # The selector's own arithmetic. `date +%j` is zero-padded, so an unforced
    # base makes bash read `008` as octal: a fatal `value too great for base` on
    # 36 nights a year (the step dies BEFORE the tier variable is assigned, so
    # the rotation produces no verdict at all) and a silently WRONG arm on
    # 010-077. Measured: `bash -ec 'case $(( 008 % 5 )) in ...'` exits 1 without
    # entering any arm; `$(( 010 % 5 ))` is 3, not 0.
    for term in unsafe_date_terms(rot.day_expr):
        print(
            f"::error::the rotation's day expression `{rot.day_expr}` uses `{term}` "
            "without a base: `date` pads with a leading zero, and bash arithmetic "
            "reads a leading-zero literal as OCTAL. Days 008/009/018/…/079 and all of "
            "080-099 abort the step (`value too great for base`) and 010-077 select "
            f"the wrong arm. Write `10#{term}`."
        )
        failed = True

    seen: dict[str, int] = {}
    for t in arms:
        if not rot.labels.get(t):
            print(f"::error::tier {t} has no `echo \"group: ...\"` label — the nightly log "
                  "would not say which group ran")
            failed = True
        if not rot.files[t]:
            stray = [v for v in rot.assigned.get(t, []) if v not in rot.spent]
            why = (
                f" — it assigns {', '.join(stray)}, which the command "
                f"(`{rot.command}`) never expands"
                if stray
                else ""
            )
            print(
                f"::error::tier {t} names no file{why}. An empty `-f` list does not skip "
                "the run: `cargo mutants` then mutates the WHOLE crate and dies at the "
                "job's timeout with no verdict, while `untiered` still reports those "
                "files as covered."
            )
            failed = True
        for p in rot.files[t]:
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
        f"rotation: {rot.modulus} tiers over `{rot.day_expr}`, spending "
        f"{', '.join('$' + v for v in rot.spent)}; {len(seen)} of {total} src/**.rs files "
        f"tiered ({total - len(seen)} in NO tier — those are graded per-diff or not at all)"
    )
    for t in arms:
        print(f"  tier {t}: {len(rot.files[t]):2d} file(s)  {rot.labels.get(t, '')}")
    return 1 if failed else 0


_SAMPLE = """
        run: |
          case $(( 10#$(date +%j) % 2 )) in
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

# The rename that walked past the old parser: arm 1 fills `FILES_4`, the command
# still spends `$FILES`, so that night mutates the whole crate — while the old
# parser reported `src/d.rs` as tiered because it collected every `-f` in the
# `case` regardless of which variable held it.
_SAMPLE_RENAMED = _SAMPLE.replace('FILES="-f src/d.rs"', 'FILES_4="-f src/d.rs"')

# Every arm assigns a variable the command never spends: the whole tier set is
# empty, which `check` used to report as `0 of N files tiered` and exit 0.
_SAMPLE_EMPTY = _SAMPLE.replace("FILES=", "TIER=")

# The shipped-until-2026-08-16 octal selector.
_SAMPLE_OCTAL = _SAMPLE.replace("10#$(date +%j)", "$(date +%j)")


def cmd_selftest() -> int:
    """Unit-check the PARSER against synthetic rotations.

    RED-provable, one mutant per case: drop the `_ESAC` break and
    `src/after_esac.rs` joins tier 1; drop the modulus capture and the
    arm/modulus cross-check in `check` can never fire; collect `-f` from any
    line instead of from assignments to the SPENT variable and the renamed-arm
    case stops being empty; drop the `any(files.values())` raise and the
    all-empty case stops raising; drop the `10#` requirement in
    `unsafe_date_terms` and the octal case reports safe.
    """
    rot = parse_rotation(_SAMPLE)
    assert rot.modulus == 2, rot.modulus
    assert rot.day_expr == "10#$(date +%j)", rot.day_expr
    assert rot.spent == ("FILES",), rot.spent
    assert rot.labels == {0: "alpha (Tier 0)", 1: "beta (Tier 1)"}, rot.labels
    assert rot.files[0] == ["src/a.rs", "src/b.rs", "src/c.rs"], rot.files[0]
    # The `-f` inside the comment is NOT a tier member: it is not in an
    # assignment to the variable the command expands. (The old parser counted it
    # and documented the false positive as deliberate; scoping to the spent
    # variable removes it, and errs toward reporting MORE files untiered — the
    # safe direction, since an over-counted tier is a silent claim of coverage.)
    assert rot.files[1] == ["src/d.rs"], rot.files[1]
    assert "src/after_esac.rs" not in rot.files[1], "the `esac` boundary is not honoured"

    covered = tier_of(rot.files)
    assert covered["src/c.rs"] == 0 and covered["src/d.rs"] == 1, covered

    # A renamed arm variable: the command spends `$FILES`, so arm 1 sends NOTHING
    # to cargo-mutants and its file must not read as tiered.
    renamed = parse_rotation(_SAMPLE_RENAMED)
    assert renamed.files[1] == [], renamed.files[1]
    assert renamed.assigned[1] == ["FILES_4"], renamed.assigned[1]
    assert "FILES_4" not in renamed.spent, renamed.spent
    assert "src/d.rs" not in tier_of(renamed.files), tier_of(renamed.files)

    # The day expression's base is graded, not assumed.
    assert unsafe_date_terms("10#$(date +%j)") == []
    assert unsafe_date_terms("$(date +%j)") == ["$(date +%j)"]
    assert unsafe_date_terms("$(date +%d) + 10#$(date +%H)") == ["$(date +%d)"]
    assert parse_rotation(_SAMPLE_OCTAL).day_expr == "$(date +%j)"

    for bad, why, expect in (
        ("run: |\n  echo nothing here\n", "no case block", "moved or changed shape"),
        (
            "case $(( 10#$(date +%j) % 3 )) in\nesac\ncargo mutants $FILES\n",
            "no arms",
            "has no arms",
        ),
        (
            "case $(( 10#$(date +%j) % 1 )) in\n  0)\n  FILES=\"-f src/a.rs\"\n  ;;\nesac\n",
            "no mutants command",
            "no `cargo mutants` command found",
        ),
        (
            "case $(( 10#$(date +%j) % 1 )) in\n  0)\n  FILES=\"-f src/a.rs\"\n  ;;\n"
            "esac\ncargo mutants -j 2 -- --lib\n",
            "a command spending no variable",
            "spends no shell variable",
        ),
        (_SAMPLE_EMPTY, "arms that tier nothing", "name no file at all"),
    ):
        try:
            parse_rotation(bad)
        except ParseError as e:
            assert expect in str(e), (why, str(e))
        else:  # pragma: no cover - the failure path is the assertion
            raise AssertionError(f"parse_rotation accepted a rotation with {why}")

    print(
        "selftest ok: arms, labels, spent-variable scoping, renamed arm, esac boundary, "
        "base-10 day expression, and all five parse failures"
    )
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
