#!/usr/bin/env python3
"""Split a PR's mutants into the ones the offline suite CAN kill and the rest.

The mutation PR gate used to spend one budget on one undifferentiated pile, and
went quiet exactly where the risk was highest: past `MUTANTS_DIFF_BUDGET` it
graded NOTHING (`over-budget-ungraded`), and inside the budget every MISSED
mutant read the same whether it was an assertion gap or a body no offline test
can reach. Measured on the 2026-08-21 refactor branch: 42 mutants in scope, 30
minutes, 16 MISSED — of which exactly ONE was a real gap (a pure function whose
whole-body stub would have let a FAILED run exit 0). The other 15 were live-only
paths (a runner that opens a real source, a probe that takes a live connection)
that `cargo mutants -- --lib --bins` cannot kill at all. Fifteen unkillable
misses is how the one real finding gets ignored.

So this script PRIORITISES instead of budgeting, on a MEASURED signal rather
than a claim:

  P1  the mutant's line sits inside a function the offline test suite EXECUTES
      (llvm-cov counted it). A test ran this code and did not notice the
      mutation — an assertion gap in the diff, which is what the gate is for.
      P1 is graded first and its verdict BLOCKS.

  P2  the mutant's line sits inside a function llvm-cov measured at ZERO
      executions under `--lib --bins`. No offline assertion can kill it (the
      "--lib on a live-only path" reading of the scope-honesty rule in
      CLAUDE.md: read a survivor as "no UNIT test executes this", not "N gaps").
      P2 is REPORTED, with the `.cargo/mutants.toml` vocabulary to triage it —
      an `exclude_re` entry carrying a live-oracle proof, or a unit oracle that
      moves the function into P1.

Everything else is P1. A function absent from the coverage export, a file the
export does not mention, an unparseable or shape-changed report, no report at
all: all fail CLOSED, to today's behaviour (grade it, and a miss is red). The
weakening this split could have introduced — "it wasn't covered, so it does not
count" — is available only for a function coverage OBSERVED at zero, and only
because no offline test could have killed it whatever the gate did.

Note the granularity: FUNCTION, not line. A never-executed BRANCH inside an
executed function stays P1, because a test can reach it — "you added a path and
tested none of it" is the gate's core finding and must keep failing. Function
extents also match the vocabulary `.cargo/mutants.toml` already triages in
(`"replace run_pool -> Result"`), so a P2 report is one paste from a reviewable
exclusion.

Usage:
  mutants_classify.py reach <llvm-cov.json> <extents.tsv>
        Convert `cargo llvm-cov --lib --bins --json` into the compact
        `file<TAB>start_line<TAB>end_line<TAB>count` extents the partition
        reads. Exits 1 (loudly) on any shape it does not recognise — an
        unrecognised report must not arrive as "nothing is covered", which
        would move the whole diff into P2.

  mutants_classify.py partition <mutant-list> [--extents <tsv>]
                      --p1 <f> --p2 <f> --drop-p1 <f> --drop-p2 <f>
        <mutant-list> is `cargo mutants --in-diff pr.diff --list`. Writes the
        two classes, plus the `--exclude-re` regexes that remove each class
        from a cargo-mutants run. Without `--extents`, everything is P1.

  mutants_classify.py verify <expected-p1> <actual-list>
        Set equality between the P1 class and what cargo-mutants ACTUALLY
        listed once the P2 exclusions were applied. The regexes are generated,
        so their reach is checked rather than assumed — an over-broad exclusion
        silently un-grading P1 mutants is the exact defect
        `mutants_exclusions.py` exists to catch in the hand-written list.

  mutants_classify.py --states     the state vocabulary, one per line
  mutants_classify.py --self-test
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# ONE mutant-name parser for the whole gate. `mutants_exclusions.py` already
# owns it (a mutant is `<path>:<line>:<col>: <description>`), and a second copy
# here would be a twin that drifts — the class this repo keeps paying for.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mutants_exclusions import _plain as strip_ansi  # noqa: E402

# The vocabulary this script's callers publish. `.github/workflows/ci.yml` must
# handle every one of them; `tests/offline/mutation_gate_priority_guard.rs`
# DERIVES the list from here rather than re-typing it, so a new state cannot be
# added without the workflow learning it.
REACH_STATES = ["measured", "unmeasured", "unverified"]
P2_AUDIT_STATES = ["oracle-holds", "oracle-lied", "skipped-over-budget"]

_EXPORT_TYPE = "llvm.coverage.json.export"


class ShapeError(Exception):
    """The coverage report is not the shape this script knows how to read.

    Raised, never swallowed: an unreadable report has to reach the operator as
    "the prioritisation did not run" (whereupon everything is graded, as
    before), not as an empty coverage set — which would read as "no function is
    executed" and move the entire diff into the report-only class.
    """


def function_extents(cov: dict, root: str = "") -> list[tuple[str, int, int, int]]:
    """`(file, start_line, end_line, exec_count)` per function in an llvm-cov export.

    Verified against a real `cargo llvm-cov --lib --bins --json` (format
    `llvm.coverage.json.export` 3.0.1, cargo-llvm-cov 0.9.0) rather than
    assumed: `data[0].functions[]` carries `count` (executions of that
    instantiation), `filenames` and `regions`, each region
    `[start_line, start_col, end_line, end_col, count, file_id, ...]`. Names are
    v0-MANGLED in this export (`_RNvMCs..3Gov7observe`), which is why the lookup
    is by LINE EXTENT and not by name: nothing here has to demangle, and the
    `Type::method` / generic / closure naming that trips the exclusion patterns
    cannot trip this.

    Generic functions appear once per instantiation and closures nest inside
    their parent's extent; the caller takes the MAX count over every extent
    containing a line, so both fail toward P1 (graded).
    """
    if cov.get("type") != _EXPORT_TYPE:
        raise ShapeError(f"expected type {_EXPORT_TYPE!r}, got {cov.get('type')!r}")
    data = cov.get("data")
    if not isinstance(data, list) or not data:
        raise ShapeError("`data` is missing or empty")
    funcs = data[0].get("functions")
    if not isinstance(funcs, list):
        raise ShapeError("`data[0].functions` is missing — was --summary-only passed?")

    out: list[tuple[str, int, int, int]] = []
    for fn in funcs:
        try:
            count = int(fn["count"])
            names = list(fn["filenames"])
            regions = list(fn["regions"])
        except (KeyError, TypeError, ValueError) as e:
            raise ShapeError(f"function record without count/filenames/regions: {e}") from None
        # A function's regions may span several files (macro expansions), so the
        # extent is taken per file_id rather than collapsed onto filenames[0].
        per_file: dict[str, tuple[int, int]] = {}
        for r in regions:
            if not isinstance(r, list) or len(r) < 6:
                raise ShapeError(f"region tuple too short: {r!r}")
            start, end, file_id = int(r[0]), int(r[2]), int(r[5])
            if file_id >= len(names):
                raise ShapeError(f"region file_id {file_id} outside filenames {names!r}")
            f = names[file_id]
            lo, hi = per_file.get(f, (start, end))
            per_file[f] = (min(lo, start), max(hi, end))
        for f, (lo, hi) in per_file.items():
            out.append((_relative(f, root), lo, hi, count))
    if not out:
        raise ShapeError("the export contains no function records at all")
    return out


def _relative(path: str, root: str) -> str:
    """Coverage paths are absolute; mutant names are repo-relative."""
    if root and path.startswith(root.rstrip("/") + "/"):
        return path[len(root.rstrip("/")) + 1 :]
    return path


def load_extents(lines: list[str]) -> dict[str, list[tuple[int, int, int]]]:
    """The extents TSV back into `{file: [(start, end, count), …]}`."""
    by_file: dict[str, list[tuple[int, int, int]]] = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        f, lo, hi, count = line.split("\t")
        by_file.setdefault(f, []).append((int(lo), int(hi), int(count)))
    return by_file


def site_line(name: str) -> tuple[str, int] | None:
    """`(file, line)` of a mutant, or None when the name is not that shape."""
    parts = strip_ansi(name).split(":", 3)
    if len(parts) < 3:
        return None
    try:
        return parts[0], int(parts[1])
    except ValueError:
        return None


def partition(
    mutants: list[str], extents: dict[str, list[tuple[int, int, int]]] | None
) -> tuple[list[str], list[str]]:
    """(P1, P2) — graded-and-blocking, and reported-only.

    A mutant is P2 only on POSITIVE evidence: a function extent in the same file
    contains its line, and every such extent was measured at zero executions.
    No extents at all (`extents is None`), no extent for the file, no extent
    containing the line, an unparseable name — all P1. Every one of those is a
    case where the gate does not KNOW, and not knowing must not buy a mutant its
    way out of being graded.
    """
    if extents is None:
        return list(mutants), []
    p1: list[str] = []
    p2: list[str] = []
    for name in mutants:
        site = site_line(name)
        if site is None:
            p1.append(name)
            continue
        f, line = site
        containing = [c for (lo, hi, c) in extents.get(f, []) if lo <= line <= hi]
        if containing and max(containing) == 0:
            p2.append(name)
        else:
            p1.append(name)
    return p1, p2


def exclusion_regexes(mutants: list[str]) -> list[str]:
    """`--exclude-re` patterns that remove EXACTLY these mutants and no others.

    cargo-mutants matches `--exclude-re` against the full name — verified on
    cargo-mutants 27.1.0: `^src/tuning/adaptive.rs:34:5: replace … with 0$`
    removes that one mutant and leaves its line-mates. So each pattern is the
    anchored, regex-ESCAPED whole name: a mutant description carries `->`, `*`,
    `+`, `(`, `[` and `.`, and an unescaped one is the over-broad exclusion this
    gate's other half (`mutants_exclusions.py`) exists to catch. The reach of
    the generated set is not trusted either — `verify` re-lists the corpus with
    them applied and demands the remainder is exactly P1.
    """
    return ["^" + re.escape(strip_ansi(n).strip()) + "$" for n in mutants]


def verify(expected: list[str], actual: list[str]) -> list[str]:
    """Errors when the post-exclusion listing is not exactly the expected class."""
    want = {strip_ansi(n).strip() for n in expected if n.strip()}
    got = {strip_ansi(n).strip() for n in actual if n.strip()}
    errors = []
    if want - got:
        sample = "\n  ".join(sorted(want - got)[:5])
        errors.append(
            f"::error::the generated exclusions removed {len(want - got)} mutant(s) that "
            "belong to the GRADED class — an over-broad pattern is un-grading code the gate "
            f"is supposed to block on:\n  {sample}"
        )
    if got - want:
        sample = "\n  ".join(sorted(got - want)[:5])
        errors.append(
            f"::error::{len(got - want)} mutant(s) survived the exclusions that were meant to "
            "be deprioritised — the restriction did not take, so the run below would not be "
            f"the class it reports:\n  {sample}"
        )
    return errors


# --------------------------------------------------------------------------
# Self-test. Every case below is RED-provable against ONE mutant in this file;
# the mutant is named in the assertion's comment.
# --------------------------------------------------------------------------

# The shape a real `cargo llvm-cov --lib --bins --json` emits, reduced to the
# four records that matter: a called free function, a called method, an
# UNCALLED method, and a function in another file. Copied from a live probe run
# (cargo-llvm-cov 0.9.0, export 3.0.1), not invented — an invented fixture would
# make this an oracle of itself.
_COV = {
    "type": _EXPORT_TYPE,
    "version": "3.0.1",
    "data": [
        {
            "functions": [
                {
                    "name": "_RNvCs_8covprobe12plain_called",
                    "count": 7,
                    "filenames": ["/repo/src/lib.rs"],
                    "regions": [[7, 41, 7, 55, 7, 0, 0, 0]],
                },
                {
                    "name": "_RNvMCs_8covprobeNtB2_3Gov7observe",
                    "count": 3,
                    "filenames": ["/repo/src/lib.rs"],
                    "regions": [[10, 5, 14, 6, 3, 0, 0, 0]],
                },
                {
                    # A CLOSURE inside `observe`: llvm-cov emits it as its own
                    # record, nested in its parent's extent and counted
                    # separately. It makes the "max over every containing
                    # extent" rule observable — with `min` the executed parent
                    # would be judged by its unexecuted closure and every mutant
                    # between lines 11 and 13 would fall out of the graded class.
                    "name": "_RNvXNvMCs_8covprobeNtB2_3Gov7observe0Bb_",
                    "count": 0,
                    "filenames": ["/repo/src/lib.rs"],
                    "regions": [[11, 9, 13, 10, 0, 0, 0, 0]],
                },
                {
                    "name": "_RNvMCs_8covprobeNtB2_3Gov12never_called",
                    "count": 0,
                    "filenames": ["/repo/src/lib.rs"],
                    "regions": [[20, 5, 26, 6, 0, 0, 0, 0]],
                },
                {
                    "name": "_RNvNtCs_8covprobe5inner13in_other_file",
                    "count": 1,
                    "filenames": ["/repo/src/inner.rs"],
                    "regions": [[1, 1, 3, 2, 1, 0, 0, 0]],
                },
            ]
        }
    ],
}


def self_test() -> int:
    """Grade the classifier over the shapes it must judge.

    RED-provable one mutant at a time:
      * `partition`'s `extents is None` arm returning `([], mutants)` — the
        no-coverage case stops being graded;
      * `containing and max(containing) == 0` widened to `not containing or
        max(...) == 0` — an UNMEASURED site buys its way into the report-only
        class, which is the whole weakening this split must not have;
      * `max(containing)` -> `min(containing)` — a closure inside an executed
        function drops out of the graded class;
      * dropping `re.escape` in `exclusion_regexes` — the pattern for a
        `with vec![]` stub stops being a valid regex at all (unterminated
        character set), which is what an unescaped generated pattern does to a
        corpus full of `->`, `*`, `+`, `(` and `[`;
      * `function_extents` returning `[]` instead of raising on a shape it does
        not know — an unreadable report reads as "nothing is covered".
    """
    extents_rows = function_extents(_COV, root="/repo")
    ext = load_extents([f"{f}\t{lo}\t{hi}\t{c}" for (f, lo, hi, c) in extents_rows])
    assert ext["src/lib.rs"] == [(7, 7, 7), (10, 14, 3), (11, 13, 0), (20, 26, 0)], ext
    assert ext["src/inner.rs"] == [(1, 3, 1)], ext

    mutants = [
        # inside an EXECUTED function -> graded
        "src/lib.rs:7:41: replace plain_called -> usize with 0",
        # a never-taken BRANCH inside an executed function is still graded: a
        # test can reach it, and "you added a path and tested none of it" is the
        # finding this gate exists for. This line also sits inside an UNEXECUTED
        # closure record nested in the executed parent, so it is the case that
        # distinguishes max-over-containing-extents from min.
        "src/lib.rs:12:9: replace < with <= in Gov::observe",
        # inside a function measured at ZERO executions -> report-only
        "src/lib.rs:21:9: replace Gov::never_called -> usize with 0",
        # a file the export never mentions -> graded (the gate does not know)
        "src/pipeline/run.rs:99:1: replace run_pool -> Result<()> with Ok(())",
        # a line no extent covers -> graded, same reason
        "src/lib.rs:999:1: replace parsed_but_uncovered -> usize with 0",
        # an unparseable name -> graded
        "not a mutant name at all",
    ]
    p1, p2 = partition(mutants, ext)
    assert p2 == ["src/lib.rs:21:9: replace Gov::never_called -> usize with 0"], p2
    assert len(p1) == 5, p1
    assert "src/lib.rs:12:9: replace < with <= in Gov::observe" in p1, p1
    assert "src/pipeline/run.rs:99:1: replace run_pool -> Result<()> with Ok(())" in p1, p1

    # No coverage at all: everything is graded, exactly as before this script.
    p1, p2 = partition(mutants, None)
    assert (p1, p2) == (mutants, []), (p1, p2)

    # Generated exclusions are exact: each removes its own mutant and nothing
    # else, over a corpus whose names differ only in the trailing literal.
    # `vec![]` is in the corpus deliberately: cargo-mutants writes that literal
    # for every `-> Vec<_>` stub (`replace introspect_all -> Result<Vec<TableInfo>>`
    # and friends live in .cargo/mutants.toml today), and UNESCAPED it is an
    # unterminated character set — an invalid regex, not a wrong one. Without
    # `re.escape` this loop raises instead of asserting, which is the point:
    # generated patterns are matched against names full of `->`, `*`, `+`, `(`
    # and `[`.
    corpus = [
        "src/lib.rs:7:41: replace plain_called -> usize with 0",
        "src/lib.rs:7:41: replace plain_called -> usize with 1",
        "src/lib.rs:21:9: replace Gov::never_called -> usize with 0",
        "src/init/mod.rs:31:1: replace introspect_all -> Result<Vec<T>> with Ok(vec![])",
    ]
    for pat, target in zip(exclusion_regexes(corpus), corpus, strict=True):
        hits = [n for n in corpus if re.search(pat, n)]
        assert hits == [target], (pat, hits)

    # …and the verification catches both directions of a restriction that did
    # not take. (An exclusion may only ever REMOVE, so `got - want` is the
    # "nothing was excluded" case and `want - got` the over-broad one.)
    assert not verify(corpus, corpus)
    errs = verify(corpus, corpus[:1])
    assert errs and "belong to the GRADED class" in errs[0], errs
    errs = verify(corpus[:1], corpus)
    assert errs and "survived the exclusions" in errs[0], errs

    # A report shape this script does not know is an ERROR, never an empty
    # coverage set — the difference between "the prioritisation did not run"
    # and "no function in this crate is executed".
    for broken in (
        {"type": "something.else", "data": [{"functions": []}]},
        {"type": _EXPORT_TYPE, "data": []},
        {"type": _EXPORT_TYPE, "data": [{}]},
        {"type": _EXPORT_TYPE, "data": [{"functions": [{"count": 1}]}]},
        {
            "type": _EXPORT_TYPE,
            "data": [
                {"functions": [{"count": 1, "filenames": ["/repo/a.rs"], "regions": [[1, 2]]}]}
            ],
        },
        {
            "type": _EXPORT_TYPE,
            "data": [
                {
                    "functions": [
                        {"count": 1, "filenames": ["/repo/a.rs"], "regions": [[1, 1, 2, 1, 1, 9]]}
                    ]
                }
            ],
        },
        {"type": _EXPORT_TYPE, "data": [{"functions": []}]},
    ):
        try:
            function_extents(broken, root="/repo")
        except ShapeError:
            pass
        else:
            raise AssertionError(f"shape accepted that should not be: {broken}")

    print(
        "self-test ok: executed / zero-coverage / unmeasured-file / uncovered-line / "
        "unparseable classification, the no-coverage fallback, exact exclusions, "
        "both verify directions, and six rejected report shapes"
    )
    return 0


def _write(path: str | None, lines: list[str]) -> None:
    if path:
        Path(path).write_text("".join(f"{line}\n" for line in lines))


def _arg(argv: list[str], flag: str) -> str | None:
    return argv[argv.index(flag) + 1] if flag in argv else None


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "--self-test":
        return self_test()
    if len(argv) == 2 and argv[1] == "--states":
        for state in REACH_STATES + P2_AUDIT_STATES:
            print(state)
        return 0

    if len(argv) >= 2 and argv[1] == "reach":
        if len(argv) != 4:
            print(__doc__, file=sys.stderr)
            return 2
        try:
            cov = json.loads(Path(argv[2]).read_text())
            rows = function_extents(cov, root=str(Path.cwd()))
        except (OSError, json.JSONDecodeError, ShapeError) as e:
            print(
                f"::error::cannot read the coverage export {argv[2]}: {e}. The mutation gate "
                "will fall back to grading EVERY in-diff mutant (its behaviour before "
                "prioritisation) rather than treat an unreadable report as 'nothing is "
                "covered', which would move the whole diff into the report-only class.",
                file=sys.stderr,
            )
            return 1
        Path(argv[3]).write_text("".join(f"{f}\t{lo}\t{hi}\t{c}\n" for (f, lo, hi, c) in rows))
        covered = sum(1 for *_, c in rows if c > 0)
        print(f"{len(rows)} function extents, {covered} executed by the offline suite")
        return 0

    if len(argv) >= 2 and argv[1] == "partition":
        if len(argv) < 3:
            print(__doc__, file=sys.stderr)
            return 2
        mutants = [
            strip_ansi(line).strip()
            for line in Path(argv[2]).read_text().splitlines()
            if strip_ansi(line).strip()
        ]
        extents_path = _arg(argv, "--extents")
        extents = None
        if extents_path:
            extents = load_extents(Path(extents_path).read_text().splitlines())
        p1, p2 = partition(mutants, extents)
        _write(_arg(argv, "--p1"), p1)
        _write(_arg(argv, "--p2"), p2)
        _write(_arg(argv, "--drop-p1"), exclusion_regexes(p1))
        _write(_arg(argv, "--drop-p2"), exclusion_regexes(p2))
        reach = "measured" if extents is not None else "unmeasured"
        print(f"reach={reach} p1={len(p1)} p2={len(p2)}")
        for name in p2:
            print(f"  report-only (offline coverage 0): {name}")
        return 0

    if len(argv) == 4 and argv[1] == "verify":
        expected = Path(argv[2]).read_text().splitlines()
        actual = Path(argv[3]).read_text().splitlines()
        errors = verify(expected, actual)
        for line in errors:
            print(line)
        return 1 if errors else 0

    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
