#!/usr/bin/env python3
"""What does the test harness know about ITSELF? One JSON per CI run, so a human can read a TREND.

The harness has grown gates about the product, gates about the gates, a mutation
prioritiser and a non-vacuity rule — and no metabase about any of it. Nobody can
answer "are more of our convention-cop guards proving their subject exists than
last month?" or "is the live-only class of mutants growing?" without reading
forty files. Five multi-agent bughunts over the 2026-08 refactors found ZERO
defects in the refactored code and SEVEN in the GUARDS those refactors touched;
three of the seven were blindness (a gate satisfiable by a doc COMMENT, a gate
whose subjects moved away while it kept passing over nothing). Those are exactly
the defects a trend line makes visible early — the numbers move the wrong way
long before a human notices the guard is dead.

So: emit the counts, publish them as a workflow artifact, print one line into the
job log. This is a THERMOMETER, not a gate. Nothing here fails a build, nothing
is a required check, and no threshold is enforced — a metric that blocks a merge
turns into a number people manage instead of a number people read.

THE ONE RULE OF THE SHAPING, and the reason it is worth a test: an UNKNOWN count
is `null`, never `0`, and every rate over an unknown is `null` too. A run whose
mutation job never happened must not publish "0 missed" — on a trend chart that
is an unbroken green streak meaning "we measured nothing", which is the same
fail-open reading `mutants_classify.py` refuses when a coverage report is
unreadable. Unknown is a value here, and it is spelled `null` / `?`.

Usage:
  harness_metrics.py census [--root <dir>] [--out <f>|-]
        Scan the tree and count what the harness holds: convention-cop guards
        (a `#[test]` file that grades a checked-in subject BY NAME), how many of
        them route that subject through `tests/offline/nonvacuity.rs` (so a
        moved subject fails loudly instead of grading the empty set), tests that
        declare themselves documentation rather than verification
        (`..._documents_...`), files that declare a blind spot in prose, and the
        DECLARED `#[test]` counts of the offline suite and the lib.

  harness_metrics.py shape --in <raw.json> [--out <f>|-]
        The PURE half: counts in, published document + summary line out. No
        filesystem scan, no environment, no subprocess — this is what
        `tests/offline/harness_metrics_guard.rs` drives from a fixture.

  harness_metrics.py emit [--root <dir>] [--out <f>] [key=value ...]
        census + the run's identity from the environment + whatever the
        workflow knows about this run's mutants, shaped and written. Prints the
        one-line human summary. `key` must be one this script declares (see
        MUTANT_COUNTS / MUTANT_LABELS): a workflow typo is an error, not a
        silently dropped metric.

  harness_metrics.py --self-test
        Grade the pure shaping and the census over fixtures.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from pathlib import Path

SCHEMA = "rivet-harness-metrics/v1"

# The mutation gate's numbers, in the vocabulary ci.yml and mutants_classify.py
# already publish. `in_scope` is the whole in-diff corpus before
# .cargo/mutants.toml, `graded` what survives it, and the offline/live split is
# the classifier's P1/P2 — the class that CAN be killed offline and the class
# that provably cannot.
MUTANT_COUNTS = (
    "in_scope",
    "excluded",
    "graded",
    "offline_reachable",
    "live_only",
    "caught",
    "missed",
    "unviable",
    "timeout",
)
MUTANT_LABELS = ("state", "reach", "report_only_audit")

GUARD_COUNTS = (
    "guard_files",
    "convention_cops",
    "subject_proven_non_empty",
    "subject_unproven",
    "documents_only_tests",
    "files_declaring_blind_spots",
)
TEST_COUNTS = ("offline_declared", "lib_declared")
RUN_LABELS = ("repo", "run_id", "sha", "event", "ref")


class ShapeError(ValueError):
    """The input is not a shape this script recognises.

    Raised rather than defaulted, for the reason the module docstring gives: a
    metrics document assembled out of a misunderstanding is worse than none,
    because it looks like data.
    """


# --------------------------------------------------------------------------
# the pure half
# --------------------------------------------------------------------------


def as_count(value: object, field: str, warnings: list[str]) -> int | None:
    """A count, or None for "not measured" — and never 0 for the second.

    `""` (an unset GitHub Actions output), `?`, `n/a` and a missing key all mean
    the same thing: this run did not measure it. Junk is also unknown, but says
    so in `warnings` — a workflow that starts feeding this garbage should be
    visible in the artifact rather than silently flat-lining at null.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ShapeError(f"`{field}` is a bool ({value!r}); counts are integers or unknown")
    if isinstance(value, int):
        if value < 0:
            raise ShapeError(f"`{field}` is negative ({value}); a count cannot be")
        return value
    text = str(value).strip()
    if text in ("", "?", "n/a", "unknown"):
        return None
    if not re.fullmatch(r"\d+", text):
        warnings.append(
            f"`{field}` is not a count ({text!r}) — recorded as unknown rather than guessed"
        )
        return None
    return int(text)


def as_label(value: object, field: str) -> str | None:
    """A label, or None when the step that would have set it never ran."""
    if value is None:
        return None
    if not isinstance(value, (str, int)):
        raise ShapeError(f"`{field}` is {type(value).__name__}; labels are strings")
    text = str(value).strip()
    return text or None


def rate(num: int | None, den: int | None) -> float | None:
    """A ratio, or None when either side is unknown OR the denominator is zero.

    Zero denominators are the ordinary case here (a docs-only PR grades no
    mutants), not an exceptional one — and "0 of 0 missed" rendered as 0.0 is a
    perfect score nobody earned.
    """
    if num is None or den is None or den == 0:
        return None
    return round(num / den, 4)


def _block(raw: object, name: str, allowed: tuple[str, ...]) -> dict:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ShapeError(f"`{name}` must be an object, got {type(raw).__name__}")
    unknown = sorted(set(raw) - set(allowed))
    if unknown:
        raise ShapeError(
            f"`{name}` carries key(s) {unknown} this script does not publish. A metric the "
            f"emitter drops silently is a metric nobody notices is gone — declare it here or "
            f"fix the caller. Known: {sorted(allowed)}"
        )
    return raw


def shape(raw: dict) -> dict:
    """Counts in, the published document out. Pure: no I/O, no clock, no env.

    Every field of the artifact is derived here, including the human summary, so
    the JSON and the line printed into the job log cannot drift apart.
    """
    if not isinstance(raw, dict):
        raise ShapeError(f"the raw input must be an object, got {type(raw).__name__}")
    warnings: list[str] = []

    run_raw = _block(raw.get("run"), "run", RUN_LABELS)
    mut_raw = _block(raw.get("mutants"), "mutants", MUTANT_COUNTS + MUTANT_LABELS)
    guards_raw = _block(raw.get("guards"), "guards", GUARD_COUNTS)
    tests_raw = _block(raw.get("tests"), "tests", TEST_COUNTS)

    run = {k: as_label(run_raw.get(k), f"run.{k}") for k in RUN_LABELS}
    mutants: dict[str, object] = {k: as_label(mut_raw.get(k), f"mutants.{k}") for k in MUTANT_LABELS}
    mutants.update(
        {k: as_count(mut_raw.get(k), f"mutants.{k}", warnings) for k in MUTANT_COUNTS}
    )
    guards = {k: as_count(guards_raw.get(k), f"guards.{k}", warnings) for k in GUARD_COUNTS}
    tests = {k: as_count(tests_raw.get(k), f"tests.{k}", warnings) for k in TEST_COUNTS}

    # Arithmetic the numbers claim about each other, checked — and REPORTED, not
    # repaired. A silently corrected total is a lie with a clean audit trail;
    # the point of the artifact is to show when the pipeline feeding it broke.
    scope, excluded, graded = mutants["in_scope"], mutants["excluded"], mutants["graded"]
    if None not in (scope, excluded, graded) and scope - excluded != graded:
        warnings.append(
            f"mutants: in_scope {scope} - excluded {excluded} != graded {graded} — the two "
            f"jobs feeding these disagree about this diff"
        )
    p1, p2 = mutants["offline_reachable"], mutants["live_only"]
    if None not in (p1, p2, graded) and p1 + p2 != graded:
        warnings.append(
            f"mutants: offline_reachable {p1} + live_only {p2} != graded {graded} — the "
            f"prioritiser and the corpus count describe different sets"
        )
    verdicts = [mutants[k] for k in ("caught", "missed", "unviable", "timeout")]
    pool = p1 if p1 is not None else graded
    if None not in verdicts and pool is not None and sum(verdicts) > pool:  # type: ignore[arg-type]
        warnings.append(
            f"mutants: {sum(verdicts)} verdicts recorded over a class of {pool} — more mutants "
            f"were run than were classified"
        )
    cops, proven, unproven = (
        guards["convention_cops"],
        guards["subject_proven_non_empty"],
        guards["subject_unproven"],
    )
    if None not in (cops, proven, unproven) and proven + unproven != cops:
        warnings.append(
            f"guards: subject_proven_non_empty {proven} + subject_unproven {unproven} != "
            f"convention_cops {cops} — the census is double-counting or losing a guard"
        )

    doc = {
        "schema": SCHEMA,
        "run": run,
        "mutants": mutants,
        "guards": guards,
        "tests": tests,
        "derived": {
            # Of the mutants that reached a VERDICT, not of the corpus: an
            # over-budget diff runs none, and 0/0 is null (see `rate`).
            "missed_rate": rate(
                mutants["missed"],
                None
                if mutants["missed"] is None or mutants["caught"] is None
                else mutants["missed"] + mutants["caught"],
            ),
            "live_only_rate": rate(mutants["live_only"], graded),
            "subject_proven_rate": rate(proven, cops),
        },
        "warnings": warnings,
    }
    doc["summary"] = summary_line(doc)
    return doc


def _n(value: object) -> str:
    """A count for human eyes: unknown is `?`, and stays visibly unknown."""
    return "?" if value is None else str(value)


def summary_line(doc: dict) -> str:
    """The one line that goes into the job log.

    Derived from the DOCUMENT, so the log and the artifact cannot disagree —
    the same reason `rivet validate` re-reads what it wrote instead of trusting
    the summary in memory.
    """
    m, g, t = doc["mutants"], doc["guards"], doc["tests"]
    labels = ", ".join(
        f"{k.replace('_', '-')}={m[k]}" for k in MUTANT_LABELS if m.get(k) is not None
    )
    return (
        f"harness-metrics: mutants {_n(m['in_scope'])} in scope -> {_n(m['graded'])} graded "
        f"({_n(m['offline_reachable'])} offline-reachable / {_n(m['live_only'])} live-only), "
        f"{_n(m['caught'])} caught / {_n(m['missed'])} missed"
        f"{f' [{labels}]' if labels else ''} | guards {_n(g['subject_proven_non_empty'])} of "
        f"{_n(g['convention_cops'])} convention cops prove a non-empty subject, "
        f"{_n(g['documents_only_tests'])} documents-only tests, "
        f"{_n(g['files_declaring_blind_spots'])} files declare a blind spot | tests "
        f"{_n(t['offline_declared'])} offline + {_n(t['lib_declared'])} lib (declared)"
    )


# --------------------------------------------------------------------------
# the census (I/O)
# --------------------------------------------------------------------------

# A guard "grades a checked-in subject BY NAME" when it names a repo path in
# CODE — the fragile class this whole exercise is about, since the subject can
# move out from under it and every "no offenders" assertion below stays green.
_SUBJECT_PATH = re.compile(
    r'"(?:\./)?(?:src|docs|tests|dev|examples|benches|\.github|\.cargo|\.githooks)/[^"]*"'
    r'|"(?:Cargo\.toml|Cargo\.lock|Makefile|README\.md)"'
)
# Slice 1's shared rule. A guard that routes its subject through it FAILS on a
# missing subject instead of grading the empty set — which is precisely the
# property this census exists to trend.
_NONVACUITY = re.compile(r"nonvacuity::(?:subject_text|require_enumerated|require_needle)")
_TEST_ATTR = re.compile(r"#\[(?:tokio::)?test\]")
# Process rule: "A name must not promise what the body cannot check" — a test that
# documents rather than verifies is named `..._documents_...` and says so.
_DOCUMENTS_ONLY = re.compile(r"\bfn\s+\w*_documents_\w*\s*\(")
# Process rule: "A test you cannot make RED should say so where a reader will see
# it." These are the phrasings the repo actually uses for that declaration.
_BLIND_SPOT = re.compile(r"CANNOT SEE|cannot be made RED|cannot go RED|CANNOT be made RED")


def _uncommented(text: str) -> str:
    """The file with `//` line comments removed.

    Load-bearing, and for a reason already paid for: `runner_frame_gate` was
    satisfiable by a doc COMMENT mentioning the call it demanded. A census that
    counts a guard as grading `docs/foo.yaml` because its module doc says so
    would make the same mistake one level up.
    """
    return "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("//"))


def _rs_files(root: Path, rel: str) -> list[Path]:
    return sorted(p for p in (root / rel).rglob("*.rs") if p.is_file())


def census(root: str | Path = ".") -> dict:
    """Count what the harness holds. Non-vacuous by construction — see the end."""
    root = Path(root)
    guards, cops, proven = 0, 0, 0
    for path in _rs_files(root, "tests/offline"):
        text = path.read_text(encoding="utf-8", errors="replace")
        code = _uncommented(text)
        if not _TEST_ATTR.search(code):
            continue  # a shared helper module (nonvacuity.rs), not a guard
        guards += 1
        if _SUBJECT_PATH.search(code):
            cops += 1
            if _NONVACUITY.search(code):
                proven += 1

    documents_only, blind = 0, 0
    for path in _rs_files(root, "tests") + _rs_files(root, "src"):
        text = path.read_text(encoding="utf-8", errors="replace")
        documents_only += len(_DOCUMENTS_ONLY.findall(_uncommented(text)))
        if _BLIND_SPOT.search(text):  # prose: read WITH the comments
            blind += 1

    def declared(paths: list[Path]) -> int:
        return sum(
            len(_TEST_ATTR.findall(p.read_text(encoding="utf-8", errors="replace"))) for p in paths
        )

    if guards == 0:
        raise ShapeError(
            f"no `#[test]` file under {root}/tests/offline — this census would publish a "
            f"harness with no guards in it, which is a bug in the scan, not a fact about the "
            f"repo. Re-point it at wherever the offline suite moved to."
        )
    return {
        "guards": {
            "guard_files": guards,
            "convention_cops": cops,
            "subject_proven_non_empty": proven,
            "subject_unproven": cops - proven,
            "documents_only_tests": documents_only,
            "files_declaring_blind_spots": blind,
        },
        # DECLARED, not executed: this job runs no cargo (it is meant to cost
        # seconds), so it counts `#[test]` attributes rather than a runner's
        # tally. The two differ by whatever is `cfg`-gated out of a given build
        # — a trend, not an oracle, and named `*_declared` so nobody reads it as
        # "the suite ran this many".
        "tests": {
            "offline_declared": declared(_rs_files(root, "tests/offline")),
            "lib_declared": declared(_rs_files(root, "src")),
        },
    }


# --------------------------------------------------------------------------
# self-test
# --------------------------------------------------------------------------

_FIXTURE = {
    "run": {"repo": "acme/rivet", "run_id": "42", "sha": "deadbeef", "event": "pull_request"},
    "mutants": {
        "state": "in-budget",
        "reach": "measured",
        "report_only_audit": "oracle-holds",
        "in_scope": 42,
        "excluded": 8,
        "graded": 34,
        "offline_reachable": 27,
        "live_only": 7,
        "caught": 25,
        "missed": 2,
        "unviable": 0,
        "timeout": 0,
    },
    "guards": {
        "guard_files": 40,
        "convention_cops": 19,
        "subject_proven_non_empty": 13,
        "subject_unproven": 6,
        "documents_only_tests": 6,
        "files_declaring_blind_spots": 3,
    },
    "tests": {"offline_declared": 285, "lib_declared": 2674},
}


def self_test() -> int:
    """Grade the pure shaping over fixtures, and the census over a fake tree.

    RED-provable one mutant at a time:
      * `as_count` returning 0 instead of None for an unset value — the whole
        fail-open reading this script exists to refuse;
      * `rate` dividing by a zero denominator (or returning 0.0 for it);
      * dropping any consistency warning — the artifact stops showing that the
        jobs feeding it disagree;
      * `_block` accepting an unknown key — a metric silently dropped;
      * `_uncommented` returning the text unchanged — a guard counted as a
        convention cop because its DOC mentions a path;
      * `census`'s `guards == 0` guard — an empty scan publishes an empty
        harness instead of failing.
    """
    doc = shape(_FIXTURE)
    assert doc["schema"] == SCHEMA, doc["schema"]
    assert doc["warnings"] == [], doc["warnings"]
    assert doc["mutants"]["missed"] == 2, doc["mutants"]
    assert doc["derived"] == {
        "missed_rate": round(2 / 27, 4),
        "live_only_rate": round(7 / 34, 4),
        "subject_proven_rate": round(13 / 19, 4),
    }, doc["derived"]
    assert "25 caught / 2 missed" in doc["summary"], doc["summary"]
    assert "13 of 19 convention cops" in doc["summary"], doc["summary"]

    # UNKNOWN IS NOT ZERO. A run whose mutation job never happened publishes
    # nulls and `?`s; a trend chart must show a hole, not a clean sheet.
    blank = {
        "mutants": {k: "" for k in MUTANT_COUNTS + MUTANT_LABELS},
        "guards": _FIXTURE["guards"],
        "tests": _FIXTURE["tests"],
    }
    doc = shape(blank)
    assert all(doc["mutants"][k] is None for k in MUTANT_COUNTS + MUTANT_LABELS), doc["mutants"]
    assert doc["derived"]["missed_rate"] is None, doc["derived"]
    assert doc["derived"]["live_only_rate"] is None, doc["derived"]
    assert "? caught / ? missed" in doc["summary"], doc["summary"]
    assert "[" not in doc["summary"], doc["summary"]  # no empty label bracket
    assert doc["run"] == dict.fromkeys(RUN_LABELS), doc["run"]

    # A zero denominator is the ordinary docs-only case, not a division error,
    # and not a perfect score either.
    zero = {
        "mutants": {"in_scope": 0, "excluded": 0, "graded": 0, "caught": 0, "missed": 0},
        "guards": {**_FIXTURE["guards"], "convention_cops": 0},
        "tests": _FIXTURE["tests"],
    }
    doc = shape(zero)
    assert doc["derived"] == {
        "missed_rate": None,
        "live_only_rate": None,
        "subject_proven_rate": None,
    }, doc["derived"]

    # Numbers that contradict each other are REPORTED, never repaired.
    doc = shape({**_FIXTURE, "mutants": {**_FIXTURE["mutants"], "graded": 30}})
    assert len(doc["warnings"]) == 2, doc["warnings"]
    assert "!= graded 30" in doc["warnings"][0], doc["warnings"]
    doc = shape({**_FIXTURE, "guards": {**_FIXTURE["guards"], "subject_unproven": 5}})
    assert any("double-counting" in w for w in doc["warnings"]), doc["warnings"]
    doc = shape({**_FIXTURE, "mutants": {**_FIXTURE["mutants"], "caught": 99}})
    assert any("were run than were classified" in w for w in doc["warnings"]), doc["warnings"]
    doc = shape({**_FIXTURE, "mutants": {**_FIXTURE["mutants"], "missed": "banana"}})
    assert doc["mutants"]["missed"] is None and any("not a count" in w for w in doc["warnings"]), (
        doc["warnings"]
    )

    # Shapes this script must REFUSE rather than average over.
    for broken in (
        [],
        {"mutants": {"missed_mutants": 1}},
        {"guards": {"convention_cops": -1}},
        {"mutants": {"caught": True}},
        {"tests": []},
    ):
        try:
            shape(broken)  # type: ignore[arg-type]
        except ShapeError:
            pass
        else:
            raise AssertionError(f"shape accepted that should not be: {broken}")

    # The census, over a tree built for it: one guard proving its subject, one
    # naming a path only in a COMMENT (not a cop), one helper with no `#[test]`.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "tests/offline").mkdir(parents=True)
        (root / "src").mkdir(parents=True)
        (root / "tests/offline/proven.rs").write_text(
            '#[test]\nfn a() { let _ = super::nonvacuity::subject_text("docs/x.yaml"); }\n'
        )
        (root / "tests/offline/bare.rs").write_text(
            '#[test]\nfn b() { std::fs::read_to_string("src/lib.rs").unwrap(); }\n'
            "#[test]\nfn c_documents_the_wire() {}\n"
        )
        (root / "tests/offline/commentary.rs").write_text(
            '//! Grades "docs/x.yaml" — in prose only.\n'
            "//! WHAT THIS CANNOT SEE: the runner.\n"
            "#[test]\nfn d() {}\n"
        )
        (root / "tests/offline/helper.rs").write_text("pub fn h() {}\n")
        (root / "src/lib.rs").write_text("#[cfg(test)]\nmod t { #[test] fn e() {} }\n")
        got = census(root)
    assert got["guards"] == {
        "guard_files": 3,
        "convention_cops": 2,
        "subject_proven_non_empty": 1,
        "subject_unproven": 1,
        "documents_only_tests": 1,
        "files_declaring_blind_spots": 1,
    }, got["guards"]
    assert got["tests"] == {"offline_declared": 4, "lib_declared": 1}, got["tests"]
    assert shape(got)["warnings"] == [], shape(got)["warnings"]

    try:
        with tempfile.TemporaryDirectory() as tmp:
            census(tmp)
    except ShapeError:
        pass
    else:
        raise AssertionError("an empty tree must not census as a harness with no guards")

    print(
        "self-test ok: shaping (full / unknown-is-null / zero-denominator / four consistency "
        "warnings / five refused shapes) and the census (comment-stripped cops, proven "
        "subjects, documents-only, blind spots, declared counts, empty-tree refusal)"
    )
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _arg(argv: list[str], flag: str, default: str | None = None) -> str | None:
    return argv[argv.index(flag) + 1] if flag in argv else default


def _emit(path: str | None, doc: dict) -> None:
    text = json.dumps(doc, indent=2, sort_keys=False) + "\n"
    if path in (None, "-"):
        sys.stdout.write(text)
    else:
        Path(str(path)).write_text(text)


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "--self-test":
        return self_test()

    cmd = argv[1] if len(argv) > 1 else ""
    try:
        if cmd == "census":
            _emit(_arg(argv, "--out", "-"), census(_arg(argv, "--root", ".") or "."))
            return 0

        if cmd == "shape":
            src = _arg(argv, "--in")
            if not src:
                print(__doc__, file=sys.stderr)
                return 2
            doc = shape(json.loads(Path(src).read_text()))
            _emit(_arg(argv, "--out", "-"), doc)
            print(doc["summary"])
            return 0

        if cmd == "emit":
            raw = census(_arg(argv, "--root", ".") or ".")
            raw["run"] = {
                "repo": os.environ.get("GITHUB_REPOSITORY"),
                "run_id": os.environ.get("GITHUB_RUN_ID"),
                "sha": os.environ.get("GITHUB_SHA"),
                "event": os.environ.get("GITHUB_EVENT_NAME"),
                "ref": os.environ.get("GITHUB_REF_NAME"),
            }
            mutants: dict[str, str] = {}
            for pair in argv[2:]:
                if pair.startswith("--"):
                    continue
                if pair in (_arg(argv, "--root"), _arg(argv, "--out")):
                    continue
                key, _, value = pair.partition("=")
                mutants[key] = value
            raw["mutants"] = mutants
            doc = shape(raw)
            _emit(_arg(argv, "--out", "harness-metrics.json"), doc)
            print(doc["summary"])
            return 0
    except (OSError, json.JSONDecodeError, ShapeError) as e:
        # Loud and non-zero, but this job is non-blocking by construction: the
        # thermometer breaking must be visible without holding a merge.
        print(f"::error::harness metrics: {e}", file=sys.stderr)
        return 1

    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
