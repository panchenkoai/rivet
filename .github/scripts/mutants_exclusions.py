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

  2. does the entry reach more than one SITE? An unanchored function name is
     a prefix: `in check` also excluded `check_null_ratios`, `check_uniqueness`,
     `check_memory`, `check_export_selection`, … — 13 mutants in six files that
     the offline guard's sample corpus happened not to contain.

     A SITE is `(file, function)`, not the bare function name. Keying on the
     name alone re-opened the hole from the other side: `"replace density_probe
     with"` matches `src/init/mysql.rs::density_probe` AND
     `src/init/postgres.rs::density_probe`, and the check printed a contented
     `ok  2 mutant(s) in density_probe` — one function, said the set, because
     the path had been thrown away. There it is the intended triage; the next
     bare free-function name that recurs across engine modules would un-grade
     mutants in files its author never opened, and this job would report green.
     Multi-site entries are therefore DECLARED in `MULTI_SITE_OK` below, with a
     reason, and a declaration that no longer spans sites is itself an error —
     a stale exception is the same false claim as a dead exclusion.

Usage:  mutants_exclusions.py <corpus-file>
        mutants_exclusions.py --self-test
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


def site_of(name: str) -> tuple[str, str]:
    """What identifies a mutant: the FILE it lives in and the function in it.

    `function_of` alone is not an identity — the tree has same-named free
    functions in sibling engine modules (`density_probe` in both
    `src/init/mysql.rs` and `src/init/postgres.rs`), and an exclusion that
    reaches both looks single-function to a set keyed on the name.
    """
    return (name.split(":", 1)[0], function_of(name))


# Exclusions ALLOWED to span more than one file, each with the reason. The key
# is the exact `exclude_re` string; the value is why one triage argument covers
# every site it reaches. Anything not listed here that spans sites is an error,
# and a listed entry that no longer spans sites (or is no longer in the config)
# is an error too — an exception outliving its subject is a claim about nothing,
# the dead-exclusion defect in a different costume.
MULTI_SITE_OK: dict[str, str] = {
    "replace density_probe with": (
        "one function per engine — src/init/mysql.rs and src/init/postgres.rs each "
        "define `density_probe`, and the triage argument (takes a live connection, "
        "unkillable offline, decisions extracted into unit-tested helpers, glue "
        "live-guarded) is the same for both. Named in the .cargo/mutants.toml comment."
    ),
    "delete ! in density_probe": (
        "same two per-engine `density_probe` bodies as the entry above; the `!` each "
        "one negates is inside the same live-only glue."
    ),
}


_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(line: str) -> str:
    """Corpus lines with the colour taken back out.

    `cargo mutants --list` honours CARGO_TERM_COLOR, and CI sets it to
    `always` — so the names arrive wrapped in SGR escapes and a plain-text
    pattern matches nothing. That is not hypothetical: it failed this job on
    2026-08-14 with all 18 exclusions reported dead, one of them passing only
    because its pattern happened to end before the first escape byte. The
    step below also forces `--color=never`; this strips defensively, because
    the next caller may not.
    """
    return _ANSI.sub("", line)


def grade(
    patterns: list[str],
    corpus: list[str],
    multi_site_ok: dict[str, str] | None = None,
) -> tuple[list[str], list[str]]:
    """(errors, ok-lines) for these exclusion patterns against this corpus.

    Pure — no file I/O, no corpus-size floor — so the self-test can drive it
    with a hand-built corpus that contains exactly the shapes being graded.
    """
    allow = MULTI_SITE_OK if multi_site_ok is None else multi_site_ok
    errors: list[str] = []
    oks: list[str] = []

    for pat in patterns:
        try:
            rx = re.compile(pat)
        except re.error as e:
            errors.append(f"::error::exclude_re `{pat}` is not a valid regex: {e}")
            continue
        hits = [n for n in corpus if rx.search(n)]
        if not hits:
            errors.append(
                f"::error::exclude_re `{pat}` matches NO mutant in the corpus. A dead "
                "exclusion triages nothing: either the mutant it was written for is "
                "still being graded (check the exact name — cargo-mutants qualifies "
                "impl methods as `Type::method`), or the code moved and the entry "
                "should be deleted."
            )
            continue
        sites = sorted({site_of(n) for n in hits})
        funcs = sorted({f for _, f in sites})
        sample = "\n  ".join(sorted(set(hits))[:8])
        if len(funcs) > 1:
            errors.append(
                f"::error::exclude_re `{pat}` reaches {len(funcs)} different "
                f"functions ({', '.join(funcs)}) — an unanchored name is a PREFIX, so "
                "it is silently removing mutants it was not written for. Anchor it "
                f"(`... in fn$`) or split it into one entry per function.\n  {sample}"
            )
            continue
        if len(sites) > 1 and pat not in allow:
            where = ", ".join(f for f, _ in sites)
            errors.append(
                f"::error::exclude_re `{pat}` reaches `{funcs[0]}` in {len(sites)} "
                f"DIFFERENT FILES ({where}) — one triage argument is being applied to "
                "code its author may never have opened (same-named free functions in "
                "sibling engine modules are routine here). Qualify the entry with its "
                "path, or declare it in MULTI_SITE_OK in this script with the reason "
                f"one argument covers every site.\n  {sample}"
            )
            continue
        if len(sites) > 1:
            oks.append(
                f"ok  {len(hits):5d} mutant(s) in {funcs[0]} across {len(sites)} "
                f"declared sites  <-  {pat}"
            )
        else:
            oks.append(f"ok  {len(hits):5d} mutant(s) in {sites[0][0]}::{funcs[0]}  <-  {pat}")

    # The exception list is graded like the exclusions themselves.
    for pat, reason in allow.items():
        if pat not in patterns:
            errors.append(
                f"::error::MULTI_SITE_OK declares `{pat}`, which is not in "
                ".cargo/mutants.toml's exclude_re — a stale exception. Delete it."
            )
            continue
        if not reason.strip():
            errors.append(f"::error::MULTI_SITE_OK entry `{pat}` has no reason.")
        hits = [n for n in corpus if re.search(pat, n)]
        if len({site_of(n) for n in hits}) <= 1:
            errors.append(
                f"::error::MULTI_SITE_OK declares `{pat}` as spanning several files, but "
                "it now reaches at most one site. The exception outlived its subject — "
                "delete it so a future multi-site match is caught."
            )
    return errors, oks


_SELFTEST_CORPUS = [
    # The real shape this check was blind to: one function name, two files.
    "src/init/mysql.rs:136:1: replace density_probe with None",
    "src/init/postgres.rs:193:1: replace density_probe with None",
    # A single-site entry, the ordinary case.
    "src/manifest.rs:10:5: replace * with + in compute_part_checksums",
    "src/manifest.rs:12:5: replace * with / in compute_part_checksums",
    # The prefix case that motivated the original check.
    "src/preflight/mod.rs:1:1: replace == with != in check",
    "src/quality.rs:2:2: replace == with != in check_null_ratios",
]


def self_test() -> int:
    """Unit-check the grader against a corpus holding each shape it must judge.

    RED-provable: key `sites` on `function_of` alone (the pre-fix behaviour) and
    the multi-file case below stops erroring; drop the MULTI_SITE_OK staleness
    check and the stale-declaration case stops erroring.
    """
    two_files = ["replace density_probe with"]
    errs, oks = grade(two_files, _SELFTEST_CORPUS, multi_site_ok={})
    assert errs and "DIFFERENT FILES" in errs[0], (errs, oks)
    assert "src/init/mysql.rs" in errs[0] and "src/init/postgres.rs" in errs[0], errs

    errs, oks = grade(two_files, _SELFTEST_CORPUS, multi_site_ok={"replace density_probe with": "declared"})
    assert not errs and "2 declared sites" in oks[0], (errs, oks)

    errs, oks = grade(["replace \\* with .* in compute_part_checksums"], _SELFTEST_CORPUS, multi_site_ok={})
    assert not errs and "src/manifest.rs::compute_part_checksums" in oks[0], (errs, oks)

    errs, _ = grade(["replace == with != in check"], _SELFTEST_CORPUS, multi_site_ok={})
    assert errs and "different functions" in errs[0], errs

    errs, _ = grade(["nothing matches this"], _SELFTEST_CORPUS, multi_site_ok={})
    assert errs and "matches NO mutant" in errs[0], errs

    # A declaration for a pattern the config no longer carries, and one whose
    # subject shrank to a single site — both stale, both errors.
    errs, _ = grade([], _SELFTEST_CORPUS, multi_site_ok={"replace density_probe with": "r"})
    assert errs and "stale exception" in errs[0], errs
    errs, _ = grade(
        ["replace \\* with \\+ in compute_part_checksums"],
        _SELFTEST_CORPUS,
        multi_site_ok={"replace \\* with \\+ in compute_part_checksums": "r"},
    )
    assert errs and "outlived its subject" in errs[0], errs

    # Every real declaration must still name a real config entry (the corpus
    # half of that is graded in CI, where the corpus exists).
    patterns = tomllib.loads(CONFIG.read_text()).get("exclude_re", [])
    for pat in MULTI_SITE_OK:
        assert pat in patterns, f"MULTI_SITE_OK declares `{pat}`, absent from exclude_re"

    print("self-test ok: multi-file, declared, single-site, prefix, dead, and both stale-exception cases")
    return 0


def main() -> int:
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        return self_test()
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    corpus = [
        _plain(line).strip()
        for line in Path(sys.argv[1]).read_text().splitlines()
        if _plain(line).strip()
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

    errors, oks = grade(patterns, corpus)
    for line in oks:
        print(line)
    for line in errors:
        print(line)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
