"""Grade a live suite's self-skip log: libtest prints `ok` for a test that returned early, so
`skip_live` writes `RIVET-SKIP <module::fn> — <why>` to a file, and this reads it the way the gate
does. A skip is explained by `core.SKIP_ALLOWED` (the gate's list) or by a prerequisite the lane
declares it lacks (`--lacking NAME`, matched against the reason); any other skip is a test the lane
was to run and did not.

    python3 -m dev.release_oracle.skip_census target/rivet-skips.log --lacking BIGQUERY_TEST_PROJECT
"""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from .core import SKIP_ALLOWED, self_skipped


def unexplained(skips: dict[str, str], lacking: list[str]) -> dict[str, str]:
    """The skips neither `SKIP_ALLOWED` nor a `lacking` prerequisite named in the reason explains."""
    return {t: why for t, why in skips.items()
            if t not in SKIP_ALLOWED and not any(l in why for l in lacking)}


def _lacking(raw: list[str]) -> list[str]:
    """`--lacking` values, each possibly several per line (a YAML block), blank lines dropped."""
    return [l.strip() for v in raw for l in v.splitlines() if l.strip()]


def _self_test() -> int:
    """RED-provable: drop the `t not in SKIP_ALLOWED` clause and the allowed record fails below."""
    allowed = next(iter(SKIP_ALLOWED))
    log = Path(tempfile.mkdtemp(prefix="rivet-skip-census-")) / "skips"
    log.write_text(f"RIVET-SKIP {allowed} — whatever it says\n"
                   "RIVET-SKIP live_x::needs_bq — x: BIGQUERY_TEST_PROJECT / RIVET_TEST_GCS_BUCKET unset\n"
                   "RIVET-SKIP live_x::ran_nothing — pgbouncer-state (:6433) is down\n")
    skips = self_skipped(log)
    assert len(skips) == 3, skips
    assert unexplained(skips, ["BIGQUERY_TEST_PROJECT"]) == {
        "live_x::ran_nothing": "pgbouncer-state (:6433) is down"}, "the unexplained skip must be the ONE named by no list"
    assert set(unexplained(skips, [])) == {"live_x::needs_bq", "live_x::ran_nothing"}
    assert unexplained(skips, _lacking(["BIGQUERY_TEST_PROJECT\n\npgbouncer-state (:6433)\n"])) == {}
    assert unexplained(self_skipped(log.with_name("absent")), []) == {}, "no log is no skip"
    print("self-test ok: a self-skip is explained by SKIP_ALLOWED or a named lacking prerequisite, never silently")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("log", nargs="?", type=Path, help="the RIVET_SKIP_LOG file (default target/rivet-skips.log)")
    ap.add_argument("--lacking", action="append", default=[],
                    help="a prerequisite this lane does not provide; a skip whose reason names it is explained")
    ap.add_argument("--self-test", action="store_true")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return _self_test()
    log = ns.log or Path("target/rivet-skips.log")
    skips = self_skipped(log)
    for t, why in sorted(skips.items()):
        print(f"SKIP {t} — {why}")
    bad = unexplained(skips, _lacking(ns.lacking))
    for t, why in sorted(bad.items()):
        print(f"::error::{t} skipped itself ({why}) and this lane was to run it: bring its prerequisite "
              "up, or name the one this lane lacks with --lacking", file=sys.stderr)
    print(f"self-skips: {len(skips)} ({log}), unexplained: {len(bad)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
