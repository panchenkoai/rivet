"""Deterministic architecture facts for the review agents: one collector, thin views (README.md next to this file).

    uv run python -m dev.pytools.archfacts collect --features both
    uv run python -m dev.pytools.archfacts view architect --lens hotspots
    uv run python -m dev.pytools.archfacts view zoom --path src/load
    uv run python -m dev.pytools.archfacts view diagnose --fn execute_load
    python3 -m dev.pytools.archfacts selftest
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import collect as collector
from . import config, scip, selftest, views

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "target" / "archfacts"


def load(args: argparse.Namespace) -> dict:
    """The facts file a view reads: `--facts`, or the one `collect` wrote for this commit, feature set and backend."""
    if args.facts:
        return json.loads(Path(args.facts).read_text())
    ra = scip.version() if args.backend == "scip" else ""
    sha, key, _ = collector.cache_key(ROOT, args.features, args.backend, ra or "")
    path = collector.facts_path(OUT, sha, key)
    if not path.exists():
        raise SystemExit(f"no facts for {sha[:12]} ({args.features}, {args.backend}): run `collect --features {args.features} --backend {args.backend}` first")
    return json.loads(path.read_text())


def main() -> int:
    ap = argparse.ArgumentParser(prog="archfacts", description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect", help="write target/archfacts/<sha>-<key>.json")
    c.add_argument("--features", choices=[*config.FEATURE_SETS, "both"], default="default")
    c.add_argument("--backend", choices=["scip", "treesitter"], default="scip")
    c.add_argument("--force", action="store_true", help="ignore the cache and re-index")
    v = sub.add_parser("view", help="render a Markdown view of collected facts")
    v.add_argument("view", choices=["architect", "zoom", "diagnose", "diff"])
    v.add_argument("--lens", default="hotspots", help=", ".join(views.LENSES))
    v.add_argument("--top", type=int, default=15)
    v.add_argument("--path")
    v.add_argument("--fn")
    v.add_argument("--features", choices=list(config.FEATURE_SETS), default="default")
    v.add_argument("--backend", choices=["scip", "treesitter"], default="scip")
    v.add_argument("--facts", help="read this facts file instead of the cached one")
    s = sub.add_parser("selftest", help="grade the tool against its fixture crate and expected.yaml")
    s.add_argument("--rivet", action="store_true", help="also check expected.yaml against rivet itself (indexes the crate)")
    s.add_argument("--require-index", action="store_true", help="fail instead of skipping when rust-analyzer is missing")
    s.add_argument("--no-index", action="store_true", help="skip the checks that need rust-analyzer (counted as SKIP)")
    args = ap.parse_args()
    if args.cmd == "selftest":
        return selftest.run(ROOT, OUT, rivet=args.rivet, require_index=args.require_index, no_index=args.no_index)
    if args.cmd == "collect":
        for features in (list(config.FEATURE_SETS) if args.features == "both" else [args.features]):
            path, facts = collector.collect(ROOT, features, args.backend, OUT, force=args.force)
            meta, un = facts["meta"], facts["unresolved"]
            idx = meta["timing"].get("index")
            cost = f"cache {meta['cache']}" + (f"; index took {idx['wall_s']}s, peak RSS {idx['peak_rss_mb']} MB" if idx else "")
            state = "approx" if meta["approx"] else ("DEGRADED" if meta["degraded"] else "ok")
            print(f"{path.relative_to(ROOT)}  [{features}, {args.backend}, {state}; unresolved {un['unresolved']}/{un['identifiers']}; {cost}]")
        return 0
    facts = load(args)
    if args.view == "architect":
        print(views.architect(facts, args.lens, args.top))
    elif args.view == "zoom":
        if not args.path:
            ap.error("view zoom needs --path")
        print(views.zoom(facts, args.path))
    elif args.view == "diagnose":
        if not args.fn:
            ap.error("view diagnose needs --fn")
        print(views.diagnose(facts, args.fn))
    else:
        print(views.diff(facts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
