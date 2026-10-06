"""Seeded-defect recall: re-introduce known bug classes as source patches and grade whether the harness catches each.

    python -m dev.seeded                                  # every seed on every engine
    python -m dev.seeded --seeds committed-every-event --engines postgres,mysql
    python -m dev.seeded --self-test                      # manifest + grading logic, no build

See docs/seeded-recall.md for what a row means and how to add a seed.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import yaml

from dev.release_oracle.core import Ledger, isolate_state_db, nextest_filter, nextest_outcomes, self_skipped

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "seeds.yaml"
SCOPES = {
    "source": ("postgres", "mysql", "mssql", "mongo", "oracle"),
    "state": ("postgres-state", "sqlite-state"),
}
OPTIONAL = ("lib",)
CAUGHT, MISSED, STALE, BROKEN, NO_CONTROL, NA = "CAUGHT", "MISSED", "STALE", "BROKEN", "NO-CONTROL", "N/A"
LIB = "lib:"


@dataclass(frozen=True)
class Row:
    """One seed on one engine, as the manifest declares it."""

    seed: str
    engine: str
    patch: str | None = None
    cells: tuple[str, ...] = ()
    na: str | None = None


@dataclass(frozen=True)
class Verdict:
    """A graded row: its status and the evidence for it."""

    row: Row
    status: str
    detail: str


def parse_manifest(doc: dict, patch_dir: Path = HERE) -> list[Row]:
    """The manifest's seed x engine rows; raises ValueError naming the first malformed entry."""
    rows: list[Row] = []
    names: set[str] = set()
    for s in (doc or {}).get("seeds") or []:
        name = s.get("name")
        if not name or name in names:
            raise ValueError(f"seed {name!r}: missing or duplicate name")
        names.add(name)
        scope = SCOPES.get(s.get("scope"))
        if scope is None:
            raise ValueError(f"seed {name}: scope must be one of {sorted(SCOPES)}")
        engines = s.get("engines") or {}
        missing = [e for e in scope if e not in engines]
        extra = [e for e in engines if e not in scope and e not in OPTIONAL]
        if missing or extra:
            raise ValueError(f"seed {name}: engines missing {missing}, outside the scope {extra} — "
                             "name every engine, with `na:` and a reason where the code path does not exist")
        for eng, e in engines.items():
            e = e or {}
            if "na" in e:
                if set(e) != {"na"} or not str(e["na"]).strip():
                    raise ValueError(f"seed {name}/{eng}: `na:` takes a reason and nothing else")
                rows.append(Row(name, eng, na=str(e["na"]).strip()))
                continue
            patch, cells = e.get("patch", s.get("patch")), e.get("cells")
            if not patch or not isinstance(cells, list):
                raise ValueError(f"seed {name}/{eng}: needs a patch and a `cells:` list, or `na:`")
            if not (patch_dir / patch).is_file():
                raise ValueError(f"seed {name}/{eng}: patch {patch} does not exist under {patch_dir}")
            rows.append(Row(name, eng, patch, tuple(str(c) for c in cells)))
    if not rows:
        raise ValueError("the manifest declares no seeds")
    return rows


def unknown_cells(rows: list[Row], root: Path = ROOT) -> list[tuple[Row, str]]:
    """Declared cells with no `fn <name>(` in the tree — a renamed test would otherwise grade as absent forever."""
    live = "\n".join(p.read_text() for p in (root / "tests").rglob("*.rs"))
    lib = "\n".join(p.read_text() for p in (root / "src").rglob("*.rs"))
    out = []
    for r in rows:
        for c in r.cells:
            src, name = (lib, c[len(LIB):]) if c.startswith(LIB) else (live, c)
            if not re.search(rf"\bfn {re.escape(name)}\(", src):
                out.append((r, c))
    return out


def patch_files(patch_text: str) -> list[str]:
    """The repo-relative files a unified diff changes."""
    return re.findall(r"^\+\+\+ b/(\S+)", patch_text, re.M)


def stale_message(seed: str, patch_text: str, git_out: str) -> str:
    """The loud refusal for a patch that no longer applies, naming the file(s) git rejected."""
    files = patch_files(patch_text)
    named = [f for f in files if f in git_out] or files
    return f"seed {seed} no longer applies to {', '.join(named)}: update the patch"


def cell_outcomes(out: str, skipped: dict[str, str], cells: tuple[str, ...]) -> dict[str, str]:
    """Each cell's verdict in one nextest run: PASS, FAIL, SKIP (self-skipped, graded nothing) or ABSENT."""
    final = nextest_outcomes(out)
    skip_names = {k.split("::")[-1] for k in skipped}
    res: dict[str, str] = {}
    for c in cells:
        bare = c.removeprefix(LIB)
        seen = [s for n, s in final.items() if n == bare or n.endswith("::" + bare)]
        if not seen:
            res[c] = "ABSENT"
        elif any(s not in ("PASS", "LEAK") for s in seen):
            res[c] = "FAIL"
        else:
            res[c] = "SKIP" if bare in skip_names else "PASS"
    return res


def grade(row: Row, control: dict[str, str], seeded: dict[str, str] | None) -> Verdict:
    """CAUGHT iff a cell green on the unpatched tree is red on the patched one; `seeded=None` = the patched tree did not build."""
    if row.na:
        return Verdict(row, NA, row.na)
    if seeded is None:
        return Verdict(row, BROKEN, "the patched tree does not build — a red build is not a catch")
    if not row.cells:
        return Verdict(row, MISSED, "no cell catches this class on this engine")
    green = [c for c in row.cells if control.get(c) == "PASS"]
    if not green:
        said = ", ".join(f"{c}={control.get(c, 'ABSENT')}" for c in row.cells)
        return Verdict(row, NO_CONTROL, f"no declared cell is green on the unpatched tree ({said})")
    red = [c for c in green if seeded.get(c) == "FAIL"]
    if red:
        return Verdict(row, CAUGHT, ", ".join(red))
    said = ", ".join(f"{c}={seeded.get(c, 'ABSENT')}" for c in green)
    return Verdict(row, MISSED, f"every control-green cell stayed green under the seed ({said})")


# ── the scratch tree ──────────────────────────────────────────────────────────


def sh(argv: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None,
       timeout: float | None = None) -> subprocess.CompletedProcess:
    """Run argv (no shell) with exactly `env` (or the inherited one); stdout+stderr merged into `.stdout`."""
    return subprocess.run(argv, cwd=cwd, env=env, timeout=timeout, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def git(cwd: Path, *args: str) -> str:
    """A git command that must succeed; its trimmed output."""
    p = sh(["git", "-C", str(cwd), *args])
    if p.returncode != 0:
        raise SystemExit(f"git {' '.join(args)} in {cwd} failed:\n{p.stdout}")
    return p.stdout.strip()


def scratch_base() -> Path:
    """Where the scratch worktree and its own target dir live (kept between runs so builds stay incremental)."""
    return Path(os.environ.get("RIVET_SEEDED_DIR", ROOT / "target" / "seeded"))


def prepare_tree(base: Path) -> tuple[Path, str]:
    """A clean scratch worktree at HEAD; refuses one a killed run left a seed in."""
    sha = git(ROOT, "rev-parse", "HEAD")
    tree = base / "tree"
    if (tree / ".git").exists():
        dirty = git(tree, "status", "--porcelain", "--untracked-files=no")
        if dirty:
            raise SystemExit(f"{tree} has local changes — a run killed mid-seed? `git -C {tree} diff` "
                             f"shows them; remove the tree to start over:\n{dirty}")
        git(tree, "switch", "--quiet", "--detach", sha)
    else:
        base.mkdir(parents=True, exist_ok=True)
        git(ROOT, "worktree", "prune")
        git(ROOT, "worktree", "add", "--quiet", "--detach", str(tree), sha)
    live = tree / "tests" / ".live-tmp"
    if not live.is_symlink():
        main_live = (ROOT / "tests" / ".live-tmp").resolve()
        main_live.mkdir(parents=True, exist_ok=True)
        live.symlink_to(main_live)
    # The Rig's oracle runs `uv run` in the tree; parallel first uses race to build one .venv.
    # On this harness's own interpreter: left to discovery, uv picks whatever Python PATH offers.
    venv = sh(["uv", "sync", "--frozen", "-q", "--python", sys.executable], cwd=tree)
    if venv.returncode != 0:
        raise SystemExit(f"uv sync in {tree} failed:\n{venv.stdout}")
    return tree, sha


def apply_seed(tree: Path, patch: Path) -> dict[Path, bytes]:
    """Snapshot every file the patch touches, then apply it; the snapshot is the restore point."""
    snap = {tree / f: (tree / f).read_bytes() for f in patch_files(patch.read_text())}
    p = sh(["git", "-C", str(tree), "apply", str(patch)])
    if p.returncode != 0:
        restore(tree, snap)
        raise RuntimeError(p.stdout)
    return snap


def restore(tree: Path, snap: dict[Path, bytes]) -> None:
    """Write each snapshotted file back with a fresh mtime (so cargo rebuilds it), then demand a clean tree."""
    for f, data in snap.items():
        f.write_bytes(data)
        os.utime(f)
    dirty = git(tree, "status", "--porcelain", "--untracked-files=no")
    if dirty:
        raise SystemExit(f"{tree} is not clean after restoring the snapshot — stop and inspect:\n{dirty}")


class Cargo:
    """Builds and runs nextest in the scratch tree with its own target dir and the tree's own rivet binary."""

    def __init__(self, tree: Path, base: Path, state_url: str | None) -> None:
        self.tree, self.base = tree, base
        env = {k: v for k, v in os.environ.items()
               if k not in ("RIVET_BIN", "RIVET_BIN_OVERRIDE", "RIVET_STATE_URL", "RIVET_TEST_STATE_URL")}
        env["CARGO_TARGET_DIR"] = str(base / "target")
        if state_url:
            env["RIVET_TEST_STATE_URL"] = state_url
        self.env = env

    @staticmethod
    def binaries(cells) -> list[str]:
        """The nextest binary flags these cells need; a seed with no cells still compiles the library."""
        if not cells:
            return ["--lib"]
        flags = []
        if any(not c.startswith(LIB) for c in cells):
            flags += ["--test", "live_suite"]
        if any(c.startswith(LIB) for c in cells):
            flags += ["--lib"]
        return flags

    def build(self, cells, log: Path) -> subprocess.CompletedProcess:
        """Compile the test binaries (and the rivet binary they drive)."""
        p = sh(["cargo", "nextest", "run", "--no-run", "--manifest-path", str(self.tree / "Cargo.toml"),
                *self.binaries(cells)], cwd=self.tree, env=self.env)
        log.write_text(p.stdout)
        return p

    def run(self, cells, log: Path) -> dict[str, str]:
        """Run exactly these cells; each one's verdict."""
        skip_log = log.with_suffix(".skips")
        skip_log.write_text("")
        p = sh(["cargo", "nextest", "run", "--manifest-path", str(self.tree / "Cargo.toml"),
                *self.binaries(cells), "--run-ignored", "all", "--no-fail-fast", "--retries", "0",
                "-E", nextest_filter([c.removeprefix(LIB) for c in cells])],
               cwd=self.tree, env={**self.env, "RIVET_SKIP_LOG": str(skip_log)})
        log.write_text(p.stdout)
        return cell_outcomes(p.stdout, self_skipped(skip_log), tuple(cells))


def recompiled(build_out: str) -> bool:
    """Did cargo compile the crate (not report it Fresh)? A Fresh build grades the unpatched binary."""
    return re.search(r"^\s*Compiling rivet-cli\b", build_out, re.M) is not None


# ── the run ───────────────────────────────────────────────────────────────────


def recall(led: Ledger, rows: list[Row]) -> list[Verdict]:
    """Grade every row: STALE check, one CONTROL run on the unpatched tree, then one build+run per patch."""
    t0 = time.perf_counter()
    led.phase("Seeded-defect recall — does the harness catch known bug classes re-introduced?")
    verdicts: dict[Row, Verdict] = {}
    for r, c in unknown_cells(rows):
        verdicts[r] = Verdict(r, STALE, f"cell {c} names no test in the tree: update the manifest")
    base = scratch_base()
    tree, sha = prepare_tree(base)
    logs = base / "logs"
    logs.mkdir(exist_ok=True)
    led.ok(f"scratch tree {tree} at {sha[:12]} (HEAD — commit first; uncommitted work is not graded)")
    gate_state = os.environ.get("RIVET_GATE_STATE_URL", "")
    state_url = isolate_state_db(gate_state, f"seed_{os.getpid()}") if gate_state else None
    led.ok(f"scratch state DB: {state_url.rsplit('/', 1)[-1] if state_url else 'none (RIVET_GATE_STATE_URL unset)'}")
    cargo = Cargo(tree, base, state_url)

    for r in rows:
        if r.patch and r not in verdicts:
            text = (HERE / r.patch).read_text()
            p = sh(["git", "-C", str(tree), "apply", "--check", str(HERE / r.patch)])
            if p.returncode != 0:
                verdicts[r] = Verdict(r, STALE, stale_message(r.seed, text, p.stdout))
    live = [r for r in rows if r not in verdicts and not r.na]
    control_cells = sorted({c for r in live for c in r.cells})
    control: dict[str, str] = {}
    if control_cells:
        led.phase(f"CONTROL — {len(control_cells)} cell(s) on the unpatched tree")
        b = cargo.build(control_cells, logs / "control.build.log")
        if b.returncode != 0:
            led.bad(f"the unpatched tree does not build (see {logs / 'control.build.log'})")
        else:
            control = cargo.run(control_cells, logs / "control.log")
            for c, v in control.items():
                (led.ok if v == "PASS" else led.bad)(f"control {c}: {v}")

    for patch in sorted({r.patch for r in live}):
        group = [r for r in live if r.patch == patch]
        cells = sorted({c for r in group for c in r.cells if control.get(c) == "PASS"})
        name = patch.replace("/", "__").removesuffix(".patch")
        led.phase(f"SEED {patch} — {', '.join(r.engine for r in group)} ({len(cells)} control-green cell(s))")
        snap = apply_seed(tree, HERE / patch)
        try:
            b = cargo.build(cells, logs / f"{name}.build.log")
            if b.returncode != 0 or not recompiled(b.stdout):
                why = "build failed" if b.returncode != 0 else "cargo did not recompile rivet-cli"
                led.bad(f"{why} (see {logs / f'{name}.build.log'})")
                seeded = None
            else:
                seeded = cargo.run(cells, logs / f"{name}.log") if cells else {}
        finally:
            restore(tree, snap)
        for r in group:
            verdicts[r] = grade(r, control, seeded)

    out = [verdicts.get(r) or grade(r, control, {}) for r in rows]
    for v in out:
        msg = f"seeded {v.row.seed} × {v.row.engine}: {v.status} — {v.detail}"
        if v.status == CAUGHT:
            led.passed(v.row.engine, "seeded", v.row.seed, "-", msg)
        elif v.status != NA:
            led.failed(v.row.engine, "seeded", v.row.seed, "-", msg)
    print(render(out))
    print(f"  seeded-defect recall took {(time.perf_counter() - t0) / 60:.1f} min; logs under {logs}")
    return out


def render(verdicts: list[Verdict]) -> str:
    """The seed × engine recall table, then one line per row that is not CAUGHT."""
    seeds = list(dict.fromkeys(v.row.seed for v in verdicts))
    engines = list(dict.fromkeys(v.row.engine for v in verdicts))
    at = {(v.row.seed, v.row.engine): v.status for v in verdicts}
    w = max(len(s) for s in seeds)
    lines = ["", "seed".ljust(w) + " | " + " | ".join(e.ljust(10) for e in engines)]
    lines.append("-" * len(lines[-1]))
    for s in seeds:
        lines.append(s.ljust(w) + " | " + " | ".join(at.get((s, e), "").ljust(10) for e in engines))
    graded = [v for v in verdicts if v.status != NA]
    caught = sum(v.status == CAUGHT for v in graded)
    lines.append(f"\nrecall: {caught}/{len(graded)} graded rows CAUGHT (N/A rows excluded)")
    for v in verdicts:
        if v.status != CAUGHT:
            lines.append(f"  {v.status:<10} {v.row.seed} × {v.row.engine}: {v.detail}")
    return "\n".join(lines)


def select(rows: list[Row], seeds: str, engines: str) -> list[Row]:
    """Narrow to the named seeds / engines (comma lists); an unknown name is an error, never an empty run."""
    for want, have, what in ((seeds, {r.seed for r in rows}, "seed"), (engines, {r.engine for r in rows}, "engine")):
        unknown = [x for x in want.split(",") if x and x not in have]
        if unknown:
            raise SystemExit(f"unknown {what}(s): {unknown}; the manifest has {sorted(have)}")
    return [r for r in rows
            if (not seeds or r.seed in seeds.split(",")) and (not engines or r.engine in engines.split(","))]


def self_test() -> int:
    """Manifest parsing and the CAUGHT / MISSED / STALE / BROKEN / NO-CONTROL logic, on fakes."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        pd = Path(d)
        (pd / "p.patch").write_text("--- a/src/x.rs\n+++ b/src/x.rs\n@@ -1 +1 @@\n-a\n+b\n")
        eng = {"postgres": {"patch": "p.patch", "cells": ["t1", "lib:u1"]}, "mysql": {"patch": "p.patch", "cells": []},
               "mssql": {"na": "why"}, "mongo": {"na": "why"}, "oracle": {"na": "why"}}
        rows = parse_manifest({"seeds": [{"name": "s", "scope": "source", "engines": eng}]}, pd)
        assert [(r.engine, r.cells, r.na) for r in rows][:3] == [
            ("postgres", ("t1", "lib:u1"), None), ("mysql", (), None), ("mssql", (), "why")], rows
        bad = [
            ({"seeds": []}, "no seeds"),
            ({"seeds": [{"name": "s", "scope": "nope", "engines": eng}]}, "scope"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {**eng, "mysql": None}}]}, "needs a patch"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {k: v for k, v in eng.items() if k != "mongo"}}]},
             "missing ['mongo']"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {**eng, "mongo": {"na": ""}}}]}, "takes a reason"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": {**eng, "mssql": {"patch": "gone.patch",
                                                                                      "cells": []}}}]}, "does not exist"),
            ({"seeds": [{"name": "s", "scope": "source", "engines": eng}] * 2}, "duplicate"),
            ({"seeds": [{"name": "s", "scope": "state", "engines": eng}]}, "outside the scope"),
        ]
        for doc, want in bad:
            try:
                parse_manifest(doc, pd)
            except ValueError as e:
                assert want in str(e), (want, str(e))
            else:
                raise AssertionError(f"accepted a manifest that should fail with {want!r}")
    pg = Row("s", "postgres", "p.patch", ("t1", "t2"))
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "PASS", "t2": "FAIL"}).status == CAUGHT
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "PASS", "t2": "PASS"}).status == MISSED
    # red already on the unpatched tree proves nothing: that cell is not graded
    assert grade(pg, {"t1": "FAIL", "t2": "PASS"}, {"t1": "FAIL", "t2": "PASS"}).status == MISSED
    assert grade(pg, {"t1": "FAIL", "t2": "SKIP"}, {"t1": "FAIL", "t2": "FAIL"}).status == NO_CONTROL
    # a self-skip under the seed graded nothing, and a cell that did not run is not red
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, {"t1": "SKIP", "t2": "ABSENT"}).status == MISSED
    assert grade(pg, {"t1": "PASS", "t2": "PASS"}, None).status == BROKEN
    assert grade(Row("s", "mysql", "p.patch", ()), {}, {}).status == MISSED
    assert grade(Row("s", "mysql", "p.patch", ()), {}, None).status == BROKEN, "a cell-less seed must still compile"
    assert grade(Row("s", "mongo", na="immutable"), {}, {}).status == NA
    sample = (
        "        PASS [   1.0s] (1/4) rivet-cli::live_suite live_a::t1\n"
        " FAIL + LEAK [   2.0s] (2/4) rivet-cli::live_suite live_a::t2\n"
        "        PASS [   0.1s] (3/4) rivet-cli state::tests::u1\n"
        "        PASS [   0.1s] (4/4) rivet-cli::live_suite live_b::t1_longer\n"
    )
    got = cell_outcomes(sample, {"state::tests::u1": "no url"}, ("t1", "t2", "lib:u1", "t3"))
    assert got == {"t1": "PASS", "t2": "FAIL", "lib:u1": "SKIP", "t3": "ABSENT"}, got
    patch = "--- a/src/a.rs\n+++ b/src/a.rs\n@@\n--- a/src/b.rs\n+++ b/src/b.rs\n@@\n"
    assert stale_message("s", patch, "error: patch failed: src/b.rs:12\nerror: src/b.rs: patch does not apply") \
        == "seed s no longer applies to src/b.rs: update the patch"
    assert "src/a.rs, src/b.rs" in stale_message("s", patch, "fatal: corrupt patch")
    assert recompiled("   Compiling rivet-cli v0.30.0 (/x)\n") and not recompiled("       Fresh rivet-cli v0.30.0\n")
    table = render([Verdict(pg, CAUGHT, "t2"), Verdict(Row("s", "mysql", "p", ()), MISSED, "none"),
                    Verdict(Row("s", "mongo", na="x"), NA, "x")])
    assert "recall: 1/2" in table and "MISSED     s × mysql" in table, table
    real = parse_manifest(yaml.safe_load(MANIFEST.read_text()))
    assert not unknown_cells(real), unknown_cells(real)
    for r in real:
        if r.patch:
            assert patch_files((HERE / r.patch).read_text()), f"{r.patch} changes no file"
    print(f"self-test ok: seeded recall — manifest ({len(real)} rows), grading, stale and build checks")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry: grade the selected seeds against the local stand."""
    ap = argparse.ArgumentParser(prog="seeded-recall")
    ap.add_argument("--seeds", default="", help="comma-separated seed names (default: all)")
    ap.add_argument("--engines", default="", help="comma-separated engines (default: all)")
    ap.add_argument("--self-test", action="store_true", help="check the manifest and the grading logic; no build")
    ns = ap.parse_args(argv)
    if ns.self_test:
        return self_test()
    rows = select(parse_manifest(yaml.safe_load(MANIFEST.read_text())), ns.seeds, ns.engines)
    led = Ledger()
    recall(led, rows)
    return 1 if led.red else 0


if __name__ == "__main__":
    sys.exit(main())
