#!/usr/bin/env python3
"""Compatibility gate: the working-tree binary against the PREVIOUS RELEASE.

Every other gate in this repo runs ONE binary. A whole class of defect lives
only BETWEEN two versions and is therefore invisible to all of them — the
artifacts rivet persists (`plan.json`, `manifest.json`, checkpoints) outlive the
binary that wrote them, so an upgrade sits in the middle of every real
plan→apply, run→load and run→validate sequence.

It has cost this repo twice already:

  * `verify` was added to the plan contract as a REQUIRED field (2026-06-02) and
    `rivet apply` rejected every `plan.json` users already had on disk. Two
    months, while a frozen-fixture test asserted the JSON shape and stayed green.
  * `export_family` was added to `ResolvedRunPlan` (this gate's first find). The
    field deserialized fine — `#[serde(default)]` — but the plan's integrity hash
    covers the WHOLE serialized struct, so plan-then-upgrade-then-apply failed in
    BOTH directions with "resolved_plan was modified after planning … Do not
    hand-edit plan files", blaming the operator for upgrading rivet.

Note the second one: serde tolerance does not imply compatibility. Only running
the two binaries against each other's artifacts shows that.

Four axes, cheapest first:

  cli       every subcommand's flag set                  offline
  schema    every config JSON-schema key path            offline
  config    a corpus of configs x flows: accept/reject    offline
  artifacts plan→apply, run→validate, mixed prefix        needs postgres

`cli`, `schema` and `config` need no services and belong on every PR. `artifacts`
needs a live postgres and belongs wherever the stack is already up.

    python3 -m dev.pytools.release_compat offline   # cli + schema + config
    python3 -m dev.pytools.release_compat all       # + artifacts (needs postgres)
    python3 -m dev.pytools.release_compat all --baseline ./rivet-0.24.1

DELIBERATE breaks are declared in `dev/release_compat_allow.txt`, one per line as
`<axis>:<key>  # reason`. An undeclared divergence fails the gate; a declared one
prints as ALLOWED with its reason. That file is the point of the gate — it turns
"did anything change for users?" from an argument into a diff someone signed.

The baseline is the DOWNLOADED released binary (a GitHub release asset or the
Homebrew bottle), never a rebuild of the parent commit: the release profile is
fat-LTO, so rebuilding costs minutes and still only approximates what users run.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Callable, Sequence

from . import shell

REPO_ROOT = Path(__file__).resolve().parents[2]
ALLOW_FILE = REPO_ROOT / "dev" / "release_compat_allow.txt"

PG = "postgres://rivet:rivet@127.0.0.1:5432/rivet"
MY = "mysql://rivet:rivet@127.0.0.1:3306/rivet"
MYC = "mysql://rivet:rivet@127.0.0.1:3307/rivet"
MS = "sqlserver://sa:Passw0rd!@127.0.0.1:1433/rivet"
MG = "mongodb://127.0.0.1:27017/rivet"


# ── allowlist ────────────────────────────────────────────────────────────────
def load_allow() -> dict[str, str]:
    """`<axis>:<key>` → reason. A break with no reason is not a decision."""
    allow: dict[str, str] = {}
    if not ALLOW_FILE.exists():
        return allow
    for raw in ALLOW_FILE.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        entry, _, reason = line.partition("#")
        entry, reason = entry.strip(), reason.strip()
        if not entry:
            continue
        if not reason:
            raise SystemExit(
                f"{ALLOW_FILE}: '{entry}' has no reason. Every declared break "
                f"carries why, or the file becomes a mute list of things "
                f"someone once silenced."
            )
        allow[entry] = reason
    return allow


# ── surface probes ───────────────────────────────────────────────────────────
def _help(binary: str, *args: str) -> str:
    p = shell.run([binary, *args, "--help"], timeout=60)
    return p.stdout + p.stderr


def subcommands(binary: str) -> list[str]:
    out, seen = [], set()
    for line in _help(binary).splitlines():
        m = re.match(r"^  ([a-z][a-z-]+)\s{2,}\S", line)
        if m and m.group(1) not in seen and m.group(1) != "help":
            seen.add(m.group(1))
            out.append(m.group(1))
    return out


def flags(binary: str, *args: str) -> dict[str, str | None]:
    """Long flags → value placeholder (None for a boolean switch)."""
    found: dict[str, str | None] = {}
    for line in _help(binary, *args).splitlines():
        for m in re.finditer(r"(--[a-z0-9][a-z0-9-]*)(\s+<([A-Z_]+)>)?", line):
            name, val = m.group(1), m.group(3)
            if name not in found or val:
                found[name] = val
    return found


def cli_surface(binary: str) -> dict[str, dict[str, str | None]]:
    surface = {"<global>": flags(binary)}
    for sc in subcommands(binary):
        surface[sc] = flags(binary, sc)
    return surface


def schema_surface(binary: str) -> dict[str, str]:
    """Every config key PATH → its declared type, flattened from the schema."""
    p = shell.run([binary, "schema", "config"], timeout=120)
    try:
        doc = json.loads(p.stdout)
    except json.JSONDecodeError as e:
        raise SystemExit(f"{binary}: `schema config` did not emit JSON: {e}")
    keys: dict[str, str] = {}

    def walk(node: object, path: str) -> None:
        if not isinstance(node, dict):
            return
        for k, v in (node.get("properties") or {}).items():
            p_ = f"{path}.{k}" if path else k
            t = v.get("type") if isinstance(v, dict) else None
            if isinstance(t, list):
                t = "|".join(str(x) for x in t)
            keys[p_] = t or ("$ref" if isinstance(v, dict) and "$ref" in v else "?")
            walk(v, p_)
        for key in ("items", "additionalProperties"):
            if isinstance(node.get(key), dict):
                walk(node[key], f"{path}[]")
        for comb in ("anyOf", "oneOf", "allOf"):
            for sub in node.get(comb) or []:
                walk(sub, path)

    walk(doc, "")
    for name, d in (doc.get("$defs") or doc.get("definitions") or {}).items():
        walk(d, f"<{name}>")
    return keys


# ── config corpus ────────────────────────────────────────────────────────────
def _cfg(src_type: str, url: str, export_lines: str, extra_top: str = "") -> str:
    return (
        f"source:\n  type: {src_type}\n  url: \"{url}\"\n"
        f"exports:\n  - name: t\n    table: t\n{export_lines}"
        f"    destination: {{ type: local, path: /tmp/rivet-compat-out }}\n{extra_top}"
    )


def corpus() -> dict[str, str]:
    """One config per FLOW x ENGINE x notable option.

    Deliberately spans every runner (`full`/`incremental`/range-`chunked`/keyset/
    time-window/CDC) on every engine, because a per-export feature wired into
    only some runners is this repo's most recurrent defect class and a corpus
    that samples one runner cannot see it.
    """
    cases: dict[str, str] = {}
    for eng, url in [("postgres", PG), ("mysql", MY), ("mssql", MS), ("mongo", MG)]:
        cases[f"full/{eng}"] = _cfg(eng, url, "    mode: full\n    format: parquet\n")
        cases[f"incremental/{eng}"] = _cfg(
            eng, url, "    mode: incremental\n    cursor_column: updated_at\n    format: parquet\n"
        )
        cases[f"chunked/{eng}"] = _cfg(
            eng, url, "    mode: chunked\n    chunk_column: id\n    chunk_size: 100000\n    format: parquet\n"
        )
        cases[f"keyset/{eng}"] = _cfg(
            eng, url, "    mode: chunked\n    chunk_by_key: id\n    chunk_size: 50000\n    format: parquet\n"
        )
        cases[f"timewindow/{eng}"] = _cfg(
            eng, url, "    mode: time_window\n    cursor_column: created_at\n    window_days: 7\n    format: parquet\n"
        )
    for eng, url in [("postgres", PG), ("mysql", MYC), ("mssql", MS), ("mongo", MG)]:
        base = "    mode: cdc\n    format: parquet\n"
        cases[f"cdc-ckpt/{eng}"] = _cfg(eng, url, base + "    cdc:\n      checkpoint: /tmp/ck\n")
        cases[f"cdc-nockpt/{eng}"] = _cfg(eng, url, base)
        cases[f"cdc-initial/{eng}"] = _cfg(
            eng, url, base + "    cdc:\n      checkpoint: /tmp/ck\n      initial: snapshot\n"
        )
        cases[f"cdc-maxev/{eng}"] = _cfg(
            eng, url, base + "    cdc:\n      checkpoint: /tmp/ck\n      max_events: 1000\n"
        )
        cases[f"cdc-daemon/{eng}"] = _cfg(
            eng, url, base + "    cdc:\n      checkpoint: /tmp/ck\n      until_current: false\n"
        )
    cases["fmt-csv"] = _cfg("postgres", PG, "    mode: full\n    format: csv\n")
    cases["fmt-ndjson"] = _cfg("postgres", PG, "    mode: full\n    format: ndjson\n")
    cases["parallel"] = _cfg(
        "postgres", PG, "    mode: chunked\n    chunk_column: id\n    parallel: 4\n    format: parquet\n"
    )
    cases["chunk-ckpt"] = _cfg(
        "postgres", PG, "    mode: chunked\n    chunk_column: id\n    chunk_checkpoint: true\n    format: parquet\n"
    )
    cases["meta-cols"] = _cfg("postgres", PG, "    mode: full\n    format: parquet\n    meta_columns: true\n")
    cases["row-hash"] = _cfg("postgres", PG, "    mode: full\n    format: parquet\n    row_hash: all\n")
    cases["drift-fail"] = _cfg("postgres", PG, "    mode: full\n    format: parquet\n    on_schema_drift: fail\n")
    cases["load-bq"] = _cfg(
        "postgres", PG, "    mode: full\n    format: parquet\n",
        "load:\n  target: bigquery\n  project: p\n  dataset: d\n",
    )
    cases["load-sf"] = _cfg(
        "postgres", PG, "    mode: full\n    format: parquet\n",
        "load:\n  target: snowflake\n  database: d\n  schema: s\n  warehouse: w\n",
    )
    cases["load-gc"] = _cfg(
        "postgres", PG, "    mode: full\n    format: parquet\n",
        "load:\n  target: bigquery\n  project: p\n  dataset: d\n  gc_orphans: true\n",
    )
    cases["load-cdc-pk"] = _cfg(
        "mysql", MYC, "    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: /tmp/ck\n",
        "load:\n  target: bigquery\n  project: p\n  dataset: d\n  pk: [id]\n",
    )
    cases["multiplex"] = (
        f"source:\n  type: mysql\n  url: \"{MYC}\"\nexports:\n  - name: t\n"
        "    tables: [a, b]\n    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: /tmp/ck\n"
        "    destination: { type: local, path: /tmp/rivet-compat-out }\n"
    )
    cases["name-ne-table"] = (
        f"source:\n  type: mysql\n  url: \"{MYC}\"\nexports:\n  - name: orders_cdc\n"
        "    table: orders\n    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: /tmp/ck\n"
        "      initial: snapshot\n    destination: { type: local, path: /tmp/rivet-compat-out }\n"
    )
    return cases


def verdict(binary: str, yaml_text: str) -> tuple[str, str]:
    """ACCEPT/REJECT for one config, with the reason when rejected.

    `rivet check` reports a config error BEFORE it opens a connection, so a
    connection failure means the config itself was accepted — which is what makes
    this axis runnable with no services up. Identifiers are masked out of the
    reason so two binaries comparing the same break match on the MESSAGE, not on
    the export name.
    """
    fd, path = tempfile.mkstemp(suffix=".yaml")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(yaml_text)
        p = shell.run([binary, "check", "-c", path], timeout=120)
        txt = p.stdout + p.stderr
        m = re.search(r"^Error: (.*)$", txt, re.M)
        if m:
            msg = m.group(1)
            if "config file" in msg or msg.startswith("export '"):
                return ("REJECT", re.sub(r"'[^']*'", "'X'", msg)[:120])
            return ("ACCEPT", "(preflight/connect)")
        return ("ACCEPT", "ok")
    finally:
        os.unlink(path)


# ── axes ─────────────────────────────────────────────────────────────────────
def _diff(old: dict, new: dict, axis: str, allow: dict[str, str]) -> list[str]:
    """Unallowed divergences as printable lines; ALLOWED ones are reported."""
    bad: list[str] = []
    for k in sorted(set(old) - set(new)):
        key = f"{axis}:{k}"
        (print(f"    ALLOWED  removed {k} — {allow[key]}") if key in allow
         else bad.append(f"    REMOVED  {k} (was {old[k]!r})"))
    for k in sorted(set(new) - set(old)):
        key = f"{axis}:{k}"
        # An ADDED key is additive — new flags/config keys do not break an
        # existing user. Reported, never fatal.
        print(f"    added    {k} = {new[k]!r}")
    for k in sorted(set(old) & set(new)):
        if old[k] == new[k]:
            continue
        key = f"{axis}:{k}"
        (print(f"    ALLOWED  changed {k} — {allow[key]}") if key in allow
         else bad.append(f"    CHANGED  {k}: {old[k]!r} → {new[k]!r}"))
    return bad


def axis_cli(base: str, new: str, allow: dict[str, str]) -> list[str]:
    print("  ── cli ──")
    a, b = cli_surface(base), cli_surface(new)
    bad = []
    for sc in sorted(set(a) | set(b)):
        if sc not in a:
            print(f"    added    subcommand {sc}")
            continue
        if sc not in b:
            key = f"cli:{sc}"
            (print(f"    ALLOWED  removed subcommand {sc} — {allow[key]}") if key in allow
             else bad.append(f"    REMOVED  subcommand {sc}"))
            continue
        bad += _diff(a[sc], b[sc], f"cli:{sc}", allow)
    print(f"    {len(set(a) & set(b))} subcommands compared")
    return bad


def axis_schema(base: str, new: str, allow: dict[str, str]) -> list[str]:
    print("  ── schema ──")
    a, b = schema_surface(base), schema_surface(new)
    bad = _diff(a, b, "schema", allow)
    print(f"    {len(a)} baseline keys compared")
    return bad


def axis_config(base: str, new: str, allow: dict[str, str]) -> list[str]:
    print("  ── config ──")
    bad, n = [], 0
    for name, yaml_text in corpus().items():
        va, vb = verdict(base, yaml_text), verdict(new, yaml_text)
        n += 1
        if va[0] == vb[0]:
            continue
        key = f"config:{name}"
        if key in allow:
            print(f"    ALLOWED  {name}: {va[0]} → {vb[0]} — {allow[key]}")
        else:
            bad.append(f"    CHANGED  {name}: {va[0]} → {vb[0]}\n             now: {vb[1]}")
    print(f"    {n} configs compared")
    return bad


def axis_artifacts(base: str, new: str, allow: dict[str, str]) -> list[str]:
    """plan→apply, run→validate and a mixed prefix, BOTH directions.

    The only axis that needs a live source, and the one that found the sealed-
    plan break: an artifact written by one version must be consumed by the other,
    and a `#[serde(default)]` field can still shift an integrity hash.
    """
    print("  ── artifacts ──")
    bad: list[str] = []
    work = Path(tempfile.mkdtemp(prefix="rivet-compat-"))
    table = "rivet_compat_probe"
    seed = shell.run(
        ["docker", "exec", "rivet-postgres-1", "psql", "-U", "rivet", "-d", "rivet", "-c",
         f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table}(id BIGINT PRIMARY KEY, v TEXT); "
         f"INSERT INTO {table} SELECT g, 'v'||g FROM generate_series(1,200) g;"],
        timeout=120,
    )
    if not seed.ok:
        print("    SKIP: postgres stand not reachable (docker exec rivet-postgres-1 failed)")
        return bad
    try:
        env = {"RIVET_COMPAT_URL": PG}
        cfg = work / "cfg.yaml"
        cfg.write_text(
            "source: { type: postgres, url_env: RIVET_COMPAT_URL }\n"
            "exports:\n"
            f"  - name: {table}\n    table: {table}\n"
            "    mode: chunked\n    chunk_column: id\n    chunk_size: 50\n"
            "    format: parquet\n"
            f"    destination: {{ type: local, path: {work}/out }}\n"
        )

        def plan_apply(planner: str, applier: str, label: str) -> None:
            shutil.rmtree(work / "out", ignore_errors=True)
            art = work / f"plan-{label}.json"
            p = shell.run([planner, "plan", "-c", str(cfg), "--format", "json", "-o", str(art)],
                          env=env, timeout=300)
            if not p.ok:
                bad.append(f"    PLAN FAILED ({label}): {p.out.strip()[:160]}")
                return
            a = shell.run([applier, "apply", str(art)], env=env, timeout=600)
            if not a.ok:
                bad.append(f"    APPLY FAILED ({label}): {a.out.strip()[:200]}")

        plan_apply(base, new, "old-plan→new-apply")
        plan_apply(new, base, "new-plan→old-apply")

        def run_read(runner: str, reader: str, label: str, sub: str) -> None:
            out = work / f"o-{label}"
            c = work / f"cfg-{label}.yaml"
            c.write_text(cfg.read_text().replace(f"{work}/out", str(out)))
            r = shell.run([runner, "run", "-c", str(c)], env=env, timeout=600)
            if not r.ok:
                bad.append(f"    RUN FAILED ({label}): {r.out.strip()[:160]}")
                return
            v = shell.run([reader, sub, "-c", str(c)], env=env, timeout=600)
            if not v.ok:
                bad.append(f"    {sub.upper()} FAILED ({label}): {v.out.strip()[:200]}")

        run_read(base, new, "old-run-new-validate", "validate")
        run_read(new, base, "new-run-old-validate", "validate")

        # A prefix holding manifests from BOTH versions — the state every
        # upgrading deployment passes through, and where a recorded-vs-derived
        # identity split shows up as a load that refuses its own output.
        mix = work / "mix"
        cmix = work / "cfg-mix.yaml"
        cmix.write_text(cfg.read_text().replace(f"{work}/out", str(mix)))
        for who in (base, new):
            r = shell.run([who, "run", "-c", str(cmix)], env=env, timeout=600)
            if not r.ok:
                bad.append(f"    MIXED RUN FAILED: {r.out.strip()[:160]}")
        for who, tag in ((base, "old"), (new, "new")):
            v = shell.run([who, "validate", "-c", str(cmix)], env=env, timeout=600)
            if not v.ok:
                bad.append(f"    MIXED VALIDATE FAILED ({tag} reader): {v.out.strip()[:200]}")
        print("    6 cross-version artifact exchanges compared")
    finally:
        shell.run(["docker", "exec", "rivet-postgres-1", "psql", "-U", "rivet", "-d", "rivet",
                   "-c", f"DROP TABLE IF EXISTS {table};"], timeout=60)
        shutil.rmtree(work, ignore_errors=True)
    return bad


# ── driver ───────────────────────────────────────────────────────────────────
def _sha256(path: str) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_baseline(explicit: str | None, candidate: str) -> str:
    """The DOWNLOADED previous release, never the working tree's own build.

    Order: `--baseline`, `$RIVET_BASELINE`, then a `rivet` on PATH.

    Every candidate is RESOLVED THROUGH SYMLINKS and then rejected if it points
    into this repo's `target/`. That check exists because it fired: a dogfood
    convenience symlink (`~/.local/bin/rivet` -> `<repo>/target/release/rivet`)
    made `which rivet` resolve to the WORKING TREE's release build. While that
    build happened to predate the branch the gate looked honest — it even
    reported the one declared break — and the moment `cargo build --release` ran,
    the gate silently began comparing the branch against itself and printed
    "ok — no undeclared incompatibility" having verified nothing.

    A path comparison could not see it: two different paths, one file. So the
    content is hashed too, and an identical hash is fatal. Neither check is
    redundant — the symlink check names the CAUSE (so the operator knows to pass
    `--baseline`), the hash check catches any other way one build reaches two
    paths (a copy, a hardlink, a stale install).
    """
    for cand in (explicit, os.environ.get("RIVET_BASELINE")):
        if cand:
            if not Path(cand).exists():
                raise SystemExit(f"baseline binary not found: {cand}")
            return _vet_baseline(str(Path(cand).resolve()), candidate)
    found = shutil.which("rivet")
    if not found:
        raise SystemExit(
            "no baseline binary. Pass --baseline <path>, set $RIVET_BASELINE, or "
            "download the previous release asset. Do NOT rebuild the parent commit "
            "— the release profile is fat-LTO and a rebuild only approximates what "
            "users run."
        )
    return _vet_baseline(str(Path(found).resolve()), candidate)


def _vet_baseline(base: str, candidate: str) -> str:
    """Refuse a baseline that is really the candidate wearing another path."""
    target_dir = (REPO_ROOT / "target").resolve()
    if str(Path(base).resolve()).startswith(str(target_dir)):
        raise SystemExit(
            f"baseline resolves INTO this repo's build tree:\n"
            f"    {base}\n"
            f"That is the working tree's own binary (often via a dogfood symlink "
            f"such as ~/.local/bin/rivet -> target/release/rivet), so the gate "
            f"would compare this branch against itself and pass having measured "
            f"nothing.\n"
            f"Pass the PREVIOUS RELEASE explicitly: --baseline <path>, or\n"
            f"    gh release download <prev-tag> --pattern 'rivet-*' --dir /tmp/base"
        )
    if Path(base).exists() and Path(candidate).exists() and _sha256(base) == _sha256(candidate):
        raise SystemExit(
            f"baseline and candidate are byte-identical ({_sha256(base)[:16]}…):\n"
            f"    baseline  {base}\n"
            f"    candidate {candidate}\n"
            f"One build at two paths measures nothing. Point --baseline at the "
            f"previous release."
        )
    return base


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    mode = "offline"
    if args and not args[0].startswith("-"):
        mode = args.pop(0)
    if mode not in ("offline", "all", "cli", "schema", "config", "artifacts"):
        print(f"unknown mode {mode!r}; use offline|all|cli|schema|config|artifacts")
        return 2
    baseline = None
    if "--baseline" in args:
        baseline = args[args.index("--baseline") + 1]
    new = os.environ.get("RIVET", str(REPO_ROOT / "target" / "debug" / "rivet"))
    if not Path(new).exists():
        raise SystemExit(f"working-tree binary not found: {new} (cargo build --bin rivet)")
    base = resolve_baseline(baseline, new)
    allow = load_allow()

    bv = shell.run([base, "--version"], timeout=60).stdout.strip()
    nv = shell.run([new, "--version"], timeout=60).stdout.strip()
    print(f"release-compat: baseline {bv} ({base})")
    print(f"                candidate {nv} ({new})")
    if allow:
        print(f"                {len(allow)} declared break(s) in {ALLOW_FILE.name}")

    axes: dict[str, Callable[[str, str, dict[str, str]], list[str]]] = {
        "cli": axis_cli, "schema": axis_schema, "config": axis_config, "artifacts": axis_artifacts,
    }
    selected = (["cli", "schema", "config"] if mode == "offline"
                else list(axes) if mode == "all" else [mode])
    bad: list[str] = []
    for name in selected:
        bad += axes[name](base, new, allow)

    print()
    if bad:
        print(f"release-compat: {len(bad)} UNDECLARED incompatibilit(ies)")
        for line in bad:
            print(line)
        print(
            f"\nEach is a change users feel across an upgrade. Fix it, or declare it in "
            f"{ALLOW_FILE.relative_to(REPO_ROOT)} with the reason."
        )
        return 1
    print("release-compat: ok — no undeclared incompatibility")
    return 0


if __name__ == "__main__":
    shell.main(lambda: main_cli())
