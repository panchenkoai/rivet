#!/usr/bin/env python3
"""Governor self-throttle A/B stand.

Runs the same self-spilling MySQL export through two rivet binaries x two
tuning modes (adaptive on/off) and grades each binary on the class invariant
behind the 2026-08-13 field regression: ON AN IDLE SOURCE, adaptivity has
nothing legitimate to react to, so `adaptive: true` must shed no workers and
cost ~nothing versus `adaptive: false`. A binary whose governor listens to a
counter the export itself inflates (the 0.24.4 bug) sheds toward
`min_parallel` and fails both gates.

Fixture: `SELECT DISTINCT` over a wide payload — every chunk materializes the
whole derived table; with `tmp_table_size` forced to 16 KB (root-flipped
globals, restored on exit) every chunk provably bumps
`Created_tmp_disk_tables`, the exact counter the buggy governor listened to.

A PASS additionally requires POSITIVE proof from rivet's own stderr that the
governor armed and can sample — `sheds == 0` is equally true of a governor that
never ran, which would certify a candidate against a measurement of nothing
(see `governor_evidence_failure`; `--self-test` grades that rule offline).

    python3 -m dev.pytools.governor_ab --rivet-a ~/.local/bin/rivet \
                                       --rivet-b target/release/rivet
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

TABLE = "gov_stand_ab"
MYSQL_CONTAINER = "rivet-mysql-1"
MYSQL_URL = "mysql://rivet:rivet@127.0.0.1:3306/rivet"

# The product's own lines about the governor's health, quoted from
# src/pipeline/governor.rs. `sheds == 0` is satisfied by a governor that never
# ARMED (`adaptive && parallel > 1` is false: `parallel: 1`, no `parallel:`
# line, `mode: full`), that could not open its monitoring connection, or whose
# sampler yields nothing — three states in which the stand grades a run that
# never exercised the mechanism. So the adaptive leg must show the ARM line and
# neither negative line before its `sheds`/ratio numbers mean anything.
GOV_ARMED = "adaptive concurrency governor active"
GOV_NO_MONITOR = "governor monitoring connection failed"
GOV_DEAF = "provides no pressure signal"


def governor_evidence_failure(stderr: str) -> str | None:
    """Why this leg's stderr proves NOTHING about the governor, or `None`.

    Pure (stderr in, reason out) so the grading rule is checkable without a
    live MySQL — `--self-test` exercises every arm.
    """
    if GOV_ARMED not in stderr:
        return (
            "the governor never armed on the adaptive leg — no "
            f"'{GOV_ARMED}' line. It arms only on `adaptive: true` AND "
            "`parallel: > 1` in a chunked/keyset export; this run graded nothing"
        )
    if GOV_NO_MONITOR in stderr:
        return (
            f"the governor could not open its monitoring connection ('{GOV_NO_MONITOR}') "
            "— parallelism stayed static, so no-shed is vacuous"
        )
    if GOV_DEAF in stderr:
        return (
            f"the governor armed but is DEAF ('{GOV_DEAF}') — its sampler returned "
            "nothing (missing counter, lost privilege, renamed view), so parallelism "
            "is frozen at the ceiling and no-shed is vacuous"
        )
    return None


# Two scenario profiles over the same self-spilling fixture:
#
# * quick — a smoke gate: seconds per leg, governor at a 200 ms interval; the
#   discriminator is the SHED COUNT (the wall damage has no time to
#   accumulate on a 4 s run).
# * field — models the 2026-08-13 production regression's SIGNAL SHAPE: the
#   governor only ever sees a counter, so a faithful model is "the counter it
#   listened to rises at the field's cadence, from the export's OWN work, on
#   a source with no foreign load, for a run long enough that shedding costs
#   real wall time". Multi-minute legs at the DEFAULT 1500 ms governor
#   interval reproduce not just the sheds but the field's wall-clock damage
#   (~2.4x on the buggy binary), so the A/B ratio becomes the verdict.
PROFILES = {
    "quick": dict(rows=60_000, payload=1024, chunk_size=3_000, batch=250, interval_ms=200),
    "field": dict(rows=600_000, payload=1024, chunk_size=20_000, batch=1_000, interval_ms=1_500),
}


def mysql_root(sql: str) -> str:
    """Run SQL as root inside the dev MySQL container, return raw stdout."""
    out = subprocess.run(
        ["docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-privet", "rivet", "-N", "-e", sql],
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


def seed(rows: int, payload: int) -> None:
    print(f"== seeding {TABLE} ({rows} rows, payload {payload}B)")
    mysql_root(f"DROP TABLE IF EXISTS {TABLE}; CREATE TABLE {TABLE} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB;")
    step = 5_000
    for start in range(1, rows, step):
        mysql_root(
            f"SET SESSION cte_max_recursion_depth={step + 1000}; "
            f"INSERT INTO {TABLE} SELECT seq, REPEAT('x',{payload}) FROM "
            f"(WITH RECURSIVE s(seq) AS (SELECT {start} UNION ALL SELECT seq+1 FROM s WHERE seq < {start + step - 1}) "
            f"SELECT seq FROM s) t;"
        )


class TmpTableGlobals:
    """Force implicit tmp tables to spill to disk; restore priors on exit.

    MySQL 8's TempTable engine keeps tmp tables in a shared 1 GB RAM pool, so a
    40 MB DISTINCT materialization never reaches disk and the fixture goes
    inert without this flip. Same flip-and-restore shape as the live test's
    guard (tests/live/live_governor.rs).
    """

    def __enter__(self) -> "TmpTableGlobals":
        self.engine = mysql_root("SELECT @@internal_tmp_mem_storage_engine;")
        self.tmp = mysql_root("SELECT @@tmp_table_size;")
        self.heap = mysql_root("SELECT @@max_heap_table_size;")
        mysql_root(
            "SET GLOBAL internal_tmp_mem_storage_engine=MEMORY; "
            "SET GLOBAL tmp_table_size=16384; SET GLOBAL max_heap_table_size=16384;"
        )
        return self

    def __exit__(self, *_exc: object) -> None:
        mysql_root(
            f"SET GLOBAL internal_tmp_mem_storage_engine={self.engine}; "
            f"SET GLOBAL tmp_table_size={self.tmp}; SET GLOBAL max_heap_table_size={self.heap}; "
            f"DROP TABLE IF EXISTS {TABLE};"
        )


def run_once(binary: Path, adaptive: bool, workdir: Path, p: dict) -> tuple[float, int, str]:
    """One export run; returns (wall_seconds, shed_count, stderr).

    stderr is returned, not just counted, because the shed count alone cannot
    tell a healthy idle governor from one that never armed or never sampled —
    see `governor_evidence_failure`.
    """
    workdir.mkdir(parents=True)
    cfg = workdir / "cfg.yaml"
    cfg.write_text(
        f"""\
source:
  type: mysql
  url: "{MYSQL_URL}"
  tuning:
    adaptive: {str(adaptive).lower()}
    min_parallel: 1
    batch_size: {p['batch']}
exports:
  - name: {TABLE}
    query: "SELECT DISTINCT id, payload FROM {TABLE}"
    mode: chunked
    chunk_column: id
    chunk_size: {p['chunk_size']}
    parallel: 4
    format: parquet
    destination: {{ type: local, path: {workdir / 'out'} }}
"""
    )
    t0 = time.monotonic()
    proc = subprocess.run(
        [str(binary), "run", "-c", str(cfg)],
        capture_output=True,
        text=True,
        env={
            "RUST_LOG": "info",
            "RIVET_GOVERNOR_INTERVAL_MS": str(p["interval_ms"]),
            "PATH": "/usr/bin:/bin",
        },
    )
    wall = time.monotonic() - t0
    if proc.returncode != 0:
        sys.exit(f"rivet run failed (adaptive={adaptive}, {binary}):\n{proc.stderr[-2000:]}")
    sheds = proc.stderr.count("backed off")
    return wall, sheds, proc.stderr


def grade(label: str, wall_off: float, wall_on: float, sheds: int, stderr_on: str) -> bool:
    """Grade one binary's A/B pair. Shared by the synthetic and replay paths so
    the two cannot drift into two definitions of PASS.

    An ungraded run (governor never armed / deaf) is NOT a pass: it is an
    aborted measurement, and the stand says so on stdout and returns False, so
    `main`'s exit code cannot certify a candidate against a run that never
    exercised the mechanism.
    """
    ratio = wall_on / wall_off
    why = governor_evidence_failure(stderr_on)
    if why is not None:
        print(f"{label:<10} off={wall_off:.2f}s on={wall_on:.2f}s ratio={ratio:.2f} sheds={sheds} -> UNGRADED")
        print(f"           ^ {why}")
        return False
    ok = sheds == 0 and wall_on <= wall_off * 1.6 + 1.0
    verdict_s = "PASS" if ok else "FAIL (self-throttle)"
    print(f"{label:<10} off={wall_off:.2f}s on={wall_on:.2f}s ratio={ratio:.2f} sheds={sheds} -> {verdict_s}")
    return ok


def verdict(binary: Path, label: str, tmp: Path, p: dict) -> bool:
    version = subprocess.run([str(binary), "--version"], capture_output=True, text=True).stdout.strip()
    print(f"== {label}: {version} ({binary})")
    wall_off, _, _ = run_once(binary, False, tmp / f"{label}_off", p)
    wall_on, sheds, stderr_on = run_once(binary, True, tmp / f"{label}_on", p)
    return grade(label, wall_off, wall_on, sheds, stderr_on)


def load_replay_config(config: Path, export: str) -> tuple[dict, dict]:
    """Parse the operator's config and extract (source, the chosen export).

    Needs PyYAML — the replay mode operates on real production configs
    (hundreds of KB, comments, every knob), and hand-rolled parsing there
    would be a second bug. The synthetic profiles need no extra dependency.
    """
    try:
        import yaml
    except ModuleNotFoundError:
        sys.exit("the --config replay mode needs PyYAML: pip install pyyaml (or run inside a venv)")
    doc = yaml.safe_load(config.read_text())
    exports = [e for e in doc.get("exports", []) if e.get("name") == export]
    if not exports:
        names = ", ".join(e.get("name", "?") for e in doc.get("exports", []))[:400]
        sys.exit(f"export '{export}' not found in {config}; have: {names}")
    return doc["source"], exports[0]


def replay_parallelism(export: dict) -> int:
    """The export's effective `parallel`, defaulting the way rivet does.

    `ExportConfig::parallel` is `#[serde(default = "default_parallel")]` = 1
    (src/config/export.rs) with no other source, so a missing/1 value means the
    governor can never arm — fail fast rather than burn two full legs on a run
    that would grade nothing.
    """
    try:
        return int(export.get("parallel", 1))
    except (TypeError, ValueError):
        return 1


def run_replay_once(
    binary: Path, source: dict, export: dict, adaptive: bool, workdir: Path, interval_ms: int
) -> tuple[float, int, str]:
    """One leg of the config replay: the operator's export, adaptivity forced,
    destination redirected to a local temp dir (never their real bucket), a
    fresh state DB per leg (no history skew), `load:`/notifications stripped.
    Read-only against the source — rivet only SELECTs."""
    import yaml

    workdir.mkdir(parents=True)
    source = dict(source)
    tuning = dict(source.get("tuning") or {})
    tuning["adaptive"] = adaptive
    source["tuning"] = tuning
    export = dict(export)
    export.pop("load", None)
    if isinstance(export.get("tuning"), dict) and "adaptive" in export["tuning"]:
        export["tuning"] = {**export["tuning"], "adaptive": adaptive}
    export["destination"] = {"type": "local", "path": str(workdir / "out")}
    cfg = workdir / "cfg.yaml"
    cfg.write_text(yaml.safe_dump({"source": source, "exports": [export]}, sort_keys=False))
    t0 = time.monotonic()
    proc = subprocess.run(
        [str(binary), "run", "-c", str(cfg)],
        capture_output=True,
        text=True,
        # The operator's environment is INHERITED here, unlike the synthetic
        # path (which writes an inline URL and needs nothing from it): the
        # recommended credential shape is `source.url_env: PROD_DB_URL`, which
        # is copied verbatim into the replay config, so a three-key sanitized
        # env would guarantee the variable is unset and every leg would fail to
        # resolve. Determinism is served by OVERRIDING the two keys that shape
        # the measurement, not by dropping the operator's credentials.
        env={**os.environ, "RUST_LOG": "info", "RIVET_GOVERNOR_INTERVAL_MS": str(interval_ms)},
    )
    wall = time.monotonic() - t0
    if proc.returncode != 0:
        sys.exit(f"rivet run failed (adaptive={adaptive}, {binary}):\n{proc.stderr[-2000:]}")
    return wall, proc.stderr.count("backed off"), proc.stderr


def replay_verdict(
    binary: Path, label: str, tmp: Path, source: dict, export: dict, interval_ms: int
) -> bool:
    version = subprocess.run([str(binary), "--version"], capture_output=True, text=True).stdout.strip()
    print(f"== {label}: {version} ({binary})")
    wall_off, _, _ = run_replay_once(binary, source, export, False, tmp / f"{label}_off", interval_ms)
    wall_on, sheds, stderr_on = run_replay_once(
        binary, source, export, True, tmp / f"{label}_on", interval_ms
    )
    return grade(label, wall_off, wall_on, sheds, stderr_on)


def self_test() -> int:
    """Grade the STAND, not rivet: every state in which a run must not be a PASS.

    The bug this pins (bughunt 2026-08-13): `sheds == 0 and ratio <= 1.6` is
    satisfied by a run in which the governor never armed or never sampled, so
    the stand certified a candidate binary against a measurement of nothing.
    Six cases, not one — the rule folds over three independent stderr markers
    plus the two numeric gates, and a single case cannot tell them apart.

    The stderr fragments are copied from the product's own format strings
    (src/pipeline/governor.rs); the live canaries in tests/live/live_governor.rs
    assert the same three markers against a REAL run, which is what keeps this
    file's copies honest.
    """
    armed = "INFO export 'big': adaptive concurrency governor active (parallel 1..4)\n"
    cases: list[tuple[str, float, float, int, str, bool]] = [
        ("healthy idle governor", 10.0, 10.5, 0, armed, True),
        ("healthy governor that shed", 10.0, 10.5, 3, armed, False),
        ("healthy governor, wall blown", 10.0, 30.0, 0, armed, False),
        ("never armed (parallel: 1 / mode: full)", 10.0, 10.5, 0, "INFO export done\n", False),
        (
            "monitor connection failed",
            10.0,
            10.5,
            0,
            armed + "WARN export 'big': governor monitoring connection failed; parallelism stays static at 4\n",
            False,
        ),
        (
            "armed but deaf",
            10.0,
            10.5,
            0,
            armed + "WARN export 'big': governor armed, but the source provides no pressure signal\n",
            False,
        ),
    ]
    failures = 0
    for name, off, on, sheds, stderr, expect in cases:
        got = grade(f"[{name}]"[:10], off, on, sheds, stderr)
        if got != expect:
            print(f"   !! self-test FAILED: {name}: expected ok={expect}, got ok={got}")
            failures += 1

    # The replay path's fail-fast decision: `parallel` defaults to 1 in rivet
    # (src/config/export.rs), and the governor arms only above it — so the
    # absent and explicit-1 shapes must both be refused BEFORE two full legs
    # run. Three shapes, because "absent" and "present but 1" are different
    # dict lookups and only one of them exercises the default.
    par_cases = [({"name": "e"}, 1), ({"name": "e", "parallel": 1}, 1), ({"name": "e", "parallel": 4}, 4)]
    for export, expect_par in par_cases:
        got_par = replay_parallelism(export)
        refused = got_par <= 1
        print(f"[parallel] {export.get('parallel', '<absent>')} -> {got_par} ({'refused' if refused else 'replayed'})")
        if got_par != expect_par:
            print(f"   !! self-test FAILED: parallel {export}: expected {expect_par}, got {got_par}")
            failures += 1

    total = len(cases) + len(par_cases)
    print(f"== self-test: {total - failures}/{total} cases passed")
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # Not `required=True`: `--self-test` grades the stand itself and needs no
    # binaries. Enforced below for every path that actually runs rivet.
    ap.add_argument("--rivet-a", type=Path, help="baseline rivet binary (e.g. the released one)")
    ap.add_argument("--rivet-b", type=Path, help="candidate rivet binary (e.g. target/release/rivet)")
    ap.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default="quick",
        help="quick = seconds-per-leg shed gate; field = multi-minute legs at the default "
        "1500ms governor interval, reproducing the 2026-08-13 wall-clock damage",
    )
    ap.add_argument(
        "--config",
        type=Path,
        help="REPLAY MODE: A/B the governor on YOUR real config against YOUR real source "
        "(read-only; destination is redirected to a local temp dir, load:/notifications "
        "stripped, fresh state per leg). Requires --export; ignores --profile's fixture.",
    )
    ap.add_argument("--export", help="export name from --config to replay (pick the shed giant)")
    ap.add_argument(
        "--interval-ms",
        type=int,
        default=1_500,
        help="governor sample interval for the replay legs (default: the production 1500)",
    )
    ap.add_argument(
        "--self-test",
        action="store_true",
        help="check the stand's own grading rule (no MySQL, no rivet) and exit",
    )
    args = ap.parse_args()

    if args.self_test:
        return self_test()
    if not args.rivet_a or not args.rivet_b:
        sys.exit("--rivet-a and --rivet-b are required (except with --self-test)")

    if args.config:
        if not args.export:
            sys.exit("--config replay needs --export <name>")
        source, export = load_replay_config(args.config, args.export)
        par = replay_parallelism(export)
        if par <= 1:
            sys.exit(
                f"export '{args.export}' has parallel: {par} — the governor arms only at "
                "parallel > 1, so both legs would grade nothing. Pick the shed giant "
                "(an export with parallel: 2+), or add `parallel:` to this one."
            )
        print(f"== replaying export '{args.export}' from {args.config} (destination → local tmp)")
        with tempfile.TemporaryDirectory(prefix="rivet-gov-replay-") as tmp:
            a_ok = replay_verdict(
                args.rivet_a.expanduser(), "A", Path(tmp), source, export, args.interval_ms
            )
            b_ok = replay_verdict(
                args.rivet_b.expanduser(), "B", Path(tmp), source, export, args.interval_ms
            )
        return 0 if b_ok else 1

    p = PROFILES[args.profile]
    seed(p["rows"], p["payload"])
    print(f"== forcing tmp-table spills (globals flipped; restored on exit) — profile '{args.profile}'")
    with TmpTableGlobals(), tempfile.TemporaryDirectory(prefix="rivet-gov-ab-") as tmp:
        a_ok = verdict(args.rivet_a.expanduser(), "A", Path(tmp), p)
        b_ok = verdict(args.rivet_b.expanduser(), "B", Path(tmp), p)
    # The stand's exit code grades the CANDIDATE (B) only — A is often the
    # known-bad baseline whose FAIL is the demonstration, not a problem.
    return 0 if b_ok else 1


if __name__ == "__main__":
    sys.exit(main())
