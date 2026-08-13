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

    python3 -m dev.pytools.governor_ab --rivet-a ~/.local/bin/rivet \
                                       --rivet-b target/release/rivet
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path

TABLE = "gov_stand_ab"
ROWS = 60_000
MYSQL_CONTAINER = "rivet-mysql-1"
MYSQL_URL = "mysql://rivet:rivet@127.0.0.1:3306/rivet"


def mysql_root(sql: str) -> str:
    """Run SQL as root inside the dev MySQL container, return raw stdout."""
    out = subprocess.run(
        ["docker", "exec", "-i", MYSQL_CONTAINER, "mysql", "-uroot", "-privet", "rivet", "-N", "-e", sql],
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


def seed() -> None:
    print(f"== seeding {TABLE} ({ROWS} rows, wide payload)")
    mysql_root(f"DROP TABLE IF EXISTS {TABLE}; CREATE TABLE {TABLE} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB;")
    step = 5_000
    for start in range(1, ROWS, step):
        mysql_root(
            f"SET SESSION cte_max_recursion_depth={step + 1000}; "
            f"INSERT INTO {TABLE} SELECT seq, REPEAT('x',1024) FROM "
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


def run_once(binary: Path, adaptive: bool, workdir: Path) -> tuple[float, int]:
    """One export run; returns (wall_seconds, shed_count)."""
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
    batch_size: 250
exports:
  - name: {TABLE}
    query: "SELECT DISTINCT id, payload FROM {TABLE}"
    mode: chunked
    chunk_column: id
    chunk_size: 3000
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
        env={"RUST_LOG": "info", "RIVET_GOVERNOR_INTERVAL_MS": "200", "PATH": "/usr/bin:/bin"},
    )
    wall = time.monotonic() - t0
    if proc.returncode != 0:
        sys.exit(f"rivet run failed (adaptive={adaptive}, {binary}):\n{proc.stderr[-2000:]}")
    sheds = proc.stderr.count("backed off")
    return wall, sheds


def verdict(binary: Path, label: str, tmp: Path) -> bool:
    version = subprocess.run([str(binary), "--version"], capture_output=True, text=True).stdout.strip()
    print(f"== {label}: {version} ({binary})")
    wall_off, _ = run_once(binary, False, tmp / f"{label}_off")
    wall_on, sheds = run_once(binary, True, tmp / f"{label}_on")
    ratio = wall_on / wall_off
    ok = sheds == 0 and wall_on <= wall_off * 1.6 + 1.0
    verdict_s = "PASS" if ok else "FAIL (self-throttle)"
    print(f"{label:<10} off={wall_off:.2f}s on={wall_on:.2f}s ratio={ratio:.2f} sheds={sheds} -> {verdict_s}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rivet-a", required=True, type=Path, help="baseline rivet binary (e.g. the released one)")
    ap.add_argument("--rivet-b", required=True, type=Path, help="candidate rivet binary (e.g. target/release/rivet)")
    args = ap.parse_args()

    seed()
    print("== forcing tmp-table spills (globals flipped; restored on exit)")
    with TmpTableGlobals(), tempfile.TemporaryDirectory(prefix="rivet-gov-ab-") as tmp:
        a_ok = verdict(args.rivet_a.expanduser(), "A", Path(tmp))
        b_ok = verdict(args.rivet_b.expanduser(), "B", Path(tmp))
    # The stand's exit code grades the CANDIDATE (B) only — A is often the
    # known-bad baseline whose FAIL is the demonstration, not a problem.
    return 0 if b_ok else 1


if __name__ == "__main__":
    sys.exit(main())
