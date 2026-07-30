#!/usr/bin/env python3
"""Port of the legacy compatibility matrices and the version-matrix dev stand.

Four former scripts live here because they are one family — "run rivet against a
deliberately OLD engine build" and "bring up / cross-check the stand those
engines run on":

    python3 -m dev.pytools.legacy_stand legacy       [TARGET ...]  # dev/legacy/run_legacy.sh
    python3 -m dev.pytools.legacy_stand full-matrix  [TARGET ...]  # dev/legacy/run_full_matrix.sh
    python3 -m dev.pytools.legacy_stand stand-up     [batch|cdc]   # dev/stand/up.sh
    python3 -m dev.pytools.legacy_stand stand-verify               # dev/stand/verify.sh

`TARGET ...` may also come from `$TARGETS` (whitespace-separated), as in the
shell. Every pinned version, port, URL, credential and container name is
reproduced VERBATIM: the pins ARE the compatibility claim, so a silently-updated
tag would mean the claim is no longer being tested. The pins, for the record:

    legacy (canonical compose, `--profile legacy`)
        pg-12 → 5412   pg-13 → 5413   pg-14 → 5414   pg-15 → 5415
        pg-16 → 5432 (primary)         mysql-57 → 3357   mysql-80 → 3306 (primary)
    stand (its own compose project, `dev/stand`)
        pg 14/18 → 5514/5518 (batch) 5614/5618 (cdc)
        mysql 5.7/8.0 → 3557/3580 (batch) 3657/3680 (cdc)
        mssql 2019/2022 → 1519/1522 (batch) 1619/1622 (cdc)
        mongo 4.4/5.0/8.0 → 27104/27105/27108 (batch) 27204/27205/27208 (cdc)

Messages, ordering and exit codes match the originals. Progress goes to stderr
(the pytools convention) so stdout stays parseable; the strings themselves are
byte-identical to the shell's, which is what a transcript diff compares.

Deviations, each marked `DEVIATION:` at its site — every one is a place where the
shell reported success (or a nonsense verdict) for a run that had not happened:

1. **`up.sh`'s health gate passed while containers were UNHEALTHY.** Both halves
   are wrong for the same reason: `grep -cEv 'healthy$'` counts lines NOT ending
   in "healthy" — and "un**healthy**" ends in "healthy", so an unhealthy
   container satisfied `[ "$unhealthy" = "0" ]` and broke the wait loop
   immediately; the confirming gate `ps --format '{{.Health}}' | grep -qv
   healthy` then found no line lacking the substring "healthy" and let the seed
   run against a broken stand. The same shape (`grep -c healthy`) already cost us
   `dev/cdc/stand.sh`, where four unhealthy containers satisfied a `-ge 4` gate.
   The port compares the Health field for EXACT equality with `healthy`.
2. **`up.sh`'s gate was also vacuous when NOTHING was up.** `unhealthy=$(docker
   compose ps … | grep -c … || true)`: with the daemon down or no containers
   created, `ps` printed nothing, `grep -c` printed `0`, `|| true` swallowed the
   pipefail status, and the gate's `grep -q` found no offending line — so a stand
   that did not exist reported healthy. The port enumerates the expected services
   (`compose config --services`, which DOES respect `--profile`) and requires
   every one of them to be present, `running` and exactly `healthy`; the `ps`
   command's own exit status is checked.
3. **`up.sh` waited on the WRONG TIER.** `docker compose ps` ignores `--profile`
   when LISTING — verified on the live stand: `--profile batch ps` reports all
   **18** containers while `--profile batch config --services` reports the
   correct **9** — so `./up.sh batch` gated on the CDC tier's health as well as
   its own, and a single broken cdc container could hold a batch-only bring-up in
   the 15-minute wait and then fail it. The port gates only on the selected
   profiles' services and names the ones at fault instead of "some services".
4. **`run_full_matrix.sh` reported success on an unknown target.** The `case`'s
   `*)` arm echoed a SKIP but touched neither `SUMMARY` nor `OVERALL_EXIT`.
   Verified on bash 3.2.57: `TARGETS="pg-12 pg-99"` prints "All targets passed."
   and exits **0** with `pg-99` absent from the summary entirely; and
   `TARGETS="pg-99"` (nothing else to fill the array) instead dies at
   `"${SUMMARY[@]}": unbound variable` under `set -u` — a different wrong answer
   per input, exactly as `matrices.py` documents for its own `case`. An unknown
   target is now a FAIL, matching its sibling `run_legacy.sh`.
5. **`run_full_matrix.sh`'s inner `case "$seed_kind"` had no default arm** — an
   unrecognised kind seeded NOTHING and printed "seed: ok" (bug class 2). Now
   fatal.
6. **A missing/unparseable `PASS:` summary line counted as a PASS.** The success
   branch had no `${pass:-?}` fallback, so an e2e log without the summary line
   produced `RESULT:  passed,  skipped` and a `PASS` row. The port treats an
   absent or unparseable summary as a FAIL (a log that does not say it passed is
   not evidence that it did), and also fails when the parsed `FAIL:` count is
   non-zero even if the child exited 0.
7. **A missing build read as a compatibility failure.** An absent binary is exit
   127, and the shell scored that as data: no `seed` binary gave
   `FAIL <target>: seed` once per target (five verdicts blaming five Postgres
   releases for an un-built tree), and an absent `rivet` with `seed` present gave
   all eight per-target cells — `doctor`, `check`, three exports, the re-run,
   `time_window`, `init` — as engine failures. Both matrices now check the
   binaries up front and name the build command.
8. **Client stderr no longer vanishes.** `>/dev/null 2>&1` on every seed and every
   `rivet` call meant "FAIL: seed" with no reason; `verify.sh` went further and
   folded stderr INTO the value (`2>&1 | head -1`), so a psql error message
   became a "tuple" — eligible to be the group's REFERENCE and, if two engines
   failed identically, to be reported as "OK — all identical". The port takes
   values from stdout only, validates the SHAPE (six pipe-separated integers;
   two for Mongo) and reports an unparseable cell as a failure with the client's
   stderr attached.
9. **Teardown.** `run_legacy.sh` cleaned up only on the way IN (`rm -f` at the
   top), so every run left `dev/legacy/.init_<target>.yaml` and
   `dev/legacy/.rivet_state.db*` behind — five `.init_*.yaml` files were sitting
   in the tree when this port was written, and `.gitignore` carries entries for
   both (`/dev/legacy/.init_*.yaml`, `.rivet_state.db*`), which is the tell that
   the leak was accepted rather than fixed. They are removed in a `finally` now.
   For the record, none of these four scripts creates a PG replication slot or a
   table — the stand's slots come from the CDC test suites, not from here — so
   there is nothing else to release.

Deviations that are diagnostic-only, not verdict-changing: the results artifacts
(`/tmp/rivet_legacy_results.txt`, `/tmp/rivet_full_matrix/*.log`) are written
whole rather than streamed line-by-line, so an interrupted run leaves no
half-written file for the next reader to trust; and `run_full_matrix.sh`'s
`sed`-derived port (which, on a URL it could not match, passed the ENTIRE URL to
the probe and reported "unreachable") is parsed properly and fails loudly.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Sequence
from urllib.parse import urlsplit

try:  # `python3 -m dev.pytools.legacy_stand`
    from . import shell
except ImportError:  # `python3 dev/pytools/legacy_stand.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail
Proc = shell.Proc
compose, docker_exec, run, tcp_open = shell.compose, shell.docker_exec, shell.run, shell.tcp_open

LEGACY_DIR = ROOT / "dev" / "legacy"
STAND_DIR = ROOT / "dev" / "stand"

USAGE = """Usage: python3 -m dev.pytools.legacy_stand <command> [ARGS]

Commands:
  legacy       [TARGET ...]     dev/legacy/run_legacy.sh — compat smoke matrix
                                (default: pg-12 pg-13 pg-14 pg-15 mysql-57)
  full-matrix  [TARGET ...]     dev/legacy/run_full_matrix.sh — full e2e per version
                                (default: pg-12 pg-13 pg-14 pg-15 pg-16 mysql-57 mysql-80)
  stand-up     [batch|cdc|all]  dev/stand/up.sh — bring up + post-seed the stand
  stand-verify                  dev/stand/verify.sh — cross-engine seed parity

TARGET may also be supplied via $TARGETS (whitespace-separated).
Binaries: $RIVET / $SEED (default target/release/{rivet,seed})."""


# ── plain output ───────────────────────────────────────────────────────────────
# These scripts' transcripts are their contract (the shell `tee`d them into a
# results file that a later `grep` read), so the strings are emitted verbatim
# rather than through shell.ok/bad, which would rewrite them as "  ✓ …".
def say(msg: str = "") -> None:
    print(msg, file=sys.stderr, flush=True)


# ══════════════════════════════════════════════════════════════════════════════
# shared: binaries and seeding
# ══════════════════════════════════════════════════════════════════════════════
# The fixture size is part of the matrix contract: every target is seeded with
# the SAME small fixture so file counts and incremental cursors are comparable
# across engine versions.
SEED_SHAPE: tuple[str, ...] = (
    "--users", "500",
    "--orders-per-user", "4",
    "--events-per-user", "10",
    "--page-views", "500",
    "--content-items", "100",
)


def _binary(env: str, default: Path) -> Path:
    """The rivet/seed binary, honouring `$RIVET` / `$SEED` exactly as the shell's
    `${RIVET:-$(pwd)/target/release/rivet}` did.

    DEVIATION 7: absence is a precondition failure, not a per-cell verdict. The
    shell ran `$RIVET doctor` regardless; with no binary that is exit 127, scored
    as `FAIL <target>: doctor` for every target — an un-built tree presented as a
    legacy-Postgres incompatibility.
    """
    path = Path(os.environ.get(env) or default)
    if not path.is_file() or not os.access(path, os.X_OK):
        raise Fail(
            f"{env.lower()} binary not executable: {path}",
            hint="cargo build --release --bin rivet --bin seed",
        )
    return path


def _seed_argv(seed: Path, kind: str, url: str) -> list[str]:
    """DEVIATION 5: an unknown kind is fatal. `run_full_matrix.sh`'s
    `case "$seed_kind"` had arms for `pg` and `mysql` and NO default, so any
    other value skipped seeding entirely and the script then printed
    "seed: ok" and ran the whole e2e suite against whatever was in the DB."""
    if kind in ("pg", "postgres"):
        return [str(seed), "--target", "postgres", "--pg-url", url, *SEED_SHAPE]
    if kind == "mysql":
        return [str(seed), "--target", "mysql", "--mysql-url", url, *SEED_SHAPE]
    raise Fail(f"unknown seed kind '{kind}' (expected pg|mysql)", code=2)


def _tail(p: Proc, *, limit: int = 200) -> str:
    """The last meaningful line of a failed command, for the message the shell
    threw away (DEVIATION 8)."""
    lines = [ln.strip() for ln in (p.stderr or p.stdout).splitlines() if ln.strip()]
    return (lines[-1][:limit] if lines else f"exit {p.returncode}")


def _targets(argv: Sequence[str], default: Sequence[str]) -> list[str]:
    """Positional arguments win, then `$TARGETS`, then the pinned default —
    the shell's `TARGETS="${TARGETS:-$TARGETS_DEFAULT}"` plus a way to say it on
    the command line."""
    if argv:
        return list(argv)
    env = os.environ.get("TARGETS", "").split()
    return env if env else list(default)


# ══════════════════════════════════════════════════════════════════════════════
# dev/legacy/run_legacy.sh
# ══════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class LegacyTarget:
    name: str
    port: int
    url: str
    kind: str  # postgres | mysql


# Pinned ports — the whole point of the matrix. macOS bash 3.2 has no
# associative arrays, which is why the original used a `case`; the mapping is
# identical.
LEGACY_TARGETS: dict[str, LegacyTarget] = {
    "pg-12": LegacyTarget("pg-12", 5412, "postgresql://rivet:rivet@localhost:5412/rivet", "postgres"),
    "pg-13": LegacyTarget("pg-13", 5413, "postgresql://rivet:rivet@localhost:5413/rivet", "postgres"),
    "pg-14": LegacyTarget("pg-14", 5414, "postgresql://rivet:rivet@localhost:5414/rivet", "postgres"),
    "pg-15": LegacyTarget("pg-15", 5415, "postgresql://rivet:rivet@localhost:5415/rivet", "postgres"),
    "mysql-57": LegacyTarget("mysql-57", 3357, "mysql://rivet:rivet@localhost:3357/rivet", "mysql"),
}
LEGACY_DEFAULT: tuple[str, ...] = ("pg-12", "pg-13", "pg-14", "pg-15", "mysql-57")
LEGACY_EXPECTED = "pg-12|pg-13|pg-14|pg-15|mysql-57"

LEGACY_RESULTS = Path("/tmp/rivet_legacy_results.txt")


@dataclass
class Ledger:
    """PASS/FAIL/SKIP counters plus the transcript the shell kept in
    `$RESULTS` — the final "FAILURES:" block re-read that file with `grep FAIL`,
    so the lines are recorded verbatim."""

    lines: list[str] = field(default_factory=list)
    passed: int = 0
    failed: int = 0
    skipped: int = 0

    def _emit(self, verdict: str, msg: str) -> None:
        line = f"  {verdict}  {msg}"
        self.lines.append(line)
        say(line)

    def pass_(self, msg: str) -> None:
        self._emit("PASS", msg)
        self.passed += 1

    def fail(self, msg: str) -> None:
        self._emit("FAIL", msg)
        self.failed += 1

    def skip(self, msg: str) -> None:
        self._emit("SKIP", msg)
        self.skipped += 1

    def verdict(self, msg: str, p: Proc) -> bool:
        """`cmd && pass "msg" || fail "msg"`, but with the reason attached when it
        failed (DEVIATION 8)."""
        if p.ok:
            self.pass_(msg)
            return True
        self.fail(f"{msg} ({_tail(p)})")
        return False


def _section(title: str) -> None:
    say("")
    say(f"▒▒▒▒▒▒ {title} ▒▒▒▒▒▒")


def _legacy_state_files() -> list[Path]:
    return sorted(LEGACY_DIR.glob(".rivet_state.db*"))


def _legacy_cleanup(targets: Sequence[str]) -> None:
    """DEVIATION 9: the shell removed the state DB and the `init` scratch YAML
    only on the way IN, so every run left `dev/legacy/.init_<target>.yaml` (five
    of them are in the working tree) and a stale `.rivet_state.db` behind."""
    for name in targets:
        shell.rm_rf(LEGACY_DIR / f".init_{name}.yaml")
    for p in _legacy_state_files():
        shell.rm_rf(p)


def legacy_matrix(target_names: Sequence[str]) -> int:
    """`dev/legacy/run_legacy.sh`: doctor → check → full → chunked → incremental
    (×2) → time_window (PG only) → init, per legacy server."""
    rivet = _binary("RIVET", ROOT / "target" / "release" / "rivet")
    seed = _binary("SEED", ROOT / "target" / "release" / "seed")

    out_dir = LEGACY_DIR / "output"
    led = Ledger()

    shell.rm_rf(LEGACY_RESULTS)  # `rm -f "$RESULTS"`: never mix two runs' verdicts
    shell.rm_rf(out_dir)
    for p in _legacy_state_files():
        shell.rm_rf(p)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        for name in target_names:
            target = LEGACY_TARGETS.get(name)
            if target is None:
                _section(name)
                led.fail(f"unknown target '{name}' (expected {LEGACY_EXPECTED})")
                continue

            _section(f"{target.name} @ localhost:{target.port}")

            # Reachability probe — a TCP connect, not a client, so the matrix does
            # not depend on a `mysqladmin` that macOS ≥ 9.0 cannot authenticate.
            if not tcp_open("127.0.0.1", target.port):
                led.skip(
                    f"{target.name}: port {target.port} unreachable "
                    f"(is `docker compose --profile legacy up -d {target.name}` running?)"
                )
                continue

            if target.kind == "postgres":
                cfg = LEGACY_DIR / "pg_legacy.yaml"
                seed_kind = "pg"
            else:
                cfg = LEGACY_DIR / "mysql_legacy.yaml"
                seed_kind = "mysql"

            # One fresh seed per target so incremental cursors start clean.
            if not led.verdict(
                f"{target.name}: seed",
                run(_seed_argv(seed, seed_kind, target.url), timeout=None),
            ):
                continue

            # The state DB lives beside the CONFIG (`config_dir/.rivet_state.db`),
            # so it is shared by every target and must be reset between them or
            # the second target resumes the first one's cursor.
            for p in _legacy_state_files():
                shell.rm_rf(p)

            env = {"DATABASE_URL": target.url}

            def rivet_run(*args: str) -> Proc:
                return run([str(rivet), *args], env=env, cwd=ROOT, timeout=None)

            led.verdict(f"{target.name}: doctor", rivet_run("doctor", "--config", str(cfg)))
            led.verdict(f"{target.name}: check", rivet_run("check", "--config", str(cfg)))

            led.verdict(
                f"{target.name}: users full (validate+reconcile)",
                rivet_run("run", "--config", str(cfg), "--export", "legacy_users_full",
                          "--validate", "--reconcile"),
            )
            led.verdict(
                f"{target.name}: orders chunked (validate+reconcile)",
                rivet_run("run", "--config", str(cfg), "--export", "legacy_orders_chunked",
                          "--validate", "--reconcile"),
            )
            led.verdict(
                f"{target.name}: orders incremental run1",
                rivet_run("run", "--config", str(cfg), "--export", "legacy_orders_incremental",
                          "--validate"),
            )
            # Re-run incremental — should succeed with 0 new rows.
            led.verdict(
                f"{target.name}: orders incremental rerun (no new rows)",
                rivet_run("run", "--config", str(cfg), "--export", "legacy_orders_incremental"),
            )

            if target.kind == "postgres":
                led.verdict(
                    f"{target.name}: events time_window",
                    rivet_run("run", "--config", str(cfg), "--export", "legacy_events_timewindow"),
                )

            # init should be able to introspect the legacy schema and emit a valid YAML.
            scratch = LEGACY_DIR / f".init_{target.name}.yaml"
            shell.rm_rf(scratch)
            init = rivet_run("init", "--source", target.url, "--table", "users", "-o", str(scratch))
            emitted = init.ok and scratch.is_file() and "exports:" in scratch.read_text()
            if emitted:
                led.pass_(f"{target.name}: init --table users")
            else:
                reason = _tail(init) if not init.ok else "no `exports:` in the emitted YAML"
                led.fail(f"{target.name}: init --table users ({reason})")
    finally:
        _legacy_cleanup(target_names)
        shell.atomic_write(LEGACY_RESULTS, "".join(f"{ln}\n" for ln in led.lines))

    say("")
    say("══════ Summary ══════")
    say(f"PASS: {led.passed} | FAIL: {led.failed} | SKIP: {led.skipped}")
    say(f"Total: {led.passed + led.failed + led.skipped}")

    if led.failed > 0:
        say("")
        say("FAILURES:")
        for line in led.lines:
            if "FAIL" in line:
                say(line)
        return 1

    say("All legacy compatibility checks passed.")
    return 0


# ══════════════════════════════════════════════════════════════════════════════
# dev/legacy/run_full_matrix.sh
# ══════════════════════════════════════════════════════════════════════════════
PRIMARY_PG_URL = "postgresql://rivet:rivet@localhost:5432/rivet"
PRIMARY_MY_URL = "mysql://rivet:rivet@localhost:3306/rivet"
LEGACY_MY_URL = "mysql://rivet:rivet@localhost:3357/rivet"


@dataclass(frozen=True)
class MatrixTarget:
    name: str
    pg_url: str
    my_url: str
    seed_kind: str  # pg | mysql
    seed_url: str


def _pg(port: int) -> str:
    return f"postgresql://rivet:rivet@localhost:{port}/rivet"


# For a PG target the primary MySQL 8.0 stays on 3306 so the suite's
# MySQL-specific sections still exercise something; for a MySQL target the
# primary PG stays on 5432 so the PG-only sections have a working server and the
# MySQL version is the only variable.
FULL_TARGETS: dict[str, MatrixTarget] = {
    "pg-12": MatrixTarget("pg-12", _pg(5412), PRIMARY_MY_URL, "pg", _pg(5412)),
    "pg-13": MatrixTarget("pg-13", _pg(5413), PRIMARY_MY_URL, "pg", _pg(5413)),
    "pg-14": MatrixTarget("pg-14", _pg(5414), PRIMARY_MY_URL, "pg", _pg(5414)),
    "pg-15": MatrixTarget("pg-15", _pg(5415), PRIMARY_MY_URL, "pg", _pg(5415)),
    "pg-16": MatrixTarget("pg-16", PRIMARY_PG_URL, PRIMARY_MY_URL, "pg", PRIMARY_PG_URL),
    "mysql-57": MatrixTarget("mysql-57", PRIMARY_PG_URL, LEGACY_MY_URL, "mysql", LEGACY_MY_URL),
    "mysql-80": MatrixTarget("mysql-80", PRIMARY_PG_URL, PRIMARY_MY_URL, "mysql", PRIMARY_MY_URL),
}
FULL_DEFAULT: tuple[str, ...] = (
    "pg-12", "pg-13", "pg-14", "pg-15", "pg-16", "mysql-57", "mysql-80",
)
FULL_EXPECTED = "pg-12|pg-13|pg-14|pg-15|pg-16|mysql-57|mysql-80"

FULL_RESULTS_DIR = Path("/tmp/rivet_full_matrix")

_SUMMARY_LINE = re.compile(r"^PASS:")


def _url_port(url: str, *, what: str) -> int:
    """DEVIATION: the original derived the port with
    `sed -E 's|.*@[^:]+:([0-9]+)/.*|\\1|'`, and sed passes a NON-matching line
    through unchanged — so a URL without an explicit port handed the whole URL to
    the TCP probe, which failed, and the target was reported "SKIP (pg
    unreachable)" when the truth was a malformed config."""
    try:
        port = urlsplit(url).port
    except ValueError:
        port = None
    if port is None:
        raise Fail(f"cannot derive a port from the {what} URL: {url}", code=2)
    return port


@dataclass(frozen=True)
class E2ESummary:
    """The `PASS: n | FAIL: n | SKIP: n` line the e2e suite prints last."""

    passed: int
    failed: int
    skipped: int


def parse_e2e_summary(log_text: str) -> E2ESummary | None:
    """Fields 2/5/8 of the summary line, as the original's three `awk` calls read
    them. Returns None when the line is absent or not three integers — which
    DEVIATION 6 treats as a failed run rather than a pass with blank counts."""
    found: E2ESummary | None = None
    for line in log_text.splitlines():
        if not _SUMMARY_LINE.match(line):
            continue
        f = line.split()
        if len(f) < 8:
            continue
        try:
            found = E2ESummary(int(f[1]), int(f[4]), int(f[7]))
        except ValueError:
            continue
    return found


def _run_to_log(argv: Sequence[str], log: Path, env: dict[str, str]) -> int:
    """Run a child with stdout+stderr merged into `log`, i.e. `cmd >log 2>&1`.

    The one place a plain `subprocess` call earns its keep: this log is the
    artifact the summary points a human at, and capturing the two streams
    separately then concatenating them would reorder the transcript. Still an
    argv list, still no shell.
    """
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as fh:
        return subprocess.run(
            [str(a) for a in argv],
            stdout=fh,
            stderr=subprocess.STDOUT,
            env={**os.environ, **env},
            cwd=str(ROOT),
        ).returncode


def full_matrix(target_names: Sequence[str]) -> int:
    """`dev/legacy/run_full_matrix.sh`: seed each version, then run the ENTIRE
    e2e suite against it via `RIVET_PG_URL` / `RIVET_MYSQL_URL`."""
    rivet = _binary("RIVET", ROOT / "target" / "release" / "rivet")
    seed = _binary("SEED", ROOT / "target" / "release" / "seed")
    # The e2e suite is `dev.pytools.e2e`. Its module file is checked rather than
    # trusted to import, because the failure this guards is a missing/renamed
    # suite — and `-m` would report that as a target-level FAIL for every version
    # in the matrix instead of one clear abort before any seeding happens.
    e2e_module = ROOT / "dev" / "pytools" / "e2e.py"
    if not e2e_module.is_file():
        raise Fail(f"e2e suite not found: {e2e_module}")
    e2e_argv = [sys.executable, "-m", "dev.pytools.e2e"]

    shell.rm_rf(FULL_RESULTS_DIR)
    FULL_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    summary: list[str] = []
    overall_exit = 0

    for name in target_names:
        target = FULL_TARGETS.get(name)
        if target is None:
            # DEVIATION 4: the shell echoed this and moved on with SUMMARY and
            # OVERALL_EXIT untouched — so a typo'd target printed an empty
            # summary and "All targets passed."
            say(f"  SKIP: unknown target '{name}' (expected {FULL_EXPECTED})")
            summary.append(f"{name}: FAIL (unknown target)")
            overall_exit = 1
            continue

        log = FULL_RESULTS_DIR / f"{target.name}.log"

        say("")
        say("╔════════════════════════════════════════════════════════════╗")
        say(f"║  TARGET: {target.name}")
        say(f"║    PG  URL → {target.pg_url}")
        say(f"║    MY  URL → {target.my_url}")
        say("╚════════════════════════════════════════════════════════════╝")

        pg_port = _url_port(target.pg_url, what="PG")
        _url_port(target.my_url, what="MySQL")  # validated for the same reason

        if not tcp_open("127.0.0.1", pg_port):
            say(f"  SKIP: PG server on port {pg_port} unreachable")
            summary.append(f"{target.name}: SKIP (pg unreachable)")
            continue
        # MySQL is not hard-required (the e2e suite auto-skips when its own probe
        # fails), but a MySQL-targeted run still has to seed it.

        # Seed the target DB fresh so incremental cursors and file counts are
        # deterministic for each matrix pass.
        seeded = run(_seed_argv(seed, target.seed_kind, target.seed_url), timeout=None)
        if not seeded.ok:
            kind = "pg" if target.seed_kind in ("pg", "postgres") else "mysql"
            say(f"  FAIL: seed ({kind}: {target.seed_url}) — {_tail(seeded)}")
            summary.append(f"{target.name}: FAIL (seed)")
            overall_exit = 1
            continue
        say("  seed: ok")

        # Force the e2e suite to talk to the target URLs.
        env = {
            "RIVET": str(rivet),
            "RIVET_PG_URL": target.pg_url,
            "RIVET_MYSQL_URL": target.my_url,
        }
        # `dev.pytools.e2e` emits the same `PASS: n | FAIL: n | SKIP: n` wire
        # format `parse_e2e_summary` reads, which is what made this swap a change
        # of argv only. `sys.executable`, not a bare "python3": the suite must run
        # under the same interpreter as this module, not whichever python3 leads
        # PATH — the matrix runs from cron and from a venv. (`_run_to_log` already
        # runs from ROOT, which is what lets `-m dev.pytools.e2e` resolve.)
        rc = _run_to_log(e2e_argv, log, env)
        text = log.read_text(errors="replace")
        s = parse_e2e_summary(text)

        # DEVIATION 6: a pass needs BOTH a clean exit AND a summary line that
        # says zero failures. The shell trusted the exit code alone and printed
        # whatever the parse produced, blank included.
        if rc == 0 and s is not None and s.failed == 0:
            say(f"  RESULT: {s.passed} passed, {s.failed} failed, {s.skipped} skipped")
            summary.append(
                f"{target.name}: PASS ({s.passed} passed, {s.skipped} skipped, log: {log})"
            )
            continue

        p = "?" if s is None else str(s.passed)
        f = "?" if s is None else str(s.failed)
        k = "?" if s is None else str(s.skipped)
        say(f"  RESULT: {p} passed, {f} FAILED, {k} skipped — see {log}")
        if s is None:
            say(f"  (no parseable `PASS: n | FAIL: n | SKIP: n` line in the log; exit {rc})")
        say("  FAILURES:")
        for line in text.splitlines():
            if line.startswith("FAIL "):
                say(f"    {line}")
        summary.append(f"{target.name}: FAIL ({p} passed, {f} failed, log: {log})")
        overall_exit = 1

    say("")
    say("══════════════════════════════════════════════════════════════")
    say("Full compatibility matrix summary")
    say("══════════════════════════════════════════════════════════════")
    for line in summary:
        say(f"  {line}")
    say("")

    if overall_exit != 0:
        say("ONE OR MORE TARGETS FAILED.")
        return 1
    say("All targets passed.")
    return 0


# ══════════════════════════════════════════════════════════════════════════════
# dev/stand/up.sh
# ══════════════════════════════════════════════════════════════════════════════
SA_PASS = "Rivet_Passw0rd!"

STAND_PROFILES: dict[str, tuple[str, ...]] = {
    "batch": ("batch",),
    "cdc": ("cdc",),
    "all": ("batch", "cdc"),
}

# (mssql port, seed file) and (mongo container, orders, events, user_mod) —
# pinned exactly as up.sh had them. The batch tier carries the heavy volume-parity
# Mongo seed; the cdc tier ten documents per collection.
STAND_MSSQL_BATCH: tuple[tuple[int, str], ...] = ((1519, "common/mssql.sql"), (1522, "common/mssql.sql"))
STAND_MSSQL_CDC: tuple[tuple[int, str], ...] = ((1619, "cdc/mssql.sql"), (1622, "cdc/mssql.sql"))
STAND_MONGO_BATCH: tuple[tuple[str, int, int, int], ...] = (
    ("stand-mongo44-batch-1", 200000, 500000, 50000),
    ("stand-mongo50-batch-1", 200000, 500000, 50000),
    ("stand-mongo80-batch-1", 200000, 500000, 50000),
)
STAND_MONGO_CDC: tuple[tuple[str, int, int, int], ...] = (
    ("stand-mongo44-cdc-1", 10, 10, 10),
    ("stand-mongo50-cdc-1", 10, 10, 10),
    ("stand-mongo80-cdc-1", 10, 10, 10),
)

HEALTH_TRIES = 180
HEALTH_DELAY = 5.0


def _profile_args(profiles: Sequence[str]) -> list[str]:
    args: list[str] = []
    for p in profiles:
        args += ["--profile", p]
    return args


def stand_services(profiles: Sequence[str]) -> tuple[str, ...]:
    """The services the selected profiles declare.

    `config --services` DOES honour `--profile` (verified: `--profile batch`
    lists exactly the nine batch services) whereas `ps` does NOT — which is
    DEVIATION 3, and why the expected set has to come from here.
    """
    p = compose(*_profile_args(profiles), "config", "--services", cwd=STAND_DIR, timeout=120)
    p.check("docker compose config --services")
    return tuple(sorted({ln.strip() for ln in p.stdout.splitlines() if ln.strip()}))


def stand_status(profiles: Sequence[str]) -> dict[str, tuple[str, str]]:
    """service → (state, health). `|` is the separator because a Go template
    emits `\\t` literally; no field can contain a pipe.

    `-a` (the shell omitted it) so a container that EXITED is reported as
    `state=exited` rather than silently missing from the listing.
    """
    p = compose(
        *_profile_args(profiles), "ps", "-a", "--format", "{{.Service}}|{{.State}}|{{.Health}}",
        cwd=STAND_DIR, timeout=120,
    )
    # DEVIATION 2: the shell let `ps` fail silently (`|| true` over a pipeline),
    # so a wedged daemon looked like a healthy stand.
    p.check("docker compose ps")
    rows: dict[str, tuple[str, str]] = {}
    for line in p.stdout.splitlines():
        parts = line.split("|")
        if len(parts) < 3 or not parts[0].strip():
            continue
        rows[parts[0].strip()] = (parts[1].strip(), parts[2].strip())
    return rows


def health_problems(expected: Sequence[str], rows: dict[str, tuple[str, str]]) -> list[str]:
    """Every expected service must be present, `running`, and EXACTLY `healthy`.

    DEVIATION 1: `grep -cEv 'healthy$'` counted "unhealthy" as healthy (it ends
    in the pattern) and `grep -qv healthy` counted it as healthy again (it
    contains the substring), so the gate passed on an unhealthy stand. Equality
    is the only comparison that distinguishes them.
    DEVIATION 2: an ABSENT service is a problem too; the shell's counts came
    from whatever `ps` listed, so listing nothing satisfied the gate.
    """
    problems: list[str] = []
    for svc in expected:
        row = rows.get(svc)
        if row is None:
            problems.append(f"{svc}: not created")
            continue
        state, health = row
        if state != "running":
            problems.append(f"{svc}: state={state or '(unknown)'}")
        elif health != "healthy":
            problems.append(f"{svc}: health={health or '(none reported)'}")
    return problems


def _wait_for_health(profiles: Sequence[str]) -> list[str]:
    expected = stand_services(profiles)
    if not expected:
        raise Fail(f"no services declared for profile(s) {' '.join(profiles)}", code=2)
    last: list[str] = []

    def ready() -> bool:
        nonlocal last
        last = health_problems(expected, stand_status(profiles))
        return not last

    shell.wait_until(ready, tries=HEALTH_TRIES, delay=HEALTH_DELAY, what="stand health")
    return last


def _sqlcmd(port: int, *args: str, timeout: float | None = 900) -> Proc:
    """Host sqlcmd. `-C` trusts the server's self-signed certificate; the flag
    order matches the shell's so a transcript of the command line is comparable."""
    return run(
        ["sqlcmd", "-S", f"127.0.0.1,{port}", "-U", "sa", "-P", SA_PASS, "-C", *args],
        timeout=timeout,
    )


def seed_mssql(port: int, seed_file: Path) -> None:
    """Idempotent: probe for `dbo.users`, seed only when absent.

    The image has no initdb hook, so this is the only way the MSSQL instances get
    data. DEVIATION 8: the shell hid the probe's stderr AND the connection error,
    so an unreachable server fell through to the seed attempt and aborted with no
    message at all (`set -e` on a redirected command).
    """
    if not seed_file.is_file():
        raise Fail(f"mssql seed file missing: {seed_file}")
    probe = _sqlcmd(port, "-Q", "SELECT 1", timeout=60)
    if not probe.ok:
        raise Fail(f"mssql:{port} unreachable: {_tail(probe)}",
                   hint="is the stand up? `docker compose ps` in dev/stand")

    seeded = _sqlcmd(
        port, "-d", "rivet", "-Q", "SET NOCOUNT ON; SELECT TOP 1 1 FROM dbo.users", "-h", "-1",
        timeout=60,
    )
    if seeded.ok:
        say(f"mssql:{port} already seeded")
        return
    # The message keeps the shell's stand-relative path (`seeds/common/mssql.sql`)
    # even though sqlcmd is handed the absolute one.
    rel = seed_file.relative_to(STAND_DIR) if seed_file.is_relative_to(STAND_DIR) else seed_file
    say(f"seeding mssql:{port} from {rel}")
    _sqlcmd(port, "-i", str(seed_file), timeout=None).check(f"seeding mssql:{port}")


def mongo_eval(container: str, js: str) -> str:
    """`mongosh` on 5.0+, the legacy `mongo` shell on 4.4 — the same two-shell
    fallback the compose healthchecks use.

    DEVIATION 8: the shell discarded mongosh's stderr and, if BOTH shells failed,
    left an empty string that the caller compared against a count.
    """
    first = docker_exec(container, "mongosh", "--quiet", "--eval", js, timeout=600)
    if first.ok:
        return first.stdout
    second = docker_exec(container, "mongo", "--quiet", "--eval", js, timeout=600)
    if second.ok:
        return second.stdout
    raise Fail(
        f"{container}: neither mongosh nor mongo could run the probe: {_tail(second)}",
        hint=f"docker exec {container} mongosh --eval 'db.runCommand({{ping:1}})'",
    )


def _mongo_value(text: str) -> str:
    """The shell squeezed the WHOLE output through `tr -d '[:space:]'`, so any
    banner or warning line was concatenated onto the number and the idempotency
    check could never match — re-seeding 700k documents on every run. Take the
    last non-blank line instead, which is the shell's `print()` output."""
    for line in reversed(text.splitlines()):
        s = "".join(line.split())
        if s:
            return s
    return ""


def _mongoimport(container: str, collection: str, count: int, user_mod: int) -> None:
    """`mongo_seed.py <coll> <count> <mod> | docker exec -i <c> mongoimport …`.

    Streamed through a pipe rather than materialised: the batch tier's `events`
    collection is 500k documents, ~45 MB of Extended JSON, and holding that in a
    Python string to hand to `run(stdin=…)` would be gratuitous.

    Both stages' exit codes are checked. Under bash only `pipefail` made the
    generator's failure visible at all, and `$?` after a pipeline reads the LAST
    stage — a generator that died half-way would otherwise import a truncated
    collection and report success.
    """
    gen = [sys.executable, str(STAND_DIR / "seeds" / "mongo_seed.py"),
           collection, str(count), str(user_mod)]
    imp = ["docker", "exec", "-i", container, "mongoimport", "--quiet",
           "-d", "rivet", "-c", collection, "--drop"]
    with subprocess.Popen(gen, stdout=subprocess.PIPE, cwd=str(STAND_DIR)) as g:
        assert g.stdout is not None
        with subprocess.Popen(imp, stdin=g.stdout, stderr=subprocess.PIPE, text=True) as i:
            g.stdout.close()  # so the generator sees EPIPE if mongoimport dies
            _, err = i.communicate()
            import_rc = i.returncode
        gen_rc = g.wait()
    if gen_rc != 0:
        raise Fail(f"mongo_seed.py {collection} {count} failed: exit {gen_rc}")
    if import_rc != 0:
        tail = [ln for ln in (err or "").splitlines() if ln.strip()]
        raise Fail(
            f"mongoimport {collection} into {container} failed: "
            f"{tail[-1] if tail else f'exit {import_rc}'}"
        )


def seed_mongo(container: str, orders: int, events: int, user_mod: int) -> None:
    """Idempotent: compare the (orders/events) doc counts, seed only on a miss.

    The seed is generated on the HOST by `seeds/mongo_seed.py` (stdlib only) and
    loaded by `mongoimport` INSIDE the container: no driver on the host, no JS
    seed files, and the `$number*` wrappers pin identical BSON types on every
    mongo version.
    """
    have = _mongo_value(
        mongo_eval(
            container,
            'print(db.getSiblingDB("rivet").orders.countDocuments({}) + "/" '
            '+ db.getSiblingDB("rivet").events.countDocuments({}))',
        )
    )
    if have == f"{orders}/{events}":
        say(f"{container} already seeded ({have})")
        return
    say(f"seeding {container} (orders {orders}, events {events})")
    _mongoimport(container, "orders", orders, user_mod)
    _mongoimport(container, "events", events, user_mod)
    mongo_eval(
        container,
        'var d=db.getSiblingDB("rivet"); d.orders.createIndex({user_id:1}); '
        'd.events.createIndex({user_id:1}); d.events.createIndex({occurred_at:1});',
    )


def stand_up(tier: str = "all") -> int:
    """`dev/stand/up.sh`: compose up, wait for health, then finish the seeding
    the initdb hooks cannot do (MSSQL has no hook; a CDC Mongo replica set has no
    PRIMARY at initdb time). Idempotent — safe to re-run any time."""
    profiles = STAND_PROFILES.get(tier)
    if profiles is None:
        say("usage: up.sh [batch|cdc]")
        return 2

    shell.require("docker", hint="install Docker / OrbStack and start the daemon")
    if not (STAND_DIR / "docker-compose.yaml").is_file():
        raise Fail(f"stand compose file missing: {STAND_DIR / 'docker-compose.yaml'}")
    shell.require("sqlcmd", hint="brew install sqlcmd  (the MSSQL images have no initdb hook)")

    compose(*_profile_args(profiles), "up", "-d", cwd=STAND_DIR, timeout=None, quiet=False).check(
        "docker compose up -d"
    )

    say("== waiting for health")
    problems = _wait_for_health(profiles)

    table = compose(
        # A literal backslash-t, as in the shell's single-quoted argument: docker's
        # `table` formatter expands the escape itself.
        *_profile_args(profiles), "ps", "--format", "table {{.Name}}\\t{{.Status}}",
        cwd=STAND_DIR, timeout=120,
    )
    for line in table.stdout.splitlines():
        say(line)

    if problems:
        say("ERROR: some services never became healthy")
        for p in problems:
            say(f"  {p}")
        return 1

    if tier != "cdc":
        for port, rel in STAND_MSSQL_BATCH:
            seed_mssql(port, STAND_DIR / "seeds" / rel)
        for container, orders, events, umod in STAND_MONGO_BATCH:
            seed_mongo(container, orders, events, umod)
    if tier != "batch":
        for port, rel in STAND_MSSQL_CDC:
            seed_mssql(port, STAND_DIR / "seeds" / rel)
        for container, orders, events, umod in STAND_MONGO_CDC:
            seed_mongo(container, orders, events, umod)

    say("== done; run ./verify.sh to cross-check the seeds")
    return 0


# ══════════════════════════════════════════════════════════════════════════════
# dev/stand/verify.sh
# ══════════════════════════════════════════════════════════════════════════════
# The common seed is deterministic, so every BATCH SQL engine must report the
# IDENTICAL tuple of
#   users | orders | events | sum(balance_cents) | sum(amount_cents) | sum(events.user_id)
# and every CDC SQL engine the identical light tuple. Sums are compared in
# integer CENTS to dodge decimal formatting differences between clients. Mongo is
# checked for doc counts only (its seed is volume-parity, not row-parity).

SQL_TUPLE = re.compile(r"^-?\d+(?:\|-?\d+){5}$")
MONGO_TUPLE = re.compile(r"^-?\d+\|-?\d+$")

PG_SQL = (
    "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM orders), "
    "(SELECT count(*) FROM events),\n"
    "            (SELECT sum((balance*100)::bigint) FROM users),\n"
    "            (SELECT sum((amount*100)::bigint) FROM orders),\n"
    "            (SELECT sum(user_id) FROM events)"
)
MYSQL_SQL = (
    "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM orders), "
    "(SELECT count(*) FROM events),\n"
    "            (SELECT sum(CAST(balance*100 AS SIGNED)) FROM users),\n"
    "            (SELECT sum(CAST(amount*100 AS SIGNED)) FROM orders),\n"
    "            (SELECT sum(user_id) FROM events)"
)
MSSQL_SQL = (
    "SET NOCOUNT ON; SELECT (SELECT count(*) FROM dbo.users), "
    "(SELECT count(*) FROM dbo.orders), (SELECT count(*) FROM dbo.events),\n"
    "            (SELECT sum(CAST(balance*100 AS BIGINT)) FROM dbo.users),\n"
    "            (SELECT sum(CAST(amount*100 AS BIGINT)) FROM dbo.orders),\n"
    "            (SELECT sum(CAST(user_id AS BIGINT)) FROM dbo.events)"
)
MONGO_JS = 'print(db.orders.countDocuments({}) + "|" + db.events.countDocuments({}))'


@dataclass(frozen=True)
class Cell:
    """One engine's answer. `value` comes from STDOUT ONLY — see DEVIATION 8:
    `2>&1 | head -1` let a client's error message become the compared value, and
    therefore the group's reference tuple."""

    label: str
    value: str
    error: str = ""

    def valid(self, pattern: re.Pattern[str]) -> bool:
        return bool(pattern.match(self.value))


def _first_line(text: str) -> str:
    for line in text.splitlines():
        s = line.strip()
        if s:
            return s
    return ""


def pg_tuple(label: str, port: int) -> Cell:
    p = run(["psql", f"postgresql://rivet:rivet@127.0.0.1:{port}/rivet", "-tA", "-F", "|",
             "-c", PG_SQL], timeout=300)
    return Cell(label, _first_line(p.stdout), _tail(p) if not p.ok else "")


def mysql_tuple(label: str, port: int) -> Cell:
    p = run(["mysql", "-h", "127.0.0.1", "-P", str(port), "-urivet", "-privet", "rivet",
             "-N", "-B", "-e", MYSQL_SQL], timeout=300)
    return Cell(label, _first_line(p.stdout).replace("\t", "|"), _tail(p) if not p.ok else "")


def mssql_tuple(label: str, port: int) -> Cell:
    p = run(["sqlcmd", "-S", f"127.0.0.1,{port}", "-U", "sa", "-P", SA_PASS, "-C",
             "-d", "rivet", "-h", "-1", "-W", "-s", "|", "-Q", MSSQL_SQL], timeout=300)
    return Cell(label, _first_line(p.stdout), _tail(p) if not p.ok else "")


def mongo_counts(label: str, port: int) -> Cell:
    p = run(["mongosh", "--quiet", f"mongodb://127.0.0.1:{port}/rivet?directConnection=true",
             "--eval", MONGO_JS], timeout=300)
    return Cell(label, _first_line(p.stdout), _tail(p) if not p.ok else "")


def check_group(name: str, cells: Sequence[Cell], pattern: re.Pattern[str]) -> bool:
    """Print each engine's tuple and require them all identical. Returns True on
    agreement. A cell that is not a well-formed tuple is a MISMATCH, never the
    reference — the shell only rejected the EMPTY string, so an error message was
    a legitimate candidate for `ref`."""
    say(f"== {name}")
    ref: str | None = None
    bad = False
    for cell in cells:
        say("  %-14s %s" % (cell.label, cell.value))
        if not cell.valid(pattern):
            detail = cell.error or ("empty — no output from the client"
                                    if not cell.value else "not a tuple of integers")
            say(f"                 ↳ unusable: {detail}")
            bad = True
            continue
        if ref is None:
            ref = cell.value
        elif cell.value != ref:
            bad = True
    if bad:
        say(f"  MISMATCH in {name}")
        return False
    say("  OK — all identical")
    return True


# Pinned label:port pairs, in the shell's order.
BATCH_SQL: tuple[tuple[str, int, str], ...] = (
    ("pg14:5514", 5514, "pg"), ("pg18:5518", 5518, "pg"),
    ("mysql57:3557", 3557, "mysql"), ("mysql80:3580", 3580, "mysql"),
    ("mssql19:1519", 1519, "mssql"), ("mssql22:1522", 1522, "mssql"),
)
CDC_SQL: tuple[tuple[str, int, str], ...] = (
    ("pg14:5614", 5614, "pg"), ("pg18:5618", 5618, "pg"),
    ("mysql57:3657", 3657, "mysql"), ("mysql80:3680", 3680, "mysql"),
    ("mssql19:1619", 1619, "mssql"), ("mssql22:1622", 1622, "mssql"),
)
BATCH_MONGO: tuple[tuple[str, int], ...] = (
    ("mongo44:27104", 27104), ("mongo50:27105", 27105), ("mongo80:27108", 27108),
)
CDC_MONGO: tuple[tuple[str, int], ...] = (
    ("mongo44:27204", 27204), ("mongo50:27205", 27205), ("mongo80:27208", 27208),
)

_SQL_CLIENTS: dict[str, Callable[[str, int], Cell]] = {
    "pg": pg_tuple, "mysql": mysql_tuple, "mssql": mssql_tuple,
}


def stand_verify() -> int:
    """`dev/stand/verify.sh`: cross-engine seed parity. Exit 0 when every group
    agrees, 1 on any mismatch or unusable cell."""
    fail = 0
    for name, spec in (
        ("batch SQL (heavy common seed)", BATCH_SQL),
        ("cdc SQL (light seed)", CDC_SQL),
    ):
        cells = [_SQL_CLIENTS[kind](label, port) for label, port, kind in spec]
        if not check_group(name, cells, SQL_TUPLE):
            fail = 1
    for name, mspec in (
        ("batch Mongo (orders|events doc counts)", BATCH_MONGO),
        ("cdc Mongo (orders|events doc counts)", CDC_MONGO),
    ):
        cells = [mongo_counts(label, port) for label, port in mspec]
        if not check_group(name, cells, MONGO_TUPLE):
            fail = 1
    return fail


# ══════════════════════════════════════════════════════════════════════════════
def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help", "help"):
        print(USAGE, file=sys.stdout if args else sys.stderr)
        return 0 if args else 2

    cmd, rest = args[0], args[1:]
    if cmd == "legacy":
        return legacy_matrix(_targets(rest, LEGACY_DEFAULT))
    if cmd == "full-matrix":
        return full_matrix(_targets(rest, FULL_DEFAULT))
    if cmd == "stand-up":
        if len(rest) > 1:
            say("usage: up.sh [batch|cdc]")
            return 2
        return stand_up(rest[0] if rest else "all")
    if cmd == "stand-verify":
        if rest:
            say("usage: verify.sh")
            return 2
        return stand_verify()

    # DEVIATION: an unknown subcommand is exit 2, not a silent success — the
    # shell's dispatch `case`es are the reason this module exists.
    print(f"Unknown command: {cmd}", file=sys.stderr)
    print(USAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    shell.main(lambda: main_cli())
