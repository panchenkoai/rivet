"""Release-oracle regression stage: the new binary vs the PREVIOUS RELEASE.

Port of `dev/release-oracle/lib/regression.sh`.

Every other stage of this gate compares rivet to a checked-in golden or to
itself. Neither notices a regression against the version users are ACTUALLY
running, and two such regressions are invisible to every correctness check yet
release-blocking:

* **format** — the new release must READ what the previous release WROTE
  (manifest + parts). A format bump that cannot open old artifacts breaks every
  existing user's data on upgrade, and does it quietly: worse than a crash.
* **perf / RSS** — a 3× slowdown or a memory blow-up ships GREEN through every
  count and value check there is.

The perf leg benchmarks against the DOWNLOADED previous-release binary
(`RIVET_PREV_RELEASE_BIN`: a GitHub release asset, or the Homebrew bottle) and
deliberately NOT against a locally rebuilt parent. Two reasons, both load-
bearing: the release profile is `lto = "fat"` with `codegen-units = 1`, so a
rebuild costs MINUTES per side; and what it produces is a local approximation,
not the artifact users run. Comparing to the published binary is both cheaper
and more honest. (`cargo install rivet-cli@<ver>` does not count either — it
rebuilds from source, which is the exact cost this avoids.)

Inputs, and the SKIP rule. `RIVET_PREV_RELEASE_BIN` is the prev binary,
`RIVET_REGRESSION_SOURCE_URL` a Postgres this stage may seed a fixture into.
When either is absent the stage SKIPs — never a silent pass, because "we could
not check" and "we checked and it was fine" are different facts and only one of
them is releasable. Wall tolerance is `RIVET_REGRESSION_WALL_TOL` (default
1.5×): a go/no-go catches gross regressions, and fine-grained perf work belongs
to the benchmark suite.

CROSS-VERSION STATE IS THE TRAP. The state DB (`.rivet_state.db`) lives next to
the config, so every binary here gets its OWN env directory. The new binary
UPGRADES the state schema on open (v18 → v19); the old binary then cannot open
what its successor migrated ("migration incomplete"). Sharing one state dir
between two versions fails the run for a reason that has nothing to do with the
regression being measured.
"""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .core import Ledger, Proc, container_for_port, docker_exec, port_of, rivet_bin, run

# ── ledger identity ────────────────────────────────────────────────────────────
# The bash `add` calls in this file were missing the STORE column
# (`add release regression - SKIP "…"` is five fields where the ledger takes
# six), so every row it recorded was shifted: the status landed in STORE and the
# detail landed in STATUS, garbling the final table. The exit code survived only
# because bash tracked RED separately from the rows. Columns are named here, and
# `Ledger` derives the verdict from the rows, so the two cannot disagree.
_R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE = "release", "regression", "-", "-"
_S_VERSION, _S_SCENARIO, _S_STORE = "scale", "memory", "-"

_MIB = 1048576


# ── timing / RSS measurement ───────────────────────────────────────────────────
@dataclass(frozen=True)
class Timing:
    """One measured run.

    `wall` is the string as it is PRINTED (kept verbatim from the parse so the
    log reads identically to the bash), `secs` the value actually COMPARED, and
    `rss` peak resident bytes, and `ok` whether the timed command actually
    SUCCEEDED.

    `ok` exists because a failed run still yields a perfectly plausible
    measurement — a binary that refuses to start reports ~11 MB and 0.01s, which
    reads as "leaner and faster" rather than "never ran". Measured 2026-08-03:
    the prev binary failed on all four engines and the report said
    `prev[users=11MB events=11MB flat×1.00]`, which a reader would take as a 3x
    RSS regression in the current tree.
    """

    wall: str
    secs: float
    rss: int
    ok: bool = True


# `/usr/bin/time` speaks two dialects: BSD/macOS `-l` and GNU `-v`. Detect by
# probing `-l` on a trivial command — NEVER by the exit status of the timed
# command itself, which is what a naive `||` fallback does: a run that
# legitimately returns non-zero would then be re-run under the wrong flag and
# its timing lost. Probed once per process; the dialect cannot change under us.
_TIME_BIN = Path("/usr/bin/time")
_bsd: bool | None = None


def _is_bsd_time() -> bool:
    global _bsd
    if _bsd is None:
        _bsd = run([str(_TIME_BIN), "-l", "true"], timeout=60).ok
    return _bsd


def _parse_wall(text: str) -> tuple[str, float]:
    """Wall seconds from either dialect, as (display string, value).

    BSD prints `0.50 real`; GNU prints
    `Elapsed (wall clock) time (h:mm:ss or m:ss): 0:00.50`. Both patterns are
    tried regardless of the detected dialect, which keeps the parse tolerant.

    The GNU form is where the bash was WRONG: it grepped the field for
    `[0-9]+\\.[0-9]+`, which picks up only the seconds component, so a 1:23.45
    run measured as 23.45 s — a 90 s regression could read as a 30 s pass. Here
    the colon-separated field is summed properly. The display string stays byte-
    identical to the bash in the sub-minute case it did handle (that is every
    run of this fixture, which is sized to be sub-second) and shows the CORRECTED
    value in the case it did not, rather than printing a number the verdict
    disagrees with.
    """
    m = re.search(r"(\d+\.\d+) real", text)
    if m:
        return m.group(1), float(m.group(1))

    # The label itself contains colons (`(h:mm:ss or m:ss)`), so the field is
    # anchored at end-of-line rather than found by the first colon after it.
    m = re.search(
        r"wall clock[^\n]*?:\s*([0-9]+(?::[0-9]+)*(?:\.[0-9]+)?)\s*$", text, re.M
    )
    if m:
        field = m.group(1)
        total = 0.0
        for part in field.split(":"):
            total = total * 60 + float(part)
        # What the bash would have shown: the last `d+.d+` token in the field.
        toks = re.findall(r"[0-9]+\.[0-9]+", field)
        legacy = toks[-1] if toks else ""
        if legacy and abs(float(legacy) - total) < 1e-9:
            return legacy, total
        return f"{total:.2f}", total

    return "0", 0.0


def _parse_rss(text: str) -> int:
    """Peak resident bytes from either dialect (BSD reports bytes, GNU kbytes)."""
    m = re.search(r"(\d+) +maximum resident set size", text)
    if m:
        return int(m.group(1))
    m = re.search(r"Maximum resident set size[^0-9]*(\d+)", text)
    if m:
        return int(m.group(1)) * 1024
    return 0


def _regr_time(binary: Path | str, *args: str) -> Timing:
    """Run `binary args…` under `/usr/bin/time`, returning wall + peak RSS.

    No timeout, as in the bash: an export of the fixture legitimately takes as
    long as it takes, and a killed run would read as a real failure.

    When `/usr/bin/time` is absent entirely (some minimal Linux images ship only
    the shell builtin) both dialects fail and this returns zeros, which the
    callers' `> 0` guards turn into a FAIL rather than a SKIP. That is the bash
    behaviour, kept deliberately — but it is a misclassification worth knowing
    about if this ever fires on a container with no coreutils `time`.
    """
    flag = "-l" if _is_bsd_time() else "-v"
    p = run([str(_TIME_BIN), flag, str(binary), *args], timeout=None,
            env=_ISOLATED_STATE)
    # The report goes to stderr, mixed with whatever the timed command wrote
    # there — the same stream the bash captured into its temp file.
    wall, secs = _parse_wall(p.stderr)
    return Timing(wall, secs, _parse_rss(p.stderr), p.returncode == 0)


def _tolerance(raw: str) -> float:
    """A tolerance env var as a number, coercing junk to 0 the way awk did —
    which makes `cur <= prev * 0` false, i.e. a nonsense tolerance FAILS loudly
    instead of being silently treated as "no limit"."""
    try:
        return float(raw)
    except ValueError:
        return 0.0


# ── source access ──────────────────────────────────────────────────────────────
# This cell's whole design is one state DB PER BINARY (see `_regr_cfg`), so that
# the current tree's schema upgrade never touches the previous release's state.
# `--state-url` defeats that: `__main__` sets `RIVET_STATE_URL` process-wide and
# every child inherits it, so both binaries meet ONE Postgres state DB — and the
# published binary correctly refuses a schema the current tree just migrated
# ("state(pg): migration incomplete — expected schema v19 but reached v20").
#
# Measured 2026-08-03: the whole cell failed on the Postgres pass for exactly
# this reason (`prev-export-failed`, and every later fragment was a consequence
# of a baseline that never ran), while the same prev binary with its own SQLite
# state exported 100000 rows into 4 parts, exit 0. Left alone, this cell can
# NEVER pass on a Postgres pass once the tree adds a migration — which is
# precisely when a cross-version check earns its keep.
#
# An empty value means "SQLite beside the config" (verified against the PUBLISHED
# binary, not just the current tree), and `run`'s env MERGES over os.environ, so
# clearing requires the empty string rather than a missing key.
_ISOLATED_STATE = {"RIVET_STATE_URL": ""}


def _regr_psql(url: str, sql: str) -> Proc:
    """Run `sql` against the regression source, via the container publishing its
    port (the URL points at a docker-published 127.0.0.1 port, so `psql` inside
    the container is the one client guaranteed to exist).

    Returns a Proc with returncode 1 when no container publishes that port,
    rather than handing an empty container name to `docker exec` (which answers
    "invalid container name or ID: value is empty").
    """
    port = port_of(url)
    name = container_for_port(port) if port is not None else None
    if name is None:
        return Proc(["psql", url], 1, "", f"no running container publishes port {port}")
    return docker_exec(
        name, "psql", "-U", "rivet", "-d", "rivet", "-v", "ON_ERROR_STOP=1", "-q",
        stdin=sql, timeout=None,
    )


# ── config writers ─────────────────────────────────────────────────────────────
def _regr_cfg(src: str, envdir: Path, dest: Path | None = None) -> Path:
    """A keyset+zstd export of the fixture into an ISOLATED env dir.

    State and output both live under `envdir`, which is the whole point: the
    state DB sits beside the config, so one dir per binary keeps the new
    version's schema upgrade away from the old version's DB. `dest` overrides
    the destination so one binary can be pointed at ANOTHER's output.
    """
    (envdir / "out").mkdir(parents=True, exist_ok=True)
    out = envdir / "out" if dest is None else dest
    cfg = envdir / "c.yaml"
    cfg.write_text(
        f'source: {{ type: postgres, url: "{src}" }}\n'
        "exports:\n"
        "  - name: regr_probe\n"
        "    table: regr_probe\n"
        "    mode: chunked\n"
        "    chunk_by_key: id\n"
        "    chunk_size: 50000\n"
        "    format: parquet\n"
        "    compression: zstd\n"
        f'    destination: {{ type: local, path: "{out}/" }}\n'
    )
    return cfg


def _scale_cfg(engine: str, url: str, table: str, envdir: Path) -> Path:
    """A batch export of an EXISTING table — keyset for the SQL engines, full
    scan for Mongo — into its own env dir (per-binary isolation, as above)."""
    tls = "\n  tls: { accept_invalid_certs: true }" if engine == "mssql" else ""
    if engine == "mongo":
        mode = "    mode: full"
    else:
        chunk = os.environ.get("RIVET_SCALE_CHUNK") or "100000"
        mode = f"    mode: chunked\n    chunk_by_key: id\n    chunk_size: {chunk}"
    (envdir / "out").mkdir(parents=True, exist_ok=True)
    cfg = envdir / "c.yaml"
    cfg.write_text(
        "source:\n"
        f"  type: {engine}\n"
        f'  url: "{url}"{tls}\n'
        "exports:\n"
        f"  - name: {table}\n"
        f"    table: {table}\n"
        f"{mode}\n"
        "    format: parquet\n"
        "    compression: zstd\n"
        f'    destination: {{ type: local, path: "{envdir}/out/" }}\n'
    )
    return cfg


def _prev_binary() -> Path | None:
    """The DOWNLOADED previous-release binary, or None.

    `is_file()` rather than a bare executable test: `[ -x ]` is also true for a
    directory, and a path that happens to name one would get as far as trying to
    exec it.
    """
    raw = os.environ.get("RIVET_PREV_RELEASE_BIN", "")
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_file() and os.access(p, os.X_OK) else None


# ── stage 1: format + perf vs the previous release ─────────────────────────────
def verify_release_regression(led: Ledger) -> None:
    prev = _prev_binary()
    if prev is None:
        led.skipped(
            _R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE,
            "release regression: no RIVET_PREV_RELEASE_BIN (download a release asset)",
            "no prev binary",
        )
        return
    src = os.environ.get("RIVET_REGRESSION_SOURCE_URL", "")
    if not src:
        led.skipped(
            _R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE,
            "release regression: no RIVET_REGRESSION_SOURCE_URL", "no source",
        )
        return

    tol = os.environ.get("RIVET_REGRESSION_WALL_TOL") or "1.5"
    tol_n = _tolerance(tol)
    prev_ver = run([str(prev), "--version"]).stdout.splitlines()
    led.phase(
        f"Release regression vs prev ({prev_ver[0] if prev_ver else ''}) — "
        "cross-version read + perf/RSS"
    )
    # Not auto-deleted: the exports under here are what a reader wants to open
    # when the format leg fails, and the bash trap removed them before anyone
    # could look.
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-regr-"))

    # A seeded 100K-row fixture: deterministic, measurable, and sub-second, so
    # the perf comparison is not at the mercy of whatever happens to be on the
    # stand.
    _regr_psql(src, """DROP TABLE IF EXISTS regr_probe;
CREATE TABLE regr_probe (id int PRIMARY KEY, a text, b numeric(18,4), c timestamptz);
INSERT INTO regr_probe SELECT g, md5(g::text), (g%1000)+0.25, '2025-01-01'::timestamptz + (g||' seconds')::interval FROM generate_series(1,100000) g;
""")

    # Fragments carry their own trailing space and are joined verbatim, so the
    # rendered message and the recorded detail match the bash byte for byte.
    fails: list[str] = []

    # ── format: prev WRITES → cur READS its manifest + parts (the upgrade path) ──
    pe = work / "prev_fmt"
    _regr_cfg(src, pe)
    if not run([str(prev), "run", "-c", str(pe / "c.yaml")], timeout=None, env=_ISOLATED_STATE).ok:
        fails.append("prev-export-failed ")
    # cur validates prev's OUTPUT from cur's OWN env, so cur's state-schema
    # upgrade never touches prev's state DB.
    ce = work / "cur_fmt"
    _regr_cfg(src, ce, pe / "out")
    validated = run([str(rivet_bin()), "validate", "-c", str(ce / "c.yaml")], timeout=None, env=_ISOLATED_STATE)
    if "passed" not in validated.out.lower():
        fails.append("cur-cannot-read-prev-output(format-break) ")

    # An INDEPENDENT reader over the same parts: DuckDB, not rivet, must agree
    # with the source row count — otherwise "cur can read prev's output" rests
    # entirely on rivet's own verdict about itself.
    duck = run([
        "duckdb", "-noheader", "-list", "-c",
        f"SELECT count(*) FROM read_parquet('{pe / 'out'}/**/*.parquet')",
    ])
    dcnt = duck.stdout.strip()
    m = re.search(r"\d+", _regr_psql(src, "SELECT count(*) FROM regr_probe;").stdout)
    scnt = m.group(0) if m else ""
    if not dcnt or dcnt != scnt:
        fails.append(f"prev-parts-rowcount[{dcnt}!={scnt}] ")

    # ── perf: cur vs the downloaded prev release, each in its own env ──
    # One warm pass per binary (page the binary in, prime the page cache), then
    # a timed pass into a FRESH output dir, so the measurement is one clean run
    # rather than a run plus whatever the warm-up left behind.
    pp, pc = work / "perf_prev", work / "perf_cur"
    _regr_cfg(src, pp)
    _regr_cfg(src, pc)
    run([str(prev), "run", "-c", str(pp / "c.yaml")], timeout=None, env=_ISOLATED_STATE)
    run([str(rivet_bin()), "run", "-c", str(pc / "c.yaml")], timeout=None, env=_ISOLATED_STATE)
    for d in (pp / "out", pc / "out"):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True, exist_ok=True)
    pt = _regr_time(prev, "run", "-c", str(pp / "c.yaml"))
    ct = _regr_time(rivet_bin(), "run", "-c", str(pc / "c.yaml"))
    if not (ct.secs > 0 and pt.secs > 0 and ct.secs <= pt.secs * tol_n):
        fails.append(f"perf-regression(cur {ct.wall}s > prev {pt.wall}s ×{tol}) ")

    _regr_psql(src, "DROP TABLE IF EXISTS regr_probe;")

    rssnote = ""
    if ct.rss > 0 and pt.rss > 0:
        rssnote = f" RSS cur {ct.rss // _MIB}MB vs prev {pt.rss // _MIB}MB"
    if not fails:
        led.passed(
            _R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE,
            "release regression: cur reads prev's output (format-compat), "
            f"perf cur {ct.wall}s <= prev {pt.wall}s ×{tol}{rssnote}",
            f"wall {ct.wall}/{pt.wall}s",
        )
    else:
        joined = "".join(fails)
        led.failed(_R_ENGINE, _R_VERSION, _R_SCENARIO, _R_STORE,
                   f"release regression: {joined}", joined)


# ── stage 2: the FLAT-RSS guarantee at scale ───────────────────────────────────
def verify_scale_memory(led: Ledger) -> None:
    """rivet STREAMS: peak RSS is O(chunk), NOT O(rows).

    That is the headline value proposition — the 454M-row, no-OOM field win over
    tools that buffer — and nothing else in this gate can see it break. The data
    scenarios run ~150K rows, so a regression that reintroduces buffering (RSS
    scaling with the table) ships GREEN through every count and value check.

    The check runs PER ENGINE over TWO tables ALREADY ON THE STAND (no seeding —
    a small one and a large one, e.g. `users` vs `events`): peak RSS on the large
    table must stay flat against the small one,
    `RSS(large) <= RSS(small) × RIVET_SCALE_RSS_TOL` (default 3×). The tolerance
    is deliberately generous because the two tables differ in ROW WIDTH:
    streaming keeps the ratio near the width ratio, while buffering blows it up
    by the ROW-COUNT ratio — orders of magnitude apart, so a loose bound still
    separates them cleanly. Measured for the current binary (that is the
    assertion) and, when available, the downloaded prev release (context only).

    Per engine, set `RIVET_SCALE_<ENGINE>_URL` plus optionally
    `RIVET_SCALE_<ENGINE>_SMALL` / `_LARGE` (default `users` / `events`). An
    engine with no URL SKIPs — never a silent pass.
    """
    prev = _prev_binary()
    tol = os.environ.get("RIVET_SCALE_RSS_TOL") or "3"
    tol_n = _tolerance(tol)
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-scale-"))

    for engine in ("postgres", "mysql", "mssql", "mongo"):
        up = engine.upper()
        uvar = f"RIVET_SCALE_{up}_URL"
        url = os.environ.get(uvar, "")
        if not url:
            led.skipped(engine, _S_VERSION, _S_SCENARIO, _S_STORE,
                        f"scale[{engine}]: no {uvar}", "no url")
            continue
        small = os.environ.get(f"RIVET_SCALE_{up}_SMALL") or "users"
        large = os.environ.get(f"RIVET_SCALE_{up}_LARGE") or "events"
        led.phase(
            f"Scale memory [{engine}] (FLAT-RSS: existing table '{large}' vs "
            f"'{small}' — streaming, not buffering, tol {tol}×)"
        )

        fails: list[str] = []
        report = ""
        for label in ("cur", "prev"):
            if label == "cur":
                binary: Path = rivet_bin()
            elif prev is None:
                continue  # no downloaded prev release: the cur assertion stands alone
            else:
                binary = prev
            sdir = work / f"{engine}_{label}_s"
            ldir = work / f"{engine}_{label}_l"
            _scale_cfg(engine, url, small, sdir)
            _scale_cfg(engine, url, large, ldir)
            st = _regr_time(binary, "run", "-c", str(sdir / "c.yaml"))
            lt = _regr_time(binary, "run", "-c", str(ldir / "c.yaml"))
            if not (st.ok and lt.ok):
                # Never print a number for a run that did not happen: a failed
                # binary's ~11 MB reads as a lean baseline and invites exactly
                # the wrong conclusion about the other column.
                report += f" {label}[RUN FAILED — no measurement]"
            else:
                ratio = f"{lt.rss / st.rss:.2f}" if st.rss > 0 else "NA"
                report += (
                    f" {label}[{small}={st.rss // _MIB}MB "
                    f"{large}={lt.rss // _MIB}MB flat×{ratio}]"
                )
            # Only the CURRENT binary is asserted on; prev is reported for
            # context, since a prev that also buffers is not a reason to ship.
            if label == "cur" and not (st.ok and lt.ok):
                fails.append("cur-run-failed(no RSS measurement to assert on) ")
            elif label == "cur" and not (st.rss > 0 and lt.rss > 0 and lt.rss <= st.rss * tol_n):
                fails.append(
                    f"rss-NOT-flat(cur {large}={lt.rss // _MIB}MB > "
                    f"{small}={st.rss // _MIB}MB ×{tol} — buffering, not streaming) "
                )

        if not fails:
            led.passed(engine, _S_VERSION, _S_SCENARIO, _S_STORE,
                       f"scale[{engine}]: flat-RSS holds —{report}", report)
        else:
            joined = "".join(fails)
            led.failed(engine, _S_VERSION, _S_SCENARIO, _S_STORE,
                       f"scale[{engine}]: {joined} —{report}", joined)
