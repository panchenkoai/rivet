#!/usr/bin/env python3
"""Port of `dev/matrices/_common/**` + `dev/matrices/setup_links.sh`.

The shared library every matrix harness leans on: stage the binary, seed the
fixtures, normalise a listing, snapshot a run's accounting, wire the navigation
symlinks. `dev/pytools/matrices.py` (the port of `run.sh` + `check_msg.sh`)
drives these; the per-matrix suites use `normalize` / `extract_summary` directly.

    python3 -m dev.pytools.matrix_common <command> [args]

      stage-rivet <matrix_dir> [repo_root]
      extract-summary <workdir>
      normalize                      # paths on stdin, listing on stdout
      seed-pa-audit [--engines=postgres|mysql|postgres,mysql]
      seed-pa-audit-all
      seed-pa-soak
      setup-links

`normalize` and `extract_summary` are COMPARISON PLUMBING: their stdout is
diffed against `expected/<id>.layout` / `expected/<id>.summary` baselines, so a
single byte of drift flips a matrix cell. They are reproduced exactly —
including `sort`'s C-collation byte ordering (the baselines were generated under
`LC_COLLATE=C.UTF-8`) and jq's `\\(…)` interpolation spelling (`null` for an
absent key, an unquoted string for a string).

The bash originals are otherwise faithful — same messages, same exit codes —
except where the shell reported success having done nothing. Each of those is
marked `DEVIATION:` at its site; the summary is:

1. `stage_rivet.sh` ran `mkdir -p "$dir"` BEFORE any caller could test `-d
   "$dir"`, so a missing `dev/matrices/<layer>/<name>` symlink was papered over
   with a stub directory holding a copied binary — and `run.sh`'s "Matrix
   directory missing — run setup_links.sh" guard, which ran after staging, then
   passed. (Verified: staging into `…/missing/surface/cli` creates the tree,
   copies the binary, prints "Staged …" and exits 0.) The directory must now
   already exist.
2. `stage_rivet.sh` printed "Staged …" and exited 0 even when the `cp` FAILED —
   the RIVET_BIN branch ends in an unconditional `exit 0`, and the repo branch
   ends in an `echo` whose status becomes the script's. (Verified against an
   unwritable destination: `cp: … Permission denied` on stderr, "Staged …" on
   stdout, rc=0.) A failed copy — or a failed `cargo build` under
   `STAGE_RIVET_BUILD=1`, which fell through into a `cp` of a nonexistent file —
   is now fatal. This one defeated `matrices.py`'s `.check()`: it was checking
   an exit code the shell never set.
3. `setup_links.sh`'s `ln -sfn` is a silent no-op when the link path is a real
   DIRECTORY: it creates the link INSIDE it (`surface/cli/cli_matrix →
   ../../cli_matrix`) and still prints "  surface/cli → ../../cli_matrix". Which
   is precisely the state deviation 1 leaves behind, so the repair step could
   not repair it. A non-symlink at the link path is now fatal.
4. `seed_pa_audit_all.sh` has no `set -e`: a failed fixture load was ignored and
   the count query that followed printed "  pg-12: pa_audit =  rows" — an empty
   count, on stdout, exit 0. Every seeding failure now counts, and the script's
   exit code reports them (it previously ALWAYS exited 0, since its last command
   was an `echo`).
5. `seed_pa_audit.sh --engines=` expanded an empty array under `set -u`: on the
   macOS bash 3.2 that is `ENGINE_LIST[@]: unbound variable`, exit 1; on bash
   4.4+ it seeds nothing and exits 0. An empty engine list is a usage error.
6. `extract_summary.sh` required `jq`, and exited 2 without it — which made the
   caller (`path_matrix/matrix.sh`) truncate the snapshot and report DIVERGED.
   JSON is parsed natively now; the rendering is jq-identical (see `jq_raw`).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Callable, Iterable, Sequence

try:  # `python3 -m dev.pytools.matrix_common`
    from . import shell
except ImportError:  # `python3 dev/pytools/matrix_common.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail
bad, warn = shell.bad, shell.warn
docker_exec, stream = shell.docker_exec, shell.stream

MATRICES_DIR: Path = ROOT / "dev" / "matrices"
COMMON: Path = MATRICES_DIR / "_common"
FIXTURES: Path = COMMON / "fixtures"

PROG = "dev/pytools/matrix_common.py"


# ── C-collation ordering ───────────────────────────────────────────────────────
# `sort` under LC_COLLATE=C (the environment the committed baselines were
# generated in, and the only locale-independent ordering) compares BYTES. Python
# compares code points, which agrees for ASCII and, because UTF-8 preserves code
# point order, for valid UTF-8 too — but NOT for the surrogate escapes we use to
# round-trip undecodable filenames. Sorting on the encoded bytes is exact.
def bytekey(s: str) -> bytes:
    """Sort key reproducing `sort` under LC_COLLATE=C."""
    return s.encode("utf-8", "surrogateescape")


def sort_c(items: Iterable[str]) -> list[str]:
    """`sort`, C collation."""
    return sorted(items, key=bytekey)


def write_text(path: Path, text: str) -> None:
    """Write a captured stream / artifact, preserving surrogate escapes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8", "surrogateescape"))


def _substitution_out(text: str) -> str:
    """`$(cmd)`: strip trailing newlines, and only newlines."""
    return text.rstrip("\n")


# ── lib/normalize.sh ───────────────────────────────────────────────────────────
# Two timestamp classes are erased so a layout listing is comparable across
# runs; `_chunk<N>` is deliberately NOT erased — chunk numbering IS the contract.
_RUNID_RE = re.compile(rb"[0-9]{8}T[0-9]{6}\.[0-9]+")
# The part stamp gained a MILLISECOND field (`%3f`) when second granularity was
# found to let two sub-second runs into one prefix clobber each other — a real
# data-loss fix, so the suffix is here to stay. It must be erased WITH the
# timestamp, not left dangling: `_064` differs on every run, so a baseline
# blessed with it would fail on the very next one. Optional, because the
# chunked/keyset writers stamp differently.
_TS_RE = re.compile(rb"[0-9]{8}_[0-9]{6}(?:_[0-9]{3})?")
# The CHUNK nonce, erased while the chunk NUMBER is kept — the number is the
# contract `p03` pins, the nonce is `rand::rng().random::<u64>()` formatted
# `{:016x}` (src/pipeline/chunked/mod.rs::chunk_part_filename), so it differs on
# every write by design: it is what makes a repair re-export land ALONGSIDE the
# original instead of overwriting it. Anchored to `_chunk<N>_` so it can only
# match this construction, never an arbitrary hex-looking path segment.
_CHUNK_NONCE_RE = re.compile(rb"(_chunk[0-9]+)_[0-9a-f]{16}\b")


def normalize_bytes(data: bytes) -> bytes:
    """`sed -E 's|…|<RUNID>|g; s|…|<TS>|g' | sort`, byte for byte.

    Bytes rather than text because the input is `find` output: a filename is an
    arbitrary byte string, and `sed`/`sort` never decoded it.

    Order matters and is kept: RUNID first, so `20260101T120000.123` becomes
    `<RUNID>` before the TS pattern could claim its `20260101T120000` prefix —
    it cannot (the separator differs), but the original ran them in this order
    and a future pattern pair might not be so lucky.

    A trailing newline is added iff there is any output, which is what `sort`
    does: it terminates its last line even when its input did not.
    """
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        # A trailing newline is a terminator, not an empty final line. An
        # UNterminated last line is still a line, and `sed` still processed it.
        lines.pop()
    out = [
        _CHUNK_NONCE_RE.sub(rb"\1", _TS_RE.sub(b"<TS>", _RUNID_RE.sub(b"<RUNID>", ln)))
        for ln in lines
    ]
    out.sort()  # bytes compare as bytes: LC_COLLATE=C, by construction
    return b"".join(ln + b"\n" for ln in out)


def normalize(text: str) -> str:
    """Text front door for `normalize_bytes`."""
    return normalize_bytes(text.encode("utf-8", "surrogateescape")).decode(
        "utf-8", "surrogateescape"
    )


def normalize_main(argv: Sequence[str]) -> int:
    if argv:
        # The original took no arguments and would have silently ignored them.
        print(f"normalize.sh takes no arguments (got: {argv[0]})", file=sys.stderr)
        return 2
    sys.stdout.buffer.write(normalize_bytes(sys.stdin.buffer.read()))
    sys.stdout.buffer.flush()
    return 0


# ── lib/extract_summary.sh ─────────────────────────────────────────────────────
# One line per run summary, tab-separated `key=value` pairs, sorted. The point is
# accounting: a code path that exports 0 rows where 30 were expected still exits
# 0, but this snapshot diverges from its baseline.
SUMMARY_FIELDS: tuple[tuple[str, str], ...] = (
    ("export", "export_name"),
    ("status", "status"),
    ("format", "format"),
    ("compression", "compression"),
    ("total_rows", "total_rows"),
    ("files_produced", "files_produced"),
)


def jq_raw(value: object) -> str:
    """Render `value` as jq's `\\(…)` interpolation / `jq -r` would.

    The spellings that matter here, all of which appear in real summaries:
    an absent key is `null` (jq indexes a missing field to null rather than
    failing), a string interpolates WITHOUT quotes, and an integer has no
    decimal point.

    Floats are the one place jq versions disagree — 1.6 canonicalises `30.0` to
    `30`, 1.7 preserves the literal — and this follows 1.7, verified against the
    jq on this machine by a differential run. No field rendered here is a float
    in any summary rivet writes, so the choice is documentation either way.
    """
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _summary_line(doc: object) -> str:
    if not isinstance(doc, dict):
        # jq: "Cannot index array with \"export_name\"" — exit 5, no stdout.
        raise ValueError(f"Cannot index {type(doc).__name__} with \"export_name\"")
    return "\t".join(f"{label}={jq_raw(doc.get(key))}" for label, key in SUMMARY_FIELDS)


def find_summaries(workdir: Path) -> list[Path] | None:
    """`find "$work/.rivet/runs" -name 'summary.json' | sort`.

    `None` when the runs directory is absent — `find` exits 1 there, and with
    `pipefail` set that was the script's exit code (the caller truncates its
    snapshot on a non-zero exit, so an absent directory yielded an empty
    snapshot; same outcome, now explicit).
    """
    runs = workdir / ".rivet" / "runs"
    if not runs.is_dir():
        return None
    found: list[str] = []
    for dirpath, dirnames, filenames in os.walk(runs, followlinks=False):
        # `-name` matches any type, so a DIRECTORY called summary.json counts —
        # faithfully, because jq would then fail on it and the line would be
        # missing from the snapshot, which is a divergence worth seeing.
        for n in list(dirnames) + list(filenames):
            if n == "summary.json":
                found.append(str(Path(dirpath) / n))
    return [Path(p) for p in sort_c(found)]


def extract_summary(workdir: Path) -> tuple[str, int]:
    """The accounting snapshot for `workdir`, plus the exit code to report.

    DEVIATION: a per-file render failure now shows up in the exit code. Under
    `pipefail` the pipeline's status came from the `while` loop, whose status is
    the status of its LAST iteration — so a corrupt summary.json anywhere but
    last dropped its line from the snapshot and still exited 0. The line is
    still dropped (the snapshot diff catches that), but the code says 5, jq's.
    """
    files = find_summaries(workdir)
    if files is None:
        return "", 1

    lines: list[str] = []
    failed = False
    for f in files:
        try:
            doc = json.loads(f.read_bytes().decode("utf-8", "surrogateescape"))
            lines.append(_summary_line(doc))
        except (OSError, ValueError) as e:
            print(f"jq: error: {e} ({f})", file=sys.stderr)
            failed = True

    return "".join(ln + "\n" for ln in sort_c(lines)), (5 if failed else 0)


def extract_summary_main(argv: Sequence[str]) -> int:
    work = argv[0] if argv else ""
    if not work or not Path(work).is_dir():
        print("extract_summary.sh <workdir>", file=sys.stderr)
        return 2
    # DEVIATION: no `command -v jq` gate. The bash exited 2 without jq, and the
    # caller's `|| : > summary` then truncated the snapshot — so a host missing
    # jq silently turned every accounting assertion into "no data".
    text, code = extract_summary(Path(work))
    sys.stdout.buffer.write(text.encode("utf-8", "surrogateescape"))
    sys.stdout.buffer.flush()
    return code


# ── lib/stage_rivet.sh ─────────────────────────────────────────────────────────
def _want_build() -> bool:
    # `[[ "${STAGE_RIVET_BUILD:-0}" == 1 ]]` — a STRING comparison, so `true`,
    # `yes` and `01` all meant "no". Kept, so a caller's habit does not change
    # meaning between the two implementations.
    return os.environ.get("STAGE_RIVET_BUILD", "0") == "1"


def _copy_binary(src: Path, dest: Path) -> None:
    """`cp src dest`, but a failure is a failure.

    DEVIATION (2): the shell printed "Staged …" and exited 0 regardless. Note
    `shutil.copy` — not `copy2` — because `cp` without `-p` carries the mode
    across but not the mtime, and a stale-looking mtime is a diagnostic.
    """
    try:
        shutil.copy(src, dest)
    except OSError as e:
        raise Fail(f"cp {src} → {dest} failed: {e.strerror or e}") from None


def stage_rivet(directory: Path, repo: Path = ROOT, *, build: bool | None = None) -> Path:
    """Copy (or build then copy) the release `rivet` into a matrix directory.

    Honours `$RIVET_BIN` when it points at an executable — CI stages the DEBUG
    binary that way, since the matrices probe surface behaviour, not throughput.
    Returns the staged path.
    """
    if build is None:
        build = _want_build()

    # DEVIATION (1): no `mkdir -p`. The directory is a symlink into
    # dev/<name>_matrix created by setup_links.sh; creating it here manufactures
    # a plausible-looking stub that every downstream `-d` guard then accepts.
    if not directory.is_dir():
        raise Fail(
            f"Matrix directory missing: {directory} "
            f"(run 'python3 -m dev.pytools.matrix_common setup-links')",
            code=2,
        )

    dest = directory / "rivet"

    env_bin = os.environ.get("RIVET_BIN")
    if env_bin:
        src = Path(env_bin)
        if os.access(src, os.X_OK) and src.is_file():
            _copy_binary(src, dest)
            print(f"Staged {env_bin} → {dest}", flush=True)
            return dest
        # The shell fell through here silently, staging whatever
        # target/release/rivet happened to be lying around while the operator
        # believed they were measuring $RIVET_BIN. Same fallback, said out loud.
        warn(f"$RIVET_BIN={env_bin} is not an executable file — falling back to {repo}/target")

    binary = repo / "target" / "release" / "rivet"
    if not (binary.is_file() and os.access(binary, os.X_OK)):
        if not build:
            print(f"Release binary not found at {binary}", file=sys.stderr)
            print("Run: cargo build --bin rivet --release", file=sys.stderr)
            print(f"Or: STAGE_RIVET_BUILD=1 {PROG} stage-rivet {directory}", file=sys.stderr)
            raise Fail("release binary missing", code=2)
        # DEVIATION (2): the build's exit code was dropped, so a compile error
        # fell through to a `cp` of a file that does not exist — which also
        # printed "Staged …" and exited 0.
        stream(["cargo", "build", "--bin", "rivet", "--release"], cwd=repo, timeout=None).check(
            "cargo build --bin rivet --release"
        )
        if not (binary.is_file() and os.access(binary, os.X_OK)):
            raise Fail(f"cargo build succeeded but {binary} is still not executable")

    _copy_binary(binary, dest)
    print(f"Staged {binary} → {dest}", flush=True)
    return dest


def stage_rivet_main(argv: Sequence[str]) -> int:
    if not argv or not argv[0]:
        print("Usage: stage_rivet.sh <matrix_dir> [repo_root]", file=sys.stderr)
        return 2
    directory = Path(argv[0])
    # The shell defaulted to `dirname($0)/../../../..`, i.e. the repo root from
    # dev/matrices/_common/lib. `shell.ROOT` is that same directory, computed
    # from this file's location instead.
    repo = Path(argv[1]) if len(argv) > 1 and argv[1] else ROOT
    if len(argv) > 2:
        print(f"Unknown argument: {argv[2]}", file=sys.stderr)
        return 2
    stage_rivet(directory, repo)
    return 0


# ── setup_links.sh ─────────────────────────────────────────────────────────────
# (target, path relative to dev/matrices) — the target is relative to the LINK's
# directory, so `surface/cli → ../../cli_matrix` resolves to dev/cli_matrix.
LINKS: tuple[tuple[str, str], ...] = (
    ("../../cli_matrix", "surface/cli"),
    ("../../cfg_matrix", "surface/cfg"),
    ("../../path_matrix", "execution/path"),
    ("../../query_matrix", "execution/query"),
    ("../../cross_version_matrix", "compatibility/cross_version"),
    ("../../legacy", "compatibility/legacy"),
    ("../../soak_matrix", "resources/soak"),
)


def link(target: str, linkpath: Path) -> None:
    """`ln -sfn "$target" "$linkpath"` — replace a symlink, in place.

    DEVIATION (3): a non-symlink at `linkpath` is fatal. `ln -sfn` does not
    replace a real directory, it links INSIDE it and exits 0 — so the one
    command whose job is to repair the link tree reported success while leaving
    it broken. That state is exactly what the old `stage_rivet.sh` produced.
    """
    linkpath.parent.mkdir(parents=True, exist_ok=True)
    if linkpath.is_symlink():
        linkpath.unlink()
    elif linkpath.exists():
        raise Fail(
            f"{linkpath} exists and is not a symlink — remove it and re-run "
            f"(a stub directory here is usually left over from staging into a missing link)",
            code=2,
        )
    linkpath.symlink_to(target)
    if not (linkpath.parent / target).exists():
        # `ln -s` happily creates a dangling link. The link then fails every
        # `-d` test downstream with no hint of why, so say it here.
        warn(f"{linkpath} points at {target}, which does not exist")
    print(f"  {linkpath} → {target}", flush=True)


def setup_links(matrices_dir: Path = MATRICES_DIR) -> None:
    print("Linking matrix harnesses under dev/matrices/:", flush=True)
    for target, rel in LINKS:
        link(target, matrices_dir / rel)
    print("Done.", flush=True)


def setup_links_main(argv: Sequence[str]) -> int:
    if argv:
        print(f"Unknown argument: {argv[0]}", file=sys.stderr)
        return 2
    setup_links()
    return 0


# ── seed_pa_audit.sh ───────────────────────────────────────────────────────────
PG_CONTAINER_DEFAULT = "rivet-postgres-1"
MY_CONTAINER_DEFAULT = "rivet-mysql-1"

_PG_COUNT = ("psql", "-U", "rivet", "-d", "rivet", "-tAc", "SELECT COUNT(*) FROM pa_audit;")
# No database argument: the count is fully qualified, exactly as in the shell.
_MY_COUNT = (
    "mysql",
    "-urivet",
    "-privet",
    "-BN",
    "-e",
    "SELECT COUNT(*) FROM rivet.pa_audit;",
)


def _load_pg_fixture(container: str, sql: str, *, what: str) -> None:
    docker_exec(
        container, "psql", "-U", "rivet", "-d", "rivet", "-f", "-", stdin=sql
    ).check(what)


def _pg_count(container: str, *, what: str) -> str:
    # DEVIATION: a failed count is fatal. In the shell it lived inside `echo`'s
    # argument, where `set -e` does not reach, so it printed "pa_audit:  rows" —
    # an empty count that reads like a formatting quirk, not a broken seed.
    return _substitution_out(docker_exec(container, *_PG_COUNT).check(what).stdout)


def _load_mysql_fixture(container: str, sql: str, *, what: str) -> None:
    docker_exec(container, "mysql", "-urivet", "-privet", "rivet", stdin=sql).check(what)


def _mysql_insert_rows(container: str, n: int, *, what: str) -> None:
    """One `docker exec` per row, as in the shell.

    MySQL has no `generate_series` in any supported version, so the rows cannot
    come from the fixture file. The per-row round trip is slow and is kept
    anyway: batching them into one statement would change which rows survive a
    mid-seed failure, and these fixtures are the oracle for row-count
    assertions across the whole matrix set.
    """
    for i in range(1, n + 1):
        docker_exec(
            container,
            "mysql",
            "-urivet",
            "-privet",
            "rivet",
            "-e",
            f"INSERT INTO pa_audit (id, name) VALUES ({i}, 'row_{i}');",
        ).check(f"{what} (row {i})")


def _mysql_count(container: str, *, what: str) -> str:
    return _substitution_out(docker_exec(container, *_MY_COUNT).check(what).stdout)


def seed_pg(container: str = PG_CONTAINER_DEFAULT) -> str:
    """Seed the 30-row `pa_audit` fixture on a Postgres container."""
    _load_pg_fixture(container, (FIXTURES / "pa_audit_pg.sql").read_text(), what=f"psql {container}")
    n = _pg_count(container, what=f"pa_audit count on {container}")
    print(f"PG pa_audit: {n} rows", flush=True)
    return n


def seed_mysql(container: str = MY_CONTAINER_DEFAULT) -> str:
    """Seed the 30-row `pa_audit` fixture on a MySQL container."""
    _load_mysql_fixture(
        container, (FIXTURES / "pa_audit_mysql.sql").read_text(), what=f"mysql {container}"
    )
    _mysql_insert_rows(container, 30, what=f"pa_audit insert on {container}")
    n = _mysql_count(container, what=f"pa_audit count on {container}")
    print(f"MySQL pa_audit: {n} rows", flush=True)
    return n


def seed_pa_audit(
    engines: str = "postgres,mysql",
    *,
    pg_container: str | None = None,
    my_container: str | None = None,
) -> None:
    """Seed `pa_audit` on the primary PG and/or MySQL container."""
    pg = pg_container or os.environ.get("PG_CONTAINER") or PG_CONTAINER_DEFAULT
    my = my_container or os.environ.get("MY_CONTAINER") or MY_CONTAINER_DEFAULT

    names = engines.split(",")
    # DEVIATION (5): `--engines=` produced an empty list. On bash 3.2 (macOS,
    # the primary dev shell) expanding it under `set -u` aborted with
    # "ENGINE_LIST[@]: unbound variable"; on bash 4.4+ it seeded nothing and
    # exited 0 — a green seed step that touched no database.
    if not any(n for n in names):
        raise Fail("No engines selected (--engines= is empty)", code=2)

    for engine in names:
        if engine in ("postgres", "pg"):
            seed_pg(pg)
        elif engine in ("mysql", "my"):
            seed_mysql(my)
        else:
            # The shell had this default arm; several of its siblings did not.
            raise Fail(f"Unknown engine '{engine}'", code=2)


def seed_pa_audit_main(argv: Sequence[str]) -> int:
    engines = "postgres,mysql"
    for a in argv:
        if a.startswith("--engines="):
            engines = a.split("=", 1)[1]
        elif a in ("-h", "--help"):
            print(
                "Usage: seed_pa_audit.sh [--engines=postgres|mysql|postgres,mysql]",
                file=sys.stderr,
            )
            return 0
        else:
            print(f"Unknown argument: {a}", file=sys.stderr)
            return 2
    seed_pa_audit(engines)
    return 0


# ── seed_pa_audit_all.sh ───────────────────────────────────────────────────────
# (container, label) in the order the shell seeded them. `rivet-postgres-1` is
# the primary container and carries the newest PG, hence its pg-16 label.
PG_ALL: tuple[tuple[str, str], ...] = (
    ("rivet-postgres-12-1", "pg-12"),
    ("rivet-postgres-13-1", "pg-13"),
    ("rivet-postgres-14-1", "pg-14"),
    ("rivet-postgres-15-1", "pg-15"),
    ("rivet-postgres-1", "pg-16"),
)
MYSQL_ALL: tuple[tuple[str, str], ...] = (
    ("rivet-mysql-1", "mysql-80"),
    ("rivet-mysql-57-1", "mysql-57"),
)


def _pg_reachable(container: str) -> bool:
    return docker_exec(container, "psql", "-U", "rivet", "-d", "rivet", "-c", "SELECT 1").ok


def _mysql_reachable(container: str) -> bool:
    return docker_exec(container, "mysql", "-urivet", "-privet", "rivet", "-e", "SELECT 1").ok


def _seed_all_pg(container: str, label: str) -> bool:
    """One PG version. True = seeded or legitimately skipped.

    An unreachable container is a SKIP, not a failure: CI does not start the
    `legacy` compose profile, and the cross-version comparator treats a missing
    version as a missing data point rather than a divergence.
    """
    if not _pg_reachable(container):
        print(f"  SKIP: {label} ({container}) unreachable", flush=True)
        return True
    try:
        _load_pg_fixture(
            container, (FIXTURES / "pa_audit_pg.sql").read_text(), what=f"psql {container}"
        )
        n = _pg_count(container, what=f"pa_audit count on {container}")
    except Fail as e:
        bad(f"{label} ({container}): {e.message}")
        return False
    print(f"  {label}: pa_audit = {n} rows", flush=True)
    return True


def _seed_all_mysql(container: str, label: str) -> bool:
    if not _mysql_reachable(container):
        print(f"  SKIP: {label} ({container}) unreachable", flush=True)
        return True
    try:
        _load_mysql_fixture(
            container, (FIXTURES / "pa_audit_mysql.sql").read_text(), what=f"mysql {container}"
        )
        _mysql_insert_rows(container, 30, what=f"pa_audit insert on {container}")
        n = _mysql_count(container, what=f"pa_audit count on {container}")
    except Fail as e:
        bad(f"{label} ({container}): {e.message}")
        return False
    print(f"  {label}: pa_audit = {n} rows", flush=True)
    return True


def seed_pa_audit_all() -> None:
    """Seed `pa_audit` on every supported PG (12–16) and MySQL (5.7, 8.0).

    DEVIATION (4): failures are collected and reported. Every version is still
    attempted — a broken PG 12 should not hide a broken MySQL 5.7 — but the
    script no longer ends on an `echo` whose exit status becomes its verdict.
    """
    failures: list[str] = []

    print("Seeding all PG versions:", flush=True)
    for container, label in PG_ALL:
        if not _seed_all_pg(container, label):
            failures.append(label)

    print("Seeding all MySQL versions:", flush=True)
    for container, label in MYSQL_ALL:
        if not _seed_all_mysql(container, label):
            failures.append(label)

    if failures:
        raise Fail(f"{len(failures)} container(s) failed to seed: {', '.join(failures)}")


def seed_pa_audit_all_main(argv: Sequence[str]) -> int:
    if argv:
        print(f"Unknown argument: {argv[0]}", file=sys.stderr)
        return 2
    seed_pa_audit_all()
    return 0


# ── seed_pa_soak.sh ────────────────────────────────────────────────────────────
def seed_pa_soak(rows: int | None = None, *, pg_container: str | None = None) -> str:
    """Seed the `pa_soak` table (default 10_000 rows) on the primary PG."""
    container = pg_container or os.environ.get("PG_CONTAINER") or PG_CONTAINER_DEFAULT
    if rows is None:
        raw = os.environ.get("ROWS", "10000")
        # The shell spliced $ROWS into a `sed` REPLACEMENT, where `&` and `\1`
        # are metacharacters and a `/` ends the expression — so a non-numeric
        # ROWS silently produced a different SQL file. Validating makes the
        # literal substitution below exactly equivalent for every valid input.
        if not raw.isdigit():
            raise Fail(f"$ROWS must be a non-negative integer (got '{raw}')", code=2)
        rows = int(raw)

    sql = (FIXTURES / "pa_soak_pg.sql").read_text().replace("__ROWS__", str(rows))
    docker_exec(container, "psql", "-U", "rivet", "-d", "rivet", "-f", "-", stdin=sql).check(
        f"psql {container}"
    )
    n = _substitution_out(
        docker_exec(
            container, "psql", "-U", "rivet", "-d", "rivet", "-tAc", "SELECT COUNT(*) FROM pa_soak;"
        )
        .check(f"pa_soak count on {container}")
        .stdout
    )
    print(f"pa_soak seeded with {n} rows", flush=True)
    return n


def seed_pa_soak_main(argv: Sequence[str]) -> int:
    if argv:
        print(f"Unknown argument: {argv[0]}", file=sys.stderr)
        return 2
    seed_pa_soak()
    return 0


# ── entry point ────────────────────────────────────────────────────────────────
COMMANDS: dict[str, Callable[[Sequence[str]], int]] = {
    "stage-rivet": stage_rivet_main,
    "extract-summary": extract_summary_main,
    "normalize": normalize_main,
    "seed-pa-audit": seed_pa_audit_main,
    "seed-pa-audit-all": seed_pa_audit_all_main,
    "seed-pa-soak": seed_pa_soak_main,
    "setup-links": setup_links_main,
}

USAGE = f"""Usage: {PROG} <command> [args]

Commands:
  stage-rivet <matrix_dir> [repo_root]   Copy/build the release binary into a matrix dir
  extract-summary <workdir>              Normalized accounting snapshot of .rivet/runs
  normalize                              Paths on stdin → normalized sorted listing
  seed-pa-audit [--engines=...]          Seed pa_audit on primary PG and/or MySQL
  seed-pa-audit-all                      Seed pa_audit on PG 12-16 + MySQL 5.7/8.0
  seed-pa-soak                           Seed pa_soak on primary PG ($ROWS, default 10000)
  setup-links                            Create dev/matrices/<layer>/<name> symlinks"""


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        print(USAGE, file=sys.stderr if not args else sys.stdout)
        return 2 if not args else 0
    # A total mapping, not a `case` without a default arm: an unrecognised
    # command must not run zero work and report success.
    handler = COMMANDS.get(args[0])
    if handler is None:
        print(f"Unknown command: {args[0]}", file=sys.stderr)
        print(USAGE, file=sys.stderr)
        return 2
    return handler(args[1:])


if __name__ == "__main__":
    shell.main(lambda: main_cli())
