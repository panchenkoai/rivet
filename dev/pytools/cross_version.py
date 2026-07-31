#!/usr/bin/env python3
"""Port of `dev/cross_version_matrix/matrix.sh` + `check_cross.sh`.

The cross-version suite: probe a small, fixed set of commands against every
supported Postgres (12–16) and MySQL (5.7, 8.0), record the exit code — and the
row count — per (version, scenario), then assert every reachable version of a
family AGREES. A divergence means one end of the supported range regressed.

    python3 -m dev.pytools.cross_version matrix   # probe, write logs/
    python3 -m dev.pytools.cross_version check    # compare, exit 1 on divergence
    python3 -m dev.pytools.cross_version all      # both

The smoke set is deliberately tiny (5 PG probes × 5 versions, 3 MySQL × 2) so a
CI gate finishes in ~30 s: running the ~85-scenario `cli_matrix` per version
would produce 700+ cells nobody can diff. Bring the databases up first:

    docker compose --profile legacy up -d postgres-12 postgres-13 postgres-14 \\
        postgres-15 mysql-57
    docker compose up -d postgres mysql
    python3 -m dev.pytools.matrix_common seed-pa-audit-all

Reproduced faithfully — same probes, same order, same log layout, same messages
and column alignment, same exit codes — except where the shell reported a pass
having measured nothing. Marked `DEVIATION:` at each site; the summary:

1. The port came from `sed -E 's|.*@[^:]+:([0-9]+)/.*|\\1|'` over the URL, and
   `sed` prints the line UNCHANGED when the pattern does not match. So an
   unparseable URL made `$port` the whole URL, `nc -z 127.0.0.1 <url>` failed,
   and the version was recorded as `SKIP` — which `check_cross.sh` treats as a
   missing data point, not a failure. A whole version silently dropping out of
   the comparison is the one outcome this matrix cannot afford. Now fatal.
2. Reachability used `nc -z`, and a missing `nc` exits 127 — indistinguishable
   from "port closed". On a host without netcat EVERY version skipped and the
   gate passed green having probed nothing. Now a socket connect.
3. `case "$cmd"` had no default arm, so a probe added to the `for` list without
   a matching arm would record `rc=0` (the status of an unmatched `case`) with
   no stdout/stderr files at all — a fabricated pass. Now a total mapping that
   raises on an unknown probe.
4. `total_rows` was extracted only `if command -v jq` — so on a host without jq
   the file was never written, and the row-count half of the comparator (the
   half that catches a version silently losing rows at rc=0) skipped itself in
   silence. JSON is parsed natively now, so the file always exists.
5. `find … | head -1` picked an arbitrary summary from unsorted `find` output.
   Now sorted.
6. The per-(version, mode) `run` workdir was never cleaned between INVOCATIONS,
   only between iterations of one invocation. So a previous run's
   `.rivet/runs/*/summary.json` was still there for `find` to pick up, and — the
   sharp edge — a `total_rows` file from an earlier successful run SURVIVED a
   later failing one, letting the comparator read a stale row count as current
   evidence of agreement. The workdir and `total_rows` are now cleared first.

Left as-is, deliberately: `matrix.sh` always exits 0 (recording a per-version rc
IS its job — `check_cross` is the gate), `logs/` accumulates across invocations
so a partial run does not erase another family's results, and the "at least one
version succeeded" sanity check covers PG only.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Callable, Sequence

try:  # `python3 -m dev.pytools.cross_version`
    from . import shell
except ImportError:  # `python3 dev/pytools/cross_version.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

try:
    from . import matrix_common
except ImportError:  # pragma: no cover - direct-script fallback
    import matrix_common  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail
run = shell.run
jq_raw = matrix_common.jq_raw
bytekey = matrix_common.bytekey

MATRIX_DIR: Path = ROOT / "dev" / "cross_version_matrix"

# ── probes ─────────────────────────────────────────────────────────────────────
# A minimal full-mode config templated with @@TYPE@@/@@URL@@ that the runner
# rewrites per version — keeps the scenarios DRY. Byte-identical to the
# heredocs, because the rendered file lands in logs/ where an operator diffs it.
TEMPLATE_FLAT = """source:
  type: @@TYPE@@
  url: "@@URL@@"
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/_probe }
"""

TEMPLATE_CHUNKED = """source:
  type: @@TYPE@@
  url: "@@URL@@"
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/_probe_chunked }
"""

TEMPLATES: dict[str, str] = {"": TEMPLATE_FLAT, "_chunked": TEMPLATE_CHUNKED}
KINDS: tuple[str, ...] = ("", "_chunked")

# Probe → argv builder. A total mapping (deviation 3): the shell's `case` had no
# default arm, and an unmatched `case` returns 0 — a probe with no arm would have
# been recorded as a pass with no output captured.
PROBE_ARGV: dict[str, Callable[[Path, Path], list[str]]] = {
    "doctor": lambda binary, cfg: [str(binary), "doctor", "-c", str(cfg)],
    "check": lambda binary, cfg: [str(binary), "check", "-c", str(cfg)],
    "plan_full": lambda binary, cfg: [
        str(binary),
        "plan",
        "-c",
        str(cfg),
        "-e",
        "pa_audit",
        "--format",
        "json",
    ],
}
PROBES: tuple[str, ...] = ("doctor", "check", "plan_full")

# (version label, source type, url) in probe order, with the section headers the
# shell printed between the families.
PG_VERSIONS: tuple[tuple[str, str, str], ...] = (
    ("pg-12", "postgres", "postgresql://rivet:rivet@127.0.0.1:5412/rivet"),
    ("pg-13", "postgres", "postgresql://rivet:rivet@127.0.0.1:5413/rivet"),
    ("pg-14", "postgres", "postgresql://rivet:rivet@127.0.0.1:5414/rivet"),
    ("pg-15", "postgres", "postgresql://rivet:rivet@127.0.0.1:5415/rivet"),
    ("pg-16", "postgres", "postgresql://rivet:rivet@127.0.0.1:5432/rivet"),
)
MYSQL_VERSIONS: tuple[tuple[str, str, str], ...] = (
    ("mysql-57", "mysql", "mysql://rivet:rivet@127.0.0.1:3357/rivet"),
    ("mysql-80", "mysql", "mysql://rivet:rivet@127.0.0.1:3306/rivet"),
)

# The comparator's probe list and order, including the `_chunked` variants.
PROBE_ORDER: tuple[str, ...] = (
    "doctor",
    "check",
    "plan_full",
    "run",
    "doctor_chunked",
    "check_chunked",
    "plan_full_chunked",
    "run_chunked",
)

PG_LABELS: tuple[str, ...] = tuple(v for v, _, _ in PG_VERSIONS)
MYSQL_LABELS: tuple[str, ...] = tuple(v for v, _, _ in MYSQL_VERSIONS)


# ── matrix.sh ──────────────────────────────────────────────────────────────────
def resolve_binary(matrix_root: Path) -> Path:
    """`$RIVET_BIN`, else the matrix dir's copy, else the workspace build.

    Falls back to `target/release` then `target/debug` — no per-matrix binary
    copy is needed (those copies were 14 MB of stale artifacts each before the
    dev/ cleanup). Note the shell fell through a SET-but-not-executable
    `$RIVET_BIN` too, and named the final candidate in its error; both kept.

    The repo root is derived as `<matrix_root>/../..` resolved, which is what the
    kernel did with the shell's `$ROOT/../../target/...`: when the harness runs
    through the `dev/matrices/compatibility/cross_version` symlink, `..` steps
    out of the LINK TARGET (dev/cross_version_matrix), landing on dev/ and then
    the repo root — the same place as a direct invocation.
    """
    repo = (matrix_root / ".." / "..").resolve()
    candidates: list[Path] = []
    env_bin = os.environ.get("RIVET_BIN")
    candidates.append(Path(env_bin) if env_bin else matrix_root / "rivet")
    candidates.append(repo / "target" / "release" / "rivet")
    candidates.append(repo / "target" / "debug" / "rivet")

    chosen = candidates[0]
    for c in candidates:
        chosen = c
        if c.is_file() and os.access(c, os.X_OK):
            return c

    print(f"rivet binary not found at {chosen}", file=sys.stderr)
    print(
        "Build: cargo build --bin rivet --release && "
        "cp target/release/rivet dev/cross_version_matrix/rivet",
        file=sys.stderr,
    )
    raise Fail("rivet binary not found", code=2)


_PORT_RE = re.compile(r".*@[^:]+:([0-9]+)/.*")


def url_port(url: str) -> int:
    """The port from a `scheme://user:pass@host:port/db` URL.

    DEVIATION (1): an unparseable URL is fatal. `sed` echoed the line back
    unchanged on no match, so `$port` became the whole URL — the reachability
    probe then failed and the version was recorded `SKIP`, which the comparator
    ignores. A typo in a URL removed a version from the guard silently.
    """
    m = _PORT_RE.match(url)
    if not m:
        raise Fail(f"cannot extract a port from source url '{url}'", code=2)
    return int(m.group(1))


def write_probe(kind: str, type_: str, url: str, logs: Path) -> Path:
    """Render `_template<kind>.yaml` → `_probe<kind>.yaml` for one version.

    `str.replace` rather than `sed -e "s|@@URL@@|$url|"`: the shell's `|`
    delimiter survived these URLs, but an `&` or a `\\1` in a substituted value
    would have been interpreted, and a `|` would have ended the expression.
    """
    template = logs / f"_template{kind}.yaml"
    probe = logs / f"_probe{kind}.yaml"
    text = template.read_text().replace("@@TYPE@@", type_).replace("@@URL@@", url)
    shell.atomic_write(probe, text)
    return probe


def write_templates(logs: Path) -> None:
    for kind, text in TEMPLATES.items():
        shell.atomic_write(logs / f"_template{kind}.yaml", text)


def _record(directory: Path, p: shell.Proc) -> int:
    """Persist one probe's stdout / stderr / exit code, as the redirects did."""
    directory.mkdir(parents=True, exist_ok=True)
    matrix_common.write_text(directory / "stdout", p.stdout)
    matrix_common.write_text(directory / "stderr", p.stderr)
    shell.atomic_write(directory / "exit_code", f"{p.returncode}\n")
    return p.returncode


def _extract_total_rows(work: Path, rundir: Path) -> None:
    """`jq -r '.total_rows'` over the run's summary, for the row-count guard.

    DEVIATION (4): no `command -v jq` gate — without jq the file was never
    written and the comparator skipped the row-count comparison entirely.
    DEVIATION (5): the summary is chosen from a SORTED list; `find | head -1`
    took whichever `find` happened to yield first.

    The rendering is jq's, including `null` for an absent key and the empty file
    the shell's `|| echo ''` fallback left behind on a parse error.
    """
    runs = work / ".rivet" / "runs"
    summaries = matrix_common.find_summaries(work) or []
    if not runs.is_dir() or not summaries:
        return
    summary = summaries[0]
    try:
        doc = json.loads(summary.read_bytes().decode("utf-8", "surrogateescape"))
        value = doc.get("total_rows") if isinstance(doc, dict) else None
        text = f"{jq_raw(value)}\n"
    except (OSError, ValueError):
        text = "\n"  # `|| echo '' > "$rundir/total_rows"`
    shell.atomic_write(rundir / "total_rows", text)


def probe_version(
    ver: str, type_: str, url: str, *, binary: Path, logs: Path, matrix_root: Path
) -> None:
    """Run every probe for one database version, recording rc (and rows).

    A version whose port is closed is a clean SKIP: the `legacy` compose profile
    is not started in CI, and `check_cross` treats a skipped version as a
    missing data point rather than a failure.
    """
    vlog = logs / ver
    vlog.mkdir(parents=True, exist_ok=True)

    port = url_port(url)
    # DEVIATION (2): a socket connect instead of `nc -z`, whose 127 for "netcat
    # is not installed" was indistinguishable from "port closed" — turning every
    # version into a SKIP on such a host, and the whole gate into a no-op.
    if not shell.tcp_open("127.0.0.1", port):
        print(f"{ver}: SKIP (port {port} unreachable)", flush=True)
        shell.atomic_write(vlog / "_status", "skip\n")
        return
    shell.atomic_write(vlog / "_status", "ok\n")

    for kind in KINDS:
        cfg = write_probe(kind, type_, url, logs)

        # Each probe: doctor + check + plan + run. rc, stdout and stderr are
        # captured per (version, probe); `run` additionally yields total_rows, so
        # the comparator can assert every version produced the SAME row count
        # from the SAME source query — pa_audit is 30 rows on every seeded
        # version. (On `apply`: it needs the config_path stored inside the plan
        # artifact to resolve the password / state DB location (ADR-0005 + F13),
        # and replaying that per (version, mode) is gymnastics that drown the
        # signal. Apply is pinned at the artifact level by
        # tests/artifact_legacy_compat.rs and at the CLI level by
        # dev/cli_matrix/pg_apply_* against the primary PG pair; the
        # cross-version dimension here is row-count correctness of `rivet run`.)
        for cmd in PROBES:
            argv = PROBE_ARGV[cmd](binary, cfg)
            # cwd is the matrix dir — where `run.sh` invoked the harness from —
            # so a probe that touches a relative path behaves as it did.
            _record(vlog / f"{cmd}{kind}", run(argv, cwd=matrix_root, timeout=None))

        # `rivet run` end to end. Each (version, mode) gets its own workdir so
        # .rivet/runs/ and the output dir do not bleed between iterations.
        rundir = vlog / f"run{kind}"
        work = rundir / "work"
        # DEVIATION (6): clear the workdir and any previous total_rows. Neither
        # was cleaned between invocations, so a failing run could be compared
        # against the row count of an earlier successful one.
        shell.rm_rf(work)
        shell.rm_rf(rundir / "total_rows")
        work.mkdir(parents=True, exist_ok=True)
        (work / "rivet.yaml").write_text(cfg.read_text())

        run_rc = _record(
            rundir, run([str(binary), "run", "-c", "rivet.yaml"], cwd=work, timeout=None)
        )
        if run_rc == 0:
            _extract_total_rows(work, rundir)

    print(f"{ver:<12} done", flush=True)


def run_matrix(matrix_root: Path = MATRIX_DIR) -> int:
    """Probe every version, writing `logs/<version>/<probe>/`.

    Always 0 unless a precondition fails: recording a per-version rc IS the job,
    and `check_cross` is the gate that turns those rcs into a verdict.
    """
    logs = matrix_root / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    binary = resolve_binary(matrix_root)
    write_templates(logs)

    print("================ Postgres versions ================", flush=True)
    for ver, type_, url in PG_VERSIONS:
        probe_version(ver, type_, url, binary=binary, logs=logs, matrix_root=matrix_root)

    print("================ MySQL versions ===================", flush=True)
    for ver, type_, url in MYSQL_VERSIONS:
        probe_version(ver, type_, url, binary=binary, logs=logs, matrix_root=matrix_root)

    print("", flush=True)
    print(f"DONE.  Logs under {logs}/<version>/<probe>/", flush=True)
    return 0


# ── check_cross.sh ─────────────────────────────────────────────────────────────
def _read(path: Path) -> str | None:
    """`$(cat file)` — trailing newlines stripped — or None if absent."""
    try:
        return path.read_text(errors="surrogateescape").rstrip("\n")
    except OSError:
        return None


def available(versions: Sequence[str], logs: Path) -> list[str]:
    """The versions whose `_status` says `ok`, in the order given.

    (The shell's `available` took a leading `prefix` argument it never used —
    every call site passed a literal `_`. Dropped.)
    """
    return [v for v in versions if _read(logs / v / "_status") == "ok"]


def collect_rcs(probe: str, versions: Sequence[str], logs: Path) -> list[tuple[str, str]]:
    """`(version, rc)` for every version that recorded this probe."""
    pairs: list[tuple[str, str]] = []
    for v in versions:
        rc = _read(logs / v / probe / "exit_code")
        if rc is not None:
            pairs.append((v, rc))
    return pairs


def _uniq(values: Sequence[str]) -> tuple[str, int]:
    """`sort -u | tr '\\n' ' '` and its `tr -s ' ' '\\n' | grep -c .` count.

    The string keeps its trailing space, because the DIVERGED message
    interpolates it inside `{ %s}` and the OK message strips exactly one.
    The count is of non-empty whitespace-separated tokens — which is what
    `grep -c .` counted, and why an EMPTY rc file yields 0 (a divergence)
    rather than 1 (agreement).
    """
    uniq = sorted(set(values), key=bytekey)
    text = "".join(v + " " for v in uniq)
    return text, len([t for t in text.split() if t])


def _strip_one_space(s: str) -> str:
    """`${uniq% }`."""
    return s[:-1] if s.endswith(" ") else s


def check_agreement(family: str, versions: Sequence[str], logs: Path) -> int:
    """Assert every reachable version of one family agrees. Returns the count of
    divergences, as the shell's `return $fail` did.

    Two comparisons, and the second is the one that catches silent loss: exit
    codes must match per probe, AND every version that ran `rivet run` must have
    produced the SAME `total_rows` from the same source query. A version losing
    rows on a path that still exits 0 is invisible to the rc check.
    """
    fail = 0
    if len(versions) < 2:
        print(
            f"  {family}: only {len(versions)} version reachable, nothing to compare",
            flush=True,
        )
        return 0

    for probe in PROBE_ORDER:
        pairs = collect_rcs(probe, versions, logs)
        if not pairs:
            continue
        uniq, distinct = _uniq([rc for _, rc in pairs])
        if distinct == 1:
            print(f"  {family:<10} {probe:<22} OK (rc={_strip_one_space(uniq)})", flush=True)
        else:
            print(
                f"  {family:<10} {probe:<22} DIVERGED rc set: {{ {uniq}}} -- per version:",
                flush=True,
            )
            for v, rc in pairs:
                print(f"      {v:<10} rc={rc}", flush=True)
            fail += 1

    for kind in KINDS:
        rowpairs: list[tuple[str, str]] = []
        for v in versions:
            n = _read(logs / v / f"run{kind}" / "total_rows")
            if n:  # `[[ -n $n ]]` — an empty file is not a data point
                rowpairs.append((v, n))
        if not rowpairs:
            continue
        uniq_rows, distinct_rows = _uniq([n for _, n in rowpairs])
        label = f"run{kind}.total_rows"
        if distinct_rows == 1:
            print(
                f"  {family:<10} {label:<22} OK (rows={_strip_one_space(uniq_rows)})",
                flush=True,
            )
        else:
            print(
                f"  {family:<10} {label:<22} DIVERGED row counts: "
                f"{{ {uniq_rows}}} -- per version:",
                flush=True,
            )
            for v, n in rowpairs:
                print(f"      {v:<10} rows={n}", flush=True)
            fail += 1

    return fail


def check_cross(matrix_root: Path = MATRIX_DIR) -> int:
    """Compare per-scenario behaviour across versions. 0 = no divergence."""
    logs = matrix_root / "logs"
    if not logs.is_dir():
        # The shell's wording, with its relative path — a harness may grep it.
        print("logs missing — run matrix.sh first.", file=sys.stderr)
        return 2

    print("================ Cross-version agreement ===========", flush=True)
    pg_versions = available(PG_LABELS, logs)
    mysql_versions = available(MYSQL_LABELS, logs)

    pg_fail = 0
    my_fail = 0
    if pg_versions:
        pg_fail = check_agreement("PG", pg_versions, logs)
    if mysql_versions:
        my_fail = check_agreement("MySQL", mysql_versions, logs)

    print("", flush=True)
    print("================ Sanity: at least one version succeeded ==================", flush=True)
    # PG only, as in the shell: every-PG-version-fails-doctor is the
    # catastrophic shape this exists to catch, and an all-PG failure with
    # matching exit codes would otherwise read as perfect agreement.
    if pg_versions:
        if any(_read(logs / v / "doctor" / "exit_code") == "0" for v in pg_versions):
            print("  PG: at least one version reached rc=0 on doctor", flush=True)
        else:
            print("  PG: ALL versions failed doctor — catastrophic regression?", file=sys.stderr)
            pg_fail += 1

    total_fail = pg_fail + my_fail
    if total_fail == 0:
        print("", flush=True)
        print("OK: no cross-version divergence.", flush=True)
        return 0
    print("", flush=True)
    print(f"{total_fail} divergence(s) found.", file=sys.stderr)
    print(
        "If intentional (e.g. PG 12 dropped a feature): document in CHANGELOG.",
        file=sys.stderr,
    )
    return 1


# ── entry point ────────────────────────────────────────────────────────────────
USAGE = """Usage: dev/pytools/cross_version.py <matrix|check|all> [--dir PATH]

  matrix   Probe every supported PG/MySQL version, writing logs/<version>/
  check    Compare recorded behaviour across versions (exit 1 on divergence)
  all      matrix, then check

  --dir PATH   Matrix directory (default: dev/cross_version_matrix)"""


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        print(USAGE, file=sys.stderr if not args else sys.stdout)
        return 2 if not args else 0

    command = args[0]
    matrix_root = MATRIX_DIR
    rest = args[1:]
    i = 0
    while i < len(rest):
        if rest[i] == "--dir":
            if i + 1 >= len(rest):
                print("--dir requires a value", file=sys.stderr)
                return 2
            matrix_root = Path(rest[i + 1])
            i += 2
            continue
        print(f"Unknown argument: {rest[i]}", file=sys.stderr)
        return 2

    # A total mapping: an unknown command must not exit 0 having done nothing.
    if command == "matrix":
        return run_matrix(matrix_root)
    if command == "check":
        return check_cross(matrix_root)
    if command == "all":
        rc = run_matrix(matrix_root)
        return rc or check_cross(matrix_root)
    print(f"Unknown command: {command}", file=sys.stderr)
    print(USAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    shell.main(lambda: main_cli())
