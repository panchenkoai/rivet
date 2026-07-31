#!/usr/bin/env python3
"""Port of the four sibling regression matrices under `dev/*_matrix/`.

One module, one subcommand per suite, because the four bash harnesses are the
same program four times: resolve `./rivet`, wipe `logs/`, loop over fixtures,
capture stdout/stderr/rc per scenario, normalize an artifact, diff it against a
committed baseline, print a fixed-width status line, exit 1 if anything
diverged. Porting them separately would have copied the shared idioms — and the
shared bugs — four more times.

    python3 -m dev.pytools.matrix_suites cli            # dev/cli_matrix/matrix.sh
    python3 -m dev.pytools.matrix_suites check-rc       # dev/cli_matrix/check_rc.sh
    python3 -m dev.pytools.matrix_suites query          # dev/query_matrix/matrix.sh
    python3 -m dev.pytools.matrix_suites path           # dev/path_matrix/matrix.sh
    python3 -m dev.pytools.matrix_suites soak           # dev/soak_matrix/matrix.sh
    python3 -m dev.pytools.matrix_suites gen-fixtures {query|path|soak}

`dev/matrices/run.sh`'s orchestrator and its message checker are already ported
in `matrices.py`; nothing here duplicates them. The four `matrix.sh` bodies, the
`check_rc.sh` baseline comparator, the three `gen_fixtures.sh` generators, and
the two shared library filters those bodies pipe through
(`_common/lib/normalize.sh`, `_common/lib/extract_summary.sh`) all live here.

Cells, order, wording, field widths and exit codes are reproduced verbatim —
including the printf column widths, so a transcript diff between the bash and
this is empty on a healthy run. Where the shell turned a broken run into a
green one, the site is marked `DEVIATION:` and the change is listed below.

DEVIATIONS (each one a place the bash could not fail):

1. **An empty fixture list was a silent pass.** All three fixture-driven suites
   loop over `$(find "$ROOT/cfg" -name '*.yaml' | sort)`. A missing/empty `cfg/`
   makes that an empty word list: every loop body is skipped, `total` stays 0,
   and the suite prints "DONE. 0 scenarios: 0 PASS, 0 FAIL, 0 NEW" and exits 0 —
   the sibling `run.sh` shape (`--tier=<typo>` → "OK: all requested matrices
   passed"), reached through an empty list instead of a subshell `exit`. Now a
   missing directory or a directory with no fixtures is fatal (exit 2).

2. **`cp … 2>/dev/null` staged the CLI configs blind.** `cli_matrix` copies
   `cfg/pg_*.yaml` into `cfg/pg/` with stderr discarded. If the copy fails (or
   the glob does not match) every PG/MySQL cell runs against an absent config,
   the rc's are uniformly wrong, and `matrix.sh` still exits 0. The configs the
   run is about to name are now verified to exist, and a failed copy is fatal.

3. **The three derived `apply` inputs were built by an unchecked `jq >file`.**
   `_stale.json` / `_drifted.json` come from `jq … plan.json > file` with no
   check on jq's presence (`cli_matrix`, unlike its siblings, never probes for
   jq), on `plan.json` existing, or on the exit code — and `>` truncates before
   jq runs. Result: an empty file, and the two *no-`--force`* cells that expect
   rc=1 got their 1 from "unparseable plan" rather than from staleness/drift.
   Both cells passed the baseline having asserted nothing. The transform is now
   done in-process (stdlib `json`, no jq), the source plan is validated, and if
   it cannot be read the dependent cells are SKIPPED (not captured) so
   `check-rc` reports them MISSING instead of grading a decoy.

4. **`logs/matrix/` was never wiped, so `check_rc` could grade a stale cell.**
   The three sibling suites `rm -rf "$LOGS"` first; `cli_matrix` only
   `mkdir -p`. A cell that failed to launch — or one deleted from the matrix —
   keeps the *previous* run's `exit_code`, which `check_rc.sh` then happily
   matches against the baseline. `cli` now wipes `logs/matrix/` first, so every
   graded rc is from this run.

5. **The soak thresholds were a gate a type change could remove.** Every bound
   is asserted with `[[ "$metric" -gt "$val" ]]`, and that arithmetic context is
   not a number parser. Two shapes, both verified against bash 3.2 (macOS) and
   bash 5 (Linux CI), neither of which grades the scenario:
   * a token that looks numeric but is not integer arithmetic — `19.5`, i.e.
     `duration_ms`/`peak_rss_mb` the day either stops being an i64 — raises
     "invalid arithmetic operator", so `[[ ]]` returns non-zero and the
     upper-bound gate reads **PASS**;
   * a token that looks like an IDENTIFIER — `jq -r` prints the bare word `null`
     for an absent field, and a typo'd `duration_ms_max=abc` is the same shape —
     is read as a variable name and, under `set -u`, **aborts the matrix
     mid-loop**: `null: unbound variable`, exit 1, no `DONE` line, and no
     mention of which scenario or which threshold file.
   One direction is a silent pass, the other an unattributable abort; only
   `total_rows_min` fails safe, and only by accident of its direction. The port
   parses metric AND bound as integers and FAILS the scenario when either is not
   one.

6. **An unknown threshold key only warned.** `case "$key"` had a `*)` arm
   printing `WARN … unknown threshold key`, then continued and passed the
   scenario. A typo'd key (`duration_ms_maxx=4000`) therefore removes the gate
   silently — the whole point of the file, absent, reported as a pass. It is now
   a scenario failure. (This is bug class 1 with a default arm that is *present*
   but not load-bearing: the arm exists and still returns success.)

7. **`while read` dropped an unterminated final line.** `check_rc.sh` and the
   soak threshold loop both read with a bare `while … read`, whose body does not
   run for a last line with no newline — so the final baseline row / final
   threshold is silently skipped. Both now process it.

8. **An unchecked EXPLAIN produced an empty "plan" that could be promoted.**
   `query_matrix` pipes into `docker exec … psql` and never looks at the result.
   Verified with a fixture whose column does not exist: psql prints
   `ERROR: column "nope_col_xyz" does not exist`, writes NOTHING to stdout, and
   **exits 0** (a script error from stdin without `ON_ERROR_STOP` is not a
   failure exit) — so the suite reported
   `NEW  no baseline (review logs/…/explain, copy to expected/…)` over a 0-byte
   file and exited 0. An operator following that instruction promotes an empty
   baseline, and the cell passes forever after. Because psql's rc is 0 here, the
   EMPTINESS test is the load-bearing half; both are checked, and either one is
   now an `EXPLAIN-FAIL` outcome that counts as a failure.

9. **A missing `docker`/`diff` blamed the database.** `docker exec … psql
   "SELECT 1" >/dev/null 2>&1` fails identically when the daemon is absent and
   when the container is down, and reports the latter. `docker` (and `diff`,
   used for the baseline diffs) are now explicit preconditions.

Faithfully KEPT, because they are not vacuous: the `%-30s`/`%-26s`/`%-32s`
column widths and every message; `check_rc`'s **string** comparison of rc's
(`"0" != "00"`); `cli`'s trailing-space `cmd` file; the fixture generators'
exact bytes; the `NEW`-on-absent-baseline outcome that exits 0 (it is how a
baseline gets promoted); and the fact that `cli`'s own exit code is 0 no matter
what the cells did — grading is `check-rc`'s job, deliberately split so the
capture survives a red baseline.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence

try:  # `python3 -m dev.pytools.matrix_suites`
    from . import shell
except ImportError:  # `python3 dev/pytools/matrix_suites.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail
atomic_write, rm_rf = shell.atomic_write, shell.rm_rf
bad, warn = shell.bad, shell.warn

DEV = ROOT / "dev"

DEFAULT_PG_URL = "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
DEFAULT_MY_URL = "mysql://rivet:rivet@127.0.0.1:3306/rivet"
DEFAULT_PG_CONTAINER = "rivet-postgres-1"

# Suite → its directory under dev/. The bash took this from `dirname "$0"`; here
# the module is one level away, so the mapping is explicit (and `--root`
# overrides it, the way `$ROOT_OVERRIDE` did for cli_matrix).
SUITE_DIRS: dict[str, str] = {
    "cli": "cli_matrix",
    "query": "query_matrix",
    "path": "path_matrix",
    "soak": "soak_matrix",
}


# ── shared plumbing ────────────────────────────────────────────────────────────
def out(line: str = "") -> None:
    """A line of the harness's REPORT.

    Kept on stdout, where the bash `printf`/`echo` put it: these lines are the
    matrix's product, wrappers grep them, and a transcript diff against the
    shell is how the port is verified. Only NEW diagnostics (the deviations) go
    to stderr via `bad`/`warn`.
    """
    print(line, flush=True)


def err(line: str) -> None:
    """A line the bash sent to stderr with `>&2` — verbatim, undecorated."""
    print(line, file=sys.stderr, flush=True)


def suite_root(suite: str, root: str | None) -> Path:
    """`ROOT="${ROOT_OVERRIDE:-$(cd "$(dirname "$0")" && pwd)}"`.

    Only `cli_matrix` honoured `$ROOT_OVERRIDE`; the flag is offered for all
    four because the suites are otherwise identical and an ad-hoc copy of one
    wants the same escape hatch.
    """
    if root:
        return Path(root).expanduser().resolve()
    if suite == "cli":
        env_root = os.environ.get("ROOT_OVERRIDE")
        if env_root:
            return Path(env_root).expanduser().resolve()
    return DEV / SUITE_DIRS[suite]


def resolve_rivet(root: Path, *, rivet_bin: str | None, hint_dir: str, cli_style: bool) -> Path:
    """`R="${RIVET_BIN:-$ROOT/rivet}"` then the two workspace fallbacks.

    The not-found message names `$ROOT/../../target/debug/rivet` — the LAST
    candidate, because each `[[ -x "$R" ]] || R=…` overwrote the variable. The
    unnormalized `/../../` is part of that string and is reproduced, so the
    message is byte-identical to the shell's.
    """
    candidates: list[str] = []
    first = rivet_bin or os.environ.get("RIVET_BIN") or f"{root}/rivet"
    candidates.append(first)
    candidates.append(f"{root}/../../target/release/rivet")
    candidates.append(f"{root}/../../target/debug/rivet")

    for c in candidates:
        p = Path(c)
        if p.is_file() and os.access(p, os.X_OK):
            return p

    shown = candidates[-1]
    if cli_style:
        raise Fail(
            f"rivet binary not found at {shown}",
            code=2,
            hint=(
                "Build it first (cargo build --bin rivet --release) and copy or symlink\n"
                "        cp target/release/rivet dev/cli_matrix/rivet"
            ),
        )
    raise Fail(
        f"rivet binary not found at {shown}",
        code=2,
        hint=(
            "Build: cargo build --bin rivet --release && "
            f"cp target/release/rivet dev/{hint_dir}/rivet"
        ),
    )


def _byte_key(value: object) -> bytes:
    """Sort key matching `sort` in the C locale — and matching macOS `sort`,
    whose UTF-8 collation is byte order anyway. The committed `path_matrix`
    layout baselines were produced under that ordering (`_SUCCESS` before
    `manifest.json` before `pa_audit_…`, which a glibc en_US.UTF-8 `sort` would
    have reversed), so the port must not "improve" it into a locale sort."""
    return str(value).encode("utf-8", "surrogateescape")


def find_fixtures(cfg: Path, *, suite: str) -> list[Path]:
    """`for yaml in $(find "$ROOT/cfg" -name '*.yaml' | sort)`.

    DEVIATION 1: an absent or empty `cfg/` was an empty word list, i.e. zero
    iterations and a 0 exit with a "DONE. 0 scenarios" line that reads like a
    pass. Both are fatal now. (`find`'s recursion and its `-name` glob are
    preserved: fixtures may sit in subdirectories, and the id is the basename.)
    """
    if not cfg.is_dir():
        raise Fail(
            f"fixture directory missing: {cfg}",
            code=2,
            hint=f"generate it: matrix_suites gen-fixtures {suite}",
        )
    found = sorted((p for p in cfg.rglob("*.yaml") if p.is_file()), key=_byte_key)
    if not found:
        raise Fail(
            f"no *.yaml fixtures under {cfg}",
            code=2,
            hint=f"generate them: matrix_suites gen-fixtures {suite}",
        )
    return found


def require_diff() -> None:
    """DEVIATION 9 (half): `diff` decides three of the four suites' verdicts. An
    absent `diff` would read as "every baseline diverged"."""
    shell.require("diff", hint="install diffutils (or Xcode command line tools)")


def diff_u(expected: Path, actual: Path, into: Path) -> bool:
    """`diff -u expected actual > into 2>&1` — True when they match.

    Shelling out to `diff` rather than using `difflib` keeps the `.diff`
    artifact byte-identical to the one operators already review. `diff`'s exit 2
    (a read error) counts as "diverged", exactly as `if diff …; then` did.
    """
    p = shell.run(["diff", "-u", str(expected), str(actual)], timeout=120)
    atomic_write(into, p.stdout + p.stderr)
    return p.ok


# ── dev/matrices/_common/lib/normalize.sh ──────────────────────────────────────
# The patterns live in `matrix_common`, imported rather than restated. They were
# duplicated here at porting time and immediately drifted: when the part stamp
# gained a millisecond field, extending one copy left the other erasing only the
# second-granularity prefix, so `path` kept diverging while `matrix_common`'s own
# normalize was already correct. One rule, one definition.


def normalize_paths(paths: Iterable[str]) -> str:
    """Timestamp-stamped path listing → stable listing.

    `sed -E 's|…|<RUNID>|g; s|…|<TS>|g' | sort`, in that order (a run id would
    otherwise be half-eaten by the `<TS>` rule). Chunk numbering is deliberately
    NOT normalized: `_chunk0/_chunk1/…` IS the contract `p03` exists to pin.
    """
    from .matrix_common import normalize_bytes

    # Delegate the WHOLE operation, not just the patterns: re-listing the regexes
    # here is what let the two copies drift in the first place, and `sort` is part
    # of the contract too (C collation, on bytes). `normalize_bytes` already does
    # substitution + sort + terminator.
    joined = "\n".join(paths).encode("utf-8", "surrogateescape")
    return normalize_bytes(joined).decode("utf-8", "surrogateescape")


def walk_layout(work: Path, *, exclude_name: str = "rivet.yaml") -> list[str]:
    """`find . -mindepth 1 -not -name 'rivet.yaml' | sed 's|^\\./||'`, run from
    `work`. Directories are listed as entries in their own right (that is how
    the baselines pin `out/p06/nested/run` as a created directory), symlinks are
    listed but not followed, and the name filter excludes matching entries at
    any depth without pruning their subtree."""
    entries: list[str] = []
    for dirpath, dirnames, filenames in os.walk(work, followlinks=False):
        base = Path(dirpath)
        for name in list(dirnames) + list(filenames):
            if name == exclude_name:
                continue
            entries.append(str((base / name).relative_to(work)))
    return entries


# ── dev/matrices/_common/lib/extract_summary.sh ────────────────────────────────
def _jq_raw(value: object) -> str:
    """Render one JSON scalar the way `jq -r` string interpolation does: `null`
    for absent/null (which is exactly the token the soak gate then had to cope
    with), unquoted strings, integers without a decimal point."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return value
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


SUMMARY_FIELDS: tuple[str, ...] = (
    "export_name",
    "status",
    "format",
    "compression",
    "total_rows",
    "files_produced",
)
_SUMMARY_LABELS: tuple[str, ...] = (
    "export",
    "status",
    "format",
    "compression",
    "total_rows",
    "files_produced",
)


def extract_summary(work: Path) -> str:
    """Every `.rivet/runs/*/summary.json` under `work` → one tab-separated
    accounting line each, sorted.

    Port of `_common/lib/extract_summary.sh`, which `path_matrix` calls through
    a two-line `exec` shim. Reading the JSON in-process removes that harness's
    dependency on jq, whose absence made the extractor exit 2 into a
    `2>/dev/null` and an empty snapshot file — a shape the baseline diff catches
    but attributes to the export rather than to the missing tool, so an
    unparseable summary now says so on stderr.
    """
    runs = work / ".rivet" / "runs"
    lines: list[str] = []
    for f in sorted((p for p in runs.rglob("summary.json") if p.is_file()), key=_byte_key):
        try:
            doc = json.loads(f.read_text(errors="surrogateescape"))
        except (OSError, ValueError) as e:
            warn(f"extract_summary: {f} unreadable ({e}) — omitted from the snapshot")
            continue
        if not isinstance(doc, dict):
            warn(f"extract_summary: {f} is not an object — omitted from the snapshot")
            continue
        lines.append(
            "\t".join(
                f"{label}={_jq_raw(doc.get(key))}"
                for label, key in zip(_SUMMARY_LABELS, SUMMARY_FIELDS)
            )
        )
    lines.sort(key=_byte_key)
    return "".join(f"{line}\n" for line in lines)


# ── dev/cli_matrix/matrix.sh ───────────────────────────────────────────────────
@dataclass
class CliMatrix:
    """The `run()` helper of `cli_matrix/matrix.sh`, plus its bookkeeping."""

    root: Path
    rivet: Path
    logs: Path
    env: dict[str, str]
    skipped: list[str] = field(default_factory=list)

    def section(self, title: str) -> None:
        out(f"================== {title} ==================")

    def cell(self, id_: str, desc: str, argv: Sequence[str]) -> int:
        """One scenario: capture argv, stdout, stderr, rc under `logs/matrix/<id>/`
        and print the summary line.

        The `cmd` artifact keeps the shell's `printf '%s '` shape — every
        argument followed by a space, then a newline, trailing space included —
        because it is committed-adjacent output people diff.
        """
        d = self.logs / id_
        d.mkdir(parents=True, exist_ok=True)
        argv = [str(a) for a in argv]
        atomic_write(d / "description", f"{desc}\n")
        atomic_write(d / "cmd", "".join(f"{a} " for a in argv) + "\n")

        # timeout=None: a cell may legitimately take minutes (a chunked run over
        # a cold database), and a timeout kill would surface as rc=124 — a
        # divergence blaming rivet for the harness's impatience.
        p = shell.run(argv, env=self.env, cwd=self.root, timeout=None)

        atomic_write(d / "stdout", p.stdout)
        atomic_write(d / "stderr", p.stderr)
        atomic_write(d / "exit_code", f"{p.returncode}\n")
        osize = (d / "stdout").stat().st_size  # `wc -c < …`
        esize = (d / "stderr").stat().st_size
        out(
            f"{id_:<30} rc={p.returncode!s:<3} "
            f"stdout={osize!s:<6} stderr={esize!s:<6}  {desc}"
        )
        return p.returncode

    def cell_skipped(self, id_: str, why: str) -> None:
        """DEVIATION 3: a cell whose INPUT could not be built is not captured.

        Leaving no `exit_code` behind makes `check-rc` report `MISSING <id>`,
        which is the truth — the alternative (running it against a truncated
        file) is what let two `apply` cells match the baseline for the wrong
        reason.
        """
        self.skipped.append(id_)
        bad(f"SKIPPED  {id_}: {why}")

    # ── derived apply inputs ───────────────────────────────────────────────────
    def derive_apply_inputs(self) -> bool:
        """`_stale.json`, `_drifted.json`, `_corrupt.json`.

        DEVIATION 3: the bash built the first two with
        `jq '…' pg_plan_incr/plan.json > $LM/_stale.json` — no jq probe (this
        suite, unlike `query`/`soak`, never checks for it), no check that the
        plan exists, no check on the exit code, and `>` truncates the target
        before jq is even exec'd. The transform is a two-field assignment; doing
        it with the stdlib removes the dependency and lets the failure be loud.
        `plan_fingerprint` covers the chunk plan's fields, not the file's bytes,
        so re-serializing is safe for `apply`.
        """
        # `echo "not json {" > "$LM/_corrupt.json"` — independent of the plan,
        # and the point of the cell is that it is NOT valid JSON.
        atomic_write(self.logs / "_corrupt.json", "not json {\n")

        src = self.logs / "pg_plan_incr" / "plan.json"
        try:
            doc = json.loads(src.read_text())
        except (OSError, ValueError) as e:
            bad(f"cannot derive stale/drifted apply inputs from {src}: {e}")
            return False
        if not isinstance(doc, dict):
            bad(f"cannot derive stale/drifted apply inputs from {src}: not a JSON object")
            return False

        stale = dict(doc)
        stale["created_at"] = "2020-01-01T00:00:00Z"
        stale["expires_at"] = "2020-01-02T00:00:00Z"
        atomic_write(self.logs / "_stale.json", json.dumps(stale, indent=2, ensure_ascii=False) + "\n")

        drifted = dict(doc)
        # `.computed.cursor_snapshot = "99999"` — jq creates the container when
        # it is absent or null, so mirror that rather than raising on it.
        computed = drifted.get("computed")
        computed = dict(computed) if isinstance(computed, dict) else {}
        computed["cursor_snapshot"] = "99999"
        drifted["computed"] = computed
        atomic_write(
            self.logs / "_drifted.json", json.dumps(drifted, indent=2, ensure_ascii=False) + "\n"
        )
        return True


def _stage_cli_configs(root: Path) -> dict[str, Path]:
    """Per-engine config dirs so the two state DBs cannot collide (audit F2).

    DEVIATION 2: `cp "$ROOT"/cfg/pg_*.yaml "$ROOT/cfg/pg/" 2>/dev/null` hid both
    a failed copy and a glob that matched nothing. Every PG cell would then run
    against a config that is not there — a full matrix of uniformly wrong rc's,
    reported at exit 0. The copy is checked and the seven configs the cells name
    are verified present before the first cell runs.
    """
    cfg = root / "cfg"
    for engine in ("pg", "my"):
        (cfg / engine).mkdir(parents=True, exist_ok=True)
        sources = sorted(cfg.glob(f"{engine}_*.yaml"), key=_byte_key)
        if not sources:
            raise Fail(
                f"no {engine}_*.yaml configs under {cfg}",
                code=2,
                hint="the cli matrix ships its configs in dev/cli_matrix/cfg/",
            )
        for s in sources:
            try:
                shutil.copy(s, cfg / engine / s.name)  # `cp`: content+mode, not mtime
            except OSError as e:
                raise Fail(f"staging {s} into {cfg / engine}: {e}", code=2) from None
        # `rm -f "$ROOT"/cfg/<engine>/.rivet_state.db*` — a re-runnable matrix
        # must not resume a previous run's cursors.
        for db in (cfg / engine).glob(".rivet_state.db*"):
            db.unlink(missing_ok=True)

    wanted = {
        "PGC": cfg / "pg" / "pg_full.yaml",
        "PGI": cfg / "pg" / "pg_incremental.yaml",
        "PGK": cfg / "pg" / "pg_chunked.yaml",
        "PGP": cfg / "pg" / "pg_param.yaml",
        "MYC": cfg / "my" / "my_full.yaml",
        "MYI": cfg / "my" / "my_incremental.yaml",
        "MYK": cfg / "my" / "my_chunked.yaml",
    }
    absent = [str(p) for p in wanted.values() if not p.is_file()]
    if absent:
        raise Fail(
            "cli matrix configs missing after staging: " + ", ".join(absent),
            code=2,
            hint="restore dev/cli_matrix/cfg/*.yaml",
        )
    return wanted


def cli(*, root: str | None = None, rivet_bin: str | None = None) -> int:
    """`dev/cli_matrix/matrix.sh` — the CLI surface matrix.

    Saves stdout/stderr/exit code per scenario under `logs/matrix/<id>/`; one
    summary line per scenario on stdout. Grading against `expected_rc.txt` is
    `check-rc`'s job, so this returns 0 whatever the cells did — the split is
    deliberate: a red baseline must not stop the capture.
    """
    r = suite_root("cli", root)
    R = resolve_rivet(r, rivet_bin=rivet_bin, hint_dir="cli_matrix", cli_style=True)
    lm = r / "logs" / "matrix"

    # DEVIATION 4: wipe first, like the three sibling suites. `mkdir -p` alone
    # left a cell that never ran holding the PREVIOUS run's exit_code, which
    # check_rc.sh then matched against the baseline and called a pass.
    rm_rf(lm)
    lm.mkdir(parents=True, exist_ok=True)

    pg_url = os.environ.get("PG_URL") or DEFAULT_PG_URL
    my_url = os.environ.get("MY_URL") or DEFAULT_MY_URL
    env = {
        "PG_URL": pg_url,
        "MY_URL": my_url,
        # DATABASE_URL is what `rivet init` defaults to (no --source-env).
        "DATABASE_URL": pg_url,
    }

    cfgs = _stage_cli_configs(r)
    PGC, PGI, PGK, PGP = cfgs["PGC"], cfgs["PGI"], cfgs["PGK"], cfgs["PGP"]
    MYC, MYI, MYK = cfgs["MYC"], cfgs["MYI"], cfgs["MYK"]

    m = CliMatrix(root=r, rivet=R, logs=lm, env=env)
    # `env -u PG_URL "$R" …` is kept as a literal argv rather than a filtered
    # environment: the `cmd` artifact then still records the unset, and the
    # scenario is about the CLI's behaviour with the variable gone.
    unset_pg = ["env", "-u", "PG_URL"]

    m.section("doctor")
    m.cell("pg_doctor_ok", "doctor PG happy", [R, "doctor", "-c", PGC])
    m.cell("my_doctor_ok", "doctor MySQL happy", [R, "doctor", "-c", MYC])
    m.cell("pg_doctor_unset", "doctor PG with env unset", [*unset_pg, R, "doctor", "-c", PGC])

    m.section("check")
    m.cell("pg_check_full", "check PG full", [R, "check", "-c", PGC])
    m.cell("my_check_full", "check MySQL full", [R, "check", "-c", MYC])
    m.cell("pg_check_incr", "check PG incremental", [R, "check", "-c", PGI])
    m.cell("pg_check_chunked", "check PG chunked", [R, "check", "-c", PGK])
    m.cell(
        "pg_check_unknown",
        "check PG --export unknown",
        [R, "check", "-c", PGC, "--export", "does_not_exist"],
    )
    m.cell("pg_check_type_report", "check PG --type-report", [R, "check", "-c", PGC, "--type-report"])
    m.cell(
        "pg_check_type_report_json",
        "check PG --type-report --json",
        [R, "check", "-c", PGC, "--json"],
    )
    m.cell("pg_check_strict", "check PG --strict", [R, "check", "-c", PGC, "--strict"])
    m.cell(
        "pg_check_target_bq",
        "check PG --target bigquery",
        [R, "check", "-c", PGC, "--target", "bigquery"],
    )

    m.section("run")
    m.cell("pg_run_full", "run PG full", [R, "run", "-c", PGC])
    m.cell("pg_run_incr", "run PG incremental", [R, "run", "-c", PGI])
    m.cell("pg_run_chunked", "run PG chunked", [R, "run", "-c", PGK])
    m.cell("pg_run_validate", "run PG full --validate", [R, "run", "-c", PGC, "--validate"])
    m.cell("pg_run_reconcile", "run PG full --reconcile", [R, "run", "-c", PGC, "--reconcile"])
    m.cell(
        "pg_run_val_rec",
        "run PG full --validate --reconcile",
        [R, "run", "-c", PGC, "--validate", "--reconcile"],
    )
    m.cell("pg_run_json", "run PG full --json", [R, "run", "-c", PGC, "--json"])
    m.cell(
        "pg_run_summary",
        "run PG full --summary-output ...",
        [R, "run", "-c", PGC, "--summary-output", lm / "pg_run_summary" / "agg.json"],
    )
    m.cell(
        "pg_run_force_no_resume", "run PG --force without --resume", [R, "run", "-c", PGC, "--force"]
    )
    m.cell(
        "pg_run_resume_no_chk",
        "run PG --resume without chunk_checkpoint",
        [R, "run", "-c", PGC, "--resume"],
    )
    m.cell(
        "pg_run_parallel_single",
        "run PG --parallel-exports (single export)",
        [R, "run", "-c", PGC, "--parallel-exports"],
    )
    m.cell(
        "pg_run_unknown",
        "run PG --export unknown",
        [R, "run", "-c", PGC, "--export", "does_not_exist"],
    )
    m.cell("pg_run_idempotent", "run PG full SECOND time (idempotency)", [R, "run", "-c", PGC])

    m.cell("my_run_full", "run MySQL full", [R, "run", "-c", MYC])
    m.cell("my_run_incr", "run MySQL incremental", [R, "run", "-c", MYI])
    m.cell("my_run_chunked", "run MySQL chunked", [R, "run", "-c", MYK])
    m.cell("my_run_validate", "run MySQL full --validate", [R, "run", "-c", MYC, "--validate"])
    m.cell("my_run_reconcile", "run MySQL full --reconcile", [R, "run", "-c", MYC, "--reconcile"])
    m.cell("my_run_json", "run MySQL full --json", [R, "run", "-c", MYC, "--json"])

    m.section("plan")
    m.cell(
        "pg_plan_full",
        "plan PG full --format json -o",
        [R, "plan", "-c", PGC, "-e", "pa_audit", "--format", "json", "-o", lm / "pg_plan_full" / "plan.json"],
    )
    m.cell(
        "pg_plan_incr",
        "plan PG incremental --format json -o",
        [R, "plan", "-c", PGI, "-e", "pa_audit", "--format", "json", "-o", lm / "pg_plan_incr" / "plan.json"],
    )
    m.cell(
        "pg_plan_chunked",
        "plan PG chunked --format json -o",
        [R, "plan", "-c", PGK, "-e", "pa_audit", "--format", "json", "-o", lm / "pg_plan_chunked" / "plan.json"],
    )
    m.cell(
        "pg_plan_pretty",
        "plan PG full --format pretty",
        [R, "plan", "-c", PGC, "-e", "pa_audit", "--format", "pretty"],
    )
    m.cell(
        "pg_plan_json_stdout",
        "plan PG full --format json (no -o)",
        [R, "plan", "-c", PGC, "-e", "pa_audit", "--format", "json"],
    )
    m.cell("pg_plan_default", "plan PG full (no flags)", [R, "plan", "-c", PGC, "-e", "pa_audit"])
    m.cell(
        "pg_plan_unknown",
        "plan PG --export unknown",
        [R, "plan", "-c", PGC, "-e", "does_not_exist", "--format", "json"],
    )
    m.cell(
        "pg_plan_bad_format",
        "plan PG --format invalid",
        [R, "plan", "-c", PGC, "-e", "pa_audit", "--format", "invalid"],
    )
    m.cell(
        "pg_plan_json_errors",
        "plan PG --json-errors",
        [R, "--json-errors", "plan", "-c", PGC, "-e", "does_not_exist", "--format", "json"],
    )
    m.cell(
        "pg_plan_param_used",
        "plan PG with --param matching ${max_id} placeholder",
        [R, "plan", "-c", PGP, "-e", "pa_audit", "--param", "max_id=20", "--format", "json"],
    )
    m.cell(
        "pg_plan_param_unused",
        "plan PG with --param unused (warning expected once)",
        [R, "plan", "-c", PGC, "-e", "pa_audit", "--param", "unused=value", "--format", "json"],
    )

    m.cell(
        "my_plan_full",
        "plan MySQL full --format json -o",
        [R, "plan", "-c", MYC, "-e", "pa_audit", "--format", "json", "-o", lm / "my_plan_full" / "plan.json"],
    )
    m.cell(
        "my_plan_incr",
        "plan MySQL incremental --format json -o",
        [R, "plan", "-c", MYI, "-e", "pa_audit", "--format", "json", "-o", lm / "my_plan_incr" / "plan.json"],
    )
    m.cell(
        "my_plan_chunked",
        "plan MySQL chunked --format json -o",
        [R, "plan", "-c", MYK, "-e", "pa_audit", "--format", "json", "-o", lm / "my_plan_chunked" / "plan.json"],
    )

    m.section("apply")
    # Re-use plans from plan step
    m.cell("pg_apply_full", "apply PG full", [R, "apply", lm / "pg_plan_full" / "plan.json"])
    m.cell("pg_apply_incr", "apply PG incremental", [R, "apply", lm / "pg_plan_incr" / "plan.json"])
    m.cell("pg_apply_chunked", "apply PG chunked", [R, "apply", lm / "pg_plan_chunked" / "plan.json"])
    m.cell("pg_apply_missing", "apply missing file", [R, "apply", "/tmp/no_such_plan_xyz.json"])
    m.cell("pg_apply_no_arg", "apply (no arg)", [R, "apply"])

    # Build derived stale + drifted from pg_plan_incr
    derived = m.derive_apply_inputs()
    if derived:
        m.cell("pg_apply_stale_no_force", "apply stale (no --force)", [R, "apply", lm / "_stale.json"])
        m.cell("pg_apply_stale_force", "apply --force stale", [R, "apply", "--force", lm / "_stale.json"])
        m.cell(
            "pg_apply_drift_no_force", "apply drifted (no --force)", [R, "apply", lm / "_drifted.json"]
        )
        m.cell(
            "pg_apply_drift_force",
            "apply --force drifted (F1)",
            [R, "apply", "--force", lm / "_drifted.json"],
        )
    else:
        for id_ in (
            "pg_apply_stale_no_force",
            "pg_apply_stale_force",
            "pg_apply_drift_no_force",
            "pg_apply_drift_force",
        ):
            m.cell_skipped(id_, "derived plan input could not be built from pg_plan_incr")
    m.cell("pg_apply_corrupt", "apply corrupt JSON", [R, "apply", lm / "_corrupt.json"])
    m.cell(
        "pg_apply_json_errors",
        "apply --json-errors",
        [R, "--json-errors", "apply", "/tmp/no_such_plan_xyz.json"],
    )

    m.cell("my_apply_full", "apply MySQL full", [R, "apply", lm / "my_plan_full" / "plan.json"])
    m.cell("my_apply_incr", "apply MySQL incremental", [R, "apply", lm / "my_plan_incr" / "plan.json"])
    m.cell("my_apply_chunked", "apply MySQL chunked", [R, "apply", lm / "my_plan_chunked" / "plan.json"])
    # Frozen v0.7.5 plan must still apply with --force (stale by definition once
    # committed). Catches plan-schema regressions visible only at apply-time.
    m.cell(
        "pg_apply_legacy_frozen",
        "apply frozen v0.7.5 plan --force",
        [R, "apply", "--force", f"{r}/../../tests/fixtures/artifacts_legacy/v0_7_5_plan_full.json"],
    )

    m.section("state")
    m.cell("pg_state_help", "state --help", [R, "state", "--help"])
    m.cell("pg_state_files", "state files (PG cfg)", [R, "state", "files", "-c", PGC])
    m.cell("pg_state_progression", "state progression", [R, "state", "progression", "-c", PGC])
    m.cell("my_state_files", "state files (MySQL cfg)", [R, "state", "files", "-c", MYC])
    m.cell("pg_state_show", "state show (PG)", [R, "state", "show", "-c", PGC])
    m.cell(
        "pg_state_reset", "state reset (PG)", [R, "state", "reset", "-c", PGC, "--export", "pa_audit"]
    )
    m.cell(
        "pg_state_reset_unknown",
        "state reset (unknown export)",
        [R, "state", "reset", "-c", PGC, "--export", "nope"],
    )
    m.cell(
        "pg_state_reset_chunks_stuck",
        "state reset-chunks --stuck-checkpoints",
        [R, "state", "reset-chunks", "-c", PGK, "--stuck-checkpoints"],
    )
    m.cell(
        "pg_state_files_export",
        "state files --export pa_audit",
        [R, "state", "files", "-c", PGC, "--export", "pa_audit", "--last", "5"],
    )

    m.section("metrics")
    m.cell("pg_metrics", "metrics PG", [R, "metrics", "-c", PGC])
    m.cell(
        "pg_metrics_export", "metrics PG --export", [R, "metrics", "-c", PGC, "--export", "pa_audit"]
    )
    m.cell("my_metrics", "metrics MySQL", [R, "metrics", "-c", MYC])

    m.section("schema")
    m.cell("schema_help", "schema --help", [R, "schema", "--help"])
    m.cell("schema_config", "schema config", [R, "schema", "config"])

    m.section("journal")
    m.cell("pg_journal", "journal PG", [R, "journal", "-c", PGC, "--export", "pa_audit"])
    m.cell("my_journal", "journal MySQL", [R, "journal", "-c", MYC, "--export", "pa_audit"])
    m.cell(
        "pg_journal_last1",
        "journal PG --last 1",
        [R, "journal", "-c", PGC, "--export", "pa_audit", "--last", "1"],
    )
    m.cell(
        "pg_journal_unknown_export",
        "journal PG --export unknown",
        [R, "journal", "-c", PGC, "--export", "does_not_exist"],
    )
    m.cell(
        "pg_journal_missing_run_id",
        "journal PG --run-id missing",
        [R, "journal", "-c", PGC, "--export", "pa_audit", "--run-id", "RIVET_NO_SUCH_RUN_ID"],
    )

    m.section("validate")
    m.cell("pg_validate", "validate PG", [R, "validate", "-c", PGC])
    m.cell("my_validate", "validate MySQL", [R, "validate", "-c", MYC])

    m.section("reconcile")
    m.cell("pg_reconcile", "reconcile PG (no chunk_checkpoint)", [R, "reconcile", "-c", PGC])
    m.cell("pg_reconcile_chk", "reconcile PG chunked", [R, "reconcile", "-c", PGK])

    m.section("repair")
    m.cell("pg_repair", "repair PG", [R, "repair", "-c", PGK])

    m.section("init")
    (lm / "_init").mkdir(parents=True, exist_ok=True)
    m.cell(
        "pg_init_table",
        "init PG --table users",
        [R, "init", "--source-env", "PG_URL", "--table", "users", "-o", lm / "_init" / "pg_users.yaml"],
    )
    m.cell(
        "my_init_table",
        "init MySQL --table users",
        [R, "init", "--source-env", "MY_URL", "--table", "users", "-o", lm / "_init" / "my_users.yaml"],
    )
    m.cell(
        "pg_init_unset",
        "init env unset",
        [
            *unset_pg,
            R,
            "init",
            "--source-env",
            "PG_URL",
            "--table",
            "users",
            "-o",
            lm / "_init" / "pg_unset.yaml",
        ],
    )
    m.cell(
        "pg_init_no_table",
        "init no --table",
        [R, "init", "--source-env", "PG_URL", "-o", lm / "_init" / "pg_no_table.yaml"],
    )

    out()
    # `ls "$LM" | grep -v '^_' | wc -l` — scenario dirs, excluding the derived
    # `_stale.json`/`_drifted.json`/`_corrupt.json`/`_init` scratch (and, as
    # `ls` does, anything dot-prefixed).
    captured = sum(
        1 for p in lm.iterdir() if not p.name.startswith("_") and not p.name.startswith(".")
    )
    out(f"DONE.  {captured} scenarios captured in {lm}")
    if m.skipped:
        bad(
            f"{len(m.skipped)} scenario(s) NOT captured: {', '.join(m.skipped)} "
            "— check-rc will report them MISSING"
        )
    return 0


# ── dev/cli_matrix/check_rc.sh ─────────────────────────────────────────────────
def _read_two_fields(line: str) -> tuple[str, str]:
    """`IFS=' ' read -r id expected_rc`.

    Space is IFS whitespace, so leading runs are skipped and the field split is
    on runs; the LAST variable takes the remainder verbatim minus trailing IFS
    whitespace (a third column would land inside `expected_rc`, and then fail
    the string comparison — which is the shell's behaviour and is kept). A tab
    is NOT a separator under `IFS=' '`, so it stays part of the field.
    """
    s = line.lstrip(" ")
    if not s:
        return "", ""
    head, sep, rest = s.partition(" ")
    if not sep:
        return head, ""
    return head, rest.lstrip(" ").rstrip(" ")


def check_rc(*, root: str | None = None) -> int:
    """`dev/cli_matrix/check_rc.sh` — per-scenario rc vs `expected_rc.txt`.

    Exit 0 → every scenario's rc matches the baseline.
    Exit 1 → at least one rc diverged (or a scenario was not captured).
    Exit 2 → the baseline or the logs are absent.

    Intentional changes: regenerate `expected_rc.txt` in the same PR that lands
    the behavior change, and document it in that PR's CHANGELOG entry. CI
    failure on uncommitted divergence is the regression guard.
    """
    r = suite_root("cli", root)
    baseline = r / "expected_rc.txt"
    logs = r / "logs" / "matrix"

    if not baseline.is_file():
        err("expected_rc.txt missing — run matrix.sh and bootstrap the baseline.")
        return 2
    if not logs.is_dir():
        err("logs/matrix/ missing — run matrix.sh first.")
        return 2

    failures = 0
    checked = 0
    missing = 0

    text = baseline.read_text(errors="surrogateescape")
    lines = text.split("\n")
    # DEVIATION 7: bash's `while read` does not run its body for a final line
    # with no trailing newline, silently dropping that assertion. Only a real
    # trailing newline is discarded here.
    if lines and lines[-1] == "":
        lines.pop()

    for raw in lines:
        id_, expected = _read_two_fields(raw)
        if not id_:
            continue
        rc_file = logs / id_ / "exit_code"
        if not rc_file.is_file():
            err(f"MISSING  {id_} (expected rc={expected})")
            missing += 1
            continue
        # `actual=$(cat …)` — command substitution strips trailing newlines, and
        # the comparison below is a STRING compare: "0" != "00", "0" != " 0".
        actual = rc_file.read_text(errors="surrogateescape").rstrip("\n")
        checked += 1
        if actual != expected:
            desc_file = logs / id_ / "description"
            desc = (
                desc_file.read_text(errors="surrogateescape").rstrip("\n")
                if desc_file.is_file()
                else ""
            )
            err(f"DIVERGED {id_}: rc {expected} → {actual}  ({desc})")
            failures += 1

    if failures == 0 and missing == 0:
        out(f"OK: {checked} scenarios match baseline.")
        return 0

    err("")
    err(f"{failures} scenario(s) diverged, {missing} scenario(s) missing.")
    err("If the change is intentional: regenerate expected_rc.txt and document in CHANGELOG.")
    return 1


# ── dev/query_matrix/matrix.sh ─────────────────────────────────────────────────
def _normalize_explain(raw: str) -> str:
    """`sed -e 's/[[:space:]]*$//' -e '/^$/d'`.

    Trailing whitespace off each line, empty lines dropped. `[[:space:]]` in the
    C locale — NOT Python's `\\s`, which also eats non-ASCII whitespace the sed
    would have left in the plan text. EXPLAIN's `->` prefixes and indentation
    are kept: they ARE the contract this matrix pins.
    """
    keep = []
    for line in raw.split("\n"):
        line = line.rstrip(" \t\r\v\f")
        if line:
            keep.append(line)
    return "".join(f"{line}\n" for line in keep)


def query(*, root: str | None = None, rivet_bin: str | None = None, pg_container: str | None = None) -> int:
    """`dev/query_matrix/matrix.sh` — what the planner actually sends to the DB.

    For each YAML: `rivet plan` → materialized `base_query` (including `${VAR}` /
    `--param` substitution) → `EXPLAIN (COSTS OFF)` on PostgreSQL → normalized
    plan → diff against `expected/<id>.plan`. Catches regressions that stay at
    rc=0 and message-OK while changing what hits the database: an operator query
    regressed into a subquery wrapper, a lost PK index, a chunked rewrite turned
    into OFFSET/LIMIT. `COSTS OFF` strips per-environment cost numbers; the
    residual variance is small and accepted into the baseline.
    """
    r = suite_root("query", root)
    R = resolve_rivet(r, rivet_bin=rivet_bin, hint_dir="query_matrix", cli_style=False)
    logs = r / "logs"
    expected_dir = r / "expected"

    # The bash required jq here only because it shelled out to it for one field;
    # that field is read with the stdlib now, so the dependency is gone. `docker`
    # and `diff` genuinely decide the verdict, so they are checked instead —
    # DEVIATION 9: an absent docker made the probe below fail identically to a
    # stopped container, and reported the container.
    shell.require("docker", hint="install Docker / OrbStack and start the daemon")
    require_diff()

    pg_url = os.environ.get("PG_URL") or DEFAULT_PG_URL
    container = pg_container or os.environ.get("PG_CONTAINER") or DEFAULT_PG_CONTAINER
    env = {"PG_URL": pg_url}

    if not shell.docker_exec(
        container, "psql", "-U", "rivet", "-d", "rivet", "-c", "SELECT 1", timeout=60
    ).ok:
        raise Fail(f"PG container '{container}' not reachable", code=2)

    fixtures = find_fixtures(r / "cfg", suite="query")

    rm_rf(logs)
    logs.mkdir(parents=True, exist_ok=True)

    fail = 0
    passed = 0
    new = 0
    total = 0

    for yaml in fixtures:
        sid = yaml.name[: -len(".yaml")]
        total += 1
        d = logs / sid
        d.mkdir(parents=True, exist_ok=True)

        # 1. plan → JSON
        p = shell.run(
            [str(R), "plan", "-c", str(yaml), "-e", "pa_audit", "--format", "json"],
            env=env,
            cwd=r,
            timeout=None,
        )
        atomic_write(d / "plan.json", p.stdout)
        atomic_write(d / "plan.stderr", p.stderr)
        if p.returncode != 0:
            out(f"{sid:<32}  PLAN-FAIL rc={p.returncode} (see logs/{sid}/plan.stderr)")
            fail += 1
            continue

        # 2. extract base_query (`jq -r '.resolved_plan.base_query'`, in-process:
        # an absent field renders as the literal "null", which the guard below
        # rejects exactly as the shell's did).
        try:
            doc = json.loads(p.stdout)
        except ValueError:
            doc = None
        resolved = doc.get("resolved_plan") if isinstance(doc, dict) else None
        base_query = _jq_raw(resolved.get("base_query")) if isinstance(resolved, dict) else ""
        if not base_query or base_query == "null":
            out(f"{sid:<32}  NO-QUERY base_query missing from plan.json")
            fail += 1
            continue
        atomic_write(d / "base_query.sql", f"{base_query}\n")

        # 3. EXPLAIN (COSTS OFF). The query goes in on stdin so no arbitrary SQL
        # is ever re-parsed by a shell.
        ex = shell.docker_exec(
            container,
            "psql",
            "-U",
            "rivet",
            "-d",
            "rivet",
            "-qAtX",
            stdin=f"EXPLAIN (COSTS OFF)\n{base_query};\n",
            timeout=300,
        )
        atomic_write(d / "explain.raw", ex.stdout)
        atomic_write(d / "explain.stderr", ex.stderr)

        # 4. Normalize.
        explain = _normalize_explain(ex.stdout)
        atomic_write(d / "explain", explain)

        # DEVIATION 8: the shell never looked at psql's result. A failed EXPLAIN
        # left an EMPTY plan, which reads as "diverged" against a baseline
        # (misattributed) and as "NEW" without one — and an empty file copied
        # into expected/ passes forever. An empty plan is not a plan.
        if not ex.ok or not explain:
            reason = f"rc={ex.returncode}" if not ex.ok else "empty plan"
            out(f"{sid:<32}  EXPLAIN-FAIL {reason} (see logs/{sid}/explain.stderr)")
            fail += 1
            continue

        # 5. Compare against baseline.
        baseline = expected_dir / f"{sid}.plan"
        if baseline.is_file():
            if diff_u(baseline, d / "explain", d / "explain.diff"):
                out(f"{sid:<32}  PASS    EXPLAIN matches baseline")
                passed += 1
            else:
                out(f"{sid:<32}  FAIL    EXPLAIN diverged (see logs/{sid}/explain.diff)")
                fail += 1
        else:
            out(
                f"{sid:<32}  NEW     no baseline "
                f"(review logs/{sid}/explain, copy to expected/{sid}.plan)"
            )
            new += 1

    out()
    out(f"DONE.  {total} scenarios: {passed} PASS, {fail} FAIL, {new} NEW")
    return 1 if fail > 0 else 0


# ── dev/path_matrix/matrix.sh ──────────────────────────────────────────────────
def path(*, root: str | None = None, rivet_bin: str | None = None) -> int:
    """`dev/path_matrix/matrix.sh` — on-disk layout + data accounting.

    Each scenario runs in an isolated workdir so `.rivet_state.db` and
    `.rivet/runs/` stay scenario-local; the destination `path:` in each YAML is
    `./out/<id>…`, resolved relative to that workdir. After the run the layout is
    normalized (timestamps → `<TS>`/`<RUNID>`, chunk numbers KEPT) and the
    accounting fields are pulled from every `summary.json`; both are diffed
    against `expected/<id>.layout` / `expected/<id>.summary` when those exist,
    and printed for promotion when they do not.

    The summary snapshot is the matrix-level counterpart to
    `tests/live_reconcile_repair.rs`: a code path that exports 0 rows where 30
    were expected stays at rc=0 and only the snapshot diverges.
    """
    r = suite_root("path", root)
    R = resolve_rivet(r, rivet_bin=rivet_bin, hint_dir="path_matrix", cli_style=False)
    logs = r / "logs"
    expected_dir = r / "expected"
    require_diff()

    env = {
        "PG_URL": os.environ.get("PG_URL") or DEFAULT_PG_URL,
        "MY_URL": os.environ.get("MY_URL") or DEFAULT_MY_URL,
    }

    fixtures = find_fixtures(r / "cfg", suite="path")

    rm_rf(logs)
    logs.mkdir(parents=True, exist_ok=True)

    fail = 0
    passed = 0
    new = 0
    total = 0

    for yaml in fixtures:
        sid = yaml.name[: -len(".yaml")]
        total += 1
        d = logs / sid
        work = d / "work"
        work.mkdir(parents=True, exist_ok=True)
        # Copy the YAML into the workdir so all relative paths resolve there.
        shutil.copy(yaml, work / "rivet.yaml")

        # Capture stdout/stderr/rc.
        p = shell.run([str(R), "run", "-c", "rivet.yaml"], env=env, cwd=work, timeout=None)
        atomic_write(d / "stdout", p.stdout)
        atomic_write(d / "stderr", p.stderr)
        rc = p.returncode
        atomic_write(d / "exit_code", f"{rc}\n")

        # Walk the workdir and emit a layout listing relative to it.
        atomic_write(d / "layout", normalize_paths(walk_layout(work)))

        # Scenarios where rc != 0 (run failed) skip the accounting snapshot.
        atomic_write(d / "summary", extract_summary(work) if rc == 0 else "")

        # Compare against baselines if they exist.
        layout_status = "NEW"
        if (expected_dir / f"{sid}.layout").is_file():
            layout_status = (
                "OK"
                if diff_u(expected_dir / f"{sid}.layout", d / "layout", d / "layout.diff")
                else "DIVERGED"
            )
        summary_status = "NEW"
        if (expected_dir / f"{sid}.summary").is_file():
            summary_status = (
                "OK"
                if diff_u(expected_dir / f"{sid}.summary", d / "summary", d / "summary.diff")
                else "DIVERGED"
            )

        if layout_status == "DIVERGED" or summary_status == "DIVERGED":
            out(
                f"{sid:<32}  rc={rc!s:<3}  FAIL    "
                f"layout={layout_status} summary={summary_status} (see logs/{sid}/*.diff)"
            )
            fail += 1
        elif layout_status == "NEW" or summary_status == "NEW":
            out(
                f"{sid:<32}  rc={rc!s:<3}  NEW     "
                f"layout={layout_status} summary={summary_status} (review logs/{sid}/, copy baselines)"
            )
            new += 1
        else:
            out(f"{sid:<32}  rc={rc!s:<3}  PASS    layout+summary match expected")
            passed += 1

    out()
    out(f"DONE.  {total} scenarios: {passed} PASS, {fail} FAIL, {new} NEW (no baseline yet)")
    return 1 if fail > 0 else 0


# ── dev/soak_matrix/matrix.sh ──────────────────────────────────────────────────
SOAK_METRICS: tuple[str, ...] = ("total_rows", "duration_ms", "peak_rss_mb", "files_produced")


def _parse_threshold_line(line: str) -> tuple[str, str] | None:
    """`while IFS='=' read -r key val` + `[[ -z "$key" || "$key" =~ ^# ]]`.

    With `IFS='='` nothing is stripped: `key` is everything before the first
    `=`, `val` everything after (further `=` stay in `val`). A comment is only
    skipped when `#` is the FIRST character — an indented `  # note` becomes a
    key, which the unknown-key arm then reports.
    """
    key, _, val = line.partition("=")
    if not key or key.startswith("#"):
        return None
    return key, val


def _as_int(raw: str) -> int | None:
    """A metric or threshold as an integer, or None when it is not one.

    DEVIATION 5: bash compared these with `[[ … -gt … ]]`. `19.5` is an
    "invalid arithmetic operator" error there, which makes an upper-bound gate
    return false — a PASS. `null` (jq's rendering of an absent field) and `abc`
    are identifier-shaped, so `set -u` aborts the whole matrix on them instead.
    Neither outcome grades the scenario; None is now a failure.
    """
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        return None


def soak(*, root: str | None = None, rivet_bin: str | None = None) -> int:
    """`dev/soak_matrix/matrix.sh` — soak / load regression guard.

    For each YAML under `cfg/`, runs `rivet run` against a 10 000-row PG table,
    captures `duration_ms` + `peak_rss_mb` + `total_rows` + `files_produced` from
    `summary.json`, and compares against `expected/<id>.thresholds`:

        total_rows_min=10000      hard lower bound — row count regression
        duration_ms_max=5000      hard upper bound — perf regression
        peak_rss_mb_max=200       hard upper bound — memory regression

    Thresholds are intentionally generous (2-3x a healthy local run) so CI
    runners with varying perf characteristics do not flap: the guard is for
    ORDER-OF-MAGNITUDE regressions ("now 50x slower / 10x memory"), not
    micro-tuning — those belong in `cargo bench` and a dedicated nightly job.

    Bring up PG first:
        docker compose up -d postgres
        ./seed.sh
    """
    r = suite_root("soak", root)
    R = resolve_rivet(r, rivet_bin=rivet_bin, hint_dir="soak_matrix", cli_style=False)
    logs = r / "logs"
    expected_dir = r / "expected"
    # The bash required jq (for four scalar reads) and computed a `start_ts` and
    # a `PATH_EXTRACT` it never used; the reads are stdlib now and the two dead
    # assignments are dropped — the per-scenario duration comes from
    # summary.json's own `duration_ms`, which is what the gate compares.

    env = {"PG_URL": os.environ.get("PG_URL") or DEFAULT_PG_URL}

    fixtures = find_fixtures(r / "cfg", suite="soak")

    rm_rf(logs)
    logs.mkdir(parents=True, exist_ok=True)

    fail = 0
    passed = 0
    new = 0
    total = 0

    for yaml in fixtures:
        sid = yaml.name[: -len(".yaml")]
        total += 1
        d = logs / sid
        work = d / "work"
        work.mkdir(parents=True, exist_ok=True)
        shutil.copy(yaml, work / "rivet.yaml")

        p = shell.run([str(R), "run", "-c", "rivet.yaml"], env=env, cwd=work, timeout=None)
        atomic_write(d / "stdout", p.stdout)
        atomic_write(d / "stderr", p.stderr)
        rc = p.returncode
        atomic_write(d / "exit_code", f"{rc}\n")

        if rc != 0:
            out(f"{sid:<26}  RUN-FAIL rc={rc}")
            fail += 1
            continue

        # Extract the first (and only) summary.json's metrics. The bash took
        # `find … | head -1`, i.e. traversal order; sorted here so a scenario
        # that ever grows a second export picks the same one twice.
        summaries = sorted(
            (q for q in (work / ".rivet" / "runs").rglob("summary.json") if q.is_file()),
            key=_byte_key,
        )
        if not summaries:
            out(f"{sid:<26}  NO-SUMMARY (summary.json missing)")
            fail += 1
            continue
        try:
            doc = json.loads(summaries[0].read_text(errors="surrogateescape"))
        except ValueError:
            doc = None
        if not isinstance(doc, dict):
            doc = {}
        metrics = {k: _jq_raw(doc.get(k)) for k in SOAK_METRICS}
        total_rows = metrics["total_rows"]
        duration_ms = metrics["duration_ms"]
        peak_rss_mb = metrics["peak_rss_mb"]
        atomic_write(
            d / "metrics", "".join(f"{k}={metrics[k]}\n" for k in SOAK_METRICS)
        )

        thresholds = expected_dir / f"{sid}.thresholds"
        if not thresholds.is_file():
            out(
                f"{sid:<26}  NEW    rows={total_rows} dur={duration_ms}ms rss={peak_rss_mb}MB "
                f"(no thresholds yet; copy from logs/{sid}/metrics)"
            )
            new += 1
            continue

        scenario_fail = False
        lines = thresholds.read_text(errors="surrogateescape").split("\n")
        # DEVIATION 7: see check_rc — a threshold on an unterminated last line
        # was silently skipped, i.e. a gate that does not exist.
        if lines and lines[-1] == "":
            lines.pop()

        for line in lines:
            parsed = _parse_threshold_line(line)
            if parsed is None:
                continue
            key, val = parsed
            if key in ("total_rows_min", "duration_ms_max", "peak_rss_mb_max"):
                metric_name = key.rsplit("_", 1)[0]
                metric_raw = metrics[metric_name]
                got = _as_int(metric_raw)
                want = _as_int(val)
                if want is None:
                    # DEVIATION 5: an identifier-shaped bound (`=abc`) aborted
                    # the run under `set -u`; a fractional one passed the gate.
                    err(f"  FAIL {sid}: threshold {key}='{val}' is not an integer (gate absent)")
                    scenario_fail = True
                    continue
                if got is None:
                    # DEVIATION 5: a fractional metric passed both upper bounds;
                    # a `null` one aborted the matrix. Unmeasurable is a failure.
                    err(
                        f"  FAIL {sid}: {metric_name}={metric_raw} is not an integer "
                        f"({key} cannot be evaluated)"
                    )
                    scenario_fail = True
                    continue
                if key == "total_rows_min":
                    if got < want:
                        err(f"  FAIL {sid}: total_rows={metric_raw} < total_rows_min={val}")
                        scenario_fail = True
                elif key == "duration_ms_max":
                    if got > want:
                        err(
                            f"  FAIL {sid}: duration_ms={metric_raw} > duration_ms_max={val} "
                            "(perf regression)"
                        )
                        scenario_fail = True
                else:
                    if got > want:
                        err(
                            f"  FAIL {sid}: peak_rss_mb={metric_raw} > peak_rss_mb_max={val} "
                            "(memory regression)"
                        )
                        scenario_fail = True
            else:
                # DEVIATION 6: the `*)` arm printed WARN and passed the
                # scenario, so a typo'd key (`duration_ms_maxx`) removed the
                # gate the file exists to impose. An unreadable threshold is a
                # failed threshold.
                err(f"  FAIL {sid}: unknown threshold key '{key}' (gate would be silently absent)")
                scenario_fail = True

        if not scenario_fail:
            out(f"{sid:<26}  PASS   rows={total_rows} dur={duration_ms}ms rss={peak_rss_mb}MB")
            passed += 1
        else:
            out(
                f"{sid:<26}  FAIL   rows={total_rows} dur={duration_ms}ms rss={peak_rss_mb}MB "
                "(see above)"
            )
            fail += 1

    out()
    out(f"DONE.  {total} scenarios: {passed} PASS, {fail} FAIL, {new} NEW")
    return 1 if fail > 0 else 0


# ── gen_fixtures.sh × 3 ────────────────────────────────────────────────────────
# The fixture bodies are byte-identical to the shell heredocs (verified by
# generating both and diffing), comments included — the comments ARE the
# scenario's statement of intent and several baselines only make sense with them.

QUERY_FIXTURES: tuple[tuple[str, str], ...] = (
    (
        "q01_full_scan.yaml",
        """# Full scan of the table — should plan as Seq Scan (no WHERE filter).
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q01 }
""",
    ),
    (
        "q02_pk_filter.yaml",
        """# PK-bounded query — should plan as Index Scan on the id PK (or Seq Scan
# with Filter, but never as Bitmap Heap on a 30-row fixture). The exact
# plan body is the contract.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE id <= 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q02 }
""",
    ),
    (
        "q03_non_index_filter.yaml",
        """# Non-indexed predicate — should plan as Seq Scan with Filter. A regression
# that adds a "helpful" subquery / CTE wrapper changes the EXPLAIN shape.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE name LIKE 'row_1%'"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q03 }
""",
    ),
    (
        "q04_order_by_pk.yaml",
        """# Ordering on the PK — small tables may sort in-memory or use the index.
# Pin whichever PG 16 picks for the fixture; future regression that wraps
# the ORDER BY in a Materialize / Sort step diverges.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit ORDER BY id"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q04 }
""",
    ),
    (
        "q05_aggregate.yaml",
        """# Aggregate — should plan as Aggregate over Seq Scan. Catches "did the
# planner push down predicate" / "did we wrap in a subquery" regressions.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT COUNT(*)::INTEGER AS n FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q05 }
""",
    ),
)

PATH_FIXTURES: tuple[tuple[str, str], ...] = (
    (
        "p01_full_parquet_local.yaml",
        """source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p01 }
""",
    ),
    (
        "p02_full_csv_local.yaml",
        """source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: csv
    destination: { type: local, path: ./out/p02 }
""",
    ),
    (
        "p03_chunked_multifile.yaml",
        """source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/p03 }
""",
    ),
    (
        "p04_multi_export.yaml",
        """source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit_a
    query: "SELECT id, name FROM pa_audit WHERE id <= 15"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p04_a }
  - name: pa_audit_b
    query: "SELECT id, name FROM pa_audit WHERE id > 15"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p04_b }
""",
    ),
    (
        "p05_stdout.yaml",
        """source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE id <= 5"
    mode: full
    format: csv
    destination: { type: stdout }
""",
    ),
    (
        "p06_subdir_path.yaml",
        """# A nested relative path. We assert that the leaf directory ./out/p06/nested/run
# is created (not just ./out/p06), so any future change that strips intermediate
# path components from `destination.path` is caught.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p06/nested/run }
""",
    ),
    (
        "p07_run_summary_landing.yaml",
        """# Same shape as p01 — fixture exists so an assertion can be pinned to a
# scenario whose only purpose is to verify .rivet/runs/<RUNID>/summary.*
# landed alongside the config (not in CWD or out/).
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE id <= 5"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p07 }
""",
    ),
)

SOAK_FIXTURES: tuple[tuple[str, str], ...] = (
    (
        "s01_full_10k.yaml",
        """source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_soak
    query: "SELECT id, name, payload FROM pa_soak"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/s01 }
""",
    ),
    (
        "s02_chunked_10k.yaml",
        """source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_soak
    table: pa_soak
    mode: chunked
    chunk_column: id
    chunk_size: 1000
    format: parquet
    destination: { type: local, path: ./out/s02 }
""",
    ),
    (
        "s03_incremental_10k.yaml",
        """source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_soak
    query: "SELECT id, name, payload, updated_at FROM pa_soak"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: { type: local, path: ./out/s03 }
""",
    ),
)

# suite → (fixtures, the noun its `echo` used)
FIXTURE_SETS: dict[str, tuple[tuple[tuple[str, str], ...], str]] = {
    "query": (QUERY_FIXTURES, "query-shape"),
    "path": (PATH_FIXTURES, "path-matrix"),
    "soak": (SOAK_FIXTURES, "soak"),
}


def gen_fixtures(suite: str, *, root: str | None = None) -> int:
    """`dev/<suite>_matrix/gen_fixtures.sh` — regenerate every YAML under `cfg/`.

    Idempotent, and each file goes through `atomic_write`: the shell's
    `cat > "$path"` truncated the target before the heredoc was written, so an
    interrupted regeneration left a half-written fixture that the NEXT run's
    baseline diff reported as legitimate drift.

    The trailing count is `find "$CFG" -name '*.yaml' | wc -l` — everything
    present afterwards, including any stale fixture this generator no longer
    writes, exactly as before (that discrepancy is how you notice one).
    """
    if suite not in FIXTURE_SETS:
        # See deviation 1's family: a `case` with no default arm returns 0, and
        # a generator that writes nothing while exiting 0 is indistinguishable
        # from one that worked.
        raise Fail(
            f"Unknown fixture suite '{suite}' (expected {', '.join(sorted(FIXTURE_SETS))})", code=2
        )
    fixtures, noun = FIXTURE_SETS[suite]
    r = suite_root(suite, root)
    cfg = r / "cfg"
    for name, body in fixtures:
        atomic_write(cfg / name, body)
    count = sum(1 for p in cfg.rglob("*.yaml") if p.is_file())
    out(f"Generated {count} {noun} fixtures under {cfg}")
    return 0


# ── CLI ────────────────────────────────────────────────────────────────────────
USAGE = """Usage: matrix_suites <suite> [OPTIONS]

Ports of the four sibling regression matrices under dev/*_matrix/.

Suites:
  cli                     dev/cli_matrix/matrix.sh    — CLI surface, rc + streams
  check-rc                dev/cli_matrix/check_rc.sh  — grade cli rc's vs expected_rc.txt
  query                   dev/query_matrix/matrix.sh  — EXPLAIN shape of the planned SQL
  path                    dev/path_matrix/matrix.sh   — on-disk layout + row accounting
  soak                    dev/soak_matrix/matrix.sh   — duration / RSS / rows thresholds
  gen-fixtures SUITE      regenerate cfg/ for query|path|soak

Options:
  --root=DIR              Matrix directory (default dev/<suite>_matrix; cli also
                          honours $ROOT_OVERRIDE)
  --rivet-bin=PATH        Binary under test (default $RIVET_BIN, then
                          <root>/rivet, then target/release, then target/debug)
  --pg-container=NAME     query only: PG container to EXPLAIN in
                          (default $PG_CONTAINER, then rivet-postgres-1)
  -h, --help              Show this help

Environment: PG_URL, MY_URL, RIVET_BIN, PG_CONTAINER, ROOT_OVERRIDE (cli).

Exit codes: 0 pass · 1 a scenario diverged · 2 a precondition failed."""


@dataclass
class _Args:
    suite: str = ""
    positional: tuple[str, ...] = ()
    root: str | None = None
    rivet_bin: str | None = None
    pg_container: str | None = None


class _Usage(Exception):
    def __init__(self, code: int, message: str | None = None) -> None:
        super().__init__(message or "")
        self.code = code
        self.message = message


def _parse_args(argv: Sequence[str]) -> _Args:
    a = _Args()
    rest: list[str] = []
    for arg in argv:
        if arg in ("-h", "--help"):
            raise _Usage(0)
        if arg.startswith("--root="):
            a.root = arg.split("=", 1)[1]
        elif arg.startswith("--rivet-bin="):
            a.rivet_bin = arg.split("=", 1)[1]
        elif arg.startswith("--pg-container="):
            a.pg_container = arg.split("=", 1)[1]
        elif arg.startswith("-"):
            raise _Usage(2, f"Unknown argument: {arg}")
        else:
            rest.append(arg)
    if not rest:
        raise _Usage(2, "A suite is required.")
    a.suite = rest[0]
    a.positional = tuple(rest[1:])
    return a


def main_cli(argv: Sequence[str] | None = None) -> int:
    """Dispatch one suite. An unknown suite is fatal — the whole point of
    deviation 1 is that "did nothing" must never read as "passed"."""
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        a = _parse_args(args)
    except _Usage as u:
        if u.message:
            print(u.message, file=sys.stderr)
        print(USAGE, file=sys.stderr if u.code else sys.stdout)
        return u.code

    if a.suite in ("cli", "matrix"):
        return cli(root=a.root, rivet_bin=a.rivet_bin)
    if a.suite in ("check-rc", "check_rc"):
        return check_rc(root=a.root)
    if a.suite == "query":
        return query(root=a.root, rivet_bin=a.rivet_bin, pg_container=a.pg_container)
    if a.suite == "path":
        return path(root=a.root, rivet_bin=a.rivet_bin)
    if a.suite == "soak":
        return soak(root=a.root, rivet_bin=a.rivet_bin)
    if a.suite in ("gen-fixtures", "gen_fixtures"):
        if len(a.positional) != 1:
            raise Fail(
                "gen-fixtures needs exactly one suite "
                f"({', '.join(sorted(FIXTURE_SETS))})",
                code=2,
            )
        return gen_fixtures(a.positional[0], root=a.root)

    raise Fail(
        f"Unknown suite '{a.suite}' (expected cli, check-rc, query, path, soak, gen-fixtures)",
        code=2,
    )


if __name__ == "__main__":
    shell.main(lambda: main_cli())
