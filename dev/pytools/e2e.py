"""The Rivet E2E matrix — port of `dev/e2e/run_e2e.sh` (585 lines).

Sixteen phases against a live stack (Postgres, MySQL, MinIO, fake-gcs), each
phase a handful of PASS/FAIL/SKIP cells, then a summary that exits 1 if any cell
failed. Same phases, same order, same cell labels, same exit codes.

TRANSCRIPT CONTRACT (why the report lines go to stdout, not `shell.ok/bad`)
-------------------------------------------------------------------------
Two consumers parse this script's stdout and neither may drift:

* `dev/legacy/run_full_matrix.sh` greps `^PASS:` and awks fields 2/5/8 out of the
  summary line, so `PASS: n | FAIL: n | SKIP: n` is a wire format.
* the suite's own tail greps `^FAIL` out of `/tmp/rivet_e2e_results.txt`, which
  is appended cell-by-cell (the bash `tee -a`) so it survives a mid-run abort —
  which is also why `shell.atomic_write` is wrong for it: an atomic rename at the
  end would lose exactly the transcript a crashed run needs.

So the per-cell `PASS  <label>` / `FAIL  <label>` / `SKIP  <label>` lines and the
section banners are printed verbatim to stdout, and `shell.log/warn/bad` carry
only the diagnostics this port ADDS (a failing command's exit code and stderr
tail), on stderr, where they cannot disturb either parser.

WHAT THE BASH GOT WRONG (each marked `DEVIATION:` at its site)
--------------------------------------------------------------
1. **Probes ignored the retarget env vars.** The whole point of `RIVET_PG_URL` /
   `RIVET_MYSQL_URL` is pointing one suite at any server version
   (`docs/reference/compatibility.md`: "no longer hardcodes localhost:5432"), but
   the reachability probes hardcoded `127.0.0.1:5432` and `-h 127.0.0.1` (3306).
   Against `pg-12` on 5412 the probe therefore proved nothing, and with the
   primary container down it printed `FATAL: Postgres not reachable` about a
   server the suite was never going to dial. Worse on MySQL: with 3306 down and
   3357 (the `mysql-57` target) up, every MySQL cell SKIPPED while the wrapper
   reported `PASS (n passed, m skipped)` — a whole matrix target silently not
   tested. Both probes now derive host+port from the URL, the way
   `dev/legacy/run_full_matrix.sh` already derives its own.
2. **The MySQL probe contradicted the script's own doctrine.** Lines 77-83 of the
   bash explain at length that a `docker compose exec` probe is a false positive
   when the host port is unreachable — then the very next line falls back to
   exactly that for MySQL. Replaced by the PG doctrine: a TCP connect to the
   address rivet dials, plus a real `mysqladmin ping` at that address when the
   client is on `$PATH`.
3. **`grep -q "MATCH"` also matches "MISMATCH".** `src/pipeline/summary.rs:695`
   prints `MATCH (n/m)` or `MISMATCH (exported X vs source Y)`, so the three
   `reconcile … MATCH` cells passed on a genuine reconcile MISMATCH — the exact
   failure they exist to catch. Now `(?<!MIS)MATCH`.
4. **`reconcile incremental skip` passed on a crashed run.** Absence of
   `reconcile:` in the output was read as proof the reconcile was skipped; a run
   that died before printing anything satisfied it. Now the run must exit 0.
5. **`recovery incr run2` called `pass` on BOTH branches** — a cell that could
   not fail. The run's own exit status now decides; the two messages still
   distinguish "no new data" from "ok".
6. **Four `check`-emits-no-warning cells passed when `check` never ran.** Same
   absence-of-evidence shape as #4: no output means the pattern is absent means
   the cell passes. Each now requires the report line for its own export
   (`Export: check_conn_safe`) to be present before the absence of a warning is
   allowed to mean anything.
7. **`--config /dev/stdin` made every inline-config cell exit 1** — and that is a
   PRODUCT bug the bash's grep-only verdicts hid. `dispatch_check` loads the
   config path more than once (`preflight::check`, then
   `check_plan_compatibility`), and a `/dev/stdin` that has already been read to
   EOF yields nothing on the second open — on macOS `/dev/stdin` is `/dev/fd/0`,
   which dups the fd and inherits its offset, so even bash's heredoc temp file is
   spent. So the four inline-config sections have always run a command that
   printed a complete, correct report and then exited **1** with a spurious
   `Error: config file '/dev/stdin': missing field 'source'`. Verified black-box
   three ways: heredoc → exit 1, real pipe → exit 1, same YAML as a file path →
   exit 0. Nothing was mis-asserted (the greps target load-time rejections, which
   happen on the FIRST read), but the exit code carried no information at all.
   The port writes each inline config to a temp file and removes it in a
   `finally`, which is what bash's heredoc was standing in for; the four cells now
   run the invocation form the assertions assume.
8. **`sleep 1` between the two `recovery_full` runs** was there so the two runs'
   parts got distinct filenames — but `src/pipeline/single.rs:386` stamps parts
   `%Y%m%d_%H%M%S_%3f`, i.e. milliseconds, so the sleep only guaranteed the cell
   can no longer catch a regression back to second granularity (the inverted-sleep
   class in the process rules, which cost this repo 3-of-6 lost incremental deltas). Gone.
   The `sleep 0.5` before the metrics read stays: it waits on another process's
   SQLite WAL, not on a filename stamp.
9. **The MinIO bucket was never created, and nothing said so.** `mc alias set
   _rivet …` fails on every current `mc` (an alias must begin with a letter);
   stderr went to /dev/null and the exit code was unread. `mc mb
   _rivet/rivet-e2e` then treated `_rivet` as a FILESYSTEM PATH, created
   `./_rivet/rivet-e2e/` in the repo root, and exited **0** — so the failure was
   invisible even to a `|| fail`. Phase 6 then failed two cells with
   `S3Error { code: "NoSuchBucket" }`, which reads as a rivet S3-writer bug.
   Reproduced on the bash before touching it: `FAIL S3 full upload`, `FAIL S3
   chunked upload`, plus an untracked `_rivet/` in the worktree. The port creates
   the bucket with a stdlib SigV4 `PUT` using the same credentials and endpoint
   the export config resolves, keeps the `mc` tiers as fallbacks with a legal
   alias, and gates the (unchanged) WARNING text on a real `HEAD` existence check
   instead of a tier's self-report. This also retires the bash's
   `pip install --quiet minio` tier, which mutated whatever Python was active and
   fails under PEP 668 anyway.
10. **Unknown arguments were ignored.** `bash run_e2e.sh --only-pg` ran the entire
   suite. Now usage + exit 2 (and `--only` actually exists).

Kept deliberately (bash behaviour that is defensible or load-bearing):

* `cleanup()` runs at the start and, at the end, ONLY when nothing failed — a
  failing run leaves `dev/e2e/output/` for post-mortem. Preserved.
* `dev/fixtures/test_params.yaml` writes into `dev/output/`, and every run leaves
  per-run summaries in `dev/e2e/.rivet/runs/<run_id>/`. `cleanup()` removes
  neither (it knows only `output/`, `.init_e2e_scratch` and `.rivet_state.db*`), so
  both accumulate untracked in the worktree — confirmed live after this port's own
  run. Left alone: nothing asserts on them, and silently widening a delete path is
  its own hazard. Worth a follow-up in `cleanup()`, not a side effect of a port.
* The GCS probe hits `localhost:4443` while the YAML dials `127.0.0.1:4443`. A
  probe/dial address split can only cause a loud SKIP, never a false pass, so the
  literal addresses stay as the bash had them.
* `rivet ... --resume` is still judged by `grep -i "rows.*0|success"` — weak (any
  successful transcript says "success") but not vacuous, so it is not rewritten;
  the exit code is only added to the FAIL detail.
* The MySQL init banner check greets a hardcoded `MySQL database "rivet"`, which
  a retarget to a differently-named database would fail. Unchanged: it is the
  seeded fixture's name, not a probe.

Bash bug classes checked for and NOT present: `case` with no default arm; a
two-operand `grep` feeding `tr -dc '0-9'`; `local x=$1 y="…${x}…"` on one line;
`/usr/bin/time -l`; `exit` inside `$(…)`; `grep -c healthy` matching "unhealthy".
`$?`-after-a-pipeline exists in shape (`… | head -c 100`, `… | wc -l`) but no
verdict rested on it — the port has no pipelines at all.

Usage (unchanged env surface):

    python3 dev/pytools/e2e.py
    RIVET_PG_URL=postgresql://rivet:rivet@127.0.0.1:5412/rivet \\
        RIVET_MYSQL_URL=mysql://rivet:rivet@127.0.0.1:3357/rivet \\
        python3 dev/pytools/e2e.py
    RIVET=./target/release/rivet python3 dev/pytools/e2e.py
    python3 dev/pytools/e2e.py --list
    python3 dev/pytools/e2e.py --only 3-pg-modes,11-reconcile
"""

from __future__ import annotations

import glob
import hashlib
import hmac
import os
import re
import shlex
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Sequence

if __package__:
    from . import shell
else:  # executed as a plain script: `python3 dev/pytools/e2e.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT

# Paths stay RELATIVE strings because they appear verbatim in cell labels
# ("no files: dev/e2e/output/pg_users_full_csv_*.csv"); they are resolved against
# ROOT at use, which is what the bash's `cd "$(dirname "$0")/../.."` bought.
OUT = "dev/e2e/output"
INIT_TMP = "dev/e2e/.init_e2e_scratch"
RESULTS = Path("/tmp/rivet_e2e_results.txt")

DEFAULT_PG_URL = "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
DEFAULT_MYSQL_URL = "mysql://rivet:rivet@127.0.0.1:3306/rivet"
DEFAULT_RIVET = "cargo run --release --bin rivet --"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_HEALTH = "http://localhost:9000/minio/health/live"
GCS_ENDPOINT = "http://localhost:4443"
GCS_HEALTH = "http://localhost:4443/storage/v1/b"
BUCKET = "rivet-e2e"


class _Abort(Exception):
    """The bash's `exit 1` from the Setup phase, after it has already printed its
    own multi-line FATAL block to stdout.

    Deliberately not `shell.Fail`: `shell.main` would render the message a second
    time, on stderr, *after* the diagnostics — and the whole value of that block
    is the order it prints in.
    """

    def __init__(self, code: int) -> None:
        super().__init__(f"exit {code}")
        self.code = code


# ── report ─────────────────────────────────────────────────────────────────────
@dataclass
class Tally:
    """The `pass`/`fail`/`skip` counters plus the `tee -a` results file."""

    results: Path = RESULTS
    passed: int = 0
    failed: int = 0
    skipped: int = 0

    def reset_file(self) -> None:
        self.results.unlink(missing_ok=True)

    def _emit(self, kind: str, label: str) -> None:
        line = f"{kind}  {label}"  # two spaces: `echo "PASS  $*"`
        print(line, flush=True)
        try:
            with self.results.open("a") as fh:
                fh.write(line + "\n")
        except OSError as e:  # a lost transcript must not silently lose the cell
            shell.warn(f"could not append to {self.results}: {e}")

    def ok(self, label: str) -> None:
        self.passed += 1
        self._emit("PASS", label)

    def bad(self, label: str) -> None:
        self.failed += 1
        self._emit("FAIL", label)

    def skip(self, label: str) -> None:
        self.skipped += 1
        self._emit("SKIP", label)

    def failures(self) -> list[str]:
        """The tail's `grep "^FAIL" "$RESULTS"`."""
        try:
            return [l for l in self.results.read_text().splitlines() if l.startswith("FAIL")]
        except OSError:
            return []


def section(title: str) -> None:
    print()
    print(f"══════ {title} ══════", flush=True)


def _why(p: shell.Proc) -> None:
    """ADDITION: name the reason a cell failed.

    The bash sent every command's output to `/dev/null` and its cells to stdout,
    so a FAIL was a bare label — in a ~100-cell suite, unactionable. This goes to
    stderr only, so neither transcript parser sees it.
    """
    detail = (p.stderr or p.stdout).strip().splitlines()
    tail = detail[-1] if detail else "(no output)"
    shell.warn(f"↳ exit {p.returncode}: {tail[:200]}")


# ── globbing (bash `compgen -G`) ───────────────────────────────────────────────
def matches(pattern: str) -> list[str]:
    """`compgen -G "$pattern"`, rooted at the repo like the bash's `cd` made it.

    `glob.glob` shares `compgen -G`'s two relevant properties: no match yields an
    empty list (not the literal pattern, the way an unquoted shell glob would),
    and dotfiles are excluded.
    """
    return sorted(glob.glob(str(ROOT / pattern)))


def assert_file_exists(t: Tally, pattern: str, label: str) -> None:
    if matches(pattern):
        t.ok(label)
    else:
        t.bad(f"{label} (no files: {pattern})")


def assert_file_count_ge(t: Tally, pattern: str, minimum: int, label: str) -> None:
    count = len(matches(pattern))
    if count >= minimum:
        t.ok(f"{label} (n={count})")
    else:
        t.bad(f"{label} (expected >={minimum}, got {count})")


def assert_no_file(t: Tally, pattern: str, label: str) -> None:
    if matches(pattern):
        t.bad(f"{label} (file exists)")
    else:
        t.ok(label)


def file_contains(rel: str, needle: str) -> bool:
    """`grep -q needle file` — a missing file is a non-match, as grep's exit 2 is."""
    try:
        return needle in (ROOT / rel).read_text()
    except OSError:
        return False


def count_export_names(rel: str) -> int:
    """`grep -E '^  - name:' file | wc -l`.

    Single file operand, so no `grep` filename prefix to leak into the count (the
    two-operand-plus-`tr -dc '0-9'` trap) — and a missing file counts 0, which the
    `>= 5` gate then fails loudly rather than treating as "fine".
    """
    try:
        text = (ROOT / rel).read_text()
    except OSError:
        return 0
    return sum(1 for line in text.splitlines() if re.match(r"^  - name:", line))


# ── HTTP (bash `curl -sf`) ─────────────────────────────────────────────────────
def curl_sf(
    url: str, *, method: str = "GET", data: bytes | None = None,
    headers: dict[str, str] | None = None, timeout: float = 3.0,
) -> bool:
    """`curl -sf URL`: true only on a <400 status.

    NOT `shell.http_up`, which counts 4xx as UP — right for "is an emulator
    listening", wrong here: these are `/minio/health/live`-shaped gates where a
    503 means NOT ready, and calling that UP would run the S3 phase against a
    half-started MinIO and blame rivet for the failures.
    """
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return 200 <= r.status < 400
    except urllib.error.HTTPError:
        return False
    except (urllib.error.URLError, OSError):
        return False


def address_of(url: str, default_host: str, default_port: int) -> tuple[str, int]:
    """host+port out of a DB URL, for the probes.

    DEVIATION (see docstring #1): the bash probed literal `127.0.0.1:5432` /
    `:3306` no matter where `RIVET_PG_URL` pointed. `dev/legacy/run_full_matrix.sh`
    already sed's the port out of the URL for its own probe; this does the same
    with a parser instead of a regex.
    """
    try:
        parts = urllib.parse.urlsplit(url)
        return (parts.hostname or default_host, parts.port or default_port)
    except ValueError:  # e.g. a non-numeric port — probe the default, loudly wrong
        shell.warn(f"could not parse an address out of {url!r}; probing {default_host}:{default_port}")
        return (default_host, default_port)


def probe_mysql(host: str, port: int) -> bool:
    """Is MySQL reachable AT THE ADDRESS rivet dials?

    DEVIATION (see docstring #2): the bash tried `mysqladmin -h 127.0.0.1` (no
    port) and, failing that, `docker compose exec -T mysql mysqladmin ping` — an
    INSIDE-the-container probe that the same script's own comment calls a false
    positive when the host port is unreachable. The ladder is now:

    1. no TCP connect at the URL's address → down, full stop (nothing inside a
       container can make an unpublished port reachable);
    2. `mysqladmin` on `$PATH` → a real protocol ping at that address, which is
       strictly stronger than the TCP connect;
    3. no client installed → the open port IS the verdict, exactly the doctrine
       the PG probe already uses.

    This never runs fewer cells than the bash did; where it differs it turns a
    misleading SKIP (whole matrix target untested, reported green) into a loud
    FAIL, which is the direction a test suite must err in.
    """
    if not shell.tcp_open(host, port):
        return False
    if shell.have("mysqladmin"):
        return shell.run(
            ["mysqladmin", "ping", "-h", host, "-P", str(port), "-uroot", "-privet"],
            timeout=30,
        ).ok
    return True


# ── MinIO bucket ───────────────────────────────────────────────────────────────
def _s3_request(method: str, endpoint: str, bucket: str, access_key: str, secret_key: str,
                *, region: str = "us-east-1", timeout: float = 10.0) -> int:
    """One SigV4-signed bucket-level request; returns the HTTP status (0 = no answer).

    Stdlib only, and deliberately signed with the SAME credentials and endpoint the
    export config resolves (`access_key_env: MINIO_ACCESS_KEY`, `endpoint:
    http://localhost:9000`) — so a failure here is evidence about the credentials
    rivet is about to use, not about a side-channel's own hardcoded pair.
    """
    host = urllib.parse.urlsplit(endpoint).netloc
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()

    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = "\n".join([
        method,
        f"/{bucket}",
        "",
        f"host:{host}",
        f"x-amz-content-sha256:{payload_hash}",
        f"x-amz-date:{amz_date}",
        "",
        signed_headers,
        payload_hash,
    ])
    scope = f"{stamp}/{region}/s3/aws4_request"
    to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical.encode()).hexdigest(),
    ])

    def _hmac(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    signing_key = _hmac(_hmac(_hmac(_hmac(f"AWS4{secret_key}".encode(), stamp), region), "s3"),
                        "aws4_request")
    signature = hmac.new(signing_key, to_sign.encode(), hashlib.sha256).hexdigest()

    req = urllib.request.Request(
        f"{endpoint}/{bucket}",
        data=b"" if method == "PUT" else None,
        method=method,
        headers={
            "Host": host,
            "x-amz-date": amz_date,
            "x-amz-content-sha256": payload_hash,
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            ),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return int(r.status)
    except urllib.error.HTTPError as e:
        return int(e.code)
    except (urllib.error.URLError, OSError):
        return 0


def s3_make_bucket(endpoint: str, bucket: str, access_key: str, secret_key: str) -> bool:
    """Create the bucket; an already-owned one (409) counts as success."""
    status = _s3_request("PUT", endpoint, bucket, access_key, secret_key)
    return 200 <= status < 400 or status == 409


def s3_bucket_exists(endpoint: str, bucket: str, access_key: str, secret_key: str) -> bool:
    """`HEAD /<bucket>`: 200 = there, 404 = NoSuchBucket."""
    return 200 <= _s3_request("HEAD", endpoint, bucket, access_key, secret_key) < 400


def ensure_minio_bucket() -> None:
    """Make sure `rivet-e2e` exists on MinIO, and SAY SO if it does not.

    DEVIATION (docstring #9): the bash's ladder did not work and could not report
    that it had not worked. On this machine, live:

    * `mc alias set _rivet …` FAILS on any current `mc` — "Alias `_rivet` should
      have alphanumeric characters … and begin with a letter" — with stderr on
      /dev/null and the exit code unread;
    * `mc mb _rivet/rivet-e2e --ignore-existing` then reads `_rivet` as a
      FILESYSTEM PATH, creates `./_rivet/rivet-e2e/` in the repo root, and exits
      **0** — so even a `|| fail` would have called it a success;
    * the bucket is therefore never created, and phase 6 fails two cells with
      `S3Error { code: "NoSuchBucket" }`, which reads as a rivet S3-writer bug.
      (Reproduced: `RIVET=./target/release/rivet bash dev/e2e/run_e2e.sh` → `FAIL
      S3 full upload`, `FAIL S3 chunked upload`, and an untracked `_rivet/` left
      in the worktree.)

    So: create it with the signed request first (no external tool, and it uses the
    credentials the export itself will use), keep the two `mc` tiers as fallbacks
    with a LEGAL alias name, and gate the WARNING on a real existence check rather
    than on any tier's self-report.
    """
    ak = os.environ["MINIO_ACCESS_KEY"]
    sk = os.environ["MINIO_SECRET_KEY"]

    if s3_make_bucket(MINIO_ENDPOINT, BUCKET, ak, sk):
        return

    if shell.have("mc"):
        # `rivete2e`, not `_rivet`: a leading underscore is not a legal mc alias.
        alias = shell.run(["mc", "alias", "set", "rivete2e", MINIO_ENDPOINT, ak, sk], timeout=60)
        if alias.ok:
            shell.run(["mc", "mb", f"rivete2e/{BUCKET}", "--ignore-existing"], timeout=60)
        else:
            shell.warn(f"mc alias set failed: {alias.out.strip()[:160]}")

    if not s3_bucket_exists(MINIO_ENDPOINT, BUCKET, ak, sk) and shell.compose(
        "exec", "-T", "minio", "mc", "version", timeout=60
    ).ok:
        shell.compose("exec", "-T", "minio", "mc", "alias", "set", "myminio",
                      MINIO_ENDPOINT, "minioadmin", "minioadmin", timeout=60)
        shell.compose("exec", "-T", "minio", "mc", "mb", f"myminio/{BUCKET}",
                      "--ignore-existing", timeout=60)

    if not s3_bucket_exists(MINIO_ENDPOINT, BUCKET, ak, sk):
        print("  WARNING: could not create MinIO bucket; S3 tests may fail", flush=True)


# ── context ────────────────────────────────────────────────────────────────────
@dataclass
class Ctx:
    """Everything the phases share: how to invoke rivet, and what is reachable."""

    rivet: tuple[str, ...]
    pg_url: str
    mysql_url: str
    t: Tally = field(default_factory=Tally)
    pg_ok: bool = False
    mysql_ok: bool = False
    minio_ok: bool = False
    gcs_ok: bool = False

    def run(self, *args: str, env: dict[str, str] | None = None) -> shell.Proc:
        """One rivet invocation, always from the repo root.

        `timeout=None` on purpose: `$RIVET` defaults to `cargo run --release`, so
        the first call may legitimately spend minutes on a fat-LTO build, and a
        timeout kill (returncode 124) would be recorded as a FAILED EXPORT — a
        harness that cannot tell "slow" from "broken" fails the wrong thing. The
        bash had no timeout either.

        `env` is per-call, so nothing leaks between phases; the bash's
        `export DATABASE_URL=… ; …; unset DATABASE_URL` around one command would
        have leaked into every later phase had that command aborted the script.

        No `stdin`: every config reaches rivet as a FILE (see `inline_config`) —
        `--config /dev/stdin` cannot work, because rivet opens the path twice.
        """
        return shell.run([*self.rivet, *args], cwd=ROOT, timeout=None, env=env)

    def cell(self, label: str, *args: str,
             env: dict[str, str] | None = None, fail_label: str | None = None) -> shell.Proc:
        """`$RIVET … >/dev/null 2>&1 && pass "label" || fail "label"`.

        `fail_label` covers the cells whose PASS and FAIL wording differ (the bash
        wrote both out longhand); omitted, one label serves both, as most do.
        """
        p = self.run(*args, env=env)
        if p.ok:
            self.t.ok(label)
        else:
            self.t.bad(fail_label if fail_label is not None else label)
            _why(p)
        return p


# ── inline configs (the bash heredocs) ─────────────────────────────────────────
@contextmanager
def inline_config(yaml: str, name: str) -> Iterator[str]:
    """A heredoc, as a real config file — and removed even when the phase throws.

    DEVIATION (docstring #7): the bash fed these to `--config /dev/stdin`, which
    `rivet` cannot read, because it opens the config path more than once
    (`dispatch_check` → `preflight::check`, then `check_plan_compatibility`) and a
    stdin already read to EOF gives up nothing on the second open. Every one of
    these cells therefore ran a command that exited 1 with
    `missing field 'source'` AFTER printing a complete report. A heredoc was only
    ever bash's way of writing an inline file, so this writes one.

    The `finally` is the point: a phase that raises mid-assertion still cleans up,
    which is what `set -e` plus explicit `rm` never managed.
    """
    fh = tempfile.NamedTemporaryFile("w", suffix=f"_{name}.yaml", delete=False)
    path = Path(fh.name)
    try:
        with fh:
            fh.write(yaml)
        yield str(path)
    finally:
        path.unlink(missing_ok=True)


BAD_GZIP_CSV_YAML = """source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - name: bad_gzip_csv
    query: "SELECT 1"
    mode: full
    format: csv
    compression: gzip
    destination:
      type: local
      path: ./dev/e2e/output
"""

MISPLACED_FIELD_YAML = """source:
  type: postgres
  url_env: RIVET_PG_URL
  batch_size: 1000
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: /tmp
"""


def _conn_yaml(source_type: str, url_env: str, name: str, parallel: int) -> str:
    """The four section-14 heredocs, which differ only in engine and `parallel`."""
    return f"""source:
  type: {source_type}
  url_env: {url_env}
exports:
  - name: {name}
    query: "SELECT id FROM users"
    mode: chunked
    chunk_column: id
    chunk_size: 100000
    parallel: {parallel}
    format: parquet
    destination:
      type: local
      path: /tmp
"""


DATE_DENSE_YAML = """source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - name: invalid_date_dense
    query: "SELECT id, ordered_at FROM orders"
    mode: chunked
    chunk_column: ordered_at
    chunk_by_days: 30
    chunk_dense: true
    format: parquet
    destination:
      type: local
      path: /tmp
"""


# ── phases ─────────────────────────────────────────────────────────────────────
def cleanup() -> None:
    shell.rm_rf(ROOT / OUT)
    shell.rm_rf(ROOT / INIT_TMP)
    for p in glob.glob(str(ROOT / "dev/e2e/.rivet_state.db*")):
        Path(p).unlink(missing_ok=True)


def phase_setup(ctx: Ctx) -> None:
    """Probe the stack; a dead Postgres is fatal, everything else degrades to SKIP."""
    section("Setup")
    cleanup()
    (ROOT / OUT).mkdir(parents=True, exist_ok=True)

    pg_host, pg_port = address_of(ctx.pg_url, "127.0.0.1", 5432)
    my_host, my_port = address_of(ctx.mysql_url, "127.0.0.1", 3306)

    # Probe the EXACT path rivet uses: a raw TCP connect to the published bind
    # (the same address tokio-postgres dials via RIVET_PG_URL). NOT
    # `pg_isready -h localhost` — libpq falls back to IPv4 and masks an
    # IPv6-only-localhost failure — and NOT `docker compose exec`, which probes
    # inside the container and passes precisely when the host port is unreachable.
    ctx.pg_ok = shell.tcp_open(pg_host, pg_port)
    ctx.mysql_ok = probe_mysql(my_host, my_port)
    ctx.minio_ok = curl_sf(MINIO_HEALTH)
    ctx.gcs_ok = curl_sf(GCS_HEALTH)

    def b(v: bool) -> str:
        return "true" if v else "false"  # the bash echoed the literal `true`/`false`

    print(f"Postgres: {b(ctx.pg_ok)} | MySQL: {b(ctx.mysql_ok)} | "
          f"MinIO: {b(ctx.minio_ok)} | fake-gcs: {b(ctx.gcs_ok)}", flush=True)

    if ctx.pg_ok:
        return

    print(f"FATAL: Postgres not reachable on the host at {pg_host}:{pg_port} "
          "(the address rivet dials).")
    print(f"  RIVET_PG_URL={ctx.pg_url}")
    print(f"  --- host listening sockets ({pg_port}/{my_port}) ---")
    listing = shell.run(["ss", "-ltn"], timeout=30) if shell.have("ss") else None
    if listing is None or not listing.ok:
        listing = shell.run(["netstat", "-ltn"], timeout=30) if shell.have("netstat") else None
    hits = [l for l in (listing.stdout.splitlines() if listing and listing.ok else [])
            if f":{pg_port}" in l or f":{my_port}" in l]
    if hits:
        print("\n".join(hits))
    else:
        print(f"    (no {pg_port}/{my_port} host bind found — "
              "the container port is not published to the host)")
    print("  --- docker compose postgres status ---")
    ps = shell.compose("ps", "postgres", timeout=60)
    if ps.stdout:
        print(ps.stdout, end="" if ps.stdout.endswith("\n") else "\n")
    raise _Abort(1)


def phase_1_doctor(ctx: Ctx) -> None:
    section("1. Doctor (connectivity check)")
    ctx.cell("PG doctor", "doctor", "--config", "dev/e2e/pg_e2e.yaml")
    if ctx.mysql_ok:
        ctx.cell("MySQL doctor", "doctor", "--config", "dev/e2e/mysql_e2e.yaml")
    else:
        ctx.t.skip("MySQL doctor (not running)")


def phase_2_check(ctx: Ctx) -> None:
    section("2. Preflight check")
    ctx.cell("PG check", "check", "--config", "dev/e2e/pg_e2e.yaml")
    if ctx.mysql_ok:
        ctx.cell("MySQL check", "check", "--config", "dev/e2e/mysql_e2e.yaml")
    else:
        ctx.t.skip("MySQL check")


def phase_3_pg_modes(ctx: Ctx) -> None:
    section("3. Postgres — all modes (local)")
    t, cfg = ctx.t, "dev/e2e/pg_e2e.yaml"

    ctx.cell("PG full CSV", "run", "--config", cfg, "--export", "pg_users_full_csv")
    assert_file_exists(t, f"{OUT}/pg_users_full_csv_*.csv", "PG full CSV file")

    ctx.cell("PG full Parquet", "run", "--config", cfg,
             "--export", "pg_users_full_parquet", "--validate")
    assert_file_exists(t, f"{OUT}/pg_users_full_parquet_*.parquet", "PG full Parquet file")

    ctx.cell("PG incremental", "run", "--config", cfg,
             "--export", "pg_orders_incremental", "--validate")
    assert_file_exists(t, f"{OUT}/pg_orders_incremental_*.parquet", "PG incremental file")

    # Incremental rerun — should succeed (no new data = no file, but still exit 0)
    ctx.cell("PG incremental rerun", "run", "--config", cfg, "--export", "pg_orders_incremental")

    ctx.cell("PG chunked", "run", "--config", cfg, "--export", "pg_orders_chunked", "--validate")
    assert_file_count_ge(t, f"{OUT}/pg_orders_chunked_*.parquet", 1, "PG chunked files")

    ctx.cell("PG time_window", "run", "--config", cfg, "--export", "pg_events_timewindow")
    assert_file_exists(t, f"{OUT}/pg_events_timewindow_*.csv", "PG time_window file")


def phase_4_pg_options(ctx: Ctx) -> None:
    section("4. Postgres — compression, skip_empty, meta, split")
    t, cfg = ctx.t, "dev/e2e/pg_e2e.yaml"

    ctx.cell("PG zstd", "run", "--config", cfg, "--export", "pg_users_zstd", "--validate")
    assert_file_exists(t, f"{OUT}/pg_users_zstd_*.parquet", "PG zstd file")

    # csv + compression is a silent no-op the manifest would lie about, so the
    # config loader rejects it (src/config/mod.rs::check_csv_compression). Assert
    # the loud rejection on a standalone bad config — it can't live in
    # pg_e2e.yaml or the whole config would fail to parse and poison every other
    # PG export.
    with inline_config(BAD_GZIP_CSV_YAML, "bad_gzip_csv") as cfg_path:
        err = ctx.run("check", "--config", cfg_path).out
    if "csv output does not support compression" in err.lower():
        t.ok("PG csv+gzip rejected")
    else:
        got = " ".join(err.split("\n"))[:160]  # `tr '\n' ' ' | cut -c1-160`
        t.bad(f"PG csv+gzip rejected (got: {got})")

    ctx.cell("PG skip_empty", "run", "--config", cfg, "--export", "pg_empty_skip")
    assert_no_file(t, f"{OUT}/pg_empty_skip_*.csv*", "PG skip_empty no file")

    ctx.cell("PG meta columns", "run", "--config", cfg, "--export", "pg_users_meta", "--validate")
    assert_file_exists(t, f"{OUT}/pg_users_meta_*.parquet", "PG meta file")

    ctx.cell("PG file split", "run", "--config", cfg, "--export", "pg_events_split", "--validate")
    assert_file_count_ge(t, f"{OUT}/pg_events_split_*_part*.csv", 2, "PG split files >=2")


def phase_5_mysql_modes(ctx: Ctx) -> None:
    section("5. MySQL — all modes (local)")
    t, cfg = ctx.t, "dev/e2e/mysql_e2e.yaml"

    if not ctx.mysql_ok:
        for label in ("MySQL full CSV", "MySQL full CSV file",
                      "MySQL full Parquet", "MySQL full Parquet file",
                      "MySQL incremental", "MySQL incremental file",
                      "MySQL chunked", "MySQL chunked files",
                      "MySQL time_window", "MySQL time_window file"):
            t.skip(label)
        return

    ctx.cell("MySQL full CSV", "run", "--config", cfg, "--export", "mysql_users_full_csv")
    assert_file_exists(t, f"{OUT}/mysql_users_full_csv_*.csv", "MySQL full CSV file")

    ctx.cell("MySQL full Parquet", "run", "--config", cfg,
             "--export", "mysql_users_full_parquet", "--validate")
    assert_file_exists(t, f"{OUT}/mysql_users_full_parquet_*.parquet", "MySQL full Parquet file")

    ctx.cell("MySQL incremental", "run", "--config", cfg,
             "--export", "mysql_orders_incremental", "--validate")
    assert_file_exists(t, f"{OUT}/mysql_orders_incremental_*.parquet", "MySQL incremental file")

    ctx.cell("MySQL chunked", "run", "--config", cfg,
             "--export", "mysql_orders_chunked", "--validate")
    assert_file_count_ge(t, f"{OUT}/mysql_orders_chunked_*.parquet", 1, "MySQL chunked files")

    ctx.cell("MySQL time_window", "run", "--config", cfg, "--export", "mysql_events_timewindow")
    assert_file_exists(t, f"{OUT}/mysql_events_timewindow_*.csv", "MySQL time_window file")


def phase_6_s3(ctx: Ctx) -> None:
    section("6. S3 (MinIO) destination")
    if not ctx.minio_ok:
        ctx.t.skip("S3 full upload (MinIO not running)")
        ctx.t.skip("S3 chunked upload")
        return

    # `${VAR:-default}` replaces an EMPTY value too, which `setdefault` would keep.
    os.environ["MINIO_ACCESS_KEY"] = os.environ.get("MINIO_ACCESS_KEY") or "minioadmin"
    os.environ["MINIO_SECRET_KEY"] = os.environ.get("MINIO_SECRET_KEY") or "minioadmin"
    ensure_minio_bucket()

    cfg = "dev/e2e/pg_s3_e2e.yaml"
    ctx.cell("S3 full upload", "run", "--config", cfg, "--export", "pg_users_s3", "--validate")
    ctx.cell("S3 chunked upload", "run", "--config", cfg,
             "--export", "pg_orders_s3_chunked", "--validate")


def phase_7_gcs(ctx: Ctx) -> None:
    section("7. GCS (fake-gcs-server) destination")
    if not ctx.gcs_ok:
        ctx.t.skip("GCS full upload (fake-gcs not running)")
        ctx.t.skip("GCS incremental upload")
        return

    # `|| true`: an existing bucket answers 409 and that is fine.
    curl_sf(f"{GCS_ENDPOINT}/storage/v1/b?project=test", method="POST",
            data=b'{"name": "rivet-e2e"}', headers={"Content-Type": "application/json"})

    cfg = "dev/e2e/pg_gcs_e2e.yaml"
    ctx.cell("GCS full upload", "run", "--config", cfg, "--export", "pg_users_gcs", "--validate")
    ctx.cell("GCS incremental upload", "run", "--config", cfg,
             "--export", "pg_orders_gcs_incremental")


def phase_8_state(ctx: Ctx) -> None:
    section("8. State management")
    cfg = "dev/e2e/pg_e2e.yaml"
    ctx.cell("state show", "state", "show", "--config", cfg)
    ctx.cell("state files", "state", "files", "--config", cfg)
    ctx.cell("metrics", "metrics", "--config", cfg, "--last", "5")
    ctx.cell("state reset", "state", "reset", "--config", cfg,
             "--export", "pg_orders_incremental")
    ctx.cell("re-export after reset", "run", "--config", cfg,
             "--export", "pg_orders_incremental", "--validate")


def phase_9_stdout(ctx: Ctx) -> None:
    section("9. Stdout destination")
    # Diagnostics only (verdict unchanged): the bash sent stderr to /dev/null, so
    # an empty stdout produced a bare `FAIL stdout destination` — and with `$RIVET`
    # defaulting to `cargo run`, the most likely cause (a build failure) lives
    # entirely in the stderr it threw away. `_why` prints it, on stderr.
    p = ctx.run("run", "--config", "dev/fixtures/test_stdout.yaml")
    first = p.stdout[:100]  # `| head -c 100`
    if first:
        ctx.t.ok("stdout destination")
    else:
        ctx.t.bad("stdout destination")
        _why(p)


def phase_10_params(ctx: Ctx) -> None:
    section("10. Parameterized queries")
    ctx.cell("params", "run", "--config", "dev/fixtures/test_params.yaml",
             "--param", "MAX_ID=10")


# DEVIATION (docstring #3): `grep -q "MATCH"` matches "MISMATCH" as a substring,
# so the cell that exists to catch a reconcile mismatch passed on one.
# src/pipeline/summary.rs:695 prints exactly `MATCH (n/m)` / `MISMATCH (…)`.
_MATCH = re.compile(r"(?<!MIS)MATCH")


def reconcile_matched(out: str) -> bool:
    """A reconcile transcript that reports MATCH and nowhere reports MISMATCH.

    Both halves earn their keep: the lookbehind stops "MISMATCH" from satisfying
    the needle, and the `not in` stops a transcript that says MISMATCH *somewhere*
    from passing on an unrelated "MATCH…" token elsewhere in the same output.
    """
    return "MISMATCH" not in out and bool(_MATCH.search(out))


def phase_11_reconcile(ctx: Ctx) -> None:
    section("11. Reconciliation (--reconcile)")
    t, cfg = ctx.t, "dev/e2e/pg_e2e.yaml"

    for export, label in (("pg_users_full_csv", "reconcile full MATCH"),
                          ("pg_orders_chunked", "reconcile chunked MATCH")):
        p = ctx.run("run", "--config", cfg, "--export", export, "--reconcile")
        if reconcile_matched(p.out):
            t.ok(label)
        else:
            t.bad(label)
            _why(p)  # a genuine MISMATCH exits 3 (src/pipeline/job.rs:458)

    # Incremental should skip reconciliation (no MATCH/MISMATCH in output).
    #
    # DEVIATION (docstring #4): the bash granted this cell on the ABSENCE of
    # "reconcile:", which a run that crashed before printing anything also
    # satisfies. The skip is only proven if the run actually completed.
    p = ctx.run("run", "--config", cfg, "--export", "pg_orders_incremental", "--reconcile")
    if "reconcile:" in p.out:
        t.bad("reconcile incremental should skip")
    elif not p.ok:
        t.bad("reconcile incremental skip")
        _why(p)
    else:
        t.ok("reconcile incremental skip")


def phase_12_recovery(ctx: Ctx) -> None:
    section("12. Recovery / rerun behavior")
    t, cfg = ctx.t, "dev/e2e/pg_recovery_e2e.yaml"

    # Full mode: two consecutive runs should both succeed and produce separate files.
    ctx.cell("recovery full run1", "run", "--config", cfg, "--export", "recovery_full",
             "--reconcile")
    count1 = len(matches(f"{OUT}/recovery_full_*.parquet"))
    # DEVIATION (docstring #8): the bash slept 1s here so the two runs' parts got
    # distinct names. src/pipeline/single.rs:386 stamps `%Y%m%d_%H%M%S_%3f`, so the
    # sleep bought nothing except immunity to a regression back to second
    # granularity — the shape that already cost this repo 3-of-6 deltas.
    ctx.cell("recovery full run2", "run", "--config", cfg, "--export", "recovery_full",
             "--reconcile")
    count2 = len(matches(f"{OUT}/recovery_full_*.parquet"))
    if count2 > count1:
        t.ok(f"recovery full separate files (n={count2})")
    else:
        t.bad(f"recovery full separate files ({count1} vs {count2})")

    # Incremental mode: second run should succeed (with 0 rows if no new data).
    reset = ctx.run("state", "reset", "--config", cfg, "--export", "recovery_incremental")
    if not reset.ok:
        # The bash dropped this exit code entirely. Kept uncounted (adding a cell
        # would change the totals) but no longer invisible: a failed reset makes
        # the next two cells measure the wrong thing.
        shell.warn("state reset recovery_incremental failed — run1 below is not a fresh cursor")
        _why(reset)
    ctx.cell("recovery incr run1", "run", "--config", cfg,
             "--export", "recovery_incremental", "--reconcile")

    # DEVIATION (docstring #5): the bash called `pass` on BOTH branches of this
    # grep, so the cell could not fail — a run2 that aborted still passed. The
    # exit status decides; the two messages still distinguish the two outcomes.
    p = ctx.run("run", "--config", cfg, "--export", "recovery_incremental")
    if not p.ok:
        t.bad("recovery incr run2")
        _why(p)
    elif re.search(r"rows.*0|no data", p.out):
        t.ok("recovery incr run2 (no new data)")
    else:
        t.ok("recovery incr run2 (ok)")

    # Chunked with checkpoint: run succeeds; resume after full completion exits 0
    # with rows=0.
    ctx.cell("recovery chunked ckpt run1", "run", "--config", cfg,
             "--export", "recovery_chunked_ckpt", "--reconcile")
    p = ctx.run("run", "--config", cfg, "--export", "recovery_chunked_ckpt", "--resume")
    if re.search(r"rows.*0|success", p.out, re.I):
        t.ok("recovery chunked resume (no pending)")
    else:
        t.bad("recovery chunked resume")
        _why(p)

    # Re-run without resume should succeed (full re-export).
    ctx.cell("recovery chunked re-export", "run", "--config", cfg,
             "--export", "recovery_chunked_ckpt", "--reconcile")

    # Metrics should show entries for recovery exports. Filter to `recovery_full`
    # explicitly so the assertion doesn't race with an unrelated later run pushing
    # it out of `--last 10`. The brief sleep gives SQLite's WAL a moment to flush
    # the last chunked-ckpt run's metric row before we read it back. (Kept: this
    # one waits on ANOTHER PROCESS's durability, not on a filename stamp.)
    time.sleep(0.5)
    p = ctx.run("metrics", "--config", cfg, "--export", "recovery_full", "--last", "10")
    # An empty history prints "No metrics recorded yet." (src/pipeline/cli.rs:343),
    # which does not contain the export name — so the needle really is a data row.
    # Exit status added: a `metrics` that errored out proves nothing either way.
    if p.ok and "recovery_full" in p.out:
        t.ok("recovery metrics entries")
    else:
        t.bad("recovery metrics entries")
        print("DEBUG: metrics output was:")
        print(p.out, end="" if p.out.endswith("\n") else "\n")


def phase_13_config_validation(ctx: Ctx) -> None:
    section("13. Config validation (misplaced fields)")
    with inline_config(MISPLACED_FIELD_YAML, "misplaced_field") as cfg_path:
        out = ctx.run("run", "--config", cfg_path).out
    # `grep -q "source.tuning"` — the `.` was an unescaped wildcard; the real
    # message (src/config/mod.rs:343) contains the literal `'source.tuning:'`, so
    # a substring test is the same verdict, strictly stricter.
    if "source.tuning" in out:
        ctx.t.ok("misplaced field detection")
    else:
        ctx.t.bad("misplaced field detection")


_CONN_WARNING = re.compile(r"meets or exceeds|check skipped", re.I)


def _conn_limit_cells(ctx: Ctx, engine: str, source_type: str, url_env: str,
                      safe_name: str, high_name: str) -> None:
    """The parallel=2 (no warning) / parallel=999 (warning + numbers) triple.

    The PG and MySQL blocks in the bash are the same six labels with a different
    prefix, so `engine` derives all of them instead of duplicating the strings.
    """
    t = ctx.t

    # DEVIATION (docstring #6): the parallel=2 cell asserts an ABSENCE, which a
    # `check` that produced no output at all also satisfies. So it must first
    # prove the check RAN: exit 0, plus the report's own `Export: <name>` header
    # for this export. Without that positive marker a crash reads as "no warning".
    with inline_config(_conn_yaml(source_type, url_env, safe_name, 2), safe_name) as cfg_path:
        p = ctx.run("check", "--config", cfg_path)
    if not (p.ok and f"Export: {safe_name}" in p.out):
        t.bad(f"{engine} check: no connection warning at parallel=2 "
              f"(check did not run: exit {p.returncode})")
        _why(p)
    elif _CONN_WARNING.search(p.out):
        t.bad(f"{engine} check: unexpected connection warning at parallel=2")
    else:
        t.ok(f"{engine} check: no connection warning at parallel=2")

    with inline_config(_conn_yaml(source_type, url_env, high_name, 999), high_name) as cfg_path:
        out = ctx.run("check", "--config", cfg_path).out
    if re.search(r"meets or exceeds", out, re.I):
        t.ok(f"{engine} check: connection limit warning at parallel=999")
    else:
        t.bad(f"{engine} check: expected 'meets or exceeds' warning at parallel=999")
    if re.search(r"max_connections", out, re.I):
        t.ok(f"{engine} check: max_connections value present in warning")
    else:
        t.bad(f"{engine} check: max_connections value missing from warning")


def phase_14_conn_limits(ctx: Ctx) -> None:
    section("14. Preflight — connection limit warnings")

    # PG: parallel well below max_connections (default 100) — no warning expected;
    # then parallel >= max_connections (999 >> 100) — must warn with exact numbers.
    _conn_limit_cells(ctx, "PG", "postgres", "RIVET_PG_URL",
                      "check_conn_safe", "check_conn_high")

    if not ctx.mysql_ok:
        ctx.t.skip("MySQL check: connection limit warning (MySQL not running)")
        ctx.t.skip("MySQL check: max_connections value present")
        ctx.t.skip("MySQL check: no connection warning at parallel=2")
        ctx.t.skip("MySQL check: max_connections value at parallel=999")
        return

    # MySQL: below max_connections (default 151), then 999 >> 151 — must warn.
    _conn_limit_cells(ctx, "MySQL", "mysql", "RIVET_MYSQL_URL",
                      "mysql_check_conn_safe", "mysql_check_conn_high")


def phase_15_date_chunking(ctx: Ctx) -> None:
    section("15. Date-based chunking (chunk_by_days)")
    t = ctx.t

    # PG: date-chunked run produces at least one output file.
    p = ctx.run("run", "--config", "dev/e2e/pg_e2e.yaml",
                "--export", "pg_orders_date_chunked", "--validate")
    if p.ok:
        t.ok("PG date-chunked: run succeeded")
    else:
        t.bad("PG date-chunked: run failed")
        _why(p)
    assert_file_count_ge(t, f"{OUT}/pg_orders_date_chunked_*.parquet", 1,
                         "PG date-chunked files")

    # PG: preflight shows date-chunked strategy in output.
    out = ctx.run("check", "--config", "dev/e2e/pg_e2e.yaml",
                  "--export", "pg_orders_date_chunked").out
    if re.search(r"date-chunked", out, re.I):
        t.ok("PG check: date-chunked strategy shown")
    else:
        t.bad("PG check: expected 'date-chunked' in strategy output")

    # PG: inline config — verify chunk_by_days rejects chunk_dense combination.
    with inline_config(DATE_DENSE_YAML, "invalid_date_dense") as cfg_path:
        out = ctx.run("run", "--config", cfg_path).out
    if re.search(r"chunk_dense|cannot combine|invalid", out, re.I):
        t.ok("PG validation: chunk_by_days + chunk_dense rejected")
    else:
        t.bad("PG validation: expected rejection of chunk_by_days + chunk_dense")

    if not ctx.mysql_ok:
        t.skip("MySQL date-chunked: run")
        t.skip("MySQL date-chunked: files")
        t.skip("MySQL check: date-chunked strategy")
        return

    p = ctx.run("run", "--config", "dev/e2e/mysql_e2e.yaml",
                "--export", "mysql_orders_date_chunked", "--validate")
    if p.ok:
        t.ok("MySQL date-chunked: run succeeded")
    else:
        t.bad("MySQL date-chunked: run failed")
        _why(p)
    assert_file_count_ge(t, f"{OUT}/mysql_orders_date_chunked_*.parquet", 1,
                         "MySQL date-chunked files")

    out = ctx.run("check", "--config", "dev/e2e/mysql_e2e.yaml",
                  "--export", "mysql_orders_date_chunked").out
    if re.search(r"date-chunked", out, re.I):
        t.ok("MySQL check: date-chunked strategy shown")
    else:
        t.bad("MySQL check: expected 'date-chunked' in strategy output")


def phase_16_init(ctx: Ctx) -> None:
    section("16. rivet init (YAML scaffold)")
    t = ctx.t
    shell.rm_rf(ROOT / INIT_TMP)
    (ROOT / INIT_TMP).mkdir(parents=True, exist_ok=True)

    # `rivet init` takes the URL on the CLI. Use the suite's RIVET_PG_URL /
    # RIVET_MYSQL_URL so this section retargets cleanly when the legacy matrix
    # overrides them.
    pg_init_url, my_init_url = ctx.pg_url, ctx.mysql_url

    pg_users = f"{INIT_TMP}/pg_users.yaml"
    ctx.cell("PG init: single table (users)", "init", "--source", pg_init_url,
             "--table", "users", "-o", pg_users)
    if (file_contains(pg_users, "exports:")
            and file_contains(pg_users, "name: users")
            and file_contains(pg_users, "mode:")):
        t.ok("PG init: users.yaml structure")
    else:
        t.bad("PG init: users.yaml missing exports/name/mode")

    pg_schema = f"{INIT_TMP}/pg_schema.yaml"
    ctx.cell("PG init: full schema (public)", "init", "--source", pg_init_url,
             "--schema", "public", "-o", pg_schema)
    pg_init_count = count_export_names(pg_schema)
    if pg_init_count >= 5:
        t.ok(f"PG init: multiple exports in schema file (n={pg_init_count})")
    else:
        t.bad(f"PG init: expected >=5 exports in schema file, got {pg_init_count}")
    if file_contains(pg_schema, 'PostgreSQL schema "public"'):
        t.ok("PG init: schema banner in header")
    else:
        t.bad("PG init: expected PostgreSQL schema banner")

    # The generated scaffold reads `url_env: DATABASE_URL`. Scoped to this one
    # call rather than `export`ed and `unset` around it, so it cannot leak into a
    # later phase if this one dies.
    ctx.cell("PG init: rivet check accepts generated file", "check", "--config", pg_users,
             env={"DATABASE_URL": pg_init_url},
             fail_label="PG init: rivet check on generated file")

    if not ctx.mysql_ok:
        t.skip("MySQL init: single table")
        t.skip("MySQL init: orders.yaml structure")
        t.skip("MySQL init: full database")
        t.skip("MySQL init: multiple exports")
        t.skip("MySQL init: database banner")
        return

    my_orders = f"{INIT_TMP}/mysql_orders.yaml"
    ctx.cell("MySQL init: single table (orders)", "init", "--source", my_init_url,
             "--table", "orders", "-o", my_orders)
    if file_contains(my_orders, "name: orders"):
        t.ok("MySQL init: orders.yaml structure")
    else:
        t.bad("MySQL init: orders.yaml missing name")

    my_db = f"{INIT_TMP}/mysql_db.yaml"
    ctx.cell("MySQL init: full database", "init", "--source", my_init_url, "-o", my_db)
    my_init_count = count_export_names(my_db)
    if my_init_count >= 5:
        t.ok(f"MySQL init: multiple exports (n={my_init_count})")
    else:
        t.bad(f"MySQL init: expected >=5 exports, got {my_init_count}")
    if file_contains(my_db, 'MySQL database "rivet"'):
        t.ok("MySQL init: database banner in header")
    else:
        t.bad("MySQL init: expected MySQL database banner")


def phase_summary(ctx: Ctx) -> int:
    section("Summary")
    t = ctx.t
    print()
    # Wire format: dev/legacy/run_full_matrix.sh awks fields 2, 5 and 8 out of
    # this exact line. Do not reflow it.
    print(f"PASS: {t.passed} | FAIL: {t.failed} | SKIP: {t.skipped}")
    print(f"Total: {t.passed + t.failed + t.skipped}")
    print()

    if t.failed > 0:
        print("FAILURES:")
        for line in t.failures():
            print(line)
        return 1

    print("All tests passed!")
    cleanup()
    return 0


PHASES: tuple[tuple[str, Callable[[Ctx], None]], ...] = (
    ("1-doctor", phase_1_doctor),
    ("2-check", phase_2_check),
    ("3-pg-modes", phase_3_pg_modes),
    ("4-pg-options", phase_4_pg_options),
    ("5-mysql-modes", phase_5_mysql_modes),
    ("6-s3", phase_6_s3),
    ("7-gcs", phase_7_gcs),
    ("8-state", phase_8_state),
    ("9-stdout", phase_9_stdout),
    ("10-params", phase_10_params),
    ("11-reconcile", phase_11_reconcile),
    ("12-recovery", phase_12_recovery),
    ("13-config-validation", phase_13_config_validation),
    ("14-conn-limits", phase_14_conn_limits),
    ("15-date-chunking", phase_15_date_chunking),
    ("16-init", phase_16_init),
)

USAGE = """Usage: python3 dev/pytools/e2e.py [--only NAME[,NAME...]] [--list]

Environment (unchanged from dev/e2e/run_e2e.sh):
  RIVET_PG_URL      default postgresql://rivet:rivet@127.0.0.1:5432/rivet
  RIVET_MYSQL_URL   default mysql://rivet:rivet@127.0.0.1:3306/rivet
  RIVET             default "cargo run --release --bin rivet --"
  MINIO_ACCESS_KEY  default minioadmin
  MINIO_SECRET_KEY  default minioadmin

Options (ADDITIONS — the bash took no arguments and silently ignored any):
  --only NAMES  Run only these phases (comma-separated; Setup always runs)
  --list        List phase names and exit 0
  -h, --help    Show this help"""


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    only: list[str] = []
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--list":
            for name, _ in PHASES:
                print(name)
            return 0
        if a in ("-h", "--help"):
            print(USAGE)
            return 0
        if a == "--only":
            if i + 1 >= len(args) or not args[i + 1]:
                print("--only requires a value", file=sys.stderr)
                print(USAGE, file=sys.stderr)
                return 2
            only += [s for s in args[i + 1].split(",") if s]
            i += 2
            continue
        # DEVIATION (docstring #10): the bash ignored every argument, so a typo'd
        # `--only-pg` ran the whole two-hour suite and reported on it.
        print(f"Unknown option: {a}", file=sys.stderr)
        print(USAGE, file=sys.stderr)
        return 2

    known = {name for name, _ in PHASES}
    unknown = [n for n in only if n not in known]
    if unknown:
        print(f"Unknown phase(s): {', '.join(unknown)}", file=sys.stderr)
        print(f"Known: {', '.join(sorted(known))}", file=sys.stderr)
        return 2

    # `export RIVET_PG_URL="${RIVET_PG_URL:-…}"` — the e2e YAMLs read these via
    # `url_env:`, so they must reach the child processes, not just this one.
    os.environ["RIVET_PG_URL"] = os.environ.get("RIVET_PG_URL") or DEFAULT_PG_URL
    os.environ["RIVET_MYSQL_URL"] = os.environ.get("RIVET_MYSQL_URL") or DEFAULT_MYSQL_URL
    rivet_cmd = os.environ.get("RIVET") or DEFAULT_RIVET

    ctx = Ctx(
        # `$RIVET` was word-split by the shell; `shlex.split` does the same
        # splitting without re-parsing anything else in the command.
        rivet=tuple(shlex.split(rivet_cmd)),
        pg_url=os.environ["RIVET_PG_URL"],
        mysql_url=os.environ["RIVET_MYSQL_URL"],
    )
    if not ctx.rivet:
        raise shell.Fail("$RIVET is empty — nothing to run",
                         hint=f'unset RIVET to use the default: "{DEFAULT_RIVET}"')
    ctx.t.reset_file()

    try:
        phase_setup(ctx)
    except _Abort as e:
        return e.code

    for name, fn in PHASES:
        if only and name not in only:
            continue
        try:
            fn(ctx)
        except _Abort as e:
            return e.code
        except Exception as e:  # noqa: BLE001 — see below
            # The bash ran under `set -uo pipefail` WITHOUT `-e`, so a broken cell
            # never stopped the suite; phases 8-16 still ran and still reported.
            # An uncaught Python exception would abort everything instead, hiding
            # every later phase behind one harness bug — so it becomes a FAIL cell
            # (loud, counted, non-zero exit) and the suite continues.
            ctx.t.bad(f"{name} (harness error: {type(e).__name__}: {e})")

    return phase_summary(ctx)


if __name__ == "__main__":
    shell.main(lambda: main_cli())
