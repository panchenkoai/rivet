"""The small dev/test shell scripts, ported one subcommand per script.

    python dev/pytools/dev_scripts.py <command> [args]

    permissions            ← dev/scripts/test_permissions.sh
    schema-evolution       ← dev/scripts/test_schema_evolution.sh
    retry-toxiproxy        ← dev/scripts/test_retry_toxiproxy.sh
    setup-toxiproxy        ← dev/scripts/setup_toxiproxy.sh
    uat-smoke              ← dev/scripts/run_uat_smoke.sh
    live                   ← dev/scripts/test_live.sh
    bench                  ← dev/scripts/bench.sh
    regen-docker-configs   ← dev/scripts/regenerate_docker_init_configs.sh

Every check, its order, and its message text is preserved: these are assertion
harnesses, so the only acceptable change is one that makes a check ASSERT MORE.
A check that quietly stops asserting is the worst outcome, so every precondition
(a tool, a container, an env var, a GRANT) stays loud, and each one names how to
satisfy it.

Deviations from the bash are marked `DEVIATION:` at the site. Summarised:

1.  **Teardown moved into `try/finally`** (`permissions`, `schema-evolution`,
    `retry-toxiproxy`). The bash relied on `set -e` plus a cleanup section at the
    BOTTOM, so the first failing assertion aborted before it — leaking test roles
    (`rivet_noaccess`, `rivet_partial`), a `schema_test_table` fixture and a live
    Toxiproxy toxic into the next run. A leaked `latency` toxic makes the NEXT
    run's baseline slow; a leaked role makes the next `CREATE ROLE` fail, whose
    error psql then swallows (see 2).
2.  **A failed GRANT is a precondition failure, not four confusing FAILs.**
    `psql` without `-v ON_ERROR_STOP=1` exits 0 after a statement error, so if
    `CREATE ROLE`/`GRANT` failed, the harness went on to test a user that does
    not exist and reported "expected 'permission denied'" — a message pointing
    at rivet for a fixture problem. The teardown half stays error-tolerant (it
    revokes from roles that may not exist yet, exactly as the bash did); the
    setup half runs with `ON_ERROR_STOP=1` and is checked.
3.  **`uat-smoke` exits non-zero when anything FAILed.** The bash's last command
    was `grep -c '^SKIP' … || true`, so the script exited 0 with 30 FAILs on the
    board — unusable as a gate, and CI would have called it green.
4.  **`uat-smoke` requires `rivet` on PATH up front.** Without the binary every
    single cell failed, producing a wall of FAILs indistinguishable from real
    regressions.
5.  **`schema-evolution` step 8 compares the run count NUMERICALLY.** The bash
    ran `assert_contains "$RUNS" "7"` — a substring match on a COUNT. Its own
    label says "(or more)", but 8, 9 and 10 runs all FAIL while 17 and 27 PASS.
6.  **`live` decides on `cargo test`'s exit status, not `grep`'s.** The pg and
    mysql arms piped cargo through `grep -E "…|FAILED|ok"`; `$?` after a pipeline
    is the LAST stage, and the pattern matches the word `FAILED` itself, so a
    failing live suite exited 0. The filter is kept (it is a readability filter)
    but the verdict now comes from cargo.
7.  **`live` refuses an unknown selector.** `test_live.sh postgress` fell into
    the `*)` arm and ran the ENTIRE ignored suite instead of the postgres subset.
8.  **`live` probes with a socket, not `nc`.** `nc -z -w1 … 2>/dev/null` reports
    "not reachable" identically whether the port is closed or `nc` is simply not
    installed — a missing tool rendered as a down service.
9.  **`bench` refuses an unknown command.** `run|*)` meant `bench.sh compre v1`
    silently ran the whole suite (minutes) instead of comparing.
10. **`setup-toxiproxy` distinguishes 409 from unreachable.** `curl -sf … && echo
    " OK" || echo " (already exists)"` printed "(already exists)" for EVERY
    failure — including "toxiproxy is not running" and a malformed body. The
    script then reported "Proxies configured." having configured nothing.
11. **`retry-toxiproxy` checks its preconditions before Q1.** A missing `pg`
    proxy surfaced as a bare `curl -sf` non-zero at Q2 under `set -e`: no
    message, exit 1, and (before 1) a leaked toxic.
12. **`regen-docker-configs` writes through `atomic_write`.** `init -o file`
    truncates its output before it can fail, and the follow-up `find … -exec sed
     -i.bak` rewrote *every* yaml under `dev/init_generated` — including stale
    files from an earlier run — leaving `.bak` litter if it died halfway.
13. **`permissions` / `schema-evolution` build once, up front.** Both timed
    `cargo run --release` with a 3-second budget; on a cold or stale target that
    budget measures the COMPILE, so the "did not retry" gate fails for a reason
    that has nothing to do with retries. Elapsed time is also kept as a float
    (`date +%s` subtraction truncates, so a real 3.99 s read as 3 and passed a
    `-le 3` gate).

Not found (checked, reported honestly): no `exit N` inside a `$(…)`; no
`grep PATTERN fileA fileB` two-operand + `tr -dc '0-9'` digit-swallow; no
`grep -c healthy` / "unhealthy" confusion; no `local x=$1 y="…${x}…"`
cross-reference (the same-line `local output="$1" pattern="$2" label="$3"` shape
appears four times but never expands one of its own names, so bash 3.2's
enclosing-scope expansion is not triggered); and no `/usr/bin/time` parsing in
`bench.sh` — it shells out to `cargo bench` and nothing else, so the BSD `-l`
(bytes) vs GNU `-v` (kbytes) split and the base-60 `0:01:23.45` sum (a greedy
`[0-9.]+$` grab reads a 90 s run as 30 s) belong to the modules that DO measure:
`dev/pytools/cdc_stand.py`, `dev/bench/smoke.py`,
`dev/release_oracle/regression.py`. One stale reference kept honest: `bench.sh`'s
comment block advertises `./dev/bench/run_bench.sh` for the e2e suites, and that
file no longer exists — `dev/bench/smoke.py` replaced it.

Two portability notes rather than bugs: `grep -qi 'access denied\\|Access denied'`
relies on the GNU `\\|` BRE extension (present in macOS's and GNU grep, absent
from POSIX BRE and busybox — where the pattern becomes a literal and the check
can NEVER pass); the alternation is also redundant under `-i`. Translated to a
real alternation here. And `docker compose ps mysql | grep -q mysql` tests for a
container NAME, not for a healthy service; kept as-is (running the mysql cells
against a sick container FAILs loudly, which beats skipping them) but parsed
row-wise so the header can never satisfy it.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Pattern, Sequence

if __package__:
    from . import shell
else:  # executed as a plain script: `python3 dev/pytools/dev_scripts.py …`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail

# ── plain stdout printing (the bash used `echo`/`echo -e`, not a logger) ────────
_TTY = sys.stdout.isatty() or os.environ.get("FORCE_COLOR") == "1"


def _paint(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _TTY else text


def _green(msg: str) -> None:
    print(_paint("32", msg), flush=True)


def _red(msg: str) -> None:
    print(_paint("31", msg), flush=True)


def _say(msg: str = "") -> None:
    print(msg, flush=True)


def _header(title: str) -> None:
    """`header() { echo ""; echo "=== $1 ==="; }`"""
    _say()
    _say(f"=== {title} ===")


# ── the rivet binary ───────────────────────────────────────────────────────────
# `RIVET="cargo run --release --bin rivet --"`, as an argv list so nothing is
# re-split by a shell.
CARGO_RIVET: tuple[str, ...] = ("cargo", "run", "--release", "--bin", "rivet", "--")


def cargo_rivet(*args: str) -> list[str]:
    return [*CARGO_RIVET, *args]


def prebuild_rivet() -> None:
    """DEVIATION (13): build before the timed assertions run.

    `test_permissions.sh` wraps `cargo run --release` in a `date +%s` fence and
    asserts ≤3 s. `cargo run` compiles first when the target is cold or stale, so
    the budget measures rustc and the "did not retry" verdict becomes a build-cache
    verdict. Building here costs the same time but spends it OUTSIDE the fence.
    """
    shell.require("cargo", hint="install Rust (https://rustup.rs)")
    shell.log("cargo build --release --bin rivet (so the timed runs measure rivet)")
    shell.stream(
        ["cargo", "build", "--release", "--bin", "rivet"], cwd=ROOT, timeout=None
    ).check("cargo build --release --bin rivet")


def installed_or_cargo_rivet() -> list[str]:
    """`command -v rivet && RIVET=rivet || RIVET="cargo run …"` — the installed
    binary wins, exactly as in `test_retry_toxiproxy.sh`."""
    if shell.have("rivet"):
        return ["rivet"]
    shell.require("cargo", hint="install Rust, or `cargo install --path .`")
    return list(CARGO_RIVET)


# ── database clients (through docker compose, as the bash did) ──────────────────
def _psql(
    sql: str | None = None,
    *,
    command: str | None = None,
    stop_on_error: bool = False,
) -> shell.Proc:
    """`docker compose exec -T postgres psql -U rivet -q`, fed either a script on
    stdin (the heredocs) or a single `-c` statement.

    `stop_on_error` adds `-v ON_ERROR_STOP=1`; see deviation 2 for why only the
    setup half gets it.
    """
    argv = ["exec", "-T", "postgres", "psql", "-U", "rivet", "-q"]
    if stop_on_error:
        argv += ["-v", "ON_ERROR_STOP=1"]
    if command is not None:
        argv += ["-c", command]
    return shell.compose(*argv, stdin=sql, cwd=ROOT)


def _mysql(sql: str) -> shell.Proc:
    """`docker compose exec -T mysql mysql -uroot -privet rivet -q` + a heredoc.

    The mysql client already stops at the first error and exits non-zero, so this
    needs no ON_ERROR_STOP equivalent — `.check()` at the call site is enough.
    """
    return shell.compose(
        "exec", "-T", "mysql", "mysql", "-uroot", "-privet", "rivet", "-q",
        stdin=sql,
        cwd=ROOT,
    )


def _require_docker() -> None:
    shell.require("docker", hint="install Docker, then `docker compose up -d`")


# ── the shared assertion tally ─────────────────────────────────────────────────
@dataclass
class Tally:
    """`PASS`/`FAIL` counters plus the three assertions both test scripts share.

    The two scripts word their failure lines slightly differently (`(expected
    'p')` + `  Output: …` vs `(expected 'p' in output)` + `  Output was: …`), so
    the wording is data rather than a fork of the logic.
    """

    expected_suffix: str = "(expected '{pattern}')"
    output_prefix: str = "  Output: "
    passed: int = 0
    failed: int = 0

    def _pass(self, label: str) -> None:
        _green(f"  PASS: {label}")
        self.passed += 1

    def _fail(self, line: str) -> None:
        _red(f"  FAIL: {line}")
        self.failed += 1

    def contains(self, output: str, pattern: str, label: str) -> bool:
        """`echo "$output" | grep -qi "$pattern"` — case-insensitive regex search
        over the whole transcript."""
        if re.search(pattern, output, re.IGNORECASE):
            self._pass(label)
            return True
        self._fail(f"{label} {self.expected_suffix.format(pattern=pattern)}")
        _say(f"{self.output_prefix}{output}")
        return False

    def not_contains(self, output: str, pattern: str, label: str) -> bool:
        if re.search(pattern, output, re.IGNORECASE):
            self._fail(f"{label} (unexpected '{pattern}' found)")
            return False
        self._pass(label)
        return True

    def fast(self, seconds: float, maximum: float, label: str) -> bool:
        """`[ "$seconds" -le "$max" ]`, but on real seconds.

        DEVIATION (13): `date +%s` arithmetic truncates both ends, so an actual
        3.99 s run compared as `3 -le 3` and PASSED the "no retry" gate. One
        decimal place keeps the message shape (`(2.4s <= 3s)`).
        """
        if seconds <= maximum:
            self._pass(f"{label} ({seconds:.1f}s <= {maximum:g}s)")
            return True
        self._fail(f"{label} ({seconds:.1f}s > {maximum:g}s -- likely retried)")
        return False

    def at_least(self, actual: int, minimum: int, label: str) -> bool:
        """DEVIATION (5): a numeric floor where the bash substring-matched a count."""
        if actual >= minimum:
            self._pass(f"{label} (found {actual})")
            return True
        self._fail(f"{label} (found {actual}, want >= {minimum})")
        return False

    def summary(self) -> int:
        _say()
        _say("================================")
        _say(f"Results: {self.passed} passed, {self.failed} failed")
        _say("================================")
        return 1 if self.failed > 0 else 0


def _timed(argv: Sequence[str], **kw) -> tuple[shell.Proc, float]:
    """Run and report elapsed wall time — the `START=$(date +%s)` … fence."""
    start = time.monotonic()
    p = shell.run(argv, **kw)
    return p, time.monotonic() - start


# ══ 1. permissions ═════════════════════════════════════════════════════════════
# Cleanup/teardown: run tolerantly. These REVOKEs name roles that may not exist,
# which is exactly why the bash could not use ON_ERROR_STOP here.
_PG_TEARDOWN_SQL = """-- Clean up existing roles
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM rivet_noaccess CASCADE;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM rivet_partial CASCADE;
REVOKE USAGE ON SCHEMA public FROM rivet_partial;
REVOKE CONNECT ON DATABASE rivet FROM rivet_partial;
DROP ROLE IF EXISTS rivet_noaccess;
DROP ROLE IF EXISTS rivet_partial;
"""

# Setup: every statement here MUST succeed or the whole harness is testing a
# user that does not exist (deviation 2).
_PG_SETUP_SQL = """-- User with NO access
CREATE ROLE rivet_noaccess LOGIN PASSWORD 'noaccess';

-- User with SELECT on users only
CREATE ROLE rivet_partial LOGIN PASSWORD 'partial';
GRANT CONNECT ON DATABASE rivet TO rivet_partial;
GRANT USAGE ON SCHEMA public TO rivet_partial;
GRANT SELECT ON users TO rivet_partial;
"""

_MYSQL_TEARDOWN_SQL = """DROP USER IF EXISTS 'rivet_noaccess'@'%';
DROP USER IF EXISTS 'rivet_partial'@'%';
FLUSH PRIVILEGES;
"""

_MYSQL_SETUP_SQL = """DROP USER IF EXISTS 'rivet_noaccess'@'%';
CREATE USER 'rivet_noaccess'@'%' IDENTIFIED BY 'noaccess';

DROP USER IF EXISTS 'rivet_partial'@'%';
CREATE USER 'rivet_partial'@'%' IDENTIFIED BY 'partial';
GRANT SELECT ON rivet.users TO 'rivet_partial'@'%';

FLUSH PRIVILEGES;
"""


def _perms_teardown() -> None:
    """DEVIATION (1): reached even when an assertion aborts the run.

    Tolerant of SQL-level errors (the roles may already be gone) but NOT silent:
    a failed `docker compose exec` is reported, because a leaked role turns the
    next run's setup into a failure whose cause is one run old.
    """
    _header("Cleanup")
    pg = _psql(_PG_TEARDOWN_SQL)
    if not pg.ok:
        shell.warn(f"PG cleanup did not run: {(pg.stderr or pg.stdout).strip()}")
    my = _mysql(_MYSQL_TEARDOWN_SQL)
    if not my.ok:
        shell.warn(f"MySQL cleanup did not run: {(my.stderr or my.stdout).strip()}")
    _say("  Restricted users dropped")


def permissions(argv: Sequence[str] = ()) -> int:
    """E2E: permission-denied errors fail instantly and are never retried.

    Port of `dev/scripts/test_permissions.sh`. Requires `docker compose up -d`
    (postgres + mysql) and the fixtures under `dev/fixtures/`.
    """
    if argv:
        raise Fail(f"permissions takes no arguments (got: {' '.join(argv)})", code=2)
    _require_docker()
    prebuild_rivet()

    t = Tally()  # "(expected 'p')" / "  Output: " — test_permissions.sh wording

    _header("Setup: PostgreSQL restricted users")
    # The container being reachable is a precondition; a REVOKE against a
    # non-existent role is not (psql exits 0 for it either way).
    _psql(_PG_TEARDOWN_SQL).check("postgres reachable (docker compose exec psql)")
    _psql(_PG_SETUP_SQL, stop_on_error=True).check(
        "PG restricted-user setup (CREATE ROLE / GRANT)"
    )
    _say("  PG users created: rivet_noaccess, rivet_partial")

    _header("Setup: MySQL restricted users")
    _mysql(_MYSQL_SETUP_SQL).check("MySQL restricted-user setup (CREATE USER / GRANT)")
    _say("  MySQL users created: rivet_noaccess, rivet_partial")

    (ROOT / "dev/output/perms").mkdir(parents=True, exist_ok=True)

    env = {"RUST_LOG": "error"}
    fx = "dev/fixtures"
    try:
        # ── PG: No access at all ───────────────────────────────────────────────
        _header("PG: No access -- should fail instantly, no retry")
        p, elapsed = _timed(
            cargo_rivet("run", "--config", f"{fx}/test_pg_noaccess.yaml"),
            env=env, cwd=ROOT, timeout=None,
        )
        out = p.out  # `2>&1`; the `|| true` is implicit — a non-zero exit is expected
        t.contains(out, "permission denied", "PG no-access error message")
        t.not_contains(out, "retry", "PG no-access did not retry")
        t.fast(elapsed, 3, "PG no-access completed quickly")

        # ── PG: Partial access ────────────────────────────────────────────────
        _header("PG: Partial access -- users OK, orders denied")
        out = shell.run(
            cargo_rivet("run", "--config", f"{fx}/test_pg_partial.yaml"),
            env=env, cwd=ROOT, timeout=None,
        ).out
        t.not_contains(out, "test_users_ok.*failed", "PG users export succeeded")
        t.contains(out, "permission denied for table orders", "PG orders correctly denied")
        t.not_contains(out, "retry", "PG partial did not retry on permission error")

        # ── MySQL: No access at all ───────────────────────────────────────────
        _header("MySQL: No access -- should fail instantly")
        p, elapsed = _timed(
            cargo_rivet("run", "--config", f"{fx}/test_mysql_noaccess.yaml"),
            env=env, cwd=ROOT, timeout=None,
        )
        out = p.out
        # `access denied\|Access denied` — a GNU BRE alternation, redundant under
        # `-i`; a real alternation here (see the module docstring).
        t.contains(out, "access denied|Access denied", "MySQL no-access error message")
        t.not_contains(out, "retry", "MySQL no-access did not retry")
        t.fast(elapsed, 3, "MySQL no-access completed quickly")

        # ── MySQL: Partial access ─────────────────────────────────────────────
        _header("MySQL: Partial access -- users OK, orders denied")
        out = shell.run(
            cargo_rivet("run", "--config", f"{fx}/test_mysql_partial.yaml"),
            env=env, cwd=ROOT, timeout=None,
        ).out
        t.not_contains(
            out, "test_mysql_users_ok.*failed", "MySQL users export succeeded"
        )
        t.contains(
            out, "denied|SELECT command denied", "MySQL orders correctly denied"
        )
        t.not_contains(out, "retry", "MySQL partial did not retry")

        # ── PG: Wrong password ────────────────────────────────────────────────
        _header("PG: Wrong password -- connection should fail, no retry loop")
        p, elapsed = _timed(
            cargo_rivet("run", "--config", f"{fx}/test_pg_wrongpass.yaml"),
            env=env, cwd=ROOT, timeout=None,
        )
        out = p.out
        t.contains(out, "password|authentication", "PG wrong password error message")
        t.fast(elapsed, 5, "PG wrong password completed quickly")
    finally:
        _perms_teardown()

    return t.summary()


# ══ 2. schema-evolution ════════════════════════════════════════════════════════
_SCHEMA_CONFIG = "dev/fixtures/test_schema_evolution.yaml"
_STATE_DB = ROOT / "dev/.rivet_state.db"

_SCHEMA_SETUP_SQL = """DROP TABLE IF EXISTS schema_test_table;
CREATE TABLE schema_test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    age INT
);
INSERT INTO schema_test_table (name, email, age) VALUES
    ('Alice', 'alice@test.com', 30),
    ('Bob', 'bob@test.com', 25);
"""


def _schema_run(env_log: str = "warn") -> str:
    """One `RUST_LOG=warn $RIVET run --config $CONFIG 2>&1` capture.

    The bash assigned this in a command substitution with no `|| true`, so under
    `set -e` a failing export aborted the script — kept as a hard failure, but
    with a message instead of a bare exit 1.
    """
    p = shell.run(
        cargo_rivet("run", "--config", _SCHEMA_CONFIG),
        env={"RUST_LOG": env_log}, cwd=ROOT, timeout=None,
    )
    p.check("rivet run (schema evolution)")
    return p.out


def _migrate(statement: str) -> None:
    """`$PSQL -c "…"` — a DDL step. Checked: a migration that did not apply makes
    every downstream assertion a lie about rivet."""
    _psql(command=statement, stop_on_error=True).check(f"psql -c {statement!r}")


def schema_evolution(argv: Sequence[str] = ()) -> int:
    """E2E: schema-evolution detection driven by real PostgreSQL migrations.

    Port of `dev/scripts/test_schema_evolution.sh`.
    """
    if argv:
        raise Fail(
            f"schema-evolution takes no arguments (got: {' '.join(argv)})", code=2
        )
    _require_docker()
    prebuild_rivet()

    # test_schema_evolution.sh wording: "(expected 'p' in output)" / "  Output was: "
    t = Tally(expected_suffix="(expected '{pattern}' in output)", output_prefix="  Output was: ")

    _header("Setup: create test table")
    (ROOT / "dev/output/schema").mkdir(parents=True, exist_ok=True)
    shell.rm_rf(_STATE_DB)
    _psql(_SCHEMA_SETUP_SQL, stop_on_error=True).check("schema_test_table setup")
    _say("  Table created with columns: id, name, email, age")

    try:
        _header("Step 1: First export -- baseline schema stored")
        out = _schema_run()
        t.not_contains(out, "schema changed", "no schema change on first run")

        _header("Step 2: Same schema -- no change expected")
        out = _schema_run()
        t.not_contains(out, "schema changed", "no schema change on identical schema")

        _header("Step 3: Migration -- ADD COLUMN phone")
        _migrate("ALTER TABLE schema_test_table ADD COLUMN phone VARCHAR(20);")
        _migrate("UPDATE schema_test_table SET phone = '+1-555-0100' WHERE id = 1;")
        out = _schema_run()
        t.contains(out, "schema changed", "schema change detected after ADD COLUMN")
        t.contains(out, "added", "reports added columns")
        t.contains(out, "phone", "mentions phone column")

        _header("Step 4: Same schema after migration -- no change")
        out = _schema_run()
        t.not_contains(out, "schema changed", "no change after schema stabilized")

        _header("Step 5: Migration -- DROP COLUMN age")
        _migrate("ALTER TABLE schema_test_table DROP COLUMN age;")
        out = _schema_run()
        t.contains(out, "schema changed", "schema change detected after DROP COLUMN")
        t.contains(out, "removed", "reports removed columns")
        t.contains(out, "age", "mentions age column")

        _header("Step 6: Migration -- ALTER COLUMN id type to BIGINT")
        _migrate("ALTER TABLE schema_test_table ALTER COLUMN id TYPE BIGINT;")
        out = _schema_run()
        t.contains(out, "schema changed", "schema change detected after ALTER TYPE")
        t.contains(out, "type changed", "reports type changes")

        _header("Step 7: Migration -- ADD status + DROP phone simultaneously")
        _migrate(
            "ALTER TABLE schema_test_table ADD COLUMN status VARCHAR(20) DEFAULT 'active';"
        )
        _migrate("ALTER TABLE schema_test_table DROP COLUMN phone;")
        out = _schema_run()
        t.contains(out, "schema changed", "schema change detected on combined migration")
        t.contains(out, "added", "reports added column")
        t.contains(out, "status", "mentions status")
        t.contains(out, "removed", "reports removed column")
        t.contains(out, "phone", "mentions phone removed")

        _header("Step 8: Verify metrics recorded for all runs")
        metrics = shell.run(
            cargo_rivet("metrics", "--config", _SCHEMA_CONFIG), cwd=ROOT, timeout=None
        )
        metrics.check("rivet metrics")
        # `grep -c "schema_test"` counts LINES, not occurrences.
        runs = sum(1 for line in metrics.out.splitlines() if "schema_test" in line)
        # DEVIATION (5): the bash asserted `grep -qi 7` on this count, so its own
        # "(or more)" promise failed at 8 and passed at 17.
        t.at_least(runs, 7, "7 runs recorded in metrics (or more)")
    finally:
        # DEVIATION (1): the fixture table and the state db no longer survive a
        # failed assertion — a stale baseline schema makes step 1 of the NEXT run
        # report a change and fail for the previous run's reason.
        _header("Cleanup")
        drop = _psql(command="DROP TABLE IF EXISTS schema_test_table;")
        if not drop.ok:
            shell.warn(f"could not drop schema_test_table: {(drop.stderr or drop.stdout).strip()}")
        shell.rm_rf(_STATE_DB)
        _say("  Test table dropped, state cleaned")

    return t.summary()


# ══ 3/4. toxiproxy ═════════════════════════════════════════════════════════════
TOXI = "http://localhost:8474"
_TOXI_CONFIG = "dev/fixtures/test_toxiproxy_pg.yaml"
_TOXI_HINT = "docker compose up -d postgres toxiproxy"


def _toxi_request(
    method: str, path: str, payload: dict | None = None, *, base: str = TOXI
) -> tuple[int, str]:
    """One Toxiproxy API call. Returns `(status, body)`; an HTTP error status is
    RETURNED, a transport error RAISES.

    That split is deviation 10: `curl -sf` collapses both into "non-zero", which
    is why "409 already exists" and "nothing is listening on 8474" printed the
    same reassuring line.
    """
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return int(r.status), r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return int(e.code), e.read().decode(errors="replace")
    except (urllib.error.URLError, OSError) as e:
        raise Fail(f"Toxiproxy API unreachable at {base}: {e}", hint=_TOXI_HINT)


def _create_proxy(name: str, listen: str, upstream: str) -> None:
    status, body = _toxi_request(
        "POST", "/proxies", {"name": name, "listen": listen, "upstream": upstream}
    )
    if 200 <= status < 300:
        _say(" OK")
    elif status == 409:
        # Toxiproxy's own "proxy already exists" — the ONE case the bash's
        # fallback message was actually right about.
        _say(" (already exists)")
    else:
        raise Fail(
            f"creating proxy {name!r} failed: HTTP {status} {body.strip()}",
            hint=_TOXI_HINT,
        )


def setup_toxiproxy(argv: Sequence[str] = ()) -> int:
    """Configure the pg + mysql proxies after `docker compose up -d toxiproxy`.

    Port of `dev/scripts/setup_toxiproxy.sh`.
    """
    if argv:
        raise Fail(f"setup-toxiproxy takes no arguments (got: {' '.join(argv)})", code=2)

    _say("→ Creating Postgres proxy (localhost:15432 → postgres:5432)...")
    _create_proxy("pg", "0.0.0.0:15432", "postgres:5432")

    _say("→ Creating MySQL proxy (localhost:13306 → mysql:3306)...")
    _create_proxy("mysql", "0.0.0.0:13306", "mysql:3306")

    _say()
    _say("Proxies configured. Inject faults with:")
    _say("  # Add 2s latency")
    _say(
        f"""  curl -X POST {TOXI}/proxies/pg/toxics -d '{{"name":"latency","type":"latency","attributes":{{"latency":2000}}}}'"""
    )
    _say("  # Reset connection after 5KB")
    _say(
        f"""  curl -X POST {TOXI}/proxies/pg/toxics -d '{{"name":"limit","type":"limit_data","attributes":{{"bytes":5000}}}}'"""
    )
    _say("  # Remove all toxics")
    _say(f"  curl -X DELETE {TOXI}/proxies/pg/toxics/latency")
    _say(f"  curl -X DELETE {TOXI}/proxies/pg/toxics/limit")
    return 0


def _add_toxic(proxy: str, spec: dict) -> None:
    status, body = _toxi_request("POST", f"/proxies/{proxy}/toxics", spec)
    if not 200 <= status < 300:
        raise Fail(
            f"adding toxic {spec.get('name')!r} to proxy {proxy!r} failed: "
            f"HTTP {status} {body.strip()}",
            hint=_TOXI_HINT,
        )


def _remove_toxic(proxy: str, name: str, *, tolerate_missing: bool = False) -> None:
    status, body = _toxi_request("DELETE", f"/proxies/{proxy}/toxics/{name}")
    if 200 <= status < 300:
        return
    if status == 404 and tolerate_missing:
        return
    raise Fail(
        f"removing toxic {name!r} from proxy {proxy!r} failed: "
        f"HTTP {status} {body.strip()}",
        hint=_TOXI_HINT,
    )


def retry_toxiproxy(argv: Sequence[str] = ()) -> int:
    """Retry-resilience E2E through Toxiproxy (Q1–Q8).

    Port of `dev/scripts/test_retry_toxiproxy.sh`.

        Rivet ──► localhost:15432 (Toxiproxy) ──► postgres:5432

    Prerequisites:
        docker compose up -d postgres toxiproxy
        python dev/pytools/dev_scripts.py setup-toxiproxy
        cargo run --release --bin seed -- --target pg --users 10000
    """
    if argv:
        raise Fail(f"retry-toxiproxy takes no arguments (got: {' '.join(argv)})", code=2)

    rivet = installed_or_cargo_rivet()

    # DEVIATION (11): both preconditions checked BEFORE Q1, so a missing proxy is
    # a named failure instead of a silent `curl -sf` abort at Q2.
    status, _ = _toxi_request("GET", "/proxies/pg")
    if status == 404:
        raise Fail(
            "Toxiproxy has no 'pg' proxy",
            hint="python dev/pytools/dev_scripts.py setup-toxiproxy",
        )
    if not 200 <= status < 300:
        raise Fail(f"Toxiproxy API returned HTTP {status} for /proxies/pg", hint=_TOXI_HINT)

    def export(*extra: str) -> shell.Proc:
        return shell.stream(
            [*rivet, "run", "--config", _TOXI_CONFIG, *extra],
            env={"RUST_LOG": "info"}, cwd=ROOT, timeout=None,
        )

    _say()
    _say("╔══════════════════════════════════════════════════════╗")
    _say("║  Toxiproxy Retry Resilience Test                    ║")
    _say("╚══════════════════════════════════════════════════════╝")
    _say()

    try:
        # ── Q1: Baseline ──────────────────────────────────────────────────────
        _say("── Q1: Baseline (clean proxy, no faults) ──")
        export("--validate").check("Q1 baseline export")
        _say("✓ Q1 passed")
        _say()

        # ── Q2: Inject latency ────────────────────────────────────────────────
        _say("── Q2: Injecting 3s latency on every packet ──")
        _add_toxic(
            "pg",
            {"name": "latency", "type": "latency", "attributes": {"latency": 3000}},
        )
        _say("   Toxic 'latency' added.")

        # ── Q3: Export under latency ──────────────────────────────────────────
        _say("── Q3: Export under latency (expect slower but success) ──")
        export("--validate").check("Q3 export under 3s latency")
        _say("✓ Q3 passed")
        _say()

        # ── Q4: Remove latency ────────────────────────────────────────────────
        _say("── Q4: Removing latency toxic ──")
        _remove_toxic("pg", "latency")
        _say("   Toxic 'latency' removed.")
        _say()

        # ── Q5: Inject connection kill ────────────────────────────────────────
        _say("── Q5: Injecting connection kill after 5 KB ──")
        _add_toxic(
            "pg",
            {"name": "limit", "type": "limit_data", "attributes": {"bytes": 5000}},
        )
        _say("   Toxic 'limit' added (connection drops after 5 KB).")

        # ── Q6: Export under connection kill ──────────────────────────────────
        # The only tolerated failure in the script: a 5 KB budget may legitimately
        # defeat the retries.
        _say("── Q6: Export under connection kill (expect retries) ──")
        if export().ok:
            _say("✓ Q6 passed (retried and succeeded)")
        else:
            _say("⚠ Q6: export failed — expected under harsh 5 KB limit.")
            _say("  Check logs above for 'retry' messages — that confirms retry logic works.")
        _say()

        # ── Q7: Remove connection-kill toxic ──────────────────────────────────
        _say("── Q7: Removing limit toxic ──")
        _remove_toxic("pg", "limit")
        _say("   Toxic 'limit' removed.")
        _say()

        # ── Q8: Final baseline ────────────────────────────────────────────────
        _say("── Q8: Final baseline (proxy clean again) ──")
        export("--validate").check("Q8 final baseline export")
        _say("✓ Q8 passed")
        _say()
    finally:
        # DEVIATION (1): a leaked toxic silently poisons the NEXT run's Q1 (a
        # 3 s-per-packet "baseline"), so both are removed on every exit path.
        for name in ("latency", "limit"):
            try:
                _remove_toxic("pg", name, tolerate_missing=True)
            except Fail as e:
                shell.warn(f"could not remove toxic {name!r}: {e.message}")

    _say("╔══════════════════════════════════════════════════════╗")
    _say("║  All Toxiproxy tests completed.                     ║")
    _say("╚══════════════════════════════════════════════════════╝")
    return 0


# ══ 5. uat-smoke ═══════════════════════════════════════════════════════════════
UAT_LEDGER = Path("/tmp/rivet_uat_smoke.txt")


@dataclass
class Ledger:
    """The `PASS/FAIL/SKIP <label>` file the plan is reconciled against.

    Written incrementally (`>> "$U"`), so a hard abort still leaves the cells
    that did run — the reason the bash appended rather than buffering.
    """

    path: Path
    lines: list[str] = field(default_factory=list)

    def _write(self, line: str) -> None:
        self.lines.append(line)
        with self.path.open("a") as fh:
            fh.write(line + "\n")

    def ok(self, label: str) -> None:
        self._write(f"PASS {label}")

    def bad(self, label: str) -> None:
        self._write(f"FAIL {label}")

    def skipped(self, label: str) -> None:
        self._write(f"SKIP {label}")

    def verdict(self, passed: bool, label: str) -> None:
        (self.ok if passed else self.bad)(label)

    def count(self, prefix: str) -> int:
        return sum(1 for line in self.lines if line.startswith(prefix))


def _curl_sf(url: str, *, timeout: float = 5.0) -> bool:
    """`curl -sf URL` semantics: a 4xx/5xx is a FAILURE.

    `shell.http_up` deliberately treats any HTTP response as "up"; that is the
    wrong oracle for `T1`, which asserts Prometheus reports itself healthy.
    """
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return 200 <= int(r.status) < 400
    except (urllib.error.URLError, OSError):
        return False


def _mysql_container_listed() -> bool:
    """`docker compose ps mysql 2>/dev/null | grep -q mysql`.

    Parsed row-wise so the `NAME IMAGE … SERVICE …` header can never satisfy the
    match. Still a NAME test, not a health test — see the module docstring for
    why that is kept.
    """
    p = shell.compose("ps", "mysql", cwd=ROOT)
    if not p.ok:
        return False
    rows = p.stdout.splitlines()
    if rows and rows[0].lstrip().startswith("NAME"):
        rows = rows[1:]
    return any("mysql" in row for row in rows if row.strip())


def uat_smoke(argv: Sequence[str] = ()) -> int:
    """Quick PG-focused smoke for USER_TEST_PLAN.md (MySQL cells only if up).

    Port of `dev/scripts/run_uat_smoke.sh`. Review `/tmp/rivet_uat_smoke.txt` and
    update Actual/Pass in the plan.
    """
    if argv:
        raise Fail(f"uat-smoke takes no arguments (got: {' '.join(argv)})", code=2)

    # DEVIATION (4): without the binary the bash printed 30 FAILs.
    shell.require(
        "rivet", hint="cargo install --path . (or add target/release to PATH)"
    )
    shell.rm_rf(UAT_LEDGER)
    L = Ledger(UAT_LEDGER)

    def rivet(*args: str, env: dict[str, str] | None = None) -> shell.Proc:
        return shell.run(["rivet", *args], env=env, cwd=ROOT, timeout=None)

    def exit0(label: str, *args: str, env: dict[str, str] | None = None) -> None:
        """`rivet … >/dev/null 2>&1 && pass X || fail X`"""
        L.verdict(rivet(*args, env=env).ok, label)

    def matches(
        label: str, pattern: str, *args: str,
        env: dict[str, str] | None = None,
        merge_stderr: bool = True,
        first_line_only: bool = False,
        flags: int = 0,
    ) -> None:
        """The `rivet … 2>&1 | grep -qE 'p'` cells.

        `merge_stderr=False` reproduces `2>/dev/null` (stdout only);
        `first_line_only` reproduces `| head -1`.
        """
        p = rivet(*args, env=env)
        text = p.out if merge_stderr else p.stdout
        if first_line_only:
            text = text.splitlines()[0] if text.splitlines() else ""
        L.verdict(re.search(pattern, text, flags) is not None, label)

    mysql_up = _mysql_container_listed()
    if not mysql_up:
        # The bash skipped silently on `docker compose ps` failing for ANY reason,
        # including docker being absent. Named, since a skip is not a pass.
        shell.warn(
            "mysql container not listed by `docker compose ps mysql` — "
            "B3/C5/C6/G2 will be SKIPped, not passed"
        )

    # ── A: CLI surface ────────────────────────────────────────────────────────
    matches("A1", "run", "--help")
    matches("A2", "compdef|rivet", "completions", "zsh",
            merge_stderr=False, first_line_only=True)
    matches("A3", "Error|error|no such|os error|not found",
            "check", "--config", "/nonexistent/no.yaml", flags=re.IGNORECASE)

    # ── B: check / doctor ─────────────────────────────────────────────────────
    exit0("B1", "check", "--config", "dev/workbench/pg_full.yaml")
    exit0("B2", "check", "--config", "dev/workbench/pg_full.yaml",
          "--export", "pg_users_csv")
    if mysql_up:
        exit0("B3", "check", "--config", "dev/workbench/mysql_full.yaml")
    else:
        L.skipped("B3_no_mysql")
    exit0("B4", "doctor", "--config", "dev/workbench/pg_full.yaml")

    # ── C: exports ────────────────────────────────────────────────────────────
    exit0("C1_sample", "run", "--config", "dev/workbench/pg_full.yaml",
          "--export", "pg_users_csv", "--validate")
    matches("C2", "validat.*pass|pass.*validat",
            "run", "--config", "dev/workbench/pg_full.yaml",
            "--export", "pg_users_parquet", "--validate", flags=re.IGNORECASE)
    exit0("C3", "run", "--config", "dev/workbench/pg_incremental.yaml",
          "--export", "pg_orders_incremental")
    exit0("C4", "run", "--config", "dev/workbench/pg_incremental.yaml",
          "--export", "pg_orders_incremental")
    if mysql_up:
        exit0("C5", "run", "--config", "dev/workbench/mysql_full.yaml")
        exit0("C6", "run", "--config", "dev/workbench/mysql_incremental.yaml")
    else:
        L.skipped("C5_C6_no_mysql")

    # ── D: state / metrics ────────────────────────────────────────────────────
    exit0("D1", "state", "show", "--config", "dev/workbench/pg_incremental.yaml")
    exit0("D2", "metrics", "--config", "dev/workbench/pg_incremental.yaml",
          "--last", "5")

    # ── E: chunked ────────────────────────────────────────────────────────────
    exit0("E1", "run", "--config", "dev/scenarios/chunked_postgres_bench.yaml",
          "--export", "bench_content_seq")
    exit0("E2_R1", "run", "--config", "dev/scenarios/chunked_postgres_bench.yaml",
          "--export", "bench_content_p4_serial", "--validate")

    # ── F: meta columns / compression ─────────────────────────────────────────
    exit0("F1", "run", "--config", "dev/fixtures/test_meta_columns.yaml", "--validate")
    exit0("F2", "run", "--config", "dev/fixtures/test_compression.yaml")
    exit0("F3", "run", "--config", "dev/fixtures/test_compression.yaml",
          "--export", "users_skip_empty")

    # ── G: structured config ──────────────────────────────────────────────────
    L.verdict(
        rivet("check", "--config", "dev/workbench/pg_structured.yaml",
              env={"PGPASSWORD": "rivet"}).ok,
        "G1",
    )
    if mysql_up:
        exit0("G2", "check", "--config", "dev/workbench/mysql_structured.yaml")
    else:
        L.skipped("G2_no_mysql")

    # ── H: preflight ──────────────────────────────────────────────────────────
    exit0("H1", "check", "--config", "dev/workbench/pg_preflight_demos.yaml")
    matches("H2", r"\[FAIL\]|FAIL|Source error|db error|auth|password|denied|refused",
            "doctor", "--config", "dev/fixtures/test_pg_wrongpass.yaml",
            flags=re.IGNORECASE)

    # ── J/L/M/N/O/P/S/T ───────────────────────────────────────────────────────
    exit0("J1", "run", "--config", "dev/scenarios/time_window_postgres.yaml")

    matches("L1", ".", "run", "--config", "dev/fixtures/test_stdout.yaml",
            merge_stderr=False, first_line_only=True)

    exit0("M1", "run", "--config", "dev/fixtures/test_params.yaml",
          "--param", "MAX_ID=10")
    exit0("M3", "check", "--config", "dev/fixtures/test_params.yaml",
          "--param", "MAX_ID=100")

    exit0("N1", "run", "--config", "dev/fixtures/test_quality.yaml",
          "--export", "users_quality_pass", "--validate")

    matches("O1", "batch_size_memory|batch_size_memory_mb|computed batch_size",
            "run", "--config", "dev/fixtures/test_memory_batch.yaml",
            env={"RUST_LOG": "info"})

    exit0("P1", "run", "--config", "dev/fixtures/test_file_split.yaml", "--validate")
    n = len(list((ROOT / "dev/output").glob("users_split_*_part*.parquet")))
    if n >= 2:
        L.ok(f"P2(n={n})")
    else:
        L.bad("P2")

    exit0("S1", "run", "--config", "dev/scenarios/chunked_postgres_bench.yaml",
          "--export", "bench_content_p4_balanced")
    exit0("S4", "run", "--config", "dev/scenarios/chunked_postgres_bench.yaml",
          "--export", "bench_content_p4", "--parallel-exports")

    L.verdict(_curl_sf("http://localhost:9090/-/healthy"), "T1")

    # ── report (`cat "$U"`, `---`, three bare counts) ─────────────────────────
    for line in L.lines:
        _say(line)
    _say("---")
    passed, failed, skipped = L.count("PASS"), L.count("FAIL"), L.count("SKIP")
    _say(str(passed))
    _say(str(failed))
    _say(str(skipped))
    shell.log(f"{passed} passed, {failed} failed, {skipped} skipped → {UAT_LEDGER}")

    # DEVIATION (3): the bash always exited 0 (its last command was
    # `grep -c '^SKIP' … || true`), so a FAIL could not fail anything.
    return 1 if failed else 0


# ══ 6. live ════════════════════════════════════════════════════════════════════
# The readability filter from `test_live.sh`. Note `|ok` matches almost any line
# containing "ok" — kept, because it is only a DISPLAY filter now (deviation 6).
_LIVE_FILTER: Pattern[str] = re.compile(
    r"test .* ok|test .* FAILED|test .* ignored|running [0-9]+ test|FAILED|ok"
)

_LIVE_USAGE = (
    "usage: live [pg|postgres|mysql|filter <pattern>]  (no selector = all live tests)"
)


def _run_filtered(
    argv: Sequence[str], pattern: Pattern[str], *, cwd: Path = ROOT
) -> shell.Proc:
    """`cmd 2>&1 | grep -E …`, live, but the VERDICT is cmd's exit status.

    The one place `shell.run`/`shell.stream` cannot express the shape: `run`
    captures (no live output) and `stream` inherits the terminal (nothing to
    filter). `$?` after the bash pipeline was grep's status, and the pattern
    matches the literal word `FAILED`, so a failing suite exited 0.
    """
    proc = subprocess.Popen(
        [str(a) for a in argv],
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    kept: list[str] = []
    assert proc.stdout is not None
    for raw in proc.stdout:
        line = raw.rstrip("\n")
        if pattern.search(line):
            kept.append(line)
            _say(line)
    rc = proc.wait()
    return shell.Proc(tuple(str(a) for a in argv), rc, "\n".join(kept), "")


def check_service(name: str, host: str, port: int) -> bool:
    """`nc -z -w1 host port`, as a socket connect (deviation 8)."""
    if not shell.tcp_open(host, port, timeout=1.0):
        _say(f"✗ {name} not reachable on {host}:{port}")
        _say(f"  hint: docker compose up -d {name}")
        return False
    _say(f"✓ {name} reachable on {host}:{port}")
    return True


def _require_service(name: str, host: str, port: int) -> None:
    """`check_service … ` under `set -e`: a down service aborts the run before a
    release build is spent on it."""
    if not check_service(name, host, port):
        raise Fail(f"{name} not reachable on {host}:{port}",
                   hint=f"docker compose up -d {name}")


def _build_release() -> None:
    _say()
    _say("▶ building release binary...")
    shell.stream(["cargo", "build", "--release", "-q"], cwd=ROOT, timeout=None).check(
        "cargo build --release -q"
    )


def live(argv: Sequence[str] = ()) -> int:
    """One-command live integration test runner (`cargo test --release -- --ignored`).

    Port of `dev/scripts/test_live.sh`:

        live                    # all live tests (postgres + mysql)
        live pg                 # postgres only
        live mysql              # mysql only
        live filter <pattern>   # tests matching a pattern
    """
    shell.require("cargo", hint="install Rust (https://rustup.rs)")
    selector = argv[0] if argv else ""
    extra = list(argv[1:])

    if selector in ("pg", "postgres"):
        if extra:
            raise Fail(f"live {selector} takes no further arguments\n{_LIVE_USAGE}", code=2)
        _require_service("postgres", "127.0.0.1", 5432)
        _build_release()
        _say("▶ running postgres live tests...")
        p = _run_filtered(
            ["cargo", "test", "--release", "--", "--ignored", "postgres"], _LIVE_FILTER
        )
    elif selector == "mysql":
        if extra:
            raise Fail(f"live mysql takes no further arguments\n{_LIVE_USAGE}", code=2)
        _require_service("mysql", "127.0.0.1", 3306)
        _build_release()
        _say("▶ running mysql live tests...")
        p = _run_filtered(
            ["cargo", "test", "--release", "--", "--ignored", "mysql"], _LIVE_FILTER
        )
    elif selector == "filter":
        # `pattern="${2:?usage: test_live.sh filter <pattern>}"`
        if not extra or not extra[0]:
            raise Fail("usage: live filter <pattern>", code=2)
        if len(extra) > 1:
            raise Fail(f"live filter takes ONE pattern\n{_LIVE_USAGE}", code=2)
        pattern = extra[0]
        # `|| true` on both probes: the pattern may not need either service.
        check_service("postgres", "127.0.0.1", 5432)
        check_service("mysql", "127.0.0.1", 3306)
        _build_release()
        _say(f"▶ running live tests matching '{pattern}'...")
        p = shell.stream(
            ["cargo", "test", "--release", "--", "--ignored", pattern],
            cwd=ROOT, timeout=None,
        )
    elif selector == "":
        _require_service("postgres", "127.0.0.1", 5432)
        _require_service("mysql", "127.0.0.1", 3306)
        _build_release()
        _say("▶ running all live tests...")
        p = shell.stream(
            ["cargo", "test", "--release", "--", "--ignored"], cwd=ROOT, timeout=None
        )
    else:
        # DEVIATION (7): the bash `*)` arm ran the WHOLE ignored suite for a typo
        # like `postgress`, which reads as "the subset passed".
        raise Fail(f"unknown selector: {selector}\n{_LIVE_USAGE}", code=2)

    # DEVIATION (6): cargo's exit status is the verdict.
    return 0 if p.ok else 1


# ══ 7. bench ═══════════════════════════════════════════════════════════════════
_BENCH_USAGE = """usage: bench [run|save <tag>|compare <tag>|group <name>]

  bench                 run all groups, print results
  bench save <tag>      run + save baseline (e.g. "v0_5_0")
  bench compare <tag>   run + compare against a saved baseline
  bench group <name>    run a single benchmark group

Criterion suites (in-process microbenchmarks):
  hot_paths       — CSV write, hash column, Parquet flush, MySQL parse
  resource_aware  — auto_shrink splits, compression codecs, row group sizing,
                    quality uniqueness cap  (Phase 2 stabilization)

End-to-end resource benchmarks (wall time + peak RSS against live Postgres) are
a separate harness — dev/bench/smoke.py, which replaced the ./dev/bench/
run_bench.sh [compression|row_group|batch_memory|quality] the bash comments still
advertise — so nothing here times, parses or post-processes anything.

Baselines live in target/criterion/ (Criterion writes them).
HTML reports: target/criterion/<group>/report/index.html"""


def bench(argv: Sequence[str] = ()) -> int:
    """Criterion benchmark runner with optional baseline save/compare.

    Port of `dev/scripts/bench.sh`. The measurement is untouched: Criterion is
    invoked with exactly the same `cargo bench --` arguments, and nothing here
    times, parses or post-processes the results.
    """
    shell.require("cargo", hint="install Rust (https://rustup.rs)")
    cmd = argv[0] if argv else "run"
    rest = list(argv[1:])
    if len(rest) > 1:
        raise Fail(f"bench {cmd} takes at most one argument\n{_BENCH_USAGE}", code=2)

    def bench_run(*args: str) -> shell.Proc:
        # timeout=None: a benchmark suite legitimately runs for many minutes, and
        # a timeout kill would be indistinguishable from a broken benchmark.
        return shell.stream(["cargo", "bench", *args], cwd=ROOT, timeout=None)

    if cmd == "save":
        # `tag="${2:-main}"` — a bare `bench save` silently overwrites the "main"
        # baseline. Kept (it is the documented default) but no longer silent.
        tag = rest[0] if rest else "main"
        if not rest:
            shell.warn("no tag given — saving/overwriting the baseline named 'main'")
        _say(f"▶ saving baseline '{tag}'")
        bench_run("--", "--save-baseline", tag).check("cargo bench --save-baseline")
        _say(f"✓ baseline '{tag}' saved (target/criterion/)")
    elif cmd == "compare":
        tag = rest[0] if rest else "main"
        if not rest:
            shell.warn("no tag given — comparing against the baseline named 'main'")
        _say(f"▶ comparing against baseline '{tag}'")
        bench_run("--", "--baseline", tag).check("cargo bench --baseline")
    elif cmd == "group":
        # `group="${2:?usage: bench.sh group <group_name>}"`
        if not rest or not rest[0]:
            raise Fail("usage: bench group <group_name>", code=2)
        group = rest[0]
        _say(f"▶ running benchmark group '{group}'")
        bench_run("--", group).check(f"cargo bench -- {group}")
    elif cmd == "run":
        _say("▶ running all benchmark groups")
        bench_run().check("cargo bench")
        _say()
        _say("HTML reports: target/criterion/*/report/index.html")
    else:
        # DEVIATION (9): the bash `run|*)` arm ran the ENTIRE suite for a typo'd
        # command, so `bench compre v1` "succeeded" without comparing anything.
        raise Fail(f"unknown command: {cmd}\n{_BENCH_USAGE}", code=2)
    return 0


# ══ 8. regen-docker-configs ════════════════════════════════════════════════════
_REGEN_TABLES: tuple[str, ...] = (
    "users",
    "orders",
    "events",
    "page_views",
    "content_items",
    "orders_sparse",
    "orders_sparse_for_export",
)

# `sed 's|path: ./output|path: ./dev/output|'` — one substitution per line, and a
# literal (sed read `.` as "any char", which nothing in a generated file relies
# on). `./dev/output` is gitignored, matching the other dev configs.
_SED_FROM = "path: ./output"
_SED_TO = "path: ./dev/output"


def _rel(path: Path) -> str:
    """Repo-relative when possible, absolute otherwise. `Path.relative_to` RAISES
    for a path outside the repo, and a message formatter that can raise turns a
    generator failure into a traceback about the message."""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _init_config(source: str, out: Path, *args: str) -> None:
    """`cargo run --release -q -- init … -o <out>`, then the path fixup, written
    atomically.

    DEVIATION (12): `init` writes `-o` directly, truncating any previous content
    before it can fail — a crashed generator left a half-written yaml that the
    next diff reported as legitimate drift. The generator writes a scratch file,
    and only a COMPLETE result replaces the target.
    """
    scratch = out.with_suffix(out.suffix + ".gen")
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        shell.run(
            ["cargo", "run", "--release", "-q", "--", "init", "--source", source,
             *args, "-o", str(scratch)],
            cwd=ROOT,
            timeout=None,
        ).check(f"rivet init -o {_rel(out)}")
        text = scratch.read_text()
        fixed = "\n".join(
            line.replace(_SED_FROM, _SED_TO, 1) for line in text.split("\n")
        )
        shell.atomic_write(out, fixed)
    finally:
        shell.rm_rf(scratch)


def regen_docker_configs(argv: Sequence[str] = ()) -> int:
    """Regenerate the YAML scaffolds from the docker-compose PG + MySQL databases.

    Port of `dev/scripts/regenerate_docker_init_configs.sh`. Prerequisites:
    `docker compose up -d postgres mysql` (row-estimate comments reflect
    `pg_class` / `TABLE_ROWS`, so seed first if you care about them).
    """
    if argv:
        raise Fail(
            f"regen-docker-configs takes no arguments (got: {' '.join(argv)})", code=2
        )
    shell.require("cargo", hint="install Rust (https://rustup.rs)")

    pg_url = os.environ.get(
        "PG_URL", "postgresql://rivet:rivet@localhost:5432/rivet?sslmode=disable"
    )
    mysql_url = os.environ.get("MYSQL_URL", "mysql://rivet:rivet@localhost:3306/rivet")

    gen = ROOT / "dev/init_generated"
    (gen / "pg").mkdir(parents=True, exist_ok=True)
    (gen / "mysql").mkdir(parents=True, exist_ok=True)

    for table in _REGEN_TABLES:
        shell.log(f"pg/{table}.yaml")
        _init_config(pg_url, gen / "pg" / f"{table}.yaml", "--table", table)

    for table in _REGEN_TABLES:
        shell.log(f"mysql/{table}.yaml")
        _init_config(mysql_url, gen / "mysql" / f"{table}.yaml", "--table", table)

    # One file per database schema: all tables/views in public (PG) or in the
    # database named by the MySQL URL.
    shell.log("pg/schema_public.yaml")
    _init_config(pg_url, gen / "pg/schema_public.yaml", "--schema", "public")
    shell.log("mysql/schema_rivet.yaml")
    _init_config(mysql_url, gen / "mysql/schema_rivet.yaml")

    _say(
        "Done. Set DATABASE_URL then, e.g.:  "
        "rivet check --config dev/init_generated/pg/schema_public.yaml"
    )
    return 0


# ══ dispatch ═══════════════════════════════════════════════════════════════════
COMMANDS: dict[str, Callable[[Sequence[str]], int]] = {
    "permissions": permissions,
    "schema-evolution": schema_evolution,
    "retry-toxiproxy": retry_toxiproxy,
    "setup-toxiproxy": setup_toxiproxy,
    "uat-smoke": uat_smoke,
    "live": live,
    "bench": bench,
    "regen-docker-configs": regen_docker_configs,
}

USAGE = """Usage: python dev/pytools/dev_scripts.py <command> [args]

Commands:
  permissions             permission-denied errors fail fast, never retry
  schema-evolution        schema-drift detection across real PG migrations
  retry-toxiproxy         retry resilience through Toxiproxy (Q1-Q8)
  setup-toxiproxy         create the pg + mysql proxies
  uat-smoke               USER_TEST_PLAN.md smoke → /tmp/rivet_uat_smoke.txt
  live [pg|mysql|filter <pat>]     cargo test --release -- --ignored
  bench [run|save <tag>|compare <tag>|group <name>]   Criterion benchmarks
  regen-docker-configs    regenerate dev/init_generated/** from the dev DBs"""


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help", "help"):
        print(USAGE, file=sys.stderr if not args else sys.stdout)
        # No arguments is a usage ERROR (there is no default script); an explicit
        # --help is not.
        return 2 if not args else 0
    name, rest = args[0], args[1:]
    fn = COMMANDS.get(name)
    if fn is None:
        # DEVIATION (bash class 1): a `case` with no default arm — or one that
        # falls through to a catch-all — reports success having done nothing (or
        # the wrong thing). An unknown command is always an error here.
        print(f"unknown command: {name}", file=sys.stderr)
        print(USAGE, file=sys.stderr)
        return 2
    return fn(rest)


if __name__ == "__main__":
    shell.main(lambda: main_cli())
