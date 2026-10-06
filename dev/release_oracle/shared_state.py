"""Four configs, ONE export name, ONE shared Postgres state — at the same time.

The deployment shape the docs recommend (a shared state backend) meets the config
shape every rollout produces (the same template per database): four `users`
exports — MySQL, PostgreSQL, SQL Server, MongoDB — each into its own prefix and
dataset, sharing `rivet_state`, started together. Blessed, then crashed at once,
then crashed one at a time while the others run clean; the CDC cycle (anchor +
backfill → load → delta → load) and the batch cycle (full → load → full → load).

The three defects this shape hid, each measured before the fix: a `running` row
of one config superseded by ANOTHER config's success (gc could collect a live
writer's parts); one `cdc_snapshot` row per export NAME (config B skipped its
baseline because config A had one); the by-name load spec retyped by the last
writer. The cell runs `tests/live/live_shared_state_same_name.rs` through the
Rig against the gate's binary, so the oracles are the test's: the source, `bq`,
and the state DB read with a plain Postgres client.

SKIP — never a silent pass — without cargo, the Postgres state URL, the BigQuery
project/bucket or `gcloud` (the REST token).
"""

from __future__ import annotations

import os
import socket
import re
import tempfile
from pathlib import Path
from collections.abc import Callable

from .core import RAN_LIVE_TESTS, SKIP_ALLOWED, Ledger, self_skipped, ROOT, have, nextest_filter, nextest_passed, rivet_bin, run, test_passed

TESTS = (
    "same_named_configs_share_a_postgres_state_cdc_cycle",
    "same_named_configs_share_a_postgres_state_batch_cycle",
)


def _listening(port: int) -> bool:
    """Whether something accepts TCP connections on localhost:`port`."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            return True
    except OSError:
        return False


def run_rig_tests(led: Ledger, scenario: str, tests: tuple[str, ...],
                  cell: Callable[[str], str], msg: Callable[[str], str], *,
                  cloud: bool = True, services: tuple[tuple[str, int], ...] = (),
                  extra_env: dict[str, str] | None = None,
                  target: tuple[str, ...] = ("--test", "live_suite")) -> None:
    """Run live tests (the Rig suite, or a cargo `target` such as `--lib`); grade each by cargo's own verdict line.

    `cloud` cells need the BigQuery project, `gcloud` and a Postgres state URL; `services`
    are local ports the tests need. A missing one is a SKIP naming it, never a FAIL.
    """
    env = {"RIVET_BIN_OVERRIDE": str(rivet_bin()), **(extra_env or {})}
    checks = [("cargo", have("cargo"))]
    if cloud:
        state = os.environ.get("RIVET_CDC_STATE_URL") or os.environ.get("RIVET_CONC_STATE_URL") or ""
        checks += [("gcloud", have("gcloud")), ("Postgres state URL", state.startswith("postgres")),
                   ("a Google credential (BQ_ORACLE_PROJECT)", bool(os.environ.get("BQ_ORACLE_PROJECT")))]
        env["RIVET_TEST_STATE_URL"] = state
    checks += [(f"{name} (:{port})", _listening(port)) for name, port in services]
    missing = [w for w, ok in checks if not ok]
    if missing:
        led.skipped("-", scenario, "rig", "postgres",
                    f"{scenario}: cannot run — missing {', '.join(missing)}",
                    f"prereq: {', '.join(missing)}")
        return
    # nextest, not plain `cargo test`: tests/live_suite.rs's own header says the
    # per-test process isolation this consolidated suite depends on is GONE under
    # the default libtest harness, where `--test-threads=1` was the mitigation.
    # `test(=X)` matches the FULL `<module>::<fn>` name, so the bare fn name never
    # matches — anchor the regex form at the end instead (same as _drive_live_tests).
    expr = nextest_filter(tests)
    if target == ("--test", "live_suite"):
        RAN_LIVE_TESTS.update(tests)
    skip_log = Path(tempfile.mkdtemp(prefix="rivet-skips-")) / "skips"
    env["RIVET_SKIP_LOG"] = str(skip_log)
    p = run(["cargo", "nextest", "run", "--manifest-path", str(ROOT / "Cargo.toml"),
             # --no-fail-fast: one failure must not cancel the rest, which then read as
             # "no PASS line" rows — two real failures showed up as seven (2026-09-27).
             *target, "--run-ignored", "all", "--no-fail-fast", "-E", expr],
            cwd=ROOT, env=env, timeout=None)
    skipped = {k.rsplit("::", 1)[-1]: v for k, v in self_skipped(skip_log).items()}
    out = (p.stdout or "") + (p.stderr or "")
    # LEAK is a test that PASSED but left a handle or child open past its end;
    # nextest's own summary counts it green ("6 passed (2 leaky)").
    passed = nextest_passed(out)
    for name in tests:
        if test_passed(name, passed) and name in skipped and not any(
                k.endswith("::" + name) for k in SKIP_ALLOWED):
            led.failed("all", scenario, cell(name), "postgres",
                       f"{msg(name)} — SELF-SKIPPED ({skipped[name]}), counted green by libtest; "
                       "allow it in core.SKIP_ALLOWED with a reason, or bring its infrastructure up",
                       "vacuous skip")
        elif test_passed(name, passed):
            led.passed("all", scenario, cell(name), "postgres", msg(name), "ok")
        else:
            # From the test's captured-output block (`--- STDOUT/STDERR: … <name> ---`):
            # the first mention is its START line and the last is the final summary,
            # neither holds evidence. The tail goes on the printed line, not only the ledger.
            m = re.search(r"--- STD(?:OUT|ERR):[^\n]*" + re.escape(name), out)
            at = m.start() if m else out.find(name)
            tail = out[at:][:4000] if at >= 0 else out[-2000:]
            led.failed("all", scenario, cell(name), "postgres",
                       f"{msg(name)} — no PASS line for {name} (failed, renamed, or filtered "
                       f"out)\n{tail[-1500:]}",
                       tail)


def _cycle(name: str) -> str:
    return "cdc" if name.endswith("cdc_cycle") else "batch"


def verify_shared_state_same_name(led: Ledger) -> None:
    led.phase("Shared state, same-named configs — four engines at once, blessed + crashed (CDC and batch)")
    run_rig_tests(
        led, "shared", TESTS,
        cell=lambda n: f"same_name:{_cycle(n)}",
        msg=lambda n: f"shared-state[{_cycle(n)}] · 4 same-named configs, one Postgres state, parallel + crashes",
    )
