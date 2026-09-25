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
import re
from collections.abc import Callable

from .core import Ledger, ROOT, have, nextest_passed, rivet_bin, run

TESTS = (
    "same_named_configs_share_a_postgres_state_cdc_cycle",
    "same_named_configs_share_a_postgres_state_batch_cycle",
)


def run_rig_tests(led: Ledger, scenario: str, tests: tuple[str, ...],
                  cell: Callable[[str], str], msg: Callable[[str], str]) -> None:
    """Run live Rig tests against the gate binary; grade each by cargo's own verdict line."""
    state = os.environ.get("RIVET_CDC_STATE_URL") or os.environ.get("RIVET_CONC_STATE_URL") or ""
    proj = os.environ.get("BQ_ORACLE_PROJECT") or run(["gcloud", "config", "get-value", "project"]).stdout.strip()
    bucket = os.environ.get("BQ_ORACLE_BUCKET", "rivet_data_test")
    missing = [w for w, ok in (
        ("cargo", have("cargo")), ("gcloud", have("gcloud")),
        ("Postgres state URL", state.startswith("postgres")), ("BigQuery project", bool(proj)),
    ) if not ok]
    if missing:
        led.skipped("-", scenario, "rig", "postgres",
                    f"{scenario}: cannot run — missing {', '.join(missing)}", "prereq")
        return
    env = {
        "RIVET_TEST_STATE_URL": state,
        "BIGQUERY_TEST_PROJECT": proj,
        "RIVET_TEST_GCS_BUCKET": bucket,
        "RIVET_BIN_OVERRIDE": str(rivet_bin()),
    }
    # nextest, not plain `cargo test`: tests/live_suite.rs's own header says the
    # per-test process isolation this consolidated suite depends on is GONE under
    # the default libtest harness, where `--test-threads=1` was the mitigation.
    # `test(=X)` matches the FULL `<module>::<fn>` name, so the bare fn name never
    # matches — anchor the regex form at the end instead (same as _drive_live_tests).
    expr = " or ".join(f"test(/{t}$/)" for t in tests)
    p = run(["cargo", "nextest", "run", "--manifest-path", str(ROOT / "Cargo.toml"),
             "--test", "live_suite", "--run-ignored", "all", "-E", expr],
            cwd=ROOT, env=env, timeout=None)
    out = (p.stdout or "") + (p.stderr or "")
    # LEAK is a test that PASSED but left a handle or child open past its end;
    # nextest's own summary counts it green ("6 passed (2 leaky)").
    passed = nextest_passed(out)
    for name in tests:
        if any(q.endswith(name) or q == name for q in passed):
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
