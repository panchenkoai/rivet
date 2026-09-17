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
project/bucket or the `bq` CLI.
"""

from __future__ import annotations

import os
import re

from .core import Ledger, ROOT, have, rivet_bin, run

TESTS = (
    "same_named_configs_share_a_postgres_state_cdc_cycle",
    "same_named_configs_share_a_postgres_state_batch_cycle",
)


def _verdict(out: str, name: str) -> str | None:
    """`ok` / `FAILED` / `ignored` for one test from cargo's listing, else None."""
    m = re.search(rf"^test \S*{re.escape(name)} \.\.\. (\w+)", out, re.M)
    return m.group(1) if m else None


def verify_shared_state_same_name(led: Ledger) -> None:
    led.phase("Shared state, same-named configs — four engines at once, blessed + crashed (CDC and batch)")
    state = os.environ.get("RIVET_CDC_STATE_URL") or os.environ.get("RIVET_CONC_STATE_URL") or ""
    proj = os.environ.get("BQ_ORACLE_PROJECT") or run(["gcloud", "config", "get-value", "project"]).stdout.strip()
    bucket = os.environ.get("BQ_ORACLE_BUCKET", "rivet_data_test")
    missing = [w for w, ok in (
        ("cargo", have("cargo")), ("bq", have("bq")),
        ("Postgres state URL", state.startswith("postgres")), ("BigQuery project", bool(proj)),
    ) if not ok]
    if missing:
        led.skipped("-", "shared", "same_name", "postgres",
                    f"shared-state: cannot run — missing {', '.join(missing)}", "prereq")
        return
    env = {
        "RIVET_TEST_STATE_URL": state,
        "BIGQUERY_TEST_PROJECT": proj,
        "RIVET_TEST_GCS_BUCKET": bucket,
        "RIVET_BIN_OVERRIDE": str(rivet_bin()),
    }
    p = run(["cargo", "test", "--test", "live_suite", "--", "--ignored", "--test-threads=1", *TESTS],
            cwd=ROOT, env=env, timeout=None)
    out = (p.stdout or "") + (p.stderr or "")
    for name in TESTS:
        cycle = "cdc" if name.endswith("cdc_cycle") else "batch"
        v = _verdict(out, name)
        msg = f"shared-state[{cycle}] · 4 same-named configs, one Postgres state, parallel + crashes"
        if v == "ok":
            led.passed("all", "shared", f"same_name:{cycle}", "postgres", msg, "ok")
        elif v is None:
            led.failed("all", "shared", f"same_name:{cycle}", "postgres",
                       f"{msg} — no verdict line for {name} (build failure or wrong filter)",
                       out[-400:])
        else:
            tail = out[out.find(name):][:1200] if name in out else out[-600:]
            led.failed("all", "shared", f"same_name:{cycle}", "postgres", f"{msg} — {v}", tail)
