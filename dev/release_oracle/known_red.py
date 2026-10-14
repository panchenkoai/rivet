"""Known reds: gate failures acknowledged with a reason and an expiry, so a red gate on main
is red ONLY for recorded reasons. A matching failure is reported as KNOWN, not blocking; an
expired entry blocks again; an entry that no failure matched in a FULL run blocks too (fixed —
remove it). Matched by a substring of the failure message, one entry per acknowledged defect.

An open product defect is pinned by a live_suite cell that asserts the CORRECT behaviour and so
fails until the fix lands: the cell is named `open_defect_*` and marked `live+gate-only` (CI has no
such ledger and would only go red), and its entry here is `<module>::<fn> — <its first panic line>`,
the defect's own symptom. The fix PR deletes the entry; the cell stays as the regression guard.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass


@dataclass(frozen=True)
class KnownRed:
    match: str
    reason: str
    expires: str  # YYYY-MM-DD


_EXPIRES = "2026-10-31"
_PROGRESS_KEY = ("stored progress has no single owner key (export, normalised source, stream, mode); the work item is the "
                 "progress key, PROGRESS-KEY-DESIGN.md")


def _open(cell: str, symptom: str, reason: str) -> KnownRed:
    """The entry of one `open_defect_*` live cell: its test path and the first line of its panic."""
    return KnownRed(f"{cell} — {symptom}", reason, _EXPIRES)


KNOWN_RED: tuple[KnownRed, ...] = (
    _open("live_reconcile_repair::open_defect_repair_after_a_chunk_column_edit_lists_each_row_once",
          "P-21: a repair under an edited chunk_column left the manifest listing 39 rows over 39 distinct ids for 40 source ids",
          f"P-21: reconcile and repair do not compare the stored plan fingerprint, so a repair re-reads the stored windows on the new column and supersedes the parts; {_PROGRESS_KEY}"),
    _open("live_plan_apply::open_defect_a_sealed_plan_keeps_the_date_placeholder",
          "P-26: the sealed plan holds no `{date}` placeholder",
          "P-26: `rivet plan` resolves `{date}` into the artifact, so an apply after UTC midnight writes under the planning date; the work item is the planner-and-paths PR (TRIAGE PR-M), pending the ADR-0005 decision on what a sealed plan freezes"),
    _open("live_destination_parity::open_defect_a_relative_destination_path_does_not_follow_the_working_directory",
          "P-27: one config with a relative destination.path delivered ids [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] under the first working directory and [11, 12, 13] under the second",
          "P-27: a relative `destination.path` resolves against the working directory while the cursor lives beside the config; the work item is the planner-and-paths PR (TRIAGE PR-M): anchor it to the config directory"),
    _open("chunking_stand::open_defect_a_float_chunk_column_under_query_keeps_its_fractional_keys_postgres",
          "rig oracle: COUNT(*): source 43, delivered 40",
          "A `double precision` chunk_column under `query:` is sliced by integer BETWEEN windows and drops the fractional keys with exit 0 (a WARN only); found beside P-24/P-25 (#458), no work item yet: refuse it or slice half-open"),
    _open("live_operator_contract::open_defect_removing_page_size_runs_after_the_heterogeneous_id_refusal_mongo",
          'refuse-then-remedy: remedy `Remove `page_size`` ended with [RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH] (exit 5), expected exit 0 and the source delivered',
          'Sweep 14: the refused resume has already claimed the cursor row, so the remedy the refusal names (a full scan without `page_size`) is refused as an interrupted run of another mode; exit 0 on v0.31.0'),
    _open("live_operator_contract::open_defect_init_cdc_writes_the_capture_instance_the_catalog_holds_mssql",
          "assertion `left == right` failed: `rivet init --mode cdc` wrote a capture instance cdc.change_tables does not hold for dbo.oc_initc_",
          "Sweep 19: `rivet init --mode cdc` on SQL Server derives `capture_instance: dbo_<table>` and does not read `cdc.change_tables`, so the run refuses an instance the catalog does not know"),
    _open("live_operator_contract::open_defect_range_chunking_takes_a_number_19_key_oracle",
          "range chunking refused an Oracle NUMBER(19) integer key (exit Some(1))",
          "2026-10-06 finding: range chunking refuses an Oracle `NUMBER(19)` primary key (scale 0) as not integer-family, while it takes `NUMBER(18)`"),
    KnownRed('upgrade[oracle/cdc-load]: cycle1/load: prev exit 1: Error: config has no top-level `load:` block',
             "Oracle CDC does not load yet (ADR-0037; the Oracle GA work). The previous release's `rivet init --mode cdc` writes no `load:` block, and this cell still drives the previous release through `load`, which refuses. `upgrade[oracle/cdc-load/init=this]` grades what the scaffold does promise (the baseline Parquet equals the source). The cycle stays unsupported until Oracle CDC loads",
             "2026-10-31"),
    KnownRed('upgrade[oracle/cdc-load/tz=Asia/Tokyo]: cycle1/load: prev exit 1: Error: config has no top-level `load:` block',
             "Oracle CDC does not load yet (ADR-0037; the Oracle GA work). The previous release's `rivet init --mode cdc` writes no `load:` block, and this cell still drives the previous release through `load`, which refuses. `upgrade[oracle/cdc-load/init=this]` grades what the scaffold does promise (the baseline Parquet equals the source). The cycle stays unsupported until Oracle CDC loads",
             "2026-10-31"),
    KnownRed('sentinels[rivet_sent_ts9/full]: SUCCEEDED WITH A CHANGED VALUE: `V` differs',
             "Oracle TIMESTAMP(9) is delivered at microseconds today: docs/type-capability-matrix.yaml's oracle TIMESTAMP(9) row is a known_defect (exact native is Timestamp(ns), ADR-0038 CP1, Oracle engine step), so the 1 ns sentinel lands truncated",
             "2026-10-31"),
    KnownRed('sentinels[rivet_sent_ts9/keyset]: SUCCEEDED WITH A CHANGED VALUE: `V` differs',
             "Oracle TIMESTAMP(9) is delivered at microseconds today: docs/type-capability-matrix.yaml's oracle TIMESTAMP(9) row is a known_defect (exact native is Timestamp(ns), ADR-0038 CP1, Oracle engine step), so the 1 ns sentinel lands truncated",
             "2026-10-31"),
    _open("live_sabotage::open_defect_a_cursor_moved_forward_delivers_or_refuses_postgres",
          "rig oracle: COUNT(*): source 43, delivered 41",
          "exit 0 over wrong data: a stored cursor moved past rows the export never delivered is continued from, although the destination's own manifests end below it (`cursor_high` of the last success manifest); no work item yet, owner decision: compare the stored cursor with the destination's last `cursor_high` and refuse a cursor above it, or declare the state row trusted"),
    _open("live_sabotage::open_defect_a_cursor_moved_forward_delivers_or_refuses_mysql",
          "rig oracle: COUNT(*): source 43, delivered 41",
          "exit 0 over wrong data: a stored cursor moved past rows the export never delivered is continued from, although the destination's own manifests end below it (`cursor_high` of the last success manifest); no work item yet, owner decision: compare the stored cursor with the destination's last `cursor_high` and refuse a cursor above it, or declare the state row trusted"),
    _open("live_sabotage::open_defect_a_cursor_moved_forward_delivers_or_refuses_mssql",
          "rig oracle: COUNT(*): source 43, delivered 41",
          "exit 0 over wrong data: a stored cursor moved past rows the export never delivered is continued from, although the destination's own manifests end below it (`cursor_high` of the last success manifest); no work item yet, owner decision: compare the stored cursor with the destination's last `cursor_high` and refuse a cursor above it, or declare the state row trusted"),
    _open("live_sabotage::open_defect_a_run_whose_azure_bucket_is_removed_delivers_or_fails_loudly_postgres",
          "the live run did not end within 180s of the action beside it",
          "other, a failure that takes minutes: with its Azure container removed mid-run the export retried a persistent 404 (`ContainerNotFound`, which its own summary calls persistent) 108 times and ended after 9 min 21 s with exit 1 and `chunk checkpoint incomplete` (by hand, Azurite); the s3 and gcs cells of the same row family end in seconds; no work item yet: a missing container is not retryable"),
    _open("live_sabotage::open_defect_a_run_out_of_file_descriptors_fails_loudly_and_the_next_delivers_postgres",
          "`rivet run` neither a delivery nor a loud failure: ended with exit 101",
          "other, a crash: a PostgreSQL export capped at 8 open files panics (`called Result::unwrap() on an Err value: Os { code: 24, .. \"Too many open files\" }` at postgres-0.19 src/config.rs:463, the client building its runtime) instead of failing with an `Error:` line; MySQL, SQL Server, Oracle and MongoDB fail loudly under the same cap on either state, and on a PostgreSQL state this export fails loudly too, at the state's connect, so the crash is the source client's and shows on a SQLite state; no work item yet"),
)


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in msg:
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
