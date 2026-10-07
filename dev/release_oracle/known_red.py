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
    _open("live_mode_transition::open_defect_crashed_range_chunk_run_is_not_adopted_by_another_source_postgres",
          "rig oracle: COUNT(*): source 30, delivered 10",
          f"P-02: a same-named chunked export of another source resumes a crashed chunk run and publishes its ranges as a success; {_PROGRESS_KEY}"),
    _open("live_mode_transition::open_defect_crashed_keyset_then_incremental_postgres",
          "rig oracle: COUNT(*): source 10, delivered 6",
          f"P-06: an incremental run continues from the high-water of a crashed keyset run whose pages no manifest lists; {_PROGRESS_KEY}"),
    _open("live_mode_transition::open_defect_source_url_without_its_default_port_continues_postgres",
          "P-18: the source URL without its default port lost the cursor: the run delivered ids [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]",
          f"P-18: the source key is the URL as spelled, so dropping `:5432` starts a whole-table pass; {_PROGRESS_KEY}"),
    _open("audit_state::open_defect_state_reset_keeps_the_cursor_of_another_source",
          "P-19: `state reset` under one config deleted the cursor of a same-named export of another source: its next run re-delivered the table (14 rows on disk for 7 source ids)",
          f"P-19: `state reset --export` deletes by export name alone; {_PROGRESS_KEY}"),
    _open("live_reconcile_repair::open_defect_repair_after_a_chunk_column_edit_lists_each_row_once",
          "P-21: a repair under an edited chunk_column left the manifest listing 39 rows over 39 distinct ids for 40 source ids",
          f"P-21: reconcile and repair do not compare the stored plan fingerprint, so a repair re-reads the stored windows on the new column and supersedes the parts; {_PROGRESS_KEY}"),
    _open("live_mode_transition::open_defect_resumed_parallel_keyset_then_incremental_postgres",
          "P-22: a resumed parallel keyset run stored a cursor below its own maximum",
          f"P-22: the resume publishes the high-water of the ranges it re-ran, not of the run; {_PROGRESS_KEY}"),
    _open("live_mode_transition::open_defect_same_table_in_another_schema_is_another_stream_postgres",
          "the same table name in another schema inherited the cursor: the run delivered ids [] of 1..=7 with exit 0",
          f"Cursor identity, found verifying #456: the source key drops the URL query, so `search_path` moves the export to another schema under the stored cursor; {_PROGRESS_KEY}"),
    _open("live_mode_transition::open_defect_incremental_query_filter_edited_postgres",
          "an edited query filter inherited the old filter's cursor: the run delivered ids [] of 1..=5 with exit 0",
          f"Cursor identity, found verifying #456: the stream is the outer FROM relation, so an edited `query:` filter keeps the stored cursor; {_PROGRESS_KEY}"),
    _open("audit_state::open_defect_state_reset_during_an_incremental_read_keeps_the_manifest_honest",
          "P-23: a `state reset` accepted during an incremental read left a manifest with no cursor_low over a delta: its parts hold ids [11, 12, 13] of 1..=13",
          "P-23: the manifest's cursor_low is a second state read after the export, and `state reset` is accepted beside a live run; the work item is the run lease (#461 holds it for chunk_checkpoint runs only; whether every run holds it is an open decision)"),
    _open("live_plan_apply::open_defect_a_sealed_plan_keeps_the_date_placeholder",
          "P-26: the sealed plan holds no `{date}` placeholder",
          "P-26: `rivet plan` resolves `{date}` into the artifact, so an apply after UTC midnight writes under the planning date; the work item is the planner-and-paths PR (TRIAGE PR-M), pending the ADR-0005 decision on what a sealed plan freezes"),
    _open("live_destination_parity::open_defect_a_relative_destination_path_does_not_follow_the_working_directory",
          "P-27: one config with a relative destination.path delivered ids [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] under the first working directory and [11, 12, 13] under the second",
          "P-27: a relative `destination.path` resolves against the working directory while the cursor lives beside the config; the work item is the planner-and-paths PR (TRIAGE PR-M): anchor it to the config directory"),
    _open("chunking_stand::open_defect_a_float_chunk_column_under_query_keeps_its_fractional_keys_postgres",
          "rig oracle: COUNT(*): source 43, delivered 40",
          "A `double precision` chunk_column under `query:` is sliced by integer BETWEEN windows and drops the fractional keys with exit 0 (a WARN only); found beside P-24/P-25 (#458), no work item yet: refuse it or slice half-open"),
    _open("audit_cli_dispatch::open_defect_apply_pool_writes_only_the_export_on_stdout",
          "`apply --pool` wrote ",
          "`rivet apply --pool` prints its pool lines on stdout, into the bytes of a `destination: stdout` export; found beside P-14/P-28 (#463), no work item yet: the lines belong on stderr"),
    _open("live_partition_by::open_defect_partition_by_exports_a_declared_numeric_column",
          "`partition_by` over a table with a NUMERIC(10,2) column failed under `rivet run`: precision/scale unavailable",
          "`partition_by` reads each partition through a subquery, which loses the catalog's NUMERIC(p,s), so the export is refused although the unpartitioned one runs; found beside P-14 (#463), no work item yet"),
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
)


def match(msg: str, today: _dt.date | None = None) -> tuple[KnownRed | None, bool]:
    """The entry `msg` matches and whether it is still in date."""
    today = today or _dt.date.today()
    for k in KNOWN_RED:
        if k.match in msg:
            return k, today <= _dt.date.fromisoformat(k.expires)
    return None, False
