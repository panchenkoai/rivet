"""The 0.27 warehouse layout the partner runs: base table + disposable buffer + compact.

Two things the partner e2e measured before this cell existed: a 317M-row baseline
landed inside `<table>__changes` behind a view (the layout they did not ask for),
and the first load of a wide history was refused on BigQuery's 4,000-partitions-
per-job cap with no way through. The cell runs the Rig tests that pin both against
the gate's binary:

  * the three ways a cycle meets a base it did not expect — a table rivet never
    loaded (refused before any write), a base dropped under a live stream (refused
    by name, buffer kept whole), and a compaction killed before its merge (buffer
    survives, the next one applies each change exactly once);
  * `live_cdc_compact` — run → load → compact on MySQL CDC with a keyset backfill:
    a physical base with `__is_deleted` as DATA (never NULL), a buffer of exactly
    the cycle's changes, one MERGE that flags deletes and keeps their last values,
    `DROP` of the buffer, a crash between MERGE and DROP re-merged idempotently;
  * `live_load_partition_batches` — 5,000 daily partitions read by keyset: the
    overwrite lands via staging + CLONE in two jobs, a file no batching splits
    (written before the partition was declared — the writer budgets what the config
    names) is refused BY NAME before any job (ledger `refused`), a CDC baseline over
    the same history lands in the base in two jobs and its CLONE'd base compacts.

Oracles are the tests': the source, `bq`, the SQLite ledger — never rivet's report.
SKIP — never a silent pass — without cargo, the Postgres state URL, the BigQuery
project/bucket or `gcloud` (the REST token).
"""

from __future__ import annotations

from .core import Ledger
from .shared_state import run_rig_tests

CELLS = {
    "base_and_buffer_cycle_run_load_compact_keeps_deletes_as_flags": "compact:cycle",
    "a_wide_history_read_by_keyset_loads_daily_partitions_in_batches": "batches:overwrite",
    "a_file_spanning_the_whole_history_is_refused_by_name_before_any_job": "batches:refusal",
    "a_cdc_baseline_over_a_wide_history_lands_in_the_base_in_batches": "batches:cdc",
    "a_base_table_rivet_never_loaded_is_refused_before_any_write": "compact:foreign",
    "a_missing_base_refuses_the_compaction_and_keeps_the_buffer": "compact:no-base",
    "a_compaction_that_dies_before_its_merge_leaves_the_buffer_whole": "compact:precrash",
}


def verify_warehouse_layout(led: Ledger) -> None:
    led.phase("Warehouse layout — base + buffer + compact, footer-batched loads over 5,000 partitions")
    run_rig_tests(
        led, "layout", tuple(CELLS),
        cell=CELLS.__getitem__,
        msg=lambda n: f"layout[{CELLS[n]}] · {n.replace('_', ' ')}",
    )
