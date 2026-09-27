"""No live test may grade rivet by rivet's own report alone.

A test that reads what rivet SAYS it did — `manifest_rows()`, a metrics row's
`total_rows` / `files_committed`, the `export_harm` it recorded — and asserts on it,
with nothing independent in the same test, holds both sides of its comparison: it
cannot fail when rivet miscounts, only when rivet stops counting. Every such test
must also call an independent reader (DuckDB over the declared parts, the source's
own count) in the same function.

The rule is a ratchet: SELF_ONLY lists the tests that break it today, each with the
date it was listed; a new offender fails, and a listed one that has since gained an
independent reader must be removed (the list only shrinks).

    python3 dev/pytools/self_oracle_lint.py            # grade the tree
    python3 dev/pytools/self_oracle_lint.py --self-test
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

SELF_REPORT = re.compile(r"manifest_rows\(|\.total_rows\b|\.files_committed\b|export_harm|rows_written")
INDEPENDENT = re.compile(
    r"duckdb_|pg_count\(|mysql_count|mssql_count|mongo_count|source_count|dir_manifest_copy_id_set|"
    r"read_parquet|count_rows\(|query_i64\(|query_bigints|query_strings|\.query_one\(|\.query\(|"
    r"SELECT count|parquet_ids|parquet_rows|parquet_i64|parquet_id_set|parquet_distinct|"
    r"read_cdc_changes|read_mongo_cdc_changes|read_ids|read_all_parts|read_declared_parts|"
    r"read_bq_|row_census|census_oracle|count_field_present|ora_text_rows|\.count_documents|"
    # the test's OWN counters: a sink it wrote, the files it lists on disk
    r"CountingSink|files_with_extension"
)

#: `file::test` → why it is allowed today. Shrink-only.
SELF_ONLY: dict[str, str] = {
    "audit_repair.rs::audit_repair_then_reconcile_converges": "listed 2026-09-27",
    "live_cdc.rs::cdc_crash_after_flush_before_ack_re_reads_on_resume": "listed 2026-09-27",
    "live_cdc.rs::cdc_idle_first_run_then_change_is_captured_not_skipped": "listed 2026-09-27",
    "live_cdc.rs::cdc_initial_snapshot_of_an_empty_table_converges_despite_skip_empty": "listed 2026-09-27",
    "live_cdc.rs::cdc_mixed_transaction_ending_on_uncaptured_table_advances_checkpoint": "listed 2026-09-27",
    "live_cdc.rs::cdc_multi_table_stream_one_binlog_connection_and_resumes": "listed 2026-09-27",
    "live_cdc.rs::cdc_resume_captures_only_new_changes": "listed 2026-09-27",
    "live_cdc.rs::mysql_cdc_refuses_a_compressed_binlog_instead_of_capturing_nothing": "listed 2026-09-27",
    "live_cdc.rs::pg_cdc_crash_after_flush_before_ack_does_not_advance_the_slot": "listed 2026-09-27",
    "live_cdc.rs::pg_cdc_idle_first_run_then_change_is_captured_not_skipped": "listed 2026-09-27",
    "live_cdc.rs::pg_cdc_mixed_transaction_ending_on_uncaptured_table_advances_checkpoint": "listed 2026-09-27",
    "live_cdc.rs::pg_cdc_resume_captures_only_new_changes": "listed 2026-09-27",
    "live_cdc.rs::pg_cdc_vanished_slot_with_checkpoint_fails_loudly_not_recreates": "listed 2026-09-27",
    "live_cdc.rs::pg_initial_snapshot_vanished_slot_fails_loudly_not_recreates": "listed 2026-09-27",
    "live_cdc_golden.rs::cdc_golden_fixture_tables_calculated_metrics": "listed 2026-09-27",
    "live_cdc_mssql.rs::mssql_cdc_crash_before_checkpoint_re_reads_on_resume": "listed 2026-09-27",
    "live_cdc_mssql.rs::mssql_cdc_idle_first_run_then_change_is_captured_not_skipped": "listed 2026-09-27",
    "live_cdc_mssql.rs::mssql_cdc_mixed_transaction_and_qualified_table_conformance": "listed 2026-09-27",
    "live_cdc_mssql.rs::mssql_cdc_resume_captures_only_new_changes": "listed 2026-09-27",
    "live_keyset_parallel.rs::parallel_keyset_midrange_error_counts_pre_failure_page_parts_postgres": "listed 2026-09-27",
    "live_pg_state.rs::pg_metrics_record_and_query": "listed 2026-09-27",
}


def tests_in(text: str) -> list[tuple[str, str]]:
    """(name, body) of every `#[test]` function in a Rust file."""
    out = []
    for m in re.finditer(r"#\[test\][^\n]*\n(?:\s*#\[[^\n]*\n)*\s*(?:pub\s+)?fn\s+(\w+)\s*\(", text):
        start = text.index("{", m.end())
        depth, i = 0, start
        while i < len(text):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        out.append((m.group(1), text[start:i + 1]))
    return out


def offenders(root: Path) -> set[str]:
    """`file::test` for every live test that reads rivet's report and nothing independent."""
    found = set()
    for f in sorted((root / "tests" / "live").glob("*.rs")):
        for name, body in tests_in(f.read_text()):
            if SELF_REPORT.search(body) and "assert" in body and not INDEPENDENT.search(body):
                found.add(f"{f.name}::{name}")
    return found


def self_test() -> int:
    """The scanner finds a self-only test, spares one with an independent reader."""
    bad = '#[test]\nfn a() {\n    let n = manifest_rows(&o);\n    assert_eq!(n, 5);\n}\n'
    good = '#[test]\nfn b() {\n    assert_eq!(manifest_rows(&o), duckdb_declared_dir_scalar(&o, "count(*)"));\n}\n'
    assert [n for n, _ in tests_in(bad)] == ["a"]
    assert SELF_REPORT.search(tests_in(bad)[0][1]) and not INDEPENDENT.search(tests_in(bad)[0][1])
    assert INDEPENDENT.search(tests_in(good)[0][1])
    print("self_oracle_lint self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    found = offenders(ROOT)
    new = sorted(found - set(SELF_ONLY))
    stale = sorted(set(SELF_ONLY) - found)
    for t in new:
        print(f"SELF-ORACLE: {t} asserts on rivet's own report with no independent reader in the test")
    for t in stale:
        print(f"STALE: {t} no longer offends — remove it from SELF_ONLY (the list only shrinks)")
    if not new and not stale:
        print(f"self_oracle_lint: ok ({len(found)} listed offender(s), none new)")
    return 1 if new or stale else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
