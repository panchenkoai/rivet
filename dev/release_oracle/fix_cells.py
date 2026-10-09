"""A fix cell must fail on the previous release.

A live cell that a `fix` commit added or edited, and that passes on the release
WITHOUT the fix, does not see the defect it was written for: it would have been
green before the fix and it will be green after a regression.

The set is derived, never listed: every `fix…` commit since the baseline's tag
(the tag is the downloaded binary's own `--version`) that touches `src/`, and in
it every `#[test]` of `tests/live/` whose text is new or changed. Each is run
through the downloaded previous-release binary (`RIVET_BIN_OVERRIDE`) and must
FAIL there. One that passes is a VACUOUS FIX CELL and fails the stage, unless
GREEN_ON_PREV names it with a reason. `open_defect_*` cells are known_red's.

The cells run on the SQLite state only: the previous binary must neither migrate
nor be refused by the gate's Postgres state. A cell that needs one self-skips
there and is recorded SKIP with its reason.

When the baseline moves to the next release, the tag moves with it: the set
starts again from the fixes made after that release, and every GREEN_ON_PREV
entry it no longer contains is reported STALE until it is removed.

    RIVET_PREV_RELEASE_BIN=<old rivet> python3 -m dev.release_oracle.fix_cells [cell ...]
    python3 -m dev.release_oracle.fix_cells --self-test
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

from .core import ROOT, Ledger, run

__all__ = ["verify_fix_cells", "GREEN_ON_PREV"]

SCEN = "fix_cells"
GIT = "/usr/bin/git" if Path("/usr/bin/git").exists() else "git"
FIX = re.compile(r"fix[(:!]")
TEST = re.compile(r"^#\[test\]\n(?:#\[[^\n]*\n)*(?:pub )?fn (\w+)\(\) \{\n(.*?)^\}", re.M | re.S)
UNTRIAGED = ("untriaged: green on v0.31.0 when first measured (2026-10-08); "
             "either a control of the fix or a vacuous fix cell, to be read")

#: Controls: the neighbour of a fix that must stay as it was, so it is green on the release without the fix.
KEYSET_GROWS = ("control of b6b15004: a keyset resume seeks past its checkpoint key, so it delivered a source that grew "
                "before the fix; the range and parallel-keyset plans are the cells that are red there")
COMPRESSION_EDIT = ("control of b6b15004: a `compression` edit under stored progress is not what the new destination "
                    "guard refuses, and the run continues as it did")
MOVED_UNFINISHED = ("control of b6b15004: a destination moved under an UNFINISHED range run delivers the source whole "
                    "where the config points, as it did; the guard refuses a moved cursor and another format")
ONE_DESTINATION = ("edited by b6b15004, not written for it: its stages now share one destination so the new guard "
                   "does not refuse them; the golden math it asserts is unchanged")
AFTER_A_PART = ("control of 3f073dd0: a run that fails AFTER a part still withdraws the marker; the fix is the run "
                "that fails before its first write")
MONGO_FORCED = ("control of 3f073dd0: `--resume --force` over a complete continued-key Mongo export is the empty "
                "delta run it was; the checkpointed SQL shapes are the cells that are red there")
MSSQL_NO_GAP = ("control of f7086105: no log gap exists here, so the refusal the fix adds from the first run after "
                "the baseline must not fire, and it did not before")
DIRECT_WIDE = ("control of 3132830a: the first version of that fix hung on a non-builtin column type beside "
               "wide rows; the previous release never sent the unnamed statement, so it reads or refuses as before")
WIRE_SAMPLE = ("control of 3132830a: a unit sample of the cell's own wire counter (executions, not statement "
               "texts); it runs no product binary")
EMPTY_PREFIX = ("control of 52f26e45: an empty prefix of an EXISTING bucket still reads `legacy`; the fix fails "
                "`validate` on a missing bucket")

#: Fix cells allowed to pass on the previous release, each with its reason. Shrink-only.
GREEN_ON_PREV: dict[str, str] = {  # ratchet-pin: fix-cells-green-on-previous-release strings
    "a_crashed_keyset_incremental_run_is_resumed_not_reread_postgres": UNTRIAGED,
    "a_query_that_does_not_rename_the_chunk_column_delivers_its_rows_postgres": UNTRIAGED,
    "a_refused_chunked_checkpoint_run_keeps_refusing_a_duplicate_postgres": UNTRIAGED,
    "a_refused_keyset_checkpoint_run_keeps_refusing_a_duplicate_postgres": UNTRIAGED,
    "a_refused_keyset_checkpoint_run_keeps_refusing_schema_drift_postgres": UNTRIAGED,
    "a_refused_parallel_chunked_checkpoint_run_keeps_refusing_a_duplicate_postgres": UNTRIAGED,
    "a_sql_server_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source": UNTRIAGED,
    "an_update_of_a_replica_identity_index_reaches_the_base_as_one_update_postgres": UNTRIAGED,
    "apply_child_processes_partitions_like_run_postgres": UNTRIAGED,
    "full_then_resume_mongo": UNTRIAGED,
    "keyset_then_resume_mongo": UNTRIAGED,
    "mongo_resume_crash_after_keyset_page_recovers_manifest_driven": UNTRIAGED,
    "parallel_keyset_incremental_on_a_float_key_delivers_the_source_rows_mysql": UNTRIAGED,
    "parallel_keyset_incremental_on_a_number5_key_delivers_the_source_rows_oracle": UNTRIAGED,
    "parallel_keyset_incremental_on_a_real_key_delivers_the_source_rows_mssql": UNTRIAGED,
    "parallel_keyset_incremental_on_a_smallint_key_delivers_the_source_rows_mssql": UNTRIAGED,
    "parallel_keyset_incremental_on_a_smallint_key_delivers_the_source_rows_mysql": UNTRIAGED,
    "parallel_keyset_incremental_on_the_other_key_types_delivers_the_source_rows_postgres": UNTRIAGED,
    "parallel_keyset_on_a_smallint_key_delivers_the_source_rows_postgres": UNTRIAGED,
    "parallel_keyset_then_resume_mongo": UNTRIAGED,
    "pg_cdc_a_declared_key_absent_from_the_old_key_does_not_split": UNTRIAGED,
    "pg_cdc_an_undecodable_old_cell_beside_an_unchanged_key_is_delivered": UNTRIAGED,
    "pg_cdc_an_update_of_a_replica_identity_index_that_is_not_the_key_stays_one_update": UNTRIAGED,
    "pg_table_added_to_tables_mid_stream_is_baselined": UNTRIAGED,
    "run_child_processes_flag_keeps_its_partition_fallback_postgres": UNTRIAGED,
    "run_partitions_the_fixture_postgres": UNTRIAGED,
    "a_keyset_checkpoint_run_killed_before_the_source_grows_delivers_the_source_mssql": KEYSET_GROWS,
    "a_keyset_checkpoint_run_killed_before_the_source_grows_delivers_the_source_mysql": KEYSET_GROWS,
    "a_keyset_checkpoint_run_killed_before_the_source_grows_delivers_the_source_oracle": KEYSET_GROWS,
    "a_keyset_checkpoint_run_killed_before_the_source_grows_delivers_the_source_postgres": KEYSET_GROWS,
    "an_incremental_export_whose_compression_is_edited_delivers_the_source_mssql": COMPRESSION_EDIT,
    "an_incremental_export_whose_compression_is_edited_delivers_the_source_mysql": COMPRESSION_EDIT,
    "an_incremental_export_whose_compression_is_edited_delivers_the_source_oracle": COMPRESSION_EDIT,
    "an_incremental_export_whose_compression_is_edited_delivers_the_source_postgres": COMPRESSION_EDIT,
    "an_unfinished_run_whose_compression_is_edited_delivers_the_source_mssql": COMPRESSION_EDIT,
    "an_unfinished_run_whose_compression_is_edited_delivers_the_source_mysql": COMPRESSION_EDIT,
    "an_unfinished_run_whose_compression_is_edited_delivers_the_source_oracle": COMPRESSION_EDIT,
    "an_unfinished_run_whose_compression_is_edited_delivers_the_source_postgres": COMPRESSION_EDIT,
    "an_unfinished_run_whose_destination_path_is_edited_delivers_the_source_mssql": MOVED_UNFINISHED,
    "an_unfinished_run_whose_destination_path_is_edited_delivers_the_source_mysql": MOVED_UNFINISHED,
    "an_unfinished_run_whose_destination_path_is_edited_delivers_the_source_oracle": MOVED_UNFINISHED,
    "an_unfinished_run_whose_destination_path_is_edited_delivers_the_source_postgres": MOVED_UNFINISHED,
    "batch_full_to_incremental_switch_golden_math": ONE_DESTINATION,
    "batch_incremental_datetime_cursor_captures_updates_golden_math": ONE_DESTINATION,
    "a_run_that_failed_after_a_part_withdraws_the_marker_mssql": AFTER_A_PART,
    "a_run_that_failed_after_a_part_withdraws_the_marker_mysql": AFTER_A_PART,
    "a_run_that_failed_after_a_part_withdraws_the_marker_oracle": AFTER_A_PART,
    "a_run_that_failed_after_a_part_withdraws_the_marker_postgres": AFTER_A_PART,
    "a_forced_resume_over_a_complete_prefix_lands_beside_it_mongo": MONGO_FORCED,
    "mssql_a_never_changed_table_is_not_refused_after_cleanup_passes_its_anchor": MSSQL_NO_GAP,
    "mssql_an_anchor_below_a_just_enabled_instance_still_delivers_every_later_change": MSSQL_NO_GAP,
    "validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_azure": EMPTY_PREFIX,
    "validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_gcs": EMPTY_PREFIX,
    "validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_s3": EMPTY_PREFIX,
    "a_composite_column_with_wide_rows_is_refused_within_the_bound_direct_postgres": DIRECT_WIDE,
    "a_table_of_an_enum_and_a_domain_with_wide_rows_is_delivered_full_direct_postgres": DIRECT_WIDE,
    "a_table_of_an_enum_and_a_domain_with_wide_rows_is_delivered_incremental_direct_postgres": DIRECT_WIDE,
    "a_table_of_an_enum_and_a_domain_with_wide_rows_is_delivered_keyset_direct_postgres": DIRECT_WIDE,
    "a_table_of_an_enum_and_a_domain_with_wide_rows_is_delivered_range_direct_postgres": DIRECT_WIDE,
    "an_array_of_an_enum_with_wide_rows_is_refused_within_the_bound_direct_postgres": DIRECT_WIDE,
    "an_answer_ends_at_the_next_request_and_two_pages_are_not_one": WIRE_SAMPLE,
}  # ratchet-pin: end


def git(*args: str) -> str:
    """git's stdout; a failing git is an error, never an empty answer."""
    return subprocess.run([GIT, *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def prev_tag(prev: Path) -> str:
    """The release tag of the baseline binary, from its own `--version`."""
    said = run([str(prev), "--version"]).stdout
    m = re.search(r"\b(\d+\.\d+\.\d+)\b", said)
    if not m:
        raise ValueError(f"{prev} --version printed no x.y.z: {said!r}")
    return f"v{m.group(1)}"


def tests_in(text: str) -> dict[str, str]:
    """Bare name → body of every top-level `#[test] fn name()` in a Rust file."""
    return dict(TEST.findall(text))


def touched(before: str, after: str) -> set[str]:
    """The tests of `after` that `before` does not have, or has with another body."""
    was = tests_in(before)
    return {name for name, body in tests_in(after).items() if was.get(name) != body}


def fix_cells(tag: str) -> dict[str, str]:
    """Bare cell name → the `fix` commit since `tag` that added or edited it (the last one wins)."""
    now = {n for f in sorted((ROOT / "tests" / "live").glob("*.rs")) for n in tests_in(f.read_text())}
    cells: dict[str, str] = {}
    for line in git("log", "--no-merges", "--reverse", "--format=%h%x02%s", f"{tag}..HEAD").splitlines():
        sha, subject = line.split("\x02", 1)
        files = git("diff-tree", "--no-commit-id", "--name-only", "-r", sha).split()
        if not FIX.match(subject) or not any(f.startswith("src/") for f in files):
            continue
        for f in files:
            if f.startswith("tests/live/") and f.endswith(".rs"):
                before = subprocess.run([GIT, "show", f"{sha}^:{f}"], cwd=ROOT, capture_output=True, text=True).stdout
                for name in touched(before, git("show", f"{sha}:{f}")):
                    cells[name] = f"{sha} {subject}"
    return {n: why for n, why in cells.items() if n in now and not n.startswith("open_defect_")}


def grade(cells: dict[str, str], verdicts: dict[str, str], skipped: dict[str, str], allowed: dict[str, str],
          tag: str) -> list[tuple[str, str, str]]:
    """(status, cell, line) per fix cell and per ledger entry; `verdicts` is bare name → nextest status on `tag`."""
    rows = []
    for cell, why in sorted(cells.items()):
        verdict = verdicts.get(cell)
        if cell in skipped:
            rows.append(("SKIP", cell, f"NOT GRADED on {tag}: self-skipped there ({skipped[cell]})"))
        elif verdict is None:
            rows.append(("FAIL", cell, f"NOT RUN on {tag}: nextest gave no verdict ({why})"))
        elif verdict in ("PASS", "LEAK"):
            if cell in allowed:
                rows.append(("PASS", cell, f"green on {tag}, allowed: {allowed[cell]}"))
            else:
                rows.append(("FAIL", cell, f"VACUOUS FIX CELL: passes on {tag}, the release without the fix ({why}). "
                             "Make it fail there, or give its reason in fix_cells.GREEN_ON_PREV"))
        elif cell in allowed:
            rows.append(("FAIL", cell, f"STALE: red on {tag} and listed in GREEN_ON_PREV; remove the entry"))
        else:
            rows.append(("PASS", cell, f"red on {tag}, as its fix requires ({why})"))
    for cell in sorted(set(allowed) - set(cells)):
        rows.append(("FAIL", cell, f"STALE: no fix commit since {tag} touches it; remove it from GREEN_ON_PREV"))
    return rows


def verify_fix_cells(led: Ledger, only: list[str] | None = None) -> None:
    """Run every fix cell since the baseline's tag through the baseline binary; one ledger row per cell."""
    from .regression import _require_prev_binary
    from .scenarios import nextest_live, work_dir
    led.phase("fix cells · each must fail on the previous release")
    prev = _require_prev_binary(led, "all", "-", SCEN, "-", "fix cells")
    if prev is None:
        return
    try:
        tag = prev_tag(prev)
        cells = fix_cells(tag)
    except (ValueError, subprocess.CalledProcessError) as e:
        led.failed("all", "-", SCEN, "-", f"fix cells: the set could not be derived: {e}", getattr(e, "stderr", "") or "")
        return
    allowed = GREEN_ON_PREV
    if only:
        cells = {c: w for c, w in cells.items() if c in only}
        allowed = {c: w for c, w in allowed.items() if c in only}
    if not cells:
        n = len(git("log", "--no-merges", "--format=%h", f"{tag}..HEAD").split())
        led.passed("all", "-", SCEN, "-", f"fix cells: no fix commit among the {n} since {tag} adds or edits a live cell")
    else:
        verdicts, _, skipped, _ = nextest_live(
            work_dir() / "fix_cells_prev.log", "test(/::(" + "|".join(sorted(cells)) + ")$/)",
            env={"RIVET_BIN": str(prev), "RIVET_BIN_OVERRIDE": str(prev),
                 "RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": "", "RIVET_TEST_STATE_URL": ""})
        bare = {name.rsplit("::", 1)[-1]: v for name, v in verdicts.items()}
        skips = {name.rsplit("::", 1)[-1]: why for name, why in skipped.items()}
        record = {"PASS": led.passed, "FAIL": led.failed, "SKIP": led.skipped}
        for status, cell, line in grade(cells, bare, skips, allowed, tag):
            record[status]("all", "-", SCEN, "-", f"fix cell · {cell} — {line}")
        return
    for _, cell, line in grade({}, {}, {}, allowed, tag):
        led.failed("all", "-", SCEN, "-", f"fix cell · {cell} — {line}")


def _self_test() -> None:
    """The derivation and the grade on fixtures: a vacuous fix cell is the one row that fails."""
    before = '#[test]\n#[ignore = "live"]\nfn kept() {\n    a();\n}\n\n#[test]\nfn edited() {\n    assert!(x);\n}\n\nfn helper() {\n}\n'
    after = before.replace("assert!(x);", "assert_eq!(x, 1);") + '\n#[test]\n#[ignore = "live"]\npub fn added() {\n    if a {\n        b();\n    }\n}\n'
    assert set(tests_in(after)) == {"kept", "edited", "added"}, tests_in(after)
    assert touched(before, after) == {"edited", "added"} and touched("", before) == {"kept", "edited"}
    assert FIX.match("fix(cdc): x") and FIX.match("fix: x") and not FIX.match("fixture: x") and not FIX.match("test(fix): x")
    cells = {"red": "a1 fix(x): one", "green": "a1 fix(x): one", "control": "b2 fix(y): two", "skipped": "b2 fix(y): two", "gone": "b2 fix(y): two"}
    on_prev = {"red": "FAIL", "green": "PASS", "control": "LEAK", "skipped": "PASS"}
    rows = {c: (s, line) for s, c, line in grade(cells, on_prev, {"skipped": "no RIVET_TEST_STATE_URL"}, {"control": "a control"}, "v1.2.3")}
    assert rows["red"][0] == "PASS" and "red on v1.2.3" in rows["red"][1], rows["red"]
    assert rows["green"][0] == "FAIL" and rows["green"][1].startswith("VACUOUS FIX CELL: passes on v1.2.3"), rows["green"]
    assert rows["control"] == ("PASS", "green on v1.2.3, allowed: a control"), rows["control"]
    assert rows["skipped"] == ("SKIP", "NOT GRADED on v1.2.3: self-skipped there (no RIVET_TEST_STATE_URL)"), rows["skipped"]
    assert rows["gone"][0] == "FAIL" and "no verdict" in rows["gone"][1], rows["gone"]
    stale = grade({"red": "a1 fix"}, {"red": "FAIL"}, {}, {"red": "r", "old": "r"}, "v1.2.3")
    assert [(s, c, line.split(":")[0]) for s, c, line in stale] == [("FAIL", "red", "STALE"), ("FAIL", "old", "STALE")], stale
    here = {n for f in (ROOT / "tests" / "live").glob("*.rs") for n in tests_in(f.read_text())}
    assert len(here) > 1000, f"the test pattern reads {len(here)} live cells: it stopped matching"
    assert all(why.strip() for why in GREEN_ON_PREV.values()), "a GREEN_ON_PREV entry has no reason"
    print("self-test ok: a fix cell that passes on the previous release fails the stage; a listed control does not")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        _self_test()
        raise SystemExit(0)
    _led = Ledger()
    verify_fix_cells(_led, sys.argv[1:] or None)
    raise SystemExit(_led.report())
