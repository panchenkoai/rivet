"""Every `#[test]` must check something of its own.

A test checks when its body has an assert-family macro, `expect_err` / `unwrap_err`
or `#[should_panic]`, or calls a non-test helper from test code that does (two levels
deep). A test with none of those cannot fail on a wrong answer, only on a panic.

Two numbers, both shrink-only:

  NO_CHECK   tests with no check at all; each listed with its reason.
  WEAK_ONLY  tests whose every assertion is a bare `is_ok()` / `is_some()` /
             `success()` / `is_empty()` / `exists()` / `is_file()`.

    python3 dev/pytools/test_checks.py              # grade the tree
    python3 dev/pytools/test_checks.py --list-weak  # the WEAK_ONLY tests, one per line
    python3 dev/pytools/test_checks.py --self-test
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: The only reasons a test may have no check of its own.
REASONS = {
    "totality": "a property test whose claim is that no input panics",
    "must not error": "the claim is that the call returns Ok; `expect` is the check",
    "compile-time": "the claim is a trait bound or a type; compiling is the check",
    "unchecked": "checks nothing; listed 2026-10-08 as work: give it an assertion or delete it",
}

#: `file::test` → reason. Shrink-only.
NO_CHECK: dict[str, str] = {  # ratchet-pin: tests-without-a-check strings
    "src/config/mod.rs::absolute_local_path_is_allowed": "must not error",
    "src/config/mod.rs::audit_csv_default_compression_still_validates": "must not error",
    "src/config/mod.rs::config_without_braces_is_untouched": "must not error",
    "src/config/mod.rs::loopback_endpoint_without_allow_anonymous_still_accepted": "must not error",
    "src/config/mod.rs::multi_table_stream_still_accepts_row_hash_true": "must not error",
    "src/config/mod.rs::remote_https_endpoint_with_allow_anonymous_is_the_only_remote_escape": "must not error",
    "src/config/mod.rs::sec_export_name_normal_still_accepted_guard": "must not error",
    "src/config/mod.rs::sec_loopback_endpoint_still_accepted_guard": "must not error",
    "src/config/mod.rs::tls_danger_knob_without_explicit_mode_still_accepted": "must not error",
    "src/config/mod.rs::tls_explicit_disable_with_knob_is_not_flagged": "must not error",
    "src/config/tests/mod.rs::incremental_coalesce_config_parses": "must not error",
    "src/config/tests/mod.rs::incremental_with_cursor_column_is_accepted": "must not error",
    "src/config/tests/validation.rs::cdc_exports_with_distinct_resources_validate_fine": "must not error",
    "src/config/tests/validation.rs::config_parse_is_total_over_arbitrary_yaml": "totality",
    "src/config/tests/validation.rs::config_parse_is_total_over_structured_hostility": "totality",
    "src/config/tests/validation.rs::correct_tuning_placement_accepted": "must not error",
    "src/config/tests/validation.rs::mysql_cdc_exports_with_distinct_server_ids_validate_fine": "must not error",
    "src/config/tests/validation.rs::qualified_override_matching_the_export_table_validates": "must not error",
    "src/config/tests/validation.rs::query_file_relative_path_accepted_at_load": "must not error",
    "src/connect/mod.rs::hostless_socket_url_with_explicit_tls_disable_is_allowed": "must not error",
    "src/destination/cloud.rs::new_with_retries_zero_builds_no_retry_probe_destination": "must not error",
    "src/destination/gcs_auth.rs::roast_gcs_adc_loader_plugs_into_opendal_refresh_hook_not_static_token": "compile-time",
    "src/format/mod.rs::create_format_parquet_uncompressed_finish_ok": "must not error",
    "src/format/parquet.rs::create_writer_gzip_succeeds": "must not error",
    "src/format/parquet.rs::create_writer_lz4_succeeds": "must not error",
    "src/format/parquet.rs::create_writer_snappy_succeeds": "must not error",
    "src/format/parquet.rs::create_writer_uncompressed_succeeds": "must not error",
    "src/format/parquet.rs::create_writer_zstd_default_level_succeeds": "must not error",
    "src/format/parquet.rs::create_writer_zstd_explicit_level_succeeds": "must not error",
    "src/format/parquet.rs::finish_without_write_produces_valid_empty_parquet": "must not error",
    "src/format/parquet.rs::row_group_rows_none_uses_library_default": "must not error",
    "src/format/parquet.rs::row_group_rows_some_succeeds": "must not error",
    "src/format/parquet.rs::write_batch_and_finish_returns_ok": "must not error",
    "src/init/mod.rs::validate_accepts_neither_or_one_bucket": "must not error",
    "src/load/orchestrate.rs::record_is_a_noop_without_a_state_store": "unchecked",
    "src/notify.rs::degraded_triggers_degraded_event": "unchecked",
    "src/notify.rs::error_message_included_in_stub": "unchecked",
    "src/notify.rs::missing_webhook_url_env_skips_silently": "unchecked",
    "src/notify.rs::multiple_triggers_any_match_fires": "unchecked",
    "src/notify.rs::no_config_does_nothing": "unchecked",
    "src/notify.rs::no_slack_does_nothing": "unchecked",
    "src/notify.rs::no_webhook_url_and_no_env_skips": "unchecked",
    "src/notify.rs::schema_change_false_does_not_trigger": "unchecked",
    "src/notify.rs::schema_change_triggers_schema_change_event": "unchecked",
    "src/notify.rs::success_does_not_trigger_failure": "unchecked",
    "src/pipeline/cdc/sink.rs::poison_on_an_uncaptured_table_is_dropped_never_bails_the_run": "must not error",
    "src/pipeline/chunked/math.rs::generate_chunks_does_not_panic_on_extreme_boundaries": "totality",
    "src/pipeline/commit.rs::transit_check_passes_on_match_and_when_store_is_silent": "must not error",
    "src/pipeline/run_store.rs::finalize_with_no_writes_is_a_noop": "must not error",
    "src/pipeline/validate.rs::csv_crlf_terminators_count_records": "must not error",
    "src/pipeline/validate.rs::csv_doubled_quotes_inside_quoted_field_with_newline": "must not error",
    "src/pipeline/validate.rs::csv_empty_body_zero_rows_passes": "must not error",
    "src/pipeline/validate.rs::csv_empty_file_zero_rows_passes": "must not error",
    "src/pipeline/validate.rs::csv_exact_row_count_passes": "must not error",
    "src/pipeline/validate.rs::csv_no_trailing_newline_counts_final_record": "must not error",
    "src/pipeline/validate.rs::csv_quoted_embedded_crlf_is_one_record": "must not error",
    "src/pipeline/validate.rs::csv_quoted_field_at_eof_without_trailing_newline": "must not error",
    "src/pipeline/validate.rs::csv_trailing_newline_does_not_count_as_row": "must not error",
    "src/plan/artifact.rs::integrity_checksum_is_not_tamper_protection": "must not error",
    "src/plan/artifact.rs::integrity_seal_legacy_empty_is_accepted_with_warning": "must not error",
    "src/resource.rs::rss_peak_sampler_stop_returns_value": "unchecked",
    "src/resource.rs::semaphore_admits_up_to_max_without_blocking": "unchecked",
    "src/source/cdc/mod.rs::max_events_zero_is_a_true_no_op_never_polls_the_stream": "must not error",
    "src/source/cdc/mod.rs::ndjson_run_drops_poison_for_an_uncaptured_table": "must not error",
    "src/source/cdc/spill.rs::how_much_disk_does_a_spilled_row_take": "unchecked",
    "src/source/cdc/value.rs::build_column_is_total_over_arbitrary_cells": "totality",
    "src/source/cdc/value.rs::cell_fixes_are_total_over_arbitrary_wire_values": "totality",
    "src/source/mongo/cdc.rs::decode_resume_token_does_not_panic_on_wellformed_bson_of_the_wrong_shape": "totality",
    "src/source/postgres/cdc.rs::map_pg_value_never_panics": "totality",
    "src/source/postgres/cdc.rs::parse_test_decoding_never_panics": "totality",
    "src/state/migrations.rs::migrating_a_current_database_does_not_wait_for_the_write_lock": "must not error",
    "tests/offline/config_fuzz.rs::pathological_yaml_does_not_panic": "totality",
    "tests/offline/planner_fuzz.rs::generate_chunks_does_not_panic_on_extreme_boundary_combinations": "totality",
    "tests/offline/state_compat.rs::random_bytes_as_state_db_produces_error_not_panic": "totality",
}  # ratchet-pin: end

WEAK_ONLY_CEILING = 190  # ratchet-pin: tests-with-only-weak-assertions

#: Fewer tests than this means the scan stopped matching, not that the tree shrank.
TESTS_FLOOR = 6000  # ratchet-pin: test-checks-scan-sees-the-tree min

SCRUB = re.compile(
    r"//[^\n]*|/\*.*?\*/"
    r'|b?r(#*)".*?"\1|b?"(?:\\.|[^"\\])*"'
    r"|b?'(?:\\(?:u\{[0-9a-fA-F]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])'",
    re.S,
)
FN = re.compile(r"((?:#!?\[[^\]]*\]\s*)*)(?:pub(?:\([^)]*\))?\s+)?(?:(?:async|unsafe|const)\s+)*fn\s+(\w+)")
CHECK = re.compile(
    r"\b(?:assert\w*|debug_assert\w*|prop_assert\w*|panic|unreachable|unimplemented)!"
    r"|\.expect_err\(|\.unwrap_err\("
)
STRONG = re.compile(r"\b(?:panic|unreachable|unimplemented)!|\.expect_err\(|\.unwrap_err\(")
ASSERT_OPEN = re.compile(r"\b((?:debug_|prop_)?assert\w*)!\s*\(")
WEAK_ARG = re.compile(r"\s*!?[\w.()&:\[\]\s]*\.(?:is_ok|is_some|success|is_empty|exists|is_file)\(\)\s*")
CALL = re.compile(r"\b([a-z_][a-z0-9_]{3,})\s*\(")


def scrub(src: str) -> str:
    """Rust source with comments dropped and every string or char literal emptied."""
    return SCRUB.sub(lambda m: " " if m.group(0)[0] == "/" else '""', src)


def _close(text: str, i: int, pair: str = "()") -> int:
    """Index of the bracket that closes the one at `i`."""
    depth = 0
    for j in range(i, len(text)):
        if text[j] == pair[0]:
            depth += 1
        elif text[j] == pair[1]:
            depth -= 1
            if depth == 0:
                return j
    raise ValueError(f"unbalanced {pair} from offset {i}")


def functions(scrubbed: str) -> list[tuple[str, str, str, int]]:
    """(name, attributes, body, offset) of every fn with a body."""
    out = []
    for m in FN.finditer(scrubbed):
        paren = scrubbed.find("(", m.end())
        if paren < 0:
            continue
        j = _close(scrubbed, paren) + 1
        while j < len(scrubbed) and scrubbed[j] not in "{;":
            j = _close(scrubbed, j, "[]") + 1 if scrubbed[j] == "[" else j + 1
        if j < len(scrubbed) and scrubbed[j] == "{":
            out.append((m.group(2), m.group(1), scrubbed[j : _close(scrubbed, j, "{}") + 1], m.start(2)))
    return out


def is_test(attrs: str) -> bool:
    """A `#[test]`, `#[tokio::test]` or other `...::test` attribute."""
    return bool(re.search(r"#\[(?:\w+::)*test\b", attrs))


def only_weak(body: str) -> bool:
    """Every assertion in the body is `assert!(x.is_ok())`-shaped, and there is at least one."""
    if STRONG.search(body):
        return False
    seen = False
    for m in ASSERT_OPEN.finditer(body):
        args = body[m.end() : _close(body, m.end() - 1)]
        depth, first = 0, args
        for k, c in enumerate(args):
            depth += c in "([{"
            depth -= c in ")]}"
            if c == "," and depth == 0:
                first = args[:k]
                break
        if m.group(1) not in ("assert", "debug_assert", "prop_assert") or not WEAK_ARG.fullmatch(first):
            return False
        seen = True
    return seen


def scan(files: dict[str, str]) -> dict:
    """Grade every test in `path → source`: totals, the tests with no check, the weak-only tests."""
    per_file, local, shared = {}, {}, {"src": {}, "tests": {}}
    for path, src in sorted(files.items()):
        text = scrub(src)
        test_from = 0 if path.startswith("tests/") or "/tests/" in path else text.find("#[cfg(test)]")
        fns = per_file[path] = functions(text)
        for name, attrs, body, at in fns:
            if not is_test(attrs) and 0 <= test_from <= at:
                local.setdefault((path, name), []).append(body)
                shared[path.split("/")[0]].setdefault(name, []).append(body)

    def helper_checks(path: str, name: str, seen: tuple = ()) -> bool:
        """A test-code fn of that name (this file first, else its tree) checks, two levels deep."""
        for body in local.get((path, name)) or shared[path.split("/")[0]].get(name, []):
            if CHECK.search(body):
                return True
            if len(seen) < 2 and any(helper_checks(path, c, seen + (name,)) for c in set(CALL.findall(body)) - {name, *seen}):
                return True
        return False

    total, none, weak = 0, set(), set()
    for path, fns in per_file.items():
        for name, attrs, body, _ in fns:
            if not is_test(attrs):
                continue
            total += 1
            if "should_panic" in attrs:
                continue
            delegated = any(helper_checks(path, c) for c in set(CALL.findall(body)))
            if not CHECK.search(body):
                if not delegated:
                    none.add(f"{path}::{name}")
            elif only_weak(body):
                weak.add(f"{path}::{name}")
    return {"tests": total, "no_check": none, "weak_only": weak}


def tree(root: Path) -> dict[str, str]:
    """Every Rust file under `src/` and `tests/`."""
    return {p.relative_to(root).as_posix(): p.read_text() for d in ("src", "tests") for p in sorted((root / d).rglob("*.rs"))}


def grade(found: dict, allowed: dict[str, str], weak_ceiling: int, floor: int) -> list[str]:
    """Every way the tree breaks the two ceilings; empty when it holds them exactly."""
    out = []
    if found["tests"] < floor:
        out.append(f"SCAN: {found['tests']} tests seen, floor {floor}: the scan stopped matching `#[test]`")
    out += [f"NO-CHECK: {t} has no assertion of its own; give it one" for t in sorted(found["no_check"] - set(allowed))]
    out += [f"STALE: {t} now checks or is gone; remove it from NO_CHECK" for t in sorted(set(allowed) - found["no_check"])]
    out += [f"REASON: {t} is listed as {r!r}; the reasons are {sorted(REASONS)}" for t, r in sorted(allowed.items()) if r not in REASONS]
    n = len(found["weak_only"])
    if n > weak_ceiling:
        out.append(f"WEAK-ONLY: {n} tests assert only is_ok/success/is_empty/exists, ceiling {weak_ceiling}; see --list-weak")
    elif n < weak_ceiling:
        out.append(f"WEAK-ONLY: {n} tests, below the ceiling {weak_ceiling}: lower WEAK_ONLY_CEILING to {n} to bank it")
    return out


def self_test() -> int:
    """The scan on a fixture: each class lands where a reader would put it."""
    fixture = {
        "src/a.rs": r'''
fn product() { assert!(true); }
#[cfg(test)]
mod tests {
    fn helper(x: u8) { deeper(x) }
    fn deeper(x: u8) { assert_eq!(x, 1); }
    #[test] fn own() { assert_eq!(1, 1); }
    #[test] fn delegated() { helper(1); }
    #[test] fn calls_product_only() { product(); }
    #[test] fn nothing() { let _ = 1; }
    #[test] fn weak() { assert!(r.is_ok()); assert!(!p.exists(), "gone {}", 1); }
    #[test] fn not_weak() { assert!(r.is_ok()); assert_eq!(a, b); }
    #[test] fn refuses() { let e = run().unwrap_err(); }
    #[test] #[should_panic(expected = "x")] fn panics() { run(); }
    // #[test] fn commented_out() {}
    proptest! {
        #[test] fn total(s in "[a\"]{0,3}", t in ".{0,9}") { let _ = parse(&s); }
        #[test] fn prop(s in "x{1,2}") -> [u8; 2] { prop_assert!(parse(&s) > 0); }
    }
    #[test] fn text_is_not_a_check() { let _ = "assert!(x)"; }
    #[tokio::test] async fn asynchronous() { run().await; }
}
''',
        "tests/b.rs": "fn rig_check() { assert!(true) }\n#[test]\nfn via_rig() { rig_check(); }\n#[test]\nfn weak_but_delegates() { assert!(x.is_ok()); rig_check(); }\n",
    }
    got = scan(fixture)
    assert got["tests"] == 14, got["tests"]
    want_none = {"src/a.rs::" + n for n in ("calls_product_only", "nothing", "total", "text_is_not_a_check", "asynchronous")}
    assert got["no_check"] == want_none, got["no_check"] ^ want_none
    assert got["weak_only"] == {"src/a.rs::weak", "tests/b.rs::weak_but_delegates"}, got["weak_only"]
    allowed = dict.fromkeys(want_none, "totality")
    assert grade(got, allowed, 2, 14) == []
    assert any(s.startswith("SCAN") for s in grade(got, allowed, 2, 15))
    assert any("nothing has no assertion" in s for s in grade(got, {k: v for k, v in allowed.items() if "nothing" not in k}, 2, 1))
    assert any(s.startswith("STALE: src/a.rs::own") for s in grade(got, {**allowed, "src/a.rs::own": "totality"}, 2, 1))
    assert any(s.startswith("REASON") for s in grade(got, {**allowed, "src/a.rs::nothing": "listed"}, 2, 1))
    assert any("ceiling 1" in s for s in grade(got, allowed, 1, 1)) and any("bank it" in s for s in grade(got, allowed, 3, 1))
    print("test_checks self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    found = scan(tree(ROOT))
    if "--list-weak" in argv:
        print("\n".join(sorted(found["weak_only"])))
        return 0
    problems = grade(found, NO_CHECK, WEAK_ONLY_CEILING, TESTS_FLOOR)
    print("\n".join(problems))
    by_reason = ", ".join(f"{sum(r == k for r in NO_CHECK.values())} {k}" for k in REASONS)
    print(f"test_checks: {found['tests']} tests, {len(found['no_check'])} without a check (listed {len(NO_CHECK)}: {by_reason}), {len(found['weak_only'])} weak-only (ceiling {WEAK_ONLY_CEILING})")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
