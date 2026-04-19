//! Golden tests for the time-window SQL builder.
//!
//! `build_time_window_query` is a pure function — unit-test coverage for
//! adversarial identifiers lives in `tests/planner_fuzz.rs`; this file pins
//! the exact output shape for a handful of representative inputs so accidental
//! regressions in the generated SQL surface in review.
//!
//! Migrated from the former `tests/v2_golden.rs` as part of the "one file per
//! domain" cleanup.

use rivet::config::{SourceType, TimeColumnType};
use rivet::pipeline::build_time_window_query;

#[test]
fn timestamp_format_produces_quoted_iso_literal() {
    let q = build_time_window_query(
        "SELECT * FROM events",
        "created_at",
        TimeColumnType::Timestamp,
        7,
        SourceType::Postgres,
    );
    assert!(q.contains("_rivet WHERE \"created_at\" >= '"), "got: {q}");
    assert!(q.contains("-"), "should have date format, got: {q}");
}

#[test]
fn unix_format_produces_numeric_epoch() {
    let q = build_time_window_query(
        "SELECT * FROM events",
        "ts",
        TimeColumnType::Unix,
        30,
        SourceType::Postgres,
    );
    assert!(q.contains("_rivet WHERE \"ts\" >= "), "got: {q}");
    let after_gte = q.split("\"ts\" >= ").nth(1).unwrap();
    let num: i64 = after_gte.trim().parse().expect("should be a number");
    assert!(
        num > 1_000_000_000,
        "unix timestamp should be large, got: {num}"
    );
}

#[test]
fn wraps_base_query_as_subquery_aliased_rivet() {
    let q = build_time_window_query(
        "SELECT id, name FROM users",
        "updated_at",
        TimeColumnType::Timestamp,
        1,
        SourceType::Postgres,
    );
    assert!(
        q.contains("FROM (SELECT id, name FROM users) AS _rivet"),
        "got: {q}"
    );
}
