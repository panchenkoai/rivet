//! Fuzz / smoke tests for planner and SQL-shaping inputs.
//!
//! QA backlog Task 4A.2. Adversarial identifiers, base queries, and window
//! parameters drive the two publicly-reachable shaping surfaces:
//!
//!   * `rivet::pipeline::build_time_window_query`
//!   * `rivet::pipeline::generate_chunks` (correctness grid lives as a unit
//!     test in `src/pipeline/chunked/math.rs`; this file adds the fuzz-like
//!     boundary sweep)
//!
//! Acceptance: no panic on any input.  Correctness beyond panic-safety is
//! covered by the property grid in the unit-test module.

use rivet::config::{SourceType, TimeColumnType};
use rivet::pipeline::{build_time_window_query, generate_chunks};

#[test]
fn generate_chunks_does_not_panic_on_extreme_boundary_combinations() {
    // Each triple generates at most a small bounded number of chunks —
    // otherwise the test would hang.  "Wide range × tiny chunk_size" is
    // out of scope for a fuzz smoke and would need a planner-level cap.
    let triples: &[(i64, i64, i64)] = &[
        (0, 0, 1),
        (i64::MIN, i64::MIN + 1, 1),
        (i64::MIN, i64::MIN, 1),
        (i64::MIN, i64::MAX, i64::MAX), // single chunk, size saturates
        (i64::MAX - 2, i64::MAX, 1),
        (i64::MAX - 2, i64::MAX, 10),
        (i64::MAX - 2, i64::MAX, i64::MAX),
        (-10, 10, 3),
        (0, 0, i64::MAX),
        (0, 0, i64::MIN), // chunk_size <= 0 → empty
        (5, 5, -1),       // chunk_size <= 0 → empty
    ];
    for &(min, max, size) in triples {
        let _ = generate_chunks(min, max, size);
    }
}

#[test]
fn build_time_window_query_does_not_panic_on_adversarial_columns() {
    // Column names a real user might (accidentally or maliciously) pass:
    // injection attempts, quotes, NUL, whitespace-only, emoji, very long,
    // leading/trailing backticks/double-quotes.
    let columns: &[&str] = &[
        "created_at",
        "",
        " ",
        "\"id\"",
        "`id`",
        "id; DROP TABLE users; --",
        "id\" OR 1=1 --",
        "id`) OR (1=1",
        "col\nname",
        "col\x00name",
        "💣",
        &"x".repeat(10_000),
        "snake_case_with_123_digits",
        "MixedCaseColumn",
        "正體中文列名",
    ];
    let sources = [SourceType::Postgres, SourceType::Mysql];
    let types = [TimeColumnType::Timestamp, TimeColumnType::Unix];
    let base_queries: &[&str] = &[
        "SELECT * FROM t",
        "",
        "SELECT * FROM ({inner}) AS z",
        "-- a SQL comment line\nSELECT 1",
        "SELECT * FROM t WHERE a > 'x\"y'",
    ];

    for &col in columns {
        for &src in &sources {
            for &tt in &types {
                for &bq in base_queries {
                    // Sweep realistic and extreme `days_window`.  `u32::MAX`
                    // used to crash this path (DateTime - TimeDelta overflow);
                    // now saturates at DateTime::MIN_UTC.
                    for &days in &[0u32, 1, 7, 30, 365, u32::MAX] {
                        let out = build_time_window_query(bq, col, tt, days, src);
                        // Contract: output is always valid UTF-8.
                        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
                    }
                }
            }
        }
    }
}
