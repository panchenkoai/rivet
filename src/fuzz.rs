//! Fuzz-only entry points — compiled only with `--features fuzzing` and driven
//! by the cargo-fuzz targets in `fuzz/`. Each wrapper feeds a raw byte slice
//! into one of Rivet's untrusted-input parsers; the fuzz contract is simply
//! **must not panic** (a garbled input must yield a clean `Err`/`None`, never an
//! `unwrap`/slice/overflow panic). This module is never compiled into the
//! published crate — the feature is off by default and enabled by no other.
//!
//! Scope note: Rivet's genuinely untrusted *text/binary parse* surface is
//! narrow. MySQL and SQL Server CDC decode the **binary** wire protocol inside
//! their driver crates (not Rivet code); the Parquet path only *writes* from
//! already-typed Arrow (it never parses untrusted Parquet). The parsers
//! below are the real Rivet-owned surface.

/// Config YAML parser. The operator-authored config is only semi-trusted, but a
/// hostile or garbled file must produce a clean error — never a panic.
pub fn config_from_yaml(data: &[u8]) {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = crate::config::Config::from_yaml(s);
    }
}

/// PostgreSQL `test_decoding` text decoder — parses adversarial-ish text off the
/// logical-replication wire (column lists, typed values, arrays, intervals,
/// timestamps). The richest Rivet-side parse surface and historically the most
/// bug-prone (timezone / datestyle / bytea rendering); see the CDC text-decode
/// rules in the project guidelines.
pub fn pg_test_decoding(data: &[u8]) {
    if let Ok(s) = std::str::from_utf8(data) {
        // A fixed valid LSN — all the parsing under test lives in `data`.
        let _ =
            crate::source::postgres::cdc::parse_test_decoding("0/16B2D48", s, &Default::default());
    }
}

/// MongoDB resume-token decoder — hex → BSON `Document::from_reader`, a real
/// untrusted binary-parse path reached from a JSON checkpoint field.
pub fn mongo_resume_token(data: &[u8]) {
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(data) {
        let _ = crate::source::mongo::cdc::decode_resume_token(&v);
    }
}

/// CDC spill tagged-frame decoder — the length-prefixed record log a crashed
/// run leaves on DISK, i.e. a persisted, corruptible input (same standing as
/// the checkpoint row in the fuzz matrix). Round-4 found two crash-classes by
/// hand — an ARRAY nesting bomb (stack overflow, uncatchable) and a lying
/// length prefix (up to 4 GiB pre-committed) — both now refused; this target
/// holds the whole frame surface to "Err, never panic".
pub fn spill_frame_decode(data: &[u8]) {
    let _ = crate::source::cdc::spill::decode_event(data);
}

/// Query-text recognisers — the operator-authored `query:` every export is read through: the table-form and plain-projection shortcuts, the outer `FROM` relation, the row-set text and the preflight relation scan. Any UTF-8 text is recognised or declined, never a panic.
pub fn sql_query_text(data: &[u8]) {
    use crate::config::SourceType::{Mssql, Mysql, Oracle, Postgres};
    let Ok(query) = std::str::from_utf8(data) else {
        return;
    };
    let _ = crate::sql::strip_select_star_from(query);
    let _ = crate::sql::row_set(query);
    let _ = crate::sql::outer_from_relation(query);
    let _ = crate::preflight::table_from_simple_query(query);
    for engine in [Postgres, Mysql, Mssql, Oracle] {
        let _ = crate::sql::aggregate_sql(engine, "min", "id", query);
        let _ = crate::sql::null_key_probe_sql(engine, "id", query);
    }
}

/// The secret redactors — every log line and error text passes through them, and that text quotes drivers, servers and operator input. Any UTF-8 text is rewritten or returned, never a panic.
pub fn redact_text(data: &[u8]) {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = crate::redact::redact_secrets(text);
    }
}
