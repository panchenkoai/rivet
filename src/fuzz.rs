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
//! already-typed Arrow (it never parses untrusted Parquet). The three parsers
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
        let _ = crate::source::postgres::cdc::parse_test_decoding("0/16B2D48", s);
    }
}

/// MongoDB resume-token decoder — hex → BSON `Document::from_reader`, a real
/// untrusted binary-parse path reached from a JSON checkpoint field.
pub fn mongo_resume_token(data: &[u8]) {
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(data) {
        let _ = crate::source::mongo::cdc::decode_resume_token(&v);
    }
}
