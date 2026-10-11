#![no_main]
//! Fuzz the query-text recognisers — the `query:` of an export read as text for
//! its table form, its outer `FROM` relation and its row set. Must not panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rivet::fuzz::sql_query_text(data);
});
