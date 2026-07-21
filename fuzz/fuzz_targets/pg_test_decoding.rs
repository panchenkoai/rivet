#![no_main]
//! Fuzz the PostgreSQL `test_decoding` text decoder — the richest Rivet-side
//! parse surface (column lists, typed values, arrays, intervals, timestamps),
//! fed adversarial text off the replication wire. Must not panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rivet::fuzz::pg_test_decoding(data);
});
