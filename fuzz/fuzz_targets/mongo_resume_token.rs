#![no_main]
//! Fuzz the MongoDB resume-token decoder — hex → BSON `Document::from_reader`,
//! reached from a JSON checkpoint field. Must not panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rivet::fuzz::mongo_resume_token(data);
});
