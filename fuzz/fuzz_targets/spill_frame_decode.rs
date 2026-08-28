#![no_main]
//! Fuzz the CDC spill tagged-frame decoder — a persisted on-disk record a
//! crashed run leaves behind. Must not panic (and must not recurse unbounded:
//! the ARRAY depth cap turns a nesting bomb into an `Err`).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rivet::fuzz::spill_frame_decode(data);
});
