#![no_main]
//! Fuzz the secret redactors — every log line and error text passes through
//! them. Must not panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rivet::fuzz::redact_text(data);
});
