#![no_main]
//! Fuzz the config YAML parser (`Config::from_yaml`): a garbled or hostile
//! config file must yield a clean error, never a panic.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rivet::fuzz::config_from_yaml(data);
});
