//! Tuning module — resolution of `SourceTuning` from YAML + adaptive helpers.
//!
//! Split into three concerns:
//! - [`profile`] — config types, three baked-in profiles, `SourceTuning`,
//!   `ResourceSummary`. The bulk of the module's surface lives here.
//! - [`memory`] — pure schema-based memory estimation
//!   ([`estimate_row_bytes`], [`compute_batch_size_from_memory`]).
//! - [`adaptive`] — live-feedback batch sizing
//!   ([`next_adaptive_batch_size`] + the `ADAPTIVE_*` constants).
//!
//! All public symbols are re-exported at the module root so existing
//! `crate::tuning::SourceTuning` / `crate::tuning::estimate_row_bytes` paths
//! keep working.

mod adaptive;
mod memory;
mod profile;

pub use adaptive::{ADAPTIVE_SAMPLE_INTERVAL, next_adaptive_batch_size};
pub use memory::estimate_row_bytes;
pub use profile::{
    BatchMemoryPolicy, SourceTuning, TuningConfig, TuningProfile, merge_tuning_config,
};
