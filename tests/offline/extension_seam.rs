//! ADR-0026 — first-party extension seam stability canary.
//!
//! `rivet-pro` (private, BSL 1.1, separate repo) depends on the exact library
//! items referenced below. This module COMPILE-LOCKS their shape: if it fails
//! to build, an item on the seam changed. That is a deliberate decision —
//! update `rivet-pro` in lockstep AND record it in `CHANGELOG.md` under a
//! `Breaking (extension seam)` line. Do NOT just edit this file to compile.
//!
//! See docs/adr/0026-first-party-extension-seam.md for the full seam table.

use rivet::google_auth::{AdcUserTokenLoader, try_authorized_user_loader};
use rivet::types::TypeMapping;
use rivet::types::target::{ExportTarget, TargetColumnSpec, TargetInput};

// ExportTarget::resolve_table(self, &[TypeMapping]) -> Vec<TargetColumnSpec>
fn seam_resolve_table(t: ExportTarget, mappings: &[TypeMapping]) -> Vec<TargetColumnSpec> {
    t.resolve_table(mappings)
}

// ExportTarget::resolve_column(self, TargetInput<'_>) -> TargetColumnSpec
fn seam_resolve_column(t: ExportTarget, input: TargetInput<'_>) -> TargetColumnSpec {
    t.resolve_column(input)
}

// The four warehouse variants rivet-pro matches on.
fn seam_variants() -> [ExportTarget; 4] {
    [
        ExportTarget::DuckDb,
        ExportTarget::BigQuery,
        ExportTarget::Snowflake,
        ExportTarget::ClickHouse,
    ]
}

/// The ADC loader's shape, compile-locked: a fallible probe returning an
/// optional loader that implements reqsign's `GoogleTokenLoad` — the only
/// property a consumer needs, since it is plugged into
/// `GoogleTokenLoader::with_customized_token_loader`.
fn seam_adc_loader() -> anyhow::Result<Option<AdcUserTokenLoader>> {
    let loaded = try_authorized_user_loader()?;
    fn assert_token_load<T: reqsign::GoogleTokenLoad>(_: &T) {}
    if let Some(l) = &loaded {
        assert_token_load(l);
    }
    Ok(loaded)
}

#[test]
fn first_party_extension_seam_is_stable() {
    // Binding each item to an explicitly-typed function pointer double-locks
    // the signature and — since CI runs `clippy --all-targets -D warnings`,
    // tests included — keeps the dead_code lint from firing on the helpers.
    let _rt: fn(ExportTarget, &[TypeMapping]) -> Vec<TargetColumnSpec> = seam_resolve_table;
    let _rc: fn(ExportTarget, TargetInput<'_>) -> TargetColumnSpec = seam_resolve_column;
    assert_eq!(seam_variants().len(), 4);
    // The ADC loader probe: it must COMPILE against the seam and must not
    // panic on a machine with no ADC file (it returns Ok(None) there).
    let _adc: fn() -> anyhow::Result<Option<AdcUserTokenLoader>> = seam_adc_loader;
    assert!(seam_adc_loader().is_ok());
}
