//! ADR-0026 — first-party extension seam stability canary.
//!
//! `rivet-pro` (private, BSL 1.1, separate repo) depends on the exact library
//! items referenced below. This module COMPILE-LOCKS their shape: if it fails
//! to build, an item on the seam changed. That is a deliberate decision —
//! update `rivet-pro` in lockstep AND record it in `CHANGELOG.md` under a
//! `Breaking (extension seam)` line. Do NOT just edit this file to compile.
//!
//! See docs/adr/0026-first-party-extension-seam.md for the full seam table.

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

#[test]
fn first_party_extension_seam_is_stable() {
    // Binding each item to an explicitly-typed function pointer double-locks
    // the signature and — since CI runs `clippy --all-targets -D warnings`,
    // tests included — keeps the dead_code lint from firing on the helpers.
    let _rt: fn(ExportTarget, &[TypeMapping]) -> Vec<TargetColumnSpec> = seam_resolve_table;
    let _rc: fn(ExportTarget, TargetInput<'_>) -> TargetColumnSpec = seam_resolve_column;
    assert_eq!(seam_variants().len(), 4);
}
