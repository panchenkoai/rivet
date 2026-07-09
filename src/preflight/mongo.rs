//! MongoDB preflight diagnostics — the document-store analogue of
//! [`super::postgres`]/[`super::mysql`]/[`super::mssql`].
//!
//! MongoDB is `mode: full` only, so every export is a collection scan: no
//! cursor, no index-range analysis, no chunk boundaries. The diagnostic reuses
//! the shared verdict / profile / parallelism helpers so `rivet check` and
//! `rivet plan` render a Mongo export the same way as a SQL one — the only
//! source-specific input is the scan-free `estimatedDocumentCount` row estimate.

use super::ExportDiagnostic;
use super::analysis::*;
use crate::config::{ExportConfig, TlsConfig};
use crate::error::Result;

/// Connect and build one [`ExportDiagnostic`] per export (multi-export `check`).
/// Rendering (TEXT table vs `--json`) is the caller's job in [`super::check`].
pub(super) fn check_mongo(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
) -> Result<Vec<ExportDiagnostic>> {
    super::collect_diagnostics(exports, |export| diagnose_mongo(url, tls, export))
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_mongo(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
) -> Result<ExportDiagnostic> {
    diagnose_mongo(url, tls, export)
}

fn diagnose_mongo(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
) -> Result<ExportDiagnostic> {
    // Scan-free row estimate via `estimatedDocumentCount` (collection metadata,
    // never a scan) — the Mongo analogue of PG `reltuples`. Resolved from the
    // `table:` shortcut (the only export shape Mongo supports); `None` when the
    // collection is unknown or the estimate probe fails, exactly like MySQL.
    let row_estimate = export
        .table
        .as_deref()
        .and_then(|coll| crate::source::mongo::estimated_count(url, tls, coll));

    // A full collection scan uses no index and has no cursor.
    let uses_index = false;
    let strategy = derive_strategy(export);
    let verdict = compute_verdict(row_estimate, uses_index, false, None, export.parallel);
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    let warnings = collect_warnings(export, row_estimate, None, None, None, None);

    Ok(ExportDiagnostic {
        export_name: export.name.clone(),
        strategy,
        mode: "full".to_string(),
        cursor_column: None,
        row_estimate,
        avg_row_bytes: None,
        cursor_min: None,
        cursor_max: None,
        scan_type: Some("collection scan (full)".to_string()),
        uses_index,
        verdict,
        recommended_profile,
        recommended_parallel,
        warnings,
        // No mode suggestion: `full` is Mongo's only mode, so the SQL engines'
        // "consider chunked/incremental" advice would point at something the
        // document source cannot do. Profile/parallel advice still rides the
        // fields above.
        suggestion: None,
    })
}
