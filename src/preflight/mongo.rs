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
    mongo: Option<&crate::config::MongoConfig>,
) -> Result<Vec<ExportDiagnostic>> {
    super::collect_diagnostics(exports, |export| diagnose_mongo(url, tls, export, mongo))
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_mongo(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
    mongo: Option<&crate::config::MongoConfig>,
) -> Result<ExportDiagnostic> {
    diagnose_mongo(url, tls, export, mongo)
}

fn diagnose_mongo(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
    mongo: Option<&crate::config::MongoConfig>,
) -> Result<ExportDiagnostic> {
    // Scan-free row estimate via `estimatedDocumentCount` (collection metadata,
    // never a scan) — the Mongo analogue of PG `reltuples`. Resolved from the
    // `table:` shortcut (the only export shape Mongo supports); `None` when the
    // collection is unknown or the estimate probe fails, exactly like MySQL.
    let row_estimate = export
        .table
        .as_deref()
        .and_then(|coll| crate::source::mongo::estimated_count(url, tls, coll));

    // The diagnostic-bypass class (round-10, closing the round-7 find):
    // `source.mongo.page_size` routes `mode: full` to the KEYSET `_id`-range
    // reader — an indexed seek that `parallel: N` fans out — but this
    // diagnostic never received the source config and hardcoded
    // "collection scan / no index / UNSAFE" for the correctly-configured
    // path. Four false claims on one line of check output.
    let keyset = mongo.is_some_and(|m| m.page_size.is_some());
    let uses_index = keyset; // `_id` seek rides the mandatory _id index
    let strategy = if keyset {
        format!(
            "keyset(_id, page_size={})",
            mongo.and_then(|m| m.page_size).unwrap_or(0)
        )
    } else {
        derive_strategy(export)
    };
    let verdict = compute_verdict(row_estimate, uses_index, false, None, export.parallel);
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    // Mongo's connection headroom (serverStatus().connections.available) — the
    // analogue of PG/MySQL max_connections for the mongo_parallel worker count.
    let db_max_connections = crate::source::mongo::max_connections(url, tls);
    let warnings = collect_warnings(export, row_estimate, None, None, None, db_max_connections);

    Ok(ExportDiagnostic {
        row_source: None,
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
        chunk_min: None,
        chunk_max: None,
        // Store the fetched headroom (not None) so the #149/#202 measured overlay's
        // re-run of collect_warnings gets Mongo's real connection limit and runs the
        // check_connection_limit check, instead of the "could not fetch" skip note —
        // parity with pg/mysql/mssql. Mongo is full-only, so the overlay always runs.
        db_max_connections,
    })
}
