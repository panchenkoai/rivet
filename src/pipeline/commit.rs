//! **Layer: Execution** — the single home for committing one output part.
//!
//! Every runner (single, chunked-sequential, keyset, chunked-parallel) produces
//! parts and must commit each to the destination in the ADR-0001 order: the
//! temp writer is finalized (I1), the file is written to the destination, the
//! manifest part is recorded (I2/M1), the file log advances (I7, warn-on-fail),
//! and the part is journaled. Before this module that sequence — plus the
//! `files_committed` counter and the crash-injection fault points — was
//! hand-copied into each runner and had already drifted (the keyset runner never
//! bumped `files_committed` and had no fault hooks; only `single` did both).
//!
//! Split at the line where the parallel engine forks: the WORKER writes the file
//! ([`write_part_file`] — I1 + `dest.write` + fingerprint, safe off-thread) and
//! the caller records it ([`record_part`] — I2/M1 + counters + journal + I7).
//! Sequential runners call both inline; the parallel engine writes in the worker
//! and records in the drained parent loop, so the I2/I7 ordering lives once for
//! both engines.

use std::path::Path;

use super::manifest_writer;
use super::summary::RunSummary;
use crate::destination::Destination;
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

/// A part written to the destination, ready to be recorded. Produced by
/// [`write_part_file`], consumed by [`record_part`].
pub(crate) struct PartRecord {
    pub file_name: String,
    pub rows: i64,
    pub bytes: u64,
    pub fingerprint: String,
}

/// How a committed part is journaled: `single` reports `FileWritten`, the
/// chunk/keyset paths report `ChunkCompleted`.
pub(crate) enum PartKind {
    File { part_index: usize },
    Chunk { chunk_index: i64 },
}

/// Seam 1 — ADR-0001 I1 + the destination-write boundary. Writes the
/// already-finalized temp file to the destination and computes its content
/// fingerprint (ADR-0012 M3) while the local temp file still exists. Safe to
/// call from a worker thread (touches no shared run state).
pub(crate) fn write_part_file(
    dest: &dyn Destination,
    tmp_path: &Path,
    rows: i64,
    file_name: String,
) -> Result<PartRecord> {
    let bytes = std::fs::metadata(tmp_path).map(|m| m.len()).unwrap_or(0);
    dest.write(tmp_path, &file_name)?;
    let fingerprint = manifest_writer::compute_part_fingerprint(tmp_path).unwrap_or_else(|e| {
        log::warn!("part fingerprint failed for '{file_name}' (not fatal): {e:#}");
        "xxh3:0000000000000000".to_string()
    });
    Ok(PartRecord {
        file_name,
        rows,
        bytes,
        fingerprint,
    })
}

/// Seam 2 — the single home for the post-write ordering that used to drift
/// across runners: the I2 fault window, the byte/file counters, the manifest
/// part (I2/M1), the journal event, and the warn-on-fail file-log write (I7).
pub(crate) fn record_part(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
    part: &PartRecord,
    kind: PartKind,
) {
    // ADR-0001 I2→I3 crash window: file at destination, manifest not yet updated.
    crate::test_hook::maybe_panic_at("after_file_write");

    summary.bytes_written += part.bytes;
    summary.files_produced += 1;
    summary.files_committed += 1;

    // ADR-0012 M1: record the committed part for the finalizer's RunManifest.
    manifest_writer::record_committed_part_with_fingerprint(
        summary,
        part.file_name.clone(),
        part.rows,
        part.bytes,
        part.fingerprint.clone(),
    );

    match kind {
        PartKind::File { part_index } => summary.journal.record(RunEvent::FileWritten {
            file_name: part.file_name.clone(),
            rows: part.rows,
            bytes: part.bytes,
            part_index,
        }),
        PartKind::Chunk { chunk_index } => summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index,
            rows: part.rows,
            file_name: Some(part.file_name.clone()),
        }),
    }

    // ADR-0001 I7: file-log (manifest) write failure is non-fatal — the file is
    // already durable at the destination; log and continue.
    if let Some(st) = state
        && let Err(e) = st.record_file(
            &summary.run_id,
            &plan.export_name,
            &part.file_name,
            part.rows,
            part.bytes as i64,
            plan.format.label(),
            Some(plan.compression.label()),
        )
    {
        log::warn!(
            "export '{}': file_log write failed for '{}' (file was produced): {:#}",
            plan.export_name,
            part.file_name,
            e
        );
    }

    // ADR-0001 I3 crash window: manifest recorded, cursor not yet advanced.
    crate::test_hook::maybe_panic_at("after_manifest_update");
}
