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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, SourceConfig, SourceType,
    };
    use crate::destination::local::LocalDestination;
    use crate::journal::RunEvent;
    use crate::pipeline::summary::RunSummary;
    use crate::plan::{ExtractionStrategy, ResolvedRunPlan};
    use crate::state::StateStore;
    use crate::tuning::SourceTuning;
    use std::io::Write;

    fn test_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "orders".into(),
            base_query: "SELECT 1".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: Default::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("/tmp".into()),
                ..Default::default()
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody@127.0.0.1:9999/nonexistent".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                environment: None,
                tuning: None,
                tls: None,
            },
            column_overrides: Default::default(),
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    fn test_summary(plan: &ResolvedRunPlan) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("test_run", plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "snapshot".into();
        s.compression = "none".into();
        s
    }

    fn test_part(file_name: &str) -> PartRecord {
        PartRecord {
            file_name: file_name.into(),
            rows: 42,
            bytes: 1024,
            fingerprint: "xxh3:1234567890abcdef".into(),
        }
    }

    // ── write_part_file ──────────────────────────────────────────────────────

    #[test]
    fn write_part_file_copies_to_destination_and_returns_real_bytes_and_fingerprint() {
        // Stage a fixture file with known content; write it through LocalDestination.
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().join("part.parquet");
        let payload: &[u8] = b"hello rivet";
        std::fs::File::create(&src_path)
            .unwrap()
            .write_all(payload)
            .unwrap();

        let dest = LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dst_dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();

        let rec =
            write_part_file(&dest, &src_path, 7, "out/part.parquet".into()).expect("write ok");

        assert_eq!(rec.file_name, "out/part.parquet");
        assert_eq!(rec.rows, 7);
        assert_eq!(rec.bytes, payload.len() as u64);
        assert!(
            rec.fingerprint.starts_with("xxh3:") && rec.fingerprint.len() == 21,
            "fingerprint should be xxh3:<16 hex chars>, got {:?}",
            rec.fingerprint
        );
        let written = dst_dir.path().join("out").join("part.parquet");
        assert_eq!(std::fs::read(&written).unwrap(), payload);
    }

    // ── record_part: counters + journal + manifest ────────────────────────────

    #[test]
    fn record_part_file_kind_bumps_counters_and_journals_file_written() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::File { part_index: 0 },
        );

        assert_eq!(summary.bytes_written, part.bytes);
        assert_eq!(summary.files_produced, 1);
        assert_eq!(summary.files_committed, 1);
        assert_eq!(summary.manifest_parts.len(), 1);
        assert_eq!(summary.manifest_parts[0].path, part.file_name);
        assert_eq!(summary.manifest_parts[0].rows, part.rows);

        let file_events = summary.journal.files();
        assert_eq!(file_events.len(), 1, "must journal one FileWritten");
        assert!(
            matches!(
                &file_events[0].event,
                RunEvent::FileWritten { part_index: 0, .. }
            ),
            "expected FileWritten{{part_index:0}}, got {:?}",
            file_events[0].event
        );
        assert!(
            summary.journal.chunk_events().is_empty(),
            "File kind must not journal ChunkCompleted"
        );
    }

    #[test]
    fn record_part_chunk_kind_journals_chunk_completed_with_file_name() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk7.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 7 },
        );

        let events = summary.journal.chunk_events();
        assert_eq!(events.len(), 1, "must journal one ChunkCompleted");
        match &events[0].event {
            RunEvent::ChunkCompleted {
                chunk_index,
                rows,
                file_name,
            } => {
                assert_eq!(*chunk_index, 7);
                assert_eq!(*rows, part.rows);
                assert_eq!(file_name.as_deref(), Some(part.file_name.as_str()));
            }
            other => panic!("expected ChunkCompleted, got {other:?}"),
        }
        assert!(
            summary.journal.files().is_empty(),
            "Chunk kind must not journal FileWritten"
        );
    }

    // ── I7: state.record_file is optional and warn-on-fail ───────────────────

    #[test]
    fn record_part_with_state_writes_file_log_entry() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let state = StateStore::open_in_memory().expect("in-memory state");
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            Some(&state),
            &part,
            PartKind::Chunk { chunk_index: 0 },
        );

        let files = state.get_files(Some(&plan.export_name), 16).unwrap();
        assert_eq!(files.len(), 1, "I7: file_log must carry exactly one entry");
        assert_eq!(files[0].file_name, part.file_name);
        assert_eq!(files[0].row_count, part.rows);
    }

    #[test]
    fn record_part_with_none_state_is_a_bypass_not_a_failure() {
        // ADR-0001 I7 says state.record_file is non-fatal. The None case is the
        // strictest form: no state to write to → the manifest/journal/counters
        // half must still complete cleanly.
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 0 },
        );

        assert_eq!(summary.files_committed, 1);
        assert_eq!(summary.manifest_parts.len(), 1);
        assert_eq!(summary.journal.chunk_events().len(), 1);
    }
}
