//! **Layer: Execution** — the single home for committing one output part.
//!
//! Every runner (single, chunked-sequential, keyset, chunked-parallel,
//! sequential_checkpoint, parallel_checkpoint) produces parts and must commit
//! each to the destination in the ADR-0001 order: the temp writer is finalized
//! (I1), the file is written to the destination, the manifest part is recorded
//! (I2/M1), the file log advances (I7, warn-on-fail), and the part is
//! journaled. Before this module that sequence — plus the `files_committed`
//! counter and the crash-injection fault points — was hand-copied into each
//! runner and had already drifted (the keyset runner never bumped
//! `files_committed` and had no fault hooks; only `single` did both;
//! parallel_checkpoint never populated `summary.manifest_parts` at all,
//! producing an empty cloud manifest — see commit e9b0796).
//!
//! Split at the line where the parallel engine forks: the WORKER writes the file
//! ([`write_part_file`] — I1 + `dest.write` + fingerprint, safe off-thread) and
//! the caller records it ([`record_part`] — I2/M1 + counters + journal + I7).
//! Sequential runners call both inline; the parallel engine writes in the worker
//! and records in the drained parent loop, so the I2/I7 ordering lives once for
//! both engines.
//!
//! ## In-step ordering within [`record_part`]
//!
//! The four side effects fire in this fixed order:
//!
//! 1. byte / file counter bumps on `summary` (`bytes_written`,
//!    `files_produced`, `files_committed`)
//! 2. **ADR-0012 M1** — `manifest_writer::record_committed_part_with_fingerprint`
//!    appends to `summary.manifest_parts`
//! 3. `summary.journal.record` (`FileWritten` or `ChunkCompleted`)
//! 4. **ADR-0001 I7** — `state.record_file` (warn-on-fail; non-fatal)
//!
//! Pre-commit_part, some runners had I7 before M1 (the chunked-parallel
//! engine in particular called `state.record_file` from each worker then
//! appended to `manifest_parts` in the post-scope drain). Both orderings
//! are correct because no externally-observable durability contract exists
//! *between* the two writes — the run has not finalized, so neither the
//! cloud manifest nor the cursor have advanced past the part. Picking one
//! order at the seam keeps all five runners consistent and makes the
//! `cfg!(debug_assertions)` coherence check in
//! [`crate::pipeline::finalize::finalize_manifest`] meaningful.

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
    /// Base64 MD5 of the part body (GCS `md5Hash` encoding), computed from the
    /// local temp file alongside the fingerprint.  Empty if it could not be
    /// computed — verification degrades to size-only for that part.
    pub md5: String,
}

/// How a committed part is journaled.
///
/// - `File { part_index }` — written by the single-file (snapshot / incremental)
///   runner: emits `RunEvent::FileWritten`.
/// - `Chunk { chunk_index }` — the chunked runners (sequential / parallel /
///   sequential_checkpoint / parallel_checkpoint): emits
///   `RunEvent::ChunkCompleted` with the real chunk window's index.
/// - `Page { page_index }` — keyset (seek-paginated) runner: emits
///   `RunEvent::ChunkCompleted` *for backward journal compatibility* but
///   conceptually a keyset page is **not** a chunk (no `[start, end]`
///   integer window, no chunk-task lifecycle row in `chunk_task`). Carrying
///   it as a distinct seam variant keeps the caller's intent visible at
///   the call site and leaves room to fork to a dedicated
///   `RunEvent::KeysetPageWritten` later without touching the runners —
///   only the match arm in [`record_part`] would change.
pub(crate) enum PartKind {
    File { part_index: usize },
    Chunk { chunk_index: i64 },
    Page { page_index: i64 },
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
    // Base64 MD5 for no-download destination verification (GCS md5Hash).
    // Non-fatal: an empty value degrades that part's check to size-only.
    let md5 = manifest_writer::compute_part_md5(tmp_path).unwrap_or_else(|e| {
        log::warn!("part md5 failed for '{file_name}' (not fatal): {e:#}");
        String::new()
    });
    Ok(PartRecord {
        file_name,
        rows,
        bytes,
        fingerprint,
        md5,
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
        part.md5.clone(),
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
        // Keyset pages reuse ChunkCompleted to preserve journal-on-disk
        // backward compatibility. See [`PartKind::Page`] for the rationale
        // and the upgrade path if/when downstream observability needs to
        // distinguish keyset pages from chunked windows.
        PartKind::Page { page_index } => summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index: page_index,
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
            md5: String::new(),
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

    // ── summary ↔ manifest coherence (CI gate, gaps #2 + #3 in invariant audit)
    //
    // record_part is the single home that mutates summary.bytes_written,
    // summary.files_produced, summary.files_committed, and
    // summary.manifest_parts. The seam exists so the four cannot drift. This
    // test pins the contract: after N record_part calls on a freshly stubbed
    // summary (no resume hydration), the four aggregates must agree with
    // manifest_parts byte-for-byte. If a future runner bypasses record_part
    // and bumps a counter inline, this test still passes — but the
    // finalize_manifest runtime debug_assert (see assert_summary_post_run)
    // will fire the moment that runner finishes a real export. Two layers,
    // both CI-enforced via `cargo test`.

    fn synthetic_parts(n: usize) -> Vec<PartRecord> {
        (0..n)
            .map(|i| PartRecord {
                file_name: format!("part_{i}.parquet"),
                rows: 100 + (i as i64) * 10,
                bytes: 1024 * ((i as u64) + 1),
                fingerprint: format!("xxh3:{i:016x}"),
                md5: String::new(),
            })
            .collect()
    }

    #[test]
    fn record_part_keeps_summary_aggregates_coherent_with_manifest_parts() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let parts = synthetic_parts(5);

        // Simulate a runner: bump total_rows then record_part for each chunk.
        // record_part does NOT touch total_rows; the runner owns that bump,
        // so we model both halves of the contract here.
        for (i, p) in parts.iter().enumerate() {
            summary.total_rows += p.rows;
            record_part(
                &plan,
                &mut summary,
                None,
                p,
                PartKind::Chunk {
                    chunk_index: i as i64,
                },
            );
        }

        let parts_rows: i64 = summary.manifest_parts.iter().map(|p| p.rows).sum();
        let parts_bytes: u64 = summary.manifest_parts.iter().map(|p| p.size_bytes).sum();

        assert_eq!(
            summary.total_rows, parts_rows,
            "non-resume run: summary.total_rows must equal sum(manifest_parts.rows)"
        );
        assert_eq!(
            summary.bytes_written, parts_bytes,
            "non-resume run: summary.bytes_written must equal sum(manifest_parts.size_bytes)"
        );
        assert_eq!(
            summary.files_produced,
            summary.manifest_parts.len(),
            "files_produced must equal manifest_parts.len() (record_part bumps both)"
        );
        assert_eq!(
            summary.files_committed,
            summary.manifest_parts.len(),
            "files_committed must equal manifest_parts.len() (record_part bumps both)"
        );
    }

    #[test]
    fn nonempty_successful_run_must_have_nonempty_manifest_parts() {
        // Contrapositive of M1: a successful run that committed at least one
        // file must surface that file in the cloud manifest. Before
        // commit e9b0796 parallel_checkpoint violated this — its
        // manifest_parts stayed empty for every run while files_committed
        // and bytes_written reported real work. This test pins the
        // contract: if files_committed > 0 then manifest_parts is non-empty.
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        summary.total_rows += part.rows;
        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 0 },
        );
        summary.status = "success".into();

        assert!(summary.files_committed > 0, "test premise: work committed");
        assert!(
            !summary.manifest_parts.is_empty(),
            "non-empty success run must surface files in manifest_parts"
        );
        assert_eq!(
            summary.files_committed,
            summary.manifest_parts.len(),
            "files_committed and manifest_parts.len() locked together by record_part"
        );
    }
}
