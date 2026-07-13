//! **Layer: Coordinator**
//!
//! Manifest finalization for cloud-or-local-file destinations.
//!
//! This module owns the *coordination* — building up the `RunManifest` over
//! the course of a run, computing per-part content fingerprints, and writing
//! the final artifact pair (`manifest.json` then `_SUCCESS`) to the destination
//! in the order required by ADR-0012 M1 (Parts Before Manifest) and M2
//! (Manifest Before SUCCESS).
//!
//! Atomicity is delegated to the `Destination` trait: backends declaring
//! `WriteCommitProtocol::Atomic` (single PUT on S3/GCS, `fs::copy` on local FS)
//! are trusted to produce a complete-or-absent visible artifact per call.
//! Partial-write detection on the read side (JSON parse failure for the
//! manifest, length/format check for `_SUCCESS`) provides defence in depth
//! against backends whose atomicity guarantee is weaker than declared.
//!
//! Stdout destinations (`WriteCommitProtocol::Streaming`) skip the manifest
//! entirely — there is no coherent "prefix" to write it to.
//!
//! Resume-aware part skipping (M8 decision matrix) and `_SUCCESS` overwrite
//! refusal (M8) live elsewhere; this module is purely a writer.

use std::io::Read;
use std::path::Path;

use crate::destination::{Destination, WriteCommitProtocol};
use crate::error::Result;
use crate::journal::PlanSnapshot;
use crate::manifest::{
    ColumnChecksum, MANIFEST_FILENAME, ManifestDestination, ManifestPart, ManifestSource,
    ManifestStatus, PartStatus, RunManifest, SUCCESS_FILENAME, success_marker_body,
};
use crate::pipeline::summary::RunSummary;

/// Per-run accumulator for manifest parts.
///
/// One `ManifestBuilder` is created at run start (from the resolved plan)
/// and `record_part` is called after each `dest.write` succeeds — i.e.
/// inside the same I2/I3 window where `file_log::record_file` is called.
/// The build is finalized once, at end of run, into a `RunManifest`.
///
/// `ManifestBuilder` is not `Send + Sync` and is held by the single
/// orchestrator thread; per-part fingerprints are computed before the part
/// is handed off so parallel workers do not need shared access.
pub struct ManifestBuilder {
    run_id: String,
    export_name: String,
    started_at: chrono::DateTime<chrono::Utc>,
    source: ManifestSource,
    destination: ManifestDestination,
    format: String,
    compression: String,
    schema_fingerprint: String,
    parts: Vec<ManifestPart>,
    /// Per-column value checksums (Form B), name-keyed, set by
    /// [`set_column_checksums`](Self::set_column_checksums) from the sink
    /// accumulator. `None` → omitted from the manifest.
    column_checksums: Option<Vec<ColumnChecksum>>,
    /// The column the Form B checksum is keyed to (cursor/key column); `None` =
    /// un-keyed. Recorded so `validate` re-keys identically.
    checksum_key_column: Option<String>,
}

impl ManifestBuilder {
    /// Construct a builder for a run.
    ///
    /// `schema_fingerprint` is the value returned by
    /// [`crate::state::schema_fingerprint`] over the dest-facing column list.
    /// `source_engine` is the resolved engine label (`"postgres"` / `"mysql"`).
    /// `source_schema` / `source_table` are the logical names extracted from
    /// the plan; pass `None` for queries that do not resolve to a single table.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan: &PlanSnapshot,
        run_id: &str,
        started_at: chrono::DateTime<chrono::Utc>,
        schema_fingerprint: String,
        source_engine: &str,
        source_schema: Option<String>,
        source_table: Option<String>,
        destination_uri: String,
    ) -> Self {
        Self {
            run_id: run_id.to_string(),
            export_name: plan.export_name.clone(),
            started_at,
            source: ManifestSource {
                engine: source_engine.to_string(),
                schema: source_schema,
                table: source_table,
                extraction: Some(crate::manifest::ExtractionMetadata {
                    strategy: plan.strategy.clone(),
                    cursor_column: None,
                    cursor_type: None,
                    cursor_low: None,
                    cursor_high: None,
                    source_row_count: None,
                }),
            },
            destination: ManifestDestination {
                kind: plan.destination_type.clone(),
                uri: destination_uri,
            },
            format: plan.format.clone(),
            compression: plan.compression.clone(),
            schema_fingerprint,
            parts: Vec::new(),
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    /// Record the run-level per-column value checksums (Form B), name-keyed,
    /// surfaced into the manifest by [`finalize`](Self::finalize). `key_column` is
    /// the column the hashes are keyed to (`xxh3(key ‖ value)`), `None` if un-keyed.
    pub fn set_column_checksums(
        &mut self,
        checksums: Vec<ColumnChecksum>,
        key_column: Option<String>,
    ) {
        self.column_checksums = Some(checksums);
        self.checksum_key_column = key_column;
    }

    /// Record a committed part.  Must be called only after `dest.write()`
    /// returned `Ok(())` for this part — ADR-0012 M1.
    ///
    /// `part_id` is the 1-based ordinal of the part within this run.
    /// `relative_path` is the destination-prefix-relative path (the same
    /// `remote_key` passed to `dest.write`).
    pub fn record_part(
        &mut self,
        part_id: u32,
        relative_path: String,
        rows: i64,
        size_bytes: u64,
        content_fingerprint: String,
        content_md5: String,
    ) {
        self.parts.push(ManifestPart {
            part_id,
            path: relative_path,
            rows,
            size_bytes,
            content_fingerprint,
            content_md5,
            status: PartStatus::Committed,
        });
    }

    /// Finalize into a `RunManifest`.  Consumes the builder.
    ///
    /// `status` reflects the overall run outcome — `Success` is the only
    /// status that licenses the `_SUCCESS` marker (M2).
    /// Record the cursor range this run covered (incremental strategies) so
    /// the warehouse can prove continuity across runs. `low` is the prior
    /// run's high (or the min seen this run); `high` is the value the next run
    /// resumes from. No-op if there is no extraction section.
    pub fn set_cursor_range(
        &mut self,
        column: Option<String>,
        cursor_type: Option<String>,
        low: Option<String>,
        high: Option<String>,
        source_row_count: Option<i64>,
    ) {
        if let Some(ex) = self.source.extraction.as_mut() {
            ex.cursor_column = column;
            ex.cursor_type = cursor_type;
            ex.cursor_low = low;
            ex.cursor_high = high;
            ex.source_row_count = source_row_count;
        }
    }

    pub fn finalize(self, status: ManifestStatus) -> RunManifest {
        let row_count: i64 = self
            .parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .map(|p| p.rows)
            .sum();
        let part_count = self
            .parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .count() as u32;
        let finished_at = chrono::Utc::now();
        RunManifest {
            manifest_version: crate::manifest::MANIFEST_VERSION,
            mode: "batch".to_string(),
            run_id: self.run_id,
            export_name: self.export_name,
            started_at: self.started_at.to_rfc3339(),
            finished_at: finished_at.to_rfc3339(),
            status,
            source: self.source,
            destination: self.destination,
            format: self.format,
            compression: self.compression,
            schema_fingerprint: self.schema_fingerprint,
            row_count,
            part_count,
            parts: self.parts,
            column_checksums: self.column_checksums,
            checksum_key_column: self.checksum_key_column,
        }
    }
}

/// Compute both part-body hashes in a **single pass** over the file: the xxh3
/// `content_fingerprint` (`"xxh3:<16-hex>"`, ADR-0012 M3) and the base64 MD5
/// (GCS `md5Hash` encoding) used for no-download destination verification.
/// Returns `(fingerprint, md5_base64)`.
///
/// Streams in 64 KiB chunks so multi-GB parts don't require a matching
/// allocation, and reads the file **once** for both digests.  Called right
/// after `dest.write` succeeds while the local temp file still exists; the
/// re-read is disk-cache cheap relative to the upload it follows.
pub fn compute_part_checksums(path: &Path) -> Result<(String, String)> {
    use base64::Engine as _;
    use md5::{Digest, Md5};
    use xxhash_rust::xxh3::Xxh3;
    let mut f = std::fs::File::open(path)?;
    let mut xxh = Xxh3::new();
    let mut md5 = Md5::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        xxh.update(&buf[..n]);
        md5.update(&buf[..n]);
    }
    Ok((
        format!("xxh3:{:016x}", xxh.digest()),
        base64::engine::general_purpose::STANDARD.encode(md5.finalize()),
    ))
}

/// Capture the run's schema fingerprint on the summary.
///
/// Computed from the dest-facing Arrow schema (the one downstream consumers
/// see — internal columns already stripped) using
/// [`crate::state::schema_fingerprint`].  Idempotent: the schema is identical
/// across chunks of a single run, so callers safely invoke this on every
/// chunk and only the first call has effect.
///
/// Why this exists: `finalize_manifest` previously derived the manifest's
/// `schema_fingerprint` from `state.get_stored_schema()`, which is only
/// populated by `single.rs::run_with_reconnect`'s schema-drift block.  The
/// chunked path never wrote a stored schema, so `manifest.schema_fingerprint`
/// landed as the placeholder `xxh3:0000000000000000` — defeating the trust
/// contract's drift-detection goal (ADR-0012 M3).  Capturing the fingerprint
/// here, at the point the schema is first seen by the sink, decouples the
/// manifest writer from the state's incidental schema cache.
pub fn record_run_schema_fingerprint(
    summary: &mut crate::pipeline::summary::RunSummary,
    dest_schema: &arrow::datatypes::Schema,
) {
    if summary.schema_fingerprint.is_some() {
        return;
    }
    let columns = crate::state::arrow_schema_to_columns(dest_schema);
    summary.schema_fingerprint = Some(crate::state::schema_fingerprint(&columns));
}

/// Same as [`record_committed_part`] but with a precomputed fingerprint.
///
/// Used by parallel-chunked aggregation, where workers compute fingerprints
/// inside their thread (the local tmp file is dropped at thread exit) and
/// the parent iterates over the shared `file_records` collection.
pub fn record_committed_part_with_fingerprint(
    summary: &mut RunSummary,
    relative_path: String,
    rows: i64,
    size_bytes: u64,
    content_fingerprint: String,
    content_md5: String,
) {
    // ADR-0012 M4: part_id must be unique within the manifest.  Before the
    // M8 resume-hydration work, `summary.manifest_parts.len() + 1` was a
    // safe ordinal because the list was always built from scratch this run.
    // After M8 hydrates parts inherited from the prior manifest, simple
    // length-based numbering can collide (hydrated has [1,2,4,5]; the next
    // write at len()=4 would get part_id=5 — duplicate).  Take max+1 of
    // existing part_ids instead.  Empty list → 1, matching the historical
    // first-part value.
    let part_id = summary
        .manifest_parts
        .iter()
        .map(|p| p.part_id)
        .max()
        .map(|m| m + 1)
        .unwrap_or(1);
    summary.manifest_parts.push(ManifestPart {
        part_id,
        path: relative_path,
        rows,
        size_bytes,
        content_fingerprint,
        content_md5,
        status: PartStatus::Committed,
    });
}

/// Outcome of a manifest-write attempt.
#[derive(Debug)]
pub enum WriteOutcome {
    /// `manifest.json` written.  `_SUCCESS` written iff `status == Success`.
    Written { success_marker: bool },
    /// Destination kind does not support a manifest (e.g. stdout).  Caller
    /// should log and continue — this is not an error.
    SkippedStreaming,
}

/// Write `manifest.json`, then (for `Success` runs only) `_SUCCESS`.
///
/// Enforces M1 (caller has already committed all parts before calling),
/// M2 (manifest before success marker), and M7 (relies on the destination's
/// declared `WriteCommitProtocol` for atomicity).
///
/// Streaming destinations skip the manifest entirely — no coherent prefix
/// exists to write it to, and a polled orchestrator would have nothing to
/// observe.  The function returns `Ok(WriteOutcome::SkippedStreaming)` in
/// that case so the caller can surface a clear "no manifest produced" note
/// in the run report.
pub fn write_manifest(dest: &dyn Destination, manifest: &RunManifest) -> Result<WriteOutcome> {
    if dest.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
        log::info!(
            "destination is streaming; manifest.json / _SUCCESS not written (ADR-0012 §Artifacts)"
        );
        return Ok(WriteOutcome::SkippedStreaming);
    }

    // Finding #44: a batch and a CDC export sharing one prefix silently
    // destroyed each other's manifest (last writer wins, prior parts orphaned
    // from validate). Refuse cross-shape overwrites at the ONE seam both
    // pipelines write through.
    crate::manifest::guard_manifest_mode(dest, &manifest.mode)?;

    let bytes = serde_json::to_vec_pretty(manifest)?;

    let manifest_tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(manifest_tmp.path(), &bytes)?;
    dest.write(manifest_tmp.path(), MANIFEST_FILENAME)?;

    let success_marker = matches!(manifest.status, ManifestStatus::Success);
    if success_marker {
        let marker_body = success_marker_body(&bytes);
        let success_tmp = tempfile::NamedTempFile::new()?;
        std::fs::write(success_tmp.path(), marker_body.as_bytes())?;
        dest.write(success_tmp.path(), SUCCESS_FILENAME)?;
    }

    Ok(WriteOutcome::Written { success_marker })
}

/// `manifest-<sanitized run_id>.json` — the run-token discipline the PARTS
/// already follow (`cdc-<run_token>-NNNN.parquet`) applied to the manifest
/// sidecar. Same sanitizer: an RFC3339 run id carries `:`/`+`, illegal in a
/// Windows filename, so map anything outside `[A-Za-z0-9._-]` to `-`.
pub fn run_unique_manifest_name(run_id: &str) -> String {
    let token: String = run_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("manifest-{token}.json")
}

/// Write an immutable, run-unique COPY of the manifest alongside the canonical
/// [`MANIFEST_FILENAME`]. The canonical name is last-writer-wins — a pointer to
/// the LATEST run, which is what guard/validate/resume/repair want. But
/// consecutive runs into one prefix (a CDC soak, the scheduler's
/// `until_current` model) each clobber it, orphaning every prior run's manifest
/// from any consumer that sums row counts ACROSS runs (the Pro loader's
/// `reconcile`). The per-run copy survives, exactly as the run-token-named
/// parts do. No guard / no `_SUCCESS`: this is a sidecar audit record, not the
/// canonical pointer, so it must not gate on (or advance) run-shape state.
pub fn write_run_unique_manifest_copy(
    dest: &dyn Destination,
    manifest: &RunManifest,
) -> Result<()> {
    if dest.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
        return Ok(());
    }
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), &bytes)?;
    dest.write(tmp.path(), &run_unique_manifest_name(&manifest.run_id))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};
    use crate::destination::local::LocalDestination;
    use std::io::Write;

    fn plan_snapshot() -> PlanSnapshot {
        PlanSnapshot {
            export_name: "public.orders".into(),
            base_query: "SELECT * FROM orders".into(),
            strategy: "snapshot".into(),
            format: "parquet".into(),
            compression: "zstd".into(),
            destination_type: "local".into(),
            tuning_profile: "balanced".into(),
            batch_size: 1000,
            validate: false,
            reconcile: false,
            resume: false,
        }
    }

    fn local_dest(base: &Path) -> LocalDestination {
        LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(base.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .expect("build LocalDestination")
    }

    // ── ManifestBuilder ─────────────────────────────────────────────────────

    #[test]
    fn builder_starts_empty() {
        let b = ManifestBuilder::new(
            &plan_snapshot(),
            "run_001",
            chrono::Utc::now(),
            "xxh3:0000000000000000".into(),
            "postgres",
            Some("public".into()),
            Some("orders".into()),
            "file:///tmp/out/".into(),
        );
        let m = b.finalize(ManifestStatus::Success);
        assert_eq!(m.part_count, 0);
        assert_eq!(m.row_count, 0);
        assert!(m.parts.is_empty());
        assert_eq!(m.validate_self_consistency(), Ok(()));
    }

    #[test]
    fn builder_aggregates_parts_into_self_consistent_manifest() {
        let mut b = ManifestBuilder::new(
            &plan_snapshot(),
            "run_002",
            chrono::Utc::now(),
            "xxh3:0123456789abcdef".into(),
            "postgres",
            None,
            None,
            "file:///tmp/out/".into(),
        );
        b.record_part(
            1,
            "part-000001.parquet".into(),
            50_000,
            4096,
            "xxh3:aaaaaaaaaaaaaaaa".into(),
            String::new(),
        );
        b.record_part(
            2,
            "part-000002.parquet".into(),
            25_000,
            2048,
            "xxh3:bbbbbbbbbbbbbbbb".into(),
            String::new(),
        );

        let m = b.finalize(ManifestStatus::Success);
        assert_eq!(m.part_count, 2);
        assert_eq!(m.row_count, 75_000);
        assert_eq!(m.parts.len(), 2);
        assert_eq!(m.validate_self_consistency(), Ok(()));
    }

    #[test]
    fn builder_records_started_and_finished_in_order() {
        let b = ManifestBuilder::new(
            &plan_snapshot(),
            "run_003",
            chrono::Utc::now(),
            "xxh3:0".into(),
            "postgres",
            None,
            None,
            "file:///x".into(),
        );
        // Sleep a fraction of a ms to make finished_at > started_at observable.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let m = b.finalize(ManifestStatus::Success);
        let started = chrono::DateTime::parse_from_rfc3339(&m.started_at).unwrap();
        let finished = chrono::DateTime::parse_from_rfc3339(&m.finished_at).unwrap();
        assert!(finished >= started, "{started:?} > {finished:?}");
    }

    #[test]
    fn builder_carries_status_through_finalize() {
        let b = ManifestBuilder::new(
            &plan_snapshot(),
            "run_004",
            chrono::Utc::now(),
            "xxh3:0".into(),
            "postgres",
            None,
            None,
            "file:///x".into(),
        );
        let m = b.finalize(ManifestStatus::Failed);
        assert_eq!(m.status, ManifestStatus::Failed);
    }

    // ── compute_part_checksums ──────────────────────────────────────────────

    #[test]
    fn single_pass_checksums_equal_independent_recompute() {
        use base64::Engine as _;
        use md5::{Digest, Md5};
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.bin");
        let data = b"the quick brown fox jumps over the lazy dog";
        std::fs::write(&p, data).unwrap();
        let (fp, md5) = compute_part_checksums(&p).unwrap();
        // The single-read path must equal hashing the bytes independently.
        assert_eq!(
            fp,
            format!("xxh3:{:016x}", xxhash_rust::xxh3::xxh3_64(data))
        );
        let mut h = Md5::new();
        h.update(data);
        assert_eq!(
            md5,
            base64::engine::general_purpose::STANDARD.encode(h.finalize())
        );
    }

    #[test]
    fn fingerprint_format_matches_adr_0012() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.bin");
        std::fs::write(&p, b"hello world").unwrap();
        let (fp, _) = compute_part_checksums(&p).unwrap();
        assert!(fp.starts_with("xxh3:"));
        assert_eq!(fp.len(), "xxh3:".len() + 16);
        let hex = &fp["xxh3:".len()..];
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn fingerprint_is_content_dependent() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bin");
        let b = dir.path().join("b.bin");
        std::fs::write(&a, b"alpha").unwrap();
        std::fs::write(&b, b"beta").unwrap();
        assert_ne!(
            compute_part_checksums(&a).unwrap().0,
            compute_part_checksums(&b).unwrap().0
        );
    }

    #[test]
    fn fingerprint_is_deterministic_for_same_content() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.bin");
        std::fs::write(&p, b"deterministic").unwrap();
        let fp1 = compute_part_checksums(&p).unwrap().0;
        let fp2 = compute_part_checksums(&p).unwrap().0;
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn fingerprint_streams_files_larger_than_buffer() {
        // Defends against a buffer-truncation bug: a file larger than the
        // 64 KiB streaming buffer must hash the same as a single-shot xxh3.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("big.bin");
        let payload: Vec<u8> = (0..(128 * 1024)).map(|i| (i % 251) as u8).collect();
        {
            let mut f = std::fs::File::create(&p).unwrap();
            f.write_all(&payload).unwrap();
        }
        let (fp, _) = compute_part_checksums(&p).unwrap();
        let one_shot = format!("xxh3:{:016x}", xxhash_rust::xxh3::xxh3_64(&payload));
        assert_eq!(fp, one_shot);
    }

    // ── write_manifest ──────────────────────────────────────────────────────

    fn build_manifest(status: ManifestStatus) -> RunManifest {
        let mut b = ManifestBuilder::new(
            &plan_snapshot(),
            "run_001",
            chrono::Utc::now(),
            "xxh3:0123456789abcdef".into(),
            "postgres",
            Some("public".into()),
            Some("orders".into()),
            "file:///tmp/out/".into(),
        );
        b.record_part(
            1,
            "part-000001.parquet".into(),
            100,
            4096,
            "xxh3:1111111111111111".into(),
            String::new(),
        );
        b.record_part(
            2,
            "part-000002.parquet".into(),
            200,
            8192,
            "xxh3:2222222222222222".into(),
            String::new(),
        );
        b.finalize(status)
    }

    #[test]
    fn write_manifest_creates_manifest_json_on_local() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        let m = build_manifest(ManifestStatus::Success);
        let outcome = write_manifest(&dest, &m).unwrap();
        assert!(matches!(
            outcome,
            WriteOutcome::Written {
                success_marker: true
            }
        ));
        assert!(dir.path().join(MANIFEST_FILENAME).exists());
        assert!(dir.path().join(SUCCESS_FILENAME).exists());
    }

    #[test]
    fn manifest_json_is_parseable_and_matches_input() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        let m = build_manifest(ManifestStatus::Success);
        write_manifest(&dest, &m).unwrap();
        let read = std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap();
        let parsed: RunManifest = serde_json::from_str(&read).unwrap();
        assert_eq!(parsed, m);
        assert_eq!(parsed.validate_self_consistency(), Ok(()));
    }

    #[test]
    fn success_marker_carries_correct_fingerprint_for_manifest_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        let m = build_manifest(ManifestStatus::Success);
        write_manifest(&dest, &m).unwrap();
        let bytes = std::fs::read(dir.path().join(MANIFEST_FILENAME)).unwrap();
        let marker = std::fs::read_to_string(dir.path().join(SUCCESS_FILENAME)).unwrap();
        let expected = success_marker_body(&bytes);
        assert_eq!(marker, expected);
    }

    #[test]
    fn failed_status_writes_manifest_but_not_success_marker() {
        // ADR-0012 M2: _SUCCESS exists iff the run completed successfully.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        let m = build_manifest(ManifestStatus::Failed);
        let outcome = write_manifest(&dest, &m).unwrap();
        assert!(matches!(
            outcome,
            WriteOutcome::Written {
                success_marker: false
            }
        ));
        assert!(dir.path().join(MANIFEST_FILENAME).exists());
        assert!(
            !dir.path().join(SUCCESS_FILENAME).exists(),
            "_SUCCESS must be absent for Failed status"
        );
    }

    #[test]
    fn interrupted_status_writes_manifest_but_not_success_marker() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        let m = build_manifest(ManifestStatus::Interrupted);
        let outcome = write_manifest(&dest, &m).unwrap();
        assert!(matches!(
            outcome,
            WriteOutcome::Written {
                success_marker: false
            }
        ));
        assert!(!dir.path().join(SUCCESS_FILENAME).exists());
    }

    #[test]
    fn streaming_destination_skips_manifest() {
        use crate::destination::stdout::StdoutDestination;
        let dest = StdoutDestination::new().unwrap();
        let m = build_manifest(ManifestStatus::Success);
        let outcome = write_manifest(&dest, &m).unwrap();
        assert!(matches!(outcome, WriteOutcome::SkippedStreaming));
    }

    // ── record_run_schema_fingerprint ──────────────────────────────────────

    fn dummy_summary() -> crate::pipeline::summary::RunSummary {
        crate::pipeline::summary::RunSummary::stub_for_testing("r", "orders")
    }

    fn schema_with(fields: &[(&str, arrow::datatypes::DataType)]) -> arrow::datatypes::Schema {
        let f: Vec<arrow::datatypes::Field> = fields
            .iter()
            .map(|(n, t)| arrow::datatypes::Field::new(*n, t.clone(), false))
            .collect();
        arrow::datatypes::Schema::new(f)
    }

    #[test]
    fn record_run_schema_fingerprint_sets_field_on_first_call() {
        use arrow::datatypes::DataType;
        let mut s = dummy_summary();
        let schema = schema_with(&[("id", DataType::Int64), ("name", DataType::Utf8)]);
        record_run_schema_fingerprint(&mut s, &schema);
        let fp = s.schema_fingerprint.as_deref().expect("must be set");
        assert!(fp.starts_with("xxh3:"));
        assert_eq!(fp.len(), "xxh3:".len() + 16);
    }

    #[test]
    fn record_run_schema_fingerprint_is_idempotent() {
        // Across chunks of one run the schema is identical; later calls
        // must not overwrite.  Pin this so a future "always overwrite"
        // refactor can't silently make the fingerprint depend on which
        // chunk happened to land last.
        use arrow::datatypes::DataType;
        let mut s = dummy_summary();
        let schema_a = schema_with(&[("id", DataType::Int64)]);
        record_run_schema_fingerprint(&mut s, &schema_a);
        let first = s.schema_fingerprint.clone();

        // Record-call with a *different* schema — should be a no-op.
        let schema_b = schema_with(&[("id", DataType::Int64), ("extra", DataType::Utf8)]);
        record_run_schema_fingerprint(&mut s, &schema_b);
        assert_eq!(s.schema_fingerprint, first, "later call must not overwrite");
    }

    #[test]
    fn record_run_schema_fingerprint_matches_state_helper_output() {
        // The helper must produce the same value as `state::schema_fingerprint`
        // applied to the same column list — finalize_manifest depends on this
        // equivalence to validate manifests against stored schemas.
        use arrow::datatypes::DataType;
        let mut s = dummy_summary();
        let schema = schema_with(&[("id", DataType::Int64), ("email", DataType::Utf8)]);
        record_run_schema_fingerprint(&mut s, &schema);

        let cols = vec![
            crate::state::SchemaColumn {
                name: "id".into(),
                data_type: format!("{:?}", DataType::Int64),
            },
            crate::state::SchemaColumn {
                name: "email".into(),
                data_type: format!("{:?}", DataType::Utf8),
            },
        ];
        assert_eq!(
            s.schema_fingerprint.unwrap(),
            crate::state::schema_fingerprint(&cols)
        );
    }

    #[test]
    fn record_run_schema_fingerprint_is_order_insensitive() {
        // The helper hashes a sorted-by-name column list (state::schema_fingerprint
        // contract).  Two schemas with reordered fields must produce the same fp.
        use arrow::datatypes::DataType;
        let mut s1 = dummy_summary();
        let mut s2 = dummy_summary();
        record_run_schema_fingerprint(
            &mut s1,
            &schema_with(&[("id", DataType::Int64), ("name", DataType::Utf8)]),
        );
        record_run_schema_fingerprint(
            &mut s2,
            &schema_with(&[("name", DataType::Utf8), ("id", DataType::Int64)]),
        );
        assert_eq!(s1.schema_fingerprint, s2.schema_fingerprint);
    }
}
