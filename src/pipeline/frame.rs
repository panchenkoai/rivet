//! The batch runner's opening frame — one seam instead of eight transcriptions.
//!
//! Every batch commit loop used to hand-write the same preamble: create the
//! destination, run the cross-shape manifest guard, derive the format's file
//! extension. Copied, the preamble is exactly where the runner-bypass class
//! breeds: the two chunk-checkpoint runners shipped WITHOUT the manifest-mode
//! guard (their own comment records it), the same way the drift gate and the
//! Form-B checksum harvest each skipped runners before — a per-export step
//! that every runner must remember is a step some runner forgets.
//!
//! [`RunnerFrame::open`] is the only way a batch runner obtains its
//! destination (enforced by the `runner_frame_gate` offline lint, not by
//! convention), so the guard cannot be skipped by construction — the same
//! trick `commit::record_part` pulls for the per-part tail.
//!
//! Deliberately NOT in the frame (grilled 2026-08-08, architecture review):
//! - **part-name stamps** — three schemes exist for three reasons (keyset
//!   keys parts off the stable `run_id` so a retry OVERWRITES its own attempt;
//!   single/mongo use a millisecond stamp for run-uniqueness; chunked names by
//!   chunk index). Forcing one scheme is a behavior change, not a deepening.
//! - **the epilogue** (fingerprint → checksum harvest → drift gate) — the
//!   parallel runners set the fingerprint through a `OnceLock` for real
//!   concurrency reasons; unifying that is the follow-up recorded in the
//!   review, gated on making `RunSummary::schema_fingerprint` private.

use crate::destination::{self, Destination};
use crate::error::Result;
use crate::plan::ResolvedRunPlan;

/// The opened frame: the destination (already guarded) and the format's file
/// extension. Construct via [`RunnerFrame::open`]; there is no other door.
pub(crate) struct RunnerFrame {
    pub dest: Box<dyn Destination>,
    pub ext: String,
}

impl RunnerFrame {
    /// Create the destination, refuse a cross-shape prefix (a batch manifest
    /// must never clobber a CDC export's audit trail — finding #44), and
    /// derive the part-file extension. Fails BEFORE the first part.
    pub(crate) fn open(plan: &ResolvedRunPlan) -> Result<Self> {
        // open == open_unguarded + the run-start cross-shape guard; sharing the
        // body keeps the two from drifting (roast 2026-08-09). The guard runs
        // before the first part either way — nothing happens between build and
        // guard that a manifest read could observe.
        let frame = Self::open_unguarded(plan)?;
        crate::manifest::guard_manifest_mode(frame.dest.as_ref(), "batch")?;
        Ok(frame)
    }

    /// [`RunnerFrame::open`] for runners that share one destination across
    /// worker threads (parallel keyset / chunked / mongo).
    pub(crate) fn open_shared(
        plan: &ResolvedRunPlan,
    ) -> Result<(std::sync::Arc<Box<dyn Destination>>, String)> {
        let f = Self::open(plan)?;
        Ok((std::sync::Arc::new(f.dest), f.ext))
    }

    /// A destination + extension for a PER-ITERATION writer (each chunk / each
    /// worker), where the cross-shape guard has ALREADY fired at run start.
    ///
    /// The guard is a run-start concern: it reads the prefix's existing
    /// `manifest.json` to refuse a cross-shape overwrite. Re-running it on every
    /// chunk is one remote GET per chunk for an answer that cannot change mid-run
    /// — a 3000-chunk GCS export would pay 3000 serialized GETs (bug-hunt
    /// 2026-08-08). The first `RunnerFrame::open` conversion over-applied the
    /// guard to the sequential-checkpoint per-chunk writer, which on `main` had
    /// created its destination WITHOUT a guard; this restores that, through the
    /// frame so the lint stays satisfied.
    ///
    /// SAFE only because every module that calls this ALSO opens a GUARDED
    /// frame at run start — pinned by `unguarded_callers_also_guard_at_run_start`
    /// (a bare open_unguarded in a module with no guarded open is the exact
    /// mistake this restore was undoing, so the lint refuses it).
    pub(crate) fn open_unguarded(plan: &ResolvedRunPlan) -> Result<Self> {
        let dest = destination::create_destination(&plan.destination)?;
        let ext = crate::format::create_format(
            plan.format,
            plan.compression,
            plan.compression_level,
            None,
        )
        .file_extension()
        .to_string();
        Ok(Self { dest, ext })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_plan(dir: &std::path::Path) -> ResolvedRunPlan {
        use crate::config::{DestinationConfig, DestinationType, SourceConfig, SourceType};
        ResolvedRunPlan {
            bytes_read: Default::default(),
            export_name: "frame_probe".into(),
            source_table: None,
            base_query: "SELECT 1".into(),
            is_split_unit: false,
            strategy: crate::plan::ExtractionStrategy::Snapshot,
            format: crate::config::FormatType::Parquet,
            compression: crate::config::CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: Default::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some(dir.to_string_lossy().into_owned()),
                ..Default::default()
            },
            quality: None,
            tuning: crate::tuning::SourceTuning::from_config(None),
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
                mongo: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    /// The invariant open_unguarded's safety rests on: any module that opens an
    /// UNGUARDED frame must ALSO open a guarded one (its run-start guard). A
    /// module that only ever calls open_unguarded has no cross-shape guard at
    /// all — the very hole the sequential-checkpoint restore was closing, so it
    /// must never reappear.
    #[test]
    fn unguarded_callers_also_guard_at_run_start() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline");
        let mut offenders = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).unwrap() {
                let path = e.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|x| x.to_str()) != Some("rs") {
                    continue;
                }
                let t = std::fs::read_to_string(&path).unwrap();
                // frame.rs defines both; skip the definition site.
                if path.ends_with("frame.rs") {
                    continue;
                }
                if t.contains("open_unguarded(")
                    && !(t.contains("RunnerFrame::open(") || t.contains("open_shared("))
                {
                    offenders.push(path.display().to_string());
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "open_unguarded without a run-start guarded open in the same module — \
             the per-iteration writer would have NO cross-shape guard:\n{}",
            offenders.join("\n")
        );
    }

    /// The frame's whole reason to exist: a destination obtained through it
    /// has ALREADY passed the cross-shape guard. A CDC manifest on the prefix
    /// must refuse the frame — before any part could be written.
    #[test]
    fn open_refuses_a_cdc_prefix_before_any_part() {
        let dir = tempfile::tempdir().unwrap();
        // Plant a CDC-mode manifest where the batch export wants to write.
        let manifest = serde_json::json!({
            "manifest_version": 1,
            "export_name": "frame_probe",
            "mode": "cdc",
            "run_id": "r1",
            "status": "success",
            "parts": []
        });
        std::fs::write(
            dir.path().join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let err = RunnerFrame::open(&local_plan(dir.path()))
            .err()
            .expect("a batch frame over a CDC prefix must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("cdc") || msg.contains("CDC"),
            "the refusal must name the cross-shape conflict: {msg}"
        );
    }

    /// …and a clean prefix opens, with the extension matching the format.
    #[test]
    fn open_yields_a_guarded_destination_and_the_formats_extension() {
        let dir = tempfile::tempdir().unwrap();
        let f = RunnerFrame::open(&local_plan(dir.path())).expect("clean prefix opens");
        assert_eq!(f.ext, "parquet");
        // The destination is real and WRITABLE — the frame did not swap in a
        // stub while looking guarded. Write one byte through it and read it
        // back off the prefix.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"x").unwrap();
        f.dest
            .write(tmp.path(), "probe.bin")
            .expect("frame's destination writes");
        assert_eq!(std::fs::read(dir.path().join("probe.bin")).unwrap(), b"x");
    }
}
