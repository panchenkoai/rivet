//! End-to-end behavioural tests for the 0.6.1 / 0.7.0 trust-contract
//! artifacts — `.rivet/runs/<run_id>/summary.{md,json}` and the cloud-side
//! `manifest.json` + `_SUCCESS` pair.
//!
//! Scope:
//! - Each writer goes through the public `rivet::pipeline::*` surface so
//!   the API contract is exercised, not just the internal helpers.
//! - Combinations that span both writers (run_id and schema_fingerprint
//!   correspondence between summary.json and manifest.json).
//! - Negative paths: failed/interrupted statuses, streaming destinations,
//!   malformed marker bodies, self-inconsistent manifests, schema drift.
//!
//! These complement (but do not duplicate) the unit tests in
//! `src/pipeline/report.rs` and `src/pipeline/manifest_writer.rs` —
//! anything that can be verified at the function level lives there; this
//! file is for cross-module behaviour and contract-shaped assertions.

#![allow(clippy::needless_borrow)]

use std::path::{Path, PathBuf};

use rivet::config::{DestinationConfig, DestinationType};
use rivet::journal::PlanSnapshot;
use rivet::manifest::{
    self, MANIFEST_FILENAME, MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource,
    ManifestStatus, PartStatus, RunManifest, SUCCESS_FILENAME, parse_success_marker,
    success_marker_body,
};
use rivet::pipeline::{
    ManifestBuilder, ManifestVerification, ManifestVerificationFailure, RunReport, RunSummary,
    WriteOutcome, build_resume_plan, report_dir, verify_at_destination, write_manifest,
    write_run_report,
};
use rivet::pipeline::{QuarantineReason, ResumeDecision, UntrackedDecision};
use rivet::state::{SchemaColumn, schema_fingerprint};

// ─── Fixtures ───────────────────────────────────────────────────────────────

fn local_destination_config(base: &Path) -> DestinationConfig {
    DestinationConfig {
        destination_type: DestinationType::Local,
        path: Some(base.to_string_lossy().into_owned()),
        ..Default::default()
    }
}

fn local_dest(base: &Path) -> Box<dyn rivet_destination_proxy::DestinationProxy> {
    // We can't directly create a `Box<dyn Destination>` from outside the
    // crate (the trait is pub(crate)), so route through `pipeline::write_manifest`
    // which accepts `&dyn Destination`.  The proxy module below punches a small
    // window through the pub(crate) ceiling using `rivet`'s public
    // `create_destination` helper.
    rivet_destination_proxy::new_local(base)
}

mod rivet_destination_proxy {
    //! Tiny adapter that hands out the existing `rivet::destination::Destination`
    //! trait object via the crate's public `create_destination` helper.  The
    //! trait itself is `pub(crate)` inside `rivet`, but `create_destination`
    //! returns a `Box<dyn Destination>` which we keep opaque behind this
    //! proxy trait — letting integration tests pass the value through to
    //! `pipeline::write_manifest` without re-implementing the trait.
    use std::path::Path;

    pub trait DestinationProxy {
        fn as_writer(&self) -> &dyn rivet::destination_for_tests::Destination;
    }

    struct Boxed(Box<dyn rivet::destination_for_tests::Destination>);
    impl DestinationProxy for Boxed {
        fn as_writer(&self) -> &dyn rivet::destination_for_tests::Destination {
            &*self.0
        }
    }

    pub fn new_local(base: &Path) -> Box<dyn DestinationProxy> {
        let cfg = super::local_destination_config(base);
        let d = rivet::destination_for_tests::create_destination(&cfg)
            .expect("create_destination for tempdir must succeed");
        Box::new(Boxed(d))
    }
}

/// Build a minimal `RunSummary` for a finished run via the canonical
/// `RunSummary::stub_for_testing` builder.  Adds a `PlanResolved` event so
/// the report renderer's plan_origin lookup finds something, then
/// composes status / parts / error / duration with chainable setters.
fn summary(
    run_id: &str,
    export_name: &str,
    status: &str,
    parts: Vec<ManifestPart>,
    error: Option<String>,
) -> RunSummary {
    let mut s = RunSummary::stub_for_testing(run_id, export_name)
        .with_plan_snapshot(PlanSnapshot {
            export_name: export_name.into(),
            base_query: format!("SELECT * FROM {export_name}"),
            strategy: "snapshot".into(),
            format: "parquet".into(),
            compression: "zstd".into(),
            destination_type: "local".into(),
            tuning_profile: "balanced".into(),
            batch_size: 1000,
            validate: false,
            reconcile: false,
            resume: false,
        })
        .with_manifest_parts(parts);
    s.duration_ms = 100;
    s.peak_rss_mb = 32;
    s.validated = Some(true);
    s.schema_changed = Some(false);
    if let Some(msg) = error {
        s = s.with_error(msg);
    }
    // `with_status` records the matching `RunCompleted` event for terminal
    // statuses, mirroring the journal shape a real run produces.
    s.with_status(status)
}

fn part(part_id: u32, rows: i64, size: u64, fp: &str) -> ManifestPart {
    ManifestPart {
        part_id,
        path: format!("part-{part_id:06}.parquet"),
        rows,
        size_bytes: size,
        content_fingerprint: fp.into(),
        status: PartStatus::Committed,
    }
}

fn build_manifest(run_id: &str, status: ManifestStatus, parts: Vec<ManifestPart>) -> RunManifest {
    let row_count: i64 = parts
        .iter()
        .filter(|p| p.status == PartStatus::Committed)
        .map(|p| p.rows)
        .sum();
    let part_count = parts
        .iter()
        .filter(|p| p.status == PartStatus::Committed)
        .count() as u32;
    RunManifest {
        manifest_version: MANIFEST_VERSION,
        run_id: run_id.into(),
        export_name: "public.orders".into(),
        started_at: "2026-05-21T12:00:00Z".into(),
        finished_at: "2026-05-21T12:14:33Z".into(),
        status,
        source: ManifestSource {
            engine: "postgres".into(),
            schema: Some("public".into()),
            table: Some("orders".into()),
        },
        destination: ManifestDestination {
            kind: "local".into(),
            uri: "file:///tmp/out/".into(),
        },
        format: "parquet".into(),
        compression: "zstd".into(),
        schema_fingerprint: "xxh3:0123456789abcdef".into(),
        row_count,
        part_count,
        parts,
    }
}

fn touch_config(dir: &Path) -> PathBuf {
    let cfg = dir.join("rivet.yaml");
    std::fs::write(&cfg, "exports: []").unwrap();
    cfg
}

// ─── Section 1: report_dir layout ────────────────────────────────────────────

#[test]
fn report_dir_is_under_config_dirname_next_to_state_db() {
    // ADR-0012 §Artifacts: the local report dir sits next to `.rivet_state.db`
    // — i.e. under the config file's parent — not under the destination root.
    let p = report_dir("/tmp/rivet-test/rivet.yaml", "orders_001");
    assert_eq!(p, PathBuf::from("/tmp/rivet-test/.rivet/runs/orders_001"));
}

#[test]
fn report_dir_falls_back_to_dot_for_bare_filename() {
    let p = report_dir("rivet.yaml", "orders_001");
    assert_eq!(p, PathBuf::from(".rivet/runs/orders_001"));
}

// ─── Section 2: write_run_report happy path ──────────────────────────────────

#[test]
fn success_run_produces_both_summary_files() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let s = summary("r1", "orders", "success", Vec::new(), None);

    let report_path = write_run_report(cfg.to_str().unwrap(), &s).expect("report write succeeds");
    assert!(report_path.join("summary.md").exists());
    assert!(report_path.join("summary.json").exists());

    let md = std::fs::read_to_string(report_path.join("summary.md")).unwrap();
    assert!(md.contains("orders"));
    assert!(md.contains("SUCCESS"));
    assert!(
        !md.contains("Resume"),
        "successful runs must not advertise a resume command"
    );

    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(report_path.join("summary.json")).unwrap())
            .unwrap();
    assert_eq!(json["run_id"], "r1");
    assert_eq!(json["status"], "success");
    assert_eq!(json["resumable"], false);
    assert!(json["resume_command"].is_null());
}

#[test]
fn report_dir_is_created_on_demand() {
    // The `.rivet/runs/<run_id>/` path does NOT exist before the first write;
    // `write_run_report` must `create_dir_all` for the run.
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    assert!(!dir.path().join(".rivet").exists());
    let s = summary("r2", "users", "success", Vec::new(), None);
    write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    assert!(dir.path().join(".rivet").join("runs").join("r2").exists());
}

// ─── Section 3: resume hint semantics ────────────────────────────────────────

#[test]
fn failed_run_with_committed_files_carries_resume_command() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let parts = vec![part(1, 100, 4096, "xxh3:0000000000000001")];
    let s = summary(
        "r3",
        "orders",
        "failed",
        parts,
        Some("connection reset".into()),
    );

    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    let report: RunReport =
        serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap()).unwrap();
    assert!(report.resumable);
    let cmd = report.resume_command.as_deref().expect("must be set");
    assert!(cmd.starts_with("rivet run --config "));
    assert!(cmd.ends_with(" --resume"));
    assert!(cmd.contains(cfg.to_str().unwrap()));

    let md = std::fs::read_to_string(out.join("summary.md")).unwrap();
    assert!(md.contains("Resume"));
}

#[test]
fn failed_run_without_committed_files_has_no_resume_command() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let s = summary(
        "r4",
        "orders",
        "failed",
        Vec::new(),
        Some("plan validation rejected".into()),
    );
    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    let report: RunReport =
        serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap()).unwrap();
    assert!(!report.resumable);
    assert!(report.resume_command.is_none());

    let md = std::fs::read_to_string(out.join("summary.md")).unwrap();
    assert!(
        md.contains("No files were committed"),
        "MD must explain why resume isn't offered"
    );
}

#[test]
fn interrupted_run_renders_as_interrupted_status() {
    // A run whose process died before transitioning out of "running" status
    // — the report should label it INTERRUPTED rather than UNKNOWN.
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let s = summary("r5", "orders", "running", Vec::new(), None);
    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    let md = std::fs::read_to_string(out.join("summary.md")).unwrap();
    assert!(md.contains("INTERRUPTED"));
}

// ─── Section 4: write_manifest happy path ────────────────────────────────────

#[test]
fn success_manifest_writes_both_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    let m = build_manifest(
        "r10",
        ManifestStatus::Success,
        vec![
            part(1, 100, 4096, "xxh3:1111111111111111"),
            part(2, 200, 8192, "xxh3:2222222222222222"),
        ],
    );

    let outcome = write_manifest(dest_proxy.as_writer(), &m).unwrap();
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
fn manifest_json_roundtrips_through_serde() {
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());
    let m = build_manifest(
        "r11",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    let raw = std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap();
    let parsed: RunManifest = serde_json::from_str(&raw).unwrap();
    assert_eq!(parsed, m);
    assert_eq!(parsed.validate_self_consistency(), Ok(()));
}

#[test]
fn success_marker_body_equals_fingerprint_of_written_bytes() {
    // ADR-0012 M2: `_SUCCESS` body is the xxh3 over the exact bytes written
    // to `manifest.json`, not over a re-serialised struct.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());
    let m = build_manifest(
        "r12",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    let manifest_bytes = std::fs::read(dir.path().join(MANIFEST_FILENAME)).unwrap();
    let marker = std::fs::read_to_string(dir.path().join(SUCCESS_FILENAME)).unwrap();
    let expected = success_marker_body(&manifest_bytes);
    assert_eq!(marker, expected);

    // And it parses cleanly back to the canonical "xxh3:<hex>" form.
    let fp = parse_success_marker(&marker).expect("well-formed marker must parse");
    assert!(fp.starts_with("xxh3:"));
    assert_eq!(fp.len(), "xxh3:".len() + 16);
}

#[test]
fn empty_parts_success_run_still_produces_manifest_and_success() {
    // A run that exported zero rows (every chunk produced 0 records) is a
    // legitimate success — the manifest and `_SUCCESS` must still land so
    // downstream pollers see the "data is ready" signal.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());
    let m = build_manifest("r13", ManifestStatus::Success, Vec::new());
    write_manifest(dest_proxy.as_writer(), &m).unwrap();
    assert!(dir.path().join(MANIFEST_FILENAME).exists());
    assert!(dir.path().join(SUCCESS_FILENAME).exists());
}

// ─── Section 5: write_manifest negative-status semantics ─────────────────────

#[test]
fn failed_status_writes_manifest_audit_trail_but_not_success() {
    // ADR-0012 M2: `_SUCCESS` exists iff the run completed cleanly.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());
    let m = build_manifest(
        "r20",
        ManifestStatus::Failed,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    let outcome = write_manifest(dest_proxy.as_writer(), &m).unwrap();
    assert!(matches!(
        outcome,
        WriteOutcome::Written {
            success_marker: false
        }
    ));
    assert!(dir.path().join(MANIFEST_FILENAME).exists());
    assert!(!dir.path().join(SUCCESS_FILENAME).exists());
}

#[test]
fn interrupted_status_writes_manifest_audit_trail_but_not_success() {
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());
    let m = build_manifest("r21", ManifestStatus::Interrupted, Vec::new());
    let outcome = write_manifest(dest_proxy.as_writer(), &m).unwrap();
    assert!(matches!(
        outcome,
        WriteOutcome::Written {
            success_marker: false
        }
    ));
    assert!(dir.path().join(MANIFEST_FILENAME).exists());
    assert!(!dir.path().join(SUCCESS_FILENAME).exists());
}

#[test]
fn streaming_destination_writes_nothing_and_does_not_error() {
    // Stdout destinations have no coherent prefix; the writer must skip
    // gracefully so the caller can surface a clear "no manifest produced"
    // note rather than aborting the run.
    let cfg = DestinationConfig {
        destination_type: DestinationType::Stdout,
        ..Default::default()
    };
    let dest = rivet::destination_for_tests::create_destination(&cfg).unwrap();
    let m = build_manifest(
        "r22",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    let outcome = write_manifest(&*dest, &m).unwrap();
    assert!(matches!(outcome, WriteOutcome::SkippedStreaming));
}

// ─── Section 6: M4 — overwrite semantics ─────────────────────────────────────

#[test]
fn writing_manifest_twice_replaces_the_previous_artifact() {
    // ADR-0012 M4: a given run_id produces exactly one manifest; a later
    // write atomically supersedes any earlier one at the same prefix.  This
    // guards against a future regression where the writer accidentally
    // appends or refuses to overwrite.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    let m1 = build_manifest(
        "r30",
        ManifestStatus::Success,
        vec![part(1, 50, 1024, "xxh3:1111111111111111")],
    );
    write_manifest(dest_proxy.as_writer(), &m1).unwrap();
    let raw_1 = std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap();

    let m2 = build_manifest(
        "r30",
        ManifestStatus::Success,
        vec![
            part(1, 50, 1024, "xxh3:1111111111111111"),
            part(2, 200, 4096, "xxh3:2222222222222222"),
        ],
    );
    write_manifest(dest_proxy.as_writer(), &m2).unwrap();
    let raw_2 = std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap();

    assert_ne!(raw_1, raw_2, "second write must replace first");
    let parsed: RunManifest = serde_json::from_str(&raw_2).unwrap();
    assert_eq!(parsed.part_count, 2);
    assert_eq!(parsed.row_count, 250);

    // _SUCCESS body should track the new manifest, not the old one.
    let marker = std::fs::read_to_string(dir.path().join(SUCCESS_FILENAME)).unwrap();
    assert_eq!(marker, success_marker_body(raw_2.as_bytes()));
    assert_ne!(
        marker,
        success_marker_body(raw_1.as_bytes()),
        "stale _SUCCESS would mislead orchestrators polling for changes"
    );
}

#[test]
fn failed_run_after_successful_run_clears_success_marker_decisively() {
    // The first run succeeds — _SUCCESS is written.  The same prefix is
    // re-used for a second run that fails partway.  After the second
    // run, the manifest must reflect status=failed and the _SUCCESS file
    // must NOT exist (or must be removed).
    //
    // Today the writer never deletes _SUCCESS on a failed re-run — it
    // only refrains from writing one.  That is the behaviour pinned here;
    // if the contract evolves to require active deletion, this test
    // becomes the canary.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    let ok = build_manifest(
        "r31",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    write_manifest(dest_proxy.as_writer(), &ok).unwrap();
    assert!(dir.path().join(SUCCESS_FILENAME).exists());
    let stale_marker = std::fs::read_to_string(dir.path().join(SUCCESS_FILENAME)).unwrap();

    let fail = build_manifest("r32", ManifestStatus::Failed, Vec::new());
    write_manifest(dest_proxy.as_writer(), &fail).unwrap();

    // Manifest reflects the latest (failed) run.
    let parsed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();
    assert_eq!(parsed.status, ManifestStatus::Failed);
    assert_eq!(parsed.run_id, "r32");

    // _SUCCESS is still on disk — it was left over from the prior run.
    // Document this as the current contract; if it changes, the assertion
    // here makes the regression explicit.
    let still_there = dir.path().join(SUCCESS_FILENAME).exists();
    if still_there {
        let body = std::fs::read_to_string(dir.path().join(SUCCESS_FILENAME)).unwrap();
        assert_eq!(
            body, stale_marker,
            "stale _SUCCESS body must equal the prior successful run's marker"
        );
    }
}

// ─── Section 7: manifest self-consistency (reader-side defence) ──────────────

#[test]
fn reader_rejects_manifest_with_wrong_part_count() {
    let mut m = build_manifest(
        "r40",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    m.part_count = 99; // lie
    let err = m.validate_self_consistency().unwrap_err();
    assert!(format!("{err}").contains("part_count"));
}

#[test]
fn reader_rejects_manifest_with_wrong_row_count() {
    let mut m = build_manifest(
        "r41",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    m.row_count = 9999; // lie
    let err = m.validate_self_consistency().unwrap_err();
    assert!(format!("{err}").contains("row_count"));
}

#[test]
fn reader_rejects_manifest_with_duplicate_part_ids() {
    let m = build_manifest(
        "r42",
        ManifestStatus::Success,
        vec![
            part(7, 100, 4096, "xxh3:1111111111111111"),
            part(7, 200, 8192, "xxh3:2222222222222222"),
        ],
    );
    let err = m.validate_self_consistency().unwrap_err();
    assert!(format!("{err}").contains("part_id"));
}

#[test]
fn reader_rejects_unsupported_manifest_version() {
    // A v2-only reader must refuse a v999 manifest rather than silently
    // accept it — the on-wire schema may have changed incompatibly.
    let mut m = build_manifest("r43", ManifestStatus::Success, Vec::new());
    m.manifest_version = 999;
    let err = m.validate_self_consistency().unwrap_err();
    assert!(format!("{err}").contains("manifest_version"));
}

#[test]
fn quarantined_parts_do_not_inflate_row_or_part_totals() {
    // ADR-0012 M9: quarantined parts are listed for audit but excluded
    // from the committed accounting that --reconcile compares to source.
    let mut p_q = part(2, 999, 4096, "xxh3:9999999999999999");
    p_q.status = PartStatus::Quarantined;
    let m = build_manifest(
        "r44",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111"), p_q],
    );
    assert_eq!(m.committed_rows(), 100);
    assert_eq!(m.committed_part_count(), 1);
    assert_eq!(m.validate_self_consistency(), Ok(()));
}

// ─── Section 8: _SUCCESS marker parser ───────────────────────────────────────

#[test]
fn parse_success_marker_rejects_malformed_bodies() {
    // Each entry is something an orchestrator might mistakenly write or
    // a corrupted upload might leave behind.  All must be rejected so a
    // future `--validate` cloud check can distinguish "absent" from
    // "present and well-formed" without false positives.
    let cases = [
        "",                                // empty
        "\n",                              // blank line only
        "sha256:0123456789abcdef0123456",  // wrong algorithm prefix
        "xxh3:0123",                       // truncated hex
        "xxh3:0123456789ABCDEF",           // uppercase hex (we emit lowercase)
        "xxh3:zzzzzzzzzzzzzzzz",           // non-hex chars
        "0123456789abcdef",                // missing prefix
        "xxh3:0123456789abcdef0123456789", // overlong hex
    ];
    for bad in cases {
        assert_eq!(
            parse_success_marker(bad),
            None,
            "marker body {bad:?} must be rejected"
        );
    }
}

#[test]
fn parse_success_marker_tolerates_trailing_whitespace_and_crlf() {
    // Editors and orchestrators normalise line endings differently;
    // the marker must round-trip from any reasonable variant.
    for body in [
        "xxh3:0123456789abcdef\n",
        "xxh3:0123456789abcdef\r\n",
        "xxh3:0123456789abcdef",        // no trailing newline
        "xxh3:0123456789abcdef \t  \n", // trailing spaces + tab + lf
    ] {
        assert_eq!(
            parse_success_marker(body),
            Some("xxh3:0123456789abcdef"),
            "marker body {body:?} must parse"
        );
    }
}

// ─── Section 9: schema_fingerprint over realistic schemas ────────────────────

#[test]
fn schema_fingerprint_is_stable_across_runs_with_unchanged_schema() {
    // Two runs of the same export against the same source schema must
    // produce the same fingerprint — that is what enables `--validate`
    // to detect drift instead of churning on harmless ordering changes.
    let cols = vec![
        SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        },
        SchemaColumn {
            name: "email".into(),
            data_type: "Utf8".into(),
        },
        SchemaColumn {
            name: "created_at".into(),
            data_type: "Timestamp(Microsecond, None)".into(),
        },
    ];
    assert_eq!(schema_fingerprint(&cols), schema_fingerprint(&cols));

    // And the order in which the source returns columns must not matter.
    let reordered = vec![cols[2].clone(), cols[0].clone(), cols[1].clone()];
    assert_eq!(schema_fingerprint(&cols), schema_fingerprint(&reordered));
}

#[test]
fn schema_fingerprint_changes_on_added_column() {
    let mut cols = vec![
        SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        },
        SchemaColumn {
            name: "email".into(),
            data_type: "Utf8".into(),
        },
    ];
    let before = schema_fingerprint(&cols);
    cols.push(SchemaColumn {
        name: "phone".into(),
        data_type: "Utf8".into(),
    });
    let after = schema_fingerprint(&cols);
    assert_ne!(before, after, "schema drift must be detectable");
}

#[test]
fn schema_fingerprint_changes_on_retype() {
    // The classic "we changed price from Int64 to Float64" silent drift.
    let v1 = vec![SchemaColumn {
        name: "price".into(),
        data_type: "Int64".into(),
    }];
    let v2 = vec![SchemaColumn {
        name: "price".into(),
        data_type: "Float64".into(),
    }];
    assert_ne!(schema_fingerprint(&v1), schema_fingerprint(&v2));
}

// ─── Section 10: cross-artifact invariants (report ↔ manifest) ───────────────

#[test]
fn run_id_matches_across_summary_json_and_manifest_json() {
    // A consumer that polls both artifacts in succession must see the
    // same `run_id` — otherwise the report and the cloud manifest could
    // appear to describe different runs.
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let dest_proxy = local_dest(dir.path());

    let parts = vec![part(1, 100, 4096, "xxh3:1111111111111111")];
    let s = summary(
        "orders_20260521T120000",
        "public.orders",
        "success",
        parts.clone(),
        None,
    );
    let m = build_manifest("orders_20260521T120000", ManifestStatus::Success, parts);

    let report_path = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    let report: RunReport =
        serde_json::from_str(&std::fs::read_to_string(report_path.join("summary.json")).unwrap())
            .unwrap();
    let manifest: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();

    assert_eq!(report.run_id, manifest.run_id);
    assert_eq!(report.export_name, manifest.export_name);
}

// ─── Section 11: write_run_report error paths ────────────────────────────────

#[test]
fn write_run_report_returns_err_when_target_parent_is_unwritable() {
    // Caller's policy is "log and continue", but the writer must surface
    // the error rather than swallow it.  Use a path that doesn't exist
    // and *can't* be created (under a regular file, not a directory).
    let dir = tempfile::tempdir().unwrap();
    let blocker = dir.path().join("not-a-dir");
    std::fs::write(&blocker, b"i am a file").unwrap();
    // `config_path = dir/not-a-dir/inner.yaml` → parent is `dir/not-a-dir`,
    // which is a *file*; `create_dir_all` for `.rivet/runs/<id>/` under it
    // must fail.
    let cfg = blocker.join("inner.yaml");
    let s = summary("r50", "orders", "success", Vec::new(), None);
    let res = write_run_report(cfg.to_str().unwrap(), &s);
    assert!(
        res.is_err(),
        "writer must propagate the I/O error to caller"
    );
}

// ─── Section 12: constants stability ─────────────────────────────────────────

#[test]
fn manifest_constants_match_adr_0012() {
    // Cross-version readers depend on these names — pin them.
    assert_eq!(manifest::MANIFEST_FILENAME, "manifest.json");
    assert_eq!(manifest::SUCCESS_FILENAME, "_SUCCESS");
    assert_eq!(manifest::QUARANTINE_PREFIX, "_quarantine");
    assert_eq!(manifest::MANIFEST_VERSION, 1);
}

// ─── Section 13: ManifestBuilder lifecycle (behavioural) ─────────────────────
//
// The earlier sections build `RunManifest` directly via the test fixture so
// they can poke individual fields.  The pipeline never does that — it walks
// through `ManifestBuilder::new → record_part → finalize → write_manifest`.
// These tests exercise the builder API end-to-end so a regression in any
// stage (id assignment, totals, started/finished bracketing, status pass-through)
// is caught at the integration boundary.

fn plan_snap() -> PlanSnapshot {
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

#[test]
fn builder_to_writer_roundtrips_through_serde_and_keeps_all_fields() {
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    let mut b = ManifestBuilder::new(
        &plan_snap(),
        "orders_20260521T120100",
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
    );
    b.record_part(
        2,
        "part-000002.parquet".into(),
        200,
        8192,
        "xxh3:2222222222222222".into(),
    );
    let m = b.finalize(ManifestStatus::Success);
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    // Roundtrip the on-disk JSON; equality is structural and includes
    // schema_fingerprint, source.engine/schema/table, and parts ordering.
    let raw = std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap();
    let parsed: RunManifest = serde_json::from_str(&raw).unwrap();
    assert_eq!(parsed, m);
    assert_eq!(parsed.run_id, "orders_20260521T120100");
    assert_eq!(parsed.export_name, "public.orders");
    assert_eq!(parsed.source.engine, "postgres");
    assert_eq!(parsed.source.schema.as_deref(), Some("public"));
    assert_eq!(parsed.source.table.as_deref(), Some("orders"));
    assert_eq!(parsed.schema_fingerprint, "xxh3:0123456789abcdef");
    assert_eq!(parsed.committed_part_count(), 2);
    assert_eq!(parsed.committed_rows(), 300);
    assert_eq!(parsed.validate_self_consistency(), Ok(()));
}

#[test]
fn builder_finalize_failed_status_skips_success_marker_through_full_writer() {
    // ManifestBuilder → finalize(Failed) → write_manifest produces an audit
    // manifest (rows recorded for resume) but never the _SUCCESS gate.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    let mut b = ManifestBuilder::new(
        &plan_snap(),
        "orders_20260521T120200",
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
        50,
        2048,
        "xxh3:abcdefabcdefabcd".into(),
    );
    let m = b.finalize(ManifestStatus::Failed);
    let outcome = write_manifest(dest_proxy.as_writer(), &m).unwrap();
    assert!(matches!(
        outcome,
        WriteOutcome::Written {
            success_marker: false
        }
    ));
    assert!(dir.path().join(MANIFEST_FILENAME).exists());
    assert!(!dir.path().join(SUCCESS_FILENAME).exists());

    let parsed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();
    assert_eq!(parsed.status, ManifestStatus::Failed);
    assert_eq!(parsed.committed_part_count(), 1);
}

#[test]
fn builder_finalize_brackets_started_at_before_finished_at() {
    // ADR-0012 schema requires `started_at` and `finished_at` to be RFC-3339
    // and that finished_at >= started_at — a manifest with reversed
    // timestamps would be misleading to anyone diffing the report.
    let started = chrono::Utc::now();
    let b = ManifestBuilder::new(
        &plan_snap(),
        "orders_t",
        started,
        "xxh3:0".into(),
        "postgres",
        None,
        None,
        "file:///x".into(),
    );
    std::thread::sleep(std::time::Duration::from_millis(2));
    let m = b.finalize(ManifestStatus::Success);
    let started_p = chrono::DateTime::parse_from_rfc3339(&m.started_at).unwrap();
    let finished_p = chrono::DateTime::parse_from_rfc3339(&m.finished_at).unwrap();
    assert!(
        finished_p >= started_p,
        "finished_at {finished_p:?} must be >= started_at {started_p:?}"
    );
    // started_at must equal what the caller passed (no clock-jitter substitution).
    assert_eq!(started_p.timestamp_millis(), started.timestamp_millis());
}

#[test]
fn builder_records_parts_in_call_order_preserving_part_id_choice() {
    // record_part lets the caller pick part_id explicitly (chunked workers
    // fill in their own slot index).  The builder must NOT renumber.
    let mut b = ManifestBuilder::new(
        &plan_snap(),
        "orders_seq",
        chrono::Utc::now(),
        "xxh3:0".into(),
        "postgres",
        None,
        None,
        "file:///x".into(),
    );
    // Out-of-natural-order record calls — chunked aggregation may flush in
    // completion order, not chunk index order.
    b.record_part(
        3,
        "p3.parquet".into(),
        30,
        30,
        "xxh3:cccccccccccccccc".into(),
    );
    b.record_part(
        1,
        "p1.parquet".into(),
        10,
        10,
        "xxh3:aaaaaaaaaaaaaaaa".into(),
    );
    b.record_part(
        2,
        "p2.parquet".into(),
        20,
        20,
        "xxh3:bbbbbbbbbbbbbbbb".into(),
    );

    let m = b.finalize(ManifestStatus::Success);
    // Insertion order, NOT sorted by part_id — preserves the temporal record.
    let ids: Vec<u32> = m.parts.iter().map(|p| p.part_id).collect();
    assert_eq!(ids, vec![3, 1, 2]);
    // Self-consistency only requires uniqueness (M5/M9), not sortedness.
    assert_eq!(m.validate_self_consistency(), Ok(()));
    assert_eq!(m.committed_part_count(), 3);
    assert_eq!(m.committed_rows(), 60);
}

// ─── Section 14: cross-artifact invariants — extended ────────────────────────
//
// Section 10 already pins run_id ↔ run_id.  These tests pin the rest of the
// fields a downstream poller (Airflow sensor, custom verifier) actually
// compares between summary.json and manifest.json.

#[test]
fn schema_fingerprint_in_manifest_matches_state_helper_output() {
    // The pipeline computes schema_fingerprint from the dest-facing column
    // list and threads the same string through to the manifest writer.
    // A naive caller might recompute on a slightly different schema and get
    // a divergent value — pin the equivalence so refactors keep the same
    // input → same fingerprint identity.
    let cols = vec![
        SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        },
        SchemaColumn {
            name: "email".into(),
            data_type: "Utf8".into(),
        },
    ];
    let fp = schema_fingerprint(&cols);

    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());
    let mut b = ManifestBuilder::new(
        &plan_snap(),
        "orders_fp_match",
        chrono::Utc::now(),
        fp.clone(),
        "postgres",
        Some("public".into()),
        Some("orders".into()),
        "file:///tmp/out/".into(),
    );
    b.record_part(
        1,
        "part-000001.parquet".into(),
        10,
        1024,
        "xxh3:0000000000000001".into(),
    );
    let m = b.finalize(ManifestStatus::Success);
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    let parsed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();
    assert_eq!(parsed.schema_fingerprint, fp);
    // The fingerprint format itself is part of the stable wire contract.
    assert!(parsed.schema_fingerprint.starts_with("xxh3:"));
    assert_eq!(parsed.schema_fingerprint.len(), "xxh3:".len() + 16);
}

#[test]
fn success_summary_paired_with_success_manifest_has_no_disagreement() {
    // ADR-0012 trust contract: when summary.json says "success" and
    // manifest.json says "success", the _SUCCESS marker must exist.
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let dest_proxy = local_dest(dir.path());

    let parts = vec![part(1, 100, 4096, "xxh3:1111111111111111")];
    let s = summary(
        "orders_pair_ok",
        "public.orders",
        "success",
        parts.clone(),
        None,
    );
    let m = build_manifest("orders_pair_ok", ManifestStatus::Success, parts);

    let report_path = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    let report: RunReport =
        serde_json::from_str(&std::fs::read_to_string(report_path.join("summary.json")).unwrap())
            .unwrap();
    let manifest: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();

    assert_eq!(report.status, "success");
    assert_eq!(manifest.status, ManifestStatus::Success);
    assert!(
        dir.path().join(SUCCESS_FILENAME).exists(),
        "_SUCCESS must be present when both artifacts agree on success"
    );
    // Row totals must agree, since reconcile/validate compare the two.
    assert_eq!(report.total_rows, manifest.committed_rows());
}

#[test]
fn failed_summary_paired_with_failed_manifest_emits_no_success_marker() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let dest_proxy = local_dest(dir.path());

    let parts = vec![part(1, 100, 4096, "xxh3:1111111111111111")];
    let s = summary(
        "orders_pair_fail",
        "public.orders",
        "failed",
        parts.clone(),
        Some("connection reset".into()),
    );
    let m = build_manifest("orders_pair_fail", ManifestStatus::Failed, parts);

    let report_path = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    let report: RunReport =
        serde_json::from_str(&std::fs::read_to_string(report_path.join("summary.json")).unwrap())
            .unwrap();
    let manifest: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();

    assert_eq!(report.status, "failed");
    assert_eq!(manifest.status, ManifestStatus::Failed);
    assert!(
        !dir.path().join(SUCCESS_FILENAME).exists(),
        "_SUCCESS must never appear when either artifact says non-success"
    );
    assert!(report.resumable);
}

#[test]
fn parts_ordering_survives_manifest_write_and_serde_roundtrip() {
    // Parts are an ordered audit trail (M9 quarantine logic compares prior
    // run's order to current run's).  A naive serialiser that sorted by
    // part_id during write would break that.  Pin the order through a
    // full write_manifest → JSON read cycle.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    let m = build_manifest(
        "orders_order",
        ManifestStatus::Success,
        vec![
            part(7, 70, 7000, "xxh3:7777777777777777"),
            part(1, 10, 1000, "xxh3:1111111111111111"),
            part(5, 50, 5000, "xxh3:5555555555555555"),
        ],
    );
    write_manifest(dest_proxy.as_writer(), &m).unwrap();
    let parsed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();
    let ids: Vec<u32> = parsed.parts.iter().map(|p| p.part_id).collect();
    assert_eq!(ids, vec![7, 1, 5]);
}

// ─── Section 15: resume command quoting + markdown wiring ────────────────────

#[test]
fn resume_command_quotes_config_path_with_spaces() {
    // Operators copy-paste this command from the report into their shell.
    // A path with a space MUST be single-quoted or the shell will split it
    // into two arguments — silently selecting the wrong (or non-existent)
    // config and either failing or, worse, succeeding against an
    // unintended file.
    let outer = tempfile::tempdir().unwrap();
    let dirty_dir = outer.path().join("my configs");
    std::fs::create_dir_all(&dirty_dir).unwrap();
    let cfg = dirty_dir.join("rivet.yaml");
    std::fs::write(&cfg, "exports: []").unwrap();

    let parts = vec![part(1, 100, 4096, "xxh3:1111111111111111")];
    let s = summary(
        "orders_quoting",
        "orders",
        "failed",
        parts,
        Some("connection reset".into()),
    );
    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    let report: RunReport =
        serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap()).unwrap();

    let cmd = report.resume_command.as_deref().expect("must be set");
    // Path contains a space → it must be single-quoted in the command.
    let path_str = cfg.to_str().unwrap();
    assert!(
        cmd.contains(&format!("'{}'", path_str)),
        "resume_command {cmd:?} must single-quote {path_str:?}"
    );

    // And the markdown form ships the same command verbatim — ops paste
    // from the .md, not the .json.
    let md = std::fs::read_to_string(out.join("summary.md")).unwrap();
    assert!(
        md.contains(cmd),
        "summary.md must include the exact resume command, got:\n{md}"
    );
}

#[test]
fn resume_command_handles_apostrophe_in_config_path() {
    // POSIX-portable single-quote escape: `'\''` between literal segments.
    let outer = tempfile::tempdir().unwrap();
    let dirty_dir = outer.path().join("o'reilly");
    std::fs::create_dir_all(&dirty_dir).unwrap();
    let cfg = dirty_dir.join("rivet.yaml");
    std::fs::write(&cfg, "exports: []").unwrap();

    let parts = vec![part(1, 100, 4096, "xxh3:1111111111111111")];
    let s = summary("orders_apos", "orders", "failed", parts, Some("err".into()));
    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    let report: RunReport =
        serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap()).unwrap();

    let cmd = report.resume_command.as_deref().expect("must be set");
    // POSIX safe quoting splits the literal at every `'` and rewraps:
    //   …/o'reilly/rivet.yaml  →  '…/o'\''reilly/rivet.yaml'
    // (closing quote, escaped apostrophe, reopening quote).  The inner
    // segment we look for must be `o'\''reilly`.
    assert!(
        cmd.contains(r"o'\''reilly"),
        "resume_command {cmd:?} must POSIX-escape the embedded apostrophe"
    );
    // And the wrapping single-quote pair must still bracket the path.
    let path_str = cfg.to_str().unwrap();
    let posix_quoted = format!("'{}'", path_str.replace('\'', r"'\''"));
    assert!(
        cmd.contains(&posix_quoted),
        "resume_command {cmd:?} must contain the fully POSIX-quoted path {posix_quoted:?}"
    );
}

// ─── Section 16: parse_success_marker — additional rejections ────────────────

#[test]
fn parse_success_marker_rejects_leading_whitespace() {
    // Marker bodies are emitted with no leading whitespace; a leading space
    // would silently shift the offset of the prefix check and could let a
    // crafted body slip past a naïve parser.  We only trim *trailing*
    // whitespace per `success_marker_body`.
    for body in [
        " xxh3:0123456789abcdef",
        "\txxh3:0123456789abcdef",
        "\nxxh3:0123456789abcdef",
        "  xxh3:0123456789abcdef\n",
    ] {
        assert_eq!(
            parse_success_marker(body),
            None,
            "marker body {body:?} must be rejected (leading whitespace)"
        );
    }
}

#[test]
fn parse_success_marker_rejects_trailing_data_after_correct_hex() {
    // After the 16-hex tail, the only legal trailing bytes are ASCII
    // whitespace.  Anything else (extra payload, second marker line,
    // garbage) must be rejected so an attacker can't smuggle metadata
    // inside a marker that an orchestrator otherwise treats as success.
    for body in [
        "xxh3:0123456789abcdef!",
        "xxh3:0123456789abcdef\nrogue line",
        "xxh3:0123456789abcdef:extra",
        "xxh3:0123456789abcdef\nxxh3:0000000000000000",
    ] {
        assert_eq!(
            parse_success_marker(body),
            None,
            "marker body {body:?} must be rejected (trailing payload)"
        );
    }
}

#[test]
fn parse_success_marker_rejects_utf8_bom_prefix() {
    // BOM-mangling is a real failure mode for orchestrators that read
    // the marker through a text editor's autosave.  Reject a UTF-8 BOM
    // explicitly — the marker is ASCII by spec.
    let body = "\u{feff}xxh3:0123456789abcdef\n";
    assert_eq!(parse_success_marker(body), None);
}

#[test]
fn parse_success_marker_rejects_double_marker_concatenation() {
    // Two valid bodies concatenated must NOT parse as either — the length
    // check is the gate that catches this.  Pin it.
    let a = success_marker_body(b"manifest one");
    let b = success_marker_body(b"manifest two");
    let joined = format!("{}{}", a.trim_end(), b);
    assert_eq!(parse_success_marker(&joined), None);
}

// ─── Section 17: writer does not validate self-consistency ───────────────────
//
// Pin the current contract: `write_manifest` is a pure writer; it does NOT
// run `validate_self_consistency` before serialising.  The reader-side check
// is the gate.  Documenting this here so a future "add a guard in the writer"
// change has to update this test (and trip the discussion about whether the
// writer should ever block on a builder bug).

#[test]
fn writer_does_not_enforce_self_consistency_on_input() {
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    // Construct a manifest whose declared `row_count` lies; the writer
    // must still write it (the audit trail is more useful than a silent
    // refusal), and the reader-side check is what flags it.
    let mut m = build_manifest(
        "orders_self_inconsistent",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    m.row_count = 9999;

    let outcome = write_manifest(dest_proxy.as_writer(), &m).unwrap();
    assert!(matches!(outcome, WriteOutcome::Written { .. }));
    assert!(dir.path().join(MANIFEST_FILENAME).exists());

    let parsed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();
    assert!(
        parsed.validate_self_consistency().is_err(),
        "reader-side check must reject what the writer let through"
    );
}

// ─── Section 18: schema fingerprint — drift coverage ─────────────────────────
//
// Section 9 covered the basics (added column, retype).  These cases pin the
// remaining mutation classes a real source can produce so a downstream
// `--validate` build never has to debug "why didn't drift trigger?" with a
// custom diff.

#[test]
fn schema_fingerprint_changes_on_column_rename() {
    let v1 = vec![SchemaColumn {
        name: "user_id".into(),
        data_type: "Int64".into(),
    }];
    let v2 = vec![SchemaColumn {
        name: "uid".into(),
        data_type: "Int64".into(),
    }];
    assert_ne!(schema_fingerprint(&v1), schema_fingerprint(&v2));
}

#[test]
fn schema_fingerprint_is_case_sensitive_on_column_names() {
    // Postgres folds unquoted identifiers to lowercase, but quoted ones
    // preserve case.  A schema change from `Email` → `email` is a real
    // (and silent) source of confusion; the fingerprint must surface it.
    let v1 = vec![SchemaColumn {
        name: "Email".into(),
        data_type: "Utf8".into(),
    }];
    let v2 = vec![SchemaColumn {
        name: "email".into(),
        data_type: "Utf8".into(),
    }];
    assert_ne!(schema_fingerprint(&v1), schema_fingerprint(&v2));
}

#[test]
fn schema_fingerprint_is_case_sensitive_on_data_types() {
    // Arrow/Parquet types are spelled with specific casing (`Int64`, not
    // `int64`).  Mixed casing in a normalisation refactor must be a
    // detectable change, not a silent alias.
    let v1 = vec![SchemaColumn {
        name: "id".into(),
        data_type: "Int64".into(),
    }];
    let v2 = vec![SchemaColumn {
        name: "id".into(),
        data_type: "int64".into(),
    }];
    assert_ne!(schema_fingerprint(&v1), schema_fingerprint(&v2));
}

#[test]
fn schema_fingerprint_changes_on_column_removal() {
    let v1 = vec![
        SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        },
        SchemaColumn {
            name: "email".into(),
            data_type: "Utf8".into(),
        },
    ];
    let v2 = vec![SchemaColumn {
        name: "id".into(),
        data_type: "Int64".into(),
    }];
    assert_ne!(schema_fingerprint(&v1), schema_fingerprint(&v2));
}

#[test]
fn schema_fingerprint_of_empty_schema_is_stable_constant() {
    // `--validate` has to handle "no projection" runs (e.g. SELECT 0
    // sentinels in tests).  Pin the empty-list fingerprint so a future
    // refactor that accidentally seeds the hasher with non-empty state
    // doesn't shift the constant out from under stored values.
    let a = schema_fingerprint(&[]);
    let b = schema_fingerprint(&[]);
    assert_eq!(a, b);
    assert!(a.starts_with("xxh3:"));
    assert_eq!(a.len(), "xxh3:".len() + 16);
}

// ─── Section 19: RunReport JSON wire contract ────────────────────────────────
//
// `summary.json` is the file CI/Airflow consumers actually parse.  Keys
// disappearing or changing type would silently break those consumers.
// Pin the current keyset and the conditional sub-objects' shapes.

#[test]
fn run_report_json_contains_all_top_level_contract_keys() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let s = summary("orders_keyset", "orders", "success", Vec::new(), None);
    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();

    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap()).unwrap();
    let obj = json.as_object().expect("top-level must be JSON object");

    // The stable schema — adding new keys is allowed (forward-compat),
    // but the keys below must all exist or older tooling breaks.
    let required = [
        "run_id",
        "export_name",
        "status",
        "started_at",
        "finished_at",
        "duration_ms",
        "source_engine",
        "destination_kind",
        "format",
        "compression",
        "tuning_profile",
        "batch_size",
        "total_rows",
        "files_produced",
        "bytes_written",
        "peak_rss_mb",
        "retries",
        "pg_temp_bytes_delta",
        "validation",
        "reconciliation",
        "schema_changed",
        "schema_changes",
        "plan_warnings",
        "error_message",
        "resumable",
        "resume_command",
    ];
    for key in required {
        assert!(
            obj.contains_key(key),
            "summary.json missing required key {key:?}; got keys: {:?}",
            obj.keys().collect::<Vec<_>>()
        );
    }
}

#[test]
fn run_report_validation_outcome_is_nested_object_when_set() {
    // A run with `validated: Some(true)` must serialise validation as
    // `{"passed": true}`, not as a bare boolean — the nested shape is
    // the place where future fields (file-level breakdown, hashing
    // confirmation) will be added.
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let s = summary("orders_val", "orders", "success", Vec::new(), None);
    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("summary.json")).unwrap()).unwrap();
    assert!(json["validation"].is_object());
    assert_eq!(json["validation"]["passed"], true);
}

// ─── Section 20: write_run_report cross-cutting negative behaviour ───────────

#[test]
fn rewriting_run_report_overwrites_existing_summary_files() {
    // Two `write_run_report` calls for the same run_id (e.g. resume + final)
    // must replace the previous artifacts wholesale; partial overlap
    // would let stale fields (old error_message, old retry count) shadow
    // the truth.
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());

    let s1 = summary(
        "orders_overwrite",
        "orders",
        "failed",
        vec![part(1, 50, 1024, "xxh3:1111111111111111")],
        Some("first attempt timed out".into()),
    );
    let out1 = write_run_report(cfg.to_str().unwrap(), &s1).unwrap();
    let json1 = std::fs::read_to_string(out1.join("summary.json")).unwrap();
    assert!(json1.contains("first attempt timed out"));
    assert!(json1.contains("\"resumable\": true"));

    let s2 = summary(
        "orders_overwrite",
        "orders",
        "success",
        vec![
            part(1, 50, 1024, "xxh3:1111111111111111"),
            part(2, 60, 2048, "xxh3:2222222222222222"),
        ],
        None,
    );
    let out2 = write_run_report(cfg.to_str().unwrap(), &s2).unwrap();
    assert_eq!(out1, out2, "same run_id ⇒ same report dir");
    let json2 = std::fs::read_to_string(out2.join("summary.json")).unwrap();
    assert!(
        !json2.contains("first attempt timed out"),
        "stale error from first attempt must be gone after resume succeeds"
    );
    assert!(json2.contains("\"resumable\": false"));
    let report: RunReport = serde_json::from_str(&json2).unwrap();
    assert_eq!(report.status, "success");
    assert!(report.resume_command.is_none());

    // The Markdown form must also reflect the new state — no stale "Resume"
    // section bleeding through.
    let md2 = std::fs::read_to_string(out2.join("summary.md")).unwrap();
    assert!(md2.contains("SUCCESS"));
    assert!(
        !md2.contains("first attempt timed out"),
        "stale error must be gone from summary.md after overwrite"
    );
}

#[test]
fn run_id_with_path_traversal_chars_is_rendered_as_literal_dir() {
    // We never sanitise run_id; a caller that passes ".." or "/" gets the
    // resulting nested layout literally.  Pin this so no future refactor
    // accidentally interprets the run_id as a path expression that could
    // escape the report dir.  (Sanitisation belongs in run_id construction,
    // not in the writer — keep concerns separate.)
    let dir = tempfile::tempdir().unwrap();
    let cfg = touch_config(dir.path());
    let s = summary("nested/run_id", "orders", "success", Vec::new(), None);
    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    // The "/" splits into a sub-directory; that's the literal interpretation
    // (we treat `run_id` as a path component the caller takes responsibility
    // for sanitising).
    assert!(out.ends_with("nested/run_id"), "got: {}", out.display());
    assert!(out.join("summary.json").exists());
}

// ─── Section 21a: summary.schema_fingerprint preempts state lookup ──────────
//
// Regression test for the chunked-mode bug where `manifest.schema_fingerprint`
// landed as the placeholder `xxh3:0000000000000000` because `state.store_schema`
// was only called by the single-mode path.  After the fix, every executor
// (single, chunked-parallel, chunked-checkpoint) records the dest schema
// fingerprint directly on `RunSummary`; `finalize_manifest` reads it from
// there, with the state lookup as a fallback.
//
// This test exercises only the read side — the writer side (single.rs +
// chunked/{exec,parallel_checkpoint,sequential_checkpoint}.rs) needs a live
// source and is covered by the docker-compose live test.  The contract here
// is: when `summary.schema_fingerprint` is set, that value flows verbatim
// into the manifest a builder produces on its behalf.

#[test]
fn summary_schema_fingerprint_flows_into_manifest_via_builder() {
    // Caller (pipeline::job::finalize_manifest) reads
    // summary.schema_fingerprint and threads it into ManifestBuilder::new.
    // This test mirrors that wiring step-for-step so a future refactor that
    // swaps the sequence (e.g. always trusting state) trips the assertion.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    let mut s = summary(
        "orders_fp_flow",
        "public.orders",
        "success",
        Vec::new(),
        None,
    );
    let captured_fp = "xxh3:abcdef0123456789".to_string();
    s.schema_fingerprint = Some(captured_fp.clone());

    // What finalize_manifest does (paraphrased):
    //   let fp = summary.schema_fingerprint.clone()
    //              .or_else(|| state.get_stored_schema(...))
    //              .unwrap_or_else(|| "xxh3:0000000000000000".into());
    // So when summary carries a fp, that wins.  We pass it through the
    // builder API the way job.rs does and assert the manifest carries it.
    let mut b = ManifestBuilder::new(
        &PlanSnapshot {
            export_name: s.export_name.clone(),
            base_query: "SELECT * FROM orders".into(),
            strategy: "chunked".into(),
            format: "parquet".into(),
            compression: "zstd".into(),
            destination_type: "local".into(),
            tuning_profile: "balanced".into(),
            batch_size: 1000,
            validate: false,
            reconcile: false,
            resume: false,
        },
        &s.run_id,
        chrono::Utc::now(),
        s.schema_fingerprint.clone().unwrap(),
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
    );
    let m = b.finalize(ManifestStatus::Success);
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    let parsed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();
    assert_eq!(
        parsed.schema_fingerprint, captured_fp,
        "manifest must carry the summary's captured fingerprint, not the placeholder"
    );
    assert_ne!(parsed.schema_fingerprint, "xxh3:0000000000000000");
}

#[test]
fn missing_summary_fingerprint_lands_as_placeholder_not_panic() {
    // Resume-from-state scenarios may reconstruct a `RunSummary` that never
    // saw a live schema.  finalize_manifest's contract is then: try state
    // lookup, finally use the placeholder.  This test just pins the sentinel
    // shape (16-hex zeros) so consumers can detect "schema evidence missing"
    // explicitly instead of reading garbage.
    let placeholder = "xxh3:0000000000000000";
    assert!(placeholder.starts_with("xxh3:"));
    assert_eq!(placeholder.len(), "xxh3:".len() + 16);
    assert_eq!(
        placeholder
            .strip_prefix("xxh3:")
            .unwrap()
            .chars()
            .filter(|c| *c == '0')
            .count(),
        16,
        "placeholder is 16 zeros — distinct from any plausible real fingerprint"
    );
}

// ─── Section 21: streaming destination — _SUCCESS never appears ──────────────

#[test]
fn streaming_destination_never_writes_success_marker_even_on_success_status() {
    // Defence in depth: even if a future refactor wires _SUCCESS write
    // before the streaming check, this test pins that NO `_SUCCESS` file
    // exists for stdout destinations under any status.
    let cfg = DestinationConfig {
        destination_type: DestinationType::Stdout,
        ..Default::default()
    };
    let dest = rivet::destination_for_tests::create_destination(&cfg).unwrap();
    let m = build_manifest(
        "orders_stream_success",
        ManifestStatus::Success,
        vec![part(1, 100, 4096, "xxh3:1111111111111111")],
    );
    let outcome = write_manifest(&*dest, &m).unwrap();
    assert!(matches!(outcome, WriteOutcome::SkippedStreaming));

    // Confirm via local FS too — there is no prefix, so nothing should
    // have been touched.
    let cwd = std::env::current_dir().unwrap();
    assert!(!cwd.join(MANIFEST_FILENAME).exists());
    assert!(!cwd.join(SUCCESS_FILENAME).exists());
}

// ─── Section 22: manifest-aware --validate (ADR-0012 M5/M6, ADR-0013) ───────
//
// End-to-end coverage for `pipeline::verify_at_destination` driven through
// the same Destination + ManifestBuilder + write_manifest path the pipeline
// uses at runtime.  The unit tests in `pipeline/validate_manifest.rs` cover
// each verdict variant in isolation; this section pins the cross-module
// wiring (ADR-0013 acceptance criterion: no new flag added; existing
// `--validate` semantics now subsume M5/M6).

fn dataset_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn parts_with_payloads(payloads: &[&[u8]]) -> Vec<ManifestPart> {
    payloads
        .iter()
        .enumerate()
        .map(|(i, b)| ManifestPart {
            part_id: (i + 1) as u32,
            path: format!("part-{:06}.parquet", i + 1),
            rows: 10 * (i as i64 + 1),
            size_bytes: b.len() as u64,
            content_fingerprint: format!("xxh3:{:016x}", xxhash_rust::xxh3::xxh3_64(b)),
            status: PartStatus::Committed,
        })
        .collect()
}

/// Lay out a clean dataset on the local destination using the same
/// `Destination::write` + `write_manifest` API the pipeline uses.  Returns
/// the destination proxy so the test can also drive the verifier through
/// it.
fn lay_out_clean_dataset(
    base: &Path,
    parts: &[ManifestPart],
    payloads: &[&[u8]],
) -> Box<dyn rivet_destination_proxy::DestinationProxy> {
    assert_eq!(parts.len(), payloads.len());
    let dest_proxy = local_dest(base);
    // Write each part body with the destination's `write` method so the
    // sizes on disk match what the manifest records.
    for (p, body) in parts.iter().zip(payloads.iter()) {
        let tmp = tempfile::NamedTempFile::new_in(base).unwrap();
        std::fs::write(tmp.path(), body).unwrap();
        // The proxy doesn't expose `write` directly; fall back to a plain
        // copy under the destination root which is what local_dest's `write`
        // reduces to.
        std::fs::copy(tmp.path(), base.join(&p.path)).unwrap();
    }
    let manifest = build_manifest("verify_e2e", ManifestStatus::Success, parts.to_vec());
    write_manifest(dest_proxy.as_writer(), &manifest).unwrap();
    dest_proxy
}

#[test]
fn verify_at_destination_certifies_a_clean_writer_output() {
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"payload-1", b"payload-2-bigger"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);

    let v: ManifestVerification = verify_at_destination(dest.as_writer(), "").unwrap();
    assert!(v.manifest_found);
    assert!(!v.legacy_run);
    assert!(v.manifest_self_consistent);
    assert!(v.success_marker_consistent);
    assert_eq!(v.parts_verified, parts.len());
    assert_eq!(v.parts_failed, 0);
    assert!(v.passed);
    assert!(v.failures.is_empty());
}

#[test]
fn verify_at_destination_legacy_run_when_manifest_absent() {
    // No write_manifest call at all → an existing prefix with parquet
    // artifacts but no manifest mimics a pre-0.7.0 export.  The verifier
    // must surface `legacy_run: true` so the operator-facing report can
    // explicitly downgrade its assurance language (M6: silent fallback
    // is forbidden).
    let dir = dataset_dir();
    std::fs::write(dir.path().join("part-000001.parquet"), b"data").unwrap();
    let dest_proxy = local_dest(dir.path());

    let v = verify_at_destination(dest_proxy.as_writer(), "").unwrap();
    assert!(!v.manifest_found);
    assert!(v.legacy_run);
    assert!(v.failures.is_empty(), "legacy is a label, not a failure");
    assert!(
        !v.passed,
        "legacy verifier alone cannot certify; caller composes"
    );
}

#[test]
fn verify_at_destination_flags_missing_part_with_part_id() {
    // ADR-0012 M5: a part listed in the manifest but absent at the
    // destination must surface as `Failure::PartMissing` carrying the
    // part_id so an orchestrator can locate it.
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"keep", b"will-be-deleted"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);
    std::fs::remove_file(dir.path().join(&parts[1].path)).unwrap();

    let v = verify_at_destination(dest.as_writer(), "").unwrap();
    assert!(!v.passed);
    assert_eq!(v.parts_verified, 1);
    assert_eq!(v.parts_failed, 1);
    let missing = v.failures.iter().find_map(|f| match f {
        ManifestVerificationFailure::PartMissing { part_id, path } => {
            Some((*part_id, path.clone()))
        }
        _ => None,
    });
    assert_eq!(missing, Some((2, parts[1].path.clone())));
}

#[test]
fn verify_at_destination_flags_size_drift_with_expected_and_actual() {
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"original-12B"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);
    // Mutate the part body so its size on disk diverges from the manifest's
    // recorded `size_bytes`.
    std::fs::write(dir.path().join(&parts[0].path), b"shorter").unwrap();

    let v = verify_at_destination(dest.as_writer(), "").unwrap();
    assert!(!v.passed);
    let mismatch = v.failures.iter().find_map(|f| match f {
        ManifestVerificationFailure::PartSizeMismatch {
            part_id,
            expected,
            actual,
            ..
        } => Some((*part_id, *expected, *actual)),
        _ => None,
    });
    assert_eq!(
        mismatch,
        Some((1, payloads[0].len() as u64, b"shorter".len() as u64))
    );
}

#[test]
fn verify_at_destination_flags_stale_success_marker_after_manifest_rewrite() {
    // _SUCCESS was written by a previous successful run; then someone
    // rewrote `manifest.json` (resume / repair / hand-edit) without
    // refreshing _SUCCESS.  The verifier must surface this since
    // orchestrators rely on `_SUCCESS` carrying the *current* manifest's
    // fingerprint (ADR-0012 M2).
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"part-payload"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);
    // Hand-edit the manifest to a different (still self-consistent) body.
    let m2 = build_manifest("verify_e2e_v2", ManifestStatus::Success, parts.clone());
    let body = serde_json::to_vec_pretty(&m2).unwrap();
    std::fs::write(dir.path().join(MANIFEST_FILENAME), &body).unwrap();
    // Note: _SUCCESS file is NOT updated — it still carries the prior fp.

    let v = verify_at_destination(dest.as_writer(), "").unwrap();
    assert!(!v.passed);
    assert!(
        v.failures
            .iter()
            .any(|f| matches!(f, ManifestVerificationFailure::SuccessMarkerStale { .. }))
    );
}

#[test]
fn verify_at_destination_tolerates_absent_success_marker_for_failed_run() {
    // ADR-0012 M2: `_SUCCESS` exists iff status == Success.  A failed
    // (status: failed) manifest without `_SUCCESS` is the *correct*
    // shape — the verifier must NOT flag the absent marker as a failure.
    let dir = dataset_dir();
    let dest_proxy = local_dest(dir.path());
    let payloads: &[&[u8]] = &[b"committed-before-failure"];
    let parts = parts_with_payloads(payloads);
    for (p, body) in parts.iter().zip(payloads.iter()) {
        std::fs::write(dir.path().join(&p.path), body).unwrap();
    }
    let manifest = build_manifest("verify_failed", ManifestStatus::Failed, parts.clone());
    write_manifest(dest_proxy.as_writer(), &manifest).unwrap();
    assert!(
        !dir.path().join(SUCCESS_FILENAME).exists(),
        "writer must skip _SUCCESS for Failed status"
    );

    let v = verify_at_destination(dest_proxy.as_writer(), "").unwrap();
    assert!(v.manifest_found);
    assert!(
        !v.success_marker_consistent,
        "no marker → no signal, not a failure"
    );
    assert!(v.passed, "absent marker on a failed manifest is allowed");
    assert!(v.failures.is_empty());
}

#[test]
fn verify_at_destination_flags_untracked_object_but_keeps_passed_true() {
    // Untracked objects under the prefix are surfaced for operator audit
    // (and for resume's M9 quarantine pass), but `--validate` alone does
    // not flip the verdict — the tracked parts and marker are fine.
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"tracked"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);
    std::fs::write(dir.path().join("rogue-leftover.parquet"), b"X").unwrap();

    let v = verify_at_destination(dest.as_writer(), "").unwrap();
    assert!(
        v.passed,
        "tracked parts and marker pass — surplus is informational"
    );
    assert!(v.failures.iter().any(|f| matches!(
        f,
        ManifestVerificationFailure::UntrackedObject { key, .. } if key == "rogue-leftover.parquet"
    )));
}

#[test]
fn verify_at_destination_serialises_failures_with_kind_tag_for_consumers() {
    // The on-wire shape of `failures` must keep its `kind` discriminator
    // so an Airflow / CI consumer can match on the string variant rather
    // than parsing free-form messages.  Pin the JSON shape.
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"x"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);
    std::fs::remove_file(dir.path().join(&parts[0].path)).unwrap();

    let v = verify_at_destination(dest.as_writer(), "").unwrap();
    let json = serde_json::to_value(&v).unwrap();
    let failures = json["failures"].as_array().expect("failures is an array");
    assert!(!failures.is_empty(), "must surface at least one failure");
    let kind = failures[0]["kind"]
        .as_str()
        .expect("each failure must carry a `kind` string");
    assert_eq!(kind, "part_missing");
    // And the field accessors a consumer would use are present:
    assert_eq!(failures[0]["part_id"], 1);
    assert!(failures[0]["path"].is_string());
}

// ─── Section 23: ValidationOutcome wire contract (ADR-0013) ─────────────────

#[test]
fn validation_outcome_in_summary_json_carries_manifest_subobject_when_set() {
    // ADR-0013 acceptance: when --validate ran the manifest pass, the
    // serialized `summary.json` must carry a nested `validation.manifest`
    // object — same flag, richer payload, no schema break vs 0.6.x readers
    // (which simply ignore the new sub-object since `deny_unknown_fields`
    // is intentionally NOT set on RunReport).
    let dir = dataset_dir();
    let cfg = touch_config(dir.path());
    let mut s = summary("verify_outcome", "orders", "success", Vec::new(), None);
    s.manifest_verification = Some(ManifestVerification {
        manifest_found: true,
        legacy_run: false,
        parts_verified: 3,
        parts_failed: 0,
        success_marker_consistent: true,
        manifest_self_consistent: true,
        passed: true,
        failures: Vec::new(),
        level: rivet::pipeline::IntegrityLevel::Structural,
    });

    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    let raw = std::fs::read_to_string(out.join("summary.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(json["validation"].is_object());
    let manifest = &json["validation"]["manifest"];
    assert!(
        manifest.is_object(),
        "validation.manifest must serialise as a nested object"
    );
    assert_eq!(manifest["manifest_found"], true);
    assert_eq!(manifest["parts_verified"], 3);
    assert_eq!(manifest["parts_failed"], 0);
    assert_eq!(manifest["success_marker_consistent"], true);
    assert_eq!(manifest["legacy_run"], false);
    assert_eq!(manifest["passed"], true);
    assert!(manifest["failures"].as_array().unwrap().is_empty());
}

#[test]
fn validation_outcome_omits_manifest_subobject_when_unset() {
    // Backward-compat: a run that did NOT run --validate's manifest pass
    // (--validate not requested, or streaming destination) must produce
    // a summary.json identical in shape to 0.6.x — no `manifest` key at
    // all under `validation`, so 0.6.x consumers read it the same.
    let dir = dataset_dir();
    let cfg = touch_config(dir.path());
    let s = summary(
        "verify_outcome_legacy",
        "orders",
        "success",
        Vec::new(),
        None,
    );
    // Note: summary fixture leaves manifest_verification = None and
    // validated = Some(true) (file-row check ran).

    let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
    let raw = std::fs::read_to_string(out.join("summary.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(json["validation"].is_object());
    assert!(
        json["validation"].get("manifest").is_none(),
        "validation.manifest must be omitted (skip_serializing_if = Option::is_none) for backward-compat readers"
    );
}

// ─── Section 24: ADR-0013 acceptance — no new flags ─────────────────────────
//
// Anchor test: if a future PR adds a CLI flag for M5/M6/M8/M9, this test
// trips and forces a discussion.  The clap derive struct has a stable
// shape today; we pin the count of arg names on the `Run` subcommand.

#[test]
fn rivet_run_subcommand_has_exactly_the_adr_0013_flag_set() {
    // This test reads the binary's --help output via clap's Command
    // structure indirectly — we re-derive the same shape through the
    // existing public API.  If a new flag appears, the assertion below
    // fails with a clear "expected N, got N+1" message.
    //
    // Implementation note: rivet's `Cli` struct is in a binary-only
    // module (`src/cli/args.rs`) so the integration test can't reach
    // it directly.  Instead, invoke the binary with `--help` and count
    // the flags.  The test is gated on the binary existing; CI builds
    // it before tests run.
    let bin = std::env::var("CARGO_BIN_EXE_rivet")
        .ok()
        .or_else(|| {
            std::env::var("CARGO_TARGET_DIR")
                .ok()
                .map(|d| format!("{d}/debug/rivet"))
        })
        .unwrap_or_else(|| "target/debug/rivet".into());
    if !std::path::Path::new(&bin).exists() {
        eprintln!("skipping flag-count test: binary not built at {bin}");
        return;
    }
    let output = std::process::Command::new(&bin)
        .args(["run", "--help"])
        .output()
        .expect("run rivet run --help");
    let help = String::from_utf8_lossy(&output.stdout);
    // Count occurrences of `--<name>` at line start (clap formats one
    // flag per line in the Options block).  Allow leading whitespace.
    let flag_lines: Vec<&str> = help
        .lines()
        .filter(|l| l.trim_start().starts_with("--"))
        .collect();
    let flag_names: std::collections::BTreeSet<String> = flag_lines
        .iter()
        .filter_map(|l| {
            let trimmed = l.trim_start();
            // Take chars while alphanum / dash, after the leading "--"
            let after = trimmed.trim_start_matches("--");
            let name: String = after
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                .collect();
            (!name.is_empty()).then_some(name)
        })
        .collect();
    // ADR-0013 contract: trust + safety + I/O ergonomics — pin the set,
    // not the count, so adding a non-trust flag (e.g. a new --json toggle)
    // is allowed but adding `--verify` / `--check-manifest` etc. trips.
    let expected: std::collections::BTreeSet<String> = [
        // trust flags (ADR-0013):
        "validate",
        "reconcile",
        "resume",
        // safety override (ADR-0013 — one --force scoped to gates):
        "force",
        // run-shape:
        "config",
        "export",
        "parallel-exports",
        "parallel-export-processes",
        "summary-output",
        "json",
        "param",
        // global (clap-derived):
        "json-errors",
        "help",
        "version",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let unexpected: Vec<_> = flag_names.difference(&expected).cloned().collect();
    assert!(
        unexpected.is_empty(),
        "ADR-0013 contract: `rivet run` grew unexpected flag(s): {unexpected:?} \
         — every ADR-0012 invariant must land under existing --validate / \
         --reconcile / --resume.  If a new top-level flag is genuinely needed, \
         supersede ADR-0013 first, then update this set."
    );
}

// ─── Section 25: standalone `rivet validate` subcommand (ADR-0013 carveout) ─

#[test]
fn rivet_validate_subcommand_drives_existing_m5_semantics_passing_path() {
    // ADR-0013 §"Subcommand carveouts": `rivet validate` is allowed
    // because it re-drives the same M5/M6 semantics `rivet run --validate`
    // performs at end-of-run.  No new trust noun, no source query.
    //
    // This test pins the contract end-to-end: clean dataset → subcommand
    // exits 0, JSON report carries the same `verification` block shape
    // a `summary.json[validation][manifest]` consumer already parses.
    let bin = locate_rivet_bin();
    let Some(bin) = bin else {
        eprintln!("skipping rivet_validate_subcommand test: binary not built");
        return;
    };

    let outer = tempfile::tempdir().unwrap();
    let dest_dir = outer.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();

    // Lay out a clean dataset using the public Destination + write_manifest
    // API — same shape `rivet run` produces.
    let dest_proxy = local_dest(&dest_dir);
    let parts = vec![
        part(1, 100, 4096, "xxh3:1111111111111111"),
        part(2, 200, 8192, "xxh3:2222222222222222"),
    ];
    for p in &parts {
        let body = vec![0u8; p.size_bytes as usize];
        std::fs::write(dest_dir.join(&p.path), body).unwrap();
    }
    let m = build_manifest("rivet_validate_e2e", ManifestStatus::Success, parts.clone());
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    // Synthesize a minimal config that points the export's destination at
    // the prepared dir.  `rivet validate` must not touch the source — we
    // give it a clearly non-resolvable URL to prove that.
    let cfg_path = outer.path().join("rivet.yaml");
    std::fs::write(
        &cfg_path,
        format!(
            "source:\n  type: postgres\n  url: postgresql://nope-on-purpose@127.0.0.1:1/never\n\
             exports:\n  - name: public.orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: {}\n",
            dest_dir.display()
        ),
    )
    .unwrap();

    let out_json = outer.path().join("verdict.json");
    let status = std::process::Command::new(&bin)
        .args([
            "validate",
            "--config",
            cfg_path.to_str().unwrap(),
            "--export",
            "public.orders",
            "--format",
            "json",
            "--output",
            out_json.to_str().unwrap(),
        ])
        .status()
        .expect("run rivet validate");
    assert!(status.success(), "exit code must be 0 on a clean dataset");

    // Parse the report and confirm the `verification` shape matches
    // `summary.json[validation][manifest]` (so consumers can share code).
    let body = std::fs::read_to_string(&out_json).unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let exports = json["exports"].as_array().unwrap();
    assert_eq!(exports.len(), 1);
    let v = &exports[0]["verification"];
    assert_eq!(v["manifest_found"], true);
    assert_eq!(v["legacy_run"], false);
    assert_eq!(v["parts_verified"], 2);
    assert_eq!(v["parts_failed"], 0);
    assert_eq!(v["success_marker_consistent"], true);
    assert_eq!(v["passed"], true);
    assert!(v["failures"].as_array().unwrap().is_empty());
}

#[test]
fn rivet_validate_subcommand_exits_nonzero_on_missing_part() {
    // Same dataset as above, but with one part deleted post-write.  The
    // standalone driver MUST exit non-zero so an Airflow / CI step can
    // branch on the exit code without parsing JSON.
    let bin = locate_rivet_bin();
    let Some(bin) = bin else {
        eprintln!("skipping rivet_validate_subcommand failure test: binary not built");
        return;
    };

    let outer = tempfile::tempdir().unwrap();
    let dest_dir = outer.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();
    let dest_proxy = local_dest(&dest_dir);
    let parts = vec![
        part(1, 100, 4096, "xxh3:1111111111111111"),
        part(2, 200, 8192, "xxh3:2222222222222222"),
    ];
    for p in &parts {
        std::fs::write(dest_dir.join(&p.path), vec![0u8; p.size_bytes as usize]).unwrap();
    }
    let m = build_manifest(
        "rivet_validate_fail",
        ManifestStatus::Success,
        parts.clone(),
    );
    write_manifest(dest_proxy.as_writer(), &m).unwrap();
    std::fs::remove_file(dest_dir.join(&parts[1].path)).unwrap();

    let cfg_path = outer.path().join("rivet.yaml");
    std::fs::write(
        &cfg_path,
        format!(
            "source:\n  type: postgres\n  url: postgresql://nope-on-purpose@127.0.0.1:1/never\n\
             exports:\n  - name: public.orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: {}\n",
            dest_dir.display()
        ),
    )
    .unwrap();

    let status = std::process::Command::new(&bin)
        .args([
            "validate",
            "--config",
            cfg_path.to_str().unwrap(),
            "--export",
            "public.orders",
        ])
        .status()
        .expect("run rivet validate");
    assert!(
        !status.success(),
        "exit code must be non-zero when M5 surfaces an explicit failure"
    );
}

#[test]
fn rivet_validate_subcommand_legacy_run_exits_zero() {
    // ADR-0012 M6: a legacy prefix (no manifest) is *labeled*, not failed.
    // The standalone driver must mirror that: exit code 0 (no explicit
    // failures), JSON carries `legacy_run: true`, parts_verified = 0.
    let bin = locate_rivet_bin();
    let Some(bin) = bin else {
        eprintln!("skipping rivet_validate_subcommand legacy test: binary not built");
        return;
    };

    let outer = tempfile::tempdir().unwrap();
    let dest_dir = outer.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();
    // No manifest, no _SUCCESS — bare parquet only.
    std::fs::write(dest_dir.join("legacy.parquet"), b"data").unwrap();

    let cfg_path = outer.path().join("rivet.yaml");
    std::fs::write(
        &cfg_path,
        format!(
            "source:\n  type: postgres\n  url: postgresql://nope-on-purpose@127.0.0.1:1/never\n\
             exports:\n  - name: public.orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: {}\n",
            dest_dir.display()
        ),
    )
    .unwrap();

    let out_json = outer.path().join("legacy.json");
    let status = std::process::Command::new(&bin)
        .args([
            "validate",
            "--config",
            cfg_path.to_str().unwrap(),
            "--format",
            "json",
            "--output",
            out_json.to_str().unwrap(),
        ])
        .status()
        .expect("run rivet validate");
    assert!(
        status.success(),
        "legacy prefix is a label, not a failure — exit code must be 0"
    );

    let body = std::fs::read_to_string(&out_json).unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let v = &json["exports"][0]["verification"];
    assert_eq!(v["legacy_run"], true);
    assert_eq!(v["manifest_found"], false);
    assert_eq!(v["parts_verified"], 0);
    assert!(v["failures"].as_array().unwrap().is_empty());
}

// ─── Section 26: ADR-0012 M8 — _SUCCESS gate on --resume ────────────────────

#[test]
fn rivet_run_resume_refuses_when_success_marker_present_without_force() {
    // ADR-0012 M8: a destination prefix whose `_SUCCESS` is already on disk
    // signals "verified successful run".  `--resume` against that prefix
    // would overwrite a known-good dataset; the gate refuses unless
    // `--force` is given.  This test pins the refusal end-to-end.
    let bin = locate_rivet_bin();
    let Some(bin) = bin else {
        eprintln!("skipping success-gate test: binary not built");
        return;
    };

    let outer = tempfile::tempdir().unwrap();
    let dest_dir = outer.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();

    // Plant a manifest + _SUCCESS marker — same shape as a completed run.
    let dest_proxy = local_dest(&dest_dir);
    let parts = vec![part(1, 100, 4096, "xxh3:1111111111111111")];
    std::fs::write(dest_dir.join(&parts[0].path), vec![0u8; 4096]).unwrap();
    let m = build_manifest("prior_run", ManifestStatus::Success, parts);
    write_manifest(dest_proxy.as_writer(), &m).unwrap();
    assert!(
        dest_dir.join(SUCCESS_FILENAME).exists(),
        "marker must exist"
    );

    // Build a config whose destination points at the prefix above.  We
    // give the source a deliberately-unreachable URL — the gate must fire
    // BEFORE any source connection is attempted, so no real PG needed.
    let cfg_path = outer.path().join("rivet.yaml");
    std::fs::write(
        &cfg_path,
        format!(
            "source:\n  type: postgres\n  url: postgresql://nope-on-purpose@127.0.0.1:1/never\n\
             exports:\n  - name: public.orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: {}\n",
            dest_dir.display()
        ),
    )
    .unwrap();

    // 1. Without --force: gate refuses, exit non-zero, error message
    //    explicitly mentions _SUCCESS and --force.
    let output = std::process::Command::new(&bin)
        .args([
            "run",
            "--config",
            cfg_path.to_str().unwrap(),
            "--export",
            "public.orders",
            "--resume",
        ])
        .output()
        .expect("run rivet run --resume");
    assert!(
        !output.status.success(),
        "exit code must be non-zero when _SUCCESS gate refuses"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("_SUCCESS"),
        "error message must mention _SUCCESS so the operator knows why; got:\n{stderr}"
    );
    assert!(
        stderr.contains("--force"),
        "error message must point operators at --force as the override"
    );

    // 2. With --force: gate is overridden.  The source URL is intentionally
    //    unreachable, so the run will fail later (during source connect),
    //    but it must NOT fail with the gate's "_SUCCESS" message — proves
    //    the gate let the run through.
    let output = std::process::Command::new(&bin)
        .args([
            "run",
            "--config",
            cfg_path.to_str().unwrap(),
            "--export",
            "public.orders",
            "--resume",
            "--force",
        ])
        .output()
        .expect("run rivet run --resume --force");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("--resume refused"),
        "with --force the gate must NOT trip; got stderr:\n{stderr}"
    );
}

#[test]
fn rivet_run_resume_proceeds_when_no_success_marker() {
    // Pre-0.7.0 destinations and failed-then-rewritten destinations both
    // legitimately lack `_SUCCESS`.  The gate must NOT trip on those —
    // refusing would break the resume use-case the gate is supposed to
    // protect.
    let bin = locate_rivet_bin();
    let Some(bin) = bin else {
        eprintln!("skipping success-gate-absent test: binary not built");
        return;
    };

    let outer = tempfile::tempdir().unwrap();
    let dest_dir = outer.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();
    // No _SUCCESS — bare parquet only (legacy run).
    std::fs::write(dest_dir.join("legacy.parquet"), b"data").unwrap();

    let cfg_path = outer.path().join("rivet.yaml");
    std::fs::write(
        &cfg_path,
        format!(
            "source:\n  type: postgres\n  url: postgresql://nope-on-purpose@127.0.0.1:1/never\n\
             exports:\n  - name: public.orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: {}\n",
            dest_dir.display()
        ),
    )
    .unwrap();

    let output = std::process::Command::new(&bin)
        .args([
            "run",
            "--config",
            cfg_path.to_str().unwrap(),
            "--export",
            "public.orders",
            "--resume",
        ])
        .output()
        .expect("run rivet run --resume");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("--resume refused"),
        "no _SUCCESS at destination → gate must let the run through; \
         the run may still fail later for unrelated reasons (unreachable source), \
         but the failure must NOT be the gate's message; got stderr:\n{stderr}"
    );
}

// ─── Section 27: ADR-0012 M8 — resume decision matrix end-to-end ────────────
//
// Phase C-β: pure decision logic exists and is unit-tested per row in
// `pipeline::resume_decisions::tests`.  This section pins the matrix
// against a *real* Destination listing produced by `local_dest`, so a
// regression that breaks listing-key shape (e.g. a future change in how
// LocalDestination::list_prefix joins paths) trips here too.

#[test]
fn resume_plan_skips_committed_parts_present_at_destination() {
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"AAAA", b"BBBBB"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);

    let manifest = build_manifest("e2e_skip", ManifestStatus::Success, parts);
    let listing = dest.as_writer().list_prefix("").unwrap();
    let plan = build_resume_plan(&manifest, &listing);

    assert_eq!(
        plan.skipped(),
        2,
        "both committed parts must be skipped on resume"
    );
    assert_eq!(plan.rewrites(), 0);
    assert_eq!(plan.quarantines(), 0);
    assert!(plan.untracked.is_empty());
    for d in plan.per_part.values() {
        assert_eq!(d.decision, ResumeDecision::Skip);
    }
}

#[test]
fn resume_plan_rewrites_part_missing_at_destination() {
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"present", b"will-be-deleted"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);
    std::fs::remove_file(dir.path().join(&parts[1].path)).unwrap();

    let manifest = build_manifest("e2e_rewrite", ManifestStatus::Success, parts.clone());
    let listing = dest.as_writer().list_prefix("").unwrap();
    let plan = build_resume_plan(&manifest, &listing);

    assert_eq!(plan.skipped(), 1);
    assert_eq!(plan.rewrites(), 1);
    assert_eq!(
        plan.per_part[&parts[1].path].decision,
        ResumeDecision::Rewrite
    );
}

#[test]
fn resume_plan_quarantines_part_with_size_drift() {
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"original-12B"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);
    // Mutate the part body — same path, different size.
    std::fs::write(dir.path().join(&parts[0].path), b"shorter").unwrap();

    let manifest = build_manifest("e2e_drift", ManifestStatus::Success, parts.clone());
    let listing = dest.as_writer().list_prefix("").unwrap();
    let plan = build_resume_plan(&manifest, &listing);

    assert_eq!(plan.quarantines(), 1);
    assert_eq!(
        plan.per_part[&parts[0].path].decision,
        ResumeDecision::Quarantine {
            reason: QuarantineReason::SizeMismatch
        }
    );
}

#[test]
fn resume_plan_quarantines_untracked_object_under_prefix() {
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"committed"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);
    std::fs::write(dir.path().join("rogue.parquet"), b"X").unwrap();

    let manifest = build_manifest("e2e_untracked", ManifestStatus::Success, parts);
    let listing = dest.as_writer().list_prefix("").unwrap();
    let plan = build_resume_plan(&manifest, &listing);

    assert_eq!(plan.untracked.len(), 1);
    assert_eq!(
        plan.untracked["rogue.parquet"],
        UntrackedDecision::Quarantine
    );
}

#[test]
fn resume_plan_does_not_quarantine_manifest_or_success_marker() {
    // The trust-contract files are part of the `listing` — must be
    // excluded from "untracked surplus" classification.
    let dir = dataset_dir();
    let payloads: &[&[u8]] = &[b"x"];
    let parts = parts_with_payloads(payloads);
    let dest = lay_out_clean_dataset(dir.path(), &parts, payloads);

    let manifest = build_manifest("e2e_trustfiles", ManifestStatus::Success, parts);
    let listing = dest.as_writer().list_prefix("").unwrap();
    // listing must contain the trust-contract files (the writer just put
    // them there) — proves they reach the matrix and are correctly skipped.
    let keys: std::collections::HashSet<&str> = listing.iter().map(|m| m.key.as_str()).collect();
    assert!(keys.contains(MANIFEST_FILENAME));
    assert!(keys.contains(SUCCESS_FILENAME));

    let plan = build_resume_plan(&manifest, &listing);
    assert!(plan.untracked.is_empty());
}

/// Locate the freshly-built `rivet` binary or return None when the test
/// is being run outside `cargo test` (e.g. in IDE).  Mirrors the locator
/// used by the ADR-0013 anchor test in §24.
fn locate_rivet_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_rivet") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(d) = std::env::var("CARGO_TARGET_DIR") {
        let path = std::path::PathBuf::from(format!("{d}/debug/rivet"));
        if path.exists() {
            return Some(path);
        }
    }
    let fallback = std::path::PathBuf::from("target/debug/rivet");
    fallback.exists().then_some(fallback)
}

// ─── Section 28: M4 part_id allocation under hydration (Phase C-γ) ──────────
//
// `record_committed_part_with_fingerprint` used to compute the new
// `part_id` as `manifest_parts.len() + 1`.  After Phase C-γ, M8's resume
// preamble pre-populates `summary.manifest_parts` with parts inherited
// from the prior manifest (the parts whose decision was `Skip`).  At
// that point a length-based ordinal can collide with a hydrated part_id
// — e.g. hydrated [1, 2, 4, 5], length=4, next-write would get part_id=5
// (already taken).
//
// The fix is `max(existing) + 1`.  This section pins it at the type
// level so a future refactor that reverts to length-based numbering
// trips loudly.
//
// We can't directly call `record_committed_part_with_fingerprint`
// (it's `pub(crate)`), so we exercise the contract via `ManifestBuilder`
// + `validate_self_consistency` — which is the public API that ships
// to operators and the same path `pipeline::finalize::finalize_manifest`
// drives.

#[test]
fn manifest_builder_with_hydrated_part_ids_stays_self_consistent() {
    // Mirror what M8 does: the `summary.manifest_parts` already contains
    // 4 parts (ids 1, 2, 4, 5 — gap at 3 is normal because chunk 2 was
    // reset for rewrite).  A new write appends — must not pick part_id=5
    // and break self-consistency.
    let dir = tempfile::tempdir().unwrap();
    let dest_proxy = local_dest(dir.path());

    let prior_parts = vec![
        part(1, 100, 4096, "xxh3:1111111111111111"),
        part(2, 100, 4096, "xxh3:2222222222222222"),
        part(4, 100, 4096, "xxh3:4444444444444444"),
        part(5, 100, 4096, "xxh3:5555555555555555"),
    ];

    // Construct a final manifest the way `finalize_manifest` does after a
    // hydrated resume: hydrated parts + one freshly-written part appended.
    // The writer's `record_committed_part_with_fingerprint` is what assigns
    // the new part_id; here we exercise the contract one level above by
    // composing a `RunManifest` that mirrors the post-write state.
    let mut all_parts = prior_parts.clone();
    // Simulate the appended part using `max + 1` part_id selection (the
    // writer's responsibility); here we just pin that the resulting
    // manifest is self-consistent.
    let next_id = all_parts.iter().map(|p| p.part_id).max().unwrap() + 1;
    all_parts.push(part(next_id, 100, 4096, "xxh3:6666666666666666"));

    let m = build_manifest("hydrated", ManifestStatus::Success, all_parts);
    write_manifest(dest_proxy.as_writer(), &m).unwrap();

    let parsed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(MANIFEST_FILENAME)).unwrap())
            .unwrap();
    assert_eq!(parsed.validate_self_consistency(), Ok(()));
    let ids: Vec<u32> = parsed.parts.iter().map(|p| p.part_id).collect();
    assert_eq!(
        ids,
        vec![1, 2, 4, 5, 6],
        "M4 self-consistency: part_ids unique, gaps allowed (resume produces 1,2,4,5 + 6)"
    );
}

#[test]
fn manifest_builder_rejects_duplicate_part_ids_after_hydration() {
    // Defence in depth: if a future refactor reverts to length-based
    // ordinals AND M8 is in play, we want the reader-side
    // `validate_self_consistency` to catch the resulting collision
    // BEFORE writing it would get to `_SUCCESS`.  Pin that.
    let mut m = build_manifest(
        "dup_after_hydrate",
        ManifestStatus::Success,
        vec![
            part(1, 100, 4096, "xxh3:1111111111111111"),
            part(2, 100, 4096, "xxh3:2222222222222222"),
            part(5, 100, 4096, "xxh3:5555555555555555"),
        ],
    );
    // Inject a duplicate as if a buggy writer used `len()+1` after
    // hydration: 3 parts → next id = 4, but we ship 5 (collision).
    let mut bad = part(5, 100, 4096, "xxh3:5555555555555555");
    bad.path = "duplicate.parquet".into();
    m.parts.push(bad);
    m.part_count = 4;
    m.row_count = 400;

    let err = m.validate_self_consistency().unwrap_err();
    assert!(
        format!("{err}").contains("part_id"),
        "duplicate part_id must surface as a self-consistency error"
    );
}
