//! Round-7 HIGH regression: on a CDC prefix the dataset is the UNION of the
//! run-unique manifest copies — validate's presence check must cover EVERY
//! cycle's committed parts, not only the canonical (latest) manifest's.
//!
//! Measured pre-fix: with a non-empty canonical, deleting a PRIOR cycle's part
//! validated `PASSED, exit 0` at sample depth (and full depth failed only as a
//! misclassified could-not-verify) — silent loss converted to "verified
//! clean". RED against narrowing `merge_split_unit_parts`'s fold back to
//! empty-canonical-only (the `|| canonical.mode == "cdc"` arm).

use std::process::Command;

fn write_pq(path: &std::path::Path, pos: &str) {
    use std::sync::Arc;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("__op", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("__pos", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("__seq", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::StringArray::from(vec!["insert"])),
            Arc::new(arrow::array::StringArray::from(vec![pos])),
            Arc::new(arrow::array::Int64Array::from(vec![0])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn manifest_json(run: &str, part: &str, size: u64) -> String {
    format!(
        r#"{{"manifest_version":1,"run_id":"{run}","export_name":"cdc","export_family":"cdc",
"mode":"cdc","started_at":"2026-08-29T00:00:00Z","finished_at":"2026-08-29T00:01:00Z",
"status":"success","source":{{"engine":"mysql","schema":null,"table":"t","extraction":null}},
"destination":{{"kind":"local","uri":"file:///x"}},"format":"parquet","compression":"zstd",
"schema_fingerprint":"xxh3:0123456789abcdef","row_count":1,"part_count":1,
"parts":[{{"part_id":0,"path":"{part}","rows":1,"size_bytes":{size},
"content_fingerprint":"xxh3:1111111111111111","content_md5":"","status":"committed"}}]}}"#
    )
}

#[test]
fn a_prior_cdc_cycles_deleted_part_fails_validate_at_sample_depth() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    write_pq(
        &out.join("cdc-r1.parquet"),
        r#"{"file":"binlog.000001","pos":100}"#,
    );
    write_pq(
        &out.join("cdc-r2.parquet"),
        r#"{"file":"binlog.000001","pos":200}"#,
    );
    let sz = |n: &str| std::fs::metadata(out.join(n)).unwrap().len();
    std::fs::write(
        out.join("manifest-r1.json"),
        manifest_json("r1", "cdc-r1.parquet", sz("cdc-r1.parquet")),
    )
    .unwrap();
    let m2 = manifest_json("r2", "cdc-r2.parquet", sz("cdc-r2.parquet"));
    std::fs::write(out.join("manifest-r2.json"), &m2).unwrap();
    std::fs::write(out.join("manifest.json"), &m2).unwrap();
    let cfg = dir.path().join("rivet.yaml");
    std::fs::write(
        &cfg,
        format!(
            "source:\n  type: mysql\n  url: mysql://nobody@localhost/nope\nexports:\n  - name: cdc\n    table: t\n    mode: cdc\n    format: parquet\n    cdc:\n      server_id: 1\n      checkpoint: {}/ck\n    destination:\n      type: local\n      path: \"{}\"\n",
            dir.path().display(),
            out.display()
        ),
    )
    .unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_rivet"))
            .args(args)
            .output()
            .unwrap()
    };

    // Round-8: an `initial: snapshot` SUB-dataset beside the cycles — its copy's
    // parts[].path are relative to snapshot/, and the round-7 fold's first cut
    // resolved them at the TABLE root, false-failing every intact snapshot
    // prefix with PART_MISSING exit 3. The intact assertion below now guards
    // that shape too (RED against un-rel-ing the fold's nested-copy skip).
    let snap = out.join("snapshot");
    std::fs::create_dir_all(&snap).unwrap();
    write_pq(
        &snap.join("snap-000.parquet"),
        r#"{"file":"binlog.000001","pos":1}"#,
    );
    std::fs::write(
        snap.join("manifest-rs.json"),
        manifest_json(
            "rs",
            "snap-000.parquet",
            std::fs::metadata(snap.join("snap-000.parquet"))
                .unwrap()
                .len(),
        ),
    )
    .unwrap();

    // Positive control: the intact prefix validates clean at both depths.
    let ok = run(&["validate", "-c", cfg.to_str().unwrap(), "--depth", "sample"]);
    assert!(
        ok.status.success(),
        "intact fixture must validate: {}",
        String::from_utf8_lossy(&ok.stderr)
    );

    // The prior cycle's part vanishes; the canonical (r2) is non-empty.
    std::fs::remove_file(out.join("cdc-r1.parquet")).unwrap();
    let bad = run(&["validate", "-c", cfg.to_str().unwrap(), "--depth", "sample"]);
    assert_eq!(
        bad.status.code(),
        Some(3),
        "a deleted historical part is VERIFIED-WRONG, not PASSED/retryable;\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&bad.stdout),
        String::from_utf8_lossy(&bad.stderr)
    );
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&bad.stdout),
        String::from_utf8_lossy(&bad.stderr)
    );
    assert!(
        all.contains("PART_MISSING"),
        "the failure must be the presence class, at SAMPLE depth (no __pos backstop there): {all}"
    );
}

/// Round-10 (closing round-7 F2): a part PRESENT at its declared size whose
/// bytes are not Parquet must fail `--depth full` even when the manifest
/// records NO column_checksums (what every keyset/chunked/mongo_parallel run
/// records today) — the old `Err(_) => continue` printed PASSED over literal
/// garbage. RED against re-narrowing part_row_count_mismatch's unopenable arm.
#[test]
fn full_depth_fails_a_non_parquet_part_even_without_recorded_checksums() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let body = b"NOTPARQUET";
    std::fs::write(out.join("part-000001.parquet"), body).unwrap();
    let m = format!(
        r#"{{"manifest_version":1,"run_id":"rb1","export_name":"orders","mode":"batch",
"started_at":"2026-08-29T01:00:00Z","finished_at":"2026-08-29T01:01:00Z","status":"success",
"source":{{"engine":"postgres","schema":"public","table":"orders","extraction":null}},
"destination":{{"kind":"local","uri":"file:///x"}},"format":"parquet","compression":"zstd",
"schema_fingerprint":"xxh3:0123456789abcdef","row_count":500,"part_count":1,
"parts":[{{"part_id":1,"path":"part-000001.parquet","rows":500,"size_bytes":{},
"content_fingerprint":"xxh3:1111111111111111","content_md5":"","status":"committed"}}]}}"#,
        body.len()
    );
    std::fs::write(out.join("manifest.json"), m).unwrap();
    let cfg = dir.path().join("rivet.yaml");
    std::fs::write(
        &cfg,
        format!(
            "source:\n  type: postgres\n  url: postgresql://nobody@localhost/nope\nexports:\n  - name: orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{}\"\n",
            out.display()
        ),
    )
    .unwrap();
    let out_cmd = Command::new(env!("CARGO_BIN_EXE_rivet"))
        .args(["validate", "-c", cfg.to_str().unwrap(), "--depth", "full"])
        .output()
        .unwrap();
    assert_eq!(
        out_cmd.status.code(),
        Some(3),
        "garbage bytes at declared size are VERIFIED-WRONG;\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out_cmd.stdout),
        String::from_utf8_lossy(&out_cmd.stderr)
    );
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out_cmd.stdout),
        String::from_utf8_lossy(&out_cmd.stderr)
    );
    assert!(all.contains("does not decode"), "{all}");
}
