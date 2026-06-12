//! Live regression for audit L15: a repair re-export must name its output file
//! after the ORIGINAL logical chunk it repairs, not `chunk0`.
//!
//! The repair runner drives the re-export through a single-chunk
//! `ChunkSource::Precomputed(vec![(start, end)])`. That source restarts chunk
//! enumeration at 0, so the writer named every repaired part `..._chunk0_...`
//! regardless of which chunk it repaired. Repairing chunk 1 therefore landed a
//! file called `chunk0`, which no longer reflected the logical chunk — an
//! operator (or a future resume) reading the prefix could not tell which chunk
//! the file belonged to. The fix renames the part to carry `a.chunk_index`.
//!
//! | ID | Scenario | Contract |
//! |---|---|---|
//! | L15 | drift chunk 1, `repair --execute` | repaired file is named `chunk1`, not a 2nd `chunk0` |

mod common;

use common::*;

/// Seed a 100-row table with chunk_size 50 (→ chunk 0 and chunk 1), run the
/// chunked export, and return the table guard, output dir, and config path.
fn seed_and_run_two_chunks() -> (
    PgTable,
    tempfile::TempDir,
    tempfile::TempDir,
    std::path::PathBuf,
) {
    let table = seed_pg_numeric_table(100);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 50
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out_dir.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run_out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet run (setup)");
    assert!(
        run_out.status.success(),
        "setup export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run_out.stderr)
    );

    (table, out_dir, cfg_dir, cfg)
}

/// Parquet files in `dir` whose name contains the given `_chunk{n}_` token.
fn parts_for_chunk(dir: &std::path::Path, n: usize) -> Vec<std::path::PathBuf> {
    let token = format!("_chunk{n}_");
    files_with_extension(dir, "parquet")
        .into_iter()
        .filter(|p| {
            p.file_name()
                .map(|f| f.to_string_lossy().contains(&token))
                .unwrap_or(false)
        })
        .collect()
}

// ─── L15: repaired file carries the original chunk index ──────────────────────

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn repair_execute_names_repaired_file_after_original_chunk_index() {
    require_alive(LiveService::Postgres);
    let (table, out_dir, _cfg_dir, cfg) = seed_and_run_two_chunks();

    // After a clean run: exactly one part for chunk 0 and one for chunk 1.
    assert_eq!(
        parts_for_chunk(out_dir.path(), 0).len(),
        1,
        "one chunk0 part after the clean run"
    );
    assert_eq!(
        parts_for_chunk(out_dir.path(), 1).len(),
        1,
        "one chunk1 part after the clean run"
    );

    // Drift the source under the SECOND chunk only (ids >= 80): 20 rows deleted
    // → chunk 1 mismatches, chunk 0 still matches. Repair targets chunk 1.
    let mut c = pg_connect();
    c.batch_execute(&format!("DELETE FROM {} WHERE id >= 80", table.name()))
        .expect("drift source under chunk 1");

    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "repair",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--execute",
        ])
        .output()
        .expect("spawn rivet repair --execute");
    assert!(
        out.status.success(),
        "repair --execute must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );

    // The repaired chunk is chunk 1, so the NEW file must be named `chunk1`
    // (now 2 chunk1 parts: original + repair), and there must be NO new chunk0
    // part — the bug produced a 2nd chunk0 file for a chunk-1 repair.
    assert_eq!(
        parts_for_chunk(out_dir.path(), 1).len(),
        2,
        "repair must add a chunk1 part alongside the original chunk1 part"
    );
    assert_eq!(
        parts_for_chunk(out_dir.path(), 0).len(),
        1,
        "repairing chunk 1 must NOT mis-name the new file as chunk0 (audit L15)"
    );
}
