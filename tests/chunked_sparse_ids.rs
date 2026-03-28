//! Chunked mode with sparse physical IDs (gaps in the key range).
//!
//! See `fixtures/migrations/001_sparse_chunk_column_example.sql` for a PostgreSQL
//! migration-shaped example (table + view with a dense chunk column).

use rivet::pipeline::generate_chunks;

#[test]
fn migration_fixture_documents_dense_chunk_surrogate() {
    let sql = include_str!("fixtures/migrations/001_sparse_chunk_column_example.sql");
    assert!(
        sql.contains("ROW_NUMBER()") && sql.contains("chunk_rownum"),
        "fixture should define ROW_NUMBER surrogate for chunk_column"
    );
    assert!(
        sql.contains("orders_sparse_for_export"),
        "fixture should define an export view"
    );
    assert!(
        sql.contains("chunk_column: chunk_rownum"),
        "fixture should show rivet config using the surrogate"
    );
}

#[test]
fn wide_physical_id_range_splits_into_many_chunks() {
    // Same span as the fixture ids 1 .. 4_000_000 with chunk_size 500_000.
    let chunks = generate_chunks(1, 4_000_000, 500_000);
    assert_eq!(chunks.len(), 8);
}

#[test]
fn sparse_physical_ids_imply_most_chunk_windows_are_empty() {
    // Three rows at ids 1, 2_000_000, 4_000_000 — only three windows contain data,
    // the rest are empty scans if chunk_column is `id`.
    let chunks = generate_chunks(1, 4_000_000, 500_000);
    let rows = 3_usize;
    assert!(
        chunks.len() > rows,
        "expected more chunk windows than rows when min/max follow physical id span"
    );
}

#[test]
fn dense_chunk_column_aligns_windows_with_row_count() {
    let row_count = 3_i64;
    let chunks = generate_chunks(1, row_count, 1);
    assert_eq!(chunks, vec![(1, 1), (2, 2), (3, 3)]);
}
