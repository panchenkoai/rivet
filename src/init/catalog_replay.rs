//! Offline replay harness for the strategy-decision logic.
//!
//! The strategy `rivet init` scaffolds for a table is a PURE function of the
//! table's catalog metadata — [`TableInfo`] (row estimate, physical bytes, column
//! types + PK shape) — with NO row data. So a real hostile DB can be distilled
//! into a checked-in catalog fixture (schema + stats, anonymized) and every
//! strategy decision the field hit replayed deterministically offline. The messy
//! production DB becomes a regression oracle with zero customer-data exposure.
//!
//! This is the same discipline as the coverage matrices, but the fixtures are
//! DISTILLED from real hostile DBs instead of hand-authored: when a field run
//! surfaces a table whose strategy is wrong, its (anonymized) catalog row is
//! appended here and the decision is locked forever. `scaffold_strategy` calls the
//! SAME `TableInfo` methods the scaffold uses (`suggest_mode`, `single_pk_column`,
//! `best_chunk_column`, `best_cursor_column`), so the harness tests the real
//! decision, never a re-implementation that could agree with a wrong spec.

use super::TableInfo;

/// The strategy `rivet init` would scaffold for `info`, as a compact label —
/// `"keyset(<pk>)"`, `"chunked(<col>)"`, `"incremental(<cursor>)"`, or `"full"`.
///
/// Mirrors `yaml_scaffold::export_block_lines`: `suggest_mode()` picks the mode,
/// then a single-column PK routes `chunked` → keyset (ADR-0020), else range chunk;
/// `incremental`/`full` pass through. Pure — the whole point is that it is a
/// data-free function of the catalog shape.
pub(crate) fn scaffold_strategy(info: &TableInfo) -> String {
    match info.suggest_mode() {
        "chunked" => match info.single_pk_column() {
            Some(pk) => format!("keyset({pk})"),
            None => format!("chunked({})", info.best_chunk_column().unwrap_or("id")),
        },
        "incremental" => format!(
            "incremental({})",
            info.best_cursor_column().unwrap_or("updated_at")
        ),
        other => other.to_string(), // "full"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The distilled hostile-catalog fixture: representative table SHAPES from the
    /// real 153-table field DB (anonymized names, real stats + types + PK shape),
    /// plus the expected scaffold strategy. Append a new row when the field
    /// surfaces a shape whose strategy is wrong — it locks the decision forever.
    const FIXTURE: &str = include_str!("fixtures/hostile_catalog.json");

    #[derive(serde::Deserialize)]
    struct FixtureRow {
        expect: String,
        /// Free-text note (why this shape matters / known-gap marker). Unused by
        /// the assertion, present for the human reading the fixture.
        #[allow(dead_code)]
        note: String,
        table: TableInfo,
    }

    #[test]
    fn scaffold_strategy_matches_every_distilled_field_shape() {
        let rows: Vec<FixtureRow> =
            serde_json::from_str(FIXTURE).expect("hostile_catalog.json must parse");
        assert!(rows.len() >= 6, "fixture must cover several shapes");
        for row in &rows {
            let got = scaffold_strategy(&row.table);
            assert_eq!(
                got, row.expect,
                "table '{}' ({}): expected strategy {}, got {}",
                row.table.table, row.note, row.expect, got
            );
        }
    }

    /// KNOWN GAP (field-observed → ERROR 3024): a table that is small by ROW COUNT
    /// but heavy by BYTES (wide TEXT/BLOB columns — e.g. a framework's audit/log
    /// table with MB-sized text) is scaffolded `full`, because `suggest_mode`'s
    /// 100k threshold is row-count-blind to row WIDTH. A full-scan of MB-sized rows
    /// holds one query open past `max_execution_time`. It HAS an integer PK, so it
    /// SHOULD keyset (bounded query time per page). This test PINS the current
    /// (wrong) behaviour so the harness is honest about the gap; when init learns
    /// to weigh `total_bytes`, flip `full` → `keyset(id)` here and it goes RED
    /// until the fix lands. Same meta-class as the sparse-key rule: a threshold
    /// blind to a second dimension.
    #[test]
    fn wide_but_short_table_is_currently_full_a_known_row_width_gap() {
        let info: TableInfo = serde_json::from_str(
            r#"{
              "schema": "s", "table": "wide_short_25k",
              "row_estimate": 25000, "total_bytes": 21474836480,
              "columns": [
                {"name":"id","data_type":"bigint","is_primary_key":true,"is_nullable":false,"numeric_precision":null,"numeric_scale":null},
                {"name":"wide_text_a","data_type":"longtext","is_primary_key":false,"is_nullable":true,"numeric_precision":null,"numeric_scale":null},
                {"name":"wide_text_b","data_type":"longtext","is_primary_key":false,"is_nullable":true,"numeric_precision":null,"numeric_scale":null}
              ]
            }"#,
        )
        .unwrap();
        // ~860 KB/row (20 GiB / 25k) — a full scan holds the query open too long.
        assert_eq!(
            scaffold_strategy(&info),
            "full",
            "documents the row-width gap: this SHOULD be keyset(id) once init weighs total_bytes"
        );
    }
}
