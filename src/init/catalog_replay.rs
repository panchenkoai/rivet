//! Offline replay harness for the strategy-decision logic — the **first-init
//! anchor**.
//!
//! This locks the decision `rivet init` makes on its FIRST pass: from the
//! table's CATALOG metadata alone — [`TableInfo`] (row estimate, physical bytes,
//! column types + PK shape), with NO row data and NO prior-run history. That is a
//! PURE function, so a real hostile DB is distilled into a checked-in catalog
//! fixture (schema + stats, anonymized) and every strategy decision the field hit
//! is replayed deterministically offline — the messy production DB becomes a
//! regression oracle with zero customer-data exposure.
//!
//! COMPANION ANCHOR (#148/#149, not yet built): a SECOND-pass init that prefers
//! the ACTUALS a full run recorded in the state DB (real durations / rows / peak
//! RSS per export) over the catalog's estimate. That is a decision from a
//! DIFFERENT input (run history, not catalog stats), so it earns its OWN anchor —
//! a golden distilled from a real full run's metrics — rather than folding into
//! this catalog-only one. Kept explicit here so the two inits stay
//! distinguishable: catalog-estimate first run vs actuals-informed re-init.
//!
//! This is the same discipline as the coverage matrices, but the fixtures are
//! DISTILLED from real hostile DBs instead of hand-authored: when a field run
//! surfaces a table whose strategy is wrong, its (anonymized) catalog row is
//! appended here and the decision is locked forever. `scaffold_strategy` calls the
//! SAME `TableInfo` methods the scaffold uses (`suggest_mode`, `single_pk_column`,
//! `best_chunk_column`, `chosen_cursor_column` — the scored picker the YAML and the
//! strategy snapshot both use), so the harness tests the real decision, never a
//! re-implementation that could agree with a wrong spec.
//!
//! ENGINE SCOPE — SQL only (PostgreSQL / MySQL / SQL Server). Those three share
//! ONE `information_schema`-shaped introspection → the same `TableInfo` → the same
//! engine-agnostic scaffold decision, so a single fixture covers all three
//! (dogfood-verified live on ALL three: a large uuid / varchar single-PK table
//! scaffolds keyset(`chunk_by_key`) and a decimal(p,0)-PK table scaffolds `full`
//! IDENTICALLY on PG, MySQL AND SQL Server — end-to-end confirmed: an init-scaffolded
//! uuid-PK keyset config exported all 120K rows). MongoDB is a SEPARATE path
//! (`init::mongo` — no SQL columns / PK; keyset is `source.mongo.page_size` on
//! `_id`, not `chunk_by_key`), so `TableInfo`/`scaffold_strategy` does NOT model it
//! and these fixtures do not apply to Mongo. Empirically Mongo init scaffolds
//! `mode: full` even for a large (150K-doc) collection — it does not auto-scaffold
//! `page_size` — so Mongo is "always full" at init, its own decision to lock in a
//! Mongo-specific golden, not here.
//!
//! COVERAGE (all auto-scaffold branches, dogfood-cross-checked live on PG / MySQL /
//! SQL Server): keyset (int / uuid / string / float / timestamp / date PK),
//! range-chunk (no-PK + composite-PK + decimal-PK-with-int-col; row-scaled
//! parallel, MySQL-wide → 1), incremental, full. `scaffold_full` models the whole
//! decision (strategy + chunk_size + parallel + checkpoint), not just the label.
//! OUT OF SCOPE — `init --mode cdc` is a FORCED mode, not a `suggest_mode` outcome,
//! so it is not modelled here; it is dogfood-verified separately to scaffold
//! `mode: cdc` + an engine-specific `cdc:` stream block on all three SQL engines.
//! `time_window` / `date-chunked` are never auto-selected (no golden needed).

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
        // Mirror yaml_scaffold EXACTLY: a KEYSET-usable single PK → keyset;
        // otherwise range chunk on an integer column. Using `keysettable_pk_column`
        // (not `single_pk_column`) is load-bearing — a decimal PK is not keysettable,
        // so a decimal-PK table with an integer column range-chunks the int column
        // rather than (wrongly) keyseting the decimal.
        "chunked" => match info.keysettable_pk_column() {
            Some(pk) => format!("keyset({pk})"),
            None => format!("chunked({})", info.best_chunk_column().unwrap_or("id")),
        },
        "incremental" => format!(
            "incremental({})",
            // Mirror yaml_scaffold::export_block_lines EXACTLY: it renders the
            // cursor via chosen_cursor_column() (the scored picker), not
            // best_cursor_column() (name-only) — and emits a REVIEW marker,
            // never a phantom `updated_at`, when there is no candidate. Using
            // best_ here re-implemented the product with a DIFFERENT picker,
            // the exact self-oracle this golden exists to prevent (roast
            // 2026-08-09; superseded fully by the #159 deletion).
            info.chosen_cursor_column().as_deref().unwrap_or("?")
        ),
        other => other.to_string(), // "full"
    }
}

/// The FULL scaffold decision `rivet init` writes — the strategy AND its
/// sub-decisions (`chunk_size`, `parallel`, `chunk_checkpoint`) — as a canonical
/// string, for `<engine>` (`postgres` / `mysql` / `mssql`). `scaffold_strategy`
/// above verifies only the STRATEGY LABEL; this is the seam the parallel-keyset
/// overclaim hid behind (init was documented to row-scale `parallel` on keyset, but
/// keyset is always SEQUENTIAL). Formats:
///   `keyset(<pk>, size=<N>, parallel=1, checkpoint)`   — keyset is ALWAYS
///        sequential (parallel=1) with crash-recovery checkpoint on.
///   `chunked(<col>, size=<N>, parallel=<P>, checkpoint)` — range chunk; `P` is
///        row-scaled AND engine/width-aware (wide rows on MySQL hold at 1).
///   `incremental(<cursor>)` · `full` — no chunk sub-decisions.
/// It calls the REAL `suggest_parallel` (engine-aware), so it cannot drift from the
/// scaffold. The `<engine>` parameter is why this is "per engine": the range-chunk
/// `parallel` genuinely differs (MySQL + wide → 1).
pub(crate) fn scaffold_full(info: &TableInfo, engine: &str) -> String {
    match info.suggest_mode() {
        "chunked" => {
            let size = info.suggest_chunk_size();
            match info.keysettable_pk_column() {
                // Keyset is sequential by construction — parallel is ALWAYS 1 (no
                // `parallel:` line scaffolded), checkpoint on for crash-recovery.
                Some(pk) => format!("keyset({pk}, size={size}, parallel=1, checkpoint)"),
                None => {
                    let col = info.best_chunk_column().unwrap_or("id");
                    let p = super::yaml_scaffold::suggest_parallel(
                        info.row_estimate,
                        info.avg_row_bytes(),
                        engine,
                    )
                    .workers;
                    format!("chunked({col}, size={size}, parallel={p}, checkpoint)")
                }
            }
        }
        "incremental" => format!(
            "incremental({})",
            // Mirror yaml_scaffold::export_block_lines EXACTLY: it renders the
            // cursor via chosen_cursor_column() (the scored picker), not
            // best_cursor_column() (name-only) — and emits a REVIEW marker,
            // never a phantom `updated_at`, when there is no candidate. Using
            // best_ here re-implemented the product with a DIFFERENT picker,
            // the exact self-oracle this golden exists to prevent (roast
            // 2026-08-09; superseded fully by the #159 deletion).
            info.chosen_cursor_column().as_deref().unwrap_or("?")
        ),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::ColumnInfo;

    // ── local builders (engine-parameterized full-decision coverage) ──────────
    fn c(name: &str, ty: &str, pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: ty.into(),
            is_primary_key: pk,
            is_nullable: false,
            numeric_precision: None,
            numeric_scale: None,
        }
    }
    fn tbl(rows: i64, bytes: i64, cols: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            schema: "s".into(),
            table: "t".into(),
            row_estimate: rows,
            total_bytes: Some(bytes),
            columns: cols,
        }
    }

    const ENGINES: [&str; 3] = ["postgres", "mysql", "mssql"];
    fn narrow(rows: i64) -> i64 {
        rows * 64
    } // ~64 B/row — never memory-caps
    fn wide(rows: i64) -> i64 {
        rows * 1500
    } // ≥1024 B/row — the MySQL-wide case

    /// FULL scaffold decision (strategy + chunk_size + parallel + checkpoint) across
    /// EVERY branch, verified on ALL THREE SQL engines. This is the coverage the
    /// label-only `scaffold_strategy` lacked — the seam the parallel-keyset overclaim
    /// hid behind. Expected values are hand-written (independent oracle), NOT derived
    /// from the code.
    #[test]
    fn scaffold_full_decision_across_every_branch_and_engine() {
        // ── KEYSET: every keysettable PK type. ALWAYS parallel=1 (sequential) +
        //    checkpoint, on every engine. chunk_size is row-scaled (100k/250k/1M/2.5M).
        let keyset_cases: &[(TableInfo, &str)] = &[
            (
                tbl(
                    150_000,
                    narrow(150_000),
                    vec![c("id", "bigint", true), c("x", "text", false)],
                ),
                "keyset(id, size=100000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    3_000_000,
                    narrow(3_000_000),
                    vec![c("id", "uuid", true), c("x", "text", false)],
                ),
                "keyset(id, size=250000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    200_000_000,
                    narrow(200_000_000),
                    vec![c("code", "varchar", true), c("x", "text", false)],
                ),
                "keyset(code, size=2500000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    2_000_000,
                    narrow(2_000_000),
                    vec![c("fk", "float", true), c("x", "text", false)],
                ),
                "keyset(fk, size=250000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    600_000,
                    narrow(600_000),
                    vec![c("ts", "timestamp", true), c("x", "text", false)],
                ),
                "keyset(ts, size=100000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    8_000_000,
                    narrow(8_000_000),
                    vec![c("d", "date", true), c("x", "text", false)],
                ),
                "keyset(d, size=250000, parallel=1, checkpoint)",
            ),
        ];
        for (info, want) in keyset_cases {
            for eng in ENGINES {
                assert_eq!(&scaffold_full(info, eng), want, "keyset on {eng}");
            }
        }

        // ── RANGE-CHUNK (no single keysettable PK, has an integer column). NARROW
        //    rows → row-scaled parallel identical on every engine (<500k→1, <5M→2,
        //    ≥5M→4). Uses a NO-PK table (single_pk_column None) and a COMPOSITE-PK
        //    table (also None) — both route to range chunk on the int column.
        let range_narrow: &[(TableInfo, &str)] = &[
            (
                tbl(
                    300_000,
                    narrow(300_000),
                    vec![c("k", "bigint", false), c("v", "int", false)],
                ),
                "chunked(k, size=100000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    2_000_000,
                    narrow(2_000_000),
                    vec![c("k", "bigint", false), c("v", "int", false)],
                ),
                "chunked(k, size=250000, parallel=2, checkpoint)",
            ),
            (
                tbl(
                    10_000_000,
                    narrow(10_000_000),
                    vec![c("k", "bigint", false), c("v", "int", false)],
                ),
                "chunked(k, size=1000000, parallel=4, checkpoint)",
            ),
            // Composite PK (two PK columns) → single_pk_column None → range chunk.
            (
                tbl(
                    2_000_000,
                    narrow(2_000_000),
                    vec![
                        c("a", "bigint", true),
                        c("b", "bigint", true),
                        c("v", "int", false),
                    ],
                ),
                "chunked(a, size=250000, parallel=2, checkpoint)",
            ),
            // Decimal PK WITH an integer column → range-chunks the INT column (`seq`,
            // the first integer column — the decimal PK is not integer), never
            // keysets the decimal.
            (
                tbl(
                    200_000,
                    narrow(200_000),
                    vec![c("id", "decimal", true), c("seq", "int", false)],
                ),
                "chunked(seq, size=100000, parallel=1, checkpoint)",
            ),
        ];
        for (info, want) in range_narrow {
            for eng in ENGINES {
                assert_eq!(&scaffold_full(info, eng), want, "range-chunk on {eng}");
            }
        }

        // ── ENGINE DIFFERENCE: a WIDE (≥1024 B/row) range-chunk table. MySQL holds
        //    parallel at 1 (a single sequential scan beats chunked on wide MySQL
        //    rows); PostgreSQL and SQL Server still row-scale to 4. This is the
        //    "по всем движкам" case — the range-chunk parallel is NOT engine-agnostic.
        let big_wide = tbl(
            10_000_000,
            wide(10_000_000),
            vec![c("k", "bigint", false), c("v", "int", false)],
        );
        assert_eq!(
            scaffold_full(&big_wide, "mysql"),
            "chunked(k, size=1000000, parallel=1, checkpoint)"
        );
        assert_eq!(
            scaffold_full(&big_wide, "postgres"),
            "chunked(k, size=1000000, parallel=4, checkpoint)"
        );
        assert_eq!(
            scaffold_full(&big_wide, "mssql"),
            "chunked(k, size=1000000, parallel=4, checkpoint)"
        );

        // ── INCREMENTAL: no key, a timestamp cursor. Engine-agnostic, no chunk knobs.
        let inc = tbl(
            2_000_000,
            narrow(2_000_000),
            vec![
                c("name", "text", false),
                c("updated_at", "timestamp", false),
            ],
        );
        for eng in ENGINES {
            assert_eq!(
                scaffold_full(&inc, eng),
                "incremental(updated_at)",
                "incremental on {eng}"
            );
        }

        // ── FULL: below threshold, no key, decimal-PK-no-int, small non-int PK.
        let full_cases: &[TableInfo] = &[
            tbl(
                27,
                8_192,
                vec![c("id", "bigint", true), c("x", "text", false)],
            ),
            tbl(
                50_000,
                narrow(50_000),
                vec![c("a", "text", false), c("b", "text", false)],
            ),
            tbl(
                150_000,
                narrow(150_000),
                vec![c("id", "decimal", true), c("name", "text", false)],
            ),
            tbl(
                50_000,
                narrow(50_000),
                vec![c("id", "uuid", true), c("x", "text", false)],
            ),
        ];
        for info in full_cases {
            for eng in ENGINES {
                assert_eq!(scaffold_full(info, eng), "full", "full on {eng}");
            }
        }
    }

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
