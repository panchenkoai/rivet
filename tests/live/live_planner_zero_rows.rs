//! Plans that ended in exit 0, `_SUCCESS` and zero rows over a source that has rows:
//! a `query:` that aliases a column to the chunk column's name, and a planner probe
//! whose value type the driver could not read. Every cell compares what was delivered
//! with what the source itself returns.

use crate::common::*;
use mysql::prelude::Queryable as _;
use std::collections::BTreeSet;

/// Combined stdout+stderr of a finished run.
fn said(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// How `engine` spells the column `name` in a config and in the delivered schema.
fn spelled(engine: SqlEngine, name: &str) -> String {
    match engine {
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => name.to_uppercase(),
        _ => name.to_string(),
    }
}

/// The distinct values of the integer column `col` in `table`, rendered as text by the source.
fn source_values(engine: SqlEngine, table: &str, col: &str) -> BTreeSet<String> {
    match engine {
        SqlEngine::Pg => pg_connect()
            .query(&format!("SELECT {col}::text FROM {table}"), &[])
            .expect("pg read")
            .iter()
            .map(|r| r.get(0))
            .collect(),
        SqlEngine::Mysql => mysql_connect()
            .query::<String, _>(format!("SELECT CAST({col} AS CHAR) FROM {table}"))
            .expect("mysql read")
            .into_iter()
            .collect(),
        SqlEngine::Mssql => {
            mssql_query_strings(&format!("SELECT CONVERT(varchar(32), {col}) FROM {table}"))
                .into_iter()
                .collect()
        }
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => ora_text_rows(&format!("SELECT TO_CHAR({col}) FROM {table}"))
            .into_iter()
            .map(|r| r[0].clone().expect("a value"))
            .collect(),
    }
}

/// The distinct rendered values of `col` over every part under `out`.
fn delivered_values(out: &std::path::Path, col: &str) -> BTreeSet<String> {
    let mut values = BTreeSet::new();
    for batch in read_all_parts(out) {
        let c = batch
            .column_by_name(col)
            .unwrap_or_else(|| panic!("column {col} is delivered"));
        for i in 0..batch.num_rows() {
            values.insert(
                arrow::util::display::array_value_to_string(c, i).expect("render a delivered cell"),
            );
        }
    }
    values
}

/// The destination holds `rows` rows whose `delivered_col` values are exactly the source's `source_col` values.
fn assert_delivers_the_source(
    engine: SqlEngine,
    rig: &Rig,
    table: &str,
    (delivered_col, source_col): (&str, &str),
    rows: usize,
    what: &str,
) {
    let source = source_values(engine, table, source_col);
    assert_eq!(source.len(), rows, "fixture: {what}");
    assert_eq!(
        (
            total_parquet_rows(&rig.out_dir()),
            delivered_values(&rig.out_dir(), &spelled(engine, delivered_col))
        ),
        (rows, source),
        "{what}"
    );
}

/// Insert `(columns) VALUES row(g)` for every `g` in `range`, 500 rows a statement.
fn insert_rows(
    engine: SqlEngine,
    table: &str,
    columns: &str,
    range: std::ops::RangeInclusive<i64>,
    row: impl Fn(i64) -> String,
) {
    let all: Vec<String> = range.map(|g| format!("({})", row(g))).collect();
    for part in all.chunks(500) {
        engine.exec(&format!(
            "INSERT INTO {table} ({columns}) VALUES {}",
            part.join(", ")
        ));
    }
}

/// A `query:` whose projection is `projection`, range-chunked on its output column `id`.
fn chunked_query_rig(engine: SqlEngine, table: &str, projection: &str) -> Rig {
    engine
        .rig(table)
        .query(&format!("SELECT {projection} FROM {table}"))
        .mode("chunked")
        .export_line(&format!("chunk_column: {}", spelled(engine, "id")))
        .export_line("chunk_size: 100")
}

/// A fresh `(id, legacy_id, name)` table of 1000 rows with `legacy_id = 1000000 + id`, and its drop guard.
fn legacy_id_table(engine: SqlEngine) -> (String, Box<dyn std::any::Any>) {
    let i = engine.int64();
    let (table, guard) = engine.create(
        "alias_chunk",
        &format!("id {i} PRIMARY KEY, legacy_id {i} NOT NULL, name VARCHAR(20) NOT NULL"),
    );
    insert_rows(engine, &table, "id, legacy_id, name", 1..=1000, |g| {
        format!("{g}, {}, 'n{g}'", 1_000_000 + g)
    });
    (table, guard)
}

/// `query:` renames `legacy_id` to the chunk column's name: the windows come from the query's own `id`, so its 1000 rows are delivered.
fn a_query_that_aliases_the_chunk_column_delivers_its_rows(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = legacy_id_table(engine);
    for projection in ["legacy_id AS id, name", "legacy_id id, name"] {
        let rig = chunked_query_rig(engine, &table, projection);
        rig.run_ok();
        assert_delivers_the_source(
            engine,
            &rig,
            &table,
            ("id", "legacy_id"),
            1000,
            &format!(
                "`SELECT {projection}` returns the table's 1000 `legacy_id` values as `id`; \
                 windows computed on the base table's `id` (1..1000) match none of them"
            ),
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_query_that_aliases_the_chunk_column_delivers_its_rows_postgres() {
    a_query_that_aliases_the_chunk_column_delivers_its_rows(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn a_query_that_aliases_the_chunk_column_delivers_its_rows_mysql() {
    a_query_that_aliases_the_chunk_column_delivers_its_rows(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn a_query_that_aliases_the_chunk_column_delivers_its_rows_mssql() {
    a_query_that_aliases_the_chunk_column_delivers_its_rows(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose up -d oracle"]
fn a_query_that_aliases_the_chunk_column_delivers_its_rows_oracle() {
    a_query_that_aliases_the_chunk_column_delivers_its_rows(SqlEngine::Oracle);
}

/// Projections that leave the chunk column the base table's own deliver the same rows as before.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_query_that_does_not_rename_the_chunk_column_delivers_its_rows_postgres() {
    let engine = SqlEngine::Pg;
    engine.alive();
    let (table, _guard) = legacy_id_table(engine);
    for projection in ["id, name", "id, name AS label", "*"] {
        let rig = chunked_query_rig(engine, &table, projection);
        rig.run_ok();
        let source = source_values(engine, &table, "id");
        assert_eq!(
            (
                total_parquet_rows(&rig.out_dir()),
                delivered_values(&rig.out_dir(), "id")
            ),
            (1000, source),
            "`SELECT {projection}`: the table's 1000 ids, each once"
        );
    }
}

/// The SQL literal of the key of row `g`.
type KeyLiteral = fn(i64) -> String;

/// A parallel keyset rig over `table`, keyed on `id`, 500 rows a page.
fn parallel_keyset_rig(engine: SqlEngine, table: &str) -> Rig {
    engine
        .rig(table)
        .mode("chunked")
        .export_line(&format!("chunk_by_key: {}", spelled(engine, "id")))
        .export_line("chunk_size: 500")
        .export_line("parallel: 2")
}

/// A parallel `keyset_incremental` export keyed on `key_type`: run 1 delivers the source's 3000 rows, run 2 the 500 keys added since, each once.
fn parallel_keyset_incremental_delivers_the_source_rows(
    engine: SqlEngine,
    key_type: &str,
    key: impl Fn(i64) -> String,
) {
    engine.alive();
    let (table, _guard) = engine.create(
        "ks_keytype",
        &format!("id {key_type} PRIMARY KEY, v INT NOT NULL"),
    );
    let row = |g: i64| format!("{}, {g}", key(g));
    insert_rows(engine, &table, "id, v", 1..=3000, row);

    let rig = parallel_keyset_rig(engine, &table).export_line("keyset_incremental: true");
    rig.run_ok();
    assert_delivers_the_source(
        engine,
        &rig,
        &table,
        ("v", "v"),
        3000,
        &format!("{key_type}: the first run has no anchor, so it delivers what the source holds"),
    );
    assert_eq!(
        delivered_values(&rig.out_dir(), &spelled(engine, "id")).len(),
        3000,
        "{key_type}: 3000 distinct keys"
    );

    insert_rows(engine, &table, "id, v", 3001..=3500, row);
    rig.run_ok();
    assert_delivers_the_source(
        engine,
        &rig,
        &table,
        ("v", "v"),
        3500,
        &format!("{key_type}: the second run delivers the 500 keys past the anchor, nothing twice"),
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_incremental_on_a_smallint_key_delivers_the_source_rows_postgres() {
    parallel_keyset_incremental_delivers_the_source_rows(SqlEngine::Pg, "SMALLINT", |g| {
        g.to_string()
    });
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_incremental_on_an_oid_key_delivers_the_source_rows_postgres() {
    parallel_keyset_incremental_delivers_the_source_rows(SqlEngine::Pg, "OID", |g| g.to_string());
}

/// The key types the probe already read deliver the same rows (`uuid` is absent: PostgreSQL before 18 has no `max(uuid)`).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_incremental_on_the_other_key_types_delivers_the_source_rows_postgres() {
    let types: [(&str, KeyLiteral); 7] = [
        ("INT", |g| g.to_string()),
        ("BIGINT", |g| g.to_string()),
        ("DOUBLE PRECISION", |g| format!("{g}.1")),
        ("DATE", |g| format!("DATE '2000-01-01' + {g}")),
        ("TIMESTAMP", |g| {
            format!("TIMESTAMP '2024-01-01 00:00:00.5' + {g} * INTERVAL '1 second'")
        }),
        ("TIMESTAMPTZ", |g| {
            format!("TIMESTAMPTZ '2024-01-01 00:00:00.5+00' + {g} * INTERVAL '1 second'")
        }),
        ("TEXT", |g| format!("'k{g:06}'")),
    ];
    for (key_type, key) in types {
        parallel_keyset_incremental_delivers_the_source_rows(SqlEngine::Pg, key_type, key);
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_incremental_on_a_smallint_key_delivers_the_source_rows_mysql() {
    parallel_keyset_incremental_delivers_the_source_rows(SqlEngine::Mysql, "SMALLINT", |g| {
        g.to_string()
    });
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn parallel_keyset_incremental_on_a_float_key_delivers_the_source_rows_mysql() {
    parallel_keyset_incremental_delivers_the_source_rows(SqlEngine::Mysql, "FLOAT", |g| {
        g.to_string()
    });
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn parallel_keyset_incremental_on_a_smallint_key_delivers_the_source_rows_mssql() {
    parallel_keyset_incremental_delivers_the_source_rows(SqlEngine::Mssql, "SMALLINT", |g| {
        g.to_string()
    });
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn parallel_keyset_incremental_on_a_real_key_delivers_the_source_rows_mssql() {
    parallel_keyset_incremental_delivers_the_source_rows(SqlEngine::Mssql, "REAL", |g| {
        g.to_string()
    });
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose up -d oracle"]
fn parallel_keyset_incremental_on_a_number5_key_delivers_the_source_rows_oracle() {
    parallel_keyset_incremental_delivers_the_source_rows(SqlEngine::Oracle, "NUMBER(5)", |g| {
        g.to_string()
    });
}

/// A `real` key has no probe reader: the parallel run is refused by name on every cycle, and without `parallel:` the same export delivers the source's rows.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_incremental_on_a_real_key_is_refused_by_name_postgres() {
    let engine = SqlEngine::Pg;
    engine.alive();
    let (table, _guard) = engine.create("ks_real", "id REAL PRIMARY KEY, v INT NOT NULL");
    insert_rows(engine, &table, "id, v", 1..=3000, |g| format!("{g}.5, {g}"));
    let rig = parallel_keyset_rig(engine, &table).export_line("keyset_incremental: true");
    for cycle in 1..=2 {
        let text = rig.run_expect_fail();
        assert!(
            text.contains("RIVET_SOURCE_PROBE_UNREADABLE")
                && text.contains("postgres: cannot read a planner probe's `float4` value.")
                && text.contains("remove `parallel:` from a keyset export"),
            "cycle {cycle}: the refusal names the type and the remedy:\n{text}"
        );
        assert_eq!(
            total_parquet_rows(&rig.out_dir()),
            0,
            "cycle {cycle}: refused before any row is written"
        );
    }

    let remedy = engine
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 500")
        .export_line("keyset_incremental: true");
    remedy.run_ok();
    assert_delivers_the_source(
        engine,
        &remedy,
        &table,
        ("v", "v"),
        3000,
        "the remedy the refusal names delivers what the source holds",
    );
}

/// Parallel keyset without `keyset_incremental` on a `smallint` key delivers the source's rows, as before.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn parallel_keyset_on_a_smallint_key_delivers_the_source_rows_postgres() {
    let engine = SqlEngine::Pg;
    engine.alive();
    let (table, _guard) = engine.create("ks_small", "id SMALLINT PRIMARY KEY, v INT NOT NULL");
    insert_rows(engine, &table, "id, v", 1..=3000, |g| format!("{g}, {g}"));
    let rig = parallel_keyset_rig(engine, &table);
    rig.run_ok();
    let source = source_values(engine, &table, "id");
    assert_eq!(
        (
            total_parquet_rows(&rig.out_dir()),
            delivered_values(&rig.out_dir(), "id")
        ),
        (3000, source),
        "the table's 3000 keys, each once"
    );
}

/// Range chunking on a `smallint` column reads its bounds and delivers the source's rows.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn range_chunking_on_a_smallint_column_delivers_the_source_rows_postgres() {
    let engine = SqlEngine::Pg;
    engine.alive();
    let (table, _guard) = engine.create("rc_small", "id SMALLINT PRIMARY KEY, v INT NOT NULL");
    insert_rows(engine, &table, "id, v", 1..=1000, |g| format!("{g}, {g}"));
    let rig = engine
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 100");
    rig.run_ok();
    let source = source_values(engine, &table, "id");
    assert_eq!(
        (
            total_parquet_rows(&rig.out_dir()),
            delivered_values(&rig.out_dir(), "id")
        ),
        (1000, source),
        "the table's 1000 keys, each once"
    );
}

/// A probe value of a type rivet cannot read stops the run by name; over an empty table the same probe is NULL and the export is an empty success.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_probe_value_of_an_unreadable_type_is_refused_by_name_postgres() {
    let engine = SqlEngine::Pg;
    engine.alive();
    let (table, _guard) =
        engine.create("rc_numeric", "id NUMERIC(15,0) PRIMARY KEY, v INT NOT NULL");
    let rig = chunked_query_rig(engine, &table, "id, v");

    let empty = rig.run();
    assert!(
        empty.status.success(),
        "an empty table's bound is NULL, which is not an unreadable value:\n{}",
        said(&empty)
    );
    assert_eq!(total_parquet_rows(&rig.out_dir()), 0);

    insert_rows(engine, &table, "id, v", 1..=100, |g| format!("{g}, {g}"));
    for cycle in 1..=2 {
        let text = rig.run_expect_fail();
        assert!(
            text.contains("RIVET_SOURCE_PROBE_UNREADABLE")
                && text.contains(
                    "postgres: cannot read a planner probe's `numeric` value. rivet reads a \
                     probe as an integer (int2, int4, int8, oid), float8, a string, a date, a \
                     timestamp or a uuid, and never takes another type for \"no rows\". Chunk, \
                     page or partition this export on a column of one of those types, remove \
                     `parallel:` from a keyset export, or use `mode: full`. Probe: SELECT \
                     min(\"id\") AS rivet_agg FROM"
                ),
            "cycle {cycle}: the refusal names the type and the remedy:\n{text}"
        );
        assert_eq!(
            total_parquet_rows(&rig.out_dir()),
            0,
            "cycle {cycle}: refused before any row is written"
        );
    }
}
