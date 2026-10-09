//! A read through a transaction-mode pooler that keeps prepared statements is described by what it
//! reads now, never by what another read of the same statement text read before it.
//!
//! pgBouncer (1.21 and later, `max_prepared_statements` above zero) keeps a named prepared statement
//! by its text and reuses it for every client, and PostgreSQL keeps the result description it fixed
//! when that text was first parsed. Every cell here sends two reads through the stand's one pooled
//! server connection and re-reads the second destination against the source.
//!
//! ```text
//! docker compose --profile pool up -d pgbouncer
//! cargo nextest run --test live_suite --run-ignored only -E 'test(/^live_pooled_reads::/)'
//! ```

use crate::common::*;

const ENGINE: SqlEngine = SqlEngine::Pg;
const ROWS: i64 = 20;

/// `rig` staged to one of the four batch modes, a page or chunk smaller than the table.
fn staged(rig: Rig, mode: &str) -> Rig {
    match mode {
        "full" => rig,
        "keyset" => ENGINE.staged(rig, "chunked", &["chunk_by_key: id", "chunk_size: 7"]),
        "range" => ENGINE.staged(rig, "chunked", &["chunk_column: id", "chunk_size: 7"]),
        "incremental" => ENGINE.staged(rig, "incremental", &["cursor_column: id"]),
        other => panic!("no batch mode `{other}`"),
    }
}

/// A fresh table of `columns` holding `ROWS` rows of `values` (an expression list over `g`), analyzed.
fn table_of(prefix: &str, columns: &str, values: &str) -> (String, Box<dyn std::any::Any>) {
    let (table, guard) = ENGINE.create(prefix, columns);
    ENGINE.exec(&format!(
        "INSERT INTO {table} SELECT {values} FROM generate_series(1, {ROWS}) g"
    ));
    ENGINE.refresh_row_estimate(&table);
    (table, guard)
}

/// The source's `(name, type)` per column and every row as text, by id, read on a direct connection.
fn source_holds(table: &str) -> (Vec<(String, String)>, Vec<Vec<String>>) {
    let mut pg = pg_connect();
    let columns: Vec<(String, String)> = pg
        .query(
            "SELECT column_name::text, data_type::text FROM information_schema.columns \
             WHERE table_name = $1 ORDER BY ordinal_position",
            &[&table],
        )
        .expect("the source's columns")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let as_text: Vec<String> = columns
        .iter()
        .map(|(name, _)| format!("{name}::text"))
        .collect();
    let rows = pg
        .query(
            &format!("SELECT {} FROM {table} t ORDER BY t.id", as_text.join(", ")),
            &[],
        )
        .expect("the source's rows")
        .iter()
        .map(|r| (0..columns.len()).map(|i| r.get(i)).collect())
        .collect();
    (columns, rows)
}

/// The Arrow type a batch export gives a PostgreSQL column type used in these cells.
fn arrow_type(pg_type: &str) -> &'static str {
    match pg_type {
        "bigint" => "Int64",
        "integer" => "Int32",
        "double precision" => "Float64",
        "text" | "USER-DEFINED" => "Utf8",
        "timestamp without time zone" => "Timestamp(µs)",
        other => panic!("fixture: no Arrow type stated for `{other}`"),
    }
}

/// The delivered parts' `(name, type)` per column and every row as text, by id.
fn delivered(rig: &Rig) -> (Vec<(String, String)>, Vec<Vec<String>>) {
    let batches = read_all_parts(&rig.out_dir());
    let schema = batches.first().expect("a delivered part").schema();
    let columns = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().to_string()))
        .collect();
    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        arrow::util::display::array_value_to_string(c, row).expect("a cell as text")
                    })
                    .collect(),
            );
        }
    }
    rows.sort_by_key(|cells| cells[0].parse::<i64>().expect("the id leads every row"));
    (columns, rows)
}

/// Export `table` through the pooler in `mode` and require the destination to equal the source: columns, types, values.
fn pooled_export_equals_the_source(table: &str, mode: &str, what: &str) {
    let rig = staged(ENGINE.rig(table).source_url(PGBOUNCER_URL), mode);
    let out = rig.run();
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "{what}: the {mode} export through the pooler failed:\n{said}"
    );
    assert!(
        !said.contains("could not resolve schema for drift check"),
        "{what}: the {mode} export through the pooler skipped its schema-drift check:\n{said}"
    );
    destination_equals_the_source(&rig, table, mode, what);
}

/// The destination of `rig` against `table` as the source holds it: columns, types, every value.
fn destination_equals_the_source(rig: &Rig, table: &str, mode: &str, what: &str) {
    let (source_columns, source_rows) = source_holds(table);
    let expected: Vec<(String, String)> = source_columns
        .iter()
        .map(|(name, ty)| (name.clone(), arrow_type(ty).to_string()))
        .collect();
    let (columns, rows) = delivered(rig);
    assert_eq!(
        columns, expected,
        "{what}: the {mode} export exited 0 under columns the source does not hold"
    );
    assert_eq!(
        rows.len(),
        source_rows.len(),
        "{what}: {mode} delivered another row count than the source holds"
    );
    for (got, want) in rows.iter().zip(&source_rows) {
        let same = got
            .iter()
            .zip(want)
            .zip(&source_columns)
            .all(|((g, w), (_, ty))| {
                if ty == "timestamp without time zone" {
                    g.replace('T', " ") == *w
                } else {
                    g == w
                }
            });
        let short = |cells: &[String]| -> Vec<String> {
            cells.iter().map(|c| c.chars().take(60).collect()).collect()
        };
        assert!(
            same,
            "{what}: {mode} delivered {:?} where the source holds {:?}",
            short(got),
            short(want)
        );
    }
}

/// A table of `first` columns, then one of `second` columns, both through the pooler in `mode`.
fn a_second_table_arrives_as_itself(mode: &str, second: (&str, &str)) {
    forget_pooled_statements();
    let (a, _a) = table_of(
        "pool_a",
        "id BIGINT PRIMARY KEY, v BIGINT NOT NULL, updated_at TIMESTAMP NOT NULL",
        "g, g * 10, TIMESTAMP '2026-01-01 00:00:00' + g * INTERVAL '1 second'",
    );
    let (b, _b) = table_of("pool_b", second.0, second.1);
    pooled_export_equals_the_source(&a, mode, "the first table");
    pooled_export_equals_the_source(&b, mode, "the second table");
}

const SAME_WIDTH: (&str, &str) = (
    "id BIGINT PRIMARY KEY, amount DOUBLE PRECISION NOT NULL, updated_at TIMESTAMP NOT NULL",
    "g, g + 0.5, TIMESTAMP '2026-02-02 00:00:00' + g * INTERVAL '1 second'",
);
const WIDER: (&str, &str) = (
    "id BIGINT PRIMARY KEY, amount DOUBLE PRECISION NOT NULL, note TEXT NOT NULL, \
     updated_at TIMESTAMP NOT NULL",
    "g, g + 0.5, 'n' || g, TIMESTAMP '2026-02-02 00:00:00' + g * INTERVAL '1 second'",
);

/// One table through the pooler in `mode`, then `alter`, then the same table again as a fresh export.
fn an_altered_table_arrives_as_the_source_holds_it(mode: &str, id_type: &str, alter: &str) {
    forget_pooled_statements();
    let (t, _t) = table_of(
        "pool_t",
        &format!("id {id_type} PRIMARY KEY, v BIGINT NOT NULL, updated_at TIMESTAMP NOT NULL"),
        "g, g * 10, TIMESTAMP '2026-01-01 00:00:00' + g * INTERVAL '1 second'",
    );
    pooled_export_equals_the_source(&t, mode, "before the ALTER");
    ENGINE.exec(&alter.replace("{t}", &t));
    ENGINE.refresh_row_estimate(&t);
    pooled_export_equals_the_source(&t, mode, "after the ALTER");
}

const RETYPED: &str = "ALTER TABLE {t} ALTER COLUMN v TYPE DOUBLE PRECISION USING v + 0.5";
const GAINED: &str = "ALTER TABLE {t} ADD COLUMN note TEXT NOT NULL DEFAULT 'n'";
const KEY_WIDENED: &str = "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT";

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_second_table_through_one_transaction_pooler_arrives_as_itself_full_postgres() {
    let _alone = pgbouncer_alone();
    a_second_table_arrives_as_itself("full", SAME_WIDTH);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_second_table_through_one_transaction_pooler_arrives_as_itself_keyset_postgres() {
    let _alone = pgbouncer_alone();
    a_second_table_arrives_as_itself("keyset", SAME_WIDTH);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_second_table_through_one_transaction_pooler_arrives_as_itself_range_postgres() {
    let _alone = pgbouncer_alone();
    a_second_table_arrives_as_itself("range", SAME_WIDTH);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_second_table_through_one_transaction_pooler_arrives_as_itself_incremental_postgres() {
    let _alone = pgbouncer_alone();
    a_second_table_arrives_as_itself("incremental", SAME_WIDTH);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_wider_second_table_through_one_transaction_pooler_is_delivered_full_postgres() {
    let _alone = pgbouncer_alone();
    a_second_table_arrives_as_itself("full", WIDER);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_wider_second_table_through_one_transaction_pooler_is_delivered_keyset_postgres() {
    let _alone = pgbouncer_alone();
    a_second_table_arrives_as_itself("keyset", WIDER);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_wider_second_table_through_one_transaction_pooler_is_delivered_range_postgres() {
    let _alone = pgbouncer_alone();
    a_second_table_arrives_as_itself("range", WIDER);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_wider_second_table_through_one_transaction_pooler_is_delivered_incremental_postgres() {
    let _alone = pgbouncer_alone();
    a_second_table_arrives_as_itself("incremental", WIDER);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_retyped_column_through_one_transaction_pooler_arrives_as_the_source_holds_it_full_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("full", "BIGINT", RETYPED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_retyped_column_through_one_transaction_pooler_arrives_as_the_source_holds_it_keyset_postgres()
{
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("keyset", "BIGINT", RETYPED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_retyped_column_through_one_transaction_pooler_arrives_as_the_source_holds_it_range_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("range", "BIGINT", RETYPED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_retyped_column_through_one_transaction_pooler_arrives_as_the_source_holds_it_incremental_postgres()
 {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("incremental", "BIGINT", RETYPED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_gained_column_through_one_transaction_pooler_is_delivered_full_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("full", "BIGINT", GAINED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_gained_column_through_one_transaction_pooler_is_delivered_keyset_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("keyset", "BIGINT", GAINED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_gained_column_through_one_transaction_pooler_is_delivered_range_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("range", "BIGINT", GAINED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_gained_column_through_one_transaction_pooler_is_delivered_incremental_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("incremental", "BIGINT", GAINED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_widened_key_through_one_transaction_pooler_is_delivered_full_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("full", "INTEGER", KEY_WIDENED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_widened_key_through_one_transaction_pooler_is_delivered_keyset_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("keyset", "INTEGER", KEY_WIDENED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_widened_key_through_one_transaction_pooler_is_delivered_range_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("range", "INTEGER", KEY_WIDENED);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_widened_key_through_one_transaction_pooler_is_delivered_incremental_postgres() {
    let _alone = pgbouncer_alone();
    an_altered_table_arrives_as_the_source_holds_it("incremental", "INTEGER", KEY_WIDENED);
}

/// An enum, a domain over INTEGER and a composite type of this cell's own, dropped with the guard.
struct OwnTypes {
    stem: String,
}

impl OwnTypes {
    fn new() -> Self {
        let stem = unique_name("pool_ty");
        ENGINE.exec(&format!(
            "CREATE TYPE {stem}_e AS ENUM ('new', 'paid', 'void')"
        ));
        ENGINE.exec(&format!(
            "CREATE DOMAIN {stem}_d AS INTEGER CHECK (VALUE > 0)"
        ));
        ENGINE.exec(&format!("CREATE TYPE {stem}_c AS (n INTEGER, s TEXT)"));
        Self { stem }
    }
}

impl Drop for OwnTypes {
    fn drop(&mut self) {
        let stem = &self.stem;
        ENGINE.exec(&format!("DROP TYPE IF EXISTS {stem}_c CASCADE"));
        ENGINE.exec(&format!("DROP DOMAIN IF EXISTS {stem}_d CASCADE"));
        ENGINE.exec(&format!("DROP TYPE IF EXISTS {stem}_e CASCADE"));
    }
}

/// How long a run over a 20-row table may take before it is read as waiting on its own connection.
const BOUND: std::time::Duration = std::time::Duration::from_secs(60);
/// A text of 512,000 characters per row: a page of them does not arrive in one socket read.
const WIDE: &str = "repeat(md5(g::text), 16000)";

/// `rig` run once within `BOUND`; a run that is still going then is killed and fails the cell.
fn run_within_the_bound(rig: &Rig, mode: &str) -> std::process::Output {
    rig.run_with_envs_bounded(&[], BOUND).unwrap_or_else(|| {
        panic!("the {mode} export did not end within {BOUND:?}: it waits on its own connection")
    })
}

/// A table of an enum, a domain and wide rows, exported in `mode` over `url`: delivered as the source holds it, within `BOUND`.
fn an_enum_and_a_domain_are_delivered(mode: &str, url: &str) {
    if url == PGBOUNCER_URL {
        forget_pooled_statements();
    }
    let types = OwnTypes::new();
    let stem = &types.stem;
    let (table, _table) = table_of(
        "pool_types",
        &format!(
            "id BIGINT PRIMARY KEY, status {stem}_e NOT NULL, qty {stem}_d NOT NULL, \
             wide TEXT NOT NULL, updated_at TIMESTAMP NOT NULL"
        ),
        &format!(
            "g, (ARRAY['new', 'paid', 'void'])[1 + g % 3]::{stem}_e, g::{stem}_d, {WIDE}, \
             TIMESTAMP '2026-03-03 00:00:00' + g * INTERVAL '1 second'"
        ),
    );
    let rig = staged(ENGINE.rig(&table).source_url(url), mode);
    let out = run_within_the_bound(&rig, mode);
    assert!(
        out.status.success(),
        "the {mode} export of an enum and a domain failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    destination_equals_the_source(&rig, &table, mode, "an enum and a domain");
}

/// A column of a type rivet has no mapping for, beside wide rows, over `url`: refused with `needle`, within `BOUND`.
fn an_unmapped_type_is_refused_within_the_bound(
    url: &str,
    suffix: &str,
    value: &str,
    needle: &str,
) {
    if url == PGBOUNCER_URL {
        forget_pooled_statements();
    }
    let types = OwnTypes::new();
    let stem = &types.stem;
    let (table, _table) = table_of(
        "pool_unmapped",
        &format!("id BIGINT PRIMARY KEY, x {stem}{suffix} NOT NULL, wide TEXT NOT NULL"),
        &format!("g, {}, {WIDE}", value.replace("{stem}", stem)),
    );
    let rig = ENGINE.rig(&table).source_url(url);
    let out = run_within_the_bound(&rig, "full");
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && said.contains(needle),
        "a `{stem}{suffix}` column must be refused with `{needle}`:\n{said}"
    );
}

const COMPOSITE: (&str, &str, &str) = ("_c", "ROW(g, 'x')::{stem}_c", "no safe Rivet mapping");
const ENUM_ARRAY: (&str, &str, &str) = (
    "_e[]",
    "ARRAY['new', 'paid']::{stem}_e[]",
    "has no flat mapping",
);

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_table_of_an_enum_and_a_domain_with_wide_rows_is_delivered_full_direct_postgres() {
    an_enum_and_a_domain_are_delivered("full", POSTGRES_URL);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_table_of_an_enum_and_a_domain_with_wide_rows_through_one_transaction_pooler_is_delivered_full_postgres()
 {
    let _alone = pgbouncer_alone();
    an_enum_and_a_domain_are_delivered("full", PGBOUNCER_URL);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_table_of_an_enum_and_a_domain_with_wide_rows_is_delivered_keyset_direct_postgres() {
    an_enum_and_a_domain_are_delivered("keyset", POSTGRES_URL);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_table_of_an_enum_and_a_domain_with_wide_rows_through_one_transaction_pooler_is_delivered_keyset_postgres()
 {
    let _alone = pgbouncer_alone();
    an_enum_and_a_domain_are_delivered("keyset", PGBOUNCER_URL);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_table_of_an_enum_and_a_domain_with_wide_rows_is_delivered_range_direct_postgres() {
    an_enum_and_a_domain_are_delivered("range", POSTGRES_URL);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_table_of_an_enum_and_a_domain_with_wide_rows_through_one_transaction_pooler_is_delivered_range_postgres()
 {
    let _alone = pgbouncer_alone();
    an_enum_and_a_domain_are_delivered("range", PGBOUNCER_URL);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_table_of_an_enum_and_a_domain_with_wide_rows_is_delivered_incremental_direct_postgres() {
    an_enum_and_a_domain_are_delivered("incremental", POSTGRES_URL);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_table_of_an_enum_and_a_domain_with_wide_rows_through_one_transaction_pooler_is_delivered_incremental_postgres()
 {
    let _alone = pgbouncer_alone();
    an_enum_and_a_domain_are_delivered("incremental", PGBOUNCER_URL);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_composite_column_with_wide_rows_is_refused_within_the_bound_direct_postgres() {
    let (suffix, value, needle) = COMPOSITE;
    an_unmapped_type_is_refused_within_the_bound(POSTGRES_URL, suffix, value, needle);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn a_composite_column_with_wide_rows_through_one_transaction_pooler_is_refused_within_the_bound_postgres()
 {
    let _alone = pgbouncer_alone();
    let (suffix, value, needle) = COMPOSITE;
    an_unmapped_type_is_refused_within_the_bound(PGBOUNCER_URL, suffix, value, needle);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn an_array_of_an_enum_with_wide_rows_is_refused_within_the_bound_direct_postgres() {
    let (suffix, value, needle) = ENUM_ARRAY;
    an_unmapped_type_is_refused_within_the_bound(POSTGRES_URL, suffix, value, needle);
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn an_array_of_an_enum_with_wide_rows_through_one_transaction_pooler_is_refused_within_the_bound_postgres()
 {
    let _alone = pgbouncer_alone();
    let (suffix, value, needle) = ENUM_ARRAY;
    an_unmapped_type_is_refused_within_the_bound(PGBOUNCER_URL, suffix, value, needle);
}
