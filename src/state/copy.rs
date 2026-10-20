//! `rivet state copy`: a config's SQLite state into an empty PostgreSQL state, every table, in one
//! transaction, each table's row count checked before the commit; the SQLite file is only read.

use std::path::Path;

use rusqlite::types::ValueRef;

use crate::error::Result;

/// Tables a copy never carries: each backend's own schema version, and leases of live processes.
const NOT_COPIED: &[&str] = &["schema_version", "rivet_schema_version", "state_lease"];

/// Rows per `INSERT` statement.
const BATCH: usize = 200;

/// What one copy carried: each table and its rows, in copy order.
#[derive(Debug, Default)]
pub struct CopyReport {
    pub tables: Vec<(String, u64)>,
}

/// The SQLite file `config` keeps its state in and the PostgreSQL state to copy it into:
/// `--to`, else a PostgreSQL `RIVET_STATE_URL`.
pub fn endpoints(config: &str, to: Option<String>) -> Result<(std::path::PathBuf, String)> {
    endpoints_in(config, to, super::state_url_var())
}

/// [`endpoints`] over the value of `RIVET_STATE_URL`.
fn endpoints_in(
    config: &str,
    to: Option<String>,
    env: Option<String>,
) -> Result<(std::path::PathBuf, String)> {
    let target = match to {
        Some(url) => url,
        None => match env.as_deref() {
            Some(url) if super::postgres_state_url(url)?.is_some() => url.to_string(),
            _ => crate::rivet_bail!(
                crate::error::codes::STATE_COPY_TARGET_MISSING,
                "`rivet state copy` needs the PostgreSQL state to copy into: pass \
                 `--to postgresql://…`, or set RIVET_STATE_URL to it"
            ),
        },
    };
    if super::postgres_state_url(&target)?.is_none() {
        crate::rivet_bail!(
            crate::error::codes::STATE_COPY_TARGET_MISSING,
            "`--to` names a SQLite state; `rivet state copy` copies into a PostgreSQL state \
             (`postgresql://…`)"
        );
    }
    let source = env
        .as_deref()
        .and_then(|url| super::sqlite_state_path(url, config))
        .unwrap_or_else(|| super::config_dir_of(config).join(super::STATE_DB_NAME));
    Ok((source, target))
}

/// What the operator does after a copy: the state to name from now on, the way back, the checkpoint files.
pub fn done_note(
    source: &Path,
    target: &str,
    report: &CopyReport,
    checkpoints: &[std::path::PathBuf],
) -> String {
    let rows: u64 = report.tables.iter().map(|(_, n)| n).sum();
    let mut note = format!(
        "copied {} table(s), {rows} row(s), from `{}` to the PostgreSQL state {}.\n\
         Run every command of this config with RIVET_STATE_URL={} from now on. The SQLite file \
         is unchanged: it is the way back, and it no longer follows the runs.",
        report.tables.len(),
        std::fs::canonicalize(source)
            .unwrap_or_else(|_| source.to_path_buf())
            .display(),
        super::redact_pg_url(target),
        super::redact_pg_url(target),
    );
    if !checkpoints.is_empty() {
        note.push_str(
            "\nA CDC checkpoint is a file, not a state row: it stays where the config names it, \
             and the next run reads it there:",
        );
        for c in checkpoints {
            note.push_str(&format!("\n  {}", c.display()));
        }
    }
    note
}

/// Copy the SQLite state at `source` into the PostgreSQL state at `target_url`, which must hold no rows.
pub fn copy_sqlite_to_postgres(source: &Path, target_url: &str) -> Result<CopyReport> {
    if !super::holds_state(source) {
        crate::rivet_bail!(
            crate::error::codes::STATE_COPY_SOURCE_EMPTY,
            "`{}` holds no state to copy (no cursor and no run): name the config whose \
             `.rivet_state.db` holds it, or its `RIVET_STATE_URL=sqlite:<path>`",
            source.display()
        );
    }
    // An in-memory copy, brought to this release's schema; the source is only read.
    let from = in_memory_copy(source)?;
    super::migrations::migrate(&from)?;
    drop(super::StateStore::open_at_ref(&super::StateRef::Postgres(
        target_url.to_string(),
    ))?);
    let mut client = super::connect_pg(target_url)?;
    let mut tx = client.transaction().map_err(|e| pg_failed("begin", &e))?;
    let tables = sqlite_tables(&from)?;
    refuse_a_target_with_rows(&mut tx, target_url)?;
    let mut report = CopyReport::default();
    for table in &tables {
        let rows = copy_table(&from, &mut tx, table)?;
        report.tables.push((table.clone(), rows));
    }
    tx.commit().map_err(|e| pg_failed("commit", &e))?;
    Ok(report)
}

/// The SQLite state at `source`, its `-wal` included, copied into memory; the file is opened read-only.
fn in_memory_copy(source: &Path) -> Result<rusqlite::Connection> {
    let read_failed = |e: rusqlite::Error| {
        crate::error::coded(
            crate::error::codes::STATE_BACKEND_FAILED,
            format!("state copy: reading `{}`: {e}", source.display()),
        )
    };
    let mem = rusqlite::Connection::open_in_memory_with_flags(
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
            | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?;
    let uri = format!("file:{}?mode=ro", source.display());
    mem.execute("ATTACH DATABASE ?1 AS src", [uri])
        .map_err(read_failed)?;
    let objects: Vec<(String, String, String)> = {
        let mut stmt = mem
            .prepare(
                "SELECT type, name, sql FROM src.sqlite_master WHERE sql IS NOT NULL \
                 AND name NOT LIKE 'sqlite_%' ORDER BY type = 'table' DESC, rowid",
            )
            .map_err(read_failed)?;
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .map_err(read_failed)?
            .collect::<std::result::Result<_, _>>()
            .map_err(read_failed)?
    };
    for (kind, name, sql) in &objects {
        mem.execute_batch(sql).map_err(read_failed)?;
        if kind == "table" {
            mem.execute_batch(&format!(
                "INSERT INTO main.\"{name}\" SELECT * FROM src.\"{name}\""
            ))
            .map_err(read_failed)?;
        }
    }
    mem.execute_batch("DETACH DATABASE src")
        .map_err(read_failed)?;
    Ok(mem)
}

/// The state tables of a SQLite state, in name order.
fn sqlite_tables(conn: &rusqlite::Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
         ORDER BY name",
    )?;
    let names = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(names
        .into_iter()
        .filter(|n| !NOT_COPIED.contains(&n.as_str()))
        .collect())
}

/// Refuse a target that holds any state row: a copy into it would mix two states.
fn refuse_a_target_with_rows(tx: &mut postgres::Transaction<'_>, url: &str) -> Result<()> {
    let tables: Vec<String> = tx
        .query(
            "SELECT table_name::text FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' \
             ORDER BY table_name",
            &[],
        )
        .map_err(|e| pg_failed("list the target's tables", &e))?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .filter(|t| !NOT_COPIED.contains(&t.as_str()))
        .collect();
    let mut held = Vec::new();
    for t in tables {
        let has: bool = tx
            .query_one(&format!("SELECT EXISTS (SELECT 1 FROM \"{t}\")"), &[])
            .map_err(|e| pg_failed(&format!("read `{t}`"), &e))?
            .get(0);
        if has {
            held.push(t);
        }
    }
    if held.is_empty() {
        return Ok(());
    }
    crate::rivet_bail!(
        crate::error::codes::STATE_COPY_TARGET_NOT_EMPTY,
        "the PostgreSQL state {} already holds state rows (in {}): a copy into it would mix two \
         states, and the runs of one would continue from the other's progress. Nothing was \
         copied. Copy into a database (or schema) of its own",
        super::redact_pg_url(url),
        held.join(", ")
    )
}

/// One column of a target table: its name and the type its values are cast to.
struct Column {
    name: String,
    cast: String,
}

/// Copy every row of `table`, checking the target's count; the rows copied.
fn copy_table(
    from: &rusqlite::Connection,
    tx: &mut postgres::Transaction<'_>,
    table: &str,
) -> Result<u64> {
    let source_rows: i64 =
        from.query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |r| {
            r.get(0)
        })?;
    let target = target_columns(tx, table)?;
    let source = sqlite_columns(from, table)?;
    if target.is_empty() {
        if source_rows == 0 {
            return Ok(0);
        }
        return Err(no_place(table, "the table"));
    }
    let mut columns = Vec::new();
    for name in &source {
        match target.iter().find(|c| &c.name == name) {
            Some(c) => columns.push(c),
            None => {
                let filled: i64 = from.query_row(
                    &format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{name}\" IS NOT NULL"),
                    [],
                    |r| r.get(0),
                )?;
                if filled > 0 {
                    return Err(no_place(table, &format!("column `{name}`")));
                }
            }
        }
    }
    let list = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = from.prepare(&format!("SELECT {list} FROM \"{table}\""))?;
    let mut rows = stmt.query([])?;
    let mut batch: Vec<Option<String>> = Vec::new();
    let mut copied = 0u64;
    while let Some(row) = rows.next()? {
        for (i, c) in columns.iter().enumerate() {
            batch.push(text_of(row.get_ref(i)?, &c.cast, table, &c.name)?);
        }
        if batch.len() == BATCH * columns.len() {
            copied += insert(tx, table, &list, &columns, &batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        copied += insert(tx, table, &list, &columns, &batch)?;
    }
    let landed: i64 = tx
        .query_one(&format!("SELECT COUNT(*) FROM \"{table}\""), &[])
        .map_err(|e| pg_failed(&format!("count `{table}`"), &e))?
        .get(0);
    if landed != source_rows {
        crate::rivet_bail!(
            crate::error::codes::STATE_COPY_COUNT_MISMATCH,
            "state copy: `{table}` holds {source_rows} row(s) in the SQLite state and {landed} in \
             the PostgreSQL state after the copy; nothing was committed"
        );
    }
    resume_sequences(tx, table, &columns)?;
    Ok(copied)
}

/// Why a SQLite table's rows have no place in this release's PostgreSQL state.
fn no_place(table: &str, what: &str) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::INTERNAL_RUN_INVARIANT,
        format!(
            "state copy: {what} of `{table}` holds rows in the SQLite state and does not exist in \
             this release's PostgreSQL state — a bug; nothing was committed"
        ),
    )
}

/// The columns of `table` in the target, with the type each value is cast to.
fn target_columns(tx: &mut postgres::Transaction<'_>, table: &str) -> Result<Vec<Column>> {
    Ok(tx
        .query(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
             WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&format!("\"{table}\"")],
        )
        .map_err(|e| pg_failed(&format!("read the columns of `{table}`"), &e))?
        .iter()
        .map(|r| Column {
            name: r.get(0),
            cast: r.get(1),
        })
        .collect())
}

/// The columns of a SQLite table, in their order.
fn sqlite_columns(conn: &rusqlite::Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))?;
    let names = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(names)
}

/// A SQLite value as the text PostgreSQL casts to `cast`.
fn text_of(v: ValueRef<'_>, cast: &str, table: &str, column: &str) -> Result<Option<String>> {
    Ok(match v {
        ValueRef::Null => None,
        ValueRef::Integer(i) => Some(i.to_string()),
        ValueRef::Real(f) => Some(f.to_string()),
        ValueRef::Text(t) => Some(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) if cast == "bytea" => Some(format!(
            "\\x{}",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        )),
        ValueRef::Blob(b) => Some(String::from_utf8(b.to_vec()).map_err(|_| {
            crate::error::coded(
                crate::error::codes::STATE_FILE_CORRUPT,
                format!(
                    "state copy: `{table}.{column}` holds bytes that are not text, and its \
                     PostgreSQL column is `{cast}`; nothing was committed"
                ),
            )
        })?),
    })
}

/// Insert `values` (whole rows of `columns`) into `table`; the rows inserted.
fn insert(
    tx: &mut postgres::Transaction<'_>,
    table: &str,
    list: &str,
    columns: &[&Column],
    values: &[Option<String>],
) -> Result<u64> {
    let width = columns.len();
    let rows = values.len() / width;
    let tuples = (0..rows)
        .map(|r| {
            let cells = columns
                .iter()
                .enumerate()
                .map(|(i, c)| format!("${}::text::{}", r * width + i + 1, c.cast))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({cells})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let params: Vec<&(dyn postgres::types::ToSql + Sync)> = values
        .iter()
        .map(|v| v as &(dyn postgres::types::ToSql + Sync))
        .collect();
    tx.execute(
        &format!("INSERT INTO \"{table}\" ({list}) VALUES {tuples}"),
        &params,
    )
    .map_err(|e| pg_failed(&format!("insert into `{table}`"), &e))
}

/// Move each serial column's sequence past the ids the copy wrote, so the next insert does not collide.
fn resume_sequences(
    tx: &mut postgres::Transaction<'_>,
    table: &str,
    columns: &[&Column],
) -> Result<()> {
    for c in columns {
        let seq: Option<String> = tx
            .query_one(
                "SELECT pg_get_serial_sequence($1, $2)",
                &[&format!("\"{table}\""), &c.name],
            )
            .map_err(|e| pg_failed(&format!("read the sequence of `{table}`"), &e))?
            .get(0);
        let Some(seq) = seq else { continue };
        tx.execute(
            &format!(
                "SELECT setval($1::text::regclass, COALESCE((SELECT MAX(\"{n}\") FROM \"{table}\"), 1), \
                 (SELECT MAX(\"{n}\") FROM \"{table}\") IS NOT NULL)",
                n = c.name
            ),
            &[&seq],
        )
        .map_err(|e| pg_failed(&format!("resume the sequence of `{table}`"), &e))?;
    }
    Ok(())
}

/// A PostgreSQL failure during the copy; nothing was committed.
fn pg_failed(what: &str, e: &postgres::Error) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::STATE_BACKEND_FAILED,
        format!(
            "state copy: {what}: {}; nothing was committed",
            super::pg_detail(e)
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const PG: &str = "postgresql://u@h/db";

    /// `--to` wins over `RIVET_STATE_URL`; the source is the `sqlite:` file when one is named, else the file beside the config.
    #[test]
    fn the_target_is_to_or_a_postgres_state_url_and_the_source_the_configs_sqlite_file() {
        let at = |to: Option<&str>, env: Option<&str>| {
            endpoints_in(
                "/c/rivet.yaml",
                to.map(str::to_string),
                env.map(str::to_string),
            )
            .map(|(s, t)| (s.display().to_string(), t))
        };
        assert_eq!(
            at(Some(PG), None).unwrap(),
            ("/c/.rivet_state.db".to_string(), PG.to_string())
        );
        assert_eq!(
            at(None, Some(PG)).unwrap(),
            ("/c/.rivet_state.db".to_string(), PG.to_string())
        );
        assert_eq!(
            at(Some(PG), Some("sqlite:app.db")).unwrap(),
            ("/c/app.db".to_string(), PG.to_string())
        );
        for (to, env) in [
            (None, None),
            (None, Some("sqlite:app.db")),
            (Some("sqlite:x.db"), None),
        ] {
            let err = at(to, env).unwrap_err();
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_STATE_COPY_TARGET_MISSING"),
                "{to:?} {env:?}: {err:#}"
            );
            assert_eq!(crate::error::classify_exit(&err), 1);
        }
    }

    /// A file with no cursor and no run is not a state to copy, refused before any connection.
    #[test]
    fn an_empty_source_is_refused_before_the_target_is_touched() {
        let dir = tempfile::tempdir().unwrap();
        let err = copy_sqlite_to_postgres(
            &dir.path().join("none.db"),
            "postgresql://nobody@127.0.0.1:1/x",
        )
        .unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_COPY_SOURCE_EMPTY")
        );
    }

    /// A SQLite value is the text PostgreSQL casts to the column's type: bytes as hex for `bytea`.
    #[test]
    fn each_sqlite_value_becomes_the_text_its_column_casts() {
        let t = |v: ValueRef<'_>, cast: &str| text_of(v, cast, "t", "c").unwrap();
        assert_eq!(t(ValueRef::Null, "text"), None);
        assert_eq!(t(ValueRef::Integer(-7), "bigint").as_deref(), Some("-7"));
        assert_eq!(t(ValueRef::Integer(1), "boolean").as_deref(), Some("1"));
        assert_eq!(
            t(ValueRef::Real(0.5), "double precision").as_deref(),
            Some("0.5")
        );
        assert_eq!(
            t(ValueRef::Text(b"caf\xc3\xa9"), "text").as_deref(),
            Some("café")
        );
        assert_eq!(
            t(ValueRef::Blob(&[0, 255]), "bytea").as_deref(),
            Some("\\x00ff")
        );
        assert_eq!(t(ValueRef::Blob(b"ok"), "text").as_deref(), Some("ok"));
        let err = text_of(ValueRef::Blob(&[0xff]), "text", "t", "c").unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_FILE_CORRUPT")
        );
    }
}
