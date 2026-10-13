//! The SQLite-vs-Postgres dispatch seam for the state layer.
//!
//! Every store method used to hand-write `match &self.conn { Sqlite(..) => …,
//! Postgres(..) => … }` — 66 sites across 14 files — duplicating the `pg_sql()`
//! placeholder translation, the `borrow_mut()`, the `as usize` cast, AND (worst)
//! the per-column row extraction, which was written once per backend so adding a
//! column meant editing both arms.
//!
//! This module concentrates that: [`StateRow`] abstracts column access over both
//! `rusqlite::Row` and `postgres::Row`, [`StateParam`] carries a bound value to
//! either backend (working around the foreign `ToSql` traits), and
//! [`StateStore::query`] / [`query_opt`] / [`execute`] run a statement on the
//! right backend so a caller writes its SQL + params + ONE extraction closure.
//!
//! [`StateStore::transaction`] is the seam's unit of work: seam statements run
//! inside its closure commit together or not at all, on either backend.
//!
//! Methods whose two arms have genuinely DIVERGENT SQL (not just placeholder
//! style) — `claim_next_chunk_task`'s `FOR UPDATE SKIP LOCKED` vs SQLite's rowid +
//! IMMEDIATE transaction — keep their explicit two-arm match; this seam is for
//! every site that differs only in dialect ceremony.

use crate::error::Result;

use super::{StateConn, StateStore, pg_sql};

/// A bound parameter, dispatched to whichever backend runs the statement. Works
/// around `rusqlite::ToSql` and `postgres::ToSql` being foreign traits we cannot
/// blanket-unify: the inner value's own `ToSql` impl is what actually binds.
#[derive(Debug)]
pub(super) enum StateParam {
    I64(i64),
    OptI64(Option<i64>),
    Text(String),
    OptText(Option<String>),
    Bool(bool),
    OptBool(Option<bool>),
    OptF64(Option<f64>),
}

impl From<i64> for StateParam {
    fn from(v: i64) -> Self {
        StateParam::I64(v)
    }
}
impl From<Option<i64>> for StateParam {
    fn from(v: Option<i64>) -> Self {
        StateParam::OptI64(v)
    }
}
impl From<String> for StateParam {
    fn from(v: String) -> Self {
        StateParam::Text(v)
    }
}
impl From<&str> for StateParam {
    fn from(v: &str) -> Self {
        StateParam::Text(v.to_string())
    }
}
impl From<Option<String>> for StateParam {
    fn from(v: Option<String>) -> Self {
        StateParam::OptText(v)
    }
}
impl From<Option<&str>> for StateParam {
    fn from(v: Option<&str>) -> Self {
        StateParam::OptText(v.map(str::to_string))
    }
}
impl From<bool> for StateParam {
    fn from(v: bool) -> Self {
        StateParam::Bool(v)
    }
}
impl From<Option<bool>> for StateParam {
    fn from(v: Option<bool>) -> Self {
        StateParam::OptBool(v)
    }
}
impl From<Option<f64>> for StateParam {
    fn from(v: Option<f64>) -> Self {
        StateParam::OptF64(v)
    }
}

impl rusqlite::types::ToSql for StateParam {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            StateParam::I64(v) => v.to_sql(),
            StateParam::OptI64(v) => v.to_sql(),
            StateParam::Text(v) => v.to_sql(),
            StateParam::OptText(v) => v.to_sql(),
            StateParam::Bool(v) => v.to_sql(),
            StateParam::OptBool(v) => v.to_sql(),
            StateParam::OptF64(v) => v.to_sql(),
        }
    }
}

/// Borrow a `StateParam` slice as Postgres bind parameters. Each inner value is
/// itself `ToSql + Sync`, so no impl on `StateParam` is needed on this side.
fn pg_params(params: &[StateParam]) -> Vec<&(dyn postgres::types::ToSql + Sync)> {
    params
        .iter()
        .map(|p| -> &(dyn postgres::types::ToSql + Sync) {
            match p {
                StateParam::I64(v) => v,
                StateParam::OptI64(v) => v,
                StateParam::Text(v) => v,
                StateParam::OptText(v) => v,
                StateParam::Bool(v) => v,
                StateParam::OptBool(v) => v,
                StateParam::OptF64(v) => v,
            }
        })
        .collect()
}

/// Column access over either backend's row. Accessors panic on a type mismatch —
/// the state schema is fixed, so a mismatch is a programmer error, matching
/// `postgres::Row::get`'s own contract (and rusqlite's `.unwrap()` here).
pub(super) trait StateRow {
    fn text(&self, i: usize) -> String;
    fn opt_text(&self, i: usize) -> Option<String>;
    fn i64(&self, i: usize) -> i64;
    fn opt_i64(&self, i: usize) -> Option<i64>;
    fn opt_bool(&self, i: usize) -> Option<bool>;
}

impl StateRow for rusqlite::Row<'_> {
    fn text(&self, i: usize) -> String {
        self.get(i).unwrap()
    }
    fn opt_text(&self, i: usize) -> Option<String> {
        self.get(i).unwrap()
    }
    fn i64(&self, i: usize) -> i64 {
        self.get(i).unwrap()
    }
    fn opt_i64(&self, i: usize) -> Option<i64> {
        self.get(i).unwrap()
    }
    fn opt_bool(&self, i: usize) -> Option<bool> {
        self.get(i).unwrap()
    }
}

impl StateRow for postgres::Row {
    fn text(&self, i: usize) -> String {
        self.get(i)
    }
    fn opt_text(&self, i: usize) -> Option<String> {
        self.get(i)
    }
    fn i64(&self, i: usize) -> i64 {
        pg_int(self, i)
            .unwrap_or_else(|| panic!("state column {} is NULL", self.columns()[i].name()))
    }
    fn opt_i64(&self, i: usize) -> Option<i64> {
        pg_int(self, i)
    }
    fn opt_bool(&self, i: usize) -> Option<bool> {
        self.get(i)
    }
}

/// Read Postgres column `i` of any integer width (INT2/INT4/INT8) as i64; any other type panics naming the column and its type.
fn pg_int(row: &postgres::Row, i: usize) -> Option<i64> {
    use postgres::types::Type;
    let col = &row.columns()[i];
    match *col.type_() {
        Type::INT2 => row.get::<_, Option<i16>>(i).map(i64::from),
        Type::INT4 => row.get::<_, Option<i32>>(i).map(i64::from),
        Type::INT8 => row.get(i),
        ref other => panic!(
            "state column {} is {other}, not an integer (INT2/INT4/INT8)",
            col.name()
        ),
    }
}

impl StateStore {
    /// Run a SELECT on the active backend and map every row through `map` — the
    /// projection is written ONCE regardless of backend. `sql` uses `?N`
    /// placeholders (translated to `$N` for Postgres).
    pub(super) fn query<T>(
        &self,
        sql: &str,
        params: &[StateParam],
        map: impl Fn(&dyn StateRow) -> T,
    ) -> Result<Vec<T>> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(sql)?;
                let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
                    Ok(map(row))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(&pg_sql(sql), &pg_params(params))?;
                Ok(rows.iter().map(|row| map(row)).collect())
            }
        }
    }

    /// `query` for at-most-one row — the first row, or `None`.
    pub(super) fn query_opt<T>(
        &self,
        sql: &str,
        params: &[StateParam],
        map: impl Fn(&dyn StateRow) -> T,
    ) -> Result<Option<T>> {
        Ok(self.query(sql, params, map)?.into_iter().next())
    }

    /// Run an INSERT/UPDATE/DELETE on the active backend; returns rows affected.
    pub(super) fn execute(&self, sql: &str, params: &[StateParam]) -> Result<usize> {
        match &self.conn {
            StateConn::Sqlite(c) => Ok(c.execute(sql, rusqlite::params_from_iter(params.iter()))?),
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                Ok(c.execute(&pg_sql(sql), &pg_params(params))? as usize)
            }
        }
    }

    /// Run `work` as one unit of work: its statements commit together on `Ok` and roll back on `Err` or panic; a nested call joins the open one. On SQLite it takes the write lock first, so a unit that reads before it writes waits for another writer.
    pub(super) fn transaction<T>(&self, work: impl FnOnce() -> Result<T>) -> Result<T> {
        if self.in_tx.get() {
            return work();
        }
        self.batch(match &self.conn {
            StateConn::Sqlite(_) => "BEGIN IMMEDIATE",
            StateConn::Postgres(_) => "BEGIN",
        })?;
        self.in_tx.set(true);
        let open = OpenTx(self);
        let out = work()?;
        self.batch("COMMIT")?;
        self.in_tx.set(false);
        drop(open);
        Ok(out)
    }

    /// Run a parameterless statement on the active backend.
    fn batch(&self, sql: &str) -> Result<()> {
        match &self.conn {
            StateConn::Sqlite(c) => Ok(c.execute_batch(sql)?),
            StateConn::Postgres(client) => Ok(client.borrow_mut().batch_execute(sql)?),
        }
    }
}

/// An open unit of work; dropping it before `COMMIT` succeeded rolls the transaction back.
struct OpenTx<'a>(&'a StateStore);

impl Drop for OpenTx<'_> {
    /// Roll back when the unit of work did not commit.
    fn drop(&mut self) {
        if self.0.in_tx.replace(false) {
            let _ = self.0.batch("ROLLBACK");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unit of work that reads before it writes holds the write lock from its start: another writer of the state file waits, and its own write lands.
    #[test]
    fn a_unit_of_work_that_reads_first_writes_beside_another_writer() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("rivet.yaml");
        std::fs::write(&cfg, "# test").unwrap();
        let s = StateStore::open(cfg.to_str().unwrap()).unwrap();
        let other = rusqlite::Connection::open(dir.path().join(".rivet_state.db")).unwrap();
        let row = "INSERT INTO export_state (export_name, last_cursor_value, last_run_at) \
                   VALUES (?1, '1', 'now')";
        let done = s.transaction(|| {
            s.query_opt("SELECT COUNT(*) FROM export_state", &[], |r| r.i64(0))?;
            let other_is_held = other.execute(row, ["theirs"]).is_err();
            s.execute(row, &["ours".into()])?;
            Ok(other_is_held)
        });
        assert!(
            matches!(done, Ok(true)),
            "a unit of work that read first lost its write to another writer, or let that writer in: {done:?}"
        );
    }

    #[test]
    fn query_execute_round_trip_over_the_seam() {
        let s = StateStore::open_in_memory().unwrap();
        // Reuse an existing table (export_state) to exercise the seam end to end.
        let n = s
            .execute(
                "INSERT INTO export_state (export_name, last_cursor_value, last_run_at) \
                 VALUES (?1, ?2, ?3)",
                &[
                    "orders".into(),
                    Some("100".to_string()).into(),
                    "now".into(),
                ],
            )
            .unwrap();
        assert_eq!(n, 1);

        let got: Option<(String, Option<String>)> = s
            .query_opt(
                "SELECT export_name, last_cursor_value FROM export_state WHERE export_name = ?1",
                &["orders".into()],
                |r| (r.text(0), r.opt_text(1)),
            )
            .unwrap();
        assert_eq!(got, Some(("orders".to_string(), Some("100".to_string()))));

        // A missing row yields None, an empty list from query.
        let none: Option<i64> = s
            .query_opt(
                "SELECT 1 FROM export_state WHERE export_name = ?1",
                &["nope".into()],
                |r| r.i64(0),
            )
            .unwrap();
        assert_eq!(none, None);
    }

    /// The Postgres accessor reads every integer width, NULL as None, and refuses a non-integer by name and type.
    #[test]
    fn pg_accessor_reads_every_integer_width_and_refuses_other_types() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let mut client = super::super::connect_pg(&url).expect("connect pg state");
        let row = client
            .query_one(
                "SELECT CAST(-7 AS INT2) AS a, 70000 AS b, CAST(5000000000 AS INT8) AS c, \
                 CAST(NULL AS INT4) AS d, 'x' AS e",
                &[],
            )
            .unwrap();
        let r: &dyn StateRow = &row;
        assert_eq!((r.i64(0), r.i64(1), r.i64(2)), (-7, 70000, 5_000_000_000));
        assert_eq!((r.opt_i64(1), r.opt_i64(3)), (Some(70000), None));
        let msg = |f: &dyn Fn()| {
            let e = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_err();
            e.downcast_ref::<String>().cloned().unwrap_or_default()
        };
        assert_eq!(
            msg(&|| {
                r.i64(3);
            }),
            "state column d is NULL"
        );
        assert_eq!(
            msg(&|| {
                r.opt_i64(4);
            }),
            "state column e is text, not an integer (INT2/INT4/INT8)"
        );
    }

    const INSERT: &str = "INSERT INTO export_state (export_name, last_cursor_value, last_run_at) \
                          VALUES (?1, NULL, 'now')";

    /// Rows in `export_state`, as `store` sees them.
    fn count(store: &StateStore) -> i64 {
        store
            .query_opt("SELECT COUNT(*) FROM export_state", &[], |r| r.i64(0))
            .unwrap()
            .unwrap()
    }

    #[test]
    fn a_committed_unit_of_work_is_visible_to_another_connection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let a = StateStore::open_at_path(&path).unwrap();
        a.transaction(|| {
            a.execute(INSERT, &["x".into()])?;
            a.execute(INSERT, &["y".into()])?;
            Ok(())
        })
        .unwrap();
        assert_eq!(count(&StateStore::open_at_path(&path).unwrap()), 2);
    }

    #[test]
    fn a_failed_unit_of_work_rolls_back_its_earlier_statements() {
        let s = StateStore::open_in_memory().unwrap();
        let r = s.transaction(|| -> Result<()> {
            s.execute(INSERT, &["a".into()])?;
            Err(anyhow::anyhow!("the second statement failed"))
        });
        assert!(r.is_err());
        assert_eq!(count(&s), 0, "the first statement must not survive");
        s.transaction(|| s.execute(INSERT, &["b".into()]).map(drop))
            .unwrap();
        assert_eq!(count(&s), 1, "the store opens a fresh unit afterwards");
    }

    #[test]
    fn a_nested_unit_of_work_joins_the_outer_one() {
        let s = StateStore::open_in_memory().unwrap();
        let r = s.transaction(|| -> Result<()> {
            s.transaction(|| s.execute(INSERT, &["inner".into()]).map(drop))?;
            Err(anyhow::anyhow!(
                "the outer unit fails after the inner one returned"
            ))
        });
        assert!(r.is_err());
        assert_eq!(
            count(&s),
            0,
            "the inner write rolls back with the outer unit"
        );
    }

    #[test]
    fn a_panicking_unit_of_work_rolls_back_and_the_store_stays_usable() {
        let s = StateStore::open_in_memory().unwrap();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = s.transaction(|| -> Result<()> {
                s.execute(INSERT, &["p".into()])?;
                panic!("worker panicked mid-unit")
            });
        }));
        assert!(r.is_err());
        assert_eq!(count(&s), 0);
        s.transaction(|| s.execute(INSERT, &["after".into()]).map(drop))
            .unwrap();
        assert_eq!(count(&s), 1);
    }
}
