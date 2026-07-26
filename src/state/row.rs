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
//! Methods whose two arms have genuinely DIVERGENT SQL (not just placeholder
//! style) — e.g. `claim_next_chunk_task_at_ref`'s `FOR UPDATE SKIP LOCKED`, the
//! batch-vs-loop inserts — keep their explicit two-arm match; this seam is for the
//! ~40-50 sites that differ only in dialect ceremony.

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

impl rusqlite::types::ToSql for StateParam {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            StateParam::I64(v) => v.to_sql(),
            StateParam::OptI64(v) => v.to_sql(),
            StateParam::Text(v) => v.to_sql(),
            StateParam::OptText(v) => v.to_sql(),
            StateParam::Bool(v) => v.to_sql(),
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
        self.get(i)
    }
    fn opt_i64(&self, i: usize) -> Option<i64> {
        self.get(i)
    }
    fn opt_bool(&self, i: usize) -> Option<bool> {
        self.get(i)
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
