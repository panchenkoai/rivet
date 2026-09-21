use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::types::{RivetType, TypeFidelity, TypeMapping, rivet_type_to_arrow};

use super::StateStore;

/// One column of a recorded load spec: the type the extractor resolved and wrote.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadSpecColumn {
    pub name: String,
    pub source_type: String,
    pub rivet_type: RivetType,
    pub fidelity: TypeFidelity,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl LoadSpecColumn {
    /// The recorded form of a resolved mapping.
    pub fn from_mapping(m: &TypeMapping) -> Self {
        Self {
            name: m.column_name.clone(),
            source_type: m.source_native_type.clone(),
            rivet_type: m.rivet_type.clone(),
            fidelity: m.fidelity,
            nullable: m.nullable,
            warnings: m.warnings.clone(),
        }
    }

    /// The mapping this column stands for, its Arrow type re-derived from the Rivet type.
    pub fn to_mapping(&self) -> TypeMapping {
        TypeMapping {
            column_name: self.name.clone(),
            source_native_type: self.source_type.clone(),
            rivet_type: self.rivet_type.clone(),
            arrow_type: rivet_type_to_arrow(&self.rivet_type),
            fidelity: self.fidelity,
            nullable: self.nullable,
            warnings: self.warnings.clone(),
        }
    }
}

/// What the load needs about one unit of an export, as recorded in the state DB.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadSpec {
    pub export_name: String,
    pub unit: Option<String>,
    pub columns: Vec<LoadSpecColumn>,
    pub primary_key: Option<Vec<String>>,
    pub run_id: Option<String>,
    pub origin: String,
    pub captured_at: String,
}

impl StateStore {
    /// Upsert the columns and primary key a successful run captured for one unit. A
    /// capture that found no key clears one a run recorded before it — the export no
    /// longer reads the relation that key belonged to — and keeps one `rivet init`
    /// recorded (`key_origin = 'init'`), the scaffold's declaration for a `query:` export.
    pub fn record_load_spec(
        &self,
        export_name: &str,
        unit: Option<&str>,
        columns: &[LoadSpecColumn],
        primary_key: Option<&[String]>,
        run_id: &str,
    ) -> Result<()> {
        let columns_json = serde_json::to_string(columns)?;
        let primary_key_json = primary_key.map(serde_json::to_string).transpose()?;
        // Bound, not `CASE WHEN ?4 IS NULL`: Postgres cannot type a parameter from `IS NULL`.
        let key_origin = primary_key_json.as_ref().map(|_| "run".to_string());
        let now = chrono::Utc::now().to_rfc3339();
        self.execute(
            "INSERT INTO export_load_spec
                 (export_name, unit, columns_json, primary_key_json, key_origin,
                  run_id, origin, captured_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'run', ?7)
             ON CONFLICT (export_name, unit) DO UPDATE SET
                 columns_json     = excluded.columns_json,
                 primary_key_json = CASE
                     WHEN excluded.primary_key_json IS NOT NULL THEN excluded.primary_key_json
                     WHEN export_load_spec.key_origin = 'init' THEN export_load_spec.primary_key_json
                     ELSE NULL END,
                 key_origin       = CASE
                     WHEN excluded.primary_key_json IS NOT NULL THEN 'run'
                     WHEN export_load_spec.key_origin = 'init' THEN 'init'
                     ELSE NULL END,
                 run_id           = excluded.run_id,
                 origin           = excluded.origin,
                 captured_at      = excluded.captured_at",
            &[
                export_name.into(),
                unit.unwrap_or("").into(),
                columns_json.clone().into(),
                primary_key_json.clone().into(),
                key_origin.into(),
                run_id.into(),
                now.clone().into(),
            ],
        )?;
        // The same spec under ITS run: what `rivet load` pins a plan to, so a
        // same-named export of another config sharing this state DB cannot type it.
        self.execute(
            "INSERT INTO export_load_spec_run
                 (export_name, unit, run_id, columns_json, primary_key_json, captured_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (export_name, unit, run_id) DO UPDATE SET
                 columns_json     = excluded.columns_json,
                 primary_key_json = excluded.primary_key_json,
                 captured_at      = excluded.captured_at",
            &[
                export_name.into(),
                unit.unwrap_or("").into(),
                run_id.into(),
                columns_json.into(),
                primary_key_json.into(),
                now.into(),
            ],
        )?;
        Ok(())
    }

    /// The spec `run_id` recorded for one unit — `None` for a run that predates the
    /// per-run table or recorded nothing. Private: the one reader is
    /// [`Self::load_spec_of_run_with_init_key`], which also borrows the init-origin
    /// key — a caller reaching for this one would skip that (ADR-0034 D1).
    fn load_spec_of_run(
        &self,
        export_name: &str,
        unit: Option<&str>,
        run_id: &str,
    ) -> Result<Option<LoadSpec>> {
        let row = self.query_opt(
            "SELECT columns_json, primary_key_json, captured_at FROM export_load_spec_run
             WHERE export_name = ?1 AND unit = ?2 AND run_id = ?3",
            &[export_name.into(), unit.unwrap_or("").into(), run_id.into()],
            |r| (r.text(0), r.opt_text(1), r.text(2)),
        )?;
        let Some((columns_json, primary_key_json, captured_at)) = row else {
            return Ok(None);
        };
        Ok(Some(LoadSpec {
            export_name: export_name.to_string(),
            unit: unit.map(str::to_string),
            columns: serde_json::from_str(&columns_json).with_context(|| {
                format!("export '{export_name}' run '{run_id}': unreadable load spec columns")
            })?,
            primary_key: primary_key_json
                .map(|p| serde_json::from_str(&p))
                .transpose()
                .with_context(|| {
                    format!(
                        "export '{export_name}' run '{run_id}': unreadable load spec primary key"
                    )
                })?,
            run_id: Some(run_id.to_string()),
            origin: "run".to_string(),
            captured_at,
        }))
    }

    /// [`Self::load_spec_of_run`], with the ONE key that may legitimately come from
    /// outside the run: the key `rivet init` recorded (`key_origin = 'init'`) — a
    /// `query:` export cannot read a key at run time, so its runs record none and
    /// the scaffold's declaration is the only one there is. A key another RUN wrote
    /// into the by-name row is never borrowed: that is the last-writer race the
    /// per-run spec exists to escape. The init key itself is per NAME: on a state DB
    /// shared by two configs, the last `rivet init` of that export name wins.
    pub fn load_spec_of_run_with_init_key(
        &self,
        export_name: &str,
        unit: Option<&str>,
        run_id: &str,
    ) -> Result<Option<LoadSpec>> {
        let Some(mut spec) = self.load_spec_of_run(export_name, unit, run_id)? else {
            return Ok(None);
        };
        if spec.primary_key.is_none() {
            let init_key = self.query_opt(
                "SELECT primary_key_json FROM export_load_spec
                 WHERE export_name = ?1 AND unit = ?2 AND key_origin = 'init'",
                &[export_name.into(), unit.unwrap_or("").into()],
                |r| r.opt_text(0),
            )?;
            if let Some(Some(json)) = init_key {
                spec.primary_key = Some(serde_json::from_str(&json).with_context(|| {
                    format!("export '{export_name}': unreadable init-recorded primary key")
                })?);
            }
        }
        Ok(Some(spec))
    }

    /// Record the source primary key `rivet init` read for an export it scaffolded,
    /// leaving any columns a run recorded in place.
    pub fn record_primary_key(
        &self,
        export_name: &str,
        unit: Option<&str>,
        primary_key: &[String],
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.execute(
            "INSERT INTO export_load_spec
                 (export_name, unit, columns_json, primary_key_json, key_origin,
                  run_id, origin, captured_at)
             VALUES (?1, ?2, NULL, ?3, 'init', NULL, 'init', ?4)
             ON CONFLICT (export_name, unit) DO UPDATE SET
                 primary_key_json = excluded.primary_key_json,
                 key_origin       = 'init'",
            &[
                export_name.into(),
                unit.unwrap_or("").into(),
                serde_json::to_string(primary_key)?.into(),
                now.into(),
            ],
        )?;
        Ok(())
    }

    /// The recorded spec for one unit of `export_name`, or `None` when nothing recorded one.
    pub fn load_spec(&self, export_name: &str, unit: Option<&str>) -> Result<Option<LoadSpec>> {
        let row = self.query_opt(
            "SELECT columns_json, primary_key_json, run_id, origin, captured_at
             FROM export_load_spec WHERE export_name = ?1 AND unit = ?2",
            &[export_name.into(), unit.unwrap_or("").into()],
            |r| {
                (
                    r.opt_text(0),
                    r.opt_text(1),
                    r.opt_text(2),
                    r.text(3),
                    r.text(4),
                )
            },
        )?;
        let Some((columns_json, primary_key_json, run_id, origin, captured_at)) = row else {
            return Ok(None);
        };
        let columns = match columns_json {
            Some(c) => serde_json::from_str(&c)
                .with_context(|| format!("export '{export_name}': unreadable load spec columns"))?,
            None => Vec::new(),
        };
        let primary_key = primary_key_json
            .map(|p| serde_json::from_str(&p))
            .transpose()
            .with_context(|| format!("export '{export_name}': unreadable load spec primary key"))?;
        Ok(Some(LoadSpec {
            export_name: export_name.to_string(),
            unit: unit.map(str::to_string),
            columns,
            primary_key,
            run_id,
            origin,
            captured_at,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TimeUnit;

    fn col(name: &str, rivet_type: RivetType) -> LoadSpecColumn {
        LoadSpecColumn {
            name: name.into(),
            source_type: "native".into(),
            rivet_type,
            fidelity: TypeFidelity::Exact,
            nullable: true,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn a_recorded_spec_reads_back_with_every_type_intact() {
        let s = StateStore::open_in_memory().unwrap();
        let columns = vec![
            col("id", RivetType::Int64),
            col(
                "amount",
                RivetType::Decimal {
                    precision: 38,
                    scale: 9,
                },
            ),
            col(
                "at",
                RivetType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some("UTC".into()),
                },
            ),
            col(
                "tags",
                RivetType::List {
                    inner: Box::new(RivetType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                },
            ),
        ];
        let pk = vec!["tenant".to_string(), "id".to_string()];
        s.record_load_spec("orders", None, &columns, Some(&pk), "run_1")
            .unwrap();

        let spec = s.load_spec("orders", None).unwrap().unwrap();
        assert_eq!(spec.columns, columns);
        assert_eq!(spec.primary_key, Some(pk));
        assert_eq!(spec.run_id.as_deref(), Some("run_1"));
        assert_eq!(spec.origin, "run");
    }

    /// Two runs of one export name — in practice two CONFIGS sharing a state DB,
    /// each with an export called `users` over a different source — must each keep
    /// the spec THEIR run recorded: the by-name row is last-writer-wins, the
    /// by-run row is what the load pins to.
    #[test]
    fn each_run_keeps_its_own_spec_while_the_by_name_row_follows_the_last_writer() {
        let s = StateStore::open_in_memory().unwrap();
        let mine = vec![col("id", RivetType::Int64), col("v", RivetType::Int64)];
        let theirs = vec![col("_id", RivetType::Int32)];
        s.record_load_spec("users", None, &mine, Some(&["id".to_string()]), "run_pg")
            .unwrap();
        s.record_load_spec(
            "users",
            None,
            &theirs,
            Some(&["_id".to_string()]),
            "run_mongo",
        )
        .unwrap();

        let by_name = s.load_spec("users", None).unwrap().unwrap();
        assert_eq!(by_name.run_id.as_deref(), Some("run_mongo"), "last writer");

        let pg = s
            .load_spec_of_run("users", None, "run_pg")
            .unwrap()
            .unwrap();
        assert_eq!(pg.columns, mine);
        assert_eq!(pg.primary_key, Some(vec!["id".to_string()]));
        let mongo = s
            .load_spec_of_run("users", None, "run_mongo")
            .unwrap()
            .unwrap();
        assert_eq!(mongo.columns, theirs);
        assert!(
            s.load_spec_of_run("users", None, "run_never")
                .unwrap()
                .is_none(),
            "a run that recorded nothing pins nothing"
        );
        // A keyless re-record of the SAME run replaces that run's row only.
        s.record_load_spec("users", None, &mine, None, "run_pg")
            .unwrap();
        assert_eq!(
            s.load_spec_of_run("users", None, "run_pg")
                .unwrap()
                .unwrap()
                .primary_key,
            None
        );
        assert_eq!(
            s.load_spec_of_run("users", None, "run_mongo")
                .unwrap()
                .unwrap()
                .primary_key,
            Some(vec!["_id".to_string()])
        );
    }

    /// A `query:` export records NO key at run time; the key `rivet init` recorded
    /// is the only one there is, and the by-name upsert keeps it (`key_origin =
    /// 'init'`). The per-run row is written verbatim (NULL key), so a load pinned to
    /// the run must borrow the INIT key — and only that: a key another RUN wrote by
    /// name is the race the pin escapes, so it is never borrowed.
    #[test]
    fn the_run_spec_borrows_an_init_recorded_key_but_never_another_runs() {
        let s = StateStore::open_in_memory().unwrap();
        let cols = vec![col("id", RivetType::Int64), col("v", RivetType::Int64)];
        s.record_primary_key("q", None, &["id".to_string()])
            .unwrap();
        s.record_load_spec("q", None, &cols, None, "run_1").unwrap();
        assert_eq!(
            s.load_spec_of_run("q", None, "run_1")
                .unwrap()
                .unwrap()
                .primary_key,
            None,
            "the run itself recorded none"
        );
        let pinned = s
            .load_spec_of_run_with_init_key("q", None, "run_1")
            .unwrap()
            .unwrap();
        assert_eq!(
            pinned.primary_key,
            Some(vec!["id".to_string()]),
            "the init key applies"
        );
        assert_eq!(pinned.columns, cols, "the run's columns stay the run's");

        // A keyless run of `t` after another RUN wrote a key by name: NOT borrowed.
        s.record_load_spec("t", None, &cols, Some(&["_id".to_string()]), "other_run")
            .unwrap();
        s.record_load_spec("t", None, &cols, None, "mine").unwrap();
        assert_eq!(
            s.load_spec_of_run_with_init_key("t", None, "mine")
                .unwrap()
                .unwrap()
                .primary_key,
            None,
            "a run-recorded by-name key is the last-writer race, never borrowed"
        );
        // The race in the order it happens: MY keyless run first, then a same-named
        // export of another config writes `_id` by name. Without the `key_origin =
        // 'init'` filter the by-name key is borrowed — the exact pin escape.
        s.record_load_spec("u", None, &cols, None, "mine").unwrap();
        s.record_load_spec("u", None, &cols, Some(&["_id".to_string()]), "theirs")
            .unwrap();
        assert_eq!(
            s.load_spec_of_run_with_init_key("u", None, "mine")
                .unwrap()
                .unwrap()
                .primary_key,
            None,
            "a key another RUN wrote by name AFTER mine is never borrowed"
        );
    }

    /// `pk: auto` reads the recorded key, so a key a RUN captured from `table: orders`
    /// must not outlive the export's move to a `query:` (or another relation) that
    /// records none: the dedup view would partition by a key the export no longer
    /// reads. RED against the pre-fix `COALESCE(excluded, old)` upsert.
    #[test]
    fn a_capture_without_a_key_clears_the_key_a_run_recorded_before_it() {
        let s = StateStore::open_in_memory().unwrap();
        let pk = vec!["id".to_string()];
        s.record_load_spec(
            "q",
            None,
            &[col("id", RivetType::Int64)],
            Some(&pk),
            "run_1",
        )
        .unwrap();
        s.record_load_spec(
            "q",
            None,
            &[col("id", RivetType::Int64), col("v", RivetType::String)],
            None,
            "run_2",
        )
        .unwrap();

        let spec = s.load_spec("q", None).unwrap().unwrap();
        assert_eq!(
            spec.primary_key, None,
            "the key belonged to a relation no longer read"
        );
        assert_eq!(spec.columns.len(), 2);
        assert_eq!(spec.run_id.as_deref(), Some("run_2"));
    }

    #[test]
    fn a_key_init_recorded_survives_the_run_that_records_the_columns() {
        let s = StateStore::open_in_memory().unwrap();
        let pk = vec!["b".to_string(), "a".to_string()];
        s.record_primary_key("q", None, &pk).unwrap();
        let init_only = s.load_spec("q", None).unwrap().unwrap();
        assert!(init_only.columns.is_empty());
        assert_eq!(init_only.origin, "init");

        s.record_load_spec("q", None, &[col("a", RivetType::Int32)], None, "run_1")
            .unwrap();
        let spec = s.load_spec("q", None).unwrap().unwrap();
        assert_eq!(spec.primary_key, Some(pk.clone()));
        assert_eq!(spec.columns.len(), 1);
        s.record_load_spec("q", None, &[col("a", RivetType::Int32)], None, "run_2")
            .unwrap();
        assert_eq!(
            s.load_spec("q", None).unwrap().unwrap().primary_key,
            Some(pk.clone()),
            "init's key outlives every keyless capture"
        );

        s.record_primary_key("q", None, &pk).unwrap();
        assert_eq!(s.load_spec("q", None).unwrap().unwrap().columns.len(), 1);

        // Once a run records a key of its own, that key is a run's: the next keyless
        // capture clears it.
        let seen = vec!["a".to_string()];
        s.record_load_spec(
            "q",
            None,
            &[col("a", RivetType::Int32)],
            Some(&seen),
            "run_3",
        )
        .unwrap();
        assert_eq!(
            s.load_spec("q", None).unwrap().unwrap().primary_key,
            Some(seen)
        );
        s.record_load_spec("q", None, &[col("a", RivetType::Int32)], None, "run_4")
            .unwrap();
        assert_eq!(s.load_spec("q", None).unwrap().unwrap().primary_key, None);
    }

    #[test]
    fn units_of_one_export_are_recorded_apart() {
        let s = StateStore::open_in_memory().unwrap();
        s.record_load_spec("cdc", Some("a"), &[col("x", RivetType::Int32)], None, "r")
            .unwrap();
        s.record_load_spec("cdc", Some("b"), &[col("y", RivetType::Bool)], None, "r")
            .unwrap();

        assert_eq!(
            s.load_spec("cdc", Some("a")).unwrap().unwrap().columns[0].name,
            "x"
        );
        assert_eq!(
            s.load_spec("cdc", Some("b")).unwrap().unwrap().columns[0].name,
            "y"
        );
        assert!(s.load_spec("cdc", None).unwrap().is_none());
        assert!(s.load_spec("other", Some("a")).unwrap().is_none());
    }

    #[test]
    fn to_mapping_rederives_the_arrow_type_from_the_rivet_type() {
        let m = col("id", RivetType::Int64).to_mapping();
        assert_eq!(m.arrow_type, Some(arrow::datatypes::DataType::Int64));
        assert_eq!(
            LoadSpecColumn::from_mapping(&m),
            col("id", RivetType::Int64)
        );
    }
}
