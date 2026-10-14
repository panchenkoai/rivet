//! One handle over every warehouse a cycle can land in (docs/cycle-matrix.yaml): the local
//! Parquet destination graded by DuckDB, ClickHouse on the stand, BigQuery and Snowflake in the
//! cloud. Values are read back through a client the loader does not use (the local parts through
//! the rig oracle's DuckDB), and only when a cell asks for them; between cycles a cell reads
//! metadata alone.

#![allow(dead_code)]

use super::*;

/// The fake-gcs bucket the ClickHouse cycles stage their parts in.
const CH_BUCKET: &str = "rivet-qa-three-cycles";
/// The env var the ClickHouse `load:` line reads its password from.
const CH_PASSWORD_ENV: &str = "RIVET_TEST_CH_PASSWORD";

/// A warehouse `rivet load` writes, named as `ExportTarget` names it; `DuckDb` is the local Parquet destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarehouseKind {
    DuckDb,
    ClickHouse,
    BigQuery,
    Snowflake,
}

/// Drops the ClickHouse database a cell created.
struct ChDatabase(String);

impl Drop for ChDatabase {
    fn drop(&mut self) {
        let sql = format!("DROP DATABASE IF EXISTS {}", self.0);
        let _ = std::panic::catch_unwind(|| clickhouse_run_sql_json(&sql));
    }
}

/// One cell's warehouse: where its rig lands, how its `rivet load` runs, and the second client that reads it back.
pub struct Warehouse {
    pub kind: WarehouseKind,
    bq: Option<BqLive>,
    sf: Option<SfLive>,
    ch: Option<ChDatabase>,
    guards: Vec<Box<dyn std::any::Any>>,
}

/// An integer BigQuery, ClickHouse and Snowflake each render their own way (a number, or a number in a string).
fn to_i64(v: &serde_json::Value) -> i64 {
    match v {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or_else(|| panic!("not an integer: {n}")),
        serde_json::Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map(|f| f as i64)
            .unwrap_or_else(|_| panic!("not an integer: {s:?}")),
        other => panic!("not an integer: {other}"),
    }
}

impl Warehouse {
    /// The stand's warehouse of `kind` for a cell labelled `label`, or `None` (a named skip) when its credential is absent.
    pub fn of(kind: WarehouseKind, label: &str) -> Option<Self> {
        let mut w = Warehouse {
            kind,
            bq: None,
            sf: None,
            ch: None,
            guards: Vec::new(),
        };
        match kind {
            WarehouseKind::DuckDb => {}
            WarehouseKind::ClickHouse => {
                require_alive(LiveService::ClickHouse);
                require_alive(LiveService::FakeGcs);
                ensure_gcs_bucket(CH_BUCKET);
                let db = unique_name(label);
                clickhouse_run_sql_json(&format!("CREATE DATABASE {db}"));
                w.ch = Some(ChDatabase(db));
            }
            WarehouseKind::BigQuery => w.bq = Some(BqLive::from_env(label)?),
            WarehouseKind::Snowflake => w.sf = Some(SfLive::from_env(label)?),
        }
        Some(w)
    }

    /// `(key, value)` integer pairs over the parts the Success manifests under `dir` declare, read by the rig oracle's DuckDB: every run's, or the newest run's alone (`latest`).
    pub fn declared_pairs(
        dir: &std::path::Path,
        key: &str,
        value: &str,
        latest: bool,
    ) -> Vec<(i64, i64)> {
        super::verify::declared_pairs(dir, key, value, latest)
    }

    /// Whether this warehouse has a load leg at all (the local destination is what the run wrote).
    pub fn loads(&self) -> bool {
        self.kind != WarehouseKind::DuckDb
    }

    /// `rig` landing in this warehouse: its destination and its `load:` line keyed on `pk`.
    pub fn shape(&self, rig: Rig, pk: &str) -> Rig {
        match self.kind {
            WarehouseKind::DuckDb => rig,
            WarehouseKind::ClickHouse => rig
                .dest_gcs(CH_BUCKET, &unique_name("cyc"), FAKE_GCS_ENDPOINT)
                .top_line(&format!(
                    "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {}, \
                     user: {CLICKHOUSE_USER}, password_env: {CH_PASSWORD_ENV}, pk: [{pk}] }}",
                    self.ch.as_ref().expect("a ClickHouse database").0
                )),
            WarehouseKind::BigQuery => {
                let bq = self.bq.as_ref().expect("a BigQuery dataset");
                rig.dest_gcs_live(&bq.bucket, &bq.prefix)
                    .top_line(&bq.load_line(&format!(", pk: [{pk}]")))
            }
            WarehouseKind::Snowflake => {
                let sf = self.sf.as_ref().expect("a Snowflake schema");
                rig.dest_gcs_live(&sf.bucket, &sf.prefix)
                    .top_line(&sf.load_line(&format!(", pk: [{pk}]")))
            }
        }
    }

    /// Drop `table` and its change log when the cell ends, however it ends.
    pub fn adopt(&mut self, table: &str) {
        let changes = format!("{table}__changes");
        match self.kind {
            WarehouseKind::BigQuery => {
                let guard = self
                    .bq
                    .as_ref()
                    .expect("BigQuery")
                    .cleanup(&[table, &changes]);
                self.guards.push(Box::new(guard));
            }
            WarehouseKind::Snowflake => {
                let guard = self.sf.as_ref().expect("Snowflake").cleanup(&[table]);
                self.guards.push(Box::new(guard));
            }
            WarehouseKind::DuckDb | WarehouseKind::ClickHouse => {}
        }
    }

    /// `rivet load` through `rig`, which must succeed; everything it said.
    pub fn load(&self, rig: &Rig) -> String {
        if !self.loads() {
            return String::new();
        }
        let envs: &[(&str, &str)] = match self.kind {
            WarehouseKind::ClickHouse => &[(CH_PASSWORD_ENV, CLICKHOUSE_PASSWORD)],
            _ => &[],
        };
        let out = rig.load_args_env(&[], envs);
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            out.status.success(),
            "rivet load into {:?} failed:\n{said}",
            self.kind
        );
        said
    }

    /// What the warehouse holds under `table` and its change log, by metadata alone (kinds, row counts, modification stamps; the local parts by name and size): equal before and after a cycle means the cycle wrote nothing there.
    pub fn fingerprint(&self, rig: &Rig, table: &str) -> String {
        let changes = format!("{table}__changes");
        match self.kind {
            WarehouseKind::DuckDb => {
                let mut parts: Vec<String> = files_with_extension(&rig.out_dir(), "parquet")
                    .iter()
                    .map(|p| {
                        let len = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
                        format!(
                            "{} {len}",
                            p.file_name().unwrap_or_default().to_string_lossy()
                        )
                    })
                    .collect();
                parts.sort();
                parts.join("\n")
            }
            WarehouseKind::ClickHouse => {
                let db = &self.ch.as_ref().expect("ClickHouse").0;
                clickhouse_run_sql_json(&format!(
                    "SELECT t.name AS name, t.engine AS engine, p.parts AS parts, p.rows AS rows, p.stamp AS stamp \
                     FROM system.tables t LEFT JOIN (SELECT database, table, count() AS parts, sum(rows) AS rows, \
                     max(modification_time) AS stamp FROM system.parts WHERE active GROUP BY database, table) p \
                     ON p.database = t.database AND p.table = t.name \
                     WHERE t.database = '{db}' AND t.name IN ('{table}', '{changes}') ORDER BY t.name"
                ))
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join("\n")
            }
            WarehouseKind::BigQuery => {
                let bq = self.bq.as_ref().expect("BigQuery");
                [table, changes.as_str()]
                    .iter()
                    .map(|t| match bq.read_bq_table_type(t) {
                        None => format!("{t}: absent"),
                        Some(kind) => {
                            let meta = bq.read_bq_meta(t);
                            format!(
                                "{t}: {kind} rows={} modified={}",
                                meta["numRows"], meta["lastModifiedTime"]
                            )
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            WarehouseKind::Snowflake => {
                let sf = self.sf.as_ref().expect("Snowflake");
                sf.read(&format!(
                    "SELECT table_name, table_type, row_count, last_altered FROM {}.INFORMATION_SCHEMA.TABLES \
                     WHERE table_schema = '{}' AND table_name IN ('{}', '{}') ORDER BY table_name",
                    sf.database(),
                    sf.schema().to_uppercase(),
                    table.to_uppercase(),
                    changes.to_uppercase()
                ))
                .iter()
                .map(|r| format!("{r:?}"))
                .collect::<Vec<_>>()
                .join("\n")
            }
        }
    }

    /// `(key, value)` of every row of `table` through the second client, sorted; `live` keeps only rows not flagged `__is_deleted` (a change-log view), `whole` reads the newest run alone on the local destination (a whole-table run's current state).
    pub fn pairs(
        &self,
        rig: &Rig,
        table: &str,
        key: &str,
        value: &str,
        live: bool,
        whole: bool,
    ) -> Vec<(i64, i64)> {
        let filter = if live { " WHERE NOT __is_deleted" } else { "" };
        let mut rows = match self.kind {
            WarehouseKind::DuckDb => {
                super::verify::declared_pairs(&rig.out_dir(), key, value, whole)
            }
            WarehouseKind::ClickHouse => clickhouse_run_sql_json(&format!(
                "SELECT {key} AS k, {value} AS v FROM {}.{table}{filter}",
                self.ch.as_ref().expect("ClickHouse").0
            ))
            .iter()
            .map(|r| (to_i64(&r["k"]), to_i64(&r["v"])))
            .collect(),
            WarehouseKind::BigQuery => {
                let bq = self.bq.as_ref().expect("BigQuery");
                bq.read_bq_rows(&format!(
                    "SELECT {key} AS k, {value} AS v FROM `{}.{}.{table}`{filter}",
                    bq.project, bq.dataset
                ))
                .iter()
                .map(|r| (to_i64(&r["k"]), to_i64(&r["v"])))
                .collect()
            }
            WarehouseKind::Snowflake => {
                let sf = self.sf.as_ref().expect("Snowflake");
                sf.read(&format!(
                    "SELECT {key} AS k, {value} AS v FROM {}{filter}",
                    sf.fq(table)
                ))
                .iter()
                .map(|r| (to_i64(&r[0]), to_i64(&r[1])))
                .collect()
            }
        };
        rows.sort_unstable();
        rows
    }

    /// `(rows, distinct keys)` of the change log `<table>__changes`, or of `table` itself where no log exists (a whole-table layout); `whole` as in [`Warehouse::pairs`].
    pub fn log_rows(&self, rig: &Rig, table: &str, key: &str, whole: bool) -> (i64, i64) {
        let changes = format!("{table}__changes");
        let log = match self.kind {
            WarehouseKind::DuckDb => {
                let pairs = super::verify::declared_pairs(&rig.out_dir(), key, key, whole);
                let distinct: std::collections::BTreeSet<i64> =
                    pairs.iter().map(|(k, _)| *k).collect();
                return (pairs.len() as i64, distinct.len() as i64);
            }
            WarehouseKind::ClickHouse => {
                let db = &self.ch.as_ref().expect("ClickHouse").0;
                let found = clickhouse_run_sql_json(&format!(
                    "SELECT count() AS n FROM system.tables WHERE database = '{db}' AND name = '{changes}'"
                ));
                to_i64(&found[0]["n"]) > 0
            }
            WarehouseKind::BigQuery => self
                .bq
                .as_ref()
                .expect("BigQuery")
                .read_bq_table_type(&changes)
                .is_some(),
            WarehouseKind::Snowflake => self
                .sf
                .as_ref()
                .expect("Snowflake")
                .kind(&changes)
                .is_some(),
        };
        let relation = if log { changes.as_str() } else { table };
        let pairs = self.pairs(rig, relation, key, key, false, whole);
        let distinct: std::collections::BTreeSet<i64> = pairs.iter().map(|(k, _)| *k).collect();
        (pairs.len() as i64, distinct.len() as i64)
    }
}
