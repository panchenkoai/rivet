//! Black-box end-to-end matrix for `rivet load` (ADR-0001).
//!
//! It drives the **shipped binaries** end-to-end and re-reads the warehouse to
//! assert oracles:
//!
//! ```text
//! seed → rivet run/cdc (dest = GCS) → rivet load [--cdc] → BigQuery | Snowflake
//!                                                       └→ oracle: bq/snow query vs source
//! ```
//!
//! Everything is `std::process` + hand-written YAML — no arrow/parquet/db-driver
//! deps. `rivet load` is a free OSS command (no license). These live cells are
//! `#[ignore]`; they need the seeded devbox stack + warehouse creds.
//!
//! Env vars (only the ones a given cell's warehouse needs are required):
//!   RIVET_TEST_GCS_BUCKET         GCS bucket exports stage through
//!   BIGQUERY_TEST_PROJECT         BigQuery project (BigQuery cells)
//!   RIVET_SF_CONNECTION           `snow` connection name (Snowflake cells)
//!   RIVET_SF_WAREHOUSE / _DATABASE / _SCHEMA / _STORAGE_INTEGRATION
//!   RIVET_SNOWFLAKE_KEY           absolute path to the `snow` private key (optional)

#![allow(dead_code)] // stages fill in cell-by-cell; the scaffold lands first.

use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

// ── Axes (the matrix columns × the mode encoded in the scenario id) ──────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Postgres,
    Mysql,
    Mssql,
    Mongo,
}

impl Engine {
    /// The `source.type` string in a rivet extraction config.
    pub fn source_type(self) -> &'static str {
        match self {
            Engine::Postgres => "postgres",
            Engine::Mysql => "mysql",
            Engine::Mssql => "mssql",
            Engine::Mongo => "mongo",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Batch,
    Cdc,
}

const PG_CLIENT: &[&str] = &["psql", "-U", "rivet", "-d", "rivet_bench", "-tAc"];
const MY_CLIENT: &[&str] = &["mysql", "-uroot", "-privet", "rivet_bench", "-N", "-e"];
/// The BigQuery dataset every matrix load targets.
const MATRIX_DATASET: &str = "rivet_matrix";

/// The resolved `(engine, mode)` target — the single home for per-engine truth
/// (batch vs the CDC-enabled instance, its container, seed flags, CDC config
/// field, query dialect). Every access reads from here. See CONTEXT.md "Lane".
struct Lane {
    source_type: &'static str,
    host_port: &'static str,
    container: &'static str,
    /// `docker exec` SQL-shell prefix; empty for a non-SQL source (Mongo).
    client: &'static [&'static str],
    seed_target: Option<&'static str>,
    seed_url_flag: &'static str,
    /// Extra export-YAML line(s) a CDC run needs (`slot:` / `server_id:`).
    cdc_field: &'static str,
    /// `information_schema` WHERE prefix that qualifies the schema.
    schema_qual: &'static str,
    /// The source client's column separator.
    sep: char,
}

impl Lane {
    fn of(engine: Engine, mode: Mode) -> Lane {
        use Engine::*;
        use Mode::*;
        // One table. Batch and CDC targets sit in the same block per engine, so
        // the 5432-vs-5434 split-brain is impossible by construction.
        match (engine, mode) {
            (Postgres, Batch) => Lane {
                source_type: "postgres",
                host_port: "127.0.0.1:5432",
                container: "rivet-postgres-1",
                client: PG_CLIENT,
                seed_target: Some("postgres"),
                seed_url_flag: "--pg-url",
                cdc_field: "",
                schema_qual: "",
                sep: '|',
            },
            (Postgres, Cdc) => Lane {
                source_type: "postgres",
                host_port: "127.0.0.1:5434",
                container: "rivet-postgres-cdc-1",
                client: PG_CLIENT,
                seed_target: Some("postgres"),
                seed_url_flag: "--pg-url",
                cdc_field: "      slot: rivet_matrix_soak\n",
                schema_qual: "",
                sep: '|',
            },
            (Mysql, Batch) => Lane {
                source_type: "mysql",
                host_port: "127.0.0.1:3306",
                container: "rivet-mysql-1",
                client: MY_CLIENT,
                seed_target: Some("mysql"),
                seed_url_flag: "--mysql-url",
                cdc_field: "",
                schema_qual: "table_schema='rivet_bench' AND ",
                sep: '\t',
            },
            (Mysql, Cdc) => Lane {
                source_type: "mysql",
                host_port: "127.0.0.1:3307",
                container: "rivet-mysql-cdc-1",
                client: MY_CLIENT,
                seed_target: Some("mysql"),
                seed_url_flag: "--mysql-url",
                cdc_field: "      server_id: 47010\n",
                schema_qual: "table_schema='rivet_bench' AND ",
                sep: '\t',
            },
            // Targeting is known; SQL-dialect + CDC fields fill in as these wire up.
            (Mssql, m) => Lane {
                source_type: "mssql",
                host_port: if m == Cdc {
                    "127.0.0.1:1434"
                } else {
                    "127.0.0.1:1433"
                },
                container: if m == Cdc {
                    "rivet-mssql-cdc-1"
                } else {
                    "rivet-mssql-1"
                },
                client: &[],
                seed_target: Some("sqlserver"),
                seed_url_flag: "--mssql-url",
                cdc_field: "",
                schema_qual: "",
                sep: '|',
            },
            (Mongo, m) => Lane {
                source_type: "mongo",
                host_port: if m == Cdc {
                    "127.0.0.1:27018"
                } else {
                    "127.0.0.1:27017"
                },
                container: if m == Cdc {
                    "rivet-mongo-rs-1"
                } else {
                    "rivet-mongo-1"
                },
                client: &[],
                seed_target: None,
                seed_url_flag: "",
                cdc_field: "",
                schema_qual: "",
                sep: '\t',
            },
        }
    }

    /// The source connection URL for `db` on this lane (127.0.0.1 — colima
    /// forwards IPv4 only, and some drivers try `::1` first).
    fn url(&self, db: &str) -> String {
        match self.source_type {
            "postgres" => format!("postgresql://rivet:rivet@{}/{db}", self.host_port),
            "mysql" => format!("mysql://rivet:rivet@{}/{db}", self.host_port),
            "mssql" => format!("sqlserver://sa:Rivet_Passw0rd!@{}/{db}", self.host_port),
            _ => format!("mongodb://{}/{db}?directConnection=true", self.host_port),
        }
    }
}

/// The warehouse a cell loads into and re-reads for its oracles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Warehouse {
    BigQuery,
    Snowflake,
}

impl Warehouse {
    /// The `load:` block's `target:` discriminator.
    fn target(self) -> &'static str {
        match self {
            Warehouse::BigQuery => "bigquery",
            Warehouse::Snowflake => "snowflake",
        }
    }
}

/// A warehouse truth check — the loaded table re-read in full. Applies to a
/// batch export AND a CDC-loaded table (TypeFidelity matters either way). Every
/// CASE declares ≥1; the runner FAILS on a `Skipped` (no-op) outcome, so a
/// `test:` cell can't hide a no-op (the guard's re-expressed strength-floor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarehouseOracle {
    /// Exported row-count == source `COUNT(*)`.
    RowCount,
    /// Per-column NULL count in the warehouse == source (a "degrade to null"
    /// cell path is a silent-loss path — counts of hand-picked columns miss it).
    NullProfile,
    /// `COUNT(DISTINCT id)` in the warehouse == source (catches type-bracket /
    /// keyset drop where a self-consistent subset passes count checks).
    DistinctId,
    /// Warehouse column type family == the resolved source semantic type.
    TypeFidelity,
    /// After a `cleanup_source` load, the GCS staging prefix holds NO objects —
    /// the cleanup side-effect (bucket wiped), not just that the data survived
    /// it. A cleanup that silently didn't run passes the data oracles but fails
    /// this one.
    StagingWiped,
}

/// A CDC-soak truth check — a process metric over the churn, evaluated only by
/// `run_soak`. Splitting it from [`WarehouseOracle`] makes "soak oracle passed to
/// `run()`" a **compile** error instead of a runtime `Skipped`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SoakOracle {
    /// Last-quarter peak RSS ≤ 1.5× first-quarter (leak detector).
    FlatRss,
    /// DISTINCT(id) across all parts ≥ churned-id count (zero gaps).
    ZeroGap,
}

// ── Fixture (the "heavy" tier lives here) ────────────────────────────────────

pub struct Fixture {
    /// The source table (`content_items`, `rivet_type_matrix`, …).
    pub table: String,
    /// Row target. Heavy-batch = 1_000_000; heavy-CDC uses a 100k base + churn.
    pub rows: usize,
}

impl Fixture {
    pub fn heavy_batch(table: &str) -> Self {
        Fixture {
            table: table.into(),
            rows: 1_000_000,
        }
    }
    pub fn heavy_cdc_base(table: &str) -> Self {
        Fixture {
            table: table.into(),
            rows: 100_000,
        }
    }
    /// A tiny fixture for a fast live smoke — validate the pipeline, not scale.
    pub fn smoke(table: &str) -> Self {
        Fixture {
            table: table.into(),
            rows: 2_000,
        }
    }
}

/// Shape of a CDC soak: how long, and how much churn per cycle. The heavy tier
/// aims at ~500k mutations over the run (the design's heavy-CDC churn budget).
pub struct SoakConfig {
    pub minutes: u64,
    pub updates_per_cycle: usize,
    pub deletes_per_cycle: usize,
}

impl SoakConfig {
    pub fn heavy() -> Self {
        SoakConfig {
            minutes: 45,
            updates_per_cycle: 200,
            deletes_per_cycle: 20,
        }
    }
    /// A minutes-long smoke of the same machinery (for the tracer).
    pub fn quick() -> Self {
        SoakConfig {
            minutes: 1,
            updates_per_cycle: 50,
            deletes_per_cycle: 5,
        }
    }
}

// ── Environment (the internal-artifact preconditions) ────────────────────────

/// Snowflake connection config (only required by Snowflake cells).
struct SfEnv {
    connection: String,
    warehouse: String,
    database: String,
    schema: String,
    storage_integration: String,
}

struct HarnessEnv {
    /// This crate's root — the `seed` binary and `dev/*/init.sql` live here.
    oss_dir: PathBuf,
    gcs_bucket: String,
    bq_project: Option<String>,
    sf: Option<SfEnv>,
}

impl HarnessEnv {
    fn load() -> Result<Self> {
        // The matrix now lives in the OSS crate, so the seed binary + init SQL are
        // in this crate's own tree.
        let oss_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let sf = match std::env::var("RIVET_SF_CONNECTION") {
            Ok(connection) => Some(SfEnv {
                connection,
                warehouse: std::env::var("RIVET_SF_WAREHOUSE")
                    .context("RIVET_SF_WAREHOUSE unset")?,
                database: std::env::var("RIVET_SF_DATABASE").context("RIVET_SF_DATABASE unset")?,
                schema: std::env::var("RIVET_SF_SCHEMA").unwrap_or_else(|_| "PUBLIC".into()),
                storage_integration: std::env::var("RIVET_SF_STORAGE_INTEGRATION")
                    .context("RIVET_SF_STORAGE_INTEGRATION unset")?,
            }),
            Err(_) => None,
        };
        Ok(HarnessEnv {
            oss_dir,
            gcs_bucket: std::env::var("RIVET_TEST_GCS_BUCKET")
                .context("RIVET_TEST_GCS_BUCKET unset")?,
            bq_project: std::env::var("BIGQUERY_TEST_PROJECT").ok(),
            sf,
        })
    }

    fn bq_project(&self) -> Result<&str> {
        self.bq_project
            .as_deref()
            .context("BIGQUERY_TEST_PROJECT unset (required for a BigQuery cell)")
    }

    fn sf(&self) -> Result<&SfEnv> {
        self.sf
            .as_ref()
            .context("RIVET_SF_* unset (required for a Snowflake cell)")
    }
}

// ── The orchestrator ─────────────────────────────────────────────────────────

pub struct Verification {
    pub engine: Engine,
    pub mode: Mode,
    pub fixture: Fixture,
    pub warehouse: Warehouse,
    initial_snapshot: bool,
}

/// Result of one oracle evaluation. `Skipped` when the oracle is a no-op for
/// the lane — which the guard treats as a strength-floor violation, not a pass.
#[derive(Debug)]
pub enum OracleOutcome {
    Pass,
    Fail { detail: String },
    Skipped { why: String },
}

impl Verification {
    pub fn new(engine: Engine, mode: Mode, fixture: Fixture) -> Self {
        Verification {
            engine,
            mode,
            fixture,
            warehouse: Warehouse::BigQuery,
            initial_snapshot: false,
        }
    }

    /// Select the warehouse this cell loads into and re-reads (default BigQuery).
    pub fn warehouse(mut self, warehouse: Warehouse) -> Self {
        self.warehouse = warehouse;
        self
    }

    /// Turn on `cdc.initial: snapshot` — the first CDC run anchors, backfills
    /// every preexisting row, then drains. Used by [`Self::run_cdc_backfill`].
    pub fn initial_snapshot(mut self) -> Self {
        self.initial_snapshot = true;
        self
    }

    /// Run the full pipeline and evaluate each requested oracle. A CASE with an
    /// empty oracle list, or any `Skipped` outcome, is a strength-floor failure.
    pub fn run(
        &self,
        oracles: &[WarehouseOracle],
    ) -> Result<Vec<(WarehouseOracle, OracleOutcome)>> {
        if oracles.is_empty() {
            bail!("strength-floor: a `test:` case must declare ≥1 oracle (ADR-0001)");
        }
        let env = HarnessEnv::load()?;
        let work = TempWork::new("rivet-pro-matrix")?;

        self.seed(&env)?;
        let gcs_prefix = self.extract_to_gcs(&env, &work)?;
        let bq_table = self.load(&env, &work, &gcs_prefix)?;

        let _ = &env; // preconditions checked; stages already consumed it.
        oracles
            .iter()
            .map(|o| {
                // StagingWiped checks the GCS prefix (known here), not the
                // warehouse — every other oracle re-reads the loaded table.
                let outcome = match o {
                    WarehouseOracle::StagingWiped => self.staging_wiped(&gcs_prefix)?,
                    _ => self.evaluate(&bq_table, *o)?,
                };
                Ok((*o, outcome))
            })
            .collect()
    }

    /// The CDC soak runner (`SoakFlatRss` / `SoakZeroGap`). Distinct from `run()`:
    /// it churns the seeded heavy base under repeated bounded CDC runs, sampling
    /// the EXTRACTOR's peak RSS each cycle, then loads every captured part to
    /// BigQuery and checks completeness against the churned id set.
    pub fn run_soak(
        &self,
        cfg: &SoakConfig,
        warehouse: &[WarehouseOracle],
        soak: &[SoakOracle],
    ) -> Result<Vec<(String, OracleOutcome)>> {
        if self.mode != Mode::Cdc {
            bail!("run_soak requires Mode::Cdc");
        }
        if warehouse.is_empty() && soak.is_empty() {
            bail!("strength-floor: a soak `test:` case must declare ≥1 oracle (ADR-0001)");
        }
        let env = HarnessEnv::load()?;
        let work = TempWork::new("rivet-pro-soak")?;
        // Trailing slash = a real GCS "directory" (see extract_to_gcs), so the
        // manifest lands at `.../content_items/manifest.json`, not concatenated.
        let prefix = format!("soak/{}/{}/", self.engine.source_type(), self.fixture.table);
        let full_prefix = format!("gs://{}/{}", env.gcs_bucket, prefix);
        let _ = Command::new("gcloud")
            .args(["storage", "rm", "-r", &format!("{full_prefix}**")])
            .status();

        self.seed(&env)?; // the heavy base
        // Same filename `pro_load` reads — one config drives both extract & load.
        let cdc_cfg = work.write("extract.yaml", &self.extraction_yaml(&env, &prefix)?);

        // Anchor first: one CDC run BEFORE any churn pins the checkpoint at the
        // current binlog position (MySQL has no server-side anchor), so every
        // subsequent churn is captured. Without it the first cycle's churn is lost.
        self.cdc_run_peak_rss(&cdc_cfg)?;
        let deadline = Instant::now() + Duration::from_secs(cfg.minutes * 60);
        let mut rss_mb: Vec<f64> = Vec::new();
        let mut churned: Vec<i64> = Vec::new();
        while Instant::now() < deadline {
            churned.extend(self.churn(cfg)?);
            rss_mb.push(self.cdc_run_peak_rss(&cdc_cfg)?);
        }
        let bq_table = self.load(&env, &work, &format!("gs://{}/{}", env.gcs_bucket, prefix))?;

        churned.sort_unstable();
        churned.dedup();
        let churned_distinct = churned.len() as i64;

        // Warehouse oracles (TypeFidelity, null-profile, …) apply to the CDC-loaded
        // table exactly as in batch; soak oracles are process metrics over the run.
        let mut out = Vec::new();
        for o in warehouse {
            out.push((format!("{o:?}"), self.evaluate(&bq_table, *o)?));
        }
        for o in soak {
            out.push((
                format!("{o:?}"),
                self.eval_soak(*o, &bq_table, &rss_mb, churned_distinct)?,
            ));
        }
        Ok(out)
    }

    /// A focused Mongo CDC cell: seed two docs, capture an update + a delete, load
    /// the change log through `rivet load --cdc`, and assert the current-state
    /// view's **soft-delete** flag. Mongo has no SQL client / OSS seed, so it
    /// drives `mongosh` directly instead of the generic seed/churn machinery.
    ///
    /// Timeline: insert `_id:1,2` → anchor CDC run → update `_id:1`, delete
    /// `_id:2` → capture CDC run → load `--cdc --pk _id`. The dedup view must show
    /// `_id:1` live and `_id:2` as a tombstone (`__is_deleted = true`).
    pub fn run_mongo_cdc_delete(&self) -> Result<Vec<(String, OracleOutcome)>> {
        if self.engine != Engine::Mongo || self.mode != Mode::Cdc {
            bail!("run_mongo_cdc_delete is Mongo+Cdc only");
        }
        let env = HarnessEnv::load()?;
        let work = TempWork::new("rivet-mongo-cdc")?;
        let coll = self.fixture.table.clone();
        let db = "rivet_bench";

        // Fresh collection + two documents.
        self.mongosh(
            db,
            &format!("db.{coll}.drop(); db.{coll}.insertMany([{{_id:1,v:'a'}},{{_id:2,v:'b'}}]);"),
        )?;

        let prefix = format!("matrix/mongo/{coll}/");
        let full_prefix = format!("gs://{}/{}", env.gcs_bucket, prefix);
        let _ = Command::new("gcloud")
            .args(["storage", "rm", "-r", &format!("{full_prefix}**")])
            .status();
        // A fresh checkpoint so the anchor pins at the current resume token.
        let _ =
            std::fs::remove_file(std::env::temp_dir().join(format!("ckpt-matrix_mongo_{coll}_")));

        let cfg = work.write("extract.yaml", &self.extraction_yaml(&env, &prefix)?);
        // Anchor: one bounded CDC run BEFORE any change pins the resume token, so
        // the mutations below are captured (Mongo has no server-side anchor).
        run(
            Command::new("rivet").args(["run", "-c"]).arg(&cfg),
            "rivet run (anchor)",
        )?;
        self.mongosh(
            db,
            &format!(
                "db.{coll}.updateOne({{_id:1}},{{$set:{{v:'a2'}}}}); db.{coll}.deleteOne({{_id:2}});"
            ),
        )?;
        run(
            Command::new("rivet").args(["run", "-c"]).arg(&cfg),
            "rivet run (capture)",
        )?;

        // Load the change log + (re)build the current-state dedup view. Mode
        // (cdc) and the pk (_id) come from the config's `load:` block.
        run(
            Command::new("rivet").args(["load", "-c"]).arg(&cfg),
            "rivet load (cdc)",
        )?;

        let view = self.warehouse_table_ref(&env, &self.loaded_table_name())?;
        let t = self.wh_table(&view);
        let live = self.wh_scalar(&format!("SELECT COUNT(*) FROM {t} WHERE NOT __is_deleted"))?;
        let tombstoned = self.wh_scalar(&format!("SELECT COUNT(*) FROM {t} WHERE __is_deleted"))?;

        Ok(vec![
            // `_id:1` survives its update; `_id:2` is hidden by the live filter.
            (
                "live_excludes_deleted".into(),
                compare("live-count", live, 1),
            ),
            // The delete is an auditable tombstone, not a silent disappearance.
            (
                "delete_sets_is_deleted".into(),
                compare("tombstone-count", tombstoned, 1),
            ),
        ])
    }

    /// A focused CDC **backfill** cell: turn CDC on for a PRE-EXISTING table via
    /// `cdc.initial: snapshot`, apply live changes, load, and assert the
    /// current-state view keeps the whole backfill.
    ///
    /// Pins the snapshot/stream seam: `initial: snapshot` rows load from a plain
    /// full-snapshot parquet (NULL `__op`/`__pos` in `__changes`), so the view
    /// must read them as live (`COALESCE(__op='delete',FALSE)`) and rank them
    /// oldest — else `WHERE NOT __is_deleted` silently drops the entire backfill.
    ///
    /// Timeline: seed N rows → anchor+snapshot+drain → insert 1 / update 1 /
    /// delete 1 → drain → load `--cdc --pk id`. Live must equal N (N−1 delete +1
    /// insert), tombstoned 1, and an untouched mid-backfill row live.
    pub fn run_cdc_backfill(&self) -> Result<Vec<(String, OracleOutcome)>> {
        if self.mode != Mode::Cdc || !self.initial_snapshot {
            bail!("run_cdc_backfill needs Mode::Cdc + .initial_snapshot()");
        }
        // The dedup PK: SQL sources key on `id`, Mongo on `_id`. MSSQL's CDC lane
        // is not wired in the harness (empty `Lane` client), so it has no cell.
        let pk = match self.engine {
            Engine::Postgres | Engine::Mysql => "id",
            Engine::Mongo => "_id",
            Engine::Mssql => {
                bail!("run_cdc_backfill: MSSQL CDC is not wired in the harness (empty Lane client)")
            }
        };
        let env = HarnessEnv::load()?;
        let work = TempWork::new("rivet-cdc-backfill")?;
        let t = self.fixture.table.clone();
        let n = self.fixture.rows as i64;

        // The preexisting data that predates CDC being turned on.
        self.seed_backfill(&t, n)?;

        let prefix = format!("matrix/{}/{}/", self.engine.source_type(), t);
        let full_prefix = format!("gs://{}/{}", env.gcs_bucket, prefix);
        let _ = Command::new("gcloud")
            .args(["storage", "rm", "-r", &format!("{full_prefix}**")])
            .status();
        let _ = std::fs::remove_file(
            std::env::temp_dir().join(format!("ckpt-{}", prefix.replace('/', "_"))),
        );
        // Postgres anchors CDC by CREATING the replication slot, so a slot left
        // by a prior run means `initial: snapshot` gets no fresh anchor and
        // resumes from a stale position instead of snapshotting. Drop it — the
        // slot IS the PG equivalent of the MySQL checkpoint file removed above.
        // (MySQL keys on server_id, Mongo on a resume token — no server-side
        // anchor to clear.)
        if self.engine == Engine::Postgres {
            let _ = self.source_raw(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE slot_name = 'rivet_matrix_soak'",
            );
        }

        let cfg = work.write("extract.yaml", &self.extraction_yaml(&env, &prefix)?);
        // Anchor → full snapshot of the N preexisting rows → drain (initial: snapshot).
        run(
            Command::new("rivet").args(["run", "-c"]).arg(&cfg),
            "rivet run (snapshot)",
        )?;
        // Live changes AFTER the backfill: one insert, one update, one delete.
        self.churn_backfill(&t, n)?;
        run(
            Command::new("rivet").args(["run", "-c"]).arg(&cfg),
            "rivet run (drain)",
        )?;
        // Mode (cdc) + the pk come from the config's `load:` block now.
        run(
            Command::new("rivet").args(["load", "-c"]).arg(&cfg),
            "rivet load (cdc)",
        )?;

        let view = self.warehouse_table_ref(&env, &self.loaded_table_name())?;
        let vt = self.wh_table(&view);
        let live = self.wh_scalar(&format!("SELECT COUNT(*) FROM {vt} WHERE NOT __is_deleted"))?;
        let tombstoned =
            self.wh_scalar(&format!("SELECT COUNT(*) FROM {vt} WHERE __is_deleted"))?;
        // A middle backfill row the stream never touched — the class the bug hid.
        let mid = n / 2;
        // Mongo serialises `_id` to a STRING in the warehouse (SQL sources keep a
        // numeric `id`), so the mid-row literal must be quoted for Mongo or the
        // `_id = <int>` comparison is a type error.
        let mid_lit = match self.engine {
            Engine::Mongo => format!("'{mid}'"),
            _ => mid.to_string(),
        };
        let mid_live = self.wh_scalar(&format!(
            "SELECT COUNT(*) FROM {vt} WHERE {pk} = {mid_lit} AND NOT __is_deleted"
        ))?;

        Ok(vec![
            // N preexisting − 1 delete + 1 insert = N live rows: the backfill survives.
            ("backfill_all_live".into(), compare("live-count", live, n)),
            // The delete is an auditable tombstone, not a silent disappearance.
            (
                "delete_sets_is_deleted".into(),
                compare("tombstone-count", tombstoned, 1),
            ),
            // The regression guard: an untouched snapshot row must read as live
            // (NULL `__op` → `COALESCE(.., FALSE)`), not vanish from the live filter.
            (
                "untouched_backfill_row_live".into(),
                compare("mid-row-live", mid_live, 1),
            ),
            // The cleanup side-effect: `cleanup_source: true` wiped the staging.
            ("staging_wiped".into(), self.staging_wiped(&full_prefix)?),
        ])
    }

    /// A focused live INCREMENTAL cell (MySQL batch source — no binlog): a
    /// cursor-based delta load into `<t>__changes` + a cursor-ordered
    /// current-state view, with `cleanup_source`. Proves the incremental
    /// contract end to end — no-loss, cursor dedup, never-tombstones, and the
    /// staging wiped — the matrix's incremental leg.
    ///
    /// Timeline: seed N rows at `ver = 1` → run + load (first pull = all N) →
    /// update `id=1` to `v='CHANGED', ver=2` and insert `id=N+1, ver=2` → run +
    /// load (the delta). The view must show **N+1** live rows, `id=1` at the
    /// ver-2 value (the cursor-ordered dedup), zero tombstones, and an empty
    /// bucket.
    pub fn run_incremental(&self) -> Result<Vec<(String, OracleOutcome)>> {
        let env = HarnessEnv::load()?;
        let work = TempWork::new("rivet-incremental")?;
        let t = self.fixture.table.clone();
        let n = self.fixture.rows as i64;

        // Seed N rows at ver=1 (the preexisting data; `ver` is the cursor).
        self.source_raw(&format!(
            "DROP TABLE IF EXISTS {t}; \
             CREATE TABLE {t} (id INT PRIMARY KEY, v VARCHAR(16), ver INT); \
             SET SESSION cte_max_recursion_depth = {depth}; \
             INSERT INTO {t} (id, v, ver) WITH RECURSIVE seq(n) AS \
             (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < {n}) \
             SELECT n, CONCAT('r', n), 1 FROM seq;",
            depth = n + 1,
        ))?;

        let prefix = format!("matrix/incremental/{t}/");
        let full_prefix = format!("gs://{}/{}", env.gcs_bucket, prefix);
        let _ = Command::new("gcloud")
            .args(["storage", "rm", "-r", &format!("{full_prefix}**")])
            .status();

        // One config drives both extract (incremental) and load. The state DB is
        // fresh (TempWork), so run 1 has no cursor and pulls all N rows.
        let cfg = work.write(
            "extract.yaml",
            &format!(
                "source:\n  type: mysql\n  url: \"{url}\"\n\
                 exports:\n  - name: {t}\n    query: \"SELECT id, v, ver FROM {t}\"\n    \
                 mode: incremental\n    cursor_column: ver\n    format: parquet\n    \
                 destination:\n      type: gcs\n      bucket: {bucket}\n      prefix: {prefix}\n\
                 load:\n  target: bigquery\n  project: {project}\n  dataset: {dataset}\n  \
                 pk: [id]\n  cleanup_source: true\n",
                url = self.lane().url("rivet_bench"),
                bucket = env.gcs_bucket,
                project = env.bq_project()?,
                dataset = MATRIX_DATASET,
            ),
        );
        run(
            Command::new("rivet").args(["run", "-c"]).arg(&cfg),
            "rivet run (incremental 1)",
        )?;
        run(
            Command::new("rivet").args(["load", "-c"]).arg(&cfg),
            "rivet load (incremental 1)",
        )?;
        // Delta: bump id=1's cursor (update), add a new row above the watermark.
        self.source_raw(&format!(
            "UPDATE {t} SET v='CHANGED', ver=2 WHERE id=1; \
             INSERT INTO {t} (id, v, ver) VALUES ({}, 'new', 2);",
            n + 1
        ))?;
        run(
            Command::new("rivet").args(["run", "-c"]).arg(&cfg),
            "rivet run (incremental 2)",
        )?;
        run(
            Command::new("rivet").args(["load", "-c"]).arg(&cfg),
            "rivet load (incremental 2)",
        )?;

        let view = self.warehouse_table_ref(&env, &t)?;
        let vt = self.wh_table(&view);
        let live = self.wh_scalar(&format!("SELECT COUNT(*) FROM {vt} WHERE NOT __is_deleted"))?;
        let tombstoned =
            self.wh_scalar(&format!("SELECT COUNT(*) FROM {vt} WHERE __is_deleted"))?;
        let id1_changed = self.wh_scalar(&format!(
            "SELECT COUNT(*) FROM {vt} WHERE id = 1 AND v = 'CHANGED'"
        ))?;

        Ok(vec![
            // N seeded − 0 delete + 1 insert = N+1 live rows (no delta lost).
            (
                "incremental_current_state".into(),
                compare("live-count", live, n + 1),
            ),
            // id=1's ver-2 update won over ver-1 via the cursor-ordered view.
            (
                "cursor_dedup".into(),
                compare("id1-ver2-won", id1_changed, 1),
            ),
            // Incremental can't observe deletes — the view never tombstones.
            (
                "never_tombstones".into(),
                compare("tombstone-count", tombstoned, 0),
            ),
            // The cleanup side-effect: `cleanup_source: true` wiped the staging.
            ("staging_wiped".into(), self.staging_wiped(&full_prefix)?),
        ])
    }

    /// Seed `n` preexisting rows/docs (PK `1..=n`) — the data `initial: snapshot`
    /// backfills. SQL sources go through the `id` seed client; Mongo has no SQL
    /// client so it seeds `_id` documents via `mongosh`.
    fn seed_backfill(&self, table: &str, n: i64) -> Result<()> {
        match self.engine {
            Engine::Postgres | Engine::Mysql => self.seed_backfill_table(table, n),
            Engine::Mongo => self.mongosh(
                "rivet_bench",
                &format!(
                    "db.{table}.drop(); let d = []; \
                     for (let i = 1; i <= {n}; i++) d.push({{_id: i, v: 'orig-' + i}}); \
                     db.{table}.insertMany(d);"
                ),
            ),
            Engine::Mssql => bail!("seed_backfill: MSSQL not wired in the harness"),
        }
    }

    /// The post-backfill churn — insert PK `n+1`, update PK `1`, delete PK `2` —
    /// the live changes the drain captures on top of the snapshot.
    fn churn_backfill(&self, table: &str, n: i64) -> Result<()> {
        match self.engine {
            Engine::Postgres | Engine::Mysql => {
                self.source_raw(&format!(
                    "INSERT INTO {table} (id, v) VALUES ({}, 'brand-new')",
                    n + 1
                ))?;
                self.source_raw(&format!(
                    "UPDATE {table} SET v = 'changed-after-snapshot' WHERE id = 1"
                ))?;
                self.source_raw(&format!("DELETE FROM {table} WHERE id = 2"))?;
                Ok(())
            }
            Engine::Mongo => self.mongosh(
                "rivet_bench",
                &format!(
                    "db.{table}.insertOne({{_id: {}, v: 'brand-new'}}); \
                     db.{table}.updateOne({{_id: 1}}, {{$set: {{v: 'changed-after-snapshot'}}}}); \
                     db.{table}.deleteOne({{_id: 2}});",
                    n + 1
                ),
            ),
            Engine::Mssql => bail!("churn_backfill: MSSQL not wired in the harness"),
        }
    }

    /// Drop + recreate the cell's table and seed `n` rows (`id` 1..=n) on a SQL
    /// source — the preexisting data `cdc.initial: snapshot` backfills.
    fn seed_backfill_table(&self, table: &str, n: i64) -> Result<()> {
        let sql = match self.engine {
            Engine::Mysql => format!(
                "DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table} (id INT PRIMARY KEY, v VARCHAR(48)); \
                 SET SESSION cte_max_recursion_depth = {depth}; \
                 INSERT INTO {table} (id, v) WITH RECURSIVE seq(n) AS \
                 (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < {n}) \
                 SELECT n, CONCAT('orig-', n) FROM seq;",
                depth = n + 1,
            ),
            Engine::Postgres => format!(
                "DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table} (id INT PRIMARY KEY, v VARCHAR(48)); \
                 INSERT INTO {table} (id, v) \
                 SELECT g, 'orig-' || g FROM generate_series(1, {n}) g;"
            ),
            _ => bail!("seed_backfill_table: unsupported engine"),
        };
        self.source_raw(&sql)?;
        Ok(())
    }

    /// Run a `mongosh` snippet against the lane's Mongo container.
    fn mongosh(&self, db: &str, js: &str) -> Result<()> {
        let lane = self.lane();
        run(
            Command::new("docker").args([
                "exec",
                lane.container,
                "mongosh",
                db,
                "--quiet",
                "--eval",
                js,
            ]),
            "mongosh",
        )
    }

    /// One churn cycle on the SOURCE: heavy UPDATEs (they stream heavy before/after
    /// images — the memory stressor) + DELETEs, returning the touched ids. Fresh
    /// heavy INSERTs are the documented next step (need per-column row generation).
    fn churn(&self, cfg: &SoakConfig) -> Result<Vec<i64>> {
        match self.engine {
            Engine::Postgres => {
                let t = &self.fixture.table;
                let upd = self.source_raw(&format!(
                    "UPDATE {t} SET updated_at = now() \
                     WHERE id IN (SELECT id FROM {t} ORDER BY id LIMIT {}) RETURNING id",
                    cfg.updates_per_cycle
                ))?;
                let del = self.source_raw(&format!(
                    "DELETE FROM {t} \
                     WHERE id IN (SELECT id FROM {t} ORDER BY id DESC LIMIT {}) RETURNING id",
                    cfg.deletes_per_cycle
                ))?;
                Ok(upd
                    .lines()
                    .chain(del.lines())
                    .filter_map(|l| l.trim().parse::<i64>().ok())
                    .collect())
            }
            Engine::Mysql => {
                let t = &self.fixture.table;
                // MySQL has no RETURNING and forbids an UPDATE/DELETE whose subquery
                // hits the same table (error 1093) — so capture the ids first, then
                // mutate by IN-list.
                let ids = |sql: &str| -> Result<Vec<i64>> {
                    Ok(self
                        .source_raw(sql)?
                        .lines()
                        .filter_map(|l| l.trim().parse().ok())
                        .collect())
                };
                let in_list =
                    |v: &[i64]| v.iter().map(i64::to_string).collect::<Vec<_>>().join(",");

                let upd = ids(&format!(
                    "SELECT id FROM {t} ORDER BY id LIMIT {}",
                    cfg.updates_per_cycle
                ))?;
                if !upd.is_empty() {
                    self.source_raw(&format!(
                        "UPDATE {t} SET updated_at = NOW() WHERE id IN ({})",
                        in_list(&upd)
                    ))?;
                }
                let del = ids(&format!(
                    "SELECT id FROM {t} ORDER BY id DESC LIMIT {}",
                    cfg.deletes_per_cycle
                ))?;
                if !del.is_empty() {
                    self.source_raw(&format!("DELETE FROM {t} WHERE id IN ({})", in_list(&del)))?;
                }
                Ok(upd.into_iter().chain(del).collect())
            }
            other => bail!("churn not wired for {other:?} — encode as a `gap`"),
        }
    }

    /// One bounded CDC extract, returning its peak RSS in MiB. `/usr/bin/time -l`
    /// (macOS) reports "maximum resident set size" in bytes on stderr.
    fn cdc_run_peak_rss(&self, cdc_cfg: &Path) -> Result<f64> {
        let out = Command::new("/usr/bin/time")
            .arg("-l")
            .arg("rivet")
            .args(["run", "-c"])
            .arg(cdc_cfg)
            .output()
            .context("spawn timed rivet run")?;
        if !out.status.success() {
            bail!(
                "cdc cycle exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let stderr = String::from_utf8_lossy(&out.stderr);
        let bytes = stderr
            .lines()
            .find(|l| l.contains("maximum resident set size"))
            .and_then(|l| l.split_whitespace().next())
            .and_then(|n| n.parse::<f64>().ok())
            .context("parse peak RSS from `/usr/bin/time -l`")?;
        Ok(bytes / 1_048_576.0)
    }

    fn eval_soak(
        &self,
        oracle: SoakOracle,
        bq_table: &str,
        rss_mb: &[f64],
        churned_distinct: i64,
    ) -> Result<OracleOutcome> {
        match oracle {
            // Leak detector: last-quarter peak ≤ 1.5× first-quarter peak.
            SoakOracle::FlatRss => {
                if rss_mb.len() < 4 {
                    return Ok(OracleOutcome::Skipped {
                        why: "soak too short for an RSS trend (<4 cycles)".into(),
                    });
                }
                let q = rss_mb.len() / 4;
                let peak = |s: &[f64]| s.iter().copied().fold(0.0_f64, f64::max);
                let first = peak(&rss_mb[..q]);
                let last = peak(&rss_mb[rss_mb.len() - q..]);
                Ok(if last <= 1.5 * first {
                    OracleOutcome::Pass
                } else {
                    OracleOutcome::Fail {
                        detail: format!(
                            "RSS grew: last-quarter {last:.0}MB > 1.5× first-quarter {first:.0}MB (leak)"
                        ),
                    }
                })
            }
            // Completeness: every churned id is present in the warehouse.
            SoakOracle::ZeroGap => {
                let captured = self.wh_scalar(&format!(
                    "SELECT COUNT(DISTINCT id) FROM {}",
                    self.wh_table(bq_table)
                ))?;
                Ok(if captured >= churned_distinct {
                    OracleOutcome::Pass
                } else {
                    OracleOutcome::Fail {
                        detail: format!(
                            "gap: warehouse has {captured} distinct ids < {churned_distinct} churned"
                        ),
                    }
                })
            }
        }
    }

    // ── stage 1: seed the fixture (OSS `seed` binary, as a process) ──────────
    fn seed(&self, env: &HarnessEnv) -> Result<()> {
        let lane = self.lane();
        let Some(target) = lane.seed_target else {
            bail!(
                "engine {:?} has no OSS CLI seed yet — encode as a `gap`",
                self.engine
            );
        };
        let db = "rivet_bench";
        run(
            Command::new("cargo")
                .current_dir(&env.oss_dir)
                .args([
                    "run",
                    "--release",
                    "--features",
                    "dev-seed",
                    "--bin",
                    "seed",
                    "--",
                ])
                .args(["--target", target, lane.seed_url_flag, &lane.url(db)])
                .args(["--content-items", &self.fixture.rows.to_string()])
                // The matrix reads only `content_items` (no FK to the other tables),
                // so seed nothing else — skips ~8M unrelated rows the default profile
                // would generate, keeping every cell fast.
                .args([
                    "--users",
                    "0",
                    "--orders-per-user",
                    "0",
                    "--events-per-user",
                    "0",
                    "--page-views",
                    "0",
                ])
                .env("RIVET_SEED_I_KNOW", "1"),
            "seed",
        )
    }

    // ── stage 2: OSS extract → GCS parquet ───────────────────────────────────
    fn extract_to_gcs(&self, env: &HarnessEnv, work: &TempWork) -> Result<String> {
        // Trailing slash makes this a real "directory": rivet's GCS `prefix` is a
        // literal string prepended to each file, so `.../content_items` (no slash)
        // yields `content_items<file>` and every run's parquet shares one string
        // prefix — the load then reads them all. `.../content_items/` isolates a run.
        let prefix = format!(
            "matrix/{}/{}/",
            self.engine.source_type(),
            self.fixture.table
        );
        let full_prefix = format!("gs://{}/{}", env.gcs_bucket, prefix);
        // Clear any prior run's parquet so `load` doesn't accumulate stale rows.
        let _ = Command::new("gcloud")
            .args(["storage", "rm", "-r", &format!("{full_prefix}**")])
            .status();
        let cfg = work.write("extract.yaml", &self.extraction_yaml(env, &prefix)?);
        run(
            Command::new("rivet").args(["run", "-c"]).arg(&cfg),
            "rivet run (extract → gcs)",
        )?;
        Ok(full_prefix)
    }

    // ── stage 3: rivet load → warehouse ──────────────────────────────────────
    fn load(&self, env: &HarnessEnv, work: &TempWork, _gcs_prefix: &str) -> Result<String> {
        // `rivet load` names the target after the export's `table:` when present,
        // else its `name:`. Batch uses `query:` (no `table:`) so the engine-qualified
        // `name` wins; CDC uses `table:` so the loaded object is the bare table name.
        let table = self.loaded_table_name();
        // ONE config drives both extract and load — the same file, which carries
        // a top-level `load:` block. `rivet load` REPLACES the target from GCS
        // (storage is the source of truth). Free OSS command, no license.
        let cfg = work.path("extract.yaml");
        run(
            Command::new("rivet").args(["load", "-c"]).arg(&cfg),
            "rivet load",
        )?;
        self.warehouse_table_ref(env, &table)
    }

    /// The unqualified name `rivet load` gives the target: the engine-qualified
    /// export `name` for batch, the bare source `table` for CDC.
    fn loaded_table_name(&self) -> String {
        match self.mode {
            Mode::Batch => format!("{}_{}", self.engine.source_type(), self.fixture.table),
            Mode::Cdc => self.fixture.table.clone(),
        }
    }

    /// The fully-qualified id of the loaded table for the cell's warehouse —
    /// what the oracle re-reads. BigQuery: `project.dataset.table`; Snowflake:
    /// `database.schema.table`.
    fn warehouse_table_ref(&self, env: &HarnessEnv, table: &str) -> Result<String> {
        Ok(match self.warehouse {
            Warehouse::BigQuery => format!("{}.{MATRIX_DATASET}.{table}", env.bq_project()?),
            Warehouse::Snowflake => {
                let sf = env.sf()?;
                format!("{}.{}.{}", sf.database, sf.schema, table)
            }
        })
    }

    /// The cleanup side-effect: after a `cleanup_source: true` load, the GCS
    /// staging prefix must hold no objects. `gcloud storage ls` prints the keys
    /// to stdout when any exist, and prints nothing (exiting non-zero, "matched
    /// no objects" on stderr) when the prefix is empty — so an empty stdout is
    /// the PASS case, non-empty is FAIL with the leftover keys.
    fn staging_wiped(&self, gcs_prefix: &str) -> Result<OracleOutcome> {
        let out = Command::new("gcloud")
            .args(["storage", "ls", &format!("{gcs_prefix}**")])
            .output()
            .context("gcloud storage ls (staging_wiped)")?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let keys: Vec<&str> = stdout
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        if keys.is_empty() {
            Ok(OracleOutcome::Pass)
        } else {
            Ok(OracleOutcome::Fail {
                detail: format!(
                    "cleanup_source did not wipe the staging: {} object(s) remain under {gcs_prefix} (e.g. {})",
                    keys.len(),
                    keys.first().copied().unwrap_or("")
                ),
            })
        }
    }

    // ── stage 4: oracle — re-read the warehouse, compare to source ───────────
    fn evaluate(&self, wh_table: &str, oracle: WarehouseOracle) -> Result<OracleOutcome> {
        let t = self.wh_table(wh_table);
        match oracle {
            WarehouseOracle::RowCount => Ok(compare(
                "row-count",
                self.wh_scalar(&format!("SELECT COUNT(*) FROM {t}"))?,
                self.source_scalar(&format!("SELECT COUNT(*) FROM {}", self.fixture.table))?,
            )),
            WarehouseOracle::DistinctId => Ok(compare(
                "distinct-id",
                self.wh_scalar(&format!("SELECT COUNT(DISTINCT id) FROM {t}"))?,
                self.source_scalar(&format!(
                    "SELECT COUNT(DISTINCT id) FROM {}",
                    self.fixture.table
                ))?,
            )),
            // Per-column NULL count in the warehouse == source. A single column
            // silently degraded to 100% NULL is invisible to row-count/sum checks
            // (the FixedSizeBinary-nulls class) — this is the check that catches it.
            WarehouseOracle::NullProfile => {
                let cols = self.source_columns()?;
                if cols.is_empty() {
                    return Ok(OracleOutcome::Skipped {
                        why: "source has no columns".into(),
                    });
                }
                for c in &cols {
                    let wc = self.wh_col(c);
                    let dest =
                        self.wh_scalar(&format!("SELECT COUNT(*) - COUNT({wc}) FROM {t}"))?;
                    let src = self.source_scalar(&format!(
                        "SELECT COUNT(*) - COUNT({c}) FROM {}",
                        self.fixture.table
                    ))?;
                    if dest != src {
                        return Ok(OracleOutcome::Fail {
                            detail: format!(
                                "null-profile[{c}]: warehouse {dest} nulls != source {src}"
                            ),
                        });
                    }
                }
                Ok(OracleOutcome::Pass)
            }
            // Warehouse column type FAMILY == the source's semantic type family.
            // This is the loaders' headline: it catches jsonb→STRING, timestamptz→
            // DATETIME, numeric→FLOAT64 drift that row/null checks sail past. Both
            // sides read from INFORMATION_SCHEMA (SQL, no JSON parsing).
            WarehouseOracle::TypeFidelity => {
                let src = self.source_column_types()?;
                let dst = self.wh_column_types(wh_table)?;
                for (name, sfam) in &src {
                    match dst.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)) {
                        None => {
                            return Ok(OracleOutcome::Fail {
                                detail: format!(
                                    "type-fidelity[{name}]: column absent in warehouse"
                                ),
                            });
                        }
                        Some((_, dfam)) if dfam != sfam => {
                            return Ok(OracleOutcome::Fail {
                                detail: format!(
                                    "type-fidelity[{name}]: warehouse {dfam} != source {sfam}"
                                ),
                            });
                        }
                        _ => {}
                    }
                }
                Ok(OracleOutcome::Pass)
            }
            // StagingWiped inspects the GCS prefix, not the warehouse table, so the
            // runner dispatches it to `staging_wiped(&gcs_prefix)` directly (it holds
            // the prefix; `evaluate` does not). Reaching here means a runner forgot
            // that branch — bail loudly rather than silently pass.
            WarehouseOracle::StagingWiped => {
                bail!("StagingWiped must be dispatched via staging_wiped(), not evaluate()")
            }
        }
    }

    /// This verification's resolved [`Lane`] — the single home for targeting.
    fn lane(&self) -> Lane {
        Lane::of(self.engine, self.mode)
    }

    /// The source connection URL (mode-aware via the lane).
    fn url(&self, db: &str) -> String {
        self.lane().url(db)
    }

    /// The `docker exec` prefix for the SOURCE (container + SQL shell), from the
    /// lane; bails for a non-SQL source (Mongo has an empty client).
    fn source_client(&self) -> Result<(&'static str, Vec<&'static str>)> {
        let lane = self.lane();
        if lane.client.is_empty() {
            bail!("SQL source client not wired for {:?}", self.engine);
        }
        Ok((lane.container, lane.client.to_vec()))
    }

    /// Raw stdout of a SOURCE query.
    fn source_raw(&self, sql: &str) -> Result<String> {
        let (container, shell) = self.source_client()?;
        let mut cmd = Command::new("docker");
        cmd.args(["exec", container]).args(&shell).arg(sql);
        capture(&mut cmd, "source query")
    }

    /// A single scalar from the SOURCE.
    fn source_scalar(&self, sql: &str) -> Result<i64> {
        self.source_raw(sql)?
            .trim()
            .parse::<i64>()
            .context("parse source scalar")
    }

    /// The fixture table's columns, in ordinal order (drives NullProfile).
    fn source_columns(&self) -> Result<Vec<String>> {
        let schema_filter = self.lane().schema_qual;
        let sql = format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE {schema_filter}table_name='{}' ORDER BY ordinal_position",
            self.fixture.table
        );
        Ok(self
            .source_raw(&sql)?
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect())
    }

    /// `(column, semantic-type-family)` for the SOURCE, in ordinal order.
    fn source_column_types(&self) -> Result<Vec<(String, String)>> {
        let lane = self.lane();
        if lane.client.is_empty() {
            bail!("column types not wired for {:?}", self.engine);
        }
        let (schema_filter, sep) = (lane.schema_qual, lane.sep);
        let sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE {schema_filter}table_name='{}' ORDER BY ordinal_position",
            self.fixture.table
        );
        Ok(self
            .source_raw(&sql)?
            .lines()
            .filter_map(|l| l.trim().split_once(sep))
            .map(|(n, t)| (n.trim().to_string(), family_src(self.engine, t.trim())))
            .collect())
    }

    /// `(column, type-family)` for the loaded table, via the warehouse's
    /// `INFORMATION_SCHEMA`.
    fn wh_column_types(&self, wh_table: &str) -> Result<Vec<(String, String)>> {
        match self.warehouse {
            Warehouse::BigQuery => {
                // `project.dataset.table` → `project.dataset.INFORMATION_SCHEMA.COLUMNS`.
                let (proj_ds, table) = wh_table.rsplit_once('.').context("bq table id")?;
                let sql = format!(
                    "SELECT column_name, data_type FROM `{proj_ds}.INFORMATION_SCHEMA.COLUMNS` \
                     WHERE table_name = '{table}'"
                );
                Ok(bq_rows(&sql)?
                    .into_iter()
                    .map(|(n, t)| (n, family_bq(&t)))
                    .collect())
            }
            Warehouse::Snowflake => {
                // `db.schema.table`; INFORMATION_SCHEMA is per-database and stores
                // unquoted identifiers upper-cased.
                let parts: Vec<&str> = wh_table.split('.').collect();
                let [db, schema, table] = parts.as_slice() else {
                    bail!("snowflake table id must be db.schema.table: {wh_table}");
                };
                // Snowflake folds every integer AND decimal into `NUMBER`; split
                // them back by scale (scale 0 = integer) so the family matches the
                // source's int/decimal distinction instead of failing every int.
                let sql = format!(
                    "SELECT column_name, \
                     CASE WHEN data_type = 'NUMBER' AND numeric_scale = 0 THEN 'INT' \
                          ELSE data_type END \
                     FROM {db}.INFORMATION_SCHEMA.COLUMNS \
                     WHERE table_schema = UPPER('{schema}') AND table_name = UPPER('{table}')"
                );
                Ok(sf_rows(&self.sf_conn()?, &sql)?
                    .into_iter()
                    .map(|(n, t)| (n, family_sf(&t)))
                    .collect())
            }
        }
    }

    /// The warehouse-quoted table id for a re-read query (BigQuery back-ticks the
    /// path; Snowflake leaves it bare so the unquoted upper-cased object resolves).
    fn wh_table(&self, t: &str) -> String {
        match self.warehouse {
            Warehouse::BigQuery => format!("`{t}`"),
            Warehouse::Snowflake => t.to_string(),
        }
    }

    /// A column reference for a re-read query (BigQuery back-tick; Snowflake bare).
    fn wh_col(&self, c: &str) -> String {
        match self.warehouse {
            Warehouse::BigQuery => format!("`{c}`"),
            Warehouse::Snowflake => c.to_string(),
        }
    }

    /// A single scalar re-read from the cell's warehouse.
    fn wh_scalar(&self, sql: &str) -> Result<i64> {
        match self.warehouse {
            Warehouse::BigQuery => bq_scalar(sql),
            Warehouse::Snowflake => sf_scalar(&self.sf_conn()?, sql),
        }
    }

    /// The `snow` connection name for a Snowflake cell.
    fn sf_conn(&self) -> Result<String> {
        std::env::var("RIVET_SF_CONNECTION").context("RIVET_SF_CONNECTION unset (Snowflake cell)")
    }

    // ── config generators (hand-written YAML — no serde dep) ─────────────────
    fn extraction_yaml(&self, env: &HarnessEnv, gcs_prefix: &str) -> Result<String> {
        let lane = self.lane();
        // The `load:` block — per-warehouse target config, declared once.
        let load_block = match self.warehouse {
            Warehouse::BigQuery => format!(
                "load:\n  target: bigquery\n  project: {project}\n  dataset: {dataset}\n  cleanup_source: true\n",
                project = env.bq_project()?,
                dataset = MATRIX_DATASET,
            ),
            Warehouse::Snowflake => {
                let sf = env.sf()?;
                format!(
                    "load:\n  target: snowflake\n  connection: {conn}\n  warehouse: {wh}\n  \
                     database: {db}\n  schema: {sc}\n  storage_integration: {si}\n  \
                     cleanup_source: true\n",
                    conn = sf.connection,
                    wh = sf.warehouse,
                    db = sf.database,
                    sc = sf.schema,
                    si = sf.storage_integration,
                )
            }
        };
        // CDC/incremental loads carry the dedup view's PK in the `load:` block —
        // the CLI no longer takes `--pk`. SQL sources key on `id`, Mongo on `_id`.
        let load_block = if self.mode == Mode::Cdc {
            let pk = match self.engine {
                Engine::Mongo => "_id",
                _ => "id",
            };
            format!("{load_block}  pk: [{pk}]\n")
        } else {
            load_block
        };
        let (mode, cdc_fields) = match self.mode {
            Mode::Batch => ("full", String::new()),
            Mode::Cdc => {
                // A per-fixture checkpoint so a bounded run resumes where the last
                // left off (the anchor-at-open discipline the OSS Rig follows).
                let ckpt =
                    std::env::temp_dir().join(format!("ckpt-{}", gcs_prefix.replace('/', "_")));
                // `initial: snapshot` backfills preexisting rows before draining.
                let initial = if self.initial_snapshot {
                    "      initial: snapshot\n"
                } else {
                    ""
                };
                (
                    "cdc",
                    format!(
                        "    cdc:\n      until_current: true\n      checkpoint: {}\n{}{}",
                        ckpt.display(),
                        initial,
                        lane.cdc_field
                    ),
                )
            }
        };
        // The export NAME becomes the loaded BigQuery table name — qualify it by
        // engine so `mysql`/`pg` runs don't collide, and so the oracle (which
        // queries `{engine}_{table}`) reads exactly what `load` created.
        let name = format!("{}_{}", lane.source_type, self.fixture.table);
        // CDC captures a TABLE (via binlog/slot); batch runs a query.
        let source_ref = match self.mode {
            Mode::Batch => format!("query: \"SELECT * FROM {}\"", self.fixture.table),
            Mode::Cdc => format!("table: {}", self.fixture.table),
        };
        // ONE config: source + exports + destination + a top-level `load:` block.
        // `rivet run` ignores `load:` (extract); `rivet load` reads it (load).
        Ok(format!(
            "source:\n  type: {ty}\n  url: \"{url}\"\nexports:\n  - name: {name}\n    \
             {source_ref}\n    mode: {mode}\n{cdc_fields}    format: parquet\n    \
             destination:\n      type: gcs\n      bucket: {bucket}\n      prefix: {prefix}\n\
             {load_block}",
            ty = lane.source_type,
            url = lane.url("rivet_bench"),
            mode = mode,
            cdc_fields = cdc_fields,
            bucket = env.gcs_bucket,
            prefix = gcs_prefix,
        ))
    }
}

// ── shell-out helpers ────────────────────────────────────────────────────────

fn run(cmd: &mut Command, what: &str) -> Result<()> {
    let status = cmd.status().with_context(|| format!("spawn {what}"))?;
    if !status.success() {
        bail!("{what} exited {status}");
    }
    Ok(())
}

fn capture(cmd: &mut Command, what: &str) -> Result<String> {
    let out = cmd.output().with_context(|| format!("spawn {what}"))?;
    if !out.status.success() {
        bail!(
            "{what} exited {}: stderr=[{}] stdout=[{}]",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
            String::from_utf8_lossy(&out.stdout).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Pass iff the warehouse re-read equals the source; the oracle's verdict.
fn compare(what: &str, dest: i64, src: i64) -> OracleOutcome {
    if dest == src {
        OracleOutcome::Pass
    } else {
        OracleOutcome::Fail {
            detail: format!("{what}: warehouse {dest} != source {src}"),
        }
    }
}

/// `bq query` returning a single scalar (CSV, header-stripped).
fn bq_scalar(sql: &str) -> Result<i64> {
    let out = capture(
        Command::new("bq").args(["query", "--nouse_legacy_sql", "--format=csv", sql]),
        "bq query",
    )?;
    out.lines()
        .nth(1) // line 0 is the header
        .and_then(|l| l.trim().parse::<i64>().ok())
        .context("parse bq scalar")
}

/// `bq query` returning `(col0, col1)` rows (CSV, header-stripped).
fn bq_rows(sql: &str) -> Result<Vec<(String, String)>> {
    let out = capture(
        Command::new("bq").args(["query", "--nouse_legacy_sql", "--format=csv", sql]),
        "bq query",
    )?;
    Ok(out
        .lines()
        .skip(1)
        .filter_map(|l| l.split_once(','))
        .map(|(a, b)| (a.trim().to_string(), b.trim().to_string()))
        .collect())
}

/// Source `data_type` → the fidelity-critical semantic family. First cut: the
/// distinctions that actually drift (json/tstz/decimal); unknown types fall back
/// to the raw string so a mismatch surfaces rather than hiding.
fn family_src(engine: Engine, data_type: &str) -> String {
    let t = data_type.to_ascii_lowercase();
    let fam = match engine {
        Engine::Postgres => match t.as_str() {
            "jsonb" | "json" => "json",
            "timestamp with time zone" => "ts_tz",
            "timestamp without time zone" => "ts_notz",
            "date" => "date",
            "numeric" => "decimal",
            "double precision" | "real" => "float",
            "smallint" | "integer" | "bigint" => "int",
            "boolean" => "bool",
            "text" | "character varying" | "character" => "text",
            "bytea" => "bytes",
            "uuid" => "uuid",
            _ => return t,
        },
        Engine::Mysql => match t.as_str() {
            "json" => "json",
            "datetime" => "ts_notz",
            "timestamp" => "ts_tz",
            "date" => "date",
            "decimal" => "decimal",
            "double" | "float" => "float",
            "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => "int",
            "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" => "text",
            "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => "bytes",
            _ => return t,
        },
        _ => return t,
    };
    fam.to_string()
}

/// BigQuery `data_type` → the same semantic family space as [`family_src`].
fn family_bq(data_type: &str) -> String {
    let fam = match data_type.to_ascii_uppercase().as_str() {
        "JSON" => "json",
        "TIMESTAMP" => "ts_tz",
        "DATETIME" => "ts_notz",
        "DATE" => "date",
        "NUMERIC" | "BIGNUMERIC" => "decimal",
        "FLOAT64" => "float",
        "INT64" => "int",
        "BOOL" => "bool",
        "STRING" => "text",
        "BYTES" => "bytes",
        other => return other.to_lowercase(),
    };
    fam.to_string()
}

/// `snow sql --format json`, returning the parsed JSON array. `snow` echoes the
/// statement before the result, so slice from the first `[`.
fn snow_json(conn: &str, sql: &str) -> Result<serde_json::Value> {
    let mut cmd = Command::new("snow");
    cmd.args(["sql", "-c", conn, "--format", "json", "-q", sql]);
    // The loader reads the key path from this env; mirror it so the oracle uses
    // the same connection identity.
    if let Ok(key) = std::env::var("RIVET_SNOWFLAKE_KEY") {
        cmd.env(
            format!(
                "SNOWFLAKE_CONNECTIONS_{}_PRIVATE_KEY_PATH",
                conn.to_uppercase()
            ),
            key,
        );
    }
    let out = capture(&mut cmd, "snow sql")?;
    let start = out.find('[').context("snow: no JSON array in output")?;
    serde_json::from_str(&out[start..]).with_context(|| format!("parse snow json from: {out}"))
}

/// `snow sql` returning a single scalar (the first column of the first row).
fn sf_scalar(conn: &str, sql: &str) -> Result<i64> {
    let v = snow_json(conn, sql)?;
    let val = v
        .get(0)
        .and_then(|row| row.as_object())
        .and_then(|o| o.values().next())
        .context("snow: empty result")?;
    match val {
        serde_json::Value::Number(n) => n.as_i64().context("snow: scalar not an i64"),
        serde_json::Value::String(s) => s.trim().parse().context("snow: parse scalar string"),
        other => bail!("snow: unexpected scalar {other}"),
    }
}

/// `snow sql` returning `(col0, col1)` rows. serde_json orders a row's keys
/// (BTreeMap default, or SELECT order under `preserve_order`) so for a
/// `SELECT column_name, data_type` both give `(name, type)`.
fn sf_rows(conn: &str, sql: &str) -> Result<Vec<(String, String)>> {
    let v = snow_json(conn, sql)?;
    let mut out = Vec::new();
    for row in v.as_array().context("snow: result not an array")? {
        let obj = row.as_object().context("snow: row not an object")?;
        let mut vals = obj.values();
        let a = vals.next().context("snow: row missing col0")?;
        let b = vals.next().context("snow: row missing col1")?;
        out.push((json_str(a), json_str(b)));
    }
    Ok(out)
}

fn json_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Snowflake `data_type` → the same semantic family space as [`family_bq`].
fn family_sf(data_type: &str) -> String {
    // Snowflake normalizes types: NUMBER(p,s), TIMESTAMP_TZ(9), VARCHAR(n), …
    // Take the head token before any `(` and map the family.
    let head = data_type
        .split('(')
        .next()
        .unwrap_or(data_type)
        .trim()
        .to_ascii_uppercase();
    let fam = match head.as_str() {
        "VARIANT" | "OBJECT" => "json",
        "TIMESTAMP_TZ" | "TIMESTAMP_LTZ" => "ts_tz",
        "TIMESTAMP_NTZ" | "DATETIME" => "ts_notz",
        "DATE" => "date",
        "INT" => "int",
        "NUMBER" | "DECIMAL" | "NUMERIC" => "decimal",
        "FLOAT" | "DOUBLE" | "REAL" => "float",
        "BOOLEAN" => "bool",
        "TEXT" | "VARCHAR" | "STRING" | "CHAR" => "text",
        "BINARY" | "VARBINARY" => "bytes",
        other => return other.to_lowercase(),
    };
    fam.to_string()
}

/// A self-cleaning tempdir for generated configs.
struct TempWork {
    dir: PathBuf,
}

impl TempWork {
    fn new(tag: &str) -> Result<Self> {
        let dir = std::env::temp_dir().join(format!("{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        Ok(TempWork { dir })
    }
    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let p = self.path(name);
        std::fs::write(&p, contents).expect("write temp config");
        p
    }
}

impl Drop for TempWork {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
