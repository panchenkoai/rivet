//! Oracle Database source (19c+) over Oracle's pure-Rust thin driver `oracledb`.
//!
//! Design notes live in `dev/research/oracle-source.md`. The two load-bearing
//! choices: every connection pins its session formats (a cursor is re-injected as
//! text and Oracle converts it through the session's NLS masks), and every export
//! query is re-projected server-side so types the driver decodes wrongly never
//! reach it (`TIMESTAMP WITH TIME ZONE` holding a region name panics the driver).

mod arrow_convert;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use oracledb::{Connection, Metadata, OracleNumber, OracleTimestamp, Row};

use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::batch_controller::{
    AdaptiveBatchController, DEFAULT_BATCH_TARGET_MB, PROBE_BATCH_SIZE,
};
use crate::source::query::build_export_query;
use crate::source::{BatchSink, ExportRequest, Source};
use crate::types::{ColumnOverrides, TypeMapping};

/// `oracledb::Error` implements only `Debug`; lift it into anyhow.
pub(crate) trait Ora<T> {
    fn ora(self) -> Result<T>;
}

impl<T> Ora<T> for std::result::Result<T, oracledb::Error> {
    fn ora(self) -> Result<T> {
        self.map_err(|e| {
            let msg = format!("{e:?}");
            match known_failure_hint(&msg) {
                Some(hint) => anyhow::anyhow!("oracle: {msg}\n  hint: {hint}"),
                None => anyhow::anyhow!("oracle: {msg}"),
            }
        })
    }
}

/// An actionable hint for failures whose cause rivet has measured.
fn known_failure_hint(msg: &str) -> Option<&'static str> {
    msg.contains("unknown TTC message type").then_some(
        "the Oracle thin driver (oracledb 26.0.0-beta.4) lost protocol sync. rivet avoids \
         both known triggers (prefetch_rows=1 for a LOB in a wide row; no statement cache, \
         whose re-execution desyncs on wide rows); please report this query shape",
    )
}

/// Session state pinned on every connection: rendering and implicit conversion
/// must not depend on the database's or the client host's defaults.
const SESSION_PIN: &[&str] = &[
    "ALTER SESSION SET TIME_ZONE = '+00:00'",
    "ALTER SESSION SET NLS_CALENDAR = 'GREGORIAN'",
    "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'",
    // A keyset / cursor seek compares keys the way ORDER BY sorts them: bytewise.
    "ALTER SESSION SET NLS_SORT = BINARY",
    "ALTER SESSION SET NLS_COMP = BINARY",
    // A DATE cursor is rendered from a microsecond timestamp, so its fraction is always zero.
    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS\".000000\"'",
    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS.FF'",
    "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS.FF TZH:TZM'",
];

/// The run-window harm counters: lock waits (count + ms), disk sorts (the temp
/// spill `job::spill_total` folds via its `temp_files` key), and read volume.
const HARM_COUNTERS_SQL: &str = "\
    SELECT 'oracle_lock_waits', NVL(SUM(total_waits), 0) FROM v$system_event \
      WHERE event LIKE 'enq: TX%' OR event LIKE 'enq: TM%' \
    UNION ALL SELECT 'oracle_lock_wait_ms', NVL(SUM(time_waited_micro), 0) / 1000 FROM v$system_event \
      WHERE event LIKE 'enq: TX%' OR event LIKE 'enq: TM%' \
    UNION ALL SELECT 'oracle_temp_files', value FROM v$sysstat WHERE name = 'sorts (disk)' \
    UNION ALL SELECT 'oracle_physical_reads', value FROM v$sysstat WHERE name = 'physical reads' \
    UNION ALL SELECT 'oracle_consistent_gets', value FROM v$sysstat WHERE name = 'consistent gets'";

/// The parts of an `oracle://user:pass@host:port/service` URL.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OracleUrl {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub service: String,
}

/// Parse `oracle://user:pass@host[:port]/service`; userinfo is percent-decoded.
pub(crate) fn parse_oracle_url(url: &str) -> Result<OracleUrl> {
    let rest = url.strip_prefix("oracle://").ok_or_else(|| {
        anyhow::anyhow!("oracle: URL must start with oracle:// (got a different scheme)")
    })?;
    let (userinfo, hostpart) = rest.rsplit_once('@').ok_or_else(|| {
        anyhow::anyhow!(
            "oracle: URL needs user:password@host — oracle://user:pass@host:1521/SERVICE"
        )
    })?;
    let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    let (authority, service) = hostpart.split_once('/').ok_or_else(|| {
        anyhow::anyhow!(
            "oracle: URL needs a service name path — oracle://user:pass@host:1521/SERVICE"
        )
    })?;
    let service = service.split(['?', '#']).next().unwrap_or_default();
    anyhow::ensure!(!service.is_empty(), "oracle: URL has an empty service name");
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h,
            p.parse::<u16>()
                .map_err(|_| anyhow::anyhow!("oracle: invalid port {p:?} in URL"))?,
        ),
        None => (authority, 1521),
    };
    anyhow::ensure!(!host.is_empty(), "oracle: URL has an empty host");
    let decode = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8_lossy()
            .into_owned()
    };
    Ok(OracleUrl {
        user: decode(user),
        password: decode(password),
        host: host.to_string(),
        port,
        service: service.to_string(),
    })
}

/// The driver connect string: plaintext for no/disabled TLS, `tcps://` otherwise.
fn connect_string(parts: &OracleUrl, tls: Option<&TlsConfig>) -> Result<String> {
    let enforced = tls.is_some_and(|t| t.mode.is_enforced());
    if let Some(t) = tls.filter(|_| enforced)
        && t.ca_file.is_some()
    {
        anyhow::bail!(
            "oracle: `tls.ca_file` is not supported yet — the driver verifies against the \
             system trust store; install the CA there, or use `tls.mode: require`"
        );
    }
    let scheme = if enforced { "tcps://" } else { "" };
    Ok(format!(
        "{scheme}{}:{}/{}",
        parts.host, parts.port, parts.service
    ))
}

/// Open and pin a connection.
pub(crate) fn connect(url: &str, tls: Option<&TlsConfig>) -> Result<Connection> {
    crate::source::require_tls_or_loopback(url, tls)?;
    let parts = parse_oracle_url(url)?;
    // The driver builds its TLS config from the process default provider, and the
    // binary links two (ring via reqwest, aws-lc via the driver): pick one.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = oracledb::Config::default()
        .set_credentials(&parts.user, &parts.password)
        .set_connect_string(&connect_string(&parts, tls)?)
        .ora()?
        // Re-executing a cached statement over a wide row desyncs the beta driver.
        .set_stmtcachesize(0);
    let conn = oracledb::connect(config)
        .ora()
        .map_err(|e| crate::source::describe_connect_error(url, e))?;
    for sql in SESSION_PIN {
        conn.execute(sql, &[]).ora()?;
    }
    Ok(conn)
}

/// The server-side expression a column is fetched through, or `None` to fetch it as is.
fn projection_expr(meta: &Metadata, quoted: &str) -> Option<Result<String>> {
    Some(Ok(match meta.db_type().name() {
        // The driver decodes a region-named zone with `todo!()`; UTC on the server instead.
        "DB_TYPE_TIMESTAMP_TZ" | "DB_TYPE_TIMESTAMP_LTZ" => format!("SYS_EXTRACT_UTC({quoted})"),
        // TO_CHAR on an INTERVAL DAY(0) raises ORA-01877; intervals are decoded
        // from the driver's signed fields instead (arrow_convert::interval_iso).
        "DB_TYPE_ROWID" | "DB_TYPE_UROWID" => format!("ROWIDTOCHAR({quoted})"),
        "DB_TYPE_JSON" => format!("JSON_SERIALIZE({quoted} RETURNING CLOB)"),
        // The driver describes XMLTYPE as DB_TYPE_OBJECT; any other object type fails here loudly.
        "DB_TYPE_XMLTYPE" | "DB_TYPE_OBJECT" => format!("XMLSERIALIZE(CONTENT {quoted} AS CLOB)"),
        "DB_TYPE_VECTOR" => format!("FROM_VECTOR({quoted} RETURNING CLOB)"),
        _ => return None,
    }))
}

/// A result column name an outer query cannot reference: the ROWID pseudo-column
/// shadows it, or it cannot be written as a quoted identifier.
fn unreferenceable(name: &str) -> bool {
    name == "ROWID" || name.contains('"') || name.len() > 128
}

/// The native type label a mapping reports: the DECLARED type, before re-projection.
fn native_type(meta: &Metadata) -> String {
    let base = meta
        .db_type()
        .name()
        .trim_start_matches("DB_TYPE_")
        .to_lowercase();
    match meta.db_type().name() {
        "DB_TYPE_NUMBER" if meta.precision() > 0 && meta.scale() != -127 => {
            format!("number({},{})", meta.precision(), meta.scale())
        }
        "DB_TYPE_TIMESTAMP" | "DB_TYPE_TIMESTAMP_TZ" | "DB_TYPE_TIMESTAMP_LTZ" => {
            format!("{base}({})", meta.scale())
        }
        _ => base,
    }
}

/// An export query as rivet fetches it.
struct Projection {
    sql: String,
    /// Each exported column's declared type (before re-projection).
    native: Vec<String>,
    /// For a LOB column, the index of its trailing "zero-length" flag column.
    empty_flags: Vec<Option<usize>>,
}

pub struct OracleSource {
    conn: Connection,
}

impl OracleSource {
    /// Connect to `oracle://user:pass@host:port/service`, honouring the shared TLS config.
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        Ok(Self {
            conn: connect(url, tls)?,
        })
    }

    /// Every row of `sql`, each cell as text, read through the same re-projection as an export.
    pub(crate) fn query_rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let sql = self.projected(sql)?.sql;
        let cursor = self.conn.query(&sql, &[]).ora()?;
        let n = cursor.columns().len();
        cursor
            .map(|row| {
                let row = row.ora()?;
                (0..n).map(|i| cell_text(&row, i)).collect()
            })
            .collect()
    }

    /// The first column of every row of `sql`, NULLs dropped.
    pub(crate) fn query_list(&mut self, sql: &str) -> Result<Vec<String>> {
        Ok(self
            .query_rows(sql)?
            .into_iter()
            .filter_map(|r| r.into_iter().next().flatten())
            .collect())
    }

    /// Every row of a `(name, number)` query, as integer pairs; `None` when the probe fails.
    fn named_counters(&self, sql: &str) -> Option<Vec<(String, i64)>> {
        let cursor = self.conn.query(sql, &[]).ok()?;
        let mut out = Vec::new();
        for row in cursor {
            let row = row.ok()?;
            let name = cell_text(&row, 0).ok()??;
            let value = cell_text(&row, 1).ok()??.parse::<f64>().ok()? as i64;
            out.push((name, value));
        }
        Some(out)
    }

    /// Column metadata of `query` without fetching a row. Locator mode, so a LOB is
    /// described as CLOB/BLOB (an inline fetch reports it as LONG / LONG RAW).
    fn describe(&self, query: &str) -> Result<Vec<Metadata>> {
        let cursor = self
            .conn
            .statement(&format!("SELECT * FROM ({query}) \"_rivet_d\" WHERE 1 = 0"))
            .ora()?
            .fetch_lobs()
            .build()
            .ora()?
            .query(&[])
            .ora()?;
        Ok(cursor.columns().to_vec())
    }

    /// `query` re-projected so every column is one the row decoder reads correctly.
    fn projected(&self, query: &str) -> Result<Projection> {
        let metas = self.describe(query)?;
        let native: Vec<String> = metas.iter().map(native_type).collect();
        let mut rewritten = false;
        let mut cols = Vec::with_capacity(metas.len());
        let mut flags = Vec::new();
        let mut empty_flags = vec![None; metas.len()];
        for (i, m) in metas.iter().enumerate() {
            let quoted = crate::sql::quote_ident(crate::config::SourceType::Oracle, m.name());
            match projection_expr(m, &quoted) {
                Some(expr) => {
                    rewritten = true;
                    cols.push(format!("{} {quoted}", expr?));
                }
                None => cols.push(quoted.clone()),
            }
            // The driver returns a zero-length LOB as NULL; the server says which it was.
            if matches!(
                m.db_type().name(),
                "DB_TYPE_CLOB" | "DB_TYPE_NCLOB" | "DB_TYPE_BLOB"
            ) {
                rewritten = true;
                empty_flags[i] = Some(metas.len() + flags.len());
                flags.push(format!(
                    "CASE WHEN DBMS_LOB.GETLENGTH({quoted}) = 0 THEN 1 END \"_rivet_empty_{i}\""
                ));
            }
        }
        let sql = if rewritten {
            if let Some(m) = metas.iter().find(|m| unreferenceable(m.name())) {
                anyhow::bail!(
                    "oracle: column {:?} must be re-read through a conversion, but that name \
                     cannot be referenced from an outer query — give it an alias in the \
                     `query:` (e.g. `... AS row_id`)",
                    m.name()
                );
            }
            cols.extend(flags);
            format!("SELECT {} FROM ({query}) \"_rivet_p\"", cols.join(", "))
        } else {
            query.to_string()
        };
        Ok(Projection {
            sql,
            native,
            empty_flags,
        })
    }
}

/// One row cell as text, whatever its type — for scalar catalog probes.
fn cell_text(row: &Row, idx: usize) -> Result<Option<String>> {
    let Some(meta) = row.columns().get(idx) else {
        return Ok(None);
    };
    Ok(match meta.db_type().name() {
        "DB_TYPE_NUMBER" => row
            .get::<Option<OracleNumber>>(idx)
            .ora()?
            .map(|n| n.to_string()),
        "DB_TYPE_BINARY_FLOAT" => row.get::<Option<f32>>(idx).ora()?.map(|v| v.to_string()),
        "DB_TYPE_BINARY_DOUBLE" => row.get::<Option<f64>>(idx).ora()?.map(|v| v.to_string()),
        "DB_TYPE_BOOLEAN" => row.get::<Option<bool>>(idx).ora()?.map(|v| v.to_string()),
        "DB_TYPE_DATE" | "DB_TYPE_TIMESTAMP" => row
            .get::<Option<OracleTimestamp>>(idx)
            .ora()?
            .map(|t| arrow_convert::timestamp_micros(&t))
            .transpose()?
            .and_then(chrono::DateTime::from_timestamp_micros)
            .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()),
        _ => row.get::<Option<String>>(idx).ora()?,
    })
}

impl Source for OracleSource {
    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()> {
        let projection = self.projected(request.query)?;
        let reprojected = ExportRequest {
            query: &projection.sql,
            ..*request
        };
        let built = build_export_query(&reprojected, crate::config::SourceType::Oracle);
        let mut ctl =
            AdaptiveBatchController::new(request.tuning, request.tuning.batch_size.max(1));
        let stmt_timeout = (request.tuning.statement_timeout_s > 0)
            .then(|| std::time::Duration::from_secs(request.tuning.statement_timeout_s));
        let max_value_bytes = request.tuning.max_value_bytes();

        let started = std::time::Instant::now();
        let fetch = u32::try_from(ctl.target().max(PROBE_BATCH_SIZE)).unwrap_or(u32::MAX);
        let stmt = self
            .conn
            .statement(&built.sql)
            .ora()?
            .fetch_array_size(fetch)
            // A larger prefetch with a LOB in a wide row desyncs the thin driver's
            // protocol (measured: fails at 3+, correct at 0..=2).
            .prefetch_rows(1)
            .build()
            .ora()?;
        let cursor = match &built.cursor_param {
            Some(v) => {
                let bind: &dyn oracledb::ToDbValue = v;
                stmt.query(&[bind]).ora()?
            }
            None => stmt.query(&[]).ora()?,
        };
        let metas = cursor.columns()[..projection.native.len()].to_vec();
        let empty_flags = projection.empty_flags.clone();
        let schema: SchemaRef = Arc::new(arrow_convert::oracle_schema(
            &metas,
            &projection.native,
            request.column_overrides,
        )?);
        ctl.raise_configured_ceiling(request.tuning.effective_batch_size(Some(&schema)));
        sink.on_schema(Arc::clone(&schema))?;

        let mut buf: Vec<Row> = Vec::with_capacity(ctl.target());
        let mut cap_applied = false;
        let mut emit = |buf: &mut Vec<Row>, ctl: &mut AdaptiveBatchController| -> Result<()> {
            let batch = arrow_convert::rows_to_batch(buf, &schema, max_value_bytes, &empty_flags)?;
            let n = buf.len();
            buf.clear();
            sink.on_batch(&batch)?;
            if !cap_applied && n > 0 {
                let per_row = (batch.get_array_memory_size() / n).max(64);
                let target_mb = request
                    .tuning
                    .batch_size_memory_mb
                    .unwrap_or(DEFAULT_BATCH_TARGET_MB);
                let safe = ((target_mb * 1024 * 1024) / per_row).max(PROBE_BATCH_SIZE);
                if let Some(new) = ctl.apply_memory_cap(safe) {
                    log::info!(
                        "Oracle batch cap: arrow≈{per_row} B/row, target={target_mb} MB → batch_size → {new}"
                    );
                }
                cap_applied = true;
            }
            ctl.after_batch(|| None);
            ctl.throttle(n);
            Ok(())
        };
        for row in cursor {
            if let Some(budget) = stmt_timeout
                && started.elapsed() > budget
            {
                return Err(
                    crate::source::StatementDurationTimeout::oracle(budget.as_secs()).into(),
                );
            }
            buf.push(row.ora()?);
            if buf.len() >= ctl.target() {
                emit(&mut buf, &mut ctl)?;
            }
        }
        if !buf.is_empty() {
            emit(&mut buf, &mut ctl)?;
        }
        Ok(())
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let sql = self.projected(sql)?.sql;
        let mut cursor = self.conn.query(&sql, &[]).ora()?;
        match cursor.next() {
            Some(row) => cell_text(&row.ora()?, 0),
            None => Ok(None),
        }
    }

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        let projection = self.projected(query)?;
        let metas = self.describe(&projection.sql)?;
        Ok(arrow_convert::oracle_type_mappings(
            &metas[..projection.native.len()],
            &projection.native,
            column_overrides,
        ))
    }

    /// Source-harm counters over the run window (needs SELECT on V$SYSSTAT /
    /// V$SYSTEM_EVENT, e.g. `SELECT_CATALOG_ROLE`; `None` without it).
    fn harm_counters(&mut self) -> Option<Vec<(String, i64)>> {
        self.named_counters(HARM_COUNTERS_SQL)
    }

    /// Foreign redo-write pressure for the governor: `redo log space requests`
    /// — sessions waiting for log space, which a read-only export cannot cause.
    fn sample_governor_pressure(&mut self) -> Option<u64> {
        self.query_scalar("SELECT value FROM v$sysstat WHERE name = 'redo log space requests'")
            .ok()
            .flatten()
            .and_then(|s| s.parse::<u64>().ok())
    }

    fn server_context(&mut self) -> Option<String> {
        let version = self.conn.version().ok()?;
        let banner = self
            .query_scalar("SELECT banner_full FROM v$version WHERE ROWNUM = 1")
            .ok()
            .flatten();
        Some(
            serde_json::json!({
                "engine": "oracle",
                "version": format!("{version:?}"),
                "banner": banner,
            })
            .to_string(),
        )
    }

    fn primary_key(&mut self, table: &str) -> Result<Option<Vec<String>>> {
        let (owner, name) = crate::sql::oracle_catalog_preds(table);
        let sql = format!(
            "SELECT cc.column_name \
             FROM all_constraints c JOIN all_cons_columns cc \
               ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name \
             WHERE c.constraint_type = 'P' AND c.table_name = {name} AND c.owner = {owner} \
             ORDER BY cc.position"
        );
        Ok(crate::source::non_empty_keys(self.query_list(&sql)?))
    }
}

/// Catalog facts the planner needs to chunk or keyset-page `qualified_table`.
pub(crate) fn introspect_oracle_table_for_chunking(
    url: &str,
    tls: Option<&TlsConfig>,
    qualified_table: &str,
) -> Result<crate::source::TableIntrospection> {
    let (owner, table) = crate::sql::oracle_catalog_preds(qualified_table);
    let mut src = OracleSource::connect_with_tls(url, tls)?;
    let row_estimate = src
        .query_scalar(&format!(
            "SELECT NVL(num_rows, 0) FROM all_tables WHERE owner = {owner} AND table_name = {table}"
        ))?
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    // Integer NUMBER(p<=18, 0): the columns range chunking can do arithmetic on.
    let int_pred = "tc.data_type = 'NUMBER' AND tc.data_scale = 0 AND tc.data_precision <= 18";
    let int_columns = src.query_list(&format!(
        "SELECT tc.column_name FROM all_tab_columns tc \
         WHERE tc.owner = {owner} AND tc.table_name = {table} AND {int_pred} ORDER BY tc.column_id"
    ))?;
    let single_int_pk = src
        .query_scalar(&format!(
            "SELECT MIN(cc.column_name) FROM all_constraints c \
             JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name \
             JOIN all_tab_columns tc ON tc.owner = cc.owner AND tc.table_name = cc.table_name \
               AND tc.column_name = cc.column_name \
             WHERE c.constraint_type = 'P' AND c.owner = {owner} AND c.table_name = {table} \
             HAVING COUNT(*) = 1 AND MIN(CASE WHEN {int_pred} THEN 1 ELSE 0 END) = 1"
        ))?;
    // Keyset keys: single-column UNIQUE indexes on NOT NULL columns of a type the
    // cursor reads back (integer NUMBER of any precision, bare NUMBER as exact text, strings, DATE,
    // zone-less TIMESTAMP(0..6) — finer is read at µs); PK first. Decimal keys are refused by exclusion.
    let keyset_keys = src.query_list(&format!(
        "SELECT col FROM ( \
           SELECT ic.column_name col, \
                  MAX(CASE WHEN c.constraint_type = 'P' THEN 1 ELSE 0 END) is_pk \
           FROM all_indexes i \
           JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name \
           JOIN all_tab_columns tc ON tc.owner = i.table_owner AND tc.table_name = i.table_name \
             AND tc.column_name = ic.column_name \
           LEFT JOIN all_constraints c ON c.owner = i.table_owner AND c.index_name = i.index_name \
             AND c.constraint_type = 'P' \
           WHERE i.uniqueness = 'UNIQUE' AND i.table_owner = {owner} AND i.table_name = {table} \
             AND tc.nullable = 'N' \
             AND (SELECT COUNT(*) FROM all_ind_columns x \
                  WHERE x.index_owner = i.owner AND x.index_name = i.index_name) = 1 \
             AND ((tc.data_type = 'NUMBER' AND tc.data_precision IS NULL AND tc.data_scale IS NULL) \
               OR (tc.data_type = 'NUMBER' AND tc.data_scale = 0) \
               OR tc.data_type IN ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR', 'DATE') \
               OR (tc.data_type LIKE 'TIMESTAMP%' AND tc.data_type NOT LIKE '%ZONE%' \
                   AND tc.data_scale <= 6)) \
           GROUP BY ic.column_name) ORDER BY is_pk DESC, col"
    ))?;
    Ok(crate::source::TableIntrospection {
        single_int_pk,
        keyset_keys,
        row_estimate,
        avg_row_bytes: None,
        int_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_an_outer_query_cannot_reference_are_detected() {
        assert!(unreferenceable("ROWID"));
        assert!(unreferenceable("FROM_TZ(CAST(\"DATE\"ASTIMESTAMP),'UTC')"));
        assert!(unreferenceable(&"A".repeat(129)));
        assert!(!unreferenceable("ROW_ID"));
        assert!(!unreferenceable("rowid"));
        assert!(!unreferenceable(&"A".repeat(128)));
    }

    #[test]
    fn a_full_oracle_url_parses_and_decodes_its_userinfo() {
        let u = parse_oracle_url("oracle://app%40x:p%3Aw@db.example:1522/ORCLPDB1").unwrap();
        assert_eq!(
            u,
            OracleUrl {
                user: "app@x".into(),
                password: "p:w".into(),
                host: "db.example".into(),
                port: 1522,
                service: "ORCLPDB1".into(),
            }
        );
        assert_eq!(parse_oracle_url("oracle://u:p@h/S").unwrap().port, 1521);
    }

    #[test]
    fn a_malformed_oracle_url_is_refused_with_the_expected_shape() {
        for bad in [
            "postgres://u:p@h/S",
            "oracle://h:1521/S",
            "oracle://u:p@h:1521",
            "oracle://u:p@h:1521/",
            "oracle://u:p@:1521/S",
            "oracle://u:p@h:x/S",
        ] {
            assert!(parse_oracle_url(bad).is_err(), "{bad} must be refused");
        }
    }

    #[test]
    fn tls_selects_tcps_and_refuses_a_ca_file_it_cannot_honour() {
        let parts = parse_oracle_url("oracle://u:p@h:1522/S").unwrap();
        assert_eq!(connect_string(&parts, None).unwrap(), "h:1522/S");
        let mut tls = TlsConfig {
            mode: crate::config::TlsMode::VerifyFull,
            ..Default::default()
        };
        assert_eq!(
            connect_string(&parts, Some(&tls)).unwrap(),
            "tcps://h:1522/S"
        );
        tls.ca_file = Some("/ca.pem".into());
        assert!(connect_string(&parts, Some(&tls)).is_err());
        tls.mode = crate::config::TlsMode::Disable;
        assert_eq!(connect_string(&parts, Some(&tls)).unwrap(), "h:1522/S");
    }
}
