//! **Layer: Execution** — MongoDB source engine (document store).
//!
//! Unlike the three SQL engines (PostgreSQL / MySQL / SQL Server), MongoDB has
//! no SQL, no fixed per-collection schema, and no `information_schema`. So the
//! SQL-shaped read seam — chunked/keyset planning, incremental predicate
//! building, catalog introspection — does **not** apply. This adapter is the
//! **OSS JSON-blob model**: every document exports as exactly two columns —
//! `_id` (`Utf8`, the document key stringified: ObjectId → hex, etc.) and
//! `document` (`Utf8` + the `arrow.json` extension type, the whole BSON
//! document rendered as relaxed extended JSON).
//!
//! Per-field typing is punted downstream (`PARSE_JSON` in the warehouse). Schema
//! inference / auto-discovery is deliberately **out of scope for OSS** (it is the
//! paid-tier `discover` mechanism); the user gets a lossless blob, not a guessed
//! columnar schema. A future `columns:` projection can carve typed columns out of
//! the blob without changing this default.
//!
//! ## Sync bridge (ADR-0011)
//!
//! The `mongodb` driver is async (tokio); the [`Source`] trait is sync
//! `&mut self`. Like the MSSQL/tiberius engine, each [`MongoSource`] owns a tokio
//! runtime and `block_on`s every driver call — no async leaks into the runner.
//!
//! ## Scope of this first slice
//!
//! `mode: full` only. Incremental / chunked / keyset are SQL-runner concepts and
//! are refused here with an actionable error. CDC (change streams) rides the
//! canonical `ChangeStream` seam and is added separately.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, options::ClientOptions};
use tokio::runtime::Runtime;

use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::{BatchSink, ExportRequest, Source};
use crate::types::{ColumnOverrides, RivetType, SourceColumn, TypeMapping};

/// Rows accumulated into one Arrow batch before it is handed to the sink. The
/// sink owns file rollover / memory budgeting; the source only needs to emit
/// reasonably-sized batches. TODO: honour `tuning.effective_batch_size`.
const BATCH_ROWS: usize = 10_000;

/// MongoDB source. Owns the async driver client + the runtime that drives it.
pub struct MongoSource {
    rt: Runtime,
    client: Client,
    /// Default database resolved from the connection URL path
    /// (`mongodb://host:27017/<db>`).
    database: String,
}

impl MongoSource {
    /// Connect to MongoDB, honouring the shared TLS gate. `url` is the resolved
    /// `mongodb://user:pass@host:port/db` (or `mongodb+srv://…`) form. A
    /// successful return has completed a `ping` round-trip.
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        // Refuse plaintext to a remote (non-loopback) host with no `tls:` block
        // (CWE-319), exactly as the SQL engines do. A `mongodb://` URL can also
        // carry `?tls=true`; full mapping of every `TlsConfig` knob onto the
        // driver's `TlsOptions` is a follow-up — today the gate is the guard and
        // loopback/dev connects in plaintext.
        crate::source::require_tls_or_loopback(url, tls)?;

        // A small multi-thread runtime (not current-thread): the mongodb driver
        // spawns background SDAM/heartbeat tasks that must make progress
        // independently of our `block_on` calls, or connection monitoring
        // starves. Two workers is plenty for one source connection.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;

        let (client, database) = rt.block_on(async {
            let opts = ClientOptions::parse(url).await?;
            let database = opts.default_database.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "mongodb url must include a database: mongodb://user:pass@host:port/<db>"
                )
            })?;
            let client = Client::with_options(opts)?;
            // Round-trip so a bad host/auth fails at connect, not first read.
            client
                .database(&database)
                .run_command(doc! { "ping": 1 })
                .await?;
            Ok::<_, anyhow::Error>((client, database))
        })?;

        Ok(Self {
            rt,
            client,
            database,
        })
    }
}

/// The two fixed columns of the JSON-blob model. Used by both the export schema
/// and `type_mappings`, so `check --type-report` and the export agree.
fn blob_mappings() -> Vec<TypeMapping> {
    vec![
        TypeMapping::from_source(
            &SourceColumn::simple("_id", "objectid", false),
            RivetType::String,
        ),
        TypeMapping::from_source(
            &SourceColumn::simple("document", "document", false),
            RivetType::Json,
        ),
    ]
}

/// Arrow schema for the JSON-blob model (`_id: Utf8`, `document: Utf8 + json`).
fn blob_schema() -> SchemaRef {
    let fields = blob_mappings()
        .iter()
        .map(|m| {
            crate::types::build_arrow_field(m)
                .expect("blob columns (String/Json) always have an Arrow mapping")
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// Extract the collection name from the query rivet's `table:` shortcut
/// produces (`SELECT * FROM <collection>`). Any other shape — a SQL `query:`, or
/// an incremental/chunked/keyset wrapper (`SELECT * FROM (<base>) …`) — is
/// refused with an actionable message, because MongoDB has no SQL and this
/// adapter only supports `mode: full`.
fn collection_from_query(query: &str) -> Result<&str> {
    const PREFIX: &str = "select * from ";
    let q = query.trim();
    let matches_prefix = q.len() >= PREFIX.len() && q[..PREFIX.len()].eq_ignore_ascii_case(PREFIX);
    if !matches_prefix {
        anyhow::bail!(
            "MongoDB source supports only `table: <collection>` with `mode: full`; the SQL \
             `query:` form is not available (MongoDB has no SQL). Got: {query}"
        );
    }
    let coll = q[PREFIX.len()..].trim();
    // A plain collection name is a single bare identifier; the charset check
    // rejects anything else (parens, whitespace, trailing SQL clauses), so an
    // incremental/chunked/keyset wrapper can never pass as a collection.
    if coll.is_empty()
        || !coll
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        anyhow::bail!(
            "MongoDB source supports only `mode: full` on a plain `table: <collection>`; \
             incremental / chunked / keyset are not available for MongoDB. Got: {query}"
        );
    }
    Ok(coll)
}

/// Stringify a document `_id` for the `_id` column. ObjectId → 24-char hex;
/// string/number keys → their natural text; anything exotic (binary, compound)
/// → its relaxed extended JSON, so the column is always populated and never
/// silently null.
fn id_to_string(id: Option<&Bson>) -> String {
    match id {
        Some(Bson::ObjectId(oid)) => oid.to_hex(),
        Some(Bson::String(s)) => s.clone(),
        // Ints, bools, etc. render as their bare relaxed-extjson value (`42`,
        // `true`); ObjectId/String are special-cased above only to drop the
        // `{"$oid":…}` wrapper / surrounding quotes.
        Some(other) => other.clone().into_relaxed_extjson().to_string(),
        // A document with no `_id` is not possible in MongoDB (the server
        // always assigns one), but never panic on malformed input.
        None => String::new(),
    }
}

/// Render a whole BSON document as a relaxed-extended-JSON string. Relaxed (not
/// canonical) keeps common scalars as native JSON (`42`, `"x"`, `true`, nested
/// objects/arrays) while still wrapping exotic BSON types losslessly
/// (`{"$oid":…}`, `{"$date":…}`, `{"$numberDecimal":…}`) — the friendliest shape
/// for a downstream `PARSE_JSON`.
fn document_to_json(doc: &Document) -> Result<String> {
    let value = Bson::Document(doc.clone()).into_relaxed_extjson();
    Ok(serde_json::to_string(&value)?)
}

/// Finish the accumulated builders into a `RecordBatch` and hand it to the sink.
fn flush(
    schema: &SchemaRef,
    ids: &mut StringBuilder,
    docs: &mut StringBuilder,
    sink: &mut dyn BatchSink,
) -> Result<()> {
    let columns: Vec<ArrayRef> = vec![Arc::new(ids.finish()), Arc::new(docs.finish())];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    sink.on_batch(&batch)?;
    Ok(())
}

impl Source for MongoSource {
    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()> {
        let coll_name = collection_from_query(request.query)?;
        let schema = blob_schema();
        sink.on_schema(schema.clone())?;

        let coll = self
            .client
            .database(&self.database)
            .collection::<Document>(coll_name);

        let total = self.rt.block_on(async {
            let mut cursor = coll.find(doc! {}).await?;
            let mut ids = StringBuilder::new();
            let mut docs = StringBuilder::new();
            let mut in_batch = 0usize;
            let mut total = 0usize;
            while let Some(d) = cursor.try_next().await? {
                ids.append_value(id_to_string(d.get("_id")));
                docs.append_value(document_to_json(&d)?);
                in_batch += 1;
                total += 1;
                if in_batch >= BATCH_ROWS {
                    flush(&schema, &mut ids, &mut docs, sink)?;
                    in_batch = 0;
                }
            }
            if in_batch > 0 {
                flush(&schema, &mut ids, &mut docs, sink)?;
            }
            Ok::<usize, anyhow::Error>(total)
        })?;

        log::info!("total: {total} documents (collection '{coll_name}')");
        Ok(())
    }

    /// The JSON-blob model has a fixed two-column schema regardless of the
    /// query, so `check --type-report` shows it without touching the server.
    fn type_mappings(
        &mut self,
        _query: &str,
        _column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        Ok(blob_mappings())
    }

    /// The only scalar rivet asks a `mode: full` source is the reconcile row
    /// count — `SELECT COUNT(*) FROM (SELECT * FROM <coll>) AS _rivet_reconcile`.
    /// Recognize that shape and answer it with `countDocuments` so
    /// `rivet run --reconcile` verifies Mongo exports like the SQL engines. Any
    /// other SQL scalar (cursor MIN/MAX, etc.) has no Mongo meaning → `None`,
    /// which callers treat as "unavailable" and skip.
    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        if !sql.to_ascii_lowercase().contains("count(") {
            return Ok(None);
        }
        let Some(coll_name) = last_from_identifier(sql) else {
            return Ok(None);
        };
        let coll = self
            .client
            .database(&self.database)
            .collection::<Document>(coll_name);
        let n = self
            .rt
            .block_on(async move { coll.count_documents(doc! {}).await })?;
        Ok(Some(n.to_string()))
    }
}

/// Pull the collection name out of the innermost `… FROM <ident>` of a SQL
/// string — the reconcile count wraps the base as
/// `SELECT COUNT(*) FROM (SELECT * FROM <coll>) AS _rivet_reconcile`, so the
/// *last* `FROM` names the collection. `None` if no bare identifier follows.
/// (This is the same "un-parse SQL back to intent" tax the batch path pays in
/// `collection_from_query` — a symptom of the SQL-shaped `Source` seam, tracked
/// as the architecture review's candidate A.)
fn last_from_identifier(sql: &str) -> Option<&str> {
    const MARKER: &str = "from ";
    let start = sql.to_ascii_lowercase().rfind(MARKER)? + MARKER.len();
    let ident = sql[start..]
        .trim_start()
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
        .next()?;
    (!ident.is_empty()).then_some(ident)
}

/// Read a possibly-nested numeric field out of a `serverStatus` document,
/// coercing Int32 / Int64 / Double to `i64`. `None` if any path segment is
/// missing or the leaf is not numeric — so a storage engine that omits a
/// section (e.g. in-memory has no `wiredTiger`) just drops that one metric.
fn nested_i64(doc: &Document, path: &[&str]) -> Option<i64> {
    let (leaf, parents) = path.split_last()?;
    let mut cur = doc;
    for key in parents {
        cur = cur.get_document(key).ok()?;
    }
    match cur.get(leaf) {
        Some(Bson::Int64(v)) => Some(*v),
        Some(Bson::Int32(v)) => Some(*v as i64),
        Some(Bson::Double(v)) => Some(*v as i64),
        _ => None,
    }
}

/// Pull the source-harm counters out of a `serverStatus` response. Only
/// **cumulative monotonic** counters are emitted (never gauges), because the
/// pipeline stores the before→after *delta* — the same contract as the SQL
/// engines' `sample_harm_counters`.
fn harm_from_server_status(status: &Document) -> Vec<(String, i64)> {
    // (metric label, serverStatus path). Chosen as the Mongo analogues of the
    // PG harm set: docs/keys *scanned* are the read-amplification signal
    // (≈ `pg_tup_returned`), docs *returned* ≈ `pg_tup_fetched`, `getmore` is
    // rivet's own streaming footprint (every cursor batch), and WiredTiger
    // cache bytes-read is the I/O-vs-cache split (≈ `pg_blks_read`).
    let probes: [(&str, &[&str]); 6] = [
        (
            "mongo_docs_scanned",
            &["metrics", "queryExecutor", "scannedObjects"],
        ),
        (
            "mongo_keys_scanned",
            &["metrics", "queryExecutor", "scanned"],
        ),
        ("mongo_docs_returned", &["metrics", "document", "returned"]),
        ("mongo_op_query", &["opcounters", "query"]),
        ("mongo_op_getmore", &["opcounters", "getmore"]),
        (
            "mongo_wt_cache_bytes_read",
            &["wiredTiger", "cache", "bytes read into cache"],
        ),
    ];
    probes
        .iter()
        .filter_map(|(name, path)| nested_i64(status, path).map(|v| ((*name).to_string(), v)))
        .collect()
}

/// Snapshot MongoDB's source-harm counters via `serverStatus` — the document-
/// store analogue of the SQL engines' `sample_harm_counters`. Returns
/// `(metric, cumulative_value)` pairs; the pipeline captures these before and
/// after the export and stores the per-metric delta in `export_harm`.
///
/// These are **server-wide** cumulative counters, so concurrent activity
/// inflates the delta; on a single-tenant pilot box it is the run's own read
/// footprint. `serverStatus` needs the `clusterMonitor` role (or the
/// `serverStatus` privilege) on authenticated deployments — `None` on any
/// connect / permission / query failure, exactly like the SQL engines:
/// harm metrics are observability, never a gate.
pub(crate) fn sample_harm_counters(
    url: &str,
    tls: Option<&TlsConfig>,
) -> Option<Vec<(String, i64)>> {
    let (rt, client, db) = connect_blocking(url, tls)?;
    rt.block_on(async move {
        let status = client
            .database(&db)
            .run_command(doc! { "serverStatus": 1 })
            .await
            .ok()?;
        Some(harm_from_server_status(&status))
    })
}

/// Scan-free document-count estimate for one collection — the preflight
/// `row_estimate` (the Mongo analogue of PG `reltuples` / MySQL `TABLE_ROWS`).
/// `estimatedDocumentCount` reads collection metadata, never scans the
/// collection. `None` on any failure — the diagnostic then shows an unknown
/// row count, exactly as MySQL does when its estimate is untrustworthy.
pub(crate) fn estimated_count(url: &str, tls: Option<&TlsConfig>, collection: &str) -> Option<i64> {
    let (rt, client, db) = connect_blocking(url, tls)?;
    rt.block_on(async move {
        let n = client
            .database(&db)
            .collection::<Document>(collection)
            .estimated_document_count()
            .await
            .ok()?;
        i64::try_from(n).ok()
    })
}

/// Build a runtime + connected client + resolved database for the standalone
/// (non-`MongoSource`) entry points — the harm sampler and the preflight count
/// estimate. `None` on any connect failure (both callers are best-effort).
/// Defaults the database to `admin` when the URL omits one; the harm sampler's
/// `serverStatus` is cluster-scoped, and a Mongo source URL always carries a
/// database, so the fallback only affects the never-scans harm path.
fn connect_blocking(url: &str, tls: Option<&TlsConfig>) -> Option<(Runtime, Client, String)> {
    crate::source::require_tls_or_loopback(url, tls).ok()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .ok()?;
    let (client, db) = rt.block_on(async {
        let opts = ClientOptions::parse(url).await.ok()?;
        let db = opts
            .default_database
            .clone()
            .unwrap_or_else(|| "admin".to_string());
        let client = Client::with_options(opts).ok()?;
        Some((client, db))
    })?;
    Some((rt, client, db))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;

    #[test]
    fn collection_parsed_from_table_shortcut_query() {
        assert_eq!(
            collection_from_query("SELECT * FROM orders").unwrap(),
            "orders"
        );
        // Case-insensitive: rivet lowercases nothing here, but be robust.
        assert_eq!(
            collection_from_query("select * from Users").unwrap(),
            "Users"
        );
        assert_eq!(
            collection_from_query("  SELECT * FROM events  ").unwrap(),
            "events"
        );
    }

    #[test]
    fn collection_rejects_non_full_and_raw_sql_shapes() {
        // Incremental / chunked / keyset wrap the base in a subquery — must be
        // refused, never mistaken for a collection named "(".
        assert!(
            collection_from_query("SELECT * FROM (SELECT * FROM orders) AS _rivet WHERE id > 5")
                .is_err()
        );
        // A hand-written SQL query has no MongoDB meaning.
        assert!(collection_from_query("SELECT id, name FROM orders WHERE id > 10").is_err());
        // A `WHERE` on the table shortcut is still a query, not a bare collection.
        assert!(collection_from_query("SELECT * FROM orders WHERE x = 1").is_err());
        assert!(collection_from_query("SELECT * FROM ").is_err());
    }

    #[test]
    fn id_string_covers_common_key_types() {
        let oid = ObjectId::parse_str("64b8f0000000000000000000").unwrap();
        assert_eq!(
            id_to_string(Some(&Bson::ObjectId(oid))),
            "64b8f0000000000000000000"
        );
        assert_eq!(id_to_string(Some(&Bson::String("abc".to_string()))), "abc");
        assert_eq!(id_to_string(Some(&Bson::Int64(42))), "42");
        assert_eq!(id_to_string(Some(&Bson::Int32(7))), "7");
        // Never panic / never silently vanish on a missing id.
        assert_eq!(id_to_string(None), "");
    }

    #[test]
    fn document_renders_as_relaxed_extended_json() {
        let d = doc! {
            "a": 1i32,
            "b": "x",
            "nested": doc! { "d": true },
        };
        let json = document_to_json(&d).unwrap();
        // Relaxed extjson keeps common scalars native (not `{"$numberInt":…}`).
        assert!(json.contains("\"a\":1"), "got: {json}");
        assert!(json.contains("\"b\":\"x\""), "got: {json}");
        assert!(json.contains("\"nested\":{\"d\":true}"), "got: {json}");
    }

    #[test]
    fn blob_schema_is_two_columns_id_and_document_json() {
        let schema = blob_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["_id", "document"]);
        // The document column carries the Arrow JSON canonical extension type so
        // parquet emits a native JSON column downstream, not a bare string.
        let doc_field = schema.field_with_name("document").unwrap();
        assert_eq!(
            doc_field.metadata().get("ARROW:extension:name"),
            Some(&"arrow.json".to_string()),
            "document column must carry the arrow.json extension type"
        );
    }

    #[test]
    fn harm_counters_extracted_from_server_status() {
        let status = doc! {
            "metrics": {
                "queryExecutor": { "scanned": 10i64, "scannedObjects": 500i64 },
                "document": { "returned": 480i64 },
            },
            "opcounters": { "query": 7i32, "getmore": 12i32 },
            "wiredTiger": { "cache": { "bytes read into cache": 1_048_576i64 } },
        };
        let harm: std::collections::HashMap<String, i64> =
            harm_from_server_status(&status).into_iter().collect();
        assert_eq!(harm.get("mongo_docs_scanned"), Some(&500));
        assert_eq!(harm.get("mongo_keys_scanned"), Some(&10));
        assert_eq!(harm.get("mongo_docs_returned"), Some(&480));
        assert_eq!(harm.get("mongo_op_query"), Some(&7)); // Int32 coerced to i64
        assert_eq!(harm.get("mongo_op_getmore"), Some(&12));
        assert_eq!(harm.get("mongo_wt_cache_bytes_read"), Some(&1_048_576));
    }

    #[test]
    fn reconcile_count_query_names_the_collection() {
        // The reconcile count wraps the base query; the LAST `FROM` names the
        // collection countDocuments runs against.
        assert_eq!(
            last_from_identifier("SELECT COUNT(*) FROM (SELECT * FROM orders) AS _rivet_reconcile"),
            Some("orders")
        );
        assert_eq!(
            last_from_identifier("select count(*) from (select * from shop_events) as _r"),
            Some("shop_events")
        );
        // No bare identifier after a FROM → no count.
        assert_eq!(last_from_identifier("SELECT 1"), None);
    }

    #[test]
    fn harm_skips_missing_sections_gracefully() {
        // e.g. the in-memory storage engine has no `wiredTiger` section — a
        // missing section drops only its own metric, never errors.
        let status = doc! { "opcounters": { "query": 3i64 } };
        let harm: std::collections::HashMap<String, i64> =
            harm_from_server_status(&status).into_iter().collect();
        assert_eq!(harm.get("mongo_op_query"), Some(&3));
        assert!(!harm.contains_key("mongo_wt_cache_bytes_read"));
        assert!(!harm.contains_key("mongo_docs_scanned"));
    }
}
