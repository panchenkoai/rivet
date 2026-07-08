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
use mongodb::Client;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{ClientOptions, CollectionOptions, ReadConcern};
use tokio::runtime::Runtime;

use crate::config::{MongoConfig, MongoJsonMode, MongoReadConcern, TlsConfig};
use crate::error::Result;
use crate::source::{BatchSink, ExportRequest, Source};
use crate::types::{ColumnOverrides, RivetType, SourceColumn, TypeMapping};

/// A connected MongoDB session: the async client plus the tokio runtime that
/// drives it, so the sync `Source` trait can `block_on` every driver call
/// (ADR-0011, mirrors MSSQL/tiberius). This is the **one** place that owns the
/// async→sync bridge — the SDAM-needs-a-worker-thread runtime, the connect +
/// ping handshake, the borrow dance — reused by the source, the harm / count
/// probes, and `rivet init`.
pub struct MongoSession {
    rt: Runtime,
    client: Client,
    /// Database resolved from the connection URL path (`mongodb://…/<db>`).
    db: String,
}

impl MongoSession {
    /// Connect + `ping`. `gate` applies the shared remote-plaintext TLS refusal
    /// (`require_tls_or_loopback`, CWE-319); `rivet init` passes `false` (dev
    /// convenience, like the SQL init helpers), every other caller `true`.
    pub fn connect(url: &str, tls: Option<&TlsConfig>, gate: bool) -> Result<Self> {
        if gate {
            crate::source::require_tls_or_loopback(url, tls)?;
        }
        // Small multi-thread runtime (not current-thread): the driver spawns
        // background SDAM/heartbeat tasks that must make progress independently
        // of our `block_on` calls, or connection monitoring starves.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let (client, db) = rt.block_on(async {
            let opts = ClientOptions::parse(url).await?;
            let db = opts.default_database.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "mongodb url must include a database: mongodb://user:pass@host:port/<db>"
                )
            })?;
            let client = Client::with_options(opts)?;
            // Round-trip so a bad host/auth fails at connect, not first read.
            client.database(&db).run_command(doc! { "ping": 1 }).await?;
            Ok::<_, anyhow::Error>((client, db))
        })?;
        Ok(Self { rt, client, db })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
    pub fn db(&self) -> &str {
        &self.db
    }
    /// Drive a future to completion on the owned runtime.
    pub fn block_on<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        self.rt.block_on(fut)
    }
}

/// MongoDB source over a [`MongoSession`], carrying the resolved `source.mongo:`
/// read options `export` applies.
pub struct MongoSource {
    session: MongoSession,
    /// Render the `document` column as canonical (type-tagged) extended JSON.
    canonical_json: bool,
    /// Scan under `readConcern: snapshot` (point-in-time; 5.0+ replica set).
    snapshot: bool,
    /// Set `noCursorTimeout` on the scan cursor.
    no_cursor_timeout: bool,
}

impl MongoSource {
    /// Connect, resolving the optional `source.mongo:` read options (absent ⇒
    /// relaxed JSON, server read concern, cursor kept alive). Used by
    /// `create_source` (with the config block) and the `doctor` / type-report
    /// probes (with `None`).
    pub fn connect(url: &str, tls: Option<&TlsConfig>, cfg: Option<&MongoConfig>) -> Result<Self> {
        let session = MongoSession::connect(url, tls, true)?;
        let (canonical_json, snapshot, no_cursor_timeout) = match cfg {
            None => (false, false, true),
            Some(c) => (
                matches!(c.json, MongoJsonMode::Canonical),
                matches!(c.read_concern, MongoReadConcern::Snapshot),
                c.no_cursor_timeout,
            ),
        };
        Ok(Self {
            session,
            canonical_json,
            snapshot,
            no_cursor_timeout,
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

/// Render a whole BSON document as an extended-JSON string. `canonical=false`
/// (relaxed) keeps common scalars native (`42`, `"x"`, `true`, nested
/// objects/arrays) while still wrapping exotic BSON types losslessly
/// (`{"$oid":…}`, `{"$date":…}`, `{"$numberDecimal":…}`) — friendliest for a
/// downstream `PARSE_JSON`. `canonical=true` type-tags every value
/// (`{"$numberLong":…}`) so Int64/Double survive a double-based JSON-number
/// parser that would otherwise clamp values beyond 2^53.
fn document_to_json(doc: &Document, canonical: bool) -> Result<String> {
    let bson = Bson::Document(doc.clone());
    let value = if canonical {
        bson.into_canonical_extjson()
    } else {
        bson.into_relaxed_extjson()
    };
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
        // The structured read-intent: the bare collection behind the `table:`
        // shortcut (ADR-0027). `None` ⇒ a hand-written `query:` or a
        // filtered/wrapped form, which has no MongoDB equivalent.
        let coll_name = request.base_relation.ok_or_else(|| {
            anyhow::anyhow!(
                "MongoDB source supports only `table: <collection>` with `mode: full` — a \
                 hand-written `query:` or a filtered/wrapped form has no MongoDB equivalent. \
                 Got query: {}",
                request.query
            )
        })?;
        let schema = blob_schema();
        sink.on_schema(schema.clone())?;

        // `readConcern: snapshot` must ride the collection handle; a plain scan
        // uses the default handle. The scalar opts are copied into locals so the
        // async scan closure doesn't borrow `self`.
        let db = self.session.client().database(self.session.db());
        let coll = if self.snapshot {
            db.collection_with_options::<Document>(
                coll_name,
                CollectionOptions::builder()
                    .read_concern(ReadConcern::snapshot())
                    .build(),
            )
        } else {
            db.collection::<Document>(coll_name)
        };
        let canonical = self.canonical_json;
        let no_cursor_timeout = self.no_cursor_timeout;
        // Source-side batch = the RSS lever. Two caps, whichever trips first:
        //   • `tuning.batch_size` — a row count (schema-based memory budget for a
        //     document store is unreliable: the `document` column is variable
        //     length, so a row estimate off the Arrow schema misfires);
        //   • `tuning.max_batch_memory_mb` — a hard byte budget on the batch,
        //     the *correct* RSS knob for a JSON blob (flush by accumulated bytes,
        //     independent of how big each document is).
        let batch_rows = request.tuning.effective_batch_size(Some(&schema)).max(1);
        let batch_byte_cap = request
            .tuning
            .max_batch_memory_mb
            .map(|mb| mb.saturating_mul(1024 * 1024));

        // Keyset (seek) pagination: the keyset runner drives the outer loop and
        // hands us one page's `page_limit` plus the previous page's max `_id`
        // (as its hex string). We page `find({_id:{$gt:after}}).sort({_id:1})
        // .limit(n)` — a bounded, index-driven range scan. `page_limit` unset ⇒
        // one full scan. Keyset needs an ObjectId `_id` (its hex sorts in the
        // same order as the binary key, so the round-trip through the string
        // column preserves order); a non-ObjectId key errors, actionably.
        let page_limit = request.page_limit;
        let after_id = match request.cursor.and_then(|c| c.last_cursor_value.as_deref()) {
            Some(hex) => Some(ObjectId::parse_str(hex).map_err(|_| {
                anyhow::anyhow!(
                    "MongoDB keyset paging requires an ObjectId `_id`; got the non-ObjectId key \
                     '{hex}'. Remove `source.mongo.page_size` to use a single-cursor full scan."
                )
            })?),
            None => None,
        };

        let total = self.session.block_on(async {
            let filter = match &after_id {
                Some(oid) => doc! { "_id": { "$gt": oid } },
                None => doc! {},
            };
            let mut find = coll.find(filter);
            if let Some(page) = page_limit {
                find = find.sort(doc! { "_id": 1 }).limit(page as i64);
            }
            if no_cursor_timeout {
                find = find.no_cursor_timeout(true);
            }
            let mut cursor = find.await?;
            let mut ids = StringBuilder::new();
            let mut docs = StringBuilder::new();
            let mut in_batch = 0usize;
            let mut batch_bytes = 0usize;
            let mut total = 0usize;
            while let Some(d) = cursor.try_next().await? {
                let id = id_to_string(d.get("_id"));
                let json = document_to_json(&d, canonical)?;
                batch_bytes += id.len() + json.len();
                ids.append_value(&id);
                docs.append_value(&json);
                in_batch += 1;
                total += 1;
                if in_batch >= batch_rows || batch_byte_cap.is_some_and(|cap| batch_bytes >= cap) {
                    flush(&schema, &mut ids, &mut docs, sink)?;
                    in_batch = 0;
                    batch_bytes = 0;
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
            .session
            .client()
            .database(self.session.db())
            .collection::<Document>(coll_name);
        let n = self
            .session
            .block_on(async move { coll.count_documents(doc! {}).await })?;
        Ok(Some(n.to_string()))
    }
}

/// Pull the collection name out of the innermost `… FROM <ident>` of a SQL
/// string — the reconcile count wraps the base as
/// `SELECT COUNT(*) FROM (SELECT * FROM <coll>) AS _rivet_reconcile`, so the
/// *last* `FROM` names the collection. `None` if no bare identifier follows.
///
/// The batch export path no longer un-parses SQL — it reads the structured
/// `ExportRequest::base_relation` (ADR-0027). This un-parser survives only
/// because `query_scalar` receives a bare SQL string with no structured
/// counterpart yet; giving the reconcile count a typed request would delete it
/// too (the remaining tail of ADR-0027).
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
    let session = MongoSession::connect(url, tls, true).ok()?;
    session.block_on(async {
        let status = session
            .client()
            .database(session.db())
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
    let session = MongoSession::connect(url, tls, true).ok()?;
    session.block_on(async {
        let n = session
            .client()
            .database(session.db())
            .collection::<Document>(collection)
            .estimated_document_count()
            .await
            .ok()?;
        i64::try_from(n).ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;

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
    fn document_renders_relaxed_and_canonical() {
        let d = doc! {
            "a": 1i32,
            "b": "x",
            "big": 9_223_372_036_854_775_807i64,
            "nested": doc! { "d": true },
        };
        // Relaxed: common scalars native, Int64 as a bare JSON number.
        let relaxed = document_to_json(&d, false).unwrap();
        assert!(relaxed.contains("\"a\":1"), "got: {relaxed}");
        assert!(relaxed.contains("\"b\":\"x\""), "got: {relaxed}");
        assert!(
            relaxed.contains("\"nested\":{\"d\":true}"),
            "got: {relaxed}"
        );
        assert!(
            relaxed.contains("\"big\":9223372036854775807"),
            "int64 is a bare number in relaxed: {relaxed}"
        );
        // Canonical: every number type-tagged so a double-based JSON parser can't
        // clamp an Int64 beyond 2^53.
        let canonical = document_to_json(&d, true).unwrap();
        assert!(
            canonical.contains("\"$numberLong\":\"9223372036854775807\""),
            "int64 is type-tagged in canonical: {canonical}"
        );
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
