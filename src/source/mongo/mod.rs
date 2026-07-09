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
//! ## Scope
//!
//! `mode: full` only (incremental / chunked / time-window are SQL-runner concepts
//! and are refused with an actionable error). Within full mode, `source.mongo.
//! page_size` opts into **keyset (seek) paging** on `_id` — bounded query time,
//! per-page parts, optional cross-run **resume** (`resume: true`, typed BSON
//! checkpoint), and `parallel: N` **`_id`-range fan-out** (any BSON `_id`). CDC
//! (change streams) rides the canonical `ChangeStream` seam and is added
//! separately.

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

pub(crate) mod cdc;

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
    /// A disjoint `_id` slice `[lo, hi)` for ONE parallel worker. When `Some`,
    /// every scan (keyset page or full) is bounded to `{_id: {$gte: lo, $lt:
    /// hi}}` (composed with the keyset `$gt` cursor). Bounds are `Bson` — a slice
    /// works for ANY `_id` type (ObjectId, integer, string), with `MinKey`/
    /// `MaxKey` as the outer sentinels. Set per-worker by the parallel runner;
    /// ranges tile the whole BSON key space so their union is the full collection
    /// with no overlap. `_id` is immutable ⇒ no doc migrates between ranges.
    /// `None` ⇒ the whole collection (the normal single reader).
    id_range: Option<(Bson, Bson)>,
    /// Collections already vetted by [`Self::ensure_uniform_id_type`] this run —
    /// the guard runs on the FIRST keyset page of every run, including a
    /// checkpointed resume (`after_id` present). Gating it on `after_id.is_none()`
    /// let `resume: true` bypass it forever: an `_id` of a new BSON band inserted
    /// after run 1 was silently unreachable on every later run (bug-hunt find).
    id_guard_checked: std::collections::HashSet<String>,
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
            id_range: None,
            id_guard_checked: std::collections::HashSet::new(),
        })
    }

    /// Bind this reader to one disjoint `_id` slice `[lo, hi)` (a parallel
    /// worker). Consuming builder — each worker owns its own `MongoSource`.
    pub fn with_id_range(mut self, lo: Bson, hi: Bson) -> Self {
        self.id_range = Some((lo, hi));
        self
    }

    /// Guard against silent loss on a heterogeneous-`_id` collection. Keyset
    /// paging seeks with `$gt`/`$lt`, which MongoDB **type-brackets**: a numeric
    /// cursor never matches a string `_id` even though strings sort after numbers,
    /// so a collection mixing `_id` types would page only the first bracket and
    /// silently drop the rest (verified: an int+string collection reads 50%). We
    /// compare the BSON bracket of the min and max `_id` (a single bracket is a
    /// contiguous band, so differing endpoints ⟺ a bracket jump exists) and refuse
    /// keyset/parallel, pointing at the full scan — a single ordered cursor, which
    /// DOES cross brackets. Numeric types share bracket 0, so a mixed
    /// Int32/Int64/Double `_id` still keysets fine.
    pub(crate) fn ensure_uniform_id_type(&self, collection: &str) -> Result<()> {
        let coll = self
            .session
            .client()
            .database(self.session.db())
            .collection::<Document>(collection);
        let span = self.session.block_on(async {
            let lo = coll.find_one(doc! {}).sort(doc! { "_id": 1 }).await?;
            let hi = coll.find_one(doc! {}).sort(doc! { "_id": -1 }).await?;
            Ok::<_, anyhow::Error>(lo.zip(hi))
        })?;
        if let Some((lo, hi)) = span
            && let (Some(lo_id), Some(hi_id)) = (lo.get("_id"), hi.get("_id"))
        {
            // NaN never matches $gt/$gte/$lt against a non-NaN operand: a keyset
            // page boundary landing ON the NaN row drops the whole remaining
            // collection, and every parallel range excludes it. NaN sorts FIRST
            // in the numeric band, so the min probe always sees it (the max
            // probe covers the all-NaN corner).
            for id in [lo_id, hi_id] {
                if matches!(id, Bson::Double(f) if f.is_nan()) {
                    anyhow::bail!(
                        "collection '{collection}' has a NaN `_id`: MongoDB range \
                         operators never match NaN, so keyset paging / parallel \
                         ranges would silently skip documents. Remove `page_size` \
                         and `parallel` to use a full ordered scan (a single \
                         cursor reads every document), or fix the `_id`."
                    );
                }
            }
            let (lo_b, hi_b) = (id_bracket(lo_id), id_bracket(hi_id));
            // Unknown band (None) is refused too: if we cannot place the type we
            // cannot promise the seek crosses it — loud beats a silent gap.
            if lo_b.is_none() || hi_b.is_none() || lo_b != hi_b {
                anyhow::bail!(
                    "collection '{collection}' has heterogeneous `_id` types \
                     ({:?} … {:?}): MongoDB keyset paging seeks with `$gt`, which is \
                     BSON-type-bracketed and would silently skip every `_id` type but \
                     one. Remove `page_size` and `parallel` to use a full ordered scan \
                     (a single cursor crosses BSON types), or normalise `_id` to one type.",
                    lo_id.element_type(),
                    hi_id.element_type(),
                );
            }
        }
        Ok(())
    }

    /// Split the collection's `_id` space into `n` disjoint, size-balanced
    /// ranges `[lo, hi)` whose union tiles the whole BSON key space (so the union
    /// read is the complete collection, no overlap) — for ANY `_id` type, not
    /// just ObjectId. Boundaries come from a `$sample` of the `_id`s (a
    /// WiredTiger random cursor — O(sample), NOT a collection scan, unlike
    /// `$bucketAuto` which examines every doc), then the N−1 quantiles of the
    /// sorted sample. Outer bounds are `MinKey`/`MaxKey`, which sort before/after
    /// every value, so the first/last range cannot miss the true extremes.
    pub fn sample_id_ranges(&self, collection: &str, n: usize) -> Result<Vec<(Bson, Bson)>> {
        self.ensure_uniform_id_type(collection)?;
        let n = n.max(1);
        let full_range = || vec![(Bson::MinKey, Bson::MaxKey)];
        if n == 1 {
            return Ok(full_range());
        }
        // ~250 samples per target range (min 2000), capped so a huge N stays well
        // under the 5%-of-collection threshold that flips $sample to a full sort.
        let sample_size = (n * 250).clamp(2000, 50_000);
        let coll = self
            .session
            .client()
            .database(self.session.db())
            .collection::<Document>(collection);
        let ids: Vec<Bson> = self.session.block_on(async move {
            let mut cursor = coll
                .aggregate(vec![
                    doc! { "$sample": { "size": sample_size as i64 } },
                    doc! { "$sort": { "_id": 1 } },
                    doc! { "$project": { "_id": 1 } },
                ])
                .await?;
            let mut ids = Vec::new();
            while let Some(d) = cursor.try_next().await? {
                // `$sort` yields the sampled `_id`s in BSON order; any type is fine
                // (the range filter compares by the same order the server sorts by).
                if let Some(id) = d.get("_id") {
                    ids.push(id.clone());
                }
            }
            Ok::<_, anyhow::Error>(ids)
        })?;
        if ids.len() < 2 {
            // Too few docs to split meaningfully — one range over everything.
            return Ok(full_range());
        }
        // N−1 interior quantile boundaries → N ranges; MinKey/MaxKey at the ends
        // sort before/after every value, so the outer slices cover the extremes
        // for ANY `_id` type.
        let mut bounds = vec![Bson::MinKey];
        for i in 1..n {
            bounds.push(ids[i * ids.len() / n].clone());
        }
        bounds.push(Bson::MaxKey);
        // Adjacent quantiles can land on the same value in a skewed sample; drop
        // the resulting empty `[x, x)` so a worker never gets a no-op range (the
        // dropped value is still covered by the next range's `$gte`).
        Ok(bounds
            .windows(2)
            .filter(|w| w[0] != w[1])
            .map(|w| (w[0].clone(), w[1].clone()))
            .collect())
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
            // Nullable: a CDC DELETE with no pre-image (pre/post-images not
            // enabled, or < 6.0) carries only `_id`, so `document` is NULL. A
            // batch export never writes a null here — nullable is a safe superset.
            &SourceColumn::simple("document", "document", true),
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

/// Encode a BSON `_id` as a lossless, engine-decodable keyset token: the raw
/// BSON bytes of `{_id: <value>}`, hex-encoded. Unlike the display `_id` column
/// (hex/text — type-ambiguous, e.g. integer `1001` vs string `"1001"`), this
/// preserves the exact BSON type across the string-typed cursor seam, so keyset
/// paging + resume work for ANY `_id`, not just ObjectId.
/// The BSON comparison *band* an `_id` falls in for a `$gt`/`$lt` keyset seek.
/// MongoDB only compares within a band; the server's sort order is
/// `MinKey < Null < Numbers < String/Symbol < Object < Array < BinData <
/// ObjectId < Boolean < Date < Timestamp < Regex < MaxKey`. The four numeric
/// types share one band (a mixed Int32/Int64/Double `_id` keysets fine); every
/// other band is its own bracket — a former catch-all arm lumped Null, Object,
/// Array and Regex together, so a `null` + `{…}` collection slipped past the
/// guard and silently lost a band (bug-hunt find). `None` = a type we cannot
/// place (MinKey/MaxKey/JS/DbPointer/…) — the caller refuses keyset rather than
/// guessing. Two `_id`s in different brackets ⟹ un-keyset-able.
fn id_bracket(v: &Bson) -> Option<u8> {
    Some(match v {
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => 0,
        Bson::Null => 1,
        Bson::String(_) | Bson::Symbol(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::ObjectId(_) => 6,
        Bson::Boolean(_) => 7,
        Bson::DateTime(_) => 8,
        Bson::Timestamp(_) => 9,
        Bson::RegularExpression(_) => 10,
        _ => return None,
    })
}

fn encode_id_cursor(id: &Bson) -> String {
    let mut buf = Vec::new();
    doc! { "_id": id.clone() }
        .to_writer(&mut buf)
        .expect("a one-field BSON document always serializes");
    bytes_to_hex(&buf)
}

/// Lower-case hex of raw bytes — the string half of the BSON-token round-trip
/// (paired with [`hex_to_bytes`]), used to carry a lossless BSON value across a
/// string-typed seam (the keyset cursor and the CDC resume token).
pub(super) fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Inverse of [`encode_id_cursor`]. Falls back to a bare 24-char ObjectId hex so
/// a cursor written before the typed token (or a value read from the display
/// column) still resolves.
fn decode_id_cursor(token: &str) -> Result<Bson> {
    if let Ok(bytes) = hex_to_bytes(token)
        && let Ok(doc) = Document::from_reader(&bytes[..])
        && let Some(id) = doc.get("_id")
    {
        return Ok(id.clone());
    }
    ObjectId::parse_str(token).map(Bson::ObjectId).map_err(|_| {
        anyhow::anyhow!("MongoDB keyset cursor '{token}' is neither a typed token nor ObjectId hex")
    })
}

fn hex_to_bytes(s: &str) -> Result<Vec<u8>> {
    // The 2-byte slices below index by BYTE; a multi-byte char boundary would
    // panic. A corrupted / foreign token is an error, never a panic.
    if !s.is_ascii() {
        anyhow::bail!("invalid hex cursor token (non-ASCII)");
    }
    if !s.len().is_multiple_of(2) {
        anyhow::bail!("odd-length hex cursor token");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| anyhow::anyhow!(e)))
        .collect()
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
        let id_range = self.id_range.clone();
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
        // as a lossless typed token (`set_source_cursor` → `decode_id_cursor`).
        // We page `find({_id:{$gt:after}}).sort({_id:1}).limit(n)` — a bounded,
        // index-driven range scan. `page_limit` unset ⇒ one full scan. Because
        // the token carries the exact BSON type (not a type-ambiguous hex/text
        // rendering), keyset works for ANY `_id` — ObjectId, integer, string —
        // and MongoDB's own `$gt`/`sort` provide the ordering.
        let page_limit = request.page_limit;
        let after_id: Option<Bson> =
            match request.cursor.and_then(|c| c.last_cursor_value.as_deref()) {
                Some(token) => Some(decode_id_cursor(token)?),
                None => None,
            };

        // Single-worker keyset, first page of THIS run — including a checkpointed
        // resume: refuse a heterogeneous-`_id` collection up front. `$gt` is
        // type-bracketed and would silently drop every `_id` band but the
        // cursor's; a resume whose cursor predates newly-inserted documents of a
        // different band would otherwise skip them forever while reporting
        // success (the guard must NOT be gated on `after_id.is_none()`).
        // (Parallel checks this in `sample_id_ranges`; a full scan — `page_limit`
        // unset — is exempt because its single cursor crosses brackets.)
        if page_limit.is_some() && id_range.is_none() && !self.id_guard_checked.contains(coll_name)
        {
            self.ensure_uniform_id_type(coll_name)?;
            self.id_guard_checked.insert(coll_name.to_string());
        }

        let total = self.session.block_on(async {
            // Lower bound: the keyset cursor (`$gt: after`) when resuming a page,
            // else this worker's range floor (`$gte: lo`). Upper bound: the range
            // ceiling (`$lt: hi`). A worker with no range and no cursor scans all.
            let filter = {
                let mut cond = Document::new();
                match &after_id {
                    Some(id) => {
                        cond.insert("$gt", id.clone());
                    }
                    None => {
                        if let Some((lo, _)) = &id_range {
                            cond.insert("$gte", lo.clone());
                        }
                    }
                }
                if let Some((_, hi)) = &id_range {
                    cond.insert("$lt", hi.clone());
                }
                if cond.is_empty() {
                    doc! {}
                } else {
                    doc! { "_id": cond }
                }
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
            // The page's max `_id` (the last row, since keyset sorts `_id` asc).
            // Reported to the sink as a typed token so the runner advances by the
            // exact BSON value, not the type-ambiguous display string.
            let mut max_id: Option<Bson> = None;
            while let Some(d) = cursor.try_next().await? {
                let id = id_to_string(d.get("_id"));
                let json = document_to_json(&d, canonical)?;
                batch_bytes += id.len() + json.len();
                ids.append_value(&id);
                docs.append_value(&json);
                if page_limit.is_some() {
                    max_id = d.get("_id").cloned();
                }
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
            // Keyset only: hand the runner the exact BSON high-water mark.
            if let Some(id) = &max_id {
                sink.set_source_cursor(encode_id_cursor(id));
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
    fn roast_non_ascii_cursor_token_is_a_clean_error_not_a_panic() {
        // A corrupted / foreign checkpoint token must surface as Err — never a
        // byte-boundary panic. '€' is 3 bytes: "€€" is 6 bytes (even, so it
        // passes the odd-length check) and the 2-byte hex slice at [0..2] cuts
        // the char in half — which panicked before hex_to_bytes rejected
        // non-ASCII input up front.
        assert!(decode_id_cursor("€€").is_err());
    }

    #[test]
    fn id_cursor_token_roundtrips_every_bson_type_losslessly() {
        // The whole point of the typed token (A′): the display column can't tell
        // integer `1001` from string `"1001"`, but the cursor MUST — otherwise a
        // keyset `$gt` on the wrong BSON type mis-pages. Prove the round-trip
        // preserves the exact type for each `_id` shape keyset supports.
        let oid = Bson::ObjectId(ObjectId::parse_str("64b8f0000000000000000000").unwrap());
        for id in [
            oid.clone(),
            Bson::Int32(1001),
            Bson::Int64(9_000_000_000),
            Bson::String("1001".to_string()), // same text as Int32(1001) — must NOT collide
            Bson::String("sku-00042".to_string()),
        ] {
            let token = encode_id_cursor(&id);
            assert_eq!(
                decode_id_cursor(&token).unwrap(),
                id,
                "typed cursor round-trip lost the type for {id:?}"
            );
        }
        // Int32(1001) and String("1001") encode to DIFFERENT tokens (no ambiguity).
        assert_ne!(
            encode_id_cursor(&Bson::Int32(1001)),
            encode_id_cursor(&Bson::String("1001".to_string()))
        );
        // Legacy / display fallback: a bare 24-char ObjectId hex still decodes.
        assert_eq!(decode_id_cursor("64b8f0000000000000000000").unwrap(), oid);
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
