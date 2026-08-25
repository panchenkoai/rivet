//! **Layer: Execution** — MongoDB CDC via change streams.
//!
//! Mongo's change stream is the log seam: `db.watch()` yields committed row
//! changes in oplog order, each carrying an opaque **resume token** that pins
//! the exact re-open position (the [`Position`] for this engine). Unlike the
//! SQL engines there is no per-table capture setup — the whole database is
//! watched — and the resume anchor is client-side (like MySQL's binlog
//! coordinates): a first open with no token starts at "now", so a run that
//! wants to survive a quiet period must persist the token at open.
//!
//! The row image is the same **JSON-blob model** as the batch source: two
//! columns, `_id` and `document`. `full_document` (UpdateLookup) is the post
//! image for insert/update/replace; a delete carries `document_key` (`_id`) and,
//! on MongoDB 6.0+ with `changeStreamPreAndPostImages`, the pre-image.
//!
//! Async→sync bridge: like the batch source, one [`MongoSession`] owns the tokio
//! runtime and `block_on`s the async change stream (ADR-0011). The stream is
//! tailable — `next_change` blocks until a change arrives, matching the MySQL
//! binlog adapter's continuous model.

use futures_util::StreamExt;
use mongodb::bson::{Document, doc};
use mongodb::change_stream::ChangeStream as DriverStream;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::options::{FullDocumentBeforeChangeType, FullDocumentType};

use super::{MongoSession, document_to_json, id_to_string};
use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position};

/// The two fixed image columns, matching the batch JSON-blob schema. Built once
/// (a change stream emits millions of events) and cloned per event — a refcount
/// bump, not a fresh Vec+Arc of the same two constant strings each time.
static IMAGE_NAMES: std::sync::LazyLock<std::sync::Arc<[String]>> =
    std::sync::LazyLock::new(|| {
        std::sync::Arc::from(vec!["_id".to_string(), "document".to_string()])
    });

pub(crate) struct MongoChangeStream {
    session: MongoSession,
    stream: DriverStream<ChangeStreamEvent<Document>>,
    /// Render `document` as canonical (type-tagged) extended JSON when set.
    canonical: bool,
    /// The watched database — the `schema` field of every emitted change.
    db_name: String,
    /// Bounded "catch up to the current oplog end and exit" run (the scheduler
    /// model). A tailable change stream never ends on its own, so `next_change`
    /// polls with [`ChangeStream::next_if_any`] and stops once the stream's
    /// position advances PAST `target_data` — matching the poll-based PG / SQL
    /// Server drain. `false` ⇒ block for the next change (a continuous daemon,
    /// the MySQL binlog model).
    until_current: bool,
    /// The stream's resume-token `_data` at open — the "current end" a bounded run
    /// drains up to. A single empty poll can precede the backlog's getMore (the
    /// server returns an empty first batch, seen intermittently and worst on 4.4),
    /// and that empty poll does NOT advance the position past this target — so
    /// `next_change` keeps polling instead of prematurely declaring "caught up"
    /// and dropping the backlog. `None` unless `until_current`.
    target_data: Option<String>,
    /// Cluster time at open — the UPPER time bound for a bounded run. An event
    /// whose `cluster_time` is past this arrived AFTER we opened, so the bounded
    /// run stops there. Without it, SUSTAINED concurrent writes keep
    /// `next_if_any` returning events, the empty-poll (`Ok(None)`) target check
    /// never fires, and the run never terminates (bug-hunt hang). `None` unless
    /// `until_current`, or when the server did not report `operationTime`.
    until_current_ts: Option<mongodb::bson::Timestamp>,
    /// Heterogeneous-`_id` warn state, mirroring the batch full-scan warning
    /// ([`super::MongoSource::warn_if_heterogeneous_id`]): a whole-db change stream
    /// can carry mixed `_id` BSON types across events, and the flat `_id` column
    /// then collides downstream just like a batch full scan. `hetero_first` is the
    /// first `_id` bracket seen; the warning fires once when a second appears.
    hetero_first: Option<Option<u8>>,
    hetero_warned: bool,
}

/// How a whole-db change-stream operation is handled: a document change we emit,
/// a DDL event we skip (a `drop`/`rename`/`dropDatabase` is not a row change — it
/// must NOT bail the whole run, especially for an uncaptured collection), or an
/// `invalidate` that genuinely ended the stream.
enum OpClass {
    Row,
    Skip,
    Invalidate,
}

fn classify_op(op: &OperationType) -> OpClass {
    match op {
        OperationType::Insert
        | OperationType::Update
        | OperationType::Replace
        | OperationType::Delete => OpClass::Row,
        OperationType::Invalidate => OpClass::Invalidate,
        // Drop, Rename, DropDatabase, and any future non-row op: skip.
        _ => OpClass::Skip,
    }
}

/// The `_data` hex of a change-stream resume token — an order-preserving keystring
/// (`{"_data": "82…"}`), so lexical comparison is oplog order. Used to tell a
/// bounded run whether the stream has advanced past its open-time target.
fn token_data(v: &serde_json::Value) -> Option<String> {
    v.get("_data").and_then(|d| d.as_str()).map(String::from)
}

/// Pure bound verdicts (#161): Mongo was the only engine whose `until_current`
/// stop rules lived inline in the drain loop instead of a testable transition
/// (its siblings: MySQL `commit_past_bound`, PG `tx_disposition`, MSSQL
/// `fill_sql`). Two rules, both unit-tested in both directions:
///
/// An EMPTY poll's disposition: the stream's resume token has silently advanced
/// past the open-time `_data` target → the backlog is drained, STOP; no target
/// (unbounded shouldn't reach here, and an unparseable boundary fails OPEN) →
/// STOP; otherwise the backlog is still coming → POLL again.
fn idle_poll_stops(advanced: Option<&str>, target: Option<&str>) -> bool {
    match (advanced, target) {
        (Some(cur), Some(tgt)) => cur > tgt,
        (_, None) => true,
        _ => false,
    }
}

/// The TIME bound: an event whose `cluster_time` is past the open-time cluster
/// time arrived AFTER we opened — a bounded run stops there (without it,
/// sustained writes keep `next_if_any` yielding and the run never terminates).
fn past_time_bound(
    until_current: bool,
    cluster_time: Option<mongodb::bson::Timestamp>,
    bound: Option<mongodb::bson::Timestamp>,
) -> bool {
    until_current && matches!((cluster_time, bound), (Some(ct), Some(b)) if ct > b)
}

/// Run the server-identity command, tolerating servers that predate `hello`.
///
/// `hello` was introduced in MongoDB **4.4.2**. rivet's declared support window
/// starts at 4.4, so 4.4.0 and 4.4.1 answer `CommandNotFound (59): no such
/// command: 'hello'` — and every stand in this repo is 4.4.30+, which is why no
/// test could have caught it. Found by an adversarial pass that read the version
/// requirement rather than trusting the stands.
///
/// `isMaster` is the pre-4.4.2 spelling and returns the SAME `operationTime` and
/// `setName` (verified on the 4.4 stand: both commands, identical values). Trying
/// it second makes the probe work across the whole window at the cost of one
/// extra round-trip on ancient servers only.
///
/// The driver error is RETURNED rather than swallowed: a caller that refuses on a
/// missing answer must be able to say WHY, and `no such command: 'hello'` tells an
/// operator in one line what a generic "the deployment does not report one" cannot.
async fn server_identity(
    client: &mongodb::Client,
    db: &str,
) -> std::result::Result<mongodb::bson::Document, mongodb::error::Error> {
    match client.database(db).run_command(doc! { "hello": 1 }).await {
        Ok(d) => Ok(d),
        Err(hello_err) => client
            .database(db)
            .run_command(doc! { "isMaster": 1 })
            .await
            .map_err(|_| hello_err),
    }
}

/// The open-time cluster-time bound a `until_current` run MUST have, or a loud
/// refusal explaining why the run cannot be bounded.
///
/// The `.ok()` this replaces swallowed every failure of the `hello` command —
/// a permission error, a transient network fault, a server that answers without
/// `operationTime` — and left `until_current_ts` as `None`. That is not a
/// harmless degradation on MongoDB, and the difference from the other engines is
/// the whole point:
///
/// - PostgreSQL, MySQL and SQL Server keep a CATCH-UP exit (a short/empty peek,
///   `BINLOG_DUMP_NON_BLOCK`'s EOF, the capture job's scan gap). With the bound
///   unset they still terminate — late, not never — so failing OPEN there is the
///   right call, as CLAUDE.md records.
/// - MongoDB has NO such backstop. `past_time_bound` is `false` for every event
///   once the bound is `None`, so the only remaining exit is the empty-poll check
///   — and under sustained writes `next_if_any` keeps returning events, so it
///   never fires. The run does not terminate late; it does not terminate.
///
/// The repo already measured that: disabling the cluster-time pin HANGS
/// `roast_until_current_terminates_under_sustained_writes_and_keeps_backlog`
/// (killed at its 30s ceiling). A silent `None` reaches the identical state by
/// accident, on a config that explicitly asked for a bounded run.
///
/// So this refuses. `until_current: false` (the daemon) is unaffected and never
/// consults the bound.
pub(crate) fn until_current_bound(
    until_current: bool,
    operation_time: Option<mongodb::bson::Timestamp>,
    probe_error: Option<String>,
) -> Result<Option<mongodb::bson::Timestamp>> {
    if !until_current {
        return Ok(None);
    }
    operation_time.map(Some).ok_or_else(|| {
        // The server's own words when we have them. A refusal that guesses at
        // "the deployment does not report one" while the server actually said
        // "not authorized on admin to execute command" sends the operator to fix
        // the wrong thing.
        let cause = match probe_error {
            Some(e) => format!("the server answered: {e}"),
            None => "the server answered without an `operationTime`".to_string(),
        };
        anyhow::anyhow!(
            "mongo cdc: `until_current: true` needs the cluster time at open, and {cause}. \
             Unlike the SQL engines, a MongoDB change stream has no catch-up exit to fall \
             back on: without this bound the run would keep draining under any ongoing \
             writes and NEVER terminate, so continuing would hang rather than finish late. \
             Check the connection's permissions and that the source is a replica set / \
             sharded cluster (a standalone reports no operationTime and cannot serve change \
             streams at all), or set `until_current: false` to stream continuously."
        )
    })
}

/// Persist a resume token as a [`Position`] LOSSLESSLY. A token can carry a BSON
/// binary `_typeBits` field (for typed sort keys — e.g. an integer `_id`), and a
/// plain `serde_json` round-trip mangles that binary, so the server rejects it on
/// resume (`Bad resume token`, error 40648). We store the token's raw BSON bytes
/// (hex) — a faithful round-trip — plus the order-preserving `_data` keystring
/// for the `until_current` bound. See the version-matrix live test that caught it.
fn encode_resume_token(token: &mongodb::change_stream::event::ResumeToken) -> Result<Position> {
    let bson = mongodb::bson::to_bson(token)?;
    let doc = bson
        .as_document()
        .ok_or_else(|| anyhow::anyhow!("mongodb cdc: resume token is not a BSON document"))?;
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)?;
    let hex = super::bytes_to_hex(&buf);
    let data = doc.get_str("_data").ok();
    // `_data` FIRST so the `__pos` column string-sorts in oplog order: `_data` is
    // the order-preserving resume keystring, whereas `rt` is the full token (with
    // `_typeBits`) whose hex is NOT length-stable across events, so a `rt`-first
    // `__pos` mis-orders the downstream MERGE dedup when token lengths differ
    // (bug-hunt). Robust to serde_json's preserve_order either way: with it on,
    // insertion order wins (`_data` first); with it off, keys sort (`"_data"` <
    // `"rt"`). See `cdc::validate::parse_pos` which keys on `_data`.
    Ok(Position(serde_json::json!({ "_data": data, "rt": hex })))
}

/// Inverse of [`encode_resume_token`], with a fallback to the pre-lossless
/// `serde_json` form so an older checkpoint still resolves.
pub(crate) fn decode_resume_token(
    v: &serde_json::Value,
) -> Result<mongodb::change_stream::event::ResumeToken> {
    if let Some(hex) = v.get("rt").and_then(|x| x.as_str()) {
        let bytes = super::hex_to_bytes(hex)?;
        let doc = Document::from_reader(&bytes[..])?;
        return Ok(mongodb::bson::from_bson(mongodb::bson::Bson::Document(
            doc,
        ))?);
    }
    // Backward-compat: a pre-`rt` checkpoint persisted the raw driver token
    // `{"_data": "<string>"}`. Deserialize ONLY that exact shape — any other
    // shape (e.g. `{"rt":{}}`, found by fuzzing) must be a clean error, never
    // handed to the `ResumeToken`/bson deserializer, which PANICS (not `Err`s)
    // on a type mismatch. The release build is `panic = "abort"`, so an unguarded
    // deserialize would abort the whole run on a corrupt/foreign checkpoint.
    // Deserialize a document built from `_data` ALONE, never `v` itself: a file
    // carrying `_data` beside foreign keys panicked the bson visitor rather than
    // erroring (fuzz crash 1c53b95b, 2026-08-17), so passing `v` through checked
    // one key and then trusted the whole object. A resume token's meaning is
    // entirely in `_data`; anything else in the file is noise to drop.
    if let Some(data) = v.get("_data").and_then(|x| x.as_str()) {
        return Ok(serde_json::from_value(
            serde_json::json!({ "_data": data }),
        )?);
    }
    anyhow::bail!(
        "mongodb cdc: unrecognized resume-token checkpoint shape \
         (expected an `rt` hex string or a `_data` string): {v}"
    )
}

impl MongoChangeStream {
    /// Open a database-wide change stream, resuming from `checkpoint` when one
    /// exists (else starting at the current oplog position). `UpdateLookup` fills
    /// `full_document` for updates so the post-image is always the whole document.
    pub(crate) fn open(
        url: &str,
        tls: Option<&TlsConfig>,
        checkpoint: Option<&std::path::Path>,
        canonical: bool,
        mode: DrainMode,
        // The export's configured `table:` names. Mongo had no routing
        // cross-check at all — this is what the zero-match warning needs.
        configured_tables: &[String],
    ) -> Result<Self> {
        let until_current = mode.is_bounded();
        let session = MongoSession::connect(url, tls)?;
        let db_name = session.db().to_string();
        // The resume token persisted by a prior run (opaque JSON → driver token).
        // A corrupt / unreadable checkpoint is a LOUD error, never silently
        // treated as "no checkpoint" — that would re-anchor at now and leave a
        // silent gap (`Position::load` returns Ok(None) only when the file is
        // absent, Err when present-but-unparseable; bug-hunt find).
        let resume = match checkpoint {
            Some(p) => Position::load(p)?,
            None => None,
        }
        .map(|pos| decode_resume_token(&pos.0))
        .transpose()?;
        // A checkpoint path with NO persisted position ⇒ a fresh checkpointed
        // run: it must pin its anchor at open (see below).
        let is_fresh = resume.is_none();
        // Declare the capture fidelity tier UP FRONT (never a silent degrade): a
        // sub-6.0 server gives current-state UpdateLookup post-images and no delete
        // pre-image, so a null `document` on an update/delete means "this tier
        // can't provide it", not "the value was null". doctor surfaces the same.
        // A configured collection the database does not hold. MEASURED before this:
        // `table: no_such_collection` ran to `status: success, rows: 0` with no
        // warning — a typo and a genuinely quiet window look identical, and the
        // first is the one an operator needs before concluding "CDC works, there is
        // just no traffic".
        //
        // A WARNING, not a refusal: on Mongo a collection is created by its first
        // write, so capturing one that does not exist YET is a legitimate setup —
        // start the stream, then let the app create it. Refusing would break that.
        // What is not legitimate is silence.
        //
        // This engine's whole share of the resolution contract is this arm: the
        // stream is scoped to ONE database, so the cross-schema ambiguity the SQL
        // engines refuse cannot arise here.
        if !configured_tables.is_empty() {
            let present: Vec<String> = session
                .block_on(async {
                    session
                        .client()
                        .database(&db_name)
                        .list_collection_names()
                        .await
                })
                .unwrap_or_default();
            if !present.is_empty() {
                let missing: Vec<&String> = configured_tables
                    .iter()
                    .filter(|c| {
                        !present.iter().any(|p| {
                            crate::source::cdc::sink::table_matches(
                                crate::source::cdc::CdcEngine::Mongo,
                                c,
                                &db_name,
                                p,
                            )
                        })
                    })
                    .collect();
                if !missing.is_empty() {
                    log::warn!(
                        "mongodb cdc: could not find {missing:?} in database `{db_name}` — it \
                         holds {present:?}. A collection is created by its first write, so \
                         this is fine if the app has not written yet; if it is a typo the \
                         capture will report success over zero rows forever."
                    );
                }
            }
        }
        let cap = probe_capability_on(&session);
        log::info!(
            "mongodb cdc: server {} — capture tier: {}",
            cap.server_version,
            cap.tier()
        );
        let stream = session.block_on(async {
            session
                .client()
                .database(&db_name)
                .watch()
                // Post-image for insert/update (current-state lookup).
                .full_document(FullDocumentType::UpdateLookup)
                // Delete/update PRE-image when the server carries it (6.0+ with
                // `changeStreamPreAndPostImages`); silently absent otherwise.
                .full_document_before_change(FullDocumentBeforeChangeType::WhenAvailable)
                // Bound how long the server holds a getMore open for a new change.
                // Short so a bounded (`until_current`) run detects "drained" quickly
                // via `next_if_any`; harmless for the daemon (`next` just re-polls).
                .max_await_time(std::time::Duration::from_millis(500))
                .resume_after(resume)
                .await
        })?;
        // The "current end" a bounded (`until_current`) run drains up to (the
        // resume-token `_data`, for the empty-poll race), plus the cluster time at
        // open (the strict upper bound that terminates under sustained writes).
        let target_data = if until_current {
            stream
                .resume_token()
                .and_then(|t| serde_json::to_value(&t).ok())
                .as_ref()
                .and_then(token_data)
        } else {
            None
        };
        // A `until_current` run that cannot pin its ceiling must FAIL, not degrade:
        // MongoDB has no catch-up exit, so an unset bound means the run never ends.
        // See `until_current_bound` for why this engine differs from the SQL ones.
        let (operation_time, probe_err) = if until_current {
            session.block_on(async {
                match server_identity(session.client(), &db_name).await {
                    Ok(d) => (d.get_timestamp("operationTime").ok(), None),
                    // Keep the driver's own words. A refusal that says "the
                    // deployment does not report one" when the server actually said
                    // "not authorized on admin" sends the operator the wrong way.
                    Err(e) => (None, Some(e.to_string())),
                }
            })
        } else {
            (None, None)
        };
        let until_current_ts = until_current_bound(until_current, operation_time, probe_err)?;
        let this = Self {
            session,
            stream,
            canonical,
            db_name,
            until_current,
            target_data,
            until_current_ts,
            hetero_first: None,
            hetero_warned: false,
        };
        // Idle-first-run anchor (MongoDB has no server-side anchor — the MySQL
        // model): a fresh checkpointed open persists its current resume token NOW.
        // A first run that captures ZERO changes writes no per-event checkpoint,
        // so without this the NEXT run would open with no token, re-anchor at
        // "current", and skip everything inserted meanwhile — exactly the
        // "enable CDC during a quiet period" ops sequence. Pinning at open makes
        // the idle first run at-least-once like every other.
        if is_fresh
            && let Some(ckpt) = checkpoint
            && let Some(pos) = this.anchor_position()
        {
            pos.save(ckpt)?;
            log::info!("mongodb cdc: pinned resume anchor at open (fresh checkpoint)");
        }
        Ok(this)
    }

    /// The resume token to open from right now — used to pin a client-side anchor
    /// before any change arrives (the MySQL model: no server-side anchor).
    pub(crate) fn anchor_position(&self) -> Option<Position> {
        self.stream
            .resume_token()
            .and_then(|t| encode_resume_token(&t).ok())
    }
}

/// Pin the resume anchor at the CURRENT oplog position (a first-run open with no
/// prior checkpoint). MongoDB has no server-side anchor — a change stream opened
/// without a token starts at "now" — so the coordinates must be persisted
/// immediately, or an idle first run would let the next run re-anchor forward and
/// skip everything in between (the MySQL binlog anchor rule, per CLAUDE.md).
pub(crate) fn pin_checkpoint_at_current(
    url: &str,
    tls: Option<&TlsConfig>,
    checkpoint: &std::path::Path,
) -> Result<()> {
    // Anchoring IS just a fresh checkpointed open: `open` pins the current resume
    // token when it finds a checkpoint path with no prior position (the idle-
    // first-run anchor). One mechanism, one place — this is the `ensure_anchor`
    // entry into it. (A non-replica-set can't `watch()`, so open fails loudly
    // before any pin.) The stream is opened only to anchor, then dropped.
    // Anchor-only open: the routing cross-check is the RUN's job, so an empty
    // configured list here is correct rather than an omission.
    MongoChangeStream::open(
        url,
        tls,
        Some(checkpoint),
        false,
        DrainMode::Continuous,
        &[],
    )
    .map(|_| ())
}

/// What a MongoDB CDC run actually delivers — probed from the server so the tier
/// is DECLARED, never silently assumed. `major < 6` ⇒ current-state UpdateLookup
/// post-images and key-only deletes; `>= 6` ⇒ full pre/post-images ride when the
/// collection has `changeStreamPreAndPostImages` enabled.
pub(crate) struct MongoCdcCapability {
    pub(crate) server_version: String,
    pub(crate) major: u32,
    pub(crate) is_replica_set: bool,
}

impl MongoCdcCapability {
    /// One-line fidelity declaration for the log / `doctor`.
    pub(crate) fn tier(&self) -> &'static str {
        if self.major >= 6 {
            "full-image-capable (6.0+) — delete/update pre-images ride when \
             changeStreamPreAndPostImages is enabled on the collection"
        } else {
            "current-state (UpdateLookup) — update post-images are current-state \
             (not point-in-time) and deletes carry _id only; upgrade to 6.0+ for pre/post-images"
        }
    }
}

/// Probe server version + replica-set membership on an existing session. Best
/// effort: a failed command degrades to `unknown`/`false` rather than erroring
/// the open — the tier line is informational, the watch itself is the gate.
fn probe_capability_on(session: &MongoSession) -> MongoCdcCapability {
    session.block_on(async {
        let db = session.client().database(session.db());
        let version = db
            .run_command(doc! { "buildInfo": 1 })
            .await
            .ok()
            .and_then(|d| d.get_str("version").ok().map(str::to_string))
            .unwrap_or_else(|| "unknown".to_string());
        let major = version
            .split('.')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        // Through the same seam as the bound probe: `hello` does not exist before
        // 4.4.2, and asking it alone made `rivet doctor` report a genuine replica
        // set as a standalone on 4.4.0/4.4.1 — telling an operator to run
        // rs.initiate() on a cluster that is already initiated.
        let is_replica_set = server_identity(session.client(), session.db())
            .await
            .is_ok_and(|d| d.get_str("setName").is_ok());
        MongoCdcCapability {
            server_version: version,
            major,
            is_replica_set,
        }
    })
}

/// Connect + probe (for `rivet doctor` — a fresh connection).
pub(crate) fn probe_capability(url: &str, tls: Option<&TlsConfig>) -> Result<MongoCdcCapability> {
    let session = MongoSession::connect(url, tls)?;
    Ok(probe_capability_on(&session))
}

/// Map one driver change event to the canonical [`ChangeEvent`] (JSON-blob image).
fn to_change_event(
    cse: ChangeStreamEvent<Document>,
    canonical: bool,
    db_name: &str,
) -> Result<ChangeEvent> {
    let op = match cse.operation_type {
        OperationType::Insert => ChangeOp::Insert,
        // A `replace` is a full-document overwrite — a row update in our model.
        OperationType::Update | OperationType::Replace => ChangeOp::Update,
        OperationType::Delete => ChangeOp::Delete,
        other => anyhow::bail!(
            "mongodb cdc: change operation {other:?} is collection/cluster level (drop, rename, \
             invalidate), not a row change — rivet streams document changes only"
        ),
    };
    let table = cse
        .ns
        .as_ref()
        .and_then(|n| n.coll.clone())
        .unwrap_or_default();

    // `_id` from the document key (always present for row changes).
    let id_str = cse
        .document_key
        .as_ref()
        .map(|dk| id_to_string(dk.get("_id")))
        .unwrap_or_default();
    let id_val = RivetValue::Bytes(id_str.into_bytes());

    // The `document` column: post-image for insert/update, pre-image (6.0+) for
    // delete, else Null (a delete with no pre-image, or an unlooked-up update).
    let doc_source = match op {
        ChangeOp::Delete => cse.full_document_before_change.as_ref(),
        _ => cse.full_document.as_ref(),
    };
    let doc_val = match doc_source {
        Some(d) => RivetValue::Bytes(document_to_json(d, canonical)?.into_bytes()),
        None => RivetValue::Null,
    };

    let image = vec![id_val, doc_val];
    let (before, after) = match op {
        ChangeOp::Delete => (Some(image), None),
        _ => (None, Some(image)),
    };

    let mut ev = ChangeEvent {
        op,
        schema: db_name.to_string(),
        table,
        before,
        after,
        // The per-event resume token is the exact re-open position.
        position: encode_resume_token(&cse.id)?,
        committed: false,
        image_names: Some(std::sync::Arc::clone(&IMAGE_NAMES)),
        seq: 0, // stamped by TxnSeq as the stream is consumed
        poison: None,
    };
    // #158: Mongo's model — a SINGLE-document write's change event IS its own commit (post-commit
    // oplog), so it is a boundary. A MULTI-document transaction (one lsid/txnNumber) shares one
    // commit across N events, so marking each `committed` can roll the sink MID-transaction — but
    // Mongo's resume token is PER-EVENT and consume-free, so a crash between the mid-txn checkpoint
    // and the tail RE-READS the remaining events (at-least-once, dedup absorbs it), never SKIPS them
    // like the PG slot / MSSQL from-LSN would (which is why those engines frame the true boundary).
    // A NAMED decision via the shared framer, not an inline `committed: true` that reads as a divergence.
    crate::source::cdc::TxnFramer::single_event_commit(&mut ev);
    Ok(ev)
}

impl ChangeStream for MongoChangeStream {
    fn engine(&self) -> crate::source::cdc::CdcEngine {
        crate::source::cdc::CdcEngine::Mongo
    }

    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        let canonical = self.canonical;
        let until_current = self.until_current;
        let target = self.target_data.clone();
        let bound_ts = self.until_current_ts;
        // Split the borrow: `block_on` reads `&session`, the future drives
        // `&mut stream` — disjoint fields.
        let session = &self.session;
        let stream = &mut self.stream;
        let db_name = &self.db_name;
        // Disjoint field borrows (distinct from session/stream/db_name) so the
        // heterogeneous-`_id` observation below can mutate warn state in-loop.
        let hetero_first = &mut self.hetero_first;
        let hetero_warned = &mut self.hetero_warned;
        loop {
            // Pull one raw event (or terminate). Bounded run: drain up to the
            // open-time target; `next_if_any` returns `None` on an empty poll, but a
            // single empty poll can precede the backlog's getMore (worst on 4.4), so
            // only stop once the position IS past the target. Daemon: block.
            let cse = if until_current {
                match session.block_on(async { stream.next_if_any().await }) {
                    Ok(Some(cse)) => cse,
                    Ok(None) => {
                        let advanced = stream
                            .resume_token()
                            .and_then(|t| serde_json::to_value(&t).ok())
                            .as_ref()
                            .and_then(token_data);
                        if idle_poll_stops(advanced.as_deref(), target.as_deref()) {
                            return None;
                        }
                        continue; // backlog still coming — poll again
                    }
                    Err(e) => return Some(Err(anyhow::Error::from(e))),
                }
            } else {
                match session.block_on(async { stream.next().await }) {
                    Some(Ok(cse)) => cse,
                    Some(Err(e)) => return Some(Err(anyhow::Error::from(e))),
                    None => return None, // stream closed
                }
            };

            // H — time bound: an event past the open-time cluster time is a NEW
            // write (arrived after we opened), so a bounded run stops there.
            // Without it, sustained writes keep `next_if_any` returning events and
            // the run never terminates (the `_data` target only fires on an empty
            // poll, which never happens under continuous writes).
            if past_time_bound(until_current, cse.cluster_time, bound_ts) {
                return None;
            }

            // G — operation classification: a whole-db watch also sees DDL
            // (`drop`, `rename`, `dropDatabase`) that is NOT a row change — SKIP it
            // and keep draining rather than bailing the whole run on a drop of any
            // (even uncaptured) collection. `invalidate` genuinely ends the stream
            // (dropDatabase / a collection-stream drop) → a loud terminal error.
            match classify_op(&cse.operation_type) {
                OpClass::Row => {
                    // Heterogeneous-`_id` display-collision warning (once per
                    // stream), mirroring the batch full-scan guard: the flat `_id`
                    // column can render distinct BSON types to the same text, so a
                    // downstream merge keyed on `_id` conflates them.
                    if !*hetero_warned {
                        let b = cse
                            .document_key
                            .as_ref()
                            .and_then(|dk| dk.get("_id"))
                            .and_then(super::id_bracket);
                        match *hetero_first {
                            None => *hetero_first = Some(b),
                            Some(first) if first != b => {
                                log::warn!(
                                    "mongodb cdc: heterogeneous `_id` types across the \
                                     change stream: {}",
                                    super::hetero_id_guidance()
                                );
                                *hetero_warned = true;
                            }
                            _ => {}
                        }
                    }
                    return Some(to_change_event(cse, canonical, db_name));
                }
                OpClass::Skip => continue,
                OpClass::Invalidate => {
                    return Some(Err(anyhow::anyhow!(
                        "mongodb cdc: the change stream was INVALIDATED (the watched \
                         database was dropped, or a captured collection dropped/renamed). \
                         The resume token is no longer usable — re-create the capture from \
                         a fresh checkpoint after confirming the source state."
                    )));
                }
            }
        }
    }

    // ack is a no-op: MongoDB retains changes in the oplog independently of
    // reads, so the persisted resume token (the checkpoint) alone makes resume
    // at-least-once — same as MySQL's binlog / SQL Server's change tables.
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Fuzzing (`fuzz/fuzz_targets/mongo_resume_token.rs`) found that a corrupt
    // checkpoint whose `rt` is not a hex string — e.g. `{"rt":{}}` — reached an
    // unguarded `serde_json::from_value::<ResumeToken>` that PANICS inside the
    // bson deserializer (a type mismatch, not an `Err`). With `panic = "abort"`
    // in the release profile that aborts the whole run on a corrupt/foreign
    // checkpoint. The decoder must return a clean error for any unrecognized
    // shape. (RED before the shape-guard: `decode_resume_token` panics here.)

    /// The exact input libFuzzer crashed on (nightly Fuzz, run 31998520492,
    /// 2026-08-17): a `_data` STRING sitting beside a dozen unrelated keys, one
    /// of them deeply nested. The guard below checked that `_data` was a string
    /// and then handed the WHOLE object to the deserializer, which PANICS
    /// (`unreachable!` in bson's seeded visitor) instead of erroring — so the
    /// precondition its own comment claimed ("Deserialize ONLY that exact
    /// shape") was named but never established. `panic = "abort"` in release
    /// makes that an aborted run on a foreign checkpoint file.
    #[test]
    fn decode_resume_token_survives_foreign_keys_beside_data() {
        let raw = r##"{"rz~tt":"t'" ,    "/":{"":{"":{"":{"t":{}}}}},   " t":"t'" ,    "RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR   ~vrtt":"t'" ,    "_data":"8" ,    " jjjjjjjjjjjr~tt":    "_data'" ,    "   rt":" E" }"##;
        let v: serde_json::Value = serde_json::from_str(raw).expect("corpus input is valid JSON");
        let token = decode_resume_token(&v)
            .expect("a `_data` string is a resume token whatever else the file carries");
        // Non-inert: the token really is the one `_data` named, not a default.
        let round: serde_json::Value = serde_json::to_value(&token).unwrap();
        assert_eq!(round.get("_data").and_then(|d| d.as_str()), Some("8"));
    }

    /// The `rt` branch is the SIBLING of the `_data` one and was never fuzzed
    /// past the hex gate — libFuzzer cannot readily synthesise valid BSON, so a
    /// well-formed document of the WRONG SHAPE is a hole the corpus cannot
    /// reach. Probed directly with real BSON: it does NOT panic, because
    /// `from_bson` decodes a `ResumeToken` as an opaque raw document rather than
    /// through the serde_json visitor that blew up on the `_data` path. It is
    /// ACCEPTED and rejected later by the server (`Bad resume token`, 40648) —
    /// loud and lossless, which is why this asserts no-panic rather than `Err`.
    #[test]
    fn decode_resume_token_does_not_panic_on_wellformed_bson_of_the_wrong_shape() {
        let mut doc = Document::new();
        doc.insert("x", mongodb::bson::doc! { "y": 1i32 });
        let mut buf = Vec::new();
        doc.to_writer(&mut buf).unwrap();
        let hex: String = buf.iter().map(|b| format!("{b:02x}")).collect();
        // The point of the test is that this returns at all.
        let _ = decode_resume_token(&json!({ "rt": hex }));
    }

    #[test]
    fn decode_resume_token_rejects_malformed_shapes_without_panicking() {
        for bad in [
            json!({"rt": {}}),    // the fuzz-found crash: `rt` is an object
            json!({"rt": 5}),     // `rt` is a number
            json!({"_data": {}}), // `_data` present but not a string
            json!({"_data": 7}),
            json!({}), // neither field
            json!([]), // not even an object
            json!("scalar"),
        ] {
            assert!(
                decode_resume_token(&bad).is_err(),
                "malformed resume token must be a clean Err, not a panic: {bad}"
            );
        }
    }

    /// #161: Mongo's until_current stop rules as pure transitions, both
    /// directions each — the one engine whose bound lived inline in the drain
    /// loop (siblings: commit_past_bound / tx_disposition / fill_sql).
    #[test]
    fn idle_poll_stops_both_directions() {
        // stop: token advanced past the open-time target — backlog drained.
        assert!(idle_poll_stops(Some("82AB"), Some("8299")));
        // stop: no target (fail OPEN on an unparseable boundary).
        assert!(idle_poll_stops(None, None));
        assert!(idle_poll_stops(Some("82AB"), None));
        // poll: backlog still coming (at or below the target, or no token yet).
        assert!(!idle_poll_stops(Some("8299"), Some("82AB")));
        assert!(!idle_poll_stops(Some("82AB"), Some("82AB")));
        assert!(!idle_poll_stops(None, Some("82AB")));
    }

    /// `until_current` must REFUSE without its bound, and only then.
    ///
    /// Honest about what this can and cannot reach: the live suite CANNOT produce
    /// the failing input. Every stand is a healthy replica set that answers `hello`
    /// with an `operationTime`, and making it not do so means faulting the server,
    /// not the fixture. So the seam is asserted here, at the one function that
    /// decides — and the branch it guards is pinned by an EXISTING live test from
    /// the other direction: disabling the cluster-time bind hangs
    /// `roast_until_current_terminates_under_sustained_writes_and_keeps_backlog`
    /// (killed at its 30s ceiling), which is precisely the state a silent `None`
    /// used to reach by accident.
    #[test]
    fn until_current_refuses_when_the_cluster_time_bound_is_unavailable() {
        use mongodb::bson::Timestamp;
        let ts = Timestamp {
            time: 1_700_000_000,
            increment: 3,
        };

        assert_eq!(
            until_current_bound(true, Some(ts), None).unwrap(),
            Some(ts),
            "the ordinary case must pass the bound through untouched"
        );
        // The daemon never consults the bound, so a missing operationTime is not
        // its problem — refusing here would break continuous streaming outright.
        assert_eq!(
            until_current_bound(false, None, None).unwrap(),
            None,
            "`until_current: false` streams forever by design and needs no ceiling"
        );
        assert_eq!(
            until_current_bound(false, Some(ts), None).unwrap(),
            None,
            "the daemon must not carry a bound even when one is available — a stray \
             ceiling would end a stream that is supposed to run continuously"
        );

        let err = until_current_bound(true, None, None)
            .expect_err("a bounded run with no ceiling must refuse, not run forever")
            .to_string();
        assert!(
            err.to_lowercase().contains("never terminate") && err.contains("until_current: false"),
            "the refusal must say what would happen (the run does not end — it does \
             not merely finish late, the way the SQL engines would) AND name the \
             setting that accepts continuous streaming instead: {err}"
        );

        // The server's OWN words when the probe failed. Without this the refusal
        // guesses — and an adversarial pass found the guess would be wrong on a
        // pre-4.4.2 server, where the real answer is `no such command: 'hello'`
        // and the emitted text talked about replica-set membership instead.
        let with_cause = until_current_bound(
            true,
            None,
            Some("CommandNotFound (59): no such command: 'hello'".to_string()),
        )
        .expect_err("still a refusal")
        .to_string();
        assert!(
            with_cause.contains("no such command: 'hello'"),
            "the driver's own error must reach the operator — it names the cause in \
             one line where a generic message sends them to check permissions they \
             never had a problem with: {with_cause}"
        );
    }

    #[test]
    fn past_time_bound_both_directions() {
        use mongodb::bson::Timestamp;
        let t = |time: u32| Some(Timestamp { time, increment: 0 });
        // stop: bounded run, event after the open-time cluster time.
        assert!(past_time_bound(true, t(101), t(100)));
        // keep: at/before the bound, daemon mode, or no bound.
        assert!(!past_time_bound(true, t(100), t(100)));
        assert!(!past_time_bound(true, t(99), t(100)));
        assert!(!past_time_bound(false, t(101), t(100)));
        assert!(!past_time_bound(true, t(101), None));
        assert!(!past_time_bound(true, None, t(100)));
    }
}
