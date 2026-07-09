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
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position};

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
}

/// The `_data` hex of a change-stream resume token — an order-preserving keystring
/// (`{"_data": "82…"}`), so lexical comparison is oplog order. Used to tell a
/// bounded run whether the stream has advanced past its open-time target.
fn token_data(v: &serde_json::Value) -> Option<String> {
    v.get("_data").and_then(|d| d.as_str()).map(String::from)
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
fn decode_resume_token(
    v: &serde_json::Value,
) -> Result<mongodb::change_stream::event::ResumeToken> {
    if let Some(hex) = v.get("rt").and_then(|x| x.as_str()) {
        let bytes = super::hex_to_bytes(hex)?;
        let doc = Document::from_reader(&bytes[..])?;
        return Ok(mongodb::bson::from_bson(mongodb::bson::Bson::Document(
            doc,
        ))?);
    }
    Ok(serde_json::from_value(v.clone())?)
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
        until_current: bool,
    ) -> Result<Self> {
        let session = MongoSession::connect(url, tls, true)?;
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
        // The "current end" a bounded (`until_current`) run drains up to.
        let target_data = if until_current {
            stream
                .resume_token()
                .and_then(|t| serde_json::to_value(&t).ok())
                .as_ref()
                .and_then(token_data)
        } else {
            None
        };
        let this = Self {
            session,
            stream,
            canonical,
            db_name,
            until_current,
            target_data,
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
    MongoChangeStream::open(url, tls, Some(checkpoint), false, false).map(|_| ())
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
        let is_replica_set = db
            .run_command(doc! { "hello": 1 })
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
    let session = MongoSession::connect(url, tls, true)?;
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

    Ok(ChangeEvent {
        op,
        schema: db_name.to_string(),
        table,
        before,
        after,
        // The per-event resume token is the exact re-open position.
        position: encode_resume_token(&cse.id)?,
        // Every change-stream event is already committed (post-commit oplog).
        committed: true,
        image_names: Some(std::sync::Arc::clone(&IMAGE_NAMES)),
        seq: 0, // stamped by TxnSeq as the stream is consumed
    })
}

impl ChangeStream for MongoChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        let canonical = self.canonical;
        let until_current = self.until_current;
        // Split the borrow: `block_on` reads `&session`, the future drives
        // `&mut stream` — disjoint fields.
        let session = &self.session;
        let stream = &mut self.stream;
        if until_current {
            let target = self.target_data.clone();
            let db_name = &self.db_name;
            // Bounded run: drain up to the open-time target. `next_if_any` returns
            // `None` when nothing is available in a `max_await_time` window — but a
            // single empty poll can precede the backlog's getMore (the stream's
            // position has NOT advanced past the target yet), so only stop once the
            // position IS past the target. Otherwise poll again — this is the fix
            // for the intermittent backlog-drop the version matrix caught on 4.4.
            loop {
                match session.block_on(async { stream.next_if_any().await }) {
                    Ok(Some(cse)) => return Some(to_change_event(cse, canonical, db_name)),
                    Ok(None) => {
                        let advanced = stream
                            .resume_token()
                            .and_then(|t| serde_json::to_value(&t).ok())
                            .as_ref()
                            .and_then(token_data);
                        match (&advanced, &target) {
                            // Position moved past the open-time end → drained, stop.
                            (Some(cur), Some(tgt)) if cur > tgt => return None,
                            // No target (shouldn't happen) → fall back to first-None.
                            (_, None) => return None,
                            // Not yet past the target — the backlog is still coming.
                            _ => continue,
                        }
                    }
                    Err(e) => return Some(Err(anyhow::Error::from(e))),
                }
            }
        } else {
            // Daemon: block for the next change (the tailable stream only ends
            // when the connection closes).
            match session.block_on(async { stream.next().await }) {
                Some(Ok(cse)) => Some(to_change_event(cse, canonical, &self.db_name)),
                Some(Err(e)) => Some(Err(anyhow::Error::from(e))),
                None => None, // stream closed
            }
        }
    }

    // ack is a no-op: MongoDB retains changes in the oplog independently of
    // reads, so the persisted resume token (the checkpoint) alone makes resume
    // at-least-once — same as MySQL's binlog / SQL Server's change tables.
}
