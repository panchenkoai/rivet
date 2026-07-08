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
use mongodb::bson::Document;
use mongodb::change_stream::ChangeStream as DriverStream;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::options::FullDocumentType;

use super::{MongoSession, document_to_json, id_to_string};
use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position};

/// The two fixed image columns, matching the batch JSON-blob schema.
const IMAGE_NAMES: [&str; 2] = ["_id", "document"];

pub(crate) struct MongoChangeStream {
    session: MongoSession,
    stream: DriverStream<ChangeStreamEvent<Document>>,
    /// Render `document` as canonical (type-tagged) extended JSON when set.
    canonical: bool,
    /// The watched database — the `schema` field of every emitted change.
    db_name: String,
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
    ) -> Result<Self> {
        let session = MongoSession::connect(url, tls, true)?;
        let db_name = session.db().to_string();
        // The resume token persisted by a prior run (opaque JSON → driver token).
        let resume = checkpoint
            .and_then(|p| Position::load(p).ok().flatten())
            .map(|pos| serde_json::from_value(pos.0))
            .transpose()?;
        let stream = session.block_on(async {
            session
                .client()
                .database(&db_name)
                .watch()
                .full_document(FullDocumentType::UpdateLookup)
                .resume_after(resume)
                .await
        })?;
        Ok(Self {
            session,
            stream,
            canonical,
            db_name,
        })
    }

    /// The resume token to open from right now — used to pin a client-side anchor
    /// before any change arrives (the MySQL model: no server-side anchor).
    pub(crate) fn anchor_position(&self) -> Option<Position> {
        self.stream
            .resume_token()
            .and_then(|t| serde_json::to_value(&t).ok())
            .map(Position)
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
    let stream = MongoChangeStream::open(url, tls, None, false)?;
    match stream.anchor_position() {
        Some(pos) => pos.save(checkpoint),
        None => anyhow::bail!(
            "mongodb cdc: the server returned no resume token to anchor at — is this a replica set?"
        ),
    }
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
        position: Position(serde_json::to_value(&cse.id)?),
        // Every change-stream event is already committed (post-commit oplog).
        committed: true,
        image_names: Some(std::sync::Arc::from(
            IMAGE_NAMES.map(str::to_string).to_vec(),
        )),
        seq: 0, // stamped by TxnSeq as the stream is consumed
    })
}

impl ChangeStream for MongoChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        let canonical = self.canonical;
        // Split the borrow: `block_on` reads `&session`, the future drives
        // `&mut stream` — disjoint fields.
        let session = &self.session;
        let stream = &mut self.stream;
        match session.block_on(async { stream.next().await }) {
            Some(Ok(cse)) => Some(to_change_event(cse, canonical, &self.db_name)),
            Some(Err(e)) => Some(Err(anyhow::Error::from(e))),
            None => None, // stream closed
        }
    }

    // ack is a no-op: MongoDB retains changes in the oplog independently of
    // reads, so the persisted resume token (the checkpoint) alone makes resume
    // at-least-once — same as MySQL's binlog / SQL Server's change tables.
}
