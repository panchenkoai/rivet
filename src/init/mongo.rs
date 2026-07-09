//! `rivet init` introspection for MongoDB.
//!
//! MongoDB is schemaless and `mode: full` only, so — unlike the SQL engines —
//! there are no columns, PKs, or cursor/chunk candidates to introspect. Each
//! collection becomes a [`TableInfo`] with an empty column list (which makes
//! [`TableInfo::suggest_mode`] resolve to `full`) and a scan-free document-count
//! estimate. The scaffold then emits a `type: mongo` source + one
//! `table: <collection>` / `mode: full` export per collection.
//!
//! The async→sync bridge is the shared [`MongoSession`]; init connects
//! ungated (dev convenience, like the SQL init helpers).

use mongodb::bson::Document;

use super::TableInfo;
use crate::error::Result;
use crate::source::mongo::MongoSession;

/// Connect once (ungated) so one session serves the whole `list_tables` +
/// per-collection `introspect` scan.
pub(super) fn connect(url: &str) -> Result<MongoSession> {
    MongoSession::connect(url, None, false)
}

/// List the user collections in the URL's database, skipping the internal
/// `system.*` namespaces so the scaffold never emits an export for them.
pub(super) fn list_tables(session: &MongoSession) -> Result<Vec<String>> {
    let names = session.block_on(async {
        session
            .client()
            .database(session.db())
            .list_collection_names()
            .await
    })?;
    Ok(names
        .into_iter()
        .filter(|n| !n.starts_with("system."))
        .collect())
}

/// One collection → a [`TableInfo`] with a scan-free row estimate and no
/// columns (schemaless). Empty columns make `suggest_mode` resolve to `full`.
pub(super) fn introspect(session: &MongoSession, collection: &str) -> Result<TableInfo> {
    let count = session
        .block_on(async {
            session
                .client()
                .database(session.db())
                .collection::<Document>(collection)
                .estimated_document_count()
                .await
        })
        .unwrap_or(0);
    Ok(TableInfo {
        schema: session.db().to_string(),
        table: collection.to_string(),
        row_estimate: i64::try_from(count).unwrap_or(0),
        total_bytes: None,
        columns: Vec::new(),
    })
}
