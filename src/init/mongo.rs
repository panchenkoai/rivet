//! `rivet init` introspection for MongoDB.
//!
//! MongoDB is schemaless and `mode: full` only, so — unlike the SQL engines —
//! there are no columns, PKs, or cursor/chunk candidates to introspect. Each
//! collection becomes a [`TableInfo`] with an empty column list (which makes
//! [`TableInfo::suggest_mode`] resolve to `full`) and a scan-free document-count
//! estimate. The scaffold then emits a `type: mongo` source + one
//! `table: <collection>` / `mode: full` export per collection.

use mongodb::bson::{Document, doc};
use mongodb::{Client, options::ClientOptions};
use tokio::runtime::Runtime;

use super::TableInfo;
use crate::error::Result;

/// Owns the async client + the runtime that drives it, so one connection serves
/// the whole `list_tables` + per-collection `introspect` scan (mirrors the SQL
/// init submodules, which connect once). Init is a dev convenience, so — like
/// the SQL `connect` helpers — it does not apply the remote-plaintext TLS gate;
/// transport security rides the connection string (`?tls=true`).
pub(super) struct MongoInitConn {
    rt: Runtime,
    client: Client,
    db: String,
}

pub(super) fn connect(url: &str) -> Result<MongoInitConn> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
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
        client.database(&db).run_command(doc! { "ping": 1 }).await?;
        Ok::<_, anyhow::Error>((client, db))
    })?;
    Ok(MongoInitConn { rt, client, db })
}

/// List the user collections in the URL's database, skipping the internal
/// `system.*` namespaces so the scaffold never emits an export for them.
pub(super) fn list_tables(conn: &mut MongoInitConn) -> Result<Vec<String>> {
    let MongoInitConn { rt, client, db } = conn;
    let names = rt.block_on(async { client.database(db).list_collection_names().await })?;
    Ok(names
        .into_iter()
        .filter(|n| !n.starts_with("system."))
        .collect())
}

/// One collection → a [`TableInfo`] with a scan-free row estimate and no
/// columns (schemaless). Empty columns make `suggest_mode` resolve to `full`.
pub(super) fn introspect(conn: &mut MongoInitConn, collection: &str) -> Result<TableInfo> {
    let MongoInitConn { rt, client, db } = conn;
    let count = rt
        .block_on(async {
            client
                .database(db)
                .collection::<Document>(collection)
                .estimated_document_count()
                .await
        })
        .unwrap_or(0);
    Ok(TableInfo {
        schema: db.clone(),
        table: collection.to_string(),
        row_estimate: i64::try_from(count).unwrap_or(0),
        total_bytes: None,
        columns: Vec::new(),
    })
}
