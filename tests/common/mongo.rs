//! MongoDB live-test helper — driver-backed fixture seeding + verification for
//! `tests/live/live_mongo.rs` and `live_cdc_mongo.rs`.
//!
//! Connects with `directConnection=true`: the port-mapped single-node replica
//! set advertises its in-container host, which a host client cannot re-resolve
//! (a `ReplicaSetNoPrimary` otherwise). One owned tokio runtime bridges the async
//! driver to the sync test body, mirroring `common::mssql`'s tiberius helper.
#![allow(dead_code)]

use std::collections::BTreeMap;

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, Collection};

pub struct MongoTest {
    rt: tokio::runtime::Runtime,
    client: Client,
    db: String,
}

impl MongoTest {
    /// Connect to a Mongo on `port` (`27017` standalone, `27018` replica set).
    pub fn connect(port: u16, db: &str) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let uri = format!("mongodb://127.0.0.1:{port}/{db}?directConnection=true");
        let client =
            rt.block_on(async { Client::with_uri_str(&uri).await.expect("mongo: connect") });
        Self {
            rt,
            client,
            db: db.to_string(),
        }
    }

    /// Test-URL for the rivet binary (matches the connection this helper uses).
    pub fn url(port: u16, db: &str) -> String {
        format!("mongodb://127.0.0.1:{port}/{db}?directConnection=true")
    }

    /// Server major version via `buildInfo` (4, 5, 6, 7, 8); `0` if unavailable.
    /// Used to skip version-gated tests — e.g. `read_concern: snapshot` needs 5.0+,
    /// so on 4.4 the snapshot test self-skips rather than failing the matrix.
    pub fn server_major(&self) -> u32 {
        self.rt.block_on(async {
            self.client
                .database("admin")
                .run_command(doc! { "buildInfo": 1 })
                .await
                .ok()
                .and_then(|d| d.get_str("version").ok().map(str::to_string))
                .and_then(|v| v.split('.').next().and_then(|s| s.parse().ok()))
                .unwrap_or(0)
        })
    }

    fn coll(&self, name: &str) -> Collection<Document> {
        self.client.database(&self.db).collection(name)
    }

    pub fn drop_collection(&self, name: &str) {
        self.rt.block_on(async {
            let _ = self.coll(name).drop().await;
        });
    }

    pub fn insert_many(&self, name: &str, docs: Vec<Document>) {
        self.rt.block_on(async {
            self.coll(name)
                .insert_many(docs)
                .await
                .expect("mongo: insert_many");
        });
    }

    /// Fresh collection of `n` docs `{_id: 1..=n, v: "row<i>"}` (integer `_id`).
    pub fn seed_int_id(&self, name: &str, n: i64) {
        self.drop_collection(name);
        let docs = (1..=n)
            .map(|i| doc! { "_id": i, "v": format!("row{i}") })
            .collect();
        self.insert_many(name, docs);
    }

    /// Fresh collection of `n` docs with string `_id` `sku-00001`…
    pub fn seed_string_id(&self, name: &str, n: i64) {
        self.drop_collection(name);
        let docs = (1..=n)
            .map(|i| doc! { "_id": format!("sku-{i:05}"), "v": i })
            .collect();
        self.insert_many(name, docs);
    }

    /// Fresh collection of `n` docs with auto ObjectId `_id`.
    pub fn seed_objectid(&self, name: &str, n: i64) {
        self.drop_collection(name);
        let docs = (0..n)
            .map(|i| doc! { "seq": i, "v": format!("v{i}") })
            .collect();
        self.insert_many(name, docs);
    }

    pub fn upsert_set(&self, name: &str, id: i64, field: &str, val: &str) {
        self.rt.block_on(async {
            self.coll(name)
                .update_one(doc! { "_id": id }, doc! { "$set": { field: val } })
                .upsert(true)
                .await
                .expect("mongo: upsert");
        });
    }

    pub fn delete_one(&self, name: &str, id: i64) {
        self.rt.block_on(async {
            self.coll(name)
                .delete_one(doc! { "_id": id })
                .await
                .expect("mongo: delete_one");
        });
    }

    /// Apply `(id, field, value)` ops inside ONE transaction — the same `_id`
    /// touched twice is exactly the intra-transaction CDC-ordering case.
    pub fn txn_updates(&self, name: &str, ops: &[(i64, &str, &str)]) {
        self.rt.block_on(async {
            let mut s = self
                .client
                .start_session()
                .await
                .expect("mongo: start_session");
            s.start_transaction()
                .await
                .expect("mongo: start_transaction");
            let c = self.coll(name);
            for (id, field, val) in ops {
                c.update_one(doc! { "_id": id }, doc! { "$set": { *field: *val } })
                    .upsert(true)
                    .session(&mut s)
                    .await
                    .expect("mongo: txn update");
            }
            s.commit_transaction().await.expect("mongo: commit");
        });
    }

    /// One transaction upserting into TWO collections — the boundary ends on the
    /// second (the "uncaptured" one). Exercises a CDC config that routes only the
    /// first collection while a transaction spans both.
    pub fn txn_two_collections(&self, a: &str, a_id: i64, b: &str, b_id: i64) {
        self.rt.block_on(async {
            let mut s = self
                .client
                .start_session()
                .await
                .expect("mongo: start_session");
            s.start_transaction()
                .await
                .expect("mongo: start_transaction");
            self.coll(a)
                .update_one(doc! { "_id": a_id }, doc! { "$set": { "v": "in_txn" } })
                .upsert(true)
                .session(&mut s)
                .await
                .expect("mongo: txn coll a");
            self.coll(b)
                .update_one(doc! { "_id": b_id }, doc! { "$set": { "v": "in_txn" } })
                .upsert(true)
                .session(&mut s)
                .await
                .expect("mongo: txn coll b");
            s.commit_transaction().await.expect("mongo: commit");
        });
    }

    pub fn count(&self, name: &str) -> u64 {
        self.rt.block_on(async {
            self.coll(name)
                .count_documents(doc! {})
                .await
                .expect("mongo: count")
        })
    }

    /// Current `_id → field` state (integer `_id`) — the oracle a dedup of the
    /// captured CDC change log must reproduce exactly.
    pub fn current_state_i64(&self, name: &str, field: &str) -> BTreeMap<i64, String> {
        self.rt.block_on(async {
            let mut cur = self.coll(name).find(doc! {}).await.expect("mongo: find");
            let mut out = BTreeMap::new();
            while let Some(d) = cur.try_next().await.expect("mongo: cursor") {
                let id = match d.get("_id") {
                    Some(Bson::Int64(i)) => *i,
                    Some(Bson::Int32(i)) => *i as i64,
                    _ => continue,
                };
                let v = match d.get(field) {
                    Some(Bson::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => continue,
                };
                out.insert(id, v);
            }
            out
        })
    }
}
