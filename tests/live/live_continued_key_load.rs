//! A run that continues past its last key (Mongo `source.mongo.resume`) loads into BigQuery by append.

use crate::common::*;

const MONGO_PORT: u16 = 27017;

/// A Mongo `resume` export's BigQuery load appends each run's new documents, never replaces the table.
#[test]
#[ignore = "live: requires mongo + BigQuery creds"]
fn a_mongo_resume_export_into_bigquery_accumulates_every_run() {
    let Some(bq) = BqLive::from_env("bqmresume") else {
        return;
    };
    require_alive(LiveService::Mongo);
    let mdb = unique_name("bqmresume");
    let m = MongoTest::connect(MONGO_PORT, &mdb);
    m.seed_objectid("t", 2000);
    let rig = Rig::mongo_batch("t")
        .source_url(&MongoTest::url(MONGO_PORT, &mdb))
        .mongo("page_size: 500, resume: true")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", pk: auto"));
    let bq_ids = |bq: &BqLive| -> Vec<String> {
        bq.read_bq_rows(&format!(
            "SELECT _id FROM `{}.{}.t` ORDER BY _id",
            bq.project, bq.dataset
        ))
        .iter()
        .map(|r| r["_id"].as_str().expect("a string _id").to_string())
        .collect()
    };

    rig.run_ok();
    rig.load_ok(&[], &[]);
    assert_eq!(
        bq_ids(&bq),
        m.ids("t"),
        "the first run loads the whole collection"
    );

    m.append_objectid("t", 500);
    rig.run_ok();
    rig.load_ok(&[], &[]);
    assert_eq!(
        bq_ids(&bq).len(),
        2500,
        "the resumed run's 500 new documents join the first 2000, not replace them"
    );
    assert_eq!(bq_ids(&bq), m.ids("t"));
}
