//! `rivet load` into Snowflake, graded by the rig's DuckDB reader: the loader drives `snow`, the reader an ADBC driver.

use crate::common::*;

/// `rivet load` must succeed.
fn load_ok(rig: &Rig) {
    let out = rig.cli(&["load"]);
    assert!(
        out.status.success(),
        "rivet load failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A full export loads onto a free name, and the next cycle replaces the table rivet itself loaded with the source as it is then.
fn a_full_load_lands_and_the_next_cycle_follows_the_source(e: SqlEngine) {
    let Some(sf) = SfLive::from_env("sf_full") else {
        return;
    };
    e.alive();
    let (table, _guard) = e.create(
        "sf_full",
        "a_key INT NOT NULL, b_key INT NOT NULL, v VARCHAR(20) NULL, PRIMARY KEY (b_key, a_key)",
    );
    for g in 1..=40 {
        e.exec(&format!(
            "INSERT INTO {table} (a_key, b_key, v) VALUES ({g}, {}, 'v{g}')",
            g % 7
        ));
    }
    let _cleanup = sf.cleanup(&[&table]);
    let rig = e
        .rig(&table)
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(""));
    rig.run_ok();
    load_ok(&rig);

    e.exec(&format!("DELETE FROM {table} WHERE a_key = 7"));
    e.exec(&format!("UPDATE {table} SET v = NULL WHERE a_key = 8"));
    e.exec(&format!(
        "INSERT INTO {table} (a_key, b_key, v) VALUES (41, 6, 'late')"
    ));
    rig.run_ok();
    load_ok(&rig);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + Snowflake creds"]
fn snowflake_full_load_lands_and_the_next_cycle_follows_the_source_postgres() {
    a_full_load_lands_and_the_next_cycle_follows_the_source(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql + Snowflake creds"]
fn snowflake_full_load_lands_and_the_next_cycle_follows_the_source_mysql() {
    a_full_load_lands_and_the_next_cycle_follows_the_source(SqlEngine::Mysql);
}
