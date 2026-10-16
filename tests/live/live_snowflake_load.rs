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

/// A full load onto a free name lands the source; a later full load whose COPY fails keeps the table the last load landed and leaves no other table behind; the next cycle lands the source again.
fn a_full_load_that_fails_keeps_the_last_table(e: SqlEngine) {
    let Some(sf) = SfLive::from_env("sf_keep") else {
        return;
    };
    e.alive();
    let (table, _guard) = e.create(
        "sf_keep",
        &format!("id {0} NOT NULL PRIMARY KEY, v {0} NOT NULL", e.int64()),
    );
    for id in 1..=20 {
        e.exec(&format!(
            "INSERT INTO {table} (id, v) VALUES ({id}, {})",
            id * 10
        ));
    }
    let _cleanup = sf.cleanup(&[&table]);
    let rig = e
        .rig(&table)
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(""))
        .a_failed_run_may_leave(
            &[Leftover::LoadAttempt],
            "the COPY of the corrupted parts fails after the load began writing",
        );
    rig.run_ok();
    load_ok(&rig);
    let first = e.id_v_pairs(&table);
    assert_eq!(
        sf.id_v_pairs(&table),
        first,
        "the first load lands the source"
    );
    assert_eq!(sf.names_from(&table), [table.to_uppercase()]);
    sf.exec(&format!(
        "GRANT SELECT ON TABLE {} TO ROLE PUBLIC",
        sf.fq(&table)
    ));

    e.exec(&format!("DELETE FROM {table} WHERE id = 3"));
    e.exec(&format!("UPDATE {table} SET v = 41 WHERE id = 4"));
    e.exec(&format!("INSERT INTO {table} (id, v) VALUES (21, 210)"));
    let before = sf.gcs_objects();
    rig.run_ok();
    let parts: Vec<String> = sf
        .gcs_objects()
        .into_iter()
        .filter(|o| o.ends_with(".parquet") && !before.contains(o))
        .collect();
    assert!(!parts.is_empty(), "the second run wrote parts");
    for part in &parts {
        sf.corrupt(part);
    }
    let out = rig.cli(&["load"]);
    let said = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        !out.status.success(),
        "a load of unreadable parts fails: {said}"
    );
    assert_eq!(
        sf.id_v_pairs(&table),
        first,
        "the failed load keeps the rows the last load landed: {said}"
    );
    assert_eq!(
        sf.names_from(&table),
        [table.to_uppercase()],
        "the failed load leaves no other table behind"
    );

    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        sf.id_v_pairs(&table),
        e.id_v_pairs(&table),
        "the next cycle lands the source"
    );
    assert_eq!(sf.names_from(&table), [table.to_uppercase()]);
    assert!(
        !sf.grants(&table).iter().any(|g| g.ends_with(" PUBLIC")),
        "a full load lands a new table, so a grant on the old one does not carry over: {:?}",
        sf.grants(&table)
    );
}

/// An incremental export first lands as a table; its first delta turns the name into a view over `<table>__changes`, and a whole pass after `state reset` joins the log.
fn an_incremental_load_starts_a_table_then_a_change_log(e: SqlEngine) {
    let Some(sf) = SfLive::from_env("sf_inc") else {
        return;
    };
    e.alive();
    let (table, _guard) = e.table("sf_inc");
    let changes = format!("{table}__changes");
    let _cleanup = sf.cleanup(&[&table]);
    e.insert(&table, 1..=10, 10, Some(1));
    let rig = e
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(""));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(sf.kind(&table).as_deref(), Some("BASE TABLE"));
    assert_eq!(sf.kind(&changes), None, "no change log yet");
    assert_eq!(sf.counts(&table, "id"), (10, 10));

    e.insert(&table, 11..=13, 5, Some(2));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(sf.kind(&table).as_deref(), Some("VIEW"));
    assert_eq!(
        sf.counts(&changes, "id"),
        (13, 13),
        "no row is loaded twice"
    );
    assert_eq!(sf.counts(&table, "id"), (13, 13));

    let reset = rig.cli(&["state", "reset", "--export", &table]);
    assert!(reset.status.success());
    e.insert(&table, 14..=15, 5, Some(3));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(sf.kind(&table).as_deref(), Some("VIEW"));
    assert_eq!(
        sf.counts(&changes, "id"),
        (28, 15),
        "13 + the 15-row whole pass"
    );
    assert_eq!(
        sf.counts(&table, "id"),
        (15, 15),
        "the view is the current state"
    );

    // `rivet compact` has nothing to merge on Snowflake: it skips by name on every attempt, exit 0, and leaves the log and the view as they are.
    let said: Vec<String> = (0..2)
        .map(|_| {
            let out = rig.cli(&["compact"]);
            format!(
                "exit {:?}\n{}{}",
                out.status.code(),
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            )
        })
        .collect();
    assert_eq!(said[0], said[1], "the second attempt answers as the first");
    assert!(said[0].starts_with("exit Some(0)"), "{}", said[0]);
    assert!(
        said[0].contains("skipped") && said[0].contains("never compacts"),
        "{}",
        said[0]
    );
    assert_eq!(sf.kind(&table).as_deref(), Some("VIEW"));
    assert_eq!(sf.counts(&changes, "id"), (28, 15));
}

/// Every column of `columns` loads with the source's value: `rows` are the VALUES tuples, the last one all NULL but the key.
fn every_type_loads_with_its_value(e: SqlEngine, columns: &str, rows: &[&str]) {
    let Some(sf) = SfLive::from_env("sf_types") else {
        return;
    };
    e.alive();
    let (table, _guard) = e.create("sf_types", columns);
    for row in rows {
        e.exec(&format!("INSERT INTO {table} VALUES ({row})"));
    }
    let _cleanup = sf.cleanup(&[&table]);
    let rig = e
        .rig(&table)
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(""));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        sf.counts(&table, "id"),
        (rows.len() as i64, rows.len() as i64)
    );
}

/// A table this state has no record of loading is refused on every attempt, by the same code, and keeps its rows.
fn a_load_refuses_a_table_it_did_not_load_on_every_attempt(e: SqlEngine) {
    let Some(sf) = SfLive::from_env("sf_foreign") else {
        return;
    };
    e.alive();
    let (table, _guard) = e.table("sf_foreign");
    let _cleanup = sf.cleanup(&[&table]);
    e.insert(&table, 1..=3, 10, Some(1));
    sf.exec(&format!("CREATE TABLE {} (id NUMBER)", sf.fq(&table)));
    sf.exec(&format!("INSERT INTO {} VALUES (77)", sf.fq(&table)));
    let rig = e
        .rig(&table)
        .dest_gcs_live(&sf.bucket, &sf.prefix)
        .top_line(&sf.load_line(""));
    rig.run_ok();
    for attempt in 1..=2 {
        let out = rig.cli(&["load"]);
        let said = String::from_utf8_lossy(&out.stderr).to_string();
        assert_eq!(out.status.code(), Some(5), "attempt {attempt}: {said}");
        assert!(
            said.contains("RIVET_LOAD_TARGET_NOT_RIVETS")
                && said.contains("no record of rivet loading it"),
            "attempt {attempt}: {said}"
        );
        assert_eq!(sf.kind(&table).as_deref(), Some("BASE TABLE"));
        assert_eq!(
            sf.counts(&table, "id"),
            (1, 1),
            "attempt {attempt}: the foreign row stays"
        );
    }
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

#[test]
#[ignore = "live: requires docker compose up -d postgres + Snowflake creds"]
fn snowflake_full_load_that_fails_keeps_the_last_table_postgres() {
    a_full_load_that_fails_keeps_the_last_table(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql + Snowflake creds"]
fn snowflake_full_load_that_fails_keeps_the_last_table_mysql() {
    a_full_load_that_fails_keeps_the_last_table(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + Snowflake creds"]
fn snowflake_incremental_load_starts_a_table_then_a_change_log_postgres() {
    an_incremental_load_starts_a_table_then_a_change_log(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql + Snowflake creds"]
fn snowflake_incremental_load_starts_a_table_then_a_change_log_mysql() {
    an_incremental_load_starts_a_table_then_a_change_log(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + Snowflake creds"]
fn snowflake_every_type_loads_with_its_value_postgres() {
    every_type_loads_with_its_value(
        SqlEngine::Pg,
        "id BIGINT PRIMARY KEY, n NUMERIC(18,4), big NUMERIC(38,10), i INT, f DOUBLE PRECISION, b BOOLEAN, \
         d DATE, ts TIMESTAMP, tz TIMESTAMPTZ, t TIME, txt TEXT, bin BYTEA, u UUID, j JSONB",
        &[
            "1, 12345678901234.5678, 1234567890123456789012345678.0123456789, 2147483647, 1.5e300, true, \
             '2024-02-29', '2024-02-29 23:59:59.123456', '2024-02-29 23:59:59.123456+02', '13:45:07.123456', \
             E'caf\u{e9} \"q\" \\\\ tab\\there ''s', '\\x00ff10', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"k\": [1, 2]}'",
            "2, -0.0001, -0.0000000001, -2147483648, -0.0, false, '0001-01-01', '1970-01-01 00:00:00', \
             '1970-01-01 00:00:00+00', '00:00:00', '', '\\x', '00000000-0000-0000-0000-000000000000', '[]'",
            "3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql + Snowflake creds"]
fn snowflake_every_type_loads_with_its_value_mysql() {
    every_type_loads_with_its_value(
        SqlEngine::Mysql,
        "id BIGINT PRIMARY KEY, n DECIMAL(18,4), big DECIMAL(38,10), i INT, uq BIGINT UNSIGNED, f DOUBLE, \
         d DATE, ts DATETIME(6), tz TIMESTAMP(6) NULL, t TIME(6), txt TEXT, bin VARBINARY(16), j JSON",
        &[
            "1, 12345678901234.5678, 1234567890123456789012345678.0123456789, 2147483647, 18446744073709551615, 1.5e300, \
             '2024-02-29', '2024-02-29 23:59:59.123456', '2024-02-29 23:59:59.123456', '13:45:07.123456', \
             'caf\u{e9} \"q\" \\\\ tab\\there ''s', x'00ff10', '{\"k\": [1, 2]}'",
            "2, -0.0001, -0.0000000001, -2147483648, 0, -0.0, '1000-01-01', '1970-01-01 00:00:01', \
             '1970-01-01 00:00:01', '00:00:00', '', x'', '[]'",
            "3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + Snowflake creds"]
fn snowflake_load_refuses_a_table_it_did_not_load_on_every_attempt_postgres() {
    a_load_refuses_a_table_it_did_not_load_on_every_attempt(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql + Snowflake creds"]
fn snowflake_load_refuses_a_table_it_did_not_load_on_every_attempt_mysql() {
    a_load_refuses_a_table_it_did_not_load_on_every_attempt(SqlEngine::Mysql);
}
