//! The succeeding path of `tests/offline/keyword_value_credentials.rs`: a libpq
//! keyword/value string authenticates, exports every row, advances its cursor, and its
//! password reaches no sink — stdout, stderr, the state DB, `plan.json`, the run reports.

use crate::common::*;
use std::path::Path;

/// The two halves of every password here, lower-cased: the state key is lower-cased, and a
/// quoted password holds a space between them.
const NEEDLES: [&str; 2] = ["kv9zq4live", "secretp40x"];

/// A login role with its own password and SELECT on one table; dropped on `Drop`.
struct RoleWithPassword(String);

impl RoleWithPassword {
    fn create(password: &str, table: &str) -> Self {
        let role = unique_name("rivet_kv");
        pg_connect()
            .batch_execute(&format!(
                "DROP ROLE IF EXISTS {role};
                 CREATE ROLE {role} LOGIN PASSWORD '{password}';
                 GRANT SELECT ON {table} TO {role};"
            ))
            .expect("create role");
        Self(role)
    }
}

impl Drop for RoleWithPassword {
    fn drop(&mut self) {
        let _ = pg_connect().batch_execute(&format!(
            "DROP OWNED BY {0}; DROP ROLE IF EXISTS {0};",
            self.0
        ));
    }
}

fn holds_password(text: &str) -> bool {
    let lower = text.to_lowercase();
    NEEDLES.iter().any(|n| lower.contains(n))
}

/// Every state table that holds the password, from the backend rivet used.
fn state_tables_holding_the_password(cfg: &Path) -> Vec<String> {
    if let Some(url) = state_url_under_test() {
        let mut c = postgres::Client::connect(&url, postgres::NoTls).expect("state backend");
        let tables: Vec<String> = c
            .query(
                "SELECT table_name::text FROM information_schema.tables \
                 WHERE table_schema = current_schema()",
                &[],
            )
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        return tables
            .into_iter()
            .filter(|t| {
                NEEDLES.iter().any(|n| {
                    c.query_one(
                        &format!("SELECT count(*) FROM \"{t}\" x WHERE lower(x::text) LIKE $1"),
                        &[&format!("%{n}%")],
                    )
                    .unwrap()
                    .get::<_, i64>(0)
                        > 0
                })
            })
            .collect();
    }
    let conn = rusqlite::Connection::open(cfg.parent().unwrap().join(".rivet_state.db"))
        .expect("open state db");
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    tables
        .into_iter()
        .filter(|t| {
            let mut stmt = conn.prepare(&format!("SELECT * FROM \"{t}\"")).unwrap();
            let cols = stmt.column_count();
            let mut rows = stmt.query([]).unwrap();
            let mut found = false;
            while let Some(row) = rows.next().unwrap() {
                for c in 0..cols {
                    found |= row.get::<_, String>(c).is_ok_and(|s| holds_password(&s));
                }
            }
            found
        })
        .collect()
}

/// Every file under `dir` that holds the password, the config and the state DB aside.
fn files_holding_the_password(dir: &Path, root: &Path, out: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap().map(Result::unwrap) {
        let path = entry.path();
        let rel = path.strip_prefix(root).unwrap().display().to_string();
        if path.is_dir() {
            files_holding_the_password(&path, root, out);
        } else if rel != "rig.yaml"
            && !rel.contains(".rivet_state.db")
            && holds_password(&String::from_utf8_lossy(&std::fs::read(&path).unwrap()))
        {
            out.push(rel);
        }
    }
}

#[test]
#[ignore = "live: postgres"]
fn a_libpq_keyword_value_string_authenticates_and_its_password_reaches_no_sink() {
    require_alive(LiveService::Postgres);
    const ROWS: i64 = 40;
    for (form, password, quote) in [
        ("unquoted", "Kv9Zq4LiveSecretP40x", ""),
        ("quoted with a space", "Kv9Zq4Live SecretP40x", "'"),
    ] {
        let table = seed_pg_numeric_table(ROWS);
        let role = RoleWithPassword::create(password, table.name());
        let export = unique_name("kv_dsn");
        let rig = Rig::pg_batch(&export)
            .source_url(&format!(
                "host=127.0.0.1 port=5432 user={} password={quote}{password}{quote} dbname=rivet",
                role.0
            ))
            .source_line("tls: { mode: disable }")
            .query(&format!("SELECT id, name FROM {}", table.name()))
            .mode("incremental")
            .export_line("cursor_column: id");
        let cfg = rig.config_path();
        let root = cfg.parent().unwrap().to_path_buf();
        let plan = root.join("plan.json");

        let mut said: Vec<(&str, std::process::Output)> = vec![("run", rig.run_args(&[]))];
        assert!(
            said[0].1.status.success(),
            "{form}: the string must authenticate and export:\n{}",
            String::from_utf8_lossy(&said[0].1.stderr)
        );
        assert_eq!(
            dir_parquet_id_set(&rig.out_dir()),
            (0..ROWS).collect(),
            "{form}: every source row is delivered"
        );
        said.push(("run again", rig.run_args(&[])));
        said.push(("check", rig.cli(&["check"])));
        said.push(("doctor", rig.cli(&["doctor"])));
        said.push(("doctor --json", rig.cli(&["doctor", "--json"])));
        said.push(("plan", rig.plan_json_env(&plan, &[], &[])));
        said.push(("state show", rig.cli(&["state", "show"])));
        said.push(("metrics", rig.cli(&["metrics"])));
        for (name, out) in &said {
            assert!(
                out.status.success(),
                "{form}: `{name}` must succeed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        assert_eq!(
            dir_parquet_id_set(&rig.out_dir()).len() as i64,
            ROWS,
            "{form}: the second run resumed from the stored cursor and delivered nothing twice"
        );

        let mut leaked: Vec<String> = said
            .iter()
            .flat_map(|(name, out)| {
                [("stdout", &out.stdout), ("stderr", &out.stderr)]
                    .into_iter()
                    .filter(|(_, bytes)| holds_password(&String::from_utf8_lossy(bytes)))
                    .map(move |(stream, _)| format!("{name}: {stream}"))
            })
            .collect();
        files_holding_the_password(&root, &root, &mut leaked);
        leaked.extend(
            state_tables_holding_the_password(&cfg)
                .into_iter()
                .map(|t| format!("state DB: {t}")),
        );
        assert!(
            leaked.is_empty(),
            "KEYWORD-VALUE PASSWORD LEAKED ({form}) into {} sink(s):\n  {}",
            leaked.len(),
            leaked.join("\n  ")
        );
        let plan_text = std::fs::read_to_string(&plan).expect("plan.json was written");
        assert!(
            plan_text.contains("password=***"),
            "{form}: the plan keeps the string with its password masked:\n{plan_text}"
        );
    }
}
