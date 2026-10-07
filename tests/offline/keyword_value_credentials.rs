//! A password written in a keyword/value connection string (libpq DSN, ADO, `;`-separated
//! JDBC properties) reaches no sink: stdout, stderr, `--json-errors`, the state DB,
//! `plan.json`, the run reports. Every command here fails before a server answers, so
//! no stand is needed; the succeeding path is `tests/live/sec_keyword_value_credentials.rs`.

use std::path::Path;

/// Mixed case on purpose: the state key is lower-cased, so the search ignores case.
const SECRET: &str = "Kv9Zq4SecretP40x";

/// `RIVET_BIN_OVERRIDE` grades another binary (a previous release) with the same cells.
fn rivet_bin() -> String {
    match std::env::var("RIVET_BIN_OVERRIDE") {
        Ok(p) if !p.is_empty() => {
            assert!(
                Path::new(&p).is_file(),
                "RIVET_BIN_OVERRIDE={p} is not a file"
            );
            p
        }
        _ => env!("CARGO_BIN_EXE_rivet").to_string(),
    }
}

/// The non-URL forms, by name. Port 1 refuses at once, so nothing waits on a timeout.
fn forms() -> Vec<(&'static str, String)> {
    vec![
        (
            "libpq",
            format!("host=127.0.0.1 port=1 user=u password={SECRET} dbname=d"),
        ),
        (
            "libpq_quoted",
            format!("host=127.0.0.1 port=1 user=u password='two {SECRET} words' dbname=d"),
        ),
        (
            "jdbc_properties",
            format!("sqlserver://127.0.0.1:1;databaseName=app;user=sa;password={SECRET}"),
        ),
        (
            "jdbc_properties_at_sign",
            format!(
                "sqlserver://127.0.0.1:1;databaseName=app;user=sa;password=p@{SECRET};encrypt=true"
            ),
        ),
        (
            "ado_password",
            format!("Server=127.0.0.1,1;Database=d;User Id=u;Password={SECRET};"),
        ),
        (
            "ado_pwd",
            format!("Server=127.0.0.1,1;Database=d;UID=u;PWD={SECRET};Encrypt=true"),
        ),
        (
            "ado_spaced_password",
            format!("Server=127.0.0.1,1;Database=d;User Id=u;Password=two {SECRET} words"),
        ),
    ]
}

/// One export in the mode that persists the most for the engine (MongoDB has no incremental).
fn config(engine: &str, url: &str) -> String {
    let shape = match engine {
        "mongo" => "    table: t\n    mode: full\n",
        _ => "    query: \"SELECT id FROM t\"\n    mode: incremental\n    cursor_column: id\n",
    };
    format!(
        "source:\n  type: {engine}\n  url: \"{url}\"\n  tls: {{ mode: disable }}\n\
         exports:\n  - name: t\n{shape}    format: parquet\n\
         \x20   destination: {{ type: local, path: ./out }}\n"
    )
}

fn holds_secret(text: &str) -> bool {
    text.to_lowercase().contains(&SECRET.to_lowercase())
}

/// Every row of every table of a SQLite state DB, as text, by table.
fn state_db_tables(db: &Path) -> Vec<(String, String)> {
    let conn = rusqlite::Connection::open(db).expect("open state db");
    let names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    names
        .into_iter()
        .map(|table| {
            let mut stmt = conn.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
            let cols = stmt.column_count();
            let mut text = String::new();
            let mut rows = stmt.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                for c in 0..cols {
                    text.push_str(&format!("{:?}\t", row.get_ref(c).unwrap()));
                    if let Ok(s) = row.get::<_, String>(c) {
                        text.push_str(&s);
                    }
                }
                text.push('\n');
            }
            (table, text)
        })
        .collect()
}

/// Every file under `dir` except the config (which holds the secret by definition), as (sink name, text).
fn files_under(dir: &Path, root: &Path, out: &mut Vec<(String, String)>) {
    for entry in std::fs::read_dir(dir).unwrap().map(Result::unwrap) {
        let path = entry.path();
        let rel = path.strip_prefix(root).unwrap().display().to_string();
        if path.is_dir() {
            files_under(&path, root, out);
        } else if rel == "c.yaml" {
            continue;
        } else if rel.ends_with(".db") {
            for (table, text) in state_db_tables(&path) {
                out.push((format!("{rel}:{table}"), text));
            }
        } else {
            out.push((
                rel,
                String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned(),
            ));
        }
    }
}

/// Run every command that can print or persist the connection string; return the sinks that held the secret and the sinks that were populated.
fn leaks_of(engine: &str, form: &str, url: &str) -> (Vec<String>, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("c.yaml"), config(engine, url)).unwrap();
    let mut sinks: Vec<(String, String)> = Vec::new();
    let commands: [(&str, Vec<&str>); 9] = [
        ("check", vec!["check", "-c", "c.yaml"]),
        (
            "check --json-errors",
            vec!["check", "-c", "c.yaml", "--json-errors"],
        ),
        ("doctor", vec!["doctor", "-c", "c.yaml"]),
        ("doctor --json", vec!["doctor", "-c", "c.yaml", "--json"]),
        (
            "plan",
            vec![
                "plan",
                "-c",
                "c.yaml",
                "--format",
                "json",
                "-o",
                "plan.json",
            ],
        ),
        ("run", vec!["run", "-c", "c.yaml"]),
        (
            "run --json-errors",
            vec!["run", "-c", "c.yaml", "--json-errors"],
        ),
        ("metrics", vec!["metrics", "-c", "c.yaml"]),
        (
            "init",
            vec!["init", "--source", url, "--tls", "disable", "--json-errors"],
        ),
    ];
    for (name, args) in commands {
        let out = std::process::Command::new(rivet_bin())
            .args(&args)
            .current_dir(dir.path())
            .env("NO_COLOR", "1")
            .env_remove("RIVET_STATE_URL")
            .output()
            .expect("spawn rivet");
        sinks.push((
            format!("{name}: stdout"),
            String::from_utf8_lossy(&out.stdout).into_owned(),
        ));
        sinks.push((
            format!("{name}: stderr"),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    files_under(dir.path(), dir.path(), &mut sinks);
    let cell = |sink: &str| format!("{engine}/{form}: {sink}");
    let leaked = sinks
        .iter()
        .filter(|(_, text)| holds_secret(text))
        .map(|(sink, _)| cell(sink))
        .collect();
    let populated = sinks
        .iter()
        .filter(|(_, text)| !text.trim().is_empty())
        .map(|(sink, _)| sink.clone())
        .collect();
    (leaked, populated)
}

#[test]
fn a_keyword_value_password_reaches_no_sink_on_any_engine() {
    let mut leaked = Vec::new();
    for engine in ["postgres", "mysql", "mssql", "mongo", "oracle"] {
        for (form, url) in forms() {
            let (cell_leaks, populated) = leaks_of(engine, form, &url);
            for sink in [
                "run: stderr",
                "doctor: stdout",
                "doctor --json: stdout",
                "run --json-errors: stderr",
                "plan.json",
                ".rivet_state.db:export_metrics",
                ".rivet_state.db:run_journal",
            ] {
                assert!(
                    populated.iter().any(|p| p == sink),
                    "{engine}/{form}: the sink `{sink}` was never written, so the cell graded nothing; \
                     populated: {populated:?}"
                );
            }
            assert!(
                populated
                    .iter()
                    .any(|p| p.starts_with(".rivet/runs/") && p.ends_with("summary.json")),
                "{engine}/{form}: no run report was written; populated: {populated:?}"
            );
            leaked.extend(cell_leaks);
        }
    }
    assert!(
        leaked.is_empty(),
        "KEYWORD-VALUE PASSWORD LEAKED into {} sink(s):\n  {}",
        leaked.len(),
        leaked.join("\n  ")
    );
}
