//! REFUSAL — the default oracle's grade for an invocation that did NOT exit 0 (tests/common/verify.rs
//! `settle`). The destination trees, the CDC checkpoint files and the state DB are fingerprinted
//! before the invocation and re-read after it: nothing may have changed except the failure's own
//! record (a journal row with a non-success status; a manifest with a non-success status and the
//! `_SUCCESS` marker it withdraws) and what the test declared with a typed [`Leftover`]
//! (`Rig::a_failed_run_may_leave`, or `RIVET_TEST_FAILED_RUN_LEAVES` on a raw run; both counted by
//! a shrink-only ceiling). A product defect is excused only as a known defect: one rig's
//! (`Rig::oracle_known_defect("a failed run left: <kind>", ..)`) or one every failed run shows
//! ([`KNOWN_PRODUCT_DEFECTS`], a pinned list); both log `RIVET-ORACLE-XFAIL`. A run the test crashed itself
//! (a fault-hook env, a signal) is not a refusal: it is logged `RIVET-ORACLE-UNGRADED` and its
//! resume is what the oracle grades.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The env a raw run sets to the comma-separated [`Leftover`] kinds its failure may leave.
pub const FAILED_RUN_LEAVES_ENV: &str = "RIVET_TEST_FAILED_RUN_LEAVES";

/// The fault hooks (src/test_hook.rs) that make an invocation fail where the test said to.
const FAULT_ENVS: &[&str] = &["RIVET_TEST_PANIC_AT", "RIVET_TEST_ERROR_AT"];

/// Product defects every failed run may show until the product is fixed: the kind and why (pinned by tests/offline/rig_oracle_ratchet.rs; the list only shrinks).
pub(crate) const KNOWN_PRODUCT_DEFECTS: &[(Leftover, &str)] = &[(
    Leftover::ObservedSchema,
    "known defect: a first run that fails stores the drift baseline (src/state/schema.rs `detect_schema_change`) in export_schema, which src/state/migrations.rs v18 documents as success-only",
)];

/// The kinds a failed run may leave with no declaration: its own record of the stop.
const OWN_RECORD: &[Leftover] = &[Leftover::FailureRecord, Leftover::FailedManifest];

/// State tables whose rows record an outcome in a `status` column.
const JOURNAL: &[&str] = &["export_metrics", "run_status", "load_run"];

/// State tables that hold measurements or bookkeeping, never a resume point.
const TELEMETRY: &[&str] = &[
    "run_journal",
    "run_aggregate",
    "export_harm",
    "export_shape",
    "strategy_snapshot",
    "state_lease",
    "schema_version",
    "rivet_schema_version",
];

/// The kind a changed row of a non-journal state table is; a table not named here is a resume point.
fn kind_of_table(table: &str) -> Leftover {
    match table {
        "file_log" => Leftover::FileLog,
        "export_schema" => Leftover::ObservedSchema,
        "chunk_run" | "chunk_task" | "keyset_range" => Leftover::ChunkCheckpoint,
        _ => Leftover::ResumePoint,
    }
}

/// The statuses a journal row may record for a run that did not succeed.
const NOT_SUCCESS: &[&str] = &["failed", "refused", "interrupted", "skipped"];

/// More files than this under one destination is not a test fixture: the tree is not fingerprinted.
const MAX_FILES: usize = 20_000;

/// A file larger than this is fingerprinted by its length and mtime, not its bytes.
const MAX_HASHED_BYTES: u64 = 64 << 20;

/// One kind of thing a run that did not exit 0 can leave behind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Leftover {
    /// A journal row recording the stop with a non-success status (allowed by default).
    FailureRecord,
    /// The destination's record of the stop: a manifest with a non-success status, and the `_SUCCESS` it withdraws (allowed by default).
    FailedManifest,
    /// A file added to the destination that is neither a marker nor a manifest.
    OrphanPart,
    /// A CDC checkpoint file added or rewritten.
    CdcCheckpoint,
    /// A per-run manifest with status success that a `mode: cdc` export added: a flush the stream committed before it stopped.
    CdcFlush,
    /// Not a leftover but a declaration: the exit is a verdict on a run that recorded success for the export, whose delivery is then logged ungraded.
    DeliveredRun,
    /// A `file_log` row: the record of a part the run wrote.
    FileLog,
    /// An `export_schema` row for an export that had none: the schema the run observed.
    ObservedSchema,
    /// A chunk or range checkpoint row (`chunk_run`, `chunk_task`, `keyset_range`).
    ChunkCheckpoint,
    /// Any other state row added, and any existing one changed or removed: the cursor, the committed boundary, a stored schema or load spec, a table the oracle does not know.
    ResumePoint,
    /// A `load_run` row with status `failed`: the load reached the warehouse write.
    LoadAttempt,
    /// A file that existed before the run and was rewritten or removed.
    ChangedFile,
    /// A `_SUCCESS` marker added or rewritten.
    SuccessMarker,
    /// A manifest added or rewritten with status success.
    SuccessManifest,
    /// A journal row with status success.
    SuccessRecord,
    /// A journal row whose status the oracle does not know.
    UnknownStatus,
}

impl Leftover {
    /// Every kind, in report order.
    pub const ALL: [Leftover; 16] = [
        Leftover::FailureRecord,
        Leftover::FailedManifest,
        Leftover::OrphanPart,
        Leftover::CdcCheckpoint,
        Leftover::CdcFlush,
        Leftover::DeliveredRun,
        Leftover::FileLog,
        Leftover::ObservedSchema,
        Leftover::ChunkCheckpoint,
        Leftover::ResumePoint,
        Leftover::LoadAttempt,
        Leftover::ChangedFile,
        Leftover::SuccessMarker,
        Leftover::SuccessManifest,
        Leftover::SuccessRecord,
        Leftover::UnknownStatus,
    ];

    /// The kind's name in verdict lines, the env and known-defect classes.
    pub fn slug(self) -> &'static str {
        match self {
            Leftover::FailureRecord => "failure-record",
            Leftover::OrphanPart => "orphan-part",
            Leftover::FailedManifest => "failed-manifest",
            Leftover::CdcCheckpoint => "cdc-checkpoint",
            Leftover::CdcFlush => "cdc-committed-flush",
            Leftover::DeliveredRun => "delivered-run",
            Leftover::FileLog => "file-log",
            Leftover::ObservedSchema => "observed-schema",
            Leftover::ChunkCheckpoint => "chunk-checkpoint",
            Leftover::ResumePoint => "resume-point",
            Leftover::LoadAttempt => "load-attempt",
            Leftover::ChangedFile => "changed-file",
            Leftover::SuccessMarker => "success-marker",
            Leftover::SuccessManifest => "success-manifest",
            Leftover::SuccessRecord => "success-record",
            Leftover::UnknownStatus => "unknown-status",
        }
    }

    /// The kind a slug names; an unknown slug is a harness error.
    pub fn from_slug(slug: &str) -> Leftover {
        Leftover::ALL
            .into_iter()
            .find(|k| k.slug() == slug)
            .unwrap_or_else(|| {
                panic!(
                    "unknown leftover kind `{slug}`; one of {:?}",
                    Leftover::ALL.map(Leftover::slug)
                )
            })
    }

    /// Whether a test may declare this kind: a failed run never legitimately declares success, and an unread status is never legitimate.
    pub fn declarable(self) -> bool {
        !matches!(
            self,
            Leftover::SuccessMarker
                | Leftover::SuccessManifest
                | Leftover::SuccessRecord
                | Leftover::UnknownStatus
        )
    }

    /// The kind a known-defect class `a failed run left: <slug>` names, else `None` (a class of the success grade).
    pub fn of_known_defect_class(class: &str) -> Option<Leftover> {
        class
            .strip_prefix("a failed run left: ")
            .map(Leftover::from_slug)
    }
}

/// The kinds `RIVET_TEST_FAILED_RUN_LEAVES=<slug>[,<slug>]` declares; an undeclarable or unknown one is a harness error.
pub(crate) fn declared_in_env(raw: &str) -> Vec<Leftover> {
    let kinds: Vec<Leftover> = raw
        .split(',')
        .map(|s| Leftover::from_slug(s.trim()))
        .collect();
    assert_declarable(&kinds);
    kinds
}

/// Panic unless every kind may be declared by a test.
pub(crate) fn assert_declarable(kinds: &[Leftover]) {
    let bad: Vec<&str> = kinds
        .iter()
        .filter(|k| !k.declarable())
        .map(|k| k.slug())
        .collect();
    assert!(
        !kinds.is_empty() && bad.is_empty(),
        "a failed run may be declared to leave only declarable kinds, at least one: {bad:?} can only be a known defect (`oracle_known_defect(\"a failed run left: <kind>\", ..)`)"
    );
}

/// Why this exit is a crash the test caused (a fault hook in its env, a signal), else `None`.
pub(crate) fn crashed_by_the_test(
    status: std::process::ExitStatus,
    envs: &[(&str, &str)],
) -> Option<String> {
    use std::os::unix::process::ExitStatusExt as _;
    if let Some((k, v)) = envs.iter().find(|(k, _)| FAULT_ENVS.contains(k)) {
        return Some(format!("the test injected {k}={v}"));
    }
    status.signal().map(|s| format!("killed by signal {s}"))
}

/// What a file in a destination is, as far as a consumer reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tag {
    Marker,
    SuccessManifest,
    FailedManifest,
    Other,
}

/// One file's fingerprint: length, content hash (length and mtime past [`MAX_HASHED_BYTES`]), and what it is.
type Print = (u64, u64, Tag);

/// Where a run keeps its state.
#[derive(Clone, Debug)]
pub(crate) enum StateAt {
    Sqlite(PathBuf),
    Postgres(String),
}

/// What the oracle fingerprints around one invocation.
pub(crate) struct Scope {
    /// Per export: its name in the state DB and its destination tree (or why it cannot be read).
    pub exports: Vec<(String, Result<PathBuf, String>)>,
    /// Per export, whether it is a CDC stream (it commits flush by flush).
    pub cdc: Vec<bool>,
    /// The CDC checkpoint files the config names.
    pub checkpoints: Vec<PathBuf>,
    /// The state backend, `None` when the invocation keeps none.
    pub state: Option<StateAt>,
}

/// The destination trees, checkpoint files and state rows at one moment.
pub(crate) struct Snapshot {
    /// Per export, in [`Scope::exports`] order.
    trees: Vec<Result<BTreeMap<PathBuf, Print>, String>>,
    checkpoints: BTreeMap<PathBuf, Print>,
    /// Table -> its rows as JSON text; `Err` when the backend could not be read.
    state: Result<BTreeMap<String, BTreeSet<String>>, String>,
    /// State tables that exist and were not compared (shared Postgres state, no `export_name` column).
    unscoped: Vec<String>,
}

impl Snapshot {
    /// Fingerprint everything `scope` names.
    pub(crate) fn take(scope: &Scope) -> Snapshot {
        let names: Vec<String> = scope.exports.iter().map(|(n, _)| n.clone()).collect();
        let (state, unscoped) = match &scope.state {
            None => (Ok(BTreeMap::new()), Vec::new()),
            Some(StateAt::Sqlite(db)) => (sqlite_rows(db), Vec::new()),
            Some(StateAt::Postgres(url)) => match postgres_rows(url, &names) {
                Ok((rows, unscoped)) => (Ok(rows), unscoped),
                Err(why) => (Err(why), Vec::new()),
            },
        };
        Snapshot {
            trees: scope
                .exports
                .iter()
                .map(|(_, root)| root.clone().and_then(|r| tree(&r)))
                .collect(),
            checkpoints: scope
                .checkpoints
                .iter()
                .filter(|p| p.is_file())
                .map(|p| (p.clone(), print(p)))
                .collect(),
            state,
            unscoped,
        }
    }
}

/// Every file under `root` with its fingerprint; the state DB's own files are not destination content.
fn tree(root: &Path) -> Result<BTreeMap<PathBuf, Print>, String> {
    let files = super::runner::files_under(root);
    if files.len() > MAX_FILES {
        return Err(format!(
            "{} holds more than {MAX_FILES} files",
            root.display()
        ));
    }
    Ok(files
        .into_iter()
        .filter(|p| {
            !p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(".rivet_state.db"))
        })
        .map(|p| {
            let fp = print(&p);
            (p, fp)
        })
        .collect())
}

/// One file's fingerprint.
fn print(p: &Path) -> Print {
    use std::hash::{Hash as _, Hasher as _};
    let name = p
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let meta = std::fs::metadata(p).ok();
    let len = meta.as_ref().map_or(0, |m| m.len());
    let mut h = std::hash::DefaultHasher::new();
    let mut tag = if name == "_SUCCESS" {
        Tag::Marker
    } else {
        Tag::Other
    };
    if len > MAX_HASHED_BYTES {
        (len, meta.and_then(|m| m.modified().ok())).hash(&mut h);
    } else {
        let bytes = std::fs::read(p).unwrap_or_default();
        bytes.hash(&mut h);
        if name.starts_with("manifest") && name.ends_with(".json") {
            let status = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|d| d.get("status").and_then(|s| s.as_str()).map(str::to_string));
            tag = match status {
                Some(s) if !s.eq_ignore_ascii_case("success") => Tag::FailedManifest,
                _ => Tag::SuccessManifest,
            };
        }
    }
    (len, h.finish(), tag)
}

/// Every row of every table in the SQLite state DB at `db`; an absent file holds nothing.
fn sqlite_rows(db: &Path) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
    if !db.is_file() {
        return Ok(BTreeMap::new());
    }
    let e = |err: rusqlite::Error| format!("{}: {err}", db.display());
    let conn = rusqlite::Connection::open(db).map_err(e)?;
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        .and_then(|mut st| st.query_map([], |r| r.get(0))?.collect())
        .map_err(e)?;
    let mut out = BTreeMap::new();
    for t in tables {
        let mut st = conn.prepare(&format!("SELECT * FROM \"{t}\"")).map_err(e)?;
        let cols: Vec<String> = st.column_names().iter().map(|c| c.to_string()).collect();
        let rows: BTreeSet<String> = st
            .query_map([], |r| {
                let mut row = serde_json::Map::new();
                for (i, c) in cols.iter().enumerate() {
                    use rusqlite::types::ValueRef as V;
                    let v = match r.get_ref(i)? {
                        V::Null => serde_json::Value::Null,
                        V::Integer(n) => n.into(),
                        V::Real(f) => f.into(),
                        V::Text(t) => String::from_utf8_lossy(t).into_owned().into(),
                        V::Blob(b) => format!("<{} bytes>", b.len()).into(),
                    };
                    row.insert(c.clone(), v);
                }
                Ok(serde_json::Value::Object(row).to_string())
            })
            .and_then(|rows| rows.collect())
            .map_err(e)?;
        out.insert(t, rows);
    }
    Ok(out)
}

/// The rows of `exports` in every table of the Postgres state at `url` that has an `export_name` column, and the tables that have none.
#[allow(clippy::type_complexity)]
fn postgres_rows(
    url: &str,
    exports: &[String],
) -> Result<(BTreeMap<String, BTreeSet<String>>, Vec<String>), String> {
    let e = |err: postgres::Error| format!("the Postgres state: {err}");
    let mut cfg: postgres::Config = url.parse().map_err(e)?;
    // A test may point a run at a state server that never answers: the snapshot must not wait for it.
    cfg.connect_timeout(std::time::Duration::from_secs(5));
    let mut c = cfg.connect(postgres::NoTls).map_err(e)?;
    let tables = c
        .query(
            "SELECT table_name::text, bool_or(column_name = 'export_name') \
             FROM information_schema.columns WHERE table_schema = current_schema() GROUP BY 1",
            &[],
        )
        .map_err(e)?;
    let (mut out, mut unscoped) = (BTreeMap::new(), Vec::new());
    for row in tables {
        let (t, scoped): (String, bool) = (row.get(0), row.get(1));
        if TELEMETRY.contains(&t.as_str()) {
            continue;
        }
        if !scoped {
            unscoped.push(t);
            continue;
        }
        let rows = c
            .query(
                &format!("SELECT to_jsonb(t)::text FROM \"{t}\" t WHERE export_name = ANY($1)"),
                &[&exports],
            )
            .map_err(e)?;
        out.insert(t, rows.iter().map(|r| r.get::<_, String>(0)).collect());
    }
    Ok((out, unscoped))
}

/// One thing a failed invocation left behind.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Finding {
    pub kind: Leftover,
    /// The export it belongs to, when the row or the path says.
    pub export: Option<String>,
    pub what: String,
}

/// Everything that differs between `before` and `after`, plus what could not be compared.
pub(crate) fn diff(
    scope: &Scope,
    before: &Snapshot,
    after: &Snapshot,
) -> (Vec<Finding>, Vec<String>) {
    let (mut found, mut blind) = (Vec::new(), Vec::new());
    for (i, (name, _)) in scope.exports.iter().enumerate() {
        match (&before.trees[i], &after.trees[i]) {
            (Ok(b), Ok(a)) => {
                for (kind, what) in file_changes(b, a, false, scope.cdc[i]) {
                    found.push(Finding {
                        kind,
                        export: Some(name.clone()),
                        what,
                    });
                }
            }
            (Err(why), _) | (_, Err(why)) => {
                blind.push(format!("destination of `{name}` not compared: {why}"))
            }
        }
    }
    for (kind, what) in file_changes(&before.checkpoints, &after.checkpoints, true, false) {
        found.push(Finding {
            kind,
            export: None,
            what,
        });
    }
    match (&before.state, &after.state) {
        (Ok(b), Ok(a)) => found.extend(state_changes(b, a)),
        (Err(why), _) | (_, Err(why)) => blind.push(format!("state not compared: {why}")),
    }
    if !after.unscoped.is_empty() {
        blind.push(format!(
            "shared Postgres state tables without `export_name` not compared: {}",
            after.unscoped.join(", ")
        ));
    }
    (found, blind)
}

/// The kind of each file added, rewritten or removed between two fingerprints.
fn file_changes(
    before: &BTreeMap<PathBuf, Print>,
    after: &BTreeMap<PathBuf, Print>,
    checkpoint: bool,
    cdc: bool,
) -> Vec<(Leftover, String)> {
    let mut out = Vec::new();
    for (p, now) in after {
        let was = before.get(p);
        if was == Some(now) {
            continue;
        }
        // The canonical manifest may turn non-success only while a per-run copy keeps the success it replaces.
        let success_kept = || {
            p.file_name().is_some_and(|n| n == "manifest.json")
                && before.iter().any(|(q, f)| {
                    q.parent() == p.parent()
                        && f.2 == Tag::SuccessManifest
                        && q.file_name()
                            .is_some_and(|n| n.to_string_lossy().starts_with("manifest-"))
                })
        };
        let kind = match (checkpoint, was.map(|w| w.2), now.2) {
            (true, _, _) => Leftover::CdcCheckpoint,
            (_, _, Tag::Marker) => Leftover::SuccessMarker,
            (_, None | Some(Tag::FailedManifest), Tag::FailedManifest) => Leftover::FailedManifest,
            (_, Some(Tag::SuccessManifest), Tag::FailedManifest) if success_kept() => {
                Leftover::FailedManifest
            }
            (_, None, Tag::SuccessManifest) if cdc && per_run_manifest(p) => Leftover::CdcFlush,
            (_, _, Tag::SuccessManifest) => Leftover::SuccessManifest,
            (_, None, Tag::Other) => Leftover::OrphanPart,
            (_, Some(_), _) => Leftover::ChangedFile,
        };
        let verb = if was.is_some() { "rewritten" } else { "added" };
        out.push((kind, format!("{} {verb} ({} bytes)", p.display(), now.0)));
    }
    for (p, was) in before.iter().filter(|(p, _)| !after.contains_key(*p)) {
        // A `_SUCCESS` is withdrawn only together with a canonical manifest that now says the run failed.
        let withdrawn = was.2 == Tag::Marker
            && p.parent()
                .and_then(|d| after.get(&d.join("manifest.json")))
                .is_some_and(|m| m.2 == Tag::FailedManifest);
        let kind = if withdrawn {
            Leftover::FailedManifest
        } else {
            Leftover::ChangedFile
        };
        out.push((kind, format!("{} removed", p.display())));
    }
    out
}

/// Whether `p` is a per-run manifest copy (`manifest-<run id>.json`), not the canonical one.
fn per_run_manifest(p: &Path) -> bool {
    p.file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with("manifest-"))
}

/// Whether an `export_state` row carries no cursor: only a checkpointed run's claim (state v34: `resume_run_id` + `resume_owner`), set or released.
fn owner_pointer(table: &str, row: &str) -> bool {
    table == "export_state"
        && serde_json::from_str::<serde_json::Value>(row)
            .is_ok_and(|v| v.get("resume_run_id").is_some() && v["last_cursor_value"].is_null())
}

/// The pid a rivet run id (`<export>_<yyyymmddThhmmss.mmm>_<pid>`) or a part name built from it ends with, for each one `text` holds.
fn run_pids(text: &str) -> BTreeSet<u32> {
    static RE: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"\d{8}T\d{6}[._]\d{3}_(\d+)").unwrap());
    RE.captures_iter(text)
        .filter_map(|c| c[1].parse().ok())
        .collect()
}

/// Drop what carries only run ids of processes other than `pid`: another live run wrote it while this invocation was refused. Returns those pids with how much each wrote; with no `pid` nothing is dropped.
pub(crate) fn without_other_runs(
    found: Vec<Finding>,
    pid: Option<u32>,
) -> (Vec<Finding>, BTreeMap<u32, usize>) {
    let mut others = BTreeMap::new();
    let Some(own) = pid else {
        return (found, others);
    };
    let kept = found
        .into_iter()
        .filter(|f| {
            let pids = run_pids(&f.what);
            let foreign = !pids.is_empty() && !pids.contains(&own);
            for p in pids.iter().filter(|_| foreign) {
                *others.entry(*p).or_default() += 1;
            }
            !foreign
        })
        .collect();
    (kept, others)
}

/// The kind of each state row added, changed or removed between two snapshots.
fn state_changes(
    before: &BTreeMap<String, BTreeSet<String>>,
    after: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<Finding> {
    let none = BTreeSet::new();
    let mut out = Vec::new();
    let tables: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
    for t in tables {
        if TELEMETRY.contains(&t.as_str()) {
            continue;
        }
        let (b, a) = (
            before.get(t).unwrap_or(&none),
            after.get(t).unwrap_or(&none),
        );
        let field = |row: &str, k: &str| -> Option<String> {
            serde_json::from_str::<serde_json::Value>(row)
                .ok()
                .and_then(|v| v.get(k).and_then(|s| s.as_str()).map(str::to_string))
        };
        let journal = JOURNAL.contains(&t.as_str());
        let gone: BTreeSet<Option<String>> = b
            .difference(a)
            .map(|row| field(row, "export_name"))
            .collect();
        let table_kind = |row: &str| match kind_of_table(t) {
            Leftover::ChunkCheckpoint => Leftover::ChunkCheckpoint,
            _ if gone.contains(&field(row, "export_name")) => Leftover::ResumePoint,
            _ if owner_pointer(t, row) => Leftover::ChunkCheckpoint,
            kind => kind,
        };
        for row in a.difference(b) {
            let status = field(row, "status");
            let kind = match (journal, status.as_deref()) {
                (false, _) => table_kind(row),
                (true, Some("success")) => Leftover::SuccessRecord,
                (true, Some("failed")) if t == "load_run" => Leftover::LoadAttempt,
                (true, Some(s)) if NOT_SUCCESS.contains(&s) => Leftover::FailureRecord,
                (true, _) => Leftover::UnknownStatus,
            };
            out.push(Finding {
                kind,
                export: field(row, "export_name"),
                what: format!("{t} + {row}"),
            });
        }
        for row in b.difference(a).filter(|_| !journal) {
            out.push(Finding {
                kind: table_kind(row),
                export: field(row, "export_name"),
                what: format!("{t} - {row}"),
            });
        }
    }
    out
}

/// What a failed invocation's findings mean for the test.
#[derive(Debug, PartialEq)]
pub(crate) enum Verdict {
    /// Nothing left but what is allowed or a known defect: what was left, each known defect shown as (reason, findings), and whether the caller's own marker was among them.
    Refused {
        left: String,
        known: Vec<(String, String)>,
        marked: bool,
    },
    /// Something undeclared: one line per finding.
    Fail(Vec<String>),
}

/// Drop what an export that recorded success left when the exit may not be its own (`may`: a sibling failed in a multi-export invocation, or the test declared [`Leftover::DeliveredRun`]); returns those exports.
pub(crate) fn without_delivering_siblings(
    found: Vec<Finding>,
    may: bool,
) -> (Vec<Finding>, BTreeSet<String>) {
    let delivered: BTreeSet<String> = found
        .iter()
        .filter(|f| f.kind == Leftover::SuccessRecord && may)
        .filter_map(|f| f.export.clone())
        .collect();
    let kept = found
        .into_iter()
        .filter(|f| match &f.export {
            Some(e) => !delivered.contains(e),
            None => delivered.is_empty(),
        })
        .collect();
    (kept, delivered)
}

/// Judge `found` against the failure's own record, what the test `declared`, the caller's `marker` known defect (kind, reason) and [`KNOWN_PRODUCT_DEFECTS`].
pub(crate) fn judge(
    found: &[Finding],
    declared: &[Leftover],
    marker: Option<(Leftover, &str)>,
) -> Verdict {
    let allowed = |k: Leftover| OWN_RECORD.contains(&k) || declared.contains(&k);
    let excuses: Vec<(Leftover, &str)> = marker
        .into_iter()
        .chain(KNOWN_PRODUCT_DEFECTS.iter().copied())
        .collect();
    let line = |f: &Finding| format!("{}: {}", f.kind.slug(), f.what);
    let bad: Vec<&Finding> = found.iter().filter(|f| !allowed(f.kind)).collect();
    let fail: Vec<String> = bad
        .iter()
        .filter(|f| !excuses.iter().any(|(k, _)| *k == f.kind))
        .map(|f| line(f))
        .collect();
    if !fail.is_empty() {
        // A known defect excuses a run that shows nothing else: beside an undeclared leftover every finding is reported.
        return Verdict::Fail(bad.iter().map(|f| line(f)).collect());
    }
    let mut n: BTreeMap<Leftover, usize> = BTreeMap::new();
    for f in found {
        *n.entry(f.kind).or_default() += 1;
    }
    let left: Vec<String> = n
        .iter()
        .map(|(k, c)| format!("{} x{c}", k.slug()))
        .collect();
    let known: Vec<(String, String)> = excuses
        .iter()
        .filter(|(k, _)| bad.iter().any(|f| f.kind == *k))
        .map(|(k, why)| {
            let lines: Vec<String> = bad
                .iter()
                .filter(|f| f.kind == *k)
                .map(|f| line(f))
                .collect();
            (
                format!("[a failed run left: {}] {why}", k.slug()),
                lines.join(" | "),
            )
        })
        .collect();
    Verdict::Refused {
        left: if left.is_empty() {
            "left nothing".into()
        } else {
            format!("left only {}", left.join(", "))
        },
        marked: marker.is_some_and(|(k, _)| bad.iter().any(|f| f.kind == k)),
        known,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch destination holding one delivered run, and a SQLite state with its cursor.
    fn fixture() -> (tempfile::TempDir, Scope) {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("part-0.parquet"), b"committed").unwrap();
        std::fs::write(out.join("_SUCCESS"), b"").unwrap();
        for m in ["manifest.json", "manifest-r1.json"] {
            std::fs::write(out.join(m), br#"{"status":"success"}"#).unwrap();
        }
        let db = dir.path().join(".rivet_state.db");
        let c = rusqlite::Connection::open(&db).unwrap();
        c.execute_batch(
            "CREATE TABLE export_state (export_name TEXT PRIMARY KEY, last_cursor_value TEXT);
             CREATE TABLE export_schema (export_name TEXT PRIMARY KEY, columns_json TEXT);
             CREATE TABLE file_log (id INTEGER PRIMARY KEY, export_name TEXT, file_name TEXT);
             CREATE TABLE chunk_task (id INTEGER PRIMARY KEY, run_id TEXT, status TEXT);
             CREATE TABLE export_metrics (id INTEGER PRIMARY KEY, export_name TEXT, status TEXT);
             CREATE TABLE load_run (load_id TEXT, export_name TEXT, status TEXT);
             CREATE TABLE run_journal (run_id TEXT, export_name TEXT);
             INSERT INTO export_state VALUES ('e', '10');
             INSERT INTO export_schema VALUES ('e', '[a]');",
        )
        .unwrap();
        let scope = Scope {
            exports: vec![("e".into(), Ok(out))],
            cdc: vec![false],
            checkpoints: vec![dir.path().join("cdc.ckpt")],
            state: Some(StateAt::Sqlite(db)),
        };
        (dir, scope)
    }

    /// The kinds a mutation of the fixture leaves, and the default grade's verdict on them.
    fn kinds_after(mutate: impl FnOnce(&Path)) -> (Vec<Leftover>, Verdict) {
        let (dir, scope) = fixture();
        let before = Snapshot::take(&scope);
        mutate(dir.path());
        let (found, blind) = diff(&scope, &before, &Snapshot::take(&scope));
        assert!(blind.is_empty(), "{blind:?}");
        let verdict = judge(&found, &[], None);
        (found.iter().map(|f| f.kind).collect(), verdict)
    }

    /// A refusal that showed no known defect.
    fn clean(left: &str) -> Verdict {
        Verdict::Refused {
            left: left.into(),
            known: Vec::new(),
            marked: false,
        }
    }

    fn sql(dir: &Path, stmt: &str) {
        rusqlite::Connection::open(dir.join(".rivet_state.db"))
            .unwrap()
            .execute_batch(stmt)
            .unwrap();
    }

    #[test]
    fn a_failed_run_that_left_nothing_or_only_its_own_record_is_a_clean_refusal() {
        assert_eq!(kinds_after(|_| {}), (vec![], clean("left nothing")));
        let (kinds, verdict) = kinds_after(|d| {
            sql(
                d,
                "INSERT INTO export_metrics VALUES (1, 'e', 'failed');
                 INSERT INTO run_journal VALUES ('r', 'e');
                 INSERT INTO load_run VALUES ('l', 'e', 'refused')",
            );
            std::fs::write(d.join("out/manifest-r2.json"), br#"{"status":"failed"}"#).unwrap();
            std::fs::write(d.join("out/manifest.json"), br#"{"status":"failed"}"#).unwrap();
            std::fs::remove_file(d.join("out/_SUCCESS")).unwrap();
        });
        assert_eq!(
            kinds,
            vec![
                Leftover::FailedManifest,
                Leftover::FailedManifest,
                Leftover::FailedManifest,
                Leftover::FailureRecord,
                Leftover::FailureRecord
            ]
        );
        assert_eq!(
            verdict,
            clean("left only failure-record x2, failed-manifest x3")
        );
    }

    #[test]
    fn a_part_written_by_a_failed_run_is_caught() {
        let (kinds, verdict) =
            kinds_after(|d| std::fs::write(d.join("out/part-1.parquet"), b"orphan").unwrap());
        assert_eq!(kinds, vec![Leftover::OrphanPart]);
        assert!(matches!(verdict, Verdict::Fail(l) if l[0].starts_with("orphan-part: ")));
    }

    #[test]
    fn a_success_marker_or_manifest_written_by_a_failed_run_is_caught() {
        let (kinds, verdict) =
            kinds_after(|d| std::fs::write(d.join("out/_SUCCESS"), b"again").unwrap());
        assert_eq!(kinds, vec![Leftover::SuccessMarker]);
        assert!(matches!(verdict, Verdict::Fail(l) if l[0].starts_with("success-marker: ")));
        let (kinds, verdict) = kinds_after(|d| {
            std::fs::write(d.join("out/manifest-r2.json"), br#"{"status":"success"}"#).unwrap()
        });
        assert_eq!(kinds, vec![Leftover::SuccessManifest]);
        assert!(matches!(verdict, Verdict::Fail(l) if l[0].starts_with("success-manifest: ")));
    }

    #[test]
    fn a_cursor_moved_by_a_failed_run_is_caught() {
        let (kinds, verdict) = kinds_after(|d| {
            sql(
                d,
                "UPDATE export_state SET last_cursor_value = '99' WHERE export_name = 'e'",
            )
        });
        assert_eq!(kinds, vec![Leftover::ResumePoint, Leftover::ResumePoint]);
        assert!(
            matches!(verdict, Verdict::Fail(l) if l[0].contains("export_state + ") && l[0].contains("99"))
        );
    }

    #[test]
    fn a_success_that_no_per_run_manifest_keeps_is_not_the_failures_own_record() {
        let (kinds, _) = kinds_after(|d| {
            std::fs::remove_file(d.join("out/_SUCCESS")).unwrap();
        });
        assert_eq!(
            kinds,
            vec![Leftover::ChangedFile],
            "a marker removed beside a manifest that still says success"
        );
        let (dir, scope) = fixture();
        std::fs::remove_file(dir.path().join("out/manifest-r1.json")).unwrap();
        let before = Snapshot::take(&scope);
        std::fs::write(
            dir.path().join("out/manifest.json"),
            br#"{"status":"failed"}"#,
        )
        .unwrap();
        let (found, _) = diff(&scope, &before, &Snapshot::take(&scope));
        assert_eq!(
            found.iter().map(|f| f.kind).collect::<Vec<_>>(),
            vec![Leftover::ChangedFile],
            "the only success manifest was overwritten"
        );
    }

    #[test]
    fn each_other_leftover_is_its_own_kind() {
        let (kinds, _) = kinds_after(|d| {
            std::fs::write(d.join("out/part-0.parquet"), b"rewritten").unwrap();
            std::fs::write(d.join("cdc.ckpt"), b"{}").unwrap();
            sql(
                d,
                "INSERT INTO chunk_task VALUES (1, 'r', 'completed');
                 INSERT INTO export_metrics VALUES (1, 'e', 'success');
                 INSERT INTO export_metrics VALUES (2, 'e', 'done');
                 INSERT INTO export_schema VALUES ('other', '[b]');
                 UPDATE export_schema SET columns_json = '[a, b]' WHERE export_name = 'e';
                 INSERT INTO file_log VALUES (1, 'e', 'part-1.parquet');
                 INSERT INTO load_run VALUES ('l', 'e', 'failed')",
            );
        });
        assert_eq!(
            kinds,
            vec![
                Leftover::ChangedFile,
                Leftover::CdcCheckpoint,
                Leftover::ChunkCheckpoint,
                Leftover::SuccessRecord,
                Leftover::UnknownStatus,
                Leftover::ResumePoint,
                Leftover::ObservedSchema,
                Leftover::ResumePoint,
                Leftover::FileLog,
                Leftover::LoadAttempt
            ]
        );
        let (kinds, _) =
            kinds_after(|d| std::fs::remove_file(d.join("out/part-0.parquet")).unwrap());
        assert_eq!(kinds, vec![Leftover::ChangedFile]);
    }

    #[test]
    fn a_declaration_allows_its_kind_only_and_a_known_defect_excuses_its_kind_only() {
        let of = |kind| Finding {
            kind,
            export: Some("e".into()),
            what: "p".into(),
        };
        let both = [of(Leftover::OrphanPart), of(Leftover::SuccessMarker)];
        assert_eq!(
            judge(&both[..1], &[Leftover::OrphanPart], None),
            clean("left only orphan-part x1")
        );
        assert!(
            matches!(judge(&both, &[Leftover::OrphanPart], None), Verdict::Fail(l) if l == ["success-marker: p"])
        );
        let marker = Some((Leftover::SuccessMarker, "a probe marker"));
        assert!(matches!(
            judge(&both, &[Leftover::OrphanPart], marker),
            Verdict::Refused { marked: true, known, .. } if known.len() == 1 && known[0].0.contains("a probe marker")
        ));
        assert!(
            matches!(judge(&both, &[], marker), Verdict::Fail(l) if l.len() == 2),
            "a known defect of one kind does not excuse another kind"
        );
        assert!(matches!(
            judge(&both[..1], &[Leftover::OrphanPart], marker),
            Verdict::Refused { marked: false, .. }
        ));
    }

    #[test]
    fn a_pinned_product_defect_is_an_xfail_and_never_hides_another_leftover() {
        let of = |kind| Finding {
            kind,
            export: Some("e".into()),
            what: "p".into(),
        };
        let (everywhere, _) = KNOWN_PRODUCT_DEFECTS[0];
        assert!(matches!(
            judge(&[of(everywhere)], &[], None),
            Verdict::Refused { marked: false, known, .. } if known.len() == 1
        ));
        assert!(
            matches!(judge(&[of(everywhere), of(Leftover::ResumePoint)], &[], None), Verdict::Fail(l) if l.len() == 2)
        );
    }

    #[test]
    fn a_sibling_export_that_recorded_success_is_not_part_of_the_refusal() {
        let f = |kind, export: &str| Finding {
            kind,
            export: Some(export.into()),
            what: "x".into(),
        };
        let found = vec![
            f(Leftover::SuccessRecord, "ok"),
            f(Leftover::OrphanPart, "ok"),
            f(Leftover::OrphanPart, "bad"),
        ];
        let (kept, delivered) = without_delivering_siblings(found.clone(), true);
        assert_eq!(kept, vec![f(Leftover::OrphanPart, "bad")]);
        assert_eq!(delivered, ["ok".to_string()].into());
        let (kept, delivered) = without_delivering_siblings(found.clone(), false);
        assert_eq!(
            (kept, delivered.len()),
            (found, 0),
            "a single export that recorded success is a finding"
        );
    }

    #[test]
    fn a_declaration_is_typed_and_a_crash_is_the_tests_own_fault_hook_or_a_signal() {
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(
            declared_in_env("orphan-part, file-log"),
            vec![Leftover::OrphanPart, Leftover::FileLog]
        );
        assert_eq!(
            Leftover::of_known_defect_class("a failed run left: success-marker"),
            Some(Leftover::SuccessMarker)
        );
        assert_eq!(Leftover::of_known_defect_class("undelivered rows"), None);
        let exit1 = std::process::ExitStatus::from_raw(1 << 8);
        assert_eq!(crashed_by_the_test(exit1, &[("RUST_LOG", "warn")]), None);
        assert!(crashed_by_the_test(exit1, &[("RIVET_TEST_PANIC_AT", "after_part")]).is_some());
        assert!(crashed_by_the_test(std::process::ExitStatus::from_raw(9), &[]).is_some());
    }

    #[test]
    #[should_panic(expected = "can only be a known defect")]
    fn a_test_cannot_declare_that_a_failed_run_leaves_a_success_marker() {
        declared_in_env("success-marker");
    }

    #[test]
    #[should_panic(expected = "unknown leftover kind")]
    fn an_unknown_leftover_kind_is_a_harness_error() {
        declared_in_env("everything");
    }

    #[test]
    fn a_flush_a_cdc_stream_committed_is_its_own_kind_and_any_other_success_manifest_is_not() {
        let kinds = |cdc: bool, file: &str| {
            let (dir, mut scope) = fixture();
            scope.cdc = vec![cdc];
            let before = Snapshot::take(&scope);
            let body = br#"{"status":"success","run":2}"#;
            std::fs::write(dir.path().join("out").join(file), body).unwrap();
            let (found, _) = diff(&scope, &before, &Snapshot::take(&scope));
            found.iter().map(|f| f.kind).collect::<Vec<_>>()
        };
        assert_eq!(kinds(true, "manifest-r2.json"), vec![Leftover::CdcFlush]);
        assert_eq!(
            kinds(false, "manifest-r2.json"),
            vec![Leftover::SuccessManifest]
        );
        assert_eq!(
            kinds(true, "manifest.json"),
            vec![Leftover::SuccessManifest]
        );
        assert_eq!(
            kinds(true, "manifest-r1.json"),
            vec![Leftover::SuccessManifest]
        );
        let flush = Finding {
            kind: Leftover::CdcFlush,
            export: None,
            what: String::new(),
        };
        assert!(matches!(judge(&[flush], &[], None), Verdict::Fail(_)));
    }

    #[test]
    fn a_checkpoint_owner_pointer_is_a_chunk_checkpoint_and_a_row_with_a_cursor_is_a_resume_point()
    {
        let kind = |row: &str| {
            let after = BTreeMap::from([(
                "export_state".to_string(),
                BTreeSet::from([row.to_string()]),
            )]);
            state_changes(&BTreeMap::new(), &after)[0].kind
        };
        assert_eq!(
            kind(
                r#"{"export_name":"e","last_cursor_value":null,"resume_run_id":"e_20261007T145821.410_7","resume_owner":"chunked"}"#
            ),
            Leftover::ChunkCheckpoint
        );
        assert_eq!(
            kind(
                r#"{"export_name":"e","last_cursor_value":"10","resume_run_id":"e_20261007T145821.410_7"}"#
            ),
            Leftover::ResumePoint
        );
        assert_eq!(
            kind(r#"{"export_name":"e","last_cursor_value":null,"resume_run_id":null}"#),
            Leftover::ChunkCheckpoint
        );
        assert_eq!(
            kind(r#"{"export_name":"e","last_cursor_value":"10","resume_run_id":null}"#),
            Leftover::ResumePoint
        );
    }

    #[test]
    fn what_another_live_run_wrote_is_not_the_refused_invocations() {
        let f = |what: &str| Finding {
            kind: Leftover::ChunkCheckpoint,
            export: Some("e".into()),
            what: what.into(),
        };
        let found = vec![
            f(r#"chunk_run + {"run_id":"e_20261007T145821.410_59977"}"#),
            f("out/e_20261007T145830_981_59977_keyset_start.parquet added (9 bytes)"),
            f(r#"chunk_run + {"run_id":"e_20261007T145821.409_60001"}"#),
            f(r#"export_schema + {"export_name":"e"}"#),
        ];
        let (kept, others) = without_other_runs(found.clone(), Some(60001));
        assert_eq!(kept, found[2..]);
        assert_eq!(others, BTreeMap::from([(59977, 2)]));
        assert_eq!(without_other_runs(found.clone(), None).0, found);
    }
}
