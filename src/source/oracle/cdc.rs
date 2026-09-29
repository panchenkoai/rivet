//! Oracle change capture through LogMiner (ADR-0037): mined from the root, framed by XID,
//! resumed from a two-SCN checkpoint that records the database it belongs to.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::NaiveDateTime;
use oracledb::{Connection, Cursor, Row};

use super::{Ora, connect};
use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::checkpoint_identity::IdentityVerdict;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{CdcEngine, ChangeEvent, ChangeOp, ChangeStream, Position, TxnFramer};

/// LogMiner returns names over 30 bytes as `UNSUPPORTED`.
const MAX_MINED_NAME: usize = 30;
/// Three select-list entries per column under Oracle's 1000-entry cap.
const MAX_COLUMNS: usize = 300;

/// How a mined column's text becomes a value; `None` from [`ColKind::of`] is a refused type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColKind {
    Number,
    Float,
    Date,
    Timestamp,
    TimestampTz,
    Text,
    Raw,
}

impl ColKind {
    /// The kind for an `ALL_TAB_COLUMNS.DATA_TYPE`, or `None` for a type the preview does not capture.
    pub(crate) fn of(data_type: &str) -> Option<Self> {
        let t = data_type.to_ascii_uppercase();
        Some(match t.as_str() {
            "NUMBER" | "FLOAT" => Self::Number,
            "BINARY_FLOAT" | "BINARY_DOUBLE" => Self::Float,
            "DATE" => Self::Date,
            "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" => Self::Text,
            "RAW" => Self::Raw,
            _ if t.starts_with("TIMESTAMP") && t.ends_with("WITH TIME ZONE") => Self::TimestampTz,
            // WITH LOCAL TIME ZONE renders in the session zone, pinned to UTC.
            _ if t.starts_with("TIMESTAMP") => Self::Timestamp,
            _ => return None,
        })
    }
}

/// `text` from `MINE_VALUE` as the value the sink builds for this kind; `Err` when it cannot be read.
pub(crate) fn decode(kind: ColKind, text: &str) -> std::result::Result<RivetValue, String> {
    let bad = || format!("unreadable {kind:?} value {text:?}");
    Ok(match kind {
        ColKind::Number => {
            let plain = canonical_number(text).ok_or_else(bad)?;
            match plain.parse::<i64>() {
                Ok(i) => RivetValue::Int(i),
                Err(_) => RivetValue::Bytes(plain.into_bytes()),
            }
        }
        ColKind::Float => RivetValue::Float(text.trim().parse::<f64>().map_err(|_| bad())?),
        ColKind::Date | ColKind::Timestamp => {
            RivetValue::DateTime(parse_datetime(text).ok_or_else(bad)?)
        }
        ColKind::TimestampTz => {
            let (wall, offset) = text.trim().rsplit_once(' ').ok_or_else(bad)?;
            let wall = parse_datetime(wall).ok_or_else(bad)?;
            let secs = parse_offset(offset).ok_or_else(bad)?;
            RivetValue::DateTime(wall - chrono::Duration::seconds(secs))
        }
        ColKind::Text => RivetValue::Bytes(text.as_bytes().to_vec()),
        ColKind::Raw => RivetValue::Bytes(decode_hex(text.trim()).ok_or_else(bad)?),
    })
}

/// Hex text (either case) as bytes; `None` on an odd length or a non-hex digit.
fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Oracle's number text (`-.5`, `1.0E+125`) in the plain form the batch export writes (`-0.5`, `1` and 125 zeros).
pub(crate) fn canonical_number(text: &str) -> Option<String> {
    let t = text.trim();
    let (neg, t) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (mant, exp) = match t.split_once(['E', 'e']) {
        Some((m, e)) => (m, e.parse::<i64>().ok()?),
        None => (t, 0),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    if int.is_empty() && frac.is_empty()
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int}{frac}");
    let point = int.len() as i64 + exp;
    let (whole, fraction) = if point <= 0 {
        (
            String::new(),
            format!("{}{digits}", "0".repeat(point.unsigned_abs() as usize)),
        )
    } else if point as usize >= digits.len() {
        (
            format!("{digits}{}", "0".repeat(point as usize - digits.len())),
            String::new(),
        )
    } else {
        let (w, f) = digits.split_at(point as usize);
        (w.to_string(), f.to_string())
    };
    let whole = whole.trim_start_matches('0');
    let fraction = fraction.trim_end_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let zero = whole == "0" && fraction.is_empty();
    let sign = if neg && !zero { "-" } else { "" };
    Some(if fraction.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    })
}

/// The mining session's datetime formats: `SYYYY` so `MINE_VALUE` keeps a BC year's sign.
const SIGNED_YEAR_PIN: &[&str] = &[
    "ALTER SESSION SET NLS_DATE_FORMAT = 'SYYYY-MM-DD\"T\"HH24:MI:SS\".000000\"'",
    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'SYYYY-MM-DD\"T\"HH24:MI:SS.FF'",
    "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'SYYYY-MM-DD\"T\"HH24:MI:SS.FF TZH:TZM'",
];

/// `SYYYY-MM-DDTHH:MI:SS[.fraction]` (a `-` year is BC), the fraction any length, kept to microseconds.
pub(crate) fn parse_datetime(text: &str) -> Option<NaiveDateTime> {
    use chrono::Datelike as _;
    let t = text.trim();
    let (bc, t) = t.strip_prefix('-').map_or((false, t), |r| (true, r));
    let (base, frac) = t.split_once('.').unwrap_or((t, ""));
    let dt = NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S").ok()?;
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let micros: u32 = format!("{:0<6}", &frac[..frac.len().min(6)]).parse().ok()?;
    let dt = if bc {
        dt.with_year(super::arrow_convert::chrono_year(-dt.year()))?
    } else {
        dt
    };
    dt.checked_add_signed(chrono::Duration::microseconds(micros.into()))
}

/// `+HH:MM` / `-HH:MM` as seconds east of UTC.
fn parse_offset(text: &str) -> Option<i64> {
    let (sign, rest) = match text.as_bytes().first()? {
        b'+' => (1, &text[1..]),
        b'-' => (-1, &text[1..]),
        _ => return None,
    };
    let (h, m) = rest.split_once(':')?;
    Some(sign * (h.parse::<i64>().ok()? * 3600 + m.parse::<i64>().ok()? * 60))
}

/// The database a checkpoint belongs to; the container fields are empty on a non-CDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OraIdentity {
    pub dbid: String,
    pub db_unique_name: String,
    pub resetlogs: String,
    pub con_name: String,
    pub con_dbid: String,
}

impl OraIdentity {
    fn from_position(pos: &Position) -> Option<Self> {
        let get = |k: &str| pos.0.get(k)?.as_str().map(str::to_string);
        Some(Self {
            dbid: get("dbid")?,
            db_unique_name: get("db_unique_name").unwrap_or_default(),
            resetlogs: get("resetlogs_change")?,
            con_name: get("con_name").unwrap_or_default(),
            con_dbid: get("con_dbid").unwrap_or_default(),
        })
    }
}

/// Judge a resume: a checkpoint from another database, incarnation or PDB is foreign.
pub(crate) fn identity_verdict(
    checkpoint: Option<&OraIdentity>,
    server: &OraIdentity,
) -> IdentityVerdict {
    let Some(c) = checkpoint else {
        return IdentityVerdict::Unverifiable(
            "oracle cdc: this checkpoint records no database identity, so rivet cannot confirm \
             it belongs to this database"
                .into(),
        );
    };
    if c.dbid != server.dbid {
        return IdentityVerdict::Foreign(format!(
            "oracle cdc: this checkpoint was written against another database (DBID {} / {}, \
             this one is {} / {}). An SCN means nothing outside the database that issued it: \
             resuming would start at an arbitrary point and skip changes silently.",
            c.dbid, c.db_unique_name, server.dbid, server.db_unique_name
        ));
    }
    if c.resetlogs != server.resetlogs {
        return IdentityVerdict::Foreign(format!(
            "oracle cdc: the database was opened RESETLOGS since this checkpoint was written \
             (incarnation SCN {} is now {}). Its redo history was rewound, so changes the \
             checkpoint covers may no longer exist while the destination still holds them.",
            c.resetlogs, server.resetlogs
        ));
    }
    if c.con_dbid != server.con_dbid {
        return IdentityVerdict::Foreign(format!(
            "oracle cdc: this checkpoint belongs to another pluggable database ({} DBID {}, \
             the connection's is {} DBID {}).",
            c.con_name, c.con_dbid, server.con_name, server.con_dbid
        ));
    }
    IdentityVerdict::Ok
}

/// A resume position: mine from `low_water`, deliver only commits after `commit_scn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Scns {
    pub low_water: u64,
    pub commit_scn: u64,
}

impl Scns {
    /// Read from a checkpoint; a file that parses but lacks either SCN is refused, never treated as absent.
    pub(crate) fn from_position(pos: &Position, path: &str) -> Result<Self> {
        let get = |k: &str| {
            pos.0
                .get(k)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
        };
        match (get("low_water"), get("commit_scn")) {
            (Some(low_water), Some(commit_scn)) if low_water <= commit_scn => Ok(Self {
                low_water,
                commit_scn,
            }),
            _ => crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_CHECKPOINT_INVALID,
                "oracle cdc: checkpoint '{path}' parses but carries no valid `low_water` / \
                 `commit_scn` pair — refusing to treat it as absent, which would re-anchor at \
                 the current SCN and skip everything since. Restore the file, or: {}",
                crate::source::cdc::checkpoint_identity::RECOVER
            ),
        }
    }

    /// What an event carries as `__pos`: the two SCNs only.
    fn position(self) -> Position {
        Position(serde_json::json!({
            "low_water": self.low_water.to_string(),
            "commit_scn": self.commit_scn.to_string(),
        }))
    }
}

/// `position` with the database identity added, as a checkpoint records it.
fn with_identity(position: &Position, id: &OraIdentity) -> Position {
    let mut v = position.0.clone();
    if let Some(o) = v.as_object_mut() {
        o.insert("dbid".into(), id.dbid.clone().into());
        o.insert("db_unique_name".into(), id.db_unique_name.clone().into());
        o.insert("resetlogs_change".into(), id.resetlogs.clone().into());
        o.insert("con_name".into(), id.con_name.clone().into());
        o.insert("con_dbid".into(), id.con_dbid.clone().into());
    }
    Position(v)
}

/// One redo log file as the catalog lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogFile {
    pub name: String,
    pub thread: u32,
    pub sequence: u64,
    pub first: u64,
    /// Exclusive upper SCN; `u64::MAX` for the current online group.
    pub next: u64,
}

/// The files that cover `[start, end]` — archived copies preferred, one per (thread, sequence) —
/// or a data-loss refusal when the history no longer reaches `start` or a sequence is missing.
pub(crate) fn plan_logs(
    archived: &[LogFile],
    online: &[LogFile],
    start: u64,
    end: u64,
) -> Result<Vec<LogFile>> {
    let mut by_seq: BTreeMap<(u32, u64), LogFile> = BTreeMap::new();
    for f in archived.iter().chain(online) {
        by_seq
            .entry((f.thread, f.sequence))
            .or_insert_with(|| f.clone());
    }
    let mut threads: BTreeMap<u32, Vec<LogFile>> = BTreeMap::new();
    for ((thread, _), f) in by_seq {
        if f.next > start && f.first <= end {
            threads.entry(thread).or_default().push(f);
        }
    }
    anyhow::ensure!(
        !threads.is_empty(),
        "oracle cdc: no redo log covers SCN {start}..{end} — the archived logs the checkpoint \
         needs are gone. The changes between the checkpoint and the oldest available log are \
         LOST to this stream. Delete the checkpoint so the next run anchors afresh FIRST, then \
         re-snapshot the tables."
    );
    let mut out = Vec::new();
    for (thread, files) in threads {
        let oldest = &files[0];
        anyhow::ensure!(
            oldest.first <= start,
            "oracle cdc: the oldest redo log still available for thread {thread} starts at SCN \
             {} but the checkpoint needs {start} — the archives in between were deleted and the \
             changes they held are LOST to this stream. Delete the checkpoint so the next run \
             anchors afresh FIRST, then re-snapshot the tables.",
            oldest.first
        );
        for w in files.windows(2) {
            anyhow::ensure!(
                w[1].sequence == w[0].sequence + 1,
                "oracle cdc: redo sequence {} of thread {thread} is missing (have {} then {}) — \
                 the changes it held are LOST to this stream. Restore the archived log, or \
                 delete the checkpoint (anchor first, then re-snapshot).",
                w[0].sequence + 1,
                w[0].sequence,
                w[1].sequence
            );
        }
        out.extend(files);
    }
    Ok(out)
}

/// Whether an idle or fully acknowledged drain may move the checkpoint to the open-time frontier.
pub(crate) fn frontier_is_due(
    exhausted: bool,
    outstanding: bool,
    frontier: Scns,
    from: Scns,
) -> bool {
    exhausted && !outstanding && frontier.commit_scn > from.commit_scn
}

/// A configured table resolved to the catalog, with the spelling its events carry.
#[derive(Debug, Clone)]
struct Captured {
    owner: String,
    table: String,
    ev_schema: String,
    ev_table: String,
    columns: Vec<(String, ColKind)>,
    names: Arc<[String]>,
}

/// The configured `[owner.]table` split the way routing compares it.
fn event_spelling(configured: &str) -> (String, String) {
    match configured.rsplit_once('.') {
        Some((o, t)) => (o.to_string(), t.to_string()),
        None => (String::new(), configured.to_string()),
    }
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The first column of `sql`'s first row as text.
fn scalar(conn: &Connection, sql: &str) -> Result<Option<String>> {
    let row = conn.query(sql, &[]).ora()?.next().transpose().ora()?;
    row.map(|r| r.get::<Option<String>>(0).ora())
        .transpose()
        .map(Option::flatten)
}

/// Every row of `sql`, each cell as text.
fn rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let cursor = conn.query(sql, &[]).ora()?;
    let n = cursor.columns().len();
    cursor
        .map(|r| {
            let r = r.ora()?;
            (0..n).map(|i| r.get::<Option<String>>(i).ora()).collect()
        })
        .collect()
}

fn scn(conn: &Connection, sql: &str) -> Result<u64> {
    scalar(conn, sql)?
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("oracle cdc: could not read an SCN from `{sql}`"))
}

/// The connection's container: `(CON_ID, name, DBID)`, name and DBID empty on a non-CDB.
fn container(conn: &Connection) -> Result<(String, String, String)> {
    let r = rows(
        conn,
        "SELECT SYS_CONTEXT('USERENV','CON_ID'), SYS_CONTEXT('USERENV','CON_NAME'), \
                SYS_CONTEXT('USERENV','CON_DBID') FROM dual",
    )?;
    let r = r.into_iter().next().unwrap_or_default();
    let cell = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
    // CON_ID 0 is a non-CDB; 1 is the root, where there is no PDB to capture from.
    match cell(0).as_str() {
        "0" => Ok((cell(0), String::new(), String::new())),
        "1" => crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_PREREQUISITE,
            "oracle cdc: the URL connects to CDB$ROOT — point it at the pluggable database \
             that holds the tables (its service name); rivet switches to the root to mine"
        ),
        _ => Ok((cell(0), cell(1), cell(2))),
    }
}

/// Resolve and vet every configured table in the PDB: types, name lengths, identity columns.
fn resolve_tables(conn: &Connection, configured: &[String]) -> Result<Vec<Captured>> {
    let major: u32 = scalar(
        conn,
        "SELECT TO_CHAR(MAX(TO_NUMBER(REGEXP_SUBSTR(version, '^[0-9]+')))) \
         FROM product_component_version",
    )?
    .and_then(|v| v.parse().ok())
    .unwrap_or(0);
    let mut out = Vec::new();
    for cfg in configured {
        let (owner, table) = crate::sql::oracle_catalog_preds(cfg);
        let cols = rows(
            conn,
            &format!(
                "SELECT owner, table_name, column_name, data_type, identity_column, \
                        virtual_column FROM all_tab_cols \
                  WHERE owner = {owner} AND table_name = {table} AND hidden_column = 'NO' \
                  ORDER BY column_id"
            ),
        )?;
        anyhow::ensure!(
            !cols.is_empty(),
            "oracle cdc: table `{cfg}` not found or not readable — Oracle stores unquoted names \
             upper-case; check the spelling and that the capture user can SELECT it"
        );
        let cell = |r: &Vec<Option<String>>, i: usize| r[i].clone().unwrap_or_default();
        let (o, t) = (cell(&cols[0], 0), cell(&cols[0], 1));
        let mut refused = Vec::new();
        let mut columns = Vec::new();
        for r in &cols {
            let (name, ty) = (cell(r, 2), cell(r, 3));
            if name.len() > MAX_MINED_NAME || name.contains(['.', '"', '\'']) {
                refused.push(format!("{name} (a name LogMiner cannot address)"));
            } else if cell(r, 5) == "YES" {
                refused.push(format!(
                    "{name} (a virtual column: it has no redo, so every change would carry NULL)"
                ));
            } else if cell(r, 4) == "YES" && major < 23 {
                refused.push(format!(
                    "{name} (an identity column: LogMiner ignores the whole table before 23)"
                ));
            }
            match ColKind::of(&ty) {
                Some(k) => columns.push((name, k)),
                None => refused.push(format!("{name} {ty}")),
            }
        }
        anyhow::ensure!(
            o.len() <= MAX_MINED_NAME && t.len() <= MAX_MINED_NAME,
            "oracle cdc: `{o}.{t}` has a name over {MAX_MINED_NAME} bytes, which LogMiner \
             reports as UNSUPPORTED and never decodes"
        );
        anyhow::ensure!(
            refused.is_empty(),
            "oracle cdc: `{o}.{t}` cannot be captured in this preview: {}. Supported: NUMBER, \
             FLOAT, BINARY_FLOAT/DOUBLE, DATE, TIMESTAMP (any zone form), VARCHAR2, NVARCHAR2, \
             CHAR, NCHAR, RAW. Export the table in batch, or capture a table without them.",
            refused.join(", ")
        );
        anyhow::ensure!(
            columns.len() <= MAX_COLUMNS,
            "oracle cdc: `{o}.{t}` has {} columns; the preview mines at most {MAX_COLUMNS}",
            columns.len()
        );
        let (ev_schema, ev_table) = event_spelling(cfg);
        let names: Arc<[String]> = columns.iter().map(|(n, _)| n.clone()).collect();
        out.push(Captured {
            owner: o,
            table: t,
            ev_schema,
            ev_table,
            columns,
            names,
        });
    }
    Ok(out)
}

/// Why a table's redo cannot carry a whole row image, from its supplemental log groups.
pub(crate) fn logging_gap(
    owner: &str,
    table: &str,
    db_all: bool,
    groups: &[String],
) -> Option<String> {
    if db_all || groups.iter().any(|g| g == "ALL COLUMN LOGGING") {
        return None;
    }
    Some(format!(
        "`{owner}.{table}` has no ALL COLUMNS supplemental logging, so an UPDATE's redo carries \
         only the changed columns and a DELETE's only the key. Run as a DBA: ALTER TABLE \
         \"{owner}\".\"{table}\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
    ))
}

/// The first captured table whose redo cannot carry a whole row, with the statement that fixes it.
fn logging_check(conn: &Connection, tables: &[Captured]) -> Result<Option<String>> {
    let db_all = scalar(conn, "SELECT supplemental_log_data_all FROM v$database")
        .ok()
        .flatten()
        .is_some_and(|v| v == "YES");
    for t in tables {
        let groups: Vec<String> = rows(
            conn,
            &format!(
                "SELECT log_group_type FROM all_log_groups WHERE owner = {} AND table_name = {}",
                lit(&t.owner),
                lit(&t.table)
            ),
        )?
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect();
        if let Some(why) = logging_gap(&t.owner, &t.table, db_all, &groups) {
            return Ok(Some(why));
        }
    }
    Ok(None)
}

/// Switch to the root and read the database identity; refuse a source that cannot be mined.
fn root_identity(conn: &Connection, con_name: String, con_dbid: String) -> Result<OraIdentity> {
    if !con_name.is_empty() {
        conn.execute("ALTER SESSION SET CONTAINER = CDB$ROOT", &[])
            .ora()
            .map_err(|e| {
                e.context(
                    "oracle cdc: the capture user must be a COMMON user (C##…) with SET \
                     CONTAINER, LOGMINING and EXECUTE_CATALOG_ROLE granted CONTAINER=ALL",
                )
            })?;
    }
    let r = rows(
        conn,
        "SELECT TO_CHAR(dbid), db_unique_name, TO_CHAR(resetlogs_change#), log_mode, \
                supplemental_log_data_min FROM v$database",
    )?;
    let r = r.into_iter().next().unwrap_or_default();
    let cell = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
    anyhow::ensure!(
        cell(3) == "ARCHIVELOG",
        "oracle cdc: the database runs in {} mode; LogMiner capture needs ARCHIVELOG so redo \
         survives a log switch. As SYSDBA: SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE \
         ARCHIVELOG; ALTER DATABASE OPEN;",
        cell(3)
    );
    anyhow::ensure!(
        cell(4) == "YES" || cell(4) == "IMPLICIT",
        "oracle cdc: minimal supplemental logging is off, so LogMiner cannot decode DML. As a \
         DBA in CDB$ROOT: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA; (redo written before it \
         stays undecodable)"
    );
    Ok(OraIdentity {
        dbid: cell(0),
        db_unique_name: cell(1),
        resetlogs: cell(2),
        con_name,
        con_dbid,
    })
}

/// The oldest open transaction's start SCN (or the current SCN), read in the capture's OWN
/// container: from `CDB$ROOT` a common user's `V$TRANSACTION` shows only the root's rows
/// (measured), so it must be read in the PDB before the switch.
fn low_water_here(conn: &Connection) -> Result<u64> {
    scn(
        conn,
        "SELECT TO_CHAR(LEAST(NVL((SELECT MIN(start_scn) FROM v$transaction), current_scn), \
                current_scn)) FROM v$database",
    )
}

/// `(low_water, bound)`: the low-water mark read first (in the PDB), then the current SCN.
fn pin_frontier(conn: &Connection, low_water: u64) -> Result<Scns> {
    let commit_scn = scn(conn, "SELECT TO_CHAR(current_scn) FROM v$database")?;
    Ok(Scns {
        low_water,
        commit_scn,
    })
}

/// The available redo files of the current incarnation.
fn list_logs(conn: &Connection, resetlogs: &str) -> Result<(Vec<LogFile>, Vec<LogFile>)> {
    let parse = |r: Vec<Option<String>>| -> Option<LogFile> {
        let n = |i: usize| r.get(i)?.as_deref()?.parse::<u64>().ok();
        Some(LogFile {
            name: r.first()?.clone()?,
            thread: n(1)? as u32,
            sequence: n(2)?,
            first: n(3)?,
            next: n(4).unwrap_or(u64::MAX),
        })
    };
    let archived = rows(
        conn,
        &format!(
            "SELECT name, TO_CHAR(thread#), TO_CHAR(sequence#), TO_CHAR(first_change#), \
                    TO_CHAR(next_change#) FROM v$archived_log \
              WHERE status = 'A' AND deleted = 'NO' AND name IS NOT NULL \
                AND resetlogs_change# = {resetlogs} ORDER BY dest_id, thread#, sequence#"
        ),
    )?;
    let online = rows(
        conn,
        "SELECT MIN(f.member), TO_CHAR(l.thread#), TO_CHAR(l.sequence#), \
                TO_CHAR(l.first_change#), \
                CASE WHEN l.status = 'CURRENT' THEN NULL ELSE TO_CHAR(l.next_change#) END \
           FROM v$log l JOIN v$logfile f ON f.group# = l.group# \
          WHERE l.status <> 'UNUSED' GROUP BY l.thread#, l.sequence#, l.first_change#, \
                l.next_change#, l.status ORDER BY l.thread#, l.sequence#",
    )?;
    Ok((
        archived.into_iter().filter_map(parse).collect(),
        online.into_iter().filter_map(parse).collect(),
    ))
}

/// The contents query: one row per captured change, three value slots per column.
fn contents_sql(tables: &[Captured], con_name: &str, after_commit: u64) -> String {
    let slots = tables.iter().map(|t| t.columns.len()).max().unwrap_or(0);
    let is = |t: &Captured| {
        format!(
            "SEG_OWNER = {} AND TABLE_NAME = {}",
            lit(&t.owner),
            lit(&t.table)
        )
    };
    let per_slot = |j: usize, f: &dyn Fn(&str) -> String| {
        let arms: String = tables
            .iter()
            .filter_map(|t| {
                let (c, _) = t.columns.get(j)?;
                let spec = lit(&format!("{}.{}.{c}", t.owner, t.table));
                Some(format!(" WHEN {} THEN {}", is(t), f(&spec)))
            })
            .collect();
        format!("CASE{arms} END")
    };
    let mut select = vec![
        "TO_CHAR(COMMIT_SCN)".to_string(),
        "TO_CHAR(SEQUENCE#)".into(),
        "RAWTOHEX(XID)".into(),
        "OPERATION".into(),
        "SEG_OWNER".into(),
        "TABLE_NAME".into(),
        "TO_CHAR(STATUS)".into(),
        "INFO".into(),
    ];
    for j in 0..slots {
        select.push(per_slot(j, &|s| {
            format!(
                "TO_CHAR(SYS.DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE, {s}) * 2 \
                 + SYS.DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE, {s}))"
            )
        }));
        select.push(per_slot(j, &|s| {
            format!("SYS.DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, {s})")
        }));
        select.push(per_slot(j, &|s| {
            format!("SYS.DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, {s})")
        }));
    }
    let captured: Vec<String> = tables.iter().map(|t| format!("({})", is(t))).collect();
    let container = if con_name.is_empty() {
        String::new()
    } else {
        format!("SRC_CON_NAME = {} AND ", lit(con_name))
    };
    format!(
        "SELECT {} FROM V$LOGMNR_CONTENTS WHERE OPERATION = 'MISSING_SCN' OR ({container}\
         COMMIT_SCN > {after_commit} AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE', \
         'UNSUPPORTED') AND ({}))",
        select.join(", "),
        captured.join(" OR ")
    )
}

/// One mined row before it becomes an event.
struct Mined {
    commit: u64,
    sequence: u64,
    xid: String,
    event: ChangeEvent,
}

pub(crate) struct OracleChangeStream {
    conn: Connection,
    /// `None` when the window is empty by construction (an anchor-only run): nothing is mined.
    cursor: Option<Cursor>,
    tables: Vec<Captured>,
    identity: OraIdentity,
    /// Where mining started and what it had already delivered.
    from: Scns,
    /// The open-time frontier the checkpoint moves to once the drain is fully acknowledged.
    frontier: Scns,
    checkpoint: PathBuf,
    carry: Option<Mined>,
    queue: VecDeque<ChangeEvent>,
    exhausted: bool,
    outstanding: Option<Position>,
}

/// Which image a column value comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Redo,
    Undo,
}

/// `(before, after)` sources for one column of `op`, from its `COLUMN_PRESENT` bits (redo 2, undo 1);
/// `None` when an image the op carries has the column in neither side.
pub(crate) fn image_sides(op: ChangeOp, present: u8) -> Option<(Option<Side>, Option<Side>)> {
    let (redo, undo) = (present & 2 != 0, present & 1 != 0);
    let either = |first: Side| match (first, redo, undo) {
        (Side::Redo, true, _) | (Side::Undo, true, false) => Some(Side::Redo),
        (Side::Undo, _, true) | (Side::Redo, false, true) => Some(Side::Undo),
        _ => None,
    };
    match op {
        ChangeOp::Insert => redo.then_some((None, Some(Side::Redo))),
        ChangeOp::Delete => undo.then_some((Some(Side::Undo), None)),
        ChangeOp::Update => Some((Some(either(Side::Undo)?), Some(either(Side::Redo)?))),
    }
}

impl OracleChangeStream {
    /// Open a bounded drain of `tables` from the checkpoint to the SCN current at open.
    pub(crate) fn open(
        url: &str,
        tls: Option<&TlsConfig>,
        checkpoint: Option<&Path>,
        tables: &[String],
    ) -> Result<Self> {
        let checkpoint = checkpoint.ok_or_else(|| {
            anyhow::anyhow!(
                "oracle cdc: a checkpoint is required — the anchor is client-side, so without \
                 one every run would start at the current SCN and capture nothing"
            )
        })?;
        anyhow::ensure!(
            !tables.is_empty(),
            "oracle cdc: name the tables to capture (`table:` / `tables:`, or `--table`)"
        );
        let conn = connect(url, tls)?;
        for sql in SIGNED_YEAR_PIN {
            conn.execute(sql, &[]).ora()?;
        }
        let (_, con_name, con_dbid) = container(&conn)?;
        let captured = resolve_tables(&conn, tables)?;
        if let Some(why) = logging_check(&conn, &captured)? {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_PREREQUISITE,
                "oracle cdc: {why}"
            );
        }
        let low_water = low_water_here(&conn)?;
        let identity = root_identity(&conn, con_name, con_dbid)?;
        let frontier = pin_frontier(&conn, low_water)?;
        let from = match Position::load(checkpoint)? {
            Some(pos) => {
                let path = checkpoint.display().to_string();
                identity_verdict(OraIdentity::from_position(&pos).as_ref(), &identity).enforce()?;
                Scns::from_position(&pos, &path)?
            }
            None => {
                // An idle first run must still leave an anchor, or the next run starts at "now".
                with_identity(&frontier.position(), &identity).save(checkpoint)?;
                frontier
            }
        };
        let cursor = if from.commit_scn >= frontier.commit_scn {
            None
        } else {
            Some(start_mining(&conn, &identity, &captured, from, frontier)?)
        };
        Ok(Self {
            conn,
            exhausted: cursor.is_none(),
            cursor,
            tables: captured,
            identity,
            from,
            frontier,
            checkpoint: checkpoint.to_path_buf(),
            carry: None,
            queue: VecDeque::new(),
            outstanding: None,
        })
    }

    /// The next mined change, or `None` at the end of the window.
    fn next_mined(&mut self) -> Result<Option<Mined>> {
        let Some(row) = self.cursor.as_mut().and_then(Iterator::next) else {
            return Ok(None);
        };
        let row = row.ora()?;
        self.mined(&row).map(Some)
    }

    fn mined(&self, row: &Row) -> Result<Mined> {
        let text = |i: usize| row.get::<Option<String>>(i).ora();
        let op = text(3)?.unwrap_or_default();
        let (owner, table) = (text(4)?.unwrap_or_default(), text(5)?.unwrap_or_default());
        if op == "MISSING_SCN" {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_LOG_GAP,
                "oracle cdc: LogMiner reports missing redo ({}) — the changes it held are LOST to \
                 this stream. Restore the archived log, or delete the checkpoint (anchor first, \
                 then re-snapshot).",
                text(7)?.unwrap_or_default()
            );
        }
        let status = text(6)?.unwrap_or_default();
        if op == "UNSUPPORTED" || status != "0" {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_UNDECODABLE,
                "oracle cdc: LogMiner cannot decode a change to `{owner}.{table}` ({op}, status \
                 {status}: {}). A DDL on the table since this redo was written is the usual \
                 cause: the online dictionary decodes only the table's current shape. \
                 Re-snapshot the table (delete the checkpoint first so the stream anchors, then \
                 snapshot).",
                text(7)?.unwrap_or_default()
            );
        }
        let t = self
            .tables
            .iter()
            .find(|t| t.owner == owner && t.table == table)
            .ok_or_else(|| {
                anyhow::anyhow!("oracle cdc: mined a row of unexpected `{owner}.{table}`")
            })?;
        let op = match op.as_str() {
            "INSERT" => ChangeOp::Insert,
            "UPDATE" => ChangeOp::Update,
            _ => ChangeOp::Delete,
        };
        let mut before = Vec::with_capacity(t.columns.len());
        let mut after = Vec::with_capacity(t.columns.len());
        for (j, (name, kind)) in t.columns.iter().enumerate() {
            let present: u8 = text(8 + 3 * j)?.and_then(|s| s.parse().ok()).unwrap_or(0);
            let (b, a) = image_sides(op, present).ok_or_else(|| {
                anyhow::anyhow!(
                    "oracle cdc: a {op:?} of `{owner}.{table}` carries no value for {name} — its \
                     redo was written without ALL COLUMNS supplemental logging (enabled later, \
                     or dropped since). Writing NULL would be a silent wrong value; re-snapshot \
                     the table (delete the checkpoint first so the stream anchors, then snapshot)."
                )
            })?;
            let value = |side: Side| -> Result<RivetValue> {
                let raw = text(if side == Side::Redo { 9 } else { 10 } + 3 * j)?;
                match raw {
                    None => Ok(RivetValue::Null),
                    Some(s) => decode(*kind, &s)
                        .map_err(|e| anyhow::anyhow!("oracle cdc: `{owner}.{table}`.{name}: {e}")),
                }
            };
            if let Some(side) = b {
                before.push(value(side)?);
            }
            if let Some(side) = a {
                after.push(value(side)?);
            }
        }
        let commit: u64 = text(0)?.and_then(|s| s.parse().ok()).unwrap_or(0);
        Ok(Mined {
            commit,
            sequence: text(1)?.and_then(|s| s.parse().ok()).unwrap_or(0),
            xid: text(2)?.unwrap_or_default(),
            event: ChangeEvent {
                op,
                schema: t.ev_schema.clone(),
                table: t.ev_table.clone(),
                before: (op != ChangeOp::Insert).then_some(before),
                after: (op != ChangeOp::Delete).then_some(after),
                position: Position(serde_json::Value::Null),
                committed: false,
                image_names: Some(Arc::clone(&t.names)),
                seq: 0,
                poison: None,
            },
        })
    }

    /// Read every transaction of one commit SCN into the queue: a checkpoint lands only after all of them.
    fn fill(&mut self) -> Result<bool> {
        let first = match self.carry.take() {
            Some(m) => m,
            None => match self.next_mined()? {
                Some(m) => m,
                None => return Ok(false),
            },
        };
        let commit = first.commit;
        let mut bytes = first.event.estimated_bytes();
        let mut group = vec![first];
        loop {
            crate::source::cdc::check_tx_buffer_caps("oracle", group.len(), bytes)?;
            match self.next_mined()? {
                Some(m) if joins_commit_group(commit, m.commit) => {
                    bytes += m.event.estimated_bytes();
                    group.push(m);
                }
                Some(m) => {
                    self.carry = Some(m);
                    break;
                }
                None => break,
            }
        }
        let order = commit_group_order(
            &group
                .iter()
                .map(|m| (m.xid.as_str(), m.sequence))
                .collect::<Vec<_>>(),
        );
        let pos = Scns {
            low_water: self.from.low_water,
            commit_scn: commit,
        }
        .position();
        let mut slots: Vec<Option<ChangeEvent>> =
            group.into_iter().map(|m| Some(m.event)).collect();
        let mut events: Vec<ChangeEvent> =
            order.into_iter().filter_map(|i| slots[i].take()).collect();
        TxnFramer::close_group(&mut events, &pos);
        self.queue.extend(events);
        Ok(true)
    }

    fn save_frontier(&mut self) -> Result<()> {
        if frontier_is_due(
            self.exhausted,
            self.outstanding.is_some(),
            self.frontier,
            self.from,
        ) {
            with_identity(&self.frontier.position(), &self.identity).save(&self.checkpoint)?;
            self.from = self.frontier;
        }
        Ok(())
    }
}

/// Add the files covering `[from.low_water, frontier]`, start LogMiner, and open the contents query.
fn start_mining(
    conn: &Connection,
    identity: &OraIdentity,
    tables: &[Captured],
    from: Scns,
    frontier: Scns,
) -> Result<Cursor> {
    let (archived, online) = list_logs(conn, &identity.resetlogs)?;
    let files = plan_logs(&archived, &online, from.low_water, frontier.commit_scn)?;
    let adds: String = files
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let how = if i == 0 { "NEW" } else { "ADDFILE" };
            format!(
                "SYS.DBMS_LOGMNR.ADD_LOGFILE({}, SYS.DBMS_LOGMNR.{how}); ",
                lit(&f.name)
            )
        })
        .collect();
    conn.execute(
        &format!(
            "BEGIN {adds}SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => {}, ENDSCN => {}, \
             OPTIONS => SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG \
             + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;",
            from.low_water, frontier.commit_scn
        ),
        &[],
    )
    .ora()?;
    let missing = scalar(
        conn,
        "SELECT TO_CHAR(COUNT(*)) FROM v$logmnr_logs WHERE status = 4",
    )?;
    anyhow::ensure!(
        missing.as_deref() == Some("0"),
        "oracle cdc: LogMiner reports a missing log file inside SCN {}..{} — the changes it \
         held are LOST to this stream. Restore the archived log, or delete the checkpoint \
         (anchor first, then re-snapshot).",
        from.low_water,
        frontier.commit_scn
    );
    conn.query(
        &contents_sql(tables, &identity.con_name, from.commit_scn),
        &[],
    )
    .ora()
}

/// Whether a row committed at `next` belongs to the group being read for commit SCN `commit`.
pub(crate) fn joins_commit_group(commit: u64, next: u64) -> bool {
    next == commit
}

/// The order of one commit SCN's rows `(xid, sequence)`: each transaction stays contiguous in
/// arrival order, its rows by `SEQUENCE#`.
pub(crate) fn commit_group_order(rows: &[(&str, u64)]) -> Vec<usize> {
    let mut run = 0usize;
    let mut keys = Vec::with_capacity(rows.len());
    for (i, (xid, seq)) in rows.iter().enumerate() {
        if i > 0 && rows[i - 1].0 != *xid {
            run += 1;
        }
        keys.push((run, *seq));
    }
    let mut idx: Vec<usize> = (0..rows.len()).collect();
    idx.sort_by_key(|&i| keys[i]);
    idx
}

impl Drop for OracleChangeStream {
    fn drop(&mut self) {
        let _ = self
            .conn
            .execute("BEGIN SYS.DBMS_LOGMNR.END_LOGMNR; END;", &[]);
    }
}

impl ChangeStream for OracleChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        if self.queue.is_empty() && !self.exhausted {
            match self.fill() {
                Ok(true) => {}
                Ok(false) => self.exhausted = true,
                Err(e) => return Some(Err(e)),
            }
        }
        match self.queue.pop_front() {
            Some(ev) => {
                if ev.committed {
                    self.outstanding = Some(ev.position.clone());
                }
                Some(Ok(ev))
            }
            None => self.save_frontier().err().map(Err),
        }
    }

    fn ack(&mut self, position: &Position) -> Result<()> {
        if self.outstanding.as_ref() == Some(position) {
            self.outstanding = None;
        }
        self.save_frontier()
    }

    fn checkpoint_of(&self, position: &Position) -> Position {
        with_identity(position, &self.identity)
    }

    fn engine(&self) -> CdcEngine {
        CdcEngine::Oracle
    }
}

/// Doctor's view of the prerequisites: `(check, Ok(detail) | Err(problem))` per item.
pub(crate) fn prerequisites(
    url: &str,
    tls: Option<&TlsConfig>,
    tables: &[String],
) -> Result<Vec<(&'static str, std::result::Result<String, String>)>> {
    let conn = connect(url, tls)?;
    let (_, con_name, con_dbid) = container(&conn)?;
    let mut out = Vec::new();
    match resolve_tables(&conn, tables) {
        Ok(t) => {
            out.push(("CDC tables", Ok(format!("{} table(s) capturable", t.len()))));
            out.push((
                "CDC supplemental logging",
                match logging_check(&conn, &t)? {
                    None => Ok("ALL COLUMNS logging on every captured table".into()),
                    Some(why) => Err(why),
                },
            ));
        }
        Err(e) => out.push(("CDC tables", Err(format!("{e:#}")))),
    }
    out.push((
        "CDC redo mining",
        root_identity(&conn, con_name, con_dbid)
            .map(|id| {
                format!(
                    "ARCHIVELOG + minimal supplemental logging (DBID {})",
                    id.dbid
                )
            })
            .map_err(|e| format!("{e:#}")),
    ));
    Ok(out)
}

/// Whether a checkpoint file carries a usable resume position (doctor's check).
pub(crate) fn checkpoint_problem(pos: &Position, path: &str) -> Option<String> {
    Scns::from_position(pos, path).err().map(|e| e.to_string())
}

/// Anchor a first run: persist the open-time frontier so an idle first run still has a start.
pub(crate) fn pin_checkpoint_at_current(
    url: &str,
    tls: Option<&TlsConfig>,
    path: &Path,
) -> Result<()> {
    let conn = connect(url, tls)?;
    let (_, con_name, con_dbid) = container(&conn)?;
    let low_water = low_water_here(&conn)?;
    let identity = root_identity(&conn, con_name, con_dbid)?;
    with_identity(&pin_frontier(&conn, low_water)?.position(), &identity).save(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_read_back_in_the_batch_exports_plain_form() {
        for (mined, plain) in [
            ("-.5", "-0.5"),
            (".5", "0.5"),
            ("42", "42"),
            ("-0", "0"),
            (
                "1.00000000000000000000000000000000000000E+125",
                &format!("1{}", "0".repeat(125)),
            ),
            ("1.5E-003", "0.0015"),
            ("123.4500", "123.45"),
            ("100", "100"),
            ("-1.2E+002", "-120"),
        ] {
            assert_eq!(canonical_number(mined).as_deref(), Some(plain), "{mined}");
        }
        assert_eq!(canonical_number("1,5"), None);
        assert_eq!(canonical_number(""), None);
    }

    #[test]
    fn an_integral_number_is_an_int_and_a_fraction_stays_exact_text() {
        assert_eq!(
            decode(ColKind::Number, "123456789012345678").unwrap(),
            RivetValue::Int(123456789012345678)
        );
        assert_eq!(
            decode(ColKind::Number, "-.5").unwrap(),
            RivetValue::Bytes(b"-0.5".to_vec())
        );
        assert_eq!(
            decode(ColKind::Number, "99999999999999999999").unwrap(),
            RivetValue::Bytes(b"99999999999999999999".to_vec())
        );
    }

    #[test]
    fn timestamps_keep_microseconds_and_an_offset_moves_to_utc() {
        let dt = |s| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap();
        assert_eq!(
            parse_datetime("2026-09-28T01:02:03.123456789"),
            Some(dt("2026-09-28 01:02:03.123456"))
        );
        assert_eq!(
            parse_datetime("2026-09-28T01:02:03."),
            Some(dt("2026-09-28 01:02:03"))
        );
        assert_eq!(
            parse_datetime("2026-09-28T01:02:03.000000"),
            Some(dt("2026-09-28 01:02:03"))
        );
        assert_eq!(
            parse_datetime("2026-09-28T01:02:03.5"),
            Some(dt("2026-09-28 01:02:03.5"))
        );
        assert_eq!(parse_datetime("28-SEP-26 01.02.03"), None);
        // SYYYY: an AD year renders with a leading blank, a BC one with `-` (Oracle -N = chrono 1-N).
        assert_eq!(
            parse_datetime(" 2026-09-28T00:00:00.000000"),
            Some(dt("2026-09-28 00:00:00"))
        );
        assert_eq!(
            parse_datetime("-0044-03-15T00:00:00.000000"),
            Some(dt("-0043-03-15 00:00:00"))
        );
        // 1 BC is chrono year 0; DuckDB's make_date(0, 6, 15) is the independent value.
        assert_eq!(
            decode(ColKind::TimestampTz, "-0001-06-15T00:00:00.000000 +00:00").unwrap(),
            RivetValue::DateTime(
                chrono::DateTime::from_timestamp_micros(-62_152_876_800_000_000)
                    .unwrap()
                    .naive_utc()
            )
        );
        assert!(
            SIGNED_YEAR_PIN.iter().all(|s| s.contains("= 'SYYYY-")),
            "{SIGNED_YEAR_PIN:?}"
        );
        assert_eq!(
            decode(ColKind::TimestampTz, "2026-09-28T01:02:03.500000 +09:00").unwrap(),
            RivetValue::DateTime(dt("2026-09-27 16:02:03.5"))
        );
        assert_eq!(
            decode(ColKind::TimestampTz, "2026-09-28T01:02:03.000000 -03:30").unwrap(),
            RivetValue::DateTime(dt("2026-09-28 04:32:03"))
        );
    }

    #[test]
    fn floats_raw_and_text_decode_as_mined() {
        assert_eq!(
            decode(ColKind::Float, "1.5E+000").unwrap(),
            RivetValue::Float(1.5)
        );
        assert!(
            matches!(decode(ColKind::Float, "Nan").unwrap(), RivetValue::Float(f) if f.is_nan())
        );
        assert_eq!(
            decode(ColKind::Float, "-Inf").unwrap(),
            RivetValue::Float(f64::NEG_INFINITY)
        );
        assert_eq!(
            decode(ColKind::Raw, "deadbeef").unwrap(),
            RivetValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(
            decode(ColKind::Text, "it's, \"q\" ").unwrap(),
            RivetValue::Bytes(b"it's, \"q\" ".to_vec())
        );
        assert!(decode(ColKind::Raw, "xyz").is_err());
    }

    #[test]
    fn column_kinds_cover_the_preview_types_and_refuse_the_rest() {
        assert_eq!(
            ColKind::of("TIMESTAMP(6) WITH TIME ZONE"),
            Some(ColKind::TimestampTz)
        );
        assert_eq!(
            ColKind::of("TIMESTAMP(9) WITH LOCAL TIME ZONE"),
            Some(ColKind::Timestamp)
        );
        assert_eq!(ColKind::of("TIMESTAMP(6)"), Some(ColKind::Timestamp));
        assert_eq!(ColKind::of("NVARCHAR2"), Some(ColKind::Text));
        for refused in [
            "CLOB",
            "BLOB",
            "LONG",
            "XMLTYPE",
            "JSON",
            "INTERVAL DAY(2) TO SECOND(6)",
            "ROWID",
            "BOOLEAN",
        ] {
            assert_eq!(ColKind::of(refused), None, "{refused}");
        }
    }

    fn id(dbid: &str, rl: &str, con: &str) -> OraIdentity {
        OraIdentity {
            dbid: dbid.into(),
            db_unique_name: "FREE".into(),
            resetlogs: rl.into(),
            con_name: "PDB".into(),
            con_dbid: con.into(),
        }
    }

    #[test]
    fn a_checkpoint_from_another_database_incarnation_or_pdb_is_refused() {
        let server = id("1", "10", "7");
        assert_eq!(
            identity_verdict(Some(&id("1", "10", "7")), &server),
            IdentityVerdict::Ok
        );
        for other in [id("2", "10", "7"), id("1", "11", "7"), id("1", "10", "8")] {
            let e = identity_verdict(Some(&other), &server)
                .enforce()
                .unwrap_err();
            assert_eq!(crate::error::classify_exit(&e), 5, "{e}");
            let e = e.to_string();
            assert!(e.contains("anchors afresh FIRST, then re-snapshot"), "{e}");
        }
        assert!(matches!(
            identity_verdict(None, &server),
            IdentityVerdict::Unverifiable(_)
        ));
    }

    #[test]
    fn a_checkpoint_round_trips_its_scns_and_identity_and_a_hollow_one_is_refused() {
        let s = Scns {
            low_water: 5,
            commit_scn: 9,
        };
        let pos = with_identity(&s.position(), &id("1", "10", "7"));
        assert_eq!(Scns::from_position(&pos, "ck").unwrap(), s);
        assert_eq!(OraIdentity::from_position(&pos), Some(id("1", "10", "7")));
        let hollow = Position(serde_json::json!({"commit_scn": "9"}));
        let err = Scns::from_position(&hollow, "ck").unwrap_err().to_string();
        assert!(
            err.ends_with(&format!(
                "Restore the file, or: {}",
                crate::source::cdc::checkpoint_identity::RECOVER
            )),
            "the remedy must be anchor FIRST, then re-snapshot: {err}"
        );
        let inverted = Position(serde_json::json!({"low_water": "10", "commit_scn": "9"}));
        assert!(Scns::from_position(&inverted, "ck").is_err());
    }

    fn log(name: &str, seq: u64, first: u64, next: u64) -> LogFile {
        LogFile {
            name: name.into(),
            thread: 1,
            sequence: seq,
            first,
            next,
        }
    }

    #[test]
    fn logs_prefer_the_archive_and_cover_the_window() {
        let archived = [log("a1", 1, 100, 200), log("a2", 2, 200, 300)];
        let online = [log("o2", 2, 200, 300), log("o3", 3, 300, u64::MAX)];
        let got = plan_logs(&archived, &online, 250, 350).unwrap();
        let names: Vec<_> = got.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["a2", "o3"]);
    }

    #[test]
    fn a_start_before_the_oldest_log_or_a_sequence_hole_is_a_data_loss_refusal() {
        let e = plan_logs(
            &[log("a2", 2, 200, 300)],
            &[log("o3", 3, 300, u64::MAX)],
            150,
            350,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("LOST"), "{e}");
        let e = plan_logs(
            &[log("a1", 1, 100, 200)],
            &[log("o3", 3, 300, u64::MAX)],
            150,
            350,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("sequence 2"), "{e}");
    }

    #[test]
    fn the_frontier_moves_only_when_drained_acknowledged_and_ahead() {
        let at = |c| Scns {
            low_water: 1,
            commit_scn: c,
        };
        assert!(frontier_is_due(true, false, at(10), at(5)));
        assert!(!frontier_is_due(false, false, at(10), at(5)));
        assert!(!frontier_is_due(true, true, at(10), at(5)));
        assert!(!frontier_is_due(true, false, at(5), at(5)));
    }

    #[test]
    fn only_all_column_logging_gives_a_whole_row() {
        assert!(logging_gap("R", "T", false, &["PRIMARY KEY LOGGING".into()]).is_some());
        assert!(logging_gap("R", "T", false, &["ALL COLUMN LOGGING".into()]).is_none());
        assert!(logging_gap("R", "T", true, &[]).is_none());
    }

    #[test]
    fn a_column_missing_from_the_image_its_op_carries_is_refused_not_nulled() {
        use ChangeOp::*;
        assert_eq!(image_sides(Insert, 2), Some((None, Some(Side::Redo))));
        assert_eq!(image_sides(Insert, 1), None);
        assert_eq!(image_sides(Delete, 1), Some((Some(Side::Undo), None)));
        assert_eq!(image_sides(Delete, 2), None);
        assert_eq!(
            image_sides(Update, 3),
            Some((Some(Side::Undo), Some(Side::Redo)))
        );
        assert_eq!(
            image_sides(Update, 1),
            Some((Some(Side::Undo), Some(Side::Undo)))
        );
        assert_eq!(
            image_sides(Update, 2),
            Some((Some(Side::Redo), Some(Side::Redo)))
        );
        assert_eq!(image_sides(Update, 0), None);
    }

    #[test]
    fn a_commit_group_takes_only_its_own_scn() {
        assert!(joins_commit_group(7, 7));
        assert!(!joins_commit_group(7, 8));
    }

    #[test]
    fn one_commit_scn_keeps_each_transaction_whole_and_orders_its_rows() {
        let rows = [("B", 3), ("B", 2), ("A", 9), ("A", 1), ("C", 5)];
        assert_eq!(commit_group_order(&rows), vec![1, 0, 3, 2, 4]);
    }

    #[test]
    fn an_event_carries_the_configured_spelling_so_routing_matches_it() {
        assert_eq!(
            event_spelling("rivet.orders"),
            ("rivet".into(), "orders".into())
        );
        assert_eq!(event_spelling("ORDERS"), (String::new(), "ORDERS".into()));
    }
}
