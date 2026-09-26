//! CDC health probes for `rivet doctor` — automates the monitoring the CDC
//! reference tells operators to do by hand (docs/reference/cdc.md):
//!
//! - **PostgreSQL**: the export's slot (exists / active / retained WAL), plus
//!   *other* inactive slots pinning WAL — the number-one CDC foot-gun (an
//!   abandoned slot from a previous tool fills the source disk).
//! - **MySQL**: binlog server config (`log_bin`, `binlog_format=ROW`,
//!   `binlog_row_image=FULL`), and the checkpoint's binlog file still being
//!   retained (`SHOW BINARY LOGS`) — a purged file means ERROR 1236 on the
//!   next run, and doctor should say so *before* the run.
//! - **SQL Server**: CDC enabled on the database, the capture instance
//!   existing, the checkpoint LSN still above `fn_cdc_get_min_lsn` (cleanup
//!   retention), and the Agent service running (a stopped Agent freezes the
//!   change tables and blocks log truncation).
//!
//! Pure verdict functions (unit-tested offline) are separated from the
//! per-engine IO gather functions, mirroring `validate.rs`'s pure/IO split.

use crate::config::{Config, DEFAULT_PG_SLOT, ExportConfig, ExportMode, SourceType};
use crate::error::Result;

use super::doctor::DoctorCheck;

/// A slot pinning more retained WAL than this fails the check — at typical
/// scheduler cadence (minutes) a healthy slot holds MBs; a GiB means the
/// consumer stopped and the disk is filling.
const PG_RETAINED_WAL_FAIL_BYTES: i64 = 1 << 30; // 1 GiB

/// The bar, with a test seam.
///
/// Crossing 1 GiB of real WAL takes minutes of writes, so a live test that wanted
/// to prove the run REACHES this check had to either burn that time or assert
/// nothing — and the version that asserted nothing is the one that would have
/// shipped. `RIVET_TEST_SLOT_WAL_BAR` lets a test cross the bar with a KiB.
///
/// Deliberately not a config knob: an operator lowering this would get a warning on
/// every ordinary backlog and learn to ignore it, which costs more than the warning
/// is worth. Same reasoning as the fault hooks — a seam for tests, not a feature.
fn retained_wal_bar() -> i64 {
    std::env::var("RIVET_TEST_SLOT_WAL_BAR")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(PG_RETAINED_WAL_FAIL_BYTES)
}

fn mib(bytes: i64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn check(name: String, ok: bool, detail: Option<String>, hint: Option<String>) -> DoctorCheck {
    DoctorCheck {
        name,
        ok,
        detail,
        hint,
    }
}

fn probe_failed(e: &anyhow::Error) -> DoctorCheck {
    check(
        "CDC health probe".into(),
        false,
        Some(super::doctor::trim_probe_error(e)),
        Some(
            "the CDC probe stopped at this error — fix what it reports; any CDC checks listed \
             above this line already ran and still apply"
                .into(),
        ),
    )
}

/// Entry point: every CDC health check for the config, or empty when the
/// config has no `mode: cdc` exports. A connection failure becomes a single
/// failed check rather than aborting doctor.
pub(super) fn collect(config: &Config, config_dir: &std::path::Path) -> Vec<DoctorCheck> {
    let cdc: Vec<&ExportConfig> = config
        .exports
        .iter()
        .filter(|e| e.mode == ExportMode::Cdc)
        .collect();
    if cdc.is_empty() {
        return Vec::new();
    }
    let url = match config.source.resolve_url() {
        Ok(u) => u,
        Err(e) => return vec![probe_failed(&e)],
    };
    let tls = config.source.tls.as_ref();
    // The engines APPEND to one vec rather than returning their own, so a probe that
    // dies half-way keeps the verdicts it already reached. This used to be
    // `Result<Vec<_>>` + `unwrap_or_else`, which REPLACED every collected verdict with
    // the generic probe failure: against a live MySQL 8.4 replica with `log_bin=OFF`
    // (2026-09-16) doctor graded "log_bin is OFF — enable binary logging", then threw
    // that away when the next query (`SHOW BINARY LOGS`) failed with ERROR 1381, and
    // printed a hint blaming source auth — which had passed. The operator fixed the
    // grants, not the binlog.
    let mut checks = Vec::new();
    let result = match config.source.source_type {
        SourceType::Postgres => pg_checks(&url, tls, &cdc, config_dir, &mut checks),
        SourceType::Mysql => mysql_checks(&url, tls, &cdc, config_dir, &mut checks),
        SourceType::Mssql => mssql_checks(&url, tls, &cdc, config_dir, &mut checks),
        // Change streams: probe the replica-set requirement + declare the capture
        // fidelity tier (6.0+ pre/post-images vs current-state UpdateLookup).
        SourceType::Mongo => mongo_checks(&url, tls, &cdc, config_dir, &mut checks),
        SourceType::Oracle => Err(anyhow::anyhow!("CDC is not supported for Oracle yet")),
    };
    if let Err(e) = result {
        checks.push(probe_failed(&e));
    }
    checks
}

// ─── PostgreSQL ──────────────────────────────────────────────────────────────

struct PgSlot {
    active: bool,
    retained_bytes: i64,
}

/// Verdict for the export's own slot. Absent is healthy (created on first
/// run); present is healthy while the retained WAL stays small.
/// Does this slot's retained WAL warrant telling the operator? Shared by `doctor`
/// (which FAILS on it) and by every `run` (which WARNS) so the threshold and the
/// wording have ONE definition. Two would drift on the first change, and the
/// run-time one is the copy nobody would notice going stale.
///
/// `None` below the threshold: a slot legitimately holds everything since the last
/// run, and a warning on every ordinary backlog is a warning that stops being read.
pub(crate) fn pg_retained_wal_warning(
    slot: &str,
    retained_bytes: i64,
    active: bool,
) -> Option<String> {
    if retained_bytes < retained_wal_bar() {
        return None;
    }
    Some(format!(
        "slot '{slot}' is pinning {} of WAL (active={active}) — the source disk is filling. \
         This run will drain it, but a drain that far behind takes time and the WAL keeps \
         growing meanwhile. If capture here is retired instead: \
         SELECT pg_drop_replication_slot('{slot}'); consider max_slot_wal_keep_size as a \
         blast-radius bound",
        mib(retained_bytes)
    ))
}

/// The same question for slots the CALLER did not name as its own.
///
/// Two callers with different knowledge, so this reports a FACT and leaves the
/// verdict to them. `doctor` sees the whole config and can name every CDC slot it
/// owns, so a leftover really is a leftover and it offers the drop command. A
/// `run` sees ONE export (`CdcCapture` carries a single `cdc_cfg`), so from there a
/// sibling export's slot — drained by the same `rivet run` a moment later — is
/// indistinguishable from an abandoned one. Telling that operator "nothing is
/// draining them, drop it" destroys a live resume anchor, which is why the run
/// path passes `may_be_owned_elsewhere` and gets wording without a verdict.
///
/// `NOT active` is not evidence of abandonment either, and specifically not for
/// rivet: the PostgreSQL adapter reads through `pg_logical_slot_peek_changes`, so
/// its own slots sit `active = false` between runs BY CONSTRUCTION. Whatever this
/// says has to survive that.
///
/// The SUM matters as much as the max. The hazard is a filling disk, and a disk is
/// filled by the total: eight slots at 334 MiB each is 2 GiB of pinned WAL that a
/// max-only test calls "small". Measured on a dev stand, where exactly that set
/// reported OK.
///
/// `slot_type` rides the listing because a PHYSICAL slot belongs to a standby, not
/// to a CDC consumer — it pins WAL just the same and is worth reporting, but
/// dropping one breaks replication, so it must be visibly not a leftover.
pub(crate) fn pg_foreign_slots_warning(
    foreign: &[(String, i64, String)],
    may_be_owned_elsewhere: bool,
) -> Option<String> {
    let bar = retained_wal_bar();
    let total: i64 = foreign.iter().map(|(_, b, _)| *b).sum();
    // The empty-list early return, said explicitly. It used to ride on the `?` of a
    // `max_by_key` whose value the guard below then compared — and when that
    // comparison turned out to be redundant, removing it would have taken the
    // early return with it silently. A `?` doing two jobs is one job too many.
    if foreign.is_empty() {
        return None;
    }
    // `total < bar` ALONE, and the removed `worst.1 < bar &&` is why: `total` is the
    // sum and `worst` the max over the same non-negative list, so `total >= worst.1`
    // always — the first clause can never decide anything the second does not.
    //
    // Found by mutation testing rather than by reading: `replace < with <=` on the
    // first clause survived, and no fixture could kill it. Reaching it needs
    // `worst.1 == bar` together with `total < bar`, which is arithmetically
    // impossible here. An unkillable mutant on a redundant clause is the clause
    // asking to be deleted, not an exclusion to be written into mutants.toml.
    if total < bar {
        return None;
    }
    // Everything that contributes to a total past the bar is worth naming; a
    // listing pruned to the offenders is unreadable when the finding IS the count.
    let mut sorted: Vec<&(String, i64, String)> = foreign.iter().collect();
    sorted.sort_by_key(|(_, b, _)| -*b);
    let listing = sorted
        .iter()
        .take(10)
        .map(|(n, b, kind)| {
            if kind == "physical" {
                format!(
                    "{n} ({}, PHYSICAL — a standby's, not a CDC leftover)",
                    mib(*b)
                )
            } else {
                format!("{n} ({})", mib(*b))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let more = foreign.len().saturating_sub(10);
    let tail = if more > 0 {
        format!(" and {more} more")
    } else {
        String::new()
    };
    let head = format!(
        "inactive slot(s) are pinning WAL: {listing}{tail} — {} across {} slot(s)",
        mib(total),
        foreign.len()
    );
    Some(if may_be_owned_elsewhere {
        format!(
            "{head}. This run drains ONE export's slot and cannot tell whether another \
             export in your config, or another tool, owns the rest — `rivet doctor -c \
             <your config>` sees them all and will say which are genuinely unclaimed. \
             Do NOT drop a slot on this warning alone"
        )
    } else {
        format!(
            "{head}, and none belongs to this config. If the consumer is gone for good: \
             SELECT pg_drop_replication_slot('{}') — but never for a slot marked \
             PHYSICAL, which is a standby's",
            sorted
                .iter()
                .find(|(_, _, k)| k != "physical")
                .map(|(n, _, _)| n.as_str())
                .unwrap_or("<slot>")
        )
    })
}

/// `resume_ckpt`: a checkpoint file exists AND carries a position — the SAME
/// decision the run makes (`cdc_job.rs`, `Position::load(p)?.is_some()`), not a
/// second copy of it.
///
/// Without it this arm reported a PASSING "slot absent — created on the first
/// run" for a slot that had been dropped or invalidated under an existing
/// checkpoint, and the very next `rivet run` hard-refused
/// (`source/postgres/cdc.rs`). `doctor && run` — the order `init` itself prints —
/// gave a green light and then a wall. Postgres was the one engine missing this:
/// MySQL, SQL Server and Mongo each load the checkpoint through `Position::load`
/// in this file, and the MSSQL arm's comment records the same defect MEASURED
/// there. The message below is the run's own, verbatim, so the operator is told
/// the same thing twice rather than two different things.
fn pg_slot_verdict(
    export: &str,
    slot: &str,
    state: Option<PgSlot>,
    resume_ckpt: bool,
) -> DoctorCheck {
    let name = format!("CDC slot '{slot}' (export '{export}')");
    match state {
        None if resume_ckpt => check(
            name,
            false,
            Some(
                "slot is missing but a resume checkpoint exists — the slot was dropped or \
                 invalidated, and the changes since then are no longer in the log. Recover in \
                 rivet's OWN order: delete the checkpoint file so the next run pins a fresh slot \
                 at the current WAL position, THEN re-snapshot the table (mode: full). \
                 Snapshotting first leaves everything changed between the snapshot and the new \
                 slot in neither."
                    .into(),
            ),
            None,
        ),
        None => check(
            name,
            true,
            Some("slot absent — created on the first run".into()),
            None,
        ),
        Some(s) if s.retained_bytes < PG_RETAINED_WAL_FAIL_BYTES => check(
            name,
            true,
            Some(format!(
                "retained WAL {}, active={}",
                mib(s.retained_bytes),
                s.active
            )),
            None,
        ),
        // The threshold and the wording come from the shared warning so `doctor` and
        // `run` cannot disagree about when a slot is too far behind.
        Some(s) => check(
            name,
            false,
            pg_retained_wal_warning(slot, s.retained_bytes, s.active),
            Some(
                "run the CDC export to drain it (advancing the slot releases WAL), or drop it \
                 if capture is retired: SELECT pg_drop_replication_slot('<slot>'); consider \
                 max_slot_wal_keep_size as a blast-radius bound"
                    .into(),
            ),
        ),
    }
}

/// Verdict over *other* inactive slots on the instance — not this config's,
/// but they pin WAL on the same disk (the abandoned-slot foot-gun).
fn pg_foreign_slots_verdict(foreign: &[(String, i64, String)]) -> DoctorCheck {
    let name = "CDC other inactive slots".to_string();
    if foreign.is_empty() {
        return check(name, true, Some("none".into()), None);
    }
    // `may_be_owned_elsewhere = false`: doctor was handed the whole config and
    // excluded every CDC slot in it, so what is left really is unclaimed by rivet
    // and the drop command is safe to offer. The run path cannot say that.
    match pg_foreign_slots_warning(foreign, false) {
        None => {
            // NAMES and the total. The names were here before and an operator wants
            // them — a lingering slot is worth knowing about while it is still
            // small. The total is the addition: reporting only the max is what let
            // 1.96 GiB across eight slots read as "small".
            let total: i64 = foreign.iter().map(|(_, b, _)| *b).sum();
            let listing = foreign
                .iter()
                .map(|(n, b, _)| format!("{n} ({})", mib(*b)))
                .collect::<Vec<_>>()
                .join(", ");
            check(
                name,
                true,
                Some(format!(
                    "inactive but small: {listing} — {} across {} slot(s)",
                    mib(total),
                    foreign.len()
                )),
                None,
            )
        }
        Some(why) => check(name, false, Some(why), None),
    }
}

fn pg_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let mut ours: Vec<String> = Vec::new();
    for e in exports {
        let slot = e
            .cdc
            .as_ref()
            .and_then(|c| c.slot.clone())
            .unwrap_or_else(|| DEFAULT_PG_SLOT.to_string());
        let row = client.query_opt(
            "SELECT active, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )?;
        let state = row.map(|r| PgSlot {
            active: r.get(0),
            retained_bytes: r.get(1),
        });
        // The run's own resume decision, read the same way it reads it — the three
        // sibling engines in this file already do exactly this.
        let resume_ckpt = match e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) {
            None => false,
            Some(raw) => {
                let p = &crate::source::cdc::resolve_checkpoint(raw, config_dir);
                crate::source::cdc::Position::load(p)?.is_some()
            }
        };
        checks.push(pg_slot_verdict(&e.name, &slot, state, resume_ckpt));
        ours.push(slot);
    }
    let rows = client.query(
        "SELECT slot_name, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint, \
                slot_type \
         FROM pg_replication_slots WHERE NOT active AND slot_name <> ALL($1)",
        &[&ours],
    )?;
    let foreign: Vec<(String, i64, String)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    checks.push(pg_foreign_slots_verdict(&foreign));
    Ok(())
}

// ─── MySQL ───────────────────────────────────────────────────────────────────

/// `true` when the server has binary logging OFF — the one state in which the
/// retention probe (`SHOW BINARY LOGS`) answers with ERROR 1381 instead of a log
/// list. Pure, so the skip decision below is graded offline even though the probe
/// it guards is live-only. MySQL renders the variable as `ON`/`OFF` or `1`/`0`
/// depending on how it is asked; both spellings mean the same thing.
fn binlog_disabled(vars: &[(String, String)]) -> bool {
    let log_bin = vars
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("log_bin"))
        .map(|(_, v)| v.as_str())
        .unwrap_or("OFF");
    !log_bin.eq_ignore_ascii_case("ON") && log_bin != "1"
}

/// The binlog server config CDC needs; anything else breaks capture quietly
/// (STATEMENT rows never arrive; MINIMAL drops the unchanged columns the
/// after-image / MERGE shape requires).
fn mysql_binlog_config_verdict(vars: &[(String, String)]) -> DoctorCheck {
    let get = |k: &str| {
        vars.iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.as_str())
    };
    let name = "CDC binlog server config".to_string();
    let format = get("binlog_format").unwrap_or("?");
    let row_image = get("binlog_row_image").unwrap_or("FULL");
    if binlog_disabled(vars) {
        return check(
            name,
            false,
            Some("log_bin is OFF — there is no binlog to capture".into()),
            Some("enable binary logging (log_bin=ON) and restart".into()),
        );
    }
    if !format.eq_ignore_ascii_case("ROW") {
        return check(
            name,
            false,
            Some(format!(
                "binlog_format={format} — rivet needs row images; STATEMENT/MIXED will not work"
            )),
            Some("SET GLOBAL binlog_format=ROW (and my.cnf for restarts)".into()),
        );
    }
    if !row_image.eq_ignore_ascii_case("FULL") {
        return check(
            name,
            false,
            Some(format!(
                "binlog_row_image={row_image} — the after-image / MERGE shape needs FULL; \
                 MINIMAL drops unchanged columns"
            )),
            Some("SET GLOBAL binlog_row_image=FULL".into()),
        );
    }
    check(
        name,
        true,
        Some("log_bin=ON, binlog_format=ROW, binlog_row_image=FULL".into()),
        None,
    )
}

enum MysqlCkpt {
    /// `cdc.checkpoint` not configured at all — every bounded run re-anchors
    /// to "now" and changes between runs are skipped. A config misfire.
    NoPathConfigured,
    /// Path configured, file not written yet (no run has happened).
    NotYetWritten,
    Loaded {
        file: String,
        pos: u64,
    },
}

/// Verdict for one export's checkpoint vs the server's retained binlogs.
fn mysql_ckpt_verdict(export: &str, ckpt: MysqlCkpt, logs: &[(String, u64)]) -> DoctorCheck {
    let name = format!("CDC checkpoint (export '{export}')");
    match ckpt {
        MysqlCkpt::NoPathConfigured => check(
            name,
            false,
            Some(
                "no `cdc.checkpoint` configured — MySQL has no server-side anchor, so every \
                 bounded run re-anchors to the current position and changes between runs are \
                 silently skipped"
                    .into(),
            ),
            Some("set cdc.checkpoint to a persistent path".into()),
        ),
        MysqlCkpt::NotYetWritten => check(
            name,
            true,
            Some("no checkpoint yet — the first run pins the open position".into()),
            None,
        ),
        MysqlCkpt::Loaded { file, pos } => {
            let Some(idx) = logs.iter().position(|(f, _)| *f == file) else {
                return check(
                    name,
                    false,
                    Some(format!(
                        "checkpoint {file}:{pos} is below binlog retention (file purged) — the \
                         next run fails with ERROR 1236"
                    )),
                    Some(
                        "restart CDC from a fresh checkpoint FIRST, then re-snapshot \
                         (mode: full) — snapshotting first leaves the changes in between \
                         in neither; size binlog retention above your CDC cadence"
                            .into(),
                    ),
                );
            };
            let lag: i64 = (logs[idx].1 as i64 - pos as i64).max(0)
                + logs[idx + 1..].iter().map(|(_, s)| *s as i64).sum::<i64>();
            check(
                name,
                true,
                Some(format!("{file}:{pos}, backlog ≈ {}", mib(lag))),
                None,
            )
        }
    }
}

fn mysql_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    use mysql::prelude::Queryable;
    let pool = crate::source::mysql::connect_pool(url, tls)?;
    let mut conn = pool.get_conn()?;

    let vars: Vec<(String, String)> = conn.query(
        "SHOW GLOBAL VARIABLES WHERE Variable_name IN \
         ('log_bin','binlog_format','binlog_row_image')",
    )?;
    checks.push(mysql_binlog_config_verdict(&vars));
    // Binlog OFF ⇒ stop here. The retention probe below (`SHOW BINARY LOGS`) fails
    // with ERROR 1381 "You are not using binary logging", which says strictly less
    // than the verdict just pushed, and every checkpoint verdict needs the log list
    // it would have returned. Returning leaves the operator one actionable line with
    // nothing contradicting it.
    if binlog_disabled(&vars) {
        return Ok(());
    }

    // SHOW BINARY LOGS: Log_name, File_size (+ Encrypted on 8.0.14+); take the
    // first two columns positionally so the extra column never breaks the map.
    let logs: Vec<(String, u64)> = conn
        .query_iter("SHOW BINARY LOGS")?
        .filter_map(|r| r.ok())
        .filter_map(|row| {
            let file: Option<String> = row.get(0);
            let size: Option<u64> = row.get(1);
            Some((file?, size?))
        })
        .collect();

    for e in exports {
        let ckpt = match e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) {
            None => MysqlCkpt::NoPathConfigured,
            Some(raw) => {
                let p = &crate::source::cdc::resolve_checkpoint(raw, config_dir);
                match crate::source::cdc::Position::load(p)? {
                    None => MysqlCkpt::NotYetWritten,
                    // The SAME decoder the run uses. This read `file` with
                    // `.unwrap_or_default()` and `pos` with `.unwrap_or(0)`, so a
                    // malformed checkpoint rendered as `Loaded { file: "", pos: 0 }` and
                    // was graded "below binlog retention (file purged) … ERROR 1236" with
                    // a RE-SNAPSHOT hint — the wrong cause and a destructive remedy for a
                    // file the run rejects as `checkpoint missing 'file'`.
                    Some(pos) => {
                        match crate::source::mysql::cdc::MysqlChangeStream::resume_from_checkpoint(
                            Some(&pos),
                            &p.display().to_string(),
                        ) {
                            Ok(Some((file, at))) => MysqlCkpt::Loaded { file, pos: at },
                            Ok(None) => MysqlCkpt::NotYetWritten,
                            // The malformed-file FAIL is the whole answer. Falling
                            // through to `mysql_ckpt_verdict` printed a SECOND check under
                            // the same name saying "no `cdc.checkpoint` configured" with a
                            // hint to set one — factually false (a path IS configured) and
                            // a no-op remedy the operator has already applied. Two
                            // contradictory verdicts on one file is worse than either.
                            Err(why) => {
                                checks.push(check(
                                    format!("CDC checkpoint (export '{}')", e.name),
                                    false,
                                    Some(why.to_string()),
                                    Some(
                                        "restore the file, or delete it to accept a fresh \
                                     anchor at the current binlog position"
                                            .into(),
                                    ),
                                ));
                                continue;
                            }
                        }
                    }
                }
            }
        };
        checks.push(mysql_ckpt_verdict(&e.name, ckpt, &logs));
    }
    Ok(())
}

// ─── SQL Server ──────────────────────────────────────────────────────────────

/// Normalise an LSN hex string (`0x…` or bare, any case) to a fixed-width
/// uppercase form so string comparison equals numeric comparison.
fn norm_lsn(s: &str) -> String {
    let h = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    format!("{:0>20}", h.to_ascii_uppercase())
}

struct MssqlHealth {
    cdc_enabled: bool,
    /// The capture instance's min LSN (hex) — `None` ⇒ instance unknown.
    instance_min_lsn: Option<String>,
    /// `None` ⇒ could not verify (no VIEW SERVER STATE).
    agent_running: Option<bool>,
}

fn mssql_verdicts(
    export: &str,
    ci: Option<&str>,
    health: &MssqlHealth,
    ckpt_lsn: Option<Option<String>>, // outer None = no path configured; inner = file state
) -> Vec<DoctorCheck> {
    let mut out = Vec::new();
    if !health.cdc_enabled {
        out.push(check(
            format!("CDC enabled on database (export '{export}')"),
            false,
            Some("sys.fn_cdc_get_max_lsn() is NULL — CDC is not enabled".into()),
            Some(
                "EXEC sys.sp_cdc_enable_db (requires db_owner); Express/Web editions have no CDC"
                    .into(),
            ),
        ));
        return out;
    }
    let Some(ci) = ci else {
        out.push(check(
            format!("CDC capture instance (export '{export}')"),
            false,
            Some("`cdc.capture_instance` is not set — required for sqlserver://".into()),
            Some("set cdc.capture_instance (e.g. dbo_orders) to the instance created by sp_cdc_enable_table".into()),
        ));
        return out;
    };
    match &health.instance_min_lsn {
        None => out.push(check(
            format!("CDC capture instance '{ci}' (export '{export}')"),
            false,
            Some("fn_cdc_get_min_lsn returned NULL — the capture instance does not exist".into()),
            Some("EXEC sys.sp_cdc_enable_table @capture_instance=… for the table".into()),
        )),
        Some(min) => {
            out.push(check(
                format!("CDC capture instance '{ci}' (export '{export}')"),
                true,
                None,
                None,
            ));
            match ckpt_lsn {
                None => out.push(check(
                    format!("CDC checkpoint (export '{export}')"),
                    false,
                    Some(
                        "no `cdc.checkpoint` configured — every run re-reads the full retained \
                         change window (duplicates on every cycle, no resume)"
                            .into(),
                    ),
                    Some("set cdc.checkpoint to a persistent path".into()),
                )),
                Some(None) => out.push(check(
                    format!("CDC checkpoint (export '{export}')"),
                    true,
                    Some("no checkpoint yet — the first run starts at the retained minimum".into()),
                    None,
                )),
                Some(Some(ckpt)) => {
                    if norm_lsn(&ckpt) < norm_lsn(min) {
                        out.push(check(
                            format!("CDC checkpoint (export '{export}')"),
                            false,
                            Some(format!(
                                "checkpoint LSN {ckpt} is below the retained minimum {min} — the \
                                 cleanup job removed changes past it; the next run fails loudly"
                            )),
                            Some(
                                "restart CDC from a fresh checkpoint, THEN re-snapshot (mode: full) — \
                                 snapshotting first leaves the changes in between in neither"
                                    .into(),
                            ),
                        ));
                    } else {
                        out.push(check(
                            format!("CDC checkpoint (export '{export}')"),
                            true,
                            Some(format!("LSN {ckpt} within retention")),
                            None,
                        ));
                    }
                }
            }
        }
    }
    out.push(match health.agent_running {
        Some(true) => check("CDC Agent (SQL Server Agent)".into(), true, None, None),
        Some(false) => check(
            "CDC Agent (SQL Server Agent)".into(),
            false,
            Some(
                "the Agent service is not running — change tables are frozen and the \
                 transaction log cannot truncate"
                    .into(),
            ),
            Some("start the SQL Server Agent service (the capture job lives there)".into()),
        ),
        None => check(
            "CDC Agent (SQL Server Agent)".into(),
            true,
            Some(
                "could not verify (needs VIEW SERVER STATE) — watch for a non-advancing max LSN"
                    .into(),
            ),
            None,
        ),
    });
    out
}

fn mssql_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    let mut src = crate::source::mssql::MssqlSource::connect_with_tls(url, tls)?;
    for e in exports {
        let ci = e.cdc.as_ref().and_then(|c| c.capture_instance.as_deref());
        let health = src.cdc_health(ci)?;
        // The SAME decision the run makes, not a second copy of it. This used to
        // reach for `lsn` with `.and_then`, so a checkpoint that exists but carries
        // no readable position collapsed to `Some(None)` and rendered as a PASSING
        // "no checkpoint yet — the first run starts at the retained minimum".
        // MEASURED: `doctor` exit 0 and `check` saying "Looks good" on the exact
        // file `run` then hard-refuses. Both preflight claims were false — the file
        // is present, and the run does not start at the minimum.
        //
        // `resume_from_checkpoint` is that decision, and it is where the reason
        // lives; a failure here carries its message verbatim so the operator is
        // told the same thing twice rather than two different things.
        let mut ckpt_error: Option<String> = None;
        let ckpt_state = match e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) {
            None => None,
            Some(raw) => {
                let p = &crate::source::cdc::resolve_checkpoint(raw, config_dir);
                let pos = crate::source::cdc::Position::load(p)?;
                match crate::source::mssql::cdc::resume_from_checkpoint(
                    pos.as_ref(),
                    &p.display().to_string(),
                ) {
                    Ok(r) => Some(r.from_lsn),
                    Err(e) => {
                        ckpt_error = Some(e.to_string());
                        Some(None)
                    }
                }
            }
        };
        let mssql_health = MssqlHealth {
            cdc_enabled: health.cdc_enabled,
            instance_min_lsn: health.instance_min_lsn,
            agent_running: health.agent_running,
        };
        // A checkpoint the RUN would refuse is a FAIL here, carrying the run's own
        // message — not a passing "no checkpoint yet".
        if let Some(why) = ckpt_error {
            checks.push(check(
                format!("CDC checkpoint (export '{}')", e.name),
                false,
                Some(why),
                Some(
                    "restore the file, or delete it to accept a fresh anchor from the \
                     retained minimum"
                        .into(),
                ),
            ));
        }
        checks.extend(mssql_verdicts(&e.name, ci, &mssql_health, ckpt_state));
    }
    Ok(())
}

// ─── MongoDB ─────────────────────────────────────────────────────────────────

/// Change-stream health: the replica-set requirement (a standalone cannot
/// `watch()`) and the DECLARED capture-fidelity tier — so an operator learns
/// before the run that a sub-6.0 server gives current-state post-images and
/// key-only deletes, never discovering it as a silent null in the output.
fn mongo_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
    config_dir: &std::path::Path,
    checks: &mut Vec<DoctorCheck>,
) -> Result<()> {
    let cap = crate::source::mongo::cdc::probe_capability(url, tls)?;

    // The checkpoint, which this took as `_exports` and never read. `create_change_
    // stream`'s Mongo arm loads it and `Position::load` HARD-FAILS on a corrupt file,
    // so the exact defect measured on SQL Server today — `doctor` exit 0 and `check`
    // saying "Looks good" on a file the run then refuses — was live here too, on the
    // engine whose resume position is a driver token nobody can hand-repair.
    for e in exports {
        let Some(raw) = e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) else {
            continue;
        };
        let path = &crate::source::cdc::resolve_checkpoint(raw, config_dir);
        let name = format!("CDC checkpoint (export '{}')", e.name);
        match crate::source::cdc::Position::load(path) {
            Ok(None) => checks.push(check(
                name,
                true,
                Some("not written yet — the first run pins its own anchor".into()),
                None,
            )),
            Ok(Some(pos)) => match crate::source::mongo::cdc::decode_resume_token(&pos.0) {
                Ok(_) => checks.push(check(
                    name,
                    true,
                    Some("resume token readable".into()),
                    None,
                )),
                Err(why) => checks.push(check(
                    name,
                    false,
                    Some(format!("{why} — the run refuses this file")),
                    Some(
                        "restore the checkpoint, or delete it to accept a fresh anchor at \
                         the current cluster time (which SKIPS everything since it was \
                         written — re-snapshot if that gap matters)"
                            .into(),
                    ),
                )),
            },
            Err(why) => checks.push(check(
                name,
                false,
                Some(why.to_string()),
                Some("restore the file, or delete it to accept a fresh anchor".into()),
            )),
        }
    }
    // Hard requirement: change streams need a replica set.
    checks.push(check(
        "CDC replica set".into(),
        cap.is_replica_set,
        Some(if cap.is_replica_set {
            format!("replica set (server {})", cap.server_version)
        } else {
            format!(
                "server {} is standalone — change streams unavailable",
                cap.server_version
            )
        }),
        (!cap.is_replica_set).then(|| {
            "MongoDB change streams require a replica set (a single-node one is fine): restart \
             mongod with --replSet and run rs.initiate()"
                .to_string()
        }),
    ));
    // The fidelity tier — informational (never a failure), but hinted for upgrade
    // on the degraded tier so the degrade is declared, not silent.
    checks.push(check(
        "CDC capture tier".into(),
        true,
        Some(cap.tier().to_string()),
        (cap.major < 6).then(|| {
            "upgrade to MongoDB 6.0+ and enable changeStreamPreAndPostImages for point-in-time \
             post-images and delete pre-images"
                .to_string()
        }),
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `collect` is the doctor's CDC entry point: nothing for a config without a
    /// stream, and — when the probe cannot even resolve the source — ONE failed
    /// check that says so, never an empty list a reader takes for "all healthy".
    /// RED against `collect` stubbed to `vec![]`. No network: the URL comes from an
    /// environment variable that is not set.
    #[test]
    fn collect_reports_a_probe_that_cannot_start_and_nothing_for_a_batch_config() {
        let cfg = |mode: &str| {
            Config::from_yaml(&format!(
                "source:\n  type: postgres\n  url_env: RIVET_DOCTOR_TEST_UNSET_URL\nexports:\n\
                 \x20 - name: t\n    table: t\n    mode: {mode}\n    format: parquet\n\
                 \x20   {}destination: {{ type: local, path: ./out }}\n",
                if mode == "cdc" {
                    "cdc: { checkpoint: ./t.ckpt }\n    "
                } else {
                    ""
                }
            ))
            .expect("a config")
        };
        let dir = std::path::Path::new(".");
        assert!(collect(&cfg("full"), dir).is_empty());
        let checks = collect(&cfg("cdc"), dir);
        assert_eq!(checks.len(), 1, "{checks:?}");
        assert!(
            !checks[0].ok && checks[0].name == "CDC health probe",
            "{checks:?}"
        );
    }

    // ── PostgreSQL verdicts ──

    #[test]
    fn pg_absent_slot_with_no_checkpoint_is_healthy_created_on_first_run() {
        let c = pg_slot_verdict("orders", "rivet_orders", None, false);
        assert!(c.ok);
        assert!(c.detail.unwrap().contains("first run"));
    }

    /// The case the old signature could not express, and the reason it was added:
    /// an absent slot is a healthy FIRST RUN only when no checkpoint claims a
    /// position. With one, the slot was dropped or invalidated and the run
    /// hard-refuses — so `doctor && run` must not green-light it.
    ///
    /// Postgres was the one engine missing this; MySQL, SQL Server and Mongo each
    /// load the checkpoint through `Position::load` in this file, and the MSSQL
    /// arm's comment records the same defect MEASURED there.
    ///
    /// Both directions, because either alone passes a broken build: the
    /// checkpoint case must FAIL, and the true first run must stay healthy.
    /// RED against dropping the `resume_ckpt` arm.
    #[test]
    fn pg_absent_slot_with_a_resume_checkpoint_fails_and_names_the_recovery_order() {
        let c = pg_slot_verdict("orders", "rivet_orders", None, true);
        assert!(
            !c.ok,
            "a dropped slot under an existing checkpoint is not a first run: {c:?}"
        );
        let detail = c.detail.expect("the verdict must say why");
        assert!(
            detail.contains("resume checkpoint exists"),
            "it must name the state, not just fail: {detail}"
        );
        assert!(
            detail.contains("delete the checkpoint file") && detail.contains("THEN re-snapshot"),
            "…and the run's OWN recovery order, or preflight and run tell the operator two \
             different things: {detail}"
        );
    }

    #[test]
    fn pg_small_retained_wal_is_healthy_large_fails_with_drop_hint() {
        let ok = pg_slot_verdict(
            "orders",
            "s",
            Some(PgSlot {
                active: false,
                retained_bytes: 10 << 20,
            }),
            false,
        );
        assert!(ok.ok, "10 MiB retained is healthy");

        let bad = pg_slot_verdict(
            "orders",
            "s",
            Some(PgSlot {
                active: false,
                retained_bytes: 2 << 30,
            }),
            false,
        );
        assert!(!bad.ok, "2 GiB retained fails");
        assert!(bad.hint.unwrap().contains("pg_drop_replication_slot"));
    }

    #[test]
    fn pg_foreign_inactive_slots_fail_only_when_pinning_wal() {
        assert!(pg_foreign_slots_verdict(&[]).ok);
        let small =
            pg_foreign_slots_verdict(&[("ingestr_leftover".into(), 1 << 20, "logical".into())]);
        assert!(small.ok, "a small inactive slot is a note, not a failure");
        let d = small.detail.unwrap();
        assert!(
            d.contains("ingestr_leftover") && d.contains("1 slot(s)"),
            "the healthy note carries BOTH — the name, because a lingering slot is \
             worth knowing about while it is still small, and the total, because \
             reporting only the max is what let 1.96 GiB across eight read as OK: {d}"
        );
        let big =
            pg_foreign_slots_verdict(&[("ingestr_leftover".into(), 5 << 30, "logical".into())]);
        assert!(!big.ok, "an abandoned slot pinning GiBs fails");
        assert!(big.detail.unwrap().contains("ingestr_leftover"));
    }

    /// The hazard is a FILLING DISK, and a disk is filled by the total. Measured on
    /// a dev stand: eight inactive slots at ~334 MiB each — 1.96 GiB pinned — and
    /// the max-only test called them "inactive but small", OK.
    #[test]
    fn a_fleet_of_sub_threshold_slots_still_fills_the_disk() {
        let third = PG_RETAINED_WAL_FAIL_BYTES / 3;
        let fleet: Vec<(String, i64, String)> = (0..8)
            .map(|i| (format!("orphan_{i}"), third, "logical".to_string()))
            .collect();
        assert!(
            fleet
                .iter()
                .all(|(_, b, _)| *b < PG_RETAINED_WAL_FAIL_BYTES),
            "the fixture must be under the bar per-slot or it proves nothing about the sum"
        );
        let w = pg_foreign_slots_warning(&fleet, false)
            .expect("8 slots at a third of the bar is 2.6x the bar in total");
        assert!(
            w.contains("8 slot(s)"),
            "say how many, or an operator cannot tell an aggregate finding from a \
             single fat slot: {w}"
        );
    }

    /// A PHYSICAL slot belongs to a standby. It pins WAL like any other and is worth
    /// reporting — but `pg_drop_replication_slot` on one breaks replication, so it
    /// must never be the slot the hint names.
    #[test]
    fn a_physical_slot_is_reported_but_never_the_one_the_drop_hint_names() {
        let w = pg_foreign_slots_warning(
            &[
                (
                    "standby_1".into(),
                    PG_RETAINED_WAL_FAIL_BYTES * 4,
                    "physical".into(),
                ),
                (
                    "dead_logical".into(),
                    PG_RETAINED_WAL_FAIL_BYTES,
                    "logical".into(),
                ),
            ],
            false,
        )
        .expect("both are past the bar");
        assert!(
            w.contains("standby_1") && w.contains("PHYSICAL"),
            "report it — it really is pinning WAL: {w}"
        );
        assert!(
            w.contains("pg_drop_replication_slot('dead_logical')"),
            "but the ready-to-paste command must name the LOGICAL one, even though the \
             physical slot is the biggest: {w}"
        );
    }

    /// A `run` sees ONE export, so a sibling export's slot is indistinguishable from
    /// an abandoned one. Telling that operator to drop it destroys a live resume
    /// anchor — and rivet's own PG slots sit `active = false` between runs by
    /// construction, because the adapter reads through `pg_logical_slot_peek_changes`.
    #[test]
    fn the_run_path_reports_the_wal_but_never_the_verdict() {
        let one = [(
            "someone_elses".to_string(),
            PG_RETAINED_WAL_FAIL_BYTES,
            "logical".to_string(),
        )];
        let from_run = pg_foreign_slots_warning(&one, true).expect("past the bar");
        assert!(
            !from_run.contains("pg_drop_replication_slot"),
            "a run must not hand over a command that can destroy a sibling export's \
             resume anchor: {from_run}"
        );
        assert!(
            from_run.contains("rivet doctor"),
            "...it must point at the tool that CAN answer the ownership question: \
             {from_run}"
        );
        let from_doctor = pg_foreign_slots_warning(&one, false).expect("past the bar");
        assert!(
            from_doctor.contains("pg_drop_replication_slot"),
            "doctor was handed the whole config and excluded every slot in it, so it \
             may say so: {from_doctor}"
        );
    }

    // ── MySQL verdicts ──

    fn vars(format: &str, image: &str) -> Vec<(String, String)> {
        vec![
            ("log_bin".into(), "ON".into()),
            ("binlog_format".into(), format.into()),
            ("binlog_row_image".into(), image.into()),
        ]
    }

    /// `log_bin=OFF` must be RECOGNISED as the skip condition, in both spellings the
    /// server uses. RED against `binlog_disabled` returning false: `mysql_checks` then
    /// runs `SHOW BINARY LOGS`, which answers ERROR 1381, and the collected verdict —
    /// the only line naming the real cause — is replaced by a generic probe failure.
    /// Measured live on a MySQL 8.4.8 replica (2026-09-16), where `@@log_bin` reads `0`.
    #[test]
    fn binlog_off_is_detected_in_both_spellings_and_named_in_the_verdict() {
        let off = |v: &str| {
            vec![
                ("log_bin".to_string(), v.to_string()),
                ("binlog_format".to_string(), "ROW".to_string()),
                ("binlog_row_image".to_string(), "FULL".to_string()),
            ]
        };
        assert!(binlog_disabled(&off("OFF")), "`OFF` is binlog disabled");
        assert!(binlog_disabled(&off("0")), "8.x renders @@log_bin as 0/1");
        assert!(!binlog_disabled(&off("ON")), "`ON` must probe retention");
        assert!(!binlog_disabled(&off("1")), "`1` must probe retention");
        assert!(
            binlog_disabled(&[]),
            "a server that did not report the variable is not provably logging"
        );
        let verdict = mysql_binlog_config_verdict(&off("0"));
        assert!(!verdict.ok);
        assert!(
            verdict.detail.unwrap().contains("log_bin is OFF"),
            "the failing verdict must name the cause, not the format/image checks below it"
        );
    }

    #[test]
    fn mysql_binlog_config_requires_row_and_full() {
        assert!(mysql_binlog_config_verdict(&vars("ROW", "FULL")).ok);
        let stmt = mysql_binlog_config_verdict(&vars("STATEMENT", "FULL"));
        assert!(!stmt.ok);
        assert!(stmt.detail.unwrap().contains("STATEMENT"));
        let minimal = mysql_binlog_config_verdict(&vars("ROW", "MINIMAL"));
        assert!(!minimal.ok, "MINIMAL breaks the after-image / MERGE shape");
    }

    #[test]
    fn mysql_missing_checkpoint_config_fails_purged_file_fails_with_1236() {
        let logs = vec![
            ("binlog.000003".to_string(), 1000u64),
            ("binlog.000004".to_string(), 500u64),
        ];
        let none = mysql_ckpt_verdict("orders", MysqlCkpt::NoPathConfigured, &logs);
        assert!(
            !none.ok,
            "a config-driven cdc export without a checkpoint skips changes between runs"
        );

        let fresh = mysql_ckpt_verdict("orders", MysqlCkpt::NotYetWritten, &logs);
        assert!(fresh.ok);

        let purged = mysql_ckpt_verdict(
            "orders",
            MysqlCkpt::Loaded {
                file: "binlog.000001".into(),
                pos: 4,
            },
            &logs,
        );
        assert!(!purged.ok);
        assert!(purged.detail.unwrap().contains("1236"));
    }

    #[test]
    fn mysql_backlog_sums_remainder_of_ckpt_file_plus_later_files() {
        let logs = vec![
            ("binlog.000003".to_string(), 1000u64),
            ("binlog.000004".to_string(), 500u64),
        ];
        let c = mysql_ckpt_verdict(
            "orders",
            MysqlCkpt::Loaded {
                file: "binlog.000003".into(),
                pos: 400,
            },
            &logs,
        );
        assert!(c.ok);
        // (1000-400) + 500 = 1100 bytes ≈ 0.0 MiB — assert the arithmetic via
        // the exact rendered value.
        assert!(c.detail.unwrap().contains("0.0 MiB"));
    }

    // ── SQL Server verdicts ──

    fn healthy() -> MssqlHealth {
        MssqlHealth {
            cdc_enabled: true,
            instance_min_lsn: Some("0x00000028000009F00005".into()),
            agent_running: Some(true),
        }
    }

    #[test]
    fn mssql_cdc_disabled_fails_with_enable_hint() {
        let h = MssqlHealth {
            cdc_enabled: false,
            instance_min_lsn: None,
            agent_running: None,
        };
        let out = mssql_verdicts("orders", Some("dbo_orders"), &h, None);
        assert_eq!(out.len(), 1);
        assert!(!out[0].ok);
        assert!(out[0].hint.as_ref().unwrap().contains("sp_cdc_enable_db"));
    }

    #[test]
    fn mssql_ckpt_below_retention_fails_within_retention_ok() {
        let h = healthy();
        let below = mssql_verdicts(
            "orders",
            Some("dbo_orders"),
            &h,
            Some(Some("0x00000010000000010001".into())),
        );
        let ckpt = below
            .iter()
            .find(|c| c.name.contains("checkpoint"))
            .unwrap();
        assert!(!ckpt.ok, "LSN below min must fail");

        let above = mssql_verdicts(
            "orders",
            Some("dbo_orders"),
            &h,
            Some(Some("0x00000030000000010001".into())),
        );
        let ckpt = above
            .iter()
            .find(|c| c.name.contains("checkpoint"))
            .unwrap();
        assert!(ckpt.ok, "LSN above min is healthy");
    }

    #[test]
    fn mssql_agent_stopped_fails_unknown_is_a_note() {
        let mut h = healthy();
        h.agent_running = Some(false);
        let out = mssql_verdicts("orders", Some("dbo_orders"), &h, Some(None));
        let agent = out.iter().find(|c| c.name.contains("Agent")).unwrap();
        assert!(!agent.ok);

        h.agent_running = None;
        let out = mssql_verdicts("orders", Some("dbo_orders"), &h, Some(None));
        let agent = out.iter().find(|c| c.name.contains("Agent")).unwrap();
        assert!(agent.ok, "unverifiable Agent is a note, not a failure");
        assert!(agent.detail.as_ref().unwrap().contains("VIEW SERVER STATE"));
    }

    #[test]
    fn norm_lsn_compares_across_prefix_and_case() {
        assert!(norm_lsn("0x0000001000000001") < norm_lsn("00000028000009f0"));
        assert_eq!(norm_lsn("0xABC"), norm_lsn("abc"));
    }
}

#[cfg(test)]
mod slot_retention_warning_tests {
    use super::*;

    /// The threshold has ONE definition, and both callers must sit on the same side
    /// of it: `doctor` fails, every `run` warns. A second threshold in the run path
    /// is the copy that goes stale unnoticed.
    #[test]
    fn an_ordinary_backlog_is_silent_and_a_pinned_slot_names_its_escape() {
        // Below the bar: a slot legitimately holds everything since the last run, and
        // a warning on every ordinary backlog is one that stops being read.
        assert_eq!(pg_retained_wal_warning("s", 0, true), None);
        assert_eq!(
            pg_retained_wal_warning("s", PG_RETAINED_WAL_FAIL_BYTES - 1, false),
            None,
            "one byte under the bar must stay silent — the boundary is the whole \
             contract, and an off-by-one here makes the warning fire on every run"
        );

        let w = pg_retained_wal_warning("mine", PG_RETAINED_WAL_FAIL_BYTES, true)
            .expect("at the bar the operator must be told");
        assert!(w.contains("mine"), "name the slot: {w}");
        assert!(
            w.contains("pg_drop_replication_slot") && w.contains("max_slot_wal_keep_size"),
            "name what to DO — a warning without an escape is noise: {w}"
        );
        assert!(
            w.contains("This run will drain it"),
            "and be honest that the run itself fixes this case, or an operator drops a \
             slot that was about to be drained: {w}"
        );
    }

    /// The foreign slots are the dangerous half: nobody is draining them, so their
    /// WAL is pinned until a human acts. Measured live at 9 abandoned slots holding
    /// 1.5 GiB each on a dev stand.
    #[test]
    fn foreign_slots_warn_past_the_bar_and_report_the_aggregate() {
        assert_eq!(pg_foreign_slots_warning(&[], false), None);
        assert_eq!(
            pg_foreign_slots_warning(&[("small".into(), 1024, "logical".into())], false),
            None,
            "an inactive slot holding a KiB is not a hazard; warning about it teaches \
             operators to ignore the message"
        );

        // EXACTLY at the bar, both directions. The sibling test above covers this
        // boundary for `pg_retained_wal_warning` and this one did not — it fed
        // `bar + 1` and nothing else, so `< bar` and `<= bar` behaved identically
        // and the mutant survived CI (`replace < with <=`, 2026-08-25). Testing one
        // function's boundary and not its twin's is exactly the asymmetry mutation
        // testing exists to find; reading the two tests side by side does not show it.
        assert!(
            pg_foreign_slots_warning(
                &[(
                    "at_the_bar".into(),
                    PG_RETAINED_WAL_FAIL_BYTES,
                    "logical".into()
                )],
                false
            )
            .is_some(),
            "a slot sitting exactly ON the bar must be reported — `<=` here would let \
             the single most common boundary value through in silence"
        );
        assert_eq!(
            pg_foreign_slots_warning(
                &[(
                    "under".into(),
                    PG_RETAINED_WAL_FAIL_BYTES - 1,
                    "logical".into()
                )],
                false
            ),
            None,
            "...and one byte under it must not be"
        );

        // The PHYSICAL label, and that it is not applied to a logical slot. Mutating
        // `kind == "physical"` to `!=` survived: nothing asserted the label appears
        // on the right one, so the two renderings were interchangeable.
        let phys = pg_foreign_slots_warning(
            &[(
                "standby".into(),
                PG_RETAINED_WAL_FAIL_BYTES + 1,
                "physical".into(),
            )],
            false,
        )
        .expect("past the bar");
        assert!(
            phys.contains("PHYSICAL — a standby's"),
            "a physical slot must be labelled — the drop hint must never name a \
             standby's slot as a CDC leftover: {phys}"
        );
        let logi = pg_foreign_slots_warning(
            &[(
                "leftover".into(),
                PG_RETAINED_WAL_FAIL_BYTES + 1,
                "logical".into(),
            )],
            false,
        )
        .expect("past the bar");
        assert!(
            !logi.contains("PHYSICAL — a standby's"),
            "...and a logical one must NOT be. Asserted on the per-slot LABEL, not on \
             the word: the message's closing caveat says `never for a slot marked \
             PHYSICAL` regardless, so a bare substring check passes on every input \
             and grades nothing: {logi}"
        );

        // The `and N more` tail's boundary. `more > 0` survived three mutants
        // (`==`, `>=`, `<`) because no fixture ever had exactly ten slots — the
        // listing takes 10, so ten is the one count at which the tail must be
        // ABSENT and eleven the one at which it must appear.
        let many = |n: usize| -> String {
            let slots: Vec<(String, i64, String)> = (0..n)
                .map(|i| {
                    (
                        format!("s{i}"),
                        PG_RETAINED_WAL_FAIL_BYTES + 1 + i as i64,
                        "logical".to_string(),
                    )
                })
                .collect();
            pg_foreign_slots_warning(&slots, false).expect("past the bar")
        };
        assert!(
            !many(10).contains("more"),
            "exactly ten slots all fit in the listing — a tail here would claim \
             something was hidden when nothing was: {}",
            many(10)
        );
        assert!(
            many(11).contains("and 1 more"),
            "and the eleventh must be counted, or the operator reads a truncated \
             listing as the whole set: {}",
            many(11)
        );

        let big = PG_RETAINED_WAL_FAIL_BYTES + 1;
        let w = pg_foreign_slots_warning(
            &[
                ("small".into(), 4096, "logical".into()),
                ("orphan_a".into(), big, "logical".into()),
                ("orphan_b".into(), big * 2, "logical".into()),
            ],
            false,
        )
        .expect("two slots past the bar must be reported");
        assert!(
            w.contains("orphan_a") && w.contains("orphan_b"),
            "list every offender, not just the worst — an operator fixing one and \
             re-running should not have to discover the rest one at a time: {w}"
        );
        // The listing now INCLUDES sub-bar slots, deliberately: the hazard is a
        // filling disk and the aggregate is the finding, so hiding the small ones
        // is what let 1.96 GiB across eight slots read as "inactive but small". It
        // is capped at ten with a "and N more" tail instead.
        assert!(
            w.contains("small"),
            "every contributor is listed — the total is what matters: {w}"
        );
        assert!(
            w.contains("pg_drop_replication_slot('orphan_b')"),
            "the ready-to-paste command should name the WORST one: {w}"
        );
        // The doctor form says none of these belongs to the config — it was handed
        // the whole config and can. The run form deliberately does not: see
        // `the_run_path_reports_the_wal_but_never_the_verdict`.
        assert!(
            w.contains("none belongs to this config"),
            "doctor's form may state ownership, which is what makes its drop hint \
             safe to offer: {w}"
        );
    }
}
