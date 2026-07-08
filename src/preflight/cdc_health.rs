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
        Some("the CDC checks need a working source connection — fix the source auth failure above first".into()),
    )
}

/// Entry point: every CDC health check for the config, or empty when the
/// config has no `mode: cdc` exports. A connection failure becomes a single
/// failed check rather than aborting doctor.
pub(super) fn collect(config: &Config) -> Vec<DoctorCheck> {
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
    let result = match config.source.source_type {
        SourceType::Postgres => pg_checks(&url, tls, &cdc),
        SourceType::Mysql => mysql_checks(&url, tls, &cdc),
        SourceType::Mssql => mssql_checks(&url, tls, &cdc),
        // Change streams: probe the replica-set requirement + declare the capture
        // fidelity tier (6.0+ pre/post-images vs current-state UpdateLookup).
        SourceType::Mongo => mongo_checks(&url, tls, &cdc),
    };
    result.unwrap_or_else(|e| vec![probe_failed(&e)])
}

// ─── PostgreSQL ──────────────────────────────────────────────────────────────

struct PgSlot {
    active: bool,
    retained_bytes: i64,
}

/// Verdict for the export's own slot. Absent is healthy (created on first
/// run); present is healthy while the retained WAL stays small.
fn pg_slot_verdict(export: &str, slot: &str, state: Option<PgSlot>) -> DoctorCheck {
    let name = format!("CDC slot '{slot}' (export '{export}')");
    match state {
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
        Some(s) => check(
            name,
            false,
            Some(format!(
                "slot is pinning {} of WAL (active={}) — the source disk is filling",
                mib(s.retained_bytes),
                s.active
            )),
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
fn pg_foreign_slots_verdict(foreign: &[(String, i64)]) -> DoctorCheck {
    let name = "CDC other inactive slots".to_string();
    if foreign.is_empty() {
        return check(name, true, Some("none".into()), None);
    }
    let worst = foreign.iter().max_by_key(|(_, b)| *b).expect("non-empty");
    let listing = foreign
        .iter()
        .map(|(n, b)| format!("{n} ({})", mib(*b)))
        .collect::<Vec<_>>()
        .join(", ");
    if worst.1 < PG_RETAINED_WAL_FAIL_BYTES {
        check(
            name,
            true,
            Some(format!("inactive but small: {listing}")),
            None,
        )
    } else {
        check(
            name,
            false,
            Some(format!(
                "inactive slot(s) pinning WAL: {listing} — an abandoned slot prevents WAL \
                 recycling and fills the source disk"
            )),
            Some(format!(
                "if the consumer is gone for good: SELECT pg_drop_replication_slot('{}');",
                worst.0
            )),
        )
    }
}

fn pg_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    exports: &[&ExportConfig],
) -> Result<Vec<DoctorCheck>> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let mut checks = Vec::new();
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
        checks.push(pg_slot_verdict(&e.name, &slot, state));
        ours.push(slot);
    }
    let rows = client.query(
        "SELECT slot_name, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
         FROM pg_replication_slots WHERE NOT active AND slot_name <> ALL($1)",
        &[&ours],
    )?;
    let foreign: Vec<(String, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    checks.push(pg_foreign_slots_verdict(&foreign));
    Ok(checks)
}

// ─── MySQL ───────────────────────────────────────────────────────────────────

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
    let log_bin = get("log_bin").unwrap_or("OFF");
    let format = get("binlog_format").unwrap_or("?");
    let row_image = get("binlog_row_image").unwrap_or("FULL");
    if !log_bin.eq_ignore_ascii_case("ON") && log_bin != "1" {
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
                        "re-snapshot the table (mode: full) and restart CDC from a fresh \
                         checkpoint; size binlog retention above your CDC cadence"
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
) -> Result<Vec<DoctorCheck>> {
    use mysql::prelude::Queryable;
    let pool = crate::source::mysql::connect_pool(url, tls)?;
    let mut conn = pool.get_conn()?;
    let mut checks = Vec::new();

    let vars: Vec<(String, String)> = conn.query(
        "SHOW GLOBAL VARIABLES WHERE Variable_name IN \
         ('log_bin','binlog_format','binlog_row_image')",
    )?;
    checks.push(mysql_binlog_config_verdict(&vars));

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
            Some(p) => match crate::source::cdc::Position::load(std::path::Path::new(p))? {
                None => MysqlCkpt::NotYetWritten,
                Some(pos) => {
                    let file = pos
                        .0
                        .get("file")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let p = pos.0.get("pos").and_then(|v| v.as_u64()).unwrap_or(0);
                    MysqlCkpt::Loaded { file, pos: p }
                }
            },
        };
        checks.push(mysql_ckpt_verdict(&e.name, ckpt, &logs));
    }
    Ok(checks)
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
                                "re-snapshot (mode: full) and restart CDC from a fresh checkpoint"
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
) -> Result<Vec<DoctorCheck>> {
    let mut src = crate::source::mssql::MssqlSource::connect_with_tls(url, tls)?;
    let mut checks = Vec::new();
    for e in exports {
        let ci = e.cdc.as_ref().and_then(|c| c.capture_instance.as_deref());
        let health = src.cdc_health(ci)?;
        let ckpt_state = match e.cdc.as_ref().and_then(|c| c.checkpoint.as_deref()) {
            None => None,
            Some(p) => Some(
                crate::source::cdc::Position::load(std::path::Path::new(p))?.and_then(|pos| {
                    pos.0
                        .get("lsn")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                }),
            ),
        };
        let mssql_health = MssqlHealth {
            cdc_enabled: health.cdc_enabled,
            instance_min_lsn: health.instance_min_lsn,
            agent_running: health.agent_running,
        };
        checks.extend(mssql_verdicts(&e.name, ci, &mssql_health, ckpt_state));
    }
    Ok(checks)
}

// ─── MongoDB ─────────────────────────────────────────────────────────────────

/// Change-stream health: the replica-set requirement (a standalone cannot
/// `watch()`) and the DECLARED capture-fidelity tier — so an operator learns
/// before the run that a sub-6.0 server gives current-state post-images and
/// key-only deletes, never discovering it as a silent null in the output.
fn mongo_checks(
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    _exports: &[&ExportConfig],
) -> Result<Vec<DoctorCheck>> {
    let cap = crate::source::mongo::cdc::probe_capability(url, tls)?;
    let mut checks = Vec::new();
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
    Ok(checks)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── PostgreSQL verdicts ──

    #[test]
    fn pg_absent_slot_is_healthy_created_on_first_run() {
        let c = pg_slot_verdict("orders", "rivet_orders", None);
        assert!(c.ok);
        assert!(c.detail.unwrap().contains("first run"));
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
        );
        assert!(ok.ok, "10 MiB retained is healthy");

        let bad = pg_slot_verdict(
            "orders",
            "s",
            Some(PgSlot {
                active: false,
                retained_bytes: 2 << 30,
            }),
        );
        assert!(!bad.ok, "2 GiB retained fails");
        assert!(bad.hint.unwrap().contains("pg_drop_replication_slot"));
    }

    #[test]
    fn pg_foreign_inactive_slots_fail_only_when_pinning_wal() {
        assert!(pg_foreign_slots_verdict(&[]).ok);
        let small = pg_foreign_slots_verdict(&[("ingestr_leftover".into(), 1 << 20)]);
        assert!(small.ok, "a small inactive slot is a note, not a failure");
        assert!(small.detail.unwrap().contains("ingestr_leftover"));
        let big = pg_foreign_slots_verdict(&[("ingestr_leftover".into(), 5 << 30)]);
        assert!(!big.ok, "an abandoned slot pinning GiBs fails");
        assert!(big.hint.unwrap().contains("ingestr_leftover"));
    }

    // ── MySQL verdicts ──

    fn vars(format: &str, image: &str) -> Vec<(String, String)> {
        vec![
            ("log_bin".into(), "ON".into()),
            ("binlog_format".into(), format.into()),
            ("binlog_row_image".into(), image.into()),
        ]
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
