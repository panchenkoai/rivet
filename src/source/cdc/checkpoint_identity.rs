//! One verdict for "does this checkpoint belong to this source", shared by every
//! engine whose resume position is only meaningful on the server that wrote it.

use crate::error::Result;

/// The re-baseline remedy every CDC data-loss message ends with, in the order the product runs it.
pub(crate) const RECOVER: &str = "Re-baseline the stream in one run: delete the checkpoint \
     file if there is one; move every file out of the export's destination (for a `tables:` \
     export, every table's directory under it): the parts there still hold rows the source may \
     no longer have, and each table's snapshot/_SUCCESS marker goes with them; delete the \
     export's `cdc_snapshot` rows (one per table) from the state DB; give the export \
     `cdc.initial: snapshot` if it has none; and if a warehouse load consumes this stream, \
     truncate its `<table>__changes` table before the next load. That run anchors FIRST and \
     re-reads every table after, so nothing falls between the two. A separate `mode: full` \
     export does not re-baseline the stream.";

/// A refusal of `code` whose text is `why`.
fn refusal(code: crate::error::Code, why: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(crate::error::CodedError::new(code, why))
}

/// Refuse a checkpoint that cannot be resumed from (unreadable, of another shape, or gone under a baseline): `SOURCE_CDC_CHECKPOINT_INVALID`, exit 5.
pub(crate) fn checkpoint_invalid(why: String) -> anyhow::Error {
    refusal(crate::error::codes::SOURCE_CDC_CHECKPOINT_INVALID, why)
}

/// Refuse a resume position the source's log no longer reaches (dropped slot, purged binlog, rolled oplog): `SOURCE_CDC_LOG_GAP`, exit 5.
pub(crate) fn log_gap(why: String) -> anyhow::Error {
    refusal(crate::error::codes::SOURCE_CDC_LOG_GAP, why)
}

/// Refuse a captured table whose rows left the source without change events (TRUNCATE, DROP): `SOURCE_CDC_TRUNCATED`, exit 5.
pub(crate) fn truncated(why: impl Into<String>) -> anyhow::Error {
    refusal(crate::error::codes::SOURCE_CDC_TRUNCATED, why)
}

/// Refuse a log span this reader cannot decode into events, so the stream re-baselines past it: `SOURCE_CDC_UNDECODABLE`, exit 5.
pub(crate) fn undecodable(why: String) -> anyhow::Error {
    refusal(crate::error::codes::SOURCE_CDC_UNDECODABLE, why)
}

/// The config and export a checkpoint belongs to, kept beside it as `<checkpoint>.owner`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CheckpointOwner {
    pub(crate) config: String,
    pub(crate) export: String,
}

/// The owner record beside the checkpoint at `ckpt`.
pub(crate) fn owner_path(ckpt: &std::path::Path) -> std::path::PathBuf {
    let mut name = ckpt.as_os_str().to_os_string();
    name.push(".owner");
    std::path::PathBuf::from(name)
}

/// `p` spelled one way whatever exists of it: its longest existing ancestor canonical (so
/// `/var` and `/private/var` agree), the rest lexical without `.` or a trailing `/`.
pub(crate) fn canonical_place(p: &std::path::Path) -> std::path::PathBuf {
    let lexical: std::path::PathBuf = p.components().collect();
    let mut head = lexical.as_path();
    let mut tail = Vec::new();
    loop {
        if let Ok(real) = std::fs::canonicalize(head) {
            return tail.iter().rev().fold(real, |acc, c| acc.join(c));
        }
        match (head.parent(), head.file_name()) {
            (Some(up), Some(name)) => {
                tail.push(name.to_os_string());
                head = up;
            }
            _ => return lexical,
        }
    }
}

/// The owner record beside the checkpoint at `ckpt`, when one was written and reads.
pub(crate) fn owner_of(ckpt: &std::path::Path) -> Option<CheckpointOwner> {
    let text = std::fs::read_to_string(owner_path(ckpt)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Whether export `export` of the config at `config` names the checkpoint `ckpt` (a config that
/// cannot be read is taken to name it: refusing is the safe answer).
fn still_names(config: &std::path::Path, export: &str, ckpt: &std::path::Path) -> bool {
    if !config.exists() {
        return false;
    }
    let Ok(cfg) = crate::config::Config::load(&config.display().to_string()) else {
        return true;
    };
    let dir = config.parent().unwrap_or(std::path::Path::new("."));
    let same = |p: &std::path::Path| canonical_place(p) == canonical_place(ckpt);
    cfg.exports.iter().any(|e| {
        e.name == export
            && e.cdc
                .as_ref()
                .and_then(|c| c.checkpoint.as_deref())
                .is_some_and(|raw| same(&super::resolve_checkpoint(raw, dir)))
    })
}

/// Claim the checkpoint `ckpt` for export `export` of the config at `config`, refusing one that
/// another config's export still reads (`SOURCE_CDC_CHECKPOINT_SHARED`, exit 5): two streams on
/// one checkpoint each continue from where the other stopped, and skip what the other read. A
/// checkpoint without an owner record (written before 0.32.2), or whose owner no longer names it
/// (a config moved or edited), is claimed.
pub(crate) fn claim_checkpoint(
    ckpt: &std::path::Path,
    config: &std::path::Path,
    export: &str,
) -> Result<()> {
    let me = CheckpointOwner {
        config: canonical_place(config).display().to_string(),
        export: export.to_string(),
    };
    let record = owner_path(ckpt);
    let held = owner_of(ckpt);
    if let Some(owner) = &held {
        if owner == &me {
            return Ok(());
        }
        if still_names(std::path::Path::new(&owner.config), &owner.export, ckpt) {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_CHECKPOINT_SHARED,
                "cdc: checkpoint '{}' belongs to export '{}' of config '{}', which still reads \
                 it; export '{}' of config '{}' would continue from where that one stopped, and \
                 each stream would skip the changes the other read. Nothing was read. Give this \
                 export a `cdc.checkpoint` of its own (`rivet init -o` names it after the \
                 config file) and keep this file: it is the other export's position. Give it \
                 `cdc.initial: snapshot` if it has none, so its first run on the new \
                 checkpoint re-reads every table, and move any file it wrote before out of its \
                 destination first: those may miss the changes the other stream read. If this \
                 config replaces that one (an edited copy of it), delete or rename that config, \
                 or point its `cdc.checkpoint` elsewhere, and run this one again",
                ckpt.display(),
                owner.export,
                owner.config,
                me.export,
                me.config
            );
        }
    }
    if let Some(dir) = record.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&record, serde_json::to_vec(&me)?)?;
    Ok(())
}

/// What a resume may do given the checkpoint's recorded identity and the server's.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IdentityVerdict {
    Ok,
    /// Resume proceeds, but nothing could be verified; the text says why.
    Unverifiable(String),
    /// Resume is refused: another source's position, or a log rewound since; the text says which.
    Foreign(String),
}

impl IdentityVerdict {
    /// Refuse a foreign checkpoint as `SOURCE_CDC_FOREIGN_CHECKPOINT` (exit 5); warn on an unverifiable one.
    pub(crate) fn enforce(self) -> Result<()> {
        match self {
            Self::Ok => Ok(()),
            Self::Unverifiable(why) => {
                log::warn!("{why}");
                Ok(())
            }
            Self::Foreign(why) => crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_FOREIGN_CHECKPOINT,
                "{why} {RECOVER}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The remedy text, pinned against a hand-written copy.
    #[test]
    fn the_rebaseline_remedy_names_the_steps_the_product_runs() {
        assert_eq!(
            RECOVER,
            "Re-baseline the stream in one run: delete the checkpoint file if there is one; move \
             every file out of the export's destination (for a `tables:` export, every table's \
             directory under it): the parts there still hold rows the source may no longer have, \
             and each table's snapshot/_SUCCESS marker goes with them; delete the export's \
             `cdc_snapshot` rows (one per table) from the state DB; give the export \
             `cdc.initial: snapshot` if it has none; and if a warehouse load consumes this \
             stream, truncate its `<table>__changes` table before the next load. That run anchors \
             FIRST and re-reads every table after, so nothing falls between the two. A separate \
             `mode: full` export does not re-baseline the stream."
        );
    }

    #[test]
    fn a_foreign_verdict_refuses_with_exit_5_and_the_recovery_order() {
        let err = IdentityVerdict::Foreign("x cdc: elsewhere.".into())
            .enforce()
            .unwrap_err();
        assert_eq!(crate::error::classify_exit(&err), 5);
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT")
        );
        assert_eq!(err.to_string(), format!("x cdc: elsewhere. {RECOVER}"));
        assert!(
            IdentityVerdict::Unverifiable("old".into())
                .enforce()
                .is_ok()
        );
        assert!(IdentityVerdict::Ok.enforce().is_ok());
    }

    #[test]
    fn each_failure_class_refuses_with_exit_5_under_its_own_code_and_keeps_its_text() {
        type Class = fn(String) -> anyhow::Error;
        let classes: [(Class, &str); 3] = [
            (checkpoint_invalid, "RIVET_SOURCE_CDC_CHECKPOINT_INVALID"),
            (log_gap, "RIVET_SOURCE_CDC_LOG_GAP"),
            (truncated, "RIVET_SOURCE_CDC_TRUNCATED"),
        ];
        for (class, code) in classes {
            let err = class(format!("x cdc: {code} happened. {RECOVER}"));
            assert_eq!(crate::error::classify_exit(&err), 5, "{code}");
            assert_eq!(crate::error::error_code(&err), Some(code));
            assert_eq!(
                err.to_string(),
                format!("x cdc: {code} happened. {RECOVER}")
            );
        }
    }
}

#[cfg(test)]
mod owner_tests {
    use super::*;

    /// A config in `dir` whose export `export` reads `ckpt`.
    fn config(dir: &std::path::Path, file: &str, export: &str, ckpt: &str) -> std::path::PathBuf {
        let path = dir.join(file);
        std::fs::write(
            &path,
            format!(
                "source:\n  type: mysql\n  url: \"mysql://u:p@localhost/app\"\nexports:\n  - name: {export}\n    \
                 table: orders\n    mode: cdc\n    format: parquet\n    cdc:\n      checkpoint: {ckpt}\n    \
                 destination: {{ type: local, path: ./out_{export} }}\n"
            ),
        )
        .unwrap();
        path
    }

    /// A checkpoint is claimed by its first reader and refused to a second config's export on
    /// every attempt while the first still names it; written before 0.32.2 (no owner record), or
    /// left by a config that moved, it is claimed.
    #[test]
    fn a_checkpoint_another_config_still_reads_is_refused_on_every_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("cdc/shared.ckpt");
        std::fs::create_dir_all(ckpt.parent().unwrap()).unwrap();
        std::fs::write(&ckpt, r#"{"file":"bin.000001","pos":4}"#).unwrap();
        let a = config(dir.path(), "a.yaml", "orders_cdc", "./cdc/shared.ckpt");
        let b = config(dir.path(), "b.yaml", "orders_cdc", "./cdc/shared.ckpt");
        claim_checkpoint(&ckpt, &a, "orders_cdc")
            .expect("a checkpoint with no owner record is claimed");
        let unwritten = dir.path().join("cdc/not_yet.ckpt");
        config(dir.path(), "c.yaml", "orders_cdc", "./cdc/not_yet.ckpt");
        config(dir.path(), "d.yaml", "orders_cdc", "./cdc/not_yet.ckpt");
        claim_checkpoint(&unwritten, &dir.path().join("c.yaml"), "orders_cdc").unwrap();
        let other_spelling = canonical_place(dir.path())
            .join(".")
            .join("cdc/not_yet.ckpt");
        let err = claim_checkpoint(&other_spelling, &dir.path().join("d.yaml"), "orders_cdc")
            .expect_err("a checkpoint not written yet is still the other config's");
        crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_SHARED", 5);
        std::fs::remove_file(dir.path().join("d.yaml")).unwrap();
        claim_checkpoint(&ckpt, &a, "orders_cdc").expect("its owner reads it again");
        for _ in 0..2 {
            let err = claim_checkpoint(&ckpt, &b, "orders_cdc").unwrap_err();
            crate::error::assert_coded(&err, "RIVET_SOURCE_CDC_CHECKPOINT_SHARED", 5);
            assert!(format!("{err:#}").contains("a.yaml"), "{err:#}");
        }
        // The first config moves its stream to a checkpoint of its own: the second may claim it.
        config(dir.path(), "a.yaml", "orders_cdc", "./cdc/a.ckpt");
        claim_checkpoint(&ckpt, &b, "orders_cdc").expect("a checkpoint its owner no longer names");
        std::fs::remove_file(&b).unwrap();
        claim_checkpoint(&ckpt, &a, "orders_cdc").expect("a checkpoint whose owner is gone");
        let owner: CheckpointOwner =
            serde_json::from_str(&std::fs::read_to_string(owner_path(&ckpt)).unwrap()).unwrap();
        assert!(owner.config.ends_with("a.yaml"), "{owner:?}");
        assert_eq!(
            std::fs::read_to_string(&ckpt).unwrap(),
            r#"{"file":"bin.000001","pos":4}"#,
            "the position itself is never rewritten"
        );
    }
}
