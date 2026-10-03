//! One verdict for "does this checkpoint belong to this source", shared by every
//! engine whose resume position is only meaningful on the server that wrote it.

use crate::error::Result;

/// The re-baseline remedy every CDC data-loss message ends with, in the order the product runs it.
pub(crate) const RECOVER: &str = "Re-baseline the stream in one run: delete the checkpoint \
     file if there is one; give the export a baseline (`cdc.initial: snapshot` or `backfill:`) if \
     it has none, or clear both done-signals of the one it has (its `cdc_snapshot` row in the \
     state DB and the destination's snapshot/_SUCCESS marker; either one left in place skips the \
     baseline); and if a warehouse load consumes this stream, truncate its `<table>__changes` \
     table before the next load. That run anchors FIRST and re-reads the table after, so nothing \
     falls between the two. A separate `mode: full` export does not re-baseline the stream.";

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
            "Re-baseline the stream in one run: delete the checkpoint file if there is one; give \
             the export a baseline (`cdc.initial: snapshot` or `backfill:`) if it has none, or \
             clear both done-signals of the one it has (its `cdc_snapshot` row in the state DB \
             and the destination's snapshot/_SUCCESS marker; either one left in place skips the \
             baseline); and if a warehouse load consumes this stream, truncate its \
             `<table>__changes` table before the next load. That run anchors FIRST and re-reads \
             the table after, so nothing falls between the two. A separate `mode: full` export \
             does not re-baseline the stream."
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
}
