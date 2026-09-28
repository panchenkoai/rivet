//! One verdict for "does this checkpoint belong to this source", shared by every
//! engine whose resume position is only meaningful on the server that wrote it.

use crate::error::Result;

/// The recovery every foreign-checkpoint refusal ends with, in the only order that loses nothing.
pub(crate) const RECOVER: &str = "Delete the checkpoint so the next run anchors afresh FIRST, \
     then re-snapshot the tables (`mode: full`): snapshotting first leaves the changes in \
     between in neither.";

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
