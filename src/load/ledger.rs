//! The load ledger's vocabulary: what a `load_run` row says, and what the rows say about a table.

use crate::load;
use crate::state::{LoadStatus, StateStore};

/// Whether the ledger says rivet loaded `fqtn`; `Unreadable` (warned) when the probe fails.
pub(crate) fn ownership_of(state: Option<&StateStore>, fqtn: &str, op: &str) -> load::Ownership {
    match state {
        Some(s) => match s.has_load_attempt(fqtn) {
            Ok(true) => load::Ownership::Own,
            Ok(false) => load::Ownership::Foreign,
            Err(e) => {
                log::warn!(
                    "{op}: the ownership probe for {fqtn} failed ({e:#}) — refusing rather \
                     than treating it as a stateless {op}"
                );
                load::Ownership::Unreadable
            }
        },
        None => load::Ownership::Unknown,
    }
}

/// The `load_run` PRIMARY KEY for one table's row in one invocation.
///
/// The OP belongs in it because `rivet load` and `rivet compact` are two different
/// RECORDS of the same table, and both derive their key from the same operator-supplied
/// run id. Without it they computed the identical string, `load_run` upserts
/// `ON CONFLICT (load_id) DO UPDATE`, and the compact's row — `mode=compact`,
/// `source_run_ids=[]`, `rows_loaded=0` — REPLACED the load's. A scheduler that stamps
/// one `RIVET_RUN_ID` per cycle and then runs load followed by compact is the ordinary
/// shape that does it, and the release gate had already met this: `blessed_flow.py`
/// works around it by minting a unique `--run-id` per cell, and says why in a comment.
/// A harness workaround for a product behaviour is a bug report, not a fix.
///
/// The `{run_id}:` PREFIX is load-bearing and must stay first — the gate's ledger check
/// scopes with `LIKE '<run-id>%'`.
pub(crate) fn ledger_load_id(run_id: &str, op: &str, table: &str) -> String {
    format!("{run_id}:{op}:{table}")
}

/// How the ledger records a load that did not complete: `refused` when it stopped before
/// any warehouse write (the target is not rivet's own for having been refused), `failed`
/// otherwise.
pub(crate) fn ledger_status(e: &anyhow::Error) -> &'static str {
    match e.downcast_ref::<load::Refused>() {
        Some(_) => LoadStatus::Refused.as_str(),
        None => LoadStatus::Failed.as_str(),
    }
}

/// The status a load's CLOSING ledger row carries after the load failed.
///
/// `refused` means "stopped before ANY warehouse write" — that is precisely why
/// `has_load_attempt` does not count it. Once a LEG has LANDED the claim is false for
/// this load: the warehouse WAS written, and the target is rivet's own.
///
/// It matters because the closing row reuses the leg's `load_id` — one audit row per
/// load, deliberately — so it REPLACES whatever the leg wrote. A `refused` replacing a
/// leg's `success` makes `has_load_attempt` return false and rivet DISOWNS the base it
/// had just created; the next load refuses it as foreign. The success path already
/// guards its closing row (`closing_record_applies`); the error path had no guard at
/// all, which is the asymmetry this closes.
///
/// `loaded_source_run` is unaffected either way — it is written only on `success` and
/// never deleted, so the skip set survives the replacement. The damage was always to
/// the audit row and, through it, to ownership.
pub(crate) fn closing_status(e: &anyhow::Error, consumed: &[String]) -> &'static str {
    if consumed.is_empty() {
        ledger_status(e)
    } else {
        LoadStatus::Failed.as_str()
    }
}
