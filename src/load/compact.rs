//! `rivet compact`: merge each base-and-buffer table's buffer into its base and drop the buffer.

use crate::error::Result;
use crate::load;
use crate::load::orchestrate::{
    failures_of, hand_off_state, ledger_load_id, ledger_status, needs_source_engine,
    no_outcome_error, open_state, ownership_of, pin_plan_to_its_run, reconnect, require_pk,
    resolve_run_id, take_table_lease,
};
use crate::load::{ObjectKind, Ownership};
use crate::state::{LoadRecord, StateStore};
use anyhow::Context as _;

/// `rivet compact` arguments.
pub struct CompactArgs {
    pub config: String,
    pub run_id: Option<String>,
    /// Worker threads to merge the config's tables on; `None` takes the default
    /// pool (16, capped at the table count), not a sequential pass.
    pub pool: Option<usize>,
}

/// What `rivet compact` may do with the base it is about to MERGE into.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CompactGate {
    Go,
    /// Proceed, saying why the check could not be made.
    Note(String),
    Refuse(String),
}

/// Whether the buffer may be merged into `base_fqtn`, from the two facts the
/// glue can cheaply fetch: what the base currently IS, and whether the ledger
/// knows rivet loaded it.
///
/// The load path checks both before it overwrites a table; compaction wrote
/// through BigQuery's own error message instead — `Not found: Table ... in
/// location US` for an absent base, and NOTHING at all for a base rivet never
/// loaded, which a MERGE would happily rewrite.
pub(crate) fn compact_gate(
    base: ObjectKind,
    ownership: Ownership,
    base_fqtn: &str,
    buffer_fqtn: &str,
) -> CompactGate {
    match (base, ownership) {
        (ObjectKind::Absent, _) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: the base table does not \
             exist. The buffer holds changes for a table that was never loaded — load the \
             backfill first (the `cdc.backfill:` export builds the base), or drop the buffer \
             to discard EVERY change buffered since the last compaction (it accumulates \
             across loads). Nothing was merged and the buffer is untouched"
        )),
        // Both layout levers are named, written one FIRST, because they have a
        // PRECEDENCE and this message used to name only the loser. `cdc_layout`
        // matches a written `load.layout:` before it consults `cdc.backfill:`
        // (plan.rs), and `rivet init` WRITES `layout: base_buffer` for every
        // compactable export — so "remove `cdc.backfill:`" was a no-op on the
        // generated config, returning this very refusal again, which left the
        // destructive branch as the only instruction that worked. The sibling
        // predicate `compact_skip_reason` has always named both.
        (ObjectKind::View, _) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: that name is a VIEW — the \
             current-state view of the changelog+view layout, which has no base to merge into. \
             To move to base+buffer, drop the view and `{buffer_fqtn}`; to stay on the view, \
             set `load.layout: log_view` (or delete a written `layout:` key — a written one \
             WINS over `cdc.backfill:`, and `rivet init` writes it), and remove `cdc.backfill:` \
             from the export if it has one"
        )),
        (ObjectKind::Other, _) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: it exists and is neither a \
             table nor a view"
        )),
        (ObjectKind::Table, Ownership::Foreign) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: it exists, and this state \
             DB's load ledger has no record of rivet loading it — a MERGE would rewrite someone \
             else's rows. Drop or rename it, or point the export at another table"
        )),
        (ObjectKind::Table, Ownership::Unreadable) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: the load ledger could not be \
             read to confirm rivet loaded it. This is NOT the stateless case — a ledger is \
             configured and the query failed, so a MERGE may rewrite someone else's rows and the \
             record that would prove otherwise is unavailable. Fix the state backend and re-run; \
             nothing was merged and the buffer is untouched"
        )),
        (ObjectKind::Table, Ownership::Unknown) => CompactGate::Note(format!(
            "  note: `{base_fqtn}` exists and there is no load ledger to confirm rivet loaded it \
             — compacting on its shape alone"
        )),
        (ObjectKind::Table, Ownership::Own) => CompactGate::Go,
    }
}

/// Why `rivet compact` passes a table by, or `None` for a base-and-buffer CDC table.
fn compact_skip_reason(
    mode: &load::plan::LoadMode,
    layout: &load::plan::CdcLayout,
) -> Option<&'static str> {
    match mode {
        // A full load OVERWRITES the whole table on every pass, so there is never
        // an accumulated buffer to merge — whatever a layout says.
        load::plan::LoadMode::Full => Some("a full load overwrites its table; nothing to merge"),
        // Otherwise compaction belongs to the LAYOUT, not the mode: any table kept
        // as a physical base with a disposable buffer has something to merge. An
        // incremental export asks for that with `load.layout: base_buffer`.
        _ if layout.compacts() => None,
        load::plan::LoadMode::Cdc => Some(
            "a changelog + view table (no `cdc.backfill:` and no `load.layout: base_buffer`); \
             nothing to merge",
        ),
        load::plan::LoadMode::Incremental => Some(
            "a changelog + view table; `load.layout: base_buffer` gives it a base to merge into",
        ),
    }
}

/// What decides the WINNER in this table's compaction: a CDC stream ranks by its
/// log position, an incremental export by the cursor its current state is ordered
/// on. One resolver so a mode that gains compaction cannot forget to say.
fn compact_order_of(
    plan: &load::plan::LoadPlan,
    engine: Option<load::cdc::SourceEngine>,
) -> Result<load::cdc::CompactOrder> {
    match plan.mode {
        load::plan::LoadMode::Cdc => Ok(load::cdc::CompactOrder::Cdc(
            engine.context("a cdc plan needs its source engine to rank the buffer")?,
        )),
        _ => plan
            .cursor_column
            .clone()
            .map(load::cdc::CompactOrder::Cursor)
            .with_context(|| {
                format!(
                    "compacting `{}` needs the export's `cursor_column:` — the buffer's \
                     latest-per-key ordering",
                    plan.table
                )
            }),
    }
}

/// Compact's PRE-MERGE phase, with every stop marked as one.
///
/// A named seam because the WIRING is the thing that was wrong, and wiring is only
/// gradeable at a boundary a test can stand on: `run_compacts` is live-only, so a
/// test of `compact_gate_of` alone grades correct logic on an input the real caller
/// never hands it, while the defect lived in what the caller did with the error.
///
/// Everything this covers is metadata — `object_kind` is a warehouse QUERY, so a
/// 503, a quota error or expired credentials arrive here having written NOTHING.
/// Unwrapped they reached the ledger as `status='failed'`, and `has_load_attempt`
/// reads a `failed` row as "rivet wrote this table", which flips the base from
/// `Foreign` to `Own` and disarms the refusal that stops a later `rivet load`
/// overwriting a table rivet never wrote.
fn compact_preflight(
    loader: &dyn load::TargetLoader,
    table: &str,
    state: Option<&StateStore>,
) -> Result<()> {
    load::before_write(compact_gate_of(loader, table, state))
}

/// The two metadata reads [`compact_gate`] decides on, and the note it may
/// print. Glue: it fetches the facts, the decision itself is the pure predicate.
fn compact_gate_of(
    loader: &dyn load::TargetLoader,
    table: &str,
    state: Option<&StateStore>,
) -> Result<()> {
    let buffer = format!("{table}__changes");
    if !matches!(loader.object_kind(&buffer)?, load::ObjectKind::Table) {
        // No buffer: `compact` says that no-op itself, and a missing base is not a
        // problem when there is nothing to merge into it.
        return Ok(());
    }
    let base_fqtn = loader.fqtn(table);
    // See the sibling in `prepare_load`: an UNANSWERABLE ledger must not read as an
    // ABSENT one, or a failed probe turns compact's `Foreign` refusal into a note and
    // the MERGE rewrites someone else's rows.
    let ownership = ownership_of(state, &base_fqtn, "compact");
    match compact_gate(
        loader.object_kind(table)?,
        ownership,
        &base_fqtn,
        &loader.fqtn(&buffer),
    ) {
        CompactGate::Go => Ok(()),
        CompactGate::Note(note) => {
            eprintln!("{note}");
            Ok(())
        }
        CompactGate::Refuse(msg) => Err(load::refused(msg)),
    }
}

/// `rivet compact`: merge every base-and-buffer table's buffer into its base and
/// drop the buffer. One MERGE per table (per partition window), labelled
/// `rivet_op:merge`; a table without a buffer is a no-op, said so.
pub fn run_compacts(args: CompactArgs) -> Result<()> {
    let plans = load::plan::plan_loads(&args.config)?;
    let run_id = resolve_run_id(args.run_id.clone());
    let state = open_state(&args.config, "compacting without a ledger");
    let cfg = crate::config::Config::load(&args.config).context("parsing rivet config")?;
    let engine = if needs_source_engine(&plans) {
        Some(load::plan::source_engine(&args.config)?)
    } else {
        None
    };
    // Counted from several workers, so an atomic — read once after the join for
    // the failure message below. Skipped tables are not attempts, so the count
    // still happens AFTER the skip gate, exactly where `attempted += 1` sat.
    //
    // The cheaper shape — filter `plans` BEFORE the pool, so `attempted` is just
    // `filtered.len()` and no atomic is needed — was considered and rejected: it
    // moves every "skipped — <why>" line to the front of the run in one block,
    // while the sequential loop interleaved them with the work. Keeping the skip
    // INSIDE the worker keeps `--pool 1` printing exactly what it always printed.
    let attempted = std::sync::atomic::AtomicUsize::new(0);
    // Same as the load leg: the parent migrated the schema before any thread starts
    // and has handed over its `StateRef`. Holding it would leave `--pool 1` with TWO
    // connections and TWO migrations — on both backends — where the sequential loop
    // had one of each.
    let (state_ref, parent_had_state) = hand_off_state(state, args.pool, plans.len());
    let outcomes = load::pool::run_workers(
        &plans,
        load::pool::effective_pool(args.pool, plans.len()),
        || reconnect(state_ref.as_ref(), "compact"),
        |state, _idx, plan| {
            if let Some(why) = compact_skip_reason(&plan.mode, &plan.layout) {
                eprintln!("  compact [{}]: skipped — {why}", plan.table);
                return Ok(());
            }
            // Counted BEFORE the refusal below, not after: a refused table IS an
            // attempt — the run took it up and then refused it — and it becomes a
            // FAILURE in the fold. Counting it only on the far side of the refusal
            // let the numerator include tables the denominator did not, so a run
            // that refused two of three printed "2 of 1 compacted table(s) failed".
            // The sequential loop could not reach that state: it had no refusal, so
            // `n <= attempted` held by construction. A table the skip gate above
            // dropped is still NOT an attempt, which is why this sits below it.
            attempted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // AFTER the skip gate on purpose: a table this run would not compact
            // anyway needs no ledger, so refusing it would be noise. One that WOULD
            // compact must not proceed without a lease — `rivet load` may hold it,
            // and a ledger-less worker takes no lease at all rather than being
            // refused (the lease is `state.map(..)`, so `None` skips it).
            if load::pool::reconnect_failure_is_fatal(parent_had_state, state.is_some()) {
                anyhow::bail!(
                    "`{}`: this worker could not reopen the state ledger the run started \
                     with — refusing the table rather than compacting it without a lease. \
                     Lower `--pool`, or fix the state backend.",
                    plan.table
                );
            }
            let load_id = ledger_load_id(&run_id, "compact", &plan.table);
            let outcome = (|| -> Result<()> {
                let pinned = pin_plan_to_its_run(plan, state.as_ref(), &cfg, "compact")?;
                let loader = load::build_loader(&pinned, &run_id);
                let target_fqtn = loader.fqtn(&pinned.table);
                let _lease = take_table_lease(state.as_ref(), &target_fqtn)?;
                // The export's OWN mode, not a hardcoded label. Compact runs on
                // `incremental` exports too — `compact_skip_reason` says so in as many
                // words — and telling the operator of a `mode: incremental` config that
                // their export "is mode: cdc" sends them looking for a `cdc:` block
                // their config cannot even contain (it is a config-load error there).
                // Measured on a 60-table incremental run: two refusals, both mislabelled.
                let pk = require_pk(&pinned, pinned.mode.ledger_str())?;
                // The base is checked BEFORE the MERGE, and only when a buffer exists:
                // an absent base surfaced as BigQuery's own `Not found: Table`, and a
                // base rivet never loaded was not checked at all. Metadata, no job.
                // `before_write` on the whole PRE-MERGE prefix, not just on the arm
                // the gate refuses through. Everything up to `loader.compact` is
                // metadata: `compact_gate_of`'s first statement is
                // `loader.object_kind(&buffer)?`, a real warehouse QUERY, so expired
                // credentials, a 503 or a quota error surface here having touched
                // nothing — and `ledger_status` maps anything that is not a `Refused`
                // to "failed". `has_load_attempt` then counts that row, which flips
                // the base from `Foreign` to `Own` and disarms the refusal that stops
                // `rivet load` overwriting a table rivet never wrote. The load path
                // has wrapped its pre-write stops from the start (ten sites); this
                // path had none, while the comment on the ledger row below claimed
                // the protection.
                let report = match compact_preflight(loader.as_ref(), &pinned.table, state.as_ref())
                    .and_then(|()| load::before_write(compact_order_of(&pinned, engine)))
                {
                    Err(e) => Err(e),
                    // Past the wrap: from here a failure may genuinely have written,
                    // so it must stay a `failed` row — that is what tells the next
                    // cycle the table is rivet's own.
                    Ok(order) => {
                        // The MERGE reads its tombstone arm off the specs it is HANDED,
                        // and the recorded spec holds source columns only — the load leg
                        // appends the flag to its own copy. Compact must do the same or
                        // every delete merges as an ordinary upsert and every insert
                        // lands with a NULL flag. `compact_skip_reason` already kept
                        // non-compacting layouts out — that gate is the `true` here, so
                        // the ONE predicate that owns this decision is the one asked.
                        let mut specs = pinned.specs.clone();
                        if load::plan::base_carries_delete_flag(true, pinned.deleted_flag) {
                            specs.push(load::cdc::flag_spec(loader.warehouse()));
                        }
                        loader.compact(&pinned.table, &specs, pk, order)
                    }
                };
                if let Some(s) = state.as_ref() {
                    let rec = LoadRecord {
                        load_id: load_id.clone(),
                        export_name: pinned.table.clone(),
                        target_table: target_fqtn.clone(),
                        warehouse: pinned.load.target.name().to_string(),
                        mode: "compact".to_string(),
                        source_run_ids: Vec::new(),
                        source_ident: String::new(),
                        rows_loaded: report.as_ref().map_or(0, |r| r.changes_rows as i64),
                        status: match &report {
                            Ok(_) => "success".to_string(),
                            // A refusal is a stop before the write, exactly as on the
                            // load path — never a `failed` row that makes the target
                            // look like rivet's own on the next attempt.
                            Err(e) => ledger_status(e).to_string(),
                        },
                        finished_at: chrono::Utc::now().to_rfc3339(),
                    };
                    if let Err(e) = s.store_load(&rec) {
                        eprintln!(
                            "  warning: compact ledger write failed for `{target_fqtn}`: {e:#}"
                        );
                    }
                }
                let report = report?;
                if report.had_buffer {
                    println!(
                        "COMPACT OK [{}]: {} change row(s) merged into `{}` in {} MERGE statement(s); buffer dropped",
                        plan.table, report.changes_rows, report.base, report.merge_jobs
                    );
                } else {
                    println!(
                        "COMPACT SKIP [{}]: no `{}__changes` buffer — nothing to merge",
                        plan.table, plan.table
                    );
                }
                Ok(())
            })();
            if let Err(e) = &outcome {
                eprintln!("  COMPACT FAILED [{}]: {e:#}", plan.table);
            }
            outcome.map_err(|e| e.context(format!("compact '{}'", plan.table)))
        },
        |plan| no_outcome_error("compact", &plan.table),
    );
    // Folded in CONFIG order, like the load leg: the pool returns one result per
    // table, indexed by the table, so the `|`-joined list below reads the same on
    // every run of the same failing config.
    let mut failures = failures_of(outcomes);
    let attempted = attempted
        .load(std::sync::atomic::Ordering::Relaxed)
        .max(failures.len());
    match failures.len() {
        0 => Ok(()),
        1 => Err(failures.pop().unwrap()),
        // COVERAGE, stated rather than implied: nothing calls `run_compacts` but
        // `dispatch`, and no test — offline or live — drives this aggregate. The
        // `n <= attempted` fix above is therefore correct by READING only. A unit
        // test over the formatter would grade correct logic on inputs the supplier
        // never produces (the ordering INSIDE the worker is the defect surface), so
        // the honest close is a live compact whose ledger drops mid-run.
        n => anyhow::bail!(
            "{n} of {attempted} compacted table(s) failed: {}",
            failures
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join(" | ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// COMPACT's pre-merge stops are refusals too — the sibling of
    /// `a_refused_load_does_not_make_a_foreign_table_rivets_own`, and it was missing.
    ///
    /// The load path has wrapped its pre-write stops in `before_write` from the start
    /// (ten sites). `run_compacts` had NONE, so every error that was not already a
    /// `Refused` journaled `status='failed'` against the BASE — including errors from
    /// `compact_gate_of`, whose very first statement is `loader.object_kind(&buffer)?`,
    /// a warehouse QUERY. An expired credential or a 503 there wrote a `failed` row
    /// having touched nothing, `has_load_attempt` counted it, and the base flipped
    /// from `Foreign` to `Own` — disarming the guard that stops a later `rivet load`
    /// overwriting a table rivet never wrote. The comment on that ledger row claimed
    /// the protection the code did not have.
    ///
    /// Graded at the SEAM, not at `compact_gate_of`: the gate was always right, and
    /// the defect was in what the caller did with its error.
    ///
    /// RED against dropping the `before_write` in `compact_preflight`.
    #[test]
    fn a_compact_stopped_before_the_merge_is_refused_not_failed() {
        let probe_failed = load::tests::FakeLoader::probe_fails("503 Service Unavailable");
        let err = compact_preflight(&probe_failed, "orders", None)
            .expect_err("a metadata probe that will not answer must stop the compact");
        assert_eq!(
            ledger_status(&err),
            "refused",
            "nothing was merged, so this must NOT journal `failed` — a failed row is \
             what `has_load_attempt` reads as `rivet wrote this table`: {err:#}"
        );
        assert!(
            format!("{err:#}").contains("503"),
            "and the real cause survives the wrap: {err:#}"
        );
    }

    /// `rivet compact` merges every base-and-buffer table, CDC or incremental; every
    /// other plan is passed by with a reason that names what it is, never silently.
    #[test]
    fn compact_passes_by_everything_but_a_base_and_buffer_cdc_table() {
        use crate::load::plan::{CdcLayout, LoadMode};
        assert_eq!(
            super::compact_skip_reason(&LoadMode::Cdc, &CdcLayout::BaseAndBuffer),
            None
        );
        // The layout is decided by `cdc.backfill:` / `load.layout:`, never by `initial:`
        // — the reason must name the levers that exist, not one that changes nothing.
        assert!(
            super::compact_skip_reason(&LoadMode::Cdc, &CdcLayout::LogAndView)
                .is_some_and(|w| w.contains("cdc.backfill:")
                    && w.contains("load.layout: base_buffer")
                    && !w.contains("initial:"))
        );
        assert!(
            super::compact_skip_reason(&LoadMode::Full, &CdcLayout::LogAndView)
                .is_some_and(|w| w.contains("overwrites its table"))
        );
        // The LAYOUT decides, not the mode: an ordinary incremental export that
        // asked for `load.layout: base_buffer` has a buffer to merge, and one that
        // did not is skipped with the key that would give it one.
        assert_eq!(
            super::compact_skip_reason(&LoadMode::Incremental, &CdcLayout::BaseAndBuffer),
            None
        );
        assert!(
            super::compact_skip_reason(&LoadMode::Incremental, &CdcLayout::LogAndView)
                .is_some_and(|w| w.contains("base_buffer"))
        );
    }
}

#[cfg(test)]
mod compact_gate_tests {
    use super::*;

    /// Every shape the base can be in when `rivet compact` reaches it. The two
    /// silent ones are why this exists: an ABSENT base used to surface as
    /// BigQuery's `Not found: Table`, and a FOREIGN one was not checked at all —
    /// the MERGE would have rewritten rows rivet never loaded.
    #[test]
    fn the_compact_gate_refuses_every_base_that_is_not_rivets_own_table() {
        let go = compact_gate(ObjectKind::Table, Ownership::Own, "p.d.t", "p.d.t__changes");
        assert_eq!(go, CompactGate::Go);

        let unknown = compact_gate(
            ObjectKind::Table,
            Ownership::Unknown,
            "p.d.t",
            "p.d.t__changes",
        );
        let CompactGate::Note(note) = unknown else {
            panic!("a stateless compact proceeds with a note: {unknown:?}")
        };
        assert!(note.contains("no load ledger"), "{note}");

        // An UNREADABLE ledger is not an ABSENT one, and the two must not share the
        // Note arm: `Unknown` means the operator chose to run without a ledger, while
        // `Unreadable` means the ledger that would have said "foreign" is the thing
        // that broke. Both used to arrive here as `Unknown`.
        let unreadable = compact_gate(
            ObjectKind::Table,
            Ownership::Unreadable,
            "p.d.t",
            "p.d.t__changes",
        );
        assert!(
            matches!(unreadable, CompactGate::Refuse(_)),
            "an unreadable ledger must REFUSE, never proceed like a stateless run: {unreadable:?}"
        );
        let CompactGate::Refuse(msg) = &unreadable else {
            unreachable!()
        };
        assert!(
            msg.contains("NOT the stateless case"),
            "the refusal must say which of the two empty answers this is: {msg}"
        );

        for (kind, ownership, wanted) in [
            (
                ObjectKind::Absent,
                Ownership::Own,
                "the base table does not exist",
            ),
            (ObjectKind::View, Ownership::Own, "that name is a VIEW"),
            (
                ObjectKind::Other,
                Ownership::Own,
                "neither a table nor a view",
            ),
            (
                ObjectKind::Table,
                Ownership::Foreign,
                "no record of rivet loading it",
            ),
            (
                ObjectKind::Table,
                Ownership::Unreadable,
                "could not be read",
            ),
        ] {
            let gate = compact_gate(kind, ownership, "p.d.t", "p.d.t__changes");
            let CompactGate::Refuse(msg) = gate else {
                panic!("{kind:?}/{ownership:?} must refuse: {gate:?}")
            };
            assert!(msg.contains(wanted), "{kind:?}/{ownership:?}: {msg}");
            assert!(
                msg.contains("`p.d.t__changes`") && msg.contains("`p.d.t`"),
                "the refusal names both tables: {msg}"
            );
        }
    }

    /// The VIEW refusal names the layout lever that actually DECIDES.
    ///
    /// It offered two escapes and the non-destructive one was inert on exactly the
    /// configs rivet generates: `cdc_layout` matches a WRITTEN `load.layout:` before
    /// it consults `cdc.backfill:`, and `rivet init` writes `layout: base_buffer` for
    /// every compactable export. So "remove `cdc.backfill:` from the export" returned
    /// this same refusal, and the only instruction that worked was the destructive
    /// one — drop the view. On a `mode: incremental` export it was worse than inert:
    /// a `cdc:` block is a config-load error there, so the key named cannot exist.
    ///
    /// This pins the SPELLINGS, not a fragment both would satisfy — the key with its
    /// underscore and the block it lives in — because the assertion that let the
    /// `--allow-source-drift` message stay wrong for months was one loose enough to
    /// admit either form.
    #[test]
    fn the_view_refusal_names_the_written_layout_key_that_overrides_the_derived_one() {
        let gate = compact_gate(ObjectKind::View, Ownership::Own, "p.d.t", "p.d.t__changes");
        let CompactGate::Refuse(msg) = gate else {
            panic!("a VIEW base must refuse: {gate:?}")
        };
        assert!(
            msg.contains("`load.layout: log_view`"),
            "the non-destructive escape must name the key that WINS, with its block and \
             its underscore — a message naming only `cdc.backfill:` sends the operator \
             to a no-op on every generated config: {msg}"
        );
        assert!(
            msg.contains("WINS over `cdc.backfill:`"),
            "and it must say WHICH lever wins, or the reader cannot tell why removing \
             the other one changed nothing: {msg}"
        );
    }
}
