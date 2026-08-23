//! **Layer: Execution** — the single home for committing one output part.
//!
//! Every runner (single, chunked-sequential, keyset, chunked-parallel,
//! sequential_checkpoint, parallel_checkpoint) produces parts and must commit
//! each to the destination in the ADR-0001 order: the temp writer is finalized
//! (I1), the file is written to the destination, the manifest part is recorded
//! (I2/M1), the file log advances (I7, warn-on-fail), and the part is
//! journaled. Before this module that sequence — plus the `files_committed`
//! counter and the crash-injection fault points — was hand-copied into each
//! runner and had already drifted (the keyset runner never bumped
//! `files_committed` and had no fault hooks; only `single` did both;
//! parallel_checkpoint never populated `summary.manifest_parts` at all,
//! producing an empty cloud manifest — see commit e9b0796).
//!
//! Split at the line where the parallel engine forks: the WORKER writes the file
//! ([`write_part_file`] — I1 + `dest.write` + fingerprint, safe off-thread) and
//! the caller records it ([`record_part`] — I2/M1 + counters + journal + I7).
//! Sequential runners call both inline; the parallel engine writes in the worker
//! and records in the drained parent loop, so the I2/I7 ordering lives once for
//! both engines.
//!
//! ## In-step ordering within [`record_part`]
//!
//! The four side effects fire in this fixed order:
//!
//! 1. byte / file counter bumps on `summary` (`bytes_written`,
//!    `files_produced`, `files_committed`)
//! 2. **ADR-0012 M1** — `manifest_writer::record_committed_part_with_fingerprint`
//!    appends to `summary.manifest_parts`
//! 3. `summary.journal.record` (`FileWritten` or `ChunkCompleted`)
//! 4. **ADR-0001 I7** — `state.record_file` (warn-on-fail; non-fatal)
//!
//! Pre-commit_part, some runners had I7 before M1 (the chunked-parallel
//! engine in particular called `state.record_file` from each worker then
//! appended to `manifest_parts` in the post-scope drain). Both orderings
//! are correct because no externally-observable durability contract exists
//! *between* the two writes — the run has not finalized, so neither the
//! cloud manifest nor the cursor have advanced past the part. Picking one
//! order at the seam keeps all five runners consistent and makes the
//! `cfg!(debug_assertions)` coherence check in
//! [`crate::pipeline::finalize::finalize_manifest`] meaningful.

use std::path::Path;

use super::manifest_writer;
use super::summary::RunSummary;
use crate::destination::Destination;
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

/// Add this invocation's row count onto the run's cumulative total. The parallel
/// runners aggregate their workers' rows into an atomic and land it here at the
/// end; on a checkpoint RESUME the summary already carries the rehydrated
/// pre-crash base (`rehydrate_manifest_parts_from_file_log` bumps `total_rows`
/// alongside `files_committed` / `bytes_written` / `manifest_parts`). This MUST
/// accumulate (`+=`), never assign — a bare `summary.total_rows = agg` clobbers
/// that base, so on resume `total_rows` under-reports (only this run's rows)
/// while every other aggregate stays cumulative, breaking the
/// `total_rows == sum(manifest_parts.rows)` coherence invariant and diverging from
/// the sequential runner (which already `+=`s). The seam exists so no parallel
/// runner can reintroduce the clobber — a future `= agg` is obviously wrong next
/// to this call.
pub(in crate::pipeline) fn accumulate_run_rows(summary: &mut RunSummary, this_run_rows: i64) {
    summary.total_rows += this_run_rows;
}

/// ADR-0029: the COMMIT UNIT a durable part and its Form-B checksum
/// contribution both belong to — the key the seam computes coverage on.
///
/// It is deliberately the unit each RUNNER commits at, not "a part": a part is
/// what lands on disk, a unit is what the runner declares complete. The two
/// differ, and that difference is the whole reason this type exists — the
/// parallel keyset runner publishes parts per PAGE (durability must show what
/// is physically on disk, the #200-1 fix) but publishes checksums per committed
/// RANGE, so its unit is [`UnitId::Range`] while `PartKind` still journals the
/// page. Pairing them by page would report every healthy range short.
///
/// Which unit a runner uses is stated at its `record_part` call site AND at its
/// checksum feed; the two must agree, and `check_post_run_invariants` fails the
/// run in debug builds when they do not (a mismatch would otherwise suppress
/// Form B on a healthy run — silently, and in the fail-safe direction, which is
/// exactly the shape this repo has shipped twice).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum UnitId {
    /// The whole runner invocation. `single` reads once into ONE sink that
    /// accumulates the checksums of every part it writes, so the sink's drain
    /// covers all of them or (on a mid-write bail) none — there is no smaller
    /// unit to key on, and pretending there is would be a lie about the sink.
    Run,
    /// A chunk window — the chunked runners (sequential / parallel / both
    /// checkpoint twins) and a `mongo_parallel` worker range.
    Chunk(i64),
    /// A keyset PAGE — the sequential keyset runner, which feeds the page's
    /// checksums and records the page's parts in the same loop iteration.
    Page(i64),
    /// A keyset RANGE — the parallel keyset runner (see the type doc).
    Range(i64),
}

/// ADR-0029 half 1 — facts that describe what the runner SAW.
///
/// They carry NO coverage obligation, they are monotonic, and recording one
/// early can never be wrong — so runners feed them EAGERLY, above any error
/// bail, and `finalize_export_records` applies them unconditionally on the
/// failure path. Before the split they rode in the same feed as the checksums
/// and were lost with them: a failed parallel-keyset run recorded the STALE
/// open-time fingerprint over durable parquet carrying the observed schema.
#[derive(Debug, Clone, Default)]
pub(crate) struct Observations {
    /// The run's dest schema (first non-empty page/sink wins) — drives the
    /// manifest fingerprint pin and the post-run `on_schema_drift` gate.
    /// `None` on runners whose drift gate runs elsewhere by design (chunked
    /// checks pre-chunk via `check_from_type_mappings`, ADR-0021).
    pub(in crate::pipeline) drift_schema: Option<arrow::datatypes::Schema>,
    /// Max observed byte length per column (shape-drift warn input); merged
    /// by max so worker/part order is irrelevant.
    pub(in crate::pipeline) column_max_bytes: std::collections::HashMap<String, u64>,
}

/// ADR-0029 half 2 — the integrity record, which is only meaningful as a set
/// covering EXACTLY the parts the manifest lists.
///
/// Stays commit-gated (a runner feeds a unit's checksums only once that unit
/// committed), and the seam no longer TRUSTS that gating: it computes coverage
/// from [`Integrity::part_units`] vs [`Integrity::covered_units`] and suppresses
/// Form B itself when a recorded part's unit contributed nothing.
#[derive(Debug, Clone, Default)]
pub(crate) struct Integrity {
    /// Run-wide sum-combined per-column value checksums (Form B input).
    pub(in crate::pipeline) column_checksums: std::collections::BTreeMap<String, u64>,
    /// The key column the checksums are keyed to (first runner-reported wins).
    pub(in crate::pipeline) checksum_key_column: Option<String>,
    /// `manifest_parts.part_id` → the commit unit that produced it, written by
    /// [`record_part`] for every part it records. Keyed on the manifest's own
    /// part_id (not the path) so it is O(8 bytes) per part rather than a second
    /// copy of every path, and so a DEDUPED re-read (which keeps the existing
    /// part_id) re-registers the same entry instead of double-counting.
    pub(in crate::pipeline) part_units: std::collections::BTreeMap<u32, UnitId>,
    /// The units that actually contributed checksums. A unit is registered even
    /// when its map is EMPTY (a CSV/JSONL sink computes none, a zero-row chunk
    /// has none): the unit did its part, and the absence of a checksum is a
    /// FORMAT fact, not a coverage fact.
    pub(in crate::pipeline) covered_units: std::collections::BTreeSet<UnitId>,
}

/// ADR-0028: the run-wide tail ledger, filled by the runners as they commit and
/// APPLIED once by [`crate::pipeline::finalize::finalize_export`] — the one seam
/// the dispatcher calls between the runner returning and `finalize_manifest`.
///
/// This exists to retire the runner-bypass class: before it, every runner
/// re-assembled the same post-write tail by hand (fingerprint pin → drift gate →
/// Form-B harvest → shape warn), and a feature wired into one runner was
/// silently absent on the others (`keyset.rs` said so verbatim; Form-B was
/// computed-then-discarded on all three large-table runners). Runners now only
/// FEED this ledger — what schema they saw, which checksums their sinks
/// computed — and the APPLICATION lives in exactly one place, ordering encoded
/// once. The telltale invariants in `check_post_run_invariants` (drift verdict
/// present, Form-B harvested-or-flagged) go RED if a runner feeds nothing.
///
/// ADR-0029 split it in two by TRUTH CONDITION, because holding facts with
/// different truth conditions in one structure with one lifecycle, fed at one
/// moment, is what made the failure path lose the fingerprint: an
/// [`Observations`] is true the moment the runner SEES it, an [`Integrity`]
/// record is only true of a unit that COMMITTED. They are fed at different
/// moments now, and the seam computes the integrity half's coverage rather than
/// trusting the order it was fed in.
#[derive(Debug, Clone, Default)]
pub(crate) struct CommitLedger {
    /// What the runner SAW — fed eagerly, applied on both paths.
    pub(in crate::pipeline) observed: Observations,
    /// What COMMITTED — commit-gated, coverage computed at harvest.
    pub(in crate::pipeline) integrity: Integrity,
}

impl CommitLedger {
    /// First-wins schema note (idempotent run-wide, like the fingerprint pin).
    /// An OBSERVATION: feed it as soon as a schema is in hand, above any bail.
    pub(in crate::pipeline) fn note_schema(&mut self, schema: &arrow::datatypes::Schema) {
        if self.observed.drift_schema.is_none() {
            self.observed.drift_schema = Some(schema.clone());
        }
    }

    /// Max-merge one sink's observed per-column byte lengths. An OBSERVATION.
    pub(in crate::pipeline) fn merge_shape(
        &mut self,
        max_bytes: &std::collections::HashMap<String, u64>,
    ) {
        for (col, len) in max_bytes {
            let e = self
                .observed
                .column_max_bytes
                .entry(col.clone())
                .or_insert(0);
            *e = (*e).max(*len);
        }
    }

    /// ADR-0029: contribute one COMMITTED unit's Form-B checksums, keyed by the
    /// unit so the seam can compare them against the parts that unit recorded.
    ///
    /// This is the ONLY way checksums enter the ledger — there is no unkeyed
    /// merge, because an unkeyed contribution is exactly the "trust the feed
    /// order" the ADR removes. Registering the unit is unconditional (see
    /// [`Integrity::covered_units`]); the checksum fold is commutative
    /// (wrapping add — see [`accumulate_column_checksums`]) so worker order and
    /// a re-contribution of the same unit both behave.
    pub(in crate::pipeline) fn contribute_checksums(
        &mut self,
        unit: UnitId,
        part: &std::collections::BTreeMap<String, u64>,
        key_column: Option<String>,
    ) {
        self.integrity.covered_units.insert(unit);
        accumulate_column_checksums(&mut self.integrity.column_checksums, part);
        // First-Some-wins key column (all pages/workers of a run share one key).
        if self.integrity.checksum_key_column.is_none() {
            self.integrity.checksum_key_column = key_column;
        }
    }

    /// ADR-0029: note which commit unit a just-recorded manifest part belongs
    /// to. Called only from [`record_part`], so no runner can record a part
    /// without declaring its unit.
    fn note_part(&mut self, part_id: u32, unit: UnitId) {
        self.integrity.part_units.insert(part_id, unit);
    }
}

/// ADR-0029 — the computed coverage verdict for one run's Form-B record.
///
/// `complete` is the only thing that decides whether Form B is recorded;
/// `short_cover` distinguishes the two reasons it can be short, and that
/// distinction is what turns "Form B is absent" from ambiguous into a recorded
/// verdict (ADR-0029 Consequences):
///
/// * a part with NO ledger entry at all — it did not go through `record_part`
///   this run, i.e. a checkpoint-resume rehydration or an M8 `Skip` clone of a
///   prior manifest's part. Legitimate and expected; Form B is suppressed
///   because its per-column contribution is genuinely unrecoverable.
/// * a part whose unit contributed NOTHING (`short_cover`) — the unit never
///   committed (a failed keyset range, a mid-write bail), or the runner paired
///   the part and the checksum under DIFFERENT unit ids. On a run whose runner
///   SUCCEEDED the first cannot happen, so `short_cover` there is the unit-id
///   mismatch ADR-0029 names as its own risk, and `check_post_run_invariants`
///   fails the run rather than let it suppress Form B in silence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct Coverage {
    pub(in crate::pipeline) complete: bool,
    pub(in crate::pipeline) short_cover: bool,
    /// Parts recorded this run whose unit contributed no checksums.
    pub(in crate::pipeline) uncovered_parts: usize,
    /// Parts in the manifest that this run never recorded (rehydrated / cloned).
    pub(in crate::pipeline) foreign_parts: usize,
}

/// ADR-0029 — compare the contributions' unit set against the RECORDED PARTS'
/// unit set, over the manifest itself.
///
/// The manifest is the subject on purpose: it is the list the Form-B record
/// claims to cover, and it is the only list that also holds the parts a resume
/// hydrated without ever calling `record_part`. Keying the comparison on
/// `record_part` calls alone would have missed exactly those, which is the case
/// the manual `column_checksums_incomplete` flag existed for.
pub(in crate::pipeline) fn compute_coverage(
    parts: &[crate::manifest::ManifestPart],
    integrity: &Integrity,
) -> Coverage {
    let mut uncovered_parts = 0usize;
    let mut foreign_parts = 0usize;
    for p in parts {
        match integrity.part_units.get(&p.part_id) {
            None => foreign_parts += 1,
            Some(unit) if !integrity.covered_units.contains(unit) => uncovered_parts += 1,
            Some(_) => {}
        }
    }
    Coverage {
        complete: uncovered_parts == 0 && foreign_parts == 0,
        short_cover: uncovered_parts > 0,
        uncovered_parts,
        foreign_parts,
    }
}

/// XOR-accumulate one part/page/chunk sink's per-column value checksums into a
/// run-wide map. Order-independent (XOR), so chunk/page/worker order and count do
/// not change the result — the MULTI-PART runners (chunked / keyset / mongo_parallel)
/// call this per part so Form B is recorded RUN-WIDE, exactly as single mode records
/// its one sink's map. Without it, those runners computed the checksum per part then
/// discarded it, so `rivet validate`'s Form-B re-read was a silent no-op on the
/// large-table paths (see docs/runner-coverage-matrix.yaml `value_checksum_form_b`).
pub(in crate::pipeline) fn accumulate_column_checksums(
    acc: &mut std::collections::BTreeMap<String, u64>,
    part: &std::collections::BTreeMap<String, u64>,
) {
    for (name, sum) in part {
        // wrapping_add, not XOR: the combiner must be commutative (parts arrive
        // in no fixed order) WITHOUT being annihilating. Under `^` two parts
        // whose column checksums coincide — a duplicated part, or two parts of
        // identical duplicated values — cancelled to zero, and the run published
        // a checksum that verified anything. Same fold as `value_checksum::Fold::Sum`.
        let e = acc.entry(name.clone()).or_insert(0);
        *e = e.wrapping_add(*sum);
    }
}

/// Record the run-wide sum-combined checksums + key column into the summary, so
/// `finalize_manifest` writes Form B and `rivet validate` can re-verify the
/// Arrow→Parquet encode / post-write fault Form A cannot see. The single harvest
/// seam every runner goes through (single mode passes its one sink's map directly;
/// the multi-part runners pass the accumulated map).
pub(in crate::pipeline) fn harvest_column_checksums(
    summary: &mut RunSummary,
    integrity: Integrity,
) {
    // ADR-0029: COMPUTE the coverage rather than trust the feed order. A record
    // that covers only SOME of the manifest's parts would make `validate --depth
    // full` re-read every part, recompute the full sum, and report a FALSE
    // mismatch on correctly-written data — the authoritative manifest's integrity
    // record would be objectively wrong. Suppress instead: absent, not
    // partial-and-lying, the same graceful degradation a rehydrated part's empty
    // md5 gets. Pre-ADR-0029 this decision was a bool the resume paths had to
    // remember to SET; now it follows from the data (a rehydrated part simply has
    // no contribution), and the flag is the RESULT.
    let cov = compute_coverage(&summary.manifest_parts, &integrity);
    if !cov.complete {
        summary.column_checksums_incomplete = true;
        summary.column_checksums_short_cover |= cov.short_cover;
        log::warn!(
            "export '{}': Form B value-checksums suppressed — the run-wide checksum covers \
             only part of this manifest ({} part(s) hydrated from a prior run and carrying no \
             per-column checksum, {} part(s) whose commit unit contributed none). `validate` \
             will size-verify parts but skip the Form B value re-read for this run.",
            summary.export_name,
            cov.foreign_parts,
            cov.uncovered_parts,
        );
        return;
    }
    // Coverage is COMPLETE and there is nothing to record: no unit computed a
    // value checksum at all. That is a FORMAT fact, not an integrity shortfall —
    // the sink's `track_checksum` skips non-Parquet by design, so a CSV/JSONL
    // export legitimately lands here with every unit registered and an empty
    // map. Leave the flag CLEAR on purpose: `check_post_run_invariants`'s Form-B
    // telltale ("empty and not flagged incomplete") is what catches a parquet
    // runner that harvested nothing, and flagging here would excuse exactly that.
    //
    // The ORDER matters and cost a live regression to learn: this early return
    // used to sit ABOVE the coverage computation, and a parallel-keyset crash
    // resume that re-ran ZERO ranges then had an empty accumulator, skipped the
    // computation, and reached finalize with rehydrated parts and no suppression
    // flag — the telltale panicked. Pre-ADR-0029 the flag came from the manual
    // `rehydrate_manifest_parts_from_file_log` assignment this ADR retires, so
    // the computed rule must be reached on the empty-accumulator path too.
    if integrity.column_checksums.is_empty() {
        return;
    }
    summary.column_checksums = integrity
        .column_checksums
        .into_iter()
        .map(|(name, checksum)| crate::manifest::ColumnChecksum {
            name,
            checksum: checksum.to_string(),
        })
        .collect();
    summary.checksum_key_column = integrity.checksum_key_column;
}

/// A part written to the destination, ready to be recorded. Produced by
/// [`write_part_file`], consumed by [`record_part`].
pub(crate) struct PartRecord {
    pub file_name: String,
    pub rows: i64,
    pub bytes: u64,
    pub fingerprint: String,
    /// Base64 MD5 of the part body (GCS `md5Hash` encoding), computed from the
    /// local temp file alongside the fingerprint.  Empty if it could not be
    /// computed — verification degrades to size-only for that part.
    pub md5: String,
}

/// How a committed part is journaled.
///
/// - `File { part_index }` — written by the single-file (snapshot / incremental)
///   runner: emits `RunEvent::FileWritten`.
/// - `Chunk { chunk_index }` — the chunked runners (sequential / parallel /
///   sequential_checkpoint / parallel_checkpoint): emits
///   `RunEvent::ChunkCompleted` with the real chunk window's index.
/// - `Page { page_index }` — keyset (seek-paginated) runner: emits
///   `RunEvent::ChunkCompleted` *for backward journal compatibility* but
///   conceptually a keyset page is **not** a chunk (no `[start, end]`
///   integer window, no chunk-task lifecycle row in `chunk_task`). Carrying
///   it as a distinct seam variant keeps the caller's intent visible at
///   the call site and leaves room to fork to a dedicated
///   `RunEvent::KeysetPageWritten` later without touching the runners —
///   only the match arm in [`record_part`] would change.
pub(crate) enum PartKind {
    File {
        part_index: usize,
    },
    Chunk {
        chunk_index: i64,
    },
    Page {
        page_index: i64,
        /// The page's high-water key — written into the SAME file_log row as the part, so a
        /// crash-recovery resume reconciles the cursor from committed parts and never re-reads a
        /// committed page (v25 cursor-atomic keyset checkpoint). `None` on a non-keyset page.
        cursor_high: Option<String>,
    },
}

/// Seam 1 — ADR-0001 I1 + the destination-write boundary. Writes the
/// already-finalized temp file to the destination and computes its content
/// fingerprint (ADR-0012 M3) while the local temp file still exists. Safe to
/// call from a worker thread (touches no shared run state).
pub(crate) fn write_part_file(
    dest: &dyn Destination,
    tmp_path: &Path,
    rows: i64,
    file_name: String,
) -> Result<PartRecord> {
    let bytes = std::fs::metadata(tmp_path).map(|m| m.len()).unwrap_or(0);
    let outcome = dest.write(tmp_path, &file_name)?;
    // Both body hashes in one read: xxh3 fingerprint (ADR-0012 M3) + base64 MD5
    // (no-download destination verification, GCS md5Hash encoding).  Non-fatal —
    // on failure the fingerprint falls back to the zero placeholder and the md5
    // to empty (that part then verifies size-only).
    let (fingerprint, md5) =
        manifest_writer::compute_part_checksums(tmp_path).unwrap_or_else(|e| {
            log::warn!("part checksums failed for '{file_name}' (not fatal): {e:#}");
            (
                crate::manifest::SCHEMA_FINGERPRINT_UNAVAILABLE.to_string(),
                String::new(),
            )
        });
    // Fail-fast transit check (ADR-0001 I1): when the store reported its own
    // checksum, it computed it from the bytes it received — a mismatch with our
    // locally-computed MD5 means the upload corrupted in flight.  Free (no
    // round-trip): the checksum rode the write response.  Encodings differ
    // (GCS base64, S3 hex ETag), so compare normalised digest bytes.
    if let Some(stored) = &outcome.content_md5 {
        use crate::pipeline::manifest_reconcile::md5_digest_bytes;
        if let (Some(local), Some(remote)) = (md5_digest_bytes(&md5), md5_digest_bytes(stored))
            && local != remote
        {
            anyhow::bail!(
                "upload integrity check failed for '{file_name}': local MD5 differs from \
                 the store-reported checksum — the part corrupted in transit"
            );
        }
    }
    Ok(PartRecord {
        file_name,
        rows,
        bytes,
        fingerprint,
        md5,
    })
}

/// Seam 1a — finalize the sink's writer and write EVERY part it produced:
/// the rotated `completed_parts` plus the final partial temp file. Before
/// this seam, every chunked/keyset runner uploaded only `sink.tmp` — the
/// parts `ExportSink::maybe_split` had rotated at `max_file_size` were
/// silently deleted with the sink (total data loss at rotation boundaries;
/// pinned by the `roast_part_loss` live tests). `single` had its own
/// correct drain; this is that drain, hoisted to the seam so no runner can
/// re-introduce the gap.
///
/// `name_for(part_idx, part_count)` returns the destination file name —
/// pass [`part_indexed_name`] over the runner's legacy single-part name so
/// unrotated chunks (`part_count == 1`) keep their existing naming and
/// resumes of old runs stay compatible.
///
/// Validation (when `validate` is `Some`) runs per part against that
/// part's own row count — the only count the part actually contains.
pub(crate) fn write_sink_parts(
    dest: &dyn Destination,
    sink: &mut crate::pipeline::sink::ExportSink,
    validate: Option<crate::config::FormatType>,
    name_for: impl Fn(usize, usize) -> String,
) -> Result<Vec<PartRecord>> {
    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }
    if sink.part_rows > 0 {
        sink.completed_parts
            .push(crate::pipeline::sink::CompletedPart {
                tmp: std::mem::replace(&mut sink.tmp, tempfile::NamedTempFile::new()?),
                rows: sink.part_rows,
            });
        sink.part_rows = 0;
    }
    let count = sink.completed_parts.len();
    let mut recs = Vec::with_capacity(count);
    for (idx, part) in sink.completed_parts.drain(..).enumerate() {
        if let Some(fmt) = validate {
            crate::pipeline::validate::validate_output(part.tmp.path(), fmt, part.rows)?;
        }
        recs.push(write_part_file(
            dest,
            part.tmp.path(),
            part.rows as i64,
            name_for(idx, count),
        )?);
    }
    Ok(recs)
}

/// Sibling naming for rotated parts: a single-part chunk keeps its legacy
/// name; a rotated chunk suffixes every part with `_p{idx}` before the
/// extension (`orders_ts_chunk3.parquet` → `orders_ts_chunk3_p0.parquet`,
/// `_p1`, …) so siblings sort together and no name collides with the
/// legacy form.
pub(crate) fn part_indexed_name(base: &str, idx: usize, count: usize) -> String {
    if count <= 1 {
        return base.to_string();
    }
    match base.rsplit_once('.') {
        Some((stem, ext)) => format!("{stem}_p{idx}.{ext}"),
        None => format!("{base}_p{idx}"),
    }
}

/// Seam 2 — the single home for the post-write ordering that used to drift
/// across runners: the I2 fault window, the byte/file counters, the manifest
/// part (I2/M1), the journal event, and the warn-on-fail file-log write (I7).
/// Returns `true` iff the part was DEDUPED (a re-read overwrote a rehydrated part of the same
/// path); the caller (the keyset page loop) then skips the per-page `total_rows` bump because
/// rehydration already counted that page.
///
/// ADR-0029: `unit` is the COMMIT UNIT this part belongs to, and it must be the
/// SAME [`UnitId`] the runner passes to `CommitLedger::contribute_checksums` for
/// that unit — the seam compares the two sets to decide whether the Form-B
/// record covers the manifest. It is a separate argument from `kind` because the
/// two genuinely differ on one runner (parallel keyset journals a page and
/// commits a range); everywhere else they coincide and the call site says so.
pub(crate) fn record_part(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
    part: &PartRecord,
    kind: PartKind,
    unit: UnitId,
) -> bool {
    // ADR-0001 I2→I3 crash window: file at destination, manifest not yet updated.
    crate::test_hook::maybe_panic_at("after_file_write");

    // ADR-0012 M1: record the committed part for the finalizer's RunManifest. Returns whether it
    // DEDUPED (a re-read overwrote a rehydrated part of the same path — the keyset after-manifest
    // resume window). Aggregates are bumped only on a genuine NEW part, so a deduped re-read does
    // not inflate files_committed / bytes_written past manifest_parts.len() (which would trip the
    // run-integrity invariant). total_rows is a per-page loop counter; the keyset runner reconciles
    // it to the manifest sum after the loop.
    let recorded = manifest_writer::record_committed_part_with_fingerprint(
        summary,
        part.file_name.clone(),
        part.rows,
        part.bytes,
        part.fingerprint.clone(),
        part.md5.clone(),
    );
    let deduped = recorded.deduped;
    if !deduped {
        summary.bytes_written += part.bytes;
        summary.files_produced += 1;
        summary.files_committed += 1;
    }
    // ADR-0029: register the part → unit pairing the seam computes coverage on.
    // Keyed by the manifest's own part_id, so a DEDUPED re-read (same part_id,
    // refreshed payload) re-registers the same entry — a re-read of a rehydrated
    // page therefore turns that part from foreign into covered, which is exactly
    // what happened: this run really did compute its checksums.
    summary.ledger.note_part(recorded.part_id, unit);

    match &kind {
        PartKind::File { part_index } => summary.journal.record(RunEvent::FileWritten {
            file_name: part.file_name.clone(),
            rows: part.rows,
            bytes: part.bytes,
            part_index: *part_index,
        }),
        PartKind::Chunk { chunk_index } => summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index: *chunk_index,
            rows: part.rows,
            file_name: Some(part.file_name.clone()),
        }),
        // Keyset pages reuse ChunkCompleted to preserve journal-on-disk
        // backward compatibility. See [`PartKind::Page`] for the rationale
        // and the upgrade path if/when downstream observability needs to
        // distinguish keyset pages from chunked windows.
        PartKind::Page { page_index, .. } => summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index: *page_index,
            rows: part.rows,
            file_name: Some(part.file_name.clone()),
        }),
    }

    // v25: the page's high-water key rides in the SAME file_log row as its part, so a resume
    // reconciles the cursor from committed parts (see keyset::run_keyset resume) and never
    // re-reads a committed page. `None` for every non-keyset part.
    let cursor_high: Option<&str> = match &kind {
        PartKind::Page { cursor_high, .. } => cursor_high.as_deref(),
        _ => None,
    };

    // ADR-0001 I7: file-log (manifest) write failure is non-fatal — the file is
    // already durable at the destination; log and continue.
    if let Some(st) = state
        && let Err(e) = st.record_durable_part(crate::state::DurablePart {
            run_id: &summary.run_id,
            export_name: &plan.export_name,
            file_name: &part.file_name,
            rows: part.rows,
            bytes: part.bytes as i64,
            format: plan.format.label(),
            compression: Some(plan.compression.label()),
            mode: plan.strategy.mode_label(),
            cursor_high,
        })
    {
        log::warn!(
            "export '{}': file_log write failed for '{}' (file was produced): {:#}",
            plan.export_name,
            part.file_name,
            e
        );
    }

    // ADR-0001 I3 crash window: manifest recorded, cursor not yet advanced.
    crate::test_hook::maybe_panic_at("after_manifest_update");
    deduped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, SourceConfig, SourceType,
    };
    use crate::destination::local::LocalDestination;
    use crate::journal::RunEvent;
    use crate::pipeline::summary::RunSummary;
    use crate::plan::{ExtractionStrategy, ResolvedRunPlan};
    use crate::state::StateStore;
    use crate::tuning::SourceTuning;
    use std::io::Write;

    /// ADR-0028 CommitLedger semantics, each of which a runner relies on:
    /// first-wins schema/key (idempotent across pages/workers), commutative
    /// wrapping-add checksum merge (worker order must not matter), max-merge
    /// shape. RED-proven against: note_schema last-wins (second schema
    /// overwrites), merge via overwrite-insert (second part clobbers), and
    /// merge_shape via min.
    /// ADR-0028: `drain_tail_into` is the FEEDING leg every sink-based runner
    /// relies on — stubbed to a no-op, no schema/checksums/shape ever reach the
    /// ledger and the seam applies nothing (live: the Form-B telltale fires).
    /// Unit-pinned here so the in-diff gate kills the stub without a stand:
    /// populate a real sink's tail fields, drain, assert the ledger got all
    /// four (schema, checksums, key, shape) and the sink was emptied.
    #[test]
    fn drain_tail_into_moves_schema_checksums_key_and_shape_to_the_ledger() {
        // ADR-0029 split the one drain in two; this pins BOTH halves together,
        // since the sink is the only feeder that owns schema, checksums, key and
        // shape at once and a half-wired runner is the bug it guards.
        use arrow::datatypes::{DataType, Field, Schema};
        let plan = test_plan();
        let mut sink = crate::pipeline::sink::ExportSink::new(&plan).expect("sink");
        sink.dest_schema = Some(std::sync::Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])));
        sink.column_checksums = [("id".to_string(), 41u64)].into();
        sink.checksum_key_col = Some(0);
        sink.cursor_column = Some("id".to_string());
        sink.column_max_bytes = [("id".to_string(), 8u64)].into();

        let mut led = CommitLedger::default();
        sink.drain_observations_into(&mut led);
        sink.drain_integrity_into(UnitId::Run, &mut led);

        assert_eq!(
            led.observed
                .drift_schema
                .as_ref()
                .map(|s| s.field(0).name().clone()),
            Some("id".to_string()),
            "the sink's dest schema must reach the ledger"
        );
        assert_eq!(led.integrity.column_checksums.get("id"), Some(&41u64));
        assert_eq!(led.integrity.checksum_key_column.as_deref(), Some("id"));
        assert_eq!(led.observed.column_max_bytes.get("id"), Some(&8u64));
        assert!(
            led.integrity.covered_units.contains(&UnitId::Run),
            "the integrity drain must REGISTER its commit unit, or the seam reads \
             every part single wrote as uncovered and suppresses Form B"
        );
        assert!(
            sink.column_checksums.is_empty() && sink.column_max_bytes.is_empty(),
            "drain must TAKE the sink's accumulators, not copy them"
        );
    }

    #[test]
    fn commit_ledger_first_wins_schema_and_key_and_commutative_merges() {
        use arrow::datatypes::{DataType, Field, Schema};
        let mut led = CommitLedger::default();

        // first-wins schema: the second (drifted) schema must NOT replace it.
        let a = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let b = Schema::new(vec![Field::new("other", DataType::Utf8, true)]);
        led.note_schema(&a);
        led.note_schema(&b);
        assert_eq!(
            led.observed
                .drift_schema
                .as_ref()
                .map(|s| s.field(0).name().clone()),
            Some("id".to_string()),
            "note_schema is first-wins — a later page/worker schema must not replace the run's"
        );

        // first-Some-wins key: a None feed leaves it open for a later Some.
        // commutative wrapping-add merge — two units with the same column SUM,
        // never overwrite (an overwrite would silently drop unit 1's coverage).
        let p1: std::collections::BTreeMap<String, u64> = [("v".to_string(), 7u64)].into();
        let p2: std::collections::BTreeMap<String, u64> = [("v".to_string(), 5u64)].into();
        led.contribute_checksums(UnitId::Chunk(0), &Default::default(), None);
        led.contribute_checksums(UnitId::Chunk(1), &p1, Some("id".into()));
        led.contribute_checksums(UnitId::Chunk(2), &p2, Some("late".into()));
        assert_eq!(led.integrity.checksum_key_column.as_deref(), Some("id"));
        assert_eq!(
            led.integrity.column_checksums.get("v"),
            Some(&12u64),
            "checksum merge must fold (wrapping add), not overwrite"
        );
        // ADR-0029: every contribution registers its unit — including the EMPTY
        // one. A zero-row chunk / a CSV sink computes no checksums and still
        // COMMITTED; reading that as "uncovered" would suppress Form B on the
        // next parquet run that happens to contain an empty chunk.
        assert_eq!(
            led.integrity.covered_units,
            [UnitId::Chunk(0), UnitId::Chunk(1), UnitId::Chunk(2)]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "an empty contribution still covers its unit"
        );

        // shape is max-merge: order-independent, the larger observation wins.
        let s1: std::collections::HashMap<String, u64> = [("t".to_string(), 100u64)].into();
        let s2: std::collections::HashMap<String, u64> = [("t".to_string(), 40u64)].into();
        led.merge_shape(&s1);
        led.merge_shape(&s2);
        assert_eq!(led.observed.column_max_bytes.get("t"), Some(&100u64));
    }

    fn test_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            split_window: None,
            bytes_read: Default::default(),
            export_name: "orders".into(),
            source_table: None,
            base_query: "SELECT 1".into(),
            is_split_unit: false,
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: Default::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("/tmp".into()),
                ..Default::default()
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody@127.0.0.1:9999/nonexistent".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                environment: None,
                tuning: None,
                tls: None,
                mongo: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    fn test_summary(plan: &ResolvedRunPlan) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("test_run", plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "snapshot".into();
        s.compression = "none".into();
        s
    }

    fn test_part(file_name: &str) -> PartRecord {
        PartRecord {
            file_name: file_name.into(),
            rows: 42,
            bytes: 1024,
            fingerprint: "xxh3:1234567890abcdef".into(),
            md5: String::new(),
        }
    }

    // ── part_indexed_name: rotation sibling naming ───────────────────────────

    #[test]
    fn part_indexed_name_keeps_legacy_name_for_single_part_and_suffixes_siblings() {
        // Single part — legacy name untouched (resume/manifest compat).
        assert_eq!(
            part_indexed_name("orders_ts_chunk3.parquet", 0, 1),
            "orders_ts_chunk3.parquet"
        );
        // Rotated chunk — every sibling suffixed before the extension.
        assert_eq!(
            part_indexed_name("orders_ts_chunk3.parquet", 0, 3),
            "orders_ts_chunk3_p0.parquet"
        );
        assert_eq!(
            part_indexed_name("orders_ts_chunk3.parquet", 2, 3),
            "orders_ts_chunk3_p2.parquet"
        );
        // No extension — suffix appended bare.
        assert_eq!(part_indexed_name("orders_chunk3", 1, 2), "orders_chunk3_p1");
    }

    // ── write_part_file ──────────────────────────────────────────────────────

    #[test]
    fn write_part_file_copies_to_destination_and_returns_real_bytes_and_fingerprint() {
        // Stage a fixture file with known content; write it through LocalDestination.
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().join("part.parquet");
        let payload: &[u8] = b"hello rivet";
        std::fs::File::create(&src_path)
            .unwrap()
            .write_all(payload)
            .unwrap();

        let dest = LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dst_dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();

        let rec =
            write_part_file(&dest, &src_path, 7, "out/part.parquet".into()).expect("write ok");

        assert_eq!(rec.file_name, "out/part.parquet");
        assert_eq!(rec.rows, 7);
        assert_eq!(rec.bytes, payload.len() as u64);
        assert!(
            rec.fingerprint.starts_with("xxh3:") && rec.fingerprint.len() == 21,
            "fingerprint should be xxh3:<16 hex chars>, got {:?}",
            rec.fingerprint
        );
        let written = dst_dir.path().join("out").join("part.parquet");
        assert_eq!(std::fs::read(&written).unwrap(), payload);
    }

    // ── write_part_file: store-reported checksum transit check ─────────────────

    /// A destination that reports a fixed content checksum (no real upload).
    struct ChecksumDest(Option<String>);
    impl crate::destination::Destination for ChecksumDest {
        fn write(&self, _p: &Path, _k: &str) -> Result<crate::destination::WriteOutcome> {
            Ok(crate::destination::WriteOutcome {
                content_md5: self.0.clone(),
            })
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            crate::destination::DestinationCapabilities {
                commit_protocol: crate::destination::WriteCommitProtocol::FinalizeOnClose,
                idempotent_overwrite: true,
                retry_safe: true,
                partial_write_risk: false,
            }
        }
    }

    fn stage(payload: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        std::fs::write(&p, payload).unwrap();
        (dir, p)
    }

    #[test]
    fn transit_check_bails_when_store_checksum_differs() {
        let (_d, src) = stage(b"hello rivet");
        // A bogus store checksum (valid base64, wrong digest) must fail the write.
        let dest = ChecksumDest(Some("AAAAAAAAAAAAAAAAAAAAAA==".into()));
        match write_part_file(&dest, &src, 1, "part.parquet".into()) {
            Ok(_) => panic!("mismatched checksum must fail"),
            Err(e) => assert!(
                e.to_string().contains("transit"),
                "expected a transit-corruption error, got: {e}"
            ),
        }
    }

    #[test]
    fn transit_check_passes_on_match_and_when_store_is_silent() {
        use base64::Engine as _;
        use md5::{Digest, Md5};
        let payload = b"hello rivet";
        let (_d, src) = stage(payload);
        // Matching checksum (real MD5 of the bytes) → OK.
        let mut h = Md5::new();
        h.update(payload);
        let real = base64::engine::general_purpose::STANDARD.encode(h.finalize());
        write_part_file(&ChecksumDest(Some(real)), &src, 1, "p.parquet".into())
            .expect("matching checksum passes");
        // No store checksum (local FS / streamed) → no check, OK.
        write_part_file(&ChecksumDest(None), &src, 1, "p.parquet".into())
            .expect("silent store passes");
    }

    // ── record_part: counters + journal + manifest ────────────────────────────

    #[test]
    fn record_part_file_kind_bumps_counters_and_journals_file_written() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::File { part_index: 0 },
            UnitId::Run,
        );

        assert_eq!(summary.bytes_written, part.bytes);
        assert_eq!(summary.files_produced, 1);
        assert_eq!(summary.files_committed, 1);
        assert_eq!(summary.manifest_parts.len(), 1);
        assert_eq!(summary.manifest_parts[0].path, part.file_name);
        assert_eq!(summary.manifest_parts[0].rows, part.rows);

        let file_events = summary.journal.files();
        assert_eq!(file_events.len(), 1, "must journal one FileWritten");
        assert!(
            matches!(
                &file_events[0].event,
                RunEvent::FileWritten { part_index: 0, .. }
            ),
            "expected FileWritten{{part_index:0}}, got {:?}",
            file_events[0].event
        );
        assert!(
            summary.journal.chunk_events().is_empty(),
            "File kind must not journal ChunkCompleted"
        );
    }

    #[test]
    fn record_part_chunk_kind_journals_chunk_completed_with_file_name() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk7.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 7 },
            UnitId::Chunk(7),
        );

        let events = summary.journal.chunk_events();
        assert_eq!(events.len(), 1, "must journal one ChunkCompleted");
        match &events[0].event {
            RunEvent::ChunkCompleted {
                chunk_index,
                rows,
                file_name,
            } => {
                assert_eq!(*chunk_index, 7);
                assert_eq!(*rows, part.rows);
                assert_eq!(file_name.as_deref(), Some(part.file_name.as_str()));
            }
            other => panic!("expected ChunkCompleted, got {other:?}"),
        }
        assert!(
            summary.journal.files().is_empty(),
            "Chunk kind must not journal FileWritten"
        );
    }

    // ── I7: state.record_file is optional and warn-on-fail ───────────────────

    #[test]
    fn record_part_with_state_writes_file_log_entry() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let state = StateStore::open_in_memory().expect("in-memory state");
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            Some(&state),
            &part,
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );

        let files = state.get_files(Some(&plan.export_name), 16).unwrap();
        assert_eq!(files.len(), 1, "I7: file_log must carry exactly one entry");
        assert_eq!(files[0].file_name, part.file_name);
        assert_eq!(files[0].row_count, part.rows);
    }

    #[test]
    fn record_part_with_none_state_is_a_bypass_not_a_failure() {
        // ADR-0001 I7 says state.record_file is non-fatal. The None case is the
        // strictest form: no state to write to → the manifest/journal/counters
        // half must still complete cleanly.
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );

        assert_eq!(summary.files_committed, 1);
        assert_eq!(summary.manifest_parts.len(), 1);
        assert_eq!(summary.journal.chunk_events().len(), 1);
    }

    // ── summary ↔ manifest coherence (CI gate, gaps #2 + #3 in invariant audit)
    //
    // record_part is the single home that mutates summary.bytes_written,
    // summary.files_produced, summary.files_committed, and
    // summary.manifest_parts. The seam exists so the four cannot drift. This
    // test pins the contract: after N record_part calls on a freshly stubbed
    // summary (no resume hydration), the four aggregates must agree with
    // manifest_parts byte-for-byte. If a future runner bypasses record_part
    // and bumps a counter inline, this test still passes — but the
    // finalize_manifest runtime debug_assert (see assert_summary_post_run)
    // will fire the moment that runner finishes a real export. Two layers,
    // both CI-enforced via `cargo test`.

    fn synthetic_parts(n: usize) -> Vec<PartRecord> {
        (0..n)
            .map(|i| PartRecord {
                file_name: format!("part_{i}.parquet"),
                rows: 100 + (i as i64) * 10,
                bytes: 1024 * ((i as u64) + 1),
                fingerprint: format!("xxh3:{i:016x}"),
                md5: String::new(),
            })
            .collect()
    }

    #[test]
    fn record_part_keeps_summary_aggregates_coherent_with_manifest_parts() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let parts = synthetic_parts(5);

        // Simulate a runner: bump total_rows then record_part for each chunk.
        // record_part does NOT touch total_rows; the runner owns that bump,
        // so we model both halves of the contract here.
        for (i, p) in parts.iter().enumerate() {
            summary.total_rows += p.rows;
            record_part(
                &plan,
                &mut summary,
                None,
                p,
                PartKind::Chunk {
                    chunk_index: i as i64,
                },
                UnitId::Chunk(i as i64),
            );
        }

        let parts_rows: i64 = summary.manifest_parts.iter().map(|p| p.rows).sum();
        let parts_bytes: u64 = summary.manifest_parts.iter().map(|p| p.size_bytes).sum();

        assert_eq!(
            summary.total_rows, parts_rows,
            "non-resume run: summary.total_rows must equal sum(manifest_parts.rows)"
        );
        assert_eq!(
            summary.bytes_written, parts_bytes,
            "non-resume run: summary.bytes_written must equal sum(manifest_parts.size_bytes)"
        );
        assert_eq!(
            summary.files_produced,
            summary.manifest_parts.len(),
            "files_produced must equal manifest_parts.len() (record_part bumps both)"
        );
        assert_eq!(
            summary.files_committed,
            summary.manifest_parts.len(),
            "files_committed must equal manifest_parts.len() (record_part bumps both)"
        );
    }

    #[test]
    fn nonempty_successful_run_must_have_nonempty_manifest_parts() {
        // Contrapositive of M1: a successful run that committed at least one
        // file must surface that file in the cloud manifest. Before
        // commit e9b0796 parallel_checkpoint violated this — its
        // manifest_parts stayed empty for every run while files_committed
        // and bytes_written reported real work. This test pins the
        // contract: if files_committed > 0 then manifest_parts is non-empty.
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        summary.total_rows += part.rows;
        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );
        summary.status = "success".into();

        assert!(summary.files_committed > 0, "test premise: work committed");
        assert!(
            !summary.manifest_parts.is_empty(),
            "non-empty success run must surface files in manifest_parts"
        );
        assert_eq!(
            summary.files_committed,
            summary.manifest_parts.len(),
            "files_committed and manifest_parts.len() locked together by record_part"
        );
    }

    // ── ADR-0029: computed Form-B coverage ───────────────────────────────────
    //
    // These drive the REAL seam (`record_part` writes the part → unit map,
    // `contribute_checksums` writes the covered set, `harvest_column_checksums`
    // compares them over `summary.manifest_parts`) rather than hand-setting the
    // verdict. Both directions are here on purpose: an absence-only suite would
    // pass against a harvest that suppresses ALWAYS, which is precisely the
    // fail-safe-but-silent failure ADR-0029 names as its own risk.

    /// Feed the ledger the way a runner does: N parts under one commit unit,
    /// then that unit's checksums. Returns the summary the seam would see.
    fn run_one_unit(plan: &ResolvedRunPlan, unit: UnitId, parts: &[PartRecord]) -> RunSummary {
        let mut summary = test_summary(plan);
        for p in parts {
            record_part(
                plan,
                &mut summary,
                None,
                p,
                PartKind::Chunk { chunk_index: 0 },
                unit,
            );
        }
        summary.ledger.contribute_checksums(
            unit,
            &[("id".to_string(), 0xdead_beefu64)].into(),
            Some("id".into()),
        );
        summary
    }

    /// THE positive. A run whose every manifest part belongs to a unit that
    /// contributed must RECORD Form B — checksums and key column both.
    ///
    /// Mandatory rather than optional (ADR-0029 "Risk"): the whole mechanism
    /// fails SAFE, so a unit-id mismatch, an off-by-one in the part_id key, or a
    /// coverage rule that simply always returns "short" all present as an absent
    /// Form B on healthy data — which no suppression test can see. RED against
    /// `Coverage.complete = false` (unconditional suppression) and against a
    /// `record_part` that skips `note_part` (every part then reads foreign).
    #[test]
    fn harvest_records_form_b_when_every_manifest_part_s_unit_contributed() {
        let plan = test_plan();
        // 3 parts under the unit: with ONE part a coverage rule that only looks
        // at the first manifest entry is indistinguishable from one that checks
        // them all.
        let parts = synthetic_parts(3);
        let mut summary = run_one_unit(&plan, UnitId::Chunk(0), &parts);
        assert_eq!(summary.manifest_parts.len(), 3, "fixture: 3 parts recorded");

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);

        assert_eq!(
            summary
                .column_checksums
                .iter()
                .map(|c| (c.name.as_str(), c.checksum.as_str()))
                .collect::<Vec<_>>(),
            vec![("id", "3735928559")],
            "a fully-covered run must RECORD Form B, not merely fail to suppress it"
        );
        assert_eq!(summary.checksum_key_column.as_deref(), Some("id"));
        assert!(
            !summary.column_checksums_incomplete && !summary.column_checksums_short_cover,
            "a fully-covered run must be flagged neither incomplete nor short-cover"
        );
    }

    /// A part RECORDED by this run whose unit contributed nothing — the failed
    /// keyset range / mid-write bail shape, and the shape a runner's unit-id
    /// mismatch produces. Form B is suppressed AND `short_cover` is set, which is
    /// what `check_post_run_invariants` reads to tell a mismatch from a
    /// legitimate resume. RED against a coverage rule that ignores
    /// `covered_units`: it then records a Form B covering a strict SUBSET of the
    /// manifest — the partial-and-lying record `validate --depth full` would
    /// false-flag on correctly-written data.
    #[test]
    fn harvest_suppresses_form_b_when_a_recorded_part_s_unit_contributed_nothing() {
        let plan = test_plan();
        let parts = synthetic_parts(2);
        let mut summary = run_one_unit(&plan, UnitId::Chunk(0), &parts);
        // A SECOND unit commits a part and never publishes its checksums.
        let orphan = PartRecord {
            file_name: "uncommitted_range.parquet".into(),
            rows: 10,
            bytes: 64,
            fingerprint: "xxh3:000000000000dead".into(),
            md5: String::new(),
        };
        record_part(
            &plan,
            &mut summary,
            None,
            &orphan,
            PartKind::Chunk { chunk_index: 1 },
            UnitId::Chunk(1),
        );

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);

        assert!(
            summary.column_checksums.is_empty(),
            "one uncovered part must suppress the WHOLE Form-B record, never publish a \
             checksum covering a subset of the manifest"
        );
        assert!(summary.column_checksums_incomplete);
        assert!(
            summary.column_checksums_short_cover,
            "a part this run RECORDED but did not cover is the never-legitimate half — it \
             must be distinguishable from a resume hydration, or the invariant cannot fire"
        );
    }

    /// The resume case, which ADR-0029 SUBSUMES: a part hydrated into the
    /// manifest without going through `record_part` (file_log rehydration / an M8
    /// `Skip` clone of a prior manifest's part) has no commit unit at all, so the
    /// seam suppresses without anyone setting a flag — the two
    /// `column_checksums_incomplete = true` assignments this ADR retired from
    /// `resume_m8.rs`. `short_cover` stays FALSE (hydration is expected, not a
    /// runner defect), which is what keeps the new invariant resume-safe with no
    /// exemption to remember.
    ///
    /// RED against a coverage rule keyed on `record_part` CALLS instead of on the
    /// MANIFEST: the hydrated part is invisible to such a rule, Form B is
    /// published over a manifest it covers only in part, and retiring the two
    /// manual sites becomes silent corruption of the integrity record.
    #[test]
    fn harvest_suppresses_form_b_for_a_hydrated_part_with_no_commit_unit() {
        let plan = test_plan();
        let parts = synthetic_parts(2);
        let mut summary = run_one_unit(&plan, UnitId::Chunk(0), &parts);
        // What `rehydrate_manifest_parts_from_file_log` / the M8 Skip clone do:
        // push straight into the manifest — no record_part, no checksums.
        summary.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: 99,
            path: "precrash_chunk7.parquet".into(),
            rows: 100,
            size_bytes: 4096,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);

        assert!(
            summary.column_checksums.is_empty(),
            "a hydrated pre-crash part carries no per-column checksum, so the run-wide record \
             cannot cover the manifest — suppress, never publish a partial"
        );
        assert!(summary.column_checksums_incomplete);
        assert!(
            !summary.column_checksums_short_cover,
            "hydration is LEGITIMATE — flagging it short-cover would fail every crash-recovery \
             resume through check_post_run_invariants"
        );
    }

    /// The same hydration case with an EMPTY accumulator — a resume that re-ran
    /// NOTHING, so no unit contributed and there is not a single checksum to
    /// weigh. The suppression verdict must STILL be recorded.
    ///
    /// This is the one the unit suite missed and the live suite caught: with the
    /// empty-accumulator early return placed ABOVE the coverage computation, a
    /// parallel-keyset crash resume whose ranges were all already `done` reached
    /// `finalize_manifest` with rehydrated parts, an empty Form B and NO flag —
    /// and the pre-existing Form-B telltale panicked the run
    /// (`parallel_keyset_crash_recovery_postgres`, plus 3 siblings). Pre-ADR-0029
    /// the flag came from the manual `rehydrate_manifest_parts_from_file_log`
    /// assignment this ADR retires, which is exactly why retiring it has to be
    /// proven on the empty-accumulator path and not only the populated one.
    ///
    /// RED against restoring that order.
    #[test]
    fn harvest_flags_a_hydration_only_resume_that_re_ran_nothing_and_has_no_checksums() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        summary.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: 1,
            path: "precrash_chunk0.parquet".into(),
            rows: 100,
            size_bytes: 4096,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });
        summary.files_committed = 1;
        summary.files_produced = 1;

        // Nothing re-ran: the ledger is untouched, so the accumulator is empty.
        let integrity = std::mem::take(&mut summary.ledger).integrity;
        assert!(
            integrity.column_checksums.is_empty(),
            "fixture: no checksums"
        );
        harvest_column_checksums(&mut summary, integrity);

        assert!(
            summary.column_checksums_incomplete,
            "a resume that hydrated parts and re-ran nothing must still RECORD the \
             suppression, or the Form-B telltale reads an unexplained empty record and \
             panics a legitimate crash-recovery run"
        );
        assert!(!summary.column_checksums_short_cover);
        summary.state_backed = true;
        summary.status = "success".into();
        summary.format = "parquet".into();
        summary.schema_changed = Some(false);
        assert!(
            summary.check_post_run_invariants(true).is_ok(),
            "and the telltales must then PASS the resume: {:?}",
            summary.check_post_run_invariants(true)
        );
    }

    /// A hydrated part that this run RE-READ and overwrote (the keyset
    /// mid-page-crash fallback) dedupes in place, keeping its part_id — and
    /// `record_part` re-registers that id against the re-reading unit, so the
    /// part stops being foreign and coverage can be complete again.
    ///
    /// This is why the part → unit map is keyed on `part_id` and not on a COUNT
    /// of `record_part` calls (which dedup makes wrong) — a count would leave the
    /// run permanently suppressed even after it re-computed every checksum. RED
    /// against keying `part_units` on anything the dedup path does not preserve.
    #[test]
    fn a_deduped_re_read_of_a_hydrated_part_becomes_covered() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        // Pre-crash part hydrated into the manifest with part_id 1.
        summary.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: 1,
            path: "part_0.parquet".into(),
            rows: 100,
            size_bytes: 1024,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });
        // This run re-reads that same page: same path → dedup in place.
        let parts = synthetic_parts(1);
        let deduped = record_part(
            &plan,
            &mut summary,
            None,
            &parts[0],
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );
        assert!(deduped, "fixture: the re-read must dedup, not append");
        assert_eq!(summary.manifest_parts.len(), 1);
        summary.ledger.contribute_checksums(
            UnitId::Chunk(0),
            &[("id".to_string(), 7u64)].into(),
            Some("id".into()),
        );

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);

        assert!(
            !summary.column_checksums.is_empty(),
            "a re-read part IS covered by this run's checksums — suppressing here would make \
             every mid-page-crash resume lose Form B for no reason"
        );
    }

    /// `compute_coverage` counts BOTH shortfall kinds independently, over a mixed
    /// manifest — the arithmetic the two flags are derived from. The fixture puts
    /// ≥2 of each kind in, so a rule that stops at the first offender is
    /// distinguishable from one that tallies; and it ends with the COMPLETE case,
    /// so "always short" is excluded.
    #[test]
    fn compute_coverage_counts_foreign_and_uncovered_parts_separately() {
        let part = |id: u32| crate::manifest::ManifestPart {
            part_id: id,
            path: format!("p{id}.parquet"),
            rows: 1,
            size_bytes: 1,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        };
        let mut integrity = Integrity::default();
        // ids 1,2 → a covered unit; 3,4 → an uncovered unit; 5,6 → no unit at all.
        for id in [1u32, 2] {
            integrity.part_units.insert(id, UnitId::Chunk(0));
        }
        for id in [3u32, 4] {
            integrity.part_units.insert(id, UnitId::Chunk(1));
        }
        integrity.covered_units.insert(UnitId::Chunk(0));
        let parts: Vec<_> = (1u32..=6).map(part).collect();

        let cov = compute_coverage(&parts, &integrity);
        assert_eq!(
            (
                cov.complete,
                cov.short_cover,
                cov.uncovered_parts,
                cov.foreign_parts
            ),
            (false, true, 2, 2)
        );

        integrity.covered_units.insert(UnitId::Chunk(1));
        let cov = compute_coverage(&parts[..4], &integrity);
        assert_eq!(
            (
                cov.complete,
                cov.short_cover,
                cov.uncovered_parts,
                cov.foreign_parts
            ),
            (true, false, 0, 0)
        );
    }

    /// The unit-id MISMATCH, end to end through the two real seams — a runner
    /// that records under one `UnitId` and contributes under another.
    ///
    /// Scope honesty: this proves the mechanism REACTS to a mismatch, not that
    /// any particular runner is paired correctly. That half needs a real source
    /// and destination, so its oracle is the live suite's per-runner Form-B cells
    /// (`docs/runner-coverage-matrix.yaml` row `value_checksum_form_b`), each of
    /// which asserts the destination manifest RECORDS `column_checksums` — a
    /// mis-paired runner suppresses and its cell goes RED — plus this test's
    /// second half: `check_post_run_invariants` FAILS the run, and it runs under
    /// `debug_assertions`, the build the live suite uses. So a mismatch is loud on
    /// every runner rather than silent on one.
    #[test]
    fn a_unit_id_mismatch_suppresses_form_b_and_fails_the_run_instead_of_going_silent() {
        let plan = test_plan();
        let parts = synthetic_parts(1);
        let mut summary = test_summary(&plan);
        record_part(
            &plan,
            &mut summary,
            None,
            &parts[0],
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );
        // The mis-pairing itself: recorded under Chunk(0), contributed as Chunk(1).
        summary.ledger.contribute_checksums(
            UnitId::Chunk(1),
            &[("id".to_string(), 1u64)].into(),
            None,
        );
        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);
        assert!(
            summary.column_checksums.is_empty() && summary.column_checksums_short_cover,
            "a UnitId mismatch must suppress AND reach the flag check_post_run_invariants reads"
        );

        summary.state_backed = true;
        summary.status = "success".into();
        summary.format = "parquet".into();
        // The drift telltale is a SEPARATE invariant and fires first; satisfy it
        // so this test grades the coverage telltale and not that one.
        summary.schema_changed = Some(false);
        let verdict = summary.check_post_run_invariants(false);
        assert!(
            verdict
                .as_ref()
                .err()
                .is_some_and(|m| m.contains("UnitId mismatch")),
            "the backstop must FAIL a successful state_backed run carrying short coverage, \
             naming the mismatch; got {verdict:?}"
        );
    }
}
