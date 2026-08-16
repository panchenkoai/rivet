//! **Layer: Observability**
//!
//! Manifest-aware verification for `--validate` (ADR-0012 §M5 / §M6,
//! constrained by the ADR-0013 trust-flag contract that says: no new flags;
//! manifest-aware checks live under the existing `--validate`).
//!
//! Although this module reads from a `Destination` (technically L2 surface),
//! it makes **no execution decisions**: it does not write data, advance
//! cursors, mutate state, or change the pipeline path.  Its only output is
//! a structured `ManifestVerification` verdict the run report renders.
//! Per ADR-0003, that places it firmly in L4 Observability — the
//! destination read surface is just the carrier.
//!
//! Inputs (read-only):
//! - the destination's `manifest.json` body
//! - the destination's `_SUCCESS` body, if present
//! - the listing of every object under the destination prefix
//!
//! Outputs:
//! - [`ManifestVerification`] — a structured verdict the run report renders
//!   into the operator-facing "Verdicts" section.
//!
//! Out of scope here:
//! - per-file row-count check (that runs *during* the export, against the
//!   local temp file before upload — see `pipeline::validate::validate_output`).
//! - source-side reconciliation (lives in [`pipeline::reconcile_cmd`] and
//!   is what `--reconcile` adds on top of this).
//! - re-fingerprinting parts (`--validate --deep`, future).
//!
//! Failure modes are explicit: each check produces a `Failure` enum variant
//! that is rendered verbatim in `summary.json` so an Airflow / CI consumer
//! can branch on the kind, not parse strings.

use serde::{Deserialize, Serialize};

use crate::destination::{Destination, ObjectMeta};
use crate::error::Result;
use crate::manifest::{
    MANIFEST_FILENAME, PartStatus, RunManifest, SUCCESS_FILENAME, is_run_unique_manifest_name,
    join_key, parse_success_marker, success_marker_body,
};
use crate::pipeline::manifest_reconcile::{PartPresence, reconcile_manifest_against_listing};

/// Upper bound on a destination control artifact (`manifest.json`) the read
/// path will materialise into memory.  A `manifest.json` is metadata — a few
/// KB to low single-digit MB even for very large datasets — so 64 MiB is far
/// above any legitimate body while still bounding the blast radius.
///
/// Security (V21, CWE-400): the manifest readers `head()` an object then read
/// its full body into a `Vec<u8>`.  An attacker who can write the destination
/// prefix (a shared bucket prefix, a world-writable export dir) can plant a
/// multi-GB `manifest.json`; an unbounded read would OOM the next `--resume`,
/// `--validate`, or `rivet repair`.  [`read_capped`] consults the size the
/// `head()` already reports and bails before the read when it exceeds this cap.
pub(crate) const MANIFEST_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Same V21/CWE-400 defense for the sibling control artifact `_SUCCESS`, read by
/// the SAME `verify_at_destination` under the SAME destination-writable threat
/// model but previously UNCAPPED. A legitimate marker is ~22 bytes
/// (`xxh3:<16-hex>\n`); a body past this small cap is corrupt or planted, so it
/// is treated as malformed WITHOUT materialising it into memory. 4 KiB is orders
/// of magnitude above any real marker yet trivially bounded.
pub(crate) const SUCCESS_MARKER_MAX_BYTES: u64 = 4096;

/// How deep a `rivet validate` pass goes — a graded verify layer over the
/// same checks, letting an operator trade thoroughness for latency / cost.
///
/// The variants are a strict superset chain: `Light ⊂ Sample ⊂ Full`.  Each
/// level runs every check the level below it does, plus more.  Defined here
/// (the pipeline layer) and re-exported for the CLI grammar so the **same**
/// enum gates the checks in [`verify_at_destination`] and parses on the
/// `--depth` flag — no CLI→pipeline back-dependency.
///
/// - **Light**: manifest read + self-consistency + `_SUCCESS` only.  Skips the
///   `list_prefix` reconcile (no per-part presence/size/checksum) and the
///   untracked-surplus scan, leaving `parts_verified = 0`.  One `head` + one
///   `read` of `manifest.json` and `_SUCCESS` — a fast "is this prefix a
///   complete, marked run?" poll with no prefix listing.
/// - **Sample**: everything Light does **plus** the part reconcile and
///   untracked surplus (one `list_prefix`).  This is the pre-graded behaviour
///   minus the Form B value re-read — full structural verification with no
///   part downloads.
/// - **Full** (default): everything Sample does **plus** the Form B value-
///   checksum re-read (re-reads parts, re-derives per-column checksums).  The
///   most thorough and the only level that downloads part bodies.  Equivalent
///   to the pre-graded behaviour, so existing callers are unchanged.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidateDepth {
    /// Manifest read + self-consistency + `_SUCCESS` only (no prefix listing).
    Light,
    /// Light + part reconcile + untracked surplus (one `list_prefix`).
    Sample,
    /// Sample + the Form B value-checksum re-read (downloads parts).
    #[default]
    Full,
}

impl ValidateDepth {
    /// True iff this level runs the `list_prefix` reconcile (part presence,
    /// size, checksum) and the untracked-surplus scan — i.e. anything above
    /// `Light`.  The single predicate the section-3/5 depth gating keys off.
    fn runs_part_reconcile(self) -> bool {
        !matches!(self, ValidateDepth::Light)
    }

    /// True iff this level downloads part *bodies* — the CDC `__pos` continuity
    /// check and the Form-B value-checksum re-read, both `Full`-only (the
    /// `part_reconcile` level above only reads listing metadata). The single
    /// predicate the section-3/5 part-download gating keys off, parallel to
    /// [`Self::runs_part_reconcile`] — so adding a depth level edits the enum,
    /// not the call sites in `validate_cmd`.
    pub(crate) fn runs_part_download(self) -> bool {
        matches!(self, ValidateDepth::Full)
    }

    /// Stable operator-/wire-facing label for the depth a verdict was produced
    /// at, surfaced in `summary.json` (`depth_level`) and `rivet validate`
    /// output.
    pub fn label(self) -> &'static str {
        match self {
            ValidateDepth::Light => "light",
            ValidateDepth::Sample => "sample",
            ValidateDepth::Full => "full",
        }
    }
}

/// Read `key` into memory only if its `head()`-reported size is within
/// `max_bytes`; otherwise bail without reading a single byte.
///
/// The single enforcement point for the V21 (CWE-400) manifest-read cap shared
/// by the three control-artifact readers (`--resume` M8 preamble, `--validate`,
/// `rivet repair`).  Each previously did `head()` then an uncapped `read()`,
/// discarding the size `head()` already returned; routing through here closes
/// that gap in one place.
///
/// Behaviour:
/// - object absent (`head` → `None`): `Err` — callers invoke this only after
///   establishing the object exists, so an absent object here is a hard error,
///   not the benign "no manifest / legacy prefix" case (which the callers
///   detect with their own `head()` first).
/// - oversized (`size_bytes > max_bytes`): `Err` naming the cap, **before** any
///   body is materialised.
/// - otherwise: the full body via [`Destination::read`].
pub(crate) fn read_capped(dest: &dyn Destination, key: &str, max_bytes: u64) -> Result<Vec<u8>> {
    match dest.head(key)? {
        None => anyhow::bail!("'{key}' not found at the destination"),
        Some(meta) => {
            if meta.size_bytes > max_bytes {
                anyhow::bail!(
                    "'{key}' is {} bytes, exceeding the {max_bytes}-byte control-artifact \
                     read cap — refusing to load it into memory (possible tampering)",
                    meta.size_bytes
                );
            }
            dest.read(key)
        }
    }
}

/// Outcome of a single `--validate` pass over a destination prefix.
///
/// Stable enough to be embedded in `summary.json` directly (see
/// `pipeline::report::ValidationOutcome`).  Forward-compat: consumers MUST
/// ignore unknown fields (no `deny_unknown_fields`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestVerification {
    /// True iff a `manifest.json` was found at the destination and parsed.
    /// `false` triggers ADR-0012 M6 fallback (legacy run); higher-level
    /// check results below are then "skipped" rather than "passed".
    pub manifest_found: bool,
    /// Mirrors ADR-0012 M6's required `legacy_run` operator-facing label.
    pub legacy_run: bool,
    /// Manifest parts whose presence and recorded `size_bytes` were
    /// confirmed at the destination.  0 when no manifest was found.
    pub parts_verified: usize,
    /// Subset of `parts_verified` whose **content** was confirmed via an MD5
    /// the store surfaced in its listing (no download) — the rest are size-only.
    /// Lets `passed: true` say how much of the dataset was content-checked
    /// rather than implying all of it was.  `#[serde(default)]` for back-compat.
    #[serde(default)]
    pub parts_md5_verified: usize,
    /// Manifest parts that were declared `committed` but not actually
    /// present, present at a different size, or otherwise mismatched.
    pub parts_failed: usize,
    /// True iff `_SUCCESS` exists at the destination AND its body matches
    /// the fingerprint of the bytes we read for `manifest.json`.  An
    /// existing `_SUCCESS` whose body diverges from the manifest is itself
    /// an integrity failure — surfaced via `failures`.
    pub success_marker_consistent: bool,
    /// Self-consistency of the manifest (`row_count`, `part_count`,
    /// duplicate `part_id`s).  Skipped when `manifest_found = false`.
    pub manifest_self_consistent: bool,
    /// Final verdict, **derived** (not hand-maintained) — `manifest_found` and
    /// no *fatal* failure ([`Failure::is_fatal`]).  Stored so it stays in the
    /// `summary.json` contract, but computed in one place
    /// ([`ManifestVerification::recompute_passed`]) so a new failure variant is
    /// fatal by default rather than relying on every site to flip a bool.
    pub passed: bool,
    /// Per-failure detail.  May be non-empty with `passed = true` for advisory
    /// (non-fatal) failures like [`Failure::UntrackedObject`].  Stable variant
    /// set; new variants land under a new manifest version per ADR-0012.
    pub failures: Vec<Failure>,
    /// The graded depth this verdict was produced at: `"light"`, `"sample"`,
    /// or `"full"` (see [`ValidateDepth`]).  Lets a consumer of `summary.json`
    /// tell **how much** was actually checked — a `passed: true` at `"light"`
    /// asserts far less than at `"full"` (no part presence was verified).
    /// `#[serde(default)]` (→ `"full"`) for back-compat: pre-graded verdicts
    /// always ran the full pass.
    #[serde(default = "default_depth_level")]
    pub depth_level: String,
}

/// `serde(default)` for [`ManifestVerification::depth_level`]: a verdict that
/// predates the graded layer always ran the full pass, so an absent field
/// deserializes to `"full"`.
fn default_depth_level() -> String {
    ValidateDepth::Full.label().to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Failure {
    /// Manifest declared a part that does not exist at the destination.
    PartMissing { part_id: u32, path: String },
    /// Manifest declared a part whose actual size differs from `size_bytes`.
    PartSizeMismatch {
        part_id: u32,
        path: String,
        expected: u64,
        actual: u64,
    },
    /// Part present at the recorded size but its content MD5 (from the store's
    /// listing metadata) differs from the manifest's — transit / at-rest
    /// corruption, caught with no download.
    PartChecksumMismatch {
        part_id: u32,
        path: String,
        expected: String,
        actual: String,
    },
    /// `--depth full` re-read the parts and a per-column VALUE checksum (Form B)
    /// disagreed with the manifest — post-write corruption an MD5/size check
    /// cannot see (a flipped bit inside a data page). Verified-wrong, so it fails
    /// the verdict and classifies as data-integrity (exit 3), not could-not-verify.
    ValueChecksumMismatch { detail: String },
    /// `--depth full` found a part whose DECLARED row count disagrees with the
    /// rows the file holds. Distinct from a value mismatch on purpose: the bytes
    /// may be perfectly intact and it is the manifest that is wrong, so an
    /// operator told "value checksum" would go looking for corruption in the
    /// data. Verified-wrong either way — every consumer that SUMS the declared
    /// counts inherits the error.
    PartRowCountMismatch { detail: String },
    /// `--depth full` re-read a CDC export's parts and the `__pos` continuity
    /// check found a gap/duplicate in the change positions — the exported change
    /// stream is incomplete or reordered. Verified-wrong (same class as a value
    /// mismatch), so it fails the verdict and classifies as data-integrity (exit
    /// 3), not could-not-verify (bughunt MED: it was folded into hard_failures →
    /// exit 1, inconsistent with the value-checksum path).
    CdcPositionViolation { detail: String },
    /// `_SUCCESS` exists but its body is malformed (not `xxh3:<16-hex>` after
    /// trim).  ADR-0012 M2 — orchestrators rely on this format being strict.
    SuccessMarkerMalformed { body_preview: String },
    /// `_SUCCESS` body parsed but does not match `xxh3(manifest.json bytes)`.
    /// Two legitimate sources: (a) someone overwrote `_SUCCESS` after the
    /// manifest was rewritten — orchestrator bug; (b) the manifest was
    /// edited in place after the run — operator bug.  Either way the
    /// manifest is no longer trustworthy.
    SuccessMarkerStale {
        marker_fingerprint: String,
        manifest_fingerprint: String,
    },
    /// `RunManifest::validate_self_consistency` rejected the manifest.
    /// Usually a writer bug (declared row_count != sum of committed parts'
    /// rows); blocks the rest of the verification because the manifest
    /// itself is unreliable.
    ManifestSelfInconsistent { detail: String },
    /// Reading `manifest.json` returned an I/O error other than "absent".
    ManifestReadError { detail: String },
    /// Reading `_SUCCESS` returned an I/O error other than "absent".
    SuccessMarkerReadError { detail: String },
    /// Listing the destination prefix returned an I/O error.  Reduces the
    /// untracked-parts check (M5 surplus) to a no-op for this run.
    ListPrefixError { detail: String },
    /// A file is present at the destination prefix but no manifest entry
    /// references it.  M9-adjacent: `--validate` only flags it; quarantine
    /// belongs to `--resume`.
    UntrackedObject { key: String, size_bytes: u64 },
    /// The export declared `verify: content` but some parts could only be
    /// size-verified (no comparable content checksum from the store) — the
    /// declared integrity contract was not met.
    ContentVerificationUnmet { size_only: usize, total: usize },
    /// A manifest was *required* at this prefix (the operator pinned a literal
    /// `--prefix`, asserting a real dataset lives here) but none was found.
    /// Without this, an absent manifest at an operator-pinned prefix maps to
    /// the M6 legacy-run label and exits 0 — indistinguishable from a verified
    /// run, so a CI gate `rivet validate && deploy` sails past a destination
    /// that was never written.  Fatal: a required-but-missing manifest is a
    /// refusal reason, not a "cannot certify" advisory.
    ManifestRequiredButAbsent { prefix: String },
}

impl Failure {
    /// Whether this failure invalidates the dataset (flips `passed` to false).
    ///
    /// Every variant is fatal **except** [`Failure::UntrackedObject`]: surplus
    /// objects are an audit signal whose cleanup is `--resume`'s job (ADR-0012
    /// M9), not a corruption of the manifest-listed parts.  New variants are
    /// fatal by default — opt out here explicitly, so a forgotten case fails
    /// closed (safe) rather than silently passing.
    pub fn is_fatal(&self) -> bool {
        !matches!(self, Failure::UntrackedObject { .. })
    }

    /// Whether this failure is COULD-NOT-VERIFY (an operational I/O error that
    /// prevented the check) rather than VERIFIED-WRONG (the check ran and found
    /// the data bad). Could-not-verify is the operational class — exit 1, retry —
    /// NOT the data-integrity stop-the-line class (exit 3). A read/list error
    /// against a healthy prefix (a chmod-000 manifest, a transient S3 list blip)
    /// must not page a corruption incident (#7 bughunt: these fell into the
    /// verdict and drove exit 3). Fatal-by-omission the other way: a NEW variant
    /// is verified-wrong (exit 3) unless it is explicitly an I/O could-not-verify.
    pub fn is_could_not_verify(&self) -> bool {
        matches!(
            self,
            Failure::ManifestReadError { .. }
                | Failure::SuccessMarkerReadError { .. }
                | Failure::ListPrefixError { .. }
        )
    }

    /// Stable `RIVET_VERIFY_*` error code for this failure variant.
    ///
    /// One code per variant, intended for orchestrators / CI to branch on
    /// without parsing the human `Display` string or the per-variant JSON
    /// fields.  The code is part of the wire contract: it is emitted next to
    /// `kind` in the JSON report and prefixed in brackets on each pretty line.
    /// Codes are append-only — never renamed once shipped (a renamed code is a
    /// silent break for any consumer keying off it).
    pub fn error_code(&self) -> &'static str {
        match self {
            Failure::PartMissing { .. } => "RIVET_VERIFY_PART_MISSING",
            Failure::PartSizeMismatch { .. } => "RIVET_VERIFY_PART_SIZE_MISMATCH",
            Failure::PartChecksumMismatch { .. } => "RIVET_VERIFY_PART_CHECKSUM_MISMATCH",
            Failure::ValueChecksumMismatch { .. } => "RIVET_VERIFY_VALUE_CHECKSUM",
            Failure::PartRowCountMismatch { .. } => "RIVET_VERIFY_PART_ROW_COUNT",
            Failure::CdcPositionViolation { .. } => "RIVET_VERIFY_CDC_POSITION",
            Failure::SuccessMarkerMalformed { .. } => "RIVET_VERIFY_SUCCESS_MALFORMED",
            Failure::SuccessMarkerStale { .. } => "RIVET_VERIFY_SUCCESS_STALE",
            Failure::ManifestSelfInconsistent { .. } => "RIVET_VERIFY_MANIFEST_INCONSISTENT",
            Failure::ManifestReadError { .. } => "RIVET_VERIFY_MANIFEST_READ_ERROR",
            Failure::SuccessMarkerReadError { .. } => "RIVET_VERIFY_SUCCESS_READ_ERROR",
            Failure::ListPrefixError { .. } => "RIVET_VERIFY_LIST_ERROR",
            Failure::UntrackedObject { .. } => "RIVET_VERIFY_UNTRACKED_OBJECT",
            Failure::ContentVerificationUnmet { .. } => "RIVET_VERIFY_CONTENT_UNMET",
            Failure::ManifestRequiredButAbsent { .. } => "RIVET_VERIFY_MANIFEST_REQUIRED",
        }
    }
}

impl std::fmt::Display for Failure {
    /// One operator-facing line per failure variant.  Used by:
    /// - `pipeline::report::render_markdown` (summary.md "failure:" lines)
    /// - `pipeline::validate_cmd::render_pretty` (`rivet validate` stdout)
    /// - any future consumer that wants a human-readable failure label
    ///
    /// The wire format (`failures[].kind` + per-variant fields) lives in
    /// the `Serialize` derive above and is the contract Airflow / CI
    /// consumers branch on.  This `Display` impl is for humans only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::PartMissing { part_id, path } => {
                write!(f, "part {} missing at {}", part_id, path)
            }
            Failure::PartSizeMismatch {
                part_id,
                path,
                expected,
                actual,
            } => write!(
                f,
                "part {} size mismatch at {}: manifest {}, dest {}",
                part_id, path, expected, actual
            ),
            Failure::PartRowCountMismatch { detail } => {
                write!(f, "part row count disagrees with the part: {}", detail)
            }
            Failure::ValueChecksumMismatch { detail } => {
                write!(
                    f,
                    "value checksum mismatch (post-write corruption): {}",
                    detail
                )
            }
            Failure::CdcPositionViolation { detail } => {
                write!(f, "cdc __pos continuity violation: {}", detail)
            }
            Failure::PartChecksumMismatch {
                part_id,
                path,
                expected,
                actual,
            } => write!(
                f,
                "part {} content mismatch at {}: manifest md5 {}, dest {}",
                part_id, path, expected, actual
            ),
            Failure::SuccessMarkerMalformed { body_preview } => {
                write!(f, "_SUCCESS body malformed: {body_preview:?}")
            }
            Failure::SuccessMarkerStale {
                marker_fingerprint,
                manifest_fingerprint,
            } => write!(
                f,
                "_SUCCESS body {} != manifest fingerprint {} (stale marker)",
                marker_fingerprint, manifest_fingerprint
            ),
            Failure::ManifestSelfInconsistent { detail } => {
                write!(f, "manifest self-consistency: {detail}")
            }
            Failure::ManifestReadError { detail } => {
                write!(f, "manifest read error: {detail}")
            }
            Failure::SuccessMarkerReadError { detail } => {
                write!(f, "_SUCCESS read error: {detail}")
            }
            Failure::ListPrefixError { detail } => {
                write!(f, "destination listing error: {detail}")
            }
            Failure::UntrackedObject { key, size_bytes } => {
                write!(f, "untracked object: {} ({} bytes)", key, size_bytes)
            }
            Failure::ContentVerificationUnmet { size_only, total } => write!(
                f,
                "verify: content not met — {size_only} of {total} part(s) only \
                 size-verified (no store checksum); lower max_file_size so parts \
                 upload as a single PUT, or the backend exposes no checksum"
            ),
            Failure::ManifestRequiredButAbsent { prefix } => write!(
                f,
                "no manifest at {prefix}: a manifest was required here (operator \
                 pinned --prefix) but none was found — this prefix was never \
                 written, or the data was relocated. Run the export first, or \
                 drop --prefix to validate the config-resolved destination."
            ),
        }
    }
}

impl ManifestVerification {
    /// Base verdict: nothing checked yet (no manifest, all counts zero, all
    /// sub-checks false, `passed = false`).  Every constructor builds on this
    /// and overrides only what differs, so a new field lands in **one** place
    /// rather than several near-identical literals.
    fn empty() -> Self {
        Self {
            manifest_found: false,
            legacy_run: false,
            parts_verified: 0,
            parts_md5_verified: 0,
            parts_failed: 0,
            success_marker_consistent: false,
            manifest_self_consistent: false,
            passed: false,
            failures: Vec::new(),
            // Base level; `verify_at_destination` overwrites this with the
            // depth it was actually called at before returning any verdict.
            depth_level: default_depth_level(),
        }
    }

    /// Recompute `passed` from the verdict's facts: a manifest was found and no
    /// **fatal** failure was recorded (advisory failures like `UntrackedObject`
    /// don't count).  The single source of truth — callers set failures and
    /// call this once, rather than flipping `passed` by hand at every site.
    fn recompute_passed(&mut self) {
        self.passed = self.manifest_found && !self.failures.iter().any(Failure::is_fatal);
    }

    /// Apply the export's `verify` policy (ADR-0013 / review D).  When content
    /// verification is required but some parts were only size-verified, record
    /// a fatal [`Failure::ContentVerificationUnmet`] and re-derive `passed`.
    /// Policy lives here (one place); the composers — run finalize and the
    /// `rivet validate` command — just call it with their export's intent.
    pub fn enforce_content_policy(&mut self, require_content: bool) {
        if require_content && self.manifest_found {
            let size_only = self.parts_verified.saturating_sub(self.parts_md5_verified);
            if size_only > 0 {
                self.failures.push(Failure::ContentVerificationUnmet {
                    size_only,
                    total: self.parts_verified,
                });
                self.recompute_passed();
            }
        }
    }

    /// Apply the "a manifest must exist here" policy (finding #20).  When the
    /// operator pinned a literal `--prefix`, an absent manifest is no longer the
    /// benign M6 legacy-run case — it almost always means the prefix was never
    /// written (a misconfigured CI gate). Convert that exact verdict — no
    /// manifest, no other failure (i.e. the [`ManifestVerification::legacy`]
    /// shape) — into a fatal [`Failure::ManifestRequiredButAbsent`] so the exit
    /// gate refuses it loudly instead of silently passing.
    ///
    /// Deliberately a no-op for every other shape: a real manifest (passed or
    /// failed), or an absent manifest that already carries a `ManifestReadError`
    /// / head failure, is left untouched — those are already classified.  Only
    /// the "legacy / cannot certify" case is escalated, and only when required.
    pub fn require_manifest_present(&mut self, prefix: &str) {
        if !self.manifest_found && !self.has_failures() {
            self.legacy_run = false;
            self.failures.push(Failure::ManifestRequiredButAbsent {
                prefix: prefix.to_string(),
            });
            self.recompute_passed();
        }
    }

    /// Construct the M6 (legacy run) verdict for a destination that has no
    /// manifest at all.  Caller composes this with the existing per-file
    /// row-count check; together they form the legacy `--validate` result.
    pub fn legacy() -> Self {
        // `passed = false` is intentional — not "validation failed" but "this
        // verifier cannot certify"; the caller layers per-file row counts on
        // top and composes the final verdict.
        Self {
            legacy_run: true,
            ..Self::empty()
        }
    }

    /// True iff this verification surfaced any explicit failure (i.e. a
    /// reason an orchestrator should refuse the run).  Distinct from
    /// `!passed`, which can also mean "legacy / not applicable".
    pub fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    /// Whether any recorded failure is VERIFIED-WRONG (the check ran and the data
    /// is bad) as opposed to purely COULD-NOT-VERIFY (an I/O read/list error).
    /// Drives the exit CLASS: a verdict with a verified-wrong failure is
    /// data-integrity (exit 3); a verdict whose failures are ALL could-not-verify
    /// is operational (exit 1). See [`Failure::is_could_not_verify`].
    pub fn has_verified_wrong_failure(&self) -> bool {
        self.failures.iter().any(|f| !f.is_could_not_verify())
    }
}

/// Run the manifest-aware verification at `manifest_dir` (the destination-
/// relative directory containing `manifest.json` and `_SUCCESS`).
///
/// `manifest_dir` is the same key shape `Destination::write` was called with
/// for the manifest itself — typically empty (`""`) for prefix-rooted runs,
/// or the per-export sub-directory.  Trailing `/` is optional.
///
/// This function does not panic on any expected I/O outcome — every read
/// failure becomes a `Failure::*ReadError` so the caller can render a
/// useful message instead of bailing.
///
/// `depth` selects the graded verify layer (see [`ValidateDepth`]):
/// - [`ValidateDepth::Light`] skips section 3 (the `list_prefix` part
///   reconcile) and section 5 (untracked surplus), leaving `parts_verified`
///   at 0 — a fast manifest + `_SUCCESS` poll with no prefix listing.
/// - [`ValidateDepth::Sample`] and [`ValidateDepth::Full`] run all five
///   sections here.  The Form B value re-read is **not** in this function;
///   it is the caller's concern (`run_validate_command`), gated on `Full`.
///
/// Regardless of depth, `depth_level` on the returned verdict records the
/// level this pass ran at.
/// Read every run-unique manifest COPY (`manifest-<run_id>.json`) named in the
/// prefix listing. Best-effort: a copy that fails to read or parse is skipped —
/// the untracked scan it feeds is advisory, so a bad sibling copy must never turn
/// validate into an error (worst case a part it would have claimed stays flagged,
/// the pre-#167 behaviour). The canonical `manifest.json` is not a copy and is
/// excluded (it is already the primary manifest).
fn read_sibling_manifests(dest: &dyn Destination, listing: &[ObjectMeta]) -> Vec<RunManifest> {
    let mut out = Vec::new();
    for meta in listing {
        let base = meta.key.rsplit('/').next().unwrap_or("");
        if !is_run_unique_manifest_name(base) {
            continue;
        }
        if let Ok(bytes) = read_capped(dest, &meta.key, MANIFEST_MAX_BYTES)
            && let Ok(m) = serde_json::from_slice::<RunManifest>(&bytes)
        {
            out.push(m);
        }
    }
    out
}

/// The set of destination keys claimed by a SAME-FAMILY sibling run-unique manifest copy
/// (#167 merge-back). A part is claimed only when a sibling of the SAME `export_family`
/// declared it `Committed`, so a foreign export's parts under a mistakenly-shared prefix are
/// still surfaced as untracked. Covers BOTH split units AND a plain export's superseded
/// historical / CDC-soak copies, so neither reads as surplus. Pure — the I/O (reading the
/// copies) is [`read_sibling_manifests`]'s job.
fn sibling_claimed_part_keys(
    siblings: &[RunManifest],
    canonical_family: &str,
    manifest_dir: &str,
) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    if canonical_family.is_empty() {
        // A legacy manifest with no family: never claim across copies (a legacy prefix is
        // single-run, and folding by empty family could hide cross-contamination).
        return out;
    }
    for m in siblings {
        if m.export_family != canonical_family {
            continue;
        }
        for p in &m.parts {
            if p.status == PartStatus::Committed {
                out.insert(join_key(manifest_dir, &p.path));
            }
        }
    }
    out
}

/// A working manifest whose `parts` are the UNION of `canonical`'s parts and every
/// SAME-FAMILY **split-unit** sibling copy's `Committed` parts, deduped by declared path. The
/// reconcile target for a `--pool --split` prefix: presence/size/md5 then cover EVERY unit,
/// not just the last writer whose parts the canonical `manifest.json` happens to list.
///
/// A sibling is folded ONLY when it carries a `split_window` — the precise mark of a
/// co-current split UNIT (finding 2). A plain export's family is its own name, so its
/// HISTORICAL repeated-run copies share the family too, but they are SUPERSEDED snapshots
/// (no `split_window`); folding them would presence-check a legitimately cleaned old part and
/// false-fail. The canonical's own copy is among the siblings, so seeding `seen` with the
/// canonical's parts keeps it from being added twice. Only `parts` differ from `canonical`.
fn merge_split_unit_parts(canonical: &RunManifest, siblings: &[RunManifest]) -> RunManifest {
    let mut merged = canonical.clone();
    let mut seen: std::collections::BTreeSet<String> =
        merged.parts.iter().map(|p| p.path.clone()).collect();
    for m in siblings {
        if m.export_family != canonical.export_family {
            continue; // never fold a FOREIGN family's parts into the check
        }
        if m.split_window.is_none() {
            continue; // only co-current SPLIT units — not superseded plain repeated-run copies
        }
        for p in &m.parts {
            if p.status == PartStatus::Committed && seen.insert(p.path.clone()) {
                merged.parts.push(p.clone());
            }
        }
    }
    merged
}

pub fn verify_at_destination(
    dest: &dyn Destination,
    manifest_dir: &str,
    depth: ValidateDepth,
) -> Result<ManifestVerification> {
    let manifest_key = join_key(manifest_dir, MANIFEST_FILENAME);
    let success_key = join_key(manifest_dir, SUCCESS_FILENAME);

    // Stamp the depth this pass ran at onto every verdict before it leaves the
    // function — including the early-return error/legacy shapes — so a consumer
    // always sees *how much* was checked.  Each `return Ok(v)` below routes
    // through `with_depth` (or sets `out.depth_level` for the main path).
    let with_depth = |mut v: ManifestVerification| -> ManifestVerification {
        v.depth_level = depth.label().to_string();
        v
    };

    // ── 1. Manifest read ───────────────────────────────────────────────
    //
    // Error-consistency contract: every I/O outcome here surfaces as a
    // structured `Failure` variant rather than as `Err`.  An operator gets
    // one verdict shape regardless of whether the destination is missing,
    // permission-denied, or temporarily unreachable.  The bubbled `Err`
    // path is reserved for *programmer* errors (caller passes a malformed
    // `manifest_dir`, a future destination breaks an internal invariant).
    let manifest_bytes = match dest.head(&manifest_key) {
        Ok(None) => return Ok(with_depth(ManifestVerification::legacy())),
        Ok(Some(_)) => match read_capped(dest, &manifest_key, MANIFEST_MAX_BYTES) {
            Ok(b) => b,
            Err(e) => {
                let mut v = ManifestVerification::legacy();
                v.legacy_run = false;
                v.failures.push(Failure::ManifestReadError {
                    detail: format!("{e:#}"),
                });
                v.passed = false;
                return Ok(with_depth(v));
            }
        },
        Err(e) => {
            // `head` failure is symmetric to a `read` failure — same kind
            // (`ManifestReadError`) so consumers don't have to branch on
            // which method tripped.  Distinct from "manifest absent"
            // (Ok(None) above) which legitimately means "legacy prefix".
            let mut v = ManifestVerification::legacy();
            v.legacy_run = false;
            v.failures.push(Failure::ManifestReadError {
                detail: format!("manifest head failed: {e:#}"),
            });
            v.passed = false;
            return Ok(with_depth(v));
        }
    };

    let manifest: RunManifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(m) => m,
        Err(e) => {
            // A malformed manifest is treated as a self-inconsistency —
            // semantically equivalent for the operator (the manifest can't
            // be trusted) but kept distinct in `failures` so the kind is
            // explicit on the wire.
            return Ok(with_depth(ManifestVerification {
                manifest_found: true,
                failures: vec![Failure::ManifestSelfInconsistent {
                    detail: format!("manifest.json parse failed: {e}"),
                }],
                ..ManifestVerification::empty()
            }));
        }
    };

    // Optimistic base: a found, self-consistent manifest that passes until a
    // check below flips it.  Overrides only what differs from `empty()`.
    // Stamp the depth here so the two early `return Ok(out)` paths in section
    // 4 (success-marker head error, non-utf8 body) carry the right level too.
    let mut out = ManifestVerification {
        manifest_found: true,
        manifest_self_consistent: true,
        passed: true,
        depth_level: depth.label().to_string(),
        ..ManifestVerification::empty()
    };

    // ── 2. Self-consistency ─────────────────────────────────────────────
    if let Err(e) = manifest.validate_self_consistency() {
        out.manifest_self_consistent = false;
        out.failures.push(Failure::ManifestSelfInconsistent {
            detail: format!("{e}"),
        });
        // Don't short-circuit — we still want to surface part-presence
        // failures because the operator may want to know both classes at
        // once rather than fix-then-rerun.
    }

    // ── 3. Reconcile parts + surplus against ONE prefix listing ────────
    //
    // Presence and untracked-surplus both fall out of a single
    // `reconcile_manifest_against_listing` over one `list_prefix` — the same
    // pure walk chunked resume uses (`build_resume_plan`).  This replaces the
    // old per-part `HEAD` loop (N round-trips) and its separate untracked
    // listing.  Per-part failures are emitted here (step 3); untracked is
    // emitted at step 5 so the failure ordering an operator reads is stable.
    //
    // Trade-off: presence now rides the listing, not per-part `HEAD`.  If the
    // listing cannot be read, an audit cannot certify the parts — so a list
    // failure flips `passed = false` (a `ListPrefixError`), rather than the
    // old behaviour where per-part HEAD still "verified" parts a failed
    // listing couldn't enumerate.  Every Rivet destination backend offers
    // strong read-after-write list consistency, so the happy path is one call.
    //
    // Graded depth: `Light` skips this `list_prefix` entirely — no part
    // reconcile, no `ListPrefixError`, `parts_verified` stays 0, and section 5
    // (untracked) is a no-op since `reconciliation` is `None`.  `Sample` and
    // `Full` run it.  A `Light` pass therefore certifies only that the
    // manifest reads, is self-consistent, and `_SUCCESS` matches — never that
    // the parts are physically present.
    let reconciliation = if depth.runs_part_reconcile() {
        match dest.list_prefix(manifest_dir) {
            Ok(listing) => {
                // #167: a `--pool --split` prefix holds N run-unique manifest copies of ONE
                // family, each declaring a DISJOINT set of parts. The canonical `manifest.json`
                // lists only the LAST writer's parts, so reconciling the canonical alone
                // presence/size/md5-checked ONLY the last unit — a missing/corrupt part of any
                // OTHER split unit was invisible (no PartMissing: not in the canonical list; no
                // UntrackedObject: not on disk), and the trust oracle silently PASSED an
                // incomplete split (adjacent-bughunt finding, HIGH). Fold every SAME-FAMILY
                // SPLIT-UNIT sibling's parts into the reconcile target so each unit is checked.
                let siblings = if manifest.export_family.is_empty() {
                    Vec::new()
                } else {
                    read_sibling_manifests(dest, &listing)
                };
                let target = merge_split_unit_parts(&manifest, &siblings);
                let mut rec = reconcile_manifest_against_listing(&target, &listing, manifest_dir);
                // Same-family sibling parts — the folded split units AND a plain export's
                // SUPERSEDED historical / CDC-soak copies (no split_window, so NOT folded
                // above) — are declared by their own copies elsewhere in the prefix, so they
                // must not read as untracked surplus (noise that also MASKS a real orphan). A
                // foreign family's parts are never claimed, so cross-contamination still shows.
                if !rec.untracked.is_empty() && !manifest.export_family.is_empty() {
                    let claimed =
                        sibling_claimed_part_keys(&siblings, &manifest.export_family, manifest_dir);
                    rec.untracked.retain(|o| !claimed.contains(&o.key));
                }
                Some(rec)
            }
            Err(e) => {
                out.failures.push(Failure::ListPrefixError {
                    detail: format!("{e:#}"),
                });
                None
            }
        }
    } else {
        None
    };
    if let Some(rec) = &reconciliation {
        for check in &rec.per_part {
            match &check.presence {
                PartPresence::Present { md5_verified } => {
                    out.parts_verified += 1;
                    if *md5_verified {
                        out.parts_md5_verified += 1;
                    }
                }
                PartPresence::SizeMismatch { expected, actual } => {
                    out.parts_failed += 1;
                    out.failures.push(Failure::PartSizeMismatch {
                        part_id: check.part_id,
                        path: check.path.clone(),
                        expected: *expected,
                        actual: *actual,
                    });
                }
                PartPresence::Missing => {
                    out.parts_failed += 1;
                    out.failures.push(Failure::PartMissing {
                        part_id: check.part_id,
                        path: check.path.clone(),
                    });
                }
                PartPresence::ChecksumMismatch { expected, actual } => {
                    out.parts_failed += 1;
                    out.failures.push(Failure::PartChecksumMismatch {
                        part_id: check.part_id,
                        path: check.path.clone(),
                        expected: expected.clone(),
                        actual: actual.clone(),
                    });
                }
            }
        }
    }

    // ── 4. _SUCCESS marker consistency ─────────────────────────────────
    //
    // Same error-consistency contract as step 1: head/read failures become
    // `Failure::SuccessMarkerReadError`, not bubbled `Err`.  Absent marker
    // (Ok(None)) stays informational, not a failure (M2: only successful
    // runs land _SUCCESS, so its absence on a failed manifest is correct).
    let success_head = match dest.head(&success_key) {
        Ok(h) => h,
        Err(e) => {
            out.failures.push(Failure::SuccessMarkerReadError {
                detail: format!("_SUCCESS head failed: {e:#}"),
            });
            out.recompute_passed();
            return Ok(out);
        }
    };
    match success_head {
        None => {
            // Absent _SUCCESS is informational, not a failure: per ADR-0012
            // M2, only successful runs land it.  A failed-then-rewritten
            // manifest legitimately lacks _SUCCESS.  Leave
            // `success_marker_consistent = false` (this is a "no signal"
            // bool, not a "broken" bool) and let the caller decide.
        }
        Some(head) if head.size_bytes > SUCCESS_MARKER_MAX_BYTES => {
            // Refuse to materialise an oversized _SUCCESS: a bare uncapped
            // dest.read of a multi-GB planted marker OOMs the validate/resume/repair
            // process (the asymmetry with step 1's manifest.json cap this closes).
            // success_head already carries the size — no extra round-trip.
            out.failures.push(Failure::SuccessMarkerMalformed {
                body_preview: format!(
                    "(oversized: {} bytes exceeds the {SUCCESS_MARKER_MAX_BYTES}-byte _SUCCESS cap; not read)",
                    head.size_bytes
                ),
            });
            out.recompute_passed();
            return Ok(out);
        }
        Some(_) => match dest.read(&success_key) {
            Err(e) => {
                out.failures.push(Failure::SuccessMarkerReadError {
                    detail: format!("{e:#}"),
                });
            }
            Ok(body) => {
                let body_str = match std::str::from_utf8(&body) {
                    Ok(s) => s,
                    Err(_) => {
                        out.failures.push(Failure::SuccessMarkerMalformed {
                            body_preview: format!("(non-utf8, {} bytes)", body.len()),
                        });
                        out.recompute_passed();
                        return Ok(out);
                    }
                };
                match parse_success_marker(body_str) {
                    None => {
                        out.failures.push(Failure::SuccessMarkerMalformed {
                            body_preview: preview(body_str),
                        });
                    }
                    Some(marker_fp) => {
                        let manifest_fp = success_marker_body(&manifest_bytes);
                        // success_marker_body returns the trailing `\n`
                        // form; trim before comparing to the parsed marker
                        // (which already trims).
                        let manifest_fp_trimmed = manifest_fp.trim_end_matches('\n');
                        if marker_fp == manifest_fp_trimmed {
                            out.success_marker_consistent = true;
                        } else {
                            out.failures.push(Failure::SuccessMarkerStale {
                                marker_fingerprint: marker_fp.to_string(),
                                manifest_fingerprint: manifest_fp_trimmed.to_string(),
                            });
                        }
                    }
                }
            }
        },
    }

    // ── 5. Untracked surplus ───────────────────────────────────────────
    //
    // Already computed by the step-3 reconciliation (sidecars, quarantine,
    // and the doctor probe are filtered there).  Emit it last so the failure
    // ordering stays parts → marker → untracked.  A list failure left
    // `reconciliation = None` and already flipped `passed` above.
    if let Some(rec) = reconciliation {
        for obj in rec.untracked {
            out.failures.push(Failure::UntrackedObject {
                key: obj.key,
                size_bytes: obj.size_bytes,
            });
        }
    }

    out.recompute_passed();
    Ok(out)
}

/// Truncate `s` to a small printable preview for error messages.
fn preview(s: &str) -> String {
    let trimmed: String = s.chars().take(40).collect();
    if s.chars().count() > 40 {
        format!("{trimmed}…")
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};
    use crate::destination::local::LocalDestination;
    use crate::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
        PartStatus, RunManifest,
    };
    use std::path::Path;

    fn local_dest(base: &Path) -> LocalDestination {
        LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(base.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap()
    }

    fn part(part_id: u32, rows: i64, size: u64, fp: &str) -> ManifestPart {
        ManifestPart {
            part_id,
            path: format!("part-{part_id:06}.parquet"),
            rows,
            size_bytes: size,
            content_fingerprint: fp.into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }
    }

    fn build_manifest(parts: Vec<ManifestPart>, status: ManifestStatus) -> RunManifest {
        let row_count: i64 = parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .map(|p| p.rows)
            .sum();
        let part_count = parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .count() as u32;
        RunManifest {
            split_window: None,
            checksum_render: None,
            row_hash: None,
            mode: "batch".to_string(),
            manifest_version: MANIFEST_VERSION,
            run_id: "r".into(),
            export_name: "public.orders".into(),
            export_family: String::new(),
            started_at: "2026-05-21T12:00:00Z".into(),
            finished_at: "2026-05-21T12:01:00Z".into(),
            status,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: Some("public".into()),
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///tmp/out".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    /// Lay out a clean dataset with manifest + _SUCCESS at the root.
    fn write_dataset(dir: &Path, m: &RunManifest, parts_with_bytes: &[(&str, &[u8])]) {
        for (name, bytes) in parts_with_bytes {
            std::fs::write(dir.join(name), bytes).unwrap();
        }
        let body = serde_json::to_vec_pretty(m).unwrap();
        std::fs::write(dir.join(MANIFEST_FILENAME), &body).unwrap();
        if matches!(m.status, ManifestStatus::Success) {
            std::fs::write(dir.join(SUCCESS_FILENAME), success_marker_body(&body)).unwrap();
        }
    }

    // ── #167 merge-back: reconcile the UNION of same-family split-unit parts ──

    fn split_win() -> crate::manifest::SplitWindow {
        crate::manifest::SplitWindow {
            key_column: "id".into(),
            lo: None,
            hi: Some("1000".into()),
        }
    }

    /// merge_split_unit_parts folds every SAME-FAMILY SPLIT-UNIT sibling's committed parts
    /// (those carrying a split_window) into the reconcile target, and EXCLUDES both a FOREIGN
    /// family's parts and a SAME-FAMILY non-split (plain, superseded) copy's parts — so a
    /// `--pool --split` snapshot is checked across ALL units without false-checking a plain
    /// export's historical run or a foreign export sharing the prefix.
    #[test]
    fn merge_split_unit_parts_folds_split_siblings_only() {
        let unit = |name: &str, path: &str, fam: &str, split: bool| {
            let mut m = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
            m.export_family = fam.into();
            m.export_name = name.into();
            m.parts[0].path = path.into();
            m.split_window = if split { Some(split_win()) } else { None };
            m
        };
        let canonical = unit("daily#3", "daily#3_p.parquet", "daily", true); // last split writer
        let siblings = vec![
            unit("daily#0", "daily#0_p.parquet", "daily", true),
            unit("daily#1", "daily#1_p.parquet", "daily", true),
            unit("daily#3", "daily#3_p.parquet", "daily", true), // canonical's OWN copy
            unit("daily", "daily_old.parquet", "daily", false),  // SAME family, PLAIN (superseded)
            unit("other#0", "other_p.parquet", "other", true),   // FOREIGN family
        ];
        let merged = merge_split_unit_parts(&canonical, &siblings);
        let paths: std::collections::BTreeSet<&str> =
            merged.parts.iter().map(|p| p.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "daily#0_p.parquet",
                "daily#1_p.parquet",
                "daily#3_p.parquet"
            ]
            .into_iter()
            .collect(),
            "fold same-family SPLIT units only — never a foreign family, never a same-family \
             plain/superseded copy: {paths:?}"
        );
        assert_eq!(
            merged
                .parts
                .iter()
                .filter(|p| p.path == "daily#3_p.parquet")
                .count(),
            1,
            "the canonical's own sibling copy must not double its part"
        );
    }

    /// A non-committed (quarantined) split-unit sibling part is NOT merged — only durable
    /// committed parts belong to the dataset the reconcile checks for presence.
    #[test]
    fn merge_split_unit_parts_skips_non_committed_siblings() {
        let mut canonical = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
        canonical.export_family = "daily".into();
        canonical.export_name = "daily#0".into();
        canonical.parts[0].path = "daily#0_p.parquet".into();
        canonical.split_window = Some(split_win());

        let mut sib = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
        sib.export_family = "daily".into();
        sib.export_name = "daily#1".into();
        sib.parts[0].path = "daily#1_q.parquet".into();
        sib.parts[0].status = PartStatus::Quarantined;
        sib.split_window = Some(split_win());

        let merged = merge_split_unit_parts(&canonical, &[sib]);
        let paths: Vec<&str> = merged.parts.iter().map(|p| p.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["daily#0_p.parquet"],
            "a quarantined split-unit part must not be folded into the reconcile target"
        );
    }

    /// sibling_claimed_part_keys claims a SAME-FAMILY sibling's committed parts (so a split
    /// unit AND a plain export's superseded historical copy are both kept out of untracked),
    /// but never a FOREIGN family's (cross-contamination must still surface as untracked).
    #[test]
    fn sibling_claimed_part_keys_claims_same_family_only() {
        let unit = |name: &str, path: &str, fam: &str| {
            let mut m = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
            m.export_family = fam.into();
            m.export_name = name.into();
            m.parts[0].path = path.into();
            m
        };
        let siblings = vec![
            unit("daily#0", "daily#0_p.parquet", "daily"),
            unit("daily", "daily_old.parquet", "daily"), // plain superseded copy, same family
            unit("other", "other_p.parquet", "other"),
        ];
        let claimed = sibling_claimed_part_keys(&siblings, "daily", "");
        assert!(
            claimed.contains("daily#0_p.parquet") && claimed.contains("daily_old.parquet"),
            "same-family parts (split unit AND plain historical) must be claimed: {claimed:?}"
        );
        assert!(
            !claimed.contains("other_p.parquet"),
            "a FOREIGN family's part must NOT be claimed — cross-contamination must surface"
        );
    }

    /// A legacy canonical (empty family) claims nothing — the widening never applies where a
    /// family cannot disambiguate.
    #[test]
    fn sibling_claimed_part_keys_empty_family_claims_nothing() {
        let mut m = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
        m.export_family = "daily".into();
        m.parts[0].path = "p.parquet".into();
        assert!(
            sibling_claimed_part_keys(&[m], "", "").is_empty(),
            "an empty canonical family must claim nothing"
        );
    }

    /// A non-committed (quarantined) sibling part is NOT claimed — only durable committed
    /// parts belong to the dataset.
    #[test]
    fn sibling_claimed_part_keys_skips_non_committed() {
        let mut m = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
        m.export_family = "daily".into();
        m.parts[0].path = "q.parquet".into();
        m.parts[0].status = PartStatus::Quarantined;
        assert!(
            sibling_claimed_part_keys(&[m], "daily", "").is_empty(),
            "a quarantined part must not be claimed as a tracked dataset part"
        );
    }

    /// End-to-end: a `--pool --split` prefix where a NON-last unit's part is missing must
    /// FAIL validation. Before the merge-back fix, verify reconciled only the canonical
    /// (last writer's) parts, so a lost part of any other unit was invisible — no PartMissing
    /// (not in the canonical list) and no UntrackedObject (not on disk) — and the trust
    /// oracle silently PASSED an incomplete split (adjacent-bughunt finding, HIGH).
    #[test]
    fn verify_over_a_split_prefix_catches_a_missing_non_last_unit_part() {
        use crate::manifest::run_unique_manifest_name;
        let dir = tempfile::tempdir().unwrap();

        // Two split units of family "orders": #0 (run r0) and #1 (run r1 = LAST → canonical).
        let mut u0 = build_manifest(
            vec![part(0, 10, 4, "xxh3:0000000000000000")],
            ManifestStatus::Success,
        );
        u0.export_family = "orders".into();
        u0.export_name = "orders#0".into();
        u0.run_id = "r0".into();
        u0.parts[0].path = "orders#0-part.parquet".into();
        u0.split_window = Some(crate::manifest::SplitWindow {
            key_column: "id".into(),
            lo: None,
            hi: Some("1000".into()),
        });

        let mut u1 = build_manifest(
            vec![part(1, 20, 5, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        u1.export_family = "orders".into();
        u1.export_name = "orders#1".into();
        u1.run_id = "r1".into();
        u1.parts[0].path = "orders#1-part.parquet".into();
        u1.split_window = Some(crate::manifest::SplitWindow {
            key_column: "id".into(),
            lo: Some("1000".into()),
            hi: None,
        });

        // Both units' run-unique copies on disk; canonical manifest.json = u1 (last writer).
        std::fs::write(
            dir.path().join(run_unique_manifest_name("r0")),
            serde_json::to_vec(&u0).unwrap(),
        )
        .unwrap();
        let u1_body = serde_json::to_vec(&u1).unwrap();
        std::fs::write(dir.path().join(run_unique_manifest_name("r1")), &u1_body).unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILENAME), &u1_body).unwrap();
        std::fs::write(
            dir.path().join(SUCCESS_FILENAME),
            success_marker_body(&u1_body),
        )
        .unwrap();
        // u1's part present (5 bytes = its declared size); u0's part DELIBERATELY MISSING.
        std::fs::write(dir.path().join("orders#1-part.parquet"), b"BBBBB").unwrap();

        let dest = local_dest(dir.path());
        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            !v.passed,
            "a missing part of a NON-last split unit must fail validation, not pass silently"
        );
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { .. })),
            "expected a PartMissing for the dropped orders#0 part; got {:?}",
            v.failures
        );
    }

    // ── happy path ───────────────────────────────────────────────────────

    #[test]
    fn happy_path_verifies_all_parts_and_success_marker() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(
            dir.path(),
            &m,
            &[
                ("part-000001.parquet", b"AAAA"),
                ("part-000002.parquet", b"BBBBB"),
            ],
        );
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found);
        assert!(!v.legacy_run);
        assert_eq!(v.parts_verified, 2);
        assert_eq!(v.parts_failed, 0);
        assert!(v.success_marker_consistent);
        assert!(v.manifest_self_consistent);
        assert!(v.passed);
        assert!(v.failures.is_empty());
    }

    // ── M6 legacy run ───────────────────────────────────────────────────

    #[test]
    fn no_manifest_returns_legacy_run_label() {
        // Empty prefix — no manifest, no parts.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.manifest_found);
        assert!(v.legacy_run);
        assert_eq!(v.parts_verified, 0);
        assert!(!v.passed);
        assert!(v.failures.is_empty(), "no failures, just a legacy label");
    }

    // ── M5 part-presence failures ───────────────────────────────────────

    #[test]
    fn missing_part_is_flagged_with_part_id_and_path() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(
            dir.path(),
            &m,
            &[("part-000001.parquet", b"AAAA")], // part 2 missing
        );
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert_eq!(v.parts_verified, 1);
        assert_eq!(v.parts_failed, 1);
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { part_id: 2, .. }))
        );
    }

    #[test]
    fn part_size_mismatch_is_flagged_with_expected_and_actual() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        // Manifest claims 4 bytes; we write 6.
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"OOPSIE")]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.passed);
        let mismatch = v
            .failures
            .iter()
            .find_map(|f| match f {
                Failure::PartSizeMismatch {
                    part_id,
                    expected,
                    actual,
                    ..
                } => Some((*part_id, *expected, *actual)),
                _ => None,
            })
            .expect("must surface the size mismatch");
        assert_eq!(mismatch, (1, 4, 6));
    }

    // ── _SUCCESS marker integrity ───────────────────────────────────────

    #[test]
    fn stale_success_marker_is_flagged_as_inconsistent() {
        // Write a manifest, then overwrite _SUCCESS with the marker for a
        // *different* manifest body — simulating an orchestrator that
        // mishandled a re-run.
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(
            dir.path().join(SUCCESS_FILENAME),
            success_marker_body(b"different manifest body"),
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.success_marker_consistent);
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::SuccessMarkerStale { .. }))
        );
    }

    #[test]
    fn malformed_success_marker_body_is_flagged() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(dir.path().join(SUCCESS_FILENAME), b"not even xxh3 shaped").unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::SuccessMarkerMalformed { .. }))
        );
    }

    #[test]
    fn absent_success_marker_does_not_fail_validation_alone() {
        // ADR-0012 M2: only successful runs land _SUCCESS.  A failed-then-
        // rewritten manifest legitimately lacks one — verification must
        // not flip `passed` just for that.
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Failed,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        // Note: write_dataset only writes _SUCCESS for status == Success,
        // so no marker exists here.
        assert!(!dir.path().join(SUCCESS_FILENAME).exists());
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found);
        assert!(
            !v.success_marker_consistent,
            "no marker => false (no signal)"
        );
        // The parts still verified, so passed = true.
        assert!(v.passed);
        assert!(v.failures.is_empty());
    }

    // ── self-consistency ────────────────────────────────────────────────

    #[test]
    fn self_inconsistent_manifest_is_flagged_but_part_check_still_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        m.row_count = 9999; // lie

        let body = serde_json::to_vec_pretty(&m).unwrap();
        std::fs::write(dir.path().join("part-000001.parquet"), b"AAAA").unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILENAME), &body).unwrap();
        std::fs::write(
            dir.path().join(SUCCESS_FILENAME),
            success_marker_body(&body),
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found);
        assert!(!v.manifest_self_consistent);
        assert!(!v.passed);
        // Parts that are physically present still get their `parts_verified`
        // counter bumped — both signals are independently useful.
        assert_eq!(v.parts_verified, 1);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::ManifestSelfInconsistent { .. }))
        );
    }

    /// A `manifest.json` that is not valid JSON at all.
    ///
    /// The branch above this one — `self_inconsistent_manifest_is_flagged_but_
    /// part_check_still_runs` — writes a manifest that PARSES cleanly and then
    /// lies (`row_count = 9999`); that is the self-consistency check far below.
    /// The branch here is the one where `serde_json::from_slice` itself FAILS,
    /// and until now nothing reached it: the 2026-08-16 nightly rotation caught
    /// `delete field manifest_found` and `delete field failures` from this very
    /// struct expression, both surviving the whole lib suite.
    ///
    /// Two halves of the operator-facing contract, both ungraded:
    ///  * the verdict must still say a manifest was FOUND — reading a corrupt
    ///    manifest as ABSENT routes the reader to the ADR-0012 M6 legacy-run
    ///    explanation, a different and wrong story about their destination;
    ///  * it must carry a FAILURE naming why — without it `rivet validate`
    ///    reports a destination that did not pass with an EMPTY `failures`
    ///    list: a red verdict and no reason on it.
    ///
    /// (The third survivor from the same report, `delete field passed`, is
    /// EQUIVALENT — every exit from the main body calls `recompute_passed()`,
    /// so that literal is unobservable. It has been in `.cargo/mutants.toml`
    /// with that reason since 2026-07-14 and needs no test.)
    #[test]
    fn an_unparseable_manifest_is_found_and_carries_a_reason() {
        let dir = tempfile::tempdir().unwrap();
        // Present on disk so the fixture is not inert: if this early return
        // ever stopped happening we would fall through to the part check
        // rather than silently do nothing at all.
        std::fs::write(dir.path().join("part-000001.parquet"), b"AAAA").unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILENAME), b"{ this is not json").unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();

        assert!(
            v.manifest_found,
            "an unreadable manifest.json is still a manifest that EXISTS — \
             reporting it absent sends the operator to the legacy-run path \
             instead of at the corruption: {v:?}"
        );
        assert!(
            !v.legacy_run,
            "a corrupt manifest is not a legacy run: {v:?}"
        );
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::ManifestSelfInconsistent { .. })),
            "a parse failure must be RECORDED as a failure — a red verdict with \
             an empty `failures` gives the operator no reason at all: {v:?}"
        );
        assert!(
            !v.passed,
            "a destination whose manifest cannot be parsed has not passed: {v:?}"
        );

        // The reason must name the artifact, not just the kind: the operator
        // reads this line, never the enum variant.
        let detail = v
            .failures
            .iter()
            .find_map(|f| match f {
                Failure::ManifestSelfInconsistent { detail } => Some(detail.clone()),
                _ => None,
            })
            .expect("asserted present above");
        assert!(
            detail.contains("manifest.json"),
            "the reason must name the file it is about: {detail}"
        );
    }

    // ── untracked objects ───────────────────────────────────────────────

    #[test]
    fn verify_counters_track_present_and_failed_parts() {
        // Mutation-tier2 gap: the verdict-shaping fields (manifest_found /
        // passed / failures) and the per-part counters (parts_verified /
        // parts_failed) had no assertion — `+= -> -=` survived. One present
        // part + one missing part pin both counters and the verdict.
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 10, 4, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        // Only part 1 exists at the destination.
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found, "manifest is at the prefix");
        assert_eq!(v.parts_verified, 1, "part 1 is present at its size");
        assert_eq!(v.parts_failed, 1, "part 2 is missing");
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { part_id: 2, .. })),
            "the missing part is named in failures: {:?}",
            v.failures
        );
        assert!(!v.passed, "a missing part must fail the verdict");
    }

    #[test]
    fn read_capped_boundary_is_exact() {
        // `size > max` (not >=): a body exactly AT the cap must load; one
        // byte over must be refused before materialising.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("at-cap.bin"), vec![0u8; 8]).unwrap();
        std::fs::write(dir.path().join("over-cap.bin"), vec![0u8; 9]).unwrap();
        let dest = local_dest(dir.path());
        assert_eq!(
            read_capped(&dest, "at-cap.bin", 8)
                .expect("exactly at cap loads")
                .len(),
            8
        );
        let err = read_capped(&dest, "over-cap.bin", 8).expect_err("over cap refuses");
        assert!(
            err.to_string().contains("read cap"),
            "actionable message: {err:#}"
        );
    }

    #[test]
    fn preview_truncates_only_past_forty_chars() {
        let s39: String = "x".repeat(39);
        let s40: String = "x".repeat(40);
        let s41: String = "x".repeat(41);
        assert_eq!(preview(&s39), s39, "under the limit: unchanged");
        assert_eq!(
            preview(&s40),
            s40,
            "exactly at the limit: unchanged (`>` not `>=`)"
        );
        assert_eq!(
            preview(&s41),
            format!("{s40}\u{2026}"),
            "past the limit: 40 chars + ellipsis"
        );
    }

    #[test]
    fn untracked_object_under_prefix_is_flagged() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(dir.path().join("rogue.parquet"), b"XX").unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            v.failures.iter().any(
                |f| matches!(f, Failure::UntrackedObject { key, .. } if key == "rogue.parquet")
            )
        );
        // Untracked objects are surfaced but do NOT flip `passed` — that
        // is the resume-side decision (M9).  Parts and marker are fine,
        // so passed remains true.
        assert!(v.passed);
    }

    #[test]
    fn quarantine_prefix_objects_are_silently_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::create_dir_all(dir.path().join(crate::manifest::QUARANTINE_PREFIX)).unwrap();
        std::fs::write(
            dir.path()
                .join(crate::manifest::QUARANTINE_PREFIX)
                .join("old.parquet"),
            b"OO",
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.passed);
        assert!(
            !v.failures
                .iter()
                .any(|f| matches!(f, Failure::UntrackedObject { .. })),
            "quarantine_prefix is the legitimate home for these — must not flag"
        );
    }

    #[test]
    fn doctor_probe_is_not_flagged_as_untracked() {
        // Regression: `rivet doctor` writes `.rivet_doctor_probe` at the
        // destination prefix and never removes it.  A subsequent
        // `rivet run --validate` against the same prefix must treat it as a
        // Rivet sidecar, not foreign data — otherwise `has_failures()` trips
        // and the run's `validated` flag is downgraded to FAIL.
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(
            dir.path().join(crate::manifest::DOCTOR_PROBE_FILENAME),
            b"ok",
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            !v.has_failures(),
            "doctor probe must not surface as a failure: {:?}",
            v.failures
        );
        assert!(v.passed);
    }

    // ── manifest_dir join semantics ─────────────────────────────────────

    #[test]
    fn verifies_in_subdirectory_when_manifest_dir_is_non_empty() {
        let outer = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(outer.path().join("sub/run")).unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        let body = serde_json::to_vec_pretty(&m).unwrap();
        std::fs::write(outer.path().join("sub/run/part-000001.parquet"), b"AAAA").unwrap();
        std::fs::write(outer.path().join("sub/run").join(MANIFEST_FILENAME), &body).unwrap();
        std::fs::write(
            outer.path().join("sub/run").join(SUCCESS_FILENAME),
            success_marker_body(&body),
        )
        .unwrap();
        let dest = local_dest(outer.path());

        let v = verify_at_destination(&dest, "sub/run", ValidateDepth::Full).unwrap();
        assert!(v.passed);
        assert_eq!(v.parts_verified, 1);

        // Trailing slash is normalised — same outcome.
        let v2 = verify_at_destination(&dest, "sub/run/", ValidateDepth::Full).unwrap();
        assert!(v2.passed);
    }

    // ── list-failure semantics (presence now rides the listing) ──────────

    /// Wraps a real `LocalDestination` but fails every `list_prefix`. Used to
    /// pin the post-refactor contract: presence is derived from the listing,
    /// so a listing we cannot read means the audit cannot certify the parts.
    struct ListFails(LocalDestination);
    impl crate::destination::Destination for ListFails {
        fn write(&self, p: &Path, k: &str) -> Result<crate::destination::WriteOutcome> {
            self.0.write(p, k)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.0.capabilities()
        }
        fn head(&self, k: &str) -> Result<Option<crate::destination::ObjectMeta>> {
            self.0.head(k)
        }
        fn read(&self, k: &str) -> Result<Vec<u8>> {
            self.0.read(k)
        }
        fn list_prefix(&self, _: &str) -> Result<Vec<crate::destination::ObjectMeta>> {
            anyhow::bail!("listing unavailable")
        }
    }

    #[test]
    fn list_failure_cannot_certify_parts_and_fails_the_audit() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(vec![part(0, 3, 3, "xxh3:0")], ManifestStatus::Success);
        write_dataset(dir.path(), &m, &[("part-000000.parquet", b"abc")]);
        let dest = ListFails(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        // The manifest itself reads + parses fine (HEAD/read still work)…
        assert!(v.manifest_found);
        assert!(v.manifest_self_consistent);
        // …but with no listing we verify zero parts and refuse to pass.
        assert!(
            !v.passed,
            "an audit that cannot list the prefix must not pass"
        );
        assert_eq!(v.parts_verified, 0);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::ListPrefixError { .. })),
            "expected a ListPrefixError, got: {:?}",
            v.failures
        );
    }

    // ── manifest read-error semantics (explicit failure, not legacy) ─────

    /// Wraps a real `LocalDestination` but fails reading `manifest.json`
    /// (head still sees it) — EACCES or a transient store error on a
    /// manifest that exists.
    struct ManifestReadFails(LocalDestination);
    impl crate::destination::Destination for ManifestReadFails {
        fn write(&self, p: &Path, k: &str) -> Result<crate::destination::WriteOutcome> {
            self.0.write(p, k)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.0.capabilities()
        }
        fn head(&self, k: &str) -> Result<Option<crate::destination::ObjectMeta>> {
            self.0.head(k)
        }
        fn read(&self, k: &str) -> Result<Vec<u8>> {
            if k.ends_with(MANIFEST_FILENAME) {
                anyhow::bail!("permission denied (simulated)")
            }
            self.0.read(k)
        }
        fn list_prefix(&self, p: &str) -> Result<Vec<crate::destination::ObjectMeta>> {
            self.0.list_prefix(p)
        }
    }

    #[test]
    fn unreadable_manifest_is_explicit_failure_not_legacy() {
        // The exit gates (`rivet validate`, run finalize) key off this exact
        // shape: `manifest_found: false` but `has_failures()` — distinct
        // from M6 legacy (`legacy_run: true`, no failures, exit 0).
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = ManifestReadFails(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.manifest_found);
        assert!(!v.legacy_run, "a read error is not the M6 legacy label");
        assert!(!v.passed);
        assert!(v.has_failures(), "orchestrators need a reason to refuse");
        assert!(
            matches!(v.failures.as_slice(), [Failure::ManifestReadError { .. }]),
            "expected exactly one ManifestReadError, got: {:?}",
            v.failures
        );
    }

    /// Same contract when `head` itself errors (cannot even stat the
    /// manifest): symmetric `ManifestReadError`, never the legacy label.
    struct ManifestHeadFails(LocalDestination);
    impl crate::destination::Destination for ManifestHeadFails {
        fn write(&self, p: &Path, k: &str) -> Result<crate::destination::WriteOutcome> {
            self.0.write(p, k)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.0.capabilities()
        }
        fn head(&self, k: &str) -> Result<Option<crate::destination::ObjectMeta>> {
            if k.ends_with(MANIFEST_FILENAME) {
                anyhow::bail!("io timeout (simulated)")
            }
            self.0.head(k)
        }
        fn read(&self, k: &str) -> Result<Vec<u8>> {
            self.0.read(k)
        }
        fn list_prefix(&self, p: &str) -> Result<Vec<crate::destination::ObjectMeta>> {
            self.0.list_prefix(p)
        }
    }

    #[test]
    fn manifest_head_error_is_explicit_failure_not_legacy() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = ManifestHeadFails(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.manifest_found);
        assert!(!v.legacy_run);
        assert!(!v.passed);
        assert!(
            matches!(
                v.failures.as_slice(),
                [Failure::ManifestReadError { detail }] if detail.contains("manifest head failed")
            ),
            "expected one ManifestReadError naming the head step, got: {:?}",
            v.failures
        );
    }

    // #5: a _SUCCESS whose head-reported size exceeds the cap must be flagged
    // WITHOUT being read — a bare uncapped read of a multi-GB planted marker OOMs
    // the validate/resume process (asymmetric with the manifest.json cap). The mock
    // PANICS if read(_SUCCESS) is called, so the test passing proves the read was
    // short-circuited by the size check.
    struct SuccessMarkerOversized(LocalDestination);
    impl crate::destination::Destination for SuccessMarkerOversized {
        fn write(&self, p: &Path, k: &str) -> Result<crate::destination::WriteOutcome> {
            self.0.write(p, k)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.0.capabilities()
        }
        fn head(&self, k: &str) -> Result<Option<crate::destination::ObjectMeta>> {
            if k.ends_with(crate::manifest::SUCCESS_FILENAME) {
                return Ok(Some(crate::destination::ObjectMeta {
                    key: k.to_string(),
                    size_bytes: SUCCESS_MARKER_MAX_BYTES * 4,
                    content_md5: None,
                }));
            }
            self.0.head(k)
        }
        fn read(&self, k: &str) -> Result<Vec<u8>> {
            assert!(
                !k.ends_with(crate::manifest::SUCCESS_FILENAME),
                "verify must NOT read an oversized _SUCCESS — the size cap must short-circuit \
                 before materialising it into memory (the OOM this guards)"
            );
            self.0.read(k)
        }
        fn list_prefix(&self, p: &str) -> Result<Vec<crate::destination::ObjectMeta>> {
            self.0.list_prefix(p)
        }
    }

    #[test]
    fn oversized_success_marker_is_malformed_and_never_read() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = SuccessMarkerOversized(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            v.failures.iter().any(|f| matches!(
                f,
                Failure::SuccessMarkerMalformed { body_preview } if body_preview.contains("oversized")
            )),
            "an oversized _SUCCESS must yield SuccessMarkerMalformed(oversized) without reading it, got: {:?}",
            v.failures
        );
    }

    #[test]
    fn passed_is_derived_advisory_failures_do_not_fail() {
        // An advisory failure (untracked surplus) keeps the verdict passing…
        let mut v = ManifestVerification {
            manifest_found: true,
            ..ManifestVerification::empty()
        };
        v.failures.push(Failure::UntrackedObject {
            key: "stray.parquet".into(),
            size_bytes: 9,
        });
        v.recompute_passed();
        assert!(v.passed, "untracked surplus is advisory, not fatal");

        // …while any fatal failure flips it.
        v.failures.push(Failure::PartMissing {
            part_id: 1,
            path: "part-000001.parquet".into(),
        });
        v.recompute_passed();
        assert!(!v.passed, "a missing part is fatal");

        // No manifest → never passes regardless of failures.
        let mut legacy = ManifestVerification::empty();
        legacy.recompute_passed();
        assert!(!legacy.passed, "no manifest found → cannot certify");
    }

    #[test]
    fn verify_content_policy_fails_only_size_only_parts() {
        // 3 parts, 2 content-checked, 1 size-only.
        let base = ManifestVerification {
            manifest_found: true,
            parts_verified: 3,
            parts_md5_verified: 2,
            ..ManifestVerification::empty()
        };
        // verify: size → size-only is acceptable, passes.
        let mut sz = base.clone();
        sz.recompute_passed();
        sz.enforce_content_policy(false);
        assert!(sz.passed, "size-only OK under verify: size");

        // verify: content → the 1 size-only part is a fatal failure.
        let mut ct = base.clone();
        ct.recompute_passed();
        ct.enforce_content_policy(true);
        assert!(!ct.passed, "a size-only part fails verify: content");
        assert!(
            ct.failures.iter().any(|f| matches!(
                f,
                Failure::ContentVerificationUnmet {
                    size_only: 1,
                    total: 3
                }
            )),
            "expected ContentVerificationUnmet, got: {:?}",
            ct.failures
        );

        // verify: content with every part md5-checked → satisfied.
        let mut all = ManifestVerification {
            parts_md5_verified: 3,
            ..base
        };
        all.recompute_passed();
        all.enforce_content_policy(true);
        assert!(
            all.passed && all.failures.is_empty(),
            "all md5 meets verify: content"
        );
    }

    // ── require_manifest_present (finding #20: operator-pinned --prefix) ──────

    #[test]
    fn require_manifest_escalates_legacy_to_fatal_absent() {
        // The exact shape `verify_at_destination` returns for an absent manifest
        // (`legacy()`): no manifest, no other failure. With a pinned `--prefix`
        // this is escalated to a fatal `ManifestRequiredButAbsent` so the exit
        // gate refuses it instead of passing as a benign legacy run.
        let mut v = ManifestVerification::legacy();
        assert!(v.legacy_run && !v.has_failures());

        v.require_manifest_present("exports/2026-06-09/orders/");

        assert!(!v.legacy_run, "no longer the benign legacy-run label");
        assert!(!v.passed, "an absent-but-required manifest cannot pass");
        assert!(
            matches!(
                v.failures.as_slice(),
                [Failure::ManifestRequiredButAbsent { prefix }]
                    if prefix == "exports/2026-06-09/orders/"
            ),
            "expected one ManifestRequiredButAbsent naming the prefix, got: {:?}",
            v.failures
        );
    }

    #[test]
    fn require_manifest_is_noop_on_a_real_passing_manifest() {
        // A found, passing verdict is untouched — `--prefix` plus real data is
        // the normal "validate this exact prefix" case and must still pass.
        let mut v = ManifestVerification {
            manifest_found: true,
            manifest_self_consistent: true,
            parts_verified: 1,
            passed: true,
            ..ManifestVerification::empty()
        };
        v.require_manifest_present("exports/orders/");
        assert!(
            v.passed && v.failures.is_empty(),
            "real dataset still passes"
        );
    }

    #[test]
    fn require_manifest_does_not_double_flag_a_read_error() {
        // An absent manifest that already carries a `ManifestReadError` (head /
        // read failed) is already a fatal, classified failure — requiring a
        // manifest here must not add a second, redundant failure.
        let mut v = ManifestVerification::legacy();
        v.legacy_run = false;
        v.failures.push(Failure::ManifestReadError {
            detail: "permission denied".into(),
        });
        v.recompute_passed();

        v.require_manifest_present("exports/orders/");

        assert!(
            matches!(v.failures.as_slice(), [Failure::ManifestReadError { .. }]),
            "must leave the existing read-error verdict alone, got: {:?}",
            v.failures
        );
    }

    // ── graded verify layer (--depth) ───────────────────────────────────

    #[test]
    fn light_depth_skips_part_reconcile_even_when_a_part_is_missing() {
        // A manifest declaring a part that is NOT on disk. At `Full`/`Sample`
        // this is a fatal `PartMissing`; at `Light` the `list_prefix` reconcile
        // is skipped entirely, so `parts_verified == 0`, no `ListPrefixError`,
        // and — with `_SUCCESS` consistent and the manifest self-consistent —
        // the verdict still passes. Light certifies the manifest + marker, not
        // the parts.
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        // Deliberately write NEITHER part — only manifest.json + _SUCCESS.
        write_dataset(dir.path(), &m, &[]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Light).unwrap();
        assert_eq!(v.depth_level, "light");
        assert_eq!(
            v.parts_verified, 0,
            "light skips the listing — no part is ever verified"
        );
        assert_eq!(
            v.parts_failed, 0,
            "no part reconcile means no part failures"
        );
        assert!(
            !v.failures.iter().any(|f| matches!(
                f,
                Failure::PartMissing { .. } | Failure::ListPrefixError { .. }
            )),
            "light must not surface part or list failures, got: {:?}",
            v.failures
        );
        assert!(
            v.success_marker_consistent,
            "_SUCCESS is still checked at light depth"
        );
        assert!(v.manifest_self_consistent);
        assert!(
            v.passed,
            "manifest + _SUCCESS are consistent, so a light pass certifies it"
        );
    }

    #[test]
    fn light_depth_never_lists_so_a_list_failure_cannot_trip() {
        // Even with a destination whose `list_prefix` always errors, a light
        // pass succeeds: it never calls `list_prefix`, so no `ListPrefixError`.
        // This is the direct contrast to `list_failure_cannot_certify_parts…`
        // (which runs at Full and *does* fail on the list error).
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(vec![part(0, 3, 3, "xxh3:0")], ManifestStatus::Success);
        write_dataset(dir.path(), &m, &[("part-000000.parquet", b"abc")]);
        let dest = ListFails(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Light).unwrap();
        assert_eq!(v.depth_level, "light");
        assert!(
            !v.failures
                .iter()
                .any(|f| matches!(f, Failure::ListPrefixError { .. })),
            "light never lists, so a failing list_prefix cannot surface, got: {:?}",
            v.failures
        );
        assert_eq!(v.parts_verified, 0);
        assert!(v.passed, "manifest + _SUCCESS consistent → light passes");
    }

    #[test]
    fn sample_depth_runs_part_reconcile_like_full() {
        // `Sample` runs every section `verify_at_destination` owns (1-5) — the
        // Form B value re-read it skips lives in the *caller*, not here. So a
        // missing part is a fatal `PartMissing` at Sample, identical to Full.
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]); // part 2 missing
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Sample).unwrap();
        assert_eq!(v.depth_level, "sample");
        assert_eq!(v.parts_verified, 1);
        assert_eq!(v.parts_failed, 1);
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { part_id: 2, .. })),
            "sample reconciles parts just like full, got: {:?}",
            v.failures
        );
    }

    #[test]
    fn depth_level_is_stamped_on_every_verdict_shape() {
        // The depth label rides the verdict even on the early-return shapes
        // (legacy / no manifest), so a consumer always sees how deep the pass
        // went.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path()); // empty prefix → legacy
        for depth in [
            ValidateDepth::Light,
            ValidateDepth::Sample,
            ValidateDepth::Full,
        ] {
            let v = verify_at_destination(&dest, "", depth).unwrap();
            assert!(v.legacy_run, "empty prefix is the legacy shape");
            assert_eq!(
                v.depth_level,
                depth.label(),
                "legacy verdict must still carry its depth label"
            );
        }
    }

    #[test]
    fn error_code_is_stable_and_distinct_per_variant() {
        // Each variant maps to its documented `RIVET_VERIFY_*` code. A
        // regression guard: renaming a code is a silent break for any CI gate
        // keying off it.
        let cases: &[(Failure, &str)] = &[
            (
                Failure::PartMissing {
                    part_id: 1,
                    path: "p".into(),
                },
                "RIVET_VERIFY_PART_MISSING",
            ),
            (
                Failure::PartSizeMismatch {
                    part_id: 1,
                    path: "p".into(),
                    expected: 1,
                    actual: 2,
                },
                "RIVET_VERIFY_PART_SIZE_MISMATCH",
            ),
            (
                Failure::PartChecksumMismatch {
                    part_id: 1,
                    path: "p".into(),
                    expected: "a".into(),
                    actual: "b".into(),
                },
                "RIVET_VERIFY_PART_CHECKSUM_MISMATCH",
            ),
            (
                Failure::SuccessMarkerMalformed {
                    body_preview: "x".into(),
                },
                "RIVET_VERIFY_SUCCESS_MALFORMED",
            ),
            (
                Failure::SuccessMarkerStale {
                    marker_fingerprint: "a".into(),
                    manifest_fingerprint: "b".into(),
                },
                "RIVET_VERIFY_SUCCESS_STALE",
            ),
            (
                Failure::ManifestSelfInconsistent { detail: "d".into() },
                "RIVET_VERIFY_MANIFEST_INCONSISTENT",
            ),
            (
                Failure::ManifestReadError { detail: "d".into() },
                "RIVET_VERIFY_MANIFEST_READ_ERROR",
            ),
            (
                Failure::SuccessMarkerReadError { detail: "d".into() },
                "RIVET_VERIFY_SUCCESS_READ_ERROR",
            ),
            (
                Failure::ListPrefixError { detail: "d".into() },
                "RIVET_VERIFY_LIST_ERROR",
            ),
            (
                Failure::UntrackedObject {
                    key: "k".into(),
                    size_bytes: 1,
                },
                "RIVET_VERIFY_UNTRACKED_OBJECT",
            ),
            (
                Failure::ContentVerificationUnmet {
                    size_only: 1,
                    total: 2,
                },
                "RIVET_VERIFY_CONTENT_UNMET",
            ),
            (
                Failure::ManifestRequiredButAbsent { prefix: "p".into() },
                "RIVET_VERIFY_MANIFEST_REQUIRED",
            ),
            (
                Failure::ValueChecksumMismatch {
                    detail: "flip".into(),
                },
                "RIVET_VERIFY_VALUE_CHECKSUM",
            ),
            (
                Failure::CdcPositionViolation {
                    detail: "gap".into(),
                },
                "RIVET_VERIFY_CDC_POSITION",
            ),
        ];
        for (failure, code) in cases {
            assert_eq!(&failure.error_code(), code, "code for {failure:?}");
            assert!(
                failure.error_code().starts_with("RIVET_VERIFY_"),
                "every code shares the RIVET_VERIFY_ prefix"
            );
        }
    }

    #[test]
    fn cdc_position_violation_is_verified_wrong_not_could_not_verify() {
        // #5 / #104 bughunt: a CDC `__pos` continuity violation (a gap/duplicate in
        // the exported change stream) is VERIFIED-WRONG — data-integrity, exit 3,
        // the same class as a value-checksum mismatch — NOT a could-not-verify I/O
        // error (exit 1). This pins the classification so a future edit that made it
        // operational (folding it back into hard_failures → exit 1, the pre-fix
        // behaviour) goes RED.
        let viol = Failure::CdcPositionViolation {
            detail: "export 'x': pos gap 5 -> 8".into(),
        };
        assert!(
            !viol.is_could_not_verify(),
            "a __pos violation is verified-wrong, not could-not-verify"
        );
        assert!(viol.is_fatal(), "a __pos violation fails the verdict");

        let mut v = ManifestVerification::empty();
        v.passed = false;
        v.failures.push(viol);
        assert!(
            v.has_verified_wrong_failure(),
            "a verdict carrying a __pos violation must classify as verified-wrong (exit 3)"
        );

        // The read-error siblings are the OTHER side of the same axis: could-not-
        // verify, so a verdict whose ONLY failure is one of them is NOT verified-wrong.
        let mut op = ManifestVerification::empty();
        op.passed = false;
        op.failures.push(Failure::ManifestReadError {
            detail: "permission denied".into(),
        });
        assert!(
            !op.has_verified_wrong_failure(),
            "a read error alone is could-not-verify (exit 1), not verified-wrong"
        );
    }
}
