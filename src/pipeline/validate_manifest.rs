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

use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{
    MANIFEST_FILENAME, RunManifest, SUCCESS_FILENAME, join_key, parse_success_marker,
    success_marker_body,
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
            Ok(listing) => Some(reconcile_manifest_against_listing(
                &manifest,
                &listing,
                manifest_dir,
            )),
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
            mode: "batch".to_string(),
            manifest_version: MANIFEST_VERSION,
            run_id: "r".into(),
            export_name: "public.orders".into(),
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

    // ── untracked objects ───────────────────────────────────────────────

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
        ];
        for (failure, code) in cases {
            assert_eq!(&failure.error_code(), code, "code for {failure:?}");
            assert!(
                failure.error_code().starts_with("RIVET_VERIFY_"),
                "every code shares the RIVET_VERIFY_ prefix"
            );
        }
    }
}
