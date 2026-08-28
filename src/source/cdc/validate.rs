//! Post-hoc `__pos` continuity check for CDC outputs.
//!
//! Reads a CDC run's parts back from the destination (in manifest order) and
//! verifies the `__pos` column is **monotonically non-decreasing** — every change
//! stayed in source-log order, no part was reordered, and no part boundary
//! overlaps the previous one. That proves *ordering + no-duplication + no-reorder*
//! integrity of what landed in the bucket.
//!
//! It deliberately does **not** claim "no transaction was missed": log positions
//! are byte offsets (MySQL) / LSNs (PostgreSQL, SQL Server), not a dense counter,
//! so a skipped transaction is indistinguishable from a large one. Completeness at
//! the *seam between runs* is instead guaranteed by the commit-boundary checkpoint
//! (and, for PostgreSQL, the slot only advancing after a durable write).

use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{MANIFEST_FILENAME, RunManifest, join_key};

/// Outcome of a `__pos` continuity check over one CDC run's output.
#[derive(Default)]
pub(crate) struct PositionCheck {
    pub parts: usize,
    pub rows: usize,
    pub first: Option<String>,
    pub last: Option<String>,
    /// Out-of-order / unparseable `__pos` findings — empty ⇒ clean.
    pub violations: Vec<String>,
}

impl PositionCheck {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// A `__pos` value normalised to a comparable key. Within a run every position is
/// the same engine's shape, so cross-variant comparison never happens (the derived
/// `Ord`'s discriminant tie-break is irrelevant).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PosKey {
    /// MySQL binlog: the file's numeric ORDINAL, then pos. Stored as the
    /// ordinal (not the filename) because a lexical filename compare inverts at
    /// the 999999 → 1000000 suffix-width rollover — the exact break the engine's
    /// `commit_past_bound` parses the ordinal to avoid (bug hunt 2026-08-08).
    /// A binlog name with no numeric suffix keeps its filename via `BinlogRaw`
    /// so two such never compare equal by accident.
    Binlog(u64, u64),
    /// A binlog file whose name has no parseable ordinal — ordered by name,
    /// then pos. Never mixed with `Binlog` in a real run (one server, one
    /// basename), but kept distinct so the fallback is not silently equal.
    BinlogRaw(String, u64),
    /// PostgreSQL LSN `hi/lo` (hex) → a single u64.
    PgLsn(u64),
    /// SQL Server LSN — fixed-width hex, lexically comparable.
    Lsn(String),
}

/// Parse one `__pos` JSON string (`{"file":…,"pos":…}` or `{"lsn":…}`) into a
/// comparable key.
fn parse_pos(s: &str) -> Option<PosKey> {
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    if let (Some(file), Some(pos)) = (v.get("file").and_then(|x| x.as_str()), v.get("pos")) {
        let pos = pos.as_u64()?;
        // Order by the numeric ordinal (shared parser with the engine's
        // commit_past_bound), falling back to the raw name only when there is
        // no suffix to parse.
        return Some(match crate::source::mysql::cdc::binlog_file_ordinal(file) {
            Some(ord) => PosKey::Binlog(ord, pos),
            None => PosKey::BinlogRaw(file.to_string(), pos),
        });
    }
    // MongoDB change-stream position `{"_data": <hex>, "rt": <hex>}`: `_data` is
    // the resume-token keystring, order-preserving under lexical compare (oplog
    // order). Key on it — a `Utf8` string key, like a generic LSN. Without this
    // arm `rivet validate --depth full` failed on every healthy mongo CDC output.
    if let Some(data) = v.get("_data").and_then(|x| x.as_str()) {
        return Some(PosKey::Lsn(data.to_string()));
    }
    let lsn = v.get("lsn").and_then(|x| x.as_str())?;
    if let Some((hi, lo)) = lsn.split_once('/') {
        let hi = u64::from_str_radix(hi, 16).ok()?;
        let lo = u64::from_str_radix(lo, 16).ok()?;
        // A PostgreSQL LSN's low half is 32 bits. A wider one is malformed, and
        // folding it in anyway makes a WRONG comparable key out of nonsense — the
        // ordering check would then compare positions that mean nothing and report
        // either a phantom backwards jump or a false clean. `None` routes it to the
        // `unparseable __pos` violation instead, which is what the caller already
        // knows how to say.
        //
        if lo >> 32 != 0 {
            return None;
        }
        // `+`, not `|`. With the guard above the halves are provably disjoint, so
        // the two produce the same number — but `|` vs `^` is then EQUIVALENT and
        // unkillable, while `+` vs `*` is not (`0/FFFFFFFF` and `1/0` both collapse
        // to 0 under `*`, so the ordering assertions catch it). Same value, a graded
        // operator instead of an unkillable one.
        return Some(PosKey::PgLsn((hi << 32) + lo));
    }
    Some(PosKey::Lsn(lsn.to_string()))
}

/// Read the `__pos` column (Utf8) out of a Parquet part body. The body is staged
/// to a temp file so the parquet reader's `File` `ChunkReader` can seek the footer.
fn read_pos_column(body: Vec<u8>) -> Result<Vec<String>> {
    use std::io::Write;

    use arrow::array::{Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut tmp = tempfile::NamedTempFile::new()?;
    tmp.write_all(&body)?;
    tmp.flush()?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(tmp.reopen()?)?.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let idx = batch
            .schema()
            .index_of("__pos")
            .map_err(|_| anyhow::anyhow!("part has no __pos column — not a CDC output?"))?;
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("__pos column is not Utf8"))?;
        for i in 0..col.len() {
            if col.is_valid(i) {
                out.push(col.value(i).to_string());
            }
        }
    }
    Ok(out)
}

/// Verify `__pos` is monotonically non-decreasing across a CDC run's Parquet parts
/// at `dest`/`prefix`. Returns the range covered and any ordering violations.
pub(crate) fn check_positions(dest: &dyn Destination, prefix: &str) -> Result<PositionCheck> {
    let manifest_key = join_key(prefix, MANIFEST_FILENAME);
    // The canonical manifest.json is ABSENT on a daemon/crashed CDC prefix (the
    // per-roll durability write emits only the run-unique copy). Do NOT report a
    // false-clean OK over ZERO parts (roast 2026-08-09, #173): fall back to the
    // run-unique copies (manifest-<run_id>.json) — the same set `load` reconciles
    // — so validate actually checks the parts that are physically present. Only
    // an empty result when there is NO manifest of any kind.
    // The run-unique COPIES first, and the canonical only when there are none.
    //
    // The canonical `manifest.json` is a LATEST-RUN POINTER, and an `until_current`
    // cycle that finds no changes legitimately declares zero parts — which is the
    // steady state for a scheduled capture, and for most tables of a `tables:`
    // multiplex most of the time. Reading it alone therefore answered "what did the
    // last run deliver", while every field printed said "the prefix". MEASURED: a
    // run captured 3 changes and `validate --depth full` verified 1 part; ONE idle
    // cycle later the canonical read `parts: []` and the same command reported
    // `PASSED, 0 parts verified` — then still PASSED after the parquet was
    // overwritten with junk, and again after it was DELETED, with the copy declaring
    // it sitting unread beside it.
    //
    // The copies are the set `load` reconciles, so validate now asks the same
    // question the loader does. They do not overlap (one per run) and the canonical
    // duplicates one of them, so preferring the copies also avoids double-counting.
    // The canonical stays the fallback for prefixes written before copies existed.
    let mut manifests: Vec<RunManifest> = Vec::new();
    for m in dest.list_prefix(prefix)? {
        // THIS prefix's own copies, not a nested leg's. The listing recurses on both
        // the local and the cloud destination, so an `initial: snapshot` export's
        // `<prefix>/snapshot/manifest-<run_id>.json` was swept up here — and its
        // `parts[].path` is relative to `snapshot/`, so validate looked for those
        // parts at the CDC root and reported `cdc __pos check could not complete: No
        // such file or directory`, exit 1, on a correct export. MEASURED: the
        // documented production shape (`cdc: { initial: snapshot }`) failed
        // `rivet validate --depth full` outright, and moving that one file aside made
        // the same command exit 0. It broke every `rivet validate && deploy` gate.
        //
        // A separator in the key RELATIVE TO THIS PREFIX means a nested leg; the
        // copies this prefix owns sit directly under it.
        //
        // Relative, because `list_prefix` returns FULL keys — `cdc/manifest-r1.json`,
        // not `manifest-r1.json`. The first cut of this guard tested the whole key,
        // so it skipped EVERY copy including this prefix's own, and `check_positions`
        // fell through to the canonical `manifest.json`. On the prefix that has none
        // — a daemon or crashed CDC run, where only the run-unique copies exist —
        // that is a false-clean `PASSED, 0 parts verified`, which is exactly the
        // #173 defect this fallback was written to remove. Nothing caught it: the
        // broken guard survived the lib suite AND all 106 live CDC tests, because
        // every one of them leaves a canonical manifest behind.
        let rel = m
            .key
            .strip_prefix(prefix)
            .unwrap_or(&m.key)
            .trim_start_matches('/');
        if rel.contains('/') {
            continue;
        }
        let base = rel;
        if crate::manifest::is_run_unique_manifest_name(base) {
            manifests.push(serde_json::from_slice::<RunManifest>(&dest.read(&m.key)?)?);
        }
    }
    if manifests.is_empty() && dest.head(&manifest_key)?.is_some() {
        manifests.push(serde_json::from_slice(&dest.read(&manifest_key)?)?);
    }
    if manifests.is_empty() {
        return Ok(PositionCheck::default());
    }

    // Check each manifest's parts INDEPENDENTLY: a run-unique copy is one run's
    // full manifest, and __pos is one monotonic sequence WITHIN a run — not
    // across runs — so concatenating parts from different runs would forge false
    // backwards-violations. Sum parts/rows; collect every run's violations.
    let (mut parts, mut rows) = (0usize, 0usize);
    let (mut first, mut last): (Option<String>, Option<String>) = (None, None);
    let mut violations = Vec::new();
    for manifest in &manifests {
        let mut items: Vec<(u32, String)> = Vec::new();
        for part in &manifest.parts {
            if part.status != crate::manifest::PartStatus::Committed {
                continue;
            }
            // A DECLARED part that is missing or undecodable is VERIFIED-WRONG
            // (a violation in the verdict), not could-not-verify: the manifest
            // promises the part and the destination refutes the promise —
            // round-7 measured the old `?` propagation grading a permanently
            // deleted part as retryable exit 1 while the headline said PASSED.
            let body = match dest.read(&join_key(prefix, &part.path)) {
                Ok(b) => b,
                Err(e) => {
                    violations.push(format!(
                        "declared part '{}' (run {}) is missing or unreadable at the \
                         destination: {e:#}",
                        part.path, manifest.run_id
                    ));
                    continue;
                }
            };
            match read_pos_column(body) {
                Ok(ps) => items.extend(ps.into_iter().map(|p| (part.part_id, p))),
                Err(e) => violations.push(format!(
                    "declared part '{}' (run {}) does not decode as the Parquet the \
                     manifest committed: {e:#}",
                    part.path, manifest.run_id
                )),
            }
        }
        let (f, l, v) = check_order(&items);
        parts += manifest.parts.len();
        rows += items.len();
        if first.is_none() {
            first = f;
        }
        if l.is_some() {
            last = l;
        }
        violations.extend(v);
    }
    Ok(PositionCheck {
        parts,
        rows,
        first,
        last,
        violations,
    })
}

/// Pure half: the monotonicity invariant over `(part_id, __pos)` in part→row
/// order, lifted out of the IO so it is testable without a destination.
fn check_order(items: &[(u32, String)]) -> (Option<String>, Option<String>, Vec<String>) {
    let mut prev: Option<PosKey> = None;
    let (mut first, mut last, mut violations) = (None, None, Vec::new());
    for (part_id, raw) in items {
        let Some(key) = parse_pos(raw) else {
            violations.push(format!("part {part_id}: unparseable __pos {raw:?}"));
            continue;
        };
        first.get_or_insert_with(|| raw.clone());
        if prev.as_ref().is_some_and(|p| key < *p) {
            violations.push(format!(
                "part {part_id}: __pos went backwards at {raw:?} (out of log order)"
            ));
        }
        prev = Some(key);
        last = Some(raw.clone());
    }
    (first, last, violations)
}

#[cfg(test)]
mod v016_checkpoint_compat {
    //! A checkpoint written by rivet 0.16 must still LOAD and still yield the
    //! same resume coordinates.
    //!
    //! `tests/compat_gate.rs` freezes the same three files but reads them with a
    //! bare `serde_json::Value` and asks only "does key X exist" — one key per
    //! engine, and `pos` is never asserted at all. That cannot fail when rivet
    //! changes: the fixture is what it inspects. It could not, as written —
    //! `Position` is `pub(crate)`, so an integration test has no access to the
    //! real loader. Hence this module, beside the code that owns the type.
    //!
    //! The two real consumers are both exercised: `Position::load` (does the
    //! file still deserialize) and `parse_pos` (are the coordinates still
    //! extracted, and as the right variant). Rename `file` to `binlog_file`,
    //! or add a required field to the persisted shape, and this goes red where
    //! the shape-only check stays green — while a user's 0.16 checkpoint
    //! silently re-anchors and skips every change since it was written.

    use super::{PosKey, parse_pos};
    use crate::source::cdc::Position;

    const DIR: &str = "tests/fixtures/compat/v0.16";

    fn load(name: &str) -> Position {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(DIR)
            .join(name);
        Position::load(&path)
            .unwrap_or_else(|e| panic!("{name}: a v0.16 checkpoint must still load: {e:#}"))
            .unwrap_or_else(|| panic!("{name}: loader reported the fixture as ABSENT"))
    }

    /// The five decisions a mutation run over this PURE file found ungraded.
    ///
    /// `validate.rs` does no I/O of its own beyond a `Destination` trait a test can
    /// supply, so every mutant in it is offline-reachable and a survivor is a plain
    /// gap — no live-only argument available. 92 mutants, 80 caught, 5 missed.
    #[test]
    fn the_position_verdict_the_order_check_and_the_lsn_key_all_answer_for_themselves() {
        use super::{PositionCheck, check_order, parse_pos};

        // THE VERDICT. `is_ok -> true` makes every `rivet validate` pass whatever it
        // found; `-> false` makes every one fail. Nothing graded the accessor that
        // turns the whole check into a yes or no.
        assert!(
            PositionCheck::default().is_ok(),
            "a check with no violations is OK — `-> false` fails every correct export"
        );
        assert!(
            !PositionCheck {
                violations: vec!["part 0: __pos went backwards".into()],
                ..Default::default()
            }
            .is_ok(),
            "a violation means NOT ok — `-> true` reports every backwards jump as a \
             clean run, which is the only thing this check produces"
        );

        // THE ORDER CHECK. `<` -> `<=` flags EQUAL positions as backwards, and equal
        // is the normal case: every row of one transaction shares its commit
        // position and is disambiguated by `__seq`. That mutant turns each
        // multi-row transaction into a violation.
        let pos = |lsn: &str| format!(r#"{{"lsn":"{lsn}"}}"#);
        let (_, _, v) = check_order(&[(0, pos("0/10")), (0, pos("0/10")), (0, pos("0/20"))]);
        assert!(
            v.is_empty(),
            "two rows at the SAME position are one transaction, not a backwards \
             jump: {v:?}"
        );
        let (first, last, v) = check_order(&[(0, pos("0/20")), (0, pos("0/10"))]);
        assert_eq!(v.len(), 1, "a strictly backwards position IS a violation");
        assert!(v[0].contains("went backwards"));
        assert_eq!(first.as_deref(), Some(pos("0/20").as_str()));
        assert_eq!(last.as_deref(), Some(pos("0/10").as_str()));

        // An unparseable value is reported, not skipped — a run whose every position
        // is junk must not read as clean.
        let (_, _, v) = check_order(&[(7, "not json".into())]);
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("part 7") && v[0].contains("unparseable"));

        // THE LSN KEY. The halves must not bleed into each other: `1/0` is one full
        // 32-bit step above `0/FFFFFFFF`, and `(hi << 32) ^ lo` only differs from
        // `|` when the low half overflows 32 bits — which is malformed input, now
        // refused rather than folded into a meaningless key.
        assert!(parse_pos(&pos("0/FFFFFFFF")) < parse_pos(&pos("1/0")));
        assert!(parse_pos(&pos("0/2")) > parse_pos(&pos("0/1")));
        assert_eq!(
            parse_pos(&pos("1/1FFFFFFFFF")),
            None,
            "a low half wider than 32 bits is not a PostgreSQL LSN; folding it in \
             silently produces an ordering key that means nothing"
        );
    }

    /// The nested-leg skip must actually skip — measured ungraded by BOTH suites.
    ///
    /// `check_positions` lists the prefix RECURSIVELY, so an `initial: snapshot`
    /// export's `<prefix>/snapshot/manifest-<run_id>.json` was swept up with the CDC
    /// leg's own copies. Its `parts[].path` is relative to `snapshot/`, so validate
    /// looked for those parts at the CDC root and reported `could not complete: No
    /// such file or directory`, exit 1, on a correct export — breaking every
    /// `rivet validate && deploy` gate on the documented production shape.
    ///
    /// The guard was added earlier this session and shipped with nothing grading it:
    /// deleting it survives the lib suite AND all 106 live CDC tests (both measured
    /// 2026-08-27). A guard nothing can fail is a guard nobody can trust to still be
    /// there — which is the whole point of this file's mutation pass.
    #[test]
    fn a_nested_snapshot_legs_manifest_is_not_read_as_this_prefixs_own() {
        use std::sync::Arc;

        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("dest root");
        let root = dir.path().join("cdc");
        std::fs::create_dir_all(root.join("snapshot")).expect("nested leg");

        // One parquet with a monotonic `__pos`, at the CDC root.
        let write_part = |at: &std::path::Path, vals: &[&str]| {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "__pos",
                DataType::Utf8,
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vals.to_vec()))],
            )
            .expect("batch");
            let f = std::fs::File::create(at).expect("part file");
            let mut w = ArrowWriter::try_new(f, schema, None).expect("writer");
            w.write(&batch).expect("write");
            w.close().expect("close");
        };
        write_part(
            &root.join("cdc-000000.parquet"),
            &[r#"{"lsn":"0/10"}"#, r#"{"lsn":"0/20"}"#],
        );

        // Built from the type via `RunManifest::for_test`, not hand-written JSON.
        // The first draft WAS hand-written and took six rounds of `missing field`
        // before it deserialized; a fixture that drifts from the struct fails on
        // parse rather than on the property it guards.
        let manifest = |part: &str| {
            serde_json::to_string(&crate::manifest::RunManifest::for_test("r1", &[(part, 2)]))
                .expect("serialize the manifest")
        };

        std::fs::write(
            root.join("manifest-r1.json"),
            manifest("cdc-000000.parquet"),
        )
        .expect("this leg's own copy");
        // The nested leg's copy — its part path is relative to `snapshot/`, so a
        // reader that adopts it looks for `<cdc-root>/snap-000000.parquet`, which
        // does not exist. That is the exit-1 the guard exists to prevent.
        std::fs::write(
            root.join("snapshot").join("manifest-r9.json"),
            manifest("snap-000000.parquet"),
        )
        .expect("nested leg's copy");

        // A CANONICAL manifest beside the run-unique copy. The fallback reads
        // `manifests.is_empty() && canonical exists`; with `&&` -> `||` the canonical
        // is adopted TOO and the same run's parts are counted twice. Without this
        // file present both operators agree and the mutant survives — measured.
        std::fs::write(
            root.join(crate::manifest::MANIFEST_FILENAME),
            manifest("cdc-000000.parquet"),
        )
        .expect("canonical manifest");

        let dest =
            crate::destination::local::LocalDestination::new(&crate::config::DestinationConfig {
                destination_type: crate::config::DestinationType::Local,
                path: Some(dir.path().to_string_lossy().into_owned()),
                ..Default::default()
            })
            .expect("local destination");
        let got = super::check_positions(&dest, "cdc")
            .expect("a correct export must not fail validation because a nested leg exists");
        assert_eq!(
            got.parts, 1,
            "only THIS prefix's manifest may be adopted; counting the nested leg's \
             makes validate read parts that were never written here"
        );
        assert_eq!(got.rows, 2);
        assert!(
            got.violations.is_empty(),
            "a monotonic single run has no backwards jump: {:?}",
            got.violations
        );
    }

    #[test]
    fn v016_mysql_checkpoint_still_yields_its_binlog_coordinates() {
        let p = load("mysql_cdc.ckpt");
        let raw = serde_json::to_string(&p.0).expect("serialize");
        assert_eq!(
            parse_pos(&raw),
            Some(PosKey::Binlog(4, 150_838_422)),
            "the 0.16 MySQL checkpoint must still resume at the SAME file+pos;              a renamed or newly-required field re-anchors the stream instead"
        );
    }

    #[test]
    fn v016_pg_and_mssql_checkpoints_still_yield_their_lsns() {
        let pg = load("pg_cdc.ckpt");
        let raw = serde_json::to_string(&pg.0).expect("serialize");
        // PG's `0/1A2B3C4D` is parsed into a single u64 (hi<<32 | lo).
        assert_eq!(
            parse_pos(&raw),
            Some(PosKey::PgLsn(0x1A2B_3C4D)),
            "the 0.16 PostgreSQL checkpoint must still resume at the same LSN"
        );

        let ms = load("mssql_cdc.ckpt");
        let raw = serde_json::to_string(&ms.0).expect("serialize");
        assert_eq!(
            parse_pos(&raw),
            Some(PosKey::Lsn("0000003400001A2B0003".into())),
            "the 0.16 SQL Server checkpoint must still resume at the same LSN"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pos_ordering_per_engine() {
        // MySQL: same file, increasing pos.
        assert!(
            parse_pos(r#"{"file":"binlog.000046","pos":100}"#).unwrap()
                < parse_pos(r#"{"file":"binlog.000046","pos":200}"#).unwrap()
        );
        // MySQL: file rotation dominates.
        assert!(
            parse_pos(r#"{"file":"binlog.000046","pos":999}"#).unwrap()
                < parse_pos(r#"{"file":"binlog.000047","pos":4}"#).unwrap()
        );
        // ROLLOVER (bug hunt 2026-08-08): binlog.1000000 comes AFTER
        // binlog.999999 ordinally, though "1000000" < "999999" lexically. The
        // old String-keyed PosKey ordered these backwards, so validate's
        // monotonicity check misjudged a rotation across the width boundary.
        assert!(
            parse_pos(r#"{"file":"binlog.999999","pos":10}"#).unwrap()
                < parse_pos(r#"{"file":"binlog.1000000","pos":1}"#).unwrap(),
            "ordinal order: .1000000 is newer than .999999 despite lexical order"
        );
        // A no-suffix name falls back to BinlogRaw and never equals an ordinal key.
        assert_ne!(
            parse_pos(r#"{"file":"binlog","pos":1}"#).unwrap(),
            parse_pos(r#"{"file":"binlog.000001","pos":1}"#).unwrap()
        );
        // PostgreSQL LSN: hi/lo hex parsed numerically (not string — "9" > "1A" as
        // strings would be wrong).
        assert!(parse_pos(r#"{"lsn":"3C/9"}"#).unwrap() < parse_pos(r#"{"lsn":"3C/1A"}"#).unwrap());
        assert!(
            parse_pos(r#"{"lsn":"3C/FFFFFFFF"}"#).unwrap()
                < parse_pos(r#"{"lsn":"3D/0"}"#).unwrap()
        );
    }

    #[test]
    fn roast_mongo_pos_parses_and_orders_by_data_keystring() {
        // MongoDB `__pos` is `{"_data": <hex>, "rt": <hex>}` — before the mongo
        // arm parse_pos returned None and `validate` hard-failed every healthy
        // mongo CDC output. Key on `_data`, the order-preserving resume keystring.
        let a = r#"{"_data":"826A4E0001","rt":"0eFFFFFF"}"#;
        let b = r#"{"_data":"826A4E0002","rt":"0e00"}"#;
        assert!(parse_pos(a).is_some(), "mongo __pos must parse");
        // Ordering follows `_data`, NOT `rt`: here b's `_data` > a's `_data` even
        // though b's `rt` sorts before a's — the exact mis-order a rt-first
        // `__pos` would produce.
        assert!(parse_pos(a).unwrap() < parse_pos(b).unwrap());
    }

    #[test]
    fn malformed_pos_is_none() {
        assert!(parse_pos("not json").is_none());
        assert!(parse_pos(r#"{"nope":1}"#).is_none());
    }

    #[test]
    fn check_order_flags_a_backwards_jump() {
        let items = vec![
            (1, r#"{"lsn":"3C/10"}"#.into()),
            (2, r#"{"lsn":"3C/05"}"#.into()), // earlier LSN in a later part
        ];
        let (.., violations) = check_order(&items);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("backwards"));
    }

    #[test]
    fn check_order_clean_when_monotonic() {
        let items = vec![
            (1, r#"{"file":"b.000001","pos":100}"#.into()),
            (2, r#"{"file":"b.000002","pos":4}"#.into()), // file rotation still forward
        ];
        let (first, last, violations) = check_order(&items);
        assert!(violations.is_empty());
        assert!(first.unwrap().contains("100") && last.unwrap().contains("000002"));
    }
}
