# Rivet — Domain Language

Shared vocabulary for Rivet's type-support and export domain. Use these terms
exactly in code, comments, ADRs, and reviews so the type pipeline reads the same
everywhere. General programming concepts are deliberately excluded.

## Type pipeline

**RivetType**:
The canonical semantic type of a column — the single place a column's *meaning*
is decided, before any Arrow type exists. Every source driver maps its native
type into exactly one `RivetType` first. Carries precision/scale, timezone, list
inner type, or an `Unsupported` reason.
_Avoid_: logical type, internal type, canonical type.

**TypeMapping**:
The provenance row for one column: `source_native_type → RivetType → arrow_type`
plus fidelity, nullability, and warnings. What the type-report prints and what
the policy layer validates.
_Avoid_: column mapping, type record.

**Fidelity**:
How faithfully a column survives to disk — `Exact`, `Compatible`,
`LogicalString`, `Lossy`, `Unsupported`, ordered best-to-worst. Strict mode
rejects `Lossy`/`Unsupported`.
_Avoid_: precision level, quality.

**Interchange**:
The target-neutral file layer (Parquet/CSV). Physical Arrow types chosen for
cross-engine fidelity; never specialized per target. Distinct from
materialization.
_Avoid_: output format, file layer.

## Target materialization

**ExportTarget**:
A downstream warehouse Rivet resolves column types against — `DuckDb`,
`BigQuery`, `Snowflake`, `ClickHouse`. A closed, in-tree, contract-tested set;
chosen at runtime from `--target X`, one per run.
_Avoid_: destination (that is the file/object sink), warehouse, sink.

**Target resolver**:
The module that turns a `RivetType` (+ precision, + logical metadata) into what a
column becomes in an `ExportTarget`. Pure, total, infallible. Dispatches on
`RivetType` — never on the physical Arrow type. Entry points
`ExportTarget::resolve_table(&[TypeMapping])` (dominant) and `resolve_column`.
_Avoid_: compat checker, target mapper.

**TargetColumnSpec**:
The uniform per-column, per-target output row: `column_name`, `target_type`,
`autoload_type`, `status`, `note`, `cast_sql`. One shape across all targets so the
type-report table and `--json` stay stable; an unmappable column is a
`status: Fail` row, not an error.
_Avoid_: target compat, column result.

**TargetInput**:
The borrowed subset a resolver is allowed to read — `column_name`, `rivet_type`,
`arrow_type` (precision only), `fidelity`. Built from a `TypeMapping` via `From`.
Keeps "dispatch on RivetType, Arrow for precision only" honest.

**Autoload type vs Native type**:
*Autoload* is the type a generic Parquet reader infers without casts (JSON →
`VARCHAR`). *Native* is the recommended target type for full semantic fidelity
(JSON → `JSON`/`VARIANT`). The gap is closed by L5 materialization SQL
(`cast_sql`), not by the interchange file.

**Materialization (L4/L5)**:
Making target-native types real — `TargetColumnSpec` (L4) plus the DDL / cast SQL
an operator or future direct loader runs (L5). The stateful **direct load**
(Epic 14) reuses the same resolver behind a `TargetLoader: TargetResolver` split,
introduced only when the first real connection lands.
_Avoid_: loading, ingestion.

**Manifest reconciliation**:
The single pure walk that compares a `RunManifest` against a destination
listing — per committed part `Present` / `Missing` / `SizeMismatch` /
`ChecksumMismatch`, plus untracked surplus (`reconcile_manifest_against_listing`).
The listing yields `{key, size_bytes, content_md5}`; when both the manifest and
the listing carry an MD5 the content is verified too — with **no download**,
since GCS/S3 surface the digest in the listing. Both consumers are thin
mappers — destination verify (`Presence → Failure`) and chunked resume
(`Presence → ResumeDecision`). _Avoid_: diff, compare, validate (overloaded).

**Content MD5**:
The part-body MD5 rivet computes locally before upload and records in the
manifest (base64, GCS `md5Hash` encoding). Destination verification compares it
to the object's listing metadata to confirm content without downloading. Backend
coverage (verified live unless noted): **GCS** base64 `md5Hash` (always);
**S3** single-part ETag hex (always; multipart composite `<hash>-<N>` → size-only);
**Azure** md5 for parts rivet uploads as a single `Put Blob` (Azure auto-computes
`Content-MD5` only then — verified live), size-only for parts large enough to
stream as `Put Block List`; **local FS** size-only. The one-shot vs stream
threshold (`cloud.rs`) is what makes Azure md5 work for small parts. Encodings are normalised
to raw digest bytes (`md5_digest_bytes`) so base64 and hex of the same digest
compare equal. _Avoid_: checksum, hash (use for the xxh3 `content_fingerprint`).

**Verdict coverage**:
How much a `--validate` pass actually certified, carried by the `ManifestVerification`
counters rather than a label: `parts_verified` with `parts_md5_verified` as the
content-checked subset (the rest size-only). Content is verified pre-upload and,
at the destination, via the free listing MD5 — never by re-download. `passed` is
**derived** (`manifest_found` and no fatal failure — advisory `UntrackedObject`
doesn't count), computed once via `recompute_passed`, not hand-flipped per site.
_Avoid_: integrity level (removed — a single-variant enum that duplicated the
md5/size-only counters), validation depth.
