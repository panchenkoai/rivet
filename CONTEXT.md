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
