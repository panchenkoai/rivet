# ADR-0014: Target Type Materialization

**Status**: Proposed  
**Date**: 2026-05-26  
**Context**: Rivet v0.7.8 ships a canonical type pipeline (`SourceColumn` → `RivetType` → Arrow/Parquet/CSV) with Parquet field metadata (`rivet.native_type`, `rivet.logical_type`, `rivet.fidelity`). Operators load files into DuckDB, BigQuery, Snowflake, and ClickHouse. Autoload from Parquet infers **physical** types only (e.g. JSON columns appear as `STRING` / `VARCHAR`), while warehouses expose **native** semi-structured and exact numeric types ([BigQuery JSON](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types), [Snowflake semi-structured types](https://docs.snowflake.com/en/sql-reference-data-types), [ClickHouse types](https://clickhouse.com/docs/sql-reference/data-types), [DuckDB types](https://duckdb.org/docs/current/sql/data_types/overview.html)).

Epic 14 already has `ExportTarget::BigQuery` and `rivet check --type-report --target bigquery` mapping Arrow **physical** types to expected warehouse types (`src/types/target.rs`). DuckDB is the most common first consumer of Rivet Parquet in benchmarks and ad-hoc analytics but is not yet a first-class `ExportTarget`. This ADR defines how Rivet separates **interchange** (files) from **materialization** (target-native types at load time) without breaking the v0.7.8 Parquet contract.

Related: [type-mapping.md](../type-mapping.md), Epic 14 in `rivet_roadmap.md`, [ADR-0012](0012-cloud-manifest-contract.md) (manifest/schema fingerprint).

---

## Goals

1. One canonical semantic layer (`RivetType` + fidelity + metadata) for all sources (PostgreSQL, MySQL, …).
2. Predictable **file interchange**: Parquet/CSV values preserved; no silent float fallback for decimals.
3. **Target-aware guidance**: per-column native type, warnings, and optional load SQL for each supported engine.
4. DuckDB as a **reference target** (strong Parquet interop, `JSON` / `UUID` / `UBIGINT`) before cloud warehouses.
5. Extensibility for future direct warehouse load (Epic 14) reusing the same resolver — not a second type system.

## Non-goals

1. Replacing Parquet with N target-specific file formats in v0.8 (one interchange artifact remains default).
2. Automatic type coercion inside Rivet's Parquet writer per target (physical Arrow types stay target-neutral).
3. PostGIS / nested arrays / full PostgreSQL exotic types (tracked separately in the type matrix roadmap).
4. A new top-level `rivet verify` subcommand (ADR-0013: extend `--validate` / type-report semantics instead).
5. Teaching DuckDB/BigQuery to read `rivet.*` metadata keys without operator or generated DDL (not a standard interchange contract).
6. **Databricks** as an `ExportTarget` or materialization matrix column (deferred; Delta/VARIANT overlap with Snowflake/CH patterns — revisit when there is operator demand).

---

## Problem

Three layers are often conflated:

| Layer | Question | Failure mode |
|-------|----------|--------------|
| **Semantic** | What did the source column *mean*? | Lost when everything becomes `Utf8` |
| **Interchange** | What is in the Parquet/CSV file? | Correct bytes, wrong *inferred* type at load |
| **Materialization** | What type should the **target table** use? | `JSON`/`VARIANT` never created; queries need casts |

Rivet today solves semantic + interchange well (e.g. `jsonb` → `Utf8` + `rivet.logical_type=json`, `fidelity=logical_string`). `schema_fingerprint` in the manifest hashes Arrow `Debug` types only — it does **not** include `rivet.*` metadata ([`schema_fingerprint`](../../src/state/schema.rs) design). Downstream engines that read Parquet schema alone therefore cannot recover JSON vs plain text.

Industry tools split the same problem differently:

- **[Sling](https://docs.slingdata.io/concepts/replication/columns)**: generic types (`json`, `decimal`, …) + per-DB `native_type_map` / `general_type_map` ([templates](https://docs.slingdata.io/concepts/replication/templates)); `column_typing` and `columns:` overrides at DDL/load time.
- **[Airbyte](https://docs.airbyte.com/platform/understanding-airbyte/supported-data-types)**: JSON Schema + `airbyte_type`; destinations v2 materialize typed tables (e.g. BigQuery `JSON` for objects, not only `STRING`).

Rivet needs an explicit **materialization** stage analogous to Sling's target DDL + Airbyte's destination typing, while keeping file-first extraction.

---

## Decision

Adopt a **five-layer type pipeline**. Layers L0–L3 are implemented in v0.7.8; L4–L5 are specified here and rolled out incrementally.

```
L0  source_native     ("jsonb", "numeric(18,2)")
      ↓
L1  RivetType         (Json, Decimal { p, s }, …)
      ↓
L2  PhysicalType     (Arrow DataType → Parquet/CSV)
      ↓
L3  TypeManifest      (rivet.* field metadata + TypeFidelity)
      ↓
L4  TargetColumnSpec  (per ExportTarget: sql_type, autoload_type, status)
      ↓
L5  Materialization   (DDL, load schema, cast SQL — operator or future loader)
```

### Invariants

**T1 — Single semantic source.** Only `RivetType` (via `TypeMapping` / `build_arrow_field`) may drive L2–L3. Source drivers must not set ad-hoc Arrow types for domain columns.

**T2 — Interchange is target-neutral.** Parquet physical types are chosen for cross-engine fidelity (e.g. `Decimal128`, `Timestamp` with timezone). Target-specific types (BigQuery `JSON`, Snowflake `VARIANT`) appear in L4–L5, not by changing L2 per export unless a separate **target profile** is explicitly enabled (future, opt-in).

**T3 — Metadata is provenance, not autoload.** Keys `rivet.native_type`, `rivet.logical_type`, `rivet.fidelity` ([`src/types/mapping.rs`](../../src/types/mapping.rs)) document intent for tooling and CI. Generic Parquet readers may ignore them.

**T4 — Plain strings stay plain.** Columns mapped to `RivetType::String` / `Text` must **not** carry `rivet.logical_type` (tests enforce this). Semantic JSON/UUID/enum must use the corresponding `RivetType` variants.

**T5 — Materialization is explicit.** Achieving target-native types requires `TargetColumnSpec` + L5 (cast or load schema). Autoload from Parquet alone is a **compatibility class**, not the native class, when `rivet.logical_type` is set.

**T6 — Fidelity gates policy.** `TypeFidelity::Lossy` / `Unsupported` behavior remains governed by `TypePolicy` and `--strict` on type-report; target resolver must not upgrade fidelity.

---

## Physical interchange (L2–L3) — current contract

Documented in [type-mapping.md](../type-mapping.md). Summary:

| Source (examples) | Rivet | Parquet (Arrow) | Parquet metadata |
|-------------------|-------|-----------------|------------------|
| `json` / `jsonb` | `Json` | `Utf8` | `logical_type=json`, `fidelity=logical_string` |
| `uuid` | `Uuid` | `FixedSizeBinary(16)` | `arrow.uuid` ext → native `LogicalType::Uuid`, `fidelity=exact` |
| `numeric(p,s)` | `Decimal` | `Decimal128/256` | `fidelity=exact` |
| `timestamptz` | `Timestamp` + UTC | `Timestamp(µs, UTC)` | `fidelity=exact` |
| PG `enum` | `Enum` | `Utf8` | `logical_type=enum` |

CSV omits list columns today; metadata is Parquet-only.

---

## Target materialization (L4–L5)

### `ExportTarget`

Extend the enum in `src/types/target.rs` (order reflects recommended implementation priority):

| Target | CLI alias | Role |
|--------|-----------|------|
| `DuckDb` | `duckdb` | Reference consumer of Parquet; local analytics/staging |
| `BigQuery` | `bigquery`, `bq` | ✅ partial (`bq_compat`) |
| `Snowflake` | `snowflake` | Cloud warehouse |
| `ClickHouse` | `clickhouse`, `ch` | Columnar OLAP |

### `TargetColumnSpec` (new struct)

Per column, per target:

```rust
pub struct TargetColumnSpec {
    pub target_type: String,       // e.g. "JSON", "VARCHAR", "UBIGINT"
    pub autoload_type: String,     // type inferred by read_parquet / BQ autodetect
    pub status: TargetStatus,      // ok | warn | fail (existing)
    pub note: Option<String>,
    pub cast_sql: Option<String>,  // e.g. "attrs::JSON" (DuckDB), "PARSE_JSON(attrs)" (BQ)
}
```

Resolver inputs: `RivetType`, `Option<DataType>` (Arrow), field metadata, `ExportTarget`, optional `TypePolicy` / column overrides.

Resolver **must** consider `rivet.logical_type` when physical type is `Utf8` / `LargeUtf8`.

### RivetType → target native (normative matrix)

**Autoload** = type a typical Parquet reader assigns without casts. **Native** = recommended table type for semantic fidelity.

| RivetType | DuckDB native | DuckDB autoload | BigQuery native | BQ autoload | Snowflake native | ClickHouse native |
|-----------|---------------|-----------------|-----------------|-------------|------------------|-------------------|
| `Json` | `JSON` | `VARCHAR` | `JSON` | `STRING` | `VARIANT` | `JSON`† |
| `Uuid` | `UUID` | `UUID` | `STRING` | `BYTES` ⚠ | `TEXT` | `UUID` |
| `Enum` | `VARCHAR` | `VARCHAR` | `STRING` | `STRING` | `STRING` | `String` |
| `Decimal(p,s)` | `DECIMAL(p,s)` | `DECIMAL(p,s)` | `NUMERIC`/`BIGNUMERIC`‡ | same | `NUMBER(p,s)` | `Decimal(p,s)` |
| `UInt64` | `UBIGINT` | `UBIGINT`/`HUGEINT` | `INT64` ⚠ | `INT64` | `NUMBER` | `UInt64` |
| `Timestamp` + TZ | `TIMESTAMPTZ` | `TIMESTAMPTZ` | `TIMESTAMP` | `TIMESTAMP` | `TIMESTAMP_TZ` | `DateTime64` |
| `Timestamp` naive | `TIMESTAMP` | `TIMESTAMP` | `DATETIME` | `DATETIME` | `TIMESTAMP_NTZ` | `DateTime64` |
| `Interval` | `INTERVAL` / `VARCHAR` § | `VARCHAR` | `INTERVAL` ⚠ | `STRING` | `INTERVAL` | `String` |
| `List { … }` | `LIST(T)` | `LIST(T)` | `ARRAY<…>` | `REPEATED …` | `ARRAY` | `Array(T)` |
| `Binary` | `BLOB` | `BLOB` | `BYTES` | `BYTES` | `BINARY` | `String`/binary |

† ClickHouse: use `JSON` when querying inside fields; opaque blob → `String` ([JSON type](https://clickhouse.com/docs/sql-reference/data-types/json)).  
‡ BigQuery precision/scale limits enforced in existing `bq_decimal_compat`.  
⚠ Warn on overflow or limited arithmetic (existing BQ `UINT64` / `INTERVAL` warnings).  
§ PG `interval` exported as ISO `Utf8` today → materialize as `VARCHAR` or parse to `INTERVAL` per policy.

### Example L5 — DuckDB view over Rivet Parquet

```sql
CREATE VIEW payload_typed AS
SELECT
  * REPLACE (
    attrs::JSON AS attrs,
    uid::UUID   AS uid
  )
FROM read_parquet('export.parquet');
```

### Example L5 — BigQuery load schema snippet

```sql
-- autoload: all strings; native: declare JSON columns in load job / external table
attrs JSON,
uid STRING
```

---

## CLI and manifest integration

### Phase A (v0.8) — type-report extension

- `rivet check --type-report --target duckdb` (and other targets as implemented).
- Columns: existing source/Rivet/Arrow/fidelity + **target native**, **autoload**, **status**, **note**.
- DuckDB: warn only when `native != autoload` and cast is recommended (JSON, UUID).

### Phase B — load plan artifact (optional)

- `rivet plan-load -c export.yaml --target duckdb` emits DDL or view SQL (L5) from planned `TypeMapping`s — no second export.
- Optional sidecar next to manifest: `type_manifest.json` listing L3+L4 per column (does not change `schema_fingerprint`).

### Phase C — Epic 14 direct load

- Warehouse writer calls same `TargetColumnSpec` resolver before INSERT/COPY/load job.
- File interchange unchanged unless operator opts into `exports[].target_profile` (future).

---

## Relationship to existing artifacts

| Artifact | Includes `rivet.*` metadata? | Includes target native type? |
|----------|------------------------------|------------------------------|
| Parquet file | Yes (field KV) | No |
| `schema_fingerprint` | No (Arrow `Debug` only) | No |
| `manifest.json` | No (today) | No (today; Phase B optional) |
| `rivet check --type-report` | Via Rivet/Arrow columns | Phase A |

ADR-0012 manifest invariants (PBM, MBS, PIT) are unchanged. Type materialization does not alter part upload order ([ADR-0004](0004-destination-write-contracts.md)).

---

## Implementation plan

| Step | Deliverable | Notes |
|------|-------------|-------|
| 1 | `duckdb_compat()` + `ExportTarget::DuckDb` | Mirror `bq_compat`; JSON/UUID/UInt64 rules |
| 2 | Refactor to `resolve_target_column(RivetType, Arrow, metadata, target)` | Shared by type-report |
| 3 | Type-report columns: `target_type`, `autoload_type`, `cast_hint` | Docs + live CLI tests |
| 4 | `docs/type-mapping.md` § Downstream targets | Link this ADR |
| 5 | `plan-load` command + optional `type_manifest.json` | Phase B |
| 6 | Snowflake / ClickHouse resolvers | Same matrix, per-engine limits |
| 7 | Direct warehouse load | Epic 14; reuse resolver |

---

## Consequences

### Positive

- Operators understand why Parquet shows `VARCHAR` for JSON and what to run in DuckDB/BQ.
- One resolver serves CLI, future load jobs, and documentation.
- DuckDB-first path validates materialization without cloud credentials.

### Negative / trade-offs

- Two-type mental model (autoload vs native) until operators apply L5 SQL.
- Parquet metadata alone is insufficient for zero-touch native types — by design (T3, T5).
- Maintaining N target tables requires discipline; matrix lives in this ADR and tests.

### Risks

- Resolver drift from warehouse docs — mitigate with contract tests keyed off [expected_contracts.yaml](../../tests/type_roundtrip/fixtures/expected_contracts.yaml) + target-specific rows.
- Confusing `schema_fingerprint` with semantic schema — document clearly; semantic snapshot is `type_manifest.json` (Phase B), not fingerprint replacement.

---

## References

- Rivet: [type-mapping.md](../type-mapping.md), `src/types/mapping.rs`, `src/types/target.rs`, `src/types/fidelity.rs`
- BigQuery: [Standard SQL data types](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
- Snowflake: [SQL data types](https://docs.snowflake.com/en/sql-reference-data-types)
- ClickHouse: [Data types](https://clickhouse.com/docs/sql-reference/data-types)
- DuckDB: [Data types overview](https://duckdb.org/docs/current/sql/data_types/overview.html)
- Sling: [Columns](https://docs.slingdata.io/concepts/replication/columns), [Templates](https://docs.slingdata.io/concepts/replication/templates)
- Airbyte: [Supported data types](https://docs.airbyte.com/platform/understanding-airbyte/supported-data-types), [Destinations V2](https://github.com/airbytehq/airbyte/blob/master/docs/release_notes/upgrading_to_destinations_v2.md)
