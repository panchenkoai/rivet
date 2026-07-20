# Summary

[Rivet](README.md)

# Why Rivet

- [Source-safe under load](why/source-safe-under-load.md)
- [Flat memory at any scale](why/flat-memory.md)
- [CDC cost, per engine](why/cdc-cost-per-engine.md)

# Guide

- [Who is Rivet for?](who-is-this-for.md)
- [Getting started](getting-started.md)
- [Concepts](concepts.md)
- [Execution semantics](semantics.md)
- [Type mapping](type-mapping.md)
- [Partitioning](partitioning.md)

# Export modes

- [Overview](modes/README.md)
- [Full](modes/full.md)
- [Incremental](modes/incremental.md)
- [Incremental — composite cursor](modes/incremental-coalesce.md)
- [Chunked](modes/chunked.md)
- [Time window](modes/time-window.md)

# Change data capture

- [CDC guide](reference/cdc.md)
- [Failure modes & recovery](reference/cdc-failure-modes.md)
- [Change ordering](cdc-seq-ordering.md)
- [Load CDC into BigQuery](cdc-bigquery-load.md)

# Sources

- [Compatibility & versions](reference/compatibility.md)
- [MongoDB](reference/mongodb.md)
- [Extraction prioritization](reference/prioritization.md)

# Destinations

- [Local files](destinations/local.md)
- [Amazon S3 / MinIO / R2](destinations/s3.md)
- [Google Cloud Storage](destinations/gcs.md)
- [Azure Blob Storage](destinations/azure.md)
- [stdout](destinations/stdout.md)
- [Cloud destinations overview](cloud-destinations.md)
- [Cloud auth](cloud-auth.md)
  - [Azure auth modes](shared/azure-auth-modes.md)
- [Cloud permissions](cloud-permissions.md)
- [Cloud smoke tests](cloud-smoke-tests.md)

# Best practices

- [Overview](best-practices/README.md)
- [Resource-aware extraction](best-practices/resource-aware-extraction.md)
- [Low-memory runners](best-practices/low-memory-runners.md)
- [SQL Server gentle extraction](best-practices/mssql-gentle-extraction.md)
- [Parquet tuning](best-practices/parquet-tuning.md)
- [Compression profiles](best-practices/compression-profiles.md)
- [Recovery & resume](best-practices/recovery-and-resume.md)
- [Quality checks](best-practices/quality-checks.md)
- [Benchmark methodology](best-practices/benchmark-methodology.md)

# Operate in production

- [Pilot guide](pilot/README.md)
  - [Demo quickstart](pilot/demo-quickstart.md)
  - [Pilot walkthrough](pilot/pilot-walkthrough.md)
  - [Production checklist](pilot/production-checklist.md)
  - [UAT checklist](pilot/uat-checklist.md)
  - [Reconcile runbook](pilot/reconcile-runbook.md)
  - [Rivet vs a cursor pipeline](pilot/rivet-vs-cursor-pipeline.md)

# Recipes

- [Verify your export](recipes/verify-your-export.md)
- [Recover an interrupted run](recipes/recover-interrupted-run.md)
- [Idempotent warehouse load](recipes/idempotent-warehouse-load.md)
- [Snowflake load](recipes/snowflake-load.md)
- [Airflow DAG](recipes/airflow/README.md)

# Reference

- [CLI](reference/cli.md)
  - [Full CLI reference](reference/cli-reference.md)
- [Config](reference/config.md)
  - [Full config reference](reference/config-reference.md)
- [rivet init](reference/init.md)
- [Tuning](reference/tuning.md)
- [Testing](reference/testing.md)
- [Architecture](architecture.md)

# Evidence & quality

- [Reliability matrix](reliability-matrix.md)
- [Cross-tool benchmark harness](bench/README.md)
- [Benchmark report — v0.5.0 (historical)](archive/benchmark_report_v0.5.0.md)
- [Mutation-testing plan](mutation-plan.md)
- [Release checklist](release-checklist.md)
- [Terminal walkthroughs](gifs/README.md)

# Architecture decisions

- [ADR-0001 — State update invariants](adr/0001-state-update-invariants.md)
- [ADR-0002 — CLI product vs library](adr/0002-cli-product-vs-library.md)
- [ADR-0003 — Layer classification](adr/0003-layer-classification.md)
- [ADR-0004 — Destination write contracts](adr/0004-destination-write-contracts.md)
- [ADR-0005 — Plan/Apply contracts](adr/0005-plan-apply-contracts.md)
- [ADR-0006 — Source-aware prioritization](adr/0006-source-aware-prioritization.md)
- [ADR-0007 — Cursor policy contracts](adr/0007-cursor-policy-contracts.md)
- [ADR-0008 — Export progression](adr/0008-export-progression.md)
- [ADR-0009 — Reconcile & repair contracts](adr/0009-reconcile-and-repair-contracts.md)
- [ADR-0010 — Two parallel engines](adr/0010-two-parallel-engines.md)
- [ADR-0011 — Source trait: Send not Sync](adr/0011-source-trait-send-not-sync.md)
- [ADR-0012 — Cloud manifest contract](adr/0012-cloud-manifest-contract.md)
- [ADR-0013 — Trust flag contract](adr/0013-trust-flag-contract.md)
- [ADR-0014 — Target type materialization](adr/0014-target-type-materialization.md)
- [ADR-0015 — Source introspection seam](adr/0015-source-introspection-seam.md)
- [ADR-0016 — Nullability propagation deferred](adr/0016-nullability-propagation-deferred.md)
- [ADR-0017 — Per-runner durability ordering](adr/0017-per-runner-durability-ordering.md)
- [ADR-0018 — Builder facades for runner-invariant ordering](adr/0018-builder-facades-for-runner-invariant-ordering.md)
- [ADR-0019 — Governor as extracted policy](adr/0019-governor-as-extracted-policy.md)
- [ADR-0020 — PG UUID PK chunking asymmetry](adr/0020-pg-uuid-pk-chunking-asymmetry.md)
- [ADR-0021 — Chunked schema drift pre-chunk](adr/0021-chunked-schema-drift-pre-chunk.md)
- [ADR-0022 — Pace-aware throttle](adr/0022-pace-aware-throttle.md)
- [ADR-0023 — CDC NDJSON & file drivers stay separate](adr/0023-cdc-ndjson-and-file-drivers-stay-separate.md)
- [ADR-0024 — PG CDC pgoutput migration](adr/0024-pg-cdc-pgoutput-migration.md)
- [ADR-0025 — CDC paged refill loop inlined per adapter](adr/0025-cdc-paged-refill-loop-inlined-per-adapter.md)
- [ADR-0026 — First-party extension seam](adr/0026-first-party-extension-seam.md)
- [ADR-0027 — Structured read relation seam](adr/0027-structured-read-relation-seam.md)
