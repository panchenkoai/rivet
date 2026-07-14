# Rivet Documentation

Fast, safe database exports: PostgreSQL / MySQL / SQL Server / MongoDB → Parquet / CSV, local or cloud (S3, GCS, Azure), with CDC, resumable chunking, and an always-on integrity layer.

## Start here

- [Getting started](getting-started.md)
- [Concepts](concepts.md)
- [Best practices](best-practices/)
- [Cloud destinations](cloud-destinations.md) · [Cloud auth](cloud-auth.md) · [Cloud permissions](cloud-permissions.md)

## Reference

- [CLI reference](reference/cli.md)
- [Architecture](architecture.md)
- [ADRs](adr/)

## Quality evidence

- [Reliability matrix](reliability-matrix.md)
- Coverage ledgers: [behaviour](behaviour-matrix.yaml) · [CDC](cdc-matrix.yaml) · [chunking](chunking-matrix.yaml) · [cross-config](cross-config-matrix.yaml) · [fail-loud](fail-loud-matrix.yaml) · [resilience](resilience-matrix.yaml) · [type-fidelity](type-fidelity-matrix.yaml) · [warehouse-load](warehouse-load-matrix.yaml)
- [Mutation-testing plan](mutation-plan.md) — how test assertions are proven able to fail
- [Release checklist](release-checklist.md)
