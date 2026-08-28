# ADR-0032 — a FENCED checkpoint journal in the destination, not a local file

Status: **proposed**
Date: 2026-08-27

## Context

A CDC resume position lives in a LOCAL FILE (`cdc.checkpoint`). That has cost this
repository three separate defects in one session, and the three are different
symptoms of one property:

* the path was resolved against the process working directory, so the same config
  run from a cron entry looked somewhere else, found nothing, and re-anchored at the
  current position — three green runs delivered `[3]` of `[1,2,3]`;
* `rivet doctor` resolved it differently again, so it graded a file that was not
  there — and ABSENT is that check's green answer;
* a checkpoint written by one MySQL server was resumed against another without a
  word, because binlog coordinates parse on any host and address a different binlog
  on each (closed by ADR-0031's sibling work: `server_uuid` + GTID containment).

The property: **the checkpoint is not where the data is.** It is ephemeral runner
state describing a durable destination, so it can be lost, moved, copied, or
disagreed about — and nothing reconciles it with what the destination holds.

The second half is worse and is not fixed by any of the three: **nothing stops two
runners believing they own the same stream.** PostgreSQL is immune by construction
— a replication slot is server-side and single — but MySQL, MongoDB and SQL Server
have CLIENT-side anchors, so two rivet processes with the same config both read,
both write parts into the same prefix, and both advance their own file. Split
brain, silently, with both runs reporting success.

## Decision

Move the resume position into the DESTINATION as a fenced journal: an append of
`{generation, run_id, position}` where advancing the generation is a
compare-and-set. A runner that loses the CAS has been fenced — another runner owns
the stream — and must stop rather than continue.

The manifest already lives there and is already a projection of run state into the
destination, so this is the same seam, not a new one.

## What was measured before proposing it (opendal 0.55, read from the vendored source)

There is **no single conditional-write primitive available on all four backends**:

| backend | `write_with_if_match` | `write_with_if_none_match` | `write_with_if_not_exists` |
| --- | --- | --- | --- |
| s3 | yes (unless disabled) | — | — |
| gcs | — | — | yes |
| azblob | — | yes | yes |
| fs (local) | — | — | yes |

So the fence needs TWO shapes, one per capability class, and the ADR's real content
is which store gets which:

* **create-only generation objects** — `fence/<generation>.json` written with
  `if_not_exists`. To advance N → N+1, create `<N+1>`; losing the race means the
  object exists and another runner won. Works on gcs, azblob and fs.
* **CAS on one object** — read `fence.json`, take its ETag, write back with
  `if_match: <etag>`. Works on s3.

Both give the same guarantee; neither is universal.

### MEASURED against the emulators (2026-08-27), and it changes the design

A probe wrote the same key twice with `if_not_exists`, then once more with a STALE
etag under `if_match`, on each backend:

| store | `if_not_exists` | `if_match` |
| --- | --- | --- |
| fs (local) | ENFORCED | no etag reported |
| MinIO (s3) | **ENFORCED** | ENFORCED |
| fake-gcs | **IGNORED** — the second write won | **IGNORED** — the stale etag won |
| Azurite | ENFORCED | **IGNORED** — the stale etag won |

Two conclusions, and the first inverts what the capability table above implies.

**The declared capability is wrong in BOTH directions.** opendal does not declare
`write_with_if_not_exists` for s3, and MinIO enforces it anyway — the option is
passed through as `If-None-Match: *` and the store honours it. opendal DOES declare
it for gcs, and fake-gcs ignores it completely. So a fence that trusts
`Capability::write_with_if_not_exists` proceeds without fencing on one backend and
refuses to try on another where it would have worked.

**The emulators are not the services.** fake-gcs ignores both conditions and
Azurite ignores `if_match`; real GCS has `ifGenerationMatch` and real Azure Blob has
ETag conditionals, both long-standing. This is the risk this ADR named before
measuring: the live suite runs on the emulators, so a fence would be exercised
against stores that silently accept every write. A test asserting REFUSAL fails
loudly there, which is survivable — the unsurvivable outcome is weakening the test
to make the emulator pass, and then real GCS is the only place the fence is graded.

### What this forces

1. **The fence must PROBE, not consult a capability flag.** At open, write a probe
   key twice and require the second to be refused. A destination that cannot fence
   must say so — and then the choice is explicit: refuse to run, or run unfenced
   with a warning that names the store.
2. **`if_not_exists` is the shape**, not ETag CAS: it is enforced on three of four
   and on the fourth (fake-gcs) neither works, so `if_match` buys nothing while
   costing a read-modify-write window.
3. **GCS fencing cannot be graded against fake-gcs.** Either the gcs cell is
   honestly `unverifiable-on-emulator` in the coverage ledger, or the stand grows a
   GCS emulator that implements preconditions. Claiming coverage from a green run
   against a store that ignores the mechanism is the exact defect this repository
   audits for.

## Consequences

**Gained.** A resume position that cannot be lost with the runner, cannot be
resolved to two different paths, and cannot be silently carried to another server —
it sits beside the data it describes. And a second runner is DETECTED rather than
tolerated, on the three engines that have no server-side anchor.

**Cost.** Every checkpoint advance becomes a conditional round-trip to the
destination instead of a local write. For a bounded run that is once per part, not
once per event, so the cost is bounded by the roll cadence — but it must be
measured, not assumed, and on a slow object store it is not free.

**Cost, harder to see:** the destination becomes REQUIRED for progress. Today a
transient destination outage fails the write and the run; with the fence it also
prevents the position advancing, which is the same outcome — but a destination that
is slow rather than down now paces the stream.

**Migration.** A local checkpoint that exists is still authoritative until a fence
exists; the first fenced run adopts its position. Nothing else depends on this
today: there is no released CDC and no persistent checkpoint on any stand.

## Alternatives considered

**Keep the file, add a lease in the state store.** rivet already has a
backend-pluggable `StateStore` with a run-status ledger, and `gc_orphans` already
uses it to tell a live run from a crash orphan. That is a real option and it is
CHEAPER — but it only fences deployments that share a state database. A stateless
or foreign-host runner is exactly the split-brain case, and it would remain
undetected.

**Do nothing and document it.** Honest, and what the anchor-model rule in the process rules
currently does: it names which engines have a server-side anchor and which do not.
The gap is that a documented hazard on three of four engines is still a hazard.
