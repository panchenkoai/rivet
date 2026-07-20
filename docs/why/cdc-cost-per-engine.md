# CDC cost, per engine

Change-data-capture reads a source's transaction log, and the one operational
hazard worth understanding before you turn it on is **log retention**: does the
*reader* hold log data on the source, and can an abandoned reader fill the source
disk? The answer is not the same across engines, and Rivet is explicit about it.

## The one engine that can pin the disk: PostgreSQL

PostgreSQL's logical replication slot is a **consume-retention** mechanism: the
server keeps WAL from the slot's confirmed position forward until the consumer
acknowledges it. That is what makes resume exactly-once — and it is also the one
place where a reader can hurt the source. While Rivet keeps up, retention is
bounded to roughly `interval × write-rate` (≈ 23.5 MB over a 6 s cycle at
~4 MB/s in the harness). But an **abandoned or stuck slot is unbounded**: during
this project's own testing, a forgotten slot pinned **29 GB of WAL and crashed
the PostgreSQL instance** by filling its disk.

**Operational rule for PostgreSQL CDC:** monitor slot lag / retained WAL, and
drop slots you no longer consume. This is inherent to how PostgreSQL logical
replication guarantees no-loss — every tool that uses a slot inherits it — but
Rivet names it plainly rather than hiding it.

## The engines that cannot: MySQL, SQL Server, MongoDB

On the other three engines, log retention is decided by the **server on its own
schedule**, independent of any reader. A Rivet CDC run — or a crashed one —
structurally cannot fill the source disk:

| Engine     | Retention model | Disk-fill hazard |
|------------|-----------------|------------------|
| PostgreSQL | logical slot — reader-pinned until ack | **Yes — monitor slot lag** |
| MySQL      | binlog purged by `binlog_expire_logs_seconds` regardless of reader | No |
| SQL Server | change tables trimmed by the CDC cleanup job regardless of reader | No |
| MongoDB    | oplog is a fixed-size capped collection (rolls over by size) | No |

On these engines the trade-off flips: because the server can trim the log out
from under a slow reader, the risk is not *disk-fill* but *falling behind*
(missing changes if you resume after the log has rolled past your checkpoint).
The mitigation is a short enough scheduler interval, not disk monitoring.

## Why this shapes the default

Because retention on PostgreSQL is coupled to acknowledgement, an abandoned
*continuous* stream is the worst case — it holds a slot open forever. That is a
large reason the OSS model is the **bounded** drain: `until_current` now
[defaults to `true`](../reference/cdc.md), so a scheduled run reads to the log
end, checkpoints, acks (advancing the slot), and exits. The next cycle resumes
from the checkpoint. Continuous streaming is an explicit opt-in
(`until_current: false`, or `rivet cdc --stream`) that logs a warning, so you
never start a never-terminating slot-holding stream by accident.

The full per-engine numbers, harnesses, and contracts are in the
[performance & harm ledger](https://github.com/panchenkoai/rivet/blob/main/docs/perf-matrix.yaml);
the recovery playbook per symptom is in
[CDC failure modes](../reference/cdc-failure-modes.md).
