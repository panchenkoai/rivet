# Oracle CDC probes (2026-09-26)

Live probes that close the §7a "not yet exercised" list of `oracle-source.md`.
Target: container `rivet-oracle-spike` (`gvenzl/oracle-free:23-slim-faststart`, banner
`Oracle AI Database 26ai Free Release 23.26.3.0.0`, arm64), host port 15210, CDB `FREE`,
PDB `FREEPDB1`. All SQL below ran through `sqlplus` inside the container. Probe users and
tables were dropped afterwards; the container is left in ARCHIVELOG with minimal + PK
supplemental logging.

Legend: [V] verified here · [X] refuted here · [U] still unknown · [I] inference.
Oracle docs relied on:
- LogMiner utility (23): https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html
- `V$LOGMNR_CONTENTS`: https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-LOGMNR_CONTENTS.html
- `V$DATABASE`: https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-DATABASE.html

## Headline findings

1. **Mining works from the PDB service with no `ADD_LOGFILE`** [V]. In a PDB,
   `ADD_LOGFILE` is refused (`ORA-65040`), but `START_LOGMNR(startscn => S)` alone
   auto-registers every archived + online log from S onward and returns only this PDB's
   rows. rivet does not need to connect to `CDB$ROOT` or use a common user.
2. **Four grants suffice for mining in a PDB** [V]: `CREATE SESSION`, `LOGMINING`,
   `EXECUTE ON DBMS_LOGMNR`, `SELECT ON V_$LOGMNR_CONTENTS`. That is narrower than the
   documented `EXECUTE_CATALOG_ROLE` + `LOGMINING`. The gap check and identity need
   three more (`V_$DATABASE`, `V_$ARCHIVED_LOG`, `V_$LOG`), plus `V_$LOGMNR_LOGS` and
   `V_$TRANSACTION`.
3. **Straddling transactions: proven both ways** [V]. Resuming from the last commit SCN
   + 1 lost the early insert of a transaction that started before X and committed after
   it. Resuming from `restart_scn` (the oldest open START) with commits
   `<= commit_scn` dropped delivered it whole.
4. **A missing log in the MIDDLE of the range is silent** [V] in both modes when the
   catalog knows it is gone (`DELETED='YES'`), and in root mode when rivet skips it
   itself. Mining goes on, `V$LOGMNR_CONTENTS` has **no `MISSING_SCN` row**, and a
   committed insert disappears. The only in-session signal is `V$LOGMNR_LOGS.STATUS=4`
   (`INFO='MISSING_LOGFILE'`). A missing log at the START of the range, or a file the
   catalog still lists as present, fails loudly.
5. **Partial rollback (`ROLLBACK TO SAVEPOINT`) inside a COMMITTED transaction** shows
   up as the original `INSERT` (`ROLLBACK=0`) plus a compensating `DELETE`
   (`ROLLBACK=1`) that carries **only the ROWID** [V]. A framer that simply drops
   `ROLLBACK=1` rows emits a row that was never committed.

## 1. ARCHIVELOG + supplemental logging

```sql
-- as SYSDBA in CDB$ROOT. The slim image has no FRA and no archive dest: set one first.
alter system set log_archive_dest_1='LOCATION=/opt/oracle/oradata/FREE/arch' scope=spfile;
shutdown immediate
startup mount
alter database archivelog;
alter database open;
alter pluggable database all open;   -- no-op here: FREEPDB1 was already READ WRITE (saved state)
alter database add supplemental log data (primary key) columns;
```
(`mkdir -p /opt/oracle/oradata/FREE/arch` beforehand, as `oracle`.)

- [V] The sequence above works; it took ~2 min end to end. `archive log list` →
  `Archive Mode / Automatic archival Enabled / Archive destination /opt/oracle/oradata/FREE/arch`.
- [V] Minimal supplemental logging was already `YES` in this image; PK was `NO` until added.
- [V] **Survives `docker restart`**: after the restart, `LOG_MODE=ARCHIVELOG`, MIN/PK
  `YES/YES`, FREEPDB1 `READ WRITE`. The container has **no volume** (`Mounts: []`), so it
  survives restart but not `docker rm` / `compose down` [I] → the stand must bake these
  steps into a setup script.
- [V] Where it lives: database-level supplemental logging is set in `CDB$ROOT` and is
  visible from the PDB (`V$DATABASE` in the PDB shows `YES/YES`). `ALTER DATABASE ADD/DROP
  SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS` is also accepted inside the PDB (PDB-level
  logging). Dropping it at PDB level left the effective value `YES`, because the CDB
  setting still applies.
- Pitfalls [V]:
  - No archive destination in the slim image: without `log_archive_dest_1` the archive
    location is the unset default. Set it explicitly.
  - The image has **two 10 MB redo groups**. A 100k-row insert took 29 s, with 297
    `Checkpoint not complete` / `cannot allocate new log` entries in the alert log. The
    stand should add bigger or more groups, or every write-heavy test measures log-switch
    stalls.
  - `rman` is **not in the slim image**, so `CROSSCHECK` / `DELETE ARCHIVELOG` cannot be
    run. §4 used `SYS.DBMS_BACKUP_RESTORE.DELETEARCHIVEDLOG` (the internal call behind
    RMAN's delete) to put the catalog into the `DELETED='YES'` state.

## 2. LogMiner across log switches; PDB vs root

DML in `FREEPDB1` (2 inserts, commit) → `SWITCH LOGFILE` ×2 + `ARCHIVE LOG CURRENT` →
update/delete/insert, commit. Archived seq 328–330, online seq 331.

**PDB service, local user, no ADD_LOGFILE** [V]:
```sql
exec dbms_logmnr.start_logmnr(startscn => :s, options => dbms_logmnr.dict_from_online_catalog)
select filename, low_scn, next_scn, status, type from v$logmnr_logs order by low_scn;
```
```
/opt/oracle/oradata/FREE/arch/1_328_1242687965.dbf  2307015  2310568  0 ARCHIVE
/opt/oracle/oradata/FREE/arch/1_329_1242687965.dbf  2310568  2310583  0 ARCHIVE
/opt/oracle/oradata/FREE/arch/1_330_1242687965.dbf  2310583  2310592  0 ARCHIVE
/opt/oracle/oradata/FREE/redo01.log                 2310592  1.8E+19  0 ONLINE
```
All 5 DML rows came back across the switches, each transaction as START … COMMIT.
`GROUP BY src_con_name` in PDB mode → only `FREEPDB1`. In root mode the same range
returned `CDB$ROOT 2074, FREEPDB1 1115`, so root-mode readers must filter by
`SRC_CON_NAME` / `CON_ID`.

**`ADD_LOGFILE` from the PDB** [V]: `ORA-65040: Operation is not allowed from within a
pluggable database` (at `DBMS_LOGMNR` line 78 = `ADD_LOGFILE`).

**Root service, common user, explicit file set** [V]: works. The file-selection query:
```sql
select name, sequence# from v$archived_log
 where dest_id = 1 and status = 'A' and deleted = 'NO'
   and resetlogs_change# = (select resetlogs_change# from v$database)
   and next_change# > :s
union all
select lf.member, l.sequence# from v$log l join v$logfile lf on lf.group# = l.group#
 where l.next_change# > :s
   and l.sequence# not in (select sequence# from v$archived_log
                            where dest_id = 1 and status = 'A' and deleted = 'NO'
                              and resetlogs_change# = (select resetlogs_change# from v$database))
order by 2;
-- first file dbms_logmnr.new, the rest dbms_logmnr.addfile; then start_logmnr(startscn => :s, ...)
```
- [V] Adding both the archived copy and the online member of the same sequence →
  `ORA-01289: cannot add duplicate logfile`. Hence the `not in` above.
- [V] `CONTINUOUS_MINE` was not used or needed.
- [V] DDL `SQL_REDO` contains password verifiers (`create user … identified by VALUES
  'S:…;T:…'`). rivet must never log DDL `SQL_REDO`.

**Minimal grants (PDB mode)**, found by adding one grant at a time to a fresh user [V]:

| grants held | result |
|---|---|
| `CREATE SESSION` | ORA-00942 on `V_$LOGMNR_CONTENTS`; `DBMS_LOGMNR` not visible |
| + `LOGMINING` | same |
| + `SELECT ON V_$LOGMNR_CONTENTS` | still fails (no `DBMS_LOGMNR`) |
| + `EXECUTE ON DBMS_LOGMNR` | **mining works** |
| then `REVOKE LOGMINING` | `ORA-01031 insufficient privileges` from `START_LOGMNR` |

No `SELECT` on the captured table was needed to decode its rows with
`DICT_FROM_ONLINE_CATALOG` [V].

## 3. Resume and the straddling transaction

Setup (two sessions): A inserts id=10 at SCN 2311453 and stays open; B inserts id=11 and
commits at 2311462; X = 2311472. Run 1 mines `[2311445, X]`:
```
2311453  03000200F6020000 START / INSERT 10 'straddle-early'     (no COMMIT in window)
2311461  0900040079030000 START / INSERT 11
2311462  0900040079030000 COMMIT  commit_scn 2311462
commit_scn  = max COMMIT_SCN emitted          = 2311462
restart_scn = min SCN of a START with no COMMIT/ROLLBACK in the window = 2311453
```
`V$TRANSACTION` at that moment: `03000200F6020000 START_SCN 2311453 ACTIVE`, the same
value [V]. Then A inserts id=12 and commits (2311627), a log switch, C inserts id=13
(2311728), another switch. Run 2 spans archived 331–333 + online:

| resume from | emitted (commits > 2311462) |
|---|---|
| `restart_scn` 2311453 (correct) | 10 `straddle-early`, 12 `straddle-late`, 13 — B not repeated |
| `commit_scn+1` 2311463 (naive) | 12, 13 — **id=10 lost** |

The run-2 filter:
```sql
select m.* from mined m
  join mined c on c.xid = m.xid and c.operation = 'COMMIT' and c.commit_scn > :commit_scn
 where m.seg_owner = 'PROBE_SRC' and m.table_name = 'T'
 order by c.commit_scn, m.scn, m.rs_id, m.ssn;
```
- [V] `START_LOGMNR(startscn, endscn)` bounds the window in PDB mode.
- [I] Deriving `restart_scn` from the mined window is correct only by induction: the
  window must start at the previous `restart_scn`, or a transaction's START row is
  outside it. At the first open, use `V$TRANSACTION` (below).
- [U] Two transactions sharing one `COMMIT_SCN`: not reproduced. If they can, a
  `commit_scn`-only watermark checkpointed between them drops the second. Either
  checkpoint only on an SCN boundary or also record the XIDs at `commit_scn`.
- [V] `V$TRANSACTION` from a PDB user shows only that PDB's transactions (root showed a
  CON_ID 1 transaction that the PDB did not).

## 4. Gap detection

| scenario | mode | result |
|---|---|---|
| middle file `rm`'d, catalog still `A/NO` | PDB | **loud**: `ORA-01284` / `ORA-00308 cannot open archived log` / `ORA-27037` |
| same, file restored | PDB | mining works again (recoverable) |
| middle file skipped by the caller | root | **silent**: `V$LOGMNR_LOGS` row `STATUS=4 INFO=MISSING_LOGFILE "Missing log file(s) for thread 1, sequence(s) 332 to 332"`; rows still returned; 0 `MISSING_SCN` rows |
| `restart_scn` inside a catalog-`DELETED` log (oldest purged) | PDB | loud but misleading: `ORA-38500 LogMiner AdHoc query failed to find redo log after 3 retries … online redo log being archived …` |
| same | root, selection query above | loud: `ORA-01291: missing log file` |
| middle log catalog-`DELETED` (seq 333), start in 332 | PDB | **silent**: STATUS=4 row, 80 inserts returned, 0 `MISSING_SCN` rows; the committed insert id=13 (SCN 2311727, inside seq 333) **absent** |

Catalog after the delete:
```
 SEQ  FIRST_CHANGE#  NEXT_CHANGE#  S  DEL
 331  2310592        2311695       D  YES
 332  2311695        2311719       A  NO
 333  2311719        2311737       D  YES
 334  2311737        2312317       A  NO
```

**Pre-mining gap check** (sequence contiguity + start coverage). It is the one query that
is loud in every case above:
```sql
with inc as (select resetlogs_change# r from v$database),
avail as (
  select thread#, sequence#, first_change#, next_change# from v$archived_log, inc
   where resetlogs_change# = inc.r and status = 'A' and deleted = 'NO'
  union
  select thread#, sequence#, first_change#, next_change# from v$log),
agg as (
  select sum(case when first_change# <= :s and next_change# > :s then 1 else 0 end) start_covered,
         min(sequence#) lo_seq, max(sequence#) hi_seq, count(distinct sequence#) have
    from avail where next_change# > :s)
select case when start_covered = 0 or have <> hi_seq - lo_seq + 1 then 'GAP' else 'OK' end
  from agg;
```
```
 START_SCN  START_COVERED  LO_SEQ  HI_SEQ  HAVE  VERDICT
 2311453    0              332     336     4     GAP   (start purged)
 2311700    1              332     336     4     GAP   (333 missing)
 2311740    1              334     336     3     OK
```
- [V] `SELECT MIN(first_change#) FROM v$archived_log WHERE status='A' AND deleted='NO'`
  (= 2311695 here) is the oldest SCN still minable. `restart_scn` below it is a gap. It
  does **not** detect a middle hole, so it is a necessary check but not a sufficient one.
- [V] `status='A' AND deleted='NO'` does not see a file removed by the OS (catalog still
  `A/NO`). That case fails loudly at `START_LOGMNR` (ORA-00308), which is acceptable.
- [I] Single thread only: on RAC the contiguity must be checked per `thread#` (out of
  scope).
- [I] A sequence whose controlfile record aged out (`control_file_record_keep_time`) has
  no row at all. The contiguity count flags it as `GAP`, which is the right direction.
- Belt-and-braces after `START_LOGMNR`:
  `select count(*) from v$logmnr_logs where status <> 0` must be 0 (needs
  `SELECT ON V_$LOGMNR_LOGS`). **Do not rely on `MISSING_SCN` rows** [X for this
  version: none appeared in either silent case].

## 5. Server identity (from a PDB connection)

| value | observed | readable with the 4 mining grants only? |
|---|---|---|
| `SYS_CONTEXT('USERENV','DBID')` | 1514744604 (the CDB DBID) | **yes** |
| `SYS_CONTEXT('USERENV','DB_UNIQUE_NAME')` | FREE | **yes** |
| `SYS_CONTEXT('USERENV','CON_DBID')` | 3638231841 (= `V$PDBS.CON_UID`/`DBID` of FREEPDB1) | **yes** |
| `SYS_CONTEXT('USERENV','CON_NAME')` / `CON_ID` / `CDB_NAME` | FREEPDB1 / 3 / FREE | **yes** |
| `V$DATABASE.RESETLOGS_CHANGE#`, `RESETLOGS_TIME` | 1935702, 2026-08-30 23:06:05 | no: needs `SELECT ON V_$DATABASE` |
| `V$DATABASE.CURRENT_SCN` | – | no: same grant |
| PDB GUID (`V$PDBS.GUID` 5A4CCE53…8219) | – | no: `V_$PDBS`/`V_$CONTAINERS` not visible even with `V_$DATABASE`; `USERENV` has no `CON_GUID`/`CON_UID` (ORA-02003) |
| `V$DATABASE_INCARNATION` | – | no |

- [V] From a PDB, `V$DATABASE.CON_ID` is 0 and `CON_DBID` is the PDB's.
- [V] `TIMESTAMP_TO_SCN(SYSTIMESTAMP)` needs no grant but lagged `CURRENT_SCN` by ~9, so
  it is not a substitute for the open-time bound.
- [V] Archived log names embed the resetlogs id (`1_<seq>_1242687965.dbf`), and
  `V$ARCHIVED_LOG.RESETLOGS_CHANGE#` is per row. That is why the selection and gap
  queries filter on the current incarnation.

## 6. Commit boundary and rollback

- [V] Every row carries `XID`. Each transaction is a `START` row, data rows, then a
  `COMMIT` row whose `COMMIT_SCN` (and `START_SCN`) is filled, or a `ROLLBACK` row. Data
  rows carry neither `COMMIT_SCN` nor `START_SCN`. In run 1, B's `COMMIT` has
  `start_scn 2311461, commit_scn 2311462`.
- [V] Full rollback:
  ```
  2312005 START
  2312005 INSERT  20 'will-rollback'        ROLLBACK=0
  2312005 UPDATE  id=1 …                    ROLLBACK=0
  2312008 UPDATE  … where ROWID = …         ROLLBACK=1   (undo of the update)
  2312008 DELETE  where ROWID = …           ROLLBACK=1   (undo of the insert)
  2312009 ROLLBACK
  ```
  → discard the whole XID.
- [V] Savepoint rollback, then commit:
  ```
  2312011 INSERT 21 'sp-keep'               ROLLBACK=0
  2312011 INSERT 22 'sp-undo'               ROLLBACK=0
  2312011 DELETE where ROWID='…AAD'         ROLLBACK=1
  2312012 COMMIT
  ```
  → the framer must cancel each `ROLLBACK=1` row against the earlier same-XID change it
  undoes. The only key is `ROWID` (no PK in the undo row), undone in reverse order [I].
  Dropping `ROLLBACK=1` rows is wrong: it would emit id=22.
- Framing rule: buffer (spill) rows by XID, emit on `COMMIT` with that row's `COMMIT_SCN`
  after cancelling rolled-back changes, drop on `ROLLBACK`, and mark `committed` only on
  the last emitted event of the XID.

## 7. Cost and blocking

- [V] ~48 MB of redo (5 × 9.7 MB archived logs; 100k inserts + 50k updates):
  `START_LOGMNR` 0.01 s; `count(*)` over all 150,806 rows 0.52 s; operation breakdown
  0.38 s; `sum(length(sql_redo))` = 41.6 MB of `SQL_REDO` text in 0.73 s. That is the
  server-side scan only; client fetch over the network was not measured [U].
- [V] Each query over `V$LOGMNR_CONTENTS` re-mines. A correlated
  `xid in (select xid from v$logmnr_contents …)` took **23.7 s** on ~4 MB where a single
  scan took 0.08 s. The adapter must read the view once, in one pass, and frame
  client-side.
- [V] During a 36 s mining cursor, the miner session held only an `AE` (edition) lock and
  no DDL locks on the table. Concurrently, an `UPDATE` of 1000 rows (1.2 s, log-switch
  stall), `ALTER TABLE big ADD` (0.05 s) and `TRUNCATE` (0.28 s) all went through. Mining
  does not block writers or DDL.
- [I, Oracle docs] With `DICT_FROM_ONLINE_CATALOG`, redo written before an `ALTER TABLE`
  is decoded against the current definition. Oracle says LogMiner then "generates
  nonexecutable SQL (including hexadecimal-to-raw formatting …)" for the old version.
  DDL on a captured table mid-range is a decode hazard to refuse or flag; not probed here.

## Status of the §7a open list

| item | status |
|---|---|
| ARCHIVELOG + archived-log registration | [V] enable sequence; PDB auto-registration; root explicit registration |
| resume across a log switch | [V] incl. straddling transaction, both ways |
| gap detection | [V] loud vs silent cases mapped; pre-mining contiguity query verdicts correct in all 3 cases |
| RAC | [U] out of scope (single thread on Free) |
| mining from the PDB service | [V] works; root not required |
| minimal grants | [V] 4 for mining, +5 views for identity/gap/anchor |
| equal `COMMIT_SCN` across XIDs | [U] |
| client-side fetch cost | [U] |
| DDL mid-range with the online catalog | [U] (Oracle docs say it degrades to hex SQL) |

## Design consequences for the rivet adapter

1. **Where it connects**: the PDB service, as a local PDB user. Use
   `START_LOGMNR(startscn, endscn, DICT_FROM_ONLINE_CATALOG)` with no `ADD_LOGFILE`: in
   PDB mode LogMiner registers the logs and filters to the PDB. Root mode (a common user,
   rivet's own file selection plus a `SRC_CON_NAME` filter) is only for non-CDB or
   pre-21c targets.
2. **Required grants** (the documented list for `rivet check` / init):
   `CREATE SESSION, LOGMINING, EXECUTE ON SYS.DBMS_LOGMNR, SELECT ON SYS.V_$LOGMNR_CONTENTS`
   (mining) + `SELECT ON SYS.V_$DATABASE, V_$ARCHIVED_LOG, V_$LOG, V_$LOGMNR_LOGS,
   V_$TRANSACTION` (identity, gap refusal, open-time anchor, open bound). No
   `SELECT ANY TABLE` is needed for decoding.
3. **Anchor model: CLIENT-side** (the MySQL model; no server retention pin). The
   checkpoint is `{restart_scn, commit_scn}`:
   - resume mines from `restart_scn` and drops XIDs whose `COMMIT_SCN <= commit_scn`;
   - after each emitted commit, `restart_scn` = min START SCN of XIDs still open in the
     window, or `commit_scn + 1` when none are open.
   A single-SCN checkpoint was RED-demonstrated here to lose a straddling transaction's
   early DML. This is the regression test to port.
4. **Idle first run**: the first checkpointed open persists the anchor **immediately**,
   before any change is seen. Set `commit_scn = V$DATABASE.CURRENT_SCN` at open and
   `restart_scn = LEAST(commit_scn, MIN(V$TRANSACTION.START_SCN))` (PDB-scoped). Write
   both even when the run captures zero changes, and test the idle variant (run 1 zero →
   change → run 2 captures it).
5. **Server identity in the checkpoint; refuse a mismatch**: `DBID`, `DB_UNIQUE_NAME`,
   `CON_DBID` (all grant-free via `SYS_CONTEXT('USERENV', …)`) + `RESETLOGS_CHANGE#`
   (`V$DATABASE`). A `RESETLOGS_CHANGE#` change means a new incarnation with the same
   DBID: refuse. A checkpoint without identity: WARN, do not refuse (the house rule).
6. **Gap refusal is load-bearing and must happen BEFORE mining**, because a middle hole
   is silent. Run the contiguity query in §4 on `restart_scn` at every open, refuse on
   `GAP` with a message naming the missing sequence range and the oldest minable SCN
   (`MIN(first_change#)` of available logs). After `START_LOGMNR`, also assert
   `V$LOGMNR_LOGS` has no `STATUS <> 0` row. Never key off `MISSING_SCN`. Rewrite
   `ORA-38500` into "the log covering SCN S is gone", since Oracle's own text blames
   online-log archiving.
7. **Transaction framing**: spill by XID; `committed` only on the last event of the
   XID, at its `COMMIT` row; cancel `ROLLBACK=1` rows against the earlier change by
   ROWID (partial rollback in a committed transaction); drop a whole XID on `ROLLBACK`.
   Read `V$LOGMNR_CONTENTS` in **one pass**: every query re-mines.
8. **Preflight**: `LOG_MODE='ARCHIVELOG'` (online-only mining loses anything that ages
   out of 2 × 10 MB of redo in seconds under load), `SUPPLEMENTAL_LOG_DATA_MIN/PK='YES'`
   visible from the PDB, plus the §4 CDC refusals already in `oracle-source.md`.
9. **Stand**: bake the §1 sequence (archive dest, ARCHIVELOG, PK supplemental logging)
   and bigger or more redo groups into the Oracle CDC stand's setup. `rman` is absent
   from the slim image; the gap tests can use `DBMS_BACKUP_RESTORE.DELETEARCHIVEDLOG` or
   the full image.
10. **Never log DDL `SQL_REDO`**: it carries password verifiers.
