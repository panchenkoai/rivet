#!/usr/bin/env python3
"""Run one differential scenario: rivet and Debezium over the SAME window.

Window alignment is the whole point of this script. The first hand-run produced
four spurious `debezium-only` rows purely because Debezium's slot was created
before rivet's, so the opening batch fell in one window and not the other. A
harness that reports legitimate timing skew as a difference gets muted within a
week, so both cursors are established BEFORE any change is applied:

    create table -> create publication -> create BOTH slots -> start Debezium
    -> wait until it is streaming -> apply changes -> run rivet -> compare

The wait matters as much as the ordering. Debezium's slot exists from the moment
`CREATE_REPLICATION_SLOT` returns, but the connector only begins consuming once
it has finished starting; applying changes before that is safe (the slot retains
them) while applying them before the SLOT exists is not.
"""
import argparse, json, os, re, shutil, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
# The stands do not all share one docker network: the SQL engines sit in
# `rivet_default`, the Mongo stands in `stand_default`. The sink must join the
# SAME network as the source, or Debezium cannot resolve either of them.
NETS = {"postgres": "rivet_default", "mysql": "rivet_default",
        "mssql": "rivet_default", "mongo": "stand_default"}
PG_HOST_IN_NET = os.environ.get("CDC_ORACLE_PG_HOST", "postgres-cdc")
PG_URL = os.environ.get("POSTGRES_CDC_URL", "postgresql://rivet:rivet@127.0.0.1:5434/rivet")
PG_EXEC = ["docker", "exec", os.environ.get("CDC_ORACLE_PG_CONTAINER", "rivet-postgres-cdc-1"),
           "psql", "postgresql://rivet:rivet@127.0.0.1:5432/rivet", "-tAc"]
MYSQL_HOST_IN_NET = os.environ.get("CDC_ORACLE_MYSQL_HOST", "mysql-cdc")
MYSQL_URL = os.environ.get("MYSQL_CDC_URL", "mysql://rivet:rivet@127.0.0.1:3307/rivet")
MYSQL_EXEC = ["docker", "exec", os.environ.get("CDC_ORACLE_MYSQL_CONTAINER", "rivet-mysql-cdc-1"),
              "mysql", "-uroot", "-privet", "rivet", "-N", "-e"]
MSSQL_HOST_IN_NET = os.environ.get("CDC_ORACLE_MSSQL_HOST", "mssql-cdc")
MSSQL_URL = os.environ.get("MSSQL_CDC_URL", "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet")
MSSQL_EXEC = ["docker", "exec", os.environ.get("CDC_ORACLE_MSSQL_CONTAINER", "rivet-mssql-cdc-1"),
              "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
              "-P", "Rivet_Passw0rd!", "-C", "-d", "rivet", "-h", "-1", "-W", "-Q"]
MONGO_HOST_IN_NET = os.environ.get("CDC_ORACLE_MONGO_HOST", "mongo80-cdc")
MONGO_URL = os.environ.get("MONGO_CDC_URL", "mongodb://127.0.0.1:27208/rivet?replicaSet=rs0&directConnection=true")
MONGO_EXEC = ["docker", "exec", os.environ.get("CDC_ORACLE_MONGO_CONTAINER", "stand-mongo80-cdc-1"),
              "mongosh", "--quiet", "rivet", "--eval"]


def mongo(js: str) -> str:
    r = subprocess.run(MONGO_EXEC + [js], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"mongosh failed: {r.stderr.strip()}\n  js: {js}")
    return r.stdout.strip()


def mssql(sql: str) -> str:
    r = subprocess.run(MSSQL_EXEC + [sql], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"sqlcmd failed: {r.stderr.strip()}\n  sql: {sql}")
    return r.stdout.strip()


def mysql(sql: str) -> str:
    r = subprocess.run(MYSQL_EXEC + [sql], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"mysql failed: {r.stderr.strip()}\n  sql: {sql}")
    return r.stdout.strip()


def psql(sql: str) -> str:
    r = subprocess.run(PG_EXEC + [sql], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"psql failed: {r.stderr.strip()}\n  sql: {sql}")
    return r.stdout.strip()


def sh(*args, check=True):
    r = subprocess.run(args, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"failed: {' '.join(args)}\n{r.stderr[-500:]}")
    return r


def _value_cols(a) -> list:
    """Which non-key columns to compare, per engine.

    MongoDB is the exception and it is structural, not a TODO: the document is a
    JSON STRING in the reference and a JSON string in rivet's `document` column,
    so a text comparison of the two would grade JSON key ORDER and whitespace
    rather than the values. The op/key comparison plus the per-column null-profile
    check in the live suite covers that engine; a value diff here would be noise.
    """
    return ["v"]


def _rivet_json_column(a) -> list:
    """rivet's side of a Mongo value lives inside the `document` JSON column.

    Debezium's ExtractNewDocumentState flattens the document to the top level, so
    `$.v` reads the same cell on both sides. Comparing the two whole documents as
    text would grade JSON key ORDER and whitespace instead — which is what made
    this look like an engine a value comparison cannot cover.
    """
    return ["--rivet-json-column", "document"] if a.engine == "mongo" else []


def run_scenario(name: str, sql, t: str) -> None:
    """Drive one change pattern. Each is a shape the two tools could legitimately
    disagree about, which is the only reason to run it through a differential
    harness rather than a plain assertion."""
    if name == "crud":
        sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b')")
        sql(f"UPDATE {t} SET v='B' WHERE id=2")
        sql(f"DELETE FROM {t} WHERE id=1")
    elif name == "key-update":
        # An UPDATE that MOVES the primary key. PostgreSQL renders it as
        # `old-key: … new-tuple: …` on one line and rivet has a dedicated split for
        # it (finding #42); Debezium emits before/after. Whether the two agree on
        # which key the event belongs to is exactly the kind of question this
        # harness answers and a self-comparison cannot.
        sql(f"INSERT INTO {t} VALUES (1,'a')")
        sql(f"UPDATE {t} SET id=9 WHERE id=1")
    elif name == "wide-txn":
        # Many rows in ONE transaction. rivet buffers a transaction whole and never
        # splits it across parts; the reference has no such constraint. A rollover
        # boundary landing inside the transaction is where an at-least-once break
        # would show up as a difference rather than as a count mismatch.
        vals = ", ".join(f"({i},'v{i}')" for i in range(1, 51))
        sql(f"INSERT INTO {t} VALUES {vals}")
        sql(f"UPDATE {t} SET v='x' WHERE id <= 25")
    elif name == "mid-stream-table":
        # A table created AFTER both cursors were pinned — Airbyte's
        # `newTableSnapshotTest`, which this repo has no equivalent of. rivet's
        # config names one table, so the point is not that it captures the new one:
        # it is that traffic on a table it does NOT capture must not disturb what it
        # does. The reference sees only its own include-list too, so agreement here
        # means both ignored the newcomer the same way.
        sql(f"CREATE TABLE {t}_late (id int PRIMARY KEY, v text)")
        sql(f"INSERT INTO {t}_late VALUES (1,'late')")
        sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b')")
        sql(f"UPDATE {t} SET v='B' WHERE id=2")
    else:
        raise SystemExit(f"unknown scenario {name}")


def run_scenario_mongo(name: str, t: str) -> None:
    """The same change patterns in the document model.

    `_id` is the key on both sides, and it is IMMUTABLE in MongoDB — so
    `key-update` cannot be expressed as an update at all. Replacing the document
    under a new `_id` is delete+insert by construction, which makes Mongo's answer
    to that scenario structurally the reference's answer. Recorded as `na` rather
    than run, because a scenario the engine cannot express is not a passing cell.
    """
    if name == "crud":
        mongo(f"db.{t}.insertMany([{{_id:1,v:'a'}},{{_id:2,v:'b'}}]); "
              f"db.{t}.updateOne({{_id:2}},{{$set:{{v:'B'}}}}); "
              f"db.{t}.deleteOne({{_id:1}})")
    elif name == "wide-txn":
        docs = ", ".join(f"{{_id:{i},v:'v{i}'}}" for i in range(1, 51))
        mongo(f"db.{t}.insertMany([{docs}]); "
              f"db.{t}.updateMany({{_id:{{$lte:25}}}},{{$set:{{v:'x'}}}})")
    elif name == "mid-stream-table":
        mongo(f"db.{t}_late.insertOne({{_id:1,v:'late'}}); "
              f"db.{t}.insertMany([{{_id:1,v:'a'}},{{_id:2,v:'b'}}]); "
              f"db.{t}.updateOne({{_id:2}},{{$set:{{v:'B'}}}})")
    elif name == "key-update":
        raise SystemExit("key-update is NA on MongoDB: _id is immutable, so the "
                         "scenario cannot be expressed — see run_scenario_mongo")
    else:
        raise SystemExit(f"unknown scenario {name}")


def _write_cfg(work, a, t, ckpt, out) -> str:
    """One rivet config writer, used for both the MySQL anchoring run and the
    scenario run — two copies would drift and the anchor would stop anchoring the
    thing being measured."""
    if a.engine == "postgres":
        src, tbl = f'{{ type: postgres, url: "{PG_URL}" }}', f"public.{t}"
        cdc_opts = f"{{ slot: riv_{t}, until_current: true }}"
    elif a.engine == "mongo":
        src, tbl = f'{{ type: mongo, url: "{MONGO_URL}" }}', t
        cdc_opts = f"{{ until_current: true, checkpoint: {ckpt} }}"
    elif a.engine == "mssql":
        src, tbl = f'{{ type: mssql, url: "{MSSQL_URL}" }}', f"dbo.{t}"
        cdc_opts = f"{{ capture_instance: dbo_{t}, until_current: true, checkpoint: {ckpt} }}"
    else:
        src, tbl = f'{{ type: mysql, url: "{MYSQL_URL}" }}', f"rivet.{t}"
        cdc_opts = f"{{ server_id: 9912, until_current: true, checkpoint: {ckpt} }}"
    cfg = os.path.join(work, f"rivet_{os.path.basename(out)}.yaml")
    with open(cfg, "w") as f:
        f.write(f"""source: {src}
exports:
  - name: oracle
    table: {tbl}
    mode: cdc
    format: parquet
    cdc: {cdc_opts}
    destination: {{ type: local, path: {out} }}
""")
    return cfg


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", default="postgres",
                    choices=["postgres", "mysql", "mssql", "mongo"])
    ap.add_argument("--table", default="oracle_t")
    ap.add_argument("--scenario", default="crud",
                    choices=["crud", "mid-stream-table", "key-update", "wide-txn"],
                    help="which change pattern to drive through both tools")
    ap.add_argument("--work", default="/tmp/cdc-oracle")
    ap.add_argument("--keep", action="store_true", help="leave the stand fixtures in place")
    a = ap.parse_args()

    # Work dir is PER ENGINE. Sharing one made the MySQL connector read the
    # offsets.dat a PostgreSQL run had left behind and die with
    # `Source offset 'file' parameter is missing` — a message that names neither
    # the stale file nor the other engine. Found by running the two in sequence.
    t = a.table
    # Per ENGINE **and** per TABLE. Sharing across engines made MySQL read a
    # PostgreSQL offsets.dat; sharing across scenarios let one run's
    # debezium.jsonl accumulate into the next, which surfaced as a `delete` in a
    # scenario that has no delete. Both were harness artefacts wearing the costume
    # of a finding, and the second was only obvious because the phantom row named
    # an operation the scenario never performed.
    # engine / table / SCENARIO. The scenario was missing until 2026-08-25 and the
    # table name is the same for all of them, so consecutive scenarios on one engine
    # shared a dir and Debezium's `debezium.jsonl` ACCUMULATED: running crud then
    # wide-txn reported wide-txn DISAGREE, listing crud's four events as
    # debezium-only. The README already recorded this artefact as fixed — the fix
    # covered the cross-ENGINE leak (MySQL reading a PostgreSQL offsets.dat) and not
    # the cross-SCENARIO one, which reads identically at the call site.
    work = os.path.join(a.work, a.engine, a.table, a.scenario)
    # CLEARED, not reused: the sink APPENDS to debezium.jsonl, so re-running one
    # scenario accumulated its own previous capture and reported a disagreement
    # against events it had already compared. Third instance of this class in the
    # harness (cross-engine, then cross-scenario, now cross-RUN) — each one wore the
    # costume of an engine defect. The dir is derived and owned here; anything worth
    # keeping is behind --work.
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)
    dbz_slot, riv_slot, pub = f"dbz_{t}", f"riv_{t}", f"pub_{t}"
    # Hyphens, NOT underscores: an underscore is illegal in a hostname per RFC 952,
    # and Debezium's http sink rejects the URL with `unsupported URI` — a message
    # that names the URI without saying which character offends. Cost three runs.
    safe = t.replace("_", "-")
    # The SCENARIO is part of the identity, same as the work dir: named from the
    # table alone, a leftover `srv-oracle-t` from the previous scenario made the
    # next one die on `Conflict. The container name is already in use` — a harness
    # failure that reads as an engine problem. Slots and publications key off the
    # table on purpose (they are server objects the scenario's own SQL manages).
    scen = a.scenario.replace("_", "-")
    sink, srv = f"sink-{safe}-{scen}", f"srv-{safe}-{scen}"
    # And remove a stale pair before starting: a run killed mid-flight (Ctrl-C, a
    # timeout) leaves them behind, and the next run must not inherit its capture.
    for c in (sink, srv):
        subprocess.run(["docker", "rm", "-f", c], capture_output=True)

    net = os.environ.get("CDC_ORACLE_NET") or NETS[a.engine]
    ckpt = os.path.join(work, "rivet.ckpt")

    def cleanup():
        sh("docker", "rm", "-f", sink, srv, check=False)
        if a.engine == "postgres":
            psql(f"DROP TABLE IF EXISTS {t}, {t}_late; DROP PUBLICATION IF EXISTS {pub}")
            psql(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                 f"WHERE slot_name IN ('{dbz_slot}','{riv_slot}')")
        elif a.engine == "mongo":
            mongo(f"db.{t}.drop(); db.{t}_late.drop()")
        elif a.engine == "mssql":
            for tt in (t, f"{t}_late"):
                mssql(f"IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_{tt}') "
                      f"EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='{tt}', "
                      f"@capture_instance='dbo_{tt}'")
                mssql(f"IF OBJECT_ID('dbo.{tt}') IS NOT NULL DROP TABLE dbo.{tt}")
        else:
            mysql(f"DROP TABLE IF EXISTS {t}, {t}_late")
        subprocess.run(["rm", "-f", ckpt], check=False)

    cleanup()
    try:
        # 1. fixture, and rivet's cursor, BEFORE any change.
        #
        # The engines pin their cursor differently and that is the whole alignment
        # question: PostgreSQL's slot is a server-side object created here, while
        # MySQL has NO server-side anchor — its checkpoint is a file, written by
        # the first run. So MySQL gets an anchoring run before the scenario, which
        # is the same guarantee reached by the other of the two documented anchor
        # models (see CLAUDE.md on per-engine anchors).
        if a.engine == "postgres":
            psql(f"CREATE TABLE {t} (id int PRIMARY KEY, v text)")
            psql(f"CREATE PUBLICATION {pub} FOR TABLE {t}")
            psql(f"SELECT pg_create_logical_replication_slot('{riv_slot}','test_decoding')")
        elif a.engine == "mongo":
            # A change stream needs the collection to EXIST before it can be
            # watched by name, so create it explicitly rather than relying on the
            # first insert — which would land before the cursor.
            mongo(f"db.createCollection('{t}')")
        elif a.engine == "mssql":
            # CDC must be enabled on the DB and the table, and the capture job needs
            # to have populated fn_cdc_get_min_lsn before either tool can read —
            # "enabled" is not "ready", the trap CLAUDE.md records for this engine.
            mssql("IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet') = 0 "
                  "EXEC sys.sp_cdc_enable_db")
            mssql(f"CREATE TABLE dbo.{t} (id int PRIMARY KEY, v varchar(50))")
            mssql(f"EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='{t}', "
                  f"@role_name=NULL, @capture_instance='dbo_{t}'")
            for _ in range(40):
                if mssql(f"SELECT sys.fn_cdc_get_min_lsn('dbo_{t}')").strip() not in ("", "NULL"):
                    break
                time.sleep(2)
            else:
                raise SystemExit("capture instance never became readable")
        else:
            mysql(f"CREATE TABLE {t} (id int PRIMARY KEY, v text)")

        # 3. sink inside the stand's network: host.docker.internal did not carry
        #    the traffic, addressing the receiver by container name does.
        subprocess.run(["cp", os.path.join(HERE, "sink.py"), work], check=True)
        sh("docker", "run", "-d", "--name", sink, "--network", net,
           "-v", f"{work}:/data", "-e", "DBZ_SINK_OUT=/data/debezium.jsonl", "-e", "DBZ_SINK_HEADERS=1",
           "python:3.12-slim", "python", "/data/sink.py")

        # Per-engine connector config. The two differ in more than credentials:
        # PostgreSQL needs a publication and a slot name; MySQL needs a server id
        # distinct from rivet's, or the two dumps evict each other.
        if a.engine == "postgres":
            connector_block = f"""debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.database.hostname={PG_HOST_IN_NET}
debezium.source.database.port=5432
debezium.source.database.user=rivet
debezium.source.database.password=rivet
debezium.source.database.dbname=rivet
debezium.source.topic.prefix=oracle
debezium.source.plugin.name=pgoutput
debezium.source.slot.name={dbz_slot}
debezium.source.publication.name={pub}
debezium.source.table.include.list=public.{t}"""
        elif a.engine == "mongo":
            connector_block = f"""debezium.source.connector.class=io.debezium.connector.mongodb.MongoDbConnector
# The replica set advertises itself as 127.0.0.1:27017, so a driver that follows
# the RS topology from inside another container dials ITSELF. directConnection
# skips topology discovery; change streams still work because the node IS an RS
# member — only the discovery step is bypassed.
debezium.source.mongodb.connection.string=mongodb://{MONGO_HOST_IN_NET}:27017/?replicaSet=rs0&directConnection=true
debezium.source.topic.prefix=oracle
debezium.source.database.include.list=rivet
debezium.source.collection.include.list=rivet.{t}
debezium.source.capture.mode=change_streams_update_full
# A Mongo DELETE carries its `_id` only in the message KEY, and the http sink
# forwards the value alone — so the body arrives as
# an empty envelope and the key is unrecoverable. ExtractNewDocumentState
# lifts the key fields INTO the value (`__key.` prefixed), which is what makes the
# delete comparable at all. Without it the harness has to exclude Mongo deletes,
# i.e. stop checking the one operation whose loss is hardest to notice downstream.
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.connector.mongodb.transforms.ExtractNewDocumentState
debezium.transforms.unwrap.add.headers=op
debezium.transforms.unwrap.add.fields=op,id
debezium.transforms.unwrap.delete.tombstone.handling.mode=rewrite"""
        elif a.engine == "mssql":
            connector_block = f"""debezium.source.connector.class=io.debezium.connector.sqlserver.SqlServerConnector
debezium.source.database.hostname={MSSQL_HOST_IN_NET}
debezium.source.database.port=1433
debezium.source.database.user=sa
debezium.source.database.password=Rivet_Passw0rd!
debezium.source.database.names=rivet
debezium.source.database.encrypt=false
debezium.source.topic.prefix=oracle
debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
debezium.source.schema.history.internal.file.filename=/data/schema-history.dat
debezium.source.table.include.list=dbo.{t}"""
        else:
            connector_block = f"""debezium.source.connector.class=io.debezium.connector.mysql.MySqlConnector
debezium.source.database.hostname={MYSQL_HOST_IN_NET}
debezium.source.database.port=3306
debezium.source.database.user=root
debezium.source.database.password=rivet
debezium.source.database.server.id=9911
debezium.source.topic.prefix=oracle
debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
debezium.source.schema.history.internal.file.filename=/data/schema-history.dat
debezium.source.table.include.list=rivet.{t}"""

        props = f"""debezium.sink.type=http
debezium.sink.http.url=http://{sink}:8088/events
debezium.format.value=json
debezium.format.value.schemas.enable=false
debezium.format.key=json
debezium.format.key.schemas.enable=false
{connector_block}
debezium.source.offset.storage.file.filename=/data/offsets.dat
debezium.source.offset.flush.interval.ms=1000
debezium.source.snapshot.mode=no_data
quarkus.log.level=WARN
"""
        # The config path is /debezium/config/ — mounting at /conf/ produces only a
        # WARNING and a process that starts and captures nothing.
        with open(os.path.join(work, "application.properties"), "w") as f:
            f.write(props)
        sh("docker", "run", "-d", "--name", srv, "--network", net,
           "-v", f"{work}/application.properties:/debezium/config/application.properties",
           "-v", f"{work}:/data", "quay.io/debezium/server:3.0.0.Final")

        # 4. wait for the connector to be STREAMING, not merely for the container
        #    to be up. Refuses rather than proceeding: a scenario run against a
        #    dead reference is the silent-agreement case this harness forbids.
        for _ in range(60):
            time.sleep(2)
            st = sh("docker", "ps", "-q", "--filter", f"name={srv}", check=False).stdout.strip()
            if not st:
                # The MESSAGE is near the start of the JSON log line and the stack
                # frames are the tail — printing the tail shows only frames and
                # diagnoses nothing, which cost two runs here.
                raw = sh("docker", "logs", srv, check=False)
                blob = (raw.stdout or "") + (raw.stderr or "")
                msgs = re.findall(r'"message"\s*:\s*"(.*?)"', blob)
                why = "\n".join(dict.fromkeys(m[:300] for m in msgs[-6:])) or blob[:600]
                raise SystemExit(f"Debezium exited before streaming:\n{why}")
            if a.engine == "postgres":
                if psql(f"SELECT active FROM pg_replication_slots "
                        f"WHERE slot_name='{dbz_slot}'") == "t":
                    break
            elif a.engine == "mongo":
                # Ask the SERVER, not the connector. Debezium writes offsets.dat
                # only after its FIRST event, and events come after this wait — a
                # deadlock by construction, and the reason the first attempt hung.
                # MongoDB reports open cursors in currentOp, and a change stream is
                # a cursor with a $changeStream pipeline stage, so its presence IS
                # the readiness signal.
                cur = mongo("db.getSiblingDB('admin').aggregate(["
                            "{$currentOp:{idleCursors:true}},"
                            "{$match:{'cursor.originatingCommand.pipeline.0.$changeStream':"
                            "{$exists:true}}}]).toArray().length")
                if cur.strip().isdigit() and int(cur.strip()) > 0:
                    break
            elif a.engine == "mssql":
                # No slot and no dump thread to ask about. The connector's progress
                # IS its offsets file, so wait for it to appear rather than for the
                # container to be up — the same distinction the other engines make
                # with `active` and the binlog dump thread.
                if os.path.getsize(os.path.join(work, "offsets.dat")) > 0 \
                        if os.path.exists(os.path.join(work, "offsets.dat")) else False:
                    break
            else:
                # MySQL exposes no per-connector flag, so ask the SERVER who is
                # dumping its binlog: Debezium shows up in SHOW PROCESSLIST as a
                # Binlog Dump thread. Waiting on the container being up instead
                # would let the scenario start against a connector that has not
                # begun reading — the silent-agreement case by another route.
                if "Binlog Dump" in mysql("SHOW PROCESSLIST"):
                    break
        else:
            raise SystemExit("Debezium never became active on its slot")

        # 5. the scenario — applied through the engine's own client
        binary = os.environ.get("RIVET_BIN", "./target/debug/rivet")
        if a.engine in ("mysql", "mssql", "mongo"):
            # MySQL has no server-side anchor: the checkpoint file IS the cursor,
            # and it is written by a run. Anchor BEFORE the scenario so both tools
            # start from the same point.
            _anchor_cfg = _write_cfg(work, a, t, ckpt, os.path.join(work, "anchor_out"))
            subprocess.run([binary, "run", "--config", _anchor_cfg],
                           capture_output=True, text=True)
        if a.engine == "mongo":
            run_scenario_mongo(a.scenario, t)
        else:
            exec_sql = {"postgres": psql, "mysql": mysql, "mssql": mssql}[a.engine]
            run_scenario(a.scenario, exec_sql, t)
        # Wait for the SOURCE to have made the changes readable, then let the
        # reference flush. A fixed sleep produced an intermittent false
        # disagreement on SQL Server: its capture job is asynchronous, and 75
        # changes did not always reach the change table within 8s — the second run
        # of the identical scenario agreed. A harness that reports its own race as
        # a finding is worse than none, so the wait is on the engine's own
        # progress marker where it has one.
        if a.engine == "mssql":
            for _ in range(30):
                if mssql(f"SELECT COUNT(*) FROM cdc.dbo_{t}_CT").strip().isdigit() and \
                   int(mssql(f"SELECT COUNT(*) FROM cdc.dbo_{t}_CT").strip()) > 0:
                    break
                time.sleep(2)
            # let the job settle past the last batch it just reported
            time.sleep(6)
        # Wait for the REFERENCE to stop producing, not for a clock. A fixed 8s
        # flush let Debezium deliver 30 of 100 changes on a 75-change SQL Server
        # scenario, and the 63 resulting `rivet-only` rows read as a finding
        # against rivet when the truth was that the reference had not finished.
        # Quiescence is the honest signal: stop when the capture has not grown for
        # three consecutive checks.
        jsonl = os.path.join(work, "debezium.jsonl")
        stable, last = 0, -1
        for _ in range(60):
            time.sleep(2)
            n = sum(1 for _ in open(jsonl)) if os.path.exists(jsonl) else 0
            stable = stable + 1 if n == last and n > 0 else 0
            last = n
            if stable >= 3:
                break
        else:
            print(f"warning: reference never quiesced (last count {last})", file=sys.stderr)

        # 6. rivet over the same window
        out = os.path.join(work, "rivet_out")
        subprocess.run(["rm", "-rf", out], check=False)
        cfg = _write_cfg(work, a, t, ckpt, out)
        r = subprocess.run([binary, "run", "--config", cfg], capture_output=True, text=True)
        if r.returncode != 0:
            print(r.stdout[-800:], r.stderr[-800:], file=sys.stderr)
            raise SystemExit("rivet run failed")

        # 7. compare — the guard inside refuses a silent capture
        return subprocess.run([sys.executable, os.path.join(HERE, "compare.py"),
                               "--rivet-dir", out,
                               "--debezium-jsonl", os.path.join(work, "debezium.jsonl"),
                               # MongoDB's key is `_id` on both sides — rivet writes
                               # it under that name and Debezium's after-document
                               # carries it likewise.
                               "--key", "_id" if a.engine == "mongo" else "id",
                               # Compare the VALUE too — ON by default as of
                               # 2026-08-25. Without it the harness is blind to cell
                               # corruption: a parquet with 100% of a column NULLed
                               # reports AGREE, measured, and says so in its own
                               # output now.
                               #
                               # It was opt-in because the DELETE path "compared
                               # unequal". Diagnosing that turned up two defects in
                               # the COMPARISON, not in either tool: (1) the two
                               # views projected columns in different orders and
                               # EXCEPT compares by POSITION, so with values on, all
                               # four rows of a four-row scenario reported as
                               # both-sides-only; (2) a DELETE carries its image in
                               # `before`, not `after`, so rivet's before-image was
                               # compared against NULL. Both fixed; PostgreSQL crud
                               # now AGREES on (op, key, v).
                               *sum((["--value", v] for v in _value_cols(a)), []),
                               *_rivet_json_column(a),
                               ]
                              # MEASURED after adding ExtractNewDocumentState: the
                              # transform flattens inserts and updates so their `_id`
                              # IS comparable, but a delete still arrives as
                              # `__deleted: true, __op: d` with no key at all. The
                              # key lives in the message KEY, which the http sink does
                              # not forward, and no transform option tried lifts it
                              # for the delete case. Excluded BY FLAG, with the reason
                              # here and in the README — an excluded op nobody wrote
                              # down is an op nobody checks.
                              + (["--exclude-op", "delete"] if a.engine == "mongo" else [])
                              ).returncode
    finally:
        if not a.keep:
            cleanup()


if __name__ == "__main__":
    sys.exit(main())
