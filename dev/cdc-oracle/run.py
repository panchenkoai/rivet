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
import argparse, json, os, re, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
NET = os.environ.get("CDC_ORACLE_NET", "rivet_default")
PG_HOST_IN_NET = os.environ.get("CDC_ORACLE_PG_HOST", "postgres-cdc")
PG_URL = os.environ.get("POSTGRES_CDC_URL", "postgresql://rivet:rivet@127.0.0.1:5434/rivet")
PG_EXEC = ["docker", "exec", os.environ.get("CDC_ORACLE_PG_CONTAINER", "rivet-postgres-cdc-1"),
           "psql", "postgresql://rivet:rivet@127.0.0.1:5432/rivet", "-tAc"]
MYSQL_HOST_IN_NET = os.environ.get("CDC_ORACLE_MYSQL_HOST", "mysql-cdc")
MYSQL_URL = os.environ.get("MYSQL_CDC_URL", "mysql://rivet:rivet@127.0.0.1:3307/rivet")
MYSQL_EXEC = ["docker", "exec", os.environ.get("CDC_ORACLE_MYSQL_CONTAINER", "rivet-mysql-cdc-1"),
              "mysql", "-uroot", "-privet", "rivet", "-N", "-e"]


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


def _write_cfg(work, a, t, ckpt, out) -> str:
    """One rivet config writer, used for both the MySQL anchoring run and the
    scenario run — two copies would drift and the anchor would stop anchoring the
    thing being measured."""
    if a.engine == "postgres":
        src, tbl = f'{{ type: postgres, url: "{PG_URL}" }}', f"public.{t}"
        cdc_opts = f"{{ slot: riv_{t}, until_current: true }}"
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
    ap.add_argument("--engine", default="postgres", choices=["postgres", "mysql"])
    ap.add_argument("--table", default="oracle_t")
    ap.add_argument("--work", default="/tmp/cdc-oracle")
    ap.add_argument("--keep", action="store_true", help="leave the stand fixtures in place")
    a = ap.parse_args()

    # Work dir is PER ENGINE. Sharing one made the MySQL connector read the
    # offsets.dat a PostgreSQL run had left behind and die with
    # `Source offset 'file' parameter is missing` — a message that names neither
    # the stale file nor the other engine. Found by running the two in sequence.
    t = a.table
    work = os.path.join(a.work, a.engine)
    os.makedirs(work, exist_ok=True)
    dbz_slot, riv_slot, pub = f"dbz_{t}", f"riv_{t}", f"pub_{t}"
    # Hyphens, NOT underscores: an underscore is illegal in a hostname per RFC 952,
    # and Debezium's http sink rejects the URL with `unsupported URI` — a message
    # that names the URI without saying which character offends. Cost three runs.
    safe = t.replace("_", "-")
    sink, srv = f"sink-{safe}", f"srv-{safe}"

    ckpt = os.path.join(work, "rivet.ckpt")

    def cleanup():
        sh("docker", "rm", "-f", sink, srv, check=False)
        if a.engine == "postgres":
            psql(f"DROP TABLE IF EXISTS {t}; DROP PUBLICATION IF EXISTS {pub}")
            psql(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                 f"WHERE slot_name IN ('{dbz_slot}','{riv_slot}')")
        else:
            mysql(f"DROP TABLE IF EXISTS {t}")
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
        else:
            mysql(f"CREATE TABLE {t} (id int PRIMARY KEY, v text)")

        # 3. sink inside the stand's network: host.docker.internal did not carry
        #    the traffic, addressing the receiver by container name does.
        subprocess.run(["cp", os.path.join(HERE, "sink.py"), work], check=True)
        sh("docker", "run", "-d", "--name", sink, "--network", NET,
           "-v", f"{work}:/data", "-e", "DBZ_SINK_OUT=/data/debezium.jsonl",
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
        sh("docker", "run", "-d", "--name", srv, "--network", NET,
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
        if a.engine == "mysql":
            # MySQL has no server-side anchor: the checkpoint file IS the cursor,
            # and it is written by a run. Anchor BEFORE the scenario so both tools
            # start from the same point.
            _anchor_cfg = _write_cfg(work, a, t, ckpt, os.path.join(work, "anchor_out"))
            subprocess.run([binary, "run", "--config", _anchor_cfg],
                           capture_output=True, text=True)
        exec_sql = psql if a.engine == "postgres" else mysql
        exec_sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b')")
        exec_sql(f"UPDATE {t} SET v='B' WHERE id=2")
        exec_sql(f"DELETE FROM {t} WHERE id=1")
        time.sleep(8)  # let the reference flush

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
                               "--key", "id"]).returncode
    finally:
        if not a.keep:
            cleanup()


if __name__ == "__main__":
    sys.exit(main())
