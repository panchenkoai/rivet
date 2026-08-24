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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="oracle_t")
    ap.add_argument("--work", default="/tmp/cdc-oracle")
    ap.add_argument("--keep", action="store_true", help="leave the stand fixtures in place")
    a = ap.parse_args()

    t, work = a.table, a.work
    os.makedirs(work, exist_ok=True)
    dbz_slot, riv_slot, pub = f"dbz_{t}", f"riv_{t}", f"pub_{t}"
    # Hyphens, NOT underscores: an underscore is illegal in a hostname per RFC 952,
    # and Debezium's http sink rejects the URL with `unsupported URI` — a message
    # that names the URI without saying which character offends. Cost three runs.
    safe = t.replace("_", "-")
    sink, srv = f"sink-{safe}", f"srv-{safe}"

    def cleanup():
        sh("docker", "rm", "-f", sink, srv, check=False)
        psql(f"DROP TABLE IF EXISTS {t}; DROP PUBLICATION IF EXISTS {pub}")
        psql(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
             f"WHERE slot_name IN ('{dbz_slot}','{riv_slot}')")

    cleanup()
    try:
        # 1. fixture + publication
        psql(f"CREATE TABLE {t} (id int PRIMARY KEY, v text)")
        psql(f"CREATE PUBLICATION {pub} FOR TABLE {t}")

        # 2. rivet's cursor before ANY change. Debezium creates ITS OWN slot at
        #    startup — pre-creating one by hand made the connector exit 1, and the
        #    alignment does not need it: what matters is that BOTH cursors exist
        #    before the first change, which the streaming wait below guarantees.
        psql(f"SELECT pg_create_logical_replication_slot('{riv_slot}','test_decoding')")

        # 3. sink inside the stand's network: host.docker.internal did not carry
        #    the traffic, addressing the receiver by container name does.
        subprocess.run(["cp", os.path.join(HERE, "sink.py"), work], check=True)
        sh("docker", "run", "-d", "--name", sink, "--network", NET,
           "-v", f"{work}:/data", "-e", "DBZ_SINK_OUT=/data/debezium.jsonl",
           "python:3.12-slim", "python", "/data/sink.py")

        props = f"""debezium.sink.type=http
debezium.sink.http.url=http://{sink}:8088/events
debezium.format.value=json
debezium.format.value.schemas.enable=false
debezium.format.key=json
debezium.format.key.schemas.enable=false
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.offset.storage.file.filename=/data/offsets.dat
debezium.source.offset.flush.interval.ms=1000
debezium.source.database.hostname={PG_HOST_IN_NET}
debezium.source.database.port=5432
debezium.source.database.user=rivet
debezium.source.database.password=rivet
debezium.source.database.dbname=rivet
debezium.source.topic.prefix=oracle
debezium.source.plugin.name=pgoutput
debezium.source.slot.name={dbz_slot}
debezium.source.publication.name={pub}
debezium.source.snapshot.mode=no_data
debezium.source.table.include.list=public.{t}
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
            active = psql(f"SELECT active FROM pg_replication_slots WHERE slot_name='{dbz_slot}'")
            if active == "t":
                break
        else:
            raise SystemExit("Debezium never became active on its slot")

        # 5. the scenario
        psql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b')")
        psql(f"UPDATE {t} SET v='B' WHERE id=2")
        psql(f"DELETE FROM {t} WHERE id=1")
        time.sleep(8)  # let the reference flush

        # 6. rivet over the same window
        out = os.path.join(work, "rivet_out")
        subprocess.run(["rm", "-rf", out], check=False)
        cfg = os.path.join(work, "rivet.yaml")
        with open(cfg, "w") as f:
            f.write(f"""source: {{ type: postgres, url: "{PG_URL}" }}
exports:
  - name: oracle
    table: public.{t}
    mode: cdc
    format: parquet
    cdc: {{ slot: {riv_slot}, until_current: true }}
    destination: {{ type: local, path: {out} }}
""")
        binary = os.environ.get("RIVET_BIN", "./target/debug/rivet")
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
