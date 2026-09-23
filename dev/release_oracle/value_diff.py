"""Source vs warehouse, value by value, through one DuckDB session.

The expected value is the SOURCE row, never a golden rivet wrote: a golden blessed
from rivet's output grades change, not correctness (it froze 16 raw UUID bytes as
"expected" for months). DuckDB reads both sides — the source through its own
scanner, BigQuery through the community extension — and each cell is reduced to a
canonical form that two correct renderings of one value share.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import re
import uuid

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def canon(v: object) -> object:
    """One canonical form per value, so a correct round trip compares equal across readers."""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, uuid.UUID):
        return v.hex
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if isinstance(v, dt.datetime):
        aware = v if v.tzinfo else v.replace(tzinfo=dt.timezone.utc)
        return ("ts", (aware - _EPOCH) // dt.timedelta(microseconds=1))
    if isinstance(v, dt.date):
        return ("date", v.isoformat())
    if isinstance(v, dt.time):
        return ("dur", 0, 0, v.hour * 3_600_000_000 + v.minute * 60_000_000 + v.second * 1_000_000 + v.microsecond)
    if isinstance(v, dt.timedelta):
        return ("dur", 0, 0, v // dt.timedelta(microseconds=1))
    if isinstance(v, decimal.Decimal):
        return ("num", format(v.normalize(), "f"))
    if isinstance(v, int):
        return ("num", str(v))
    if isinstance(v, float):
        return ("num", repr(v))
    if isinstance(v, (list, tuple)):
        # rivet lands an array as ARRAY<STRUCT<item T>> (docs/type-mapping.md): same values, one wrapper deeper.
        return [canon(x["item"]) if isinstance(x, dict) and set(x) == {"item"} else canon(x) for x in v]
    if isinstance(v, dict):
        return {str(k): canon(x) for k, x in sorted(v.items())}
    if isinstance(v, str):
        clock = re.fullmatch(r"(-?)(\d{1,3}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?", v)
        if clock:
            sign, h, m, sec, frac = clock.groups()
            micros = ((int(h) * 60 + int(m)) * 60 + int(sec)) * 1_000_000 + int((frac or "0").ljust(6, "0"))
            return ("dur", 0, 0, -micros if sign else micros)
        interval = _interval(v)
        if interval is not None:
            return ("dur", *interval)
        s = v.strip()
        if s[:1] in "{[":
            try:
                return canon(json.loads(s))
            except ValueError:
                pass
        return v
    return str(v)


def _interval(s: str) -> tuple[int, int, int] | None:
    """(months, days, microseconds) of an ISO-8601 duration (`P1Y2M3D`) or DuckDB interval text (`1 year 2 months 3 days`)."""
    iso = re.fullmatch(
        r"P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)D)?(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?[\d.]+)S)?)?", s
    )
    if iso and s != "P":
        y, mo, d, h, mi, sec = (g or "0" for g in iso.groups())
        micros = (int(h) * 3600 + int(mi) * 60) * 1_000_000 + round(float(sec) * 1_000_000)
        return (int(y) * 12 + int(mo), int(d), micros)
    parts = re.fullmatch(
        r"(?:(-?\d+) years? ?)?(?:(-?\d+) mons?(?:ths?)? ?)?(?:(-?\d+) days? ?)?(?:(-?\d+):(\d+):([\d.]+))?", s.strip()
    )
    if parts and any(parts.groups()):
        y, mo, d, h, mi, sec = (g or "0" for g in parts.groups())
        micros = (int(h) * 3600 + int(mi) * 60) * 1_000_000 + round(float(sec) * 1_000_000)
        return (int(y) * 12 + int(mo), int(d), micros)
    return None


def diff_rows(
    source: list[dict],
    dest: list[dict],
    key: str = "id",
    padded: frozenset = frozenset(),
    bits: frozenset = frozenset(),
) -> list[str]:
    """Every column whose values differ between source and destination, keyed by `key`, with one example each; `padded` names CHAR(n) columns (trailing blanks insignificant), `bits` names BIT(n) columns (bytes read as an unsigned integer)."""
    src = {canon(r[key]): r for r in source}
    dst = {canon(r[key]): r for r in dest}
    out: list[str] = []
    missing = sorted(map(str, set(src) - set(dst)))
    extra = sorted(map(str, set(dst) - set(src)))
    if missing:
        out.append(f"rows missing from the warehouse: {missing[:5]}")
    if extra:
        out.append(f"rows only in the warehouse: {extra[:5]}")
    columns = sorted(set().union(*(r.keys() for r in source)) if source else set())
    for col in columns:
        for k in sorted(set(src) & set(dst), key=str):
            if col not in dst[k]:
                out.append(f"column `{col}` is not in the warehouse table")
                break
            a, b = src[k].get(col), dst[k].get(col)
            if col in padded and isinstance(a, str) and isinstance(b, str):
                a, b = a.rstrip(" "), b.rstrip(" ")
            if col in bits and isinstance(a, (bytes, bytearray)):
                a = int.from_bytes(a, "big")
            a, b = canon(a), canon(b)
            if a != b:
                out.append(f"`{col}` differs at {key}={k}: source {src[k].get(col)!r} vs warehouse {dst[k].get(col)!r}")
                break
    return out


def fetch(ora, sql: str) -> list[dict]:
    """Rows of `sql` over the oracle's DuckDB session, as dicts."""
    rel = ora.db.sql(sql)
    names = rel.columns
    return [dict(zip(names, row)) for row in rel.fetchall()]


def invalid_text_columns(ora, relation: str) -> list[str]:
    """The text columns of `relation` holding bytes that are not UTF-8 — a STRING the warehouse cannot render."""
    bad = []
    for name, typ, *_ in ora.db.sql(f"DESCRIBE {relation}").fetchall():
        if str(typ) != "VARCHAR":
            continue
        try:
            ora.db.sql(f"SELECT {name} FROM {relation}").fetchall()
        except Exception as e:  # noqa: BLE001 — the decode failure IS the finding
            if "unicode" in str(e).lower():
                bad.append(name)
    return bad


def source_select(ora, relation: str) -> str:
    """`SELECT *` over `relation`, with INTERVAL columns read as text (DuckDB's Python conversion folds months into days)."""
    described = ora.db.sql(f"DESCRIBE {relation}").fetchall()
    intervals = [row[0] for row in described if str(row[1]).startswith("INTERVAL")]
    replace = ", ".join(f"CAST({c} AS VARCHAR) AS {c}" for c in intervals)
    replace_clause = f"REPLACE ({replace}) " if replace else ""
    return f"SELECT * {replace_clause}FROM {relation} ORDER BY id"


def pg_padded_columns(ora, table: str) -> frozenset:
    """The CHAR(n) columns of a PostgreSQL table attached as `pg`, from its own catalog."""
    inner = (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' AND data_type = 'character'"
    ).replace("'", "''")
    rows = ora.db.sql(f"SELECT * FROM postgres_query('pg', '{inner}')").fetchall()
    return frozenset(r[0] for r in rows)


def mysql_bit_columns(ora, database: str, table: str) -> frozenset:
    """The BIT(n) columns of a MySQL table attached as `my`, from its own catalog."""
    inner = (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{database}' AND table_name = '{table}' AND data_type = 'bit'"
    ).replace("'", "''")
    rows = ora.db.sql(f"SELECT * FROM mysql_query('my', '{inner}')").fetchall()
    return frozenset(r[0] for r in rows)


def source_attach(engine: str, url: str) -> tuple[dict, str]:
    """The `Oracle` keyword that attaches `url`, and the DuckDB schema prefix its tables live under."""
    from urllib.parse import urlparse

    u = urlparse(url)
    db = u.path.lstrip("/")
    if engine == "postgres":
        return {"postgres": url}, "pg.public"
    if engine == "mysql":
        dsn = f"host={u.hostname} port={u.port} user={u.username} password={u.password} database={db}"
        return {"mysql": dsn}, f"my.{db}"
    if engine == "mssql":
        return {"mssql": "mssql://" + url.split("://", 1)[1]}, "ms.dbo"
    if engine == "mongo":
        return {"mongo": f"mongodb://{u.netloc}"}, f"mg.{db}"
    raise ValueError(f"no DuckDB source attach for {engine}")


def tuple_mismatches(ora, source: str, dest: str) -> tuple[int, list[str]]:
    """(rows differing both ways, source columns absent at the destination), the source strictly CAST to the destination's column types — never TRY_CAST, which would read an uncastable value as NULL."""
    dst_types = {r[0]: r[1] for r in ora.db.sql(f"DESCRIBE SELECT * FROM {dest}").fetchall()}
    src_cols = [r[0] for r in ora.db.sql(f"DESCRIBE SELECT * FROM {source}").fetchall()]
    shared = [c for c in src_cols if c in dst_types]
    missing = [c for c in src_cols if c not in dst_types]
    s_proj = ", ".join(f'CAST("{c}" AS {dst_types[c]}) AS "{c}"' for c in shared)
    d_proj = ", ".join(f'"{c}"' for c in shared)
    n = ora.db.sql(
        f"WITH s AS (SELECT {s_proj} FROM {source}), d AS (SELECT {d_proj} FROM {dest}) "
        "SELECT (SELECT count(*) FROM (SELECT * FROM s EXCEPT ALL SELECT * FROM d)) + "
        "(SELECT count(*) FROM (SELECT * FROM d EXCEPT ALL SELECT * FROM s))"
    ).fetchone()[0]
    return n, missing


def mongo_document_columns(ora, source: str, dest: str) -> str:
    """`dest` (rivet's `_id` + extended-JSON `document`) unpacked into the source's columns and types, so `tuple_mismatches` can compare them."""
    cols = ora.db.sql(f"DESCRIBE SELECT * FROM {source}").fetchall()
    proj = []
    for name, typ, *_ in cols:
        if name == "_id":
            proj.append(f'CAST("_id" AS {typ}) AS "_id"')
            continue
        cell = f"document->'{name}'"
        text = f"""CASE WHEN json_type({cell}) = 'OBJECT' THEN {cell}->>'$."$date"' ELSE document->>'{name}' END"""
        proj.append(f'CAST({text} AS {typ}) AS "{name}"')
    return f"(SELECT {', '.join(proj)} FROM {dest})"


def compare_to_parquet(engine: str, url: str, table: str, preamble: str, dest: str) -> tuple[int, list[str]]:
    """`tuple_mismatches` between the source table and a parquet relation (`preamble` sets up its store)."""
    from .duck import Oracle

    attach, prefix = source_attach(engine, url)
    with Oracle(**attach) as ora:
        if preamble:
            ora.db.sql(preamble)
        source = f"{prefix}.{table}"
        if engine == "mongo":
            dest = mongo_document_columns(ora, source, dest)
        return tuple_mismatches(ora, source, dest)


def chain_census(
    engine: str, url: str, table: str, bucket: str, prefix: str, state: str, dataset: str, wh_table: str
) -> dict:
    """Source, declared manifests, parquet footers in GCS, rivet's ledger and BigQuery, counted in ONE DuckDB session for one run."""
    from .duck import Oracle

    attach, src_prefix = source_attach(engine, url)
    root = f"gs://{bucket}/{prefix.strip('/')}"
    with Oracle(bigquery=True, gcs=True, **attach) as ora:
        if state.startswith("postgres"):
            ora.db.sql(f"ATTACH '{state}' AS st (TYPE postgres, READ_ONLY)")
        else:
            ora.db.sql(f"INSTALL sqlite; LOAD sqlite; ATTACH '{state}' AS st (TYPE sqlite, READ_ONLY)")
        mans = fetch(ora, f"SELECT * FROM read_json_auto('{root}/manifest-*.json', union_by_name = true)")
        from .scenarios import success_part_names

        ok = [m for m in mans if str(m.get("status") or "success").lower() == "success"]
        run_ids = sorted({m["run_id"] for m in ok})
        declared = sorted({
            n if n.startswith("gs://") else f"{root}/{n}" for m in ok for n in success_part_names(m)
        })
        held = sorted(r[0] for r in ora.db.sql(f"SELECT file FROM glob('{root}/**/*.parquet')").fetchall())
        in_list = ", ".join(f"'{r}'" for r in run_ids) or "NULL"
        lst = ", ".join(f"'{d}'" for d in declared)
        one = lambda sql: ora.db.sql(sql).fetchone()[0]  # noqa: E731
        return {
            "run_ids": run_ids,
            "source": one(f"SELECT count(*) FROM {src_prefix}.{table}"),
            "manifest": sum(int(m.get("row_count") or 0) for m in ok),
            "footers": one(f"SELECT coalesce(sum(num_rows), 0) FROM parquet_file_metadata([{lst}])") if declared else 0,
            "undeclared": sorted(set(held) - set(declared)),
            "missing": sorted(set(declared) - set(held)),
            "metrics": one(f"SELECT coalesce(sum(total_rows), 0) FROM st.export_metrics WHERE run_id IN ({in_list})"),
            "file_log": one(f"SELECT coalesce(sum(row_count), 0) FROM st.file_log WHERE run_id IN ({in_list})"),
            "loaded": one(
                "SELECT coalesce(sum(rows_loaded), 0) FROM st.load_run WHERE "
                + " OR ".join(f"source_run_ids LIKE '%{r}%'" for r in run_ids or ["\x00"])
            ),
            "warehouse": one(f"SELECT count(*) FROM bq.{dataset}.{wh_table}"),
        }


def chain_disagreements(c: dict) -> list[str]:
    """What is wrong with a `chain_census`, empty when every point agrees."""
    out = []
    if len(c["run_ids"]) != 1:
        out.append(f"expected ONE successful run's manifest, found {c['run_ids']}")
    counts = {k: c[k] for k in ("source", "manifest", "footers", "metrics", "file_log", "loaded", "warehouse")}
    if len(set(counts.values())) != 1:
        out.append(f"counts disagree: {counts}")
    if not c["source"]:
        out.append("the source is empty — a census of nothing agrees with everything")
    if c["undeclared"]:
        out.append(f"parquet in the bucket that no manifest declares: {c['undeclared'][:3]}")
    if c["missing"]:
        out.append(f"parts a manifest declares that the bucket does not hold: {c['missing'][:3]}")
    return out


def compare_to_bigquery(
    engine: str, url: str, dataset: str, table: str, warehouse_table: str | None = None
) -> tuple[int, list[str]]:
    """(warehouse rows, differences) between the source table and its BigQuery load (`warehouse_table`, default the same name), read by DuckDB on both sides."""
    from .duck import Oracle

    attach, prefix = source_attach(engine, url)
    with Oracle(bigquery=True, **attach) as ora:
        return _rows_against_source(ora, engine, prefix, table, f"bq.{dataset}.{warehouse_table or table}")


def compare_rows_to_parquet(engine: str, url: str, table: str, parquet: str) -> tuple[int, list[str]]:
    """(delivered rows, differences) between the source table and a `read_parquet(...)` relation, every value in canonical form."""
    from .duck import Oracle

    attach, prefix = source_attach(engine, url)
    with Oracle(**attach) as ora:
        return _rows_against_source(ora, engine, prefix, table, f"(SELECT * FROM {parquet})")


def _rows_against_source(ora, engine: str, prefix: str, table: str, dest: str) -> tuple[int, list[str]]:
    """`diff_rows` of the source table against `dest` in one session; unreadable text is a finding."""
    bad = invalid_text_columns(ora, dest)
    if bad:
        return 0, [f"STRING column(s) {bad} hold bytes that are not UTF-8 — unreadable as text"]
    src = fetch(ora, source_select(ora, f"{prefix}.{table}"))
    dst = fetch(ora, f"SELECT * FROM {dest} ORDER BY id")
    padded = pg_padded_columns(ora, table) if engine == "postgres" else frozenset()
    bits = mysql_bit_columns(ora, prefix.split(".", 1)[1], table) if engine == "mysql" else frozenset()
    return len(dst), diff_rows(src, dst, padded=padded, bits=bits)


def _self_test() -> None:
    u = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011")
    assert canon(u) == canon(u.bytes), "a UUID and its 16 bytes are one value"
    t = dt.datetime(2035, 8, 7, 9, 8, 7, 987654)
    assert canon(t) == canon(t.replace(tzinfo=dt.timezone.utc))
    assert canon(t) != canon(t.replace(microsecond=0)), "microseconds must count"
    assert canon(decimal.Decimal("1.50")) == canon(decimal.Decimal("1.5"))
    assert canon(decimal.Decimal("100")) == canon(100)
    assert canon('{"b":1,"a":2}') == canon({"a": 2, "b": 1})
    assert canon(dt.time(4, 0)) != canon(dt.timedelta(hours=100)), "a wrapped TIME is a difference"
    assert diff_rows([{"id": 1, "u": u}], [{"id": 1, "u": b"\x00" * 16}]), "garbage bytes are a finding"
    assert not diff_rows([{"id": 1, "u": u}], [{"id": 1, "u": u.bytes}])
    assert canon("P1Y2M3D") == canon("1 year 2 months 3 days")
    assert canon("P1Y2M3D") != canon("P1Y2M4D")
    assert canon([1, 2]) == canon([{"item": 1}, {"item": 2}])
    assert not diff_rows([{"id": 1, "c": "a"}], [{"id": 1, "c": "a  "}], padded=frozenset({"c"}))
    assert diff_rows([{"id": 1, "c": "a"}], [{"id": 1, "c": "a  "}]), "varchar blanks count"
    assert canon("09:08:07.987654") == canon(dt.time(9, 8, 7, 987654))
    assert canon("100:00:00") != canon(dt.time(4, 0)), "a MySQL TIME past 24h must not match its wrap"
    assert canon("-01:30:00") == canon(-dt.timedelta(hours=1, minutes=30))
    assert not diff_rows([{"id": 1, "b": b"\xff"}], [{"id": 1, "b": 255}], bits=frozenset({"b"}))
    assert canon("00:00:00") == canon("PT0S"), "a zero interval read as clock text or as ISO is one value"
    assert canon("P1D") != canon("24:00:00"), "one day and 24 hours differ in PostgreSQL interval semantics"
    good = {"run_ids": ["r"], "source": 3, "manifest": 3, "footers": 3, "metrics": 3, "file_log": 3,
            "loaded": 3, "warehouse": 3, "undeclared": [], "missing": []}
    assert not chain_disagreements(good)
    for k in ("manifest", "footers", "metrics", "file_log", "loaded", "warehouse"):
        assert chain_disagreements({**good, k: 4}), f"a wrong {k} must be a disagreement"
    assert chain_disagreements({**good, "undeclared": ["x"]})
    assert chain_disagreements({**good, "run_ids": []})
    assert chain_disagreements({**good, **{k: 0 for k in good if k not in ("run_ids", "undeclared", "missing")}})
    from .scenarios import success_part_names

    parts = [{"path": "a.parquet", "status": "committed"}, {"path": "b.parquet", "status": "rejected"}]
    assert success_part_names({"status": "success", "parts": parts}) == ["a.parquet"]
    for st in ("failed", "interrupted", "running", "Failed"):
        assert success_part_names({"status": st, "parts": parts}) == [], f"a {st} manifest delivers nothing"
    print("value_diff self-test ok")


def _cli(argv: list[str]) -> int:
    """`bigquery <engine> <source-url> <dataset> <source-table> <warehouse-table>` → JSON `{rows, diffs}` on stdout."""
    import sys

    if argv[:1] != ["bigquery"] or len(argv) != 6:
        print("usage: value_diff bigquery <engine> <url> <dataset> <table> <warehouse_table>", file=sys.stderr)
        return 2
    rows, diffs = compare_to_bigquery(*argv[1:])
    print(json.dumps({"rows": rows, "diffs": diffs}))
    return 0


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        raise SystemExit(_cli(sys.argv[1:]))
    _self_test()
